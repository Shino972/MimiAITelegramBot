import asyncio
import aiosqlite
import html
import re
import json
import os
import time
import uuid
import logging
import io
from typing import List, Set, Dict, Optional, Deque
from collections import deque
from datetime import datetime, timedelta

import random
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from PIL import Image
from dateutil.relativedelta import relativedelta
from aiohttp import ClientSession
from PyCharacterAI import get_client
from PyCharacterAI.exceptions import SessionClosedError

from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ChatMemberStatus, ChatAction
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    CallbackQuery, 
    Message, 
    LabeledPrice, 
    PreCheckoutQuery,
    BufferedInputFile,
    FSInputFile,
    ChatPermissions
)
from aiogram.filters import BaseFilter
from aiogram.types import InlineQuery, InlineQueryResultArticle, InputTextMessageContent
from aiogram.utils.markdown import hbold
import traceback

logging.basicConfig(level=logging.INFO)

router = Router()

DB_NAME = 'database.db'
REQUIRED_MESSAGES = 1
ADMIN_USER_ID = 7395604316
STICKER_IDS = []

LOCAL_GIFS = {}

async def init_db():
    """Инициализация базы данных"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS message_history (
                chat_id INTEGER,
                user_id INTEGER,
                target_user_id INTEGER DEFAULT NULL,
                message_text TEXT,
                timestamp INTEGER,
                PRIMARY KEY (chat_id, timestamp)
            )
        ''')
        
        await db.execute('''
            CREATE TABLE IF NOT EXISTS words (
                chat_id INTEGER,
                word TEXT,
                timestamp INTEGER,
                PRIMARY KEY (chat_id, word)
            )
        ''')
        
        await db.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                chat_id INTEGER PRIMARY KEY,
                message_count INTEGER DEFAULT 0,
                joined_timestamp INTEGER,
                title TEXT,
                is_active BOOLEAN DEFAULT TRUE
            )
        ''')
        
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                joined_timestamp INTEGER
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS media_tracking (
                chat_id INTEGER PRIMARY KEY,
                last_send_timestamp INTEGER
            )
        ''')
        await db.commit()
        
logger = logging.getLogger(__name__)

async def should_send_daily_media(chat_id: int) -> bool:
    """Проверяет, нужно ли отправлять медиа в группу сегодня"""
    current_date = datetime.now().date()
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Get last send timestamp
        cursor = await db.execute(
            'SELECT last_send_timestamp FROM media_tracking WHERE chat_id = ?',
            (chat_id,)
        )
        result = await cursor.fetchone()
        
        if not result:
            return True
            
        last_timestamp = result[0]
        last_date = datetime.fromtimestamp(last_timestamp).date()
        
        return last_date < current_date

async def update_last_media_timestamp(chat_id: int):
    """Обновляет timestamp последней отправки медиа"""
    current_timestamp = int(datetime.now().timestamp())
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT OR REPLACE INTO media_tracking (chat_id, last_send_timestamp)
            VALUES (?, ?)
        ''', (chat_id, current_timestamp))
        await db.commit()

async def send_random_daily_media(message: types.Message):
    """Отправляет случайное медиа в группу"""
    try:
        if not await should_send_daily_media(message.chat.id):
            return
            
        # 50/50 chance for sticker or GIF
        if random.random() < 0.5:
            # Отправка стикера
            sticker_id = random.choice(STICKER_IDS)
            await message.bot.send_sticker(
                chat_id=message.chat.id,
                sticker=sticker_id
            )
        else:
            # Отправка локального GIF
            random_gif_path = random.choice(list(LOCAL_GIFS.values()))
            
            try:
                # Проверяем существование файла
                if not os.path.exists(random_gif_path):
                    print(f"File not found: {random_gif_path}")
                    return
                    
                # Создаем FSInputFile для локального файла
                gif = FSInputFile(random_gif_path)
                
                # Отправляем анимацию
                await message.bot.send_animation(
                    chat_id=message.chat.id,
                    animation=gif
                )
            except Exception as e:
                print(f"Error sending GIF.")
            
        await update_last_media_timestamp(message.chat.id)
        
    except Exception as e:
        print(f"Error sending daily media.")

async def save_message_history(chat_id: int, user_id: int, message_text: str, target_user_id: Optional[int] = None):
    """Сохранение сообщения в историю и обновление счетчика"""
    current_time = datetime.now()
    current_timestamp = int(current_time.timestamp() * 1000000)

    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute('''
                INSERT INTO message_history (chat_id, user_id, target_user_id, message_text, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (chat_id, user_id, target_user_id, message_text, current_timestamp))

            # Обновляем счетчик сообщений для группы
            await db.execute('''
                UPDATE groups 
                SET message_count = message_count + 1 
                WHERE chat_id = ?
            ''', (chat_id,))

            await db.commit()
        except Exception as e:
            if "UNIQUE constraint failed" in str(e):
                current_timestamp += random.randint(1, 1000)
                await db.execute('''
                    INSERT INTO message_history (chat_id, user_id, target_user_id, message_text, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                ''', (chat_id, user_id, target_user_id, message_text, current_timestamp))

                await db.execute('''
                    UPDATE groups 
                    SET message_count = message_count + 1 
                    WHERE chat_id = ?
                ''', (chat_id,))

                await db.commit()
            else:
                raise e

async def save_words(chat_id: int, text: str):
    """Сохранение слов для конкретной группы"""
    words = set(re.findall(r'\b\w+\b', text.lower()))
    current_timestamp = int(datetime.now().timestamp())
    
    async with aiosqlite.connect(DB_NAME) as db:
        for word in words:
            await db.execute('''
                INSERT OR REPLACE INTO words (chat_id, word, timestamp)
                VALUES (?, ?, ?)
            ''', (chat_id, word, current_timestamp))
        await db.commit()

async def get_group_stats(chat_id: int) -> Dict:
    """Получение статистики группы"""
    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем количество сообщений
        async with db.execute(
            'SELECT message_count FROM groups WHERE chat_id = ?',
            (chat_id,)
        ) as cursor:
            message_count = await cursor.fetchone()
            message_count = message_count[0] if message_count else 0

        # Получаем количество слов
        async with db.execute(
            'SELECT COUNT(DISTINCT word) FROM words WHERE chat_id = ?',
            (chat_id,)
        ) as cursor:
            word_count = await cursor.fetchone()
            word_count = word_count[0] if word_count else 0

        return {
            'messages': message_count,
            'words': word_count
        }

@router.my_chat_member()
async def on_chat_member_updated(event: types.ChatMemberUpdated):
    if event.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:

        current_timestamp = int(datetime.now().timestamp())
            
        await event.bot.send_sticker(
            chat_id=event.chat.id,
            sticker="CAACAgIAAxkBAAENh3Jni7bLzswV2_T4tZS8g0u51ftBSwACIlQAAi5qMUhTl9wP9WT12DYE"
        )
                
        await event.bot.send_message(
            chat_id=event.chat.id,
            text=f"""
            ❤️ Благодарю админа, что добавил меня в свою милую чат-группу!\n\n😘 Чтобы я начал отвечать, нужно набрать минимум {REQUIRED_MESSAGES} сообщений. А затем я уже начинаю отвечать. 💕\n\n🍼 Используйте /stats – Для показа статистику группы.\n💞 Используйте /help – Для добавление меня в свою группу.
            """
        )

async def get_daily_message_stats(chat_id: int) -> List[Dict]:
    """Получение статистики сообщений по дням"""
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('''
            SELECT 
                date(datetime(timestamp/1000000, 'unixepoch')) as date,
                COUNT(*) as message_count
            FROM message_history 
            WHERE chat_id = ?
            GROUP BY date
            ORDER BY date
        ''', (chat_id,)) as cursor:
            daily_stats = await cursor.fetchall()
            return daily_stats

async def create_stats_image(chat_id: int, stats: Dict) -> BufferedInputFile:
    """Создание изображения со статистикой"""
    # Получаем данные по дням
    daily_stats = await get_daily_message_stats(chat_id)
    
    # Преобразуем данные для графика
    dates = [datetime.strptime(str(row[0]), '%Y-%m-%d') for row in daily_stats]
    counts = [row[1] for row in daily_stats]
    
    # Создаем график
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor('#1a1a2e')
    ax.set_facecolor('#1a1a2e')
    
    # Настраиваем стиль графика
    ax.grid(True, linestyle='--', alpha=0.2, color='#2d374d')
    
    # Рисуем основную линию с неоновым эффектом
    ax.plot(dates, counts, '-', color='#00ff9d', linewidth=2, alpha=0.8)
    
    # Добавляем точки с неоновым свечением
    ax.scatter(dates, counts, color='#00ff9d', s=50, alpha=1, 
              zorder=5, edgecolor='white', linewidth=1)
    
    # Настраиваем оси
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    
    # Стилизуем подписи осей
    ax.tick_params(axis='both', colors='#8884d8')
    
    # Добавляем легкое свечение линиям
    for spine in ax.spines.values():
        spine.set_edgecolor('#8884d8')
        spine.set_linewidth(1)
    
    # Поворачиваем подписи дат для лучшей читаемости
    plt.xticks(rotation=45)
    
    # Добавляем отступы
    plt.tight_layout()
    
    # Сохраняем график в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight',
                facecolor='#1a1a2e', edgecolor='none')
    buf.seek(0)
    plt.close()
    
    # Создаем BufferedInputFile для отправки через aiogram
    return BufferedInputFile(buf.getvalue(), filename="stats.png")

@router.message(Command("stats"))
async def stats_handler(message: types.Message):
    """Обработчик команды /stats"""
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        # Получаем статистику
        stats = await get_group_stats(message.chat.id)
        user_id = message.from_user.id
        
        # Формируем подпись заранее
        caption = (
            f"📊 <b>Статистика группы:</b>\n"
            f"🤍 Сообщений получено: <code>{stats['messages']}</code>\n"
            f"💌 Слов собрано: <code>{stats['words']}</code>"
        )
        
        if stats['messages'] < REQUIRED_MESSAGES:
            caption += f"\n🤍 До активации <b>Mimi Typh</b> осталось: <code>{REQUIRED_MESSAGES - stats['messages']}</code> сообщений."
        
        try:
            # Создаем изображение
            image = await create_stats_image(message.chat.id, stats)
            
            # Отправляем фото с подписью
            await message.answer_photo(
                photo=image,
                caption=caption
            )
            
        except Exception as e:
            print(f"Error creating stats image: {e}")
            # Теперь caption доступна и в except блоке
            await message.answer(caption)

# Состояния для FSM
class AdminStates(StatesGroup):
    CONFIRM_USER_DELETE = State()

@router.message(Command("admin"))
async def handle_admin_command(message: Message, state: FSMContext):
    """Обработчик команды /admin"""
    if message.from_user.id != ADMIN_USER_ID:
        return

    # Получение статистики
    async with aiosqlite.connect(DB_NAME) as db:
        # Общее количество пользователей
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cursor.fetchone())[0]

        # Активные группы
        cursor = await db.execute("SELECT COUNT(*) FROM groups WHERE is_active = TRUE")
        active_groups = (await cursor.fetchone())[0]

    text = (
        f"🔐 <b>Админ-панель</b>\n\n"
        f"👥 Всего пользователей: <code>{total_users}</code>\n"
        f"💬 Активных групп: <code>{active_groups}</code>\n\n"
    )

    # Создание клавиатуры
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить DB пользователя", callback_data="admin_delete_user")]
    ])
    await message.answer(text, reply_markup=keyboard)

@router.callback_query(F.data == "admin_delete_user")
async def start_delete_user(callback: CallbackQuery, state: FSMContext):
    """Начало процесса удаления пользователя"""
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("❌ Только для администратора!", show_alert=True)
        return
    
    await callback.message.answer(
        "🗑 <b>Удаление пользователя</b>\n\n"
        "Введите ID пользователя или перешлите сообщение от него:"
    )
    await state.set_state(AdminStates.CONFIRM_USER_DELETE)
    await callback.answer()

# Обработчик для ввода ID пользователя
@router.message(AdminStates.CONFIRM_USER_DELETE)
async def process_user_for_deletion(message: Message, state: FSMContext, bot: Bot):
    user_id = None
    
    # Если это пересланное сообщение
    if message.forward_from:
        user_id = message.forward_from.id
    # Или если введен ID вручную
    elif message.text and message.text.isdigit():
        user_id = int(message.text)
    else:
        await message.answer("❌ Некорректный ввод. Отправьте ID пользователя или перешлите его сообщение.")
        return
    
    try:
        # Получаем информацию о пользователе
        user = await bot.get_chat(user_id)
        
        # Получаем статистику пользователя из БД
        async with aiosqlite.connect(DB_NAME) as db:
            # Количество сообщений
            cursor = await db.execute(
                "SELECT COUNT(*) FROM message_history WHERE user_id = ?",
                (user_id,))
            message_count = (await cursor.fetchone())[0]
            
            # Количество групп
            cursor = await db.execute(
                "SELECT COUNT(DISTINCT chat_id) FROM message_history WHERE user_id = ?",
                (user_id,))
            groups_count = (await cursor.fetchone())[0]
            
            # Дата регистрации
            cursor = await db.execute(
                "SELECT joined_timestamp FROM users WHERE user_id = ?",
                (user_id,))
            join_data = await cursor.fetchone()
            join_date = datetime.fromtimestamp(join_data[0]).strftime('%d.%m.%Y') if join_data else "неизвестно"
        
        # Формируем информационную карточку
        user_card = (
            f"👤 <b>Информация о пользователе</b>\n\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"📛 Имя: {html.escape(user.first_name)}\n"
            f"👤 Username: @{user.username if user.username else 'нет'}\n"
            f"📅 Зарегистрирован: {join_date}\n\n"
            f"📊 Статистика:\n"
            f"✉️ Сообщений: {message_count}\n"
            f"💬 Групп: {groups_count}\n\n"
            f"<b>❗️ Внимание! Это действие удалит ВСЕ данные пользователя без возможности восстановления!</b>"
        )
        
        # Создаем клавиатуру подтверждения
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить удаление",
                    callback_data=f"confirm_delete_{user_id}"
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="cancel_delete"
                )
            ]
        ])
        
        # Сохраняем ID пользователя в состоянии
        await state.update_data(user_id=user_id)
        
        # Отправляем карточку с кнопками подтверждения
        await message.answer(user_card, reply_markup=keyboard)
        
    except Exception as e:
        error_msg = (
            f"❌ Ошибка получения информации о пользователе:\n"
            f"<code>{html.escape(str(e))}</code>\n\n"
            f"Проверьте правильность введенных данных."
        )
        await message.answer(error_msg)
        await state.clear()

# Обработчик подтверждения удаления
@router.callback_query(F.data.startswith("confirm_delete_"))
async def confirm_user_deletion(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("❌ Только для администратора!", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[2])
    
    try:
        # Получаем информацию о пользователе для логов
        user = await bot.get_chat(user_id)
        
        # Удаляем все данные пользователя
        async with aiosqlite.connect(DB_NAME) as db:
            # Удаляем из всех таблиц
            await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM message_history WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM last_button_press WHERE user_id = ?", (user_id,))
            await db.commit()
            
            # Проверяем, что удаление прошло успешно
            cursor = await db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
            if await cursor.fetchone():
                raise Exception("Не удалось удалить пользователя из базы данных")
        
        # Формируем сообщение об успехе
        success_msg = (
            f"✅ Пользователь <code>{user_id}</code> успешно удален!\n\n"
            f"📛 Имя: {html.escape(user.first_name)}\n"
            f"👤 Username: @{user.username if user.username else 'нет'}\n\n"
            f"Все данные безвозвратно удалены из системы."
        )
        
        # Удаляем сообщение с подтверждением
        await callback.message.delete()
        
        # Отправляем сообщение об успехе
        await callback.message.answer(success_msg)
        
        # Логируем действие
        logger.warning(f"Администратор удалил пользователя {user_id} ({user.first_name})")
        
    except Exception as e:
        error_msg = (
            f"❌ Ошибка при удалении пользователя <code>{user_id}</code>:\n"
            f"<code>{html.escape(str(e))}</code>"
        )
        await callback.message.edit_text(error_msg)
    
    await state.clear()
    await callback.answer()

# Обработчик отмены удаления
@router.callback_query(F.data == "cancel_delete")
async def cancel_user_deletion(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Удаление пользователя отменено.")
    await state.clear()
    await callback.answer()

standard_triggers = {'мими', 'mimi', 'МИМИ', 'MIMI', 'Мими', 'Mimi'}

class MessageTracker:
    def __init__(self, message_limit: int = 3, time_window: int = 5):
        self.message_limit = message_limit
        self.time_window = time_window  # в секундах
        self.user_messages: Dict[int, deque] = {}
        
    def is_spam(self, user_id: int) -> bool:
        if user_id not in self.user_messages:
            self.user_messages[user_id] = deque()
            
        messages = self.user_messages[user_id]
        current_time = datetime.now()
        
        # Удаляем старые сообщения за пределами временного окна
        while messages and (current_time - messages[0]) > timedelta(seconds=self.time_window):
            messages.popleft()
            
        # Проверяем, не превышен ли лимит сообщений
        if len(messages) >= self.message_limit:
            logger.warning(f"Spam detected from user {user_id}: {len(messages)} messages in {self.time_window} seconds")
            return True
            
        # Добавляем новое сообщение в историю
        messages.append(current_time)
        return False

class ConnectionPool:
    def __init__(self, api_key: str, pool_size: int = 5):
        self.api_key = api_key
        self.pool_size = pool_size
        self.connections: List = []  # Здесь будут храниться клиенты новой API
        self.locks: List[asyncio.Lock] = []
        self.initialized = False
        self.init_lock = asyncio.Lock()
        
    async def initialize(self):
        if self.initialized:
            return
            
        async with self.init_lock:
            if self.initialized:  # Double check
                return
                
            for _ in range(self.pool_size):
                client = await get_client(token=self.api_key)
                await client.account.fetch_me()  # Проверяем подключение
                self.connections.append(client)
                self.locks.append(asyncio.Lock())
                
            self.initialized = True
            logger.info(f"Initialized connection pool with {self.pool_size} connections")
    
    async def get_connection(self):
        await self.initialize()
        
        while True:
            for i, lock in enumerate(self.locks):
                if not lock.locked():
                    return i, self.connections[i], lock
            await asyncio.sleep(0.1)

class ChatManager:
    def __init__(self, api_key: str, char_id: str, pool_size: int = 5, 
                 min_delay: float = 3.0, max_delay: float = 5.0,
                 message_limit: int = 3, time_window: int = 5):
        self.char_id = char_id
        self.pool = ConnectionPool(api_key, pool_size)
        self.user_chats: Dict[int, dict] = {}
        self.chat_locks: Dict[int, asyncio.Lock] = {}
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.message_tracker = MessageTracker(message_limit, time_window)

    async def send_message(self, user_id: int, message: str) -> str:
        # Проверяем на спам
        if self.message_tracker.is_spam(user_id):
            logger.warning(f"Ignoring message from user {user_id} due to spam protection")
            return None
            
        # Добавляем случайную задержку перед отправкой
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.info(f"Waiting {delay:.1f} seconds before processing message for user {user_id}")
        await asyncio.sleep(delay)
        
        # Создаем lock для пользователя если его нет
        if user_id not in self.chat_locks:
            self.chat_locks[user_id] = asyncio.Lock()
            
        try:
            # Получаем соединение из пула
            conn_id, client, conn_lock = await self.pool.get_connection()
            
            async with conn_lock:  # Блокируем соединение
                try:
                    # Проверяем существует ли чат для пользователя
                    if user_id not in self.user_chats:
                        # Создаем новый чат с новой API
                        chat, greeting_message = await client.chat.create_chat(self.char_id)
                        self.user_chats[user_id] = {
                            'chat_id': chat.chat_id,
                            'last_activity': datetime.now()
                        }
                        logger.info(f"Created new chat for user {user_id}")
                    
                    chat_data = self.user_chats[user_id]
                    
                    # Отправляем сообщение с новой API
                    answer = await client.chat.send_message(
                        self.char_id,
                        chat_data['chat_id'],
                        message
                    )
                    
                    chat_data['last_activity'] = datetime.now()
                    return answer.get_primary_candidate().text
                    
                except SessionClosedError as e:
                    logger.error(f"Session closed for user {user_id}: {e}")
                    # Удаляем чат при ошибке, чтобы он пересоздался
                    if user_id in self.user_chats:
                        del self.user_chats[user_id]
                    raise
                except Exception as e:
                    logger.error(f"Error in connection {conn_id} for user {user_id}: {e}")
                    if user_id in self.user_chats:
                        del self.user_chats[user_id]
                    raise
                    
        except Exception as e:
            logger.error(f"Failed to send message for user {user_id}: {e}")
            raise

    async def close(self):
        for client in self.pool.connections:
            await client.close_session()

# Глобальный экземпляр менеджера чатов с новыми параметрами API
chat_manager = ChatManager(
    api_key='4d9f28f3e0446491d0b99e135e68e85f040f33aa',  # Ваш новый токен
    char_id='cYXxq0NFDa8lHhgtiAdv-9a534eDWbg-YiUtIfX7yoE'  # Ваш новый character_id
)

async def ensure_group_exists(chat_id: int, chat_title: str) -> None:
    """Check if group exists in database and create if not"""
    current_timestamp = int(datetime.now().timestamp())
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Check if group exists
        cursor = await db.execute('SELECT chat_id FROM groups WHERE chat_id = ?', (chat_id,))
        existing_group = await cursor.fetchone()
        
        if not existing_group:
            # If group doesn't exist, create new entry
            await db.execute('''
                INSERT INTO groups 
                (chat_id, message_count, joined_timestamp, title, is_active)
                VALUES (?, 0, ?, ?, TRUE)
            ''', (chat_id, current_timestamp, chat_title))
            await db.commit()

@router.message(F.chat.type == ChatType.PRIVATE)
async def private_message_handler(message: types.Message):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🦴 Добавить в группу",
                    url=f"https://t.me/MimiTyph_sBot?startgroup=true"
                )
            ]
        ]
    )
    
    # Сохраняем информацию о пользователе
    current_timestamp = int(datetime.now().timestamp())
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT OR IGNORE INTO users (user_id, username, joined_timestamp)
            VALUES (?, ?, ?)
        ''', (message.from_user.id, message.from_user.username, current_timestamp))
        await db.commit()

    # Создаем FSInputFile для локального файла
    animation = FSInputFile("/root/Mimi/start.mp4")
    
    await message.answer_animation(
        animation=animation,
        caption=(
            "🩷 Привет, дорогой! Меня зовут <b>Мими 🍼</b>, и я очень рада быть "
            "добавленной в твою замечательную чат-группу.\n\n"
            "Я могу отвечать на ваши сообщение таким образом, который <b>все будут любить!</b> 💕 \n\n"
            "<i>PS: Вы автоматически подтвежаете с <a href='https://telegra.ph/Politika-Konfidencialnosti-i-Usloviya-Ispolzovaniya-03-27'>Условиями использования</a></i>"
        ), reply_markup=keyboard
    )


@router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def group_message_handler(message: types.Message, bot: Bot):
    if not message.text:
        return

    try:
        # Проверяем и добавляем группу, если её нет в БД
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT 1 FROM groups WHERE chat_id = ?',
                (message.chat.id,)
            )
            group_exists = await cursor.fetchone()
            
            if not group_exists:
                # Группы нет в БД - добавляем
                await db.execute(
                    'INSERT INTO groups (chat_id, title, created_at) VALUES (?, ?, datetime("now"))',
                    (message.chat.id, message.chat.title)
                )
                await db.commit()
                logger.info(f"Добавлена новая группа в БД: {message.chat.title} (ID: {message.chat.id})")

        await ensure_group_exists(message.chat.id, message.chat.title)
        await save_message_history(message.chat.id, message.from_user.id, message.text)
        await save_words(message.chat.id, message.text)

        stats = await get_group_stats(message.chat.id)
        
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = "triggers"',
                (message.chat.id,)
            )
            triggers_module_active = await cursor.fetchone()
            
            triggers = standard_triggers.copy()
            
            cursor = await db.execute(
                'SELECT response_chance FROM group_config WHERE chat_id = ?',
                (message.chat.id,)
            )
            chance_row = await cursor.fetchone()
            response_chance = chance_row[0]/100 if chance_row else 0.01

        message_words = set(message.text.lower().split())
        should_respond = any(trigger.lower() in message_words for trigger in triggers)
        is_reply_to_bot = message.reply_to_message and message.reply_to_message.from_user.id == message.bot.id
        
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT response_chance FROM group_config WHERE chat_id = ?',
                (message.chat.id,)
            )
            chance_row = await cursor.fetchone()
            response_chance = chance_row[0]/100 if chance_row else 0.01

        random_response_chance = random.random() < response_chance
        
        if should_respond or is_reply_to_bot or random_response_chance:
            if stats['messages'] < REQUIRED_MESSAGES:
                await message.reply(
                    f"♡ Для активации Mimi Typh нужно {REQUIRED_MESSAGES} сообщений. "
                    f"Текущий прогресс: {stats['messages']}/{REQUIRED_MESSAGES} ‹𝟹"
                )
                return

            typing_task = None
            try:
                INITIAL_DELAY = random.uniform(0.3, 1.5)
                MIN_TYPING_TIME = 1.5
                BASE_CHAR_DELAY = 0.05
                TYPING_VARIABILITY = 0.03
                THINKING_PAUSE_CHANCE = 0.15
                THINKING_PAUSE_DURATION = (0.8, 2.0)
                
                await asyncio.sleep(INITIAL_DELAY)

                async def realistic_typing():
                    try:
                        last_action_time = time.time()
                        while True:
                            current_time = time.time()
                            elapsed = current_time - last_action_time
                            
                            if random.random() < THINKING_PAUSE_CHANCE:
                                pause = random.uniform(*THINKING_PAUSE_DURATION)
                                await asyncio.sleep(pause)
                                last_action_time = time.time()
                                continue
                            
                            await message.bot.send_chat_action(message.chat.id, "typing")
                            
                            delay = random.uniform(2.5, 4.5)
                            await asyncio.sleep(delay)
                            last_action_time = time.time()
                            
                    except asyncio.CancelledError:
                        pass

                typing_task = asyncio.create_task(realistic_typing())
                
                start_time = time.time()
                
                response = await chat_manager.send_message(
                    user_id=message.from_user.id,
                    message=message.text
                )
                
                if response:
                    base_typing_time = len(response) * (BASE_CHAR_DELAY + random.uniform(-TYPING_VARIABILITY, TYPING_VARIABILITY))
                    
                    if len(response.split()) > 5:
                        base_typing_time += random.uniform(0.5, 1.5) * (len(response.split()) // 8)
                    
                    typing_duration = max(MIN_TYPING_TIME, base_typing_time)
                else:
                    typing_duration = MIN_TYPING_TIME
                
                elapsed = time.time() - start_time
                if elapsed < typing_duration:
                    remaining_delay = typing_duration - elapsed
                    
                    while remaining_delay > 0:
                        chunk = min(remaining_delay, random.uniform(0.7, 1.8))
                        await asyncio.sleep(chunk)
                        remaining_delay -= chunk
                        
                        if remaining_delay > 0.5 and random.random() < 0.3:
                            await asyncio.sleep(random.uniform(0.2, 0.7))
                            remaining_delay -= 0.7

                if response:
                    await save_message_history(
                        chat_id=message.chat.id,
                        user_id=0,
                        message_text=response,
                        target_user_id=message.from_user.id
                    )
                    
                    async with aiosqlite.connect(DB_NAME) as db:
                        analysis_delay = random.uniform(0.4, 1.2)
                        await asyncio.sleep(analysis_delay)
                        
                        cursor = await db.execute('''
                            SELECT message_text 
                            FROM message_history 
                            WHERE chat_id = ? 
                            AND user_id = 0
                            ORDER BY timestamp DESC 
                            LIMIT 5
                        ''', (message.chat.id,))
                        history = await cursor.fetchall()

                    await asyncio.sleep(random.uniform(0.05, 0.3))
                    await message.reply(
                        text=html.escape(response)
                    )

                await send_random_daily_media(message)
        
            except Exception as e:
                    logger.error(f"Ошибка обработки: {str(e)}")
            finally:
                if typing_task and not typing_task.done():
                    typing_task.cancel()
                    try:
                        await typing_task
                    except:
                        pass
                
                await asyncio.sleep(0.1)
                await message.bot.send_chat_action(message.chat.id, "cancel")

    except Exception as e:
        error_id = str(uuid.uuid4())[:8]
        error_trace = traceback.format_exc()
        
        logger.error(f"Error #{error_id}: {str(e)}\n{error_trace}")
        
        await message.answer(
            f"<i>~Ой, кажется произошла ошибка #{error_id}. Администратор уже уведомлен.</i>"
        )
        
        if ADMIN_USER_ID:
            error_card = (
                f"🚨 <b>New Error #{error_id}</b>\n\n"
                f"🆔 <b>Chat ID</b>: {message.chat.id}\n"
                f"👤 <b>User</b>: @{message.from_user.username}\n"
                f"📝 <b>Message</b>: {message.text[:200]}\n\n"
                f"🔧 <b>Error</b>:\n<code>{str(e)[:1000]}</code>"
            )
            
            try:
                await bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text=error_card
                )
                
                if len(error_trace) > 1000:
                    with io.StringIO(error_trace) as trace_file:
                        trace_file.name = f"error_{error_id}_traceback.txt"
                        await bot.send_document(
                            chat_id=ADMIN_USER_ID,
                            document=trace_file,
                            caption=f"Full traceback for error #{error_id}"
                        )
            except Exception as admin_e:
                logger.error(f"Failed to send error notification: {admin_e}")

async def main():
    await init_db()
    bot = Bot(
        token="8148475045:AAFq-CpuOLoidWwMZzFNCvbenB3mmy6fnKg",
        default=DefaultBotProperties(parse_mode="HTML")
    )
    
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    
    try:
        await dp.start_polling(bot, 
            drop_pending_updates=True,
            timeout=30)
    finally:
        await bot.session.close()
        await chat_manager.close()

if __name__ == "__main__":
    asyncio.run(main())