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
import platform
from typing import List, Set, Dict, Optional, Deque
from collections import deque
from datetime import datetime, timedelta
from uuid import uuid4
from html import escape
import socket
import requests
import aiohttp

import random
import numpy as np
import psutil
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
ADMIN_USER_ID = 7777 # ADMIN ID
STICKER_IDS = [] # STICKER IDS (OPTION)

LOCAL_GIFS = {} # LOCAL GIFTS (OPTION)

async def init_db():
    """Init DB"""
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

        await db.execute('''
            CREATE TABLE IF NOT EXISTS last_button_press (
                user_id INTEGER PRIMARY KEY,
                last_press_time REAL
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS group_config (
                chat_id INTEGER PRIMARY KEY,
                response_chance INTEGER DEFAULT 1
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS premium_groups (
                group_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                end_date DATETIME
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS pending_free_premium_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER,
                user_id INTEGER,
                link TEXT,
                status TEXT DEFAULT 'pending',
                request_time TEXT
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS group_modules (
                group_id INTEGER,
                module_name TEXT,
                is_active INTEGER DEFAULT 0,
                PRIMARY KEY (group_id, module_name)
            )''')
        
        await db.execute('''
            CREATE TABLE IF NOT EXISTS group_settings_backup (
                group_id INTEGER PRIMARY KEY,
                settings_json TEXT,
                modules_json TEXT,
                backup_date TEXT
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS blocked_stickers (
                group_id INTEGER,
                sticker_id TEXT,
                blocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_id, sticker_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS blocked_packs (
                group_id INTEGER,
                pack_name TEXT,
                blocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_id, pack_name)
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS group_triggers (
                group_id INTEGER,
                trigger TEXT,
                PRIMARY KEY (group_id, trigger)
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS hidden_messages (
                message_id TEXT PRIMARY KEY,
                chat_id INTEGER,
                creator_id INTEGER,
                target_user_id INTEGER,
                message_text TEXT
            )
        ''')
        await db.commit()
        
async def should_send_daily_media(chat_id: int) -> bool:
    current_date = datetime.now().date()
    
    async with aiosqlite.connect(DB_NAME) as db:
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
    current_timestamp = int(datetime.now().timestamp())
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT OR REPLACE INTO media_tracking (chat_id, last_send_timestamp)
            VALUES (?, ?)
        ''', (chat_id, current_timestamp))
        await db.commit()

async def send_random_daily_media(message: types.Message):
    try:
        if not await should_send_daily_media(message.chat.id):
            return
            
        if random.random() < 0.5:
            sticker_id = random.choice(STICKER_IDS)
            await message.bot.send_sticker(
                chat_id=message.chat.id,
                sticker=sticker_id
            )
        else:
            random_gif_path = random.choice(list(LOCAL_GIFS.values()))
            
            try:
                if not os.path.exists(random_gif_path):
                    print(f"File not found: {random_gif_path}")
                    return
                    
                gif = FSInputFile(random_gif_path)
                
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
    current_time = datetime.now()
    current_timestamp = int(current_time.timestamp() * 1000000)

    async with aiosqlite.connect(DB_NAME) as db:
        try:
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
        async with db.execute(
            'SELECT message_count FROM groups WHERE chat_id = ?',
            (chat_id,)
        ) as cursor:
            message_count = await cursor.fetchone()
            message_count = message_count[0] if message_count else 0

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

log = logging.getLogger('adverts')

async def show_advert(user_id: int):

    async with aiohttp.ClientSession() as session:

        async with session.post(
            'https://api.gramads.net/ad/SendPost',
            headers={
                'Authorization': '',
                'Content-Type': 'application/json',
            },
            json={'SendToChatId': user_id},
        ) as response:

            if not response.ok:

                log.error('Gramads: %s' % str(await response.json()))

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
    daily_stats = await get_daily_message_stats(chat_id)
    
    dates = [datetime.strptime(str(row[0]), '%Y-%m-%d') for row in daily_stats]
    counts = [row[1] for row in daily_stats]
    
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor('#1a1a2e')
    ax.set_facecolor('#1a1a2e')
    
    ax.grid(True, linestyle='--', alpha=0.2, color='#2d374d')
    
    ax.plot(dates, counts, '-', color='#00ff9d', linewidth=2, alpha=0.8)
    
    ax.scatter(dates, counts, color='#00ff9d', s=50, alpha=1, 
              zorder=5, edgecolor='white', linewidth=1)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    
    ax.tick_params(axis='both', colors='#8884d8')
    
    for spine in ax.spines.values():
        spine.set_edgecolor('#8884d8')
        spine.set_linewidth(1)
    
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight',
                facecolor='#1a1a2e', edgecolor='none')
    buf.seek(0)
    plt.close()
    
    return BufferedInputFile(buf.getvalue(), filename="stats.png")

@router.message(Command("stats"))
async def stats_handler(message: types.Message):
    """/stats"""
    if message.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        stats = await get_group_stats(message.chat.id)
        user_id = message.from_user.id
        
        caption = (
            f"📊 <b>Статистика группы:</b>\n"
            f"🤍 Сообщений получено: <code>{stats['messages']}</code>\n"
            f"💌 Слов собрано: <code>{stats['words']}</code>"
        )
        
        if stats['messages'] < REQUIRED_MESSAGES:
            caption += f"\n🤍 До активации <b>Mimi Typh</b> осталось: <code>{REQUIRED_MESSAGES - stats['messages']}</code> сообщений."
        
        try:
            image = await create_stats_image(message.chat.id, stats)
            
            await message.answer_photo(
                photo=image,
                caption=caption
            )
            await show_advert(user_id)
            
        except Exception as e:
            print(f"Error creating stats image: {e}")
            await message.answer(caption)

logger = logging.getLogger(__name__)

class AdminStates(StatesGroup):
    GRANT_GROUP_ID = State()
    GRANT_GROUP_DAYS = State()
    CONFIRM_USER_DELETE = State()
    waiting_for_views = State()
    BROADCAST_MESSAGE = State()

@router.message(Command("admin"))
async def handle_admin_command(message: Message, state: FSMContext):
    """/admin"""
    if message.from_user.id != ADMIN_USER_ID:
        return

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT COUNT(*) FROM groups WHERE is_active = TRUE")
        active_groups = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT group_id, end_date FROM premium_groups")
        premium_groups = await cursor.fetchall()
    
    premium_groups_list = "\n".join(
        [f"💬 {gid} (до {datetime.fromisoformat(end).strftime('%d.%m.%Y')})" 
         for gid, end in premium_groups]
    ) if premium_groups else "❌ Нет premium-групп"

    text = (
        f"🔐 <b>Админ-панель</b>\n\n"
        f"👥 Всего пользователей: <code>{total_users}</code>\n"
        f"💬 Активных групп: <code>{active_groups}</code>\n\n"
        f"🏆 Premium-группы:\n{premium_groups_list}"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Выдать Premium", callback_data="admin_grant_group_premium")],
        [InlineKeyboardButton(text="🗑 Удалить DB пользователя", callback_data="admin_delete_user")],
        [InlineKeyboardButton(text="Рассылка", callback_data="broadcast")]
    ])
    await message.answer(text, reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "broadcast")
async def handle_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_USER_ID:
        return
    
    await callback.message.answer(
        "📢 Введите сообщение для рассылки (можно с медиа):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")]
        ]))
    await state.set_state(AdminStates.BROADCAST_MESSAGE)
    await callback.answer()

@router.message(StateFilter(AdminStates.BROADCAST_MESSAGE))
async def process_broadcast_message(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id != ADMIN_USER_ID:
        return
    
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT user_id FROM users")
        users = [row[0] for row in await cursor.fetchall()]
        
        cursor = await db.execute("SELECT chat_id FROM groups WHERE is_active = TRUE")
        groups = [row[0] for row in await cursor.fetchall()]
    
    total_recipients = len(users) + len(groups)
    await message.answer(f"📢 Начинаю рассылку для {total_recipients} получателей...")
    
    message_to_send = message
    
    for i, user_id in enumerate(users, 1):
        try:
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"Ошибка при отправке пользователю {user_id}: {e}")
        
        if i % 20 == 0:
            await message.answer(f"⏳ Отправлено {i}/{len(users)} пользователям...")
    
    for j, group_id in enumerate(groups, 1):
        try:
            await bot.copy_message(
                chat_id=group_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"Ошибка при отправке в группу {group_id}: {e}")
        
        if j % 10 == 0:
            await message.answer(f"⏳ Отправлено {j}/{len(groups)} группам...")
    
    await message.answer(f"✅ Рассылка завершена!\n"
                         f"👥 Пользователей: {len(users)}\n"
                         f"💬 Групп: {len(groups)}")
    await state.clear()

@router.callback_query(lambda c: c.data == "admin_cancel")
async def handle_admin_cancel(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_USER_ID:
        return
    
    await state.clear()
    await callback.message.answer("❌ Действие отменено")
    await callback.answer()

@router.callback_query(F.data == "admin_delete_user")
async def start_delete_user(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("❌ Только для администратора!", show_alert=True)
        return
    
    await callback.message.answer(
        "🗑 <b>Удаление пользователя</b>\n\n"
        "Введите ID пользователя или перешлите сообщение от него:"
    )
    await state.set_state(AdminStates.CONFIRM_USER_DELETE)
    await callback.answer()

@router.message(AdminStates.CONFIRM_USER_DELETE)
async def process_user_for_deletion(message: Message, state: FSMContext, bot: Bot):
    user_id = None
    
    if message.forward_from:
        user_id = message.forward_from.id
    elif message.text and message.text.isdigit():
        user_id = int(message.text)
    else:
        await message.answer("❌ Некорректный ввод. Отправьте ID пользователя или перешлите его сообщение.")
        return
    
    try:
        user = await bot.get_chat(user_id)
        
        async with aiosqlite.connect(DB_NAME) as db:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM message_history WHERE user_id = ?",
                (user_id,))
            message_count = (await cursor.fetchone())[0]
            
            cursor = await db.execute(
                "SELECT COUNT(DISTINCT chat_id) FROM message_history WHERE user_id = ?",
                (user_id,))
            groups_count = (await cursor.fetchone())[0]
            
            cursor = await db.execute(
                "SELECT joined_timestamp FROM users WHERE user_id = ?",
                (user_id,))
            join_data = await cursor.fetchone()
            join_date = datetime.fromtimestamp(join_data[0]).strftime('%d.%m.%Y') if join_data else "неизвестно"
        
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
        
        await state.update_data(user_id=user_id)
        
        await message.answer(user_card, reply_markup=keyboard)
        
    except Exception as e:
        error_msg = (
            f"❌ Ошибка получения информации о пользователе:\n"
            f"<code>{html.escape(str(e))}</code>\n\n"
            f"Проверьте правильность введенных данных."
        )
        await message.answer(error_msg)
        await state.clear()

@router.callback_query(F.data.startswith("confirm_delete_"))
async def confirm_user_deletion(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("❌ Только для администратора!", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[2])
    
    try:
        user = await bot.get_chat(user_id)
        
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM message_history WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM last_button_press WHERE user_id = ?", (user_id,))
            await db.commit()
            
            cursor = await db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
            if await cursor.fetchone():
                raise Exception("Не удалось удалить пользователя из базы данных")
        
        success_msg = (
            f"✅ Пользователь <code>{user_id}</code> успешно удален!\n\n"
            f"📛 Имя: {html.escape(user.first_name)}\n"
            f"👤 Username: @{user.username if user.username else 'нет'}\n\n"
            f"Все данные безвозвратно удалены из системы."
        )
        
        await callback.message.delete()
        
        await callback.message.answer(success_msg)
        
        logger.warning(f"Администратор удалил пользователя {user_id} ({user.first_name})")
        
    except Exception as e:
        error_msg = (
            f"❌ Ошибка при удалении пользователя <code>{user_id}</code>:\n"
            f"<code>{html.escape(str(e))}</code>"
        )
        await callback.message.edit_text(error_msg)
    
    await state.clear()
    await callback.answer()

@router.callback_query(F.data == "cancel_delete")
async def cancel_user_deletion(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Удаление пользователя отменено.")
    await state.clear()
    await callback.answer()

@router.callback_query(F.data == "admin_grant_group_premium")
async def start_grant_group_premium(callback: CallbackQuery, state: FSMContext):
    """Начало процесса выдачи premium группе"""
    await callback.message.answer("📝 Введите ID группы:")
    await state.set_state(AdminStates.GRANT_GROUP_ID)
    await callback.answer()

@router.message(AdminStates.GRANT_GROUP_ID)
async def process_group_id(message: Message, state: FSMContext, bot: Bot):
    """Обработка ID группы с проверкой её существования"""
    try:
        group_id = int(message.text)
        try:
            chat = await bot.get_chat(group_id)
            
            bot_member = await bot.get_chat_member(group_id, bot.id)
            if not bot_member.status == ChatMemberStatus.ADMINISTRATOR:
                await message.answer("❌ Бот не является администратором в этой группе!")
                await state.clear()
                return
                
            await state.update_data(group_id=group_id)
            await message.answer("⏳ Введите количество дней:")
            await state.set_state(AdminStates.GRANT_GROUP_DAYS)
            
        except Exception as e:
            await message.answer(f"❌ Ошибка! Не удалось найти группу или бот не имеет к ней доступа. Убедитесь, что:\n"
                               f"1. Группа существует\n"
                               f"2. Бот добавлен в группу\n"
                               f"3. Бот имеет права администратора")
            await state.clear()
            
    except ValueError:
        await message.answer("❌ Ошибка! Введите числовой ID группы.")
        await state.clear()

@router.message(AdminStates.GRANT_GROUP_DAYS)
async def process_group_premium_days(message: Message, state: FSMContext, bot: Bot):
    try:
        days = int(message.text)
        if days <= 0:
            raise ValueError
            
        data = await state.get_data()
        group_id = data['group_id']
        
        now = datetime.now()
        end_date = now + relativedelta(days=days)
        
        async with aiosqlite.connect(DB_NAME) as db:
            cursor = await db.execute('''
                SELECT settings_json, modules_json 
                FROM group_settings_backup 
                WHERE group_id = ?
            ''', (group_id,))
            backup = await cursor.fetchone()
            
            await db.execute('''
                INSERT OR REPLACE INTO premium_groups 
                (group_id, user_id, end_date) 
                VALUES (?, ?, ?)
            ''', (group_id, ADMIN_USER_ID, end_date.isoformat()))
            
            if backup:
                settings = json.loads(backup[0])
                modules = json.loads(backup[1])
                
                await db.execute('''
                    INSERT OR REPLACE INTO group_config 
                    (chat_id, response_chance) VALUES (?, ?)
                ''', (group_id, settings['response_chance']))
                
                for module_name, is_active in modules.items():
                    await db.execute('''
                        INSERT OR REPLACE INTO group_modules 
                        (group_id, module_name, is_active) VALUES (?, ?, ?)
                    ''', (group_id, module_name, is_active))
                
                await db.execute('DELETE FROM group_settings_backup WHERE group_id = ?', (group_id,))
            
            await db.commit()

        await message.answer(
            f"✅ Группе <code>{group_id}</code> выдан premium до "
            f"{end_date.strftime('%d.%m.%Y %H:%M')}\n"
            f"{'🔧 Настройки группы восстановлены из бэкапа' if backup else ''}"
        )
        
        try:
            await bot.send_message(
                group_id,
                f"🎉 Этому группу выдан Premium на {days} дней!\n"
                f"📅 Окончание: {end_date.strftime('%d.%m.%Y %H:%M')}\n"
                f"{'⚙ Настройки и модули были восстановлены' if backup else ''}"
            )
        except Exception as e:
            print(f"Ошибка уведомления группы: {e}")

        await state.clear()

    except ValueError:
        await message.answer("❌ Некорректное число дней.")
#==============================================================================================
#==============================================================================================
#==============================================================================================

available_modules = ['ping', 'bansticker', 'triggers', 'pl']

group_subscription_prices = {
    1: 100,   # 200 руб. / 2 = 100 XTR
    3: 280,    # 559 руб. / 2 = 280 XTR
    6: 520,    # 1039 руб. / 2 = 520 XTR
    9: 750,    # 1499 руб. / 2 = 750 XTR
    12: 900    # 1799 руб. / 2 = 900 XTR
}

def get_group_premium_keyboard(group_id: int, initiator_id: int):
    buttons = [
        [InlineKeyboardButton(text="100⭐/месяц", callback_data=f"gpremium_{group_id}_1_{initiator_id}")],
        [InlineKeyboardButton(text="280⭐/3 месяца", callback_data=f"gpremium_{group_id}_3_{initiator_id}")],
        [InlineKeyboardButton(text="520⭐/6 месяцев", callback_data=f"gpremium_{group_id}_6_{initiator_id}")],
        [InlineKeyboardButton(text="750⭐/9 месяцев", callback_data=f"gpremium_{group_id}_9_{initiator_id}")],
        [InlineKeyboardButton(text="900⭐/12 месяцев", callback_data=f"gpremium_{group_id}_12_{initiator_id}")],
        [InlineKeyboardButton(text="🎁 Бесплатный премиум", callback_data=f"gfree_premium_{group_id}_{initiator_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=f"back_to_config_{group_id}_{initiator_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def get_group_config_keyboard(chat_id: int, has_premium: bool, response_chance: int, initiator_id: int) -> InlineKeyboardMarkup:
    buttons = []
    
    if has_premium:
        buttons.append([
            InlineKeyboardButton(
                text=f"⚖️ Вероятность ответа: {response_chance}%", 
                callback_data=f"config_chance_{chat_id}_{response_chance}_{initiator_id}"
            )
        ])
        buttons.append([
            InlineKeyboardButton(
                text="📦 Модули",
                callback_data=f"manage_modules_{chat_id}_{initiator_id}"
            )
        ])
    
    buttons.append([
        InlineKeyboardButton(
            text="🔄 Продлить подписку" if has_premium else "💎 Оформить подписку",
            callback_data=f"group_subscribe_{chat_id}_{initiator_id}"
        )
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

last_press_times = {}

async def check_flood(user_id: int, interval: int = 1) -> bool:
    current_time = time.time()
    last_press = last_press_times.get(user_id, 0)
    
    if current_time - last_press < interval:
        return True
    
    last_press_times[user_id] = current_time
    return False

async def check_group_premium_status(group_id: int) -> bool:
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT end_date FROM premium_groups WHERE group_id = ?',
            (group_id,)
        )
        result = await cursor.fetchone()
        if result:
            return datetime.fromisoformat(result[0]) > datetime.now()
    return False

@router.callback_query(lambda c: c.data.startswith("back_to_config_"))
async def back_to_config_handler(callback: CallbackQuery, bot: Bot):
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return


    data = callback.data.split("_")
    group_id = int(data[3])
    initiator_id = int(data[4])
    user_id = callback.from_user.id
    first_name = html.escape(callback.from_user.first_name)
    
    if user_id != initiator_id:
        await callback.answer("❌ Не твоя кнопка!", show_alert=True)
        return

    try:
        has_premium = await check_group_premium_status(group_id)

        member = await bot.get_chat_member(group_id, user_id)
        if member.status not in ["administrator", "creator"]:
            await callback.answer("❌ Нужны права админа!", show_alert=True)
            return

        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT end_date FROM premium_groups WHERE group_id = ?',
                (group_id,)
            )
            result = await cursor.fetchone()
            has_premium = result and datetime.fromisoformat(result[0]) > datetime.now()

            response_chance = 1
            if has_premium:
                cursor = await db.execute(
                    'SELECT response_chance FROM group_config WHERE chat_id = ?',
                    (group_id,)
                )
                config = await cursor.fetchone()
                response_chance = config[0] if config else 1

        text = (f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n ⚙️ Настройки группы\n\n"
                f"🔹 Premium статус: {'активен' if has_premium else 'не активен'}")

        keyboard = await get_group_config_keyboard(group_id, has_premium, response_chance, initiator_id)
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer()
    except Exception as e:
        await callback.answer(f"⚠️ Упс, ошибка...", show_alert=True)

@router.callback_query(lambda c: c.data.startswith("group_subscribe_"))
async def group_subscribe_handler(callback: CallbackQuery, bot: Bot):
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return

    data = callback.data.split("_")
    chat_id = int(data[2])
    initiator_id = int(data[3])
    user_id = callback.from_user.id
    first_name = html.escape(callback.from_user.first_name)
    
    if user_id != initiator_id:
        await callback.answer("❌ Не твоя кнопка!", show_alert=True)
        return

    try:
        member = await bot.get_chat_member(chat_id, user_id)
        if member.status not in ["administrator", "creator"]:
            await callback.answer("❌ Нужны права админа!", show_alert=True)
            return

        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT end_date FROM premium_groups WHERE group_id = ?',
                (chat_id,)
            )
            result = await cursor.fetchone()
            
            text = f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n 🌟 Выберите срок Premium подписки для группы\n\n"
            if result:
                end_date = datetime.fromisoformat(result[0])
                if end_date > datetime.now():
                    remaining = end_date - datetime.now()
                    text += f"🔹 Текущая подписка активна до: {end_date.strftime('%d.%m.%Y %H:%M')}\n"
                    text += f"⏳ Осталось: {remaining.days} дн. {remaining.seconds//3600} ч."
                else:
                    text += "🔹 Подписка истекла"
            else:
                text += "🔹 Подписка не активирована"

        await callback.message.edit_text(
            text,
            reply_markup=get_group_premium_keyboard(chat_id, initiator_id)
        )
        await callback.answer()
    except Exception as e:
        await callback.answer(f"⚠️ Упс, ошибка...", show_alert=True)

@router.callback_query(lambda c: c.data.startswith("gpremium_"))
async def process_group_premium_purchase(callback: CallbackQuery, bot: Bot):
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return
    data = callback.data.split("_")
    group_id = int(data[1])
    months = int(data[2])
    initiator_id = int(data[3])
    user_id = callback.from_user.id
    first_name = html.escape(callback.from_user.first_name)
    current_time = time.time()
    
    if user_id != initiator_id:
        await callback.answer("❌ Не твоя кнопка!", show_alert=True)
        return

    try:
        try:
            await bot.send_chat_action(user_id, "typing")
        except Exception as e:
            if "bot was blocked" in str(e).lower():
                await callback.answer("❌ Бот заблокирован. Разблокируйте в ЛС.", show_alert=True)
                return
            elif "user is deactivated" in str(e).lower():
                await callback.answer("❌ Ваш аккаунт удален.", show_alert=True)
                return

        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT 1 FROM users WHERE user_id = ?',
                (user_id,)
            )
            user_exists = await cursor.fetchone()
            
            if not user_exists:
                await callback.answer(
                    "❌ Сначала начните с /start",
                    show_alert=True
                )
                return

        member = await bot.get_chat_member(group_id, user_id)
        if member.status not in ["administrator", "creator"]:
            await callback.answer("❌ Нужны права админа!", show_alert=True)
            return
        
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT last_press_time FROM last_button_press WHERE user_id = ?',
                (user_id,)
            )
            result = await cursor.fetchone()
            
            if result and (current_time - result[0]) < 300:
                remaining = int(300 - (current_time - result[0]))
                await callback.answer(f"⏳ Повторите через {remaining} сек.", show_alert=True)
                return
            
            await db.execute('''
                INSERT OR REPLACE INTO last_button_press 
                (user_id, last_press_time) VALUES (?, ?)
            ''', (user_id, current_time))
            await db.commit()

        amount_xtr = group_subscription_prices[months]
        await send_group_invoice(user_id, group_id, months, amount_xtr, bot)
        await callback.answer("✅ Счёт отправлен.")
        
    except Exception as e:
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)

@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await pre_checkout_query.answer(ok=True)

async def send_group_invoice(user_id: int, group_id: int, months: int, amount_xtr: int, bot: Bot):
    try:
        chat = await bot.get_chat(group_id)
        group_name = chat.title
    except Exception as e:
        print(f"Ошибка при получении информации о группе: {e}")
        group_name = "группы"

    prices = [LabeledPrice(label=f'Group Premium на {months} мес.', amount=amount_xtr)]
    
    await bot.send_invoice(
        chat_id=user_id,
        title=f'💎 Premium подписка на {months} месяцев',
        description=f'👑 Оплата Premium статуса для {html.escape(group_name)} на {months} месяцев ({amount_xtr} ⭐).',
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter=f'group_premium_{months}',
        payload=f'group_premium_{group_id}_{months}',
        protect_content=True
    )

@router.message(lambda message: message.successful_payment and 
                message.successful_payment.invoice_payload.startswith('group_premium_'))
async def process_successful_group_payment(message: Message):
    user_id = message.from_user.id
    first_name = html.escape(message.from_user.first_name)
    payload = message.successful_payment.invoice_payload.split('_')
    group_id = int(payload[2])
    months = int(payload[3])
    
    now = datetime.now()
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute('''
            SELECT settings_json, modules_json 
            FROM group_settings_backup 
            WHERE group_id = ?
        ''', (group_id,))
        backup = await cursor.fetchone()
        
        cursor = await db.execute(
            'SELECT end_date FROM premium_groups WHERE group_id = ?',
            (group_id,)
        )
        result = await cursor.fetchone()
        
        if result and (end_date := datetime.fromisoformat(result[0])) > now:
            new_end_date = end_date + relativedelta(months=months)
        else:
            new_end_date = now + relativedelta(months=months)
        
        await db.execute('''
            INSERT OR REPLACE INTO premium_groups 
            (group_id, user_id, end_date) VALUES (?, ?, ?)
        ''', (group_id, user_id, new_end_date.isoformat()))
        
        if backup:
            settings = json.loads(backup[0])
            modules = json.loads(backup[1])
            
            await db.execute('''
                INSERT OR REPLACE INTO group_config 
                (chat_id, response_chance) VALUES (?, ?)
            ''', (group_id, settings['response_chance']))
            
            for module_name, is_active in modules.items():
                await db.execute('''
                    INSERT OR REPLACE INTO group_modules 
                    (group_id, module_name, is_active) VALUES (?, ?, ?)
                ''', (group_id, module_name, is_active))
            
            await db.execute('DELETE FROM group_settings_backup WHERE group_id = ?', (group_id,))
        
        await db.commit()
    
    try:
        await message.bot.send_message(
            group_id,
            f"✅ Premium для группы активирован на {months} мес.! 🎉\n"
            f"📅 До: {new_end_date.strftime('%d.%m.%Y %H:%M')}\n"
            f"💳 Оплатил: <a href=\"tg://user?id={user_id}\">{first_name}</a>",
            reply_markup=get_group_config_keyboard(group_id, True)
        )
    except Exception as e:
        print(f"Не удалось уведомить группу: {e}")

    await message.answer(
        f"✅ Premium для группы успешно активирован на {months} месяцев!\n"
        f"📅 До: {new_end_date.strftime('%d.%m.%Y %H:%M')}"
    )

class FreePremiumStates(StatesGroup):
    waiting_for_link = State()

@router.callback_query(lambda c: c.data.startswith("gfree_premium_"))
async def free_premium_handler(callback: types.CallbackQuery, bot: Bot):
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return

    data = callback.data.split("_")
    group_id = int(data[2])
    initiator_id = int(data[3])
    user_id = callback.from_user.id
    first_name = html.escape(callback.from_user.first_name)
    
    if user_id != initiator_id:
        await callback.answer("❌ Не твоя кнопка!", show_alert=True)
        return

    try:
        member = await bot.get_chat_member(group_id, user_id)
        if member.status not in ["administrator", "creator"]:
            await callback.answer("❌ Нужны права админа!", show_alert=True)
            return

        text = (
            f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n"
            "🎁 <b>Бесплатный премиум</b>\n\n"
            "Чтобы получить бесплатный премиум для вашей группы, вам нужно:\n"
            "1. Опубликовать видео в TikTok с упоминанием тега бота.\n"
            "2. Видео должно набрать минимум 2000 просмотров.\n\n"
            "За каждые 2000 просмотров вы получаете 30 дней премиума.\n\n"
            "Нажмите кнопку ниже, чтобы отправить ссылку на видео."
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1. Видео в TikTok", callback_data=f"submit_tiktok_{group_id}_{initiator_id}")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"group_subscribe_{group_id}_{initiator_id}")]
        ])
        
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer()
    except Exception as e:
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)

@router.callback_query(lambda c: c.data.startswith("submit_tiktok_"))
async def submit_tiktok_handler(callback: types.CallbackQuery, bot: Bot, state: FSMContext):
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return

    data = callback.data.split("_")
    group_id = int(data[2])
    initiator_id = int(data[3])
    user_id = callback.from_user.id
    
    if user_id != initiator_id:
        await callback.answer("❌ Не твоя кнопка!", show_alert=True)
        return

    try:
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT 1 FROM pending_free_premium_requests WHERE group_id = ? AND status = ?',
                (group_id, 'pending')
            )
            result = await cursor.fetchone()
            if result:
                await callback.answer("❌ У вас уже есть запрос.", show_alert=True)
                return

        await state.set_state(FreePremiumStates.waiting_for_link)
        await state.update_data(group_id=group_id, initiator_id=initiator_id)
        
        await callback.message.edit_text("📎 Пожалуйста, отправьте ссылку на ваше видео в TikTok.")
        await callback.answer()
    except Exception as e:
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)

@router.message(FreePremiumStates.waiting_for_link)
async def process_tiktok_link(message: types.Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    link = message.text.strip()
    
    if not link.startswith("http"):
        await message.answer("❌ Пожалуйста, отправьте реальную ссылку.")
        return
    
    data = await state.get_data()
    group_id = data['group_id']
    initiator_id = data['initiator_id']
    
    if user_id != initiator_id:
        return
    
    try:
        async with aiosqlite.connect('database.db') as db:
            await db.execute('''
                INSERT INTO pending_free_premium_requests 
                (group_id, user_id, link, status, request_time)
                VALUES (?, ?, ?, ?, ?)
            ''', (group_id, user_id, link, 'pending', datetime.now().isoformat()))
            await db.commit()
        
        await bot.send_message(
            ADMIN_USER_ID,
            f"Новый запрос на Бесплатный премиум:\n\n"
            f"Группа: {group_id}\n\n"
            f"Пользователь: {user_id}\n\n"
            f"Ссылка: {link}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_free_{group_id}_{user_id}")],
                [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_free_{group_id}_{user_id}")]
            ])
        )
        
        await message.answer("✅ Ваш запрос отправлен на рассмотрение.")
        await state.clear()
    except Exception as e:
        await message.answer("⚠️ Упс, ошибка...")
        await state.clear()

@router.callback_query(lambda c: c.data.startswith("reject_free_"))
async def reject_free_premium_handler(callback: types.CallbackQuery, bot: Bot):
    if callback.from_user.id != ADMIN_USER_ID:
        return
    
    data = callback.data.split("_")
    group_id = int(data[2])
    user_id = int(data[3])
    
    try:
        async with aiosqlite.connect('database.db') as db:
            await db.execute(
                'UPDATE pending_free_premium_requests SET status = ? WHERE group_id = ? AND user_id = ? AND status = ?',
                ('rejected', group_id, user_id, 'pending')
            )
            await db.commit()
        
        await bot.send_message(user_id, "❌ Ваш запрос на бесплатный премиум был отклонен.")
        await callback.message.edit_text("Запрос отклонен.")
        await callback.answer()
    except Exception as e:
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)

@router.callback_query(lambda c: c.data.startswith("approve_free_"))
async def approve_free_premium_handler(callback: types.CallbackQuery, bot: Bot, state: FSMContext):
    if callback.from_user.id != ADMIN_USER_ID:
        return
    
    data = callback.data.split("_")
    group_id = int(data[2])
    user_id = int(data[3])
    
    try:
        await state.set_state(AdminStates.waiting_for_views)
        await state.update_data(group_id=group_id, user_id=user_id)
        
        await callback.message.edit_text("✅ Отлично! Пожалуйста, укажите количество просмотров видео.")
        await callback.answer()
    except Exception as e:
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)

@router.message(AdminStates.waiting_for_views)
async def process_views(message: types.Message, state: FSMContext, bot: Bot):
    if message.from_user.id != ADMIN_USER_ID:
        return
    
    try:
        views = int(message.text)
        if views < 2000:
            await message.answer("❌ Видео должно иметь минимум 2000 просмотров.")
            return
        
        data = await state.get_data()
        group_id = data['group_id']
        user_id = data['user_id']
        
        days = (views // 2000) * 30
        now = datetime.now()
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT end_date FROM premium_groups WHERE group_id = ?',
                (group_id,)
            )
            result = await cursor.fetchone()
            if result and (end_date := datetime.fromisoformat(result[0])) > now:
                new_end_date = end_date + timedelta(days=days)
            else:
                new_end_date = now + timedelta(days=days)
            
            await db.execute('''
                INSERT OR REPLACE INTO premium_groups 
                (group_id, user_id, end_date) VALUES (?, ?, ?)
            ''', (group_id, user_id, new_end_date.isoformat()))
            
            await db.execute(
                'UPDATE pending_free_premium_requests SET status = ? WHERE group_id = ? AND user_id = ? AND status = ?',
                ('approved', group_id, user_id, 'pending')
            )
            await db.commit()
        
        await bot.send_message(
            user_id,
            f"✅ Запрос на бесплатный премиум одобрен!\n"
            f"Премиум для группы продлен на {days} дней."
        )
        await message.answer(f"✅ Премиум продлен на {days} дней.")
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, укажите число просмотров.")
    except Exception as e:
        await message.answer("⚠️ Упс, ошибка...")
        await state.clear()



async def check_expired_group_premium(bot: Bot):
    while True:
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute('''
                SELECT group_id, user_id, end_date 
                FROM premium_groups 
                WHERE datetime(end_date) < datetime('now')
            ''')
            expired_groups = await cursor.fetchall()
            
            for group_id, user_id, end_date in expired_groups:
                try:
                    chat = await bot.get_chat(group_id)
                    group_name = chat.title
                    group_mention = f'<a href="tg://user?id={group_id}">{html.escape(group_name)}</a>'
                except Exception as e:
                    print(f"Ошибка получения информации о группе {group_id}: {e}")
                    group_mention = f"группы {group_id}"

                cursor = await db.execute('''
                    SELECT response_chance FROM group_config 
                    WHERE chat_id = ?
                ''', (group_id,))
                config = await cursor.fetchone()
                
                cursor = await db.execute('''
                    SELECT module_name, is_active FROM group_modules 
                    WHERE group_id = ?
                ''', (group_id,))
                modules = await cursor.fetchall()
                
                settings_json = json.dumps({'response_chance': config[0] if config else 1})
                modules_json = json.dumps(dict(modules))
                
                await db.execute('''
                    INSERT OR REPLACE INTO group_settings_backup 
                    (group_id, settings_json, modules_json, backup_date)
                    VALUES (?, ?, ?, ?)
                ''', (group_id, settings_json, modules_json, datetime.now().isoformat()))
                
                await db.execute('DELETE FROM group_config WHERE chat_id = ?', (group_id,))
                await db.execute('DELETE FROM group_modules WHERE group_id = ?', (group_id,))
                await db.execute('DELETE FROM premium_groups WHERE group_id = ?', (group_id,))
                
                try:
                    await bot.send_message(
                        group_id,
                        f"❌ Premium подписка для данного группы истекла!\n"
                        f"Все настройки сброшены на значения по умолчанию.\n"
                        f"Для продления используйте команду /gpremium",
                        disable_web_page_preview=True
                    )
                    await bot.send_message(
                        user_id,
                        f"❌ Premium подписка для {group_mention} истекла!\n"
                        f"Настройки сохранены и будут восстановлены при продлении.",
                        disable_web_page_preview=True
                    )
                except Exception as e:
                    print(f"Ошибка уведомления: {e}")
            
            await db.commit()
        await asyncio.sleep(3600)

@router.message(lambda m: m.text and m.text.lower().strip().startswith(".cfg"))
@router.message(Command("gpremium"))
async def cmd_group_config(message: Message, bot: Bot):
    user_id = message.from_user.id
    first_name = html.escape(message.from_user.first_name)
    chat_id = message.chat.id
    
    if message.chat.type not in ["group", "supergroup"]:
        return
    
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        if member.status not in ["administrator", "creator"]:
            return

        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT end_date FROM premium_groups WHERE group_id = ?',
                (chat_id,)
            )
            result = await cursor.fetchone()
            has_premium = result and datetime.fromisoformat(result[0]) > datetime.now()

            response_chance = 1
            if has_premium:
                cursor = await db.execute(
                    'SELECT response_chance FROM group_config WHERE chat_id = ?',
                    (chat_id,)
                )
                config = await cursor.fetchone()
                response_chance = config[0] if config else 1

            text = (f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n ⚙️ Настройки группы\n\n"
                   f"🔹 Premium статус: {'активен' if has_premium else 'не активен'}")

            keyboard = await get_group_config_keyboard(chat_id, has_premium, response_chance, user_id)
            await message.answer(text, reply_markup=keyboard)
                
    except Exception as e:
        await message.answer(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, ⚠️ Упс, ошибка...")

@router.callback_query(F.data.startswith("config_chance_"))
async def chance_handler(callback: CallbackQuery, bot: Bot):
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return
    
    data = callback.data.split("_")
    chat_id = int(data[2])
    current_chance = int(data[3])
    initiator_id = int(data[4])
    user_id = callback.from_user.id
    first_name = html.escape(callback.from_user.first_name)

    if not await check_group_premium_status(chat_id):
        await callback.answer("❌ Требуется Premium подписка!", show_alert=True)
        return
    
    if user_id != initiator_id:
        await callback.answer("❌ Не твоя кнопка!", show_alert=True)
        return
    
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        if member.status not in ["administrator", "creator"]:
            await callback.answer("❌ Нужны права админа!", show_alert=True)
            return
    except Exception as e:
        print(f"Ошибка проверки прав: {e}")
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)
        return

    new_chance = current_chance % 3 + 1

    async with aiosqlite.connect('database.db') as db:
        await db.execute('''
            INSERT OR REPLACE INTO group_config 
            (chat_id, response_chance) VALUES (?, ?)
        ''', (chat_id, new_chance))
        await db.commit()

        cursor = await db.execute(
            'SELECT end_date FROM premium_groups WHERE group_id = ?',
            (chat_id,)
        )
        result = await cursor.fetchone()
        has_premium = result and datetime.fromisoformat(result[0]) > datetime.now()

    try:
        keyboard = await get_group_config_keyboard(chat_id, has_premium, new_chance, initiator_id)
        await callback.message.edit_text(
            f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n ⚙️ Настройки группы\n\n"
            f"🔹 Premium статус: {'активен' if has_premium else 'не активен'}",
            reply_markup=keyboard
        )
        await callback.answer(f"✅ Установлено: {new_chance}%")
    except Exception as e:
        print(f"Ошибка при обновлении сообщения: {e}")
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)

async def generate_modules_interface(
    group_id: int,
    initiator_id: int,
    bot: Bot,
    message: Message,
    user_id: int,
    first_name: str
) -> None:
    try:
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT module_name, is_active FROM group_modules WHERE group_id = ?',
                (group_id,)
            )
            modules = await cursor.fetchall()
            active_modules = {module[0]: module[1] for module in modules}

        buttons = []
        for module in available_modules:
            status = "✅" if active_modules.get(module, 0) else "❌"
            buttons.append([
                InlineKeyboardButton(
                    text=f"{html.escape(module)} {status}",
                    callback_data=f"toggle_module_{group_id}_{module}_{initiator_id}"
                )
            ])
        
        buttons.append([
            InlineKeyboardButton(
                text="🔙 Назад", 
                callback_data=f"back_to_config_{group_id}_{initiator_id}"
            )
        ])

        text = (
            f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n"
            "📦 <b>Управление модулями</b>\n\n"
            "Выберите модуль для включения/выключения:"
        )

        await message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        
    except Exception as e:
        logging.error(f"Ошибка генерации интерфейса: {e}")
        await message.answer("<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n⚠️ Упс, ошибка...")

@router.callback_query(lambda c: c.data.startswith("manage_modules_"))
async def manage_modules_handler(callback: CallbackQuery, bot: Bot):
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return
    
    try:
        data = callback.data.split('_')
        group_id = int(data[2])
        initiator_id = int(data[3])
        user_id = callback.from_user.id
        first_name = html.escape(callback.from_user.first_name)

        if not await check_group_premium_status(group_id):
            await callback.answer("❌ Требуется Premium подписка!", show_alert=True)
            return

        if user_id != initiator_id:
            await callback.answer("❌ Не твоя кнопка!", show_alert=True)
            return

        try:
            member = await bot.get_chat_member(group_id, user_id)
            if member.status not in ["administrator", "creator"]:
                await callback.answer("❌ Нужны права админа!", show_alert=True)
                return
        except Exception as e:
            await callback.answer("⚠️ Упс, ошибка...", show_alert=True)
            return
        
        await generate_modules_interface(
            group_id=group_id,
            initiator_id=initiator_id,
            bot=bot,
            message=callback.message,
            user_id=user_id,
            first_name=first_name
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Ошибка в manage_modules_handler: {e}")
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)

@router.callback_query(lambda c: c.data.startswith("toggle_module_"))
async def toggle_module_handler(callback: CallbackQuery, bot: Bot):
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return
    
    try:
        data = callback.data.split('_')
        group_id = int(data[2])
        module_name = data[3]
        initiator_id = int(data[4])
        user_id = callback.from_user.id
        first_name = html.escape(callback.from_user.first_name)

        if not await check_group_premium_status(group_id):
            await callback.answer("❌ Требуется Premium подписка!", show_alert=True)
            return

        if user_id != initiator_id:
            await callback.answer("❌ Не твоя кнопка!", show_alert=True)
            return

        try:
            member = await bot.get_chat_member(group_id, user_id)
            if member.status not in ["administrator", "creator"]:
                await callback.answer("❌ Нужны права админа!", show_alert=True)
                return
        except Exception as e:
            await callback.answer("⚠️ Упс, ошибка...", show_alert=True)
            return

        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = ?',
                (group_id, module_name)
            )
            result = await cursor.fetchone()
            new_status = 1 - (result[0] if result else 0)

            await db.execute('''
                INSERT INTO group_modules (group_id, module_name, is_active)
                VALUES (?, ?, ?)
                ON CONFLICT(group_id, module_name) DO UPDATE SET is_active = excluded.is_active
            ''', (group_id, module_name, new_status))
            await db.commit()

        await generate_modules_interface(
            group_id=group_id,
            initiator_id=initiator_id,
            bot=bot,
            message=callback.message,
            user_id=user_id,
            first_name=first_name
        )
        
        await callback.answer(
            f"Модуль {html.escape(module_name)} {'включен' if new_status else 'выключен'}"
        )
        
    except Exception as e:
        logging.error(f"Ошибка в toggle_module_handler: {e}")
        await callback.answer("⚠️ Упс, ошибка...", show_alert=True)

@router.message(lambda m: m.text and m.text.startswith(".module"))
async def handle_module_command(message: Message, bot: Bot):
    try:
        await message.delete()
    except Exception as e:
        logging.error(f"Ошибка при удалении сообщения: {e}")

    user_id = message.from_user.id
    chat_id = message.chat.id
    first_name = html.escape(message.from_user.first_name)

    if message.chat.type not in ["group", "supergroup"]:
        return

    try:
        member = await bot.get_chat_member(chat_id, user_id)
        if not await check_group_premium_status(chat_id):
            await message.answer(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n❌ Эта команда доступна только с Premium подпиской!")
            return
        
        if member.status not in ["administrator", "creator"]:
            await message.answer(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n❌ Нужны права админа!")
            return
    except Exception as e:
        await message.answer("❌ Ошибка проверки прав.")
        return

    args = message.text.split()[1:]
    response = await process_module_command(chat_id, args)
    
    formatted_response = (
        f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n"
        f"{response}"
    )
    
    await message.answer(formatted_response)

async def process_module_command(group_id: int, args: list) -> str:
    if not args:
        return "❌ Используйте <code>.module help</code> для справки."

    if args[0] == 'help':
        return (
            "🛠 Справка по командам модулей:\n\n"
            "<code>.module -a &lt;название&gt;</code> - активировать модуль\n"
            "<code>.module -d &lt;название&gt;</code> - деактивировать модуль\n"
            "<code>.module -ls</code> - список доступных модулей\n"
            "<code>.module -a -ls</code> - список активных модулей\n"
            "<code>.module help</code> - эта справка"
        )

    if args[0] == '-ls':
        modules_list = [f"{i+1}. <code>{html.escape(module)}</code>" for i, module in enumerate(available_modules)]
        return "📦 Доступные модули:\n" + "\n".join(modules_list)

    if len(args) >= 2 and args[0] == '-a' and args[1] == '-ls':
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute('''
                SELECT module_name FROM group_modules 
                WHERE group_id = ? AND is_active = 1
            ''', (group_id,))
            active_modules = await cursor.fetchall()
            
            valid_active_modules = []
            for module in active_modules:
                if module[0] in available_modules:
                    valid_active_modules.append(module[0])
                else:
                    await db.execute('''
                        DELETE FROM group_modules 
                        WHERE group_id = ? AND module_name = ?
                    ''', (group_id, module[0]))
                    await db.commit()
            
            if valid_active_modules:
                active_modules_list = [f"{i+1}. <code>{html.escape(m)}</code>" 
                                     for i, m in enumerate(valid_active_modules)]
                return "✅ Активные модули:\n" + "\n".join(active_modules_list)
            return "ℹ️ Нет активных модулей."

    if args[0] == '-a' and len(args) >= 2:
        module_name = args[1]
        if module_name not in available_modules:
            return f"❌ Модуль <code>{html.escape(module_name)}</code> не существует. Используйте <code>.module -ls</code> для списка доступных модулей."
        
        async with aiosqlite.connect('database.db') as db:
            if module_name not in available_modules:
                return f"❌ Модуль <code>{html.escape(module_name)}</code> не доступен."
                
            await db.execute('''
                INSERT INTO group_modules (group_id, module_name, is_active)
                VALUES (?, ?, 1)
                ON CONFLICT(group_id, module_name) DO UPDATE SET is_active = 1
            ''', (group_id, module_name))
            await db.commit()
        return f"✅ Модуль <code>{html.escape(module_name)}</code> активирован."

    if args[0] == '-d' and len(args) >= 2:
        module_name = args[1]
        
        if module_name not in available_modules:
            return f"❌ Модуль <code>{html.escape(module_name)}</code> не существует."
            
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute('''
                SELECT 1 FROM group_modules 
                WHERE group_id = ? AND module_name = ? AND is_active = 1
            ''', (group_id, module_name))
            exists = await cursor.fetchone()
            
            if not exists:
                return f"ℹ️ Модуль <code>{html.escape(module_name)}</code> и так не активен."
                
            await db.execute('''
                UPDATE group_modules 
                SET is_active = 0 
                WHERE group_id = ? AND module_name = ?
            ''', (group_id, module_name))
            await db.commit()
        return f"✅ Модуль <code>{html.escape(module_name)}</code> деактивирован."

    return "❌ Неверная команда. Используйте <code>.module help</code> для справки."

async def get_real_server_info():
    # 1.SYSTEM INFO
    system_info = {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
    }
    
    # 2. RAM INFO
    ram = psutil.virtual_memory()
    ram_usage = f"{ram.percent}% ({ram.used / (1024**3):.1f} GB / {ram.total / (1024**3):.1f} GB)"
    
    # 3. UPTIME
    boot_time = datetime.fromtimestamp(psutil.boot_time())
    uptime = datetime.now() - boot_time
    uptime_str = str(uptime).split('.')[0]  # Убираем микросекунды
    
    # 4. CPU
    cpu_usage = f"{psutil.cpu_percent()}%"
    cpu_count = psutil.cpu_count()
    
    # 5. DISK
    disk = psutil.disk_usage('/')
    disk_usage = f"{disk.percent}% ({disk.used / (1024**3):.1f} GB / {disk.total / (1024**3):.1f} GB)"
    
    # 6. TG LOCATION
    try:
        telegram_ip = socket.gethostbyname("api.telegram.org")
        response = requests.get(f"https://ipinfo.io/{telegram_ip}/json").json()
        telegram_location = f"{response.get('country', '?')}, {response.get('city', 'Unknown')}"
        telegram_org = response.get('org', 'Unknown')
    except Exception as e:
        telegram_location = "Не удалось определить"
        telegram_org = "Неизвестно"
    
    # 7. RUSSIAN AVIALABLE
    try:
        russian_block = requests.get("https://api.telegram.org", timeout=5).ok
        russian_block_status = "🔴 Заблокирован (РКН)" if not russian_block else "🟢 Доступен"
    except:
        russian_block_status = "🔴 Заблокирован (РКН)"
    
    # 8. NETWORK STATS
    net_io = psutil.net_io_counters()
    network_usage = {
        "bytes_sent": f"{net_io.bytes_sent / (1024**2):.2f} MB",
        "bytes_recv": f"{net_io.bytes_recv / (1024**2):.2f} MB",
        "packets_sent": net_io.packets_sent,
        "packets_recv": net_io.packets_recv
    }
    
    return {
        "server_location": "🇳🇱 Нидерланды, Amsterdam",
        "telegram_location": telegram_location,
        "telegram_org": telegram_org,
        "russian_block_status": russian_block_status,
        "system": f"{system_info['system']} {system_info['release']} ({system_info['machine']})",
        "ram_usage": ram_usage,
        "cpu_usage": f"{cpu_usage} ({cpu_count} ядер)",
        "disk_usage": disk_usage,
        "uptime": uptime_str,
        "network_usage": network_usage
    }

@router.message(Command("ping", prefix="!/."))
@router.message(F.text.lower().in_(["пинг", ".пинг", "бот", ".бот"]))
async def ping_command(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    first_name = message.from_user.first_name

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = "ping"',
            (chat_id,)
        )
        result = await cursor.fetchone()
        
    if not result or not result[0]:
        return
    
    start_time = time.time()
    msg = await message.answer("🏓 Измерение пинга и сбор системной информации...")
    end_time = time.time()
    ping = round((end_time - start_time) * 1000, 2)

    try:
        server_info = await get_real_server_info()
        response = (
            f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n"
            f"📊 <b>Системная информация:</b>\n"
            f"⏱ Пинг: <code>{ping}мс</code>\n"
            f"📍 Локация сервера: <code>{server_info['server_location']}</code>\n"
            f"🌍 Центр Telegram: <code>{server_info['telegram_location']}</code>\n"
            f"🏢 Организация: <code>{server_info['telegram_org']}</code>\n"
            f"🇷🇺 Статус в РФ: <code>{server_info['russian_block_status']}</code>\n"
            #f"💻 Система: <code>{server_info['system']}</code>\n"
            #f"🧠 RAM: <code>{server_info['ram_usage']}</code>\n"
            #f"⚡ CPU: <code>{server_info['cpu_usage']}</code>\n"
            #f"💾 Диск: <code>{server_info['disk_usage']}</code>\n"
            f"⏳ Аптайм: <code>{server_info['uptime']}</code>\n"
            f"📤 Сетевой трафик (отправлено): <code>{server_info['network_usage']['bytes_sent']}</code>\n"
            f"📥 Сетевой трафик (получено): <code>{server_info['network_usage']['bytes_recv']}</code>"
        )
    except Exception as e:
        response = f"⚠️ Ошибка получения данных: {str(e)}"
    
    await msg.edit_text(response)

async def get_supported_languages() -> Optional[List[Dict]]:
    url = "https://emkc.org/api/v2/piston/runtimes"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    return await response.json()
                return None
    except Exception as e:
        print(f"Error fetching languages: {e}")
        return None

async def execute_code(language: str, version: str, code: str) -> Optional[Dict]:
    url = "https://emkc.org/api/v2/piston/execute"
    payload = {
        "language": language,
        "version": version,
        "files": [{"content": code}]
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=15) as response:
                if response.status == 200:
                    return await response.json()
                
                error_text = await response.text()
                print(f"Piston API error {response.status}: {error_text}")
                return None
                
    except asyncio.TimeoutError:
        print("Piston API timeout")
        return None
    except Exception as e:
        print(f"Piston API exception: {str(e)}")
        return None

@router.message(lambda m: m.text and m.text.startswith(".pl"))
async def pl_command_handler(message: Message, bot: Bot):
    chat_id = message.chat.id
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = "pl"',
            (chat_id,)
        )
        result = await cursor.fetchone()
        if not result or not result[0]:
            return
    try:
        await message.delete()
    except:
        pass

    user_id = message.from_user.id
    first_name = html.escape(message.from_user.first_name)
    args = message.text.split()[1:]
    
    if not args:
        help_text = (
            f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n"
            "📚 <b>Справка по командам .pl</b>\n\n"
            "<code>.pl run &lt;язык&gt;</code> - выполнить код (ответьте на сообщение с кодом)\n"
            "<code>.pl ver &lt;язык&gt;</code> - показать версии языка\n"
            "<code>.pl langs</code> - список доступных языков\n\n"
            "Примеры:\n"
            "<code>.pl run python</code> (в ответ на сообщение с кодом)\n"
            "<code>.pl ver javascript</code>\n"
            "<code>.pl langs</code>"
        )
        await message.answer(help_text)
        return

    command = args[0].lower()
    
    if command == "langs":
        languages = await get_supported_languages()
        if not languages:
            await message.answer(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n🚫 Не удалось получить список языков")
            return
        PAGE_SIZE = 15
        pages = [languages[i:i + PAGE_SIZE] for i in range(0, len(languages), PAGE_SIZE)]
        current_page = 0
        
        lang_list = []
        for idx, lang in enumerate(pages[current_page], 1):
            lang_num = current_page * PAGE_SIZE + idx
            lang_list.append(f"{lang_num}. <code>{lang['language']}</code> ({lang['version']})")
        
        response = (
            f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n"
            f"📚 <b>Доступные языки (страница {current_page + 1}/{len(pages)}):</b>\n" + 
            "\n".join(lang_list)
        )
        
        keyboard = []
        if len(pages) > 1:
            nav_buttons = []
            if current_page > 0:
                nav_buttons.append(InlineKeyboardButton(
                    text="◀️ Назад", 
                    callback_data=f"pl_langs_{chat_id}_{user_id}_{current_page - 1}"
                ))
            if current_page < len(pages) - 1:
                nav_buttons.append(InlineKeyboardButton(
                    text="▶️ Вперед", 
                    callback_data=f"pl_langs_{chat_id}_{user_id}_{current_page + 1}"
                ))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
        
        keyboard.append([
            InlineKeyboardButton(
                text="❌ Закрыть", 
                callback_data=f"pl_close_{chat_id}_{user_id}"
            )
        ])
        
        await message.answer(
            response,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )

    elif command == "run":
        if not message.reply_to_message or not message.reply_to_message.text:
            await message.answer(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n❌ Ответьте на сообщение с кодом\nПример: <code>.pl run python</code>")
            return

        if len(args) < 2:
            await message.answer(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n❌ Укажите язык программирования\nПример: <code>.pl run python</code>")
            return

        lang_name = args[1].lower()
        code = message.reply_to_message.text
        
        processing_msg = await message.answer(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n⚙️ Выполняю код...")
        
        languages = await get_supported_languages()
        if not languages:
            await processing_msg.edit_text(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n🚫 Сервис выполнения кода недоступен")
            return

        target_lang = None
        for lang in languages:
            if lang['language'].lower() == lang_name:
                target_lang = lang
                break

        if not target_lang:
            await processing_msg.edit_text(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n🚫 Язык <code>{html.escape(lang_name)}</code> не поддерживается")
            return

        result = await execute_code(
            language=target_lang['language'],
            version=target_lang['version'],
            code=code
        )

        if not result:
            await processing_msg.edit_text(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n⚠️ Ошибка выполнения кода")
            return

        output = result.get('run', {}).get('output', 'Нет вывода')
        truncated = output[:3000] + "..." if len(output) > 3000 else output
        
        response_text = (
            f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n"
            f"🖥 <b>Результат выполнения:</b>\n"
            f"Язык: <code>{target_lang['language']}</code>\n"
            f"Версия: <code>{target_lang['version']}</code>\n\n"
            f"<code>{html.escape(truncated)}</code>"
        )
        
        await processing_msg.edit_text(response_text)

    elif command in ["ver", "version"]:
        if len(args) < 2:
            await message.answer(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n❌ Укажите язык программирования\nПример: <code>.pl ver python</code>")
            return

        lang_name = args[1].lower()
        languages = await get_supported_languages()
        if not languages:
            await message.answer(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n🚫 Сервис недоступен")
            return

        versions = []
        lang_display_name = None
        for lang in languages:
            if lang['language'].lower() == lang_name:
                versions.append(lang['version'])
                if not lang_display_name:
                    lang_display_name = lang['language']

        if not versions:
            await message.answer(f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n🚫 Язык <code>{html.escape(lang_name)}</code> не найден")
            return

        response = (
            f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n"
            f"🔍 <b>Доступные версии {lang_display_name}:</b>\n" +
            "\n".join(f"• <code>{v}</code>" for v in versions) +
            "\n\nℹ️ Используйте: <code>.pl run {язык}</code> (в ответ на код)"
        )
        await message.answer(response)

    else:
        await message.answer(
            f"👤 <a href=\"tg://user?id={user_id}\">{first_name}</a>\n\n"
            "❌ Неизвестная команда\n"
            "Используйте:\n"
            "<code>.pl run &lt;язык&gt;</code> - выполнить код\n"
            "<code>.pl ver &lt;язык&gt;</code> - версии языка\n"
            "<code>.pl langs</code> - список языков"
        )

@router.callback_query(lambda c: c.data.startswith("pl_langs_"))
async def handle_langs_pagination(callback: CallbackQuery, bot: Bot):
    data = callback.data.split("_")
    chat_id = int(data[2])
    user_id = int(data[3])
    page = int(data[4])

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = "pl"',
            (chat_id,)
        )
        module_active = await cursor.fetchone()
        
        if not module_active or not module_active[0]:
            return
    
    if callback.from_user.id != user_id:
        await callback.answer("❌ Не твоя кнопка!", show_alert=True)
        return
    
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return
    
    languages = await get_supported_languages()
    if not languages:
        await callback.answer("🚫 Не удалось получить список языков", show_alert=True)
        return
    
    PAGE_SIZE = 15
    pages = [languages[i:i + PAGE_SIZE] for i in range(0, len(languages), PAGE_SIZE)]
    
    page = max(0, min(page, len(pages) - 1))
    
    lang_list = []
    for idx, lang in enumerate(pages[page], 1):
        lang_num = page * PAGE_SIZE + idx
        lang_list.append(f"{lang_num}. <code>{lang['language']}</code> ({lang['version']})")
    
    response = (
        f"👤 <a href=\"tg://user?id={user_id}\">{callback.from_user.first_name}</a>\n\n"
        f"📚 <b>Доступные языки (страница {page + 1}/{len(pages)}):</b>\n" + 
        "\n".join(lang_list)
    )
    
    keyboard = []
    if len(pages) > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"pl_langs_{chat_id}_{user_id}_{page - 1}"))
        if page < len(pages) - 1:
            nav_buttons.append(InlineKeyboardButton(text="▶️ Вперед", callback_data=f"pl_langs_{chat_id}_{user_id}_{page + 1}"))
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton(text="❌ Закрыть", callback_data=f"pl_close_{chat_id}_{user_id}")])
    
    await callback.message.edit_text(
        response,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("pl_close_"))
async def handle_close_menu(callback: CallbackQuery, bot: Bot):
    data = callback.data.split("_")
    chat_id = int(data[2])
    user_id = int(data[3])
    
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = "pl"',
            (chat_id,)
        )
        module_active = await cursor.fetchone()
        
        if not module_active or not module_active[0]:
            return
    
    if callback.from_user.id != user_id:
        await callback.answer("❌ Не твоя кнопка!", show_alert=True)
        return
    
    if await check_flood(callback.from_user.id):
        await callback.answer("⏳ Подождите немножечко", show_alert=False)
        return
    
    try:
        await callback.message.delete()
        await callback.answer("✅ Меню закрыто")
    except Exception as e:
        await callback.answer("❌ Не удалось закрыть меню", show_alert=True)

standard_triggers = {'мими', 'mimi', 'МИМИ', 'MIMI', 'Мими', 'Mimi'}

async def add_trigger(group_id: int, trigger: str) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute('''
                INSERT INTO group_triggers (group_id, trigger)
                VALUES (?, ?)
            ''', (group_id, trigger.lower()))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def remove_trigger(group_id: int, trigger: str) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('''
            DELETE FROM group_triggers 
            WHERE group_id = ? AND trigger = ?
        ''', (group_id, trigger.lower()))
        await db.commit()
        return cursor.rowcount > 0

async def get_group_triggers(group_id: int) -> List[str]:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('''
            SELECT trigger FROM group_triggers
            WHERE group_id = ?
        ''', (group_id,))
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


@router.message(Command("triggers", prefix="."))
async def handle_triggers_command(message: Message, bot: Bot):
    chat_id = message.chat.id
    user_id = message.from_user.id
    first_name = html.escape(message.from_user.first_name)
    
    if message.chat.type not in ["group", "supergroup"]:
        return
    
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        if member.status not in ["administrator", "creator"]:
            await message.reply(f"❌ <a href=\"tg://user?id={user_id}\">{first_name}</a>, нужны права админа!")
            return
    except Exception as e:
        await message.reply("❌ Ошибка проверки прав.")
        return
    
    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = "triggers"',
            (chat_id,)
        )
        module_active = await cursor.fetchone()
        
        if not module_active or not module_active[0]:
            return
    
    args = message.text.split()[1:]
    
    if not args:
        triggers = await get_group_triggers(chat_id)
        if triggers:
            response = (
                f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, текущие триггеры:\n\n" +
                "\n".join(f"• <code>{html.escape(t)}</code>" for t in triggers)
            )
        else:
            response = f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, используются стандартные триггеры."
        await message.reply(response)
        return
    
    subcommand = args[0].lower()
    
    if subcommand == "add" and len(args) >= 2:
        trigger = ' '.join(args[1:]).strip()
        if await add_trigger(chat_id, trigger):
            await message.reply(f"✅ Триггер <code>{html.escape(trigger)}</code> успешно добавлен!")
        else:
            await message.reply(f"ℹ️ Триггер <code>{html.escape(trigger)}</code> уже существует.")
    
    elif subcommand == "remove" and len(args) >= 2:
        trigger = ' '.join(args[1:]).strip()
        if await remove_trigger(chat_id, trigger):
            await message.reply(f"✅ Триггер <code>{html.escape(trigger)}</code> успешно удалён!")
        else:
            await message.reply(f"❌ Триггер <code>{html.escape(trigger)}</code> не найден.")
    
    elif subcommand == "reset":
        async with aiosqlite.connect('database.db') as db:
            await db.execute('DELETE FROM group_triggers WHERE group_id = ?', (chat_id,))
            await db.commit()
        await message.reply("✅ Все триггеры сброшены. Будут использоваться стандартные.")
    
    else:
        help_text = (
            f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, использование команды:\n\n"
            "<code>.triggers</code> - показать текущие триггеры\n"
            "<code>.triggers add [слово]</code> - добавить триггер\n"
            "<code>.triggers remove [слово]</code> - удалить триггер\n"
            "<code>.triggers reset</code> - сбросить все триггеры"
        )
        await message.reply(help_text)
        
@router.message(Command("bansticker"))
async def banstick_command(message: Message, bot: Bot):
    await handle_ban_command(message, bot, ban_type="sticker")

@router.message(Command("banstickerpack"))
async def banpack_command(message: Message, bot: Bot):
    await handle_ban_command(message, bot, ban_type="pack")

async def handle_ban_command(message: Message, bot: Bot, ban_type: str):
    chat_id = message.chat.id
    user_id = message.from_user.id
    first_name = html.escape(message.from_user.first_name)

    if message.chat.type not in ["group", "supergroup"]:
        return

    member = await bot.get_chat_member(chat_id, user_id)
    if member.status not in ["administrator", "creator"]:
        await message.reply(f"❌ <a href=\"tg://user?id={user_id}\">{first_name}</a>, нужны права админа!")
        return

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = ?',
            (chat_id, 'bansticker')
        )
        result = await cursor.fetchone()
        if not result or not result[0]:
            return

    if not message.reply_to_message or not message.reply_to_message.sticker:
        await message.reply(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, ответь на стикер для блокировки.")
        return

    sticker = message.reply_to_message.sticker
    sticker_id = sticker.file_unique_id
    pack_name = sticker.set_name

    async with aiosqlite.connect('database.db') as db:
        if ban_type == "sticker":
            await db.execute(
                'INSERT OR IGNORE INTO blocked_stickers (group_id, sticker_id, blocked_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                (chat_id, sticker_id)
            )
            await db.commit()
            await message.reply(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, стикер <code>{sticker_id}</code> заблокирован.")
            await bot.delete_message(chat_id, message.reply_to_message.message_id)
        
        elif ban_type == "pack":
            if not pack_name:
                await message.reply(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, этот стикер не из стикерпака.")
                return
            await db.execute(
                'INSERT OR IGNORE INTO blocked_packs (group_id, pack_name, blocked_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                (chat_id, pack_name)
            )
            await db.commit()
            await message.reply(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, стикерпак <code>{pack_name}</code> заблокирован.")
            await bot.delete_message(chat_id, message.reply_to_message.message_id)

@router.message(Command("unsticker"))
async def unstick_command(message: Message, bot: Bot):
    chat_id = message.chat.id
    user_id = message.from_user.id
    first_name = html.escape(message.from_user.first_name)

    if message.chat.type not in ["group", "supergroup"]:
        return

    member = await bot.get_chat_member(chat_id, user_id)
    if member.status not in ["administrator", "creator"]:
        await message.reply(f"❌ <a href=\"tg://user?id={user_id}\">{first_name}</a>, нужны права админа!")
        return

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = ?',
            (chat_id, 'bansticker')
        )
        result = await cursor.fetchone()
        if not result or not result[0]:
            return

    args = message.text.split()[1:]
    if not args:
        await show_blocked_list(message, bot)
    else:
        await unblock_item(message, bot, args[0])

async def show_blocked_list(message: Message, bot: Bot):
    chat_id = message.chat.id
    user_id = message.from_user.id
    first_name = html.escape(message.from_user.first_name)

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT sticker_id, blocked_at FROM blocked_stickers WHERE group_id = ? ORDER BY blocked_at DESC',
            (chat_id,)
        )
        stickers = await cursor.fetchall()

        cursor = await db.execute(
            'SELECT pack_name, blocked_at FROM blocked_packs WHERE group_id = ? ORDER BY blocked_at DESC',
            (chat_id,)
        )
        packs = await cursor.fetchall()

    if not stickers and not packs:
        await message.reply(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, нет заблокированных стикеров или стикерпаков.")
        return

    response = f"<a href=\"tg://user?id={user_id}\">{first_name}</a>,\n<b>Заблокированные стикеры и стикерпаки:</b>\n\n"

    if stickers:
        response += "<b>Стикеры:</b>\n"
        for index, (sticker_id, blocked_at) in enumerate(stickers, 1):
            formatted_time = datetime.fromisoformat(blocked_at).strftime("%d.%m.%Y %H:%M:%S")
            response += f"{index}. <code>{html.escape(sticker_id)}</code> (заблокирован: {formatted_time})\n"
            if len(response) > 4000:
                break

    if packs and len(response) < 4000:
        response += "\n<b>Стикерпаки:</b>\n"
        for index, (pack_name, blocked_at) in enumerate(packs, 1):
            formatted_time = datetime.fromisoformat(blocked_at).strftime("%d.%m.%Y %H:%M:%S")
            response += f"{index}. <code>{html.escape(pack_name)}</code> (заблокирован: {formatted_time})\n"
            if len(response) > 4000:
                break

    if len(response) > 4096:
        response = response[:4093] + "..."

    await message.reply(response)

async def unblock_item(message: Message, bot: Bot, item_id: str):
    chat_id = message.chat.id
    user_id = message.from_user.id
    first_name = html.escape(message.from_user.first_name)

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'DELETE FROM blocked_stickers WHERE group_id = ? AND sticker_id = ?',
            (chat_id, item_id)
        )
        if cursor.rowcount > 0:
            await db.commit()
            await message.reply(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, стикер {item_id} разблокирован.")
            return

        cursor = await db.execute(
            'DELETE FROM blocked_packs WHERE group_id = ? AND pack_name = ?',
            (chat_id, item_id)
        )
        if cursor.rowcount > 0:
            await db.commit()
            await message.reply(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, стикерпак {item_id} разблокирован.")
            return

    await message.reply(f"<a href=\"tg://user?id={user_id}\">{first_name}</a>, не найден заблокированный стикер.")

class RegexpInlineQueryFilter(BaseFilter):
    def __init__(self, regexp: str, flags: int = 0):
        self.regexp = regexp
        self.flags = flags

    async def __call__(self, inline_query: InlineQuery) -> dict | bool:
        match = re.match(self.regexp, inline_query.query, self.flags)
        if match:
            return {"match": match}
        return False

pattern = r'^hide\s+(\d+|-?\d+)\s+(.+)'
flags = re.IGNORECASE

@router.inline_query(RegexpInlineQueryFilter(regexp=pattern, flags=flags))
async def handle_inline_hide(inline_query: InlineQuery, bot: Bot, match: re.Match):
    target_id_str = match.group(1)
    message_text = match.group(2).strip()

    if len(message_text) > 200:
        await inline_query.answer(
            results=[],
            switch_pm_text="⚠️ Сообщение не должно превышать 200 символов.",
            switch_pm_parameter="help_hidden_msg"
        )
        return

    chat_id = inline_query.from_user.id
    creator_id = inline_query.from_user.id

    try:
        target_user_id = int(target_id_str)
    except ValueError:
        await inline_query.answer(
            results=[],
            switch_pm_text="⚠️ Неверный формат ID",
            switch_pm_parameter="help_hidden_msg"
        )
        return

    message_id = str(uuid4())
    async with aiosqlite.connect('database.db') as db:
        await db.execute('''
            INSERT INTO hidden_messages (message_id, chat_id, creator_id, target_user_id, message_text)
            VALUES (?, ?, ?, ?, ?)
        ''', (message_id, chat_id, creator_id, target_user_id, message_text))
        await db.commit()

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💭 Раскрыть", callback_data=f"reveal_{message_id}")]
    ])

    target_display = f"ID {target_user_id}"
    result = InlineQueryResultArticle(
        id=message_id,
        title=f"Скрытое сообщение для {target_display}",
        input_message_content=InputTextMessageContent(
            message_text=f"<i>🤫 Скрытое сообщение для {target_display}\nот <a href=\"tg://user?id={creator_id}\">{html.escape(inline_query.from_user.first_name)}</a></i>"
        ),
        reply_markup=keyboard
    )
    
    await inline_query.answer([result], cache_time=1)

@router.callback_query(lambda c: c.data.startswith("reveal_"))
async def handle_reveal_callback(callback: CallbackQuery, bot: Bot):
    message_id = callback.data.split("_")[1]
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id if callback.message else user_id

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT creator_id, target_user_id, message_text FROM hidden_messages WHERE message_id = ?',
            (message_id,)
        )
        result = await cursor.fetchone()
        
        if not result:
            await callback.answer("⚠️ Сообщение не найдено или удалено.", show_alert=True)
            return
        
        creator_id, target_user_id, message_text = result
        
        if user_id not in [creator_id, target_user_id]:
            await callback.answer("☠ Anti-Piracy Screen ☠\n\tYour information is being sent to the proper authorities.\n\tDo not attempt to turn on the button again.\n\tPiracy carries up to 10 years imprisonment and a 10,000 fine", show_alert=True)
            return

        await callback.answer(message_text, show_alert=True)

@router.message(F.sticker)
async def check_sticker(message: Message, bot: Bot):
    chat_id = message.chat.id
    sticker = message.sticker
    sticker_id = sticker.file_unique_id
    pack_name = sticker.set_name

    async with aiosqlite.connect('database.db') as db:
        cursor = await db.execute(
            'SELECT is_active FROM group_modules WHERE group_id = ? AND module_name = ?',
            (chat_id, 'bansticker')
        )
        result = await cursor.fetchone()
        if not result or not result[0]:
            return

        cursor = await db.execute(
            'SELECT 1 FROM blocked_stickers WHERE group_id = ? AND sticker_id = ?',
            (chat_id, sticker_id)
        )
        if await cursor.fetchone():
            await message.delete()
            return

        if pack_name:
            cursor = await db.execute(
                'SELECT 1 FROM blocked_packs WHERE group_id = ? AND pack_name = ?',
                (chat_id, pack_name)
            )
            if await cursor.fetchone():
                await message.delete()
                return

#==============================================================================================
#==============================================================================================
#==============================================================================================

class MessageTracker:
    def __init__(self, message_limit: int = 3, time_window: int = 5):
        self.message_limit = message_limit
        self.time_window = time_window
        self.user_messages: Dict[int, deque] = {}
        
    def is_spam(self, user_id: int) -> bool:
        if user_id not in self.user_messages:
            self.user_messages[user_id] = deque()
            
        messages = self.user_messages[user_id]
        current_time = datetime.now()
        
        while messages and (current_time - messages[0]) > timedelta(seconds=self.time_window):
            messages.popleft()
            
        if len(messages) >= self.message_limit:
            logger.warning(f"Spam detected from user {user_id}: {len(messages)} messages in {self.time_window} seconds")
            return True
            
        messages.append(current_time)
        return False

class ConnectionPool:
    def __init__(self, api_key: str, pool_size: int = 5):
        self.api_key = api_key
        self.pool_size = pool_size
        self.connections: List = []
        self.locks: List[asyncio.Lock] = []
        self.initialized = False
        self.init_lock = asyncio.Lock()
        
    async def initialize(self):
        if self.initialized:
            return
            
        async with self.init_lock:
            if self.initialized:
                return
                
            for _ in range(self.pool_size):
                client = await get_client(token=self.api_key)
                await client.account.fetch_me()
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
        if self.message_tracker.is_spam(user_id):
            logger.warning(f"Ignoring message from user {user_id} due to spam protection")
            return None
            
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.info(f"Waiting {delay:.1f} seconds before processing message for user {user_id}")
        await asyncio.sleep(delay)
        
        if user_id not in self.chat_locks:
            self.chat_locks[user_id] = asyncio.Lock()
            
        try:
            conn_id, client, conn_lock = await self.pool.get_connection()
            
            async with conn_lock:
                try:
                    if user_id not in self.user_chats:
                        chat, greeting_message = await client.chat.create_chat(self.char_id)
                        self.user_chats[user_id] = {
                            'chat_id': chat.chat_id,
                            'last_activity': datetime.now()
                        }
                        logger.info(f"Created new chat for user {user_id}")
                    
                    chat_data = self.user_chats[user_id]
                    
                    answer = await client.chat.send_message(
                        self.char_id,
                        chat_data['chat_id'],
                        message
                    )
                    
                    chat_data['last_activity'] = datetime.now()
                    return answer.get_primary_candidate().text
                    
                except SessionClosedError as e:
                    logger.error(f"Session closed for user {user_id}: {e}")
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

chat_manager = ChatManager(
    api_key='',  # TOKEN ACCOUNT
    char_id='cYXxq0NFDa8lHhgtiAdv-9a534eDWbg-YiUtIfX7yoE'  # CHARACTER ID
)

async def ensure_group_exists(chat_id: int, chat_title: str) -> None:
    current_timestamp = int(datetime.now().timestamp())
    
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('SELECT chat_id FROM groups WHERE chat_id = ?', (chat_id,))
        existing_group = await cursor.fetchone()
        
        if not existing_group:
            await db.execute('''
                INSERT INTO groups 
                (chat_id, message_count, joined_timestamp, title, is_active)
                VALUES (?, 0, ?, ?, TRUE)
            ''', (chat_id, current_timestamp, chat_title))
            await db.commit()

@router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def group_message_handler(message: types.Message, bot: Bot):
    if not message.text:
        return

    try:
        async with aiosqlite.connect('database.db') as db:
            cursor = await db.execute(
                'SELECT 1 FROM groups WHERE chat_id = ?',
                (message.chat.id,)
            )
            group_exists = await cursor.fetchone()
            
            if not group_exists:
                await db.execute(
                    '''INSERT INTO groups (chat_id, title, joined_timestamp) 
                    VALUES (?, ?, ?)''',
                    (message.chat.id, message.chat.title, int(time.time()))
                )
                await db.execute(
                    'INSERT INTO group_config (chat_id, response_chance) VALUES (?, ?)',
                    (message.chat.id, 1)
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
            
            if triggers_module_active and triggers_module_active[0]:
                custom_triggers = await get_group_triggers(message.chat.id)
                triggers.update(custom_triggers)
            
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
    
    current_timestamp = int(datetime.now().timestamp())
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT OR IGNORE INTO users (user_id, username, joined_timestamp)
            VALUES (?, ?, ?)
        ''', (message.from_user.id, message.from_user.username, current_timestamp))
        await db.commit()

    animation = FSInputFile("start.mp4")
    
    await message.answer_animation(
        animation=animation,
        caption=(
            "🩷 Привет, дорогой! Меня зовут <b>Мими 🍼</b>, и я очень рада быть "
            "добавленной в твою замечательную чат-группу.\n\n"
            "Я могу отвечать на ваши сообщение таким образом, который <b>все будут любить!</b> 💕 \n\n"
            "<i>PS: Вы автоматически подтвежаете с <a href='https://telegra.ph/Politika-Konfidencialnosti-i-Usloviya-Ispolzovaniya-03-27'>Условиями использования</a></i>"
        ), reply_markup=keyboard
    )

async def main():
    await init_db()
    bot = Bot(
        token="",
        default=DefaultBotProperties(parse_mode="HTML") 
    )
    
    asyncio.create_task(check_expired_group_premium(bot))
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
