import asyncio
import aiosqlite
import re
from typing import List, Set, Dict
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import CommandStart, Command
from aiogram.enums import ChatType
import random
from datetime import datetime
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import io
from PIL import Image
import numpy as np
from aiogram.types import BufferedInputFile
from aiogram.types import FSInputFile
from aiogram.enums import ChatAction
import os
import time
import logging
from aiohttp import ClientSession
from typing import Optional
from aiogram.utils.markdown import hbold

from g4f.client import AsyncClient

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
                is_active BOOLEAN DEFAULT TRUE,
                left_by_admin BOOLEAN DEFAULT FALSE
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
            CREATE TABLE IF NOT EXISTS mailings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                message_type TEXT NOT NULL,
                total_groups INTEGER NOT NULL,
                successful INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                completed BOOLEAN DEFAULT 0,
                timestamp REAL NOT NULL
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS user_mood (
                user_id INTEGER,
                chat_id INTEGER,
                mood TEXT,
                timestamp INTEGER,
                PRIMARY KEY (user_id, chat_id)
            )
        ''')
        
        await db.execute('''
            CREATE TABLE IF NOT EXISTS user_cooldown (
                 user_id INTEGER NOT NULL,
                 chat_id INTEGER NOT NULL,
                 last_used INTEGER NOT NULL,
                 PRIMARY KEY (user_id, chat_id)
             );
        ''')
        await db.commit()
        
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
                print(f"Error sending GIF {random_gif_path}: {e}")
            
        await update_last_media_timestamp(message.chat.id)
        
    except Exception as e:
        print(f"Error sending daily media: {e}")

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

async def get_group_words(chat_id: int) -> Set[str]:
    """Получение слов только для конкретной группы"""
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            'SELECT word FROM words WHERE chat_id = ?',
            (chat_id,)
        ) as cursor:
            words = await cursor.fetchall()
            return {word[0] for word in words}

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

async def get_admin_stats() -> Dict:
    """Получение общей статистики для админа"""
    async with aiosqlite.connect(DB_NAME) as db:
        # Количество групп
        async with db.execute('SELECT COUNT(*) FROM groups') as cursor:
            group_count = (await cursor.fetchone())[0]

        # Количество пользователей
        async with db.execute('SELECT COUNT(*) FROM users') as cursor:
            user_count = (await cursor.fetchone())[0]

        return {
            'groups': group_count,
            'users': user_count
        }

@router.my_chat_member()
async def on_chat_member_updated(event: types.ChatMemberUpdated):
    if event.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:

        current_timestamp = int(datetime.now().timestamp())
        is_active = event.new_chat_member.status not in ["left", "kicked"]
        
        async with aiosqlite.connect(DB_NAME) as db:
            cursor = await db.execute('''
                SELECT is_active, left_by_admin 
                FROM groups 
                WHERE chat_id = ? 
                ORDER BY joined_timestamp DESC 
                LIMIT 1
            ''', (event.chat.id,))
            group_history = await cursor.fetchone()
            
            if group_history and not group_history[0] and group_history[1]:
                await event.bot.leave_chat(event.chat.id)
                return
            
            if group_history:
                await db.execute('''
                    UPDATE groups 
                    SET joined_timestamp = ?, 
                        title = ?, 
                        is_active = ?,
                        left_by_admin = FALSE
                    WHERE chat_id = ?
                ''', (current_timestamp, event.chat.title, is_active, event.chat.id))
            else:
                await db.execute('''
                    INSERT INTO groups 
                    (chat_id, message_count, joined_timestamp, title, is_active, left_by_admin)
                    VALUES (?, 0, ?, ?, ?, FALSE)
                ''', (event.chat.id, current_timestamp, event.chat.title, is_active))
            
            await db.commit()
            
            if is_active:
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
        
        try:
            # Создаем изображение
            image = await create_stats_image(message.chat.id, stats)
            
            # Формируем подпись
            caption = (
                f"📊 <b>Статистика группы:</b>\n"
                f"🤍 Сообщений получено: <code>{stats['messages']}</code>\n"
                f"💌 Слов собрано: <code>{stats['words']}</code>"
            )
            
            if stats['messages'] < REQUIRED_MESSAGES:
                caption += f"\n🤍 До активации <b>Mimi Typh</b> осталось: <code>{REQUIRED_MESSAGES - stats['messages']}</code> сообщений."
            
            # Отправляем фото с подписью
            await message.answer_photo(
                photo=image,
                caption=caption, parse_mode='HTML'
            )
            
        except Exception as e:
            print(f"Error creating stats image: {e}")
            await message.answer(caption)

# Обновленный парсинг ответа
def parse_gpt_response(response: str) -> tuple:
    """
    Парсит ответ GPT и возвращает:
    - user_arousal: уровень возбуждения пользователя
    - mimi_arousal: уровень возбуждения Mimi
    - highlights: список ключевых моментов (максимум 3)
    """
    user_arousal = "0%"
    mimi_arousal = "0%"
    highlights = []

    try:
        # Ищем проценты возбуждения
        user_match = re.search(r"User Arousal:\s*(\d+%)", response)
        mimi_match = re.search(r"Mimi Arousal:\s*(\d+%)", response)

        if user_match:
            user_arousal = user_match.group(1)
        if mimi_match:
            mimi_arousal = mimi_match.group(1)

        # Ищем ключевые моменты
        for line in response.split("\n"):
            if re.match(r"^\d+\.\s+(User|Mimi):", line):
                highlights.append(line.strip())

        # Ограничиваем количество ключевых моментов до 3
        highlights = highlights[:3]

    except Exception as e:
        logging.error(f"Ошибка при парсинге ответа GPT: {e}")

    return user_arousal, mimi_arousal, highlights

# Обновленный вывод результата
async def send_arousal_result(message: types.Message, user_arousal: str, mimi_arousal: str, highlights: list):
    """
    Формирует и отправляет результат анализа.
    """
    # Определяем эмодзи для Mimi
    mimi_percent = int(mimi_arousal.strip('%'))
    emoji = ""
    if 20 <= mimi_percent < 50:
        emoji = "🤍"  # Легкое возбуждение (белое сердечко)
    elif 50 <= mimi_percent <= 60:
        emoji = "💓"  # Среднее возбуждение (пульсирующее сердечко)
    elif 70 <= mimi_percent <= 80:
        emoji = "❤️"  # Высокое возбуждение (красное сердечко)
    elif 90 <= mimi_percent < 100:
        emoji = "❤️‍🔥"  # Очень высокое возбуждение (горящее сердечко)
    elif mimi_percent >= 100:
        emoji = "🖤"  # Максимальное возбуждение (черное сердечко)

    result = [
        f"<b>📊 Уровень любви:</b>",
        f"👤 Пользователь: {user_arousal}",
        f"🍼 Mimi: {mimi_arousal} {emoji}"
    ]

    # Добавляем ключевые моменты только если они есть
    if highlights:
        result.append("")  # Пустая строка для разделения
        result.append("<b>💖 Ключевые моменты:</b>")
        result.extend(highlights[:5])  # Ограничиваем 5 пунктами

    await message.reply("\n".join(result), parse_mode='HTML')

@router.message(Command("arousal"))
async def arousal_command_handler(message: types.Message, state: FSMContext):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        await message.reply("🚫 Эта команда доступна только в группах!")
        return

    user_id = message.from_user.id
    chat_id = message.chat.id
    current_time = int(datetime.now().timestamp())

    try:
        async with aiosqlite.connect(DB_NAME) as db:
            # Проверка кулдауна (60 секунд)
            cursor = await db.execute(
                'SELECT last_used FROM user_cooldown WHERE user_id = ? AND chat_id = ?',
                (user_id, chat_id)
            )
            cooldown = await cursor.fetchone()

            if cooldown:
                last_used = cooldown[0]
                if current_time - last_used < 10:
                    remaining = 10 - (current_time - last_used)
                    await message.reply(f"⏳ Пожалуйста, подождите {remaining} секунд.")
                    return

            # Записываем интервал после проверки кулдауна
            await db.execute(
                'INSERT OR REPLACE INTO user_cooldown (user_id, chat_id, last_used) VALUES (?, ?, ?)',
                (user_id, chat_id, current_time)
            )
            await db.commit()

            cursor = await db.execute('''
                SELECT user_id, message_text, timestamp 
                FROM message_history 
                WHERE chat_id = ? 
                AND (
                    (user_id = ?)
                    OR 
                    (user_id = 0 AND target_user_id = ?)
                )
                ORDER BY timestamp DESC 
                LIMIT 30
            ''', (chat_id, user_id, user_id))
            messages = await cursor.fetchall()
            messages = messages[::-1]  # Переворот для хронологического порядка
            
            dialog = []
            for idx, msg in enumerate(messages, 1):
                role = "Mimi" if msg[0] == 0 else "User"
                dialog.append(f"{idx}. {role}: {msg[1]}")

            if len(messages) < 5:
                await message.reply("❌ Недостаточно сообщений!")
                return

            dialog_text = "\n".join(dialog)
            truncated_text = dialog_text[:4096 * 4]

            # Отправляем в GPT-4
            async with GPTManager() as gpt:
                response = await gpt.analyze_arousal(truncated_text)

            # Парсим ответ
            user_arousal, mimi_arousal, highlights = parse_gpt_response(response)

            # Отправляем результат
            await send_arousal_result(message, user_arousal, mimi_arousal, highlights)

    except Exception as e:
        logging.error(f"Arousal Error: {e}")

class GPTManager:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        self.session = ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def analyze_mood(self, text: str) -> str:
        """Анализ настроения с закрытием сессии"""
        prompt = """Определи настроение текста одним словом из списка:
        Положительные:
        Счастье, Радость, Вдохновение, Оптимизм, Уверенность, Любовь, 
        Восторг, Легкость, Удовлетворение, Гармония, Надежда, 
        Восторженность, Успокойствие, Радостное ожидание, Гордость, Веселье

        Негативные:
        Грусть, Тоска, Гнев, Раздражение, Страх, Беспокойство, Одиночество,
        Зависть, Печаль, Тревога, Разочарование, Обида, Скорбь,
        Тоска по прошлому, Отчаяние, Ненависть, Вина, Угнетенность

        Нейтральные:
        Спокойствие, Безразличие, Ожидание, Равнодушие, Пассивность,
        Усталость, Невозмутимость, Рутинность, Неспешность

        Переменные:
        Беспокойство, Паника, Эйфория, Подъем, Нервозность, Неопределенность,
        Раздражение, Волнение, Непонимание, Чувство вины, Неудовлетворенность

        Текст: '{text}'
        Настроение:""".format(text=text[:2000])  # Ограничение длины текста

        try:
            async with ClientSession() as session:
                client = AsyncClient(session=session)
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=50
                )
                return response.choices[0].message.content.strip()
        except Exception as e:
            logging.error(f"GPT Error: {e}")
            return "Неопределенность"

    async def analyze_arousal(self, text: str) -> str:
        """Анализ возбуждения с улучшенным промптом"""
        try:
            # Явно структурированный промпт
            system_prompt = """Ты профессиональный анализатор диалогов. 
            Анализируй ТОЛЬКО предоставленные сообщения. Формат ответа СТРОГО:
            
            User Arousal: X%
            Mimi Arousal: Y%
            [Номер]. [Роль]: [Текст]

            Правила:
            1. Проценты - целые числа от 0 до 100
            2. Номера сообщений только из списка
            3. Если контента нет - оба значения 0%
            4. Ключевые моменты только с явным сексуальным подтекстом"""

            async with ClientSession() as session:
                client = AsyncClient(session=session)
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",  # Используем полную версию GPT-4
                    messages=[
                        {
                            "role": "system",
                            "content": system_prompt
                        },
                        {
                            "role": "user",
                            "content": text
                        }
                    ],
                    max_tokens=500,
                    temperature=0.1,  # Минимизируем случайность
                    top_p=0.1,
                    frequency_penalty=0.5
                )
                
                result = response.choices[0].message.content.strip()
                
                # Валидация базового формата
                if not re.search(r"User Arousal:\s*\d+%", result):
                    raise ValueError("Некорректный формат ответа")
                    
                return result
                
        except Exception as e:
            logging.error(f"GPT-4 Arousal Error: {str(e)}")
            return "User Arousal: 0%\nMimi Arousal: 0%"  # Возвращаем безопасный формат

@router.message(Command("mood"))
async def mood_command_handler(message: types.Message):
    if message.chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
        await message.reply("🚫 Эта команда доступна только в группах!")
        return

    user_id = message.from_user.id
    chat_id = message.chat.id

    try:
        current_time = int(datetime.now().timestamp())

        async with aiosqlite.connect(DB_NAME) as db:
            # Проверяем кулдаун
            cursor = await db.execute(
                'SELECT last_used FROM user_cooldown WHERE user_id = ? AND chat_id = ?',
                (user_id, chat_id)
            )
            cooldown = await cursor.fetchone()

            if cooldown:
                last_used = cooldown[0]
                if current_time - last_used < 10:
                    remaining = 10 - (current_time - last_used)
                    await message.reply(f"⏳ Пожалуйста, подождите {remaining} секунд.")
                    return

            # Получаем историю сообщений
            cursor = await db.execute('''
                SELECT message_text 
                FROM message_history 
                WHERE chat_id = ? 
                AND user_id = 0
                ORDER BY timestamp DESC 
                LIMIT 5
            ''', (chat_id,))
            messages = await cursor.fetchall()

            if not messages:
                await message.reply("😶 Мими еще не общалась с вами в этой группе.")
                return

            # Сохраняем результат настроения
            await db.execute(
                'INSERT OR REPLACE INTO user_cooldown (user_id, chat_id, last_used) VALUES (?, ?, ?)',
                (user_id, chat_id, current_time)
            )
            await db.commit()

            # Анализ настроения
            async with GPTManager() as gpt:
                mood = await gpt.analyze_mood("\n".join([m[0] for m in messages]))

            timestamp = int(datetime.now().timestamp())
            await db.execute('''
                INSERT OR REPLACE INTO user_mood 
                VALUES (?, ?, ?, ?)
            ''', (user_id, chat_id, mood, timestamp))
            await db.commit()

            # Форматируем ответ
            emoji_map = {
                # Положительные
                'Счастье': '🌈', 'Радость': '😊', 'Вдохновение': '✨',
                'Оптимизм': '🌞', 'Уверенность': '💪', 'Любовь': '❤️',
                'Вострог': '🤩', 'Легкость': '🍃', 'Удовлетворение': '😌',
                'Гармония': '🎶', 'Надежда': '🌟', 'Востроженность': '🎉',
                'Успокойствие': '🧘', 'Радостное ожидание': '🎁', 
                'Гордость': '🦁', 'Веселье': '🎪',
                
                # Негативные
                'Грусть': '😢', 'Тоска': '🌧', 'Гнев': '💢', 
                'Раздражение': '😠', 'Страх': '😨', 'Беспокойство': '😟',
                'Одиночество': '🚶', 'Зависть': '💚', 'Печаль': '😞',
                'Тревога': '😖', 'Разочарование': '😣', 'Обида': '💔',
                'Скорбь': '⚰️', 'Тоска по прошлому': '🕰', 
                'Отчаяние': '😫', 'Ненависть': '👿', 'Вина': '😳',
                'Угнетенность': '🏚',
                
                # Нейтральные
                'Спокойствие': '😐', 'Безразличие': '🫤', 'Ожидание': '⏳',
                'Равнодушие': '😶', 'Пассивность': '🛌', 'Усталость': '😴',
                'Невозмутимость': '🎭', 'Рутинность': '🔁', 'Неспешность': '🐌',
                
                # Переменные
                'Паника': '😱', 'Эйфория': '🥴', 'Подъем': '🚀',
                'Нервозность': '😬', 'Неопределенность': '🎲',
                'Волнение': '🥺', 'Непонимание': '❓', 
                'Чувство вины': '😔', 'Неудовлетворенность': '🤷'
            }

            await message.reply(
                f"{emoji_map.get(mood, '🌀')} <b>Текущее настроение Мими к вам:</b> <i>{mood}</i>", 
                parse_mode='HTML'
            )

    except Exception as e:
        logging.error(f"Mood Error: {e}")

async def export_database_content() -> str:
    """Export database content to a text file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"database_export_{timestamp}.txt"
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            async with aiosqlite.connect(DB_NAME) as db:
                f.write(f"База данных: {DB_NAME}\n")
                f.write(f"Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Get all tables
                async with db.execute("SELECT name FROM sqlite_master WHERE type='table'") as cursor:
                    tables = await cursor.fetchall()
                    
                    for table in tables:
                        table_name = table[0]
                        f.write(f"\n{'=' * 50}\n")
                        f.write(f"Таблица: {table_name}\n")
                        f.write(f"{'=' * 50}\n\n")
                        
                        async with db.execute(f"SELECT * FROM {table_name}") as table_cursor:
                            columns = [description[0] for description in table_cursor.description]
                            f.write("Столбцы: " + " | ".join(columns) + "\n")
                            f.write("-" * 50 + "\n")
                            
                            rows = await table_cursor.fetchall()
                            for row in rows:
                                formatted_row = " | ".join(str(item) for item in row)
                                f.write(formatted_row + "\n")
                            
                            f.write(f"\nВсего записей: {len(rows)}\n")
        
        return output_file
    except Exception as e:
        print(f"Error during export: {e}")
        return None

async def get_groups_info() -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('''
            SELECT chat_id, message_count, joined_timestamp, title, is_active
            FROM groups
            ORDER BY joined_timestamp DESC
        ''') as cursor:
            groups = await cursor.fetchall()
            return [{
                'chat_id': g[0],
                'message_count': g[1], 
                'joined_date': datetime.fromtimestamp(g[2]).strftime('%d.%m.%Y'),
                'title': g[3],
                'is_member': g[4]
            } for g in groups]
            
            groups_info = []
            for group in groups:
                chat_id, message_count, joined_timestamp = group
                try:
                    # Получаем информацию о группе через бота
                    chat = await router.bot.get_chat(chat_id)
                    
                    groups_info.append({
                        'chat_id': chat_id,
                        'title': chat.title,
                        'is_member': True,
                        'message_count': message_count,
                        'joined_date': datetime.fromtimestamp(joined_timestamp).strftime('%d.%m.%Y')
                    })
                except Exception:
                    groups_info.append({
                        'chat_id': chat_id,
                        'title': "Неизвестная группа",
                        'is_member': False,
                        'message_count': message_count,
                        'joined_date': datetime.fromtimestamp(joined_timestamp).strftime('%d.%m.%Y')
                    })
            
            return groups_info

class MailingStates(StatesGroup):
    waiting_for_mailing_message = State()

@router.message(Command("admin"))
async def admin_handler(message: Message):
    """Обработчик команды /admin"""
    if message.from_user.id == ADMIN_USER_ID:
        stats = await get_admin_stats()
        
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📊 Информация о группах",
                        callback_data="show_groups"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📨 Создать рассылку",
                        callback_data="create_mailing"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📥 Получить в txt",
                        callback_data="export_database"
                    )
                ]
            ]
        )
        
        await message.reply(
            f"👑 <b>Админ-панель:</b>\n"
            f"📱 Количество групп: <code>{stats['groups']}</code>\n"
            f"👤 Количество пользователей: <code>{stats['users']}</code>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )

# Add the export callback handler
@router.callback_query(F.data == "export_database")
async def export_database_handler(callback: CallbackQuery):
    """Handle database export request"""
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("У вас нет прав администратора!", show_alert=True)
        return
        
    await callback.answer("⏳ Экспортирую базу данных...", show_alert=True)
    
    # Export the database
    output_file = await export_database_content()
    
    if output_file and os.path.exists(output_file):
        try:
            # Send the file
            file = FSInputFile(output_file)
            await callback.message.answer_document(
                document=file,
                caption="📥 Экспорт базы данных завершен!"
            )
            
            # Clean up the file
            os.remove(output_file)
            
        except Exception as e:
            print(f"Error sending file: {e}")
            await callback.message.answer("❌ Произошла ошибка при отправке файла.")
    else:
        await callback.message.answer("❌ Произошла ошибка при экспорте базы данных.")

# Обработчик создания рассылки
@router.callback_query(F.data == "create_mailing")
async def create_mailing_handler(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_USER_ID:
        return
        
    await callback.message.edit_text(
        "📨 <b>Создание рассылки</b>\n\n"
        "Отправьте сообщение, которое хотите разослать.\n"
        "Поддерживаются все типы сообщений (текст, фото, видео и т.д.)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔙 Назад",
                        callback_data="back_to_admin"
                    )
                ]
            ]
        )
    )
    # Устанавливаем состояние
    await state.set_state(MailingStates.waiting_for_mailing_message)

# Обработчик получения сообщения для рассылки
@router.message(MailingStates.waiting_for_mailing_message)
async def process_mailing_message(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_USER_ID:
        return
        
    # Сохраняем сообщение для рассылки
    await state.update_data(mailing_message=message)
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Начать рассылку",
                    callback_data="confirm_mailing"
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data="cancel_mailing"
                )
            ]
        ]
    )
    
    await message.reply(
        "📨 <b>Подтверждение рассылки</b>\n\n"
        "Выше представлено сообщение для рассылки.\n"
        "Подтвердите отправку или отмените рассылку.",
        parse_mode="HTML",
        reply_markup=keyboard
    )

# Обработчик подтверждения рассылки
@router.callback_query(F.data == "confirm_mailing")
async def confirm_mailing_handler(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_USER_ID:
        return
        
    data = await state.get_data()
    mailing_message = data.get("mailing_message")
    
    if not mailing_message:
        await callback.answer("❌ Ошибка: сообщение для рассылки не найдено")
        return
        
    # Получаем список активных групп
    groups = await get_groups_info()
    active_groups = [group for group in groups if group['is_member']]
    
    # Создаем запись о рассылке в БД
    mailing_id = await create_mailing_record(
        admin_id=callback.from_user.id,
        message_type=mailing_message.content_type,
        total_groups=len(active_groups)
    )
    
    # Счетчики для статистики
    successful = 0
    failed = 0
    
    # Выполняем рассылку
    progress_message = await callback.message.answer(
        "📤 Выполняется рассылка...\n"
        "⏳ Задержка между отправками: 0.5 секунд"
    )
    
    for group in active_groups:
        try:
            # Копируем исходное сообщение в группу
            await mailing_message.copy_to(
                chat_id=group['chat_id'],
                parse_mode="HTML"
            )
            successful += 1
            
            # Обновляем прогресс каждые 5 групп
            if successful % 5 == 0:
                await progress_message.edit_text(
                    f"📤 Выполняется рассылка...\n"
                    f"✅ Отправлено: {successful}\n"
                    f"❌ Ошибок: {failed}\n"
                    f"📊 Прогресс: {successful + failed}/{len(active_groups)}\n"
                    f"⏳ Задержка между отправками: 0.5 секунд"
                )
            
            # Задержка между отправками
            await asyncio.sleep(0.5)
            
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка при отправке в группу {group['chat_id']}: {str(e)}")
            await asyncio.sleep(0.5)
    
    # Обновляем статистику рассылки в БД
    await update_mailing_stats(mailing_id, successful, failed)
    
    total_time = (successful + failed) * 0.5
    await progress_message.edit_text(
        f"📨 <b>Рассылка завершена</b>\n\n"
        f"✅ Успешно отправлено: <code>{successful}</code>\n"
        f"❌ Ошибок отправки: <code>{failed}</code>\n"
        f"📊 Всего групп: <code>{len(active_groups)}</code>\n"
        f"⏱ Время выполнения: <code>{total_time:.1f}</code> сек",
        parse_mode="HTML"
    )
    
    await state.clear()

@router.callback_query(F.data == "cancel_mailing")
async def cancel_mailing_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "❌ Рассылка отменена",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔙 Вернуться в админ-панель",
                        callback_data="back_to_admin"
                    )
                ]
            ]
        )
    )

@router.callback_query(F.data == "back_to_admin")
async def back_to_admin_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    
    # Получаем актуальную статистику
    stats = await get_admin_stats()
    
    # Создаем клавиатуру админ-панели
    keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📊 Информация о группах",
                        callback_data="show_groups"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📨 Создать рассылку",
                        callback_data="create_mailing"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📥 Получить в txt",
                        callback_data="export_database"
                    )
                ]
            ]
        )
    
    # Обновляем сообщение с админ-панелью
    await callback.message.edit_text(
        f"👑 <b>Админ-панель:</b>\n"
        f"📱 Количество групп: <code>{stats['groups']}</code>\n"
        f"👤 Количество пользователей: <code>{stats['users']}</code>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

# Вспомогательные функции для работы с БД
async def create_mailing_record(admin_id: int, message_type: str, total_groups: int) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('''
            INSERT INTO mailings (admin_id, message_type, total_groups, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (admin_id, message_type, total_groups, datetime.now().timestamp()))
        await db.commit()
        return cursor.lastrowid

async def update_mailing_stats(mailing_id: int, successful: int, failed: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            UPDATE mailings 
            SET successful = ?, failed = ?, completed = 1
            WHERE id = ?
        ''', (successful, failed, mailing_id))
        await db.commit()

@router.callback_query(lambda c: c.data.startswith(("show_groups", "page_")))
async def show_groups_callback(callback_query: CallbackQuery):
    """Обработчик нажатия на кнопку показа групп с пагинацией"""
    if callback_query.from_user.id != ADMIN_USER_ID:
        await callback_query.answer("У вас нет прав администратора!", show_alert=True)
        return

    # Получаем текущую страницу из callback_data
    current_page = 1
    if callback_query.data.startswith("page_"):
        current_page = int(callback_query.data.split("_")[1])
    
    groups_info = await get_groups_info()
    
    if not groups_info:
        await callback_query.answer("Нет доступных групп", show_alert=True)
        return
    
    # Настройки пагинации
    items_per_page = 5
    total_pages = (len(groups_info) + items_per_page - 1) // items_per_page
    
    # Получаем группы для текущей страницы
    start_idx = (current_page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    current_groups = groups_info[start_idx:end_idx]
    
    # Формируем сообщение со списком групп
    message_text = f"📋 <b>Информация о группах (стр. {current_page}/{total_pages}):</b>\n\n"
    
    for group in current_groups:
        status = "✅ Активен" if group['is_member'] else "❌ Не активен"
        message_text += (
            f"👥 <b>Группа:</b> {group['title']}\n"
            f"📝 ID: <code>{group['chat_id']}</code>\n"
            f"📊 Сообщений: <code>{group['message_count']}</code>\n"
            f"📅 Дата добавления: <code>{group['joined_date']}</code>\n"
            f"⚡️ Статус: {status}\n\n"
        )
    
    # Создаем кнопки пагинации
    keyboard = []
    
    # Добавляем кнопки навигации
    nav_buttons = []
    if current_page > 1:
        nav_buttons.append(InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=f"page_{current_page-1}"
        ))
    
    if current_page < total_pages:
        nav_buttons.append(InlineKeyboardButton(
            text="Вперед ▶️",
            callback_data=f"page_{current_page+1}"
        ))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Добавляем кнопку обновления
    keyboard.append([
        InlineKeyboardButton(
            text="🔄 Обновить",
            callback_data=f"show_groups_{int(datetime.now().timestamp())}"
        )
    ])
    keyboard.append([
        InlineKeyboardButton(
            text="🚪 Покинуть группу", 
            callback_data="start_leave_group"
        )
    ])
    
    
    try:
        await callback_query.message.edit_text(
            message_text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
        await callback_query.answer("Информация обновлена")
    except aiogram.exceptions.TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await callback_query.answer("Информация актуальна", show_alert=True)
        else:
            print(f"Error updating message: {str(e)}")
            await callback_query.answer(
                "Произошла ошибка при обновлении информации.",
                show_alert=True
            )
    except Exception as e:
        print(f"Error updating message: {str(e)}")
        await callback_query.answer(
            "Произошла ошибка при обновлении информации.",
            show_alert=True
        )
        
class LeaveGroupStates(StatesGroup):
    waiting_for_group_id = State()

@router.callback_query(F.data == "start_leave_group")
async def start_leave_group(callback: CallbackQuery, state: FSMContext):
    """Обработчик нажатия на кнопку выхода из группы"""
    if callback.from_user.id != ADMIN_USER_ID:
        await callback.answer("У вас нет прав администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "Пожалуйста, отправьте ID группы, которую нужно покинуть:\n\n"
        "❗️ Для отмены нажмите кнопку ниже",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_leave")]
        ])
    )
    await state.set_state(LeaveGroupStates.waiting_for_group_id)
    await callback.answer()

@router.callback_query(F.data == "cancel_leave")
async def cancel_leave(callback: CallbackQuery, state: FSMContext):
    """Отмена процесса выхода из группы"""
    await state.clear()
    # Получаем актуальную статистику
    stats = await get_admin_stats()
    
    keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📊 Информация о группах",
                        callback_data="show_groups"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📨 Создать рассылку",
                        callback_data="create_mailing"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📥 Получить в txt",
                        callback_data="export_database"
                    )
                ]
            ]
        )
    
    await callback.message.edit_text(
        f"👑 <b>Админ-панель:</b>\n"
        f"📱 Количество групп: <code>{stats['groups']}</code>\n"
        f"👤 Количество пользователей: <code>{stats['users']}</code>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer("Действие отменено")

@router.callback_query(F.data == "return_to_menu")
async def return_to_menu(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню админа"""
    await state.clear()
    # Получаем актуальную статистику
    stats = await get_admin_stats()
    
    keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📊 Информация о группах",
                        callback_data="show_groups"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📨 Создать рассылку",
                        callback_data="create_mailing"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📥 Получить в txt",
                        callback_data="export_database"
                    )
                ]
            ]
        )
    
    await callback.message.edit_text(
        f"👑 <b>Админ-панель:</b>\n"
        f"📱 Количество групп: <code>{stats['groups']}</code>\n"
        f"👤 Количество пользователей: <code>{stats['users']}</code>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@router.message(LeaveGroupStates.waiting_for_group_id)
async def process_group_id(message: Message, state: FSMContext):
    """Обработка полученного ID группы"""
    if message.from_user.id != ADMIN_USER_ID:
        return

    try:
        chat_id = int(message.text)
        
        try:
            await message.bot.leave_chat(chat_id)
            
            # Обновляем статус в БД и отмечаем, что бот был удален админом
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute('''
                    UPDATE groups 
                    SET is_active = FALSE, left_by_admin = TRUE
                    WHERE chat_id = ?
                ''', (chat_id,))
                await db.commit()
            
            success_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Вернуться в меню", callback_data="return_to_menu")]
            ])
            
            await message.reply(
                f"✅ Бот успешно покинул группу с ID {chat_id}",
                reply_markup=success_keyboard
            )
        
        except Exception as e:
            error_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="start_leave_group")],
                [InlineKeyboardButton(text="◀️ Вернуться в меню", callback_data="return_to_menu")]
            ])
            
            await message.reply(
                f"❌ Ошибка при выходе из группы: {str(e)}",
                reply_markup=error_keyboard
            )
    
    except ValueError:
        retry_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="start_leave_group")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_leave")]
        ])
        
        await message.reply(
            "❌ Пожалуйста, отправьте корректный ID группы (только цифры)",
            reply_markup=retry_keyboard
        )
    
    finally:
        await state.clear()

from characterai import aiocai
import asyncio
from typing import Dict, List
from datetime import datetime, timedelta
import logging
import random
from collections import deque

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        self.connections: List[aiocai.Client] = []
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
                client = aiocai.Client(self.api_key)
                await client.get_me()  # Проверяем подключение
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
                        # Создаем новый чат
                        chat_instance = await client.connect()
                        new, _ = await chat_instance.new_chat(self.char_id, (await client.get_me()).id)
                        self.user_chats[user_id] = {
                            'chat_id': new.chat_id,
                            'last_activity': datetime.now()
                        }
                        logger.info(f"Created new chat for user {user_id}")
                    
                    chat_data = self.user_chats[user_id]
                    
                    # Отправляем сообщение
                    chat_instance = await client.connect()
                    response = await chat_instance.send_message(
                        self.char_id,
                        chat_data['chat_id'],
                        message
                    )
                    
                    chat_data['last_activity'] = datetime.now()
                    return response.text
                    
                except Exception as e:
                    logger.error(f"Error in connection {conn_id} for user {user_id}: {e}")
                    # Удаляем чат при ошибке, чтобы он пересоздался
                    if user_id in self.user_chats:
                        del self.user_chats[user_id]
                    raise
                    
        except Exception as e:
            logger.error(f"Failed to send message for user {user_id}: {e}")
            raise

    async def close(self):
        for client in self.pool.connections:
            await client.close()

# Глобальный экземпляр менеджера чатов
chat_manager = ChatManager(
    api_key='c5f381d0f06ab1719536b913535f968c7936343c',
    char_id='YRYumWKfFP7GvCLMssT5k8SeUD--qbQOPTs2j6TBKmw',
    pool_size=5,
    min_delay=3.0,        # Минимальная задержка в секундах
    max_delay=4.0,        # Максимальная задержка в секундах
    message_limit=3,      # Максимальное количество сообщений
    time_window=8         # Временное окно в секундах
)

@router.message(Command("help"))
async def help_command_handler(message: types.Message):
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
    
    # Создаем FSInputFile для локального файла
    animation = FSInputFile("/root/MimiFurry/VID_20250328_023540_803.mp4")
    
    await message.answer_animation(
        animation=animation,
        caption=(
            "🩷 Привет, дорогой! Меня зовут <b>Мими 🍼</b>, и я очень рада быть "
            "добавленной в твою замечательную чат-группу.\n\n"
            "<a href='https://t.me/MimiTyph_sBot?startgroup=true'>Нажми на эту чертову кн-</a>\n\n"
            "Я могу отвечать на ваши сообщение таким образом, который <b>все будут любить!</b> 💕 \n\n"
            "<i>PS: Вы автоматически подтвежаете с <a href='https://telegra.ph/Politika-Konfidencialnosti-i-Usloviya-Ispolzovaniya-03-27'>Условиями использования</a></i>\n\n"
            "📌 <b>Команды бота:</b>\n"
            "/help - показать это сообщение\n"
            "/stats - показать статистики группы"
        ), 
        parse_mode='HTML', 
        reply_markup=keyboard
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

@router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def group_message_handler(message: types.Message):
    if not message.text:
        return

    try:
        await ensure_group_exists(message.chat.id, message.chat.title)
        await save_message_history(message.chat.id, message.from_user.id, message.text)
        await save_words(message.chat.id, message.text)

        stats = await get_group_stats(message.chat.id)
        
        bot_names = {'мими', 'mimi', 'МИМИ', 'MIMI', 'Мими', 'Mimi'}
        message_words = set(message.text.lower().split())
        should_respond = any(name in message_words for name in bot_names)
        is_reply_to_bot = message.reply_to_message and message.reply_to_message.from_user.id == message.bot.id
        
        # Добавляем случайный ответ с шансом 1%
        random_response_chance = random.random() < 0.01 # 1% chance
        
        if should_respond or is_reply_to_bot or random_response_chance:
            if stats['messages'] < REQUIRED_MESSAGES:
                await message.reply(
                    f"♡ Для активации Mimi Typh нужно {REQUIRED_MESSAGES} сообщений. "
                    f"Текущий прогресс: {stats['messages']}/{REQUIRED_MESSAGES} ‹𝟹",
                    parse_mode='HTML'
                )
                return

            typing_task = None
            try:
                # Реалистичные параметры
                INITIAL_DELAY = random.uniform(0.3, 1.5)  # Человеческая задержка перед ответом
                MIN_TYPING_TIME = 1.5  # Минимальное время показа эффекта
                BASE_CHAR_DELAY = 0.05  # Базовая задержка на символ
                TYPING_VARIABILITY = 0.03  # Разброс скорости печати
                THINKING_PAUSE_CHANCE = 0.15  # Вероятность паузы "обдумывания"
                THINKING_PAUSE_DURATION = (0.8, 2.0)  # Длительность паузы
                
                # Имитация человеческой задержки перед ответом
                await asyncio.sleep(INITIAL_DELAY)

                async def realistic_typing():
                    try:
                        last_action_time = time.time()
                        while True:
                            # Случайная вариация интервала между действиями
                            current_time = time.time()
                            elapsed = current_time - last_action_time
                            
                            # Иногда имитируем паузу в "обдумывании"
                            if random.random() < THINKING_PAUSE_CHANCE:
                                pause = random.uniform(*THINKING_PAUSE_DURATION)
                                await asyncio.sleep(pause)
                                last_action_time = time.time()
                                continue
                            
                            # Отправляем действие с переменной задержкой
                            await message.bot.send_chat_action(message.chat.id, "typing")
                            
                            # Случайная задержка до следующего действия
                            delay = random.uniform(2.5, 4.5)
                            await asyncio.sleep(delay)
                            last_action_time = time.time()
                            
                    except asyncio.CancelledError:
                        pass

                # Запускаем эффект печати
                typing_task = asyncio.create_task(realistic_typing())
                
                # Фиксируем время начала
                start_time = time.time()
                
                # Генерация ответа
                response = await chat_manager.send_message(
                    user_id=message.from_user.id,
                    message=message.text
                )
                
                # Рассчитываем реалистичное время печати с учетом:
                # - базовой скорости
                # - случайных колебаний
                # - возможных пауз
                if response:
                    base_typing_time = len(response) * (BASE_CHAR_DELAY + random.uniform(-TYPING_VARIABILITY, TYPING_VARIABILITY))
                    
                    # Добавляем возможные паузы "обдумывания"
                    if len(response.split()) > 5:
                        base_typing_time += random.uniform(0.5, 1.5) * (len(response.split()) // 8)
                    
                    typing_duration = max(MIN_TYPING_TIME, base_typing_time)
                else:
                    typing_duration = MIN_TYPING_TIME
                
                # Добавляем дополнительную задержку если ответ пришел слишком быстро
                elapsed = time.time() - start_time
                if elapsed < typing_duration:
                    remaining_delay = typing_duration - elapsed
                    
                    # Разбиваем задержку на части с возможными паузами
                    while remaining_delay > 0:
                        chunk = min(remaining_delay, random.uniform(0.7, 1.8))
                        await asyncio.sleep(chunk)
                        remaining_delay -= chunk
                        
                        # С шансом добавить дополнительную паузу
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
                    
                    # Анализ настроения с имитацией работы
                    async with aiosqlite.connect(DB_NAME) as db:
                        # Имитируем задержку анализа
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

                    # Отправка с небольшой случайной задержкой
                    await asyncio.sleep(random.uniform(0.05, 0.3))
                    await message.reply(response)

                # Медиа с соответствующей индикацией
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
                
                # Финализация эффекта - небольшая задержка перед завершением
                await asyncio.sleep(0.1)
                await message.bot.send_chat_action(message.chat.id, "cancel")

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        await message.answer("⚠️ Произошла неожиданная ошибка. Администратор уже уведомлен.")

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
    animation = FSInputFile("/root/MimiFurry/VID_20250328_023540_803.mp4")
    
    await message.answer_animation(
        animation=animation,
        caption=(
            "🩷 Привет, дорогой! Меня зовут <b>Мими 🍼</b>, и я очень рада быть "
            "добавленной в твою замечательную чат-группу.\n\n"
            "<a href='https://t.me/MimiTyph_sBot?startgroup=true'>Нажми на эту чертову кн-</a>\n\n"
            "Я могу отвечать на ваши сообщение таким образом, который <b>все будут любить!</b> 💕 \n\n"
            "<i>PS: Вы автоматически подтвежаете с <a href='https://telegra.ph/Politika-Konfidencialnosti-i-Usloviya-Ispolzovaniya-03-27'>Условиями использования</a></i>"
        ), parse_mode='HTML', reply_markup=keyboard
    )
    
async def main():
    await init_db()
    bot = Bot(token="8148475045:AAFq-CpuOLoidWwMZzFNCvbenB3mmy6fnKg")
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    
    try:
        await dp.start_polling(bot, 
            drop_pending_updates=True,  # Сбрасываем все pending обновления
            timeout=30)
    finally:
        await bot.session.close()
        await chat_manager.close()

if __name__ == "__main__":
    asyncio.run(main())