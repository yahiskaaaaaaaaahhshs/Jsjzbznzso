import re
import json
import time
import asyncio
import os
import aiohttp
import csv
import io
import random
import weakref
from collections import defaultdict, deque
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import sqlite3
from asyncio import Semaphore, Queue, TimeoutError
import threading

# ==================== CONFIG ====================
TOKEN = "8735764992:AAEMKvlWXyruQj49KLz4IJHIDVU6ophL644"
BOT_USERNAME = 'newpayubot'
MESSAGE_PREFIX = '/st'

API_ID = 29612794
API_HASH = '6edc1a58a202c9f6e62dc98466932bad'
OWNER_ID = 8129488947
SESSION_DIR = "sessions"
TIMEOUT_SECONDS = 8  # Reduced from 15
MAX_SINGLE_LIMIT = 100
MAX_MASS_LIMIT = 10
MAX_MASS_CHECKS_PER_HOUR = 100
SINGLE_WINDOW_HOURS = 1
MASS_WINDOW_HOURS = 1
MAX_CONCURRENT_CHECKS = 5  # Increased from 2
SINGLE_CHECK_DELAY = 1  # Reduced from 2
MASS_COOLDOWN = 10  # Reduced from 15
CACHE_TTL = 300  # Cache results for 5 minutes

if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# Database with connection pooling
class DatabasePool:
    def __init__(self, db_path, pool_size=10):
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool = Queue(maxsize=pool_size)
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        for _ in range(self.pool_size):
            conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=10000')
            conn.execute('PRAGMA temp_store=MEMORY')
            await self._pool.put(conn)
    
    async def get_connection(self):
        return await self._pool.get()
    
    async def return_connection(self, conn):
        await self._pool.put(conn)

db_pool = DatabasePool('bot_data.db', pool_size=10)

# Global variables
sessions_queue = deque()
session_clients = {}
session_locks = {}
user_cooldown = {}
mass_checking_active = {}
user_mass_cooldown = {}
card_attempts = defaultdict(lambda: deque(maxlen=10))
result_cache = {}
cache_lock = asyncio.Lock()
stats_lock = asyncio.Lock()

# ==================== FAST DB OPERATIONS ====================
async def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    conn = await db_pool.get_connection()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        if fetch_one:
            result = cursor.fetchone()
        elif fetch_all:
            result = cursor.fetchall()
        else:
            result = None
            conn.commit()
        
        return result
    finally:
        await db_pool.return_connection(conn)

async def init_db():
    conn = await db_pool.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS sessions
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_name TEXT UNIQUE,
              phone_number TEXT,
              created_at TIMESTAMP,
              is_active BOOLEAN DEFAULT 1)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS banned_users
             (user_id INTEGER PRIMARY KEY,
              banned_until TIMESTAMP,
              reason TEXT)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS user_stats
             (user_id INTEGER PRIMARY KEY,
              first_name TEXT,
              username TEXT,
              total_checks INTEGER DEFAULT 0,
              single_checks_today INTEGER DEFAULT 0,
              mass_checks_today INTEGER DEFAULT 0,
              last_single_check TIMESTAMP,
              last_mass_check TIMESTAMP,
              single_last_reset TIMESTAMP,
              mass_last_reset TIMESTAMP,
              join_date TIMESTAMP,
              custom_limit INTEGER DEFAULT 0)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS bot_config
             (key TEXT PRIMARY KEY,
              value TEXT)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS admin_config
             (key TEXT PRIMARY KEY,
              value TEXT)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS approved_cards
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              card_data TEXT,
              status TEXT,
              response TEXT,
              checked_at TIMESTAMP,
              checked_by INTEGER)''')
        
        cursor.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('check_delay', '1')")
        cursor.execute("INSERT OR IGNORE INTO admin_config (key, value) VALUES ('approved_channel', '')")
        conn.commit()
    finally:
        await db_pool.return_connection(conn)

# ==================== FAST HELPER FUNCTIONS ====================
def is_owner(user_id):
    return user_id == OWNER_ID

async def is_banned_fast(user_id):
    result = await execute_query(
        "SELECT banned_until, reason FROM banned_users WHERE user_id = ? AND banned_until > ?",
        (user_id, datetime.now().isoformat()),
        fetch_one=True
    )
    if result:
        return True, datetime.fromisoformat(result[0]), result[1]
    return False, None, None

async def register_user_fast(user):
    result = await execute_query(
        "SELECT user_id FROM user_stats WHERE user_id = ?",
        (user.id,),
        fetch_one=True
    )
    if not result:
        await execute_query(
            "INSERT INTO user_stats (user_id, first_name, username, total_checks, join_date, single_last_reset, mass_last_reset) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user.id, user.first_name, user.username, 0, datetime.now().isoformat(), datetime.now().isoformat(), datetime.now().isoformat())
        )

async def get_user_limit_fast(user_id):
    if is_owner(user_id):
        return 999999
    result = await execute_query(
        "SELECT custom_limit FROM user_stats WHERE user_id = ?",
        (user_id,),
        fetch_one=True
    )
    if result and result[0] > 0:
        return result[0]
    return MAX_SINGLE_LIMIT

async def update_single_stats_fast(user):
    now = datetime.now()
    result = await execute_query(
        "SELECT single_last_reset, single_checks_today FROM user_stats WHERE user_id = ?",
        (user.id,),
        fetch_one=True
    )
    
    if result and result[0]:
        last_reset = datetime.fromisoformat(result[0])
        if now - last_reset > timedelta(hours=SINGLE_WINDOW_HOURS):
            single_checks_today = 1
        else:
            single_checks_today = (result[1] or 0) + 1
    else:
        single_checks_today = 1
    
    await execute_query(
        "UPDATE user_stats SET total_checks = total_checks + 1, single_checks_today = ?, single_last_reset = ?, first_name = ?, username = ? WHERE user_id = ?",
        (single_checks_today, now.isoformat(), user.first_name, user.username, user.id)
    )
    return single_checks_today

async def update_mass_stats_fast(user, count):
    now = datetime.now()
    result = await execute_query(
        "SELECT mass_last_reset, mass_checks_today FROM user_stats WHERE user_id = ?",
        (user.id,),
        fetch_one=True
    )
    
    if result and result[0]:
        last_reset = datetime.fromisoformat(result[0])
        if now - last_reset > timedelta(hours=MASS_WINDOW_HOURS):
            mass_checks_today = count
        else:
            mass_checks_today = (result[1] or 0) + count
    else:
        mass_checks_today = count
    
    await execute_query(
        "UPDATE user_stats SET total_checks = total_checks + ?, mass_checks_today = ?, mass_last_reset = ? WHERE user_id = ?",
        (count, mass_checks_today, now.isoformat(), user.id)
    )
    return mass_checks_today

async def get_single_check_count_fast(user_id):
    result = await execute_query(
        "SELECT single_checks_today FROM user_stats WHERE user_id = ?",
        (user_id,),
        fetch_one=True
    )
    return result[0] if result else 0

async def get_mass_check_count_fast(user_id):
    result = await execute_query(
        "SELECT mass_checks_today FROM user_stats WHERE user_id = ?",
        (user_id,),
        fetch_one=True
    )
    return result[0] if result else 0

async def get_delay_fast():
    result = await execute_query(
        "SELECT value FROM bot_config WHERE key = 'check_delay'",
        fetch_one=True
    )
    return int(result[0]) if result else 1

async def get_approved_channel_fast():
    result = await execute_query(
        "SELECT value FROM admin_config WHERE key = 'approved_channel'",
        fetch_one=True
    )
    return result[0] if result else ""

async def save_approved_card_fast(card_data, status, response, user_id):
    await execute_query(
        "INSERT INTO approved_cards (card_data, status, response, checked_at, checked_by) VALUES (?, ?, ?, ?, ?)",
        (card_data, status, response, datetime.now().isoformat(), user_id)
    )

def auto_ban_card_fast(card_hash, user_id):
    now = time.time()
    card_attempts[card_hash].append((now, user_id))
    
    # Clean old attempts
    while card_attempts[card_hash] and now - card_attempts[card_hash][0][0] > 120:
        card_attempts[card_hash].popleft()
    
    user_attempts = [uid for _, uid in card_attempts[card_hash] if uid == user_id]
    
    if len(user_attempts) >= 5:
        return True
    return False

# ==================== FAST SESSION MANAGEMENT ====================
async def load_sessions_fast():
    global sessions_queue
    result = await execute_query(
        "SELECT session_name FROM sessions WHERE is_active = 1 ORDER BY id",
        fetch_all=True
    )
    sessions_queue = deque([row[0] for row in result])
    
    # Initialize session clients
    for session_name in sessions_queue:
        if session_name not in session_clients:
            session_path = os.path.join(SESSION_DIR, session_name)
            client = TelegramClient(session_path, API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                session_clients[session_name] = client
                session_locks[session_name] = asyncio.Lock()
    
    return len(sessions_queue)

def get_next_session_fast():
    if sessions_queue:
        session = sessions_queue[0]
        sessions_queue.rotate(-1)
        return session
    return None

# ==================== FAST CARD CHECKING ====================
async def send_card_via_session_fast(card_data, session_name):
    """ULTRA FAST card checking with persistent session"""
    
    # Check cache first
    cache_key = f"{card_data}"
    async with cache_lock:
        if cache_key in result_cache:
            cached_time, cached_result = result_cache[cache_key]
            if time.time() - cached_time < CACHE_TTL:
                return cached_result
    
    client = session_clients.get(session_name)
    if not client:
        return {"error": "Session not connected", "status": "Error"}
    
    try:
        async with session_locks[session_name]:
            entity = await client.get_entity(f'@{BOT_USERNAME}')
            message = f"{MESSAGE_PREFIX} {card_data}"
            await client.send_message(entity, message)
            
            start_time = time.time()
            
            while time.time() - start_time < TIMEOUT_SECONDS:
                await asyncio.sleep(0.5)  # Faster polling
                
                history = await client(GetHistoryRequest(
                    peer=entity,
                    limit=1,
                    offset_date=None,
                    offset_id=0,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0
                ))
                
                if history.messages:
                    msg = history.messages[0]
                    text = msg.message
                    
                    if 'CC:' in text or 'Status:' in text:
                        # Fast parse
                        status_match = re.search(r'Status:\s*(.*?)(?:\n|$)', text)
                        response_match = re.search(r'Response:\s*(.*?)(?:\n|$)', text)
                        bank_match = re.search(r'Bank:\s*(.*?)(?:\n|$)', text)
                        type_match = re.search(r'Type:\s*(.*?)(?:\n|$)', text)
                        country_match = re.search(r'Country:\s*(.*?)(?:\n|$)', text)
                        
                        result = {
                            "cc": card_data,
                            "status": status_match.group(1).strip() if status_match else "",
                            "response": response_match.group(1).strip() if response_match else "",
                            "gateway": "Stripe",
                            "bank": bank_match.group(1).strip() if bank_match else "",
                            "type": type_match.group(1).strip() if type_match else "",
                            "country": country_match.group(1).strip() if country_match else ""
                        }
                        
                        # Cache result
                        async with cache_lock:
                            result_cache[cache_key] = (time.time(), result)
                        
                        return result
            
            return {"error": "timeout", "status": "Error", "response": "No response"}
            
    except Exception as e:
        return {"error": str(e), "status": "Error", "response": "Connection error"}

def validate_card_fast(card_input):
    """ULTRA FAST validation"""
    card_input = card_input.strip().replace(' ', '')
    
    # Check format cc|mm|yy|cvv
    match = re.match(r'^(\d{15,16})\|(\d{1,2})\|(\d{2,4})\|(\d{3,4})$', card_input)
    if match:
        card_num = match.group(1)
        month = match.group(2).zfill(2)
        year = match.group(3)
        cvv = match.group(4)
        
        if len(year) == 4:
            year = year[-2:]
        year = year.zfill(2)
        
        if 1 <= int(month) <= 12:
            return True, f"{card_num}|{month}|{year}|{cvv}"
    
    # Check continuous format
    match = re.match(r'^(\d{15,16})(\d{2})(\d{2,4})(\d{3,4})$', card_input)
    if match:
        card_num = match.group(1)
        month = match.group(2).zfill(2)
        year = match.group(3)
        cvv = match.group(4)
        
        if len(year) == 4:
            year = year[-2:]
        year = year.zfill(2)
        
        if 1 <= int(month) <= 12:
            return True, f"{card_num}|{month}|{year}|{cvv}"
    
    return False, None

def parse_cards_fast(text):
    """ULTRA FAST card parsing"""
    cards = []
    lines = text.strip().split('\n')
    
    for line in lines[:MAX_MASS_LIMIT]:
        line = line.strip()
        if not line:
            continue
        
        match = re.search(r'(\d{15,16})\s*[|:]\s*(\d{1,2})\s*[|:]\s*(\d{2,4})\s*[|:]\s*(\d{3,4})', line)
        if match:
            card_num = match.group(1)
            month = match.group(2).zfill(2)
            year = match.group(3)
            cvv = match.group(4)
            
            if len(year) == 4:
                year = year[-2:]
            year = year.zfill(2)
            
            if 1 <= int(month) <= 12:
                cards.append(f"{card_num}|{month}|{year}|{cvv}")
                if len(cards) >= MAX_MASS_LIMIT:
                    break
    
    return cards

# ==================== BIN LOOKUP ====================
bin_cache = {}
async def get_bin_info_fast(bin_num):
    if bin_num in bin_cache:
        cached_time, cached_data = bin_cache[bin_num]
        if time.time() - cached_time < 86400:  # Cache for 24 hours
            return cached_data
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://lookup.binlist.net/{bin_num}"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    result = {
                        "bin": bin_num,
                        "bank": data.get('bank', {}).get('name', 'Unknown'),
                        "brand": data.get('scheme', 'Unknown').upper(),
                        "category": 'Prepaid' if data.get('prepaid', False) else 'Standard',
                        "type": data.get('type', 'Unknown').capitalize(),
                        "country": data.get('country', {}).get('name', 'Unknown')
                    }
                    bin_cache[bin_num] = (time.time(), result)
                    return result
                else:
                    return {"error": "BIN not found"}
    except Exception as e:
        return {"error": f"API error: {str(e)}"}

# ==================== COMMAND HANDLERS ====================
async def start(update: Update, context):
    user = update.effective_user
    await register_user_fast(user)
    
    keyboard = [[InlineKeyboardButton("Support", url="https://t.me/SoenxSupportBot")]]
    await update.message.reply_text(
        f"<b>Welcome {user.first_name} to SoenxBot - ⚡</b>\n\n/cmds to see available commands.\n/profile to Get Your Info.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def profile(update: Update, context):
    user = update.effective_user
    username = user.username if user.username else 'No username'
    single_checks = await get_single_check_count_fast(user.id)
    mass_checks = await get_mass_check_count_fast(user.id)
    user_limit = await get_user_limit_fast(user.id)
    
    keyboard = [[InlineKeyboardButton("Back", callback_data="back_to_cmds")]]
    
    await update.message.reply_text(
        f"<b>User ID</b> • <code>{user.id}</code>\n"
        f"<b>Name</b> • {user.full_name}\n"
        f"<b>Username</b> • @{username}\n"
        f"<b>First Name</b> • {user.first_name}\n"
        f"<b>Single Checks Today</b> • {single_checks}/{user_limit}\n"
        f"<b>Mass Checks Today</b> • {mass_checks}/{MAX_MASS_CHECKS_PER_HOUR}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def cmds(update: Update, context):
    keyboard = [
        [InlineKeyboardButton("Profile", callback_data="profile")],
        [InlineKeyboardButton("Rules", callback_data="rules")]
    ]
    await update.message.reply_text(
        "<b>Auth</b>\n• /chk - Stripe Auth\n\n<b>Charge</b>\n• /ss - Stripe+Square $0.001(trail)\n\n<b>Mass</b>\n• /mchk - Stripe Auth Mass\n\n<b>Other</b>\n• /bin - BIN/IIN Check\n• /rules - Check Bot Rules\n• /info - Your Profile",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def rules(update: Update, context):
    keyboard = [[InlineKeyboardButton("Back", callback_data="back_to_cmds")]]
    
    await update.message.reply_text(
        "<b>Rules:</b>\n\n<b>1.</b> Attempting more than 5 transactions with the same card within a minute will lead to a ban.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def info(update: Update, context):
    await profile(update, context)

# ==================== ULTRA FAST CHECK HANDLER ====================
async def handle_chk_command(update: Update, context):
    user = update.effective_user
    await register_user_fast(user)
    
    # Get card input
    card_input = None
    if update.message.reply_to_message:
        card_input = update.message.reply_to_message.text
    elif context.args:
        card_input = ' '.join(context.args)
    else:
        await update.message.reply_text(
            "<b>Usage:</b>\n<code>/chk cc|mm|yyyy|cvv</code>\n\n<b>Example:</b>\n<code>/chk 4663490011245506|02|2027|453</code>",
            parse_mode="HTML"
        )
        return
    
    # Validate card
    is_valid, parsed_card = validate_card_fast(card_input)
    if not is_valid:
        await update.message.reply_text(
            "<b>Usage:</b>\n<code>/chk cc|mm|yyyy|cvv</code>\n\n<b>Example:</b>\n<code>/chk 4663490011245506|02|2027|453</code>",
            parse_mode="HTML"
        )
        return
    
    # Check ban
    banned, until, reason = await is_banned_fast(user.id)
    if banned:
        await update.message.reply_text(f"⛔ Banned until: {until.strftime('%Y-%m-%d %H:%M:%S')}\nReason: {reason}")
        return
    
    # Check limit
    single_checks = await get_single_check_count_fast(user.id)
    user_limit = await get_user_limit_fast(user.id)
    if single_checks >= user_limit and not is_owner(user.id):
        await update.message.reply_text("⚠️ Limit reached (100 checks/hour).")
        return
    
    # Cooldown
    current_time = time.time()
    delay = await get_delay_fast()
    if user.id in user_cooldown:
        elapsed = current_time - user_cooldown[user.id]
        if elapsed < delay:
            await update.message.reply_text(f"⏳ Wait {int(delay - elapsed)} seconds.")
            return
    
    user_cooldown[user.id] = current_time
    
    # Auto-ban check
    card_hash = parsed_card.split('|')[0]
    if auto_ban_card_fast(card_hash, user.id):
        await update.message.reply_text("⛔ Banned for 5 minutes - too many attempts with same card.")
        return
    
    # Send processing message
    msg = await update.message.reply_text("🔄 Processing...")
    
    # Get session
    session_name = get_next_session_fast()
    if not session_name:
        await msg.edit_text("❌ No active session.")
        return
    
    # Check card
    result = await send_card_via_session_fast(parsed_card, session_name)
    
    # Format response
    if "error" in result:
        formatted = (
            f"<b>☇ CC:</b> {parsed_card}\n"
            f"<b>ヾ⌿ Status:</b> Error\n"
            f"<b>ヾ⌿ Response:</b> {result.get('error', 'Unknown')}\n"
            f"<b>· · · · · · · · · · · · · · ·</b>\n"
            f"<b>ヾ⌿ Checked By:</b> <a href='tg://user?id={user.id}'>{user.first_name}</a>"
        )
    else:
        formatted = (
            f"<b>☇ CC:</b> {result.get('cc', parsed_card)}\n"
            f"<b>ヾ⌿ Status:</b> {result.get('status', 'N/A')}\n"
            f"<b>ヾ⌿ Response:</b> {result.get('response', 'N/A')}\n"
            f"<b>· · · · · · · · · · · · · · ·</b>\n"
            f"<b>ヾ⌿ Bank:</b> {result.get('bank', 'N/A')}\n"
            f"<b>ヾ⌿ Type:</b> {result.get('type', 'N/A')}\n"
            f"<b>ヾ⌿ Country:</b> {result.get('country', 'N/A')}\n"
            f"<b>· · · · · · · · · · · · · · ·</b>\n"
            f"<b>ヾ⌿ Gate:</b> Stripe\n"
            f"<b>ヾ⌿ Checked By:</b> <a href='tg://user?id={user.id}'>{user.first_name}</a>"
        )
        
        # Save approved cards
        if 'approved' in result.get('status', '').lower():
            await save_approved_card_fast(parsed_card, result.get('status', ''), result.get('response', ''), user.id)
            channel = await get_approved_channel_fast()
            if channel:
                try:
                    await context.bot.send_message(channel, formatted, parse_mode="HTML")
                except:
                    pass
    
    await msg.edit_text(formatted, parse_mode="HTML")
    await update_single_stats_fast(user)

async def handle_ss_command(update: Update, context):
    """Same as chk but with different gateway display"""
    user = update.effective_user
    await register_user_fast(user)
    
    card_input = None
    if update.message.reply_to_message:
        card_input = update.message.reply_to_message.text
    elif context.args:
        card_input = ' '.join(context.args)
    else:
        await update.message.reply_text(
            "<b>Usage:</b>\n<code>/ss cc|mm|yyyy|cvv</code>\n\n<b>Example:</b>\n<code>/ss 4663490011245506|02|2027|453</code>",
            parse_mode="HTML"
        )
        return
    
    is_valid, parsed_card = validate_card_fast(card_input)
    if not is_valid:
        await update.message.reply_text(
            "<b>Usage:</b>\n<code>/ss cc|mm|yyyy|cvv</code>\n\n<b>Example:</b>\n<code>/ss 4663490011245506|02|2027|453</code>",
            parse_mode="HTML"
        )
        return
    
    banned, until, reason = await is_banned_fast(user.id)
    if banned:
        await update.message.reply_text(f"⛔ Banned until: {until.strftime('%Y-%m-%d %H:%M:%S')}")
        return
    
    single_checks = await get_single_check_count_fast(user.id)
    user_limit = await get_user_limit_fast(user.id)
    if single_checks >= user_limit and not is_owner(user.id):
        await update.message.reply_text("⚠️ Limit reached.")
        return
    
    current_time = time.time()
    delay = await get_delay_fast()
    if user.id in user_cooldown:
        elapsed = current_time - user_cooldown[user.id]
        if elapsed < delay:
            await update.message.reply_text(f"⏳ Wait {int(delay - elapsed)} seconds.")
            return
    
    user_cooldown[user.id] = current_time
    
    card_hash = parsed_card.split('|')[0]
    if auto_ban_card_fast(card_hash, user.id):
        await update.message.reply_text("⛔ Banned for 5 minutes.")
        return
    
    msg = await update.message.reply_text("🔄 Processing...")
    
    session_name = get_next_session_fast()
    if not session_name:
        await msg.edit_text("❌ No active session.")
        return
    
    result = await send_card_via_session_fast(parsed_card, session_name)
    
    if "error" in result:
        formatted = (
            f"<b>☇ CC:</b> {parsed_card}\n"
            f"<b>ヾ⌿ Status:</b> Error\n"
            f"<b>ヾ⌿ Response:</b> {result.get('error', 'Unknown')}\n"
            f"<b>· · · · · · · · · · · · · · ·</b>\n"
            f"<b>ヾ⌿ Checked By:</b> <a href='tg://user?id={user.id}'>{user.first_name}</a>"
        )
    else:
        formatted = (
            f"<b>☇ CC:</b> {result.get('cc', parsed_card)}\n"
            f"<b>ヾ⌿ Status:</b> {result.get('status', 'N/A')}\n"
            f"<b>ヾ⌿ Response:</b> {result.get('response', 'N/A')}\n"
            f"<b>· · · · · · · · · · · · · · ·</b>\n"
            f"<b>ヾ⌿ Bank:</b> {result.get('bank', 'N/A')}\n"
            f"<b>ヾ⌿ Type:</b> {result.get('type', 'N/A')}\n"
            f"<b>ヾ⌿ Country:</b> {result.get('country', 'N/A')}\n"
            f"<b>· · · · · · · · · · · · · · ·</b>\n"
            f"<b>ヾ⌿ Gate:</b> Stripe+Square $0.001(trail)\n"
            f"<b>ヾ⌿ Checked By:</b> <a href='tg://user?id={user.id}'>{user.first_name}</a>"
        )
        
        if 'approved' in result.get('status', '').lower():
            await save_approved_card_fast(parsed_card, result.get('status', ''), result.get('response', ''), user.id)
    
    await msg.edit_text(formatted, parse_mode="HTML")
    await update_single_stats_fast(user)

async def handle_mchk_command(update: Update, context):
    """ULTRA FAST mass check"""
    user = update.effective_user
    await register_user_fast(user)
    
    if user.id in mass_checking_active and mass_checking_active[user.id]:
        await update.message.reply_text("⏳ Active mass check. Please wait.")
        return
    
    if user.id in user_mass_cooldown:
        elapsed = time.time() - user_mass_cooldown[user.id]
        if elapsed < MASS_COOLDOWN:
            await update.message.reply_text(f"⏳ Wait {int(MASS_COOLDOWN - elapsed)} seconds.")
            return
    
    # Get cards
    card_text = None
    if update.message.reply_to_message:
        if update.message.reply_to_message.document:
            file = await context.bot.get_file(update.message.reply_to_message.document.file_id)
            file_content = await file.download_as_bytearray()
            card_text = file_content.decode('utf-8', errors='ignore')
        else:
            card_text = update.message.reply_to_message.text
    elif context.args:
        card_text = ' '.join(context.args)
    else:
        await update.message.reply_text(
            "<b>Usage:</b>\n<code>/mchk</code> (reply to message with cards)\n\n<b>Format:</b>\n<code>cc|mm|yyyy|cvv</code> (one per line)\n<b>Max 10 cards</b>",
            parse_mode="HTML"
        )
        return
    
    cards = parse_cards_fast(card_text)
    if not cards:
        await update.message.reply_text("❌ No valid cards found.")
        return
    
    if len(cards) > MAX_MASS_LIMIT:
        cards = cards[:MAX_MASS_LIMIT]
    
    # Check ban and limits
    banned, until, reason = await is_banned_fast(user.id)
    if banned:
        await update.message.reply_text(f"⛔ Banned")
        return
    
    mass_checks_done = await get_mass_check_count_fast(user.id)
    if mass_checks_done + len(cards) > MAX_MASS_CHECKS_PER_HOUR and not is_owner(user.id):
        await update.message.reply_text(f"⚠️ Limit reached. {MAX_MASS_CHECKS_PER_HOUR - mass_checks_done} remaining.")
        return
    
    if not sessions_queue:
        await update.message.reply_text("❌ No active sessions.")
        return
    
    mass_checking_active[user.id] = True
    status_msg = await update.message.reply_text(f"🔄 Mass checking {len(cards)} cards...")
    
    results = []
    approved = []
    declined = []
    errors = []
    
    # Process cards in parallel batches
    batch_size = MAX_CONCURRENT_CHECKS
    for i in range(0, len(cards), batch_size):
        batch = cards[i:i+batch_size]
        tasks = []
        
        for card in batch:
            session_name = get_next_session_fast()
            if session_name:
                tasks.append(send_card_via_session_fast(card, session_name))
            else:
                tasks.append(asyncio.sleep(0, result={"error": "No session"}))
        
        batch_results = await asyncio.gather(*tasks)
        
        for card, result in zip(batch, batch_results):
            if "error" in result:
                errors.append(card)
                results.append({"card": card, "status": "Error", "response": result.get('error', 'Unknown')})
            else:
                results.append({
                    "card": card,
                    "status": result.get('status', 'N/A'),
                    "response": result.get('response', 'N/A')
                })
                
                if 'approved' in result.get('status', '').lower():
                    approved.append(card)
                    await save_approved_card_fast(card, result.get('status', ''), result.get('response', ''), user.id)
                elif 'declined' in result.get('status', '').lower():
                    declined.append(card)
                else:
                    errors.append(card)
        
        # Update progress
        await status_msg.edit_text(f"✅ Checked {min(i+batch_size, len(cards))}/{len(cards)}\nApproved: {len(approved)}\nDeclined: {len(declined)}\nError: {len(errors)}")
    
    # Send results
    result_text = f"<b>☇ Mass Check Results</b>\n<b>· · · · · · · · · · · · · · ·</b>\n"
    for idx, res in enumerate(results[:20]):
        result_text += f"\n<b>☇ CC:</b> <code>{res['card']}</code>  {idx+1}\n"
        result_text += f"<b>ヾ⌿ Status:</b> {res.get('status', 'N/A')}\n"
        result_text += f"<b>· · · · · · · · · · · · · · ·</b>\n"
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if approved:
        app_file = io.BytesIO('\n'.join(approved).encode())
        await update.message.reply_document(app_file, filename=f"@Soenx_app_{timestamp}.txt", caption=f"✅ Approved ({len(approved)})")
    if declined:
        dec_file = io.BytesIO('\n'.join(declined).encode())
        await update.message.reply_document(dec_file, filename=f"@Soenx_dec_{timestamp}.txt", caption=f"❌ Declined ({len(declined)})")
    if errors:
        err_file = io.BytesIO('\n'.join(errors).encode())
        await update.message.reply_document(err_file, filename=f"@Soenx_err_{timestamp}.txt", caption=f"⚠️ Error ({len(errors)})")
    
    await status_msg.edit_text(result_text[:4000], parse_mode="HTML")
    await update_mass_stats_fast(user, len(cards))
    user_mass_cooldown[user.id] = time.time()
    await asyncio.sleep(MASS_COOLDOWN)
    mass_checking_active[user.id] = False

async def handle_bin_command(update: Update, context):
    user = update.effective_user
    await register_user_fast(user)
    
    bin_input = None
    if update.message.reply_to_message:
        bin_input = update.message.reply_to_message.text
    elif context.args:
        bin_input = ' '.join(context.args)
    else:
        await update.message.reply_text("<b>Usage:</b>\n<code>/bin 466349</code>", parse_mode="HTML")
        return
    
    banned, until, reason = await is_banned_fast(user.id)
    if banned:
        await update.message.reply_text(f"⛔ Banned")
        return
    
    bin_digits = re.search(r'(\d{6})', bin_input)
    if not bin_digits:
        await update.message.reply_text("❌ Invalid BIN. Provide first 6 digits.")
        return
    
    msg = await update.message.reply_text("🔄 Fetching BIN info...")
    result = await get_bin_info_fast(bin_digits.group(1))
    
    if "error" in result:
        formatted = f"<b>❌ Error:</b> {result['error']}"
    else:
        formatted = (
            f"<b>BIN/IIN:</b> {result['bin']}\n"
            f"<b>Bank:</b> {result['bank']}\n"
            f"<b>Brand:</b> {result['brand']}\n"
            f"<b>Category:</b> {result['category']}\n"
            f"<b>Type:</b> {result['type']}\n"
            f"<b>Country:</b> {result['country']}"
        )
    
    await msg.edit_text(formatted, parse_mode="HTML")

# ==================== SESSION MANAGEMENT ====================
async def add_session(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Owner only.")
        return
    
    await update.message.reply_text(
        "📁 Send .session file\n\n"
        "Sessions stay connected for speed\n"
        "Multiple sessions = load balancing"
    )

async def handle_session_file(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if not update.message.document:
        return
    
    doc = update.message.document
    if not doc.file_name.endswith('.session'):
        await update.message.reply_text("❌ Send .session file only.")
        return
    
    file_name = f"session_{int(time.time())}_{random.randint(1000,9999)}.session"
    file_path = os.path.join(SESSION_DIR, file_name)
    
    try:
        msg = await update.message.reply_text("🔄 Verifying...")
        file = await context.bot.get_file(doc.file_id)
        await file.download_to_drive(file_path)
        
        client = TelegramClient(file_path, API_ID, API_HASH)
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
            await execute_query(
                "INSERT INTO sessions (session_name, phone_number, created_at, is_active) VALUES (?, ?, ?, ?)",
                (file_name, me.phone, datetime.now().isoformat(), 1)
            )
            session_clients[file_name] = client
            session_locks[file_name] = asyncio.Lock()
            sessions_queue.append(file_name)
            await msg.edit_text(f"✅ Session added!\n📱 {me.phone}\n⚡ Total: {len(sessions_queue)}")
        else:
            await msg.edit_text("❌ Invalid session. Not authorized.")
            os.remove(file_path)
            
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        if os.path.exists(file_path):
            os.remove(file_path)

async def sessions_list(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    result = await execute_query("SELECT session_name, phone_number, created_at FROM sessions WHERE is_active = 1", fetch_all=True)
    if not result:
        await update.message.reply_text("📭 No sessions.")
        return
    
    text = "<b>📁 Active Sessions</b>\n\n"
    for name, phone, created in result:
        text += f"📱 {phone}\n📁 {name[:20]}\n📅 {created[:10]}\n\n"
    text += f"⚡ Total: {len(result)} | Load Balancing: ON"
    await update.message.reply_text(text, parse_mode="HTML")

async def remove_session(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /rm <session_name>")
        return
    
    name = context.args[0]
    await execute_query("DELETE FROM sessions WHERE session_name = ?", (name,))
    
    if name in session_clients:
        try:
            await session_clients[name].disconnect()
        except:
            pass
        del session_clients[name]
    
    if name in session_locks:
        del session_locks[name]
    
    if name in sessions_queue:
        sessions_queue.remove(name)
    
    path = os.path.join(SESSION_DIR, name)
    if os.path.exists(path):
        os.remove(path)
    
    await update.message.reply_text(f"✅ Removed: {name}")

# ==================== OWNER COMMANDS ====================
async def ban_user(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id> [reason]")
        return
    
    try:
        user_id = int(context.args[0])
        reason = ' '.join(context.args[1:]) or "No reason"
        banned_until = (datetime.now() + timedelta(days=36500)).isoformat()
        await execute_query(
            "INSERT OR REPLACE INTO banned_users (user_id, banned_until, reason) VALUES (?, ?, ?)",
            (user_id, banned_until, reason)
        )
        await update.message.reply_text(f"✅ Banned {user_id}")
    except:
        await update.message.reply_text("❌ Invalid ID")

async def unban_user(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
        await execute_query("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
        await update.message.reply_text(f"✅ Unbanned {user_id}")
    except:
        await update.message.reply_text("❌ Invalid ID")

async def tban_user(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /tban <user_id> <minutes> [reason]")
        return
    
    try:
        user_id = int(context.args[0])
        minutes = int(context.args[1])
        reason = ' '.join(context.args[2:]) or "No reason"
        banned_until = (datetime.now() + timedelta(minutes=minutes)).isoformat()
        await execute_query(
            "INSERT OR REPLACE INTO banned_users (user_id, banned_until, reason) VALUES (?, ?, ?)",
            (user_id, banned_until, reason)
        )
        await update.message.reply_text(f"✅ Banned {user_id} for {minutes} minutes")
    except:
        await update.message.reply_text("❌ Invalid")

async def reset_user(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /reset <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
        now = datetime.now().isoformat()
        await execute_query(
            "UPDATE user_stats SET single_checks_today = 0, mass_checks_today = 0, single_last_reset = ?, mass_last_reset = ? WHERE user_id = ?",
            (now, now, user_id)
        )
        await update.message.reply_text(f"✅ Reset {user_id}")
    except:
        await update.message.reply_text("❌ Invalid")

async def increase_limit(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /increase <user_id> <limit>")
        return
    
    try:
        user_id = int(context.args[0])
        limit = int(context.args[1])
        await execute_query(
            "UPDATE user_stats SET custom_limit = ? WHERE user_id = ?",
            (limit, user_id)
        )
        await update.message.reply_text(f"✅ User {user_id} limit = {limit}")
    except:
        await update.message.reply_text("❌ Invalid")

async def set_delay_cmd(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if not context.args:
        current = await get_delay_fast()
        await update.message.reply_text(f"⏱️ Current delay: {current}s")
        return
    
    try:
        seconds = int(context.args[0])
        await execute_query("UPDATE bot_config SET value = ? WHERE key = 'check_delay'", (str(seconds),))
        await update.message.reply_text(f"✅ Delay = {seconds}s")
    except:
        await update.message.reply_text("❌ Invalid")

async def set_channel(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if not context.args:
        current = await get_approved_channel_fast()
        await update.message.reply_text(f"📢 Current channel: {current or 'Not set'}")
        return
    
    channel = context.args[0]
    await execute_query(
        "INSERT OR REPLACE INTO admin_config (key, value) VALUES ('approved_channel', ?)",
        (channel,)
    )
    await update.message.reply_text(f"✅ Approved cards → {channel}")

async def broadcast(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    if not context.args and not update.message.reply_to_message:
        await update.message.reply_text("Usage: /broadcast <msg>")
        return
    
    msg = update.message.reply_to_message.text if update.message.reply_to_message else ' '.join(context.args)
    if not msg:
        return
    
    users = await execute_query("SELECT DISTINCT user_id FROM user_stats", fetch_all=True)
    success = 0
    status = await update.message.reply_text(f"📢 Sending to {len(users)} users...")
    
    for user in users:
        try:
            await context.bot.send_message(user[0], f"📢 Broadcast:\n\n{msg}")
            success += 1
        except:
            pass
        await asyncio.sleep(0.02)
    
    await status.edit_text(f"✅ Sent to {success}/{len(users)} users")

async def stats_cmd(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    total_users = await execute_query("SELECT COUNT(*) FROM user_stats", fetch_one=True)
    total_checks = await execute_query("SELECT SUM(total_checks) FROM user_stats", fetch_one=True)
    
    text = (
        f"<b>📊 Bot Stats</b>\n\n"
        f"👥 Users: {total_users[0] or 0}\n"
        f"🔍 Checks: {total_checks[0] or 0}\n"
        f"📁 Sessions: {len(sessions_queue)}\n"
        f"⚡ Concurrent: {MAX_CONCURRENT_CHECKS}\n"
        f"🟢 Status: Online"
    )
    await update.message.reply_text(text, parse_mode="HTML")

async def userdata(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    users = await execute_query(
        "SELECT user_id, first_name, username, total_checks, single_checks_today, mass_checks_today, join_date FROM user_stats",
        fetch_all=True
    )
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['User ID', 'Name', 'Username', 'Total Checks', 'Single Today', 'Mass Today', 'Join Date'])
    writer.writerows(users)
    output.seek(0)
    
    await update.message.reply_document(
        io.BytesIO(output.getvalue().encode()),
        filename=f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

async def hitsfile(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    cards = await execute_query(
        "SELECT card_data, status, response, checked_at FROM approved_cards ORDER BY checked_at DESC LIMIT 1000",
        fetch_all=True
    )
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Card', 'Status', 'Response', 'Time'])
    writer.writerows(cards)
    output.seek(0)
    
    await update.message.reply_document(
        io.BytesIO(output.getvalue().encode()),
        filename=f"approved_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

async def owner_menu(update: Update, context):
    if not is_owner(update.effective_user.id):
        return
    
    text = (
        "<b>👑 Owner Commands</b>\n\n"
        "<b>📁 Sessions</b>\n"
        "/add - Add session\n"
        "/sessions - List sessions\n"
        "/rm &lt;name&gt; - Remove\n\n"
        "<b>⚙️ Settings</b>\n"
        "/delay &lt;sec&gt; - Set delay\n"
        "/reset &lt;id&gt; - Reset user\n"
        "/increase &lt;id&gt; &lt;limit&gt; - Set limit\n"
        "/setchannel &lt;channel&gt; - Approve channel\n\n"
        "<b>🔨 Moderation</b>\n"
        "/ban &lt;id&gt; [reason]\n"
        "/unban &lt;id&gt;\n"
        "/tban &lt;id&gt; &lt;min&gt;\n\n"
        "<b>📢 Broadcast</b>\n"
        "/broadcast &lt;msg&gt;\n\n"
        "<b>📊 Data</b>\n"
        "/userdata - Export users\n"
        "/hitsfile - Export approved\n"
        "/stats - Statistics"
    )
    await update.message.reply_text(text, parse_mode="HTML")

# ==================== CALLBACKS ====================
async def button_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    if query.data == "profile":
        user = update.effective_user
        single = await get_single_check_count_fast(user.id)
        mass = await get_mass_check_count_fast(user.id)
        limit = await get_user_limit_fast(user.id)
        
        text = (
            f"<b>User ID</b> • <code>{user.id}</code>\n"
            f"<b>Name</b> • {user.full_name}\n"
            f"<b>Single Checks</b> • {single}/{limit}\n"
            f"<b>Mass Checks</b> • {mass}/{MAX_MASS_CHECKS_PER_HOUR}"
        )
        keyboard = [[InlineKeyboardButton("Back", callback_data="back_to_cmds")]]
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif query.data == "rules":
        text = "<b>Rules:</b>\n\n1. Same card 5 times in 2 minutes = 5 min ban"
        keyboard = [[InlineKeyboardButton("Back", callback_data="back_to_cmds")]]
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif query.data == "back_to_cmds":
        keyboard = [
            [InlineKeyboardButton("Profile", callback_data="profile")],
            [InlineKeyboardButton("Rules", callback_data="rules")]
        ]
        text = "<b>Auth</b>\n• /chk - Stripe Auth\n\n<b>Charge</b>\n• /ss - Stripe+Square $0.001\n\n<b>Mass</b>\n• /mchk - Mass Check\n\n<b>Other</b>\n• /bin - BIN Check\n• /rules - Rules\n• /info - Profile"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

# ==================== MAIN ====================
async def main():
    print("🚀 Starting ULTRA FAST bot...")
    
    # Initialize database pool
    await db_pool.initialize()
    await init_db()
    
    # Load sessions
    session_count = await load_sessions_fast()
    print(f"📁 Loaded {session_count} sessions")
    
    # Build application
    application = Application.builder().token(TOKEN).build()
    
    # User commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("profile", profile))
    application.add_handler(CommandHandler("cmds", cmds))
    application.add_handler(CommandHandler("rules", rules))
    application.add_handler(CommandHandler("info", info))
    
    # Main commands
    application.add_handler(CommandHandler("chk", handle_chk_command))
    application.add_handler(CommandHandler("ss", handle_ss_command))
    application.add_handler(CommandHandler("mchk", handle_mchk_command))
    application.add_handler(CommandHandler("bin", handle_bin_command))
    
    # Owner commands
    application.add_handler(CommandHandler("ownermode", owner_menu))
    application.add_handler(CommandHandler("add", add_session))
    application.add_handler(CommandHandler("sessions", sessions_list))
    application.add_handler(CommandHandler("rm", remove_session))
    application.add_handler(CommandHandler("delay", set_delay_cmd))
    application.add_handler(CommandHandler("reset", reset_user))
    application.add_handler(CommandHandler("increase", increase_limit))
    application.add_handler(CommandHandler("ban", ban_user))
    application.add_handler(CommandHandler("unban", unban_user))
    application.add_handler(CommandHandler("tban", tban_user))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("userdata", userdata))
    application.add_handler(CommandHandler("hitsfile", hitsfile))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("setchannel", set_channel))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # File handler
    application.add_handler(MessageHandler(filters.Document.ALL, handle_session_file))
    application.add_handler(MessageHandler(filters.COMMAND, lambda u,c: u.message.reply_text("❌ Unknown command. Use /cmds")))
    
    print(f"✅ Bot is LIVE! Ready for {len(sessions_queue) * MAX_CONCURRENT_CHECKS}+ concurrent checks")
    await application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    asyncio.run(main())
