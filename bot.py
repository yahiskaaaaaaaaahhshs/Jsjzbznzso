import re
import json
import time
import asyncio
import os
import aiohttp
import csv
import io
import random
from collections import defaultdict, deque
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import sqlite3

# ==================== CONFIG ====================
TOKEN = "8735764992:AAEMKvlWXyruQj49KLz4IJHIDVU6ophL644"
BOT_USERNAME = 'newpayubot'
MESSAGE_PREFIX = '/st'

API_ID = 29612794
API_HASH = '6edc1a58a202c9f6e62dc98466932bad'
OWNER_ID = 8129488947
SESSION_DIR = "sessions"
TIMEOUT_SECONDS = 15
MAX_SINGLE_LIMIT = 100
MAX_MASS_LIMIT = 10
MAX_MASS_CHECKS_PER_HOUR = 100
SINGLE_WINDOW_HOURS = 1
MASS_WINDOW_HOURS = 1
MAX_CONCURRENT_CHECKS = 2
SINGLE_CHECK_DELAY = 2
MASS_COOLDOWN = 15

if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# Clear sessions folder on start
for f in os.listdir(SESSION_DIR):
    try:
        os.remove(os.path.join(SESSION_DIR, f))
    except:
        pass

# Database
conn = sqlite3.connect('bot_data.db', check_same_thread=False, timeout=30)
c = conn.cursor()

c.execute('PRAGMA journal_mode=WAL')
c.execute('PRAGMA synchronous=NORMAL')

c.execute('''CREATE TABLE IF NOT EXISTS sessions
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_name TEXT UNIQUE,
              phone_number TEXT,
              created_at TIMESTAMP,
              is_active BOOLEAN DEFAULT 1)''')

c.execute('''CREATE TABLE IF NOT EXISTS banned_users
             (user_id INTEGER PRIMARY KEY,
              banned_until TIMESTAMP,
              reason TEXT)''')

c.execute('''CREATE TABLE IF NOT EXISTS user_stats
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

c.execute('''CREATE TABLE IF NOT EXISTS bot_config
             (key TEXT PRIMARY KEY,
              value TEXT)''')

c.execute('''CREATE TABLE IF NOT EXISTS admin_config
             (key TEXT PRIMARY KEY,
              value TEXT)''')

c.execute('''CREATE TABLE IF NOT EXISTS approved_cards
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              card_data TEXT,
              status TEXT,
              response TEXT,
              checked_at TIMESTAMP,
              checked_by INTEGER)''')

conn.commit()

c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('check_delay', '2')")
c.execute("INSERT OR IGNORE INTO admin_config (key, value) VALUES ('approved_channel', '')")
conn.commit()

# Global variables
sessions_queue = deque()
user_cooldown = {}
mass_checking_active = {}
user_mass_cooldown = {}
card_attempts = defaultdict(list)

# ==================== HELPER FUNCTIONS ====================

def is_owner(user_id):
    return user_id == OWNER_ID

def get_user_limit(user_id):
    if is_owner(user_id):
        return 999999
    try:
        c.execute("SELECT custom_limit FROM user_stats WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if result and result[0] > 0:
            return result[0]
    except:
        pass
    return MAX_SINGLE_LIMIT

def get_mass_hour_limit(user_id):
    if is_owner(user_id):
        return 999999
    return MAX_MASS_CHECKS_PER_HOUR

def is_banned(user_id):
    try:
        c.execute("SELECT banned_until, reason FROM banned_users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if result:
            banned_until = datetime.fromisoformat(result[0])
            if banned_until > datetime.now():
                return True, banned_until, result[1]
            else:
                c.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
                conn.commit()
    except:
        pass
    return False, None, None

def auto_ban_card(card_hash, user_id):
    now = datetime.now()
    card_attempts[card_hash].append((now, user_id))
    
    card_attempts[card_hash] = [(t, uid) for t, uid in card_attempts[card_hash] if now - t < timedelta(minutes=2)]
    
    user_attempts = [uid for _, uid in card_attempts[card_hash] if uid == user_id]
    
    if len(user_attempts) >= 5:
        banned_until = now + timedelta(minutes=5)
        c.execute("INSERT OR REPLACE INTO banned_users (user_id, banned_until, reason) VALUES (?, ?, ?)",
                  (user_id, banned_until.isoformat(), "Same card used 5 times within 2 minutes"))
        conn.commit()
        return True
    return False

def register_user(user):
    try:
        c.execute("SELECT user_id FROM user_stats WHERE user_id = ?", (user.id,))
        if not c.fetchone():
            c.execute("INSERT INTO user_stats (user_id, first_name, username, total_checks, join_date, single_last_reset, mass_last_reset) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (user.id, user.first_name, user.username, 0, datetime.now().isoformat(), datetime.now().isoformat(), datetime.now().isoformat()))
            conn.commit()
    except:
        pass

def update_single_stats(user):
    try:
        now = datetime.now()
        c.execute("SELECT single_last_reset, single_checks_today FROM user_stats WHERE user_id = ?", (user.id,))
        result = c.fetchone()
        
        if result and result[0]:
            last_reset = datetime.fromisoformat(result[0])
            if now - last_reset > timedelta(hours=SINGLE_WINDOW_HOURS):
                single_checks_today = 1
            else:
                single_checks_today = (result[1] or 0) + 1
        else:
            single_checks_today = 1
        
        c.execute("UPDATE user_stats SET total_checks = total_checks + 1, single_checks_today = ?, single_last_reset = ?, first_name = ?, username = ? WHERE user_id = ?",
                  (single_checks_today, now.isoformat(), user.first_name, user.username, user.id))
        conn.commit()
        return single_checks_today
    except:
        return 0

def update_mass_stats(user, count):
    try:
        now = datetime.now()
        c.execute("SELECT mass_last_reset, mass_checks_today FROM user_stats WHERE user_id = ?", (user.id,))
        result = c.fetchone()
        
        if result and result[0]:
            last_reset = datetime.fromisoformat(result[0])
            if now - last_reset > timedelta(hours=MASS_WINDOW_HOURS):
                mass_checks_today = count
            else:
                mass_checks_today = (result[1] or 0) + count
        else:
            mass_checks_today = count
        
        c.execute("UPDATE user_stats SET total_checks = total_checks + ?, mass_checks_today = ?, mass_last_reset = ? WHERE user_id = ?",
                  (count, mass_checks_today, now.isoformat(), user.id))
        conn.commit()
        return mass_checks_today
    except:
        return 0

def get_single_check_count(user_id):
    try:
        c.execute("SELECT single_checks_today FROM user_stats WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        return result[0] if result else 0
    except:
        return 0

def get_mass_check_count(user_id):
    try:
        c.execute("SELECT mass_checks_today FROM user_stats WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        return result[0] if result else 0
    except:
        return 0

def reset_user_checks(user_id):
    try:
        c.execute("UPDATE user_stats SET single_checks_today = 0, mass_checks_today = 0, single_last_reset = ?, mass_last_reset = ? WHERE user_id = ?", 
                  (datetime.now().isoformat(), datetime.now().isoformat(), user_id))
        conn.commit()
    except:
        pass

def set_user_limit(user_id, limit):
    try:
        c.execute("UPDATE user_stats SET custom_limit = ? WHERE user_id = ?", (limit, user_id))
        conn.commit()
    except:
        pass

def get_all_stats():
    try:
        c.execute("SELECT COUNT(DISTINCT user_id), SUM(total_checks) FROM user_stats")
        result = c.fetchone()
        return result[0] or 0, result[1] or 0
    except:
        return 0, 0

def get_delay():
    try:
        c.execute("SELECT value FROM bot_config WHERE key = 'check_delay'")
        result = c.fetchone()
        return int(result[0]) if result else 2
    except:
        return 2

def set_delay(seconds):
    try:
        c.execute("UPDATE bot_config SET value = ? WHERE key = 'check_delay'", (str(seconds),))
        conn.commit()
    except:
        pass

def get_approved_channel():
    try:
        c.execute("SELECT value FROM admin_config WHERE key = 'approved_channel'")
        result = c.fetchone()
        return result[0] if result else ""
    except:
        return ""

def set_approved_channel(channel_id):
    try:
        c.execute("INSERT OR REPLACE INTO admin_config (key, value) VALUES ('approved_channel', ?)", (channel_id,))
        conn.commit()
    except:
        pass

def save_approved_card(card_data, status, response, user_id):
    try:
        c.execute("INSERT INTO approved_cards (card_data, status, response, checked_at, checked_by) VALUES (?, ?, ?, ?, ?)",
                  (card_data, status, response, datetime.now().isoformat(), user_id))
        conn.commit()
    except:
        pass

def load_sessions():
    global sessions_queue
    try:
        c.execute("SELECT session_name FROM sessions WHERE is_active = 1 ORDER BY id")
        sessions = [row[0] for row in c.fetchall()]
        sessions_queue = deque(sessions)
    except:
        sessions_queue = deque()
    return list(sessions_queue)

def get_next_session():
    if sessions_queue:
        session = sessions_queue[0]
        sessions_queue.rotate(-1)
        return session
    return None

def clear_all_sessions():
    global sessions_queue
    try:
        for f in os.listdir(SESSION_DIR):
            if f.endswith('.session'):
                try:
                    os.remove(os.path.join(SESSION_DIR, f))
                except:
                    pass
        c.execute("DELETE FROM sessions")
        conn.commit()
        sessions_queue = deque()
    except:
        pass

def validate_and_parse_card(card_input):
    card_input = card_input.strip().replace(' ', '')
    
    if '|' in card_input or ':' in card_input:
        if '|' in card_input:
            parts = card_input.split('|')
        else:
            parts = card_input.split(':')
        
        if len(parts) >= 4:
            card_num = parts[0].strip()
            month = parts[1].strip().zfill(2)
            year = parts[2].strip()
            cvv = parts[3].strip()
            
            if len(year) == 4:
                year = year[-2:]
            elif len(year) == 2:
                year = year
            else:
                return False, None
            
            year = year.zfill(2)
            
            if int(month) < 1 or int(month) > 12:
                return False, None
            
            if re.match(r'^\d{15,16}$', card_num) and re.match(r'^\d{2}$', month) and re.match(r'^\d{2}$', year) and re.match(r'^\d{3,4}$', cvv):
                return True, f"{card_num}|{month}|{year}|{cvv}"
    
    match = re.search(r'^(\d{15,16})(\d{2})(\d{2})(\d{3,4})$', card_input)
    if match:
        card_num = match.group(1)
        month = match.group(2).zfill(2)
        year = match.group(3).zfill(2)
        cvv = match.group(4)
        
        if int(month) >= 1 and int(month) <= 12:
            return True, f"{card_num}|{month}|{year}|{cvv}"
    
    match = re.search(r'^(\d{15,16})(\d{2})(\d{4})(\d{3,4})$', card_input)
    if match:
        card_num = match.group(1)
        month = match.group(2).zfill(2)
        year = match.group(3)[-2:].zfill(2)
        cvv = match.group(4)
        
        if int(month) >= 1 and int(month) <= 12:
            return True, f"{card_num}|{month}|{year}|{cvv}"
    
    return False, None

def parse_multiple_cards(text):
    cards = []
    lines = text.strip().split('\n')
    
    for line in lines:
        if len(cards) >= MAX_MASS_LIMIT:
            break
            
        line = line.strip()
        if not line:
            continue
        
        card_match = re.search(r'(\d{15,16})\s*[|:]\s*(\d{1,2})\s*[|:]\s*(\d{2,4})\s*[|:]\s*(\d{3,4})', line)
        if card_match:
            card_num = card_match.group(1)
            month = card_match.group(2).zfill(2)
            year = card_match.group(3)
            cvv = card_match.group(4)
            
            if len(year) == 4:
                year = year[-2:]
            year = year.zfill(2)
            
            if 1 <= int(month) <= 12:
                cards.append(f"{card_num}|{month}|{year}|{cvv}")
            continue
        
        cont_match = re.search(r'(\d{15,16})(\d{2})(\d{2,4})(\d{3,4})', line)
        if cont_match:
            card_num = cont_match.group(1)
            month = cont_match.group(2).zfill(2)
            year = cont_match.group(3)
            cvv = cont_match.group(4)
            
            if len(year) == 4:
                year = year[-2:]
            year = year.zfill(2)
            
            if 1 <= int(month) <= 12:
                cards.append(f"{card_num}|{month}|{year}|{cvv}")
            continue
    
    return cards[:MAX_MASS_LIMIT]

def parse_bot_reply(text):
    cc_match = re.search(r'CC:\s*(.*?)(?:\n|$)', text)
    status_match = re.search(r'Status:\s*(.*?)(?:\n|$)', text)
    response_match = re.search(r'Response:\s*(.*?)(?:\n|$)', text)
    bank_match = re.search(r'Bank:\s*(.*?)(?:\n|$)', text)
    type_match = re.search(r'Type:\s*(.*?)(?:\n|$)', text)
    country_match = re.search(r'Country:\s*(.*?)(?:\n|$)', text)
    
    status = status_match.group(1).strip() if status_match else ""
    
    return {
        "cc": cc_match.group(1).strip() if cc_match else "",
        "status": status,
        "response": response_match.group(1).strip() if response_match else "",
        "gateway": "Stripe",
        "bank": bank_match.group(1).strip() if bank_match else "",
        "type": type_match.group(1).strip() if type_match else "",
        "country": country_match.group(1).strip() if country_match else ""
    }

def export_user_data():
    try:
        c.execute("SELECT user_id, first_name, username, total_checks, last_single_check, single_checks_today, mass_checks_today, join_date FROM user_stats")
        users = c.fetchall()
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['User ID', 'First Name', 'Username', 'Total Checks', 'Last Check', 'Single Checks Today', 'Mass Checks Today', 'Join Date'])
        writer.writerows(users)
        
        output.seek(0)
        return output.getvalue()
    except:
        return ""

def export_approved_cards():
    try:
        c.execute("SELECT card_data, status, response, checked_at, checked_by FROM approved_cards ORDER BY checked_at DESC")
        cards = c.fetchall()
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Card Data', 'Status', 'Response', 'Checked At', 'Checked By'])
        writer.writerows(cards)
        
        output.seek(0)
        return output.getvalue()
    except:
        return ""

# ==================== ASYNC FUNCTIONS ====================

async def get_bin_info(bin_num):
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://lookup.binlist.net/{bin_num}"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    country_name = data.get('country', {}).get('name', 'Unknown')
                    scheme = data.get('scheme', 'Unknown').upper()
                    card_type = data.get('type', 'Unknown').capitalize()
                    
                    bank = data.get('bank', {})
                    bank_name = bank.get('name', 'Unknown')
                    
                    prepaid = data.get('prepaid', False)
                    category = 'Prepaid' if prepaid else 'Standard'
                    
                    return {
                        "bin": bin_num,
                        "bank": bank_name,
                        "brand": scheme,
                        "category": category,
                        "type": card_type,
                        "country": country_name
                    }
                else:
                    return {"error": "BIN not found"}
    except Exception as e:
        return {"error": f"API error: {str(e)}"}

async def send_card_via_session(card_data, session_name):
    session_path = os.path.join(SESSION_DIR, session_name)
    
    client = None
    try:
        client = TelegramClient(session_path, API_ID, API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            return {"error": "Session not authorized", "status": "Error"}
        
        entity = await client.get_entity(f'@{BOT_USERNAME}')
        message = f"{MESSAGE_PREFIX} {card_data}"
        await client.send_message(entity, message)
        
        start_time = time.time()
        
        while time.time() - start_time < TIMEOUT_SECONDS:
            await asyncio.sleep(1.5)
            
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
                    parsed = parse_bot_reply(text)
                    if parsed['status'] and parsed['status'] != '⏳':
                        return parsed
        
        return {"error": "timeout", "status": "Error", "response": "No response from gateway"}
        
    except Exception as e:
        return {"error": str(e), "status": "Error", "response": "Connection error"}
    finally:
        if client:
            try:
                await client.disconnect()
            except:
                pass

async def verify_session_file(file_path):
    try:
        client = TelegramClient(file_path, API_ID, API_HASH)
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            await client.disconnect()
            return True, me.phone if me.phone else "Unknown"
        else:
            await client.disconnect()
            return False, "Session not authorized"
    except Exception as e:
        return False, str(e)

# ==================== COMMAND HANDLERS ====================

async def start(update: Update, context):
    user = update.effective_user
    register_user(user)
    
    keyboard = [[InlineKeyboardButton("Support", url="https://t.me/SoenxSupportBot")]]
    await update.message.reply_text(
        f"<b>Welcome {user.first_name} to SoenxBot - ⚡</b>\n\n/cmds to see available commands.\n/profile to Get Your Info.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def profile(update: Update, context):
    user = update.effective_user
    username = user.username if user.username else 'No username'
    single_checks = get_single_check_count(user.id)
    mass_checks = get_mass_check_count(user.id)
    user_limit = get_user_limit(user.id)
    
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

async def handle_chk_command(update: Update, context):
    user = update.effective_user
    register_user(user)
    
    card_input = None
    
    if update.message.reply_to_message:
        card_input = update.message.reply_to_message.text
    elif context.args:
        card_input = ' '.join(context.args)
    else:
        await update.message.reply_text(
            "<b>Usage:</b>\n"
            "<code>/chk cc|mm|yyyy|cvv</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/chk 4663490011245506|02|2027|453</code>",
            parse_mode="HTML"
        )
        return
    
    is_valid, parsed_card = validate_and_parse_card(card_input)
    
    if not is_valid:
        await update.message.reply_text(
            "<b>Usage:</b>\n"
            "<code>/chk cc|mm|yyyy|cvv</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/chk 4663490011245506|02|2027|453</code>",
            parse_mode="HTML"
        )
        return
    
    banned, until, reason = is_banned(user.id)
    if banned:
        await update.message.reply_text(f"⛔ You are temporarily banned.\nReason: {reason}\nBanned until: {until.strftime('%Y-%m-%d %H:%M:%S')}")
        return
    
    single_checks = get_single_check_count(user.id)
    user_limit = get_user_limit(user.id)
    
    if single_checks >= user_limit and not is_owner(user.id):
        await update.message.reply_text("⚠️ Limit reached (100 checks). Contact owner to reset your limit.")
        return
    
    current_time = time.time()
    if user.id in user_cooldown:
        elapsed = current_time - user_cooldown[user.id]
        delay = get_delay()
        if elapsed < delay:
            wait_time = int(delay - elapsed)
            await update.message.reply_text(f"⏳ Please wait {wait_time} seconds before next check.")
            return
    
    user_cooldown[user.id] = current_time
    
    card_hash = parsed_card.split('|')[0]
    if auto_ban_card(card_hash, user.id):
        await update.message.reply_text("⛔ You have been banned for 5 minutes for using the same card too many times.")
        return
    
    if update.message.reply_to_message:
        msg = await update.message.reply_to_message.reply_text("🔄 Processing... Please wait")
    else:
        msg = await update.message.reply_text("🔄 Processing... Please wait")
    
    session_name = get_next_session()
    
    if not session_name:
        await msg.edit_text("❌ No active session available. Owner please use /add to add a session.")
        return
    
    result = await send_card_via_session(parsed_card, session_name)
    
    if "error" in result:
        if result.get("error") == "timeout":
            formatted = (
                f"<b>☇ CC:</b> {parsed_card}\n"
                f"<b>ヾ⌿ Status:</b> Error\n"
                f"<b>ヾ⌿ Response:</b> No response from gateway\n"
                f"<b>· · · · · · · · · · · · · · ·</b>\n"
                f"<b>ヾ⌿ Bank:</b> N/A\n"
                f"<b>ヾ⌿ Type:</b> N/A\n"
                f"<b>ヾ⌿ Country:</b> N/A\n"
                f"<b>· · · · · · · · · · · · · · ·</b>\n"
                f"<b>ヾ⌿ Checked By:</b> <a href='tg://user?id={user.id}'>{user.first_name}</a> (<code>{user.id}</code>)"
            )
        else:
            formatted = f"<b>❌ Error:</b> {result.get('error', 'Unknown error')}"
        
        await msg.edit_text(formatted, parse_mode="HTML")
        return
    
    is_approved = 'approved' in result.get('status', '').lower()
    
    if is_approved:
        save_approved_card(parsed_card, result.get('status', ''), result.get('response', ''), user.id)
        approved_channel = get_approved_channel()
        if approved_channel:
            try:
                await context.bot.send_message(
                    approved_channel,
                    f"<b>☇ CC:</b> {parsed_card}\n"
                    f"<b>ヾ⌿ Status:</b> {result.get('status', 'N/A')}\n"
                    f"<b>ヾ⌿ Response:</b> {result.get('response', 'N/A')}\n"
                    f"<b>· · · · · · · · · · · · · · ·</b>\n"
                    f"<b>ヾ⌿ Bank:</b> {result.get('bank', 'N/A')}\n"
                    f"<b>ヾ⌿ Type:</b> {result.get('type', 'N/A')}\n"
                    f"<b>ヾ⌿ Country:</b> {result.get('country', 'N/A')}",
                    parse_mode="HTML"
                )
            except:
                pass
    
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
        f"<b>ヾ⌿ Checked By:</b> <a href='tg://user?id={user.id}'>{user.first_name}</a> (<code>{user.id}</code>)"
    )
    
    await msg.edit_text(formatted, parse_mode="HTML")
    update_single_stats(user)

async def handle_ss_command(update: Update, context):
    user = update.effective_user
    register_user(user)
    
    card_input = None
    
    if update.message.reply_to_message:
        card_input = update.message.reply_to_message.text
    elif context.args:
        card_input = ' '.join(context.args)
    else:
        await update.message.reply_text(
            "<b>Usage:</b>\n"
            "<code>/ss cc|mm|yyyy|cvv</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/ss 4663490011245506|02|2027|453</code>",
            parse_mode="HTML"
        )
        return
    
    is_valid, parsed_card = validate_and_parse_card(card_input)
    
    if not is_valid:
        await update.message.reply_text(
            "<b>Usage:</b>\n"
            "<code>/ss cc|mm|yyyy|cvv</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/ss 4663490011245506|02|2027|453</code>",
            parse_mode="HTML"
        )
        return
    
    banned, until, reason = is_banned(user.id)
    if banned:
        await update.message.reply_text(f"⛔ You are temporarily banned.\nReason: {reason}\nBanned until: {until.strftime('%Y-%m-%d %H:%M:%S')}")
        return
    
    single_checks = get_single_check_count(user.id)
    user_limit = get_user_limit(user.id)
    
    if single_checks >= user_limit and not is_owner(user.id):
        await update.message.reply_text("⚠️ Limit reached (100 checks). Contact owner to reset your limit.")
        return
    
    current_time = time.time()
    if user.id in user_cooldown:
        elapsed = current_time - user_cooldown[user.id]
        delay = get_delay()
        if elapsed < delay:
            wait_time = int(delay - elapsed)
            await update.message.reply_text(f"⏳ Please wait {wait_time} seconds before next check.")
            return
    
    user_cooldown[user.id] = current_time
    
    card_hash = parsed_card.split('|')[0]
    if auto_ban_card(card_hash, user.id):
        await update.message.reply_text("⛔ You have been banned for 5 minutes for using the same card too many times.")
        return
    
    if update.message.reply_to_message:
        msg = await update.message.reply_to_message.reply_text("🔄 Processing... Please wait")
    else:
        msg = await update.message.reply_text("🔄 Processing... Please wait")
    
    session_name = get_next_session()
    
    if not session_name:
        await msg.edit_text("❌ No active session available. Owner please use /add to add a session.")
        return
    
    result = await send_card_via_session(parsed_card, session_name)
    
    if "error" in result:
        if result.get("error") == "timeout":
            formatted = (
                f"<b>☇ CC:</b> {parsed_card}\n"
                f"<b>ヾ⌿ Status:</b> Error\n"
                f"<b>ヾ⌿ Response:</b> No response from gateway\n"
                f"<b>· · · · · · · · · · · · · · ·</b>\n"
                f"<b>ヾ⌿ Bank:</b> N/A\n"
                f"<b>ヾ⌿ Type:</b> N/A\n"
                f"<b>ヾ⌿ Country:</b> N/A\n"
                f"<b>· · · · · · · · · · · · · · ·</b>\n"
                f"<b>ヾ⌿ Checked By:</b> <a href='tg://user?id={user.id}'>{user.first_name}</a> (<code>{user.id}</code>)"
            )
        else:
            formatted = f"<b>❌ Error:</b> {result.get('error', 'Unknown error')}"
        
        await msg.edit_text(formatted, parse_mode="HTML")
        return
    
    is_approved = 'approved' in result.get('status', '').lower()
    
    if is_approved:
        save_approved_card(parsed_card, result.get('status', ''), result.get('response', ''), user.id)
        approved_channel = get_approved_channel()
        if approved_channel:
            try:
                await context.bot.send_message(
                    approved_channel,
                    f"<b>☇ CC:</b> {parsed_card}\n"
                    f"<b>ヾ⌿ Status:</b> {result.get('status', 'N/A')}\n"
                    f"<b>ヾ⌿ Response:</b> {result.get('response', 'N/A')}\n"
                    f"<b>· · · · · · · · · · · · · · ·</b>\n"
                    f"<b>ヾ⌿ Bank:</b> {result.get('bank', 'N/A')}\n"
                    f"<b>ヾ⌿ Type:</b> {result.get('type', 'N/A')}\n"
                    f"<b>ヾ⌿ Country:</b> {result.get('country', 'N/A')}",
                    parse_mode="HTML"
                )
            except:
                pass
    
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
        f"<b>ヾ⌿ Checked By:</b> <a href='tg://user?id={user.id}'>{user.first_name}</a> (<code>{user.id}</code>)"
    )
    
    await msg.edit_text(formatted, parse_mode="HTML")
    update_single_stats(user)

async def handle_mchk_command(update: Update, context):
    user = update.effective_user
    register_user(user)
    
    if user.id in mass_checking_active and mass_checking_active[user.id]:
        await update.message.reply_text("⏳ You already have an active mass check. Please wait for it to complete.\nAfter complete, wait 15 seconds before starting new mass check.", parse_mode="HTML")
        return
    
    if user.id in user_mass_cooldown:
        elapsed = time.time() - user_mass_cooldown[user.id]
        if elapsed < MASS_COOLDOWN:
            wait_time = int(MASS_COOLDOWN - elapsed)
            await update.message.reply_text(f"⏳ Please wait {wait_time} seconds before starting a new mass check.", parse_mode="HTML")
            return
    
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
            "<b>Usage:</b>\n"
            "<code>/mchk</code> (reply to message with cards or send cards in command)\n\n"
            "<b>Format:</b>\n"
            "<code>cc|mm|yyyy|cvv</code>\n"
            "<code>cc|mm|yyyy|cvv</code>\n\n"
            "<b>Max 10 cards per mass check</b>",
            parse_mode="HTML"
        )
        return
    
    cards = parse_multiple_cards(card_text)
    
    if not cards:
        await update.message.reply_text("❌ No valid cards found. Use format: cc|mm|yyyy|cvv (one per line)", parse_mode="HTML")
        return
    
    if len(cards) > MAX_MASS_LIMIT:
        await update.message.reply_text(f"❌ Max {MAX_MASS_LIMIT} cards per mass check. You sent {len(cards)} cards.\nFirst {MAX_MASS_LIMIT} cards will be checked.", parse_mode="HTML")
        cards = cards[:MAX_MASS_LIMIT]
    
    banned, until, reason = is_banned(user.id)
    if banned:
        await update.message.reply_text(f"⛔ You are temporarily banned.\nReason: {reason}\nBanned until: {until.strftime('%Y-%m-%d %H:%M:%S')}")
        return
    
    mass_checks_done = get_mass_check_count(user.id)
    mass_hour_limit = get_mass_hour_limit(user.id)
    
    if mass_checks_done + len(cards) > mass_hour_limit and not is_owner(user.id):
        remaining = mass_hour_limit - mass_checks_done
        await update.message.reply_text(
            f"⚠️ Limit reached! You have {remaining} mass checks remaining this hour.\nMax 100 cards total in mass checks per hour.",
            parse_mode="HTML"
        )
        return
    
    if not sessions_queue:
        await update.message.reply_text("❌ No active sessions available. Owner please use /add to add a session.")
        return
    
    mass_checking_active[user.id] = True
    
    status_msg = await update.message.reply_text(
        f"<b>Cards:</b> {len(cards)}\n"
        f"<b>Checking:</b> 0/{len(cards)}\n"
        f"<b>Remaining:</b> {len(cards)}\n"
        f"<b>Checked:</b> 0\n"
        f"<b>Approved:</b> 0\n"
        f"<b>Declined:</b> 0\n"
        f"<b>Error:</b> 0\n\n"
        f"<b>🔄 Mass check started...</b>",
        parse_mode="HTML"
    )
    
    results = []
    approved_cards = []
    declined_cards = []
    error_cards = []
    checked = 0
    
    for i, card in enumerate(cards):
        if user.id not in mass_checking_active or not mass_checking_active[user.id]:
            await status_msg.edit_text("⏹️ Mass check cancelled by user.", parse_mode="HTML")
            break
        
        card_hash = card.split('|')[0]
        if auto_ban_card(card_hash, user.id):
            results.append({"card": card, "status": "Banned", "response": "User banned for same card spam"})
            error_cards.append(card)
        else:
            session_name = get_next_session()
            if not session_name:
                results.append({"card": card, "status": "Error", "response": "No session available"})
                error_cards.append(card)
            else:
                result = await send_card_via_session(card, session_name)
                
                if "error" in result:
                    results.append({"card": card, "status": "Error", "response": result.get('error', 'Unknown')})
                    error_cards.append(card)
                else:
                    results.append({
                        "card": card,
                        "status": result.get('status', 'N/A'),
                        "response": result.get('response', 'N/A')
                    })
                    
                    if 'approved' in result.get('status', '').lower():
                        approved_cards.append(card)
                        save_approved_card(card, result.get('status', ''), result.get('response', ''), user.id)
                    elif 'declined' in result.get('status', '').lower():
                        declined_cards.append(card)
                    else:
                        error_cards.append(card)
        
        checked = i + 1
        remaining = len(cards) - checked
        
        await status_msg.edit_text(
            f"<b>Cards:</b> {len(cards)}\n"
            f"<b>Checking:</b> {checked}/{len(cards)}\n"
            f"<b>Remaining:</b> {remaining}\n"
            f"<b>Checked:</b> {checked}\n"
            f"<b>Approved:</b> {len(approved_cards)}\n"
            f"<b>Declined:</b> {len(declined_cards)}\n"
            f"<b>Error:</b> {len(error_cards)}\n\n"
            f"<b>🔄 Checking card {checked}/{len(cards)}...</b>",
            parse_mode="HTML"
        )
        
        await asyncio.sleep(0.5)
    
    result_text = f"<b>☇ Mass Check Results</b>\n"
    result_text += f"<b>· · · · · · · · · · · · · · ·</b>\n"
    
    for idx, res in enumerate(results):
        result_text += f"\n<b>☇ CC:</b> <code>{res['card']}</code>  {idx+1}\n"
        result_text += f"<b>ヾ⌿ Status:</b> {res.get('status', 'N/A')}\n"
        result_text += f"<b>ヾ⌿ Response:</b> {res.get('response', 'N/A')}\n"
        result_text += f"<b>· · · · · · · · · · · · · · ·</b>\n"
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if approved_cards:
        approved_file = io.BytesIO('\n'.join(approved_cards).encode('utf-8'))
        await update.message.reply_document(document=approved_file, filename=f"@Soenx_app_{timestamp}.txt", caption=f"✅ Approved Cards ({len(approved_cards)})")
    
    if declined_cards:
        declined_file = io.BytesIO('\n'.join(declined_cards).encode('utf-8'))
        await update.message.reply_document(document=declined_file, filename=f"@Soenx_dec_{timestamp}.txt", caption=f"❌ Declined Cards ({len(declined_cards)})")
    
    if error_cards:
        error_file = io.BytesIO('\n'.join(error_cards).encode('utf-8'))
        await update.message.reply_document(document=error_file, filename=f"@Soenx_err_{timestamp}.txt", caption=f"⚠️ Error Cards ({len(error_cards)})")
    
    await status_msg.edit_text(result_text[:4000], parse_mode="HTML")
    
    update_mass_stats(user, len(cards))
    
    user_mass_cooldown[user.id] = time.time()
    
    await asyncio.sleep(MASS_COOLDOWN)
    mass_checking_active[user.id] = False

async def handle_bin_command(update: Update, context):
    user = update.effective_user
    register_user(user)
    
    bin_input = None
    
    if update.message.reply_to_message:
        bin_input = update.message.reply_to_message.text
    elif context.args:
        bin_input = ' '.join(context.args)
    else:
        await update.message.reply_text(
            "<b>Usage:</b>\n<code>/bin 466349</code>\n\n"
            "<b>Example:</b>\n<code>/bin 466349</code>",
            parse_mode="HTML"
        )
        return
    
    banned, until, reason = is_banned(user.id)
    if banned:
        await update.message.reply_text(f"⛔ You are temporarily banned.\nReason: {reason}\nBanned until: {until.strftime('%Y-%m-%d %H:%M:%S')}")
        return
    
    bin_digits = re.search(r'(\d{6})', bin_input)
    if not bin_digits:
        await update.message.reply_text("❌ Invalid BIN. Please provide first 6 digits of card.")
        return
    
    bin_num = bin_digits.group(1)
    
    if update.message.reply_to_message:
        msg = await update.message.reply_to_message.reply_text("🔄 Fetching BIN information...")
    else:
        msg = await update.message.reply_text("🔄 Fetching BIN information...")
    
    result = await get_bin_info(bin_num)
    
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

async def userdata(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    csv_data = export_user_data()
    
    if not csv_data.strip():
        await update.message.reply_text("📭 No user data found.")
        return
    
    await update.message.reply_document(
        document=io.BytesIO(csv_data.encode('utf-8')),
        filename=f"userdata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        caption="User Data Export"
    )

async def hitsfile(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    csv_data = export_approved_cards()
    
    if not csv_data.strip():
        await update.message.reply_text("📭 No approved cards found.")
        return
    
    await update.message.reply_document(
        document=io.BytesIO(csv_data.encode('utf-8')),
        filename=f"approved_cards_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        caption="Approved Cards Export"
    )

# ==================== SESSION MANAGEMENT ====================

async def add_session(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    await update.message.reply_text(
        "📁 Send your Telegram session file (.session)\n\n"
        "Note: Multiple sessions can be added for load balancing.\n"
        "Sessions will be rotated automatically.\n\n"
        "How to get session file:\n"
        "Use Telethon to generate .session file\n"
        "Send the .session file directly to this bot"
    )

async def handle_session_file(update: Update, context):
    user_id = update.effective_user.id
    
    if not is_owner(user_id):
        return
    
    if not update.message.document:
        return
    
    document = update.message.document
    file_name = document.file_name
    
    if not file_name.endswith('.session'):
        await update.message.reply_text("❌ Invalid file type. Send a .session file only.")
        return
    
    file_id = document.file_id
    new_file_name = f"session_{int(time.time())}_{random.randint(1000, 9999)}.session"
    file_path = os.path.join(SESSION_DIR, new_file_name)
    
    try:
        msg = await update.message.reply_text("🔄 Verifying session file...")
        
        file = await context.bot.get_file(file_id)
        await file.download_to_drive(file_path)
        
        is_valid, info = await verify_session_file(file_path)
        
        if is_valid:
            c.execute("INSERT INTO sessions (session_name, phone_number, created_at, is_active) VALUES (?, ?, ?, ?)",
                      (new_file_name, info, datetime.now(), 1))
            conn.commit()
            
            load_sessions()
            
            await msg.edit_text(f"✅ Session added successfully!\n\n📱 Phone: {info}\n📁 File: {new_file_name}\n🔄 Total active sessions: {len(sessions_queue)}\n\nUse /chk to check cards.")
        else:
            await msg.edit_text(f"❌ Invalid session file!\nError: {info}\n\nSend valid authorized .session file.")
            if os.path.exists(file_path):
                os.remove(file_path)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        if os.path.exists(file_path):
            os.remove(file_path)

async def list_sessions(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    load_sessions()
    
    if not sessions_queue:
        await update.message.reply_text("📭 No sessions found. Use /add to add session files.")
        return
    
    try:
        c.execute("SELECT session_name, phone_number, created_at, is_active FROM sessions WHERE is_active = 1")
        sessions_data = c.fetchall()
    except:
        sessions_data = []
    
    if not sessions_data:
        await update.message.reply_text("No active sessions.")
        return
    
    for session_name, phone, created_at, is_active in sessions_data:
        status = "🟢 ACTIVE" if is_active else "🔴 INACTIVE"
        await update.message.reply_text(
            f"<b>Session Details</b>\n\n"
            f"📱 Phone: {phone}\n"
            f"📁 File: {session_name}\n"
            f"📅 Created: {created_at[:19]}\n"
            f"Status: {status}",
            parse_mode="HTML"
        )
    
    await update.message.reply_text(f"<b>Total Active Sessions:</b> {len(sessions_queue)}\n<b>Load Balancing:</b> Active", parse_mode="HTML")

async def remove_session(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /rm <session_name>")
        return
    
    session_name = context.args[0]
    
    try:
        c.execute("DELETE FROM sessions WHERE session_name = ?", (session_name,))
        conn.commit()
        
        if session_name in sessions_queue:
            sessions_queue.remove(session_name)
        
        session_path = os.path.join(SESSION_DIR, session_name)
        if os.path.exists(session_path):
            os.remove(session_path)
        
        await update.message.reply_text(f"✅ Session {session_name} removed.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")

# ==================== OWNER COMMANDS ====================

async def ban_user(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id> [reason]")
        return
    
    try:
        user_id = int(context.args[0])
        reason = ' '.join(context.args[1:]) if len(context.args) > 1 else "No reason"
        
        banned_until = datetime.now() + timedelta(days=36500)
        c.execute("INSERT OR REPLACE INTO banned_users (user_id, banned_until, reason) VALUES (?, ?, ?)",
                  (user_id, banned_until.isoformat(), reason))
        conn.commit()
        
        await update.message.reply_text(f"✅ User {user_id} permanently banned.\nReason: {reason}")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def unban_user(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
        c.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
        conn.commit()
        await update.message.reply_text(f"✅ User {user_id} unbanned.")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def tban_user(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /tban <user_id> <minutes> [reason]")
        return
    
    try:
        user_id = int(context.args[0])
        minutes = int(context.args[1])
        reason = ' '.join(context.args[2:]) if len(context.args) > 2 else "No reason"
        
        banned_until = datetime.now() + timedelta(minutes=minutes)
        c.execute("INSERT OR REPLACE INTO banned_users (user_id, banned_until, reason) VALUES (?, ?, ?)",
                  (user_id, banned_until.isoformat(), reason))
        conn.commit()
        
        await update.message.reply_text(f"✅ User {user_id} banned for {minutes} minutes.\nReason: {reason}\nUntil: {banned_until.strftime('%Y-%m-%d %H:%M:%S')}")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID or minutes.")

async def reset_user(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /reset <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
        reset_user_checks(user_id)
        await update.message.reply_text(f"✅ User {user_id} limit reset.")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def increase_limit(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /increase <user_id> <limit>")
        return
    
    try:
        user_id = int(context.args[0])
        limit = int(context.args[1])
        set_user_limit(user_id, limit)
        await update.message.reply_text(f"✅ User {user_id} limit set to {limit} checks per hour.")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID or limit.")

async def set_delay_cmd(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if not context.args:
        current = get_delay()
        await update.message.reply_text(f"⏱️ Current delay: {current} seconds\nUsage: /delay <seconds>")
        return
    
    try:
        seconds = int(context.args[0])
        if seconds < 0:
            await update.message.reply_text("❌ Delay cannot be negative.")
            return
        set_delay(seconds)
        await update.message.reply_text(f"✅ Delay set to {seconds} seconds.")
    except ValueError:
        await update.message.reply_text("❌ Provide valid number.")

async def set_approved_channel_cmd(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if not context.args:
        current = get_approved_channel()
        await update.message.reply_text(f"📢 Current approved channel: {current or 'Not set'}\nUsage: /setchannel <channel_id or @username>")
        return
    
    channel = context.args[0]
    set_approved_channel(channel)
    await update.message.reply_text(f"✅ Approved cards will be sent to: {channel}")

async def broadcast(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    if not context.args and not update.message.reply_to_message:
        await update.message.reply_text("Usage: /broadcast <message> or reply to a message")
        return
    
    if update.message.reply_to_message:
        msg = update.message.reply_to_message.text
    else:
        msg = ' '.join(context.args)
    
    if not msg:
        await update.message.reply_text("❌ Message cannot be empty.")
        return
    
    try:
        c.execute("SELECT DISTINCT user_id FROM user_stats")
        users = c.fetchall()
    except:
        users = []
    
    success = 0
    fail = 0
    
    status_msg = await update.message.reply_text(f"📢 Sending to {len(users)} users...")
    
    for user in users:
        try:
            await context.bot.send_message(user[0], f"📢 Broadcast from Owner:\n\n{msg}")
            success += 1
        except:
            fail += 1
        await asyncio.sleep(0.05)
    
    await status_msg.edit_text(f"✅ Broadcast done!\nSent: {success}\nFailed: {fail}")

async def stats(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    total_users, total_checks = get_all_stats()
    active_sessions = len(sessions_queue)
    current_delay = get_delay()
    
    stats_text = (
        "<b>📊 Bot Statistics</b>\n\n"
        f"👥 Total Users: {total_users}\n"
        f"🔍 Total Checks: {total_checks}\n"
        f"📁 Active Sessions: {active_sessions}\n"
        f"⏱️ Check Delay: {current_delay} seconds\n"
        f"📦 Max Mass Limit: {MAX_MASS_LIMIT} cards\n"
        f"⏰ Mass Cooldown: {MASS_COOLDOWN} seconds\n"
        "🟢 Bot Status: Online"
    )
    
    await update.message.reply_text(stats_text, parse_mode="HTML")

async def owner_menu(update: Update, context):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Only owner can use this command.")
        return
    
    commands = (
        "<b>👑 Owner Commands</b>\n\n"
        "<b>📁 Session Management</b>\n"
        "/add - Add session file\n"
        "/sessions - View all sessions\n"
        "/rm <name> - Remove session\n\n"
        "<b>⚙️ Bot Settings</b>\n"
        "/delay <sec> - Set delay\n"
        "/reset <user_id> - Reset user limit\n"
        "/increase <user_id> <limit> - Set user limit\n"
        "/setchannel <channel> - Set approved channel\n\n"
        "<b>🔨 User Management</b>\n"
        "/ban <user_id> [reason] - Permanent ban\n"
        "/unban <user_id> - Unban user\n"
        "/tban <user_id> <minutes> [reason] - Temp ban\n\n"
        "<b>📢 Broadcast</b>\n"
        "/broadcast <msg> - Broadcast message\n\n"
        "<b>📊 Data & Stats</b>\n"
        "/userdata - Export user data\n"
        "/hitsfile - Export approved cards\n"
        "/stats - Show statistics"
    )
    
    await update.message.reply_text(commands, parse_mode="HTML")

# ==================== CALLBACK HANDLERS ====================

async def button_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    if query.data == "profile":
        user = update.effective_user
        username = user.username if user.username else 'No username'
        single_checks = get_single_check_count(user.id)
        mass_checks = get_mass_check_count(user.id)
        user_limit = get_user_limit(user.id)
        
        keyboard = [[InlineKeyboardButton("Back", callback_data="back_to_cmds")]]
        
        await query.edit_message_text(
            f"<b>User ID</b> • <code>{user.id}</code>\n"
            f"<b>Name</b> • {user.full_name}\n"
            f"<b>Username</b> • @{username}\n"
            f"<b>First Name</b> • {user.first_name}\n"
            f"<b>Single Checks Today</b> • {single_checks}/{user_limit}\n"
            f"<b>Mass Checks Today</b> • {mass_checks}/{MAX_MASS_CHECKS_PER_HOUR}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == "rules":
        keyboard = [[InlineKeyboardButton("Back", callback_data="back_to_cmds")]]
        
        await query.edit_message_text(
            "<b>Rules:</b>\n\n<b>1.</b> Attempting more than 5 transactions with the same card within a minute will lead to a ban.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == "back_to_cmds":
        keyboard = [
            [InlineKeyboardButton("Profile", callback_data="profile")],
            [InlineKeyboardButton("Rules", callback_data="rules")]
        ]
        
        await query.edit_message_text(
            "<b>Auth</b>\n• /chk - Stripe Auth\n\n<b>Charge</b>\n• /ss - Stripe+Square $0.001(trail)\n\n<b>Mass</b>\n• /mchk - Stripe Auth Mass\n\n<b>Other</b>\n• /bin - BIN/IIN Check\n• /rules - Check Bot Rules\n• /info - Your Profile",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ==================== MESSAGE HANDLERS ====================

async def unknown(update: Update, context):
    await update.message.reply_text("❌ Unknown command. Use /cmds to see available commands.")

async def handle_message(update: Update, context):
    await handle_session_file(update, context)

# ==================== MAIN ====================

def main():
    load_sessions()
    
    # Create application
    application = Application.builder().token(TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("profile", profile))
    application.add_handler(CommandHandler("cmds", cmds))
    application.add_handler(CommandHandler("rules", rules))
    application.add_handler(CommandHandler("info", info))
    application.add_handler(CommandHandler("chk", handle_chk_command))
    application.add_handler(CommandHandler("ss", handle_ss_command))
    application.add_handler(CommandHandler("mchk", handle_mchk_command))
    application.add_handler(CommandHandler("bin", handle_bin_command))
    
    # Owner handlers
    application.add_handler(CommandHandler("ownermode", owner_menu))
    application.add_handler(CommandHandler("add", add_session))
    application.add_handler(CommandHandler("sessions", list_sessions))
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
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("setchannel", set_approved_channel_cmd))
    
    # Callbacks and messages
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_message))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.COMMAND, unknown))
    
    print("🤖 Bot is starting...")
    print(f"👑 Owner ID: {OWNER_ID}")
    print(f"📁 Sessions loaded: {len(sessions_queue)}")
    
    # Start bot
    application.run_polling()

if __name__ == "__main__":
    main()
