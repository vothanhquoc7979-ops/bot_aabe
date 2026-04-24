import os
import random
import sqlite3
import asyncio
import logging
import json
import re
import secrets
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from contextlib import contextmanager

import requests
import aiohttp
from fastapi import FastAPI, Request, HTTPException, Depends, Query, Response, Cookie
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.enums import ParseMode
import threading

# ================= BROADCAST ENGINE =================
BROADCAST_CONCURRENCY = 30  # Telegram limit
BROADCAST_CHUNK_SIZE = 500
BROADCAST_PROGRESS_INTERVAL = 50
BROADCAST_MAX_RETRIES = 1   # Chỉ retry 1 lần thay vì 3 - nhanh hơn

class BroadcastStats:
    """Thread-safe statistics tracker for broadcast"""
    def __init__(self):
        self.lock = threading.Lock()
        self.success = 0
        self.failed = 0
        self.blocked = 0
        self.total = 0
        self.processed = 0

    def increment_success(self):
        with self.lock:
            self.success += 1
            self.processed += 1

    def increment_failed(self):
        with self.lock:
            self.failed += 1
            self.processed += 1

    def increment_blocked(self):
        with self.lock:
            self.blocked += 1
            self.processed += 1

    def get_snapshot(self):
        with self.lock:
            return {
                'success': self.success,
                'failed': self.failed,
                'blocked': self.blocked,
                'total': self.total,
                'processed': self.processed
            }

    def reset(self, total: int):
        with self.lock:
            self.success = 0
            self.failed = 0
            self.blocked = 0
            self.total = total
            self.processed = 0

async def _send_single_message_fast(
    session: aiohttp.ClientSession,
    user_id: int,
    text: str,
    retries: int = BROADCAST_MAX_RETRIES
) -> tuple[bool, str]:
    """
    Gửi message cực nhanh - KHÔNG log, KHÔNG delay, KHÔNG retry delay.
    """
    api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": user_id, "text": text, "parse_mode": "HTML"}

    for attempt in range(retries + 1):
        try:
            async with session.post(api_url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    return (False, "")  # Success

                data = await resp.json()
                error_desc = data.get("description", "").lower()

                # Permanent errors - blocked/deactivated = mark as blocked
                if "blocked" in error_desc or "deactivated" in error_desc or "bot was blocked" in error_desc:
                    return (True, error_desc)

                # "chat not found" = user never started bot or deleted account - don't retry, just fail
                if "chat not found" in error_desc:
                    return (False, "chat_not_found")

                # Retry immediately for other errors (no sleep)

        except (asyncio.TimeoutError, aiohttp.ClientError):
            pass  # Retry immediately, no delay

    return (False, "failed")

async def run_broadcast(
    users: list,
    message: str,
    progress_callback=None,
    admin_chat_id: int = None
) -> dict:
    """
    Broadcast engine tối ưu tốc độ cao nhất:
    - Concurrent 30 connections (Telegram limit)
    - Chunk 500 users/batch (memory-safe)
    - Retry 1 lần (không delay)
    - Zero blocking trong hot path
    - Progress notification non-blocking
    """
    stats = BroadcastStats()
    stats.reset(len(users))

    connector = aiohttp.TCPConnector(
        limit=BROADCAST_CONCURRENCY * 2,
        keepalive_timeout=30
    )

    # Thread-safe queue for blocked users (to mark later in DB)
    import queue
    blocked_queue = queue.Queue()

    async def send_one(session: aiohttp.ClientSession, user: dict) -> None:
        """Send to one user via aiohttp, update stats"""
        is_blocked, error = await _send_single_message_fast(session, user['user_id'], message)

        if is_blocked:
            stats.increment_blocked()
            blocked_queue.put(user['user_id'])  # Queue for later DB update
        elif error:
            stats.increment_failed()
        else:
            stats.increment_success()

    async with aiohttp.ClientSession(connector=connector) as session:
        total_chunks = (len(users) + BROADCAST_CHUNK_SIZE - 1) // BROADCAST_CHUNK_SIZE

        for chunk_idx in range(total_chunks):
            start = chunk_idx * BROADCAST_CHUNK_SIZE
            end = min(start + BROADCAST_CHUNK_SIZE, len(users))
            chunk = users[start:end]

            # Process chunk - ALL tasks run concurrently
            tasks = [send_one(session, u) for u in chunk]
            await asyncio.gather(*tasks)

    # Get final stats
    snap = stats.get_snapshot()

    # Send final result directly to admin (non-blocking)
    if admin_chat_id:
        pct = snap['processed'] * 100 // snap['total'] if snap['total'] else 0
        msg = f"✅ <b>BROADCAST HOÀN TẤT</b>\n\n📤 Tổng: {snap['total']}\n✅ Thành công: {snap['success']}\n🚫 Blocked: {snap['blocked']}\n❌ Thất bại: {snap['failed']}"
        asyncio.create_task(_send_admin_notification(session, admin_chat_id, msg))

    # Mark blocked users in DB (outside hot path)
    while not blocked_queue.empty():
        try:
            uid = blocked_queue.get_nowait()
            mark_user_blocked(uid)
        except Exception:
            pass

    return snap

    return stats.get_snapshot()

async def _send_admin_notification(session: aiohttp.ClientSession, chat_id: int, text: str) -> None:
    """Non-blocking admin notification"""
    try:
        api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        async with session.post(api_url, json=payload, timeout=aiohttp.ClientTimeout(total=3)) as resp:
            pass
    except Exception:
        pass


# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================= TIMEZONE =================
# Múi giờ Việt Nam (UTC+7)
VIETNAM_TZ = timezone(timedelta(hours=7))

def get_vietnam_time():
    """Lấy thời gian hiện tại theo múi giờ Việt Nam"""
    return datetime.now(VIETNAM_TZ)

def to_vietnam_time(dt: datetime):
    """Chuyển đổi datetime sang múi giờ Việt Nam"""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        # Naive datetime - giả định là UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(VIETNAM_TZ)

# ================= CONFIG =================
# BOT_TOKEN: try env first, DB fallback after init_db()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
SESSION_SECRET = os.getenv("SESSION_SECRET", secrets.token_hex(32))

# Session storage (in-memory, sẽ reset khi restart)
active_sessions = {}

DEFAULT_BANK_ID = os.getenv("BANK_ID", "970418")
DEFAULT_BANK_ACCOUNT = os.getenv("BANK_ACCOUNT", "")
DEFAULT_BANK_NAME = os.getenv("BANK_NAME", "NGUYEN VAN A")

SEPAY_API_KEY = os.getenv("SEPAY_API_KEY", "")
CASSO_API_KEY = os.getenv("CASSO_API_KEY", "")

DB_PATH = os.getenv("DB_PATH", "data.db")

# Thời gian hủy đơn tự động (phút)
ORDER_TIMEOUT_MINUTES = 10

# Auto check state
auto_check_active = False
auto_check_task = None
order_timeout_task = None

# ================= DATABASE =================
def _ensure_contact_seller_column(db):
    """Thêm cột contact_seller vào bảng products nếu DB cũ chưa có (khi upload database cũ)."""
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(products)")
        cols = [row[1] for row in cur.fetchall()]
        if cols and 'contact_seller' not in cols:
            cur.execute("ALTER TABLE products ADD COLUMN contact_seller INTEGER DEFAULT 0")
            db.commit()
            logger.info("✅ Migration: added contact_seller column to products table")
    except Exception as e:
        logger.warning("Migration contact_seller: %s", e)

def _ensure_orders_customer_name_column(db):
    """Thêm cột customer_name (tên thật khách hàng) vào bảng orders nếu DB cũ chưa có."""
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(orders)")
        cols = [row[1] for row in cur.fetchall()]
        if cols and 'customer_name' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN customer_name TEXT")
            db.commit()
            logger.info("✅ Migration: added customer_name column to orders table")
    except Exception as e:
        logger.warning("Migration customer_name: %s", e)

@contextmanager
def get_db():
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    _ensure_contact_seller_column(db)
    _ensure_orders_customer_name_column(db)
    try:
        yield db
    finally:
        db.close()

def run_migrations():
    """Chạy migration để thêm các cột mới vào database cũ"""
    with get_db() as db:
        cur = db.cursor()

        # Kiểm tra và lấy danh sách các bảng hiện có
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cur.fetchall()]

        # Migration cho categories
        if 'categories' in existing_tables:
            try:
                cur.execute("ALTER TABLE categories ADD COLUMN sort_order INTEGER DEFAULT 0")
                logger.info("✅ Migration: added sort_order column to categories table")
            except Exception:
                pass

        # Migration cho products
        if 'products' in existing_tables:
            try:
                cur.execute("ALTER TABLE products ADD COLUMN contact_seller INTEGER DEFAULT 0")
                logger.info("✅ Migration: added contact_seller column to products table")
            except Exception:
                pass

            try:
                cur.execute("ALTER TABLE products ADD COLUMN sort_order INTEGER DEFAULT 0")
                logger.info("✅ Migration: added sort_order column to products table")
            except Exception:
                pass

        # Migration cho orders
        if 'orders' in existing_tables:
            try:
                cur.execute("ALTER TABLE orders ADD COLUMN message_id INTEGER")
                logger.info("✅ Migration: added message_id column to orders table")
            except Exception:
                pass

            try:
                cur.execute("ALTER TABLE orders ADD COLUMN customer_name TEXT")
                logger.info("✅ Migration: added customer_name column to orders table")
            except Exception:
                pass

            try:
                cur.execute("ALTER TABLE orders ADD COLUMN quantity INTEGER DEFAULT 1")
                logger.info("✅ Migration: added quantity column to orders table")
            except Exception:
                pass

        db.commit()

def init_db():
    # Chạy migration trước
    run_migrations()

    with get_db() as db:
        cur = db.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        # Bảng lưu TẤT CẢ users đã /start bot
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            is_blocked INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            icon TEXT DEFAULT '📦',
            is_active INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER,
            name TEXT NOT NULL,
            description TEXT,
            price INTEGER NOT NULL,
            is_active INTEGER DEFAULT 1,
            contact_seller INTEGER DEFAULT 0,
            sort_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            customer_name TEXT,
            order_code TEXT UNIQUE NOT NULL,
            product_id INTEGER,
            product_name TEXT,
            amount INTEGER NOT NULL,
            quantity INTEGER DEFAULT 1,
            status TEXT DEFAULT 'PENDING',
            message_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            paid_at TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
        """)
        
        # Thêm cột message_id nếu chưa có (cho DB cũ)
        try:
            cur.execute("ALTER TABLE orders ADD COLUMN message_id INTEGER")
            logger.info("✅ Added message_id column to orders table")
        except Exception as e:
            # Column already exists - this is fine
            pass
        
        # Verify message_id column exists
        cur.execute("PRAGMA table_info(orders)")
        columns = [col[1] for col in cur.fetchall()]
        if 'message_id' not in columns:
            logger.error("❌ message_id column NOT FOUND in orders table!")
        else:
            logger.info("✅ message_id column verified in orders table")
        
        # Thêm cột quantity nếu chưa có (cho DB cũ)
        try:
            cur.execute("ALTER TABLE orders ADD COLUMN quantity INTEGER DEFAULT 1")
        except:
            pass
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            content TEXT NOT NULL,
            is_sold INTEGER DEFAULT 0,
            sold_to INTEGER,
            order_code TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sold_at TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT UNIQUE,
            amount INTEGER,
            content TEXT,
            bank_account TEXT,
            transaction_date TEXT,
            processed INTEGER DEFAULT 0,
            order_code TEXT,
            raw_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        default_configs = [
            ('bank_id', DEFAULT_BANK_ID),
            ('bank_account', DEFAULT_BANK_ACCOUNT),
            ('bank_name', DEFAULT_BANK_NAME),
            ('sepay_api_key', SEPAY_API_KEY),
            ('casso_api_key', CASSO_API_KEY),
            ('auto_check_interval', '10'),
            ('auto_check_active', '1'),
            ('auto_check_method', 'api'),
        ]
        
        for key, value in default_configs:
            cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (key, value))
        
        db.commit()
        logger.info("Database initialized")

init_db()

# Load BOT_TOKEN and ADMIN_IDS from DB if not set via environment
def _load_bot_config_from_db():
    """Nạp BOT_TOKEN và ADMIN_IDS từ DB nếu chưa có trong env"""
    global BOT_TOKEN, ADMIN_IDS
    _db_token = get_config("bot_token", "")
    if not BOT_TOKEN and _db_token:
        BOT_TOKEN = _db_token
        logger.info("✅ Loaded BOT_TOKEN from database config")
    _db_admin_ids = get_config("admin_ids_list", "")
    if _db_admin_ids:
        for _id_str in _db_admin_ids.split(","):
            _id_str = _id_str.strip()
            if _id_str.isdigit():
                _aid = int(_id_str)
                if _aid not in ADMIN_IDS:
                    ADMIN_IDS.append(_aid)
        if _db_admin_ids:
            logger.info(f"✅ Loaded ADMIN_IDS from database config: {ADMIN_IDS}")

_load_bot_config_from_db()

# ================= AUTH FUNCTIONS =================
def generate_session_token():
    """Tạo session token ngẫu nhiên"""
    return secrets.token_hex(32)

def hash_password(password: str) -> str:
    """Hash password"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password: str) -> bool:
    """Kiểm tra password - check config first, then env"""
    saved_password = get_config("admin_password", "")
    if saved_password:
        return password == saved_password
    return password == ADMIN_PASSWORD

def create_session(username: str) -> str:
    """Tạo session mới"""
    token = generate_session_token()
    active_sessions[token] = {
        "username": username,
        "created_at": datetime.now(),
        "expires_at": datetime.now() + timedelta(hours=24)
    }
    return token

def verify_session(token: str) -> bool:
    """Kiểm tra session có hợp lệ không"""
    if not token or token not in active_sessions:
        return False
    
    session = active_sessions[token]
    if datetime.now() > session["expires_at"]:
        # Session hết hạn
        del active_sessions[token]
        return False
    
    return True

def get_session_user(token: str) -> Optional[str]:
    """Lấy username từ session"""
    if verify_session(token):
        return active_sessions[token]["username"]
    return None

def delete_session(token: str):
    """Xóa session (logout)"""
    if token in active_sessions:
        del active_sessions[token]

async def check_admin_auth(request: Request) -> bool:
    """Middleware kiểm tra authentication"""
    token = request.cookies.get("admin_session")
    return verify_session(token)

# ================= CONFIG FUNCTIONS =================
def get_config(key: str, default: str = "") -> str:
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT value FROM config WHERE key=?", (key,))
        row = cur.fetchone()
        return row['value'] if row else default

def set_config(key: str, value: str):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, value))
        db.commit()

# ================= CATEGORY FUNCTIONS =================
def ensure_columns():
    """Đảm bảo các cột cần thiết tồn tại trong database"""
    with get_db() as db:
        cur = db.cursor()
        try:
            cur.execute("ALTER TABLE categories ADD COLUMN sort_order INTEGER DEFAULT 0")
        except:
            pass
        try:
            cur.execute("ALTER TABLE products ADD COLUMN contact_seller INTEGER DEFAULT 0")
        except:
            pass
        try:
            cur.execute("ALTER TABLE products ADD COLUMN sort_order INTEGER DEFAULT 0")
        except:
            pass
        try:
            cur.execute("ALTER TABLE orders ADD COLUMN message_id INTEGER")
        except:
            pass
        try:
            cur.execute("ALTER TABLE orders ADD COLUMN customer_name TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE orders ADD COLUMN quantity INTEGER DEFAULT 1")
        except:
            pass
        db.commit()

def auto_generate_sort_order():
    """Tự động gán sort_order khi restore database"""
    with get_db() as db:
        cur = db.cursor()

        # Categories: sort_order = 0,1,2,3...
        cur.execute("SELECT id FROM categories WHERE is_active = 1 ORDER BY id")
        categories = cur.fetchall()
        for idx, cat in enumerate(categories):
            cur.execute("UPDATE categories SET sort_order = ? WHERE id = ?", (idx, cat[0]))

        # Products: mỗi category có sort_order riêng 0,1,2,3...
        cur.execute("SELECT id, category_id FROM products ORDER BY category_id, id")
        products = cur.fetchall()

        current_category = None
        order_in_category = 0
        for product in products:
            product_id, category_id = product
            if category_id != current_category:
                current_category = category_id
                order_in_category = 0
            
            # Chỉ dùng index đơn giản trong category
            cur.execute("UPDATE products SET sort_order = ? WHERE id = ?", (order_in_category, product_id))
            order_in_category += 1

        db.commit()
        logger.info("✅ Auto-generated sort_order")

def get_categories():
    ensure_columns()  # Đảm bảo cột tồn tại
    with get_db() as db:
        cur = db.cursor()
        cur.execute("""
            SELECT c.*,
                   (SELECT COUNT(*) FROM products WHERE category_id = c.id AND is_active = 1) as product_count
            FROM categories c
            WHERE c.is_active = 1
            ORDER BY c.sort_order, c.name
        """)
        return [dict(row) for row in cur.fetchall()]

def get_category(category_id: int):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM categories WHERE id=?", (category_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def create_category(name: str, description: str = "", icon: str = "📦", sort_order: int = 0):
    with get_db() as db:
        cur = db.cursor()
        cur.execute(
            "INSERT INTO categories (name, description, icon, sort_order) VALUES (?, ?, ?, ?)",
            (name, description, icon, sort_order)
        )
        db.commit()
        return cur.lastrowid

def update_category(category_id: int, name: str, description: str, icon: str, sort_order: int = None):
    """Cập nhật category"""
    with get_db() as db:
        cur = db.cursor()
        updates = ["name=?", "description=?", "icon=?"]
        params = [name, description, icon]

        # Lấy sort_order hiện tại
        cur.execute("SELECT sort_order FROM categories WHERE id = ?", (category_id,))
        row = cur.fetchone()
        old_sort_order = row[0] if row else 0

        if sort_order is not None:
            # Khi thay đổi sort_order thủ công, cần tính lại vị trí
            updates.append("sort_order=?")
            params.append(sort_order)

            # Lấy danh sách categories, sắp xếp theo sort_order
            cur.execute("""
                SELECT id, sort_order FROM categories 
                WHERE id != ?
                ORDER BY sort_order, id
            """, (category_id,))
            categories = cur.fetchall()

            # Chèn category này vào đúng vị trí
            new_order_list = []
            inserted = False
            for c in categories:
                if not inserted and (sort_order < c[1] or (sort_order == c[1] and category_id < c[0])):
                    new_order_list.append(category_id)
                    inserted = True
                new_order_list.append(c[0])
            if not inserted:
                new_order_list.append(category_id)

            # Cập nhật lại sort_order cho tất cả
            for idx, cid in enumerate(new_order_list):
                cur.execute("UPDATE categories SET sort_order = ? WHERE id = ?", (idx, cid))

        params.append(category_id)
        cur.execute(
            f"UPDATE categories SET {', '.join(updates)} WHERE id=?",
            params
        )
        db.commit()

def delete_category(category_id: int):
    """Xóa vĩnh viễn danh mục khỏi database"""
    with get_db() as db:
        cur = db.cursor()
        # Xóa các sản phẩm trong danh mục trước
        cur.execute("DELETE FROM stocks WHERE product_id IN (SELECT id FROM products WHERE category_id=?)", (category_id,))
        cur.execute("DELETE FROM products WHERE category_id=?", (category_id,))
        # Xóa danh mục
        cur.execute("DELETE FROM categories WHERE id=?", (category_id,))
        db.commit()

# ================= PRODUCT FUNCTIONS =================
def get_products(category_id: int = None, active_only: bool = True):
    ensure_columns()  # Đảm bảo cột tồn tại
    with get_db() as db:
        cur = db.cursor()
        query = """
            SELECT p.*, c.name as category_name, c.icon as category_icon,
                   (SELECT COUNT(*) FROM stocks WHERE product_id = p.id AND is_sold = 0) as stock_count
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE 1=1
        """
        params = []
        
        if active_only:
            query += " AND p.is_active = 1"
        if category_id:
            query += " AND p.category_id = ?"
            params.append(category_id)
        
        query += " ORDER BY c.sort_order, p.sort_order, c.name, p.name"
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

def get_product(product_id: int):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("""
            SELECT p.*, c.name as category_name,
                   (SELECT COUNT(*) FROM stocks WHERE product_id = p.id AND is_sold = 0) as stock_count
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE p.id = ?
        """, (product_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def create_product(category_id: int, name: str, price: int, description: str = "", contact_seller: bool = False, sort_order: int = 0):
    with get_db() as db:
        cur = db.cursor()
        cur.execute(
            "INSERT INTO products (category_id, name, price, description, contact_seller, sort_order) VALUES (?, ?, ?, ?, ?, ?)",
            (category_id, name, price, description, 1 if contact_seller else 0, sort_order)
        )
        db.commit()
        return cur.lastrowid

def update_product(product_id: int, category_id: int = None, name: str = None, price: int = None, description: str = None, contact_seller: bool = None, sort_order: int = None):
    """Cập nhật thông tin sản phẩm"""
    with get_db() as db:
        cur = db.cursor()
        updates = []
        params = []

        # Lấy category_id hiện tại của sản phẩm
        cur.execute("SELECT category_id, sort_order FROM products WHERE id = ?", (product_id,))
        row = cur.fetchone()
        old_category_id = row[0] if row else None
        old_sort_order = row[1] if row else 0

        new_category_id = category_id if category_id is not None else old_category_id

        if category_id is not None:
            updates.append("category_id=?")
            params.append(category_id)
        if name is not None:
            updates.append("name=?")
            params.append(name)
        if price is not None:
            updates.append("price=?")
            params.append(price)
        if description is not None:
            updates.append("description=?")
            params.append(description)
        if contact_seller is not None:
            updates.append("contact_seller=?")
            params.append(1 if contact_seller else 0)
        if sort_order is not None:
            # Khi thay đổi sort_order thủ công, cần tính lại vị trí
            updates.append("sort_order=?")
            params.append(sort_order)

            # Lấy danh sách sản phẩm trong category mới, sắp xếp theo sort_order
            cur.execute("""
                SELECT id, sort_order FROM products 
                WHERE category_id = ? AND id != ?
                ORDER BY sort_order, id
            """, (new_category_id, product_id))
            products = cur.fetchall()

            # Chèn sản phẩm này vào đúng vị trí
            new_order_list = []
            inserted = False
            for p in products:
                if not inserted and (sort_order < p[1] or (sort_order == p[1] and product_id < p[0])):
                    new_order_list.append(product_id)
                    inserted = True
                new_order_list.append(p[0])
            if not inserted:
                new_order_list.append(product_id)

            # Cập nhật lại sort_order cho tất cả
            for idx, pid in enumerate(new_order_list):
                cur.execute("UPDATE products SET sort_order = ? WHERE id = ?", (idx, pid))

        if updates:
            params.append(product_id)
            query = f"UPDATE products SET {', '.join(updates)} WHERE id=?"
            cur.execute(query, params)
            db.commit()

def delete_product(product_id: int):
    """Xóa vĩnh viễn sản phẩm khỏi database"""
    with get_db() as db:
        cur = db.cursor()
        # Xóa các stock liên quan trước
        cur.execute("DELETE FROM stocks WHERE product_id=?", (product_id,))
        # Xóa sản phẩm
        cur.execute("DELETE FROM products WHERE id=?", (product_id,))
        db.commit()

def toggle_product(product_id: int):
    """Tạm dừng/bật lại sản phẩm"""
    with get_db() as db:
        cur = db.cursor()
        cur.execute("UPDATE products SET is_active = NOT is_active WHERE id=?", (product_id,))
        db.commit()

# ================= STOCK FUNCTIONS =================
def get_stocks(product_id: int = None, is_sold: int = None, limit: int = 100):
    with get_db() as db:
        cur = db.cursor()
        query = """
            SELECT s.*, p.name as product_name
            FROM stocks s
            LEFT JOIN products p ON s.product_id = p.id
            WHERE 1=1
        """
        params = []
        
        if product_id:
            query += " AND s.product_id = ?"
            params.append(product_id)
        if is_sold is not None:
            query += " AND s.is_sold = ?"
            params.append(is_sold)
        
        query += f" ORDER BY s.id DESC LIMIT {limit}"
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

def add_stocks(product_id: int, contents: List[str]):
    with get_db() as db:
        cur = db.cursor()
        cur.executemany(
            "INSERT INTO stocks (product_id, content) VALUES (?, ?)",
            [(product_id, c.strip()) for c in contents if c.strip()]
        )
        db.commit()
        return cur.rowcount

def get_available_stock(product_id: int = None, order_code: str = None, user_id: int = None, quantity: int = 1):
    """Lấy nhiều stock theo số lượng yêu cầu"""
    with get_db() as db:
        cur = db.cursor()
        query = "SELECT id, content FROM stocks WHERE is_sold=0"
        params = []
        if product_id:
            query += " AND product_id=?"
            params.append(product_id)
        query += f" ORDER BY id ASC LIMIT {quantity}"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        if rows:
            stocks = []
            for row in rows:
                # Cập nhật stock với order_code và user_id để track
                cur.execute(
                    "UPDATE stocks SET is_sold=1, sold_at=CURRENT_TIMESTAMP, order_code=?, sold_to=? WHERE id=?",
                    (order_code, user_id, row['id'])
                )
                stocks.append(dict(row))
            db.commit()
            return stocks
        return None

def delete_stock(stock_id: int):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("DELETE FROM stocks WHERE id=? AND is_sold=0", (stock_id,))
        db.commit()
        return cur.rowcount > 0

def get_stock_count(product_id: int = None):
    with get_db() as db:
        cur = db.cursor()
        query = "SELECT COUNT(*) as count FROM stocks WHERE is_sold=0"
        params = []
        if product_id:
            query += " AND product_id=?"
            params.append(product_id)
        cur.execute(query, params)
        return cur.fetchone()['count']

# ================= ORDER FUNCTIONS =================
def create_order(user_id: int, username: str, order_code: str, product_id: int, product_name: str, amount: int, quantity: int = 1, customer_name: str = None):
    """customer_name: tên hiển thị thật (first_name + last_name), dùng cho dòng Khách hàng trong thông báo."""
    with get_db() as db:
        cur = db.cursor()
        cur.execute(
            """INSERT INTO orders (user_id, username, order_code, product_id, product_name, amount, quantity, status, customer_name) 
               VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)""",
            (user_id, username, order_code, product_id, product_name, amount, quantity, customer_name or '')
        )
        db.commit()
        return cur.lastrowid

def get_order(order_code: str):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM orders WHERE order_code=?", (order_code,))
        row = cur.fetchone()
        return dict(row) if row else None

def update_order_message_id(order_code: str, message_id: int):
    """Lưu message_id của tin nhắn thanh toán để có thể xóa sau"""
    logger.info(f"📝 Saving message_id={message_id} for order {order_code}")
    with get_db() as db:
        cur = db.cursor()
        cur.execute("UPDATE orders SET message_id=? WHERE order_code=?", (message_id, order_code))
        rows_affected = cur.rowcount
        db.commit()
        logger.info(f"📝 Updated {rows_affected} rows for order {order_code}")

def get_orders(status: str = None, limit: int = 50):
    with get_db() as db:
        cur = db.cursor()
        query = "SELECT * FROM orders WHERE 1=1"
        params = []
        if status:
            query += " AND status=?"
            params.append(status)
        query += f" ORDER BY created_at DESC LIMIT {limit}"
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

def get_pending_orders():
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM orders WHERE status='PENDING' ORDER BY created_at DESC")
        return [dict(row) for row in cur.fetchall()]

def get_user_pending_order(user_id: int):
    """Lấy đơn hàng đang chờ của user"""
    with get_db() as db:
        cur = db.cursor()
        cur.execute(
            "SELECT * FROM orders WHERE user_id=? AND status='PENDING' ORDER BY created_at DESC LIMIT 1",
            (user_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

def find_order_by_content(content: str, amount: int = None):
    """
    Tìm đơn hàng theo nội dung chuyển khoản - CHỈ MATCH CHÍNH XÁC MÃ ĐƠN
    Đảm bảo gửi đúng người, đúng đơn hàng
    """
    orders = get_pending_orders()
    
    # Chuẩn hóa content: uppercase, bỏ dấu cách thừa
    content_clean = content.upper().replace(" ", "").strip()
    
    logger.info(f"=== FINDING ORDER ===")
    logger.info(f"Content: '{content}'")
    logger.info(f"Content cleaned: '{content_clean}'")
    logger.info(f"Amount: {amount}")
    logger.info(f"Pending orders: {[o['order_code'] for o in orders]}")
    
    # PHẢI tìm thấy mã đơn hàng trong nội dung chuyển khoản
    for order in orders:
        order_code = order['order_code'].upper()
        order_code_clean = order_code.replace(" ", "")
        
        # Check 1: Mã đơn đầy đủ (DH123456)
        if order_code in content_clean or order_code_clean in content_clean:
            logger.info(f"✅ EXACT MATCH: {order_code} found in content")
            
            # Double check: nếu có amount thì phải khớp hoặc lớn hơn
            if amount and amount < order['amount']:
                logger.warning(f"Amount mismatch: got {amount}, need {order['amount']}")
                continue
            
            return order
        
        # Check 2: Chỉ số đơn hàng (123456) - phải có ít nhất 6 số liên tiếp
        order_number = order_code.replace("DH", "")
        if len(order_number) >= 6 and order_number in content_clean:
            logger.info(f"✅ NUMBER MATCH: {order_number} found in content")
            
            if amount and amount < order['amount']:
                logger.warning(f"Amount mismatch: got {amount}, need {order['amount']}")
                continue
            
            return order
    
    # KHÔNG match theo số tiền nữa - phải có mã đơn chính xác
    logger.warning(f"❌ NO MATCH: No order code found in '{content}'")
    return None

def mark_order_paid(order_code: str):
    with get_db() as db:
        cur = db.cursor()
        cur.execute(
            "UPDATE orders SET status='PAID', paid_at=CURRENT_TIMESTAMP WHERE order_code=?",
            (order_code,)
        )
        db.commit()
        return cur.rowcount > 0

def cancel_order(order_code: str):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("UPDATE orders SET status='CANCELLED' WHERE order_code=? AND status='PENDING'", (order_code,))
        db.commit()
        return cur.rowcount > 0

def cancel_expired_orders():
    """Hủy các đơn hàng quá thời gian và trả về danh sách orders đã hủy (kèm message_id để xóa)"""
    with get_db() as db:
        cur = db.cursor()
        timeout_time = datetime.now() - timedelta(minutes=ORDER_TIMEOUT_MINUTES)
        
        # Lấy danh sách orders hết hạn trước khi update
        cur.execute(
            """SELECT order_code, user_id, message_id FROM orders 
               WHERE status='PENDING' AND created_at < ?""",
            (timeout_time.strftime('%Y-%m-%d %H:%M:%S'),)
        )
        expired_orders = [dict(row) for row in cur.fetchall()]
        
        # Update status
        cur.execute(
            """UPDATE orders SET status='TIMEOUT' 
               WHERE status='PENDING' AND created_at < ?""",
            (timeout_time.strftime('%Y-%m-%d %H:%M:%S'),)
        )
        db.commit()
        return expired_orders

def get_user_orders(user_id: int, limit: int = 10):
    with get_db() as db:
        cur = db.cursor()
        cur.execute(
            "SELECT * FROM orders WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        )
        return [dict(row) for row in cur.fetchall()]

# ================= STATS FUNCTIONS =================
def get_stats():
    with get_db() as db:
        cur = db.cursor()
        
        cur.execute("SELECT COUNT(*) as count FROM orders")
        total_orders = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM orders WHERE status='PAID'")
        paid_orders = cur.fetchone()['count']
        
        cur.execute("SELECT COALESCE(SUM(amount), 0) as total FROM orders WHERE status='PAID'")
        total_revenue = cur.fetchone()['total']
        
        cur.execute("SELECT COUNT(*) as count FROM stocks WHERE is_sold=0")
        stock_available = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM stocks WHERE is_sold=1")
        stock_sold = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM categories WHERE is_active=1")
        total_categories = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM products WHERE is_active=1")
        total_products = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(DISTINCT user_id) as count FROM orders WHERE status='PAID'")
        total_customers = cur.fetchone()['count']
        
        # Count all bot users
        cur.execute("SELECT COUNT(*) as count FROM bot_users WHERE is_blocked=0")
        total_users = cur.fetchone()['count']
        
        return {
            'total_orders': total_orders,
            'paid_orders': paid_orders,
            'total_users': total_users,
            'total_revenue': total_revenue,
            'stock_available': stock_available,
            'stock_sold': stock_sold,
            'total_categories': total_categories,
            'total_products': total_products,
            'total_customers': total_customers
        }

# ================= BOT USERS FUNCTIONS =================
def save_bot_user(user_id: int, username: str = None, first_name: str = None, last_name: str = None):
    """Lưu user khi họ /start bot"""
    with get_db() as db:
        cur = db.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO bot_users (user_id, username, first_name, last_name)
            VALUES (?, ?, ?, ?)
        """, (user_id, username, first_name, last_name))
        db.commit()

def get_all_bot_users():
    """Lấy TẤT CẢ users đã /start bot"""
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM bot_users WHERE is_blocked = 0 ORDER BY created_at DESC")
        return [dict(row) for row in cur.fetchall()]

def get_bot_user_count():
    """Đếm số users đã /start bot"""
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) as count FROM bot_users WHERE is_blocked = 0")
        return cur.fetchone()['count']

def mark_user_blocked(user_id: int):
    """Đánh dấu user đã block bot"""
    with get_db() as db:
        cur = db.cursor()
        cur.execute("UPDATE bot_users SET is_blocked = 1 WHERE user_id = ?", (user_id,))
        db.commit()

def get_sales_history(limit: int = 50):
    """Lấy lịch sử bán hàng - user nào mua tài khoản nào"""
    with get_db() as db:
        cur = db.cursor()
        cur.execute("""
            SELECT 
                o.order_code,
                o.user_id,
                o.username,
                o.product_name,
                o.amount,
                o.quantity,
                o.paid_at,
                GROUP_CONCAT(s.content, '|||') as accounts_sent
            FROM orders o
            LEFT JOIN stocks s ON s.order_code = o.order_code
            WHERE o.status = 'PAID'
            GROUP BY o.order_code
            ORDER BY o.paid_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]

# ================= TRANSACTION FUNCTIONS =================
def log_transaction(transaction_id: str, amount: int, content: str, bank_account: str = "", transaction_date: str = "", raw_data: str = ""):
    with get_db() as db:
        cur = db.cursor()
        try:
            cur.execute(
                """INSERT INTO transactions 
                   (transaction_id, amount, content, bank_account, transaction_date, raw_data)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (transaction_id, amount, content, bank_account, transaction_date, raw_data)
            )
            db.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Đã tồn tại

def is_transaction_processed(transaction_id: str):
    with get_db() as db:
        cur = db.cursor()
        cur.execute("SELECT processed FROM transactions WHERE transaction_id=?", (transaction_id,))
        row = cur.fetchone()
        return row['processed'] == 1 if row else False

def mark_transaction_processed(transaction_id: str, order_code: str):
    with get_db() as db:
        cur = db.cursor()
        cur.execute(
            "UPDATE transactions SET processed=1, order_code=? WHERE transaction_id=?",
            (order_code, transaction_id)
        )
        db.commit()

def get_recent_transactions(limit: int = 20):
    with get_db() as db:
        cur = db.cursor()
        cur.execute(
            "SELECT * FROM transactions ORDER BY created_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in cur.fetchall()]

# ================= VIETQR FUNCTIONS =================
def generate_vietqr_url(amount: int, content: str) -> str:
    import urllib.parse
    
    bank_id = get_config('bank_id', DEFAULT_BANK_ID)
    bank_account = get_config('bank_account', DEFAULT_BANK_ACCOUNT)
    bank_name = get_config('bank_name', DEFAULT_BANK_NAME)
    
    account_name = urllib.parse.quote(bank_name)
    add_info = urllib.parse.quote(content)
    
    url = f"https://img.vietqr.io/image/{bank_id}-{bank_account}-compact2.png"
    url += f"?amount={amount}&addInfo={add_info}&accountName={account_name}"
    
    return url

def get_bank_info():
    bank_names = {
        '970418': 'BIDV', '970436': 'Vietcombank', '970407': 'Techcombank',
        '970422': 'MB Bank', '970416': 'ACB', '970432': 'VPBank',
        '970423': 'TPBank', '970403': 'Sacombank', '970405': 'Agribank', 
        '970415': 'VietinBank'
    }
    
    bank_id = get_config('bank_id', DEFAULT_BANK_ID)
    return {
        'bank_id': bank_id,
        'bank_name': bank_names.get(bank_id, bank_id),
        'bank_account': get_config('bank_account', DEFAULT_BANK_ACCOUNT),
        'account_name': get_config('bank_name', DEFAULT_BANK_NAME)
    }

def download_qr_image(url: str) -> bytes:
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.content
    except Exception as e:
        logger.error(f"Error downloading QR: {e}")
    return None

# ================= PAYMENT CHECK FUNCTIONS =================
async def fetch_sepay_transactions():
    """Lấy giao dịch từ SePay API"""
    api_key = get_config('sepay_api_key', SEPAY_API_KEY)
    if not api_key:
        logger.warning("SePay API key not configured")
        return []
    
    try:
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(
            "https://my.sepay.vn/userapi/transactions/list",
            headers=headers,
            params={"limit": 20},
            timeout=15
        )
        
        logger.info(f"SePay API response: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            transactions = data.get('transactions', [])
            logger.info(f"SePay returned {len(transactions)} transactions")
            return transactions
        else:
            logger.error(f"SePay API error: {response.text}")
    except Exception as e:
        logger.error(f"SePay API error: {e}")
    
    return []

async def fetch_casso_transactions():
    """Lấy giao dịch từ Casso API"""
    api_key = get_config('casso_api_key', CASSO_API_KEY)
    if not api_key:
        return []
    
    try:
        headers = {"Authorization": f"Apikey {api_key}"}
        response = requests.get(
            "https://oauth.casso.vn/v2/transactions",
            headers=headers,
            params={"pageSize": 20, "sort": "DESC"},
            timeout=15
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get('data', {}).get('records', [])
    except Exception as e:
        logger.error(f"Casso API error: {e}")
    
    return []

async def process_single_transaction(tx_id: str, amount: int, content: str, raw_data: str = "") -> Optional[str]:
    """
    Xử lý một giao dịch - CHỈ gửi khi tìm thấy MÃ ĐƠN CHÍNH XÁC
    Đảm bảo gửi đúng người, đúng đơn hàng
    """
    
    logger.info(f"=== PROCESSING TRANSACTION ===")
    logger.info(f"TX ID: {tx_id}")
    logger.info(f"Amount: {amount}")
    logger.info(f"Content: '{content}'")
    
    # Kiểm tra đã xử lý chưa
    if is_transaction_processed(tx_id):
        logger.info(f"⏭️ Transaction {tx_id} already processed, skipping")
        return None
    
    # Log transaction vào DB
    log_transaction(tx_id, amount, content, raw_data=raw_data)
    
    # Chỉ xử lý tiền vào > 0
    if amount <= 0:
        logger.info(f"⏭️ Skipping: amount={amount} <= 0")
        return None
    
    # TÌM ĐƠN HÀNG - PHẢI CÓ MÃ ĐƠN CHÍNH XÁC
    order = find_order_by_content(content, amount)
    
    if not order:
        logger.warning(f"❌ KHÔNG TÌM THẤY ĐƠN HÀNG cho nội dung: '{content}'")
        logger.warning(f"   Giao dịch này sẽ được lưu nhưng KHÔNG tự động gửi hàng")
        
        # Thông báo admin có giao dịch không match
        for admin_id in ADMIN_IDS:
            send_telegram_sync(
                admin_id,
                f"⚠️ <b>GIAO DỊCH KHÔNG KHỚP ĐƠN</b>\n\n"
                f"💰 Số tiền: {amount:,}đ\n"
                f"📝 Nội dung: <code>{content}</code>\n\n"
                f"Không tìm thấy mã đơn hàng trong nội dung.\n"
                f"Kiểm tra và xử lý thủ công nếu cần."
            )
        return None
    
    # Kiểm tra trạng thái đơn
    if order['status'] != 'PENDING':
        logger.info(f"⏭️ Order {order['order_code']} status={order['status']}, not PENDING")
        return None
    
    # Kiểm tra số tiền
    if amount < order['amount']:
        logger.warning(f"⚠️ Thiếu tiền: nhận {amount}, cần {order['amount']}")
        send_telegram_sync(
            order['user_id'],
            f"⚠️ <b>SỐ TIỀN KHÔNG ĐỦ!</b>\n\n"
            f"Đơn: <code>{order['order_code']}</code>\n"
            f"Cần: {order['amount']:,}đ\n"
            f"Nhận: {amount:,}đ\n\n"
            f"Vui lòng chuyển thêm <b>{order['amount'] - amount:,}đ</b> với cùng nội dung."
        )
        return None
    
    # ✅ THÀNH CÔNG - Xử lý đơn hàng
    logger.info(f"✅ MATCH FOUND!")
    logger.info(f"   Order: {order['order_code']}")
    logger.info(f"   User ID: {order['user_id']}")
    logger.info(f"   Username: {order.get('username', 'N/A')}")
    logger.info(f"   Product: {order.get('product_name', 'N/A')}")
    logger.info(f"   Amount: {order['amount']} (received: {amount})")
    
    # Đánh dấu đã thanh toán
    mark_order_paid(order['order_code'])
    mark_transaction_processed(tx_id, order['order_code'])
    
    # Lấy số lượng từ order
    quantity = order.get('quantity', 1) or 1
    
    # Lấy hàng từ kho (theo product_id và quantity) - lưu order_code và user_id để track
    stocks = get_available_stock(order.get('product_id'), order['order_code'], order['user_id'], quantity)
    
    if stocks and len(stocks) > 0:
        # GỬI SẢN PHẨM CHO ĐÚNG NGƯỜI TẠO ĐƠN
        logger.info(f"📦 Sending {len(stocks)} products to user {order['user_id']}")
        
        # Format thời gian cho người mua
        created_at_str = order.get('created_at', '')
        if created_at_str:
            try:
                if isinstance(created_at_str, str):
                    dt = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
                else:
                    dt = created_at_str
                dt_vn = to_vietnam_time(dt)
                created_at_display = dt_vn.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                created_at_display = str(created_at_str)
        else:
            created_at_display = 'N/A'
        
        quantity = order.get('quantity', 1) or 1
        unit_price = amount // quantity if quantity > 0 else amount
        customer_display = (order.get('customer_name') or order.get('username') or 'N/A').strip()
        username_display = (order.get('username') or 'N/A').strip()
        if username_display and not username_display.startswith('@'):
            username_display = '@' + username_display
        
        # Kiểm tra nếu cần gửi file TXT: từ 2 sản phẩm trở lên HOẶC content dài
        need_txt_file = len(stocks) >= 2
        if not need_txt_file:
            for s in stocks:
                content = s.get('content', '')
                if len(content) > 150 or content.count('\n') >= 5:
                    need_txt_file = True
                    break
        
        if need_txt_file:
            # Tạo nội dung file
            file_content = f"ĐƠN HÀNG: {order['order_code']}\n"
            file_content += f"Sản phẩm: {order.get('product_name', 'N/A')}\n"
            file_content += f"Số lượng: {len(stocks)}\n"
            file_content += f"Số tiền: {amount:,}đ\n"
            file_content += f"{'='*40}\n\n"
            for i, s in enumerate(stocks):
                file_content += f"{i+1}. {s['content']}\n"
            
            # Gửi file qua Telegram API
            try:
                import io
                file_bytes = file_content.encode('utf-8')
                filename = f"{order['order_code']}_products.txt"
                
                buyer_caption = (
                    f"🧾 <b>THANH TOÁN THÀNH CÔNG</b>\n\n"
                    f"🆔 Order ID: {order['order_code']}\n"
                    f"👤 Khách hàng: {customer_display}\n"
                    f"   └ Username: {username_display}\n"
                    f"   └ User ID: {order['user_id']}\n\n"
                    f"📦 Gói: {order.get('product_name', 'N/A')}\n"
                    f"🔢 Số lượng: {len(stocks)}\n"
                    f"💵 Đơn giá: {unit_price:,}đ\n"
                    f"💰 Tổng thanh toán: {amount:,}đ\n"
                    f"🕒 Thời gian tạo đơn: {created_at_display}\n\n"
                    f"🧾 <b>SẢN PHẨM:</b> (trong file đính kèm)\n\n"
                    f"Cảm ơn bạn đã mua hàng! 🙏"
                )
                
                response = requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                    data={
                        "chat_id": order['user_id'],
                        "caption": buyer_caption,
                        "parse_mode": "HTML"
                    },
                    files={"document": (filename, io.BytesIO(file_bytes), "text/plain")}
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to send file: {response.text}")
                    raise Exception(f"Telegram API error: {response.status_code}")
                    
                logger.info(f"✅ Sent {len(stocks)} products as TXT file to {order['user_id']}")
                
            except Exception as e:
                logger.error(f"Error sending file: {e}")
                products_text = "\n".join([f"{i+1}. {s['content']}" for i, s in enumerate(stocks)])
                send_telegram_sync(
                    order['user_id'],
                    f"🧾 <b>THANH TOÁN THÀNH CÔNG</b>\n\n"
                    f"🆔 Order ID: {order['order_code']}\n"
                    f"👤 Khách hàng: {customer_display}\n"
                    f"   └ Username: {username_display}\n"
                    f"   └ User ID: {order['user_id']}\n\n"
                    f"📦 Gói: {order.get('product_name', 'N/A')}\n"
                    f"🔢 Số lượng: {len(stocks)}\n"
                    f"💵 Đơn giá: {unit_price:,}đ\n"
                    f"💰 Tổng thanh toán: {amount:,}đ\n"
                    f"🕒 Thời gian tạo đơn: {created_at_display}\n\n"
                    f"🧾 <b>SẢN PHẨM:</b>\n{products_text}\n\nCảm ơn bạn đã mua hàng! 🙏"
                )
        else:
            # 1 sản phẩm: gửi text kèm nội dung sản phẩm
            products_text = "\n".join([s['content'] for s in stocks])
            send_telegram_sync(
                order['user_id'],
                f"🧾 <b>THANH TOÁN THÀNH CÔNG</b>\n\n"
                f"🆔 Order ID: {order['order_code']}\n"
                f"👤 Khách hàng: {customer_display}\n"
                f"   └ Username: {username_display}\n"
                f"   └ User ID: {order['user_id']}\n\n"
                f"📦 Gói: {order.get('product_name', 'N/A')}\n"
                f"🔢 Số lượng: {len(stocks)}\n"
                f"💵 Đơn giá: {unit_price:,}đ\n"
                f"💰 Tổng thanh toán: {amount:,}đ\n"
                f"🕒 Thời gian tạo đơn: {created_at_display}\n\n"
                f"🧾 <b>SẢN PHẨM:</b>\n{products_text}\n\n"
                f"Cảm ơn bạn đã mua hàng! 🙏"
            )
        
        # Thông báo cho người bán (admin)
        created_at_admin = order.get('created_at', '')
        if created_at_admin:
            try:
                if isinstance(created_at_admin, str):
                    dt = datetime.strptime(created_at_admin, '%Y-%m-%d %H:%M:%S')
                else:
                    dt = created_at_admin
                dt_vn = to_vietnam_time(dt)
                created_at_admin = dt_vn.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                created_at_admin = str(created_at_admin)
        else:
            created_at_admin = 'N/A'
        kho_con = get_stock_count(order.get('product_id'))
        customer_admin = (order.get('customer_name') or order.get('username') or 'N/A').strip()
        username_admin = (order.get('username') or 'N/A').strip()
        if username_admin and not username_admin.startswith('@'):
            username_admin = '@' + username_admin
        for admin_id in ADMIN_IDS:
            send_telegram_sync(
                admin_id,
                f"💰 <b>ĐƠN HÀNG THÀNH CÔNG</b>\n\n"
                f"🆔 Order ID: {order['order_code']}\n"
                f"👤 Khách hàng: {customer_admin}\n"
                f"   └ Username: {username_admin}\n"
                f"   └ User ID: {order['user_id']}\n"
                f"🎁 Gói: {order.get('product_name', 'N/A')}\n"
                f"🔢 Số lượng mua: {len(stocks)}\n"
                f"💰 Tổng thanh toán: {amount:,}đ\n"
                f"📦 Kho còn: {kho_con}\n"
                f"🕒 Thời gian tạo đơn: {created_at_admin}"
            )
        
        logger.info(f"✅ Order {order['order_code']} completed - sent {len(stocks)} products to user {order['user_id']}")
        
        # Xóa tin nhắn thanh toán (QR code) sau khi gửi sản phẩm thành công
        order_code = order['order_code']
        
        # Thử lấy từ memory trước (reliable hơn)
        payment_msg = order_payment_messages.get(order_code)
        if payment_msg:
            logger.info(f"📱 Found payment message in memory: {payment_msg}")
            result = delete_telegram_message_sync(payment_msg['chat_id'], payment_msg['message_id'])
            logger.info(f"📱 Delete message result: {result}")
            # Cleanup
            del order_payment_messages[order_code]
        elif order.get('message_id'):
            # Fallback to DB
            logger.info(f"📱 Using message_id from DB: {order.get('message_id')}")
            result = delete_telegram_message_sync(order['user_id'], order['message_id'])
            logger.info(f"📱 Delete message result: {result}")
        else:
            logger.warning(f"📱 No message_id found for order {order_code}")
        
        return order['order_code']
    else:
        # Hết hàng hoặc không đủ số lượng
        logger.warning(f"❌ OUT OF STOCK for order {order['order_code']} (need {quantity})")
        
        send_telegram_sync(
            order['user_id'],
            f"⚠️ <b>XIN LỖI - HẾT HÀNG!</b>\n\n"
            f"Đơn <code>{order['order_code']}</code> đã thanh toán thành công.\n"
            f"Tuy nhiên sản phẩm đã hết hàng.\n\n"
            f"Admin sẽ liên hệ hoàn tiền hoặc giao hàng sau.\n"
            f"Xin lỗi vì sự bất tiện! 🙏"
        )
        
        for admin_id in ADMIN_IDS:
            send_telegram_sync(
                admin_id, 
                f"🚨 <b>HẾT HÀNG!</b>\n\n"
                f"Đơn: <code>{order['order_code']}</code>\n"
                f"User: {order['user_id']} (@{order.get('username', 'N/A')})\n"
                f"Sản phẩm: {order.get('product_name', 'N/A')}\n"
                f"Tiền: {amount:,}đ\n\n"
                f"⚠️ Cần xử lý thủ công!"
            )
        
        return order['order_code']

async def check_all_transactions():
    """Kiểm tra tất cả giao dịch từ SePay và Casso"""
    processed_count = 0
    
    # Check SePay
    try:
        transactions = await fetch_sepay_transactions()
        for tx in transactions:
            tx_id = str(tx.get('id', tx.get('referenceCode', '')))
            amount = int(tx.get('transferAmount', 0))
            content = tx.get('content', '') or tx.get('description', '') or ''
            transfer_type = tx.get('transferType', 'in')
            
            # Chỉ xử lý tiền vào
            if transfer_type.lower() == 'in' and amount > 0:
                result = await process_single_transaction(tx_id, amount, content, json.dumps(tx))
                if result:
                    processed_count += 1
    except Exception as e:
        logger.error(f"Error checking SePay: {e}")
    
    # Check Casso
    try:
        transactions = await fetch_casso_transactions()
        for tx in transactions:
            tx_id = str(tx.get('id', ''))
            amount = int(tx.get('amount', 0))
            content = tx.get('description', '') or ''
            
            if amount > 0:
                result = await process_single_transaction(f"casso_{tx_id}", amount, content, json.dumps(tx))
                if result:
                    processed_count += 1
    except Exception as e:
        logger.error(f"Error checking Casso: {e}")
    
    return processed_count

async def check_payment_for_order(order_code: str) -> dict:
    """Kiểm tra thanh toán cho một đơn hàng cụ thể"""
    order = get_order(order_code)
    if not order:
        return {"success": False, "message": "Đơn hàng không tồn tại"}
    
    if order['status'] == 'PAID':
        return {"success": True, "message": "Đơn hàng đã được thanh toán", "paid": True}
    
    if order['status'] != 'PENDING':
        return {"success": False, "message": f"Đơn hàng đã {order['status']}", "paid": False}
    
    # Check tất cả giao dịch
    processed = await check_all_transactions()
    
    # Kiểm tra lại order
    order = get_order(order_code)
    if order['status'] == 'PAID':
        return {"success": True, "message": "Thanh toán thành công! Sản phẩm đã được gửi.", "paid": True}
    
    return {"success": True, "message": "Chưa tìm thấy giao dịch. Vui lòng đợi 1-2 phút sau khi chuyển khoản.", "paid": False}

# ================= AUTO CHECK LOOP =================
async def auto_check_loop():
    """Vòng lặp tự động kiểm tra thanh toán"""
    global auto_check_active
    
    logger.info("Auto check loop started")
    
    while auto_check_active:
        try:
            interval = int(get_config('auto_check_interval', '10'))
            
            processed = await check_all_transactions()
            if processed > 0:
                logger.info(f"Auto check: processed {processed} orders")
            
            await asyncio.sleep(interval)
            
        except Exception as e:
            logger.error(f"Auto check error: {e}")
            await asyncio.sleep(10)
    
    logger.info("Auto check loop stopped")

async def order_timeout_loop():
    """Vòng lặp hủy đơn hàng hết hạn"""
    while True:
        try:
            expired_orders = cancel_expired_orders()
            if expired_orders:
                logger.info(f"Cancelled {len(expired_orders)} expired orders")
                # Xóa tin nhắn thanh toán và thông báo cho user
                for order in expired_orders:
                    order_code = order['order_code']
                    
                    # Thử lấy từ memory trước
                    payment_msg = order_payment_messages.get(order_code)
                    if payment_msg:
                        delete_telegram_message_sync(payment_msg['chat_id'], payment_msg['message_id'])
                        del order_payment_messages[order_code]
                    elif order.get('message_id'):
                        delete_telegram_message_sync(order['user_id'], order['message_id'])
                    
                    # Gửi thông báo đơn đã hủy
                    send_telegram_sync(
                        order['user_id'],
                        f"⏰ <b>ĐƠN HÀNG ĐÃ HẾT HẠN</b>\n\n"
                        f"Đơn <code>{order_code}</code> đã bị hủy do quá {ORDER_TIMEOUT_MINUTES} phút không thanh toán.\n\n"
                        f"Vui lòng tạo đơn mới nếu bạn vẫn muốn mua! 🛒"
                    )
                    
            await asyncio.sleep(60)  # Check mỗi phút
        except Exception as e:
            logger.error(f"Order timeout error: {e}")
            await asyncio.sleep(60)

# ================= TELEGRAM BOT =================
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()

async def reinit_bot(new_token: str):
    """Tái khởi động bot với token mới"""
    global bot, BOT_TOKEN
    try:
        if bot:
            await bot.session.close()
    except Exception:
        pass
    BOT_TOKEN = new_token
    bot = Bot(token=BOT_TOKEN)
    logger.info("✅ Bot reinitialized with new token")

# Webhook mode settings
WEBHOOK_PATH = f"/webhook/telegram/{BOT_TOKEN}"
USE_WEBHOOK = os.getenv("USE_WEBHOOK", "false").lower() == "true"
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "")  # Your Railway domain

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def send_telegram_sync(user_id: int, text: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": user_id, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        logger.error(f"Error sending message: {e}")

def delete_telegram_message_sync(chat_id: int, message_id: int):
    """Xóa tin nhắn thanh toán"""
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
            json={"chat_id": chat_id, "message_id": message_id},
            timeout=10
        )
        if response.status_code == 200:
            logger.info(f"Deleted message {message_id} in chat {chat_id}")
            return True
        else:
            logger.warning(f"Failed to delete message: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
        return False

def get_main_keyboard():
    """Inline keyboard cho các chức năng"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🛍️ Sản phẩm", callback_data="shop"),
            InlineKeyboardButton(text="📋 Lịch sử", callback_data="my_orders")
        ],
        [
            InlineKeyboardButton(text="📞 Liên hệ Admin", callback_data="contact_admin")
        ]
    ])
    return keyboard

def get_persistent_keyboard():
    """Reply Keyboard cố định ở dưới màn hình"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🛍️ Sản phẩm"),
                KeyboardButton(text="📞 Liên hệ Admin")
            ]
        ],
        resize_keyboard=True,
        is_persistent=True
    )
    return keyboard

# Lưu trạng thái chờ nhập số lượng
user_pending_quantity = {}

# Lưu message_id của tin nhắn thanh toán để xóa sau (in-memory)
# Format: {order_code: {'message_id': int, 'chat_id': int}}
order_payment_messages = {}

def get_categories_keyboard():
    categories = get_categories()
    buttons = []
    for cat in categories:
        buttons.append([InlineKeyboardButton(
            text=f"{cat['icon']} {cat['name']}",
            callback_data=f"cat_{cat['id']}"
        )])
    # Thêm nút Làm mới và Quay lại
    buttons.append([
        InlineKeyboardButton(text="🔄 Làm mới", callback_data="shop"),
        InlineKeyboardButton(text="🔙 Quay lại", callback_data="back_main")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_products_keyboard(category_id: int):
    products = get_products(category_id=category_id)
    buttons = []
    for p in products:
        if p.get('contact_seller'):
            buttons.append([InlineKeyboardButton(
                text=f"{p['name']} - {p['price']:,}đ • Liên hệ",
                callback_data=f"contact_prod_{p['id']}"
            )])
        else:
            stock = p.get('stock_count', 0)
            status = f"✅ {stock}" if stock > 0 else "❌ Hết"
            buttons.append([InlineKeyboardButton(
                text=f"{p['name']} - {p['price']:,}đ [{status}]",
                callback_data=f"prod_{p['id']}"
            )])
    buttons.append([
        InlineKeyboardButton(text="🔄 Làm mới", callback_data=f"refresh_cat_{category_id}"),
        InlineKeyboardButton(text="🔙 Quay lại", callback_data="shop")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_products_keyboard_all():
    """Hiển thị TẤT CẢ sản phẩm (không qua danh mục)"""
    products = get_products()
    buttons = []
    for p in products:
        if p.get('contact_seller'):
            buttons.append([InlineKeyboardButton(
                text=f"{p['name']} - {p['price']:,}đ • Liên hệ",
                callback_data=f"contact_prod_{p['id']}"
            )])
        else:
            stock = p.get('stock_count', 0)
            status = f"✅ {stock}" if stock > 0 else "❌ Hết"
            buttons.append([InlineKeyboardButton(
                text=f"{p['name']} - {p['price']:,}đ [{status}]",
                callback_data=f"prod_{p['id']}"
            )])
    buttons.append([
        InlineKeyboardButton(text="🔄 Làm mới", callback_data="shop"),
        InlineKeyboardButton(text="📋 Lịch sử", callback_data="my_orders")
    ])
    buttons.append([
        InlineKeyboardButton(text="📞 Liên hệ Admin", callback_data="contact_admin")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_payment_keyboard(order_code: str):
    """Keyboard sau khi tạo đơn hàng - chỉ có nút huỷ"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Huỷ đơn này", callback_data=f"cancel_{order_code}")]
    ])

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user = message.from_user
    
    # LƯU USER VÀO DATABASE để broadcast
    save_bot_user(user.id, user.username, user.first_name, user.last_name)
    
    welcome_text = f"🎉 <b>Chào mừng {user.first_name} đến với Shop!</b>"
    
    # Gửi lời chào với persistent keyboard
    await message.answer(
        welcome_text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_persistent_keyboard()
    )
    
    # Hiển thị sản phẩm luôn
    products = get_products()
    
    if not products:
        await message.answer(
            "❌ <b>Chưa có sản phẩm nào!</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    keyboard = get_products_keyboard_all()
    await message.answer(
        "🛍️ <b>DANH SÁCH SẢN PHẨM</b>\n\n👇 Chọn sản phẩm để mua:",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )

# Handler cho nút Reply Keyboard "Sản phẩm"
@dp.message(F.text == "🛍️ Sản phẩm")
async def handle_menu_button(message: types.Message):
    """Xử lý khi nhấn nút Sản phẩm - hiện danh sách sản phẩm luôn"""
    products = get_products()
    
    if not products:
        await message.answer(
            "🛍️ <b>SẢN PHẨM</b>\n\n❌ Chưa có sản phẩm nào!",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    await message.answer(
        "🛍️ <b>DANH SÁCH SẢN PHẨM</b>\n\n👇 Chọn sản phẩm để mua:",
        parse_mode=ParseMode.HTML,
        reply_markup=get_products_keyboard_all()
    )

# Handler cho nút Reply Keyboard "Liên hệ Admin"
@dp.message(F.text == "📞 Liên hệ Admin")
async def handle_contact_button(message: types.Message):
    """Xử lý khi nhấn nút Liên hệ Admin"""
    zalo_link = get_config('zalo_link', os.getenv('ZALO_LINK', ''))
    telegram_channel = get_config('telegram_channel', os.getenv('TELEGRAM_CHANNEL', ''))
    
    contact_text = f"""
📞 <b>LIÊN HỆ ADMIN</b>

Nếu bạn cần hỗ trợ, vui lòng liên hệ qua:

━━━━━━━━━━━━━━━━━━━━
📱 <b>Zalo:</b> <a href="{zalo_link}">Nhấn vào đây</a>
📢 <b>Telegram:</b> <a href="{telegram_channel}">Nhấn vào đây</a>
━━━━━━━━━━━━━━━━━━━━

💬 <b>Các vấn đề thường gặp:</b>
• Chưa nhận được sản phẩm sau thanh toán
• Sản phẩm không hoạt động  
• Muốn đổi/trả sản phẩm
• Câu hỏi về sản phẩm

⏰ <b>Thời gian phản hồi:</b> 5-30 phút
🕐 <b>Hỗ trợ:</b> 8:00 - 22:00 hàng ngày
"""
    
    contact_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📱 Chat Zalo", url=zalo_link),
            InlineKeyboardButton(text="📢 Telegram", url=telegram_channel)
        ]
    ])
    
    await message.answer(contact_text, parse_mode=ParseMode.HTML, reply_markup=contact_kb, disable_web_page_preview=True)

@dp.message(Command("check"))
async def cmd_check(message: types.Message):
    """Kiểm tra đơn hàng đang chờ của user"""
    order = get_user_pending_order(message.from_user.id)
    
    if not order:
        await message.answer("❌ Bạn không có đơn hàng nào đang chờ thanh toán!")
        return
    
    await message.answer("🔄 Đang kiểm tra thanh toán...")
    
    result = await check_payment_for_order(order['order_code'])
    
    if result.get('paid'):
        await message.answer("✅ " + result['message'])
    else:
        await message.answer(
            f"⏳ <b>ĐANG CHỜ THANH TOÁN</b>\n\n"
            f"Đơn: <code>{order['order_code']}</code>\n"
            f"Số tiền: {order['amount']:,}đ\n\n"
            f"{result['message']}\n\n"
            f"💡 Nhấn nút bên dưới để kiểm tra lại",
            parse_mode=ParseMode.HTML,
            reply_markup=get_payment_keyboard(order['order_code'])
        )

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bạn không có quyền!")
        return
    
    stats = get_stats()
    recent_tx = get_recent_transactions(5)
    
    tx_text = ""
    for tx in recent_tx:
        status = "✅" if tx['processed'] else "⏳"
        tx_text += f"{status} {tx['amount']:,}đ - {tx['content'][:20]}...\n"
    
    text = f"""
🔐 <b>ADMIN PANEL</b>

📊 <b>Thống kê:</b>
• Doanh thu: {stats['total_revenue']:,}đ
• Đơn hàng: {stats['paid_orders']}/{stats['total_orders']}
• Kho: {stats['stock_available']} còn / {stats['stock_sold']} đã bán
• Auto check: {'🟢 ON' if auto_check_active else '🔴 OFF'}

📥 <b>Giao dịch gần đây:</b>
{tx_text if tx_text else 'Chưa có giao dịch'}

🌐 Admin Panel: <code>/admin</code> trên domain
"""
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    """Admin gửi broadcast đến TẤT CẢ users đã /start bot - dùng concurrent engine"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bạn không có quyền!")
        return

    # Lấy nội dung sau command
    text = message.text.replace("/broadcast", "").strip()

    if not text:
        await message.answer(
            "📣 <b>GỬI THÔNG BÁO</b>\n\n"
            "Cú pháp: <code>/broadcast [nội dung]</code>\n\n"
            "Ví dụ:\n"
            "<code>/broadcast Mới thêm capcut 35d 🔥\nAnh em mua nhanh kẻo hết!</code>\n\n"
            f"👥 Tổng users: <b>{get_bot_user_count()}</b>",
            parse_mode=ParseMode.HTML
        )
        return

    users = get_all_bot_users()
    if not users:
        await message.answer("❌ Không có user nào để gửi!")
        return

    # Wrap message với prefix
    full_message = f"📣 <b>Thông báo từ shop:</b>\n{text}"

    # Progress callback để update message
    progress_msg = await message.answer(f"📤 Đang gửi thông báo đến {len(users)} users...\n⚙️ Engine: concurrency={BROADCAST_CONCURRENCY}, chunk={BROADCAST_CHUNK_SIZE}")

    last_progress = ""

    def on_progress(snapshot):
        nonlocal last_progress
        pct = snapshot['processed'] * 100 // snapshot['total'] if snapshot['total'] else 0
        new_progress = f"📤 Progress: {snapshot['processed']}/{snapshot['total']} ({pct}%)\n✅ {snapshot['success']} | 🚫 {snapshot['blocked']} | ❌ {snapshot['failed']}"
        if new_progress != last_progress:
            last_progress = new_progress

    # Run broadcast engine - this is an async function
    final_stats = await run_broadcast(
        users=users,
        message=full_message,
        progress_callback=on_progress,
        admin_chat_id=message.from_user.id
    )

    await progress_msg.edit_text(
        f"✅ <b>GỬI THÔNG BÁO HOÀN TẤT</b>\n\n"
        f"📤 Tổng: {final_stats['total']}\n"
        f"✅ Thành công: {final_stats['success']}\n"
        f"🚫 Đã block bot: {final_stats['blocked']}\n"
        f"❌ Lỗi: {final_stats['failed']}",
        parse_mode=ParseMode.HTML
    )

@dp.message(Command("sales"))
async def cmd_sales(message: types.Message):
    """Admin xem lịch sử bán hàng"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bạn không có quyền!")
        return
    
    sales = get_sales_history(10)
    
    if not sales:
        await message.answer("📊 Chưa có giao dịch thành công nào!")
        return
    
    text = f"📊 <b>LỊCH SỬ BÁN HÀNG</b>\n\n"
    
    for s in sales:
        text += f"✅ <code>{s['order_code']}</code>\n"
        text += f"   👤 User: {s['user_id']} (@{s['username'] or 'N/A'})\n"
        text += f"   🛍️ SP: {s['product_name']}\n"
        text += f"   💰 {s['amount']:,}đ\n"
        if s['account_sent']:
            text += f"   📦 Acc: <code>{s['account_sent'][:30]}...</code>\n"
        text += "\n"
    
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message(Command("addstock"))
async def cmd_addstock(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Bạn không có quyền!")
        return
    
    text = message.text.replace("/addstock", "").strip()
    if not text:
        products = get_products()
        if not products:
            await message.answer("❌ Chưa có sản phẩm!")
            return
        
        text = "📝 <b>THÊM HÀNG VÀO KHO</b>\n\nCú pháp:\n<code>/addstock [product_id]\n[acc1]\n[acc2]</code>\n\n<b>Sản phẩm:</b>\n"
        for p in products:
            text += f"• ID {p['id']}: {p['name']} ({get_stock_count(p['id'])} trong kho)\n"
        
        await message.answer(text, parse_mode=ParseMode.HTML)
        return
    
    lines = text.split("\n")
    try:
        product_id = int(lines[0])
        contents = [l.strip() for l in lines[1:] if l.strip()]
        
        if contents:
            count = add_stocks(product_id, contents)
            await message.answer(f"✅ Đã thêm {count} sản phẩm vào kho!")
        else:
            await message.answer("❌ Không có nội dung!")
    except:
        await message.answer("❌ Sai cú pháp! Dòng đầu là ID sản phẩm.")

# ================= CALLBACK HANDLERS =================
@dp.callback_query(F.data == "shop")
async def callback_shop(callback: types.CallbackQuery):
    """Hiển thị danh sách sản phẩm (bỏ qua danh mục)"""
    await callback.answer()
    products = get_products()
    
    if not products:
        try:
            await callback.message.edit_text(
                "🛍️ <b>SẢN PHẨM</b>\n\n❌ Chưa có sản phẩm nào!",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
        except:
            await callback.message.answer(
                "🛍️ <b>SẢN PHẨM</b>\n\n❌ Chưa có sản phẩm nào!",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
        return
    
    text = "🛍️ <b>DANH SÁCH SẢN PHẨM</b>\n\n👇 Chọn sản phẩm để mua:"
    
    try:
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_products_keyboard_all()
        )
    except:
        await callback.message.answer(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_products_keyboard_all()
        )

@dp.callback_query(F.data.startswith("cat_"))
async def callback_category(callback: types.CallbackQuery):
    await callback.answer()
    category_id = int(callback.data.replace("cat_", ""))
    category = get_category(category_id)
    
    if not category:
        return
    
    products = get_products(category_id=category_id)
    stock_total = sum(p.get('stock_count', 0) for p in products)
    
    text = f"{category['icon']} <b>{category['name']}</b>\n\n"
    text += f"📦 Tổng: <b>{len(products)}</b> sản phẩm | 📊 Còn: <b>{stock_total}</b>\n\n"
    text += "👇 <b>Chọn sản phẩm:</b>"
    
    try:
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_products_keyboard(category_id)
        )
    except:
        await callback.message.answer(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_products_keyboard(category_id)
        )

@dp.callback_query(F.data.startswith("refresh_cat_"))
async def callback_refresh_category(callback: types.CallbackQuery):
    """Làm mới danh sách sản phẩm trong category"""
    await callback.answer("🔄 Đã làm mới!")
    category_id = int(callback.data.replace("refresh_cat_", ""))
    category = get_category(category_id)
    
    if not category:
        return
    
    products = get_products(category_id=category_id)
    stock_total = sum(p.get('stock_count', 0) for p in products)
    
    text = f"{category['icon']} <b>{category['name']}</b>\n\n"
    text += f"📦 Tổng: <b>{len(products)}</b> sản phẩm | 📊 Còn: <b>{stock_total}</b>\n\n"
    text += "👇 <b>Chọn sản phẩm:</b>"
    
    try:
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_products_keyboard(category_id)
        )
    except:
        pass

def _telegram_contact_display():
    """Lấy link và tên hiển thị Telegram từ TELEGRAM_CHANNEL (mặc định liên hệ người bán)"""
    raw = get_config('telegram_channel', os.getenv('TELEGRAM_CHANNEL', ''))
    if not raw:
        return '', 'N/A'
    raw = raw.strip()
    if '/t.me/' in raw or 't.me/' in raw:
        part = raw.split('t.me/')[-1].split('?')[0].strip('/')
        if part:
            display = '@' + part if not part.startswith('@') else part
            return raw, display
    return raw, raw if raw.startswith('@') else ('@' + raw)

def build_contact_seller_message(product: dict):
    """Tạo nội dung tin nhắn + keyboard cho sản phẩm loại Liên hệ người bán"""
    telegram_url, telegram_display = _telegram_contact_display()
    zalo_link = get_config('zalo_link', os.getenv('ZALO_LINK', ''))
    desc = (product.get('description') or '').strip()
    name = (product.get('name') or '').strip()
    # Tên sản phẩm to, rõ — dùng block nổi bật
    text = (
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🛍️ <b>{name}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 Giá: {product['price']:,}₫\n"
    )
    if desc:
        text += f"📝 {desc}\n\n"
    text += "📞 Liên hệ để mua hàng\n\n"
    if telegram_url:
        text += f"💬 Telegram: {telegram_display}\n"
    if zalo_link:
        text += f"📱 Zalo: <a href=\"{zalo_link}\">Nhấn vào đây</a>\n"
    text += "\nVui lòng liên hệ trực tiếp để được hỗ trợ!"
    buttons = []
    if telegram_url:
        buttons.append([InlineKeyboardButton(text=f"📞 Liên hệ: {telegram_display}", url=telegram_url)])
    buttons.append([InlineKeyboardButton(text="⬅️ Quay lại danh sách", callback_data="shop")])
    return text, InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.callback_query(F.data.startswith("contact_prod_"))
async def callback_contact_product(callback: types.CallbackQuery):
    """Sản phẩm loại Liên hệ người bán: chỉ hiển thị thông tin + nút liên hệ"""
    await callback.answer()
    product_id = int(callback.data.replace("contact_prod_", ""))
    product = get_product(product_id)
    if not product or not product.get('contact_seller'):
        return
    text, kb = build_contact_seller_message(product)
    try:
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data.startswith("prod_"))
async def callback_product(callback: types.CallbackQuery):
    """Chọn sản phẩm -> Yêu cầu nhập số lượng"""
    await callback.answer()
    product_id = int(callback.data.replace("prod_", ""))
    product = get_product(product_id)
    
    if not product:
        return
    
    if product.get('contact_seller'):
        text, kb = build_contact_seller_message(product)
        try:
            await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            await callback.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return
    
    stock_count = product.get('stock_count', 0)
    
    if stock_count == 0:
        await callback.message.answer("❌ <b>Hết hàng!</b>", parse_mode=ParseMode.HTML)
        return
    
    # Kiểm tra có đơn pending không
    existing_order = get_user_pending_order(callback.from_user.id)
    if existing_order:
        await callback.message.answer(
            f"⚠️ <b>Bạn có đơn hàng đang chờ!</b>\n\n"
            f"Mã: <code>{existing_order['order_code']}</code>\n"
            f"Số tiền: {existing_order['amount']:,}đ\n\n"
            f"Vui lòng thanh toán hoặc hủy đơn cũ trước.",
            parse_mode=ParseMode.HTML,
            reply_markup=get_payment_keyboard(existing_order['order_code'])
        )
        return
    
    # Lấy mô tả sản phẩm
    description = product.get('description', '')
    desc_text = f"\n📝 <i>{description}</i>\n" if description else ""
    
    # Nếu stock < 3: hiển thị nút bấm, >= 3: nhập số
    if stock_count < 3:
        # Hiển thị nút bấm số lượng
        qty_buttons = []
        for i in range(1, stock_count + 1):
            qty_buttons.append(InlineKeyboardButton(
                text=f"{i}",
                callback_data=f"buyqty_{product_id}_{i}"
            ))
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            qty_buttons,
            [InlineKeyboardButton(text="❌ Hủy", callback_data="cancel_qty")]
        ])
        
        await callback.message.answer(
            f"🛍️ <b>{product['name']}</b>\n\n"
            f"💰 Giá: <b>{product['price']:,}đ</b> / 1 sản phẩm\n"
            f"📦 Còn: <b>{stock_count}</b> sản phẩm\n"
            f"{desc_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🛒 <b>Chọn số lượng muốn mua:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
    else:
        # Lưu trạng thái chờ nhập số lượng
        user_pending_quantity[callback.from_user.id] = {
            'product_id': product_id,
            'product_name': product['name'],
            'price': product['price'],
            'max_qty': stock_count
        }
        
        # Keyboard hủy
        cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Hủy", callback_data="cancel_qty")]
        ])
        
        await callback.message.answer(
            f"🛍️ <b>{product['name']}</b>\n\n"
            f"💰 Giá: <b>{product['price']:,}đ</b> / 1 sản phẩm\n"
            f"📦 Còn: <b>{stock_count}</b> sản phẩm\n"
            f"{desc_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✏️ <b>Nhập số lượng muốn mua</b> (1 - {stock_count}):\n\n"
            f"<i>Gửi một số từ 1 đến {stock_count}</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=cancel_kb
        )

@dp.callback_query(F.data == "cancel_qty")
async def callback_cancel_quantity(callback: types.CallbackQuery):
    """Hủy nhập số lượng"""
    await callback.answer("Đã hủy!")
    user_id = callback.from_user.id
    if user_id in user_pending_quantity:
        del user_pending_quantity[user_id]
    await callback.message.delete()

@dp.callback_query(F.data.startswith("buyqty_"))
async def callback_buy_quantity(callback: types.CallbackQuery):
    """Xử lý khi user chọn số lượng bằng nút bấm"""
    await callback.answer()
    
    # Parse data: buyqty_productId_quantity
    parts = callback.data.split("_")
    product_id = int(parts[1])
    quantity = int(parts[2])
    
    product = get_product(product_id)
    if not product:
        await callback.message.answer("❌ Sản phẩm không tồn tại!")
        return
    
    # Kiểm tra có đơn pending không
    existing_order = get_user_pending_order(callback.from_user.id)
    if existing_order:
        await callback.message.answer(
            f"⚠️ <b>Bạn có đơn hàng đang chờ!</b>\n\n"
            f"Mã: <code>{existing_order['order_code']}</code>\n"
            f"Số tiền: {existing_order['amount']:,}đ\n\n"
            f"Vui lòng thanh toán hoặc hủy đơn cũ trước.",
            parse_mode=ParseMode.HTML,
            reply_markup=get_payment_keyboard(existing_order['order_code'])
        )
        return
    
    # Tạo đơn hàng
    await create_order_and_send_payment(
        callback.from_user,
        product,
        quantity,
        callback.message
    )
    
    # Xóa tin nhắn chọn số lượng
    try:
        await callback.message.delete()
    except:
        pass

@dp.message(F.text.regexp(r'^\d+$'))
async def handle_quantity_input(message: types.Message):
    """Xử lý khi user nhập số lượng"""
    user_id = message.from_user.id
    
    # Kiểm tra có đang chờ nhập số lượng không
    if user_id not in user_pending_quantity:
        return  # Không xử lý
    
    pending = user_pending_quantity[user_id]
    quantity = int(message.text)
    
    # Validate số lượng
    if quantity < 1:
        await message.answer("❌ Số lượng phải từ 1 trở lên!")
        return
    
    if quantity > pending['max_qty']:
        await message.answer(f"❌ Không đủ hàng! Chỉ còn <b>{pending['max_qty']}</b> sản phẩm.", parse_mode=ParseMode.HTML)
        return
    
    # Xóa trạng thái chờ
    del user_pending_quantity[user_id]
    
    # Lấy lại product để đảm bảo data mới nhất
    product = get_product(pending['product_id'])
    if not product or product.get('stock_count', 0) < quantity:
        await message.answer("❌ Sản phẩm đã hết hoặc không đủ số lượng!")
        return
    
    # Tạo đơn hàng
    await create_order_and_send_payment(message.from_user, product, quantity, message)

async def create_order_and_send_payment(user, product, quantity: int, message):
    """Tạo đơn hàng và gửi thông tin thanh toán"""
    total_price = product['price'] * quantity
    order_code = f"DH{random.randint(100000, 999999)}"
    
    customer_name = f"{user.first_name or ''} {user.last_name or ''}".strip() or (user.username or "N/A")
    create_order(
        user_id=user.id,
        username=user.username or user.first_name,
        order_code=order_code,
        product_id=product['id'],
        product_name=product['name'],
        amount=total_price,
        quantity=quantity,
        customer_name=customer_name
    )
    
    bank_info = get_bank_info()
    qr_url = generate_vietqr_url(total_price, order_code)
    
    payment_text = f"""📋 Mã đơn: <code>{order_code}</code>
👤 User: <code>{user.id}</code>
🛍️ Sản phẩm: <b>{product['name']}</b>
📦 Số lượng: <b>{quantity}</b>
💰 Tổng: <b>{total_price:,}đ</b>

👉 Chuyển khoản đúng nội dung <code>{order_code}</code> và số tiền <code>{total_price:,}</code>đ.
Sau khi thanh toán, bot sẽ tự động giao tài khoản.

⏰ Đơn được giữ <b>{ORDER_TIMEOUT_MINUTES} phút</b>, quá thời gian sẽ huỷ và trả acc về kho."""
    
    sent_msg = None
    try:
        qr_image = download_qr_image(qr_url)
        if qr_image:
            photo = BufferedInputFile(qr_image, filename="qr.png")
            sent_msg = await bot.send_photo(
                chat_id=user.id,
                photo=photo,
                caption=payment_text,
                parse_mode=ParseMode.HTML,
                reply_markup=get_payment_keyboard(order_code)
            )
    except Exception as e:
        logger.error(f"Error sending QR: {e}")
    
    # Nếu không gửi được QR thì gửi text
    if not sent_msg:
        sent_msg = await message.answer(
            payment_text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_payment_keyboard(order_code)
        )
    
    # Lưu message_id để xóa sau khi thanh toán/timeout (IN-MEMORY - reliable)
    if sent_msg and sent_msg.message_id:
        order_payment_messages[order_code] = {
            'message_id': sent_msg.message_id,
            'chat_id': user.id
        }
        logger.info(f"📤 Saved payment message: order={order_code}, message_id={sent_msg.message_id}, chat_id={user.id}")
        
        # Also try to save to DB as backup
        try:
            update_order_message_id(order_code, sent_msg.message_id)
        except:
            pass
    else:
        logger.error(f"📤 Could not save message_id - sent_msg is None!")

@dp.callback_query(F.data.startswith("check_"))
async def callback_check_payment(callback: types.CallbackQuery):
    """Khách nhấn nút kiểm tra thanh toán"""
    order_code = callback.data.replace("check_", "")
    
    await callback.answer("🔄 Đang kiểm tra...")
    
    # Gửi thông báo đang check
    checking_msg = await callback.message.answer("🔄 <b>Đang kiểm tra giao dịch...</b>\n\nVui lòng đợi...", parse_mode=ParseMode.HTML)
    
    result = await check_payment_for_order(order_code)
    
    await checking_msg.delete()
    
    if result.get('paid'):
        await callback.message.answer(
            f"✅ <b>THANH TOÁN THÀNH CÔNG!</b>\n\n{result['message']}",
            parse_mode=ParseMode.HTML
        )
    else:
        order = get_order(order_code)
        if order and order['status'] == 'PENDING':
            await callback.message.answer(
                f"⏳ <b>CHƯA NHẬN ĐƯỢC TIỀN</b>\n\n"
                f"Đơn: <code>{order_code}</code>\n"
                f"Số tiền cần chuyển: <b>{order['amount']:,}đ</b>\n\n"
                f"💡 {result['message']}\n\n"
                f"<i>Nhấn nút để kiểm tra lại</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_payment_keyboard(order_code)
            )
        else:
            await callback.message.answer(f"❌ {result['message']}")

@dp.callback_query(F.data.startswith("cancel_"))
async def callback_cancel_order(callback: types.CallbackQuery):
    """Khách nhấn hủy đơn hàng"""
    order_code = callback.data.replace("cancel_", "")
    
    # Bỏ qua nếu là cancel_qty
    if order_code == "qty":
        return
    
    order = get_order(order_code)
    
    if not order:
        await callback.answer("Đơn hàng không tồn tại!", show_alert=True)
        return
    
    if order['user_id'] != callback.from_user.id:
        await callback.answer("Bạn không có quyền hủy đơn này!", show_alert=True)
        return
    
    if order['status'] != 'PENDING':
        await callback.answer(f"Đơn hàng đã {order['status']}!", show_alert=True)
        return
    
    cancel_order(order_code)
    await callback.answer("Đã hủy đơn hàng!")
    
    # Xóa tin nhắn thanh toán (QR code)
    try:
        await callback.message.delete()
    except:
        pass
    
    await callback.message.answer(
        f"❌ <b>ĐÃ HỦY ĐƠN HÀNG</b>\n\nMã: <code>{order_code}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.callback_query(F.data == "my_orders")
async def callback_my_orders(callback: types.CallbackQuery):
    await callback.answer()
    orders = get_user_orders(callback.from_user.id)
    
    if not orders:
        await callback.message.answer("📋 Bạn chưa có đơn hàng nào!")
        return
    
    text = "📋 <b>ĐƠN HÀNG CỦA BẠN</b>\n\n"
    for order in orders:
        status_emoji = {"PAID": "✅", "PENDING": "⏳", "CANCELLED": "❌", "TIMEOUT": "⏰"}.get(order['status'], "❓")
        text += f"{status_emoji} <code>{order['order_code']}</code>\n"
        text += f"   {order['product_name']} - {order['amount']:,}đ\n\n"
    
    await callback.message.answer(text, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "help")
async def callback_help(callback: types.CallbackQuery):
    await callback.answer()
    bank_info = get_bank_info()
    
    help_text = f"""
❓ <b>HƯỚNG DẪN MUA HÀNG</b>

<b>1.</b> Nhấn "🛒 Danh mục"
<b>2.</b> Chọn danh mục → Chọn sản phẩm
<b>3.</b> Nhập số lượng muốn mua
<b>4.</b> Chuyển khoản theo QR hoặc thông tin:

🏦 <b>{bank_info['bank_name']}</b>
💳 <code>{bank_info['bank_account']}</code>
👤 <code>{bank_info['account_name']}</code>

<b>5.</b> Nhấn <b>🔍 KIỂM TRA THANH TOÁN</b>
<b>6.</b> Nhận sản phẩm tự động!

⚠️ <b>Lưu ý:</b>
• Chuyển đúng số tiền
• Ghi đúng nội dung (mã đơn hàng)
• Đơn tự động hủy sau {ORDER_TIMEOUT_MINUTES} phút
• Nếu mua nhiều (>5), sẽ nhận file TXT
"""
    await callback.message.answer(help_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "contact_admin")
async def callback_contact_admin(callback: types.CallbackQuery):
    await callback.answer()
    
    zalo_link = get_config('zalo_link', os.getenv('ZALO_LINK', ''))
    telegram_channel = get_config('telegram_channel', os.getenv('TELEGRAM_CHANNEL', ''))
    
    contact_text = f"""
📞 <b>LIÊN HỆ ADMIN</b>

Nếu bạn cần hỗ trợ, vui lòng liên hệ qua:

━━━━━━━━━━━━━━━━━━━━
📱 <b>Zalo:</b> <a href="{zalo_link}">Nhấn vào đây</a>
📢 <b>Telegram:</b> <a href="{telegram_channel}">Nhấn vào đây</a>
━━━━━━━━━━━━━━━━━━━━

💬 <b>Các vấn đề thường gặp:</b>
• Chưa nhận được sản phẩm sau thanh toán
• Sản phẩm không hoạt động  
• Muốn đổi/trả sản phẩm
• Câu hỏi về sản phẩm

⏰ <b>Thời gian phản hồi:</b> 5-30 phút
🕐 <b>Hỗ trợ:</b> 8:00 - 22:00 hàng ngày
"""
    
    contact_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📱 Chat Zalo", url=zalo_link),
            InlineKeyboardButton(text="📢 Telegram", url=telegram_channel)
        ],
        [InlineKeyboardButton(text="🏠 Về trang chủ", callback_data="back_main")]
    ])
    
    await callback.message.answer(contact_text, parse_mode=ParseMode.HTML, reply_markup=contact_kb, disable_web_page_preview=True)

@dp.callback_query(F.data == "back_main")
async def callback_back_main(callback: types.CallbackQuery):
    """Quay lại danh sách sản phẩm"""
    await callback.answer()
    products = get_products()
    
    if not products:
        text = "🛍️ <b>SẢN PHẨM</b>\n\n❌ Chưa có sản phẩm nào!"
        kb = get_main_keyboard()
    else:
        text = "🛍️ <b>DANH SÁCH SẢN PHẨM</b>\n\n👇 Chọn sản phẩm để mua:"
        kb = get_products_keyboard_all()
    
    try:
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except:
        await callback.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

# ================= FASTAPI =================
app = FastAPI(title="Telegram Shop Bot", version="2.1.0")

# Fix Jinja2 cache issue on some environments
import jinja2
class FixedEnvironment(jinja2.Environment):
    def get_template(self, name):
        try:
            return super().get_template(name)
        except TypeError:
            # Workaround for cache corruption
            self._load_templates.clear() if hasattr(self, '_load_templates') else None
            self.cache.clear()
            return super().get_template(name)

# Monkey-patch before creating templates
_original_jinja2templates_init = None

templates = Jinja2Templates(directory="templates")

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# Pydantic Models
class CategoryCreate(BaseModel):
    name: str
    description: str = ""
    icon: str = "📦"
    sort_order: int = 0

class ProductCreate(BaseModel):
    category_id: int
    name: str
    price: int
    description: str = ""
    contact_seller: bool = False
    sort_order: int = 0

class StockCreate(BaseModel):
    product_id: int
    contents: List[str]

class BankConfig(BaseModel):
    bank_id: str
    bank_account: str
    bank_name: str

class AutoCheckConfig(BaseModel):
    api_key: str = ""
    interval: int = 10
    method: str = "api"

# ================= PYDANTIC AUTH MODELS =================
class LoginRequest(BaseModel):
    username: str
    password: str

# ================= WEB ROUTES =================
@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <html><head><title>Shop Bot</title></head>
    <body style="font-family:Arial;text-align:center;padding:50px;">
        <h1>🛒 Telegram Shop Bot</h1>
        <p>Bot đang hoạt động!</p>
        <p><a href="/admin">Admin Panel</a></p>
    </body></html>
    """

@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    """Trang đăng nhập"""
    # Nếu đã đăng nhập thì redirect về admin
    token = request.cookies.get("admin_session")
    if verify_session(token):
        return RedirectResponse(url="/admin", status_code=302)

    # Direct HTML response to avoid Jinja2 TemplateResponse cache issue
    html_content = """
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Đăng nhập - Admin Panel</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
            body { font-family: 'Inter', sans-serif; }
            .gradient-bg { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }
        </style>
    </head>
    <body class="gradient-bg min-h-screen flex items-center justify-center p-4">
        <div class="bg-white rounded-2xl shadow-2xl p-8 w-full max-w-md">
            <div class="text-center mb-8">
                <div class="w-20 h-20 bg-gradient-to-r from-indigo-500 to-purple-500 rounded-full flex items-center justify-center mx-auto mb-4">
                    <i class="fas fa-robot text-white text-3xl"></i>
                </div>
                <h1 class="text-2xl font-bold text-gray-800">Admin Panel</h1>
                <p class="text-gray-500">Đăng nhập để quản lý shop</p>
            </div>

            <form id="loginForm" onsubmit="handleLogin(event)">
                <div class="space-y-4">
                    <div>
                        <label class="block text-gray-600 mb-2">Tên đăng nhập</label>
                        <div class="relative">
                            <span class="absolute left-4 top-3 text-gray-400"><i class="fas fa-user"></i></span>
                            <input type="text" id="username" required placeholder="admin"
                                class="w-full pl-12 pr-4 py-3 border rounded-xl focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition">
                        </div>
                    </div>
                    <div>
                        <label class="block text-gray-600 mb-2">Mật khẩu</label>
                        <div class="relative">
                            <span class="absolute left-4 top-3 text-gray-400"><i class="fas fa-lock"></i></span>
                            <input type="password" id="password" required placeholder="••••••••"
                                class="w-full pl-12 pr-4 py-3 border rounded-xl focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition">
                        </div>
                    </div>
                    <div id="errorMessage" class="hidden bg-red-50 text-red-600 p-3 rounded-xl text-sm">
                        <i class="fas fa-exclamation-circle mr-2"></i>
                        <span></span>
                    </div>
                    <button type="submit" id="loginBtn"
                        class="w-full bg-gradient-to-r from-indigo-500 to-purple-500 text-white py-3 rounded-xl hover:opacity-90 transition font-semibold flex items-center justify-center gap-2">
                        <i class="fas fa-sign-in-alt"></i>
                        Đăng nhập
                    </button>
                </div>
            </form>

            <div class="mt-6 text-center text-sm text-gray-500">
                <p>🔒 Bảo mật bởi Session Authentication</p>
            </div>
        </div>

        <script>
        async function handleLogin(event) {
            event.preventDefault();
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            const errorDiv = document.getElementById('errorMessage');
            const loginBtn = document.getElementById('loginBtn');
            loginBtn.disabled = true;
            loginBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Đang đăng nhập...';
            try {
                const response = await fetch('/admin/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username, password })
                });
                const data = await response.json();
                if (data.success) {
                    window.location.href = '/admin';
                } else {
                    errorDiv.classList.remove('hidden');
                    errorDiv.querySelector('span').textContent = data.message || 'Đăng nhập thất bại!';
                }
            } catch (error) {
                errorDiv.classList.remove('hidden');
                errorDiv.querySelector('span').textContent = 'Lỗi kết nối server!';
            } finally {
                loginBtn.disabled = false;
                loginBtn.innerHTML = '<i class="fas fa-sign-in-alt"></i> Đăng nhập';
            }
        }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.post("/admin/login")
async def admin_login(request: Request, response: Response):
    """Xử lý đăng nhập"""
    try:
        data = await request.json()
        username = data.get("username", "")
        password = data.get("password", "")
        
        # Kiểm tra credentials
        if username == ADMIN_USERNAME and verify_password(password):
            # Tạo session
            token = create_session(username)
            
            # Set cookie
            response = JSONResponse(content={"success": True, "message": "Đăng nhập thành công!"})
            response.set_cookie(
                key="admin_session",
                value=token,
                httponly=True,
                max_age=86400,  # 24 hours
                samesite="lax"
            )
            return response
        else:
            return JSONResponse(
                content={"success": False, "message": "Sai tên đăng nhập hoặc mật khẩu!"},
                status_code=401
            )
    except Exception as e:
        return JSONResponse(
            content={"success": False, "message": str(e)},
            status_code=400
        )

@app.get("/admin/logout")
async def admin_logout(request: Request):
    """Đăng xuất"""
    token = request.cookies.get("admin_session")
    if token:
        delete_session(token)
    
    response = RedirectResponse(url="/admin/login", status_code=302)
    response.delete_cookie("admin_session")
    return response

@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request):
    """Trang admin - yêu cầu đăng nhập"""
    token = request.cookies.get("admin_session")

    if not verify_session(token):
        return RedirectResponse(url="/admin/login", status_code=302)

    # Read admin.html directly to avoid Jinja2 cache issue
    try:
        with open("templates/admin.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except Exception as e:
        logger.error(f"Error loading admin.html: {e}")
        return HTMLResponse(content="Error loading admin page", status_code=500)

@app.get("/api/admin/check-auth")
async def check_auth(request: Request):
    """Kiểm tra trạng thái đăng nhập"""
    token = request.cookies.get("admin_session")
    is_authenticated = verify_session(token)
    
    return {
        "authenticated": is_authenticated,
        "username": get_session_user(token) if is_authenticated else None
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "auto_check": auto_check_active}

# ================= ADMIN AUTH DEPENDENCY =================
async def require_admin(request: Request):
    """Dependency để kiểm tra authentication cho API"""
    token = request.cookies.get("admin_session")
    if not verify_session(token):
        raise HTTPException(status_code=401, detail="Unauthorized - Vui lòng đăng nhập")
    return True

# ================= ADMIN API =================

# ================= BACKUP/RESTORE =================
@app.get("/api/admin/backup")
async def api_backup_database(request: Request, auth: bool = Depends(require_admin)):
    """Download database backup"""
    import shutil
    from fastapi.responses import FileResponse
    
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=404, detail="Database not found")
    
    # Create backup with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"shop_backup_{timestamp}.db"
    
    return FileResponse(
        path=DB_PATH,
        filename=backup_filename,
        media_type="application/octet-stream"
    )

@app.post("/api/admin/restore")
async def api_restore_database(request: Request, auth: bool = Depends(require_admin)):
    """Restore database from uploaded file"""
    from fastapi import UploadFile, File
    
    form = await request.form()
    file = form.get("file")
    
    if not file:
        raise HTTPException(status_code=400, detail="No file uploaded")
    
    try:
        # Read uploaded file
        contents = await file.read()
        
        # Verify it's a valid SQLite database
        if not contents.startswith(b'SQLite format 3'):
            raise HTTPException(status_code=400, detail="Invalid SQLite database file")
        
        # Backup current database
        if os.path.exists(DB_PATH):
            backup_path = f"{DB_PATH}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            import shutil
            shutil.copy2(DB_PATH, backup_path)
            logger.info(f"Created backup at {backup_path}")
        
        # Write new database
        with open(DB_PATH, 'wb') as f:
            f.write(contents)

        # Run migrations and auto-generate sort_order
        run_migrations()
        auto_generate_sort_order()

        logger.info("Database restored successfully")
        return {"success": True, "message": "Database restored successfully. Please restart the bot."}
        
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/backup/settings")
async def api_backup_settings(request: Request, auth: bool = Depends(require_admin)):
    """Export settings as JSON"""
    settings = {
        "bank_config": get_bank_info(),
        "categories": get_categories(),
        "products": get_products(active_only=False),
        "stocks": get_stocks(is_sold=0, limit=10000),  # Chỉ lấy stock chưa bán
        "exported_at": datetime.now().isoformat()
    }
    
    from fastapi.responses import Response
    import json
    
    return Response(
        content=json.dumps(settings, ensure_ascii=False, indent=2),
        media_type="application/json",
        headers={
            "Content-Disposition": f"attachment; filename=shop_settings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        }
    )

@app.post("/api/admin/restore/settings")
async def api_restore_settings(request: Request, auth: bool = Depends(require_admin)):
    """Restore settings from JSON file"""
    import json
    
    form = await request.form()
    file = form.get("file")
    
    if not file:
        raise HTTPException(status_code=400, detail="No file uploaded")
    
    try:
        contents = await file.read()
        settings = json.loads(contents.decode('utf-8'))
        
        restored = {"categories": 0, "products": 0, "stocks": 0, "bank": False}
        
        # Restore bank config
        if "bank_config" in settings:
            bank = settings["bank_config"]
            set_config("bank_id", bank.get("bank_id", ""))
            set_config("bank_account", bank.get("bank_account", ""))
            set_config("bank_name", bank.get("bank_name", ""))
            set_config("account_name", bank.get("account_name", ""))
            restored["bank"] = True
        
        # Restore categories
        if "categories" in settings:
            for cat in settings["categories"]:
                try:
                    create_category(cat["name"], cat.get("description", ""), cat.get("icon", "📦"))
                    restored["categories"] += 1
                except:
                    pass
        
        # Restore products
        if "products" in settings:
            # Get category mapping (old name -> new id)
            new_categories = {c["name"]: c["id"] for c in get_categories()}
            
            for prod in settings["products"]:
                try:
                    cat_name = prod.get("category_name", "")
                    cat_id = new_categories.get(cat_name, 1)
                    create_product(cat_id, prod["name"], prod["price"], prod.get("description", ""))
                    restored["products"] += 1
                except:
                    pass
        
        # Restore stocks
        if "stocks" in settings:
            # Get product mapping (old name -> new id)
            new_products = {p["name"]: p["id"] for p in get_products(active_only=False)}
            
            for stock in settings["stocks"]:
                try:
                    prod_name = stock.get("product_name", "")
                    prod_id = new_products.get(prod_name)
                    if prod_id:
                        add_stocks(prod_id, [stock["content"]])
                        restored["stocks"] += 1
                except:
                    pass
        
        logger.info(f"Settings restored: {restored}")
        return {
            "success": True, 
            "message": f"Khôi phục thành công! Categories: {restored['categories']}, Products: {restored['products']}, Stocks: {restored['stocks']}, Bank: {'✓' if restored['bank'] else '✗'}"
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON file")
    except Exception as e:
        logger.error(f"Restore settings failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class SettingsRequest(BaseModel):
    admin_password: str = ""
    admin_ids: str = ""

@app.post("/api/admin/config/settings")
async def api_save_settings(data: SettingsRequest, request: Request, auth: bool = Depends(require_admin)):
    """Save admin settings"""
    global ADMIN_PASSWORD
    
    try:
        # Update admin password if provided
        if data.admin_password and data.admin_password.strip():
            new_password = data.admin_password.strip()
            ADMIN_PASSWORD = new_password
            set_config("admin_password", new_password)
            logger.info("Admin password updated")
        
        # Update admin IDs if provided  
        if data.admin_ids and data.admin_ids.strip():
            set_config("admin_ids", data.admin_ids.strip())
            logger.info("Admin IDs updated")
        
        return {"success": True, "message": "Cài đặt đã được lưu!"}
    except Exception as e:
        logger.error(f"Save settings error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/config/settings")
async def api_get_settings(request: Request, auth: bool = Depends(require_admin)):
    """Get current settings"""
    return {
        "admin_ids": get_config("admin_ids", ""),
        "admin_password_set": bool(get_config("admin_password", "") or ADMIN_PASSWORD != "admin123")
    }

# ================= BOT CONFIG API =================
class BotConfigRequest(BaseModel):
    bot_token: str = ""
    admin_ids: str = ""

@app.get("/api/admin/config/bot")
async def api_get_bot_config(request: Request, auth: bool = Depends(require_admin)):
    """Lấy cấu hình bot (BOT_TOKEN ẩn một phần, ADMIN_IDS)"""
    token = get_config("bot_token", "") or os.getenv("BOT_TOKEN", "")
    masked = ""
    if token:
        masked = token[:8] + "..." + token[-4:] if len(token) > 12 else "***"
    admin_ids_db = get_config("admin_ids_list", "")
    admin_ids_env = os.getenv("ADMIN_IDS", "")
    return {
        "bot_token_set": bool(token),
        "bot_token_masked": masked,
        "bot_token_source": "database" if get_config("bot_token", "") else ("env" if os.getenv("BOT_TOKEN") else "none"),
        "admin_ids": admin_ids_db or admin_ids_env,
        "current_admin_ids": ADMIN_IDS,
    }

@app.post("/api/admin/config/bot")
async def api_save_bot_config(data: BotConfigRequest, request: Request, auth: bool = Depends(require_admin)):
    """Lưu BOT_TOKEN và ADMIN_IDS vào DB, tái khởi động bot nếu token thay đổi"""
    global ADMIN_IDS, BOT_TOKEN
    try:
        token_changed = False

        # Lưu BOT_TOKEN nếu có
        if data.bot_token and data.bot_token.strip():
            new_token = data.bot_token.strip()
            old_token = get_config("bot_token", "") or os.getenv("BOT_TOKEN", "")
            if new_token != old_token:
                token_changed = True
            set_config("bot_token", new_token)
            logger.info("✅ BOT_TOKEN saved to database config")

        # Lưu ADMIN_IDS nếu có
        if data.admin_ids and data.admin_ids.strip():
            clean_ids = data.admin_ids.strip()
            set_config("admin_ids_list", clean_ids)
            # Cập nhật runtime
            new_ids = []
            for _id_str in clean_ids.split(","):
                _id_str = _id_str.strip()
                if _id_str.isdigit():
                    new_ids.append(int(_id_str))
            if new_ids:
                ADMIN_IDS[:] = new_ids
            logger.info(f"✅ ADMIN_IDS updated: {ADMIN_IDS}")

        # Tái khởi động bot nếu token thay đổi
        if token_changed:
            new_token = data.bot_token.strip()
            BOT_TOKEN = new_token
            await reinit_bot(new_token)
            return {
                "success": True,
                "message": "✅ Đã lưu! Bot token mới đã được áp dụng. Vui lòng restart service để đảm bảo hoạt động ổn định.",
                "restart_recommended": True
            }

        return {"success": True, "message": "✅ Cấu hình đã được lưu thành công!"}
    except Exception as e:
        logger.error(f"Save bot config error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/stats")
async def api_stats(request: Request, auth: bool = Depends(require_admin)):
    stats = get_stats()
    stats['auto_check_active'] = auto_check_active
    return stats

@app.get("/api/admin/categories")
async def api_get_categories(request: Request, auth: bool = Depends(require_admin)):
    return get_categories()

@app.post("/api/admin/categories")
async def api_create_category(data: CategoryCreate, request: Request, auth: bool = Depends(require_admin)):
    try:
        cat_id = create_category(data.name, data.description, data.icon, data.sort_order)
        return {"success": True, "id": cat_id}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.get("/api/admin/categories/{category_id}")
async def api_get_category(category_id: int, request: Request, auth: bool = Depends(require_admin)):
    """Lấy thông tin 1 category"""
    cat = get_category(category_id)
    if cat:
        return cat
    return {"error": "Category not found"}

@app.put("/api/admin/categories/{category_id}")
async def api_update_category(category_id: int, data: CategoryCreate, request: Request, auth: bool = Depends(require_admin)):
    """Cập nhật category"""
    try:
        update_category(category_id, data.name, data.description, data.icon, data.sort_order)
        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.delete("/api/admin/categories/{category_id}")
async def api_delete_category(category_id: int, request: Request, auth: bool = Depends(require_admin)):
    delete_category(category_id)
    return {"success": True}

@app.post("/api/admin/categories/reorder")
async def api_reorder_categories(data: dict, request: Request, auth: bool = Depends(require_admin)):
    """Cập nhật thứ tự danh mục - dùng số đơn giản 0,1,2,3..."""
    try:
        categories = data.get("categories", [])
        if not categories:
            return {"success": True}

        with get_db() as db:
            cur = db.cursor()

            # Cập nhật sort_order = index
            for idx, item in enumerate(categories):
                cur.execute(
                    "UPDATE categories SET sort_order = ? WHERE id = ?",
                    (idx, item.get("id"))
                )

            db.commit()
        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.get("/api/admin/products")
async def api_get_products(request: Request, category_id: str = "", auth: bool = Depends(require_admin)):
    cat_id = int(category_id) if category_id and category_id.isdigit() else None
    return get_products(category_id=cat_id, active_only=False)

@app.post("/api/admin/products")
async def api_create_product(data: ProductCreate, request: Request, auth: bool = Depends(require_admin)):
    try:
        prod_id = create_product(data.category_id, data.name, data.price, data.description, data.contact_seller, data.sort_order)
        return {"success": True, "id": prod_id}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.get("/api/admin/products/{product_id}")
async def api_get_product(product_id: int, request: Request, auth: bool = Depends(require_admin)):
    """Lấy thông tin 1 sản phẩm"""
    product = get_product(product_id)
    if product:
        return product
    return {"error": "Product not found"}

@app.put("/api/admin/products/{product_id}")
async def api_update_product(product_id: int, data: ProductCreate, request: Request, auth: bool = Depends(require_admin)):
    """Cập nhật sản phẩm"""
    try:
        update_product(
            product_id,
            category_id=data.category_id,
            name=data.name,
            price=data.price,
            description=data.description,
            contact_seller=data.contact_seller,
            sort_order=data.sort_order
        )
        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.post("/api/admin/products/{product_id}/toggle")
async def api_toggle_product(product_id: int, request: Request, auth: bool = Depends(require_admin)):
    toggle_product(product_id)
    return {"success": True}

@app.post("/api/admin/products/reorder")
async def api_reorder_products(data: dict, request: Request, auth: bool = Depends(require_admin)):
    """Cập nhật thứ tự sản phẩm - tính index riêng cho mỗi category"""
    try:
        products = data.get("products", [])
        if not products:
            return {"success": True}

        with get_db() as db:
            cur = db.cursor()

            # Lấy category_id của từng product từ database
            product_ids = [p.get("id") for p in products]
            if not product_ids:
                return {"success": True}
                
            placeholders = ','.join('?' * len(product_ids))
            cur.execute(f"SELECT id, category_id FROM products WHERE id IN ({placeholders})", product_ids)
            product_cat_map = {row[0]: row[1] for row in cur.fetchall()}

            # Nhóm products theo category_id từ database
            # products gửi lên có thể trộn lẫn, cần tách ra
            by_category = {}
            for idx, item in enumerate(products):
                product_id = item.get("id")
                cat_id = product_cat_map.get(product_id)
                if cat_id not in by_category:
                    by_category[cat_id] = []
                by_category[cat_id].append(product_id)

            # Cập nhật sort_order = index trong category (0,1,2,3...)
            for cat_id, prod_ids in by_category.items():
                for idx, pid in enumerate(prod_ids):
                    cur.execute("UPDATE products SET sort_order = ? WHERE id = ?", (idx, pid))

            db.commit()
        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.delete("/api/admin/products/{product_id}")
async def api_delete_product(product_id: int, request: Request, auth: bool = Depends(require_admin)):
    delete_product(product_id)
    return {"success": True}

@app.get("/api/admin/stock")
async def api_get_stock(request: Request, product_id: str = "", is_sold: str = "", auth: bool = Depends(require_admin)):
    prod_id = int(product_id) if product_id and product_id.isdigit() else None
    is_sold_int = int(is_sold) if is_sold and is_sold.isdigit() else None
    return get_stocks(product_id=prod_id, is_sold=is_sold_int)

@app.post("/api/admin/stock")
async def api_add_stock(data: StockCreate, request: Request, auth: bool = Depends(require_admin)):
    try:
        count = add_stocks(data.product_id, data.contents)
        return {"success": True, "count": count}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.delete("/api/admin/stock/{stock_id}")
async def api_delete_stock(stock_id: int, request: Request, auth: bool = Depends(require_admin)):
    return {"success": delete_stock(stock_id)}

@app.get("/api/admin/orders")
async def api_get_orders(request: Request, status: str = "", limit: int = 50, auth: bool = Depends(require_admin)):
    return get_orders(status=status if status else None, limit=limit)

@app.post("/api/admin/orders/{order_code}/confirm")
async def api_confirm_order(order_code: str, request: Request, auth: bool = Depends(require_admin)):
    order = get_order(order_code)
    if not order:
        return {"success": False, "message": "Order not found"}
    
    if order['status'] == 'PAID':
        return {"success": False, "message": "Already paid"}
    
    mark_order_paid(order_code)
    
    quantity = order.get('quantity', 1) or 1
    stocks = get_available_stock(order.get('product_id'), order_code, order['user_id'], quantity)
    
    if stocks and len(stocks) > 0:
        products_text = "\n".join([f"{i+1}. <code>{s['content']}</code>" for i, s in enumerate(stocks)])
        send_telegram_sync(
            order['user_id'],
            f"🎉 <b>ĐƠN HÀNG ĐƯỢC XÁC NHẬN!</b>\n\n"
            f"📦 <b>SẢN PHẨM ({len(stocks)}):</b>\n{products_text}"
        )
        return {"success": True, "delivered": True, "count": len(stocks)}
    else:
        send_telegram_sync(order['user_id'], "⚠️ Hết hàng! Admin sẽ liên hệ.")
        return {"success": True, "delivered": False}

@app.post("/api/admin/orders/{order_code}/cancel")
async def api_cancel_order(order_code: str, request: Request, auth: bool = Depends(require_admin)):
    cancel_order(order_code)
    return {"success": True}

@app.get("/api/admin/config/bank")
async def api_get_bank_config(request: Request, auth: bool = Depends(require_admin)):
    return {
        "bank_id": get_config('bank_id', DEFAULT_BANK_ID),
        "bank_account": get_config('bank_account', DEFAULT_BANK_ACCOUNT),
        "bank_name": get_config('bank_name', DEFAULT_BANK_NAME)
    }

@app.post("/api/admin/config/bank")
async def api_save_bank_config(data: BankConfig, request: Request, auth: bool = Depends(require_admin)):
    set_config('bank_id', data.bank_id)
    set_config('bank_account', data.bank_account)
    set_config('bank_name', data.bank_name)
    return {"success": True}

@app.get("/api/admin/config/autocheck")
async def api_get_autocheck_config(request: Request, auth: bool = Depends(require_admin)):
    return {
        "api_key": get_config('sepay_api_key', ''),
        "interval": int(get_config('auto_check_interval', '10')),
        "method": get_config('auto_check_method', 'api'),
        "is_active": auto_check_active
    }

@app.post("/api/admin/config/autocheck")
async def api_save_autocheck_config(data: AutoCheckConfig, request: Request, auth: bool = Depends(require_admin)):
    set_config('sepay_api_key', data.api_key)
    set_config('auto_check_interval', str(data.interval))
    set_config('auto_check_method', data.method)
    return {"success": True}

@app.post("/api/admin/autocheck/toggle")
async def api_toggle_autocheck(request: Request, auth: bool = Depends(require_admin)):
    global auto_check_active, auto_check_task
    
    auto_check_active = not auto_check_active
    set_config('auto_check_active', '1' if auto_check_active else '0')
    
    if auto_check_active and auto_check_task is None:
        auto_check_task = asyncio.create_task(auto_check_loop())
    elif not auto_check_active and auto_check_task:
        auto_check_task.cancel()
        auto_check_task = None
    
    return {"success": True, "is_active": auto_check_active}

@app.post("/api/admin/autocheck/manual")
async def api_manual_check(request: Request, auth: bool = Depends(require_admin)):
    processed = await check_all_transactions()
    return {"success": True, "processed": processed}

@app.get("/api/admin/transactions")
async def api_get_transactions(request: Request, limit: int = 20, auth: bool = Depends(require_admin)):
    return get_recent_transactions(limit)

# ================= SALES & BROADCAST API =================
@app.get("/api/admin/sales")
async def api_get_sales(request: Request, auth: bool = Depends(require_admin)):
    """Lấy lịch sử bán hàng - user nào mua tài khoản nào"""
    return get_sales_history(100)

class BroadcastRequest(BaseModel):
    message: str
    
@app.post("/api/admin/broadcast")
async def api_broadcast(data: BroadcastRequest, request: Request, auth: bool = Depends(require_admin)):
    """Gửi broadcast đến TẤT CẢ users đã /start bot - dùng concurrent engine"""
    users = get_all_bot_users()
    if not users:
        return {"success": True, "total": 0, "sent": 0, "blocked": 0, "failed": 0}

    full_message = f"📣 <b>Thông báo từ shop:</b>\n{data.message}"

    # Run broadcast engine
    final_stats = await run_broadcast(
        users=users,
        message=full_message
    )

    return {
        "success": True,
        "total": final_stats['total'],
        "sent": final_stats['success'],
        "blocked": final_stats['blocked'],
        "failed": final_stats['failed']
    }

@app.get("/api/admin/broadcast/count")
async def api_broadcast_count(request: Request, auth: bool = Depends(require_admin)):
    """Đếm số users sẽ nhận broadcast"""
    return {"count": get_bot_user_count()}

@app.get("/api/admin/bot-users")
async def api_get_bot_users(request: Request, auth: bool = Depends(require_admin)):
    """Lấy danh sách tất cả users đã /start bot"""
    return get_all_bot_users()

# ================= WEBHOOKS =================
@app.get("/webhook/sepay")
async def sepay_webhook_test():
    """Test endpoint - SePay có thể gọi GET để verify URL"""
    return {
        "status": "ok",
        "message": "SePay webhook is ready",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/webhook/sepay")
async def sepay_webhook(request: Request):
    """Nhận webhook từ SePay khi có giao dịch"""
    try:
        # Log raw request
        body = await request.body()
        logger.info(f"=== SEPAY WEBHOOK RECEIVED ===")
        logger.info(f"Headers: {dict(request.headers)}")
        logger.info(f"Body: {body.decode('utf-8')}")
        
        data = json.loads(body)
        
        # SePay fields: id, gateway, transactionDate, accountNumber, subAccount,
        # transferType, transferAmount, accumulated, code, content, referenceCode, description
        content = data.get("content", "") or data.get("description", "") or data.get("code", "") or ""
        amount = int(data.get("transferAmount", 0) or data.get("amount", 0) or 0)
        transfer_type = str(data.get("transferType", "in")).lower()
        tx_id = str(data.get("id", "") or data.get("referenceCode", "") or f"sepay_{datetime.now().timestamp()}")
        
        logger.info(f"Parsed: amount={amount}, content='{content}', type={transfer_type}, tx_id={tx_id}")
        
        # Chỉ xử lý tiền VÀO
        if transfer_type not in ["in", "credit", ""]:
            logger.info(f"Ignored: transfer_type={transfer_type}")
            return {"status": "ignored", "reason": "outgoing_transaction"}
        
        if amount <= 0:
            logger.info(f"Ignored: amount={amount}")
            return {"status": "ignored", "reason": "no_amount"}
        
        # Xử lý giao dịch
        result = await process_single_transaction(tx_id, amount, content, json.dumps(data))
        
        if result:
            logger.info(f"SUCCESS: Order {result} processed")
            return {"status": "success", "order_code": result}
        else:
            logger.info(f"No matching order found")
            return {"status": "received", "message": "no_matching_order"}
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return {"status": "error", "message": "invalid_json"}
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}

@app.get("/webhook/casso")
async def casso_webhook_test():
    """Test endpoint cho Casso"""
    return {"status": "ok", "message": "Casso webhook ready"}

@app.post("/webhook/casso")
async def casso_webhook(request: Request):
    try:
        body = await request.body()
        logger.info(f"=== CASSO WEBHOOK RECEIVED ===")
        logger.info(f"Body: {body.decode('utf-8')}")
        
        data = json.loads(body)
        transactions = data.get("data", [])
        processed = 0
        
        for tx in transactions:
            content = tx.get("description", "")
            amount = int(tx.get("amount", 0))
            tx_id = f"casso_{tx.get('id', datetime.now().timestamp())}"
            
            if amount > 0:
                result = await process_single_transaction(tx_id, amount, content, json.dumps(tx))
                if result:
                    processed += 1
        
        return {"status": "success", "processed": processed}
        
    except Exception as e:
        logger.error(f"Casso error: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/api/admin/webhook/test")
async def test_webhook_manually(request: Request, auth: bool = Depends(require_admin)):
    """Test webhook thủ công - gửi fake transaction"""
    try:
        data = await request.json()
        amount = data.get("amount", 50000)
        content = data.get("content", "TEST123456")
        
        logger.info(f"Manual test: amount={amount}, content={content}")
        
        result = await process_single_transaction(
            f"test_{datetime.now().timestamp()}", 
            amount, 
            content,
            json.dumps({"test": True})
        )
        
        return {
            "success": True,
            "processed": result is not None,
            "order_code": result
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/admin/webhook/status")
async def webhook_status(request: Request, auth: bool = Depends(require_admin)):
    """Kiểm tra trạng thái webhook"""
    return {
        "sepay_url": "/webhook/sepay",
        "casso_url": "/webhook/casso",
        "status": "ready",
        "pending_orders": len(get_pending_orders()),
        "recent_transactions": len(get_recent_transactions(10))
    }

# ================= TELEGRAM WEBHOOK =================
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Nhận updates từ Telegram qua webhook"""
    try:
        update = types.Update(**await request.json())
        await dp.feed_update(bot, update)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return {"ok": False, "error": str(e)}

# ================= STARTUP =================
@app.on_event("startup")
async def on_startup():
    global auto_check_active, auto_check_task, order_timeout_task, bot

    logger.info("Starting bot...")

    if not BOT_TOKEN:
        logger.warning("⚠️  BOT_TOKEN chưa được cấu hình!")
        logger.warning("   → Vào /admin → Cài đặt → Nhập BOT_TOKEN để kích hoạt bot.")
        logger.info("   Web server và Admin Panel vẫn hoạt động bình thường.")
    else:
        # Khởi tạo bot nếu chưa có
        if bot is None:
            bot = Bot(token=BOT_TOKEN)

        # Chọn mode: Webhook hoặc Polling
        if USE_WEBHOOK and WEBAPP_HOST:
            # Webhook mode (Production)
            webhook_url = f"https://{WEBAPP_HOST}{WEBHOOK_PATH}"
            try:
                await bot.set_webhook(webhook_url)
                logger.info(f"Webhook set: {webhook_url}")
            except Exception as e:
                logger.error(f"Failed to set webhook: {e}")
        else:
            # Polling mode (Development)
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("Deleted old webhook, starting polling...")
            except Exception as e:
                logger.warning(f"Could not delete webhook: {e}")

            # Start polling với error handling
            async def safe_polling():
                while True:
                    try:
                        if bot:
                            await dp.start_polling(bot, drop_pending_updates=True)
                    except Exception as e:
                        logger.error(f"Polling error: {e}")
                        await asyncio.sleep(5)

            asyncio.create_task(safe_polling())

    # Bật auto check (hoạt động kể cả khi chưa có bot token)
    auto_check_active = True
    auto_check_task = asyncio.create_task(auto_check_loop())

    # Bật task hủy đơn hết hạn
    order_timeout_task = asyncio.create_task(order_timeout_loop())

    logger.info(f"Server started! Auto check: ON | Bot: {'✅ Active' if BOT_TOKEN else '⚠️  No token'}")

@app.on_event("shutdown")
async def on_shutdown():
    global auto_check_active
    auto_check_active = False

    if bot:
        try:
            if USE_WEBHOOK:
                await bot.delete_webhook()
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass

