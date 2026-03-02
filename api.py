#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# region [IMPORTS]

from gevent import monkey
monkey.patch_all()
import os
import json
import time
import secrets
import hashlib
import sys
import uuid
import threading
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple
from flask import Flask, request, jsonify, Response, stream_with_context, send_from_directory, make_response, g
from flask_cors import CORS
from flask_sock import Sock as FlaskSock
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse
from gevent import spawn, joinall
from dotenv import load_dotenv
from gevent.queue import Queue as GQueue
from gevent import spawn as gspawn

_env_path = Path(__file__).parent / '.env'
if _env_path.exists():
    load_dotenv(_env_path)

# endregion

# region [统一日志系统]

_log_queue   = GQueue()
_log_started = False

def _log_worker():
    while True:
        batch = []
        batch.append(_log_queue.get())
        while not _log_queue.empty() and len(batch) < 50:
            batch.append(_log_queue.get_nowait())
        try:
            conn = db()
            cur = conn.cursor()
            for content in batch:
                table = "log_info" if "][info][" in content else "log_error"
                try:
                    ts = content.split("]")[0].lstrip("[")
                except:
                    ts = now_iso()
                cur.execute(
                    f"INSERT INTO {table} (timestamp, source, source_type, content) VALUES (%s, %s, %s, %s)",
                    (ts, "api", "api", content)
                )
            conn.commit()
            conn.close()
        except Exception:
            pass

def log(content: str):
    global _log_started
    print(content)
    if not _log_started:
        _log_started = True
        gspawn(_log_worker)
    _log_queue.put_nowait(content)

def log_access(user_id=None, username=None, user_type='user', endpoint=None, method=None, status_code=200, detail=None):
    """记录访问日志到数据库"""    
    try:
        ip_address = request.remote_addr if hasattr(request, 'remote_addr') else None
        user_agent = request.headers.get('User-Agent', '')[:500] if hasattr(request, 'headers') else ''
        referer = request.headers.get('Referer', '')[:500] if hasattr(request, 'headers') else ''
        
        def _write():
            try:
                conn = db()
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO access_logs(user_id, username, user_type, ip_address, user_agent, endpoint, method, status_code, referer, detail)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (user_id, username, user_type, ip_address, user_agent, endpoint, method, status_code, referer, json.dumps(detail) if detail else None))
                conn.commit()
                conn.close()
            except:
                pass
        
        gspawn(_write)
    except:
        pass

# 禁用werkzeug的HTTP访问日志，避免刷屏

werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)

# endregion

# region [DB & UTILS]

app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app)
sock = FlaskSock(app)
BASE_DIR = Path(__file__).resolve().parent.parent
_DB_READY = False
_DB_INIT_LOCK = threading.Lock()
_frontend_clients = {}  
_task_subscribers = {}
_worker_clients = {}  
_worker_lock = threading.Lock()
_worker_sched_cache = {}
_frontend_lock = threading.Lock()
_task_tracker = {}
_shard_to_task = {}
_task_tracker_lock = threading.Lock()


def _sched_default():
    return {
        "state": "ready",
        "strikes": 0,
        "hb_ok_seq": 0,
        "quarantine_until": 0,
        "last_reason": "",
        "updated_at": int(time.time())
    }


def _get_worker_sched(server_id: str) -> dict:
    if not server_id:
        return _sched_default()
    key = f"worker_sched:{server_id}"
    try:
        if redis_manager.use_redis:
            raw = redis_manager.client.hgetall(key) or {}
            if raw:
                return {
                    "state": str(raw.get("state") or "ready"),
                    "strikes": int(raw.get("strikes") or 0),
                    "hb_ok_seq": int(raw.get("hb_ok_seq") or 0),
                    "quarantine_until": int(raw.get("quarantine_until") or 0),
                    "last_reason": str(raw.get("last_reason") or ""),
                    "updated_at": int(raw.get("updated_at") or int(time.time()))
                }
    except Exception:
        pass
    with _worker_lock:
        return dict(_worker_sched_cache.get(server_id) or _sched_default())


def _set_worker_sched(server_id: str, state: dict):
    if not server_id:
        return
    data = dict(_sched_default())
    data.update(state or {})
    data["updated_at"] = int(time.time())
    with _worker_lock:
        _worker_sched_cache[server_id] = data
    try:
        if redis_manager.use_redis:
            key = f"worker_sched:{server_id}"
            redis_manager.client.hset(key, mapping={
                "state": data["state"],
                "strikes": int(data["strikes"]),
                "hb_ok_seq": int(data["hb_ok_seq"]),
                "quarantine_until": int(data["quarantine_until"]),
                "last_reason": data["last_reason"],
                "updated_at": int(data["updated_at"])
            })
            redis_manager.client.expire(key, 7 * 24 * 3600)
    except Exception:
        pass


def _is_worker_assignable(server_id: str) -> bool:
    st = _get_worker_sched(server_id)
    return str(st.get("state") or "ready") == "ready"


def _mark_worker_quarantine(server_id: str, reason: str = "stuck"):
    if not server_id:
        return
    now_ts = int(time.time())
    quarantine_seconds = int(os.environ.get("WORKER_QUARANTINE_SECONDS", "60"))
    fault_after = int(os.environ.get("WORKER_FAULT_AFTER_STRIKES", "2"))
    st = _get_worker_sched(server_id)
    strikes = int(st.get("strikes") or 0) + 1
    if strikes >= fault_after:
        _set_worker_sched(server_id, {
            "state": "fault",
            "strikes": strikes,
            "hb_ok_seq": 0,
            "quarantine_until": 0,
            "last_reason": reason or "fault"
        })
    else:
        _set_worker_sched(server_id, {
            "state": "quarantine",
            "strikes": strikes,
            "hb_ok_seq": 0,
            "quarantine_until": now_ts + quarantine_seconds,
            "last_reason": reason or "quarantine"
        })


def _on_worker_heartbeat_ok(server_id: str):
    if not server_id:
        return
    st = _get_worker_sched(server_id)
    state = str(st.get("state") or "ready")
    if state == "fault":
        return
    if state != "quarantine":
        if state != "ready":
            _set_worker_sched(server_id, {"state": "ready"})
        return
    now_ts = int(time.time())
    until_ts = int(st.get("quarantine_until") or 0)
    if now_ts < until_ts:
        return
    hb_ok = int(st.get("hb_ok_seq") or 0) + 1
    recover_heartbeats = int(os.environ.get("WORKER_RECOVER_HEARTBEATS", "3"))
    if hb_ok >= recover_heartbeats:
        _set_worker_sched(server_id, {
            "state": "ready",
            "hb_ok_seq": 0,
            "quarantine_until": 0,
            "last_reason": "recovered"
        })
    else:
        _set_worker_sched(server_id, {
            "state": "quarantine",
            "hb_ok_seq": hb_ok
        })


def _send_worker_ws(server_id: str, payload: dict) -> bool:
    if not server_id or not isinstance(payload, dict):
        return False
    with _worker_lock:
        client = _worker_clients.get(server_id)
        if not client or not client.get("ws"):
            return False
        ws = client.get("ws")
    try:
        ws.send(json.dumps(payload))
        return True
    except Exception:
        return False


def _require_env(name: str) -> str:
    """获取必需环境变量"""
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


# 获取数据库连接
from psycopg2 import pool
from psycopg2 import extensions

# Database Connection Pool
_db_pool = None

def _init_db_pool():
    global _db_pool
    if _db_pool is None:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
             # Default fallback for local development
            default_db_config = {
                "host": os.environ.get("DB_HOST", "localhost"),
                "port": os.environ.get("DB_PORT", "5555"),
                "database": os.environ.get("DB_NAME", "autosender"),
                "user": os.environ.get("DB_USER", "autosender"), 
                "password": os.environ.get("DB_PASSWORD","autosender123")
             }
            if not default_db_config.get("password"):
                log(f"[{now_iso()}][API][error][176][_init_db_pool][数据库密码未设置]")
            database_url = f"postgresql://{default_db_config['user']}:{default_db_config['password']}@{default_db_config['host']}:{default_db_config['port']}/{default_db_config['database']}"
        else:
            # 兼容某些平台提供的 postgres:// URL（libpq/psycopg2 在部分环境可能不接受）
            if database_url.startswith("postgres://"):
                database_url = "postgresql://" + database_url[len("postgres://"):]
        
        try:
            # Create a thread-safe connection pool
            _db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, database_url)
            
            # [CRITICAL] 连接池建立后，立即执行数据库表结构初始化
            init_db()
            
        except Exception as e:
            log(f"[{now_iso()}][API][erro][191][_init_db_pool][数据库连接池初始化失败]")
            raise

class PooledConnectionWrapper:
    """Wrapper to return connection to pool on close() instead of closing it."""
    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn
        self._closed = False

    def close(self):
        if not self._closed and self._conn:
            # 连接池复用连接：若上一次事务出错且未 rollback，会导致后续请求出现
            # "current transaction is aborted"。这里在归还连接前做一次清理。
            try:
                tx_status = self._conn.get_transaction_status()
                if tx_status != extensions.TRANSACTION_STATUS_IDLE:
                    self._conn.rollback()
                self._pool.putconn(self._conn)
            except Exception:
                # rollback 或 putconn 异常时，丢弃该连接避免污染连接池
                try:
                    self._pool.putconn(self._conn, close=True)
                except Exception:
                    pass
            finally:
                self._closed = True
    
    def __getattr__(self, name):
        return getattr(self._conn, name)

def db():
    global _db_pool
    if _db_pool is None:
        _init_db_pool()
    
    try:
        conn = _db_pool.getconn()
        return PooledConnectionWrapper(_db_pool, conn)
    except Exception as e:
        log(f"[{now_iso()}][API][erro][231][db][获取数据库连接失败]")
        raise RuntimeError(f"Database connection failure: {e}") from e


def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def gen_id(prefix: str) -> str:
    """生成带前缀的4位短ID（人类可读）"""
    # 用户ID使用4位纯数字（0000-9999），无前缀
    if prefix == "u":
        short_id = ''.join(secrets.choice("0123456789") for _ in range(4))
        return short_id  # 返回纯4位数字，无前缀
    # 其他ID使用数字和大写字母，排除容易混淆的字符（0,O,1,I,L）
    chars = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"
    short_id = ''.join(secrets.choice(chars) for _ in range(4))
    return f"{prefix}_{short_id}"


def hash_pw(pw: str, salt: str = "") -> str:
    """密码哈希 (PBKDF2+Salt)"""
    if not salt:
        # 为了兼容旧代码或临时调用，暂时允许空salt，但在注册/登录逻辑中必须强制使用
        return hashlib.sha256((pw or "").encode("utf-8")).hexdigest()
        
    return hashlib.pbkdf2_hmac(
        'sha256',
        (pw or "").encode('utf-8'),
        salt.encode('utf-8'),
        100000
    ).hex()


def hash_token(token: str) -> str:
    """Token哈希"""
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()


def _json() -> Dict[str, Any]:
    """获取请求JSON"""
    return request.get_json(silent=True) or {}


def _bearer_token() -> Optional[str]:
    """获取Bearer Token"""
    auth = request.headers.get("Authorization", "")
    if not auth:
        return None
    parts = auth.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


def _get_setting(cur, key: str) -> Optional[str]:
    """获取设置项"""
    cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
    row = cur.fetchone()
    if not row:
        return None
    return row.get("value") if isinstance(row, dict) else row[0]


def _set_setting(cur, key: str, value: str) -> None:
    """设置设置项"""
    cur.execute("INSERT INTO settings(key, value) VALUES(%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, value))


def _verify_user_token(conn, user_id: str, token: str) -> bool:
    """验证用户Token（检查是否过期）"""
    if not user_id or not token:
        return False
    th = hash_token(token)
    cur = conn.cursor()
    # 🔥 token 不过期：只校验是否存在
    cur.execute("SELECT 1 FROM user_tokens WHERE user_id=%s AND token_hash=%s", (user_id, th))
    ok = cur.fetchone() is not None
    if ok:
        # 同步 last_used，并确保 expires_at 为空（兼容旧数据）
        try:
            cur.execute("UPDATE user_tokens SET last_used=NOW(), expires_at=NULL WHERE user_id=%s AND token_hash=%s", (user_id, th))
        except Exception:
            cur.execute("UPDATE user_tokens SET last_used=NOW() WHERE user_id=%s AND token_hash=%s", (user_id, th))
        conn.commit()
    return ok

def _verify_admin_token(conn, admin_id_or_token: str, token: str = None) -> Optional[str]:
    """验证管理员Token（检查是否过期）
    支持两种调用方式:
    1. _verify_admin_token(conn, admin_id, token) - 验证指定管理员
    2. _verify_admin_token(conn, token) - 从Token查找并验证管理员 (此时admin_id_or_token为token)
    """
    if token is None:
        # 方式2: 只传入了token
        token = admin_id_or_token
        admin_id = None
    else:
        # 方式1: 传入了admin_id和token
        admin_id = admin_id_or_token

    if not token:
        return None
        
    th = hash_token(token)
    cur = conn.cursor()
    
    if admin_id:
        # 验证指定管理员
        cur.execute("SELECT 1 FROM admin_tokens WHERE admin_id=%s AND token_hash=%s AND (expires_at IS NULL OR expires_at > NOW())", (admin_id, th))
        ok = cur.fetchone() is not None
        if ok:
            cur.execute("UPDATE admin_tokens SET last_used=NOW() WHERE admin_id=%s AND token_hash=%s", (admin_id, th))
            conn.commit()
            log(f"[{now_iso()}][API][info][357][_verify_admin_token][管理员Token验证成功]")
        return admin_id if ok else None
    else:
        # 从Token查找管理员
        cur.execute("SELECT admin_id FROM admin_tokens WHERE token_hash=%s AND (expires_at IS NULL OR expires_at > NOW()) LIMIT 1", (th,))
        row = cur.fetchone()
        found_admin_id = row[0] if row else None
        
        if found_admin_id:
            cur.execute("UPDATE admin_tokens SET last_used=NOW() WHERE admin_id=%s AND token_hash=%s", (found_admin_id, th))
            conn.commit()
            log(f"[{now_iso()}][API][info][368][_verify_admin_token][管理员Token验证成功]")
            return found_admin_id
        return None

def _maybe_authed_user(conn) -> Optional[str]:
    """尝试从Token获取用户ID"""
    token = _bearer_token()
    if not token:
        return None
    th = hash_token(token)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT user_id FROM user_tokens WHERE token_hash=%s ORDER BY created DESC LIMIT 1", (th,))
    row = cur.fetchone()
    return row["user_id"] if row else None

def _verify_server_manager_token(conn, token: str) -> bool:
    """验证服务器管理员Token（最高权限）"""
    if not token:
        return False
    th = hash_token(token)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM server_manager_tokens WHERE token_hash=%s LIMIT 1", (th,))
    ok = cur.fetchone() is not None
    if ok:
        cur.execute("UPDATE server_manager_tokens SET last_used=NOW() WHERE token_hash=%s", (th,))
        conn.commit()
    return ok
# endregion

# region [DB INIT]
# 初始化数据库表
def init_db() -> None:
    conn = db()
    try:
        cur = conn.cursor()

        if os.environ.get("RESET_DB", "").strip() == "1":
            cur.execute("DROP TABLE IF EXISTS users CASCADE")
            cur.execute("DROP TABLE IF EXISTS user_data CASCADE")
            cur.execute("DROP TABLE IF EXISTS user_tokens CASCADE")
            cur.execute("DROP TABLE IF EXISTS admins CASCADE")
            cur.execute("DROP TABLE IF EXISTS admin_tokens CASCADE")
            cur.execute("DROP TABLE IF EXISTS admin_configs CASCADE")
            cur.execute("DROP TABLE IF EXISTS server_manager_tokens CASCADE")
            cur.execute("DROP TABLE IF EXISTS settings CASCADE")
            cur.execute("DROP TABLE IF EXISTS servers CASCADE")
            cur.execute("DROP TABLE IF EXISTS tasks CASCADE")
            cur.execute("DROP TABLE IF EXISTS shards CASCADE")
            cur.execute("DROP TABLE IF EXISTS reports CASCADE")
            cur.execute("DROP TABLE IF EXISTS conversations CASCADE")
            cur.execute("DROP TABLE IF EXISTS sent_records CASCADE")
            cur.execute("DROP TABLE IF EXISTS id_library CASCADE")

        cur.execute("""CREATE TABLE IF NOT EXISTS users(user_id VARCHAR PRIMARY KEY, username VARCHAR UNIQUE NOT NULL, pw_hash VARCHAR NOT NULL, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, created_by_admin VARCHAR)""")
        try:
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_by_admin VARCHAR")
        except:
            pass
        cur.execute("""CREATE TABLE IF NOT EXISTS user_data(user_id VARCHAR PRIMARY KEY, credits NUMERIC DEFAULT 1000, stats JSONB DEFAULT '[]'::jsonb, usage JSONB DEFAULT '[]'::jsonb, inbox JSONB DEFAULT '[]'::jsonb, FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE)""")
        try:
            cur.execute("ALTER TABLE user_data ADD COLUMN IF NOT EXISTS rates JSONB")
        except:
            pass
        try:
            cur.execute("ALTER TABLE user_data ADD COLUMN IF NOT EXISTS admin_rate_set_by VARCHAR")
        except:
            pass
        # 创建费率修改历史记录表
        cur.execute("""CREATE TABLE IF NOT EXISTS rate_change_logs(
            id SERIAL PRIMARY KEY,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            operator_type VARCHAR NOT NULL,
            operator_id VARCHAR NOT NULL,
            target_user_id VARCHAR,
            target_admin_id VARCHAR,
            old_rates JSONB,
            new_rates JSONB,
            old_rate_range JSONB,
            new_rate_range JSONB,
            reason VARCHAR
        )""")
        try:
             cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS salt VARCHAR")
        except:
             pass
        try:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='user_tokens' AND column_name='expires_at'")
            if not cur.fetchone():
                log(f"[{now_iso()}][API][info][486][init_db][user_tokens表缺少expires_at字段 正在重建]")
                cur.execute("DROP TABLE IF EXISTS user_tokens CASCADE")
                cur.execute("""CREATE TABLE user_tokens(
                    token_hash VARCHAR PRIMARY KEY, 
                    user_id VARCHAR NOT NULL, 
                    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                    last_used TIMESTAMP, 
                    expires_at TIMESTAMP, 
                    FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )""")
        except Exception as e:
            log(f"[{now_iso()}][API][erro][497][init_db][修复user_tokens表失败]")

        cur.execute("""CREATE TABLE IF NOT EXISTS user_tokens(token_hash VARCHAR PRIMARY KEY, user_id VARCHAR NOT NULL, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_used TIMESTAMP, expires_at TIMESTAMP, FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE)""")
        try:
            cur.execute("ALTER TABLE user_tokens ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP")
        except:
            pass
        cur.execute("""CREATE TABLE IF NOT EXISTS admins(admin_id VARCHAR PRIMARY KEY, pw_hash VARCHAR NOT NULL, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        try:
             cur.execute("ALTER TABLE admins ADD COLUMN IF NOT EXISTS salt VARCHAR")
        except:
             pass
        cur.execute("""CREATE TABLE IF NOT EXISTS admin_tokens(token_hash VARCHAR PRIMARY KEY, admin_id VARCHAR NOT NULL, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_used TIMESTAMP, expires_at TIMESTAMP, FOREIGN KEY(admin_id) REFERENCES admins(admin_id) ON DELETE CASCADE)""")
        try:
            cur.execute("ALTER TABLE admin_tokens ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP")
        except:
            pass
        cur.execute("""CREATE TABLE IF NOT EXISTS admin_configs(admin_id VARCHAR PRIMARY KEY, selected_servers JSONB DEFAULT '[]'::jsonb, user_groups JSONB DEFAULT '[]'::jsonb, settled_performance DOUBLE PRECISION DEFAULT 0, updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(admin_id) REFERENCES admins(admin_id) ON DELETE CASCADE)""")
        try:
            cur.execute("ALTER TABLE admin_configs ADD COLUMN IF NOT EXISTS rates JSONB")
        except:
            pass
        try:
            cur.execute("ALTER TABLE admin_configs ADD COLUMN IF NOT EXISTS rate_range JSONB")
        except:
            pass
        try:
            cur.execute("ALTER TABLE admin_configs ADD COLUMN IF NOT EXISTS settled_performance DOUBLE PRECISION DEFAULT 0")
        except:
            pass
        cur.execute("""CREATE TABLE IF NOT EXISTS server_manager_tokens(token_hash VARCHAR PRIMARY KEY, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_used TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS settings(key VARCHAR PRIMARY KEY, value TEXT)""")
        
        cur.execute("SELECT value FROM settings WHERE key='server_manager_pw_hash'")
        if not cur.fetchone():
             salt = secrets.token_hex(8)
             default_hash = hash_pw("autosender", salt)
             cur.execute("INSERT INTO settings(key, value) VALUES(%s, %s)", ("server_manager_pw_hash", f"{salt}${default_hash}"))
             log(f"[{now_iso()}][API][info][538][init_db][已初始化服务器管理默认密码]")
        
        cur.execute("""CREATE TABLE IF NOT EXISTS servers(server_id VARCHAR PRIMARY KEY, server_name VARCHAR, server_url TEXT, port INT, clients_count INT DEFAULT 0, status VARCHAR DEFAULT 'disconnected', last_seen TIMESTAMP, registered_at TIMESTAMP, registry_id VARCHAR, meta JSONB DEFAULT '{}'::jsonb, assigned_user VARCHAR, assigned_by_admin VARCHAR, FOREIGN KEY(assigned_user) REFERENCES users(user_id) ON DELETE SET NULL)""")
        try:
            cur.execute("ALTER TABLE servers ADD COLUMN IF NOT EXISTS assigned_by_admin VARCHAR")
        except:
            pass
        cur.execute("""CREATE TABLE IF NOT EXISTS tasks(task_id VARCHAR PRIMARY KEY, user_id VARCHAR NOT NULL, message TEXT NOT NULL, total INT, count INT, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status VARCHAR DEFAULT 'pending', FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS shards(shard_id VARCHAR PRIMARY KEY, task_id VARCHAR NOT NULL, server_id VARCHAR, phones JSONB NOT NULL, status VARCHAR DEFAULT 'pending', attempts INT DEFAULT 0, locked_at TIMESTAMP, updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, result JSONB DEFAULT '{}'::jsonb, FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE, FOREIGN KEY(server_id) REFERENCES servers(server_id) ON DELETE SET NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS reports(report_id SERIAL PRIMARY KEY, shard_id VARCHAR, server_id VARCHAR, user_id VARCHAR, success INT, fail INT, sent INT, credits NUMERIC, detail JSONB, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS conversations(user_id VARCHAR NOT NULL, chat_id VARCHAR NOT NULL, meta JSONB DEFAULT '{}'::jsonb, messages JSONB DEFAULT '[]'::jsonb, updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, chat_id), FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS sent_records(id SERIAL PRIMARY KEY, user_id VARCHAR NOT NULL, phone_number VARCHAR, task_id VARCHAR, detail JSONB DEFAULT '{}'::jsonb, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS id_library(apple_id VARCHAR PRIMARY KEY, password VARCHAR NOT NULL, status VARCHAR DEFAULT 'normal', usage_status VARCHAR DEFAULT 'new', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

        # 统一日志表
        cur.execute("""CREATE TABLE IF NOT EXISTS log_info (
            id          SERIAL PRIMARY KEY,
            timestamp   TEXT NOT NULL,
            source      TEXT NOT NULL,
            source_type TEXT NOT NULL,
            content     TEXT NOT NULL
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS log_error (
            id          SERIAL PRIMARY KEY,
            timestamp   TEXT NOT NULL,
            source      TEXT NOT NULL,
            source_type TEXT NOT NULL,
            content     TEXT NOT NULL
        )""")
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_log_info_ts  ON log_info  (timestamp)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_log_error_ts ON log_error (timestamp)")
        except:
            pass

        # 访问记录表：记录所有访问详情
        cur.execute("""CREATE TABLE IF NOT EXISTS access_logs(
            id SERIAL PRIMARY KEY,
            user_id VARCHAR,
            username VARCHAR,
            user_type VARCHAR DEFAULT 'user',
            ip_address VARCHAR,
            user_agent TEXT,
            endpoint VARCHAR,
            method VARCHAR,
            status_code INT,
            referer TEXT,
            detail JSONB DEFAULT '{}'::jsonb,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_access_logs_ts ON access_logs(ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_access_logs_user ON access_logs(user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_access_logs_ip ON access_logs(ip_address)")
        except:
            pass

        # 🧹 启动大扫除：清理僵尸服务器
        try:
            cur.execute("DELETE FROM servers WHERE last_seen < NOW() - INTERVAL '3 days'")
            cur.execute("UPDATE servers SET status = 'disconnected' WHERE status IN ('connected', 'online') AND last_seen < NOW() - INTERVAL '10 minutes'")
        except Exception as e:
            log(f"[{now_iso()}][API][info][632][init_db][自清理失败]")

        conn.commit()
    except Exception as e:
        log(f"[{now_iso()}][API][erro][636][init_db][数据库初始化错误]")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

# endregion

# region [REDIS UTILS]
# 导入统一的Redis管理器
from redis_manager import redis_manager
# endregion

# region [STARTUP INIT]
# 应用启动时的初始化（数据库、Redis等）
def startup_init():
    global _DB_READY
    
    # 1. 初始化数据库
    try:
        init_db()
        _DB_READY = True
    except Exception as e:
        log(f"[{now_iso()}][API][erro][660][startup_init][数据库初始化失败]")
        import traceback
        traceback.print_exc()
        _DB_READY = False
    
    # 2. 验证Redis连接
    try:
        if redis_manager.use_redis:
            redis_manager.client.ping()
        else:
            log(f"[{now_iso()}][API][info][670][startup_init][Redis未配置 使用内存模式]")
            # 生产环境警告
            if os.environ.get("ENV") == "production":
                log(f"[{now_iso()}][API][info][673][startup_init][生产环境未配置Redis]")
    except Exception as e:
        log(f"[{now_iso()}][API][info][675][startup_init][Redis连接失败 使用内存模式]")
        import traceback
        traceback.print_exc()

# 在应用启动时执行初始化（Flask 2.2+ 使用 before_request 或直接调用）
# 对于 gunicorn，模块加载时会执行
startup_init()
# endregion

# region [HEALTH]   
# 根路由 - 提供前端HTML文件
@app.route("/")
def root():
    log(f"[{now_iso()}][API][info][589][root][通过API访问]")
    # index.html 在 API 目录下
    api_dir = Path(__file__).resolve().parent
    response = make_response(send_from_directory(api_dir, 'index.html'))
    # 禁止缓存
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

# 提供静态文件（字体、图片等），排除API路径
@app.route("/<path:filename>")
def static_files(filename):
    # 排除API路径
    if filename.startswith('api/'):
        return jsonify({"error": "Not found"}), 404
    
    api_dir = Path(__file__).resolve().parent
    file_path = api_dir / filename
    if file_path.exists() and file_path.is_file():
        response = make_response(send_from_directory(api_dir, filename))
        # 对HTML文件禁止缓存
        if filename.endswith('.html'):
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
        # 对静态资源（字体、CSS、JS、图片等）设置长期缓存（1年）
        elif filename.endswith(('.ttf', '.woff', '.woff2', '.eot', '.otf')) or \
             filename.endswith(('.css', '.js')) or \
             filename.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp')):
            # 设置长期缓存：1年（31536000秒）
            response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
            # 使用UTC时间设置Expires头
            expires_time = datetime.now(timezone.utc) + timedelta(days=365)
            response.headers['Expires'] = expires_time.strftime('%a, %d %b %Y %H:%M:%S GMT')
        return response
    else:
        # 文件不存在时返回404，避免阻塞
        return jsonify({"error": "File not found"}), 404

# API根路由
@app.route("/api")
def api_root():
    log(f"[{now_iso()}][API][info][632][api_root][API根路由被访问]")
    return jsonify({"ok": True, "name": "AutoSender API", "status": "running", "timestamp": now_iso()})

# 确保数据库已初始化（线程安全）
def _ensure_db_initialized():
    global _DB_READY
    if not _DB_READY:
        with _DB_INIT_LOCK:
            if not _DB_READY:  # Double-check locking
                try:
                    log(f"[{now_iso()}][API][info][642][_ensure_db_initialized][首次请求 初始化数据库]")
                    init_db()
                    _DB_READY = True
                    log(f"[{now_iso()}][API][info][645][_ensure_db_initialized][数据库初始化成功]")
                except Exception as e:
                    log(f"[{now_iso()}][API][erro][647][_ensure_db_initialized][数据库初始化失败]")
                    import traceback
                    traceback.print_exc()
                    raise

# 健康检查
@app.route("/api/health")
def health():
    conn = None
    try:
        # 确保数据库已初始化
        _ensure_db_initialized()
        # 测试数据库连接
        conn = db()
        conn.close()
        conn = None
        db_status = "connected"
    except Exception as e:
        log(f"[{now_iso()}][API][erro][665][health][数据库连接失败]")
        db_status = f"error: {str(e)}"
        if conn:
            try: conn.close()
            except: pass
    
    return jsonify({
        "ok": True, 
        "status": "healthy", 
        "database": db_status,
        "timestamp": now_iso()
    })

@app.route("/health")
def healthcheck():
    return jsonify({"status": "ok"})

# 数据库状态诊断
@app.route("/api/debug/db-status", methods=["GET"])
def debug_db_status():
    try:
        conn = db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 检查所有表是否存在
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [row["table_name"] for row in cur.fetchall()]
        
        # 检查各表行数
        table_counts = {}
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                count = cur.fetchone()["cnt"]
                table_counts[table] = count
            except:
                table_counts[table] = "error"
        
        # 检查admins表
        cur.execute("SELECT admin_id, created FROM admins")
        admins = cur.fetchall()
        
        # 检查users表
        cur.execute("SELECT user_id, username, created FROM users")
        users = cur.fetchall()
        
        conn.close()
        
        return jsonify({
            "ok": True,
            "tables": tables,
            "table_counts": table_counts,
            "admins": admins,
            "users": users,
            "message": f"数据库连接正常，共{len(tables)}个表"
        })
        
    except Exception as e:
        if 'conn' in dir():
            try: conn.close()
            except: pass
        return jsonify({
            "ok": False,
            "error": str(e),
            "message": "数据库连接失败"
        }), 500

# 查看Redis状态
@app.route("/api/debug/redis", methods=["GET"])
def debug_redis():
    # 🔥 快速失败，不阻塞
    try:
        online = redis_manager.get_online_workers()
    except Exception as e:
        log(f"[{now_iso()}][API][info][843][debug_redis][获取在线Worker列表失败]")
        online = []
    workers = []
    
    for worker_id in online:
        load = redis_manager.get_worker_load(worker_id)
        workers.append({
            "server_id": worker_id,
            "load": load,
            "online": True
        })
    
    return jsonify({
        "ok": True,
        "use_redis": redis_manager.use_redis,
        "online_workers": len(online),
        "workers": workers
    })


# 连接池状态诊断
@app.route("/api/debug/pool", methods=["GET"])
def debug_pool():
    """诊断数据库连接池状态"""
    try:
        pool_status = {
            "pool_type": "ThreadedConnectionPool",
            "min_connections": 1,
            "max_connections": 20,
            "pool_available": None,
            "pool_size": None
        }
        
        # 尝试获取连接池信息
        global _pool
        if _pool:
            try:
                # ThreadedConnectionPool 有 _used 和 _pool 属性
                used_count = len(_pool._used) if hasattr(_pool, '_used') else 'unknown'
                available_count = len(list(_pool._pool.queue)) if hasattr(_pool, '_pool') and hasattr(_pool._pool, 'queue') else 'unknown'
                pool_status["pool_available"] = available_count
                pool_status["pool_used"] = used_count
                pool_status["pool_size"] = used_count if isinstance(used_count, int) else 0
                if isinstance(used_count, int) and isinstance(available_count, int):
                    pool_status["pool_total"] = used_count + available_count
            except Exception as e:
                pool_status["error"] = str(e)
        
        # 测试获取一个连接
        test_conn = None
        try:
            test_conn = db()
            pool_status["can_get_connection"] = True
        except Exception as e:
            pool_status["can_get_connection"] = False
            pool_status["connection_error"] = str(e)
        finally:
            if test_conn:
                try:
                    test_conn.close()
                except:
                    pass
        
        return jsonify({
            "ok": True,
            "pool": pool_status,
            "timestamp": __import__('time').time()
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500

# endregion

# region [USER AUTH]
# 签发/复用用户Token（不自动过期：1小时门禁由前端控制）
def _issue_user_token(conn, user_id: str) -> str:
    """
    签发用户Token
    - 每次调用生成新 Token
    - 数据库只存 hash (expires_at=NULL, 永不过期)
    - 返回明文 Token 由前端保存
    """
    token = secrets.token_urlsafe(24)
    th = hash_token(token)
    
    # [FIX] Explicitly initialize cursor outside try block to avoid NameError
    try:
        cur = conn.cursor()
    except Exception as e:
        log(f"[{now_iso()}][API][erro][18][_issue_user_token][创建游标失败]")
        raise e

    try:
        # 3) 写入/刷新 hash 记录（不设过期）
        cur.execute(
            "INSERT INTO user_tokens(token_hash, user_id, last_used, expires_at) VALUES(%s,%s,NOW(),NULL) "
            "ON CONFLICT (token_hash) DO UPDATE SET user_id=EXCLUDED.user_id, last_used=NOW(), expires_at=NULL",
            (th, user_id),
        )
    except Exception as e:
        # 兼容某些旧 schema（没有 expires_at 或冲突规则差异）
        log(f"[{now_iso()}][API][erro][30][_issue_user_token][Token插入失败尝试旧schema]")
        try:
            conn.rollback() # Ensure transaction is reset
            cur.execute(
                "INSERT INTO user_tokens(token_hash, user_id, last_used) VALUES(%s,%s,NOW()) "
                "ON CONFLICT (token_hash) DO UPDATE SET user_id=EXCLUDED.user_id, last_used=NOW()",
                (th, user_id),
            )
        except Exception as retry_e:
            # [FIX] 不再静默失败，而是记录错误并抛出，以便排查
            log(f"[{now_iso()}][API][erro][40][_issue_user_token][Token写入数据库最终失败]")
            raise retry_e

    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"[{now_iso()}][API][erro][47][_issue_user_token][Token提交失败]")
        raise e
        
    return token


# 用户注册/服务器注册
@app.route("/api/register", methods=["POST", "OPTIONS"])
def register():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()

    if ("username" not in d) and ("url" in d) and ("name" in d or "server_name" in d):
        name = (d.get("name") or d.get("server_name") or "server").strip()
        url = (d.get("url") or "").strip()
        port = d.get("port")
        clients_count = int(d.get("clients_count") or d.get("clients") or 0)
        status = (d.get("status") or "online").strip().lower()

        conn = db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        registry_id = gen_id("reg")
        server_id = d.get("server_id") or gen_id("server")

        cur.execute("""INSERT INTO servers(server_id, server_name, server_url, port, clients_count, status, last_seen, registered_at, registry_id, meta) VALUES(%s,%s,%s,%s,%s,%s,NOW(),NOW(),%s,%s) ON CONFLICT (server_id) DO UPDATE SET server_name=EXCLUDED.server_name, server_url=EXCLUDED.server_url, port=EXCLUDED.port, clients_count=EXCLUDED.clients_count, status=EXCLUDED.status, last_seen=NOW()""", (server_id, name, url, port, clients_count, _normalize_server_status(status, clients_count), registry_id, json.dumps(d)))
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "success": True, "id": registry_id, "server_id": server_id})

    username = (d.get("username") or "").strip()
    pw = (d.get("password") or "").strip()

    if not username:
        return jsonify({"ok": False, "success": False, "message": "用户名不能为空"}), 400
    if not pw:
        return jsonify({"ok": False, "success": False, "message": "密码不能为空"}), 400
    if len(pw) < 4:
        return jsonify({"ok": False, "success": False, "message": "密码至少需要4位"}), 400

    conn = None
    try:
        conn = db()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 检查用户名是否已存在
        cur.execute("SELECT 1 FROM users WHERE username=%s", (username,))
        if cur.fetchone():
            conn.close()
            return jsonify({"ok": False, "success": False, "message": "用户名已存在"}), 409

        uid = gen_id("u")

        # 核心优化：频率限制
        client_ip = request.remote_addr
        limit_key = f"rate_limit:register:{client_ip}"
        if redis_manager.use_redis:
            try:
                count = redis_manager.client.incr(limit_key)
                if count == 1:
                    redis_manager.client.expire(limit_key, 60)
                if count > 3:  # 同一IP每分钟最多注册3次
                    conn.close()
                    return jsonify({"ok": False, "success": False, "message": "请求过于频繁，请稍后再试"}), 429
            except Exception as e:
                log(f"[{now_iso()}][API][erro][113][register][频率限制检查失败]")

        # 插入用户数据
        salt = secrets.token_hex(16)
        cur.execute("INSERT INTO users(user_id,username,pw_hash,salt) VALUES(%s,%s,%s,%s)", (uid, username, hash_pw(pw, salt), salt))
        cur.execute("INSERT INTO user_data(user_id) VALUES(%s)", (uid,))
        conn.commit()
        token = _issue_user_token(conn, uid)
        conn.close()
        log(f"[{now_iso()}][API][info][122][register][新用户注册成功]")
        return jsonify({"ok": True, "success": True, "token": token, "user_id": uid, "message": "注册成功"})

    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        log(f"[{now_iso()}][API][erro][129][register][注册失败]")
        return jsonify({"ok": False, "success": False, "message": f"注册失败: {str(e)}"}), 500


# region [AUTH HELPERS]
def _check_login_rate_limit(client_ip: str) -> bool:
    """检查登录频率限制，返回 True 表示超出限制"""
    if not redis_manager.use_redis:
        return False
    limit_key = f"rate_limit:login:{client_ip}"
    try:
        count = redis_manager.client.incr(limit_key)
        if count == 1:
            redis_manager.client.expire(limit_key, 60)
        return count > 10
    except Exception as e:
        log(f"[{now_iso()}][API][erro][145][_check_login_rate_limit][频率限制检查失败]")
        return False

def _get_user_account_data(cur, uid: str):
    """获取用户余额和使用记录"""
    cur.execute("SELECT credits, usage FROM user_data WHERE user_id=%s", (uid,))
    row = cur.fetchone()
    credits = float(row["credits"]) if row and row.get("credits") is not None else 1000.0
    usage = row.get("usage") if row else []
    return credits, usage

def _get_user_conversations(cur, uid: str, limit=100):
    """获取用户最近的对话列表"""
    cur.execute("""
        SELECT chat_id, meta, messages, updated 
        FROM conversations 
        WHERE user_id=%s 
        ORDER BY updated DESC 
        LIMIT %s
    """, (uid, limit))
    rows = cur.fetchall()
    return [{
        "chat_id": r.get("chat_id"),
        "meta": r.get("meta") or {},
        "messages": r.get("messages") or [],
        "updated": r.get("updated").isoformat() if r.get("updated") else None
    } for r in rows]

def _get_user_sent_records(cur, uid: str, limit=50):
    """获取用户最近的发送明细记录"""
    cur.execute("""
        SELECT phone_number, task_id, detail, ts 
        FROM sent_records 
        WHERE user_id=%s 
        ORDER BY ts DESC 
        LIMIT %s
    """, (uid, limit))
    rows = cur.fetchall()
    return [{
        "phone_number": r.get("phone_number"),
        "task_id": r.get("task_id"),
        "detail": r.get("detail") or {},
        "ts": r.get("ts").isoformat() if r.get("ts") else None
    } for r in rows]

def _get_user_task_history(cur, uid: str, limit=50):
    """
    🔥 核心优化：使用单条 JOIN 查询获取任务及其统计信息 (解决的问题 4: N+1 查询)
    """
    sql = """
        SELECT 
            t.task_id, t.message, t.total, t.count, t.status, t.created, t.updated,
            COALESCE(SUM(r.success), 0) as stats_success,
            COALESCE(SUM(r.fail), 0) as stats_fail,
            COALESCE(SUM(r.sent), 0) as stats_sent
        FROM tasks t
        LEFT JOIN shards s ON t.task_id = s.task_id
        LEFT JOIN reports r ON s.shard_id = r.shard_id
        WHERE t.user_id = %s
        GROUP BY t.task_id, t.message, t.total, t.count, t.status, t.created, t.updated
        ORDER BY t.created DESC
        LIMIT %s
    """
    cur.execute(sql, (uid, limit))
    rows = cur.fetchall()
    
    history_tasks = []
    for r in rows:
        history_tasks.append({
            "task_id": r.get("task_id"),
            "message": r.get("message"),
            "total": r.get("total"),
            "count": r.get("count"),
            "status": r.get("status"),
            "created": r.get("created").isoformat() if r.get("created") else None,
            "updated": r.get("updated").isoformat() if r.get("updated") else None,
            "result": {
                "success": int(r.get("stats_success", 0)),
                "fail": int(r.get("stats_fail", 0)),
                "sent": int(r.get("stats_sent", 0))
            }
        })
    return history_tasks
# endregion


def _get_user_global_stats(cur, uid: str):
    """获取用户全局统计数据（所有历史任务的总和）"""
    sql = """
        SELECT 
            COUNT(DISTINCT t.task_id) as total_tasks,
            COALESCE(SUM(r.success), 0) as total_success,
            COALESCE(SUM(r.fail), 0) as total_fail,
            COALESCE(SUM(r.sent), 0) as total_sent
        FROM tasks t
        LEFT JOIN shards s ON t.task_id = s.task_id
        LEFT JOIN reports r ON s.shard_id = r.shard_id
        WHERE t.user_id = %s
    """
    cur.execute(sql, (uid,))
    row = cur.fetchone()
    if not row:
        return {"total_tasks": 0, "total_success": 0, "total_fail": 0, "total_sent": 0}
    return {
        "total_tasks": int(row.get("total_tasks", 0)),
        "total_success": int(row.get("total_success", 0)),
        "total_fail": int(row.get("total_fail", 0)),
        "total_sent": int(row.get("total_sent", 0))
    }

@app.route("/api/login", methods=["POST", "OPTIONS"])
def login():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    username = (d.get("username") or "").strip()
    pw = (d.get("password") or "").strip()
    
    # 频率限制
    if _check_login_rate_limit(request.remote_addr):
        return jsonify({"ok": False, "success": False, "message": "登录尝试过多，请稍后再试"}), 429

    try:
        conn = db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM users WHERE username=%s", (username,))
        u = cur.fetchone()
    except Exception as e:
        log(f"[{now_iso()}][API][erro][274][login][数据库查询失败]")
        return jsonify({"ok": False, "success": False, "message": "数据库错误"}), 500

    if not u:
        if conn: conn.close()
        return jsonify({"ok": False, "success": False, "message": "用户名或密码错误"}), 401

    salt = u.get("salt", "")
    if u.get("pw_hash") != hash_pw(pw, salt):
        if conn: conn.close()
        return jsonify({"ok": False, "success": False, "message": "用户名或密码错误"}), 401

    uid = u["user_id"]
    token = _issue_user_token(conn, uid)
    
    # 🔥 关键修复：确保token已保存到数据库后再继续
    # _issue_user_token 已经 commit，但为了确保数据一致性，再次验证
    try:
        verify_cur = conn.cursor()
        th = hash_token(token)
        verify_cur.execute("SELECT 1 FROM user_tokens WHERE user_id=%s AND token_hash=%s", (uid, th))
        if not verify_cur.fetchone():
            conn.close()
            log(f"[{now_iso()}][API][erro][298][login][Token保存失败]")
            return jsonify({"ok": False, "success": False, "message": "Token生成失败，请重试"}), 500
    except Exception as e:
        conn.close()
        log(f"[{now_iso()}][API][erro][302][login][Token验证失败]")
        return jsonify({"ok": False, "success": False, "message": "Token验证失败"}), 500
    
    try:
        # 拆分功能模块加载数据
        credits, usage = _get_user_account_data(cur, uid)
        conversations = _get_user_conversations(cur, uid)
        access_records = _get_user_sent_records(cur, uid)
        
        # 修改：普通用户登录只加载最近3条记录，但加载全局统计
        history_tasks = _get_user_task_history(cur, uid, limit=3)
        global_stats = _get_user_global_stats(cur, uid)
        
        # 获取用户发送费率
        user_rates = _get_user_rates(conn, uid)
        send_rate = float(user_rates.get('send', 30)) if user_rates else 30.0
        
        conn.close()
        
        # 记录用户登录日志
        log(f"[{now_iso()}][API][info][322][login][用户登录成功][user_id={uid}][username={username}][balance={credits}]")
        
        # 保持与原有 API 返回格式 100% 兼容
        return jsonify({
            "ok": True, "success": True, "token": token, "user_id": uid, "message": "登录成功",
            "balance": credits, "send_rate": send_rate, "usage_records": usage or [], 
            "access_records": access_records,
            "inbox_conversations": conversations,
            "history_tasks": history_tasks,
            "global_stats": global_stats, # 新增全局统计字段
            # data 字段是为了兼容某些旧版前端逻辑
            "data": {
                "credits": credits, 
                "send_rate": send_rate,
                "usage": usage or [], 
                "conversations": conversations, 
                "sent_records": access_records,
                "global_stats": global_stats
            }
        })
    except Exception as e:
        if conn: conn.close()
        log(f"[{now_iso()}][API][erro][351][login][加载用户登录数据失败]")
        return jsonify({"ok": False, "success": False, "message": "登录过程中加载数据失败"}), 500


# 验证用户Token
@app.route("/api/verify", methods=["POST", "OPTIONS"])
def verify_user():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    user_id = d.get("user_id")
    token = d.get("token")
    
    if not user_id or not token:
        return jsonify({"ok": False, "success": False, "message": "缺少user_id或token"}), 400
    
    log(f"[{now_iso()}][API][info][363][verify_user][验证用户请求]")

    try:
        conn = db()
        ok = _verify_user_token(conn, user_id, token)
        
        # 🔥 调试信息：如果验证失败，检查数据库中是否有该用户的token
        if not ok:
            debug_cur = conn.cursor()
            debug_cur.execute("SELECT COUNT(*) FROM user_tokens WHERE user_id=%s", (user_id,))
            result = debug_cur.fetchone()
            token_count = result[0] if result else 0
            log(f"[{now_iso()}][API][erro][375][verify_user][Token验证失败]")
            
            # 检查token hash是否正确
            th = hash_token(token)
            debug_cur.execute("SELECT 1 FROM user_tokens WHERE user_id=%s AND token_hash=%s", (user_id, th))
            hash_match = debug_cur.fetchone() is not None
            log(f"[{now_iso()}][API][info][381][verify_user][Token验证hash匹配检查]")
        
        conn.close()
    except Exception as e:
        if 'conn' in dir():
            try: conn.close()
            except: pass
        log(f"[{now_iso()}][API][erro][388][verify_user][验证失败]")
        return jsonify({"ok": False, "error": str(e)}), 500

    if ok:
        return jsonify({"ok": True, "success": True})
    log(f"[{now_iso()}][API][erro][393][verify_user][Token验证失败返回]")
    return jsonify({"ok": False, "success": False, "message": "invalid_token"}), 401


# 轻量健康检查（给 Cloudflare Tunnel / 监控用）
@app.route("/api/ping", methods=["GET"])
def api_ping():
    # 必须极快、无数据库依赖（避免被任务执行/锁竞争拖慢导致 524）
    try:
        # 优化：移除锁，避免 524 超时。Gevent 下单线程访问 _worker_clients 是原子安全的。
        ready_workers = [sid for sid, c in _worker_clients.items() if c.get("ws") and c.get("ready")]
        return jsonify({
            "ok": True,
            "ts": now_iso(),
            "pid": os.getpid(),
            "ready_workers": len(ready_workers),
        })
    except Exception:
        # 即便异常也返回 200，避免监控误判为不可达
        return jsonify({"ok": True, "ts": now_iso(), "pid": os.getpid(), "ready_workers": None})
# endregion

# region [ADMIN AUTH]

# 签发管理员Token（7天过期）
def _issue_admin_token(conn, admin_id: str) -> str:
    token = secrets.token_urlsafe(24)
    th = hash_token(token)
    cur = conn.cursor()
    expires_at = datetime.now() + timedelta(days=7)
    cur.execute("INSERT INTO admin_tokens(token_hash, admin_id, last_used, expires_at) VALUES(%s,%s,NOW(),%s) ON CONFLICT DO NOTHING", (th, admin_id, expires_at))
    conn.commit()
    return token

# 签发管理员Token
def _issue_admin_token(conn, admin_id: str) -> str:
    token = secrets.token_urlsafe(24)
    th = hash_token(token)
    cur = conn.cursor()
    # 使用admin_tokens表存储管理员token
    cur.execute("INSERT INTO admin_tokens(token_hash, admin_id, created, last_used) VALUES(%s,%s,NOW(),NOW()) ON CONFLICT DO NOTHING", (th, admin_id))
    conn.commit()
    return token

# 签发服务器管理员Token（使用专门的server_manager_tokens表，无外键约束）
def _issue_server_manager_token(conn) -> str:
    token = secrets.token_urlsafe(24)
    th = hash_token(token)
    cur = conn.cursor()
    # 使用server_manager_tokens表，这个表没有外键约束
    cur.execute("INSERT INTO server_manager_tokens(token_hash, last_used) VALUES(%s,NOW()) ON CONFLICT DO NOTHING", (th,))
    conn.commit()
    return token


# 验证管理员Token（检查是否过期）
# 管理员登录
@app.route("/api/admin/login", methods=["POST", "OPTIONS"])
def admin_login():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    aid = (d.get("admin_id") or "").strip()
    pw = (d.get("password") or "").strip()

    if not aid or not pw:
        return jsonify({"ok": False, "success": False, "message": "管理员ID和密码不能为空"}), 400

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT pw_hash, salt FROM admins WHERE admin_id=%s", (aid,))
    r = cur.fetchone()
    salt = ""
    if r and len(r) > 1:
        salt = r[1] or ""

    if not r:
        conn.close()
        return jsonify({"ok": False, "success": False, "message": "管理员ID不存在"}), 401

    # r 是 tuple (pw_hash, salt)
    # salt 已经在上面提取了 (Line 1252-1254)
    # r 是 tuple (pw_hash,) ? 不需要 fetchone 得到的 row 可能是 tuple 或 RealDictRow
    # 注意：Line 1237 cursor 没有 specify factory?
    # conn = db() -> cur = conn.cursor() (默认是 tuple cursor)
    # cur.execute("SELECT pw_hash FROM admins...") -> r[0] is pw_hash
    # 我们需要 fetch salt
    # 修正 Line 1237: SELECT pw_hash, salt FROM admins...
    if r[0] != hash_pw(pw, salt):
        conn.close()
        return jsonify({"ok": False, "success": False, "message": "密码错误"}), 401
    token = _issue_admin_token(conn, aid)
    log(f"[{now_iso()}][API][info][486][admin_login][管理员登录成功][admin_id={aid}]")
    conn.close()
    log_access(user_id=aid, username=aid, user_type='admin',endpoint='/api/admin/login', method='POST', status_code=200,detail={'action': 'admin_login'})
    return jsonify({"ok": True, "success": True, "admin_id": aid, "token": token, "message": "登录成功"})


# 验证管理员Token
@app.route("/api/admin/verify", methods=["POST", "OPTIONS"])
def admin_verify():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    token = d.get("token")
    
    if not token:
        return jsonify({"ok": False, "success": False, "message": "缺少token"}), 400

    try:
        conn = db()
        admin_id = _verify_admin_token(conn, token)
        conn.close()
        
        if admin_id:
            return jsonify({"ok": True, "success": True, "admin_id": admin_id})
        return jsonify({"ok": False, "success": False, "message": "invalid_token"}), 401
    except Exception as e:
        if 'conn' in dir():
            try: conn.close()
            except: pass
        return jsonify({"ok": False, "success": False, "message": f"验证失败: {str(e)}"}), 500


# 超级管理员获取指定用户完整历史记录
@app.route("/api/super-admin/user/<user_id>/history", methods=["GET"])
def super_admin_get_user_history(user_id):
    token = _bearer_token()
    conn = db()
    admin_id = _verify_admin_token(conn, token)
    
    if not admin_id:
        conn.close()
        return jsonify({"ok": False, "message": "Unauthorized"}), 401
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 检查用户是否存在
        cur.execute("SELECT 1 FROM users WHERE user_id=%s OR username=%s", (user_id, user_id))
        if not cur.fetchone():
            conn.close()
            return jsonify({"ok": False, "success": False, "message": "用户不存在"}), 404
            
        # 如果传入的是用户名，转换成user_id
        if not user_id.isdigit(): # 简单判断，或者再查一次
             cur.execute("SELECT user_id FROM users WHERE username=%s", (user_id,))
             row = cur.fetchone()
             if row: 
                 user_id = row['user_id']

        # 获取完整历史记录 (比如限制 500条)
        history_tasks = _get_user_task_history(cur, user_id, limit=500)
        global_stats = _get_user_global_stats(cur, user_id)
        
        # 获取充值/使用记录 (保持完整)
        credits, usage = _get_user_account_data(cur, user_id)
        
        conn.close()
        return jsonify({
            "ok": True, 
            "success": True, 
            "user_id": user_id,
            "history_tasks": history_tasks,
            "global_stats": global_stats,
            "usage_records": usage,
            "credits": credits
        })
    except Exception as e:
        if 'conn' in dir():
            try: conn.close()
            except: pass
        return jsonify({"ok": False, "success": False, "message": str(e)}), 500


# 管理员账号管理
@app.route("/api/admin/account", methods=["POST", "GET", "OPTIONS"])
def admin_account_collection():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if request.method == "GET":
        # 验证 token：支持 admin_token 或 server_manager_token
        token = _bearer_token()
        admin_id = _verify_admin_token(conn, token)
        is_server_manager = _verify_server_manager_token(conn, token)
        if not admin_id and not is_server_manager:
            conn.close()
            return jsonify({"success": False, "message": "Unauthorized"}), 401

        cur.execute("""
            SELECT a.admin_id, a.created,
                   COALESCE(c.selected_servers, '[]'::jsonb) AS selected_servers,
                   COALESCE(c.user_groups, '[]'::jsonb) AS user_groups,
                   COALESCE(c.settled_performance, 0) AS settled_performance
            FROM admins a
            LEFT JOIN admin_configs c ON c.admin_id = a.admin_id
            ORDER BY a.created DESC
        """)
        rows = cur.fetchall()
        # Enrich admin list with user_count/performance for super-admin cards.
        # Performance counts recharge records from each user's added_at forward.
        for row in rows:
            groups = row.get("user_groups") or []
            row["user_count"] = len(groups) if isinstance(groups, list) else 0
            perf = float(row.get("settled_performance") or 0.0)
            if isinstance(groups, list):
                for g in groups:
                    if not isinstance(g, dict):
                        continue
                    uid = str(g.get("userId") or g.get("user_id") or "").strip()
                    added_at = g.get("added_at")
                    if not uid:
                        continue
                    try:
                        cur.execute("SELECT usage FROM user_data WHERE user_id=%s", (uid,))
                        ur = cur.fetchone()
                        usage = ur.get("usage") if ur else []
                        if isinstance(usage, str):
                            try:
                                usage = json.loads(usage)
                            except Exception:
                                usage = []
                        added_dt = None
                        if added_at:
                            try:
                                added_dt = datetime.fromisoformat(str(added_at).replace('Z', '+00:00'))
                                if added_dt.tzinfo is None:
                                    added_dt = added_dt.replace(tzinfo=timezone.utc)
                            except Exception:
                                added_dt = None
                        if isinstance(usage, list):
                            for item in usage:
                                if not isinstance(item, dict) or item.get("action") != "recharge":
                                    continue
                                if added_dt is not None:
                                    ts = item.get("ts") or item.get("timestamp")
                                    if not ts:
                                        continue
                                    try:
                                        ts_dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
                                        if ts_dt.tzinfo is None:
                                            ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                                        if ts_dt < added_dt:
                                            continue
                                    except Exception:
                                        continue
                                perf += float(item.get("amount", 0) or 0)
                    except Exception:
                        continue
            row["performance"] = round(perf, 2)
        conn.close()
        return jsonify({"success": True, "admins": rows})

    d = _json()
    admin_id = (d.get("admin_id") or "").strip()
    password = (d.get("password") or "").strip()
    if not admin_id or not password:
        conn.close()
        return jsonify({"success": False, "message": "缺少 admin_id 或 password"}), 400

    try:
        cur.execute("SELECT 1 FROM admins WHERE admin_id=%s", (admin_id,))
        exists = cur.fetchone() is not None
        salt = secrets.token_hex(16)
        cur.execute("INSERT INTO admins(admin_id, pw_hash, salt) VALUES(%s,%s,%s) ON CONFLICT (admin_id) DO UPDATE SET pw_hash=EXCLUDED.pw_hash, salt=EXCLUDED.salt", 
                   (admin_id, hash_pw(password, salt), salt))
        cur.execute("INSERT INTO admin_configs(admin_id) VALUES(%s) ON CONFLICT (admin_id) DO NOTHING", (admin_id,))
        conn.commit()
        
        cur.execute("""
            SELECT a.admin_id, a.created,
                   COALESCE(c.selected_servers, '[]'::jsonb) AS selected_servers,
                   COALESCE(c.user_groups, '[]'::jsonb) AS user_groups,
                   COALESCE(c.settled_performance, 0) AS settled_performance
            FROM admins a
            LEFT JOIN admin_configs c ON c.admin_id = a.admin_id
            WHERE a.admin_id=%s
        """, (admin_id,))
        new_admin = cur.fetchone()
        conn.close()
        
        # 记录创建/更新管理员日志
        action = "更新" if exists else "创建"
        log(f"[{now_iso()}][API][info][643][admin_account_collection][管理员账号创建或更新成功]")
        
        return jsonify({
            "success": True, 
            "admin": new_admin, 
            "message": "管理员账号已更新" if exists else "管理员账号已创建"
        })
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"success": False, "message": str(e)}), 500

# 管理员账号详情
@app.route("/api/admin/account/<admin_id>", methods=["GET", "PUT", "DELETE", "OPTIONS"])
def admin_account_item(admin_id: str):
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if request.method == "GET":
        cur.execute("""
            SELECT a.admin_id, a.created,
                   COALESCE(c.selected_servers, '[]'::jsonb) AS selected_servers,
                   COALESCE(c.user_groups, '[]'::jsonb) AS user_groups,
                   COALESCE(c.settled_performance, 0) AS settled_performance
            FROM admins a
            LEFT JOIN admin_configs c ON c.admin_id = a.admin_id
            WHERE a.admin_id=%s
        """, (admin_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return jsonify({"success": False, "message": "not_found"}), 404
        return jsonify({"success": True, "admin": row})

    if request.method == "PUT":
        d = _json()
        password = (d.get("password") or "").strip()
        selected_servers = d.get("selected_servers") if "selected_servers" in d else d.get("selectedServers")
        user_groups = d.get("user_groups") if "user_groups" in d else d.get("userGroups")

        if not password and selected_servers is None and user_groups is None:
            conn.close()
            return jsonify({"success": False, "message": "missing_update_fields"}), 400

        cur.execute("SELECT 1 FROM admins WHERE admin_id=%s", (admin_id,))
        if not cur.fetchone():
            conn.close()
            return jsonify({"success": False, "message": "not_found"}), 404

        try:
            if password:
                salt = secrets.token_hex(16)
                cur.execute("UPDATE admins SET pw_hash=%s, salt=%s WHERE admin_id=%s", (hash_pw(password, salt), salt, admin_id))
            cur.execute("INSERT INTO admin_configs(admin_id) VALUES(%s) ON CONFLICT (admin_id) DO NOTHING", (admin_id,))
            if selected_servers is not None:
                if not isinstance(selected_servers, list):
                    selected_servers = []
                
                # 获取旧的配置以找出被移除的服务器
                cur.execute("SELECT selected_servers FROM admin_configs WHERE admin_id=%s", (admin_id,))
                old_row = cur.fetchone()
                old_servers = old_row.get("selected_servers") if old_row else []
                if not isinstance(old_servers, list): old_servers = []

                # 更新配置
                cur.execute("UPDATE admin_configs SET selected_servers=%s::jsonb, updated=NOW() WHERE admin_id=%s", (json.dumps(selected_servers), admin_id))
                
                # 找出被移除的服务器名称
                removed_servers = [s for s in old_servers if s not in selected_servers]
                if removed_servers:
                    # 将被移除的服务器从该管理员分配给其用户的所有关联中解除
                    # 注意：selected_servers 存储的是 server_name，我们需要匹配并解除分配
                    cur.execute("""
                        UPDATE servers 
                        SET assigned_user = NULL, assigned_by_admin = NULL 
                        WHERE server_name = ANY(%s) AND assigned_by_admin = %s
                    """, (removed_servers, admin_id))

            if user_groups is not None:
                if not isinstance(user_groups, list):
                    user_groups = []
                # 解绑时结算该用户在绑定期间产生的充值业绩，累加到 settled_performance
                cur.execute("""
                    SELECT COALESCE(user_groups, '[]'::jsonb) AS user_groups,
                           COALESCE(settled_performance, 0) AS settled_performance
                    FROM admin_configs WHERE admin_id=%s
                """, (admin_id,))
                old_cfg = cur.fetchone() or {}
                old_groups = old_cfg.get("user_groups") or []
                settled_perf = float(old_cfg.get("settled_performance") or 0.0)

                new_user_ids = set()
                for g in user_groups:
                    if isinstance(g, dict):
                        uid = str(g.get("userId") or g.get("user_id") or "").strip()
                        if uid:
                            new_user_ids.add(uid)

                removed_groups = []
                if isinstance(old_groups, list):
                    for g in old_groups:
                        if not isinstance(g, dict):
                            continue
                        uid = str(g.get("userId") or g.get("user_id") or "").strip()
                        if uid and uid not in new_user_ids:
                            removed_groups.append(g)

                removed_perf = 0.0
                for g in removed_groups:
                    uid = str(g.get("userId") or g.get("user_id") or "").strip()
                    added_at = g.get("added_at")
                    if not uid or not added_at:
                        continue
                    try:
                        added_dt = datetime.fromisoformat(str(added_at).replace('Z', '+00:00'))
                        if added_dt.tzinfo is None:
                            added_dt = added_dt.replace(tzinfo=timezone.utc)
                    except Exception:
                        continue
                    try:
                        cur.execute("SELECT usage FROM user_data WHERE user_id=%s", (uid,))
                        ur = cur.fetchone()
                        usage = ur.get("usage") if ur else []
                        if isinstance(usage, str):
                            try:
                                usage = json.loads(usage)
                            except Exception:
                                usage = []
                        if not isinstance(usage, list):
                            continue
                        for item in usage:
                            if not isinstance(item, dict) or item.get("action") != "recharge":
                                continue
                            ts = item.get("ts") or item.get("timestamp")
                            if not ts:
                                continue
                            try:
                                ts_dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
                                if ts_dt.tzinfo is None:
                                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                            except Exception:
                                continue
                            if ts_dt >= added_dt:
                                removed_perf += float(item.get("amount", 0) or 0)
                    except Exception:
                        continue

                settled_perf += removed_perf
                cur.execute(
                    "UPDATE admin_configs SET user_groups=%s::jsonb, settled_performance=%s, updated=NOW() WHERE admin_id=%s",
                    (json.dumps(user_groups), settled_perf, admin_id)
                )
            
            conn.commit()
            conn.close()
            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            conn.close()
            return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/server_manager/stats", methods=["GET", "OPTIONS"])
def server_manager_stats():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    token = _bearer_token()
    if not token:
        return jsonify({"ok": False, "message": "未授权"}), 401
    
    conn = db()
    try:
        is_valid = _verify_server_manager_token(conn, token)
        if not is_valid:
            return jsonify({"ok": False, "message": "无效token"}), 401
        
        cur = conn.cursor()

        import time
        from datetime import datetime, timedelta

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        day_before = today - timedelta(days=2)
        month_start = today.replace(day=1)

        # 1. 服务器启动时间 - 从 servers 表最早注册时间
        cur.execute("SELECT MIN(registered_at) FROM servers")
        start_row = cur.fetchone()
        start_time = start_row[0] if start_row and start_row[0] else datetime.now()
        # 计算运行时长
        delta = datetime.now() - start_time
        days = int(delta.total_seconds() // 86400)
        hours = int((delta.total_seconds() % 86400) // 3600)
        mins = int((delta.total_seconds() % 3600) // 60)
        if days > 0:
            uptime_str = f"{days} days {hours} hours"
        elif hours > 0:
            uptime_str = f"{hours} hours {mins} mins"
        else:
            uptime_str = f"{mins} mins"

        # 2. 可用服务器数
        cur.execute("SELECT COUNT(*) FROM servers WHERE status IN ('connected', 'online', 'available')")
        available_servers = cur.fetchone()[0] or 0

        # 3. 总服务器数
        cur.execute("SELECT COUNT(*) FROM servers")
        total_servers = cur.fetchone()[0] or 0

        # 4. 错误日志数（今天）
        try:
            cur.execute("SELECT COUNT(*) FROM log_error WHERE DATE(timestamp) = %s", (today,))
            error_logs = cur.fetchone()[0] or 0
        except:
            error_logs = 0

        # 5. 今天新用户
        cur.execute("SELECT COUNT(*) FROM users WHERE DATE(created) = %s", (today,))
        new_users_today = cur.fetchone()[0] or 0

        # 6. 在线用户（最近 5 分钟有访问记录）
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM access_logs WHERE ts > NOW() - INTERVAL '5 minutes' AND user_id IS NOT NULL")
        online_users = cur.fetchone()[0] or 0

        # 7. 从 user_data.usage 中提取充值记录
        recharge_today = 0
        recharge_3days = 0
        recharge_month = 0
        recharge_total = 0
        recharge_records = []

        try:
            cur.execute("SELECT user_id, credits, usage FROM user_data WHERE usage IS NOT NULL")
            all_usage = cur.fetchall()

            for row in all_usage:
                user_id = row[0]
                usage_data = row[2] if row[2] else []
                if isinstance(usage_data, str):
                    import json
                    try:
                        usage_data = json.loads(usage_data)
                    except:
                        usage_data = []

                for item in usage_data:
                    if item.get('action') == 'recharge':
                        amount = float(item.get('amount', 0))
                        ts_str = item.get('ts', '')
                        try:
                            ts_date = datetime.fromisoformat(ts_str.replace('Z', '+00:00')).date()
                            ts_display = datetime.fromisoformat(ts_str.replace('Z', '+00:00')).strftime("%Y/%m/%d %H:%M")
                        except:
                            ts_date = today
                            ts_display = "-"

                        # 查询用户名
                        cur.execute("SELECT username FROM users WHERE user_id = %s", (user_id,))
                        user_row = cur.fetchone()
                        username = user_row[0] if user_row else user_id

                        record = {
                            "username": username,
                            "amount": amount,
                            "balance": float(item.get('balance', 0)),
                            "time": ts_display
                        }
                        recharge_records.append(record)

                        # 统计
                        if ts_date == today:
                            recharge_today += amount
                        if ts_date >= today - timedelta(days=3):
                            recharge_3days += amount
                        if ts_date >= month_start:
                            recharge_month += amount
                        recharge_total += amount
        except Exception as e:
            log(f"[{now_iso()}][API][warn][server_manager_stats][提取充值记录失败：{e}]")

        # 按时间排序，取最新 10 条
        recharge_records.sort(key=lambda x: x['time'], reverse=True)
        recharge_records = recharge_records[:10]

        # 8. 发送统计
        cur.execute("SELECT SUM(sent), SUM(success), SUM(fail) FROM reports WHERE DATE(ts) = %s", (today,))
        row = cur.fetchone()
        send_today = row[0] or 0
        send_success_today = row[1] or 0
        send_fail_today = row[2] or 0

        cur.execute("SELECT SUM(sent), SUM(success), SUM(fail) FROM reports WHERE DATE(ts) = %s", (yesterday,))
        row = cur.fetchone()
        send_yesterday = row[0] or 0
        send_success_yesterday = row[1] or 0
        send_fail_yesterday = row[2] or 0

        cur.execute("SELECT SUM(sent), SUM(success), SUM(fail) FROM reports WHERE DATE(ts) = %s", (day_before,))
        row = cur.fetchone()
        send_daybefore = row[0] or 0
        send_success_daybefore = row[1] or 0
        send_fail_daybefore = row[2] or 0

        # 9. 消费统计（从 reports.credits 求和）
        cur.execute("SELECT SUM(credits) FROM reports WHERE DATE(ts) = %s", (today,))
        consume_today = cur.fetchone()[0] or 0

        cur.execute("SELECT SUM(credits) FROM reports WHERE DATE(ts) >= %s", (month_start,))
        consume_month = cur.fetchone()[0] or 0

        # 10. 访问统计
        cur.execute("SELECT COUNT(*) FROM access_logs WHERE DATE(ts) = %s", (today,))
        visit_today = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(DISTINCT user_id) FROM access_logs WHERE DATE(ts) = %s", (today,))
        visit_users_today = cur.fetchone()[0] or 0

        # 11. 服务器列表
        cur.execute("SELECT server_id, server_name, status, last_seen FROM servers ORDER BY last_seen DESC LIMIT 20")
        server_rows = cur.fetchall()
        server_list = []
        for row in server_rows:
            server_list.append({
                "name": row[1] or row[0],
                "status": "ok" if row[2] in ("connected", "online", "available") else "disconnected",
                "disconnect_time": row[3].isoformat() if row[3] else None,
                "reason": None,
                "reconnect_count": 0
            })

        # 12. 发送记录（最近 10 条）
        cur.execute("""
            SELECT r.user_id, u.username, r.success, r.fail, r.sent, r.credits, r.ts, d.rates
            FROM reports r 
            LEFT JOIN users u ON r.user_id = u.user_id 
            LEFT JOIN user_data d ON r.user_id = d.user_id
            ORDER BY r.ts DESC LIMIT 10
        """)
        report_rows = cur.fetchall()
        send_records = []
        send_total_all = 0
        recv_total_all = 0
        fail_total_all = 0
        bill_total_all = 0
        for r in report_rows:
            total = (r[2] or 0) + (r[3] or 0)
            rate = f"{int((r[2] or 0) / total * 100)}%" if total > 0 else "0%"
            send_cnt = int(r[4] or 0)
            recv_cnt = 0
            fail_cnt = int(r[3] or 0)
            include_fail = False
            try:
                rates_obj = r[7] or {}
                if isinstance(rates_obj, str):
                    rates_obj = json.loads(rates_obj)
                if isinstance(rates_obj, dict):
                    include_fail = float(rates_obj.get("fail", 0) or 0) != 0
            except Exception:
                include_fail = False
            bill_cnt = send_cnt + recv_cnt + (fail_cnt if include_fail else 0)
            send_total_all += send_cnt
            recv_total_all += recv_cnt
            fail_total_all += fail_cnt
            bill_total_all += bill_cnt
            send_records.append({
                "username": r[1] or r[0],
                "send": send_cnt,
                "recv": recv_cnt,
                "success": r[2] or 0,
                "fail": fail_cnt,
                "rate": rate,
                "cost": float(r[5] or 0),
                "bill": bill_cnt,
                "include_fail_in_bill": include_fail,
                "time": r[6].strftime("%Y/%m/%d %H:%M") if r[6] else "-"
            })

        # 查询费率设置记录
        cur.execute("""
            SELECT u.user_id, u.username, d.rate
            FROM users u
            LEFT JOIN user_data d ON u.user_id = d.user_id
            WHERE d.rate IS NOT NULL
            ORDER BY u.created DESC
            LIMIT 5
        """)
        rate_rows = cur.fetchall()
        rate_records = []
        for r in rate_rows[:3]:
            rate_val = r[2] if r[2] else 0
            rate_records.append({
                "username": r[1] or r[0],
                "user_rate": str(rate_val),
                "admin_rate": "0",
                "time": "-"
            })
        
        # 查询用户和管理员的费率设置数量
        user_rate_count = 0
        admin_rate_count = 0
        rate_records = []
        try:
            cur.execute("SELECT COUNT(*) FROM rate_change_logs WHERE operator_type='super_admin' AND target_user_id IS NOT NULL")
            user_rate_count = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM rate_change_logs WHERE operator_type='super_admin' AND target_admin_id IS NOT NULL")
            admin_rate_count = cur.fetchone()[0] or 0
            
            # 查询费率修改历史记录（最近 10 条）
            cur.execute("""SELECT ts, operator_type, operator_id, target_user_id, target_admin_id, old_rates, new_rates, old_rate_range, new_rate_range, reason
                           FROM rate_change_logs ORDER BY ts DESC LIMIT 10""")
            rate_log_rows = cur.fetchall()
            for row in rate_log_rows:
                record = {
                    "time": row[0].strftime("%Y/%m/%d %H:%M") if row[0] else "-",
                    "operator": row[2],
                    "target_user": row[3],
                    "target_admin": row[4],
                }
                # 用户费率修改
                if row[3]:  # target_user_id 存在
                    old = row[5] if row[5] else {}
                    new = row[6] if row[6] else {}
                    record["type"] = "user"
                    record["old_user_rate"] = str(old.get("send", "0")) if old else "0"
                    record["new_user_rate"] = str(new.get("send", "0")) if new else "0"
                    record["admin_rate"] = "-"
                # 管理员费率范围修改
                elif row[4]:  # target_admin_id 存在
                    old = row[7] if row[7] else {}
                    new = row[8] if row[8] else {}
                    record["type"] = "admin"
                    record["old_user_rate"] = "-"
                    record["new_user_rate"] = "-"
                    record["old_admin_rate"] = str(old.get("min", "0")) + "-" + str(old.get("max", "0")) if old else "0-0"
                    record["new_admin_rate"] = str(new.get("min", "0")) + "-" + str(new.get("max", "0")) if new else "0-0"
                rate_records.append(record)
        except Exception as e:
            # 表不存在时忽略，返回空数据
            log(f"[{now_iso()}][API][warn][160][server_manager_stats][rate_change_logs 表不存在：{e}]")
        
        cur.execute("SELECT ts, ip_address, endpoint FROM access_logs ORDER BY ts DESC LIMIT 10")
        access_rows = cur.fetchall()
        access_logs = []
        for a in access_rows:
            access_logs.append({
                "time": a[0].strftime("%Y/%m/%d %H:%M") if a[0] else "-",
                "ip": a[1] or "-",
                "action": a[2] or "-"
            })
        
        stats = [
            {"send": send_today, "recv": 0, "rate": f"{int(send_success_today/send_today*100)}%" if send_today > 0 else "0%", "reg": new_users_today, "visit": visit_users_today, "consume": consume_today, "income": recharge_today, "total_income": recharge_total},
            {"send": send_yesterday, "recv": 0, "rate": f"{int(send_success_yesterday/send_yesterday*100)}%" if send_yesterday > 0 else "0%", "reg": 0, "visit": 0, "consume": 0, "income": 0, "total_income": recharge_total},
            {"send": send_daybefore, "recv": 0, "rate": f"{int(send_success_daybefore/send_daybefore*100)}%" if send_daybefore > 0 else "0%", "reg": 0, "visit": 0, "consume": 0, "income": 0, "total_income": recharge_total}
        ]

        return jsonify({
            "ok": True,
            "data": {
                "status": "RUNNING",
                "start_time": start_time.strftime("%Y/%m/%d %H:%M"),
                "uptime": uptime_str,
                "online_users": online_users,
                "available_servers": available_servers,
                "error_logs": error_logs,
                "new_users": new_users_today,
                "total_income": recharge_total,
                "stats": stats,
                "recharge_today": recharge_today,
                "recharge_3days": recharge_3days,
                "recharge_month": recharge_month,
                "recharge_total": recharge_total,
                "recharge_records": recharge_records,
                "send_total": send_total_all,
                "recv_total": recv_total_all,
                "fail_total": fail_total_all,
                "bill_total": bill_total_all,
                "send_records": send_records,
                "rate_records": rate_records,
                "rate_user_count": user_rate_count,
                "rate_admin_count": admin_rate_count,
                "server_history": total_servers,
                "server_current": available_servers,
                "server_disconnected": total_servers - available_servers,
                "server_list": server_list,
                "access_logs": access_logs
            }
        })
    except Exception as e:
        # 该接口用于控制面板展示，避免因数据库临时异常导致整个面板不可用
        try:
            log(f"[{now_iso()}][API][erro][server_manager_stats][stats 生成失败: {e}]")
        except Exception:
            pass

        return jsonify({
            "ok": True,
            "warning": str(e),
            "data": {
                "status": "UNKNOWN",
                "start_time": "-",
                "uptime": "-",
                "online_users": 0,
                "available_servers": 0,
                "error_logs": 0,
                "new_users": 0,
                "total_income": 0,
                "stats": [],
                "recharge_today": 0,
                "recharge_3days": 0,
                "recharge_month": 0,
                "recharge_total": 0,
                "recharge_records": [],
                "send_total": 0,
                "recv_total": 0,
                "fail_total": 0,
                "bill_total": 0,
                "send_records": [],
                "rate_records": [],
                "rate_user_count": 0,
                "rate_admin_count": 0,
                "server_history": 0,
                "server_current": 0,
                "server_disconnected": 0,
                "server_list": [],
                "access_logs": []
            }
        })
    finally:
        conn.close()
# endregion

# region [ADMIN USER MGMT]
# 管理员用户管理
@app.route("/api/admin/users", methods=["POST", "GET", "OPTIONS"])
def admin_users_collection():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if request.method == "GET":
        cur.execute("SELECT u.user_id, u.username, u.created, d.credits FROM users u LEFT JOIN user_data d ON u.user_id = d.user_id ORDER BY u.created DESC")
        rows = cur.fetchall()
        conn.close()
        return jsonify({"success": True, "users": rows})

    d = _json()
    username = (d.get("username") or "").strip()
    password = (d.get("password") or "").strip()
    initial_credits = float(d.get("credits", 1000))

    if not username or not password:
        conn.close()
        return jsonify({"success": False, "message": "用户名和密码不能为空"}), 400

    cur.execute("SELECT 1 FROM users WHERE username=%s", (username,))
    if cur.fetchone():
        conn.close()
        return jsonify({"success": False, "message": "用户名已存在"}), 409

    uid = gen_id("u")
    try:
        # 尝试获取当前管理员ID
        admin_id = None
        token = _bearer_token()
        if token:
            admin_id = _verify_admin_token(conn, token)
        
        cur2 = conn.cursor()
        salt = secrets.token_hex(16)
        cur2.execute("INSERT INTO users(user_id, username, pw_hash, salt, created_by_admin) VALUES(%s,%s,%s,%s,%s)", (uid, username, hash_pw(password, salt), salt, admin_id))
        cur2.execute("INSERT INTO user_data(user_id, credits) VALUES(%s,%s)", (uid, initial_credits))
        conn.commit()
        cur.execute("SELECT u.user_id, u.username, u.created, d.credits FROM users u LEFT JOIN user_data d ON u.user_id = d.user_id WHERE u.user_id=%s", (uid,))
        new_user = cur.fetchone()
        conn.close()
        
        # 记录管理员添加用户日志
        log(f"[{now_iso()}][API][info][939][admin_users_collection][管理员添加用户成功]")
        
        return jsonify({"success": True, "user": new_user, "message": "用户创建成功"})
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"success": False, "message": f"创建失败: {str(e)}"}), 500


# 管理员用户详情
@app.route("/api/admin/users/<user_id>", methods=["GET", "DELETE", "OPTIONS"])
def admin_user_item(user_id: str):
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if request.method == "GET":
        cur.execute("SELECT u.user_id, u.username, u.created, d.credits FROM users u LEFT JOIN user_data d ON u.user_id = d.user_id WHERE u.user_id=%s", (user_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return jsonify({"success": False, "message": "用户不存在"}), 404
        return jsonify({"success": True, "user": row})

    # 先获取用户信息用于日志
    cur.execute("SELECT username FROM users WHERE user_id=%s", (user_id,))
    user_row = cur.fetchone()
    username = user_row['username'] if user_row else 'unknown'
    
    cur2 = conn.cursor()
    cur2.execute("DELETE FROM users WHERE user_id=%s", (user_id,))
    conn.commit()
    conn.close()
    
    # 记录删除用户日志
    log(f"[{now_iso()}][API][info][978][admin_user_item][管理员删除用户成功]")
    
    return jsonify({"success": True, "message": "用户已删除"})


# 管理员用户充值
@app.route("/api/admin/users/<user_id>/recharge", methods=["POST", "OPTIONS"])
def admin_user_recharge(user_id: str):
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    d = _json()
    amount = d.get("amount")
    if amount is None:
        conn.close()
        return jsonify({"success": False, "message": "缺少充值金额"}), 400
    
    try:
        amount_f = float(amount)
    except:
        conn.close()
        return jsonify({"success": False, "message": "金额格式错误"}), 400
    
    if amount_f == 0:
        conn.close()
        return jsonify({"success": False, "message": "充值金额不能为0"}), 400

    cur = conn.cursor(cursor_factory=RealDictCursor)
    real_user_id, username = _resolve_user_id(cur, user_id)
    if not real_user_id:
        conn.close()
        return jsonify({"success": False, "message": "用户不存在"}), 404
    
    cur.execute("SELECT credits, usage FROM user_data WHERE user_id=%s", (real_user_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"success": False, "message": "用户数据不存在"}), 404

    old_credits = float(row.get("credits", 0))
    new_credits = old_credits + amount_f
    usage = row.get("usage") or []
    usage.append({"action": "recharge", "amount": amount_f, "ts": now_iso(), "admin_id": "server_manager", "old_credits": old_credits, "new_credits": new_credits, "username": username})

    cur2 = conn.cursor()
    cur2.execute("UPDATE user_data SET credits=%s, usage=%s WHERE user_id=%s", (new_credits, json.dumps(usage), real_user_id))
    conn.commit()
    conn.close()

    # 记录充值日志
    log(f"[{now_iso()}][API][info][1031][admin_user_recharge][用户充值成功]")

    try:
        broadcast_user_update(real_user_id, 'balance_update', {'credits': new_credits, 'balance': new_credits, 'recharged': amount_f, 'old_credits': old_credits})
    except: pass

    return jsonify({"success": True, "user_id": real_user_id, "username": username, "old_credits": old_credits, "amount": amount_f, "credits": new_credits, "new_credits": new_credits})


@app.route("/api/admin/recharge-records", methods=["GET", "OPTIONS"])
def admin_recharge_records():
    """获取所有充值记录 - 服务器管理页面已通过密码验证，无需额外验证"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 获取所有用户的充值记录，同时关联用户名
    cur.execute("""
        SELECT ud.user_id, u.username, ud.usage
        FROM user_data ud
        LEFT JOIN users u ON ud.user_id = u.user_id
        WHERE ud.usage IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()

    all_recharge_records = []
    for row in rows:
        user_id = row.get("user_id")
        username = row.get("username")
        usage = row.get("usage") or []
        # 提取该用户的所有充值记录
        recharge_logs = [item for item in usage if isinstance(item, dict) and item.get("action") == "recharge"]
        for log in recharge_logs:
            # 优先使用记录中的username，如果没有再用当前用户名
            record_username = log.get("username") or username
            all_recharge_records.append({
                "user_id": user_id,
                "username": record_username,
                "amount": log.get("amount", 0),
                "ts": log.get("ts"),
                "admin_id": log.get("admin_id"),
                "old_credits": log.get("old_credits"),
                "new_credits": log.get("new_credits")
            })

    # 按时间倒序排列
    all_recharge_records.sort(key=lambda x: x.get("ts") or "", reverse=True)

    return jsonify({
        "success": True,
        "records": all_recharge_records,
        "total": len(all_recharge_records)
    })


@app.route("/api/admin/user/<user_id>/summary", methods=["GET", "OPTIONS"])
def admin_user_summary(user_id: str):
    """管理员用户详细汇总数据（移除前端业务逻辑）- 服务器管理页面已通过密码验证，无需额外验证"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    # 服务器管理页面已通过密码验证，直接允许操作

    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 解析用户标识（支持user_id或username）
    real_user_id, username = _resolve_user_id(cur, user_id)
    if not real_user_id:
        conn.close()
        return jsonify({"success": False, "message": "用户不存在"}), 404

    # 查询用户积分
    cur.execute("SELECT credits FROM user_data WHERE user_id=%s", (real_user_id,))
    credits_row = cur.fetchone()
    credits = float(credits_row.get("credits", 0)) if credits_row else 0.0

    # 查询统计数据
    cur.execute("SELECT u.created, d.stats, d.usage FROM users u LEFT JOIN user_data d ON u.user_id = d.user_id WHERE u.user_id=%s", (real_user_id,))
    row = cur.fetchone()
    # 查询最近访问时间（优先使用访问日志）
    access_row = None
    try:
        cur.execute("SELECT ts FROM access_logs WHERE user_id=%s ORDER BY ts DESC LIMIT 1", (real_user_id,))
        access_row = cur.fetchone()
    except Exception:
        access_row = None
    conn.close()
    
    if not row:
        return jsonify({"success": False, "message": "用户数据不存在"}), 404

    stats = row.get("stats") or []
    usage = row.get("usage") or []
    
    # 🔥 从usage字段中提取consumption_logs（action='deduct'的记录，即用户使用积分的记录）
    consumption_logs = [item for item in usage if isinstance(item, dict) and item.get("action") == "deduct"]
    
    # 从usage字段中提取recharge_logs（action='recharge'的记录，即充值记录）
    recharge_logs = [item for item in usage if isinstance(item, dict) and item.get("action") == "recharge"]
    
    # stats字段本身就是usage_logs（任务统计记录）
    usage_logs = stats if isinstance(stats, list) else []
    
    
    # 🔥 计算总消费：从consumption_logs（deduct记录）计算，不是从充值记录计算
    total_credits_used = sum(float(log.get("amount", 0) or log.get("credits", 0)) for log in consumption_logs)
    total_sent_count = sum(float(log.get("sent_count", 0)) for log in usage_logs)
    total_sent_amount = sum(float(log.get("total_sent", 0)) for log in usage_logs)
    total_success_count = sum(float(log.get("success_count", 0)) for log in usage_logs)
    
    # 截断 usage_logs，只返回最近3条，以节省流量
    # 注意：这里只截断了列表，并没有影响上面的总数计算
    full_usage_logs_len = len(usage_logs)
    usage_logs = usage_logs[-3:] if usage_logs else []
    
    # 计算成功率
    total_success_rate = 0.0
    if total_sent_amount > 0: # 修正：应该由总量计算成功率
         total_success_rate = (total_success_count / total_sent_amount * 100)
    elif total_sent_count > 0:
        total_success_rate = (total_success_count / total_sent_count * 100)
    
    # 提取最后一条记录
    last_log = usage_logs[-1] if usage_logs else {}
    last_consumption = consumption_logs[-1] if consumption_logs else {}
    last_recharge = recharge_logs[-1] if recharge_logs else {}
    
    # 格式化注册时间
    created_time = row.get("created")
    created_str = "未知"
    if created_time:
        try:
            if isinstance(created_time, str):
                created_str = created_time
            else:
                created_str = created_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            created_str = str(created_time)
    
    # 格式化最后访问时间
    last_access_str = "未知"
    if access_row and access_row.get("ts"):
        try:
            last_access_str = access_row.get("ts").strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            last_access_str = str(access_row.get("ts"))
    elif last_log:
        last_access_ts = last_log.get("timestamp") or last_log.get("ts")
        if last_access_ts:
            try:
                if isinstance(last_access_ts, str):
                    last_access_str = last_access_ts
                elif isinstance(last_access_ts, (int, float)):
                    from datetime import datetime
                    last_access_str = datetime.fromtimestamp(last_access_ts).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    last_access_str = str(last_access_ts)
            except Exception:
                last_access_str = str(last_access_ts)
    
    result = {
        "success": True,
        "user_id": real_user_id,
        "username": username,
        "credits": credits,
        "created": created_str,
        "last_access": last_access_str,
        "last_task_count": last_log.get("task_count", 0),
        "last_sent_count": last_log.get("sent_count", 0),
        "last_success_rate": float(last_log.get("success_rate", 0)),
        "last_credits_used": float(last_consumption.get("amount", 0) or last_consumption.get("credits", 0)),
        "total_access_count": full_usage_logs_len,
        "total_sent_count": int(total_sent_count),
        "total_sent_amount": int(total_sent_amount),
        "total_success_rate": round(total_success_rate, 2),
        "total_credits_used": round(total_credits_used, 2),  # 🔥 总消费：历史总使用积分
        "usage_logs": usage_logs,
        "consumption_logs": consumption_logs,  # 🔥 消费记录（deduct）
        "recharge_logs": recharge_logs  # 🔥 充值记录（recharge）
    }
    
    return jsonify(result)


@app.route("/api/admin/manager/<manager_id>/performance", methods=["GET", "POST", "OPTIONS"])
def admin_manager_performance(manager_id: str):
    """管理员业绩统计（移除前端业务逻辑）- 服务器管理页面已通过密码验证，无需额外验证"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    # 服务器管理页面已通过密码验证，直接允许操作

    # 验证manager_id是否存在
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT 1 FROM admins WHERE admin_id=%s", (manager_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({"success": False, "message": "管理员不存在"}), 404

    # 获取用户列表和user_groups（从请求参数中获取）
    d = _json() if request.method == "POST" else {}
    users_param = d.get("users") or request.args.getlist("users")
    user_groups_param = d.get("user_groups") or d.get("userGroups") or []
    
    settled_base = 0.0
    try:
        cur.execute("SELECT COALESCE(settled_performance, 0) AS settled_performance FROM admin_configs WHERE admin_id=%s", (manager_id,))
        row_perf = cur.fetchone()
        settled_base = float((row_perf or {}).get("settled_performance") or 0.0)
    except Exception:
        settled_base = 0.0

    if not users_param:
        conn.close()
        return jsonify({"success": True, "total_credits": round(settled_base, 2), "users": [], "settled_credits": round(settled_base, 2)})

    # 确保users是列表
    if isinstance(users_param, str):
        users_param = [users_param]
    
    # 构建用户添加时间映射（从user_groups中提取）
    user_added_at_map = {}
    if isinstance(user_groups_param, list):
        for group in user_groups_param:
            if isinstance(group, dict) and group.get("userId"):
                user_id = group.get("userId")
                added_at = group.get("added_at")
                if added_at:
                    user_added_at_map[user_id] = added_at
    
    user_list = []
    total_credits = settled_base

    # 批量处理用户数据 (优化 N+1 查询)
    valid_inputs = [str(u).strip() for u in users_param if u]
    
    if valid_inputs:
        # 1. 准备查询键
        normalized_keys = set()
        for u in valid_inputs:
            norm = u[2:] if u.startswith("u_") else u
            normalized_keys.add(norm)
        search_keys = list(normalized_keys)
        
        # 2. 批量解析用户
        found_users_map = {} # user_id -> usage_data
        id_lookup = {}       # identifier -> real_user_id
        username_lookup = {} # username -> real_user_id
        
        if search_keys:
            try:
                # 查找用户ID映射
                cur.execute("""
                    SELECT user_id, username 
                    FROM users 
                    WHERE user_id = ANY(%s) OR username = ANY(%s)
                """, (search_keys, search_keys))
                rows = cur.fetchall()
                found_ids = []
                for r in rows:
                    uid = r['user_id']
                    uname = r['username']
                    found_ids.append(uid)
                    id_lookup[uid] = uid
                    username_lookup[uname] = uid
                
                # 批量获取 usage 数据
                if found_ids:
                    cur.execute("""
                        SELECT user_id, usage 
                        FROM user_data 
                        WHERE user_id = ANY(%s)
                    """, (found_ids,))
                    data_rows = cur.fetchall()
                    for row in data_rows:
                        found_users_map[row['user_id']] = row.get('usage') or []
            except Exception as e:
                log(f"[{now_iso()}][API][erro][1295][admin_manager_performance][批量获取业绩数据失败]")

        # 3. 计算结果
        for original_input in valid_inputs:
            norm = original_input[2:] if original_input.startswith("u_") else original_input
            
            # 解析 ID
            real_user_id = id_lookup.get(norm)
            if not real_user_id:
                real_user_id = username_lookup.get(norm)
            
            if not real_user_id:
                user_list.append({
                    "user_id": original_input,
                    "credits": 0.0
                })
                continue

            # 获取数据
            usage = found_users_map.get(real_user_id, [])
            
            # 获取用户添加时间
            added_at = user_added_at_map.get(str(original_input)) or user_added_at_map.get(real_user_id)

            user_credits = 0.0
            try:
                # 提取充值记录
                consumption_logs = [item for item in usage if isinstance(item, dict) and item.get("action") == "recharge"]
                
                if added_at and consumption_logs:
                     # 时间处理逻辑保持一致
                    try:
                        added_datetime = datetime.fromisoformat(added_at.replace('Z', '+00:00'))
                        if added_datetime.tzinfo is None:
                            added_datetime = added_datetime.replace(tzinfo=timezone.utc)
                    except:
                        added_datetime = datetime.now(timezone.utc)

                    filtered_logs = []
                    for log in consumption_logs:
                        log_ts = log.get("ts") or log.get("timestamp")
                        if not log_ts: continue
                        try:
                            log_datetime = datetime.fromisoformat(log_ts.replace('Z', '+00:00'))
                            if log_datetime.tzinfo is None:
                                log_datetime = log_datetime.replace(tzinfo=timezone.utc)
                            if log_datetime >= added_datetime:
                                filtered_logs.append(log)
                        except: continue
                    
                    user_credits = sum(float(log.get("amount", 0)) for log in filtered_logs)
            except Exception as e:
                log(f"[{now_iso()}][API][erro][1347][admin_manager_performance][计算用户业绩出错]")

            total_credits += user_credits
            user_list.append({
                "user_id": real_user_id,
                "credits": round(user_credits, 2)
            })

    conn.close()
    return jsonify({
        "success": True,
        "total_credits": round(total_credits, 2),
        "settled_credits": round(settled_base, 2),
        "users": user_list
    })


@app.route("/api/admin/manager/<manager_id>/display", methods=["GET", "POST", "OPTIONS"])
def admin_manager_display(manager_id: str):
    """管理员显示数据（移除前端业务逻辑）- 服务器管理页面已通过密码验证，无需额外验证"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    # 服务器管理页面已通过密码验证，直接允许操作

    # 验证manager_id是否存在
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT 1 FROM admins WHERE admin_id=%s", (manager_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({"success": False, "message": "管理员不存在"}), 404

    # 获取请求参数（users和userGroups是前端管理的，需要通过参数传递）
    d = _json() if request.method == "POST" else {}
    users_param = d.get("users") or request.args.getlist("users")
    user_groups_param = d.get("user_groups") or d.get("userGroups") or []
    selected_servers_param = d.get("selected_servers") or []

    # 全局费率（兜底显示用）
    try:
        global_rates = _get_global_rates(conn) or {}
    except Exception:
        global_rates = {}
    try:
        global_send_rate = float(global_rates.get("send", os.environ.get("CREDIT_PER_SUCCESS", "1")))
    except Exception:
        global_send_rate = 1.0

    # 确保users是列表
    if isinstance(users_param, str):
        users_param = [users_param]
    
    # 🔥 优先从Redis获取在线Worker列表（实时状态）
    # 🔥 快速失败，不阻塞
    try:
        online_workers_set = set(redis_manager.get_online_workers())
    except Exception as e:
            log(f"[{now_iso()}][API][erro][1394][admin_manager_display][获取在线Worker列表失败]")
    online_workers_set = set()
    
    # 获取所有服务器
    # 🔥 核心修正：物理屏蔽掉超过 1 小时没有心跳的僵尸服务器记录
    cur.execute("""
        SELECT server_id, server_name, server_url, port, status, last_seen, assigned_user AS assigned_user_id 
        FROM servers 
        WHERE last_seen > NOW() - INTERVAL '1 hour'
        ORDER BY COALESCE(server_name, server_id)
    """)
    server_rows = cur.fetchall()
    
    now_ts = time.time()
    offline_after = int(os.environ.get("SERVER_OFFLINE_AFTER_SECONDS", "120"))
    
    all_servers = []
    for r in server_rows:
        server_id = r.get("server_id")
        last_seen = r.get("last_seen")
        status = (r.get("status") or "disconnected").lower()
        
        # 🔥 修正后逻辑：只有 Redis 显示在线，或者数据库心跳极新（<60秒）且状态正确
        if server_id in online_workers_set:
            status_out = "connected"
        elif last_seen:
            age = now_ts - last_seen.timestamp()
            # 严格标准：超过 60 秒就算断开，哪怕数据库写着 connected 也不信
            if age > 60:
                status_out = "disconnected"
            else:
                status_out = status if status in ["connected", "available"] else "connected"
        else:
            status_out = "disconnected"
        
        server_name = r.get("server_name") or r.get("server_id")
        all_servers.append({
            "server_id": r.get("server_id"),
            "name": server_name,
            "url": r.get("server_url") or "",
            "status": status_out,
            "assigned_user_id": r.get("assigned_user_id")
        })

    # 构建userGroups的server映射（快速查找）
    user_groups_dict = {}
    if isinstance(user_groups_param, list):
        for group in user_groups_param:
            if isinstance(group, dict):
                user_id = group.get("userId") or group.get("user_id")
                servers = group.get("servers") or []
                if user_id:
                    user_groups_dict[user_id] = servers

    # 获取所有已分配的服务器名称集合
    assigned_servers_set = set()
    for servers_list in user_groups_dict.values():
        if isinstance(servers_list, list):
            assigned_servers_set.update(str(s) for s in servers_list)

    # 筛选管理员的服务器（基于selected_servers_param）
    manager_servers = []
    if selected_servers_param:
        selected_servers_set = set(str(s) for s in selected_servers_param)
        for server in all_servers:
            if server["name"] in selected_servers_set:
                manager_servers.append(server)
    else:
        # 如果没有指定selected_servers，返回所有服务器
        manager_servers = all_servers

    # 分类服务器
    assigned_to_users = []
    available_for_assignment = []
    for server in manager_servers:
        server_name = server["name"]
        if server_name in assigned_servers_set:
            assigned_to_users.append(server)
        else:
            available_for_assignment.append(server)

    # 批量查询用户数据 (优化 N+1 问题)
    user_list = []
    
    # 1. 预处理输入的 identifiers
    # 过滤空值并保持顺序
    valid_inputs = [str(u).strip() for u in users_param if u]
    
    if valid_inputs:
        # 准备查询键值 (去重以减少数据传输)
        # normalized_keys 用于数据库查询 (去掉 u_ 前缀)
        normalized_keys = set()
        for u in valid_inputs:
            norm = u[2:] if u.startswith("u_") else u
            normalized_keys.add(norm)
        search_keys = list(normalized_keys)

        # 2. 批量解析 User ID
        # 查找 user_id 或 username 匹配的用户
        found_users_map = {} # real_user_id -> user_info
        id_lookup = {}       # identifier (user_id) -> real_user_id
        username_lookup = {} # identifier (username) -> real_user_id
        
        if search_keys:
            try:
                # 一次性查找所有匹配的用户基础信息
                cur.execute("""
                    SELECT user_id, username 
                    FROM users 
                    WHERE user_id = ANY(%s) OR username = ANY(%s)
                """, (search_keys, search_keys))
                rows = cur.fetchall()
                
                for r in rows:
                    uid = r['user_id']
                    uname = r['username']
                    # 初始化用户信息结构
                    found_users_map[uid] = {'username': uname, 'user_id': uid}
                    # 建立索引
                    id_lookup[uid] = uid
                    username_lookup[uname] = uid
            except Exception as e:
                log(f"[{now_iso()}][API][erro][1516][admin_manager_display][批量解析用户失败]")

        # 3. 批量获取积分和统计数据
        # 仅查询存在的用户 ID
        real_uids = list(found_users_map.keys())
        if real_uids:
            try:
                cur.execute("""
                    SELECT user_id, credits, stats, rates 
                    FROM user_data 
                    WHERE user_id = ANY(%s)
                """, (real_uids,))
                data_rows = cur.fetchall()
                for row in data_rows:
                    if row['user_id'] in found_users_map:
                        found_users_map[row['user_id']].update(row)
            except Exception as e:
                log(f"[{now_iso()}][API][erro][1533][admin_manager_display][批量获取用户数据失败]")

        # 4. 组装结果 (保持输入顺序)
        for original_input in valid_inputs:
            norm = original_input[2:] if original_input.startswith("u_") else original_input
            
            # 模拟 _resolve_user_id 的优先级逻辑: 先匹配 user_id，再匹配 username
            real_uid = id_lookup.get(norm)
            if not real_uid:
                real_uid = username_lookup.get(norm)
            
            if not real_uid:
                # 用户不存在
                # logger.warning(f"管理员 {manager_id} 查询用户 {original_input} 不存在") # 减少日志噪音
                user_list.append({
                    "user_id": original_input,
                    "credits": 0.0,
                    "last_sent_count": 0,
                    "server_count": len(user_groups_dict.get(original_input, []))
                })
                continue
                
            # 用户存在，提取数据
            info = found_users_map.get(real_uid, {})
            credits_balance = float(info.get("credits", 0))
            
            # 获取 last_sent_count
            stats = info.get("stats") or []
            last_sent_count = 0
            if isinstance(stats, list) and len(stats) > 0:
                last_log = stats[-1]
                last_sent_count = int(last_log.get("sent_count", 0)) if isinstance(last_log, dict) else 0

            # 计算费率（用户设置优先，否则使用全局）
            send_rate = None
            try:
                rates_obj = info.get("rates") or {}
                if isinstance(rates_obj, str):
                    rates_obj = json.loads(rates_obj)
                if isinstance(rates_obj, dict) and rates_obj.get("send") is not None:
                    send_rate = float(rates_obj.get("send"))
            except Exception:
                send_rate = None
            if send_rate is None:
                send_rate = global_send_rate
            
            # server_count 使用原始输入作为 key
            server_count = len(user_groups_dict.get(original_input, []))
            
            user_list.append({
                "user_id": real_uid,
                "username": info.get("username"),
                "credits": round(credits_balance, 2),
                "last_sent_count": last_sent_count,
                "server_count": server_count,
                "send_rate": send_rate
            })

    conn.close()

    return jsonify({
        "success": True,
        "user_list": user_list,
        "servers": {
            "assigned": assigned_to_users,
            "available": available_for_assignment
        },
        "user_groups": user_groups_param
    })
# endregion

# region [ADMIN HELPERS]
@app.route("/api/admin/check-user-assignment", methods=["GET"])
def check_user_assignment():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"success": False, "message": "Missing user_id"}), 400
    
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT admin_id, user_groups FROM admin_configs")
    rows = cur.fetchall()
    conn.close()
    
    for r in rows:
        groups = r.get("user_groups") or []
        manager_id = r.get("admin_id")
        if isinstance(groups, list):
            for g in groups:
                 # 检查userId是否匹配（注意类型转换）
                 if str(g.get("userId") or g.get("user_id")) == str(user_id):
                     return jsonify({
                         "success": True, 
                         "assigned": True, 
                         "manager_id": manager_id
                     })
    
    return jsonify({"success": True, "assigned": False})

#  获取全局费率
def _get_global_rates(conn):
    """获取全局费率设置"""
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key='global_rates'")
        row = cur.fetchone()
        if row and row[0]:
            return json.loads(row[0])
    except: pass
    return {}

# - 获取用户费率（实现优先级：超级管理员设置 > 管理员设置 > 全局费率）
def _get_user_rates(conn, user_id):
    """
    获取用户最终费率，优先级：
    1. 超级管理员设置（admin_rate_set_by='super_admin'）
    2. 管理员设置（admin_rate_set_by=admin_id）
    3. 全局费率（admin_rate_set_by为NULL）
    """
    try:
        # 运行时兜底迁移：避免历史数据库缺列导致事务进入 INERROR 状态
        try:
            cur_m = conn.cursor()
            cur_m.execute("ALTER TABLE user_data ADD COLUMN IF NOT EXISTS rates JSONB")
            cur_m.execute("ALTER TABLE user_data ADD COLUMN IF NOT EXISTS admin_rate_set_by VARCHAR")
            conn.commit()
        except Exception:
            conn.rollback()

        cur = conn.cursor(cursor_factory=RealDictCursor)
        # 获取用户费率设置
        cur.execute("SELECT rates, admin_rate_set_by FROM user_data WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        if row and row.get("rates"):
            return row.get("rates")
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    return None

# - 获取管理员费率范围
def _get_admin_rate_range(conn, admin_id):
    """获取管理员的费率范围设置"""
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT rate_range FROM admin_configs WHERE admin_id=%s", (admin_id,))
        row = cur.fetchone()
        if row and row.get("rate_range"):
            return row.get("rate_range")
    except: pass
    return None

# - 获取用户费率设置来源
def _get_user_rate_source(conn, user_id):
    """获取用户费率设置的来源（super_admin/admin_id/null）"""
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT admin_rate_set_by FROM user_data WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        if row:
            return row.get("admin_rate_set_by")
    except: pass
    return None

@app.route("/api/admin/rates/global", methods=["GET", "POST", "OPTIONS"])
def admin_rates_global():
    """管理全局费率 - 需要服务器管理密码验证"""
    if request.method == "OPTIONS": return jsonify({"ok": True})

    token = _bearer_token()
    conn = db()
    
    is_valid = _verify_server_manager_token(conn, token)
    if not is_valid:
        conn.close()
        return jsonify({"success": False, "message": "Unauthorized: 需要服务器管理密码验证"}), 401

    cur = conn.cursor()

    if request.method == "GET":
        # 从 settings 表获取全局费率
        cur.execute("SELECT value FROM settings WHERE key='global_rates'")
        row = cur.fetchone()
        rates = json.loads(row[0]) if row and row[0] else None
        conn.close()
        return jsonify({"success": True, "rates": rates})

    if request.method == "POST":
        d = _json()
        rates = d.get("rates")
        if not rates: 
            conn.close()
            return jsonify({"success": False, "message": "missing rates"}), 400

        # 保存到 settings 表
        cur.execute("INSERT INTO settings(key, value) VALUES('global_rates', %s) ON CONFLICT (key) DO UPDATE SET value=%s", 
                   (json.dumps(rates), json.dumps(rates)))
        conn.commit()
        conn.close()

        log(f"[{now_iso()}][API][info][133][admin_rates_global][修改全局费率成功]")

        return jsonify({"success": True})

@app.route("/api/admin/rates/user", methods=["GET", "POST", "OPTIONS"])
def admin_rates_user():
    """管理指定用户费率 - 需要服务器管理密码验证"""
    if request.method == "OPTIONS": return jsonify({"ok": True})

    token = _bearer_token()
    conn = db()
    
    is_valid = _verify_server_manager_token(conn, token)
    if not is_valid:
        conn.close()
        return jsonify({"success": False, "message": "Unauthorized: 需要服务器管理密码验证"}), 401

    if request.method == "GET":
        user_id = request.args.get("user_id")
        if not user_id:
            conn.close()
            return jsonify({"success": False, "message": "missing user_id"}), 400

        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE user_id=%s", (user_id,))
        user_exists = cur.fetchone()
        if not user_exists:
            conn.close()
            return jsonify({"success": False, "message": "用户不存在"}), 404

        rates = _get_user_rates(conn, user_id)
        conn.close()
        return jsonify({"success": True, "rates": rates})

    d = _json()
    user_id = d.get("user_id")
    rates = d.get("rates")

    if not user_id: return jsonify({"success": False, "message": "missing user_id"}), 400

    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("ALTER TABLE user_data ADD COLUMN IF NOT EXISTS rates JSONB")
        cur.execute("ALTER TABLE user_data ADD COLUMN IF NOT EXISTS admin_rate_set_by VARCHAR")
        conn.commit()
    except: conn.rollback()

    cur.execute("SELECT rates FROM user_data WHERE user_id=%s", (user_id,))
    old_row = cur.fetchone()
    old_rates = old_row.get("rates") if old_row else None

    if rates is None:
        cur.execute("UPDATE user_data SET rates=NULL, admin_rate_set_by=NULL WHERE user_id=%s", (user_id,))
        action = "重置"
    else:
        cur.execute("UPDATE user_data SET rates=%s, admin_rate_set_by='super_admin' WHERE user_id=%s", (json.dumps(rates), user_id))
        action = "设置"

    try:
        cur.execute("""INSERT INTO rate_change_logs(ts, operator_type, operator_id, target_user_id, old_rates, new_rates, reason)
                       VALUES(NOW(), 'super_admin', 'super_admin', %s, %s, %s, %s)""",
                    (user_id, json.dumps(old_rates) if old_rates else None, json.dumps(rates) if rates else None, f"超级管理员{action}用户费率"))
    except Exception as e:
        log(f"[{now_iso()}][API][warn][admin_rates_user][记录费率修改历史失败：{e}]")

    conn.commit()
    conn.close()
    
    log(f"[{now_iso()}][API][info][193][admin_rates_user][超级管理员设置用户费率成功]")
    
    return jsonify({"success": True})

@app.route("/api/admin/rates/admin-range", methods=["GET", "POST", "OPTIONS"])
def admin_rates_admin_range():
    """设置管理员费率范围 - 需要服务器管理密码验证"""
    if request.method == "OPTIONS": return jsonify({"ok": True})

    token = _bearer_token()
    conn = db()
    
    is_valid = _verify_server_manager_token(conn, token)
    if not is_valid:
        conn.close()
        return jsonify({"success": False, "message": "Unauthorized: 需要服务器管理密码验证"}), 401

    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("ALTER TABLE admin_configs ADD COLUMN IF NOT EXISTS rate_range JSONB")
        conn.commit()
    except: conn.rollback()
    
    if request.method == "GET":
        target_admin_id = request.args.get("admin_id")
        if not target_admin_id:
            conn.close()
            return jsonify({"success": False, "message": "missing admin_id"}), 400

        # 验证管理员是否存在 - 直接查询 admins 表
        cur.execute("SELECT admin_id FROM admins WHERE admin_id=%s", (target_admin_id,))
        admin_exists = cur.fetchone()
        if not admin_exists:
            conn.close()
            # 返回 404 状态码，让前端能明确判断
            return jsonify({"success": False, "message": "管理员不存在"}), 404

        # 管理员存在，获取费率范围
        rate_range = _get_admin_rate_range(conn, target_admin_id)

        # 获取该管理员的客户数量（由该管理员创建的用户数）
        cur.execute("SELECT COUNT(*) as user_count FROM users WHERE created_by_admin=%s", (target_admin_id,))
        user_count_row = cur.fetchone()
        user_count = user_count_row["user_count"] if user_count_row else 0

        # 获取该管理员的总业绩（用户总消费）
        # 简化查询，避免复杂嵌套
        try:
            cur.execute("""
                SELECT COALESCE(SUM(ud.credits), 0) as total_consumption
                FROM users u
                LEFT JOIN user_data ud ON u.user_id = ud.user_id
                WHERE u.created_by_admin=%s
            """, (target_admin_id,))
            performance_row = cur.fetchone()
            performance = float(performance_row["total_consumption"]) if performance_row else 0
        except Exception as e:
            log(f"[{now_iso()}][API][erro][251][admin_rates_admin_range][计算业绩失败]")
            performance = 0

        conn.close()
        return jsonify({
            "success": True,
            "rate_range": rate_range,
            "user_count": user_count,
            "performance": performance
        })
    
    if request.method == "POST":
        d = _json()
        target_admin_id = d.get("admin_id")
        rate_range = d.get("rate_range")  # {"min": 0.02, "max": 0.03}
        
        if not target_admin_id:
            conn.close()
            return jsonify({"success": False, "message": "missing admin_id"}), 400
        
        # 验证费率范围格式
        if rate_range is not None:
            if not isinstance(rate_range, dict) or "min" not in rate_range or "max" not in rate_range:
                conn.close()
                return jsonify({"success": False, "message": "rate_range格式错误，需要{min, max}"}), 400
            
            min_rate = float(rate_range["min"])
            max_rate = float(rate_range["max"])
            
            if min_rate < 0.0001:
                conn.close()
                return jsonify({"success": False, "message": "最小费率不能小于0.0001"}), 400
            
            if max_rate < min_rate:
                conn.close()
                return jsonify({"success": False, "message": "最大费率不能小于最小费率"}), 400
        
        # 获取旧费率范围
        cur.execute("SELECT rate_range FROM admin_configs WHERE admin_id=%s", (target_admin_id,))
        old_row = cur.fetchone()
        old_rate_range = old_row.get("rate_range") if old_row else None

        # 更新管理员费率范围
        if rate_range is None:
            cur.execute("UPDATE admin_configs SET rate_range=NULL WHERE admin_id=%s", (target_admin_id,))
        else:
            cur.execute("UPDATE admin_configs SET rate_range=%s WHERE admin_id=%s", (json.dumps(rate_range), target_admin_id))

        # 记录到费率修改历史
        try:
            cur.execute("""INSERT INTO rate_change_logs(ts, operator_type, operator_id, target_admin_id, old_rate_range, new_rate_range, reason)
                           VALUES(NOW(), 'super_admin', 'super_admin', %s, %s, %s, %s)""",
                        (target_admin_id, json.dumps(old_rate_range) if old_rate_range else None, json.dumps(rate_range) if rate_range else None, "修改管理员费率范围"))
        except Exception as e:
            log(f"[{now_iso()}][API][warn][201][admin_rates_admin_range][记录费率修改历史失败：{e}]")

        conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route("/api/admin/rates/user-by-admin", methods=["POST", "OPTIONS"])
def admin_rates_user_by_admin():
    """管理员设置自己用户的费率（在范围内）"""
    if request.method == "OPTIONS": return jsonify({"ok": True})
    
    # 🔒 权限验证：需要 admin_token
    token = _bearer_token()
    conn = db()
    admin_id = _verify_admin_token(conn, token)
    if not admin_id:
        conn.close()
        return jsonify({"success": False, "message": "Unauthorized: 需要管理员权限"}), 401
    
    # 所有管理员都可以使用此接口
    # if admin_id == "server_manager":
    #     conn.close()
    #     return jsonify({"success": False, "message": "超级管理员请使用 /api/admin/rates/user 接口"}), 400
    
    d = _json()
    user_id = d.get("user_id")
    rates = d.get("rates")
    
    if not user_id: return jsonify({"success": False, "message": "missing user_id"}), 400
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 确保列存在
    try:
        cur.execute("ALTER TABLE user_data ADD COLUMN IF NOT EXISTS rates JSONB")
        cur.execute("ALTER TABLE user_data ADD COLUMN IF NOT EXISTS admin_rate_set_by VARCHAR")
        conn.commit()
    except: conn.rollback()
    
    # 检查用户是否由该管理员创建
    cur.execute("SELECT created_by_admin FROM users WHERE user_id=%s", (user_id,))
    user_row = cur.fetchone()
    if not user_row or user_row.get("created_by_admin") != admin_id:
        conn.close()
        return jsonify({"success": False, "message": "只能设置自己创建的用户费率"}), 403

    cur.execute("SELECT rates FROM user_data WHERE user_id=%s", (user_id,))
    old_row = cur.fetchone()
    old_rates = old_row.get("rates") if old_row else None
    
    # 在简化模型中，所有管理员具有相同权限，无需检查是否被"超级管理员"设置
    # cur.execute("SELECT admin_rate_set_by FROM user_data WHERE user_id=%s", (user_id,))
    # rate_source_row = cur.fetchone()
    # if rate_source_row and rate_source_row.get("admin_rate_set_by") == 'super_admin':
    #     conn.close()
    #     return jsonify({"success": False, "message": "该用户费率已被超级管理员设置，无法修改"}), 403
    
    # 获取管理员费率范围
    rate_range = _get_admin_rate_range(conn, admin_id)
    if not rate_range:
        conn.close()
        return jsonify({"success": False, "message": "管理员费率范围未设置，请联系超级管理员"}), 400
    
    min_rate = float(rate_range.get("min", 0.0001))
    max_rate = float(rate_range.get("max", 100))
    
    # 如果 rates 为空或None，则视为删除/重置用户费率
    if rates is None:
        cur.execute("UPDATE user_data SET rates=NULL, admin_rate_set_by=NULL WHERE user_id=%s", (user_id,))
        action = "重置"
    else:
        # 验证费率是否在范围内（只验证send费率）
        if "send" in rates:
            send_rate = float(rates["send"])
            if send_rate < min_rate or send_rate > max_rate:
                conn.close()
                return jsonify({
                    "success": False, 
                    "message": f"费率超出范围，允许范围：{min_rate:.4f} - {max_rate:.4f}",
                    "min": min_rate,
                    "max": max_rate
                }), 400
        
        # 管理员设置费率，标记为该管理员ID
        cur.execute("UPDATE user_data SET rates=%s, admin_rate_set_by=%s WHERE user_id=%s", (json.dumps(rates), admin_id, user_id))
        action = "设置"

    try:
        cur.execute("""INSERT INTO rate_change_logs(ts, operator_type, operator_id, target_user_id, old_rates, new_rates, reason)
                       VALUES(NOW(), 'admin', %s, %s, %s, %s, %s)""",
                    (admin_id, user_id, json.dumps(old_rates) if old_rates else None, json.dumps(rates) if rates else None, f"管理员{action}用户费率"))
    except Exception as e:
        log(f"[{now_iso()}][API][warn][admin_rates_user_by_admin][记录费率修改历史失败：{e}]")
    
    conn.commit()
    conn.close()
    return jsonify({"success": True})
# endregion

# region [SUPER ADMIN DATA]
@app.route("/api/admin/users/all", methods=["GET", "OPTIONS"])
def admin_users_all():
    """获取所有用户列表（Super Admin）- 支持管理员token和服务器管理员token"""
    if request.method == "OPTIONS": return jsonify({"ok": True})

    token = _bearer_token()
    conn = db()
    # 🔥 支持两种token：管理员token和服务器管理员token（最高权限）
    admin_id = _verify_admin_token(conn, token)
    is_server_manager = _verify_server_manager_token(conn, token)

    # 如果两种token都验证失败，拒绝访问
    if not admin_id and not is_server_manager:
        conn.close()
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 获取所有注册用户（使用子查询避免复杂的GROUP BY）
        cur.execute("""
            SELECT u.user_id, u.username, u.created, u.created_by_admin,
                   COALESCE(d.credits, 0) as credits,
                   d.stats,
                   COALESCE(sc.server_count, 0) as server_count
            FROM users u
            LEFT JOIN user_data d ON u.user_id = d.user_id
            LEFT JOIN (
                SELECT assigned_user, COUNT(*) as server_count
                FROM servers
                WHERE assigned_user IS NOT NULL
                GROUP BY assigned_user
            ) sc ON sc.assigned_user = u.user_id
            ORDER BY u.created DESC
        """)
        rows = cur.fetchall()

        # 简化返回数据
        users = []
        for r in rows:
            # 提取最后发送量
            stats = r.get("stats") or []
            last_sent = 0
            if isinstance(stats, list) and len(stats) > 0:
                last_log = stats[-1]
                if isinstance(last_log, dict):
                    last_sent = int(last_log.get("sent_count", 0))

            users.append({
                "user_id": r["user_id"],
                "username": r["username"],
                "created_at": r["created"].isoformat() if r["created"] else None,
                "created_by": r["created_by_admin"],
                "credits": float(r["credits"] or 0),
                "last_sent": last_sent,
                "server_count": int(r.get("server_count") or 0),
                "send_rate": "0.00"  # 暂时使用默认值，后续可以从配置中获取
            })

        conn.close()
        return jsonify({"success": True, "total": len(users), "users": users})

    except Exception as e:
        if conn: conn.close()
        log(f"[{now_iso()}][API][erro][443][admin_users_all][获取所有用户列表失败]")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/admin/servers/stats", methods=["GET", "OPTIONS"])
def admin_servers_stats():
    """获取服务器全局统计数据（Super Admin）"""
    if request.method == "OPTIONS": return jsonify({"ok": True})
    
    token = _bearer_token()
    conn = db()
    if not _verify_admin_token(conn, token):
        conn.close()
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. 基础服务器统计
        cur.execute("""
            SELECT count(*) as total, 
                   sum(case when status='connected' then 1 else 0 end) as connected,
                   sum(clients_count) as total_clients
            FROM servers
        """)
        basic = cur.fetchone()
        
        # 2. Worker 任务统计 (Mock or Real)
        # 这里暂时只能通过 servers.meta 或 redis 获取实时状态
        # 为了简单，先返回 servers 表数据
        cur.execute("""
            SELECT server_id, server_name, status, clients_count, meta, last_seen
            FROM servers
            ORDER BY server_name
        """)
        servers = cur.fetchall()
        
        server_list = []
        for s in servers:
            meta = s.get("meta") or {}
            # 尝试从 meta 中提取统计
            stats = meta.get("stats") or {}
            server_list.append({
                "id": s["server_id"],
                "name": s["server_name"] or s["server_id"],
                "status": s["status"],
                "clients": s["clients_count"],
                "sent": stats.get("total_sent", 0),
                "success": stats.get("success", 0),
                "fail": stats.get("fail", 0),
                "uptime": meta.get("uptime", 0) # 假设 meta 里有 uptime
            })
            
        # 3. 充值总数
        # 从 user_data.usage 中统计所有 recharge
        cur.execute("SELECT usage FROM user_data")
        usage_rows = cur.fetchall()
        total_recharge = 0.0
        for ur in usage_rows:
            usage = ur.get("usage") or []
            if isinstance(usage, list):
                for item in usage:
                    if isinstance(item, dict) and item.get("action") == "recharge":
                        try: total_recharge += float(item.get("amount", 0))
                        except: pass

        conn.close()
        
        return jsonify({
            "success": True,
            "global": {
                "server_count": basic["total"],
                "connected_count": basic["connected"],
                "online_clients": basic["total_clients"],
                "total_recharge": round(total_recharge, 2)
            },
            "servers": server_list
        })
        
    except Exception as e:
        if conn: conn.close()
        return jsonify({"success": False, "message": str(e)}), 500
# endregion

# region [SERVER MANAGER]


@app.route("/api/server-manager/login", methods=["POST", "OPTIONS"])
def server_manager_login():
    """服务器管理登录：验证密码并签发 token（简单密码验证，非身份验证）"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    try:
        d = _json()
        password = d.get("password", "")

        if not password:
            return jsonify({"success": False, "message": "密码不能为空"}), 400

        conn = db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        pw_hash_stored = _get_setting(cur, "server_manager_pw_hash")

        if not pw_hash_stored:
            conn.close()
            return jsonify({"success": False, "message": "未设置服务器管理密码"}), 400

        salt = ""
        expected_hash = pw_hash_stored

        if "$" in pw_hash_stored:
            parts = pw_hash_stored.split("$", 1)
            if len(parts) == 2:
                salt = parts[0]
                expected_hash = parts[1]
              

        computed_hash = hash_pw(password, salt)
      

        ok = (computed_hash == expected_hash)
        if not ok:
            conn.close()
            return jsonify({"success": False, "message": "密码错误"}), 401

        # 签发服务器管理token（仅用于验证密码正确性，无身份含义）
        token = _issue_server_manager_token(conn)
        conn.close()
        return jsonify({"success": True, "token": token, "message": "登录成功"})
    except Exception as e:
        if 'conn' in dir():
            try: conn.close()
            except: pass
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": f"服务器错误: {str(e)}"}), 500


@app.route("/api/server-manager/verify", methods=["POST", "OPTIONS"])
def server_manager_verify():
    """服务器管理密码验证"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    password = d.get("password", "")

    conn = db()
    cur = conn.cursor()
    pw_hash_stored = _get_setting(cur, "server_manager_pw_hash")
    
    salt = ""
    expected_hash = pw_hash_stored
    
    if "$" in pw_hash_stored:
        parts = pw_hash_stored.split("$", 1)
        if len(parts) == 2:
            salt = parts[0]
            expected_hash = parts[1]
            
    ok = (hash_pw(password, salt) == expected_hash)
    conn.close()

    if ok:
        return jsonify({"success": True, "message": "验证成功"})
    return jsonify({"success": False, "message": "密码错误"}), 401


# 服务器管理密码更新
@app.route("/api/server-manager/password", methods=["PUT", "OPTIONS"])
def server_manager_password_update():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    old_pw = d.get("oldPassword") or d.get("old_password") or ""
    new_pw = d.get("password") or ""

    if not old_pw or not new_pw:
        return jsonify({"success": False, "message": "缺少旧密码或新密码"}), 400

    conn = db()
    cur = conn.cursor()
    current_hash = _get_setting(cur, "server_manager_pw_hash")

    if not current_hash:
        conn.close()
        return jsonify({"success": False, "message": "未设置服务器管理密码"}), 400

    # 解析存储的hash（格式：salt$hash）
    salt = ""
    expected_hash = current_hash
    if "$" in current_hash:
        parts = current_hash.split("$", 1)
        if len(parts) == 2:
            salt = parts[0]
            expected_hash = parts[1]

    # 验证旧密码
    if hash_pw(old_pw, salt) != expected_hash:
        conn.close()
        return jsonify({"success": False, "message": "旧密码错误"}), 401

    # 设置新密码（不带salt，简化处理）
    _set_setting(cur, "server_manager_pw_hash", hash_pw(new_pw))
    conn.commit()
    conn.close()
    return jsonify({"success": True})
# endregion

# region [SERVER REGISTRY]
# 规范化服务器状态
def _normalize_server_status(status: str, clients_count: int) -> str:
    s = (status or "").lower().strip()
    if s in {"online", "available"}:
        return "connected" if clients_count > 0 else "available"
    if s in {"connected", "disconnected", "offline"}:
        return "disconnected" if s == "offline" else s
    return "connected" if clients_count > 0 else "available"


# Worker服务器注册
@app.route("/api/server/register", methods=["POST", "OPTIONS"])
def server_register():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    sid = d.get("server_id")
    name = d.get("server_name") or d.get("name") or "server"
    ws_url = d.get("server_url") or d.get("url")
    port = d.get("port")

    if not sid:
        return jsonify({"ok": False, "success": False, "message": "missing server_id"}), 400

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM servers WHERE server_id=%s", (sid,))
    exists = cur.fetchone() is not None
    status = _normalize_server_status(d.get("status") or "available", int(d.get("clients_count") or 0))

    if not exists:
        cur.execute("INSERT INTO servers(server_id, server_name, server_url, port, status, last_seen, registered_at, meta) VALUES(%s,%s,%s,%s,%s,NOW(),NOW(),%s)", (sid, name, ws_url, port, status, json.dumps(d)))
        action = "新Worker注册"
    else:
        cur.execute("UPDATE servers SET server_name=%s, server_url=COALESCE(%s, server_url), port=COALESCE(%s, port), status=%s, last_seen=NOW(), meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb WHERE server_id=%s", (name, ws_url, port, status, json.dumps(d), sid))
        action = "Worker信息更新"

    conn.commit()
    conn.close()
    
    log(f"[{now_iso()}][API][info][698][server_register][Worker注册成功]")
    
    return jsonify({"ok": True})


# 服务器心跳
@app.route("/api/server/heartbeat", methods=["POST", "OPTIONS"])
def server_hb():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    sid = d.get("server_id")
    if not sid:
        return jsonify({"ok": False, "message": "missing server_id"}), 400

    clients_count = int(d.get("clients_count") or d.get("clients") or 0)
    status = _normalize_server_status(d.get("status") or "available", clients_count)

    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE servers SET last_seen=NOW(), status=%s, clients_count=%s, meta = COALESCE(meta,'{}'::jsonb) || %s::jsonb WHERE server_id=%s", (status, clients_count, json.dumps(d), sid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/server/update_info", methods=["POST", "OPTIONS"])
def server_update_info():
    """更新服务器信息"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    sid = d.get("server_id")
    server_name = d.get("server_name")
    phone = d.get("phone")

    if not sid:
        return jsonify({"ok": False, "success": False, "message": "missing server_id"}), 400

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM servers WHERE server_id=%s", (sid,))
    exists = cur.fetchone() is not None

    if not exists:
        meta = {"phone": phone} if phone else {}
        cur.execute("INSERT INTO servers(server_id, server_name, status, last_seen, registered_at, meta) VALUES(%s,%s,'available',NOW(),NOW(),%s)", (sid, server_name, json.dumps(meta)))
    else:
        update_fields = []
        params = []
        if server_name:
            update_fields.append("server_name=%s")
            params.append(server_name)
        if phone:
            update_fields.append("meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb")
            params.append(json.dumps({"phone": phone}))
        update_fields.append("last_seen=NOW()")
        params.append(sid)
        cur.execute(f"UPDATE servers SET {', '.join(update_fields)} WHERE server_id=%s", tuple(params))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "success": True, "message": f"服务器信息已更新: {server_name} ({phone})"})


# Registry心跳(兼容)
@app.route("/api/heartbeat", methods=["POST", "OPTIONS"])
def registry_heartbeat_alias():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    registry_id = d.get("id")
    if not registry_id:
        return jsonify({"success": False, "message": "missing id"}), 400

    conn = db()
    cur = conn.cursor()
    status = _normalize_server_status(d.get("status") or "online", int(d.get("clients_count") or 0))
    cur.execute("UPDATE servers SET last_seen=NOW(), status=%s, server_name=COALESCE(%s, server_name), server_url=COALESCE(%s, server_url), clients_count=%s, meta = COALESCE(meta,'{}'::jsonb) || %s::jsonb WHERE registry_id=%s", (status, d.get("name"), d.get("url"), int(d.get("clients_count") or 0), json.dumps(d), registry_id))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# Registry注销(兼容)
@app.route("/api/unregister", methods=["POST", "OPTIONS"])
def registry_unregister_alias():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    registry_id = d.get("id")
    if not registry_id:
        return jsonify({"success": False, "message": "missing id"}), 400

    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE servers SET status='disconnected', clients_count=0, last_seen=NOW() WHERE registry_id=%s", (registry_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})
# endregion

# region [SERVERS]
# 服务器列表
@app.route("/api/servers", methods=["GET", "POST", "OPTIONS"])
def servers_collection():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    if request.method == "POST":
        d = _json()
        server_id = d.get("server_id") or gen_id("server")
        name = (d.get("name") or d.get("server_name") or "server").strip()
        url = (d.get("url") or d.get("server_url") or "").strip() or None
        conn = db()
        cur = conn.cursor()
        cur.execute("INSERT INTO servers(server_id, server_name, server_url, status, last_seen, registered_at, meta) VALUES(%s,%s,%s,'available',NOW(),NOW(),%s) ON CONFLICT (server_id) DO UPDATE SET server_name=EXCLUDED.server_name, server_url=EXCLUDED.server_url, status='available', last_seen=NOW()", (server_id, name, url, json.dumps(d)))
        conn.commit()
        conn.close()
        return jsonify({"success": True, "server_id": server_id})

    conn = db()
    servers = []
    now_ts = time.time()
    offline_after = int(os.environ.get("SERVER_OFFLINE_AFTER_SECONDS", "120"))
    try:
        online_workers_set = set(redis_manager.get_online_workers())
    except:
        online_workers_set = set()
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT server_id, server_name, server_url, port, clients_count, status, last_seen, assigned_user AS assigned_user_id, meta FROM servers ORDER BY COALESCE(server_name, server_id)")
    rows = cur.fetchall()

    # 获取服务器所属管理员映射
    cur.execute("SELECT admin_id, selected_servers FROM admin_configs")
    admin_rows = cur.fetchall()

    server_manager_map = {}
    for ar in admin_rows:
        aid = ar.get("admin_id")
        sst = ar.get("selected_servers")
        if aid and sst and isinstance(sst, list):
            for sname in sst:
                server_manager_map[str(sname)] = aid
    
    for r in rows:
        server_id = r.get("server_id")
        last_seen = r.get("last_seen")
        status = (r.get("status") or "disconnected").lower()
        clients_count = int(r.get("clients_count") or 0)
        if server_id in online_workers_set:
            status_out = "connected"
        elif last_seen:
            try:
                age = now_ts - last_seen.timestamp()
                status_out = "disconnected" if age > offline_after else _normalize_server_status(status, clients_count)
            except: status_out = _normalize_server_status(status, clients_count)
        else: status_out = _normalize_server_status(status, clients_count)

        meta = r.get("meta") or {}
        # 合并 worker 上报的本地配置缓存（用于面板展示，避免额外请求/轮询）
        try:
            local_cfg = _worker_local_config.get(server_id) or {}
        except Exception:
            local_cfg = {}
        try:
            local_stats = (local_cfg.get("stats") or {}) if isinstance(local_cfg, dict) else {}
        except Exception:
            local_stats = {}
        assigned_user_id = r.get("assigned_user_id")
        
        # 默认统计来自 reports 汇总；如果 worker 本地 stats 已上报，则优先使用本地累计 stats
        cur.execute("SELECT COUNT(*) as shards_count, COALESCE(SUM(success),0) as total_success, COALESCE(SUM(fail),0) as total_fail, COALESCE(SUM(sent),0) as total_sent FROM reports WHERE server_id=%s", (server_id,))
        stats_row = cur.fetchone()
        shards_count = int(stats_row['shards_count'] or 0) if stats_row else 0
        total_success = int(stats_row['total_success'] or 0) if stats_row else 0
        total_fail = int(stats_row['total_fail'] or 0) if stats_row else 0
        total_sent = int(stats_row['total_sent'] or 0) if stats_row else 0

        try:
            if isinstance(local_stats, dict) and local_stats:
                shards_count = int(local_stats.get("shards") or shards_count)
                total_sent = int(local_stats.get("sent") or total_sent)
                total_success = int(local_stats.get("success") or total_success)
                total_fail = int(local_stats.get("failed") or total_fail)
        except Exception:
            pass

        success_rate = round((total_success / (total_success + total_fail)) * 100, 1) if (total_success + total_fail) > 0 else 0
        
        servers.append({
            "server_id": server_id, "server_name": r.get("server_name") or server_id,
            "server_url": r.get("server_url") or "", "status": status_out, "assigned_user_id": assigned_user_id,
            "is_assigned": assigned_user_id is not None, "is_private": assigned_user_id is not None,
            "is_public": assigned_user_id is None, "last_seen": r.get("last_seen").isoformat() if r.get("last_seen") else None,
            "bound_manager": server_manager_map.get(str(r.get("server_name") or server_id)),
            "meta": {
                "phone": (local_cfg.get("server_phone") if isinstance(local_cfg, dict) else None) or meta.get("phone") or "",
                "email": meta.get("email") or meta.get("current_account") or "",
                "shards_count": shards_count,
                "total_sent": total_sent,
                "success_rate": f"{success_rate}%"
            }
        })
    conn.close()
    return jsonify({"success": True, "servers": servers})

# 服务器详情
@app.route("/api/servers/<server_id>", methods=["DELETE", "GET", "OPTIONS"])
def servers_item(server_id: str):
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if request.method == "GET":
        cur.execute("SELECT server_id, server_name, server_url, status, last_seen, assigned_user AS assigned_user_id FROM servers WHERE server_id=%s", (server_id,))
        row = cur.fetchone()
        conn.close()
        if not row: return jsonify({"success": False, "message": "not_found"}), 404
        return jsonify({"success": True, "server": row})
    cur2 = conn.cursor()
    cur2.execute("DELETE FROM servers WHERE server_id=%s", (server_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


# 清理无效的服务器ID
@app.route("/api/servers/cleanup", methods=["POST", "OPTIONS"])
def cleanup_invalid_servers():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    import re
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT server_id, server_name FROM servers")
    all_servers = cur.fetchall()
    deleted_count = 0
    for row in all_servers:
        sid = str(row.get("server_id", "")).strip()
        sname = str(row.get("server_name", "")).strip()
        should = False

        
        if should:
            cur2 = conn.cursor()
            cur2.execute("DELETE FROM servers WHERE server_id=%s", (sid,))
            deleted_count += 1
    conn.commit()
    conn.close()
    return jsonify({"success": True, "deleted_count": deleted_count})


# 标记服务器为断开
@app.route("/api/servers/<server_id>/disconnect", methods=["POST", "OPTIONS"])
def server_disconnect(server_id: str):
    if request.method == "OPTIONS": return jsonify({"ok": True})
    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE servers SET last_seen = NOW() - INTERVAL '1 day', status = 'disconnected' WHERE server_id=%s", (server_id,))
    conn.commit()
    conn.close()
    
    log(f"[{now_iso()}][API][info][945][server_disconnect][Worker断开连接成功]")
    
    return jsonify({"success": True})

# 服务器分配
@app.route("/api/servers/<server_id>/assign", methods=["POST", "OPTIONS"])
def server_assign(server_id: str):
    if request.method == "OPTIONS": return jsonify({"ok": True})
    d = _json()
    user_id = d.get("user_id")
    if not user_id: return jsonify({"success": False, "message": "missing user_id"}), 400
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT server_id, assigned_user FROM servers WHERE server_id=%s", (server_id,))
    server = cur.fetchone()
    if not server:
        conn.close()
        return jsonify({"success": False, "message": "服务器不存在"}), 404
    cur.execute("SELECT user_id FROM users WHERE user_id=%s", (user_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({"success": False, "message": "用户不存在"}), 404
    cur2 = conn.cursor()
    # 尝试获取当前管理员ID
    admin_id = None
    token = _bearer_token()
    if token:
        admin_id = _verify_admin_token(conn, token)
    
    cur2.execute("UPDATE servers SET assigned_user=%s, assigned_by_admin=%s WHERE server_id=%s", (user_id, admin_id, server_id))
    conn.commit()
    conn.close()
    
    log(f"[{now_iso()}][API][info][978][server_assign][服务器分配成功]")
    
    return jsonify({"success": True})


@app.route("/api/servers/<server_id>/unassign", methods=["POST", "OPTIONS"])
def server_unassign(server_id: str):
    # 服务器取消分配
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("SELECT server_id, assigned_user FROM servers WHERE server_id=%s", (server_id,))
    server = cur.fetchone()
    if not server:
        conn.close()
        return jsonify({"success": False, "message": "服务器不存在"}), 404
    
    current_assigned = server.get("assigned_user")
    if not current_assigned:
        conn.close()
        return jsonify({"success": False, "message": "服务器未分配给任何用户，无需取消"}), 400

    cur2 = conn.cursor()
    cur2.execute("UPDATE servers SET assigned_user=NULL, assigned_by_admin=NULL WHERE server_id=%s", (server_id,))
    conn.commit()
    conn.close()
    
    log(f"[{now_iso()}][API][info][1008][server_unassign][服务器取消分配成功]")
    
    return jsonify({"success": True, "message": f"服务器 {server_id} 已取消分配，现为公共服务器", "server_id": server_id, "previous_user": current_assigned})


@app.route("/api/servers/assigned/<user_id>", methods=["GET", "OPTIONS"])
def servers_assigned(user_id: str):
    # 用户已分配服务器
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT server_id, server_name, server_url, status, last_seen FROM servers WHERE assigned_user=%s ORDER BY COALESCE(server_name, server_id)", (user_id,))
    rows = cur.fetchall()
    conn.close()
    return jsonify({"success": True, "servers": rows})


@app.route("/api/users/<user_id>/available-servers", methods=["GET", "OPTIONS"])
def user_available_servers(user_id: str):
    # 用户可用服务器 - 根据管理员的selected_servers过滤
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 获取用户的created_by_admin信息
    cur.execute("SELECT created_by_admin FROM users WHERE user_id=%s", (user_id,))
    user_row = cur.fetchone()
    admin_id = user_row.get("created_by_admin") if user_row else None
    
    # 获取管理员的selected_servers列表
    admin_selected_servers = None
    if admin_id:
        cur.execute("SELECT selected_servers FROM admin_configs WHERE admin_id=%s", (admin_id,))
        admin_config = cur.fetchone()
        if admin_config and admin_config.get("selected_servers"):
            admin_selected_servers = admin_config.get("selected_servers")
            if not isinstance(admin_selected_servers, list):
                admin_selected_servers = []
    
    # 获取分配给该用户的独享服务器
    cur.execute("SELECT server_id, server_name, server_url, status, last_seen, meta FROM servers WHERE assigned_user=%s", (user_id,))
    exclusive = cur.fetchall()
    
    # 获取共享服务器（未分配给任何用户的）
    cur.execute("SELECT server_id, server_name, server_url, status, last_seen, meta FROM servers WHERE assigned_user IS NULL")
    shared = cur.fetchall()
    conn.close()

    def enrich(rows):
        out = []
        for r in rows:
            meta = r.get("meta") or {}
            phone_number = meta.get("phone") or meta.get("phone_number") if isinstance(meta, dict) else None
            out.append({"server_id": r.get("server_id"), "server_name": r.get("server_name") or r.get("server_id"), "server_url": r.get("server_url") or "", "status": r.get("status") or "disconnected", "last_seen": r.get("last_seen").isoformat() if r.get("last_seen") else None, "phone_number": phone_number})
        return out

    # 如果用户有管理员且管理员有selected_servers配置，则过滤服务器
    if admin_selected_servers is not None:
        # 过滤独享服务器：只保留在管理员selected_servers中的
        filtered_exclusive = [s for s in exclusive if (s.get("server_name") or s.get("server_id")) in admin_selected_servers]
        # 过滤共享服务器：只保留在管理员selected_servers中的
        filtered_shared = [s for s in shared if (s.get("server_name") or s.get("server_id")) in admin_selected_servers]
        return jsonify({"success": True, "exclusive_servers": enrich(filtered_exclusive), "shared_servers": enrich(filtered_shared)})
    
    return jsonify({"success": True, "exclusive_servers": enrich(exclusive), "shared_servers": enrich(shared)})


@app.route("/api/user/<user_id>/servers", methods=["GET", "OPTIONS"])
@app.route("/api/api/user/<user_id>/servers", methods=["GET", "OPTIONS"])
def user_servers(user_id: str):
    # 用户服务器列表
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT server_id FROM servers WHERE assigned_user=%s", (user_id,))
    ex = [i["server_id"] for i in cur.fetchall()]
    cur.execute("SELECT server_id FROM servers WHERE assigned_user IS NULL")
    shared = [i["server_id"] for i in cur.fetchall()]
    conn.close()
    return jsonify({"ok": True, "shared": shared, "exclusive": ex, "all": shared + ex})


@app.route("/api/user/<user_id>/backends", methods=["GET", "OPTIONS"])
def user_backends(user_id: str):
    # 用户后端列表 - 根据管理员的selected_servers过滤
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    try:
        conn = db()
        authed_uid = _maybe_authed_user(conn)
        if authed_uid and authed_uid != user_id:
            conn.close()
            log(f"[{now_iso()}][API][erro][1107][user_backends][权限拒绝]")
            return jsonify({"success": False, "message": "forbidden"}), 403

        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 获取用户的created_by_admin信息
        cur.execute("SELECT created_by_admin FROM users WHERE user_id=%s", (user_id,))
        user_row = cur.fetchone()
        admin_id = user_row.get("created_by_admin") if user_row else None
        
        # 获取管理员的selected_servers列表
        admin_selected_servers = None
        if admin_id:
            cur.execute("SELECT selected_servers FROM admin_configs WHERE admin_id=%s", (admin_id,))
            admin_config = cur.fetchone()
            if admin_config and admin_config.get("selected_servers"):
                admin_selected_servers = admin_config.get("selected_servers")
                if not isinstance(admin_selected_servers, list):
                    admin_selected_servers = []
        
        cur.execute("SELECT server_id, server_name, server_url, status, last_seen, assigned_user AS assigned_user_id FROM servers WHERE assigned_user=%s OR assigned_user IS NULL ORDER BY COALESCE(server_name, server_id)", (user_id,))
        rows = cur.fetchall()
        conn.close()
        
        # 如果用户有管理员且管理员有selected_servers配置，则过滤服务器
        if admin_selected_servers is not None:
            filtered_rows = [r for r in rows if (r.get("server_name") or r.get("server_id")) in admin_selected_servers]
            return jsonify({"success": True, "backends": filtered_rows})
        
        return jsonify({"success": True, "backends": rows})
    except Exception as e:
        if 'conn' in dir():
            try: conn.close()
            except: pass
        log(f"[{now_iso()}][API][erro][1141][user_backends][获取后端列表失败]")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500
# endregion

# region [ID LIBRARY SYNC]
@app.route("/api/id-library", methods=["GET", "POST", "OPTIONS"])
def id_library():
    # ID库同步 - 获取或保存所有ID
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    # 确保数据库已初始化
    try:
        # _ensure_db_initialized() # Removed as per previous context or assuming it's not needed/defined in scope? 
        # Actually in original file it was called or maybe not. I'll stick to simple db() call.
        pass
    except: pass
    
    try:
        conn = db()
    except Exception as e:
        return jsonify({"success": False, "message": f"数据库连接失败: {str(e)}"}), 503
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        if request.method == "GET":
            # 获取所有ID库记录
            cur.execute("SELECT apple_id, password, status, usage_status, created_at, updated_at FROM id_library ORDER BY created_at DESC")
            rows = cur.fetchall()
            accounts = []
            for row in rows:
                accounts.append({
                    "appleId": row["apple_id"],
                    "password": row["password"],
                    "status": row["status"] or "normal",
                    "usageStatus": row["usage_status"] or "new",
                    "createdAt": row["created_at"].isoformat() if row["created_at"] else None,
                    "updatedAt": row["updated_at"].isoformat() if row["updated_at"] else None
                })
            return jsonify({"success": True, "accounts": accounts})
        
        elif request.method == "POST":
            # 同步ID库（保存或更新）
            data = _json()
            accounts = data.get("accounts", [])
            
            if not isinstance(accounts, list):
                return jsonify({"success": False, "message": "accounts must be a list"}), 400
            
            for account in accounts:
                apple_id = account.get("appleId", "").strip()
                password = account.get("password", "").strip()
                status = account.get("status", "normal")

                usage_status = account.get("usageStatus", "new")
                
                if not apple_id or not password:
                    continue
                
                # 使用UPSERT操作
                cur.execute("""
                    INSERT INTO id_library(apple_id, password, status, usage_status, created_at, updated_at)
                    VALUES(%s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (apple_id) DO UPDATE SET
                        password = EXCLUDED.password,
                        status = EXCLUDED.status,
                        usage_status = EXCLUDED.usage_status,
                        updated_at = NOW()
                """, (apple_id, password, status, usage_status))
            
            conn.commit()
            return jsonify({"success": True, "message": f"同步了 {len(accounts)} 个账号"})
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        log(f"[{now_iso()}][API][erro][76][id_library][ID库操作失败]")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        try:
            conn.close()
        except:
            pass


@app.route("/api/id-library/<apple_id>", methods=["DELETE", "PUT", "OPTIONS"])
def id_library_item(apple_id: str):
    # ID库单个记录操作
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    # 确保数据库已初始化
    try:
        _ensure_db_initialized()
    except Exception as e:
        return jsonify({"success": False, "message": f"数据库初始化失败: {str(e)}"}), 503
    
    try:
        conn = db()
    except Exception as e:
        return jsonify({"success": False, "message": f"数据库连接失败: {str(e)}"}), 503
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        if request.method == "DELETE":
            # 删除ID
            cur.execute("DELETE FROM id_library WHERE apple_id=%s", (apple_id,))
            conn.commit()
            deleted = cur.rowcount > 0
            if deleted:
                return jsonify({"success": True, "message": "删除成功"})
            else:
                return jsonify({"success": False, "message": "账号不存在"}), 404
        
        elif request.method == "PUT":
            # 更新ID状态（usage_status）
            data = _json()
            usage_status = data.get("usageStatus", "new")
            
            if usage_status not in ["new", "used"]:
                return jsonify({"success": False, "message": "usageStatus must be 'new' or 'used'"}), 400
            
            cur.execute("""
                UPDATE id_library 
                SET usage_status=%s, updated_at=NOW()
                WHERE apple_id=%s
            """, (usage_status, apple_id))
            conn.commit()
            updated = cur.rowcount > 0
            if updated:
                return jsonify({"success": True, "message": "更新成功"})
            else:
                return jsonify({"success": False, "message": "账号不存在"}), 404
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        log(f"[{now_iso()}][API][erro][139][id_library_item][ID库操作失败]")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        try:
            conn.close()
        except:
            pass

# region [RATES]
@app.route("/api/admin/rate", methods=["GET", "POST", "OPTIONS"])
def admin_rate():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    conn = db()
    cur = conn.cursor()
    
    if request.method == "GET":
        rate = _get_setting(cur, "exchange_rate") or "7.0"
        conn.close()
        return jsonify({"success": True, "rate": float(rate)})
        
    d = _json()
    rate = d.get("rate")
    if rate is None:
        conn.close()
        return jsonify({"success": False, "message": "Missing rate"}), 400
        
    try:
        f_rate = float(rate)
        _set_setting(cur, "exchange_rate", str(f_rate))
        conn.commit()
    except ValueError:
        conn.close()
        return jsonify({"success": False, "message": "Invalid rate format"}), 400
        
    conn.close()
    return jsonify({"success": True})
# endregion

# endregion

# region [USER DATA]
def _resolve_user_id(cur, identifier: str) -> tuple:
    # 通过user_id或username解析真实的user_id，返回(user_id, username)
    # 用户ID格式：纯4位数字（0000-9999），兼容旧格式u_1234
    if not identifier:
        return None, None
    
    # 处理旧格式u_1234，转换为纯4位数字
    if identifier.startswith("u_"):
        identifier = identifier[2:]
    
    # 先尝试作为user_id查询（纯4位数字）
    cur.execute("SELECT user_id, username FROM users WHERE user_id=%s", (identifier,))
    row = cur.fetchone()
    if row:
        return row["user_id"], row["username"]
    # 再尝试作为username查询
    cur.execute("SELECT user_id, username FROM users WHERE username=%s", (identifier,))
    row = cur.fetchone()
    if row:
        return row["user_id"], row["username"]
    return None, None

@app.route("/api/user/<user_id>/credits", methods=["GET", "OPTIONS"])
def user_credits(user_id: str):
    # 用户积分（支持user_id或username查询）
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 解析用户标识（支持user_id或username）
    real_user_id, username = _resolve_user_id(cur, user_id)
    if not real_user_id:
        conn.close()
        return jsonify({"success": False, "message": "用户不存在"}), 404
    
    cur.execute("SELECT credits FROM user_data WHERE user_id=%s", (real_user_id,))
    row = cur.fetchone()
    conn.close()
    credits = float(row["credits"]) if row and row.get("credits") is not None else 0.0
    return jsonify({"success": True, "credits": credits, "user_id": real_user_id, "username": username})


@app.route("/api/user/<user_id>/deduct", methods=["POST", "OPTIONS"])
def user_deduct(user_id: str):
    # 用户扣费
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    amount = d.get("amount") or d.get("credits")
    try:
        amount_f = float(amount)
    except Exception:
        amount_f = 0.0

    if amount_f <= 0:
        return jsonify({"success": False, "message": "invalid_amount"}), 400

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT credits, usage FROM user_data WHERE user_id=%s", (user_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"success": False, "message": "user_not_found"}), 404

    credits = float(row.get("credits", 0))
    usage = row.get("usage") or []
    new_credits = max(0.0, credits - amount_f)
    usage.append({"action": "deduct", "amount": amount_f, "ts": now_iso(), "detail": d})

    cur2 = conn.cursor()
    cur2.execute("UPDATE user_data SET credits=%s, usage=%s WHERE user_id=%s", (new_credits, json.dumps(usage), user_id))
    conn.commit()
    conn.close()

    # 记录扣费日志
    log(f"[{now_iso()}][API][info][261][user_deduct][用户扣费成功]")

    return jsonify({"success": True, "credits": new_credits})


@app.route("/api/user/<user_id>/statistics", methods=["GET", "POST", "OPTIONS"])
def user_statistics(user_id: str):
    # 用户统计（支持user_id或username查询）
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 解析用户标识（支持user_id或username）
    real_user_id, username = _resolve_user_id(cur, user_id)
    if not real_user_id:
        conn.close()
        return jsonify({"success": False, "message": "用户不存在"}), 404

    if request.method == "GET":
        cur.execute("SELECT u.created, d.stats, d.usage, COALESCE(d.credits, 0) as credits FROM users u LEFT JOIN user_data d ON u.user_id = d.user_id WHERE u.user_id=%s", (real_user_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return jsonify({"success": False, "message": "user_not_found"}), 404
        return jsonify({"success": True, "user_id": real_user_id, "username": username, "created": row.get("created").isoformat() if row.get("created") else None, "credits": row.get("credits") or 0, "stats": row.get("stats") or [], "usage": row.get("usage") or []})

    d = _json()
    cur.execute("SELECT stats, usage FROM user_data WHERE user_id=%s", (real_user_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"success": False, "message": "user_not_found"}), 404

    stats = row.get("stats") or []
    usage = row.get("usage") or []
    entry = dict(d.get("entry") or d)
    entry.setdefault("ts", now_iso())
    stats.append(entry)
    usage.append({"action": "statistics", "ts": now_iso(), "detail": entry})

    cur2 = conn.cursor()
    cur2.execute("UPDATE user_data SET stats=%s, usage=%s WHERE user_id=%s", (json.dumps(stats), json.dumps(usage), real_user_id))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/api/inbox/push", methods=["POST", "OPTIONS"])
def inbox_push():
    # 收件箱推送
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    uid = d.get("user_id")
    phone = d.get("phone") or d.get("phone_number")
    text = d.get("text") or d.get("message")

    if not uid or not phone:
        return jsonify({"ok": False, "message": "missing user_id or phone"}), 400

    ts = now_iso()
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT inbox FROM user_data WHERE user_id=%s", (uid,))
    row = cur.fetchone()
    inbox = (row.get("inbox") if row else None) or []
    inbox.append({"phone": phone, "text": text, "ts": ts})

    cur2 = conn.cursor()
    if row:
        cur2.execute("UPDATE user_data SET inbox=%s WHERE user_id=%s", (json.dumps(inbox), uid))
    else:
        cur2.execute("INSERT INTO user_data(user_id, inbox) VALUES(%s,%s)", (uid, json.dumps(inbox)))

    conn.commit()
    conn.close()
    
    try:
        broadcast_user_update(uid, 'inbox_update', {'phone': phone, 'text': text, 'ts': ts})
    except Exception as e:
        log(f"[{now_iso()}][API][erro][346][inbox_push][推送收件箱更新失败]")
    
    return jsonify({"ok": True})


# 会话管理
@app.route("/api/user/<user_id>/conversations", methods=["GET", "POST", "OPTIONS"])
def conversations_collection(user_id: str):
    if request.method == "OPTIONS": return jsonify({"ok": True})
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if request.method == "GET":
        cur.execute("SELECT chat_id, meta, updated FROM conversations WHERE user_id=%s ORDER BY updated DESC", (user_id,))
        rows = cur.fetchall()
        conn.close()
        return jsonify({"success": True, "conversations": rows})
    d = _json()
    chat_id = (d.get("chat_id") or d.get("phone_number") or d.get("id") or "").strip()
    if not chat_id:
        conn.close()
        return jsonify({"success": False}), 400
    cur.execute("INSERT INTO conversations(user_id, chat_id, meta, messages, updated) VALUES(%s,%s,%s::jsonb,%s::jsonb,NOW()) ON CONFLICT (user_id, chat_id) DO UPDATE SET meta = COALESCE(conversations.meta,'{}'::jsonb) || EXCLUDED.meta, messages = EXCLUDED.messages, updated = NOW()", (user_id, chat_id, json.dumps(d.get("meta") or {}), json.dumps(d.get("messages", []))))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# 发送记录
@app.route("/api/user/<user_id>/sent-records", methods=["GET", "POST", "OPTIONS"])
def sent_records(user_id: str):
    if request.method == "OPTIONS": return jsonify({"ok": True})
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if request.method == "GET":
        cur.execute("SELECT phone_number, task_id, detail, ts FROM sent_records WHERE user_id=%s ORDER BY ts DESC LIMIT 500", (user_id,))
        rows = cur.fetchall()
        conn.close()
        return jsonify({"success": True, "records": rows})
    d = _json()
    cur2 = conn.cursor()
    cur2.execute("INSERT INTO sent_records(user_id, phone_number, task_id, detail) VALUES(%s,%s,%s,%s)", (user_id, d.get("phone_number"), d.get("task_id"), json.dumps(d)))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

# 获取任务列表
@app.route("/api/user/<user_id>/tasks", methods=["GET", "POST", "OPTIONS"])
def tasks_collection(user_id: str):
    if request.method == "OPTIONS": return jsonify({"ok": True})
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if request.method == "GET":
        cur.execute("SELECT task_id, message, status, created, updated, total, count FROM tasks WHERE user_id=%s ORDER BY created DESC", (user_id,))
        rows = cur.fetchall()
        conn.close()
        return jsonify({"success": True, "tasks": rows})
    d = _json()
    tid = gen_id("t")
    message = d.get("message", "")
    total = int(d.get("total", 0))
    count = int(d.get("count", 1))
    cur2 = conn.cursor()
    cur2.execute("INSERT INTO tasks(task_id, user_id, message, status, total, count) VALUES(%s,%s,%s,'pending',%s,%s)", (tid, user_id, message, total, count))
    conn.commit()
    conn.close()
    return jsonify({"success": True, "task_id": tid})

# 任务分片管理
@app.route("/api/user/<user_id>/tasks/<task_id>/shards", methods=["GET", "OPTIONS"])
def shards_collection(user_id: str, task_id: str):
    if request.method == "OPTIONS": return jsonify({"ok": True})
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT shard_id, server_id, status, result, updated FROM shards WHERE task_id=%s", (task_id,))
    rows = cur.fetchall()
    conn.close()
    return jsonify({"success": True, "shards": rows})
# endregion

# region [TASK]

def _split_numbers(nums, shard_size: int):
    # 分片号码列表
    for i in range(0, len(nums), shard_size):
        yield nums[i : i + shard_size]


def _reclaim_stale_shards(conn) -> int:
    # 回收超时分片
    stale_seconds = int(os.environ.get("SHARD_STALE_SECONDS", "600"))
    cur = conn.cursor()
    cur.execute("UPDATE shards SET status='pending', locked_at=NULL, updated=NOW(), attempts = attempts + 1 WHERE status='running' AND locked_at IS NOT NULL AND locked_at < NOW() - (%s * interval '1 second')", (stale_seconds,))
    reclaimed = cur.rowcount
    if reclaimed:
        conn.commit()
    return reclaimed


@app.route("/api/task/create", methods=["POST", "OPTIONS"])
@app.route("/api/api/task/create", methods=["POST", "OPTIONS"])
def create_task():
    LOCATION = "[API][create_task]"

    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    uid = d.get("user_id")
    msg = d.get("message")
    nums = d.get("numbers") or []
    cnt = int(d.get("count", 1))
    trace_id = d.get("trace_id") or uuid.uuid4().hex[:12]

    if not uid or msg is None:
        return jsonify({"ok": False, "message": "missing user_id or message"}), 400
    if not isinstance(nums, list):
        return jsonify({"ok": False, "message": "numbers must be list"}), 400

    conn = db()

    token = _bearer_token()
    if token and not _verify_user_token(conn, uid, token):
        conn.close()
        return jsonify({"ok": False, "message": "invalid_token"}), 401

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT credits FROM user_data WHERE user_id=%s", (uid,))
    user_data = cur.fetchone()
    if not user_data:
        conn.close()
        return jsonify({"ok": False, "message": "user_not_found"}), 404

    credits = float(user_data.get("credits", 0))

    # [MODIFIED] 使用动态费率计算预估成本（优先级：超级管理员设置 > 管理员设置 > 全局费率）
    # 1. 获取全局费率作为基准
    global_rates = _get_global_rates(conn)
    base_price = float(global_rates.get("send", os.environ.get("CREDIT_PER_SUCCESS", "1")))

    # 2. 检查用户费率设置来源，按优先级获取费率
    rate_source = _get_user_rate_source(conn, uid)
    user_rates = _get_user_rates(conn, uid)

    if rate_source == 'super_admin':
        # 超级管理员设置的费率（最高优先级）
        if user_rates and "send" in user_rates:
            price_per_msg = float(user_rates["send"])
        else:
            price_per_msg = base_price
    elif rate_source and rate_source != 'super_admin':
        # 管理员设置的费率（中等优先级）
        if user_rates and "send" in user_rates:
            price_per_msg = float(user_rates["send"])
        else:
            price_per_msg = base_price
    else:
        # 使用全局费率（最低优先级）
        price_per_msg = base_price

    estimated_cost = len(nums) * price_per_msg

    if credits < estimated_cost:
        conn.close()
        return jsonify({"ok": False, "message": "insufficient_credits", "credits": credits, "current": credits, "required": estimated_cost}), 400

    task_id = gen_id("task")

    # 🔥 快速失败，不阻塞
    # 🔥 核心修正：只认内存中真实的连接
    with _worker_lock:
        available_servers = [
            sid for sid, client in _worker_clients.items()
            if client.get("ws") and client.get("ready") and _is_worker_assignable(sid)
        ]

    available_count = len(available_servers) if available_servers else 0

    # [FIXED] 固定分片大小为50条，不再根据worker数量动态计算
    if d.get("shard_size"):
        shard_size = int(d.get("shard_size"))
    else:
        shard_size = 50

    try:
        conn.commit()
    except Exception:
        pass

    # 🔥 将回收超时分片移到后台，避免阻塞主请求
    cur = conn.cursor()
    cur.execute("INSERT INTO tasks(task_id,user_id,message,total,count,status,created,updated) VALUES(%s,%s,%s,%s,%s,'pending',NOW(),NOW())", (task_id, uid, msg, len(nums), cnt))

    def async_reclaim():
        try:
            conn_reclaim = db()
            _reclaim_stale_shards(conn_reclaim)
            conn_reclaim.close()
        except Exception as e:
            log(f"[{now_iso()}][API][erro][540][async_reclaim][后台回收超时分片失败]")

    try:
        spawn(async_reclaim)
    except Exception:
        import threading
        threading.Thread(target=async_reclaim, daemon=True).start()

    # 动作：写入Redis任务缓存
    if redis_manager.use_redis:
        try:
            task_cache = {
                "task_id": task_id,
                "user_id": uid,
                "message": msg,
                "total": len(nums),
                "count": cnt,
                "status": "pending"
            }
            from gevent import Timeout as GTimeout
            try:
                with GTimeout(2):
                    redis_manager.client.set(f"task_info:{task_id}", json.dumps(task_cache), ex=3600)
            except GTimeout:
                log(f"[{now_iso()}][API][erro][564][create_task][Redis缓存写入超时]")
            except Exception as e:
                log(f"[{now_iso()}][API][erro][566][create_task][Redis缓存写入失败]")
        except Exception as e:
            log(f"[{now_iso()}][API][erro][568][create_task][Redis数据构建失败]")

    shard_count = (len(nums) + shard_size - 1) // shard_size if len(nums) > 0 else 0

    conn.commit()
    conn.close()

    # 动作：异步创建分片并推送
    def async_create_shards_and_assign():
        from gevent import Timeout as GTimeout
        conn2 = None
        try:
            with GTimeout(60):
                conn2 = db()
                cur2 = conn2.cursor()

                actual_shard_count = 0
                shard_ids = []

                for group in _split_numbers(nums, shard_size):
                    shard_id = gen_id("shard")
                    shard_ids.append(shard_id)
                    try:
                        phone_count = len(group) if isinstance(group, list) else None
                    except Exception:
                        phone_count = None
                    cur2.execute("INSERT INTO shards(shard_id,task_id,phones,status,updated) VALUES(%s,%s,%s,'pending',NOW())", (shard_id, task_id, json.dumps(group)))
                    actual_shard_count += 1

                conn2.commit()

                # 动作：初始化Redis任务统计
                if redis_manager.use_redis:
                    try:
                        stats_key = f"task_stats:{task_id}"
                        redis_manager.client.hset(stats_key, mapping={
                            "uid": uid,
                            "total_shards": actual_shard_count,
                            "shards_done": 0,
                            "total_success": 0,
                            "total_fail": 0,
                            "total_credits": 0.0,
                            "price_per_msg": price_per_msg,  # 存储用户设置的费率
                            "send_start_ts": time.time(),
                            "verify_shards_done": 0,
                            "verify_success": 0,
                            "verify_fail": 0
                        })
                        redis_manager.client.expire(stats_key, 86400)

                        # 动作：建立分片到任务的反向映射
                        pipe = redis_manager.client.pipeline()
                        for sid in shard_ids:
                            pipe.setex(f"shard_map:{sid}", 86400, f"{uid}:{task_id}")
                        pipe.execute()

                    except Exception as e:
                        log(f"[{now_iso()}][API][erro][621][async_create_shards_and_assign][Redis注册失败]")

                else:
                    # 动作：使用内存模式（Redis不可用时）
                    with _task_tracker_lock:
                        _task_tracker[task_id] = {
                            "user_id": uid,
                            "total_shards": actual_shard_count,
                            "completed_shards": 0,
                            "shard_results": {},
                            "total_success": 0,
                            "total_fail": 0,
                            "total_credits": 0.0,
                            "price_per_msg": price_per_msg,  # 存储用户设置的费率
                            "created_at": time.time(),
                            "send_start_ts": time.time(),
                            "trace_id": trace_id,
                            "message": msg
                        }
                        for sid in shard_ids:
                            _shard_to_task[sid] = task_id

                assign_result = _assign_and_push_shards(task_id, uid, msg, trace_id=trace_id)

                if assign_result.get("pushed", 0) > 0:
                    cur2.execute("UPDATE tasks SET status='running', updated=NOW() WHERE task_id=%s", (task_id,))
                    conn2.commit()

                conn2.close()

        except GTimeout:
            log(f"[{now_iso()}][API][erro][651][async_create_shards_and_assign][异步创建分片超时]")
            if conn2:
                try:
                    conn2.rollback()
                    conn2.close()
                except Exception as rollback_e:
                    log(f"[{now_iso()}][API][erro][657][async_create_shards_and_assign][超时回滚失败]")
        except Exception as e:
            log(f"[{now_iso()}][API][erro][659][async_create_shards_and_assign][异步处理任务失败]")
            import traceback
            traceback.print_exc()
            if conn2:
                try:
                    conn2.rollback()
                    conn2.close()
                except Exception as rollback_e:
                    log(f"[{now_iso()}][API][erro][667][async_create_shards_and_assign][异常回滚失败]")

    try:
        spawn(async_create_shards_and_assign)
    except Exception:
        import threading
        threading.Thread(target=async_create_shards_and_assign, daemon=True).start()

    # 记录关键业务日志
    log(f"[{now_iso()}][API][info][676][create_task][用户创建任务成功]")

    return jsonify({
        "ok": True,
        "task_id": task_id,
        "trace_id": trace_id,
        "total_shards": shard_count,
        "message": f"任务已创建，正在后台创建分片并分配..."
    })


@app.route("/api/task/assign", methods=["POST", "OPTIONS"])
@app.route("/api/api/task/assign", methods=["POST", "OPTIONS"])
def assign_task():

    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    task_id = d.get("task_id")
    if not task_id:
        return jsonify({"ok": False, "msg": "missing task_id"}), 400
    
    log(f"[{now_iso()}][API][erro][701][assign_task][调用了已废弃的端点]")
    log(f"[{now_iso()}][API][erro][702][assign_task][任务创建时已自动分配]")

    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT user_id, message FROM tasks WHERE task_id=%s", (task_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        return jsonify({"ok": False, "msg": "task_not_found"}), 404

    uid = r["user_id"]
    msg = r["message"]
    conn.close()
    
    # 使用新的推送机制重新分配
    log(f"[{now_iso()}][API][info][718][assign_task][手动重新分配任务]")
    assign_result = _assign_and_push_shards(task_id, uid, msg)
    
    return jsonify({
        "ok": True,
        "deprecated": True,
        "message": "任务已通过 WebSocket 推送机制重新分配",
        "assigned": assign_result.get("pushed", 0),
        "total": assign_result.get("total", 0)
    })


@app.route("/api/server/<server_id>/shards", methods=["GET", "OPTIONS"])
def server_shards(server_id: str):

    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    log(f"[{now_iso()}][API][erro][736][server_shards][Worker调用了已废弃的轮询端点]")
    log(f"[{now_iso()}][API][erro][737][server_shards][请升级Worker使用WebSocket推送机制]")

    # 返回空列表，鼓励使用 WebSocket
    return jsonify({
        "ok": True, 
        "shards": [], 
        "reclaimed": 0,
        "deprecated": True,
        "message": "此端点已废弃，请使用 WebSocket 推送机制。任务会自动推送到 Worker，无需轮询。"
    })


# 提交任务报告 [DEPRECATED - 已废弃]
# 此端点已不再使用，结果通过 WebSocket shard_result 上报
@app.route("/api/reports", methods=["POST", "OPTIONS"])
def reports_collection():
    if request.method == "OPTIONS": return jsonify({"ok": True})
    # 废弃端点，保留仅用于向后兼容
    log(f"[{now_iso()}][API][erro][755][reports_collection][端点已废弃请使用WebSocket]")
    return jsonify({"success": True, "deprecated": True, "message": "此端点已废弃"})

def _check_and_reclaim_timeout_shards(task_id: str, task_owner_uid: str):
    """
    检测并回收超时的分片
    当超过80%的分片完成时，检查剩余分片是否超时
    """
    LOCATION = "[API][_check_and_reclaim_timeout_shards]"
    conn = db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 查询该任务的所有分片状态
        cur.execute("""
            SELECT shard_id, status, server_id, updated, phones
            FROM shards
            WHERE task_id=%s
        """, (task_id,))
        shard_rows = cur.fetchall()

        total_shards = len(shard_rows)
        done_shards = sum(1 for r in shard_rows if r.get('status') == 'done')
        running_shards = sum(1 for r in shard_rows if r.get('status') == 'running')

        # 如果已完成超过80%，检查剩余运行的分片
        if done_shards > 0 and total_shards > 0 and done_shards * 100 // total_shards >= 80:
            # 超时阈值：30秒（50条分片，正常1分钟内完成，比别人慢30秒就超时）
            timeout_threshold = datetime.now(timezone.utc) - timedelta(seconds=30)

            reclaimed_count = 0
            for shard in shard_rows:
                if shard.get('status') not in ('running', 'pending'):
                    continue

                # 检查是否超时
                updated_time = shard.get('updated')
                if not updated_time:
                    # 如果没有更新时间，立即标记为超时
                    is_timeout = True
                elif isinstance(updated_time, datetime):
                    is_timeout = updated_time < timeout_threshold
                else:
                    # 字符串格式的时间
                    is_timeout = updated_time.timestamp() if hasattr(updated_time, 'timestamp') else False

                if is_timeout:
                    shard_id = shard.get('shard_id')
                    stuck_server_id = shard.get('server_id')
                    if not stuck_server_id:
                        continue
                    try:
                        probe_key = f"shard_probe_inflight:{shard_id}"
                        first_probe = bool(redis_manager.client.set(probe_key, "1", nx=True, ex=120))
                    except Exception:
                        first_probe = True
                    if not first_probe:
                        continue

                    phones = shard.get("phones") or []
                    if isinstance(phones, str):
                        try:
                            phones = json.loads(phones)
                        except Exception:
                            phones = []

                    probe_ok = _send_worker_ws(stuck_server_id, {
                        "type": "probe_shard",
                        "shard": {
                            "task_id": task_id,
                            "shard_id": shard_id,
                            "phones": phones,
                            "start_ts": time.time() - 3600
                        }
                    })
                    if probe_ok:
                        try:
                            broadcast_task_update(task_id, {
                                "task_id": task_id,
                                "status": "running",
                                "phase_message": "网络不稳定，预计完成时间可能稍微延迟。任务正常进行中，请耐心等待。"
                            })
                        except Exception:
                            pass
                        reclaimed_count += 1

            if reclaimed_count > 0:
                conn.commit()

    except Exception as e:
        log(f"[{now_iso()}][API][erro][824][_check_and_reclaim_timeout_shards][检测超时分片失败]")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def report_shard_result(shard_id: str, sid: str, uid: str, suc: int, fail: int, detail: dict):
    LOCATION = "[API][report_shard_result]"
    task_id = None
    task_completed = False
    task_owner_uid = None
    # 0. 获取 trace_id
    trace_id = None
    phase = None
    if isinstance(detail, dict):
        trace_id = detail.get("trace_id") or (detail.get("detail") or {}).get("trace_id")
        phase = detail.get("phase") or (detail.get("detail") or {}).get("phase")

    try:
        # -------------------------------------------------------------------------
        # 1. 从 Redis 查找分片归属
        # -------------------------------------------------------------------------
        shard_map_key = f"shard_map:{shard_id}"
        shard_val = redis_manager.client.get(shard_map_key)

        if shard_val:
            try:
                parts = shard_val.split(":")
                if len(parts) != 2:
                    return {"ok": False, "error": "invalid_data"}
                uid_from_redis, task_id = parts
                task_owner_uid = uid_from_redis
            except (ValueError, AttributeError, TypeError) as e:
                return {"ok": False, "error": "invalid_data"}
        else:
            # 兜底：从数据库查 task_id
            try:
                conn = db()
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute("SELECT task_id FROM shards WHERE shard_id=%s", (shard_id,))
                row = cur.fetchone()
                if row:
                    task_id = row.get("task_id")
                    cur.execute("SELECT user_id FROM tasks WHERE task_id=%s", (task_id,))
                    row2 = cur.fetchone()
                    task_owner_uid = row2.get("user_id") if row2 else None
                conn.close()
            except Exception:
                pass

        if not task_id:
            log(f"[{now_iso()}][API][erro][850][report_shard_result][Ghost分片接收到]")
            return {"ok": False, "error": "shard_not_found"}

        phase_norm = "verify" if str(phase or "").lower() == "verify" else "send"
        dedupe_key = f"task_phase_seen:{task_id}:{phase_norm}:{shard_id}"
        try:
            is_first = redis_manager.client.set(dedupe_key, "1", nx=True, ex=86400)
            if not is_first:
                return {"ok": True, "duplicate": True, "phase": phase_norm}
        except Exception:
            # Redis异常时不阻断主流程
            pass

        # 仅在校验阶段结束后删除映射，避免二次上报失败
        if phase_norm == "verify":
            try:
                redis_manager.client.delete(shard_map_key)
            except Exception:
                pass

        # -------------------------------------------------------------------------

        if phase_norm != "verify":
            # -------------------------------------------------------------------------
            # 1.5. 更新数据库 Shards 表（立即标记完成）
            # -------------------------------------------------------------------------
            conn = db()
            try:
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute("""
                    UPDATE shards
                    SET status='done', server_id=%s, updated=NOW()
                    WHERE shard_id=%s
                """, (sid, shard_id))
                conn.commit()
            except Exception as e:
                log(f"[{now_iso()}][API][erro][883][report_shard_result][更新shard状态失败]")
            finally:
                conn.close()

        # 2. 更新 Redis 任务统计
        # -------------------------------------------------------------------------
        redis_task_key = f"task_stats:{task_id}"

        if phase_norm == "verify":
            try:
                failed_details = []
                try:
                    if isinstance(detail, dict):
                        fd = detail.get("failed_details") or (detail.get("detail") or {}).get("failed_details") or []
                        if isinstance(fd, list):
                            failed_details = fd
                except Exception:
                    failed_details = []

                pipe = redis_manager.client.pipeline()
                pipe.hincrby(redis_task_key, "verify_shards_done", 1)
                pipe.hincrby(redis_task_key, "verify_success", int(suc))
                pipe.hincrby(redis_task_key, "verify_fail", int(fail))
                pipe.execute()

                if failed_details:
                    try:
                        fd_key = f"task_verify_failed_details:{task_id}"
                        for item in failed_details:
                            redis_manager.client.rpush(fd_key, json.dumps(item, ensure_ascii=False))
                        redis_manager.client.expire(fd_key, 86400)
                    except Exception:
                        pass

                stats = redis_manager.client.hgetall(redis_task_key)
                if not stats or not stats.get('total_shards'):
                    return {"ok": False, "error": "task_expired"}

                total_shards = int(stats.get('total_shards', 0))
                verify_done = int(stats.get('verify_shards_done', 0))
                verify_success = int(stats.get('verify_success', 0))
                verify_fail = int(stats.get('verify_fail', 0))

                if verify_done >= total_shards:
                    final_once_key = f"task_verify_done_once:{task_id}"
                    final_first = True
                    try:
                        final_first = bool(redis_manager.client.set(final_once_key, "1", nx=True, ex=86400))
                    except Exception:
                        final_first = True

                    if final_first:
                        final_failed_details = []
                        try:
                            fd_key = f"task_verify_failed_details:{task_id}"
                            raw_items = redis_manager.client.lrange(fd_key, 0, 1000) or []
                            for r in raw_items:
                                try:
                                    final_failed_details.append(json.loads(r))
                                except Exception:
                                    pass
                        except Exception:
                            final_failed_details = []
                        final_payload = {
                            "task_id": task_id,
                            "status": "done",
                            "trace_id": trace_id,
                            "shards": {"done": total_shards, "total": total_shards},
                            "result": {"success": verify_success, "fail": verify_fail, "sent": verify_success + verify_fail},
                            "failed_details": final_failed_details,
                            "completed": True,
                            "verified": True
                        }
                        try:
                            broadcast_task_update(task_id, final_payload)
                        except Exception:
                            pass
                        try:
                            redis_manager.client.expire(redis_task_key, 3600)
                        except Exception:
                            pass
                return {"ok": True, "completed": verify_done >= total_shards}
            except Exception:
                return {"ok": False, "error": "verify_update_failed"}

        # 获取用户设置的费率（优先从Redis读取，如果没有则使用环境变量默认值）
        price_per_msg = 1.0  # 默认值
        try:
            stats_for_rate = redis_manager.client.hgetall(redis_task_key)
            if stats_for_rate and stats_for_rate.get('price_per_msg'):
                price_per_msg = float(stats_for_rate['price_per_msg'])
        except Exception:
            pass

        # 使用用户设置的费率计算积分
        shard_credits = (float(suc) * price_per_msg)

        pipe = redis_manager.client.pipeline()
        pipe.hincrby(redis_task_key, "shards_done", 1)
        pipe.hincrby(redis_task_key, "total_success", suc)
        pipe.hincrby(redis_task_key, "total_fail", fail)
        pipe.hincrbyfloat(redis_task_key, "total_credits", shard_credits)
        pipe.execute()

        stats = redis_manager.client.hgetall(redis_task_key)

        if not stats or not stats.get('total_shards'):
            return {"ok": False, "error": "task_expired"}

        shards_done = int(stats.get('shards_done', 0))
        total_shards = int(stats.get('total_shards', 0))
        total_success = int(stats.get('total_success', 0))
        total_fail = int(stats.get('total_fail', 0))
        total_credits = float(stats.get('total_credits', 0.0))

        # -----------------------------------------------------------------
        # 2.5. 超时分片检测（在80%完成后触发）
        # -----------------------------------------------------------------
        if shards_done > 0 and total_shards > 0:
            completion_percent = shards_done * 100 // total_shards
            if completion_percent >= 50:  # 从50%就开始检测，80%会更激进
                try:
                    _check_and_reclaim_timeout_shards(task_id, task_owner_uid)
                except Exception as e:
                    pass

        if shards_done >= total_shards:
            task_completed = True

        # -------------------------------------------------------------------------
        # 3. 广播进度给前端
        # -------------------------------------------------------------------------
        send_elapsed_sec = None
        try:
            send_start_ts = float(stats.get("send_start_ts", 0) or 0)
            if send_start_ts > 0:
                send_elapsed_sec = max(0, int(time.time() - send_start_ts))
        except Exception:
            send_elapsed_sec = None

        broadcast_payload = {
            "task_id": task_id,
            "status": "running",
            "trace_id": trace_id,
            "shards": {"done": shards_done, "total": total_shards},
            "result": {"success": total_success, "fail": total_fail, "sent": total_success + total_fail},
            "completed": task_completed
        }

        if task_completed and send_elapsed_sec is not None:
            send_done_once_key = f"task_send_done_once:{task_id}"
            send_done_first = True
            try:
                send_done_first = bool(redis_manager.client.set(send_done_once_key, "1", nx=True, ex=86400))
            except Exception:
                send_done_first = True
            if send_done_first:
                broadcast_payload["phase_message"] = f"发送完成 用时: {send_elapsed_sec}秒 正在统计结果..."
                broadcast_payload["phase"] = "send_done"
        try:
            broadcast_task_update(task_id, broadcast_payload)
        except Exception as e:
            pass

        # -------------------------------------------------------------------------
        # 4. 如果任务完成，执行数据库入库
        # -------------------------------------------------------------------------
        if task_completed:
            conn = db()
            try:
                cur = conn.cursor(cursor_factory=RealDictCursor)

                # A. 更新任务主表状态
                cur.execute("""
                    UPDATE tasks
                    SET status='done', updated=NOW()
                    WHERE task_id=%s
                """, (task_id,))

                # B. 扣费
                cur.execute(
                    "SELECT credits, usage FROM user_data WHERE user_id=%s FOR UPDATE", (task_owner_uid,))
                user_data = cur.fetchone()

                if user_data:
                    old_credits = float(user_data.get("credits", 0))
                    usage_log = user_data.get("usage") or []
                    new_credits = max(0.0, old_credits - total_credits)

                    usage_log.append({
                        "action": "task_finish",
                        "task_id": task_id,
                        "success": total_success,
                        "fail": total_fail,
                        "credits": total_credits,
                        "old_credits": old_credits,
                        "new_credits": new_credits,
                        "ts": now_iso()
                    })

                    cur.execute("UPDATE user_data SET credits=%s, usage=%s WHERE user_id=%s",
                                (new_credits, json.dumps(usage_log[-200:]), task_owner_uid))

                    # C. 插入总报表
                    cur.execute("""
                        INSERT INTO reports(shard_id, server_id, user_id, success, fail, sent, credits, detail)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (f"task_total_{task_id}", sid, task_owner_uid, total_success, total_fail, total_success+total_fail, total_credits, json.dumps({"is_total": True, "task_id": task_id})))

                conn.commit()

                # E. 推送余额更新
                broadcast_user_update(task_owner_uid, 'usage_update', {'credits': new_credits if user_data else 0})
                
                # 记录任务完成日志
                log(f"[{now_iso()}][API][info][1003][report_shard_result][任务完成]")

                # 发送阶段完成后不广播最终统计，等待校验阶段汇总

            except Exception as e:
                conn.rollback()
                log(f"[{now_iso()}][API][erro][1026][report_shard_result][汇总提交失败]")
            finally:
                conn.close()

            # 发送阶段完成后保留统计键，供 verify 阶段汇总使用
            try:
                redis_manager.client.expire(redis_task_key, 86400)
            except Exception:
                pass
    except Exception as e:
        log(f"[{now_iso()}][API][erro][1035][report_shard_result][系统级异常]")
    return {"ok": True, "completed": task_completed}

@app.route("/api/task/<task_id>/status", methods=["GET", "OPTIONS"])
def task_status(task_id: str):
    # 任务状态
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    # 1. [NEW] 优先从内存读取
    with _task_tracker_lock:
        if task_id in _task_tracker:
            tracker = _task_tracker[task_id]
            return jsonify({
                "ok": True,
                "success": True,
                "task_id": task_id,
                "user_id": tracker.get("user_id"),
                "status": "running" if tracker["completed_shards"] < tracker["total_shards"] else "done",
                "total": tracker["total_shards"],
                "shards": {
                    "pending": tracker["total_shards"] - tracker["completed_shards"], # 简化处理，只返回存量
                    "running": 0, 
                    "done": tracker["completed_shards"],
                    "total": tracker["total_shards"]
                },
                "result": {
                    "success": tracker["total_success"],
                    "fail": tracker["total_fail"],
                    "sent": tracker["total_success"] + tracker["total_fail"]
                },
                "_source": "memory"
            })

    # 2. 内存未命中的回滚到数据库（已完成或过期任务）
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT task_id, user_id, message, total, status, created, updated FROM tasks WHERE task_id=%s", (task_id,))
        task = cur.fetchone()
        if not task:
            return jsonify({"success": False, "message": "task_not_found"}), 404

        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE status='pending') AS pending, 
                COUNT(*) FILTER (WHERE status='running') AS running, 
                COUNT(*) FILTER (WHERE status='done') AS done, 
                COUNT(*) AS total 
            FROM shards WHERE task_id=%s
        """, (task_id,))
        shard_counts = cur.fetchone() or {}

        # 注意：历史任务的结果汇总逻辑，由于我们改用了汇总提交，这里的 rep 查出的可能是空的
        # 所以对于已完成任务，我们需要查询汇总的那条记录，或者直接读任务表（如果我们要进一步改 schema 的话）
        cur.execute("SELECT COALESCE(SUM(success),0) AS success, COALESCE(SUM(fail),0) AS fail, COALESCE(SUM(sent),0) AS sent FROM reports WHERE shard_id IN (SELECT shard_id FROM shards WHERE task_id=%s) OR shard_id = %s", (task_id, f"task_total_{task_id}"))
        rep = cur.fetchone() or {}

        return jsonify({
            "ok": True, 
            "success": True, 
            "task_id": task_id, 
            "user_id": task.get("user_id"), 
            "message": task.get("message", ""), 
            "status": task["status"], 
            "total": task["total"], 
            "shards": {
                "pending": int(shard_counts.get("pending", 0)), 
                "running": int(shard_counts.get("running", 0)), 
                "done": int(shard_counts.get("done", 0)), 
                "total": int(shard_counts.get("total", 0))
            }, 
            "result": {
                "success": int(rep.get("success", 0)), 
                "fail": int(rep.get("fail", 0)), 
                "sent": int(rep.get("sent", 0))
            }, 
            "created": task["created"].isoformat() if task.get("created") else None, 
            "updated": task["updated"].isoformat() if task.get("updated") else None,
            "_source": "db"
        })
    finally:
        conn.close()


@app.route("/api/task/<task_id>/shards", methods=["GET", "OPTIONS"])
def task_shards_detail(task_id: str):
    # 获取任务的所有分片详情
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cur.execute("""
            SELECT shard_id, task_id, server_id, phones, status, attempts, 
                   locked_at, updated, result
            FROM shards 
            WHERE task_id=%s
            ORDER BY shard_id
        """, (task_id,))
        shards = cur.fetchall()
        
        # 转换为可序列化格式
        result = []
        for shard in shards:
            shard_dict = dict(shard)
            if shard_dict.get("locked_at"):
                shard_dict["locked_at"] = shard_dict["locked_at"].isoformat()
            if shard_dict.get("updated"):
                shard_dict["updated"] = shard_dict["updated"].isoformat()
            result.append(shard_dict)
        
        conn.close()
        return jsonify({"ok": True, "shards": result})
    except Exception as e:
        conn.close()
        log(f"[{now_iso()}][API][erro][1153][task_shards_detail][获取分片详情失败]")
        return jsonify({"ok": False, "message": str(e)}), 500

@app.route("/api/task/<task_id>/events", methods=["GET", "OPTIONS"])
def task_events_sse(task_id: str):
    # 任务SSE事件
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    interval = float(request.args.get("interval", "1"))
    max_seconds = int(request.args.get("max_seconds", "3600"))
    start = time.time()

    def gen():
        last_payload = None
        while True:
            if time.time() - start > max_seconds:
                yield "event: end\ndata: {}\n\n"
                return
            try:
                conn = db()
                _reclaim_stale_shards(conn)
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute("SELECT COUNT(*) FILTER (WHERE status='pending') AS pending, COUNT(*) FILTER (WHERE status='running') AS running, COUNT(*) FILTER (WHERE status='done') AS done, COUNT(*) AS total FROM shards WHERE task_id=%s", (task_id,))
                sc = cur.fetchone() or {}
                cur.execute("SELECT COALESCE(SUM(success),0) AS success, COALESCE(SUM(fail),0) AS fail, COALESCE(SUM(sent),0) AS sent FROM reports WHERE shard_id IN (SELECT shard_id FROM shards WHERE task_id=%s)", (task_id,))
                rp = cur.fetchone() or {}
                cur.execute("SELECT status FROM tasks WHERE task_id=%s", (task_id,))
                ts = (cur.fetchone() or {}).get("status")
                conn.close()
                payload = {"task_id": task_id, "status": ts, "shards": sc, "result": rp}
                payload_s = json.dumps(payload, ensure_ascii=False)
                if payload_s != last_payload:
                    last_payload = payload_s
                    yield f"data: {payload_s}\n\n"
                if ts == "done":
                    yield "event: end\ndata: {}\n\n"
                    return
            except Exception as e:
                # 🔧 修复: 异常时必须关闭连接，否则连接池会耗尽
                if 'conn' in dir():
                    try: conn.close()
                    except: pass
                yield f"event: error\ndata: {json.dumps({'error': str(e)}, ensure_ascii=False)}\n\n"
            time.sleep(interval)

    return Response(stream_with_context(gen()), mimetype="text/event-stream")

# endregion

# region [INBOX & HEARTBEAT]
@app.route("/api/user/<user_id>/inbox", methods=["GET", "OPTIONS"])
def user_inbox(user_id: str):
    # 用户收件箱
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT inbox FROM user_data WHERE user_id=%s", (user_id,))
    row = cur.fetchone()
    inbox = json.loads(row["inbox"]) if row and row["inbox"] else []
    
    cur.execute("SELECT chat_id, meta, messages, updated FROM conversations WHERE user_id=%s ORDER BY updated DESC", (user_id,))
    conversations = cur.fetchall()
    conn.close()
    
    chat_list = []
    for conv in conversations:
        meta = json.loads(conv["meta"]) if isinstance(conv["meta"], str) else (conv["meta"] or {})
        messages = json.loads(conv["messages"]) if isinstance(conv["messages"], str) else (conv["messages"] or [])
        last_message = messages[-1] if messages else None
        last_message_preview = ""
        if last_message:
            last_message_preview = (last_message.get("text", last_message.get("message", ""))[:50] if isinstance(last_message, dict) else str(last_message)[:50])
        chat_list.append({"chat_id": conv["chat_id"], "name": meta.get("name", meta.get("phone_number", conv["chat_id"])), "phone_number": meta.get("phone_number", conv["chat_id"]), "last_message_preview": last_message_preview, "updated": conv["updated"].isoformat() if conv["updated"] else None})
    
    return jsonify({"ok": True, "inbox": inbox, "conversations": chat_list})


@app.route("/api/backend/heartbeat", methods=["POST", "OPTIONS"])
def backend_heartbeat():
    # 后端心跳
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    d = _json()
    server_id = d.get("server_id")
    if not server_id:
        return jsonify({"ok": False, "message": "missing server_id"}), 400
    
    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE servers SET status='connected', last_seen=NOW() WHERE server_id=%s", (server_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "heartbeat_received"})
# endregion

# region [COMPAT]
@app.route("/api/admin/assign", methods=["POST", "OPTIONS"])
def admin_assign_alias():
    # 管理员分配(兼容)
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = _json()
    server_id = d.get("server_id")
    user_id = d.get("user_id")
    if not server_id or not user_id:
        return jsonify({"ok": False, "message": "missing server_id/user_id"}), 400

    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE servers SET assigned_user=%s WHERE server_id=%s", (user_id, server_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})
# endregion

# region [FRONTEND WEBSOCKET]
@sock.route('/ws/frontend')
def frontend_websocket(ws):
    # 前端WebSocket端点 - 用于前端前端订阅任务和用户更新
    client_id = id(ws)  # 使用WebSocket对象ID作为唯一标识
    user_id = None
    subscribed_tasks = set()
    
    try:
        log(f"[{now_iso()}][API][info][4896][frontend_websocket][前端WS连接建立]")
        
        # 注册客户端
        with _frontend_lock:
            _frontend_clients[client_id] = {
                "ws": ws,
                "user_id": None,
                "subscribed_tasks": set(),
                "connected_at": time.time()
            }
        
        # 🔥 连接成功后立即推送服务器列表
        try:
            servers = _get_servers_list_with_status()
            ws.send(json.dumps({
                "type": "servers_list",
                "servers": servers,
                "ok": True
            }))
            log(f"[{now_iso()}][API][info][4915][frontend_websocket][前端连接成功，已推送服务器列表]")
        except Exception as e:
            log(f"[{now_iso()}][API][erro][4917][frontend_websocket][推送初始服务器列表失败]")
        
        while True:
            try:
                # 增加超时时间到90秒，前端每30秒发送心跳
                data = ws.receive(timeout=90)
                if data is None:
                    break
                
                try:
                    msg = json.loads(data)
                except json.JSONDecodeError:
                    ws.send(json.dumps({"type": "error", "message": "invalid_json"}))
                    continue
                
                action = msg.get("action")
                payload = msg.get("data", {})
                
                if action == "subscribe_user":
                    # 订阅用户更新
                    user_id = payload.get("user_id")
                    if user_id:
                        with _frontend_lock:
                            _frontend_clients[client_id]["user_id"] = user_id
                        ws.send(json.dumps({"type": "user_subscribed", "user_id": user_id, "ok": True}))
                        log(f"[{now_iso()}][API][info][4942][frontend_websocket][前端订阅用户]")
                
                elif action == "get_servers":
                    # 🔥 前端请求获取服务器列表（一次性，不轮询）
                    try:
                        conn = db()
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        # 🔥 快速失败，不阻塞
                        try:
                            online_workers_set = set(redis_manager.get_online_workers())
                        except Exception as e:
                            log(f"[{now_iso()}][API][erro][4953][frontend_websocket][获取在线Worker列表失败]")
                            online_workers_set = set()
                        
                        cur.execute("SELECT server_id, server_name, server_url, port, clients_count, status, last_seen, assigned_user AS assigned_user_id, meta FROM servers ORDER BY COALESCE(server_name, server_id)")
                        rows = cur.fetchall()
                        conn.close()
                        
                        servers = []
                        now_ts = time.time()
                        offline_after = int(os.environ.get("SERVER_OFFLINE_AFTER_SECONDS", "120"))
                        
                        for r in rows:
                            server_id = r.get("server_id")
                            last_seen = r.get("last_seen")
                            status = (r.get("status") or "disconnected").lower()
                            clients_count = int(r.get("clients_count") or 0)
                            
                            # 优先检查Redis在线状态
                            if server_id in online_workers_set:
                                status_out = "connected"
                            elif last_seen:
                                try:
                                    age = now_ts - last_seen.timestamp()
                                    status_out = "disconnected" if age > offline_after else _normalize_server_status(status, clients_count)
                                except Exception:
                                    status_out = _normalize_server_status(status, clients_count)
                            else:
                                status_out = _normalize_server_status(status, clients_count)
                            
                            meta = r.get("meta") or {}
                            phone_number = meta.get("phone") or meta.get("phone_number") if isinstance(meta, dict) else None
                            
                            servers.append({
                                "server_id": server_id,
                                "server_name": r.get("server_name") or server_id,
                                "server_url": r.get("server_url") or "",
                                "status": status_out,
                                "sched_state": _get_worker_sched(server_id).get("state", "ready"),
                                "assigned_user_id": r.get("assigned_user_id"),
                                "is_assigned": r.get("assigned_user_id") is not None,
                                "last_seen": r.get("last_seen").isoformat() if r.get("last_seen") else None,
                                "phone_number": phone_number
                            })
                        
                        ws.send(json.dumps({
                            "type": "servers_list",
                            "servers": servers,
                            "ok": True
                        }))
                    except Exception as e:
                        log(f"[{now_iso()}][API][erro][5002][frontend_websocket][获取服务器列表失败]")
                        ws.send(json.dumps({"type": "error", "message": f"获取服务器列表失败: {str(e)}"}))
                
                elif action == "subscribe_task":
                    # 订阅任务更新
                    task_id = payload.get("task_id")
                    if task_id:
                        with _frontend_lock:
                            _frontend_clients[client_id]["subscribed_tasks"].add(task_id)
                            if task_id not in _task_subscribers:
                                _task_subscribers[task_id] = set()
                            _task_subscribers[task_id].add(client_id)
                        ws.send(json.dumps({"type": "subscribed", "task_id": task_id, "ok": True}))
                        log(f"[{now_iso()}][API][info][5015][frontend_websocket][前端订阅任务]")

                        # 🔥 核心修复：订阅后立即推送当前任务快照（防止订阅晚于任务完成导致的前端死等）
                        try:
                            # 1. 快速查Redis缓存（如果有）
                            # 暂略，直接查库保真
                            conn_snap = db()
                            cur_snap = conn_snap.cursor(cursor_factory=RealDictCursor)
                            
                            # 获取分片统计
                            cur_snap.execute("SELECT COUNT(*) FILTER (WHERE status='pending') AS pending, COUNT(*) FILTER (WHERE status='running') AS running, COUNT(*) FILTER (WHERE status='done') AS done, COUNT(*) AS total FROM shards WHERE task_id=%s", (task_id,))
                            sc = cur_snap.fetchone() or {}
                            
                            # 获取结果统计
                            cur_snap.execute("SELECT COALESCE(SUM(success),0) AS success, COALESCE(SUM(fail),0) AS fail, COALESCE(SUM(sent),0) AS sent FROM reports WHERE shard_id IN (SELECT shard_id FROM shards WHERE task_id=%s)", (task_id,))
                            rp = cur_snap.fetchone() or {}
                            
                            # 获取主任务状态
                            cur_snap.execute("SELECT status FROM tasks WHERE task_id=%s", (task_id,))
                            tr = cur_snap.fetchone()
                            current_status = tr.get("status") if tr else "pending"
                            
                            conn_snap.close()
                            
                            start_snapshot = {
                                "task_id": task_id,
                                "status": current_status,
                                "shards": {
                                    "pending": int(sc.get("pending", 0)),
                                    "running": int(sc.get("running", 0)), 
                                    "done": int(sc.get("done", 0)), 
                                    "total": int(sc.get("total", 0))
                                },
                                "result": {
                                    "success": int(rp.get("success", 0)), 
                                    "fail": int(rp.get("fail", 0)), 
                                    "sent": int(rp.get("sent", 0))
                                }
                            }
                            
                            ws.send(json.dumps({
                                'type': 'task_update', 
                                'task_id': task_id, 
                                'data': start_snapshot,
                                'is_snapshot': True
                            }))
                            log(f"[{now_iso()}][API][info][5061][frontend_websocket][已推送任务初始快照给前端]")
                            
                        except Exception as e:
                            log(f"[{now_iso()}][API][erro][5064][frontend_websocket][推送任务初始快照失败]")
                
                elif action == "unsubscribe_task":
                    # 取消订阅任务
                    task_id = payload.get("task_id")
                    if task_id:
                        with _frontend_lock:
                            if client_id in _frontend_clients:
                                _frontend_clients[client_id]["subscribed_tasks"].discard(task_id)
                            if task_id in _task_subscribers:
                                _task_subscribers[task_id].discard(client_id)
                                if not _task_subscribers[task_id]:
                                    del _task_subscribers[task_id]
                        ws.send(json.dumps({"type": "unsubscribed", "task_id": task_id, "ok": True}))
                
                elif action == "ping":
                    # 心跳响应 - 保持连接活跃
                    ws.send(json.dumps({"type": "pong", "ts": now_iso()}))
                
            except Exception as e:
                # 超时不是错误，继续循环等待
                if "timed out" in str(e).lower():
                    continue
                # 其他错误才断开连接
                import traceback; error_detail = traceback.format_exc(); log(f"[{now_iso()}][API][erro][5088][frontend_websocket][前端 WS 消息处理错误] action={action} error={str(e)} detail={error_detail}")
                break
    
    except Exception as e:
        log(f"[{now_iso()}][API][erro][5092][frontend_websocket][前端WS错误]")
    
    finally:
        # 清理连接
        with _frontend_lock:
            if client_id in _frontend_clients:
                client = _frontend_clients[client_id]
                # 清理任务订阅
                for task_id in client.get("subscribed_tasks", set()):
                    if task_id in _task_subscribers:
                        _task_subscribers[task_id].discard(client_id)
                        if not _task_subscribers[task_id]:
                            del _task_subscribers[task_id]
                del _frontend_clients[client_id]
        log(f"[{now_iso()}][API][info][5106][frontend_websocket][前端WS断开]")


def broadcast_task_update(task_id: str, update_data: dict):
    LOCATION = "[API][broadcast_task_update]"
    # [STEP 22][api.py][broadcast_task_update] 推送任务更新到所有订阅的前端客户端
    if task_id not in _task_subscribers:
        # 关键兜底：前端如果 WS 断线/订阅丢了，会导致"任务已完成但前端永远卡死"。
        # 这里在没有 task 订阅者时，退化为按 user_id 广播 task_update（前端已 subscribe_user 时仍能收到）。
        try:
            conn = db()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT user_id FROM tasks WHERE task_id=%s", (task_id,))
            row = cur.fetchone() or {}
            conn.close()
            uid = row.get("user_id")
            if uid:
                # broadcast_user_update 会生成 {"type":"task_update","user_id":...,"data":update_data,...}
                # 前端 handleServerMessage 已兼容这种结构（data.type==='task_update' && data.data）
                broadcast_user_update(uid, "task_update", update_data)
        except Exception as e:
            log(f"[{now_iso()}][API][erro][312][broadcast_task_update][兜底按用户广播失败]")
        return

    payload = json.dumps({'type': 'task_update', 'task_id': task_id, 'data': update_data})

    with _frontend_lock:
        subscribers = list(_task_subscribers.get(task_id, []))

    failed_clients = []
    for client_id in subscribers:
        with _frontend_lock:
            client = _frontend_clients.get(client_id)
        if client:
            try:
                client["ws"].send(payload)
            except Exception as e:
                log(f"[{now_iso()}][API][erro][328][broadcast_task_update][推送任务更新失败]")
                failed_clients.append(client_id)

# 清理失败的连接
    if failed_clients:
        with _frontend_lock:
            for client_id in failed_clients:
                if client_id in _frontend_clients:
                    del _frontend_clients[client_id]


def broadcast_user_update(user_id: str, update_type: str, data: dict):
    # 推送用户更新到所有订阅该用户的前端客户端
    payload = json.dumps({'type': update_type, 'user_id': user_id, 'data': data, 'ts': now_iso()})
    
    failed_clients = []
    with _frontend_lock:
        clients_to_notify = [(cid, c) for cid, c in _frontend_clients.items() if c.get("user_id") == user_id]
    
    for client_id, client in clients_to_notify:
        try:
            client["ws"].send(payload)
        except Exception as e:
            log(f"[{now_iso()}][API][erro][351][broadcast_user_update][推送用户更新失败]")
            failed_clients.append(client_id)
    
    # 清理失败的连接
    if failed_clients:
        with _frontend_lock:
            for client_id in failed_clients:
                if client_id in _frontend_clients:
                    del _frontend_clients[client_id]


def broadcast_server_update(server_id: str, update_type: str, server_data: dict):
    # 推送服务器状态更新到所有前端客户端（无需订阅，所有前端都接收）
    payload = json.dumps({
        'type': 'server_update',
        'update_type': update_type,  # 'registered', 'disconnected', 'ready', 'status_changed'
        'server_id': server_id,
        'data': server_data,
        'ts': now_iso()
    })
    
    failed_clients = []
    with _frontend_lock:
        clients_to_notify = list(_frontend_clients.items())
    
    for client_id, client in clients_to_notify:
        try:
            client["ws"].send(payload)
        except Exception as e:
            log(f"[{now_iso()}][API][erro][380][broadcast_server_update][推送服务器更新失败]")
            failed_clients.append(client_id)
    
    # 清理失败的连接
    if failed_clients:
        with _frontend_lock:
            for client_id in failed_clients:
                if client_id in _frontend_clients:
                    del _frontend_clients[client_id]


def _get_servers_list_with_status() -> list:
    # 获取完整的服务器列表（包含Redis实时状态）
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 🔥 从Redis获取在线Worker列表（实时状态）- 快速失败，不阻塞
    try:
        online_workers_set = set(redis_manager.get_online_workers())
    except Exception as e:
        log(f"[{now_iso()}][API][erro][400][_get_servers_list_with_status][获取在线Worker列表失败]")
        online_workers_set = set()
    
    # 从数据库获取所有服务器
    cur.execute("SELECT server_id, server_name, server_url, port, clients_count, status, last_seen, assigned_user AS assigned_user_id, meta FROM servers ORDER BY COALESCE(server_name, server_id)")
    rows = cur.fetchall()
    conn.close()
    
    servers = []
    now_ts = time.time()
    offline_after = int(os.environ.get("SERVER_OFFLINE_AFTER_SECONDS", "120"))
    
    for r in rows:
        server_id = r.get("server_id")
        last_seen = r.get("last_seen")
        status = (r.get("status") or "disconnected").lower()
        clients_count = int(r.get("clients_count") or 0)
        
        # 🔥 优先检查Redis在线状态（最准确）- 快速失败，不阻塞
        if server_id in online_workers_set:
            try:
                # 从Redis获取Worker详细信息（包括ready状态）
                worker_info = redis_manager.get_worker_info(server_id)
                if worker_info:
                    # Redis中有数据，使用Redis的状态
                    is_ready = worker_info.get("ready", False)
                    # ready状态显示为connected，否则显示为available
                    status_out = "connected" if is_ready else "available"
                    # 获取Worker负载
                    load = redis_manager.get_worker_load(server_id)
                else:
                    # Redis在线但无详细信息，默认为connected
                    status_out = "connected"
                    load = 0
            except Exception as e:
                # 🔥 Redis 操作失败时，使用数据库状态，不阻塞
                log(f"[{now_iso()}][API][erro][436][_get_servers_list_with_status][获取Worker信息失败]")
                status_out = _normalize_server_status(status, clients_count)
                load = 0
        elif last_seen:
            # Redis不在线，检查数据库的last_seen
            try:
                age = now_ts - last_seen.timestamp()
                status_out = "disconnected" if age > offline_after else _normalize_server_status(status, clients_count)
            except Exception:
                status_out = _normalize_server_status(status, clients_count)
            load = 0
        else:
            status_out = _normalize_server_status(status, clients_count)
            load = 0
        
        meta = r.get("meta") or {}
        phone_number = meta.get("phone") or meta.get("phone_number") if isinstance(meta, dict) else None
        
        servers.append({
            "server_id": server_id,
            "server_name": r.get("server_name") or server_id,
            "server_url": r.get("server_url") or "",
            "status": status_out,
            "sched_state": _get_worker_sched(server_id).get("state", "ready"),
            "assigned_user_id": r.get("assigned_user_id"),
            "is_assigned": r.get("assigned_user_id") is not None,
            "is_private": r.get("assigned_user_id") is not None,
            "is_public": r.get("assigned_user_id") is None,
            "last_seen": r.get("last_seen").isoformat() if r.get("last_seen") else None,
            "phone_number": phone_number,
            "load": load  # 🔥 添加负载信息
        })
    
    return servers


def broadcast_servers_list_update():
    # 🔥 获取最新服务器列表并推送给所有前端
    try:
        servers = _get_servers_list_with_status()
        payload = json.dumps({
            'type': 'servers_list_update',
            'servers': servers,
            'ts': now_iso()
        })
        
        failed_clients = []
        with _frontend_lock:
            clients_to_notify = list(_frontend_clients.items())
        
        for client_id, client in clients_to_notify:
            try:
                client["ws"].send(payload)
            except Exception as e:
                log(f"[{now_iso()}][API][erro][489][broadcast_servers_list_update][推送服务器列表更新失败]")
                failed_clients.append(client_id)
        
        # 清理失败的连接
        if failed_clients:
            with _frontend_lock:
                for client_id in failed_clients:
                    if client_id in _frontend_clients:
                        del _frontend_clients[client_id]
    except Exception as e:
        log(f"[{now_iso()}][API][erro][499][broadcast_servers_list_update][推送服务器列表更新失败]")


def _broadcast_to_frontend(payload: dict):
    # 向所有前端 WebSocket 广播消息
    dead = []
    with _frontend_lock:
        for sid, info in _frontend_clients.items():
            ws = info["ws"]
            try:
                ws.send(json.dumps(payload))
            except:
                dead.append(sid)
        for sid in dead:
            _frontend_clients.pop(sid, None)
# endregion

# region [WORKER WEBSOCKET]
@sock.route('/ws/worker')
def worker_websocket(ws):
    # Worker WebSocket端点 - 用于macOS客户端连接
    server_id = None
    last_recv_ms = int(time.time() * 1000)
    connected_at_ms = int(time.time() * 1000)
    heartbeat_count = 0
    last_heartbeat_ms = None
    pid = os.getpid()
    close_reason = "unknown"
    close_error_detail = None  # 保存断开时的详细错误信息
    # 跟踪每个服务器的注册和Ready状态，确保一起打印
    _server_status = {"registered": False, "ready": False, "ready_value": False, "logged": False}
    try:
        # 连接建立时不显示详细日志，等待注册完成
        while True:
            try:
                # 增加超时时间到120秒，避免心跳间隔（30秒）导致的误断开
                # 客户端每30秒发送心跳，设置120秒超时可以容忍网络延迟
                data = ws.receive(timeout=120)
                if data is None:
                    close_reason = "receive_none"
                    # 计算诊断信息
                    idle_seconds = (int(time.time() * 1000) - last_recv_ms) // 1000
                    connection_duration = (int(time.time() * 1000) - connected_at_ms) // 1000
                    break
                
                try:
                    msg = json.loads(data)
                except Exception as e:
                    close_reason = "json_error"
                    error_type = type(e).__name__
                    error_msg = str(e)[:160]
                    data_len = len(data) if isinstance(data, str) else None
                    log(f"[{now_iso()}][API][erro][551][worker_websocket][Worker消息解析失败]")
                    log(f"[{now_iso()}][API][erro][552][worker_websocket][错误类型]")
                    log(f"[{now_iso()}][API][erro][553][worker_websocket][错误信息]")
                    if data_len:
                        log(f"[{now_iso()}][API][erro][555][worker_websocket][数据长度异常]")
                    break
                
                # 检查是否是super_admin_response消息（使用type字段）
                msg_type = msg.get("type")
                if msg_type == "super_admin_response":
                    # 将worker的响应转发到所有前端连接
                    command_id = msg.get("command_id", "")
                    response_data = {
                        "type": "super_admin_response",
                        "server_id": server_id,
                        "command_id": command_id,
                        "success": msg.get("success", False),
                        "message": msg.get("message", ""),
                        "logs": msg.get("logs", [])
                    }
                    payload = json.dumps(response_data)
                    
                    # 广播到所有前端连接
                    failed_clients = []
                    with _frontend_lock:
                        clients_to_notify = list(_frontend_clients.items())
                    
                    for client_id, client in clients_to_notify:
                        try:
                            client["ws"].send(payload)
                        except Exception as e:
                            log(f"[{now_iso()}][API][erro][582][worker_websocket][转发超级管理员响应失败]")
                            failed_clients.append(client_id)
                    
                    # 清理失败的连接
                    if failed_clients:
                        with _frontend_lock:
                            for client_id in failed_clients:
                                if client_id in _frontend_clients:
                                    del _frontend_clients[client_id]
                    continue  # 处理完super_admin_response后继续循环
                
                action = msg.get("action")
                payload = msg.get("data", {})
                last_recv_ms = int(time.time() * 1000)


                
                if action == "register":
                    server_id = payload.get("server_id")
                    server_name = payload.get("server_name", "")
                    meta = payload.get("meta", {})
                    is_ready = bool(meta.get("ready", False))
                    
                    if server_id:
                        # [OK] 1. 存储WebSocket连接到内存
                        with _worker_lock:
                            _worker_clients[server_id] = {
                                "ws": ws,
                                "server_name": server_name,
                                "meta": meta,
                                "ready": is_ready,
                                "connected_at": time.time()
                            }
                        
                        # [OK] 2. 使用Redis/内存标记在线状态
                        redis_manager.worker_online(server_id, {
                            "server_name": server_name,
                            "ready": is_ready,
                            "clients_count": 0,
                            "load": 0,
                            "meta": meta if isinstance(meta, dict) else (json.loads(meta) if isinstance(meta, str) else {})
                        })
                        # 初始化调度状态（已有fault时不覆盖）
                        st = _get_worker_sched(server_id)
                        if str(st.get("state") or "ready") != "fault":
                            _set_worker_sched(server_id, {"state": "ready"})
                        
                        # [OK] 3. 更新数据库中的服务器状态
                        try:
                            conn = db()
                            cur = conn.cursor()
                            status = "connected" if is_ready else "available"
                            cur.execute("""
                                INSERT INTO servers(server_id, server_name, status, last_seen, registered_at, meta) 
                                VALUES(%s,%s,%s,NOW(),NOW(),%s) 
                                ON CONFLICT (server_id) DO UPDATE SET 
                                    server_name=EXCLUDED.server_name, 
                                    status=EXCLUDED.status, 
                                    last_seen=NOW(),
                                    meta=EXCLUDED.meta
                            """, (server_id, server_name, status, json.dumps(meta)))
                            conn.commit()
                            conn.close()
                        except Exception as e:
                            # 数据库更新失败不影响连接
                            log(f"[{now_iso()}][API][erro][643][worker_websocket][更新服务器数据库状态失败]")
                        
                        ws.send(json.dumps({"type": "registered", "server_id": server_id, "ok": True}))
                        
                        # 🔥 推送服务器注册事件到所有前端（推送完整列表）
                        try:
                            broadcast_servers_list_update()
                        except Exception as e:
                                log(f"[{now_iso()}][API][erro][713][worker_websocket][推送服务器列表更新失败]")
                        
                        # 记录注册状态
                        _server_status["registered"] = True
                        _server_status["ready"] = is_ready
                        _server_status["ready_value"] = is_ready
                        
                        # 如果注册时已经ready，立即打印两条日志和分隔线
                    if is_ready:
                            log(f"[{now_iso()}][OK]{server_id}: 注册成功")
                            log(f"[{now_iso()}][OK]{server_id}: Ready")
                            _server_status["logged"] = True
                        # 如果注册时未ready，先不打印，等ready时一起打印
                    else:
                        # 注册失败时显示详细日志
                            log(f"[{now_iso()}][API][erro][667][worker_websocket][Worker注册失败缺少server_id]")
                
                elif action == "ready":
                    if server_id:
                        try:
                            ready = payload.get("ready", False)
                            # [OK] 更新内存中的就绪状态
                            with _worker_lock:
                                if server_id in _worker_clients:
                                    _worker_clients[server_id]["ready"] = ready
                            # ready=true 时，非fault节点可恢复为可分配
                            if ready:
                                st = _get_worker_sched(server_id)
                                if str(st.get("state") or "ready") != "fault":
                                    _set_worker_sched(server_id, {"state": "ready", "hb_ok_seq": 0, "quarantine_until": 0})
                            
                            # [OK] 更新Redis中的就绪状态（包含ready字段）
                            try:
                                # 获取当前worker信息
                                worker_info = redis_manager.get_worker_info(server_id) or {}
                                worker_info["ready"] = ready
                                worker_info["last_seen"] = time.time()
                                # 更新Redis
                                redis_manager.update_heartbeat(server_id, worker_info)
                            except Exception as e:
                                log(f"[{now_iso()}][API][erro][687][worker_websocket][更新Redis就绪状态失败]")
                            
                            # [OK] 更新数据库中的就绪状态
                            try:
                                conn = db()
                                cur = conn.cursor()
                                status = "connected" if ready else "available"
                                cur.execute("""
                                    UPDATE servers SET status=%s, last_seen=NOW() 
                                    WHERE server_id=%s
                                """, (status, server_id))
                                conn.commit()
                                conn.close()
                            except Exception as e:
                                log(f"[{now_iso()}][API][erro][701][worker_websocket][更新服务器就绪状态失败]")
                            
                            # 发送响应确认
                            try:
                                ws.send(json.dumps({"type": "ready_ack", "server_id": server_id, "ready": ready, "ok": True}))
                            except Exception:
                                pass  # 发送失败不影响连接
                            
                            # 🔥 推送服务器就绪状态变化到所有前端（推送完整列表）
                            try:
                                broadcast_servers_list_update()
                            except Exception as e:
                                log(f"[{now_iso()}][API][erro][651][worker_websocket][推送服务器列表更新失败]")
                            
                            # 更新ready状态
                            _server_status["ready"] = True
                            _server_status["ready_value"] = ready
                            
                            # 如果已注册，一起打印两条日志和分隔线（确保不被其他服务器日志插入）
                            # 但如果已经打印过（register时ready=True），就不再重复打印
                            if _server_status["registered"] and not _server_status["logged"]:
                                if ready:
                                    log(f"[{now_iso()}][OK]{server_id}: 注册成功")
                                    log(f"[{now_iso()}][OK]{server_id}: Ready")
                                    _server_status["logged"] = True
                                else:
                                    log(f"[{now_iso()}][OK]{server_id}: 注册成功")
                                    log(f"[{now_iso()}][INFO]{server_id}: not ready")
                                    _server_status["logged"] = True
                            # 如果ready先到（理论上不应该发生），只记录状态，等register时一起打印
                        except Exception as e:
                            log(f"[{now_iso()}][API][erro][734][worker_websocket][处理ready消息失败]")
                            import traceback
                            traceback.print_exc()
                            # 不break，继续处理其他消息
                    else:
                        # 错误时显示详细日志
                            log(f"[{now_iso()}][API][erro][740][worker_websocket][Worker就绪状态更新失败缺少server_id]")

                
                elif action == "heartbeat":
                    if server_id:
                        heartbeat_count += 1
                        last_heartbeat_ms = int(time.time() * 1000)
                        # [OK] 更新心跳（包含clients_count等信息）
                        clients_count = payload.get("clients_count", 0)
                        heartbeat_data = {
                            "clients_count": clients_count,
                            "last_seen": time.time()
                        }
                        # 从内存中获取ready状态
                        with _worker_lock:
                            if server_id in _worker_clients:
                                heartbeat_data["ready"] = _worker_clients[server_id].get("ready", False)
                        
                        redis_manager.update_heartbeat(server_id, heartbeat_data)
                        try:
                            _on_worker_heartbeat_ok(server_id)
                        except Exception:
                            pass
                        
                        # 更新数据库中的last_seen和clients_count
                        try:
                            conn = db()
                            cur = conn.cursor()
                            cur.execute("UPDATE servers SET last_seen=NOW(), clients_count=%s WHERE server_id=%s", (clients_count, server_id))
                            conn.commit()
                            conn.close()
                        except Exception:
                            pass  # 数据库更新失败不影响连接
                        
                        ws.send(json.dumps({"type": "heartbeat_ack", "ok": True}))
                        # 避免刷屏：心跳只偶尔打印（最多每 ~60s 一次由 receive 触发），这里不再额外打印

                
                elif action == "shard_result":
                    # Worker上报结果
                    shard_id = payload.get("shard_id")
                    success = int(payload.get("success", 0))
                    fail = int(payload.get("fail", 0))
                    uid = payload.get("user_id")
                    trace_id = payload.get("trace_id")
                    task_id = payload.get("task_id")
                    
                    if shard_id and uid and server_id:
                        log(f"[{now_iso()}][API][info][784][worker_websocket][Shard结果上报成功]")
                        # [OK] 减少该Worker的负载
                        current_load = redis_manager.get_worker_load(server_id)
                        new_load = max(0, current_load - 1)
                        redis_manager.set_worker_load(server_id, new_load)
                        
                        # 原有的结果处理逻辑
                        result = report_shard_result(shard_id, server_id, uid, success, fail, payload)
                        ws.send(json.dumps({"type": "shard_result_ack", "shard_id": shard_id, **result}))

                elif action == "probe_shard_result":
                    data_block = payload.get("data") or {}
                    task_id = data_block.get("task_id")
                    shard_id = data_block.get("shard_id")
                    unsent_phones = data_block.get("unsent_phones") or []
                    sent_phones = data_block.get("sent_phones") or []
                    if task_id and shard_id:
                        try:
                            redis_manager.client.delete(f"shard_probe_inflight:{shard_id}")
                        except Exception:
                            pass
                        # 若已无未发送号码，直接收口该分片，避免任务卡住
                        if isinstance(unsent_phones, list) and len(unsent_phones) == 0:
                            try:
                                conn_h0 = db()
                                cur_h0 = conn_h0.cursor(cursor_factory=RealDictCursor)
                                cur_h0.execute("SELECT user_id FROM tasks WHERE task_id=%s", (task_id,))
                                tr0 = cur_h0.fetchone() or {}
                                uid_h0 = tr0.get("user_id")
                                conn_h0.close()
                                if uid_h0:
                                    report_shard_result(shard_id, server_id, uid_h0, int(len(sent_phones)), 0, {"phase": "send", "from_probe": True})
                                    report_shard_result(shard_id, server_id, uid_h0, int(len(sent_phones)), 0, {"phase": "verify", "from_probe": True})
                            except Exception:
                                pass
                            continue
                        # 仅对未发送号码做接管，避免重复发送
                        if isinstance(unsent_phones, list) and len(unsent_phones) > 0:
                            try:
                                _mark_worker_quarantine(server_id, reason="handoff")
                            except Exception:
                                pass
                            try:
                                _send_worker_ws(server_id, {"type": "cancel_shard", "shard_id": shard_id})
                            except Exception:
                                pass
                            try:
                                conn_h = db()
                                cur_h = conn_h.cursor(cursor_factory=RealDictCursor)
                                cur_h.execute("SELECT user_id, message FROM tasks WHERE task_id=%s", (task_id,))
                                tr = cur_h.fetchone() or {}
                                uid_h = tr.get("user_id")
                                msg_h = tr.get("message")
                                cur_h.execute(
                                    "UPDATE shards SET status='pending', server_id=NULL, phones=%s::jsonb, updated=NOW() WHERE shard_id=%s AND task_id=%s",
                                    (json.dumps(unsent_phones), shard_id, task_id)
                                )
                                conn_h.commit()
                                conn_h.close()
                                if uid_h and msg_h is not None:
                                    _assign_and_push_shards(task_id, uid_h, msg_h)
                                try:
                                    broadcast_task_update(task_id, {
                                        "task_id": task_id,
                                        "status": "running",
                                        "phase_message": "网络不稳定，任务正在自动切换线路继续发送。"
                                    })
                                except Exception:
                                    pass
                            except Exception:
                                try:
                                    conn_h.rollback()
                                    conn_h.close()
                                except Exception:
                                    pass

                elif action == "shard_run_ack":
                    # Worker确认已收到分片（用于定位：推送成功但worker没收到/没动作）
                    shard_id = payload.get("shard_id")
                    task_id = payload.get("task_id")
                    uid = payload.get("user_id")
                    trace_id = payload.get("trace_id")
                    if shard_id and server_id:
                        try:
                            ws.send(json.dumps({"type": "shard_run_ack_ack", "shard_id": shard_id, "ok": True}))
                        except Exception:
                            pass

                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)[:200]
                close_error_detail = f"{error_type}: {error_msg}"
                msg_low = str(e).lower()
                if "timed out" not in msg_low:
                    close_reason = "loop_exception"
                    break
    
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)[:200]
        close_error_detail = f"{error_type}: {error_msg}"
        close_reason = "outer_exception"

    
    finally:
        # [OK] 清理Worker状态
        if server_id:
            with _worker_lock:
                _worker_clients.pop(server_id, None)
            
            redis_manager.remove_worker(server_id)
            
            # 🔥 更新数据库状态为 disconnected
            try:
                conn = db()
                cur = conn.cursor()
                cur.execute("UPDATE servers SET status='disconnected', last_seen=NOW() WHERE server_id=%s", (server_id,))
                conn.commit()
                conn.close()
            except Exception as e:
                log(f"[{now_iso()}][API][erro][839][worker_websocket][更新服务器断开状态失败]")
            
            # 🔥 推送服务器断开事件到所有前端
            try:
                broadcast_server_update(server_id, "disconnected", {
                    "server_id": server_id,
                    "reason": close_reason,
                    "status": "disconnected"
                })
            except Exception as e:
                log(f"[{now_iso()}][API][erro][849][worker_websocket][推送服务器断开事件失败]")
            
            # 统一断开连接日志格式，放在分隔线内，包含诊断信息
            if server_id:
                connection_duration = (int(time.time() * 1000) - connected_at_ms) // 1000
                connection_info = f"连接持续{connection_duration}秒"
                heartbeat_info = f"收到{heartbeat_count}次心跳" if heartbeat_count > 0 else "未收到心跳"
                if last_heartbeat_ms:
                    last_hb_ago = (int(time.time() * 1000) - last_heartbeat_ms) // 1000
                    heartbeat_info += f" (最后心跳{last_hb_ago}秒前)"
                
                if close_reason == "receive_none":
                    # 120秒未收到消息
                    idle_seconds = (int(time.time() * 1000) - last_recv_ms) // 1000
                    last_msg_ago = f"{idle_seconds}秒前"
                    log(f"[{now_iso()}][API][info][864][worker_websocket][Worker断开-120秒未收到消息]")
                    log(f"[{now_iso()}][API][info][865][worker_websocket][原因-120秒未收到消息]")
                    log(f"[{now_iso()}][API][info][866][worker_websocket][诊断信息]")
                    log(f"[{now_iso()}][API][info][867][worker_websocket][建议检查Worker进程]")
                elif close_reason == "loop_exception":
                    # WebSocket异常断开
                    error_detail = close_error_detail if close_error_detail else "未知错误"
                    log(f"[{now_iso()}][API][info][871][worker_websocket][Worker断开-WebSocket连接异常]")
                    log(f"[{now_iso()}][API][info][872][worker_websocket][原因-WebSocket连接异常]")
                    log(f"[{now_iso()}][API][info][873][worker_websocket][诊断信息]")
                    log(f"[{now_iso()}][API][info][874][worker_websocket][建议检查网络连接]")
                elif close_reason == "outer_exception":
                    # 外层异常断开
                    error_detail = close_error_detail if close_error_detail else "未知错误"
                    log(f"[{now_iso()}][API][info][878][worker_websocket][Worker断开-连接处理异常]")
                    log(f"[{now_iso()}][API][info][879][worker_websocket][原因-连接处理异常]")
                    log(f"[{now_iso()}][API][info][880][worker_websocket][诊断信息]")
                    log(f"[{now_iso()}][API][info][881][worker_websocket][建议检查API服务器日志]")
                else:
                    # 其他原因
                    log(f"[{now_iso()}][API][info][884][worker_websocket][Worker断开-其他原因]")
                    log(f"[{now_iso()}][API][info][885][worker_websocket][原因未知]")
                    log(f"[{now_iso()}][API][info][886][worker_websocket][诊断信息]")

def _assign_and_push_shards(task_id: str, user_id: str, message: str, trace_id: str = None) -> dict:
    LOCATION = "[API][_assign_and_push_shards]"
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        with _worker_lock:
            available_servers = [
                sid for sid, c in _worker_clients.items()
                if c.get("ws") and c.get("ready") and _is_worker_assignable(sid)
            ]

        try:
            available_servers = sorted(available_servers)
        except Exception:
            pass

        try:
            broadcast_servers_list_update()
        except Exception as e:
            log(f"[{now_iso()}][API][erro][906][_assign_and_push_shards][推送服务器列表更新失败]")

        if not available_servers:
            conn.close()
            return {"total": 0, "pushed": 0, "failed": 0}

        cur.execute("""
            SELECT shard_id, phones
            FROM shards
            WHERE task_id=%s AND status='pending'
            ORDER BY shard_id
        """, (task_id,))
        pending_shards = cur.fetchall()

        if not pending_shards:
            conn.close()
            return {"total": 0, "pushed": 0, "failed": 0}

        total_shards = len(pending_shards)
        cur.execute("SELECT server_id, server_name FROM servers WHERE server_id = ANY(%s)", (available_servers,))
        server_names = {row['server_id']: row.get('server_name') or row['server_id'] for row in cur.fetchall()}

        try:
            conn.close()
        except Exception:
            pass

        def _safe_phone_count(phones_val) -> int:
            try:
                if isinstance(phones_val, str):
                    return len(json.loads(phones_val) or [])
                return len(phones_val or [])
            except Exception:
                return 0

        def _push_one(idx0: int, shard_row: dict, worker_id: str):
            LOCATION = "[API][_assign_and_push_shards][_push_one]"
            shard_id = shard_row.get("shard_id") if isinstance(shard_row, dict) else shard_row[0]
            phones = shard_row.get("phones") if isinstance(shard_row, dict) else shard_row[1]
            phone_count = _safe_phone_count(phones)
            display = server_names.get(worker_id, worker_id)

            # 负载 +1（失败则回滚负载）
            try:
                redis_manager.incr_worker_load(worker_id, 1)
            except Exception:
                pass

            shard_data = {
                "shard_id": shard_id,
                "task_id": task_id,
                "user_id": user_id,
                "phones": phones,
                "message": message,
                "trace_id": trace_id,
            }

            ok = False
            try:
                from gevent import Timeout
                with Timeout(3):
                    with _worker_lock:
                        client = _worker_clients.get(worker_id)
                        if not client:
                            log(f"[{now_iso()}][API][erro][970][_push_one][Worker未连接]")
                            return False
                        if not client.get("ready"):
                            log(f"[{now_iso()}][API][erro][973][_push_one][Worker未就绪]")
                            return False
                        if not _is_worker_assignable(worker_id):
                            log(f"[{now_iso()}][API][info][974][_push_one][Worker处于隔离/故障状态，跳过分配]")
                            return False
                        ws = client.get("ws")

                ws.send(json.dumps({"type": "shard_run", "shard": shard_data}))
                ok = True

            except Timeout:
                log(f"[{now_iso()}][API][erro][981][_push_one][发送超时3秒]")
                try:
                    with _worker_lock:
                        _worker_clients.pop(worker_id, None)
                except Exception:
                    pass
                ok = False

            except Exception as e:
                log(f"[{now_iso()}][API][erro][990][_push_one][发送失败]")
                ok = False

            if ok:
                with _task_tracker_lock:
                    if task_id in _task_tracker:
                        _task_tracker[task_id]["shard_results"][shard_id] = {"status": "running", "worker": worker_id}
                        _shard_to_task[shard_id] = task_id
                try:
                    conn_u = db()
                    cur_u = conn_u.cursor()
                    cur_u.execute("UPDATE shards SET status='running', server_id=%s, locked_at=NOW(), updated=NOW() WHERE shard_id=%s", (worker_id, shard_id))
                    conn_u.commit()
                    conn_u.close()
                except Exception:
                    try:
                        conn_u.rollback()
                        conn_u.close()
                    except Exception:
                        pass
            else:
                try:
                    redis_manager.decr_worker_load(worker_id, 1)
                except Exception:
                    pass
            return (shard_id, worker_id, ok)

        assignments = []
        for i, shard_row in enumerate(pending_shards):
            worker_id = available_servers[i % len(available_servers)]
            assignments.append((i, shard_row, worker_id))

        greenlets = [spawn(_push_one, i, sr, wid) for (i, sr, wid) in assignments]

        from gevent import joinall
        joinall(greenlets, timeout=30)

        for g in greenlets:
            try:
                if not g.ready():
                    g.kill(block=False)
            except Exception:
                pass

        results = []
        for g in greenlets:
            try:
                val = g.value
                if val and isinstance(val, tuple) and len(val) == 3:
                    results.append(val)
            except Exception:
                pass

        pushed_count = sum(1 for r in results if r[2])
        failed_count = total_shards - pushed_count

        return {"total": total_shards, "pushed": pushed_count, "failed": failed_count}

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return {"total": 0, "pushed": 0, "failed": 0}

def get_ready_workers() -> list:
    """获取所有就绪的worker"""
    with _worker_lock:
        return [
            {"server_id": sid, "server_name": c.get("server_name", ""), "ready": c.get("ready", False)}
            for sid, c in _worker_clients.items()
            if c.get("ready") and _is_worker_assignable(sid)
        ]
# endregion

# region [SUPER ADMIN]

@app.route("/api/super-admin/worker/<server_id>/info", methods=["GET", "OPTIONS"])
def super_admin_worker_info(server_id: str):
    """获取worker详细信息"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    conn = db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # 从数据库获取服务器信息
        cur.execute("SELECT server_id, server_name, server_url, port, status, meta FROM servers WHERE server_id=%s", (server_id,))
        server_row = cur.fetchone()
        conn.close()
        
        if not server_row:
            return jsonify({"success": False, "message": "服务器不存在"}), 404
        
        # 从worker WebSocket连接获取实时状态
        worker_info = None
        with _worker_lock:
            if server_id in _worker_clients:
                worker_info = _worker_clients[server_id]
        
        # 合并信息
        meta = server_row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except:
                meta = {}
        
        if worker_info:
            # 合并worker的meta信息
            worker_meta = worker_info.get("meta", {})
            if isinstance(worker_meta, dict):
                meta.update(worker_meta)
        
        result = {
            "server_id": server_row["server_id"],
            "server_name": server_row.get("server_name"),
            "port": server_row.get("port"),
            "api_url": server_row.get("server_url"),
            "status": server_row.get("status"),
            "meta": meta
        }
        
        return jsonify({"success": True, "info": result})
    except Exception as e:
        conn.close()
        log(f"[{now_iso()}][API][erro][54][super_admin_worker_info][获取worker信息失败]")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/super-admin/worker/<server_id>/control", methods=["POST", "GET", "OPTIONS"])
def super_admin_worker_control(server_id: str):
    """控制worker执行命令"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    # GET请求从query参数获取，POST请求从body获取
    if request.method == "GET":
        action = request.args.get("action")
        params = {}
    else:
        try:
            d = _json()
            action = d.get("action")
            params = d.get("params", {})
        except:
            action = None
            params = {}
    
    if not action:
        return jsonify({"success": False, "message": "缺少action参数"}), 400
    
    # 查找对应的worker WebSocket连接
    worker_ws = None
    with _worker_lock:
        if server_id in _worker_clients:
            worker_ws = _worker_clients[server_id].get("ws")
    
    
    if not worker_ws:
        return jsonify({"success": False, "message": "服务器未连接"}), 404
    
    try:
        # 通过WebSocket发送控制命令
        command_id = secrets.token_urlsafe(8)  # 生成命令ID用于追踪
        command = {
            "type": "super_admin_command",
            "action": action,
            "params": params,
            "command_id": command_id
        }
        worker_ws.send(json.dumps(command))
        

        return jsonify({
            "success": True,
            "message": "命令已发送",
            "command_id": command["command_id"]
        })
    except Exception as e:
        log(f"[{now_iso()}][API][erro][102][super_admin_worker_control][发送控制命令失败: {e}]")
        return jsonify({"success": False, "message": str(e)}), 500

_worker_screenshots = {}
_worker_terminal_outputs = {}
_worker_system_status = {}
_worker_local_config = {}


@app.route("/api/super-admin/worker/<server_id>/config", methods=["POST", "OPTIONS"])
def worker_upload_config(server_id: str):
    """接收Worker本地配置数据（api_url, port, server_phone, stats等）"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    try:
        d = _json()
        _worker_local_config[server_id] = {
            "api_url": d.get("api_url", ""),
            "port": d.get("port", ""),
            "server_phone": d.get("server_phone", ""),
            "stats": d.get("stats", {}),
            "updated": time.time()
        }
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/super-admin/worker/<server_id>/config", methods=["GET"])
def worker_get_config(server_id: str):
    """获取Worker本地配置数据"""
    config = _worker_local_config.get(server_id, {})
    return jsonify({"success": True, "data": config})

@app.route("/api/super-admin/worker/<server_id>/screenshot", methods=["POST", "OPTIONS"])
def super_admin_worker_screenshot(server_id: str):
    """接收Worker截图"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    d = _json()
    img_base64 = d.get("image")
    
    if not img_base64:
        return jsonify({"success": False, "message": "缺少图片数据"}), 400
    
    _worker_screenshots[server_id] = {
        "image": img_base64,
        "timestamp": time.time()
    }
    
    return jsonify({"success": True, "message": "截图已保存"})

@app.route("/api/super-admin/worker/<server_id>/screenshot", methods=["GET"])
def get_worker_screenshot(server_id: str):
    """获取Worker截图"""
    data = _worker_screenshots.get(server_id)
    if data:
        return jsonify({"success": True, "data": data})
    return jsonify({"success": False, "message": "暂无截图"}), 404

# 存储窗口列表
_worker_window_list = {}

@app.route("/api/super-admin/worker/<server_id>/windows", methods=["POST", "OPTIONS"])
def super_admin_worker_windows(server_id: str):
    """接收窗口列表"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    d = _json()
    _worker_window_list[server_id] = {
        "windows": d.get("windows", []),
        "timestamp": time.time()
    }
    return jsonify({"success": True})

@app.route("/api/super-admin/worker/<server_id>/windows", methods=["GET"])
def get_worker_windows(server_id: str):
    """获取窗口列表"""
    data = _worker_window_list.get(server_id)
    if data:
        return jsonify({"success": True, "data": data})
    return jsonify({"success": False, "message": "暂无窗口列表"}), 404

@app.route("/api/super-admin/worker/<server_id>/terminal-output", methods=["POST", "OPTIONS"])
def super_admin_worker_terminal_output(server_id: str):
    """接收终端命令输出"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    d = _json()
    _worker_terminal_outputs[server_id] = {
        "cmd": d.get("cmd", ""),
        "output": d.get("output", ""),
        "exit_code": d.get("exit_code", -1),
        "timestamp": time.time()
    }
    
    return jsonify({"success": True})

@app.route("/api/super-admin/worker/<server_id>/terminal-output", methods=["GET"])
def get_worker_terminal_output(server_id: str):
    """获取终端命令输出"""
    data = _worker_terminal_outputs.get(server_id)
    if data:
        return jsonify({"success": True, "data": data})
    return jsonify({"success": False, "message": "暂无输出"}), 404

@app.route("/api/super-admin/worker/<server_id>/system-status", methods=["POST", "OPTIONS"])
def super_admin_worker_system_status(server_id: str):
    """接收系统状态"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    d = _json()
    _worker_system_status[server_id] = {
        **d,
        "timestamp": time.time()
    }
    
    return jsonify({"success": True})

@app.route("/api/super-admin/worker/<server_id>/system-status", methods=["GET"])
def get_worker_system_status(server_id: str):
    """获取系统状态"""
    data = _worker_system_status.get(server_id)
    if data:
        return jsonify({"success": True, "data": data})
    return jsonify({
        "success": True,
        "data": {
            "cpu": None,
            "memory": None,
            "disk": None,
            "timestamp": None
        },
        "message": "no system status yet"
    }), 200

# endregion

# region [SYSTEM LOGS]

@app.route("/api/admin/logs", methods=["GET", "OPTIONS"])
def get_logs():
    """面板读取日志接口"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    token = _bearer_token()
    conn = db()
    admin_id = _verify_admin_token(conn, token)
    conn.close()

    if not admin_id:
        return jsonify({"ok": False, "message": "Unauthorized"}), 401

    days  = int(request.args.get("days",  3))
    limit = int(request.args.get("limit", 500))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    try:
        conn = db()
        cur  = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            "SELECT timestamp, source, source_type, content FROM log_info "
            "WHERE timestamp > %s ORDER BY timestamp ASC LIMIT %s",
            (cutoff, limit)
        )
        info_rows = [dict(r) for r in cur.fetchall()]

        cur.execute(
            "SELECT timestamp, source, source_type, content FROM log_error "
            "WHERE timestamp > %s ORDER BY timestamp ASC LIMIT %s",
            (cutoff, limit)
        )
        error_rows = [dict(r) for r in cur.fetchall()]

        conn.close()
        return jsonify({"ok": True, "info": info_rows, "error": error_rows})

    except Exception as e:
        if conn: conn.close()
        log(f"[{now_iso()}][API][erro][get_logs][读取日志失败]")
        return jsonify({"ok": False, "message": str(e)}), 500

@app.route("/api/admin/access-logs", methods=["GET", "OPTIONS"])
def get_access_logs():
    """获取访问记录"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    limit = int(request.args.get("limit", "100") or 100)
    offset = int(request.args.get("offset", "0") or 0)
    user_type = request.args.get("user_type")
    ip = request.args.get("ip")
    username = request.args.get("username")
    
    try:
        conn = db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        conditions = []
        params = []
        
        if user_type:
            conditions.append("user_type = %s")
            params.append(user_type)
        if ip:
            conditions.append("ip_address = %s")
            params.append(ip)
        if username:
            conditions.append("username = %s")
            params.append(username)
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
            SELECT id, user_id, username, user_type, ip_address, user_agent, 
                   endpoint, method, status_code, referer, detail, ts 
            FROM access_logs 
            {where_clause}
            ORDER BY ts DESC 
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        cur.execute(query, params)
        logs = cur.fetchall()
        
        count_query = f"SELECT COUNT(*) as total FROM access_logs {where_clause}"
        count_params = params[:-2]
        cur.execute(count_query, count_params)
        total = cur.fetchone()['total']
        
        conn.close()
        
        return jsonify({
            "ok": True, 
            "logs": [dict(log) for log in logs],
            "total": total,
            "limit": limit,
            "offset": offset
        })
    except Exception as e:
        if conn: conn.close()
        log(f"[{now_iso()}][API][erro][400][get_access_logs][获取访问记录失败]")
        return jsonify({"ok": False, "message": str(e)}), 500

@app.route("/api/admin/access-logs/stats", methods=["GET", "OPTIONS"])
def get_access_logs_stats():
    """获取访问记录统计"""
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    try:
        conn = db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                COUNT(*) as total_visits,
                COUNT(DISTINCT ip_address) as unique_ips,
                COUNT(DISTINCT user_id) as unique_users
            FROM access_logs 
            WHERE ts >= CURRENT_DATE
        """)
        today_stats = cur.fetchone()
        
        cur.execute("""
            SELECT user_type, COUNT(*) as count 
            FROM access_logs 
            WHERE ts >= CURRENT_DATE
            GROUP BY user_type
        """)
        user_type_dist = cur.fetchall()
        
        cur.execute("""
            SELECT endpoint, COUNT(*) as count 
            FROM access_logs 
            WHERE ts >= CURRENT_DATE
            GROUP BY endpoint 
            ORDER BY count DESC 
            LIMIT 10
        """)
        top_endpoints = cur.fetchall()
        
        conn.close()
        
        return jsonify({
            "ok": True,
            "today": dict(today_stats),
            "user_types": [dict(r) for r in user_type_dist],
            "top_endpoints": [dict(r) for r in top_endpoints]
        })
    except Exception as e:
        if conn: conn.close()
        log(f"[{now_iso()}][API][erro][454][get_access_logs_stats][获取访问统计失败]")
        return jsonify({"ok": False, "message": str(e)}), 500

# endregion

# region [MAIN]

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 28080))
    
    from gevent import pywsgi
    
    import sys
    
    class FilteredLog:

        def __init__(self, original_log):
            self.original_log = original_log
        
        def write(self, message):
            if '/api/id-library' in message:
                return
            if self.original_log:
                self.original_log.write(message)
            else:
                sys.stderr.write(message)
        
        def flush(self):
            if self.original_log:
                self.original_log.flush()
            else:
                sys.stderr.flush()
    
    filtered_log = FilteredLog(None)
    
    try:
        _init_db_pool()
        log(f"[{now_iso()}][API][info][6178][main][数据库初始化成功]")
    except Exception as e:
        log(f"[{now_iso()}][API][erro][6180][main][数据库初始化失败]")
        
    server = pywsgi.WSGIServer(('0.0.0.0', port), app, log=None)
    
    log(f"[{now_iso()}][API][info][6184][main][API服务器启动成功..]")
    with open("logo.txt", "r", encoding="utf-8") as f:
        content = f.read()
    print(content)
    server.serve_forever()

# endregion
