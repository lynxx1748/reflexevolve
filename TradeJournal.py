"""Local record of decisions, orders, closes, fills, and account value.

The exchange can be asked again for prices. It cannot be asked what the bot
believed at the moment it sent an order. This file keeps that record in the
same SQLite database the bot already opens.
"""
import json
import sqlite3
import threading
import time
from datetime import datetime

import pytz

cst = pytz.timezone("US/Central")


def _now():
    return datetime.now(cst).strftime("%Y-%m-%d %H:%M:%S")


def _num(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


class TradeJournal:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self._last_equity_at = 0.0
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT,
                    trade_side TEXT,
                    status TEXT NOT NULL,
                    qty TEXT,
                    entry_price REAL,
                    stop_price REAL,
                    target_price REAL,
                    risk_reward REAL,
                    order_id TEXT,
                    position_id TEXT,
                    exit_price REAL,
                    realized_pnl REAL,
                    closed_at TEXT,
                    period TEXT,
                    studies_json TEXT,
                    exchange_code INTEGER,
                    exchange_msg TEXT
                );
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT,
                    action TEXT NOT NULL,
                    reason TEXT,
                    entry_price REAL,
                    stop_price REAL,
                    target_price REAL,
                    risk_reward REAL,
                    studies_json TEXT
                );
                CREATE TABLE IF NOT EXISTS position_history (
                    position_id TEXT PRIMARY KEY,
                    symbol TEXT,
                    side TEXT,
                    qty REAL,
                    entry_price REAL,
                    close_price REAL,
                    realized_pnl REAL,
                    fee REAL,
                    funding REAL,
                    opened_at TEXT,
                    closed_at TEXT,
                    raw_json TEXT
                );
                CREATE TABLE IF NOT EXISTS fills (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    side TEXT,
                    qty REAL,
                    price REAL,
                    realized_pnl REAL,
                    reduce_only TEXT,
                    filled_at TEXT,
                    UNIQUE (symbol, side, qty, price, filled_at)
                );
                CREATE TABLE IF NOT EXISTS equity_snapshots (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    available REAL,
                    margin REAL,
                    unrealized_pnl REAL,
                    equity REAL
                );
                """
            )

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def record_decision(self, symbol, action, side="", reason="", entry_price=None,
                        stop_price=None, target_price=None, risk_reward=None, studies=None):
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO decisions (
                    created_at, symbol, side, action, reason,
                    entry_price, stop_price, target_price, risk_reward, studies_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _now(), symbol, side, action, reason,
                    _num(entry_price), _num(stop_price), _num(target_price), _num(risk_reward),
                    json.dumps(studies or {}, default=str),
                ),
            )

    def record_order(self, symbol, side, trade_side, qty, entry_price, stop_price=None,
                     target_price=None, risk_reward=None, period="", studies=None, response=None,
                     position_id=""):
        code = None
        message = None
        order_id = None
        if isinstance(response, dict):
            code = response.get("code")
            message = response.get("msg")
            data = response.get("data") or {}
            if isinstance(data, dict):
                order_id = data.get("orderId")
        status = "submitted" if code == 0 else "rejected"
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO trades (
                    created_at, updated_at, symbol, side, trade_side, status, qty,
                    entry_price, stop_price, target_price, risk_reward, order_id,
                    position_id, period, studies_json, exchange_code, exchange_msg
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _now(), _now(), symbol, side, trade_side, status, str(qty),
                    _num(entry_price), _num(stop_price), _num(target_price), _num(risk_reward),
                    order_id, position_id or None, period,
                    json.dumps(studies or {}, default=str), code, message,
                ),
            )

    def record_close(self, symbol, position_id, exit_price, realized_pnl, qty):
        closed_at = _now()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE trades
                SET status = 'closed',
                    updated_at = ?,
                    closed_at = ?,
                    exit_price = ?,
                    realized_pnl = ?,
                    position_id = COALESCE(?, position_id),
                    qty = COALESCE(?, qty)
                WHERE id = (
                    SELECT id FROM trades
                    WHERE symbol = ? AND trade_side = 'OPEN' AND status IN ('submitted', 'open')
                    ORDER BY id DESC
                    LIMIT 1
                )
                """,
                (closed_at, closed_at, _num(exit_price), _num(realized_pnl), position_id or None, str(qty), symbol),
            )

    def upsert_positions(self, rows):
        if not rows:
            return 0
        saved = 0
        with self._lock, self._connect() as connection:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                position_id = str(row.get("positionId") or "")
                if not position_id:
                    position_id = f"{row.get('symbol')}:{row.get('mtime')}:{row.get('maxQty')}"
                connection.execute(
                    """
                    INSERT INTO position_history (
                        position_id, symbol, side, qty, entry_price, close_price,
                        realized_pnl, fee, funding, opened_at, closed_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(position_id) DO UPDATE SET
                        realized_pnl = excluded.realized_pnl,
                        close_price = excluded.close_price,
                        fee = excluded.fee,
                        funding = excluded.funding,
                        raw_json = excluded.raw_json
                    """,
                    (
                        position_id,
                        row.get("symbol"),
                        row.get("side"),
                        _num(row.get("maxQty")),
                        _num(row.get("entryPrice")),
                        _num(row.get("closePrice")),
                        _num(row.get("realizedPNL")),
                        _num(row.get("fee")),
                        _num(row.get("funding")),
                        str(row.get("ctime") or ""),
                        str(row.get("mtime") or ""),
                        json.dumps(row, default=str),
                    ),
                )
                saved += 1
        return saved

    def upsert_fills(self, rows):
        if not rows:
            return 0
        saved = 0
        with self._lock, self._connect() as connection:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                connection.execute(
                    """
                    INSERT OR IGNORE INTO fills (
                        symbol, side, qty, price, realized_pnl, reduce_only, filled_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row.get("symbol"),
                        row.get("side"),
                        _num(row.get("qty")),
                        _num(row.get("price")),
                        _num(row.get("realizedPNL")),
                        str(row.get("reduceOnly")),
                        str(row.get("ctime") or ""),
                    ),
                )
                saved += 1
        return saved

    def record_equity(self, available, margin, unrealized, equity, min_interval=300):
        now = time.time()
        if now - self._last_equity_at < min_interval:
            return
        self._last_equity_at = now
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO equity_snapshots (
                    created_at, available, margin, unrealized_pnl, equity
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (_now(), _num(available), _num(margin), _num(unrealized), _num(equity)),
            )

    def total_realized_pnl(self):
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0) FROM position_history"
            ).fetchone()
        return float(row[0] or 0)
