import sqlite3
import json
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "../../data/laicai.db")

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 全局锁表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS global_locks (
        lock_name TEXT PRIMARY KEY,
        holder_pid INTEGER,
        acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 股票注册表：存储配置的基本信息和开关
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stocks_registry (
        symbol TEXT PRIMARY KEY,
        name TEXT,
        market TEXT,
        sync_enabled INTEGER DEFAULT 1,
        buy_range_min REAL,
        buy_range_max REAL,
        sell_range_min REAL,
        sell_range_max REAL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 同步状态表：存储双向指针
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sync_state (
        symbol TEXT,
        timeframe TEXT,
        forward_cursor TEXT,  -- 最新已拉取的日期 (e.g., 2026-03-01)
        backward_cursor TEXT, -- 最远已拉取的日期 (e.g., 2024-03-01)
        is_fully_backfilled INTEGER DEFAULT 0, -- 是否已拉取到 IPO
        status TEXT DEFAULT 'IDLE',
        running_pid INTEGER,
        last_heartbeat TIMESTAMP,
        PRIMARY KEY (symbol, timeframe)
    )
    """)

    # K线数据表 (日线级)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_candles (
        symbol TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        amount REAL,
        turnover REAL,
        PRIMARY KEY (symbol, date)
    )
    """)
    
    # 分钟线表 (按需滚动存储)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS intraday_candles (
        symbol TEXT,
        timeframe TEXT,
        timestamp TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY (symbol, timeframe, timestamp)
    )
    """)

    conn.commit()
    conn.close()

def apply_config(config_path):
    if not os.path.exists(config_path):
        print(f"Error: Config file {config_path} not found.")
        return

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for stock in config.get("stocks", []):
        symbol = stock["symbol"]
        name = stock.get("name", "")
        market = stock.get("market", "")
        enabled = 1 if stock.get("sync_enabled", True) else 0
        buy_min, buy_max = stock.get("buy_range", [None, None])
        sell_min, sell_max = stock.get("sell_range", [None, None])

        # UPSERT 注册表
        cursor.execute("""
        INSERT INTO stocks_registry (symbol, name, market, sync_enabled, buy_range_min, buy_range_max, sell_range_min, sell_range_max)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            name=excluded.name,
            market=excluded.market,
            sync_enabled=excluded.sync_enabled,
            buy_range_min=excluded.buy_range_min,
            buy_range_max=excluded.buy_range_max,
            sell_range_min=excluded.sell_range_min,
            sell_range_max=excluded.sell_range_max,
            updated_at=CURRENT_TIMESTAMP
        """, (symbol, name, market, enabled, buy_min, buy_max, sell_min, sell_max))

        # 初始化同步状态 (如果不存在)
        cursor.execute("""
        INSERT OR IGNORE INTO sync_state (symbol, timeframe)
        VALUES (?, 'daily')
        """, (symbol,))

    conn.commit()
    count = len(config.get("stocks", []))
    print(f"SUCCESS: Applied config for {count} stocks. Database updated.")
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 apply_watchlist.py [init|apply]")
    elif sys.argv[1] == "init":
        init_db()
        print("Database initialized.")
    elif sys.argv[1] == "apply":
        cfg = os.path.join(os.path.dirname(__file__), "../../config/watchlist.json")
        apply_config(cfg)
