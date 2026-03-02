import os
import sys
import json
import sqlite3
import time
import random
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import List, Tuple, Optional

import akshare as ak
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

# 路径配置
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 注入 lib 路径以支持导入
sys.path.append(os.path.join(BASE_DIR, "data-svc"))

# 导入审计组件
from lib.auditor import DataAuditor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(BASE_DIR, "data/sync.log"))
    ]
)
logger = logging.getLogger("SyncEngineV2")

DB_PATH = os.path.join(BASE_DIR, "data/laicai.db")
CONFIG_PATH = os.path.join(BASE_DIR, "config/config.json")

def load_config():
    tmpl = os.path.join(BASE_DIR, "config/config.template.json")
    if not os.path.exists(CONFIG_PATH):
        logger.warning(f"Config not found at {CONFIG_PATH}, using defaults from template.")
        with open(tmpl, 'r') as f: return json.load(f)
    with open(CONFIG_PATH, 'r') as f: return json.load(f)

CONF = load_config()

def setup_proxy():
    if CONF.get("proxy", {}).get("enabled"):
        os.environ["HTTP_PROXY"] = CONF["proxy"]["http"]
        os.environ["HTTPS_PROXY"] = CONF["proxy"]["https"]
        logger.info(f"Proxy enabled: {os.environ['HTTP_PROXY']}")

class SyncEngine:
    def __init__(self):
        self.pid = os.getpid()
        self.db_path = DB_PATH
        self.auditor = DataAuditor(DB_PATH)
        setup_proxy()

    @contextmanager
    def get_conn(self):
        """为每个线程/任务提供独立的数据库连接，确保线程安全"""
        conn = sqlite3.connect(self.db_path, timeout=30.0) # 增加超时等待以减少锁定冲突
        try:
            yield conn
        finally:
            conn.close()

    def acquire_lock(self) -> bool:
        now = datetime.datetime.now().isoformat()
        try:
            with self.get_conn() as conn:
                # 自动清理 10 分钟前的心跳过期锁
                conn.execute("DELETE FROM global_locks WHERE lock_name = 'sync_worker_master' AND datetime(acquired_at, '+10 minutes') < datetime('now')")
                conn.execute("INSERT INTO global_locks (lock_name, holder_pid, acquired_at) VALUES (?, ?, ?)",
                               ('sync_worker_master', self.pid, now))
                conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def release_lock(self):
        with self.get_conn() as conn:
            conn.execute("DELETE FROM global_locks WHERE lock_name = 'sync_worker_master' AND holder_pid = ?", (self.pid,))
            conn.commit()

    def update_heartbeat(self, symbol: str, status: str = 'RUNNING'):
        with self.get_conn() as conn:
            conn.execute("""
                UPDATE sync_state 
                SET status = ?, running_pid = ?, last_heartbeat = CURRENT_TIMESTAMP 
                WHERE symbol = ?
            """, (status, self.pid, symbol))
            conn.commit()

    def get_pending_tasks(self) -> List[Tuple]:
        with self.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT s.symbol, r.market, s.forward_cursor, s.backward_cursor, s.is_fully_backfilled
                FROM sync_state s
                JOIN stocks_registry r ON s.symbol = r.symbol
                WHERE r.sync_enabled = 1
            """)
            return cursor.fetchall()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _fetch_from_akshare(self, symbol: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
        df = pd.DataFrame()
        # 识别 ETF/Index
        is_etf = symbol.startswith(("SH", "SZ")) and any(char.isdigit() for char in symbol) and len(symbol) >= 8

        if is_etf:
            code = symbol[2:]
            df = ak.fund_etf_hist_em(symbol=code, start_date=start_date, end_date=end_date, adjust="qfq")
        elif market.startswith("A-"):
            df = ak.stock_zh_a_daily(symbol=symbol.lower(), start_date=start_date, end_date=end_date, adjust="qfq")
        elif market == "HK":
            df = ak.stock_hk_daily(symbol=symbol, adjust="qfq")
        elif market == "US":
            df = ak.stock_us_daily(symbol=symbol, adjust="qfq")
            
        return df

    def fetch_and_save(self, symbol: str, market: str, start_date: str, end_date: str) -> int:
        try:
            logger.info(f"[{symbol}] Syncing: {start_date} -> {end_date}")
            
            api_start = start_date.replace('-', '')
            api_end = end_date.replace('-', '')
            local_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}" if '-' not in start_date else start_date
            local_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}" if '-' not in end_date else end_date

            df = self._fetch_from_akshare(symbol, market, api_start, api_end)

            if df.empty: return 0

            df['date'] = df['date'].astype(str).str.slice(0, 10)
            df = df[(df['date'] >= local_start) & (df['date'] <= local_end)]

            if df.empty: return 0

            # --- Data Audit Layer 2: Sanity Check ---
            is_valid, errors = self.auditor.sanity_check(df)
            if not is_valid:
                logger.error(f"[{symbol}] Audit Sanity check failed: {errors}")
                return -1

            df['symbol'] = symbol
            cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'turnover']
            for col in cols: 
                if col not in df.columns: df[col] = 0.0
            
            with self.get_conn() as conn:
                with conn: # 显式事务
                    conn.executemany("""
                        INSERT OR REPLACE INTO daily_candles (symbol, date, open, high, low, close, volume, amount, turnover)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, df[cols].values.tolist())
            
            return len(df)
        except Exception as e:
            logger.error(f"[{symbol}] Fetch/Save Failed: {e}")
            return -1

    def verify_ipo_reached(self, symbol: str, market: str, backward_cursor: str) -> bool:
        curr_dt = datetime.datetime.strptime(backward_cursor, "%Y-%m-%d")
        probe_start_dt = curr_dt - datetime.timedelta(days=180 + 30)
        probe_end_dt = curr_dt - datetime.timedelta(days=180)
        
        probe_start = probe_start_dt.strftime("%Y-%m-%d")
        probe_end = probe_end_dt.strftime("%Y-%m-%d")
        
        logger.info(f"[{symbol}] Probing deep historical window: {probe_start} -> {probe_end}")
        count = self.fetch_and_save(symbol, market, probe_start, probe_end)
        
        return count == 0

    def sync_symbol(self, symbol: str, market: str, f_cursor: str, b_cursor: str, is_full: int):
        try:
            self.update_heartbeat(symbol)
            today = datetime.date.today().strftime("%Y-%m-%d")
            
            # --- 阶段 1: 向后补齐 (Forward) ---
            start_f = (datetime.datetime.strptime(f_cursor, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d") if f_cursor else "2024-01-01"
            if start_f <= today:
                if self.fetch_and_save(symbol, market, start_f, today) >= 0:
                    with self.get_conn() as conn:
                        conn.execute("UPDATE sync_state SET forward_cursor = ? WHERE symbol = ?", (today, symbol))
                        conn.commit()
                        if not b_cursor:
                            conn.execute("UPDATE sync_state SET backward_cursor = ? WHERE symbol = ?", (start_f, symbol))
                            conn.commit()

            # --- 阶段 2: 向前深挖 (Backward) ---
            if not is_full:
                curr_b_dt = datetime.datetime.strptime(b_cursor, "%Y-%m-%d") if b_cursor else datetime.datetime.now()
                target_b_dt = curr_b_dt - datetime.timedelta(days=365)
                target_b = target_b_dt.strftime("%Y-%m-%d")
                end_b = (curr_b_dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                
                count = self.fetch_and_save(symbol, market, target_b, end_b)
                if count == 0: 
                    if self.verify_ipo_reached(symbol, market, target_b):
                        with self.get_conn() as conn:
                            conn.execute("UPDATE sync_state SET is_fully_backfilled = 1 WHERE symbol = ?", (symbol,))
                            conn.commit()
                        logger.info(f"[{symbol}] IPO reached and verified. Fully backfilled.")
                elif count > 0:
                    with self.get_conn() as conn:
                        conn.execute("UPDATE sync_state SET backward_cursor = ? WHERE symbol = ?", (target_b, symbol))
                        conn.commit()
            
            self.update_heartbeat(symbol, 'IDLE')
            delay = random.uniform(CONF["sync_settings"]["safe_delay_min"], CONF["sync_settings"]["safe_delay_max"])
            logger.info(f"[{symbol}] Jitter sleep: {delay:.2f}s...")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"[{symbol}] Sync Task Exception: {e}")

    def run_sync(self):
        if not self.acquire_lock():
            logger.error("Master Lock exists. Exit.")
            return

        try:
            tasks = self.get_pending_tasks()
            with ThreadPoolExecutor(max_workers=3, thread_name_prefix="SyncWorker") as executor:
                executor.map(lambda p: self.sync_symbol(*p), tasks)
            
            logger.info("Sync cycle finished. Triggering final audit...")
            self.run_audit_report()
        finally:
            self.release_lock()

    def run_audit_report(self):
        logger.info("Starting Cycle Audit Report...")
        report = self.auditor.audit_all_enabled_stocks()
        if not report:
            logger.info("Audit: PASS. No gaps found. ✅")
        else:
            logger.warning(f"Audit: FAIL. Gaps found in {len(report)} stocks.")

    def run_audit_and_fix(self):
        logger.info("Starting Deep Audit & Fix...")
        report = self.auditor.audit_all_enabled_stocks()
        if not report:
            logger.info("Audit: NO GAPS. Data integrity confirmed! ✅")
            return

        logger.warning(f"Audit: Found gaps in {len(report)} stocks. Healing...")
        for symbol, gaps in report.items():
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT market FROM stocks_registry WHERE symbol = ?", (symbol,))
                row = cursor.fetchone()
                if not row: continue
                market = row[0]
            
            for gap_date in gaps:
                logger.info(f"Fixing {symbol} gap: {gap_date}")
                self.fetch_and_save(symbol, market, gap_date, gap_date)
                time.sleep(1)
        logger.info("Healing complete.")

if __name__ == "__main__":
    engine = SyncEngine()
    try:
        if "--audit" in sys.argv:
            engine.run_audit_and_fix()
        else:
            engine.run_sync()
    finally:
        # 确保进程退出前释放锁
        engine.release_lock()
