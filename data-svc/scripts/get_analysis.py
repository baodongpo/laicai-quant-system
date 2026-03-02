import sys
import os
import json
import sqlite3
import datetime

# 加入 lib 路径
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from lib.indicators import QuantEngine

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No symbol provided"}))
        return

    symbol = sys.argv[1]
    
    # 路径发现
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_path = os.path.join(base_dir, "data/laicai.db")
    
    if not os.path.exists(db_path):
        print(json.dumps({"error": f"Database not found at {db_path}"}))
        return

    engine = QuantEngine(db_path)
    res = engine.get_indicators(symbol)
    
    if res:
        # 增加元数据
        res["timestamp"] = datetime.datetime.now().isoformat()
        res["provider"] = "LaiCai_Local_DB"
        print(json.dumps(res, ensure_ascii=False))
    else:
        print(json.dumps({"error": "Insufficient data in local DB", "symbol": symbol}))

if __name__ == "__main__":
    main()
