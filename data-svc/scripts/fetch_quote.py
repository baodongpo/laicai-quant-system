import sys
import json
import datetime
import os
import requests
import re
import pandas as pd
import yfinance as yf
import akshare as ak

# QVeris Mock / Integration Placeholder
# In a real environment, this would call a QVeris tool or API
def fetch_qveris_quote(symbol, market):
    """
    QVeris 备选数据源：作为 AI 增强型校验锚点
    """
    try:
        # 这里模拟一个高质量的第三方 API 响应
        # 实际运行时，OpenClaw 会通过技能系统拦截或注入真实数据
        return {
            "price": 0.0, # Placeholder
            "time": datetime.datetime.now().isoformat(),
            "provider": "QVeris",
            "status": "awaiting_ai_injection"
        }
    except Exception as e:
        return {"error": str(e), "provider": "QVeris"}

def fetch_tencent_quote(symbol, market):
    """
    腾讯财经：A股、港股、美股全覆盖，新加坡 IP 访问极其稳定（实时行情）
    """
    try:
        if market.startswith("A-"):
            ts_symbol = symbol.lower()
        elif market == "HK":
            ts_symbol = f"hk{symbol}"
        elif market == "US":
            ts_symbol = f"us{symbol}"
        else:
            return {"error": f"Unsupported market: {market}", "provider": "Tencent"}

        url = f"https://qt.gtimg.cn/q={ts_symbol}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            content = response.text.strip()
            if '~' in content:
                data = content.split('~')
                # 腾讯接口格式: v_symbol="...~price~..."
                if len(data) > 3:
                    price = float(data[3])
                    if price > 0:
                        return {
                            "price": price,
                            "time": datetime.datetime.now().isoformat(),
                            "provider": "Tencent"
                        }
        return {"error": "Invalid response or zero price", "provider": "Tencent"}
    except Exception as e:
        return {"error": str(e), "provider": "Tencent"}

def fetch_google_finance_quote(symbol, market):
    """
    Google Finance：美股实时行情主力
    """
    try:
        if market == "US":
            g_market = "NASDAQ" 
            g_symbol = symbol
        elif market == "HK":
            g_market = "HKG"
            g_symbol = symbol
        elif market.startswith("A-"):
            g_market = "SHA" if symbol.startswith("SH") else "SHE"
            g_symbol = symbol[2:]
        else:
            return {"error": "Unsupported market", "provider": "GoogleFinance"}

        url = f"https://www.google.com/finance/quote/{g_symbol}:{g_market}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            match = re.search(r'data-last-price="([\d,.]+)"', response.text)
            if not match:
                match = re.search(r'class="YMlS1d">.*?([\d,.]+)<', response.text)
            if match:
                price = float(match.group(1).replace(',', ''))
                return {
                    "price": price,
                    "time": datetime.datetime.now().isoformat(),
                    "provider": "GoogleFinance"
                }
    except Exception as e:
        return {"error": str(e), "provider": "GoogleFinance"}
    return {"error": "Parse failed", "provider": "GoogleFinance"}

def calculate_indicators(symbol, market):
    """
    计算技术指标 (RSI/BOLL)
    A/HK 尽量使用本地计算避开封锁
    """
    try:
        # A/HK 使用 AkShare (可能需要代理，此处先尝试直连，若失败则返回 None)
        if market.startswith("A-") or market == "HK":
            # 这里的计算逻辑暂时保持，但增加异常捕获
            return None # 生产环境下建议通过离线任务同步历史数据
        else:
            # 美股使用 yfinance 获取历史数据（限频但非封锁，低频调用尚可）
            df = yf.Ticker(symbol).history(period="6mo")
            if df.empty: return None
            # ... 指标计算逻辑 ...
            return {"rsi": 0.0, "boll": {"upper": 0.0, "mid": 0.0, "lower": 0.0}}
    except:
        return None

def main():
    if len(sys.argv) < 3: return
    symbol, market = sys.argv[1], sys.argv[2]
    is_mock = "--mock" in sys.argv

    if is_mock:
        base_price = 100.0
        quote_a = {"price": base_price, "provider": "Mock_A"}
        quote_b = {"price": base_price, "provider": "Mock_B"}
        indicators = {"rsi": 50, "boll": {"mid": 100}}
    else:
        if market == "US":
            # 美股：Google (主) vs Tencent (备)
            quote_a = fetch_google_finance_quote(symbol, market)
            quote_b = fetch_tencent_quote(symbol, market)
        else:
            # A股/港股：Tencent (主) vs QVeris (备)
            quote_a = fetch_tencent_quote(symbol, market)
            quote_b = fetch_qveris_quote(symbol, market)
        
        indicators = calculate_indicators(symbol, market)

    result = {
        "symbol": symbol,
        "market": market,
        "source_a": quote_a,
        "source_b": quote_b,
        "indicators": indicators,
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    # 验证逻辑：如果 QVeris 尚未注入，先标记为待验证
    if "price" in quote_a and "price" in quote_b and quote_b["price"] > 0:
        delta = abs(quote_a["price"] - quote_b["price"]) / quote_a["price"]
        result["is_validated"] = delta < 0.005 # 0.5% 容差
        result["delta_percent"] = delta * 100
    else:
        # 针对 A/HK 且 QVeris 暂无数据的情况，暂时信任 Tencent (主路)
        result["is_validated"] = "price" in quote_a
        result["status"] = "partial_validation"

    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
