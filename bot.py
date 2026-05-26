import os, time, threading, requests
from flask import Flask, jsonify
from collections import deque

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────────
JUPITER_KEY      = os.environ.get("JUPITER_KEY", "")
CLAUDE_KEY       = os.environ.get("CLAUDE_KEY", "")
WALLET           = os.environ.get("WALLET", "")
PAPER_MODE       = os.environ.get("PAPER_MODE", "true").lower() == "true"
RISK_PCT         = float(os.environ.get("RISK_PCT", "2"))
TP_PCT           = float(os.environ.get("TP_PCT", "4"))
SL_PCT           = float(os.environ.get("SL_PCT", "2"))
SLIP_BPS         = int(os.environ.get("SLIP_BPS", "50"))
SCAN_INTERVAL    = int(os.environ.get("SCAN_INTERVAL", "60"))
MONITOR_INTERVAL = int(os.environ.get("MONITOR_INTERVAL", "15"))
EMA_FAST_LEN     = int(os.environ.get("EMA_FAST", "9"))
EMA_SLOW_LEN     = int(os.environ.get("EMA_SLOW", "21"))
VOL_THRESHOLD    = float(os.environ.get("VOL_THRESHOLD", "1.2"))
MAX_OPEN_TRADES  = int(os.environ.get("MAX_OPEN_TRADES", "3"))

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

TOKENS = {
    "SOL": {
        "mint":      "So11111111111111111111111111111111111111112",
        "coingecko": "solana",
        "type":      "major",
        "min_vol":   1.2,
    },
    "JUP": {
        "mint":      "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
        "coingecko": "jupiter-exchange-solana",
        "type":      "defi",
        "min_vol":   1.3,
    },
    "RAY": {
        "mint":      "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        "coingecko": "raydium",
        "type":      "defi",
        "min_vol":   1.3,
    },
    "PYTH": {
        "mint":      "HZ1JovNiVvGrk7LEKQ5odzBHiqgzaZXdJMNmkMEVSxPt",
        "coingecko": "pyth-network",
        "type":      "defi",
        "min_vol":   1.3,
    },
    "WIF": {
        "mint":      "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
        "coingecko": "dogwifcoin",
        "type":      "meme",
        "min_vol":   1.5,
    },
    "BONK": {
        "mint":      "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        "coingecko": "bonk",
        "type":      "meme",
        "min_vol":   1.5,
    },
    "POPCAT": {
        "mint":      "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        "coingecko": "popcat",
        "type":      "meme",
        "min_vol":   1.6,
    },
    "MYRO": {
        "mint":      "HhJpBhRRn4g56VsyLuT8DL5Bv31HkXqsrahTTUCZeZg4",
        "coingecko": "myro",
        "type":      "meme",
        "min_vol":   1.6,
    },
    "SAMO": {
        "mint":      "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        "coingecko": "samoyedcoin",
        "type":      "meme",
        "min_vol":   1.5,
    },
}

capital          = float(os.environ.get("STARTING_CAPITAL", "60"))
capital_lock     = threading.Lock()
open_trades      = {}
trades_lock      = threading.Lock()
price_history    = {sym: deque(maxlen=50) for sym in TOKENS}
trade_log        = []
scan_active      = True
completed_trades = []
log_lock         = threading.Lock()

def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 300:
            trade_log.pop(0)

def ema(prices, period):
    p = list(prices)
    if len(p) < period:
        return None
    k = 2 / (period + 1)
    val = p[0]
    for px in p[1:]:
        val = px * k + val * (1 - k)
    return val

def get_prices_bulk():
    try:
        mints = ",".join([t["mint"] for t in TOKENS.values()])
        res = requests.get(
            "https://api.jup.ag/price/v2",
            params={"ids": mints},
            timeout=8
        )
        data = res.json().get("data", {})
        prices = {}
        for sym, info in TOKENS.items():
            mint = info["mint"]
            if mint in data:
                prices[sym] = float(data[mint]["price"])
        return prices
    except Exception as e:
        log("warn", f"Bulk price fetch failed: {e}")
        return {}

def get_volume_ratio(coingecko_id):
    try:
        res = requests.get(
            f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart",
            params={"vs_currency": "usd", "days": "1", "interval": "hourly"},
            timeout=6
        )
        vols = [v[1] for v in res.json().get("total_volumes", [])]
        if len(vols) >= 2:
            avg = sum(vols[-20:]) / max(len(vols[-20:]), 1)
            return vols[-1] / avg if avg > 0 else 1.0
    except:
        pass
    return 1.0

def check_signal(symbol, price, vol_ratio):
    hist = price_history[symbol]
    hist.append(price)
    fast = ema(hist, EMA_FAST_LEN)
    slow = ema(hist, EMA_SLOW_LEN)
    if fast is None or slow is None:
        return None
    min_vol = TOKENS[symbol]["min_vol"]
    if fast > slow and vol_ratio >= min_vol:
        tp = price * (1 + TP_PCT / 100)
        sl = price * (1 - SL_PCT / 100)
        return {
            "symbol":    symbol,
            "mint":      TOKENS[symbol]["mint"],
            "type":      TOKENS[symbol]["type"],
            "price":     price,
            "ema_fast":  fast,
            "ema_slow":  slow,
            "vol_ratio": vol_ratio,
            "tp":        tp,
            "sl":        sl,
        }
    return None

def ask_claude(signal):
    try:
        type_note = {
            "major": "Major coin — standard risk is fine.",
            "defi":  "DeFi token — slightly higher volatility,
