import os, time, threading, requests
from flask import Flask, jsonify
from collections import deque

app = Flask(__name__)

CLAUDE_KEY       = os.environ.get("CLAUDE_KEY", "")
WALLET           = os.environ.get("WALLET", "")
PAPER_MODE       = os.environ.get("PAPER_MODE", "true").lower() == "true"
RISK_PCT         = float(os.environ.get("RISK_PCT", "2"))
TP_PCT           = float(os.environ.get("TP_PCT", "4"))
SL_PCT           = float(os.environ.get("SL_PCT", "2"))
SCAN_INTERVAL    = int(os.environ.get("SCAN_INTERVAL", "120"))
MONITOR_INTERVAL = int(os.environ.get("MONITOR_INTERVAL", "30"))
EMA_FAST_LEN     = int(os.environ.get("EMA_FAST", "9"))
EMA_SLOW_LEN     = int(os.environ.get("EMA_SLOW", "21"))
MAX_OPEN_TRADES  = int(os.environ.get("MAX_OPEN_TRADES", "3"))

TOKENS = {
    "SOL":  {"gecko": "solana",                  "type": "major", "min_vol": 1.2, "mint": "So11111111111111111111111111111111111111112"},
    "JUP":  {"gecko": "jupiter-exchange-solana", "type": "defi",  "min_vol": 1.3, "mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"},
    "RAY":  {"gecko": "raydium",                 "type": "defi",  "min_vol": 1.3, "mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"},
    "WIF":  {"gecko": "dogwifcoin",              "type": "meme",  "min_vol": 1.5, "mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"},
    "BONK": {"gecko": "bonk",                    "type": "meme",  "min_vol": 1.5, "mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"},
}

capital          = float(os.environ.get("STARTING_CAPITAL", "60"))
capital_lock     = threading.Lock()
open_trades      = {}
trades_lock      = threading.Lock()
price_history    = {sym: deque(maxlen=50) for sym in TOKENS}
trade_log        = []
completed_trades = []
log_lock         = threading.Lock()
scan_active      = True

def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 200:
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
    """Fetch all prices in one CoinGecko call — rate limit friendly."""
    try:
        ids = ",".join([t["gecko"] for t in TOKENS.values()])
        res = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids":                 ids,
                "vs_currencies":       "usd",
                "include_24hr_vol":    "true",
                "include_24hr_change": "true",
            },
            headers={"accept": "application/json"},
            timeout=10
        )
        data = res.json()

        if "status" in data and "error" in str(data):
            log("warn", f"CoinGecko rate limit hit — waiting 30s")
            time.sleep(30)
            return {}

        gecko_to_sym = {t["gecko"]: sym for sym, t in TOKENS.items()}
        prices = {}
        for gecko_id, vals in data.items():
            sym = gecko_to_sym.get(gecko_id)
            if sym and "usd" in vals:
                prices[sym] = float(vals["usd"])
                log("info", f"${vals['usd']:.4f}", sym)

        if not prices:
            log("warn", f"No prices returned")
        return prices

    except Exception as e:
        log("warn", f"Price fetch failed: {e}")
        return {}

def get_volume_ratio(symbol):
    """Get 24hr volume ratio — uses cached data from bulk call."""
    try:
        gecko_id = TOKENS[symbol]["gecko"]
        res = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids":              gecko_id,
                "vs_currencies":    "usd",
                "include_24hr_vol": "true",
            },
            headers={"accept": "application/json"},
            timeout=8
        )
        data = res.json()
        vol = data.get(gecko_id, {}).get("usd_24h_vol", 0)
        baselines = {"major": 2e9, "defi": 5e7, "meme": 1e8}
        baseline  = baselines.get(TOKENS[symbol]["type"], 1e8)
        ratio     = vol / baseline if baseline > 0 else 1.0
        return min(max(ratio, 0.1), 5.0)
    except:
        return 1.0

def check_signal(symbol, price, vol_ratio):
    hist = price_history[symbol]
    hist.append(price)
    fast = ema(hist, EMA_FAST_LEN)
    slow = ema(hist, EMA_SLOW_LEN)
    if fast is None or slow is None:
        return None
    log("info", f"EMA{EMA_FAST_LEN}:{fast:.4f} EMA{EMA_SLOW_LEN}:{slow:.4f} Vol:{vol_ratio:.2f}x", symbol)
    if fast > slow and vol_ratio >= TOKENS[symbol]["min_vol"]:
        return {
            "symbol":    symbol,
            "mint":      TOKENS[symbol]["mint"],
            "type":      TOKENS[symbol]["type"],
            "price":     price,
            "ema_fast":  fast,
            "ema_slow":  slow,
            "vol_ratio": vol_ratio,
            "tp":        price * (1 + TP_PCT / 100),
            "sl":        price * (1 - SL_PCT / 100),
        }
    return None

def ask_claude(signal):
    try:
        type_note = {
            "major": "Major coin — standard risk.",
            "defi":  "DeFi token — confirm strong signal.",
            "meme":  "MEME coin — be extra strict.",
        }.get(signal["type"], "")
        prompt = f"""Solana momentum trade filter.
Token: {signal['symbol']} ({signal['type']})
Price: ${signal['price']:.6f}
EMA{EMA_FAST_LEN}: {signal['ema_fast']:.6f} | EMA{EMA_SLOW_LEN}: {signal['ema_slow']:.6f}
Volume: {signal['vol_ratio']:.2f}x baseline
TP: +{TP_PCT}% | SL: -{SL_PCT}% | R:R: {TP_PCT/SL_PCT:.1f}:1
{type_note}
APPROVE or REJECT + one reason."""
        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model":      "claude-sonnet-4-20250514",
                "max_tokens": 60,
                "messages":   [{"role": "user", "content": prompt}]
            },
            headers={
                "x-api-key":         CLAUDE_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json"
            },
            timeout=10
        )
        text = res.json()["content"][0]["text"]
        log("ai", text, signal["symbol"])
        return "APPROVE" in text.upper(), text
    except Exception as e:
        log("warn", f"Claude unavailable: {e}")
        return signal["vol_ratio"] >= TOKENS[signal["symbol"]]["min_vol"], "Local filter"

def enter_trade(signal):
    global capital
    with capital_lock:
        risk_amt = capital * (RISK_PCT / 100)
    symbol          = signal["symbol"]
    price           = signal["price"]
    tokens_received = risk_amt / price
    log("ok", f"[PAPER] Buy ${risk_amt:.2f} -> {symbol} @ ${price:.4f} | Got {tokens_received:.4f}", symbol)
    with trades_lock:
        open_trades[symbol] = {
            "symbol":          symbol,
            "mint
