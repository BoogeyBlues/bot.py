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
        prices = {}
        for sym, binance_sym in BINANCE_SYMBOLS.items():
            res = requests.get(
                f"https://api.binance.com/api/v3/ticker/price",
                params={"symbol": binance_sym},
                timeout=8
            )
            data = res.json()
            if "price" in data:
                prices[sym] = float(data["price"])
            time.sleep(0.2)
        return prices
    except Exception as e:
        log("warn", f"Price fetch failed: {e}")
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
            "defi":  "DeFi token — slightly higher volatility, confirm strong signal.",
            "meme":  "MEME coin — HIGH volatility. Only approve if signal is very strong.",
        }.get(signal['type'], "")

        prompt = f"""You are a Solana crypto momentum trade filter.
Signal:
- Token: {signal['symbol']} ({signal['type'].upper()})
- Price: ${signal['price']:.6f}
- EMA {EMA_FAST_LEN}: {signal['ema_fast']:.6f}
- EMA {EMA_SLOW_LEN}: {signal['ema_slow']:.6f}
- Volume ratio: {signal['vol_ratio']:.2f}x average
- Take Profit: +{TP_PCT}% | Stop Loss: -{SL_PCT}%
- R:R ratio: {TP_PCT/SL_PCT:.1f}:1
Note: {type_note}
APPROVE only if: clear EMA crossover, volume above threshold, R:R >= 2:1.
Reply: APPROVE or REJECT + one sentence reason."""

        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model":    "claude-sonnet-4-20250514",
                "max_tokens": 80,
                "messages": [{"role": "user", "content": prompt}]
            },
            headers={
                "x-api-key":         CLAUDE_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json"
            },
            timeout=10
        )
        text = res.json()["content"][0]["text"]
        log("ai", f"Claude: {text}", signal['symbol'])
        return "APPROVE" in text.upper(), text
    except Exception as e:
        log("warn", f"Claude unavailable: {e}")
        approved = signal['vol_ratio'] >= TOKENS[signal['symbol']]['min_vol'] and (TP_PCT / SL_PCT) >= 2
        return approved, "Local filter"

def get_quote(amount_usdc, output_mint):
    try:
        res = requests.get(
            "https://api.jup.ag/swap/v1/quote",
            params={
                "inputMint":   USDC_MINT,
                "outputMint":  output_mint,
                "amount":      int(amount_usdc * 1_000_000),
                "slippageBps": SLIP_BPS
            },
            headers={"Authorization": f"Bearer {JUPITER_KEY}"},
            timeout=8
        )
        return res.json()
    except Exception as e:
        log("err", f"Quote failed: {e}")
        return None

def get_sell_quote(token_amount, input_mint, decimals=9):
    try:
        res = requests.get(
            "https://api.jup.ag/swap/v1/quote",
            params={
                "inputMint":   input_mint,
                "outputMint":  USDC_MINT,
                "amount":      int(token_amount * (10 ** decimals)),
                "slippageBps": SLIP_BPS
            },
            headers={"Authorization": f"Bearer {JUPITER_KEY}"},
            timeout=8
        )
        return res.json()
    except Exception as e:
        log("err", f"Sell quote failed: {e}")
        return None

def execute_swap(quote):
    try:
        res = requests.post(
            "https://api.jup.ag/swap/v1/swap",
            json={
                "quoteResponse":             quote,
                "userPublicKey":             WALLET,
                "wrapAndUnwrapSol":          True,
                "prioritizationFeeLamports": 1000
            },
            headers={
                "Authorization": f"Bearer {JUPITER_KEY}",
                "Content-Type":  "application/json"
            },
            timeout=15
        )
        return res.json()
    except Exception as e:
        log("err", f"Swap failed: {e}")
        return None

def enter_trade(signal):
    global capital
    with capital_lock:
        risk_amt = capital * (RISK_PCT / 100)
    symbol = signal['symbol']
    price  = signal['price']
    mint   = signal['mint']
    if PAPER_MODE:
        log("info", f"[PAPER] Buy ${risk_amt:.2f} USDC → {symbol} @ ${price:.6f}", symbol)
        tokens_received = risk_amt / price
        log("ok", f"[PAPER] Got {tokens_received:.4f} {symbol}", symbol)
    else:
        quote = get_quote(risk_amt, mint)
        if not quote:
            return False
        result = execute_swap(quote)
        if not result or "error" in result:
            log("err", f"Swap failed: {result}", symbol)
            return False
        tokens_received = int(quote.get("outAmount", 0)) / 1e9
        log("ok", f"Bought {tokens_received:.4f} {symbol} | Tx: {result.get('txid','?')}", symbol)
    with trades_lock:
        open_trades[symbol] = {
            "symbol":          symbol,
            "mint":            mint,
            "type":            signal['type'],
            "entry":           price,
            "tp":              signal['tp'],
            "sl":              signal['sl'],
            "risk_amt":        risk_amt,
            "tokens_received": tokens_received,
            "opened_at":       time.strftime('%H:%M:%S'),
        }
    log("ok", f"Entry: ${price:.6f} | TP: ${signal['tp']:.6f} | SL: ${signal['sl']:.6f}", symbol)
    return True

def exit_trade(symbol, current_price, reason):
    global capital
    with trades_lock:
        if symbol not in open_trades:
            return
        trade = open_trades.pop(symbol)
    risk_amt = trade['risk_amt']
    rr = TP_PCT / SL_PCT
    pnl = risk_amt * rr if reason == "TP" else -risk_amt
    with capital_lock:
        capital += pnl
    log("ok" if pnl > 0 else "err",
        f"{'✅ TP' if reason=='TP' else '❌ SL'} @ ${current_price:.6f} | {'+' if pnl>0 else ''}${pnl:.2f} | Capital: ${capital:.2f}", symbol)
    completed_trades.append({
        "symbol": symbol,
        "type":   trade['type'],
        "entry":  trade['entry'],
        "exit":   current_price,
        "result": reason,
        "pnl":    round(pnl, 2),
        "time":   time.strftime('%H:%M:%S'),
    })
    if not PAPER_MODE and reason == "TP":
        tokens = trade.get('tokens_received', 0)
        if tokens > 0:
            quote = get_sell_quote(tokens, trade['mint'])
            if quote:
                result = execute_swap(quote)
                log("ok", f"Exit tx: {result.get('txid','?') if result else 'failed'}", symbol)
    if capital >= 100:
        log("ok", "🎯 GOAL REACHED — $60 → $100!")
    if capital < 5:
        log("err", "Capital critical — stopping")
        global scan_active
        scan_active = False

def monitor_loop():
    while True:
        time.sleep(MONITOR_INTERVAL)
        with trades_lock:
            symbols = list(open_trades.keys())
        if not symbols:
            continue
        prices = get_prices_bulk()
        for symbol in symbols:
            with trades_lock:
                if symbol not in open_trades:
                    continue
                trade = open_trades[symbol]
            price = prices.get(symbol)
            if price is None:
                continue
            move = ((price - trade['entry']) / trade['entry']) * 100
            log("info", f"${price:.6f} | Move: {move:+.2f}% | TP: ${trade['tp']:.6f} | SL: ${trade['sl']:.6f}", symbol)
            if price >= trade['tp']:
                exit_trade(symbol, price, "TP")
            elif price <= trade['sl']:
                exit_trade(symbol, price, "SL")

def scanner_loop():
    log("ok", f"Scanning: {', '.join(TOKENS.keys())}")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else 'LIVE'} | Risk: {RISK_PCT}%/trade | Max: {MAX_OPEN_TRADES} trades")
    log("info", f"Warming up EMA ({EMA_SLOW_LEN} samples)...")
    for _ in range(EMA_SLOW_LEN):
        prices = get_prices_bulk()
        for sym, price in prices.items():
            price_history[sym].append(price)
        time.sleep(5)
    log("ok", "Warm-up done — scanning for signals")
    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)
            if num_open >= MAX_OPEN_TRADES:
                log("info", f"Max trades open ({num_open}/{MAX_OPEN_TRADES})")
                time.sleep(SCAN_INTERVAL)
                continue
            prices = get_prices_bulk()
            if not prices:
                time.sleep(SCAN_INTERVAL)
                continue
            signals_found = []
            for symbol, price in prices.items():
                with trades_lock:
                    if symbol in open_trades:
                        continue
                vol_ratio = get_volume_ratio(TOKENS[symbol]["coingecko"])
                time.sleep(1)
                signal = check_signal(symbol, price, vol_ratio)
                if signal:
                    log("ok", f"Signal! Vol:{vol_ratio:.2f}x EMA({signal['ema_fast']:.4f}/{signal['ema_slow']:.4f})", symbol)
                    signals_found.append(signal)
            signals_found.sort(key=lambda s: s['vol_ratio'], reverse=True)
            for signal in signals_found:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN_TRADES:
                        break
                    if signal['symbol'] in open_trades:
                        continue
                approved, reason = ask_claude(signal)
                if approved:
                    enter_trade(signal)
                else:
                    log("ai", f"Rejected: {reason}", signal['symbol'])
            if not signals_found:
                log("info", f"No signals across {len(prices)} tokens")
        except Exception as e:
            log("err", f"Scanner error: {e}")
        time.sleep(SCAN_INTERVAL)

@app.route("/", methods=["GET"])
def home():
    with trades_lock:
        n = len(open_trades)
    return f"JupiterBot | Capital: ${capital:.2f} | Trades: {n}/{MAX_OPEN_TRADES} | {'PAPER' if PAPER_MODE else 'LIVE'}", 200

@app.route("/status", methods=["GET"])
def status():
    wins   = len([t for t in completed_trades if t['result'] == "TP"])
    losses = len([t for t in completed_trades if t['result'] == "SL"])
    wr     = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0
    return jsonify({
        "capital":         round(capital, 2),
        "goal":            100,
        "progress_pct":    round(capital / 100 * 100, 1),
        "paper_mode":      PAPER_MODE,
        "open_trades":     open_trades,
        "wins":            wins,
        "losses":          losses,
        "win_rate_pct":    wr,
        "tokens_watching": list(TOKENS.keys()),
        "settings": {
            "risk_pct":        RISK_PCT,
            "tp_pct":          TP_PCT,
            "sl_pct":          SL_PCT,
            "max_open_trades": MAX_OPEN_TRADES,
            "scan_interval":   SCAN_INTERVAL,
        }
    })

@app.route("/trades", methods=["GET"])
def trades():
    return jsonify({"completed": completed_trades[-50:], "open": open_trades})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-50:]})

if __name__ == "__main__":
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", f"JupiterBot starting — {len(TOKENS)} tokens | Port {port}")
    app.run(host="0.0.0.0", port=port)
