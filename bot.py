import os, time, threading, requests, base64, json
from flask import Flask, jsonify
from collections import deque

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────────
CLAUDE_KEY         = os.environ.get("CLAUDE_KEY", "")
WALLET             = os.environ.get("WALLET", "")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY", "")
PAPER_MODE         = os.environ.get("PAPER_MODE", "true").lower() == "true"
RISK_PCT           = float(os.environ.get("RISK_PCT", "1"))
TP_PCT             = float(os.environ.get("TP_PCT", "4"))
SL_PCT             = float(os.environ.get("SL_PCT", "2"))
SCAN_INTERVAL      = int(os.environ.get("SCAN_INTERVAL", "120"))
MONITOR_INTERVAL   = int(os.environ.get("MONITOR_INTERVAL", "30"))
EMA_FAST_LEN       = int(os.environ.get("EMA_FAST", "5"))
EMA_SLOW_LEN       = int(os.environ.get("EMA_SLOW", "10"))
MAX_OPEN_TRADES    = int(os.environ.get("MAX_OPEN_TRADES", "1"))

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# SolanaTracker public RPC — no API key needed for swaps
SWAP_API  = "https://swap-v2.solanatracker.io"
PUBLIC_RPC = "https://rpc.solanatracker.io/public?advancedTx=true"

TOKENS = {
    "SOL":  {"gecko": "solana",                  "type": "major", "min_vol": 1.2, "mint": "So11111111111111111111111111111111111111112"},
    "JUP":  {"gecko": "jupiter-exchange-solana", "type": "defi",  "min_vol": 1.3, "mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"},
    "RAY":  {"gecko": "raydium",                 "type": "defi",  "min_vol": 1.3, "mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"},
    "WIF":  {"gecko": "dogwifcoin",              "type": "meme",  "min_vol": 1.5, "mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"},
    "BONK": {"gecko": "bonk",                    "type": "meme",  "min_vol": 1.5, "mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"},
}

capital          = float(os.environ.get("STARTING_CAPITAL", "54.86"))
capital_lock     = threading.Lock()
open_trades      = {}
trades_lock      = threading.Lock()
price_history    = {sym: deque(maxlen=50) for sym in TOKENS}
vol_cache        = {sym: 1.0 for sym in TOKENS}
trade_log        = []
completed_trades = []
log_lock         = threading.Lock()
scan_active      = True

# ── LOGGING ─────────────────────────────────────────────────────
def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 200:
            trade_log.pop(0)

# ── EMA ─────────────────────────────────────────────────────────
def ema(prices, period):
    p = list(prices)
    if len(p) < period:
        return None
    k = 2 / (period + 1)
    val = p[0]
    for px in p[1:]:
        val = px * k + val * (1 - k)
    return val

# ── PRICE FETCH ──────────────────────────────────────────────────
def safe_coingecko_call(params, retries=3):
    for attempt in range(retries):
        try:
            res = requests.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params=params,
                headers={"accept": "application/json"},
                timeout=10
            )
            data = res.json()
            if "status" in data or not data:
                wait = 30 * (attempt + 1)
                log("warn", f"Rate limit — waiting {wait}s")
                time.sleep(wait)
                continue
            return data
        except Exception as e:
            log("warn", f"API error: {e}")
            time.sleep(10)
    return {}

def get_prices_bulk():
    ids  = ",".join([t["gecko"] for t in TOKENS.values()])
    data = safe_coingecko_call({
        "ids":              ids,
        "vs_currencies":    "usd",
        "include_24hr_vol": "true",
    })
    gecko_to_sym = {t["gecko"]: sym for sym, t in TOKENS.items()}
    prices = {}
    for gecko_id, vals in data.items():
        sym = gecko_to_sym.get(gecko_id)
        if sym and "usd" in vals:
            prices[sym] = float(vals["usd"])
            vol = float(vals.get("usd_24h_vol", 0))
            baselines = {"major": 2e9, "defi": 5e7, "meme": 1e8}
            baseline  = baselines.get(TOKENS[sym]["type"], 1e8)
            vol_cache[sym] = min(max(vol / baseline, 0.1), 5.0)
            log("info", f"${vals['usd']:.4f} | Vol:{vol_cache[sym]:.2f}x", sym)
    if not prices:
        log("warn", "No prices returned")
    return prices

# ── SOLANATRACKER SWAP ───────────────────────────────────────────
def execute_swap(amount_usdc, output_mint, symbol):
    """
    Execute swap using SolanaTracker API.
    - No API key needed
    - No geo-restrictions
    - Supports Raydium, Orca, Pump.fun, Meteora
    - 0.5% fee per successful swap only
    - Funds go directly to Phantom wallet
    """
    try:
        log("info", f"Getting swap route: ${amount_usdc:.2f} USDC -> {symbol}", symbol)

        # Step 1: Get swap instructions
        params = {
            "from":     USDC_MINT,
            "to":       output_mint,
            "amount":   amount_usdc,
            "slippage": 15,
            "payer":    WALLET,
        }

        res = requests.get(
            f"{SWAP_API}/swap-instructions",
            params=params,
            timeout=15
        )
        swap_data = res.json()

        if "error" in swap_data:
            log("err", f"Swap route error: {swap_data['error']}", symbol)
            return None

        # Step 2: Get serialized transaction
        tx_data = swap_data.get("txn") or swap_data.get("transaction") or swap_data.get("tx")

        if not tx_data:
            log("err", f"No transaction in response: {list(swap_data.keys())}", symbol)
            return None

        log("info", f"Transaction built — submitting to Solana...", symbol)

        # Step 3: Submit to Solana RPC
        rpc_res = requests.post(
            PUBLIC_RPC,
            json={
                "jsonrpc": "2.0",
                "id":      1,
                "method":  "sendTransaction",
                "params":  [
                    tx_data,
                    {
                        "encoding":            "base64",
                        "skipPreflight":       True,
                        "preflightCommitment": "processed",
                        "maxRetries":          5,
                    }
                ]
            },
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        rpc_data = rpc_res.json()

        if "error" in rpc_data:
            log("err", f"RPC submit error: {rpc_data['error']}", symbol)
            return None

        tx_sig = rpc_data.get("result")
        if tx_sig:
            log("ok", f"Swap submitted! Tx: {tx_sig[:20]}...", symbol)
            log("ok", f"Solscan: https://solscan.io/tx/{tx_sig}", symbol)
            log("ok", f"Check Phantom — balance updating!", symbol)
            return tx_sig

        log("err", f"No signature in RPC response: {rpc_data}", symbol)
        return None

    except Exception as e:
        log("err", f"Swap failed: {e}", symbol)
        return None

# ── SIGNAL ──────────────────────────────────────────────────────
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

# ── CLAUDE FILTER ────────────────────────────────────────────────
def ask_claude(signal):
    try:
        if not CLAUDE_KEY or CLAUDE_KEY in ["none", ""]:
            approved = signal["vol_ratio"] >= TOKENS[signal["symbol"]]["min_vol"]
            return approved, "Local filter (no Claude key)"
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
        log("warn", f"Claude error: {e}")
        return signal["vol_ratio"] >= TOKENS[signal["symbol"]]["min_vol"], "Local filter"

# ── ENTER TRADE ──────────────────────────────────────────────────
def enter_trade(signal):
    global capital
    with capital_lock:
        risk_amt = capital * (RISK_PCT / 100)

    symbol = signal["symbol"]
    price  = signal["price"]
    mint   = signal["mint"]

    if PAPER_MODE:
        tokens_received = risk_amt / price
        log("ok", f"[PAPER] Buy ${risk_amt:.2f} USDC -> {symbol} @ ${price:.4f} | Got {tokens_received:.6f}", symbol)
    else:
        log("info", f"[LIVE] SolanaTracker swap: ${risk_amt:.2f} USDC -> {symbol}", symbol)
        tx_sig = execute_swap(risk_amt, mint, symbol)
        if not tx_sig:
            log("err", "Swap failed — skipping trade", symbol)
            return False
        tokens_received = risk_amt / price
        log("ok", f"[LIVE] Trade executed! ${risk_amt:.2f} USDC -> {symbol}", symbol)

    with trades_lock:
        open_trades[symbol] = {
            "symbol":          symbol,
            "mint":            mint,
            "type":            signal["type"],
            "entry":           price,
            "tp":              signal["tp"],
            "sl":              signal["sl"],
            "risk_amt":        risk_amt,
            "tokens_received": risk_amt / price,
            "opened_at":       time.strftime("%H:%M:%S"),
            "live":            not PAPER_MODE,
        }
    return True

# ── EXIT TRADE ───────────────────────────────────────────────────
def exit_trade(symbol, current_price, reason):
    global capital
    with trades_lock:
        if symbol not in open_trades:
            return
        trade = open_trades.pop(symbol)

    risk_amt = trade["risk_amt"]
    pnl      = risk_amt * (TP_PCT / SL_PCT) if reason == "TP" else -risk_amt

    with capital_lock:
        capital += pnl

    log("ok" if pnl > 0 else "err",
        f"{'TP HIT' if reason=='TP' else 'SL HIT'} @ ${current_price:.4f} | {'+' if pnl>0 else ''}${pnl:.2f} | Capital: ${capital:.2f}", symbol)

    completed_trades.append({
        "symbol": symbol,
        "entry":  trade["entry"],
        "exit":   current_price,
        "result": reason,
        "pnl":    round(pnl, 2),
        "live":   trade.get("live", False),
        "time":   time.strftime("%H:%M:%S"),
    })

    if capital >= 100:
        log("ok", "GOAL REACHED - $54.86 to $100!")
    if capital < 5:
        global scan_active
        scan_active = False
        log("err", "Capital critical — stopping")

# ── MONITOR LOOP ─────────────────────────────────────────────────
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
            if not price:
                continue
            move = ((price - trade["entry"]) / trade["entry"]) * 100
            log("info", f"${price:.4f} | Move:{move:+.2f}% | TP:${trade['tp']:.4f} | SL:${trade['sl']:.4f}", symbol)
            if price >= trade["tp"]:
                exit_trade(symbol, price, "TP")
            elif price <= trade["sl"]:
                exit_trade(symbol, price, "SL")

# ── SCANNER LOOP ─────────────────────────────────────────────────
def scanner_loop():
    global scan_active
    log("ok", f"Scanner starting | {', '.join(TOKENS.keys())}")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else 'LIVE'} | Risk:{RISK_PCT}% | TP:{TP_PCT}% | SL:{SL_PCT}%")
    log("ok", f"Swap: {'PAPER' if PAPER_MODE else 'SolanaTracker -> Phantom (Raydium/Orca)'}")
    log("info", f"Warming up EMA ({EMA_SLOW_LEN} samples)...")

    for i in range(EMA_SLOW_LEN):
        prices = get_prices_bulk()
        for sym, price in prices.items():
            price_history[sym].append(price)
        log("info", f"Warm-up {i+1}/{EMA_SLOW_LEN} | Got {len(prices)} prices")
        time.sleep(30)

    log("ok", "Warm-up complete — scanning for signals")

    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)

            if num_open >= MAX_OPEN_TRADES:
                log("info", f"Max trades open ({num_open}/{MAX_OPEN_TRADES})")
                time.sleep(SCAN_INTERVAL)
                continue

            log("info", "--- Scan start ---")
            prices = get_prices_bulk()

            if not prices:
                log("warn", "No prices — skipping scan")
                time.sleep(SCAN_INTERVAL)
                continue

            signals_found = []
            for symbol, price in prices.items():
                with trades_lock:
                    if symbol in open_trades:
                        continue
                vol_ratio = vol_cache.get(symbol, 1.0)
                signal    = check_signal(symbol, price, vol_ratio)
                if signal:
                    log("ok", f"SIGNAL FOUND! Vol:{vol_ratio:.2f}x", symbol)
                    signals_found.append(signal)

            signals_found.sort(key=lambda s: s["vol_ratio"], reverse=True)

            for signal in signals_found:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN_TRADES:
                        break
                    if signal["symbol"] in open_trades:
                        continue
                approved, reason = ask_claude(signal)
                if approved:
                    enter_trade(signal)
                else:
                    log("ai", f"Rejected: {reason}", signal["symbol"])

            if not signals_found:
                log("info", f"No signals across {len(prices)} tokens")

        except Exception as e:
            log("err", f"Scanner error: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ENDPOINTS ──────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    with trades_lock:
        n = len(open_trades)
    return f"JupiterBot | Capital: ${capital:.2f} | Trades: {n}/{MAX_OPEN_TRADES} | {'PAPER' if PAPER_MODE else 'LIVE -> Phantom'}", 200

@app.route("/status", methods=["GET"])
def status():
    wins   = len([t for t in completed_trades if t["result"] == "TP"])
    losses = len([t for t in completed_trades if t["result"] == "SL"])
    wr     = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0
    return jsonify({
        "capital":         round(capital, 2),
        "goal":            100,
        "progress_pct":    round(capital / 100 * 100, 1),
        "paper_mode":      PAPER_MODE,
        "swap_engine":     "SolanaTracker (Raydium/Orca)" if not PAPER_MODE else "PAPER",
        "wallet":          f"{WALLET[:8]}...{WALLET[-4:]}" if WALLET else "NOT SET",
        "open_trades":     open_trades,
        "wins":            wins,
        "losses":          losses,
        "win_rate_pct":    wr,
        "tokens_watching": list(TOKENS.keys()),
        "scan_active":     scan_active,
    })

@app.route("/trades", methods=["GET"])
def trades():
    return jsonify({"completed": completed_trades[-50:], "open": open_trades})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-50:]})

# ── START ────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", f"JupiterBot starting — {len(TOKENS)} tokens | Port {port}")
    log("ok", f"Wallet: {WALLET[:8]}...{WALLET[-4:] if WALLET else 'NOT SET'}")
    log("ok", f"Swap engine: {'SolanaTracker API -> Phantom' if not PAPER_MODE else 'PAPER MODE'}")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
