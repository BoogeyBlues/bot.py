import os, time, threading, requests
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
SCAN_INTERVAL      = int(os.environ.get("SCAN_INTERVAL", "60"))
MONITOR_INTERVAL   = int(os.environ.get("MONITOR_INTERVAL", "30"))
EMA_FAST_LEN       = int(os.environ.get("EMA_FAST", "5"))
EMA_SLOW_LEN       = int(os.environ.get("EMA_SLOW", "10"))
MAX_OPEN_TRADES    = int(os.environ.get("MAX_OPEN_TRADES", "1"))

SOL_RPC    = "https://api.mainnet-beta.solana.com"
PUMPPORTAL = "https://pumpportal.fun/api/trade-local"

# DexScreener pair addresses for Solana tokens
# These are the most liquid USDC pairs on Raydium
TOKENS = {
    "SOL":  {
        "type":     "major",
        "min_vol":  1.2,
        "mint":     "So11111111111111111111111111111111111111112",
        "dex_pair": "8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj",  # SOL/USDC Raydium
    },
    "JUP":  {
        "type":     "defi",
        "min_vol":  1.3,
        "mint":     "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
        "dex_pair": "C1MgLojNLWBKADvu9BHdtgzz1oZX4dZ5zGdGcgvvW5G4",  # JUP/USDC
    },
    "WIF":  {
        "type":     "meme",
        "min_vol":  1.5,
        "mint":     "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
        "dex_pair": "EP2ib6dYdEaFLiMLfVcB75ptqvPrdEm3Ha5cMdnRRoiB",  # WIF/USDC
    },
    "BONK": {
        "type":     "meme",
        "min_vol":  1.5,
        "mint":     "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        "dex_pair": "Bzc9NZfMqkXR6fz1DBph7BDf9BroyEf6pnzESP7v5iiw",  # BONK/USDC
    },
    "RAY":  {
        "type":     "defi",
        "min_vol":  1.3,
        "mint":     "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        "dex_pair": "AVs9TA4nWDzfPJE9gGVNJMVhcQy3V9PGazuz33BfG2RA",  # RAY/USDC
    },
}

capital          = float(os.environ.get("STARTING_CAPITAL", "54.86"))
capital_lock     = threading.Lock()
open_trades      = {}
trades_lock      = threading.Lock()
price_history    = {sym: deque(maxlen=50) for sym in TOKENS}
vol_history      = {sym: deque(maxlen=20) for sym in TOKENS}
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

# ── DEXSCREENER PRICE FETCH ──────────────────────────────────────
def get_prices_bulk():
    """
    DexScreener API — completely free, no rate limits, no API key.
    Fetches all 5 tokens in one call using pair addresses.
    """
    try:
        pair_addresses = ",".join([t["dex_pair"] for t in TOKENS.values()])
        res = requests.get(
            f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair_addresses}",
            timeout=10
        )
        data = res.json()
        pairs = data.get("pairs", [])

        if not pairs:
            log("warn", "DexScreener returned no pairs")
            return {}

        # Map pair address back to symbol
        pair_to_sym = {t["dex_pair"]: sym for sym, t in TOKENS.items()}
        prices = {}

        for pair in pairs:
            pair_addr = pair.get("pairAddress", "")
            sym = pair_to_sym.get(pair_addr)
            if not sym:
                continue

            price = float(pair.get("priceUsd", 0) or 0)
            vol24h = float(pair.get("volume", {}).get("h24", 0) or 0)

            if price > 0:
                prices[sym] = price
                # Calculate volume ratio vs rolling average
                vol_history[sym].append(vol24h)
                avg_vol = sum(vol_history[sym]) / len(vol_history[sym]) if vol_history[sym] else vol24h
                vol_ratio = vol24h / avg_vol if avg_vol > 0 else 1.0
                # Normalize ratio to reasonable range
                vol_ratio = min(max(vol_ratio, 0.1), 5.0)
                # Store in vol_cache via global
                globals().setdefault("vol_cache", {})[sym] = vol_ratio
                log("info", f"${price:.4f} | Vol:{vol_ratio:.2f}x | 24h:${vol24h:,.0f}", sym)

        return prices

    except Exception as e:
        log("warn", f"DexScreener error: {e}")
        return {}

def get_sol_price():
    """Get SOL price for USDC->SOL conversion using DexScreener."""
    try:
        res = requests.get(
            f"https://api.dexscreener.com/latest/dex/pairs/solana/{TOKENS['SOL']['dex_pair']}",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        if pairs:
            return float(pairs[0].get("priceUsd", 0))
    except Exception as e:
        log("warn", f"SOL price error: {e}")
    return None

# Ensure vol_cache exists globally
vol_cache = {sym: 1.0 for sym in TOKENS}

# ── PUMPPORTAL SWAP ──────────────────────────────────────────────
def execute_swap(amount_usdc, output_mint, symbol):
    """
    PumpPortal Local Transaction API.
    No API key. No geo-restrictions.
    Signs with Phantom private key. Funds go to Phantom.
    """
    try:
        from solders.keypair import Keypair
        from solders.transaction import VersionedTransaction
        from solana.rpc.api import Client
        from solana.rpc.types import TxOpts
        from solana.rpc.commitment import Confirmed

        sol_price = get_sol_price()
        if not sol_price:
            log("err", "Could not get SOL price", symbol)
            return None

        sol_amount = round(amount_usdc / sol_price, 6)
        log("info", f"${amount_usdc:.4f} USDC = {sol_amount} SOL -> {symbol}", symbol)

        response = requests.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={
                "publicKey":        WALLET,
                "action":           "buy",
                "mint":             output_mint,
                "denominatedInSol": "true",
                "amount":           sol_amount,
                "slippage":         15,
                "priorityFee":      0.0005,
                "pool":             "raydium"
            },
            timeout=15
        )

        if response.status_code != 200:
            log("err", f"PumpPortal {response.status_code}: {response.text[:150]}", symbol)
            return None

        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx = VersionedTransaction(
            VersionedTransaction.from_bytes(response.content).message,
            [keypair]
        )

        client = Client(SOL_RPC)
        opts = TxOpts(skip_preflight=True, preflight_commitment=Confirmed)
        result = client.send_raw_transaction(bytes(tx), opts=opts)

        tx_sig = str(result.value)
        if tx_sig and len(tx_sig) > 10:
            log("ok", f"Swap done! Tx: {tx_sig[:20]}...", symbol)
            log("ok", f"Solscan: https://solscan.io/tx/{tx_sig}", symbol)
            log("ok", "Check Phantom — balance updated!", symbol)
            return tx_sig

        log("err", f"Bad signature: {result}", symbol)
        return None

    except Exception as e:
        log("err", f"Swap error: {e}", symbol)
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
            return approved, "Local filter"
        type_note = {
            "major": "Major coin — standard risk.",
            "defi":  "DeFi token — confirm strong signal.",
            "meme":  "MEME coin — be extra strict.",
        }.get(signal["type"], "")
        prompt = f"""Solana momentum trade filter.
Token: {signal['symbol']} ({signal['type']})
Price: ${signal['price']:.6f}
EMA{EMA_FAST_LEN}: {signal['ema_fast']:.6f} | EMA{EMA_SLOW_LEN}: {signal['ema_slow']:.6f}
Volume: {signal['vol_ratio']:.2f}x average
TP: +{TP_PCT}% | SL: -{SL_PCT}% | R:R: {TP_PCT/SL_PCT:.1f}:1
{type_note}
APPROVE or REJECT + one reason."""
        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 60,
                "messages": [{"role": "user", "content": prompt}]
            },
            headers={
                "x-api-key": CLAUDE_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
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
        log("ok", f"[PAPER] Buy ${risk_amt:.2f} -> {symbol} @ ${price:.4f}", symbol)
    else:
        log("info", f"[LIVE] Swapping ${risk_amt:.2f} -> {symbol}", symbol)
        tx_sig = execute_swap(risk_amt, mint, symbol)
        if not tx_sig:
            log("err", "Swap failed — skipping", symbol)
            return False
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
        log("ok", "GOAL REACHED — $54.86 to $100!")
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
    log("ok", f"Price: DexScreener (no rate limits) | Swap: PumpPortal -> Phantom")
    log("info", f"Warming up EMA ({EMA_SLOW_LEN} samples, 10s apart)...")

    for i in range(EMA_SLOW_LEN):
        prices = get_prices_bulk()
        for sym, price in prices.items():
            price_history[sym].append(price)
        log("info", f"Warm-up {i+1}/{EMA_SLOW_LEN} | Got {len(prices)} prices")
        time.sleep(10)  # 10s between warmup calls — DexScreener has no rate limits

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
                log("warn", "No prices — retrying in 15s")
                time.sleep(15)
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
        "price_source":    "DexScreener",
        "swap_engine":     "PumpPortal -> Phantom" if not PAPER_MODE else "PAPER",
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
    log("ok", f"Price: DexScreener | Swap: {'PumpPortal -> Phantom' if not PAPER_MODE else 'PAPER MODE'}")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
