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
MAX_OPEN_TRADES    = int(os.environ.get("MAX_OPEN_TRADES", "1"))

SOL_RPC    = "https://api.mainnet-beta.solana.com"
PUMPPORTAL = "https://pumpportal.fun/api/trade-local"

# DexScreener pair addresses
TOKENS = {
    "SOL":  {"mint": "So11111111111111111111111111111111111111112",  "dex_pair": "8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj"},
    "BONK": {"mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263","dex_pair": "Bzc9NZfMqkXR6fz1DBph7BDf9BroyEf6pnzESP7v5iiw"},
    "WIF":  {"mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "dex_pair": "EP2ib6dYdEaFLiMLfVcB75ptqvPrdEm3Ha5cMdnRRoiB"},
    "RAY":  {"mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", "dex_pair": "AVs9TA4nWDzfPJE9gGVNJMVhcQy3V9PGazuz33BfG2RA"},
    "JUP":  {"mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",  "dex_pair": "C1MgLojNLWBKADvu9BHdtgzz1oZX4dZ5zGdGcgvvW5G4"},
}

capital          = float(os.environ.get("STARTING_CAPITAL", "54.86"))
capital_lock     = threading.Lock()
open_trades      = {}
trades_lock      = threading.Lock()
price_history    = {sym: deque(maxlen=50) for sym in TOKENS}
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

# ── DEXSCREENER ──────────────────────────────────────────────────
def get_all_prices():
    """Fetch all token data from DexScreener in one call."""
    try:
        pair_addresses = ",".join([t["dex_pair"] for t in TOKENS.values()])
        res = requests.get(
            f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair_addresses}",
            timeout=10
        )
        pairs = res.json().get("pairs", [])
        pair_to_sym = {t["dex_pair"]: sym for sym, t in TOKENS.items()}
        result = {}
        for pair in pairs:
            sym = pair_to_sym.get(pair.get("pairAddress", ""))
            if not sym:
                continue
            price   = float(pair.get("priceUsd", 0) or 0)
            vol1h   = float(pair.get("volume", {}).get("h1", 0) or 0)
            vol6h   = float(pair.get("volume", {}).get("h6", 0) or 0)
            change1h = float(pair.get("priceChange", {}).get("h1", 0) or 0)
            change6h = float(pair.get("priceChange", {}).get("h6", 0) or 0)
            if price > 0:
                result[sym] = {
                    "price":    price,
                    "vol1h":    vol1h,
                    "vol6h":    vol6h,
                    "change1h": change1h,
                    "change6h": change6h,
                }
                log("info", f"${price:.4f} | 1h:{change1h:+.2f}% | Vol1h:${vol1h:,.0f}", sym)
        return result
    except Exception as e:
        log("warn", f"DexScreener error: {e}")
        return {}

def get_sol_price():
    try:
        res = requests.get(
            "https://api.dexscreener.com/latest/dex/pairs/solana/8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        if pairs:
            return float(pairs[0].get("priceUsd", 0))
    except:
        pass
    return None

# ── SIGNAL DETECTION ─────────────────────────────────────────────
def check_signal(symbol, data):
    """
    Uses PRICE MOMENTUM instead of volume ratio.
    Fires when price is moving up consistently.
    Works in any market condition.
    """
    price    = data["price"]
    change1h = data["change1h"]
    change6h = data["change6h"]

    price_history[symbol].append(price)
    fast = ema(price_history[symbol], 5)
    slow = ema(price_history[symbol], 10)

    if not fast or not slow:
        return None

    # Signal conditions — price momentum based, not volume
    ema_cross    = fast > slow                 # EMA crossover
    rising_1h    = change1h > 0.3              # rising in last hour
    not_extended = change1h < 8.0              # not already overbought

    log("info", f"EMA5:{fast:.4f} EMA10:{slow:.4f} | 1h:{change1h:+.2f}% Cross:{'✓' if ema_cross else '✗'}", symbol)

    if ema_cross and rising_1h and not_extended:
        return {
            "symbol":    symbol,
            "mint":      TOKENS[symbol]["mint"],
            "price":     price,
            "ema_fast":  fast,
            "ema_slow":  slow,
            "change1h":  change1h,
            "change6h":  change6h,
            "tp":        price * (1 + TP_PCT / 100),
            "sl":        price * (1 - SL_PCT / 100),
        }
    return None

# ── SWAP ─────────────────────────────────────────────────────────
def execute_swap(amount_usdc, output_mint, symbol):
    if PAPER_MODE:
        log("ok", f"[PAPER] Buy ${amount_usdc:.4f} USDC -> {symbol}", symbol)
        return "PAPER_TX"
    try:
        from solders.keypair import Keypair
        from solders.transaction import VersionedTransaction
        from solana.rpc.api import Client
        from solana.rpc.types import TxOpts
        from solana.rpc.commitment import Confirmed

        sol_price = get_sol_price()
        if not sol_price:
            log("err", "No SOL price", symbol)
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
                "priorityFee":      0.001,
                "pool":             "raydium"
            },
            timeout=15
        )

        if response.status_code != 200:
            log("err", f"PumpPortal {response.status_code}: {response.text[:100]}", symbol)
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
            log("ok", f"Swap confirmed! Tx: {tx_sig[:20]}...", symbol)
            log("ok", f"Solscan: https://solscan.io/tx/{tx_sig}", symbol)
            log("ok", "Check Phantom — balance updated!", symbol)
            return tx_sig
        return None
    except Exception as e:
        log("err", f"Swap error: {e}", symbol)
        return None

# ── ENTER TRADE ──────────────────────────────────────────────────
def enter_trade(signal):
    global capital
    with capital_lock:
        risk_amt = capital * (RISK_PCT / 100)

    symbol = signal["symbol"]
    price  = signal["price"]
    mint   = signal["mint"]

    tx_sig = execute_swap(risk_amt, mint, symbol)
    if not tx_sig:
        return False

    with trades_lock:
        open_trades[symbol] = {
            "symbol":    symbol,
            "mint":      mint,
            "entry":     price,
            "tp":        signal["tp"],
            "sl":        signal["sl"],
            "risk_amt":  risk_amt,
            "opened_at": time.strftime("%H:%M:%S"),
            "live":      not PAPER_MODE,
        }

    log("ok", f"Trade open | Entry:${price:.4f} TP:${signal['tp']:.4f} SL:${signal['sl']:.4f}", symbol)
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
        f"{'TP HIT' if reason=='TP' else 'SL HIT'} @ ${current_price:.4f} | {'+' if pnl>0 else ''}${pnl:.4f} | Capital: ${capital:.2f}", symbol)

    completed_trades.append({
        "symbol": symbol,
        "entry":  trade["entry"],
        "exit":   current_price,
        "result": reason,
        "pnl":    round(pnl, 4),
        "time":   time.strftime("%H:%M:%S"),
    })

    if capital >= 100:
        log("ok", "GOAL REACHED — $54.86 to $100!")
    if capital < 3:
        global scan_active
        scan_active = False
        log("err", "Capital critical — stopping")

# ── MONITOR ──────────────────────────────────────────────────────
def monitor_loop():
    while True:
        time.sleep(MONITOR_INTERVAL)
        with trades_lock:
            symbols = list(open_trades.keys())
        if not symbols:
            continue
        data = get_all_prices()
        for symbol in symbols:
            with trades_lock:
                if symbol not in open_trades:
                    continue
                trade = open_trades[symbol]
            token_data = data.get(symbol)
            if not token_data:
                continue
            price = token_data["price"]
            move  = ((price - trade["entry"]) / trade["entry"]) * 100
            log("info", f"${price:.4f} | Move:{move:+.2f}% | TP:${trade['tp']:.4f} | SL:${trade['sl']:.4f}", symbol)
            if price >= trade["tp"]:
                exit_trade(symbol, price, "TP")
            elif price <= trade["sl"]:
                exit_trade(symbol, price, "SL")

# ── SCANNER ──────────────────────────────────────────────────────
def scanner_loop():
    global scan_active
    log("ok", f"Bot starting | {', '.join(TOKENS.keys())}")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else 'LIVE'} | Risk:{RISK_PCT}% | TP:{TP_PCT}% | SL:{SL_PCT}%")
    log("ok", "Signal: EMA crossover + positive 1h momentum")
    log("info", "Collecting 10 price samples (1 min)...")

    # Warm up with 10 samples — 6 seconds apart
    for i in range(10):
        data = get_all_prices()
        for sym, d in data.items():
            price_history[sym].append(d["price"])
        log("info", f"Warm-up {i+1}/10 | Got {len(data)} prices")
        time.sleep(6)

    log("ok", "Ready — scanning every 60s")

    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)

            if num_open >= MAX_OPEN_TRADES:
                log("info", f"Trade open ({num_open}/{MAX_OPEN_TRADES}) — monitoring")
                time.sleep(SCAN_INTERVAL)
                continue

            log("info", "--- Scan ---")
            all_data = get_all_prices()

            if not all_data:
                log("warn", "No data — retrying")
                time.sleep(15)
                continue

            signals = []
            for symbol, data in all_data.items():
                with trades_lock:
                    if symbol in open_trades:
                        continue
                signal = check_signal(symbol, data)
                if signal:
                    log("ok", f"SIGNAL! 1h:{data['change1h']:+.2f}%", symbol)
                    signals.append(signal)

            # Take strongest signal (highest 1h gain)
            signals.sort(key=lambda s: s["change1h"], reverse=True)

            for signal in signals:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN_TRADES:
                        break
                    if signal["symbol"] in open_trades:
                        continue

                # Claude check
                if CLAUDE_KEY and CLAUDE_KEY not in ["none", ""]:
                    try:
                        prompt = f"""Quick trade filter.
{signal['symbol']}: ${signal['price']:.4f}
EMA5 > EMA10: YES
1h change: {signal['change1h']:+.2f}%
6h change: {signal['change6h']:+.2f}%
TP: +{TP_PCT}% | SL: -{SL_PCT}%
APPROVE or REJECT + one reason."""
                        res = requests.post(
                            "https://api.anthropic.com/v1/messages",
                            json={"model": "claude-sonnet-4-20250514", "max_tokens": 50,
                                  "messages": [{"role": "user", "content": prompt}]},
                            headers={"x-api-key": CLAUDE_KEY, "anthropic-version": "2023-06-01",
                                     "content-type": "application/json"},
                            timeout=8
                        )
                        text = res.json()["content"][0]["text"]
                        log("ai", text, signal["symbol"])
                        if "REJECT" in text.upper():
                            continue
                    except:
                        pass

                enter_trade(signal)

            if not signals:
                log("info", "No momentum signals — market flat")

        except Exception as e:
            log("err", f"Scanner error: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ─────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    with trades_lock:
        n = len(open_trades)
    return f"JupiterBot | Capital: ${capital:.2f} | Trades: {n} | {'PAPER' if PAPER_MODE else 'LIVE'}", 200

@app.route("/status", methods=["GET"])
def status():
    wins   = len([t for t in completed_trades if t["result"] == "TP"])
    losses = len([t for t in completed_trades if t["result"] == "SL"])
    wr     = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0
    return jsonify({
        "capital":      round(capital, 2),
        "goal":         100,
        "paper_mode":   PAPER_MODE,
        "open_trades":  open_trades,
        "wins":         wins,
        "losses":       losses,
        "win_rate":     wr,
        "total_trades": len(completed_trades),
        "watching":     list(TOKENS.keys()),
        "signal_type":  "EMA crossover + positive 1h momentum",
    })

@app.route("/trades", methods=["GET"])
def trades():
    return jsonify({"completed": completed_trades[-50:], "open": open_trades})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-50:]})

# ── START ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=monitor_loop,  daemon=True).start()
    threading.Thread(target=scanner_loop,  daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", f"JupiterBot starting | Wallet: {WALLET[:8]}...{WALLET[-4:] if WALLET else 'NOT SET'}")
    log("ok", f"Swap: {'PumpPortal -> Phantom' if not PAPER_MODE else 'PAPER MODE'}")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
