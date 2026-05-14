import os, time, threading, requests
from flask import Flask, request as freq, jsonify

app = Flask(__name__)

# ── CONFIG FROM ENVIRONMENT VARIABLES ──────────────────────────
JUPITER_KEY = os.environ.get("JUPITER_KEY", "")
CLAUDE_KEY  = os.environ.get("CLAUDE_KEY", "")
WALLET      = os.environ.get("WALLET", "")
PAPER_MODE  = os.environ.get("PAPER_MODE", "true").lower() == "true"
RISK_PCT    = float(os.environ.get("RISK_PCT", "2"))      # % of capital per trade
TP_PCT      = float(os.environ.get("TP_PCT", "4"))        # take profit %
SL_PCT      = float(os.environ.get("SL_PCT", "2"))        # stop loss %
SLIP_BPS    = int(os.environ.get("SLIP_BPS", "50"))       # slippage in basis points
CHECK_EVERY = int(os.environ.get("CHECK_EVERY", "15"))    # seconds between price checks

# ── STATE ───────────────────────────────────────────────────────
capital     = float(os.environ.get("STARTING_CAPITAL", "60"))
open_trade  = None   # dict when a trade is active, None otherwise
trade_lock  = threading.Lock()

# ── HELPERS ─────────────────────────────────────────────────────
def log(tag, msg):
    print(f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {msg}", flush=True)

def get_sol_price():
    """Fetch current SOL/USDC price from Jupiter price API."""
    try:
        res = requests.get(
            "https://api.jup.ag/price/v2",
            params={"ids": "So11111111111111111111111111111111111111112"},
            headers={"Authorization": f"Bearer {JUPITER_KEY}"},
            timeout=5
        )
        data = res.json()
        price = float(data["data"]["So11111111111111111111111111111111111111112"]["price"])
        return price
    except Exception as e:
        log("warn", f"Price fetch failed: {e}")
        return None

def get_jupiter_quote(amount_usdc):
    """Get swap quote from Jupiter."""
    try:
        res = requests.get(
            "https://api.jup.ag/swap/v1/quote",
            params={
                "inputMint":  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
                "outputMint": "So11111111111111111111111111111111111111112",       # SOL
                "amount":     int(amount_usdc * 1_000_000),                        # USDC has 6 decimals
                "slippageBps": SLIP_BPS
            },
            headers={"Authorization": f"Bearer {JUPITER_KEY}"},
            timeout=8
        )
        return res.json()
    except Exception as e:
        log("err", f"Quote failed: {e}")
        return None

def execute_swap(quote):
    """Submit swap transaction to Jupiter."""
    try:
        res = requests.post(
            "https://api.jup.ag/swap/v1/swap",
            json={
                "quoteResponse": quote,
                "userPublicKey": WALLET,
                "wrapAndUnwrapSol": True,
                "prioritizationFeeLamports": 1000
            },
            headers={
                "Authorization": f"Bearer {JUPITER_KEY}",
                "Content-Type": "application/json"
            },
            timeout=15
        )
        return res.json()
    except Exception as e:
        log("err", f"Swap failed: {e}")
        return None

def ask_claude(signal_data):
    """Ask Claude to approve or reject a trade signal."""
    try:
        prompt = f"""You are a crypto trade risk filter for a momentum bot.

Signal:
- Pair: {signal_data['pair']}
- Price: ${signal_data['price']:.4f}
- EMA Fast: {signal_data['ema_fast']:.4f} (above slow ✓)
- Volume Ratio: {signal_data['vol_ratio']:.2f}x average
- Take Profit: ${signal_data['tp']:.4f} (+{TP_PCT}%)
- Stop Loss: ${signal_data['sl']:.4f} (-{SL_PCT}%)
- Capital: ${signal_data['capital']:.2f}
- Risk Amount: ${signal_data['risk_amt']:.2f}
- Reward:Risk = {TP_PCT/SL_PCT:.1f}:1

Approve ONLY if: EMA crossover is clear, volume > 1.2x avg, R:R >= 2:1.
Reply with exactly: APPROVE or REJECT, then one short reason."""

        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 80,
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
        log("ai", f"Claude: {text}")
        return "APPROVE" in text.upper(), text
    except Exception as e:
        log("warn", f"Claude unavailable: {e} — using local filter")
        approved = signal_data['vol_ratio'] > 1.2 and (TP_PCT / SL_PCT) >= 2
        return approved, "Local filter applied"

# ── EXIT MONITOR ─────────────────────────────────────────────────
def monitor_trade():
    """
    Runs in a background thread.
    Checks price every CHECK_EVERY seconds.
    Closes trade when TP or SL is hit.
    """
    global open_trade, capital

    log("info", f"Monitoring started — TP: ${open_trade['tp']:.4f} | SL: ${open_trade['sl']:.4f}")

    while True:
        time.sleep(CHECK_EVERY)

        with trade_lock:
            if open_trade is None:
                log("info", "Monitor: trade already closed")
                return

        current_price = get_sol_price()
        if current_price is None:
            log("warn", "Monitor: couldn't fetch price, retrying...")
            continue

        tp = open_trade['tp']
        sl = open_trade['sl']
        entry = open_trade['entry']
        risk_amt = open_trade['risk_amt']
        rr = TP_PCT / SL_PCT

        log("info", f"Price: ${current_price:.4f} | Entry: ${entry:.4f} | TP: ${tp:.4f} | SL: ${sl:.4f}")

        # ── HIT TAKE PROFIT ──
        if current_price >= tp:
            profit = risk_amt * rr
            with trade_lock:
                capital += profit
                closed = open_trade.copy()
                open_trade = None

            log("ok", f"✅ TAKE PROFIT HIT @ ${current_price:.4f} | +${profit:.2f} | Capital: ${capital:.2f}")

            if not PAPER_MODE:
                # Sell SOL back to USDC
                sol_amount = closed.get('sol_received', 0)
                if sol_amount > 0:
                    quote = get_jupiter_quote(sol_amount)  # approx — in production use SOL→USDC quote
                    if quote:
                        result = execute_swap(quote)
                        log("ok", f"Exit tx submitted: {result.get('txid', 'unknown')}")
            return

        # ── HIT STOP LOSS ──
        elif current_price <= sl:
            loss = risk_amt
            with trade_lock:
                capital -= loss
                open_trade = None

            log("err", f"❌ STOP LOSS HIT @ ${current_price:.4f} | -${loss:.2f} | Capital: ${capital:.2f}")

            if capital < 5:
                log("err", "Capital too low — bot shutting down")
                os._exit(1)  # Railway will restart automatically
            return

        # ── STILL OPEN ──
        else:
            move_pct = ((current_price - entry) / entry) * 100
            log("info", f"Trade open | Move: {move_pct:+.2f}% | Watching...")

# ── WEBHOOK ENDPOINT ─────────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    global open_trade, capital

    # Don't accept new signals if trade is open
    if open_trade is not None:
        log("info", "Webhook received but trade already open — skipping")
        return jsonify({"status": "busy", "msg": "Trade already open"}), 200

    data = freq.get_json(silent=True) or {}
    log("info", f"Webhook received: {data}")

    # Parse signal from TradingView
    try:
        price = float(data.get("price", 0))
        symbol = data.get("symbol", "SOL/USDC")
        ema_fast = float(data.get("ema_fast", price * 1.001))
        ema_slow = float(data.get("ema_slow", price * 0.999))
        vol_ratio = float(data.get("vol_ratio", 1.0))
    except Exception as e:
        log("err", f"Bad signal data: {e}")
        return jsonify({"status": "error", "msg": "Invalid signal"}), 400

    if price <= 0:
        return jsonify({"status": "error", "msg": "Missing price"}), 400

    # Calculate levels
    tp = price * (1 + TP_PCT / 100)
    sl = price * (1 - SL_PCT / 100)
    risk_amt = capital * (RISK_PCT / 100)

    signal = {
        "pair": symbol,
        "price": price,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "vol_ratio": vol_ratio,
        "tp": tp,
        "sl": sl,
        "capital": capital,
        "risk_amt": risk_amt
    }

    # Ask Claude
    approved, reason = ask_claude(signal)

    if not approved:
        log("ai", f"Trade rejected: {reason}")
        return jsonify({"status": "rejected", "reason": reason}), 200

    log("ok", f"Trade approved! Entering {symbol} @ ${price:.4f}")

    # Execute entry
    if PAPER_MODE:
        log("info", f"[PAPER] Simulating swap: ${risk_amt:.2f} USDC → SOL @ ${price:.4f}")
        sol_received = risk_amt / price
        log("ok", f"[PAPER] Received ~{sol_received:.6f} SOL")
    else:
        log("info", f"Fetching Jupiter quote for ${risk_amt:.2f} USDC...")
        quote = get_jupiter_quote(risk_amt)
        if not quote:
            return jsonify({"status": "error", "msg": "Quote failed"}), 500

        log("info", "Executing swap on Jupiter...")
        result = execute_swap(quote)
        if not result or "error" in result:
            log("err", f"Swap failed: {result}")
            return jsonify({"status": "error", "msg": "Swap failed"}), 500

        sol_received = int(quote.get("outAmount", 0)) / 1e9
        log("ok", f"Swap tx: {result.get('txid', 'unknown')} | Got {sol_received:.6f} SOL")

    # Store trade + start monitor thread
    with trade_lock:
        open_trade = {
            "pair": symbol,
            "entry": price,
            "tp": tp,
            "sl": sl,
            "risk_amt": risk_amt,
            "sol_received": sol_received if not PAPER_MODE else risk_amt / price,
            "opened_at": time.time()
        }

    monitor_thread = threading.Thread(target=monitor_trade, daemon=True)
    monitor_thread.start()

    return jsonify({
        "status": "entered",
        "pair": symbol,
        "entry": price,
        "tp": tp,
        "sl": sl,
        "risk_usd": risk_amt,
        "paper": PAPER_MODE
    }), 200

# ── STATUS ENDPOINT ──────────────────────────────────────────────
@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "running": True,
        "capital": round(capital, 2),
        "paper_mode": PAPER_MODE,
        "open_trade": open_trade,
        "risk_pct": RISK_PCT,
        "tp_pct": TP_PCT,
        "sl_pct": SL_PCT,
    })

@app.route("/", methods=["GET"])
def home():
    trade_info = f"OPEN: {open_trade['pair']} @ ${open_trade['entry']:.4f}" if open_trade else "No open trade"
    return f"✅ JupiterBot running | Capital: ${capital:.2f} | {trade_info}", 200

# ── START ────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log("ok", f"JupiterBot starting on port {port}")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else '🔴 LIVE'} | Capital: ${capital:.2f} | Risk: {RISK_PCT}% | TP: {TP_PCT}% | SL: {SL_PCT}%")
    app.run(host="0.0.0.0", port=port)
