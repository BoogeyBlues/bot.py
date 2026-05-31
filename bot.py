import os, time, threading, requests
from flask import Flask, jsonify
from collections import defaultdict

try:
    from solders.keypair import Keypair
    from solders.transaction import VersionedTransaction
    from solana.rpc.api import Client
    from solana.rpc.types import TxOpts
    _SOLANA_AVAILABLE = True
except ImportError:
    _SOLANA_AVAILABLE = False

_session = requests.Session()
_session.trust_env = False

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────
WALLET             = os.environ.get("WALLET", "")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY", "")
_PAPER_ENV         = os.environ.get("PAPER_MODE", "true").lower()
PAPER_MODE         = _PAPER_ENV == "true" or not WALLET or not WALLET_PRIVATE_KEY
TRADE_AMOUNT       = float(os.environ.get("TRADE_AMOUNT", "10"))
TP_PCT             = float(os.environ.get("TP_PCT", "30"))
SL_PCT             = float(os.environ.get("SL_PCT", "15"))
MAX_HOLD_SECS      = int(os.environ.get("MAX_HOLD_SECS", "90"))
MAX_OPEN           = int(os.environ.get("MAX_OPEN", "3"))
MAX_PER_COIN       = int(os.environ.get("MAX_PER_COIN", "1"))
SCAN_INTERVAL      = int(os.environ.get("SCAN_INTERVAL", "5"))
MIN_BOND_PCT       = float(os.environ.get("MIN_BOND_PCT", "1"))
MAX_BOND_PCT       = float(os.environ.get("MAX_BOND_PCT", "40"))
MAX_TOKEN_AGE_MINS = int(os.environ.get("MAX_TOKEN_AGE_MINS", "30"))
MIN_REPLIES        = int(os.environ.get("MIN_REPLIES", "1"))

SOL_RPC    = "https://api.mainnet-beta.solana.com"
PUMPPORTAL = "https://pumpportal.fun/api/trade-local"

# ── STATE ──────────────────────────────────────────────────
capital          = float(os.environ.get("STARTING_CAPITAL", "54.86"))
capital_lock     = threading.Lock()
open_trades      = {}
trades_lock      = threading.Lock()
coin_trade_count = defaultdict(int)
blacklisted      = set()
trade_log        = []
completed_trades = []
log_lock         = threading.Lock()
scan_active      = True

# ── LOGGING ─────────────────────────────────────────────────
def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 500:
            trade_log.pop(0)

# ── PRICE FROM BONDING CURVE ───────────────────────────────────────────────
def get_price(mint):
    try:
        res = _session.get(
            f"https://frontend-api-v3.pump.fun/coins/{mint}",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://pump.fun/"},
            timeout=5
        )
        if res.status_code == 200:
            data = res.json()
            vsol = float(data.get("virtual_sol_reserves", 0) or 0)
            vtok = float(data.get("virtual_token_reserves", 0) or 0)
            bond = min((vsol / 85_000_000_000) * 100, 99.9) if vsol > 0 else 0
            if vsol > 0 and vtok > 0:
                return vsol / vtok, bond
    except:
        pass
    return None, None

# ── SOL PRICE ─────────────────────────────────────────────────────────────────
def get_sol_price():
    try:
        res   = _session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=8
        )
        price = float(res.json()["solana"]["usd"])
        if price > 0:
            return price
    except:
        pass
    try:
        res   = _session.get(
            "https://api.dexscreener.com/latest/dex/pairs/solana/8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        if pairs:
            price = float(pairs[0].get("priceUsd", 0))
            if price > 0:
                return price
    except:
        pass
    return 150

# ── FETCH COINS ───────────────────────────────────────────────────────────────
def get_coins():
    endpoints = [
        "https://frontend-api-v3.pump.fun/coins/currently-live?offset=0&limit=50&includeNsfw=false&order=DESC",
        "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false",
        "https://frontend-api-v2.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false",
    ]
    now = time.time()
    for url in endpoints:
        try:
            res = _session.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
                    "Accept": "application/json",
                    "Referer": "https://pump.fun/",
                    "Origin": "https://pump.fun",
                },
                timeout=8
            )
            if res.status_code != 200:
                continue
            data  = res.json()
            items = data if isinstance(data, list) else data.get("coins", [])
            if not items:
                continue
            coins = []
            for coin in items[:50]:
                mint     = coin.get("mint", "")
                complete = coin.get("complete", False)
                vsol     = float(coin.get("virtual_sol_reserves", 0) or 0)
                vtok     = float(coin.get("virtual_token_reserves", 0) or 0)
                if not mint or complete or vsol <= 0 or vtok <= 0:
                    continue
                bond     = min((vsol / 85_000_000_000) * 100, 99.9)
                created  = coin.get("created_timestamp", 0) or 0
                age_mins = (now - created / 1000) / 60 if created else 999
                coins.append({
                    "mint":     mint,
                    "symbol":   coin.get("symbol", mint[:8]),
                    "bond_pct": round(bond, 1),
                    "price":    vsol / vtok,
                    "twitter":  bool(coin.get("twitter")),
                    "telegram": bool(coin.get("telegram")),
                    "replies":  int(coin.get("reply_count", 0) or 0),
                    "age_mins": round(age_mins, 1),
                })
            if coins:
                log("info", f"Fetched {len(coins)} coins")
                return coins
        except Exception as e:
            log("warn", f"Fetch error: {e}")
    return []

# ── ENTRY FILTER ──────────────────────────────────────────────────────────────
def should_enter(coin):
    mint = coin["mint"]
    if mint in blacklisted:
        return False, "Blacklisted"
    if coin_trade_count[mint] >= MAX_PER_COIN:
        return False, "Already traded"
    if coin["bond_pct"] < MIN_BOND_PCT:
        return False, f"Bond {coin['bond_pct']:.1f}% too low"
    if coin["bond_pct"] > MAX_BOND_PCT:
        return False, f"Bond {coin['bond_pct']:.1f}% too high"
    if coin["age_mins"] > MAX_TOKEN_AGE_MINS:
        return False, f"Too old {coin['age_mins']:.0f}m"
    if not coin.get("twitter") and not coin.get("telegram"):
        return False, "No social links"
    if coin.get("replies", 0) < MIN_REPLIES:
        return False, "No replies"
    return True, f"Bond:{coin['bond_pct']:.1f}% Age:{coin['age_mins']:.1f}m"

# ── EXECUTE BUY ──────────────────────────────────────────────────────────────
def execute_buy(mint, symbol):
    if PAPER_MODE:
        log("ok", f"[PAPER] Buy ${TRADE_AMOUNT}", symbol)
        return "PAPER_TX"
    try:
        sol_price  = get_sol_price()
        sol_amount = round(TRADE_AMOUNT / sol_price, 6)
        res = _session.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={"publicKey": WALLET, "action": "buy", "mint": mint,
                  "denominatedInSol": "true", "amount": sol_amount,
                  "slippage": 25, "priorityFee": 0.005, "pool": "pump"},
            timeout=15
        )
        if res.status_code != 200:
            log("err", f"PumpPortal {res.status_code}: {res.text[:80]}", symbol)
            return None
        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx      = VersionedTransaction(VersionedTransaction.from_bytes(res.content).message, [keypair])
        client  = Client(SOL_RPC)
        result  = client.send_raw_transaction(bytes(tx), opts=TxOpts(skip_preflight=True, preflight_commitment="confirmed"))
        sig     = str(result.value)
        if sig and len(sig) > 10:
            log("ok", f"Bought! {sig[:20]}...", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Buy error: {e}", symbol)
        return None

# ── EXECUTE SELL ─────────────────────────────────────────────────────────────
def execute_sell(tokens, mint, symbol):
    if PAPER_MODE:
        log("ok", f"[PAPER] Sell {symbol}", symbol)
        return "PAPER_TX"
    try:
        res = _session.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={"publicKey": WALLET, "action": "sell", "mint": mint,
                  "denominatedInSol": "false", "amount": tokens,
                  "slippage": 25, "priorityFee": 0.005, "pool": "pump"},
            timeout=15
        )
        if res.status_code != 200:
            log("err", f"PumpPortal sell {res.status_code}: {res.text[:80]}", symbol)
            return None
        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx      = VersionedTransaction(VersionedTransaction.from_bytes(res.content).message, [keypair])
        client  = Client(SOL_RPC)
        result  = client.send_raw_transaction(bytes(tx), opts=TxOpts(skip_preflight=True, preflight_commitment="confirmed"))
        sig     = str(result.value)
        if sig and len(sig) > 10:
            log("ok", f"Sold! {sig[:20]}...", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Sell error: {e}", symbol)
        return None

# ── ENTER TRADE ──────────────────────────────────────────────────────────────
def enter_trade(coin):
    global capital
    mint   = coin["mint"]
    symbol = coin["symbol"]
    price  = coin["price"]

    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            return False
    with capital_lock:
        if capital < TRADE_AMOUNT:
            log("warn", f"Not enough capital (${capital:.2f})")
            return False

    tx = execute_buy(mint, symbol)
    if not tx:
        return False

    tp = price * (1 + TP_PCT / 100)
    sl = price * (1 - SL_PCT / 100)

    with trades_lock:
        open_trades[mint] = {
            "symbol":    symbol,
            "mint":      mint,
            "entry":     price,
            "tp":        tp,
            "sl":        sl,
            "amount":    TRADE_AMOUNT,
            "tokens":    TRADE_AMOUNT / price if price > 0 else 0,
            "opened_at": time.time(),
            "time_str":  time.strftime("%H:%M:%S"),
            "bond_entry": coin["bond_pct"],
        }
    coin_trade_count[mint] += 1
    with capital_lock:
        capital -= TRADE_AMOUNT

    log("ok", f"IN | TP:+{TP_PCT}% SL:-{SL_PCT}% | Max:{MAX_HOLD_SECS}s | Bond:{coin['bond_pct']:.1f}%", symbol)
    return True

# ── EXIT TRADE ───────────────────────────────────────────────────────────────
def exit_trade(mint, current_price, reason):
    global capital
    with trades_lock:
        if mint not in open_trades:
            return
        trade = open_trades.pop(mint)

    amount = trade["amount"]
    entry  = trade["entry"]
    hold_s = time.time() - trade["opened_at"]

    if reason == "TP":
        pnl = amount * TP_PCT / 100
    elif reason == "SL":
        pnl = -(amount * SL_PCT / 100)
    else:
        pnl = ((current_price - entry) / entry) * amount if entry > 0 else 0

    pnl = max(-amount, min(pnl, amount * 3))
    with capital_lock:
        capital += amount + pnl

    emoji = "✅" if pnl >= 0 else "❌"
    log("ok" if pnl >= 0 else "err",
        f"{emoji} {reason} | {'+' if pnl>=0 else ''}${pnl:.2f} | {hold_s:.0f}s | Cap:${capital:.2f}",
        trade["symbol"])

    execute_sell(trade["tokens"], mint, trade["symbol"])
    completed_trades.append({
        "symbol": trade["symbol"],
        "entry":  entry,
        "exit":   current_price,
        "result": reason,
        "pnl":    round(pnl, 4),
        "hold_s": round(hold_s, 1),
        "time":   time.strftime("%H:%M:%S"),
    })

# ── MONITOR LOOP ──────────────────────────────────────────────────────────────
def monitor_loop():
    while True:
        time.sleep(2)
        with trades_lock:
            mints = list(open_trades.keys())
        for mint in mints:
            with trades_lock:
                if mint not in open_trades:
                    continue
                trade = open_trades[mint]
            hold_s = time.time() - trade["opened_at"]
            price, bond = get_price(mint)
            if price is None:
                if hold_s >= MAX_HOLD_SECS:
                    exit_trade(mint, trade["entry"], "TIME")
                continue
            move = ((price - trade["entry"]) / trade["entry"]) * 100 if trade["entry"] > 0 else 0
            log("info", f"Bond:{bond:.1f}% | {move:+.1f}% | {hold_s:.0f}s/{MAX_HOLD_SECS}s", trade["symbol"])
            if price >= trade["tp"]:
                exit_trade(mint, price, "TP")
            elif price <= trade["sl"]:
                exit_trade(mint, price, "SL")
            elif hold_s >= MAX_HOLD_SECS:
                exit_trade(mint, price, "TIME")

# ── SCANNER LOOP ─────────────────────────────────────────────────────────────
def scanner_loop():
    global scan_active
    log("ok", "=" * 55)
    log("ok", "PumpFun Sniper — FAST MODE")
    log("ok", f"Entry: Bond {MIN_BOND_PCT}-{MAX_BOND_PCT}% | Age <{MAX_TOKEN_AGE_MINS}m | Social required")
    log("ok", f"Trade: ${TRADE_AMOUNT} | TP:{TP_PCT}% | SL:{SL_PCT}% | Max:{MAX_HOLD_SECS}s")
    log("ok", f"Scan every {SCAN_INTERVAL}s | Max {MAX_OPEN} open")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else 'LIVE -> ' + WALLET[:8] + '...'}")
    log("ok", "=" * 55)

    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)
            if num_open >= MAX_OPEN:
                time.sleep(2)
                continue

            coins = get_coins()
            if not coins:
                time.sleep(SCAN_INTERVAL)
                continue

            passed = 0
            for coin in coins:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN:
                        break
                    if coin["mint"] in open_trades:
                        continue

                ok, reason = should_enter(coin)
                if not ok:
                    continue

                log("ok", f"SIGNAL → {reason}", coin["symbol"])
                if enter_trade(coin):
                    passed += 1

            if passed == 0 and num_open == 0:
                log("info", f"Scanning... ({len(coins)} coins, none qualify yet)")

        except Exception as e:
            log("err", f"Scanner: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ─────────────────────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    with trades_lock:
        n = len(open_trades)
    return f"PumpFun Sniper | Capital:${capital:.2f} | Open:{n}/{MAX_OPEN} | {'PAPER' if PAPER_MODE else 'LIVE'}", 200

@app.route("/status", methods=["GET"])
def status():
    wins  = len([t for t in completed_trades if t["result"] == "TP"])
    loss  = len([t for t in completed_trades if t["result"] == "SL"])
    times = len([t for t in completed_trades if t["result"] == "TIME"])
    pnl   = sum(t["pnl"] for t in completed_trades)
    wr    = round(wins / max(wins + loss, 1) * 100, 1)
    with trades_lock:
        n = len(open_trades)
    return jsonify({
        "capital":     round(capital, 2),
        "paper_mode":  PAPER_MODE,
        "open_trades": n,
        "completed":   len(completed_trades),
        "wins":        wins,
        "losses":      loss,
        "time_exits":  times,
        "win_rate":    wr,
        "total_pnl":   round(pnl, 4),
        "strategy":    f"Bond:{MIN_BOND_PCT}-{MAX_BOND_PCT}% | TP:{TP_PCT}% | SL:{SL_PCT}% | Max:{MAX_HOLD_SECS}s",
    })

@app.route("/trades", methods=["GET"])
def trades():
    with trades_lock:
        open_list = [{k: v for k, v in t.items() if k != "opened_at"} for t in open_trades.values()]
    return jsonify({"open": open_list, "completed": completed_trades[-50:]})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-100:]})

@app.route("/blacklist/<mint>", methods=["GET"])
def blacklist_coin(mint):
    blacklisted.add(mint)
    return jsonify({"blacklisted": mint})

if __name__ == "__main__":
    if not PAPER_MODE and not _SOLANA_AVAILABLE:
        log("warn", "solders/solana not installed — falling back to PAPER mode")
        PAPER_MODE = True
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)
