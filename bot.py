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

# Strategy 1 — Bond Runner (enter at ~60%, exit at ~65-70%)
BOND_ENTRY_MIN  = float(os.environ.get("BOND_ENTRY_MIN", "58"))
BOND_ENTRY_MAX  = float(os.environ.get("BOND_ENTRY_MAX", "63"))
BOND_TP         = float(os.environ.get("BOND_TP", "67"))   # exit when bond hits this %
BOND_SL_PCT     = float(os.environ.get("BOND_SL_PCT", "10"))  # price SL %
BOND_MAX_SECS   = int(os.environ.get("BOND_MAX_SECS", "600"))  # 10 min max

# Strategy 2 — Dormant Spike (old token, sudden 2-3x)
SPIKE_MIN_AGE_H = float(os.environ.get("SPIKE_MIN_AGE_H", "12"))  # must be this old
SPIKE_MIN_1H    = float(os.environ.get("SPIKE_MIN_1H", "100"))    # 1h change % (100 = 2x)
SPIKE_TP_PCT    = float(os.environ.get("SPIKE_TP_PCT", "40"))     # price TP %
SPIKE_SL_PCT    = float(os.environ.get("SPIKE_SL_PCT", "15"))     # price SL %
SPIKE_MAX_SECS  = int(os.environ.get("SPIKE_MAX_SECS", "300"))    # 5 min max

# General
MAX_OPEN        = int(os.environ.get("MAX_OPEN", "3"))
MAX_PER_COIN    = int(os.environ.get("MAX_PER_COIN", "1"))
SCAN_INTERVAL   = int(os.environ.get("SCAN_INTERVAL", "10"))
MIN_REPLIES     = int(os.environ.get("MIN_REPLIES", "10"))

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

# ── PUMP.FUN PRICE + BOND ─────────────────────────────────────────────────
def get_pump_data(mint):
    try:
        res = _session.get(
            f"https://frontend-api-v3.pump.fun/coins/{mint}",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://pump.fun/"},
            timeout=5
        )
        if res.status_code == 200:
            d    = res.json()
            vsol = float(d.get("virtual_sol_reserves", 0) or 0)
            vtok = float(d.get("virtual_token_reserves", 0) or 0)
            bond = min((vsol / 85_000_000_000) * 100, 99.9) if vsol > 0 else 0
            if vsol > 0 and vtok > 0:
                return vsol / vtok, bond
    except:
        pass
    return None, None

# ── SOL PRICE ─────────────────────────────────────────────────────────────────
def get_sol_price():
    for url, key in [
        ("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd", None),
        ("https://api.dexscreener.com/latest/dex/pairs/solana/8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj", "pairs"),
    ]:
        try:
            res = _session.get(url, timeout=8)
            if key is None:
                return float(res.json()["solana"]["usd"])
            pairs = res.json().get("pairs", [])
            if pairs:
                return float(pairs[0].get("priceUsd", 0))
        except:
            continue
    return 150

# ── DEXSCREENER (for spike detection) ───────────────────────────────────────
def get_dex_data(mint):
    try:
        res   = _session.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=8)
        pairs = res.json().get("pairs", [])
        sol   = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol:
            return None
        pair = max(sol, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        return {
            "price":     float(pair.get("priceUsd", 0) or 0),
            "liq":       float(pair.get("liquidity", {}).get("usd", 0) or 0),
            "change1h":  float(pair.get("priceChange", {}).get("h1", 0) or 0),
            "change24h": float(pair.get("priceChange", {}).get("h24", 0) or 0),
        }
    except:
        return None

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
                    "Accept":     "application/json",
                    "Referer":    "https://pump.fun/",
                    "Origin":     "https://pump.fun",
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
            for c in items[:50]:
                mint = c.get("mint", "")
                if not mint or c.get("complete", False):
                    continue
                vsol = float(c.get("virtual_sol_reserves", 0) or 0)
                vtok = float(c.get("virtual_token_reserves", 0) or 0)
                if vsol <= 0 or vtok <= 0:
                    continue
                bond      = min((vsol / 85_000_000_000) * 100, 99.9)
                created   = c.get("created_timestamp", 0) or 0
                age_hours = (now - created / 1000) / 3600 if created else 0
                coins.append({
                    "mint":      mint,
                    "symbol":    c.get("symbol", mint[:8]),
                    "bond_pct":  round(bond, 1),
                    "price":     vsol / vtok,
                    "twitter":   bool(c.get("twitter")),
                    "telegram":  bool(c.get("telegram")),
                    "replies":   int(c.get("reply_count", 0) or 0),
                    "age_hours": round(age_hours, 2),
                })
            if coins:
                return coins
        except Exception as e:
            log("warn", f"Fetch: {e}")
    return []

# ── EXECUTE BUY ──────────────────────────────────────────────────────────────
def execute_buy(mint, symbol):
    if PAPER_MODE:
        log("ok", f"[PAPER] Buy ${TRADE_AMOUNT}", symbol)
        return "PAPER_TX"
    try:
        sol_amount = round(TRADE_AMOUNT / get_sol_price(), 6)
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
        log("err", f"Buy: {e}", symbol)
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
        log("err", f"Sell: {e}", symbol)
        return None

# ── ENTER TRADE ──────────────────────────────────────────────────────────────
def enter_trade(coin, strategy):
    global capital
    mint   = coin["mint"]
    symbol = coin["symbol"]
    price  = coin["price"]

    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            return False
    with capital_lock:
        if capital < TRADE_AMOUNT:
            log("warn", f"Low capital ${capital:.2f}")
            return False

    tx = execute_buy(mint, symbol)
    if not tx:
        return False

    if strategy == "bond":
        sl_price = price * (1 - BOND_SL_PCT / 100)
        max_secs = BOND_MAX_SECS
        log("ok", f"[BOND] IN at bond:{coin['bond_pct']:.1f}% | TP:bond>{BOND_TP}% | SL:-{BOND_SL_PCT}%", symbol)
    else:
        sl_price = price * (1 - SPIKE_SL_PCT / 100)
        max_secs = SPIKE_MAX_SECS
        log("ok", f"[SPIKE] IN | 1h:+{coin.get('change1h',0):.0f}% | TP:+{SPIKE_TP_PCT}% | SL:-{SPIKE_SL_PCT}%", symbol)

    with trades_lock:
        open_trades[mint] = {
            "symbol":     symbol,
            "mint":       mint,
            "entry":      price,
            "sl":         sl_price,
            "amount":     TRADE_AMOUNT,
            "tokens":     TRADE_AMOUNT / price if price > 0 else 0,
            "opened_at":  time.time(),
            "time_str":   time.strftime("%H:%M:%S"),
            "bond_entry": coin.get("bond_pct", 0),
            "max_secs":   max_secs,
            "strategy":   strategy,
        }
    coin_trade_count[mint] += 1
    with capital_lock:
        capital -= TRADE_AMOUNT
    return True

# ── EXIT TRADE ───────────────────────────────────────────────────────────────
def exit_trade(mint, price_now, reason, bond_now=None):
    global capital
    with trades_lock:
        if mint not in open_trades:
            return
        trade = open_trades.pop(mint)

    amount   = trade["amount"]
    entry    = trade["entry"]
    strategy = trade["strategy"]
    hold_s   = time.time() - trade["opened_at"]

    if entry > 0:
        pnl = ((price_now - entry) / entry) * amount
    else:
        pnl = 0
    pnl = max(-amount, min(pnl, amount * 5))

    with capital_lock:
        capital += amount + pnl

    tag   = f"[{strategy.upper()}]"
    bond_str = f" | Bond:{bond_now:.1f}%" if bond_now else ""
    emoji = "✅" if pnl >= 0 else "❌"
    log("ok" if pnl >= 0 else "err",
        f"{emoji} {tag} {reason} | {'+' if pnl>=0 else ''}${pnl:.2f} | {hold_s:.0f}s{bond_str} | Cap:${capital:.2f}",
        trade["symbol"])

    execute_sell(trade["tokens"], mint, trade["symbol"])
    completed_trades.append({
        "symbol":   trade["symbol"],
        "strategy": strategy,
        "entry":    entry,
        "exit":     price_now,
        "result":   reason,
        "pnl":      round(pnl, 4),
        "hold_s":   round(hold_s, 1),
        "time":     time.strftime("%H:%M:%S"),
    })

# ── MONITOR LOOP ─────────────────────────────────────────────────────────────
def monitor_loop():
    while True:
        time.sleep(3)
        with trades_lock:
            mints = list(open_trades.keys())
        for mint in mints:
            with trades_lock:
                if mint not in open_trades:
                    continue
                trade = open_trades[mint]
            hold_s   = time.time() - trade["opened_at"]
            strategy = trade["strategy"]
            max_secs = trade["max_secs"]
            price, bond = get_pump_data(mint)
            if price is None:
                if hold_s >= max_secs:
                    exit_trade(mint, trade["entry"], "TIME")
                continue
            move = ((price - trade["entry"]) / trade["entry"]) * 100 if trade["entry"] > 0 else 0
            if strategy == "bond":
                log("info", f"[BOND] bond:{bond:.1f}% target:{BOND_TP}% | price:{move:+.1f}% | {hold_s:.0f}s", trade["symbol"])
                if bond >= BOND_TP:
                    exit_trade(mint, price, "TP", bond)
                elif price <= trade["sl"]:
                    exit_trade(mint, price, "SL", bond)
                elif hold_s >= max_secs:
                    exit_trade(mint, price, "TIME", bond)
            else:
                tp_price = trade["entry"] * (1 + SPIKE_TP_PCT / 100)
                log("info", f"[SPIKE] {move:+.1f}% (target +{SPIKE_TP_PCT}%) | {hold_s:.0f}s", trade["symbol"])
                if price >= tp_price:
                    exit_trade(mint, price, "TP")
                elif price <= trade["sl"]:
                    exit_trade(mint, price, "SL")
                elif hold_s >= max_secs:
                    exit_trade(mint, price, "TIME")

# ── SCANNER LOOP ─────────────────────────────────────────────────────────────
def scanner_loop():
    global scan_active
    log("ok", "=" * 55)
    log("ok", "Pump.fun Sniper — DUAL STRATEGY")
    log("ok", f"[BOND RUNNER] Enter:{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% | Exit:>{BOND_TP}% | SL:-{BOND_SL_PCT}%")
    log("ok", f"[DORMANT SPIKE] Age:>{SPIKE_MIN_AGE_H}h | 1h spike:>{SPIKE_MIN_1H}% | TP:+{SPIKE_TP_PCT}% | SL:-{SPIKE_SL_PCT}%")
    log("ok", f"Required: Twitter + Telegram + {MIN_REPLIES}+ replies")
    log("ok", f"${TRADE_AMOUNT}/trade | {MAX_OPEN} max open | {'PAPER' if PAPER_MODE else 'LIVE'}")
    log("ok", "=" * 55)

    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)
            if num_open >= MAX_OPEN:
                time.sleep(3)
                continue

            coins = get_coins()
            if not coins:
                time.sleep(SCAN_INTERVAL)
                continue

            bond_candidates  = []
            spike_candidates = []

            for coin in coins:
                mint = coin["mint"]
                if mint in blacklisted or coin_trade_count[mint] >= MAX_PER_COIN:
                    continue
                with trades_lock:
                    if mint in open_trades:
                        continue

                # Require BOTH Twitter AND Telegram
                if not coin.get("twitter") or not coin.get("telegram"):
                    continue
                # Require enough replies (proxy for real engagement)
                if coin.get("replies", 0) < MIN_REPLIES:
                    continue

                bond      = coin["bond_pct"]
                age_hours = coin["age_hours"]

                # Strategy 1: Bond Runner — entering at 60% heading to graduation
                if BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX:
                    bond_candidates.append(coin)
                    log("info", f"[BOND CANDIDATE] {bond:.1f}% | {coin['replies']} replies | Age:{age_hours:.1f}h", coin["symbol"])

                # Strategy 2: Dormant Spike — old token suddenly pumping
                elif age_hours >= SPIKE_MIN_AGE_H:
                    dex = get_dex_data(mint)
                    if dex and dex["change1h"] >= SPIKE_MIN_1H and dex["liq"] >= 500:
                        coin["price"]     = dex["price"] if dex["price"] > 0 else coin["price"]
                        coin["change1h"]  = dex["change1h"]
                        spike_candidates.append(coin)
                        log("ok", f"[SPIKE CANDIDATE] +{dex['change1h']:.0f}% 1h | Age:{age_hours:.1f}h | Liq:${dex['liq']:,.0f}", coin["symbol"])

            for coin in bond_candidates[:2]:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN or coin["mint"] in open_trades:
                        break
                enter_trade(coin, "bond")

            for coin in spike_candidates[:2]:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN or coin["mint"] in open_trades:
                        break
                enter_trade(coin, "spike")

            if not bond_candidates and not spike_candidates:
                log("info", f"Scanning {len(coins)} coins | {num_open}/{MAX_OPEN} open | no signals yet")

        except Exception as e:
            log("err", f"Scanner: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ─────────────────────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    with trades_lock:
        n = len(open_trades)
    return f"Pump Sniper | ${capital:.2f} | Open:{n}/{MAX_OPEN} | {'PAPER' if PAPER_MODE else 'LIVE'}", 200

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
        "strategies": {
            "bond_runner":    f"Entry:{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% | TP:>{BOND_TP}% | SL:-{BOND_SL_PCT}%",
            "dormant_spike":  f"Age:>{SPIKE_MIN_AGE_H}h | 1h:>{SPIKE_MIN_1H}% | TP:+{SPIKE_TP_PCT}% | SL:-{SPIKE_SL_PCT}%",
        }
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
        log("warn", "solders/solana not installed — PAPER mode")
        PAPER_MODE = True
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)
