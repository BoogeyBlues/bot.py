import os, time, threading, requests, json, re, csv, io
from flask import Flask, jsonify, Response
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
_session.trust_env = False  # bypass Railway proxy env vars

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────────
WALLET             = os.environ.get("WALLET", "")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY", "")
_PAPER_ENV         = os.environ.get("PAPER_MODE", "true").lower()
PAPER_MODE         = _PAPER_ENV == "true" or not WALLET or not WALLET_PRIVATE_KEY

# Progressive sizing: 18% of capital, clamped
TRADE_PCT    = float(os.environ.get("TRADE_PCT",  "18"))
MIN_TRADE    = float(os.environ.get("MIN_TRADE",  "5"))
MAX_TRADE    = float(os.environ.get("MAX_TRADE",  "500"))

# Bond Runner strategy
BOND_ENTRY_MIN  = float(os.environ.get("BOND_ENTRY_MIN", "58"))
BOND_ENTRY_MAX  = float(os.environ.get("BOND_ENTRY_MAX", "63"))
BOND_TP         = float(os.environ.get("BOND_TP",        "67"))
BOND_SL_PCT     = float(os.environ.get("BOND_SL_PCT",    "10"))
BOND_MAX_SECS   = int(os.environ.get("BOND_MAX_SECS",    "600"))

# Dormant Spike strategy
SPIKE_MIN_AGE_H = float(os.environ.get("SPIKE_MIN_AGE_H", "12"))
SPIKE_MIN_1H    = float(os.environ.get("SPIKE_MIN_1H",    "100"))
SPIKE_TP_PCT    = float(os.environ.get("SPIKE_TP_PCT",    "40"))
SPIKE_SL_PCT    = float(os.environ.get("SPIKE_SL_PCT",    "15"))
SPIKE_MAX_SECS  = int(os.environ.get("SPIKE_MAX_SECS",    "300"))

# Exit protection
SLIP_TRIGGER   = float(os.environ.get("SLIP_TRIGGER",  "90"))
SLIP_DROP_TO   = float(os.environ.get("SLIP_DROP_TO",  "85"))
SLIP_WAIT_SECS = int(os.environ.get("SLIP_WAIT_SECS",  "6"))
SHARP_DROP_PCT = float(os.environ.get("SHARP_DROP_PCT", "4"))

# Bundle mode: "avoid" or "ride"
BUNDLE_MODE    = os.environ.get("BUNDLE_MODE", "avoid").lower()
BUNDLE_RIDE_TP = float(os.environ.get("BUNDLE_RIDE_TP", "88"))

# USDC profit lock: once capital hits this threshold, lock each win's profit into USDC
USDC_LOCK_THRESHOLD = float(os.environ.get("USDC_LOCK_THRESHOLD", "80"))
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_QUOTE = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP  = "https://quote-api.jup.ag/v6/swap"

# Notifications
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
NTFY_TOPIC       = os.environ.get("NTFY_TOPIC", "")  # e.g. "my-sniper-bot-abc123"

# Social / quality gates
MIN_REPLIES  = int(os.environ.get("MIN_REPLIES",  "10"))
MIN_LIQ      = float(os.environ.get("MIN_LIQ",    "500"))

# General
MAX_OPEN      = int(os.environ.get("MAX_OPEN",      "3"))
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", "10"))

SOL_RPC    = "https://api.mainnet-beta.solana.com"
PUMPPORTAL = "https://pumpportal.fun/api/trade-local"
LEARN_FILE = "/tmp/bot_learn.json"

MILESTONES = [100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000]

# ── STATE ────────────────────────────────────────────────────────
capital           = float(os.environ.get("STARTING_CAPITAL", "39.67"))
SOL_ALLOCATED     = float(os.environ.get("SOL_ALLOCATED",     "19.67"))  # SOL wallet funded for trading
capital_lock      = threading.Lock()
open_trades       = {}
trades_lock       = threading.Lock()
blacklisted_mints = set()
trade_log         = []
completed_trades  = []
log_lock          = threading.Lock()
scan_active       = True
_milestones_hit   = set()
_milestone_lock   = threading.Lock()
usdc_locked       = 0.0   # total USD value locked into USDC
usdc_lock         = threading.Lock()

# ── LOGGING ─────────────────────────────────────────────────────
def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 300:
            trade_log.pop(0)

# ── NOTIFICATIONS ────────────────────────────────────────────────
def notify(title, body):
    """Send push notification via Telegram and/or ntfy — runs in background thread."""
    def _send():
        # Telegram
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            try:
                _session.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    json={"chat_id": TELEGRAM_CHAT_ID, "text": f"*{title}*\n{body}",
                          "parse_mode": "Markdown"},
                    timeout=8
                )
            except Exception as e:
                log("warn", f"Telegram notify failed: {e}")
        # ntfy.sh (free, no account needed)
        if NTFY_TOPIC:
            try:
                _session.post(
                    f"https://ntfy.sh/{NTFY_TOPIC}",
                    data=body.encode("utf-8"),
                    headers={"Title": title, "Priority": "high", "Tags": "chart_increasing"},
                    timeout=8
                )
            except Exception as e:
                log("warn", f"ntfy notify failed: {e}")
    threading.Thread(target=_send, daemon=True).start()

# ── PROGRESSIVE SIZING ───────────────────────────────────────────
def trade_size():
    with capital_lock:
        cap = capital
    raw = cap * TRADE_PCT / 100
    return round(max(MIN_TRADE, min(MAX_TRADE, raw)), 2)

# ── MILESTONES ───────────────────────────────────────────────────
def check_milestones():
    with capital_lock:
        cap = capital
    with _milestone_lock:
        for m in MILESTONES:
            if cap >= m and m not in _milestones_hit:
                _milestones_hit.add(m)
                ts = trade_size()
                log("ok", f"MILESTONE ${m:,} REACHED! New trade size: ${ts:.2f}", "GOAL")
                notify(f"🏆 MILESTONE ${m:,} REACHED!",
                       f"Capital: ${cap:.2f}\nNew trade size: ${ts:.2f}\nKeep going!")

# ── ADAPTIVE LEARNING ────────────────────────────────────────────
def record_trade(trade_data):
    try:
        history = []
        if os.path.exists(LEARN_FILE):
            with open(LEARN_FILE, "r") as f:
                history = json.load(f)
        history.append(trade_data)
        with open(LEARN_FILE, "w") as f:
            json.dump(history[-200:], f)
        if len(history) % 20 == 0:
            auto_tune(history)
    except Exception as e:
        log("warn", f"Learning record: {e}")

def auto_tune(history):
    global BOND_ENTRY_MIN, BOND_ENTRY_MAX, MIN_REPLIES, SPIKE_TP_PCT
    try:
        recent = history[-40:]
        wins   = [t for t in recent if t.get("pnl", 0) > 0]
        losses = [t for t in recent if t.get("pnl", 0) <= 0]

        bond_wins  = [t for t in wins   if t.get("strategy") == "bond"]
        spike_wins = [t for t in wins   if t.get("strategy") == "spike"]
        bond_all   = [t for t in recent if t.get("strategy") == "bond"]
        spike_all  = [t for t in recent if t.get("strategy") == "spike"]

        bond_wr  = len(bond_wins)  / max(len(bond_all),  1)
        spike_wr = len(spike_wins) / max(len(spike_all), 1)

        if bond_wins:
            avg_win_entry = sum(t.get("bond_entry", BOND_ENTRY_MIN) for t in bond_wins) / len(bond_wins)
            BOND_ENTRY_MIN = round(min(max(avg_win_entry - 3, 50), 70), 1)
            BOND_ENTRY_MAX = round(min(BOND_ENTRY_MIN + 8, 78), 1)

        if spike_wr > bond_wr + 0.2 and SPIKE_TP_PCT < 80:
            SPIKE_TP_PCT = round(SPIKE_TP_PCT * 1.1, 1)

        stats = {
            "tuned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "trades_analyzed": len(recent),
            "win_rate": round(len(wins) / max(len(recent), 1) * 100, 1),
            "bond_win_rate": round(bond_wr * 100, 1),
            "spike_win_rate": round(spike_wr * 100, 1),
            "new_bond_entry": f"{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%",
            "new_spike_tp": SPIKE_TP_PCT,
        }
        log("ok", f"Auto-tuned: bond={BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% spike_tp={SPIKE_TP_PCT}%")
        try:
            with open(LEARN_FILE.replace(".json", "_stats.json"), "w") as f:
                json.dump(stats, f, indent=2)
        except Exception:
            pass
    except Exception as e:
        log("warn", f"Auto-tune: {e}")

# ── PUMP.FUN COINS ───────────────────────────────────────────────
def get_pumpfun_coins():
    endpoints = [
        "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false",
        "https://frontend-api-v3.pump.fun/coins/currently-live?offset=0&limit=50&includeNsfw=false&order=DESC",
        "https://frontend-api-v2.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
        "Accept": "application/json",
        "Referer": "https://pump.fun/",
        "Origin": "https://pump.fun",
    }
    for url in endpoints:
        try:
            res = _session.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                continue
            data  = res.json()
            items = data if isinstance(data, list) else data.get("coins", [])
            if not items:
                continue
            coins = []
            for coin in items[:50]:
                mint  = coin.get("mint", "")
                vsol  = float(coin.get("virtual_sol_reserves", 0) or 0)
                bond  = min((vsol / 85_000_000_000) * 100, 99.9) if vsol > 0 else 0
                if mint and not coin.get("complete", False):
                    coins.append({
                        "mint":       mint,
                        "symbol":     coin.get("symbol", mint[:8]),
                        "bond_pct":   round(bond, 1),
                        "twitter":    bool(coin.get("twitter")),
                        "telegram":   bool(coin.get("telegram")),
                        "dev":        coin.get("creator", ""),
                        "replies":    int(coin.get("reply_count", 0) or 0),
                        "created_at":   int(coin.get("created_timestamp", 0) or 0),
                        "last_trade":   int(coin.get("last_trade_timestamp", 0) or 0),
                        "complete":     False,
                    })
            log("info", f"pump.fun API: {len(coins)} live coins")
            return coins
        except Exception as e:
            log("warn", f"Endpoint failed: {e}")
    return []

def get_bonding_details(mint):
    try:
        res = _session.get(
            f"https://frontend-api-v3.pump.fun/coins/{mint}",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://pump.fun/"},
            timeout=8
        )
        if res.status_code == 200:
            data  = res.json()
            vsol  = float(data.get("virtual_sol_reserves", 0) or 0)
            bond  = min((vsol / 85_000_000_000) * 100, 99.9)
            return {
                "bond_pct":   round(bond, 1),
                "complete":   data.get("complete", False),
                "replies":    int(data.get("reply_count", 0) or 0),
                "twitter":    bool(data.get("twitter")),
                "telegram":   bool(data.get("telegram")),
                "created_at": int(data.get("created_timestamp", 0) or 0),
            }
    except Exception:
        pass
    return None

# ── MARKET DATA ──────────────────────────────────────────────────
def get_market_data(mint):
    try:
        res   = _session.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}", timeout=8)
        pairs = [p for p in res.json().get("pairs", []) if p.get("chainId") == "solana"]
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        return {
            "price":    float(pair.get("priceUsd", 0) or 0),
            "liq":      float(pair.get("liquidity", {}).get("usd", 0) or 0),
            "change1h": float(pair.get("priceChange", {}).get("h1", 0) or 0),
            "age_h":    (time.time() - float(pair.get("pairCreatedAt", time.time() * 1000)) / 1000) / 3600,
        }
    except Exception:
        return None

def get_sol_price():
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
    except Exception:
        pass
    try:
        res   = _session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=8
        )
        price = float(res.json()["solana"]["usd"])
        if price > 0:
            return price
    except Exception:
        pass
    return None

# ── RUGCHECK ────────────────────────────────────────────────────
def run_rugcheck(mint):
    try:
        res   = _session.get(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary", timeout=10)
        data  = res.json()
        risks = [r.get("name", "").lower() for r in data.get("risks", [])]
        return {
            "has_mint_auth":   any("mint" in r for r in risks),
            "has_freeze_auth": any("freeze" in r for r in risks),
            "is_bundled":      any("insider" in r or "bundle" in r for r in risks),
        }
    except Exception:
        return None

# ── USDC PROFIT LOCK ─────────────────────────────────────────────
def lock_profit_to_usdc(profit_usd):
    """Swap profit_usd worth of SOL into USDC via Jupiter after winning trade."""
    global usdc_locked
    if profit_usd <= 0:
        return
    if PAPER_MODE:
        with usdc_lock:
            usdc_locked += profit_usd
        log("ok", f"[PAPER] Locked ${profit_usd:.4f} profit -> USDC | Total locked: ${usdc_locked:.2f}", "USDC")
        return
    try:
        sol_price = get_sol_price()
        if not sol_price:
            log("warn", "Cannot get SOL price — skipping USDC lock", "USDC")
            return
        # Convert profit USD to lamports (1 SOL = 1e9 lamports)
        sol_amount  = profit_usd / sol_price
        lamports    = int(sol_amount * 1_000_000_000)
        if lamports < 5_000:  # ignore dust (<0.000005 SOL)
            return

        # Get Jupiter quote: SOL -> USDC
        res = _session.get(
            JUPITER_QUOTE,
            params={
                "inputMint":   WSOL_MINT,
                "outputMint":  USDC_MINT,
                "amount":      lamports,
                "slippageBps": 50,  # 0.5% slippage
            },
            timeout=10
        )
        if res.status_code != 200:
            log("warn", f"Jupiter quote failed {res.status_code}", "USDC")
            return
        quote = res.json()

        # Get swap transaction
        swap_res = _session.post(
            JUPITER_SWAP,
            json={
                "quoteResponse":    quote,
                "userPublicKey":    WALLET,
                "wrapAndUnwrapSol": True,
                "prioritizationFeeLamports": 1000,
            },
            headers={"Content-Type": "application/json"},
            timeout=15
        )
        if swap_res.status_code != 200:
            log("warn", f"Jupiter swap tx failed {swap_res.status_code}", "USDC")
            return

        import base64
        raw_tx   = base64.b64decode(swap_res.json()["swapTransaction"])
        keypair  = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx       = VersionedTransaction(VersionedTransaction.from_bytes(raw_tx).message, [keypair])
        client   = Client(SOL_RPC)
        result   = client.send_raw_transaction(bytes(tx), opts=TxOpts(skip_preflight=True, preflight_commitment="confirmed"))
        sig      = str(result.value)

        usdc_out = float(quote.get("outAmount", 0)) / 1_000_000  # USDC has 6 decimals
        with usdc_lock:
            usdc_locked += usdc_out
        log("ok", f"Locked ${usdc_out:.4f} USDC | Total: ${usdc_locked:.4f} | sig={sig[:20]}...", "USDC")
        log("ok", f"https://solscan.io/tx/{sig}", "USDC")
    except Exception as e:
        log("warn", f"USDC lock error: {e}", "USDC")

# ── TRADE EXECUTION ──────────────────────────────────────────────
def execute_buy(mint, symbol, amount):
    if PAPER_MODE:
        log("ok", f"[PAPER] Buy ${amount:.2f} -> {symbol}", symbol)
        return "PAPER_TX"
    try:
        sol_price = get_sol_price()
        if not sol_price:
            log("err", "Cannot get SOL price — buy aborted", symbol)
            return None
        sol_amount = round(amount / sol_price, 6)

        res = _session.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={"publicKey": WALLET, "action": "buy", "mint": mint,
                  "denominatedInSol": "true", "amount": sol_amount,
                  "slippage": 20, "priorityFee": 0.001, "pool": "pump"},
            timeout=15
        )
        if res.status_code != 200:
            log("err", f"PumpPortal buy {res.status_code}: {res.text[:80]}", symbol)
            return None

        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx      = VersionedTransaction(VersionedTransaction.from_bytes(res.content).message, [keypair])
        client  = Client(SOL_RPC)
        result  = client.send_raw_transaction(bytes(tx), opts=TxOpts(skip_preflight=True, preflight_commitment="confirmed"))
        sig     = str(result.value)
        if sig and len(sig) > 10:
            log("ok", f"Bought! sig={sig[:20]}...", symbol)
            log("ok", f"https://solscan.io/tx/{sig}", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Buy error: {e}", symbol)
        return None

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
                  "slippage": 20, "priorityFee": 0.001, "pool": "pump"},
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
            log("ok", f"Sold! sig={sig[:20]}...", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Sell error: {e}", symbol)
        return None

# ── ENTER / EXIT ─────────────────────────────────────────────────
def enter_trade(mint, symbol, entry_price, amount, strategy, bond_entry=0, replies=0):
    global capital
    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            return False
    with capital_lock:
        if capital < amount:
            return False

    tx = execute_buy(mint, symbol, amount)
    if not tx:
        return False

    with capital_lock:
        capital -= amount

    with trades_lock:
        open_trades[mint] = {
            "symbol":          symbol,
            "mint":            mint,
            "strategy":        strategy,
            "entry":           entry_price,
            "amount":          amount,
            "tokens":          amount / max(entry_price, 1e-12),
            "opened_at":       time.time(),
            "bond_entry":      bond_entry,
            "bond_high":       bond_entry,
            "bond_prev":       bond_entry,
            "bond_slip_start": None,
            "replies":         replies,
        }

    log("ok", f"ENTER [{strategy.upper()}] ${amount:.2f} | bond={bond_entry:.1f}%", symbol)
    notify(f"🟢 BUY {symbol}",
           f"Strategy: {strategy.upper()}\nAmount: ${amount:.2f}\nBond: {bond_entry:.1f}%\nReplies: {replies}")
    return True

def exit_trade(mint, price, reason, bond=0):
    global capital
    with trades_lock:
        if mint not in open_trades:
            return
        trade = open_trades.pop(mint)

    amount = trade["amount"]
    pnl    = ((price - trade["entry"]) / trade["entry"]) * amount if trade["entry"] > 0 else 0
    pnl    = max(-amount, min(pnl, amount * 5))
    hold_m = (time.time() - trade["opened_at"]) / 60

    with capital_lock:
        capital += amount + pnl

    sign = "+" if pnl >= 0 else ""
    log("ok" if pnl >= 0 else "err",
        f"{'WIN' if pnl>=0 else 'LOSS'} {reason} | {sign}${pnl:.4f} | {hold_m:.1f}m | cap=${capital:.2f}",
        trade["symbol"])
    emoji = "✅" if pnl >= 0 else "❌"
    notify(f"{emoji} {'WIN' if pnl>=0 else 'LOSS'} {trade['symbol']}",
           f"Reason: {reason}\nPnL: {sign}${pnl:.4f}\nHeld: {hold_m:.1f} min\nCapital: ${capital:.2f}")

    execute_sell(trade["tokens"], mint, trade["symbol"])

    rec = {
        "symbol":     trade["symbol"],
        "strategy":   trade["strategy"],
        "entry":      trade["entry"],
        "exit":       price,
        "result":     reason,
        "pnl":        round(pnl, 4),
        "hold_m":     round(hold_m, 1),
        "bond_entry": trade["bond_entry"],
        "replies":    trade["replies"],
        "hour":       int(time.strftime("%H")),
        "time":       time.strftime("%H:%M:%S"),
    }
    completed_trades.append(rec)
    record_trade(rec)
    check_milestones()

    # Lock profits into USDC once capital >= threshold
    if pnl > 0:
        with capital_lock:
            cap_now = capital
        if cap_now >= USDC_LOCK_THRESHOLD:
            threading.Thread(target=lock_profit_to_usdc, args=(pnl,), daemon=True).start()

    if capital < 2:
        global scan_active
        scan_active = False
        log("err", "Capital below $2 — scanner halted", "HALT")

# ── MONITOR LOOP ────────────────────────────────────────────────
def monitor_loop():
    while True:
        time.sleep(3)
        with trades_lock:
            mints = list(open_trades.keys())
        for mint in mints:
            with trades_lock:
                if mint not in open_trades:
                    continue
                trade = dict(open_trades[mint])
            symbol   = trade["symbol"]
            strategy = trade["strategy"]
            elapsed  = time.time() - trade["opened_at"]

            details = get_bonding_details(mint)
            bond    = details["bond_pct"] if details else 0
            market  = get_market_data(mint)
            price   = market["price"] if market and market["price"] > 0 else trade["entry"]

            with trades_lock:
                if mint not in open_trades:
                    continue
                if bond > open_trades[mint]["bond_high"]:
                    open_trades[mint]["bond_high"] = bond
                bond_high  = open_trades[mint]["bond_high"]
                bond_prev  = open_trades[mint]["bond_prev"]
                slip_start = open_trades[mint]["bond_slip_start"]
                open_trades[mint]["bond_prev"] = bond

            bond_drop = bond - bond_prev

            # Instant exit: sharp bond drop of 4%+ while near graduation
            if bond_high >= SLIP_TRIGGER and bond_drop <= -SHARP_DROP_PCT:
                log("warn", f"SHARP DROP bond={bond:.1f}% drop={bond_drop:.1f}%", symbol)
                exit_trade(mint, price, "SHARP_DROP", bond)
                continue

            # Gradual slip: bond was >=90% and fell to <=85%, wait 6s for retrace
            if bond_high >= SLIP_TRIGGER and bond <= SLIP_DROP_TO:
                with trades_lock:
                    if mint not in open_trades:
                        continue
                    if open_trades[mint]["bond_slip_start"] is None:
                        open_trades[mint]["bond_slip_start"] = time.time()
                        log("warn", f"Bond slip {bond:.1f}% — watching {SLIP_WAIT_SECS}s", symbol)
                    elif time.time() - open_trades[mint]["bond_slip_start"] >= SLIP_WAIT_SECS:
                        exit_trade(mint, price, "BOND_SLIP", bond)
                        continue
            else:
                with trades_lock:
                    if mint in open_trades:
                        open_trades[mint]["bond_slip_start"] = None

            # Bundle ride exit
            if strategy == "bundle" and bond >= BUNDLE_RIDE_TP:
                exit_trade(mint, price, "BUNDLE_TP", bond)
                continue

            # Bond Runner exits
            if strategy == "bond":
                if bond >= BOND_TP:
                    exit_trade(mint, price, "BOND_TP", bond)
                    continue
                if price <= trade["entry"] * (1 - BOND_SL_PCT / 100):
                    exit_trade(mint, price, "BOND_SL", bond)
                    continue
                if elapsed >= BOND_MAX_SECS:
                    exit_trade(mint, price, "BOND_TIME", bond)
                    continue

            # Spike exits
            if strategy == "spike":
                move = ((price - trade["entry"]) / trade["entry"]) * 100
                if move >= SPIKE_TP_PCT:
                    exit_trade(mint, price, "SPIKE_TP", bond)
                    continue
                if move <= -SPIKE_SL_PCT:
                    exit_trade(mint, price, "SPIKE_SL", bond)
                    continue
                if elapsed >= SPIKE_MAX_SECS:
                    exit_trade(mint, price, "SPIKE_TIME", bond)
                    continue

            pct = ((price - trade["entry"]) / trade["entry"]) * 100
            log("info", f"[{strategy}] bond={bond:.1f}% price={pct:+.1f}% {elapsed/60:.1f}m", symbol)

# ── SCANNER LOOP ─────────────────────────────────────────────────
def scanner_loop():
    log("ok", "=" * 55)
    log("ok", "PumpFun Sniper — Bond Runner + Dormant Spike")
    log("ok", f"Bond entry: {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% | TP: {BOND_TP}%")
    log("ok", f"Spike: {SPIKE_MIN_AGE_H}h+ dormant, {SPIKE_MIN_1H}%+ 1h move")
    log("ok", f"Trade size: {TRADE_PCT}% of capital (min ${MIN_TRADE} max ${MAX_TRADE})")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else 'LIVE'}")
    log("ok", "=" * 55)

    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)
            if num_open >= MAX_OPEN:
                time.sleep(SCAN_INTERVAL)
                continue

            log("info", f"--- Scan | Open:{num_open}/{MAX_OPEN} | Size:${trade_size():.2f} ---")
            coins = get_pumpfun_coins()
            if not coins:
                log("warn", "No coins fetched")
                time.sleep(30)
                continue

            # Scan summary counters for diagnostics
            n_social = n_replies = n_bond_range = n_spike_range = 0

            for coin in coins:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN:
                        break
                mint   = coin["mint"]
                symbol = coin["symbol"]

                if mint in blacklisted_mints:
                    continue
                with trades_lock:
                    if mint in open_trades:
                        continue

                # Require Twitter OR Telegram (at least one)
                if not coin.get("twitter") and not coin.get("telegram"):
                    continue
                n_social += 1

                # Active trading: last trade within 5 minutes
                last_trade = coin.get("last_trade", 0)
                secs_since = (time.time() - last_trade / 1000) if last_trade > 0 else 9999
                if secs_since > 300:
                    continue
                n_replies += 1  # reuse counter — now means "recently active"

                bond = coin.get("bond_pct", 0)
                if BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX:
                    n_bond_range += 1

                created_at = coin.get("created_at", 0)
                age_h = (time.time() - created_at / 1000) / 3600 if created_at > 0 else 0
                if age_h >= SPIKE_MIN_AGE_H:
                    n_spike_range += 1

                # ── Bundle ride ────────────────────────────────────────
                if BUNDLE_MODE == "ride" and 0 < bond < 75:
                    rug = run_rugcheck(mint)
                    if rug and rug.get("is_bundled") and not rug.get("has_mint_auth") and not rug.get("has_freeze_auth"):
                        market = get_market_data(mint)
                        if market and market["price"] > 0 and market["liq"] >= MIN_LIQ:
                            amt = trade_size()
                            log("ok", f"BUNDLE RIDE | bond={bond:.1f}%", symbol)
                            enter_trade(mint, symbol, market["price"], amt, "bundle", bond, replies)
                            time.sleep(0.5)
                            continue

                # ── Bond Runner ────────────────────────────────────────
                if BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX:
                    details = get_bonding_details(mint)
                    if details:
                        bond = details["bond_pct"]
                        if details.get("complete"):
                            continue
                    if not (BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX):
                        continue

                    rug = run_rugcheck(mint)
                    if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                        log("warn", "Mint/freeze auth — skip", symbol)
                        blacklisted_mints.add(mint)
                        continue
                    if rug and rug.get("is_bundled") and BUNDLE_MODE == "avoid":
                        log("warn", "Bundle detected — skip", symbol)
                        continue

                    market = get_market_data(mint)
                    if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                        continue

                    amt = trade_size()
                    log("ok", f"BOND RUNNER | bond={bond:.1f}% replies={replies}", symbol)
                    enter_trade(mint, symbol, market["price"], amt, "bond", bond, replies)
                    time.sleep(0.5)
                    continue

                # ── Dormant Spike ──────────────────────────────────────
                created_at = coin.get("created_at", 0)
                age_h      = (time.time() - created_at / 1000) / 3600 if created_at > 0 else 0

                if age_h >= SPIKE_MIN_AGE_H:
                    market = get_market_data(mint)
                    if not market:
                        continue
                    if market["change1h"] >= SPIKE_MIN_1H and market["liq"] >= MIN_LIQ and market["price"] > 0:
                        rug = run_rugcheck(mint)
                        if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                            log("warn", "Mint/freeze auth — skip", symbol)
                            blacklisted_mints.add(mint)
                            continue
                        if rug and rug.get("is_bundled") and BUNDLE_MODE == "avoid":
                            log("warn", "Bundle detected — skip", symbol)
                            continue
                        amt = trade_size()
                        log("ok", f"DORMANT SPIKE | age={age_h:.1f}h 1h={market['change1h']:+.0f}%", symbol)
                        enter_trade(mint, symbol, market["price"], amt, "spike", bond, replies)
                        time.sleep(0.5)

                time.sleep(0.2)

            log("info",
                f"Filter summary: {len(coins)} coins | "
                f"{n_social} have-social | {n_replies} active<5m | "
                f"{n_bond_range} in bond range | {n_spike_range} dormant")
            if n_social == 0:
                log("warn", "0 coins have Twitter or Telegram — market may be slow")
            elif n_replies == 0:
                log("warn", f"{n_social} coins have socials but none traded in last 5 min")

        except Exception as e:
            log("err", f"Scanner: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ENDPOINTS ───────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    with capital_lock:
        cap = capital
    with trades_lock:
        n = len(open_trades)
    with usdc_lock:
        locked = usdc_locked
    return (f"PumpFun Sniper | Capital:${cap:.2f} | "
            f"USDC locked:${locked:.2f} | Trade:${trade_size():.2f} | "
            f"Open:{n}/{MAX_OPEN} | {'PAPER' if PAPER_MODE else 'LIVE'}"), 200

@app.route("/status", methods=["GET"])
def status():
    wins  = [t for t in completed_trades if t["pnl"] > 0]
    total = len(completed_trades)
    pnl   = sum(t["pnl"] for t in completed_trades)
    with capital_lock:
        cap = capital
    return jsonify({
        "capital":        round(cap, 2),
        "sol_allocated":  SOL_ALLOCATED,
        "trade_size":     trade_size(),
        "paper_mode":     PAPER_MODE,
        "open_trades":    len(open_trades),
        "total_trades":   total,
        "wins":           len(wins),
        "losses":         total - len(wins),
        "win_rate":       round(len(wins) / max(total, 1) * 100, 1),
        "total_pnl":      round(pnl, 4),
        "usdc_locked":    round(usdc_locked, 4),
        "usdc_threshold": USDC_LOCK_THRESHOLD,
        "milestones_hit": sorted(_milestones_hit),
        "next_milestone": next((m for m in MILESTONES if m > cap), None),
        "settings": {
            "bond_entry":    f"{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%",
            "bond_tp":       f"{BOND_TP}%",
            "spike_min_age": f"{SPIKE_MIN_AGE_H}h",
            "spike_min_1h":  f"{SPIKE_MIN_1H}%",
            "bundle_mode":   BUNDLE_MODE,
            "min_replies":   MIN_REPLIES,
        }
    })

@app.route("/trades", methods=["GET"])
def trades():
    with trades_lock:
        open_list = [{k: v for k, v in t.items()
                      if k not in ("opened_at", "bond_slip_start")}
                     for t in open_trades.values()]
    return jsonify({"open": open_list, "completed": completed_trades[-50:]})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-100:]})

@app.route("/learn", methods=["GET"])
def learn():
    try:
        stats_file = LEARN_FILE.replace(".json", "_stats.json")
        if os.path.exists(stats_file):
            with open(stats_file) as f:
                return jsonify(json.load(f))
        return jsonify({"status": "no data yet - need 20 completed trades"})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/blacklist/<mint>", methods=["GET"])
def blacklist_route(mint):
    blacklisted_mints.add(mint)
    return jsonify({"blacklisted": mint})

@app.route("/export/wins", methods=["GET"])
def export_wins():
    wins = [t for t in completed_trades if t.get("pnl", 0) > 0]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Date/Time", "Symbol", "Strategy", "Entry Price", "Exit Price",
                     "Profit ($)", "Hold (min)", "Bond Entry %", "Replies", "Exit Reason"])
    for t in wins:
        writer.writerow([
            t.get("time", ""),
            t.get("symbol", ""),
            t.get("strategy", "").upper(),
            round(t.get("entry", 0), 8),
            round(t.get("exit", 0), 8),
            round(t.get("pnl", 0), 4),
            round(t.get("hold_m", 0), 1),
            round(t.get("bond_entry", 0), 1),
            t.get("replies", 0),
            t.get("result", ""),
        ])
    buf.seek(0)
    filename = f"winning_trades_{time.strftime('%Y%m%d')}.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})

@app.route("/export/all", methods=["GET"])
def export_all():
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Date/Time", "Symbol", "Strategy", "Entry Price", "Exit Price",
                     "PnL ($)", "Result", "Hold (min)", "Bond Entry %", "Replies"])
    for t in completed_trades:
        writer.writerow([
            t.get("time", ""),
            t.get("symbol", ""),
            t.get("strategy", "").upper(),
            round(t.get("entry", 0), 8),
            round(t.get("exit", 0), 8),
            round(t.get("pnl", 0), 4),
            t.get("result", ""),
            round(t.get("hold_m", 0), 1),
            round(t.get("bond_entry", 0), 1),
            t.get("replies", 0),
        ])
    buf.seek(0)
    filename = f"all_trades_{time.strftime('%Y%m%d')}.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})

if __name__ == "__main__":
    if not PAPER_MODE:
        if not WALLET or not WALLET_PRIVATE_KEY:
            print("[FATAL] LIVE mode requires WALLET and WALLET_PRIVATE_KEY env vars. Exiting.")
            raise SystemExit(1)
        if not _SOLANA_AVAILABLE:
            print("[FATAL] LIVE mode requires solders and solana packages.")
            raise SystemExit(1)
    elif _PAPER_ENV != "true" and (not WALLET or not WALLET_PRIVATE_KEY):
        log("warn", "WALLET/WALLET_PRIVATE_KEY not set — PAPER mode")

    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", "=" * 55)
    log("ok", f"Mode      : {'PAPER' if PAPER_MODE else 'LIVE'}")
    log("ok", f"Wallet    : {WALLET[:8] if WALLET else 'NOT SET'}...")
    log("ok", f"Capital   : ${capital:.2f} USDC (trading budget)")
    log("ok", f"SOL wallet: ${SOL_ALLOCATED:.2f} funded for on-chain execution")
    log("ok", f"Trade size: ${trade_size():.2f} ({TRADE_PCT}% of capital)")
    log("ok", f"USDC lock : activates at ${USDC_LOCK_THRESHOLD:.0f} capital")
    log("ok", "=" * 55)
    app.run(host="0.0.0.0", port=port, use_reloader=False)
