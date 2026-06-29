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

# ── REDIS PERSISTENCE (Upstash REST) ────────────────────────────
def _redis_cmd(*args):
    if not REDIS_URL or not REDIS_TOKEN:
        return None
    try:
        r = requests.post(
            REDIS_URL,
            headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
            json=list(args),
            timeout=5
        )
        if r.status_code == 200:
            return r.json().get("result")
    except Exception:
        pass
    return None

def redis_save(key, obj):
    _redis_cmd("SET", key, json.dumps(obj))

def redis_load(key):
    raw = _redis_cmd("GET", key)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return None

# ── CONFIG ──────────────────────────────────────────────────────
WALLET             = os.environ.get("WALLET", "")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY", "")
_PAPER_ENV         = os.environ.get("PAPER_MODE", "true").lower()
PAPER_MODE         = _PAPER_ENV == "true" or not WALLET or not WALLET_PRIVATE_KEY
PROFIT_GOAL       = float(os.environ.get("PROFIT_GOAL", "25000"))
RISK_LEVEL        = os.environ.get("RISK_LEVEL", "standard").lower()  # conservative / standard / aggressive
BOT_NAME          = os.environ.get("BOT_NAME", "Boogey's Treasure Chest")

# Position sizing — capital-tiered (protects small accounts)
MIN_TRADE         = float(os.environ.get("MIN_TRADE",   "3"))
MAX_TRADE         = float(os.environ.get("MAX_TRADE",   "500"))
FIXED_TRADE_SIZE  = float(os.environ.get("FIXED_TRADE_SIZE", "0"))  # 0 = use tiered %

# Capital tiers per risk level: (min_capital, trade_pct, daily_max_trades)
_RISK_TIERS = {
    "conservative": [(5_000,0.12,999),(500,0.10,999),(100,0.08,999),(0,0.05,999)],
    "standard":     [(5_000,0.18,999),(500,0.15,999),(100,0.12,999),(0,0.08,999)],
    "aggressive":   [(5_000,0.22,999),(500,0.18,999),(100,0.15,999),(0,0.12,999)],
}
_CAP_TIERS = _RISK_TIERS.get(RISK_LEVEL, _RISK_TIERS["standard"])

MAX_DAILY_LOSS_PCT = float(os.environ.get("MAX_DAILY_LOSS_PCT", "20"))  # stop day if down >20% of start capital

# Risk limits
DAILY_LOSS_MAX    = int(os.environ.get("DAILY_LOSS_MAX",  "6"))   # retune after N consecutive losses
LOSS_COOLDOWN_HRS = float(os.environ.get("LOSS_COOLDOWN_HRS", "0.083")) # 5-min pause then resume
ANALYZE_EVERY     = int(os.environ.get("ANALYZE_EVERY",   "5"))   # retune every 5 trades for faster learning

# Bond Runner strategy
BOND_ENTRY_MIN  = float(os.environ.get("BOND_ENTRY_MIN", "25"))
BOND_ENTRY_MAX  = float(os.environ.get("BOND_ENTRY_MAX", "80"))
BOND_TP         = float(os.environ.get("BOND_TP",        "67"))
BOND_SL_PCT     = float(os.environ.get("BOND_SL_PCT",    "10"))
BOND_MAX_SECS   = int(os.environ.get("BOND_MAX_SECS",    "240"))   # 4 min hard cap
BOND_STALE_SECS = int(os.environ.get("BOND_STALE_SECS",  "120"))   # exit if bond hasn't moved in 2 min

# Dormant Spike strategy
SPIKE_MIN_AGE_H = float(os.environ.get("SPIKE_MIN_AGE_H", "12"))
SPIKE_MIN_1H    = float(os.environ.get("SPIKE_MIN_1H",    "30"))
SPIKE_TP_PCT    = float(os.environ.get("SPIKE_TP_PCT",    "40"))
SPIKE_SL_PCT    = float(os.environ.get("SPIKE_SL_PCT",    "15"))
SPIKE_MAX_SECS  = int(os.environ.get("SPIKE_MAX_SECS",    "180"))   # 3 min hard cap

# Trench strategy — coins 85-97% bonded, about to graduate (fast pump at migration)
TRENCH_ENTRY_MIN = float(os.environ.get("TRENCH_ENTRY_MIN", "85"))
TRENCH_ENTRY_MAX = float(os.environ.get("TRENCH_ENTRY_MAX", "97"))
TRENCH_TP_PCT    = float(os.environ.get("TRENCH_TP_PCT",    "25"))
TRENCH_SL_PCT    = float(os.environ.get("TRENCH_SL_PCT",    "12"))
TRENCH_MAX_SECS  = int(os.environ.get("TRENCH_MAX_SECS",    "90"))  # 90s — very fast

# Migration bounce — coins that just graduated to Raydium (first 2 min momentum)
MIGRATE_MAX_AGE  = int(os.environ.get("MIGRATE_MAX_AGE",    "120")) # enter within 2 min of graduation
MIGRATE_TP_PCT   = float(os.environ.get("MIGRATE_TP_PCT",   "30"))
MIGRATE_SL_PCT   = float(os.environ.get("MIGRATE_SL_PCT",   "12"))
MIGRATE_MAX_SECS = int(os.environ.get("MIGRATE_MAX_SECS",   "120"))
GRAD_THROUGH     = os.environ.get("GRAD_THROUGH", "true").lower() != "false"  # hold bond positions through graduation to Raydium

# Exit protection
SLIP_TRIGGER   = float(os.environ.get("SLIP_TRIGGER",  "90"))
SLIP_DROP_TO   = float(os.environ.get("SLIP_DROP_TO",  "85"))
SLIP_WAIT_SECS = int(os.environ.get("SLIP_WAIT_SECS",  "6"))

# Trailing stop loss — activates once trade is up TSL_ACTIVATE_PCT, then trails BOND_SL_PCT below peak
TSL_ACTIVATE_PCT = float(os.environ.get("TSL_ACTIVATE_PCT", "5"))  # lock-in starts at +5%
SHARP_DROP_PCT = float(os.environ.get("SHARP_DROP_PCT", "4"))

# Bundle mode: "avoid" or "ride"
BUNDLE_MODE    = os.environ.get("BUNDLE_MODE", "avoid").lower()
BUNDLE_RIDE_TP = float(os.environ.get("BUNDLE_RIDE_TP", "88"))

# USDC profit lock
USDC_LOCK_THRESHOLD = float(os.environ.get("USDC_LOCK_THRESHOLD", "80"))
USDC_MINT  = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT  = "So11111111111111111111111111111111111111112"
GMGN_ROUTE = "https://gmgn.ai/defi/router/v1/sol/tx/get_swap_route"

# Copy trading via GMGN smart wallets
COPY_TRADE        = os.environ.get("COPY_TRADE", "true").lower() == "true"
COPY_WINRATE_MIN  = float(os.environ.get("COPY_WINRATE_MIN",  "60"))
COPY_WINRATE_MAX  = float(os.environ.get("COPY_WINRATE_MAX",  "99"))
COPY_MAX_WALLETS  = int(os.environ.get("COPY_MAX_WALLETS",    "5"))
COPY_MAX_AGE_SECS = int(os.environ.get("COPY_MAX_AGE_SECS",  "120"))  # ignore trades older than 2 min
# Manually tracked wallets — comma-separated Solana addresses; merged with GMGN auto-discovered wallets
TRACKED_WALLETS   = [w.strip() for w in os.environ.get("TRACKED_WALLETS", "").split(",") if w.strip()]
COPY_REFRESH_MINS = int(os.environ.get("COPY_REFRESH_MINS",  "60"))   # refresh wallet list hourly
COPY_TP_PCT       = float(os.environ.get("COPY_TP_PCT",       "40"))
COPY_SL_PCT       = float(os.environ.get("COPY_SL_PCT",       "15"))
COPY_MAX_SECS     = int(os.environ.get("COPY_MAX_SECS",       "180"))
GMGN_RANK         = "https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d"
GMGN_ACTIVITY     = "https://gmgn.ai/defi/quotation/v1/wallet_activity/sol"
GMGN_API_KEY       = os.environ.get("GMGN_API_KEY", "")
GMGN_TOP_HOLDERS   = "https://gmgn.ai/defi/quotation/v1/tokens/top_holders/sol"
GMGN_CREATED_TOKENS= "https://gmgn.ai/defi/quotation/v1/portfolio/sol"
GMGN_SIGNALS_URL   = "https://gmgn.ai/defi/quotation/v1/signals/sol"
GMGN_KOL_TRACK     = "https://gmgn.ai/defi/quotation/v1/tracks/kol/sol"
GMGN_SM_TRACK      = "https://gmgn.ai/defi/quotation/v1/tracks/smartmoney/sol"
GMGN_TRENDING_URL  = "https://gmgn.ai/defi/quotation/v1/tokens/trending/sol"
GMGN_HOT_SEARCH    = "https://gmgn.ai/defi/quotation/v1/tokens/hot_search/sol"

# Notifications
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
NTFY_TOPIC       = os.environ.get("NTFY_TOPIC", "")

# Social / quality gates
MIN_REPLIES  = int(os.environ.get("MIN_REPLIES",  "10"))
MIN_LIQ      = float(os.environ.get("MIN_LIQ",    "75"))

# General
MAX_OPEN      = int(os.environ.get("MAX_OPEN",      "6"))
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", "5"))

SOL_RPC     = "https://api.mainnet-beta.solana.com"
PUMPPORTAL  = "https://pumpportal.fun/api/trade-local"
DATA_DIR    = os.environ.get("DATA_DIR", "/tmp")
os.makedirs(DATA_DIR, exist_ok=True)
LEARN_FILE  = os.path.join(DATA_DIR, "bot_learn.json")
STATE_FILE  = os.path.join(DATA_DIR, "bot_state.json")
WEEK_FILE   = os.path.join(DATA_DIR, "bot_week.json")
REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

MILESTONES = [100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000]

# ── STATE ────────────────────────────────────────────────────────
capital           = float(os.environ.get("STARTING_CAPITAL", "39.67"))
STARTING_CAPITAL  = capital  # snapshot of configured start, for UI display
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
usdc_locked       = 0.0
usdc_lock         = threading.Lock()
# Daily tracking — resets at midnight
_daily_date       = ""
_daily_trades     = 0
_daily_wins       = 0
_daily_losses     = 0
_day_start_cap    = 0.0    # capital at start of day — used for daily loss % guard
_pause_until      = 0.0    # Unix timestamp — bot pauses trading until this time
_daily_cap_notified = False  # prevent Telegram spam when daily cap is active
_daily_lock       = threading.Lock()
# Weekly tracking
_week_start_date  = ""
_week_day_logs    = []     # one entry per day: {date, trades, wins, losses, pnl, start_cap, end_cap}
_copy_wallets     = []   # [{address, winrate}]
_copy_wallet_time = 0.0
_copied_mints     = {}   # mint -> timestamp, to avoid double-copy
_copy_lock        = threading.Lock()
_gmgn_backoff     = 0    # seconds to wait before retrying GMGN rank
_sold_mints       = {}   # mint -> timestamp, cooldown after selling to prevent re-buy
_gmgn_sm_signal_mints  = set()   # smart money buy signal mints (type 12)
_gmgn_surge_mints      = set()   # price surge signal mints (type 6)
_gmgn_kol_mints        = set()   # KOL buy mints
_gmgn_sm_sell_mints    = set()   # smart money sell mints (exit/skip filter)
_gmgn_trending_mints   = set()   # trending tokens (1h price movers on GMGN)
_gmgn_hot_mints        = set()   # hot search tokens (what people are searching on GMGN)
_gmgn_signal_time      = 0.0     # last signal refresh time
_signal_lock           = threading.Lock()
scan_log               = []
_scan_log_lock         = threading.Lock()

# ── LOGGING ─────────────────────────────────────────────────────
def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 300:
            trade_log.pop(0)

def _log_scan(symbol, mint, bond, sig, result, fi, msg):
    entry = {
        "sym":    symbol,
        "mint":   (mint[:5] + "…" + mint[-4:]) if len(mint) > 10 else mint,
        "bond":   round(float(bond or 0), 1),
        "sig":    int(sig or 0),
        "result": result,
        "fi":     fi,
        "msg":    msg,
        "ts":     round(time.time(), 3),
    }
    with _scan_log_lock:
        scan_log.insert(0, entry)
        if len(scan_log) > 30:
            scan_log.pop()

# ── NOTIFICATIONS ────────────────────────────────────────────────
_notify_queue = []
_notify_q_lock = threading.Lock()

def notify(title, body):
    """Queue a notification — sent by dedicated thread to avoid Telegram rate limits."""
    with _notify_q_lock:
        _notify_queue.append((title, body))

def _notify_worker():
    """Single thread sends queued notifications 1/sec so nothing gets dropped."""
    while True:
        item = None
        with _notify_q_lock:
            if _notify_queue:
                item = _notify_queue.pop(0)
        if item:
            title, body = item
            if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
                try:
                    _session.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID,
                              "text": f"*{title}*\n{body}",
                              "parse_mode": "Markdown"},
                        timeout=8
                    )
                except Exception as e:
                    log("warn", f"Telegram notify failed: {e}")
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
            time.sleep(1)  # 1 message/sec — well within Telegram's 30/sec limit
        else:
            time.sleep(0.2)

# ── PROGRESSIVE SIZING ───────────────────────────────────────────
def _cap_tier(cap):
    """Return (trade_pct, daily_max) for current capital level."""
    for threshold, pct, daily_max in _CAP_TIERS:
        if cap >= threshold:
            return pct, daily_max
    return _CAP_TIERS[-1][1], _CAP_TIERS[-1][2]

def trade_size():
    if FIXED_TRADE_SIZE > 0:
        return FIXED_TRADE_SIZE
    with capital_lock:
        cap = capital
    pct, _ = _cap_tier(cap)
    raw = cap * pct
    return round(max(MIN_TRADE, min(MAX_TRADE, raw)), 2)

def daily_trade_limit():
    with capital_lock:
        cap = capital
    _, daily_max = _cap_tier(cap)
    return daily_max

# ── DAILY LIMITS ─────────────────────────────────────────────────
def _save_daily_state():
    state = {
        "date":         _daily_date,
        "trades":       _daily_trades,
        "wins":         _daily_wins,
        "losses":       _daily_losses,
        "pause_until":  _pause_until,
        "capital":      capital,
        "week_start":   _week_start_date,
        "week_logs":    _week_day_logs,
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass
    redis_save("bot_state", state)

def _load_daily_state():
    global _daily_date, _daily_trades, _daily_wins, _daily_losses
    global _pause_until, capital, _week_start_date, _week_day_logs, completed_trades
    today = time.strftime("%Y-%m-%d")

    # Try Redis first (survives redeploys), fall back to local file
    s = redis_load("bot_state")
    if not s:
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE) as f:
                    s = json.load(f)
        except Exception:
            s = {}
    if s:
        try:
            if s.get("date") == today:
                _daily_date   = s["date"]
                _daily_trades = s.get("trades",      0)
                _daily_wins   = s.get("wins",        0)
                _daily_losses = s.get("losses",      0)
                _pause_until  = s.get("pause_until", 0.0)
                capital       = s.get("capital",     capital)
            _week_start_date = s.get("week_start", "")
            _week_day_logs   = s.get("week_logs",  [])
            paused_msg = f" | paused until {time.strftime('%H:%M', time.localtime(_pause_until))}" if _pause_until > time.time() else ""
            log("ok", f"Restored: {_daily_trades} trades | {_daily_wins}W {_daily_losses}L | cap=${capital:.2f}{paused_msg}")
        except Exception as e:
            log("warn", f"State restore: {e}")

    # Reload trade history — Redis first, then local file
    trades_data = redis_load("bot_trades")
    if not trades_data:
        try:
            if os.path.exists(LEARN_FILE):
                with open(LEARN_FILE) as f:
                    trades_data = json.load(f)
        except Exception:
            trades_data = []
    if trades_data:
        completed_trades.clear()
        completed_trades.extend(trades_data)
        log("ok", f"Reloaded {len(completed_trades)} completed trades")

def _reset_daily_if_needed():
    global _daily_date, _daily_trades, _daily_wins, _daily_losses, _pause_until
    global _week_start_date, _week_day_logs, _day_start_cap
    today = time.strftime("%Y-%m-%d")
    with _daily_lock:
        if _daily_date != today:
            if _daily_date:
                _week_day_logs.append({
                    "date":    _daily_date,
                    "trades":  _daily_trades,
                    "wins":    _daily_wins,
                    "losses":  _daily_losses,
                    "capital": capital,
                })
            if not _week_start_date:
                _week_start_date = today
            _daily_date    = today
            _daily_trades  = 0
            _daily_wins    = 0
            _daily_losses  = 0
            _pause_until   = 0.0
            _daily_cap_notified = False
            _day_start_cap = capital  # snapshot for daily loss % guard
            limit = daily_trade_limit()
            with capital_lock:
                cap = capital
            pct, _ = _cap_tier(cap)
            log("ok", f"New day {today} | Day {len(_week_day_logs)+1} | cap=${cap:.2f} | trade={pct*100:.0f}% (${trade_size():.2f}) | limit={limit}/day")
            _save_daily_state()
            if len(_week_day_logs) > 0:
                notify(
                    f"🌅 *Boogeys Sniper* — New Day\n"
                    f"Date: {today}\n"
                    f"Capital: ${cap:,.2f}\n"
                    f"Daily limits reset — sniping resumed."
                )

def daily_limit_reached():
    global _daily_cap_notified
    _reset_daily_if_needed()
    with _daily_lock:
        if _pause_until > time.time():
            resume = time.strftime("%H:%M", time.localtime(_pause_until))
            log("info", f"Cooling down after {_daily_losses} losses — resumes {resume}")
            return True
        # Capital-tiered daily trade cap
        limit = daily_trade_limit()
        if _daily_trades >= limit:
            log("info", f"Daily cap: {_daily_trades}/{limit} trades at current capital level — resumes tomorrow")
            if not _daily_cap_notified:
                _daily_cap_notified = True
                with capital_lock:
                    cap_now = capital
                notify(
                    f"🔒 *Boogeys Sniper* — Daily Cap\n"
                    f"{_daily_trades} trades | {_daily_wins}W {_daily_losses}L\n"
                    f"Cap: ${cap_now:,.2f}\n"
                    f"Done for today. Auto-resumes at midnight."
                )
            return True
        # Max daily loss guard — stop if down >MAX_DAILY_LOSS_PCT% from today's open
        if _day_start_cap > 0:
            with capital_lock:
                cap_now = capital
            loss_pct = (_day_start_cap - cap_now) / _day_start_cap * 100
            if loss_pct >= MAX_DAILY_LOSS_PCT:
                log("warn", f"Daily loss guard: down {loss_pct:.1f}% today (${_day_start_cap - cap_now:.2f}) — stopping until tomorrow")
                if not _daily_cap_notified:
                    _daily_cap_notified = True
                    notify(
                        f"🛑 *Boogeys Sniper* — Loss Guard\n"
                        f"Down {loss_pct:.1f}% today (${_day_start_cap - cap_now:.2f})\n"
                        f"Stopping to protect capital. Auto-resumes at midnight."
                    )
                return True
        return False

def record_daily_trade(won):
    global _daily_wins, _daily_losses, _pause_until
    with _daily_lock:
        if won:
            _daily_wins += 1
        else:
            _daily_losses += 1
        log("ok" if won else "info",
            f"Daily: {_daily_trades} trades | {_daily_wins}W {_daily_losses}L")
        if not won and _daily_losses % DAILY_LOSS_MAX == 0:
            resume_ts   = time.time() + LOSS_COOLDOWN_HRS * 3600
            _pause_until = resume_ts
            resume_str   = time.strftime("%H:%M", time.localtime(resume_ts))
            log("warn", f"{_daily_losses} losses — pausing 30min to retune. Resumes {resume_str}")
            notify("🔧 Retuning",
                   f"{_daily_losses} losses hit.\nPausing 30min, retuning strategy.\nResumes: {resume_str}")
            threading.Thread(target=_retune_strategies, daemon=True).start()
    _save_daily_state()

def _retune_strategies():
    """Run after hitting daily loss limit — analyze history and adjust params."""
    time.sleep(3)
    try:
        history = []
        if os.path.exists(LEARN_FILE):
            with open(LEARN_FILE) as f:
                history = json.load(f)
        if len(history) >= 5:
            auto_tune(history)
            resume_str = time.strftime("%H:%M", time.localtime(_pause_until))
            notify("✅ Strategy Retuned",
                   f"New bond entry: {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%\n"
                   f"Stale exit: {BOND_STALE_SECS}s\n"
                   f"Spike TP: {SPIKE_TP_PCT}%\n"
                   f"Trading resumes at {resume_str}.")
        else:
            log("info", "Not enough history yet to retune — will use defaults", "TUNE")
    except Exception as e:
        log("warn", f"Retune error: {e}", "TUNE")

def _send_daily_summary():
    """Midnight Telegram summary for the day."""
    try:
        with capital_lock:
            cap = capital
        today_pnl = cap - _day_start_cap if _day_start_cap > 0 else 0
        wr = round(_daily_wins / max(_daily_trades, 1) * 100, 1)
        # Today's exit breakdown
        today_trades = [t for t in completed_trades if t.get("date", "") == time.strftime("%Y-%m-%d")]
        exit_counts = {}
        for t in today_trades:
            r = t.get("result", "?")
            exit_counts[r] = exit_counts.get(r, 0) + 1
        exit_str = " | ".join(f"{k}:{v}" for k, v in sorted(exit_counts.items(), key=lambda x: -x[1])[:4])
        # Progress toward goal
        goal = PROFIT_GOAL
        to_go = goal - cap
        day_num = len(_week_day_logs) + 1
        # Next tune in N trades
        total_done = len(completed_trades)
        next_tune_in = ANALYZE_EVERY - (total_done % ANALYZE_EVERY)
        pct_tier, _ = _cap_tier(cap)
        msg = (f"Day {day_num} wrap-up\n"
               f"────────────────────\n"
               f"Trades: {_daily_trades} | {_daily_wins}W {_daily_losses}L ({wr}% WR)\n"
               f"PnL today: ${today_pnl:+.2f}\n"
               f"Capital: ${cap:.2f} (${to_go:,.0f} to ${PROFIT_GOAL:,.0f})\n"
               f"Trade size: {pct_tier*100:.0f}% (${trade_size():.2f})\n"
               f"Bond range: {BOND_ENTRY_MIN}–{BOND_ENTRY_MAX}%\n"
               f"Next tune in: {next_tune_in} trade(s)\n"
               f"Exits: {exit_str or 'none today'}")
        log("ok", f"Daily summary: {_daily_wins}W/{_daily_losses}L pnl=${today_pnl:+.2f} cap=${cap:.2f}", "DAY")
        notify(f"📊 Day {day_num} Summary", msg)
    except Exception as e:
        log("warn", f"Daily summary error: {e}", "DAY")

def _send_weekly_report():
    """Deep analysis after WEEK_DAYS days — reshapes strategy for next week."""
    try:
        history = []
        if os.path.exists(LEARN_FILE):
            with open(LEARN_FILE) as f:
                history = json.load(f)
        if not history:
            return

        wins   = [t for t in history if t.get("pnl", 0) > 0]
        losses = [t for t in history if t.get("pnl", 0) <= 0]
        total  = len(history)
        wr     = round(len(wins) / max(total, 1) * 100, 1)
        total_pnl = sum(t["pnl"] for t in history)

        # Best bond entry range (5% buckets)
        buckets = {}
        for t in history:
            b = round(t.get("bond_entry", 0) / 5) * 5
            if b not in buckets:
                buckets[b] = {"wins": 0, "total": 0}
            buckets[b]["total"] += 1
            if t.get("pnl", 0) > 0:
                buckets[b]["wins"] += 1
        best_bucket = max(buckets.items(), key=lambda x: x[1]["wins"] / max(x[1]["total"], 1)) if buckets else None

        # Best hour of day
        hour_wins = {}
        for t in wins:
            h = t.get("hour", 0)
            hour_wins[h] = hour_wins.get(h, 0) + 1
        best_hour = max(hour_wins.items(), key=lambda x: x[1])[0] if hour_wins else None

        # Most common loss reason
        loss_reasons = {}
        for t in losses:
            r = t.get("result", "?")
            loss_reasons[r] = loss_reasons.get(r, 0) + 1
        top_loss = max(loss_reasons.items(), key=lambda x: x[1])[0] if loss_reasons else "none"

        # Day-by-day capital
        cap_progression = " → ".join(f"${d['capital']:.0f}" for d in _week_day_logs) if _week_day_logs else "N/A"

        # Apply best settings for week 2
        if best_bucket:
            best_b = best_bucket[0]
            BOND_ENTRY_MIN_new = max(50.0, best_b - 2)
            BOND_ENTRY_MAX_new = min(78.0, best_b + 4)
        else:
            BOND_ENTRY_MIN_new = BOND_ENTRY_MIN
            BOND_ENTRY_MAX_new = BOND_ENTRY_MAX

        auto_tune(history)

        report = (
            f"Week 1 Complete!\n"
            f"{'='*20}\n"
            f"Trades: {total} | {len(wins)}W {len(losses)}L\n"
            f"Win rate: {wr}%\n"
            f"Total PnL: ${total_pnl:+.2f}\n"
            f"Capital: ${capital:.2f}\n\n"
            f"Best bond range: {best_bucket[0] if best_bucket else '?'}%\n"
            f"Best hour: {best_hour}:00\n"
            f"Top loss reason: {top_loss}\n\n"
            f"Capital path:\n{cap_progression}\n\n"
            f"Week 2 settings:\n"
            f"Bond: {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%\n"
            f"Stale: {BOND_STALE_SECS}s | SL: {BOND_SL_PCT}%"
        )
        log("ok", f"WEEK 1 DONE | {wr}% WR | PnL ${total_pnl:+.2f} | cap ${capital:.2f}", "WEEK")
        notify("📈 Week 1 Complete!", report)

        # Save full report
        try:
            with open("/tmp/bot_week_report.json", "w") as f:
                json.dump({
                    "week": 1, "trades": total, "wins": len(wins), "losses": len(losses),
                    "win_rate": wr, "total_pnl": round(total_pnl, 4),
                    "final_capital": round(capital, 2),
                    "best_bond_bucket": best_bucket[0] if best_bucket else None,
                    "best_hour": best_hour, "top_loss_reason": top_loss,
                    "day_logs": _week_day_logs,
                    "week2_settings": {
                        "bond_entry": f"{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%",
                        "stale_secs": BOND_STALE_SECS, "sl_pct": BOND_SL_PCT,
                    }
                }, f, indent=2)
        except Exception:
            pass
    except Exception as e:
        log("warn", f"Weekly report error: {e}", "WEEK")

def daily_summary_loop():
    """Sends midnight summary every day. Sends deep weekly report every 7 days."""
    while True:
        now = time.localtime()
        secs_to_midnight = (23 - now.tm_hour) * 3600 + (59 - now.tm_min) * 60 + (60 - now.tm_sec)
        time.sleep(secs_to_midnight + 5)
        _send_daily_summary()
        # Weekly deep report every 7 days — then keeps running
        if len(_week_day_logs) % 7 == 6:
            time.sleep(10)
            _send_weekly_report()

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
        trimmed = history[-200:]
        with open(LEARN_FILE, "w") as f:
            json.dump(trimmed, f)
        redis_save("bot_trades", trimmed)
        if len(history) % ANALYZE_EVERY == 0:
            log("ok", f"Analyzing last {ANALYZE_EVERY} trades — retuning strategy...", "TUNE")
            auto_tune(history)
            log("ok", f"Tuned: bond={BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% stale={BOND_STALE_SECS}s SL={BOND_SL_PCT}% spikeTP={SPIKE_TP_PCT}%", "TUNE")
    except Exception as e:
        log("warn", f"Learning record: {e}")

def auto_tune(history):
    global BOND_ENTRY_MIN, BOND_ENTRY_MAX, SPIKE_TP_PCT, BOND_STALE_SECS, BOND_SL_PCT, BOND_MAX_SECS
    try:
        recent = history[-60:]
        wins   = [t for t in recent if t.get("pnl", 0) > 0]
        losses = [t for t in recent if t.get("pnl", 0) <= 0]

        bond_wins   = [t for t in wins   if t.get("strategy") == "bond"]
        spike_wins  = [t for t in wins   if t.get("strategy") == "spike"]
        bond_all    = [t for t in recent if t.get("strategy") == "bond"]
        spike_all   = [t for t in recent if t.get("strategy") == "spike"]
        bond_losses = [t for t in losses if t.get("strategy") == "bond"]

        bond_wr  = len(bond_wins)  / max(len(bond_all),  1)
        spike_wr = len(spike_wins) / max(len(spike_all), 1)

        # Tune bond entry range toward what's winning
        if bond_wins:
            avg_win_entry = sum(t.get("bond_entry", BOND_ENTRY_MIN) for t in bond_wins) / len(bond_wins)
            BOND_ENTRY_MIN = round(min(max(avg_win_entry - 2, 50), 72), 1)
            BOND_ENTRY_MAX = round(min(BOND_ENTRY_MIN + 6, 78), 1)
        elif bond_wr < 0.35 and len(bond_all) >= 5:
            # Poor win rate — tighten entry, look for higher momentum
            BOND_ENTRY_MIN = round(min(BOND_ENTRY_MIN + 1.5, 68), 1)
            BOND_ENTRY_MAX = round(min(BOND_ENTRY_MAX + 1.5, 74), 1)

        # Tune stale exit based on how long winners actually held
        if bond_wins:
            avg_win_hold_secs = (sum(t.get("hold_m", 2) for t in bond_wins) / len(bond_wins)) * 60
            BOND_STALE_SECS = max(90, min(300, int(avg_win_hold_secs * 0.7)))

        # Tune hard timeout — give it at least as long as average winner
        if bond_wins:
            avg_win_hold_secs = (sum(t.get("hold_m", 2) for t in bond_wins) / len(bond_wins)) * 60
            BOND_MAX_SECS = max(180, min(480, int(avg_win_hold_secs * 1.5)))

        # Loosen SL if losses are all from price drop (not stale/timeout)
        sl_losses = [t for t in bond_losses if t.get("result") == "BOND_SL"]
        if len(sl_losses) > len(bond_losses) * 0.6 and BOND_SL_PCT > 6:
            BOND_SL_PCT = round(BOND_SL_PCT - 1, 1)  # tighten SL to cut losses faster

        # Spike tuning
        if spike_wr > bond_wr + 0.2 and SPIKE_TP_PCT < 80:
            SPIKE_TP_PCT = round(SPIKE_TP_PCT * 1.1, 1)
        elif spike_wr < 0.3 and SPIKE_TP_PCT > 25:
            SPIKE_TP_PCT = round(SPIKE_TP_PCT * 0.9, 1)

        overall_wr = round(len(wins) / max(len(recent), 1) * 100, 1)
        stats = {
            "tuned_at":        time.strftime("%Y-%m-%d %H:%M:%S"),
            "trades_analyzed": len(recent),
            "overall_wr":      f"{overall_wr}%",
            "bond_wr":         f"{round(bond_wr*100,1)}%",
            "spike_wr":        f"{round(spike_wr*100,1)}%",
            "bond_entry":      f"{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%",
            "bond_stale_secs": BOND_STALE_SECS,
            "bond_max_secs":   BOND_MAX_SECS,
            "bond_sl_pct":     BOND_SL_PCT,
            "spike_tp_pct":    SPIKE_TP_PCT,
        }
        log("ok", f"Auto-tuned: bond={BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% "
                  f"stale={BOND_STALE_SECS}s sl={BOND_SL_PCT}% wr={overall_wr}%", "TUNE")
        notify("🧠 Auto-Tuned",
               f"Analyzed {len(recent)} trades\n"
               f"Overall WR: {overall_wr}%\n"
               f"Bond WR: {round(bond_wr*100,1)}% | Spike WR: {round(spike_wr*100,1)}%\n"
               f"────────────────────\n"
               f"Bond entry: {BOND_ENTRY_MIN}–{BOND_ENTRY_MAX}%\n"
               f"Stale exit: {BOND_STALE_SECS}s | SL: {BOND_SL_PCT}%\n"
               f"Spike TP: {SPIKE_TP_PCT}%")
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
        "https://client-api-2.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://pump.fun/",
        "Origin": "https://pump.fun",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }
    for url in endpoints:
        try:
            res = _session.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                log("warn", f"pump.fun {res.status_code}: {url[-50:]}")
                continue
            data  = res.json()
            items = data if isinstance(data, list) else data.get("coins", data.get("data", []))
            if not items:
                continue
            coins = []
            for coin in items[:100]:
                mint  = coin.get("mint", "")
                vsol  = float(coin.get("virtual_sol_reserves", 0) or 0)
                vtok  = float(coin.get("virtual_token_reserves", 0) or 0)
                bond  = min((vsol / 85_000_000_000) * 100, 99.9) if vsol > 0 else 0
                if not mint or coin.get("complete", False):
                    continue
                # Resilient social field lookups — API field names vary by version
                socials  = coin.get("socials") or {}
                has_tw   = bool(coin.get("twitter") or coin.get("twitter_url") or socials.get("twitter"))
                has_tg   = bool(coin.get("telegram") or coin.get("telegram_url") or socials.get("telegram"))
                has_web  = bool(coin.get("website") or coin.get("website_url") or socials.get("website"))
                # Resilient timestamp field lookups
                last_ts  = int(
                    coin.get("last_trade_timestamp") or
                    coin.get("last_trade_time") or
                    coin.get("last_trade") or
                    coin.get("updated_timestamp") or
                    0
                )
                created_ts = int(
                    coin.get("created_timestamp") or
                    coin.get("created_at") or
                    coin.get("creation_time") or
                    0
                )
                # Detect pump.swap protocol: native SOL quote mint (all 1s = system program)
                quote_mint = coin.get("quote_mint", "")
                is_pump_swap = (
                    quote_mint == "11111111111111111111111111111111"
                    or bool(coin.get("pump_swap_pool"))
                    or bool(coin.get("is_cashback_enabled"))
                )
                coins.append({
                    "mint":       mint,
                    "symbol":     coin.get("symbol", mint[:8]),
                    "bond_pct":   round(bond, 1),
                    "vsol":       vsol / 1e9,
                    "vtok":       vtok,
                    "twitter":    has_tw,
                    "telegram":   has_tg,
                    "website":    has_web,
                    "dev":        coin.get("creator", "") or coin.get("dev", ""),
                    "replies":    int(coin.get("reply_count", 0) or 0),
                    "created_at": created_ts,
                    "last_trade": last_ts,
                    "complete":   False,
                    "pump_swap":  is_pump_swap,
                })
            log("info", f"pump.fun API: {len(coins)} live coins")
            if coins:
                return coins
        except Exception as e:
            log("warn", f"pump.fun endpoint failed: {e}")
    return []

def get_recently_graduated():
    """Pump.fun coins that just graduated to Raydium within MIGRATE_MAX_AGE seconds."""
    hdrs = {"User-Agent": "Mozilla/5.0", "Referer": "https://pump.fun/", "Accept": "application/json"}
    endpoints = [
        "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false&complete=true",
        "https://frontend-api-v2.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false&complete=true",
    ]
    now = time.time()
    for url in endpoints:
        try:
            res = _session.get(url, headers=hdrs, timeout=10)
            if res.status_code != 200:
                continue
            data  = res.json()
            items = data if isinstance(data, list) else data.get("coins", [])
            recent = []
            for coin in items:
                mint = coin.get("mint", "")
                if not mint:
                    continue
                # king_of_the_hill_timestamp is set when the coin graduates
                koth = int(coin.get("king_of_the_hill_timestamp", 0) or 0)
                last = int(coin.get("last_trade_timestamp", 0) or 0)
                grad_ts = koth if koth > 0 else last
                age_secs = (now - grad_ts / 1000) if grad_ts > 0 else 9999
                if 0 < age_secs <= MIGRATE_MAX_AGE:
                    recent.append({
                        "mint":     mint,
                        "symbol":   coin.get("symbol", mint[:8]),
                        "dev":      coin.get("creator", ""),
                        "grad_age": int(age_secs),
                    })
            if recent:
                log("info", f"Migration scan: {len(recent)} recently graduated", "GRAD")
            return recent
        except Exception as e:
            log("warn", f"get_recently_graduated: {e}", "GRAD")
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

def check_holder_concentration(mint) -> tuple:
    """(ok, reason) — ok=False if top-10 wallets hold >60% of supply."""
    try:
        hdrs = {"User-Agent":"Mozilla/5.0","Referer":"https://gmgn.ai/","Origin":"https://gmgn.ai"}
        r = _session.get(f"{GMGN_TOP_HOLDERS}/{mint}", headers=hdrs, params={"limit":10}, timeout=8)
        if r.status_code != 200:
            return True, ""
        data = r.json().get("data") or {}
        holders = data.get("holders") or data if isinstance(data, list) else []
        top10_pct = sum(float(h.get("amount_percentage") or h.get("percent") or 0) for h in holders[:10])
        if top10_pct > 60:
            return False, f"top10={top10_pct:.0f}%"
        return True, ""
    except Exception:
        return True, ""

def check_dev_history(dev_wallet) -> tuple:
    """(ok, reason) — ok=False if dev has 2+ tokens that rugged (>95% drop from ATH)."""
    if not dev_wallet:
        return True, ""
    try:
        hdrs = {"User-Agent":"Mozilla/5.0","Referer":"https://gmgn.ai/","Origin":"https://gmgn.ai"}
        r = _session.get(
            f"{GMGN_CREATED_TOKENS}/{dev_wallet}/created_tokens",
            headers=hdrs, params={"order_by":"token_ath_mc","direction":"desc","limit":20}, timeout=8)
        if r.status_code != 200:
            return True, ""
        raw = r.json().get("data") or {}
        tokens = raw.get("tokens") or (raw if isinstance(raw, list) else [])
        rugs = sum(
            1 for t in tokens
            if float(t.get("token_ath_mc") or 0) > 50_000
            and float(t.get("market_cap") or 0) < float(t.get("token_ath_mc") or 1) * 0.05
        )
        if rugs >= 2:
            return False, f"dev rugged {rugs}x"
        return True, ""
    except Exception:
        return True, ""

def _refresh_gmgn_signals():
    """Refresh all GMGN signal mint sets: smart-money, KOL, trending, hot search."""
    global _gmgn_sm_signal_mints, _gmgn_surge_mints, _gmgn_kol_mints
    global _gmgn_sm_sell_mints, _gmgn_trending_mints, _gmgn_hot_mints, _gmgn_signal_time
    try:
        hdrs = {
            "Referer":    "https://gmgn.ai/",
            "Origin":     "https://gmgn.ai",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        if GMGN_API_KEY:
            hdrs["Authorization"] = f"Bearer {GMGN_API_KEY}"

        def _addrs(items):
            return {i.get("address") or i.get("mint") or i.get("token_address","")
                    for i in (items or [])
                    if i.get("address") or i.get("mint") or i.get("token_address")}

        def _fetch_signal(stype):
            r = _session.get(GMGN_SIGNALS_URL, headers=hdrs,
                             params={"signal_type": stype, "chain":"sol","limit":100}, timeout=8)
            return _addrs(r.json().get("data") or []) if r.status_code == 200 else set()

        sm  = _fetch_signal(12)   # smart money buy
        srg = _fetch_signal(6)    # price surge

        # KOL buys
        kol = set()
        if GMGN_API_KEY:
            r2 = _session.get(GMGN_KOL_TRACK, headers=hdrs,
                              params={"side":"buy","limit":50}, timeout=8)
            if r2.status_code == 200:
                kol = _addrs(r2.json().get("data") or [])

        # Smart-money sells — separate set, used only for exit filter
        sm_sell = set()
        r3 = _session.get(GMGN_SM_TRACK, headers=hdrs,
                          params={"side":"sell","limit":50}, timeout=8)
        if r3.status_code == 200:
            sm_sell = _addrs(r3.json().get("data") or [])

        # Trending tokens — 1h price movers
        trending = set()
        r4 = _session.get(GMGN_TRENDING_URL, headers=hdrs,
                          params={"period":"1h","limit":50}, timeout=8)
        if r4.status_code == 200:
            trending = _addrs(r4.json().get("data") or [])

        # Hot search — what traders are actively searching right now
        hot = set()
        r5 = _session.get(GMGN_HOT_SEARCH, headers=hdrs,
                          params={"limit":30}, timeout=8)
        if r5.status_code == 200:
            hot = _addrs(r5.json().get("data") or [])

        with _signal_lock:
            _gmgn_sm_signal_mints = sm
            _gmgn_surge_mints     = srg
            _gmgn_kol_mints       = kol
            _gmgn_sm_sell_mints   = sm_sell
            _gmgn_trending_mints  = trending
            _gmgn_hot_mints       = hot
            _gmgn_signal_time     = time.time()
        log("info",
            f"GMGN signals: sm_buy={len(sm)} surge={len(srg)} kol={len(kol)} "
            f"sm_sell={len(sm_sell)} trending={len(trending)} hot={len(hot)}", "GMGN")
    except Exception as e:
        log("warn", f"GMGN signal refresh error: {e}", "GMGN")

def run_signal_refresh_loop():
    """Background thread: refresh GMGN signals every 5 minutes."""
    while True:
        _refresh_gmgn_signals()
        time.sleep(300)

def gmgn_signal_score(mint) -> int:
    """
    Returns 0-5 signal score for a mint:
      +1 smart money buy  +1 price surge  +1 KOL buy
      +1 trending (1h)    +1 hot search
    """
    with _signal_lock:
        return (
            (1 if mint in _gmgn_sm_signal_mints else 0) +
            (1 if mint in _gmgn_surge_mints      else 0) +
            (1 if mint in _gmgn_kol_mints         else 0) +
            (1 if mint in _gmgn_trending_mints    else 0) +
            (1 if mint in _gmgn_hot_mints         else 0)
        )

def gmgn_smart_money_selling(mint) -> bool:
    """Returns True if smart money is actively selling this mint."""
    with _signal_lock:
        return mint in _gmgn_sm_sell_mints

# ── USDC PROFIT LOCK ─────────────────────────────────────────────
def lock_profit_to_usdc(profit_usd):
    """Swap profit_usd worth of SOL into USDC via GMGN after winning trade."""
    global usdc_locked
    if profit_usd <= 0:
        return
    if PAPER_MODE:
        with usdc_lock:
            usdc_locked += profit_usd
        log("ok", f"[PAPER] Locked ${profit_usd:.4f} profit -> USDC | Total: ${usdc_locked:.2f}", "USDC")
        return
    try:
        sol_price = get_sol_price()
        if not sol_price:
            log("warn", "Cannot get SOL price — skipping USDC lock", "USDC")
            return
        sol_amount = profit_usd / sol_price
        lamports   = int(sol_amount * 1_000_000_000)
        if lamports < 5_000:
            return

        # Get swap route from GMGN
        res = _session.get(
            GMGN_ROUTE,
            params={
                "token_in_address":  WSOL_MINT,
                "token_out_address": USDC_MINT,
                "in_amount":         lamports,
                "from_address":      WALLET,
                "slippage":          0.5,
            },
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if res.status_code != 200:
            log("warn", f"GMGN route failed {res.status_code}: {res.text[:80]}", "USDC")
            return

        data    = res.json()
        raw_tx  = data.get("data", {}).get("raw_tx", {}).get("swapTransaction", "")
        out_amt = data.get("data", {}).get("quote", {}).get("outputAmount", 0)
        if not raw_tx:
            log("warn", f"GMGN returned no transaction: {str(data)[:120]}", "USDC")
            return

        import base64
        tx_bytes = base64.b64decode(raw_tx)
        keypair  = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx       = VersionedTransaction(VersionedTransaction.from_bytes(tx_bytes).message, [keypair])
        client   = Client(SOL_RPC)
        result   = client.send_raw_transaction(bytes(tx), opts=TxOpts(skip_preflight=True, preflight_commitment="confirmed"))
        sig      = str(result.value)

        usdc_out = float(out_amt) / 1_000_000  # USDC has 6 decimals
        with usdc_lock:
            usdc_locked += usdc_out
        log("ok", f"GMGN locked ${usdc_out:.4f} USDC | Total: ${usdc_locked:.4f} | sig={sig[:20]}...", "USDC")
        log("ok", f"https://solscan.io/tx/{sig}", "USDC")
    except Exception as e:
        log("warn", f"USDC lock error: {e}", "USDC")

# ── TRADE EXECUTION ──────────────────────────────────────────────
def execute_buy(mint, symbol, amount, pump_swap=False, raydium=False):
    if PAPER_MODE:
        log("ok", f"[PAPER] Buy ${amount:.2f} -> {symbol}", symbol)
        return "PAPER_TX"
    try:
        sol_price = get_sol_price()
        if not sol_price:
            log("err", "Cannot get SOL price — buy aborted", symbol)
            return None
        sol_amount = round(amount / sol_price, 6)
        if raydium:
            pool = "raydium"
        elif pump_swap:
            pool = "pump-swap"
        else:
            pool = "pump"

        res = _session.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={"publicKey": WALLET, "action": "buy", "mint": mint,
                  "denominatedInSol": "true", "amount": sol_amount,
                  "slippage": 20, "priorityFee": 0.001, "pool": pool},
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

def execute_sell(tokens, mint, symbol, pump_swap=False, raydium=False):
    if PAPER_MODE:
        log("ok", f"[PAPER] Sell {symbol}", symbol)
        return "PAPER_TX"
    try:
        if raydium:
            pool = "raydium"
        elif pump_swap:
            pool = "pump-swap"
        else:
            pool = "pump"
        res = _session.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={"publicKey": WALLET, "action": "sell", "mint": mint,
                  "denominatedInSol": "false", "amount": tokens,
                  "slippage": 20, "priorityFee": 0.001, "pool": pool},
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
def enter_trade(mint, symbol, entry_price, amount, strategy, bond_entry=0, replies=0, pump_swap=False, raydium=False):
    global capital, _daily_trades
    if daily_limit_reached():
        _log_scan(symbol, mint, bond_entry, 0, "cap", -1, "DAILY CAP / COOLDOWN")
        return False
    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            return False
    # Skip recently sold coins (30 min cooldown)
    with _copy_lock:
        sold_at = _sold_mints.get(mint, 0)
        if sold_at and time.time() - sold_at < 1800:
            return False
    with capital_lock:
        if capital < amount:
            return False

    tx = execute_buy(mint, symbol, amount, pump_swap=pump_swap, raydium=raydium)
    if not tx:
        return False

    with _daily_lock:
        _daily_trades += 1

    with capital_lock:
        capital -= amount

    with trades_lock:
        open_trades[mint] = {
            "symbol":            symbol,
            "mint":              mint,
            "strategy":          strategy,
            "entry":             entry_price,
            "amount":            amount,
            "tokens":            amount / max(entry_price, 1e-12),
            "opened_at":         time.time(),
            "bond_entry":        bond_entry,
            "bond_high":         bond_entry,
            "bond_prev":         bond_entry,
            "bond_last_moved":   time.time(),
            "bond_slip_start":   None,
            "price_high":        entry_price,   # trailing SL tracks peak price
            "replies":           replies,
            "pump_swap":         pump_swap,
            "raydium":           raydium,
        }

    log("ok", f"ENTER [{strategy.upper()}] ${amount:.2f} | bond={bond_entry:.1f}%", symbol)
    _log_scan(symbol, mint, bond_entry, 0, "pass", -1, f"ENTERED [{strategy.upper()}] ${amount:.2f}")
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

    execute_sell(trade["tokens"], mint, trade["symbol"], pump_swap=trade.get("pump_swap", False), raydium=trade.get("raydium", False))
    with _copy_lock:
        _sold_mints[mint] = time.time()  # 30 min cooldown before re-buying
    record_daily_trade(won=(pnl > 0))

    pnl_pct = round((price - trade["entry"]) / max(trade["entry"], 1e-12) * 100, 2)
    rec = {
        "id":         len(completed_trades) + 1,
        "symbol":     trade["symbol"],
        "mint":       mint,
        "strategy":   trade["strategy"],
        "entry":      trade["entry"],
        "exit":       price,
        "peak":       round(trade.get("price_high", trade["entry"]), 8),
        "amount":     round(trade["amount"], 2),
        "result":     reason,
        "pnl":        round(pnl, 4),
        "pnl_pct":    pnl_pct,
        "hold_m":     round(hold_m, 1),
        "bond_entry": trade["bond_entry"],
        "bond_high":  round(trade.get("bond_high", 0), 1),
        "replies":    trade["replies"],
        "hour":       int(time.strftime("%H")),
        "opened_ts":  round(trade["opened_at"]),
        "closed_ts":  round(time.time()),
        "date":       time.strftime("%Y-%m-%d"),
        "time":       time.strftime("%H:%M:%S"),
    }
    completed_trades.append(rec)
    record_trade(rec)
    check_milestones()
    _save_daily_state()

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

            # Paper mode: simulate price from bond % movement when DexScreener has no data
            if PAPER_MODE and price == trade["entry"] and bond > 0 and trade.get("bond_entry", 0) > 0:
                bond_move = bond - trade["bond_entry"]
                price = trade["entry"] * (1 + bond_move / 100)

            with trades_lock:
                if mint not in open_trades:
                    continue
                if bond > open_trades[mint]["bond_high"]:
                    open_trades[mint]["bond_high"]      = bond
                    open_trades[mint]["bond_last_moved"] = time.time()
                if price > open_trades[mint]["price_high"]:
                    open_trades[mint]["price_high"] = price
                bond_high       = open_trades[mint]["bond_high"]
                bond_prev       = open_trades[mint]["bond_prev"]
                bond_last_moved = open_trades[mint].get("bond_last_moved", time.time())
                slip_start      = open_trades[mint]["bond_slip_start"]
                price_high      = open_trades[mint]["price_high"]
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

            # Stale exit: bond hasn't moved in BOND_STALE_SECS — momentum dead
            stale_secs = time.time() - bond_last_moved
            if strategy in ("bond", "bundle") and elapsed > 30 and stale_secs >= BOND_STALE_SECS:
                log("warn", f"Bond stale {stale_secs:.0f}s — exiting", symbol)
                exit_trade(mint, price, "STALE", bond)
                continue

            # Compute trailing SL level for all strategies
            # Once trade is up TSL_ACTIVATE_PCT, stop trails below price_high
            entry_gain_pct = ((price_high - trade["entry"]) / trade["entry"]) * 100
            if entry_gain_pct >= TSL_ACTIVATE_PCT:
                tsl_price = price_high * (1 - BOND_SL_PCT / 100)
            else:
                tsl_price = trade["entry"] * (1 - BOND_SL_PCT / 100)

            # Graduation follow-through: bond position graduated — ride on Raydium via migrate rules
            if strategy == "bond" and GRAD_THROUGH and details and details.get("complete"):
                with trades_lock:
                    if mint in open_trades:
                        open_trades[mint]["strategy"]       = "migrate"
                        open_trades[mint]["raydium"]        = True
                        open_trades[mint]["grad_opened_at"] = time.time()
                strategy = "migrate"
                log("ok", f"GRADUATED → riding on Raydium (bond={bond:.1f}%)", symbol)

            # Bond Runner exits
            if strategy == "bond":
                if bond >= BOND_TP:
                    exit_trade(mint, price, "BOND_TP", bond)
                    continue
                if price <= tsl_price:
                    reason = "BOND_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "BOND_SL"
                    exit_trade(mint, price, reason, bond)
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
                if price <= tsl_price:
                    reason = "SPIKE_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "SPIKE_SL"
                    exit_trade(mint, price, reason, bond)
                    continue
                if elapsed >= SPIKE_MAX_SECS:
                    exit_trade(mint, price, "SPIKE_TIME", bond)
                    continue

            # Copy trade exits
            if strategy == "copy":
                move = ((price - trade["entry"]) / trade["entry"]) * 100
                if move >= COPY_TP_PCT:
                    exit_trade(mint, price, "COPY_TP", bond)
                    continue
                if price <= tsl_price:
                    reason = "COPY_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "COPY_SL"
                    exit_trade(mint, price, reason, bond)
                    continue
                if elapsed >= COPY_MAX_SECS:
                    exit_trade(mint, price, "COPY_TIME", bond)
                    continue

            # Trench exits — near-graduation play, very fast window
            if strategy == "trench":
                move = ((price - trade["entry"]) / trade["entry"]) * 100
                if bond >= 99:
                    # Coin graduated while we held — exit before Raydium migration confusion
                    exit_trade(mint, price, "TRENCH_GRAD", bond)
                    continue
                if move >= TRENCH_TP_PCT:
                    exit_trade(mint, price, "TRENCH_TP", bond)
                    continue
                if price <= tsl_price:
                    reason = "TRENCH_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "TRENCH_SL"
                    exit_trade(mint, price, reason, bond)
                    continue
                if elapsed >= TRENCH_MAX_SECS:
                    exit_trade(mint, price, "TRENCH_TIME", bond)
                    continue

            # Migration bounce exits — Raydium momentum after graduation
            if strategy == "migrate":
                # grad_opened_at resets the clock at graduation so full MIGRATE_MAX_SECS applies
                migrate_elapsed = time.time() - trade.get("grad_opened_at", trade["opened_at"])
                move = ((price - trade["entry"]) / trade["entry"]) * 100
                if move >= MIGRATE_TP_PCT:
                    exit_trade(mint, price, "MIGRATE_TP", bond)
                    continue
                if price <= tsl_price:
                    reason = "MIGRATE_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "MIGRATE_SL"
                    exit_trade(mint, price, reason, bond)
                    continue
                if migrate_elapsed >= MIGRATE_MAX_SECS:
                    exit_trade(mint, price, "MIGRATE_TIME", bond)
                    continue

            pct = ((price - trade["entry"]) / trade["entry"]) * 100
            tsl_info = f" TSL@{tsl_price:.6f}" if entry_gain_pct >= TSL_ACTIVATE_PCT else ""
            log("info", f"[{strategy}] bond={bond:.1f}% price={pct:+.1f}% peak={entry_gain_pct:+.1f}%{tsl_info} {elapsed/60:.1f}m", symbol)

# ── COPY TRADING ─────────────────────────────────────────────────
def fetch_smart_wallets():
    global _copy_wallets, _copy_wallet_time, _gmgn_backoff
    if _gmgn_backoff > 0:
        if time.time() < _gmgn_backoff:
            return
        _gmgn_backoff = 0
    headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://gmgn.ai/",
        "Origin":          "https://gmgn.ai",
    }
    try:
        res = _session.get(
            GMGN_RANK,
            params={"orderby": "winrate", "direction": "desc", "limit": 100},
            headers=headers,
            timeout=12
        )
        if res.status_code == 403:
            _gmgn_backoff = time.time() + 600   # back off 10 min on 403 (Railway IPs rotate)
            log("warn", "GMGN rank blocked (403) — will retry in 10min", "COPY")
            return
        if res.status_code != 200:
            log("warn", f"GMGN rank {res.status_code}", "COPY")
            return
        rank_list = res.json().get("data", {}).get("rank", [])
        qualified = []
        for w in rank_list:
            wr_raw = float(w.get("winrate", 0) or 0)
            wr     = wr_raw * 100 if wr_raw <= 1 else wr_raw  # handle 0-1 or 0-100 format
            addr   = w.get("address", "")
            if addr and COPY_WINRATE_MIN <= wr < COPY_WINRATE_MAX:
                qualified.append({"address": addr, "winrate": round(wr, 1)})
        qualified = sorted(qualified, key=lambda x: x["winrate"], reverse=True)[:COPY_MAX_WALLETS]
        with _copy_lock:
            _copy_wallets     = qualified
            _copy_wallet_time = time.time()
        log("ok", f"Tracking {len(qualified)} wallets | WR {COPY_WINRATE_MIN}-{COPY_WINRATE_MAX}%", "COPY")
        for w in qualified:
            log("info", f"  {w['address'][:8]}... WR:{w['winrate']}%", "COPY")
    except Exception as e:
        log("warn", f"fetch_smart_wallets: {e}", "COPY")

def copy_trade_loop():
    time.sleep(15)
    fetch_smart_wallets()
    while scan_active:
        try:
            # Refresh wallet list every hour (or retry after backoff expires)
            with _copy_lock:
                stale = time.time() - _copy_wallet_time > COPY_REFRESH_MINS * 60
            if stale or (_gmgn_backoff > 0 and time.time() >= _gmgn_backoff):
                fetch_smart_wallets()

            with _copy_lock:
                wallets = list(_copy_wallets)
                # Expire copied mints older than 10 minutes
                now = time.time()
                expired = [m for m, t in _copied_mints.items() if now - t > 600]
                for m in expired:
                    _copied_mints.pop(m, None)

            # Always include manually tracked wallets (from TRACKED_WALLETS env var)
            tracked_addrs = {w["address"] for w in wallets}
            for addr in TRACKED_WALLETS:
                if addr not in tracked_addrs:
                    wallets.append({"address": addr, "winrate": 100.0})

            if not wallets:
                time.sleep(60)
                continue

            for w in wallets:
                if daily_limit_reached():
                    break
                addr = w["address"]
                try:
                    res = _session.get(
                        GMGN_ACTIVITY,
                        params={"address": addr, "type": "buy", "limit": 5},
                        headers={"User-Agent": "Mozilla/5.0"},
                        timeout=8
                    )
                    if res.status_code != 200:
                        continue
                    acts = res.json().get("data", {}).get("activities", [])
                    for act in acts:
                        mint   = act.get("token_address", "")
                        symbol = act.get("token_symbol", mint[:8] if mint else "?")
                        if not mint:
                            continue
                        # Only mirror very recent trades
                        ts       = int(act.get("timestamp", 0) or 0)
                        age_secs = time.time() - ts if ts > 0 else 9999
                        if age_secs > COPY_MAX_AGE_SECS:
                            continue
                        with _copy_lock:
                            if mint in _copied_mints:
                                continue
                        with trades_lock:
                            if mint in open_trades:
                                continue
                        if mint in blacklisted_mints:
                            continue
                        # Safety check
                        rug = run_rugcheck(mint)
                        if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                            blacklisted_mints.add(mint)
                            continue
                        if rug and rug.get("is_bundled") and BUNDLE_MODE == "avoid":
                            continue
                        holder_ok, holder_reason = check_holder_concentration(mint)
                        if not holder_ok:
                            log("warn", f"SKIP: {holder_reason}", symbol)
                            continue
                        dev_wallet = act.get("creator", "") or act.get("dev", "")
                        dev_ok, dev_reason = check_dev_history(dev_wallet)
                        if not dev_ok:
                            log("warn", f"SKIP: {dev_reason}", symbol)
                            continue
                        if gmgn_smart_money_selling(mint):
                            log("warn", "SKIP: smart money selling", symbol)
                            continue
                        sig_score = gmgn_signal_score(mint)
                        market = get_market_data(mint)
                        if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                            continue
                        with _copy_lock:
                            _copied_mints[mint] = time.time()
                        amt = trade_size()
                        log("ok", f"COPY {addr[:8]}... WR:{w['winrate']}% | ${amt:.2f} | sig={sig_score}", symbol)
                        notify(f"📋 COPY {symbol}",
                               f"Wallet: {addr[:8]}...\nWin rate: {w['winrate']}%\nAmount: ${amt:.2f}")
                        enter_trade(mint, symbol, market["price"], amt, "copy", 0, 0)
                    time.sleep(0.5)
                except Exception as e:
                    log("warn", f"Wallet {addr[:8]} activity: {e}", "COPY")
        except Exception as e:
            log("err", f"Copy loop: {e}", "COPY")
        time.sleep(15)

# ── SCANNER LOOP ─────────────────────────────────────────────────
def scanner_loop():
    log("ok", "=" * 55)
    log("ok", "PumpFun Sniper — Bond Runner + Dormant Spike")
    log("ok", f"Bond entry: {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% | TP: {BOND_TP}%")
    log("ok", f"Spike: {SPIKE_MIN_AGE_H}h+ dormant, {SPIKE_MIN_1H}%+ 1h move")
    with capital_lock:
        _sp, _ = _cap_tier(capital)
    log("ok", f"Trade size: ~{_sp*100:.0f}% of capital (min ${MIN_TRADE} max ${MAX_TRADE})")
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
                if daily_limit_reached():
                    _log_scan(coin.get("symbol","?"), coin.get("mint",""), coin.get("bond_pct",0), 0, "cap", -1, "DAILY CAP / COOLDOWN")
                    break
                mint      = coin["mint"]
                symbol    = coin["symbol"]
                _bond_pre = coin.get("bond_pct", 0)
                _sig_pre  = sum([bool(coin.get("twitter")), bool(coin.get("telegram")), bool(coin.get("website"))])

                if mint in blacklisted_mints:
                    continue
                with trades_lock:
                    if mint in open_trades:
                        continue

                # Require Twitter, Telegram, or Website (at least one social signal)
                if not coin.get("twitter") and not coin.get("telegram") and not coin.get("website"):
                    _log_scan(symbol, mint, _bond_pre, 0, "social", 0, "NO SOCIAL LINKS")
                    continue
                n_social += 1

                # Active trading: last trade within 15 minutes
                # If last_trade==0 (field missing/changed), don't gate — coins already sorted by recency
                last_trade = coin.get("last_trade", 0)
                if last_trade > 0:
                    secs_since = time.time() - last_trade / 1000
                    if secs_since > 900:
                        _log_scan(symbol, mint, _bond_pre, _sig_pre, "active", 1, "LAST TRADE >15MIN")
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
                        holder_ok, holder_reason = check_holder_concentration(mint)
                        if not holder_ok:
                            log("warn", f"SKIP: {holder_reason}", symbol)
                            continue
                        dev_wallet = coin.get("creator", "") or coin.get("dev", "")
                        dev_ok, dev_reason = check_dev_history(dev_wallet)
                        if not dev_ok:
                            log("warn", f"SKIP: {dev_reason}", symbol)
                            continue
                        if gmgn_smart_money_selling(mint):
                            log("warn", "SKIP: smart money selling", symbol)
                            continue
                        sig_score = gmgn_signal_score(mint)
                        market = get_market_data(mint)
                        if market and market["price"] > 0 and market["liq"] >= MIN_LIQ:
                            amt = trade_size()
                            log("ok", f"BUNDLE RIDE | bond={bond:.1f}% | sig={sig_score}", symbol)
                            enter_trade(mint, symbol, market["price"], amt, "bundle", bond, 0, pump_swap=coin.get("pump_swap", False))
                            time.sleep(0.5)
                            continue

                # ── Bond Runner ────────────────────────────────────────
                if BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX:
                    details = get_bonding_details(mint)
                    if details:
                        bond = details["bond_pct"]
                        if details.get("complete"):
                            log("info", f"BOND SKIP: already graduated bond={bond:.1f}%", symbol)
                            _log_scan(symbol, mint, bond, _sig_pre, "bond", 2, "ALREADY GRADUATED")
                            continue
                    if not (BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX):
                        log("info", f"BOND SKIP: bond moved to {bond:.1f}% (range {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%)", symbol)
                        _log_scan(symbol, mint, bond, _sig_pre, "bond", 2, f"BOND {bond:.0f}% MOVED")
                        continue

                    rug = run_rugcheck(mint)
                    if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                        log("warn", f"BOND SKIP: mint/freeze auth rug={rug}", symbol)
                        blacklisted_mints.add(mint)
                        _log_scan(symbol, mint, bond, _sig_pre, "rug", 3, "RUG · MINT/FREEZE AUTH")
                        continue
                    if rug and rug.get("is_bundled") and BUNDLE_MODE == "avoid":
                        log("warn", f"BOND SKIP: bundled", symbol)
                        _log_scan(symbol, mint, bond, _sig_pre, "rug", 3, "BUNDLED TOKEN")
                        continue
                    holder_ok, holder_reason = check_holder_concentration(mint)
                    if not holder_ok:
                        log("warn", f"SKIP: {holder_reason}", symbol)
                        _log_scan(symbol, mint, bond, _sig_pre, "holder", 4, holder_reason[:18].upper())
                        continue
                    dev_wallet = coin.get("creator", "") or coin.get("dev", "")
                    dev_ok, dev_reason = check_dev_history(dev_wallet)
                    if not dev_ok:
                        log("warn", f"SKIP: {dev_reason}", symbol)
                        _log_scan(symbol, mint, bond, _sig_pre, "dev", 5, dev_reason[:18].upper())
                        continue
                    if gmgn_smart_money_selling(mint):
                        log("warn", "SKIP: smart money selling", symbol)
                        _log_scan(symbol, mint, bond, _sig_pre, "sm", 6, "SMART $ SELLING")
                        continue
                    sig_score = gmgn_signal_score(mint)

                    market = get_market_data(mint)
                    # Fallback: price from bonding curve when DexScreener hasn't indexed yet
                    if not market or market["price"] <= 0:
                        _vsol = coin.get("vsol", 0)
                        _vtok = coin.get("vtok", 0)
                        _sp   = get_sol_price()
                        if _vsol > 0 and _vtok > 0 and _sp:
                            bc_price = (_vsol / _vtok) * _sp
                            market = {"price": bc_price, "liq": 0, "change1h": 0, "age_h": 0}
                            log("info", f"BOND: using bonding curve price ${bc_price:.8f}", symbol)
                        else:
                            continue
                    # Skip liquidity check for bond runner — bonding curve IS the liquidity

                    amt = trade_size()
                    log("ok", f"BOND RUNNER | bond={bond:.1f}% | sig={sig_score}", symbol)
                    enter_trade(mint, symbol, market["price"], amt, "bond", bond, 0, pump_swap=coin.get("pump_swap", False))
                    time.sleep(0.5)
                    continue

                # ── Trench (near-graduation) ────────────────────────────
                # Coins 85-97% bonded are about to migrate — pre-graduation pump
                if TRENCH_ENTRY_MIN <= bond <= TRENCH_ENTRY_MAX:
                    rug = run_rugcheck(mint)
                    if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                        blacklisted_mints.add(mint)
                        continue
                    if rug and rug.get("is_bundled") and BUNDLE_MODE == "avoid":
                        continue
                    if gmgn_smart_money_selling(mint):
                        _log_scan(symbol, mint, bond, _sig_pre, "sm", 6, "SMART $ SELLING")
                        continue
                    sig_score = gmgn_signal_score(mint)
                    market = get_market_data(mint)
                    if not market or market["price"] <= 0:
                        _vsol = coin.get("vsol", 0)
                        _vtok = coin.get("vtok", 0)
                        _sp   = get_sol_price()
                        if _vsol > 0 and _vtok > 0 and _sp:
                            bc_price = (_vsol / _vtok) * _sp
                            market = {"price": bc_price, "liq": 0, "change1h": 0, "age_h": 0}
                        else:
                            continue
                    amt = trade_size()
                    log("ok", f"TRENCH | bond={bond:.1f}% | sig={sig_score}", symbol)
                    enter_trade(mint, symbol, market["price"], amt, "trench", bond, 0, pump_swap=coin.get("pump_swap", False))
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
                        holder_ok, holder_reason = check_holder_concentration(mint)
                        if not holder_ok:
                            log("warn", f"SKIP: {holder_reason}", symbol)
                            continue
                        dev_wallet = coin.get("creator", "") or coin.get("dev", "")
                        dev_ok, dev_reason = check_dev_history(dev_wallet)
                        if not dev_ok:
                            log("warn", f"SKIP: {dev_reason}", symbol)
                            continue
                        if gmgn_smart_money_selling(mint):
                            log("warn", "SKIP: smart money selling", symbol)
                            continue
                        sig_score = gmgn_signal_score(mint)
                        amt = trade_size()
                        log("ok", f"DORMANT SPIKE | age={age_h:.1f}h 1h={market['change1h']:+.0f}% | sig={sig_score}", symbol)
                        enter_trade(mint, symbol, market["price"], amt, "spike", bond, 0, pump_swap=coin.get("pump_swap", False))
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

            # ── Migration Bounce Scan ────────────────────────────────
            # Coins that just graduated to Raydium — enter in the first 2 min window
            grad_coins = get_recently_graduated()
            for gc in grad_coins:
                gmint = gc["mint"]
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN:
                        break
                    if gmint in open_trades:
                        continue
                with _copy_lock:
                    if gmint in _copied_mints:
                        continue
                if gmint in blacklisted_mints or daily_limit_reached():
                    continue
                market = get_market_data(gmint)
                if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                    continue
                rug = run_rugcheck(gmint)
                if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                    blacklisted_mints.add(gmint)
                    continue
                if gmgn_smart_money_selling(gmint):
                    continue
                dev_ok, dev_reason = check_dev_history(gc.get("dev", ""))
                if not dev_ok:
                    log("warn", f"SKIP: {dev_reason}", gc["symbol"])
                    continue
                with _copy_lock:
                    _copied_mints[gmint] = time.time()
                sig_score = gmgn_signal_score(gmint)
                amt = trade_size()
                log("ok", f"MIGRATION | {gc['grad_age']}s ago | liq=${market['liq']:.0f} | sig={sig_score}", gc["symbol"])
                notify(f"🚀 MIGRATION {gc['symbol']}",
                       f"Just graduated to Raydium!\nAge: {gc['grad_age']}s\nLiq: ${market['liq']:.0f}\nAmount: ${amt:.2f}")
                enter_trade(gmint, gc["symbol"], market["price"], amt, "migrate", 0, 0, raydium=True)
                time.sleep(0.5)

            # ── GMGN Signal Scan ─────────────────────────────────────
            # Enter coins GMGN flags as hot (trending, hot search, SM buy, KOL)
            # even if they aren't in pump.fun's live top-50
            with _signal_lock:
                signal_mints = list(
                    (_gmgn_sm_signal_mints | _gmgn_surge_mints | _gmgn_kol_mints
                     | _gmgn_trending_mints | _gmgn_hot_mints) - blacklisted_mints
                )
            n_signal_entered = 0
            for sig_mint in signal_mints:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN:
                        break
                    if sig_mint in open_trades:
                        continue
                with _copy_lock:
                    if sig_mint in _copied_mints:
                        continue
                if daily_limit_reached():
                    break
                market = get_market_data(sig_mint)
                if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                    continue
                rug = run_rugcheck(sig_mint)
                if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                    blacklisted_mints.add(sig_mint)
                    continue
                if rug and rug.get("is_bundled") and BUNDLE_MODE == "avoid":
                    continue
                if gmgn_smart_money_selling(sig_mint):
                    continue
                with _copy_lock:
                    _copied_mints[sig_mint] = time.time()
                sig_score = gmgn_signal_score(sig_mint)
                sig_sym   = sig_mint[:8]
                amt       = trade_size()
                log("ok", f"GMGN SIGNAL | liq=${market['liq']:.0f} | sig={sig_score}", sig_sym)
                notify(f"📡 SIGNAL {sig_sym}", f"GMGN signal entry\nLiq: ${market['liq']:.0f}\nSig score: {sig_score}\nAmount: ${amt:.2f}")
                enter_trade(sig_mint, sig_sym, market["price"], amt, "copy", 0, 0)
                n_signal_entered += 1
                time.sleep(0.5)
            if n_signal_entered:
                log("info", f"GMGN signal scan: entered {n_signal_entered} | pool={len(signal_mints)}")

        except Exception as e:
            log("err", f"Scanner: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ENDPOINTS ───────────────────────────────────────────────

_CURSOR = """<style>
@media(pointer:fine){
  body{
    background-image:linear-gradient(rgba(10,0,8,.62),rgba(10,0,8,.62)),
      url('/static/tankgirl.png')!important;
    background-size:cover!important;background-position:center!important;
    background-attachment:fixed!important;
  }
  .bg-art{display:none!important}
  .wrap{z-index:auto!important}
  *{cursor:none!important}
  #dora-cur{position:fixed;pointer-events:none;z-index:99999;
    width:69px;height:96px;image-rendering:pixelated;
    top:-200px;left:-200px}
}
</style>
<script>
(function(){
  var c=document.createElement('img');
  c.id='dora-cur';
  c.src='/static/doraemon_walk.gif';
  c.style.cssText='position:fixed;pointer-events:none;z-index:99999;width:69px;height:96px;image-rendering:pixelated;top:-200px;left:-200px;display:none';
  document.body.appendChild(c);
  function move(x,y){c.style.display='block';c.style.left=(x-34)+'px';c.style.top=(y-96)+'px';}
  document.addEventListener('mousemove',function(e){move(e.clientX,e.clientY);});
  document.addEventListener('touchmove',function(e){
    var t=e.touches[0];move(t.clientX,t.clientY);
  },{passive:true});
  document.addEventListener('touchstart',function(e){
    var t=e.touches[0];move(t.clientX,t.clientY);
  },{passive:true});
})();
</script>"""

@app.after_request
def _inject_cursor(resp):
    if 'text/html' in resp.content_type:
        html = resp.get_data(as_text=True)
        if '</body>' in html:
            resp.set_data(html.replace('</body>', _CURSOR + '</body>', 1))
    return resp

@app.route("/", methods=["GET"])
def home():
    from flask import request as _req
    theme = _req.args.get("theme", "classic")
    with capital_lock:
        cap = capital
    with trades_lock:
        open_list = list(open_trades.values())
    with usdc_lock:
        locked = usdc_locked
    wins  = [t for t in completed_trades if t["pnl"] > 0]
    total = len(completed_trades)
    wr    = round(len(wins) / max(total, 1) * 100, 1)
    pnl   = sum(t["pnl"] for t in completed_trades)
    mode  = "PAPER" if PAPER_MODE else "LIVE"
    pct, limit = _cap_tier(cap)
    next_m = next((m for m in MILESTONES if m > cap), None)
    progress_pct = min(round(cap / max(next_m, 1) * 100, 1), 100) if next_m else 100

    cap_points = [{"day": d["date"][-5:], "cap": round(d["capital"], 2)} for d in _week_day_logs[-14:]]
    cap_points.append({"day": "Today", "cap": round(cap, 2)})
    cap_json = json.dumps(cap_points)

    recent = list(reversed(completed_trades[-20:]))
    rows = ""
    for t in recent:
        color = "#4ade80" if t["pnl"] >= 0 else "#f87171"
        icon  = "▲" if t["pnl"] >= 0 else "▼"
        sign  = "+" if t["pnl"] >= 0 else ""
        rows += (f'<tr>'
                 f'<td><span class="badge badge-strategy">{t["strategy"].upper()}</span></td>'
                 f'<td class="sym">{t["symbol"]}</td>'
                 f'<td style="color:{color};font-weight:700">{icon} {sign}${t["pnl"]:.4f}</td>'
                 f'<td><span class="badge">{t["result"]}</span></td>'
                 f'<td class="muted">{t["hold_m"]:.1f}m</td>'
                 f'<td class="muted">{t["time"]}</td>'
                 f'</tr>')

    open_rows = ""
    for t in open_list:
        elapsed = round((time.time() - t["opened_at"]) / 60, 1)
        open_rows += (f'<tr>'
                      f'<td class="sym">{t["symbol"]}</td>'
                      f'<td><span class="badge badge-strategy">{t["strategy"].upper()}</span></td>'
                      f'<td class="gold">${t["amount"]:.2f}</td>'
                      f'<td class="muted">{t.get("bond_entry",0):.1f}%</td>'
                      f'<td class="muted pulse-text">{elapsed}m</td>'
                      f'</tr>')

    if theme == "punk":
        return _home_punk(cap, open_list, locked, wins, total, wr, pnl, mode,
                          pct, limit, next_m, progress_pct, cap_json, rows, open_rows)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>{BOT_NAME}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{
    --gold:#f5c542;--gold2:#e8a800;--bg:#080810;--surface:#10101a;
    --surface2:#16162a;--border:#ffffff0d;--text:#e8e8f0;--muted:#5a5a7a;
  }}
  body{{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;
    min-height:100vh;overflow-x:hidden}}

  /* animated starfield */
  body::before{{content:'';position:fixed;inset:0;
    background:radial-gradient(ellipse at 20% 50%,#1a0a3a22 0%,transparent 60%),
               radial-gradient(ellipse at 80% 20%,#0a1a3a22 0%,transparent 60%);
    pointer-events:none;z-index:0}}

  .wrap{{max-width:900px;margin:0 auto;padding:20px 16px;position:relative}}

  /* HEADER */
  header{{text-align:center;padding:16px 0 24px;position:relative}}
  .vid-wrap{{position:relative;width:100%;margin:0 auto -8px;line-height:0}}
  .vid-wrap video{{width:100%;display:block;mix-blend-mode:screen}}
  .vid-wrap a{{display:block;cursor:pointer}}
  .chest-icon{{font-size:2.8rem;display:block;margin-bottom:8px;
    filter:drop-shadow(0 0 20px #f5c54288)}}
  h1{{font-size:1.9rem;font-weight:800;letter-spacing:-.02em;
    background:linear-gradient(135deg,var(--gold) 0%,#fff 50%,var(--gold2) 100%);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
  .tagline{{color:var(--muted);font-size:.8rem;margin-top:6px;letter-spacing:.08em;text-transform:uppercase}}
  .mode-pill{{display:inline-flex;align-items:center;gap:6px;padding:4px 14px;
    border-radius:999px;font-size:.72rem;font-weight:700;letter-spacing:.05em;
    margin-top:12px;border:1px solid;
    {'background:#f5c54218;color:#f5c542;border-color:#f5c54244' if PAPER_MODE else 'background:#4ade8018;color:#4ade80;border-color:#4ade8044'}}}
  .dot{{width:6px;height:6px;border-radius:50%;
    background:{'#f5c542' if PAPER_MODE else '#4ade80'};
    box-shadow:0 0 6px {'#f5c542' if PAPER_MODE else '#4ade80'};
    animation:pulse 2s infinite}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}

  /* NAV LINKS */
  nav{{display:flex;justify-content:center;flex-wrap:wrap;gap:8px;margin:20px 0 28px}}
  nav a{{color:var(--muted);font-size:.78rem;font-weight:500;text-decoration:none;
    padding:6px 14px;border-radius:8px;border:1px solid var(--border);
    background:var(--surface);transition:all .2s;letter-spacing:.03em}}
  nav a:hover{{color:var(--gold);border-color:#f5c54244;background:#f5c54210}}

  /* STAT CARDS */
  .cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:20px}}
  @media(max-width:540px){{.cards{{grid-template-columns:1fr 1fr}}}}
  .card{{background:var(--surface);border:1px solid var(--border);border-radius:16px;
    padding:16px 14px;position:relative;overflow:hidden;transition:transform .2s}}
  .card:hover{{transform:translateY(-2px)}}
  .card::before{{content:'';position:absolute;inset:0;
    background:linear-gradient(135deg,#ffffff04 0%,transparent 100%);pointer-events:none}}
  .card .lbl{{font-size:.68rem;font-weight:600;color:var(--muted);
    text-transform:uppercase;letter-spacing:.08em}}
  .card .val{{font-size:1.55rem;font-weight:800;margin-top:5px;line-height:1}}
  .card .sub{{font-size:.7rem;color:var(--muted);margin-top:5px}}
  .card.gold-card{{border-color:#f5c54230;background:linear-gradient(135deg,#1a140a,#10101a)}}
  .gold{{color:var(--gold)}}
  .green{{color:#4ade80}}
  .red{{color:#f87171}}
  .blue{{color:#60a5fa}}
  .muted{{color:var(--muted)}}

  /* GLOW CARD */
  .card.glow::after{{content:'';position:absolute;inset:-1px;border-radius:16px;
    background:linear-gradient(135deg,#f5c54240,transparent,#f5c54220);
    -webkit-mask:linear-gradient(#fff 0 0) content-box,linear-gradient(#fff 0 0);
    -webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none}}

  /* SECTIONS */
  .section{{background:var(--surface);border:1px solid var(--border);
    border-radius:16px;padding:18px;margin-bottom:14px}}
  .section-hdr{{display:flex;align-items:center;justify-content:space-between;
    margin-bottom:14px}}
  .section-hdr h2{{font-size:.72rem;font-weight:700;color:var(--muted);
    text-transform:uppercase;letter-spacing:.1em}}
  .section-hdr a{{font-size:.7rem;color:var(--gold);text-decoration:none;
    padding:4px 10px;border-radius:6px;border:1px solid #f5c54230;
    background:#f5c54210;transition:all .2s}}
  .section-hdr a:hover{{background:#f5c54220}}

  /* PROGRESS */
  .prog-labels{{display:flex;justify-content:space-between;
    font-size:.72rem;color:var(--muted);margin-bottom:8px}}
  .prog-track{{background:#ffffff08;border-radius:999px;height:10px;overflow:hidden}}
  .prog-fill{{height:10px;border-radius:999px;
    background:linear-gradient(90deg,var(--gold2),var(--gold),#fff8dc);
    width:{progress_pct}%;box-shadow:0 0 12px #f5c54266;
    transition:width 1s cubic-bezier(.4,0,.2,1)}}
  .milestones{{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px}}
  .ms{{font-size:.68rem;padding:3px 10px;border-radius:6px;border:1px solid var(--border);
    color:var(--muted);background:var(--surface2)}}
  .ms.hit{{color:var(--gold);border-color:#f5c54240;background:#f5c54210}}

  /* TABLE */
  table{{width:100%;border-collapse:collapse;font-size:.78rem}}
  thead tr{{border-bottom:1px solid var(--border)}}
  th{{padding:8px 10px;color:var(--muted);font-weight:600;
    font-size:.68rem;text-transform:uppercase;letter-spacing:.06em;text-align:left}}
  td{{padding:9px 10px;border-bottom:1px solid #ffffff06}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#ffffff04}}
  .sym{{font-weight:700;font-size:.82rem;letter-spacing:.02em}}
  .badge{{display:inline-block;padding:2px 7px;border-radius:5px;font-size:.64rem;
    font-weight:700;letter-spacing:.04em;background:#ffffff0a;color:var(--muted);border:1px solid var(--border)}}
  .badge-strategy{{background:#60a5fa18;color:#60a5fa;border-color:#60a5fa30}}

  /* CHART */
  .chart-wrap{{position:relative;height:160px}}

  /* FOOTER */
  footer{{text-align:center;padding:24px 0 8px;color:var(--muted);font-size:.72rem}}

  /* ACTION BUTTONS */
  .actions{{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px}}
  .btn{{padding:10px 18px;border-radius:10px;font-size:.78rem;font-weight:600;
    text-decoration:none;border:1px solid;cursor:pointer;transition:all .2s;
    display:inline-flex;align-items:center;gap:6px;letter-spacing:.02em}}
  .btn-gold{{background:linear-gradient(135deg,#f5c542,#e8a800);
    color:#000;border-color:#f5c542;box-shadow:0 0 20px #f5c54240}}
  .btn-gold:hover{{box-shadow:0 0 30px #f5c54260;transform:translateY(-1px)}}
  .btn-ghost{{background:#ffffff08;color:var(--text);border-color:var(--border)}}
  .btn-ghost:hover{{background:#ffffff12;border-color:#ffffff20}}

  .pulse-text{{animation:pulse 2s infinite}}
  .empty{{text-align:center;padding:28px;color:var(--muted);font-size:.82rem}}
  .bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;object-position:center;opacity:.35;pointer-events:none;z-index:0}}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<div class="wrap">

  <header>
    <div class="vid-wrap">
      <a href="/"><video autoplay loop muted playsinline>
        <source src="/static/boogeys_pot.webm" type="video/webm">
        <source src="/static/header.mp4" type="video/mp4">
      </video></a>
    </div>
    <p class="tagline">Autonomous pump.fun sniper &nbsp;·&nbsp; goal ${PROFIT_GOAL:,.0f}</p>
    <div class="mode-pill"><span class="dot"></span>{mode} MODE</div>
  </header>

  <nav>
    <a href="/live">⚡ Live Feed</a>
    <a href="/trades">📋 All Trades</a>
    <a href="/status">📊 Status</a>
    <a href="/learn">🧠 Strategy</a>
    <a href="/setup">⚙️ Setup</a>
    <a href="https://pump.fun" target="_blank">🚀 Pump.fun</a>
    <a href="https://solscan.io" target="_blank">🔍 Solscan</a>
  </nav>

  <div class="actions">
    <a href="/" class="btn btn-gold">⚡ Refresh Now</a>
    <a href="/live" class="btn btn-ghost">📡 Live Feed</a>
    <a href="/trades" class="btn btn-ghost">💼 All Trades</a>
  </div>

  <div class="cards">
    <div class="card gold-card glow">
      <div class="lbl">Capital</div>
      <div class="val gold">${cap:.2f}</div>
      <div class="sub">Started at ${STARTING_CAPITAL:.2f}</div>
    </div>
    <div class="card">
      <div class="lbl">Total PnL</div>
      <div class="val {'green' if pnl>=0 else 'red'}">{'+' if pnl>=0 else ''}${pnl:.2f}</div>
      <div class="sub">{total} trades closed</div>
    </div>
    <div class="card">
      <div class="lbl">Win Rate</div>
      <div class="val {'green' if wr>=50 else 'red'}">{wr}%</div>
      <div class="sub">{len(wins)}W &nbsp;/&nbsp; {total-len(wins)}L</div>
    </div>
    <div class="card">
      <div class="lbl">Trade Size</div>
      <div class="val blue">${trade_size():.2f}</div>
      <div class="sub">{pct*100:.0f}% tier · {_daily_trades}/{limit} today</div>
    </div>
    <div class="card">
      <div class="lbl">Open Trades</div>
      <div class="val {'green' if open_list else ''}">{len(open_list)}<span style="font-size:1rem;font-weight:500;color:var(--muted)">/{MAX_OPEN}</span></div>
      <div class="sub">{'🟢 Active' if open_list else '🔍 Scanning...'}</div>
    </div>
    <div class="card">
      <div class="lbl">USDC Locked</div>
      <div class="val blue">${locked:.2f}</div>
      <div class="sub">Profit secured</div>
    </div>
  </div>

  <div class="section">
    <div class="section-hdr">
      <h2>🏆 Milestone Progress</h2>
      <span style="font-size:.72rem;color:var(--muted)">${cap:.2f} → ${next_m:,}</span>
    </div>
    <div class="prog-labels"><span>${cap:.2f}</span><span>${next_m:,}</span></div>
    <div class="prog-track"><div class="prog-fill"></div></div>
    <div class="milestones">
      {''.join(f'<span class="ms{" hit" if cap >= m else ""}">${m:,}</span>' for m in MILESTONES)}
    </div>
  </div>

  <div class="section">
    <div class="section-hdr">
      <h2>📈 Capital Growth</h2>
      <a href="/status">Full Data →</a>
    </div>
    <div class="chart-wrap"><canvas id="capChart"></canvas></div>
  </div>

  {"" if not open_list else f'''
  <div class="section">
    <div class="section-hdr"><h2>⚡ Open Trades ({len(open_list)})</h2></div>
    <table>
      <thead><tr><th>Symbol</th><th>Strategy</th><th>Size</th><th>Bond In</th><th>Held</th></tr></thead>
      <tbody>{open_rows}</tbody>
    </table>
  </div>'''}

  <div class="section">
    <div class="section-hdr">
      <h2>📋 Recent Trades</h2>
      <a href="/trades">View All →</a>
    </div>
    <table>
      <thead><tr><th>Strategy</th><th>Symbol</th><th>PnL</th><th>Exit</th><th>Hold</th><th>Time</th></tr></thead>
      <tbody>{rows if rows else '<tr><td colspan="6" class="empty">No trades yet — bot is scanning...</td></tr>'}</tbody>
    </table>
  </div>

  <footer>
    Auto-refreshes every 30s &nbsp;·&nbsp; Retuning every {ANALYZE_EVERY} trades &nbsp;·&nbsp;
    Built by Boogey &nbsp;·&nbsp;
    <a href="https://github.com/BoogeyBlues/bot.py" target="_blank" style="color:var(--gold);text-decoration:none">GitHub ↗</a>
  </footer>

</div>
<script>
const data = {cap_json};
new Chart(document.getElementById('capChart'), {{
  type:'line',
  data:{{
    labels: data.map(d=>d.day),
    datasets:[{{
      data: data.map(d=>d.cap),
      borderColor:'#f5c542',
      backgroundColor:'rgba(245,197,66,0.08)',
      fill:true,tension:0.45,
      pointRadius:4,pointHoverRadius:6,
      pointBackgroundColor:'#f5c542',
      pointBorderColor:'#080810',pointBorderWidth:2,
    }}]
  }},
  options:{{
    responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{display:false}},tooltip:{{
      backgroundColor:'#16162a',borderColor:'#f5c54240',borderWidth:1,
      titleColor:'#f5c542',bodyColor:'#e8e8f0',
      callbacks:{{label:ctx=>' $'+ctx.parsed.y.toFixed(2)}}
    }}}},
    scales:{{
      x:{{grid:{{color:'#ffffff06'}},ticks:{{color:'#5a5a7a',font:{{size:10}}}}}},
      y:{{grid:{{color:'#ffffff06'}},ticks:{{color:'#5a5a7a',font:{{size:10}},
        callback:v=>'$'+v}}}}
    }}
  }}
}});
</script>
</body></html>"""
    return html, 200


def _home_punk(cap, open_list, locked, wins, total, wr, pnl, mode,
               pct, limit, next_m, progress_pct, cap_json, rows, open_rows):
    sign      = "+" if pnl >= 0 else ""
    pnl_color = "#39ff14" if pnl >= 0 else "#ff006e"
    wr_color  = "#39ff14" if wr >= 50 else ("#ffee00" if wr >= 35 else "#ff006e")
    mode_color= "#ffee00" if mode == "PAPER" else "#39ff14"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<meta http-equiv="refresh" content="30">
<title>{BOT_NAME}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{--pink:#ff006e;--cyan:#00f5ff;--yellow:#ffee00;--green:#39ff14;
    --bg:#0a0008;--card:#110010;--border:#ffffff15}}
  body{{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;
    max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}}
  .vid-wrap{{width:100%;line-height:0;position:relative}}
  .vid-wrap video{{width:100%;display:block;mix-blend-mode:screen}}
  .vid-wrap a{{display:block;cursor:pointer}}
  .vid-wrap::after{{content:'';position:absolute;bottom:0;left:0;right:0;height:40px;
    background:linear-gradient(transparent,var(--bg))}}
  nav{{display:flex;gap:0;border-bottom:2px solid var(--pink);overflow-x:auto;
    scrollbar-width:none}}
  nav::-webkit-scrollbar{{display:none}}
  nav a{{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;
    padding:10px 14px;white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;
    border-right:1px solid var(--border);transition:all .15s}}
  nav a:hover{{background:var(--pink);color:#000}}
  .mode-strip{{background:var(--card);border-bottom:2px solid var(--yellow);
    padding:6px 16px;display:flex;align-items:center;justify-content:space-between}}
  .mode-strip .left{{font-size:.62rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:#888}}
  .mode-pill{{font-size:.68rem;font-weight:900;padding:3px 12px;letter-spacing:.08em;background:{mode_color};color:#000}}
  .hero{{padding:20px 16px 16px;background:linear-gradient(180deg,#1a000f,var(--bg));
    border-bottom:3px solid var(--pink);text-align:center}}
  .hero-lbl{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.15em;margin-bottom:4px}}
  .hero-cap{{font-family:'Bebas Neue',sans-serif;font-size:4.2rem;letter-spacing:.02em;
    color:var(--yellow);text-shadow:0 0 30px #ffee0066,3px 3px 0 var(--pink);line-height:1}}
  .hero-pnl{{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:600;
    margin-top:8px;color:{pnl_color}}}
  .hero-sub{{font-size:.7rem;color:#888;margin-top:6px}}
  .grid{{display:grid;grid-template-columns:1fr 1fr;gap:2px;background:var(--pink);border:2px solid var(--pink)}}
  .stat{{background:var(--card);padding:14px 16px}}
  .stat .lbl{{font-size:.58rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
  .stat .val{{font-family:'Bebas Neue',sans-serif;font-size:2rem;margin-top:2px;line-height:1}}
  .stat .sub{{font-size:.62rem;color:#888;margin-top:3px}}
  .pink{{color:var(--pink)}} .cyan{{color:var(--cyan)}} .yellow{{color:var(--yellow)}}
  .green{{color:var(--green)}} .muted{{color:#888}}
  .prog-wrap{{padding:14px 16px;background:var(--card);border-bottom:2px solid var(--border)}}
  .prog-lbl{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;
    letter-spacing:.1em;margin-bottom:8px;display:flex;justify-content:space-between}}
  .prog-track{{background:#ffffff10;height:6px;overflow:hidden}}
  .prog-fill{{background:linear-gradient(90deg,var(--pink),var(--yellow),var(--cyan));
    height:6px;width:{progress_pct}%;box-shadow:0 0 10px var(--pink)}}
  .milestones{{display:flex;flex-wrap:wrap;gap:4px;margin-top:10px}}
  .ms{{font-size:.58rem;padding:3px 8px;font-weight:700;letter-spacing:.04em;
    border:1px solid #ffffff15;color:#666;background:#ffffff05}}
  .ms.hit{{color:var(--yellow);border-color:var(--yellow);background:#ffee0015;box-shadow:0 0 6px #ffee0040}}
  .chart-wrap{{padding:16px;background:var(--card);border-bottom:2px solid var(--border)}}
  .chart-hdr{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px}}
  .chart-inner{{position:relative;height:130px}}
  .section{{background:var(--card);border-bottom:2px solid var(--border)}}
  .section-hdr{{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--border)}}
  .section-hdr h2{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
  .section-hdr a{{font-size:.62rem;color:var(--cyan);text-decoration:none;font-weight:700;letter-spacing:.06em}}
  .tbl-wrap{{overflow-x:auto}}
  table{{width:100%;border-collapse:collapse;font-size:.72rem}}
  th{{padding:8px 10px;font-size:.56rem;font-weight:700;color:#888;text-transform:uppercase;
    letter-spacing:.08em;text-align:left;border-bottom:1px solid var(--border);white-space:nowrap;background:#0d000c}}
  td{{padding:9px 10px;border-bottom:1px solid #ffffff05;vertical-align:middle}}
  .sym{{font-weight:900;font-size:.8rem}}
  .mono{{font-family:'JetBrains Mono',monospace;font-size:.68rem}}
  .badge{{display:inline-block;padding:2px 6px;font-size:.56rem;font-weight:900;letter-spacing:.06em;border:1px solid}}
  .badge.win{{color:var(--green);border-color:var(--green);background:#39ff1415}}
  .badge.loss{{color:var(--pink);border-color:var(--pink);background:#ff006e15}}
  .badge.strat{{color:var(--cyan);border-color:var(--cyan);background:#00f5ff12}}
  .badge.exit{{color:#888;border-color:#444;background:#ffffff05}}
  .theme-bar{{padding:10px 16px;display:flex;align-items:center;justify-content:space-between;
    border-top:2px solid var(--border);background:var(--card)}}
  .theme-bar span{{font-size:.6rem;color:#888;font-weight:700;text-transform:uppercase;letter-spacing:.08em}}
  .theme-links{{display:flex;gap:6px}}
  .theme-links a{{font-size:.6rem;font-weight:700;padding:4px 10px;text-decoration:none;border:1px solid;letter-spacing:.06em}}
  .theme-links a.active{{background:var(--pink);color:#000;border-color:var(--pink)}}
  .theme-links a:not(.active){{color:#888;border-color:#555}}
  footer{{padding:14px 16px;text-align:center;font-size:.6rem;color:#444;border-top:1px solid var(--border)}}
  footer a{{color:var(--cyan);text-decoration:none}}
  .bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;object-position:center;opacity:.35;pointer-events:none;z-index:0}}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
  <div class="vid-wrap">
    <a href="/"><video autoplay loop muted playsinline>
      <source src="/static/boogeys_pot.webm" type="video/webm">
      <source src="/static/header.mp4" type="video/mp4">
    </video></a>
  </div>
  <nav>
    <a href="/live">⚡ LIVE</a>
    <a href="/trades">📋 TRADES</a>
    <a href="/status">📊 STATUS</a>
    <a href="/learn">🧠 STRAT</a>
    <a href="/setup">⚙️ SETUP</a>
    <a href="https://pump.fun" target="_blank">🚀 PUMP</a>
    <a href="https://solscan.io" target="_blank">🔍 SCAN</a>
  </nav>
  <div class="mode-strip">
    <span class="left">Refreshes 30s · Goal ${PROFIT_GOAL:,.0f}</span>
    <span class="mode-pill">{mode}</span>
  </div>
  <div class="hero">
    <div class="hero-lbl">Current Capital</div>
    <div class="hero-cap">${cap:.2f}</div>
    <div class="hero-pnl">{sign}${pnl:.2f} total PnL</div>
    <div class="hero-sub">Started ${STARTING_CAPITAL:.2f} &nbsp;·&nbsp; {total} trades closed</div>
  </div>
  <div class="grid">
    <div class="stat">
      <div class="lbl">Win Rate</div>
      <div class="val" style="color:{wr_color}">{wr}%</div>
      <div class="sub">{len(wins)}W / {total-len(wins)}L</div>
    </div>
    <div class="stat">
      <div class="lbl">Trade Size</div>
      <div class="val cyan">${trade_size():.2f}</div>
      <div class="sub">{pct*100:.0f}% tier</div>
    </div>
    <div class="stat">
      <div class="lbl">Open Now</div>
      <div class="val {'green' if open_list else 'yellow'}">{len(open_list)}<span style="font-size:1.1rem;color:#888">/{MAX_OPEN}</span></div>
      <div class="sub">{'active' if open_list else 'scanning'}</div>
    </div>
    <div class="stat">
      <div class="lbl">USDC Locked</div>
      <div class="val cyan">${locked:.2f}</div>
      <div class="sub">Secured</div>
    </div>
    <div class="stat">
      <div class="lbl">Today</div>
      <div class="val yellow">{_daily_trades}<span style="font-size:1.1rem;color:#888">/{limit}</span></div>
      <div class="sub">{_daily_wins}W {_daily_losses}L</div>
    </div>
    <div class="stat">
      <div class="lbl">Next Target</div>
      <div class="val pink">${next_m:,}</div>
      <div class="sub">{progress_pct}% there</div>
    </div>
  </div>
  <div class="prog-wrap">
    <div class="prog-lbl"><span>MILESTONE PROGRESS</span><span style="color:var(--yellow)">{progress_pct}%</span></div>
    <div class="prog-track"><div class="prog-fill"></div></div>
    <div class="milestones">
      {''.join(f'<span class="ms{" hit" if cap >= m else ""}">${m:,}</span>' for m in MILESTONES)}
    </div>
  </div>
  <div class="chart-wrap">
    <div class="chart-hdr">CAPITAL GROWTH</div>
    <div class="chart-inner"><canvas id="capChart"></canvas></div>
  </div>
  {"" if not open_list else f'''<div class="section">
    <div class="section-hdr"><h2>⚡ OPEN ({len(open_list)})</h2></div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Symbol</th><th>Strat</th><th>$</th><th>Bond</th><th>Held</th></tr></thead>
      <tbody>{open_rows}</tbody>
    </table></div>
  </div>'''}
  <div class="section">
    <div class="section-hdr"><h2>RECENT TRADES</h2><a href="/trades">ALL →</a></div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Strat</th><th>Symbol</th><th>PnL</th><th>Exit</th><th>Hold</th></tr></thead>
      <tbody>{rows if rows else '<tr><td colspan="5" style="text-align:center;padding:24px;color:#444">No trades yet</td></tr>'}</tbody>
    </table></div>
  </div>
  <div class="theme-bar">
    <span>Theme</span>
    <div class="theme-links">
      <a href="/?theme=punk" class="active">PUNK</a>
      <a href="/?theme=classic">CLASSIC</a>
    </div>
  </div>
  <footer>{BOT_NAME} &nbsp;·&nbsp;
    <a href="https://github.com/BoogeyBlues/bot.py" target="_blank">GitHub ↗</a>
  </footer>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script>
const data = {cap_json};
new Chart(document.getElementById('capChart'),{{
  type:'line',
  data:{{labels:data.map(d=>d.day),datasets:[{{
    data:data.map(d=>d.cap),borderColor:'#ff006e',
    backgroundColor:'rgba(255,0,110,0.08)',fill:true,tension:0.45,
    pointRadius:3,pointBackgroundColor:'#ffee00',
    pointBorderColor:'#0a0008',pointBorderWidth:2
  }}]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{display:false}},tooltip:{{backgroundColor:'#110010',
      borderColor:'#ff006e',borderWidth:1,titleColor:'#ffee00',bodyColor:'#fff',
      callbacks:{{label:ctx=>' $'+ctx.parsed.y.toFixed(2)}}}}}},
    scales:{{
      x:{{grid:{{color:'#ffffff08'}},ticks:{{color:'#666',font:{{size:9}}}}}},
      y:{{grid:{{color:'#ffffff08'}},ticks:{{color:'#666',font:{{size:9}},callback:v=>'$'+v}}}}
    }}
  }}
}});
</script>
</body></html>"""
    return html, 200


@app.route("/status/api", methods=["GET"])
def status_api():
    wins  = [t for t in completed_trades if t["pnl"] > 0]
    total = len(completed_trades)
    pnl   = sum(t["pnl"] for t in completed_trades)
    with capital_lock:
        cap = capital
    sol_price = get_sol_price()
    next_tune_in = ANALYZE_EVERY - (total % ANALYZE_EVERY) if total > 0 else ANALYZE_EVERY
    return jsonify({
        "capital": round(cap, 2), "paper_mode": PAPER_MODE,
        "open_trades": len(open_trades), "total_trades": total,
        "wins": len(wins), "losses": total - len(wins),
        "win_rate": round(len(wins) / max(total, 1) * 100, 1),
        "total_pnl": round(pnl, 4),
        "sol_price": round(sol_price, 2) if sol_price else None,
        "next_tune_in": next_tune_in,
        "today": {"trades": _daily_trades, "wins": _daily_wins, "losses": _daily_losses,
                  "paused_until": time.strftime("%H:%M", time.localtime(_pause_until)) if _pause_until > time.time() else None},
    })

@app.route("/status", methods=["GET"])
def status():
    wins   = [t for t in completed_trades if t["pnl"] > 0]
    losses = [t for t in completed_trades if t["pnl"] <= 0]
    total  = len(completed_trades)
    pnl    = sum(t["pnl"] for t in completed_trades)
    with capital_lock:
        cap = capital
    wr = round(len(wins) / max(total, 1) * 100, 1)
    pct, limit = _cap_tier(cap)
    next_m = next((m for m in MILESTONES if m > cap), None)
    progress_pct = min(round(cap / max(next_m, 1) * 100, 1), 100) if next_m else 100
    paused = _pause_until > time.time()
    daily_loss_pct = round((_day_start_cap - cap) / max(_day_start_cap, 1) * 100, 1) if _day_start_cap > 0 else 0

    if not scan_active:
        health = ("#f87171", "🔴", "HALTED", "Capital too low — scanner stopped")
    elif paused:
        resume = time.strftime("%H:%M", time.localtime(_pause_until))
        health = ("#fbbf24", "🟡", "COOLING DOWN", f"Retuning strategies — resumes {resume}")
    elif wr < 35 and total >= 10:
        health = ("#f87171", "🔴", "LOW WIN RATE", f"{wr}% — auto-tuning in progress")
    elif wr < 50 and total >= 10:
        health = ("#fbbf24", "🟡", "CAUTION", f"{wr}% win rate — watching closely")
    elif PAPER_MODE:
        health = ("#fbbf24", "🟡", "PAPER MODE", "Simulated trades — no real funds at risk")
    else:
        health = ("#4ade80", "🟢", "LIVE & HEALTHY", "Scanning and trading normally")

    def c3(val, g, y):
        return "#4ade80" if val <= g else ("#fbbf24" if val <= y else "#f87171")

    wr_c   = "#4ade80" if wr >= 55 else ("#fbbf24" if wr >= 40 else "#f87171")
    pnl_c  = "#4ade80" if pnl >= 0 else "#f87171"
    dl_c   = c3(daily_loss_pct, 10, 18)
    scan_c = "#4ade80" if (not paused and scan_active) else ("#fbbf24" if paused else "#f87171")
    scan_t = "SCANNING" if (not paused and scan_active) else ("PAUSED" if paused else "HALTED")

    day_rows = ""
    for d in reversed(_week_day_logs[-7:]):
        dwr = round(d.get("wins", 0) / max(d.get("trades", 1), 1) * 100, 1)
        dc  = "#4ade80" if dwr >= 50 else ("#fbbf24" if dwr >= 35 else "#f87171")
        cap_c = "#4ade80" if d["capital"] >= STARTING_CAPITAL else "#f87171"
        day_rows += (f'<tr><td>{d["date"]}</td>'
                     f'<td class="mono">{d.get("trades",0)}</td>'
                     f'<td class="mono green">{d.get("wins",0)}W</td>'
                     f'<td class="mono red">{d.get("losses",0)}L</td>'
                     f'<td class="mono" style="color:{dc}">{dwr}%</td>'
                     f'<td class="mono" style="color:{cap_c}">${d["capital"]:.2f}</td></tr>')

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<meta http-equiv="refresh" content="20">
<title>Status — {BOT_NAME}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{--acc:#ffee00;--bg:#0a0008;--card:#110010;--border:#ffffff15}}
  body{{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}}
  .bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;object-position:center;opacity:.35;pointer-events:none;z-index:0}}
  .wrap{{position:relative}}
  nav{{display:flex;gap:0;border-bottom:2px solid var(--acc);overflow-x:auto;scrollbar-width:none}}
  nav::-webkit-scrollbar{{display:none}}
  nav a{{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;padding:10px 14px;white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;border-right:1px solid var(--border);transition:all .15s}}
  nav a:hover{{background:var(--acc);color:#000}}
  nav a.active{{background:var(--acc);color:#000}}
  .page-title{{font-family:'Bebas Neue',sans-serif;font-size:3rem;color:var(--acc);text-shadow:0 0 24px #ffee0088;padding:18px 16px 8px;line-height:1;letter-spacing:.04em}}
  .health-banner{{margin:0 12px 12px;padding:16px;border-left:4px solid {health[0]};background:{health[0]}10;display:flex;align-items:center;gap:12px}}
  .health-icon{{font-size:1.8rem}}
  .health-title{{font-size:1rem;font-weight:900;color:{health[0]};letter-spacing:.04em;text-transform:uppercase}}
  .health-sub{{font-size:.68rem;color:#aaa;margin-top:2px}}
  .health-dot{{width:10px;height:10px;border-radius:50%;background:{health[0]};box-shadow:0 0 10px {health[0]};margin-left:auto;animation:pulse 1.5s infinite;flex-shrink:0}}
  @keyframes pulse{{0%,100%{{opacity:1;transform:scale(1)}}50%{{opacity:.5;transform:scale(.8)}}}}
  .grid{{display:grid;grid-template-columns:1fr 1fr;gap:2px;background:var(--acc);border:2px solid var(--acc);margin:0 12px 12px}}
  .stat{{background:var(--card);padding:14px 16px}}
  .stat .lbl{{font-size:.58rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
  .stat .val{{font-family:'Bebas Neue',sans-serif;font-size:2rem;margin-top:2px;line-height:1;color:var(--acc)}}
  .stat .sub{{font-size:.62rem;color:#888;margin-top:3px}}
  .section{{background:var(--card);border-top:2px solid var(--border);padding:14px;margin:0 12px 12px}}
  .section-hdr{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px}}
  .prog-track{{background:#ffffff10;height:6px;overflow:hidden;margin:8px 0 10px}}
  .prog-fill{{background:linear-gradient(90deg,var(--acc),#fff);height:6px;width:{progress_pct}%;box-shadow:0 0 8px var(--acc)}}
  .milestones{{display:flex;flex-wrap:wrap;gap:4px}}
  .ms{{font-size:.58rem;padding:3px 8px;font-weight:700;letter-spacing:.04em;border:1px solid #ffffff15;color:#666;background:#ffffff05}}
  .ms.hit{{color:var(--acc);border-color:var(--acc);background:#ffee0015;box-shadow:0 0 6px #ffee0040}}
  .row{{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:1px solid var(--border);font-size:.78rem}}
  .row:last-child{{border-bottom:none}}
  .row-key{{color:#888;font-size:.7rem}}
  .row-val{{font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:600;color:var(--acc)}}
  .table-wrap{{overflow-x:auto;border:1px solid var(--border)}}
  table{{width:100%;border-collapse:collapse;font-size:.72rem}}
  th{{padding:8px 10px;font-size:.56rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.08em;text-align:left;border-bottom:1px solid var(--border);background:#0d000c}}
  td{{padding:8px 10px;border-bottom:1px solid #ffffff05}}
  .mono{{font-family:'JetBrains Mono',monospace;font-size:.68rem}}
  .green{{color:#39ff14}} .red{{color:#ff006e}}
  footer{{padding:14px 16px;text-align:center;font-size:.6rem;color:#444;border-top:1px solid var(--border)}}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<div class="wrap">
  <nav>
    <a href="/">HOME</a>
    <a href="/live">LIVE</a>
    <a href="/trades">TRADES</a>
    <a href="/status" class="active">STATUS</a>
    <a href="/learn">STRATEGY</a>
    <a href="/setup">SETUP</a>
  </nav>

  <div class="page-title">BOT STATUS</div>

  <div class="health-banner">
    <div class="health-icon">{health[1]}</div>
    <div>
      <div class="health-title">{health[2]}</div>
      <div class="health-sub">{health[3]} · Day {len(_week_day_logs)+1}</div>
    </div>
    <div class="health-dot"></div>
  </div>

  <div class="grid">
    <div class="stat">
      <div class="lbl">Capital</div>
      <div class="val">${cap:.2f}</div>
      <div class="sub">Started ${STARTING_CAPITAL:.2f}</div>
    </div>
    <div class="stat">
      <div class="lbl">Total PnL</div>
      <div class="val" style="color:{pnl_c}">{'+' if pnl>=0 else ''}${pnl:.4f}</div>
      <div class="sub">{total} trades closed</div>
    </div>
    <div class="stat">
      <div class="lbl">Win Rate</div>
      <div class="val" style="color:{wr_c}">{wr}%</div>
      <div class="sub">{len(wins)}W / {len(losses)}L</div>
    </div>
    <div class="stat">
      <div class="lbl">Scanner</div>
      <div class="val" style="color:{scan_c};font-size:1.4rem">{scan_t}</div>
      <div class="sub">{'Looking for entries' if scan_t=='SCANNING' else f'Resumes {time.strftime("%H:%M", time.localtime(_pause_until))}' if paused else 'Capital too low'}</div>
    </div>
    <div class="stat">
      <div class="lbl">Daily Drawdown</div>
      <div class="val" style="color:{dl_c}">{daily_loss_pct:.1f}%</div>
      <div class="sub">Max {MAX_DAILY_LOSS_PCT:.0f}%</div>
    </div>
    <div class="stat">
      <div class="lbl">Today Trades</div>
      <div class="val">{_daily_trades}<span style="font-size:1.1rem;color:#888">/{limit}</span></div>
      <div class="sub">{_daily_wins}W {_daily_losses}L</div>
    </div>
  </div>

  <div class="section">
    <div class="section-hdr">MILESTONE PROGRESS — next ${next_m:,} ({progress_pct}%)</div>
    <div class="prog-track"><div class="prog-fill"></div></div>
    <div class="milestones">
      {''.join(f'<span class="ms{" hit" if cap >= m else ""}">${m:,}</span>' for m in MILESTONES)}
    </div>
  </div>

  <div class="section">
    <div class="section-hdr">ACTIVE SETTINGS</div>
    <div class="row"><span class="row-key">Mode</span>
      <span class="row-val" style="color:{'#ffee00' if PAPER_MODE else '#39ff14'}">{'PAPER' if PAPER_MODE else 'LIVE'}</span></div>
    <div class="row"><span class="row-key">Trade Size</span>
      <span class="row-val">{pct*100:.0f}% = ${trade_size():.2f}</span></div>
    <div class="row"><span class="row-key">Daily Cap</span>
      <span class="row-val">{limit} trades/day</span></div>
    <div class="row"><span class="row-key">Max Daily Loss</span>
      <span class="row-val" style="color:#ff006e">{MAX_DAILY_LOSS_PCT:.0f}% of open capital</span></div>
    <div class="row"><span class="row-key">Loss Cooldown</span>
      <span class="row-val">{DAILY_LOSS_MAX} losses → {int(LOSS_COOLDOWN_HRS*60)}min pause + retune</span></div>
    <div class="row"><span class="row-key">SOL Price</span>
      <span class="row-val" id="sol-price">...</span></div>
    <div class="row"><span class="row-key">Bond Entry Range</span>
      <span class="row-val">{BOND_ENTRY_MIN}% – {BOND_ENTRY_MAX}%</span></div>
    <div class="row"><span class="row-key">Bond Take Profit</span>
      <span class="row-val" style="color:#39ff14">{BOND_TP}%</span></div>
    <div class="row"><span class="row-key">Stop Loss</span>
      <span class="row-val" style="color:#ff006e">{BOND_SL_PCT}% · Trailing SL at +{TSL_ACTIVATE_PCT}%</span></div>
    <div class="row"><span class="row-key">Retune Interval</span>
      <span class="row-val">Every {ANALYZE_EVERY} trades</span></div>
    <div class="row"><span class="row-key">Next Auto-Tune</span>
      <span class="row-val" id="next-tune">...</span></div>
  </div>

  {"" if not _week_day_logs else f'''<div class="section" style="margin:0 12px 12px">
    <div class="section-hdr" style="font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px">DAY-BY-DAY LOG</div>
    <div class="table-wrap"><table>
      <thead><tr><th>Date</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Win Rate</th><th>Capital</th></tr></thead>
      <tbody>{day_rows}</tbody>
    </table></div>
  </div>'''}

  <footer>{BOT_NAME} · Refreshes every 20s</footer>
</div>
<script>
fetch('/status/api').then(r=>r.json()).then(d=>{{
  if(d.sol_price) document.getElementById('sol-price').textContent='$'+d.sol_price.toLocaleString();
  if(d.next_tune_in !== undefined) {{
    const el = document.getElementById('next-tune');
    el.textContent = d.next_tune_in === 1 ? '1 trade away' : d.next_tune_in+' trades away';
    if(d.next_tune_in <= 2) el.style.color='#39ff14';
  }}
}}).catch(()=>{{}});
</script>
</body></html>"""
    return html, 200

@app.route("/trades/api", methods=["GET"])
def trades_api():
    with trades_lock:
        open_list = [{k: v for k, v in t.items()
                      if k not in ("opened_at", "bond_slip_start")}
                     for t in open_trades.values()]
    return jsonify({"open": open_list, "completed": completed_trades[-50:]})

@app.route("/trades", methods=["GET"])
def trades():
    with trades_lock:
        open_list = list(open_trades.values())
    with capital_lock:
        cap = capital

    wins   = [t for t in completed_trades if t["pnl"] > 0]
    losses = [t for t in completed_trades if t["pnl"] <= 0]
    total  = len(completed_trades)
    wr     = round(len(wins) / max(total, 1) * 100, 1)
    total_pnl = round(sum(t["pnl"] for t in completed_trades), 4)
    avg_win   = round(sum(t["pnl"] for t in wins)   / max(len(wins),   1), 4)
    avg_loss  = round(sum(t["pnl"] for t in losses) / max(len(losses), 1), 4)
    best      = max(completed_trades, key=lambda t: t["pnl"], default=None)
    worst     = min(completed_trades, key=lambda t: t["pnl"], default=None)

    # Build table rows — newest first
    rows = ""
    for t in reversed(completed_trades):
        won   = t["pnl"] > 0
        color = "#4ade80" if won else "#f87171"
        badge = f'<span class="badge {"win" if won else "loss"}">{"WIN" if won else "LOSS"}</span>'
        sign  = "+" if t["pnl"] >= 0 else ""
        rows += f"""<tr class="trade-row" data-id="{t['id']}" onclick="openModal({t['id']})">
          <td class="td-num">#{t['id']}</td>
          <td>{t['date']}<br><span class="muted">{t['time']}</span></td>
          <td class="sym">{t['symbol']}</td>
          <td><span class="badge strat">{t['strategy'].upper()}</span></td>
          <td class="mono">${t['entry']:.6f}</td>
          <td class="mono">${t['exit']:.6f}</td>
          <td class="mono" style="color:{color}">{sign}{t['pnl_pct']:.1f}%</td>
          <td class="mono" style="color:{color};font-weight:700">{sign}${t['pnl']:.4f}</td>
          <td>{t['hold_m']:.1f}m</td>
          <td><span class="badge exit">{t['result']}</span></td>
          <td>{badge}</td>
        </tr>"""

    # Open trades rows
    open_rows = ""
    for t in open_list:
        elapsed = round((time.time() - t["opened_at"]) / 60, 1)
        cur_pct = round((t.get("price_high", t["entry"]) - t["entry"]) / max(t["entry"], 1e-12) * 100, 1)
        open_rows += f"""<tr>
          <td class="sym">{t['symbol']}</td>
          <td><span class="badge strat">{t['strategy'].upper()}</span></td>
          <td class="mono">${t['amount']:.2f}</td>
          <td class="mono">{t.get('bond_entry',0):.1f}%</td>
          <td class="mono {'green' if cur_pct>=0 else 'red'}">{'+' if cur_pct>=0 else ''}{cur_pct:.1f}%</td>
          <td class="pulse">{elapsed}m</td>
        </tr>"""

    # JSON data for modals
    trades_json = json.dumps(completed_trades)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<meta http-equiv="refresh" content="30">
<title>Trades — {BOT_NAME}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{--acc:#39ff14;--bg:#0a0008;--card:#110010;--border:#ffffff15}}
  body{{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}}
  .bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;object-position:center;opacity:.35;pointer-events:none;z-index:0}}
  .wrap{{position:relative}}
  nav{{display:flex;gap:0;border-bottom:2px solid var(--acc);overflow-x:auto;scrollbar-width:none}}
  nav::-webkit-scrollbar{{display:none}}
  nav a{{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;padding:10px 14px;white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;border-right:1px solid var(--border);transition:all .15s}}
  nav a:hover{{background:var(--acc);color:#000}}
  nav a.active{{background:var(--acc);color:#000}}
  .page-title{{font-family:'Bebas Neue',sans-serif;font-size:3rem;color:var(--acc);text-shadow:0 0 24px #39ff1488;padding:18px 16px 8px;line-height:1;letter-spacing:.04em}}
  .stat-strip{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:2px;background:var(--acc);border:2px solid var(--acc);margin:0 12px 12px}}
  .stat{{background:var(--card);padding:12px 14px}}
  .stat .lbl{{font-size:.58rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
  .stat .val{{font-family:'Bebas Neue',sans-serif;font-size:1.8rem;margin-top:2px;line-height:1;color:var(--acc)}}
  .section{{background:var(--card);border-top:2px solid var(--border);padding:14px;margin:0 12px 12px}}
  .section-hdr{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px}}
  .table-wrap{{overflow-x:auto;border:1px solid var(--border)}}
  table{{width:100%;border-collapse:collapse;font-size:.72rem}}
  th{{padding:8px 10px;font-size:.56rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.08em;text-align:left;border-bottom:1px solid var(--border);white-space:nowrap;background:#0d000c}}
  td{{padding:9px 10px;border-bottom:1px solid #ffffff05;vertical-align:middle}}
  .trade-row{{cursor:pointer;transition:background .15s}}
  .trade-row:hover td{{background:#39ff1410}}
  .td-num{{color:#888;font-size:.65rem}}
  .sym{{font-weight:900;font-size:.8rem}}
  .mono{{font-family:'JetBrains Mono',monospace;font-size:.68rem}}
  .pulse{{animation:pulse 2s infinite;color:var(--acc)}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.5}}}}
  .badge{{display:inline-block;padding:2px 6px;font-size:.56rem;font-weight:900;letter-spacing:.06em;border:1px solid}}
  .badge.win{{color:#39ff14;border-color:#39ff14;background:#39ff1415}}
  .badge.loss{{color:#ff006e;border-color:#ff006e;background:#ff006e15}}
  .badge.strat{{color:#00f5ff;border-color:#00f5ff;background:#00f5ff12}}
  .badge.exit{{color:#888;border-color:#444;background:#ffffff05}}
  .green{{color:#39ff14}} .red{{color:#ff006e}} .muted{{color:#888}}
  .overlay{{display:none;position:fixed;inset:0;background:#000000cc;z-index:100;align-items:center;justify-content:center;padding:20px}}
  .overlay.open{{display:flex}}
  .modal{{background:#0d0d14;border:1px solid #39ff1430;width:100%;max-width:480px;padding:24px;position:relative;box-shadow:0 0 60px #39ff1420}}
  .modal-close{{position:absolute;top:14px;right:16px;background:none;border:none;color:#888;font-size:1.3rem;cursor:pointer;width:28px;height:28px;display:flex;align-items:center;justify-content:center;transition:all .2s}}
  .modal-close:hover{{background:#ffffff10;color:#fff}}
  .modal h2{{font-family:'Bebas Neue',sans-serif;font-size:1.6rem;margin-bottom:4px;color:var(--acc)}}
  .modal .sub{{font-size:.72rem;color:#888;margin-bottom:20px}}
  .timeline{{position:relative;padding-left:20px;margin:20px 0}}
  .timeline::before{{content:'';position:absolute;left:6px;top:8px;bottom:8px;width:2px;background:linear-gradient(180deg,var(--acc),#39ff1450)}}
  .tl-item{{position:relative;margin-bottom:16px}}
  .tl-item:last-child{{margin-bottom:0}}
  .tl-dot{{position:absolute;left:-17px;top:4px;width:10px;height:10px;border-radius:50%;border:2px solid var(--bg)}}
  .tl-label{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.08em}}
  .tl-val{{font-size:.9rem;font-weight:700;margin-top:2px;font-family:'JetBrains Mono',monospace}}
  .tl-sub{{font-size:.68rem;color:#888;margin-top:1px}}
  .detail-grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:14px}}
  .detail-box{{background:#ffffff06;padding:10px}}
  .detail-box .lbl{{font-size:.58rem;color:#888;text-transform:uppercase;font-weight:700;letter-spacing:.06em}}
  .detail-box .val{{font-size:.9rem;font-weight:700;margin-top:4px;font-family:'JetBrains Mono',monospace;color:var(--acc)}}
  .solscan-btn{{display:block;text-align:center;margin-top:14px;padding:10px;background:#39ff1415;border:1px solid #39ff1430;color:var(--acc);text-decoration:none;font-size:.78rem;font-weight:700;letter-spacing:.06em;transition:all .2s}}
  .solscan-btn:hover{{background:#39ff1425}}
  footer{{padding:14px 16px;text-align:center;font-size:.6rem;color:#444;border-top:1px solid var(--border)}}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<div class="wrap">
  <nav>
    <a href="/">HOME</a>
    <a href="/live">LIVE</a>
    <a href="/trades" class="active">TRADES</a>
    <a href="/status">STATUS</a>
    <a href="/learn">STRATEGY</a>
    <a href="/setup">SETUP</a>
  </nav>

  <div class="page-title">TRADES</div>

  <div class="stat-strip">
    <div class="stat">
      <div class="lbl">Win Rate</div>
      <div class="val" style="color:{'#39ff14' if wr>=50 else '#ff006e'}">{wr}%</div>
    </div>
    <div class="stat">
      <div class="lbl">Total PnL</div>
      <div class="val" style="color:{'#39ff14' if total_pnl>=0 else '#ff006e'}">{'+' if total_pnl>=0 else ''}${total_pnl}</div>
    </div>
    <div class="stat">
      <div class="lbl">Trades</div>
      <div class="val">{total}</div>
    </div>
  </div>

  {"" if not open_list else f'''<div class="section">
    <div class="section-hdr">OPEN NOW ({len(open_list)})</div>
    <div class="table-wrap"><table>
      <thead><tr><th>Symbol</th><th>Strategy</th><th>Size</th><th>Bond In</th><th>Move</th><th>Held</th></tr></thead>
      <tbody>{open_rows}</tbody>
    </table></div>
  </div>'''}

  <div class="section">
    <div class="section-hdr">CLOSED TRADES — tap row for full breakdown</div>
    <div class="table-wrap"><table>
      <thead><tr>
        <th>#</th><th>Date / Time</th><th>Symbol</th><th>Strategy</th>
        <th>Entry</th><th>Exit</th><th>PnL %</th><th>PnL $</th>
        <th>Hold</th><th>Exit Reason</th><th>Result</th>
      </tr></thead>
      <tbody>
        {rows if rows else '<tr><td colspan="11" style="text-align:center;padding:32px;color:#888">No closed trades yet — bot is scanning...</td></tr>'}
      </tbody>
    </table></div>
  </div>

  <footer>{BOT_NAME} · Refreshes every 30s</footer>
</div>

<!-- TRADE DETAIL MODAL -->
<div class="overlay" id="overlay" onclick="closeModal(event)">
  <div class="modal" id="modal">
    <button class="modal-close" onclick="closeModal()">✕</button>
    <h2 id="m-symbol"></h2>
    <div class="sub" id="m-sub"></div>

    <div class="timeline">
      <div class="tl-item">
        <div class="tl-dot" style="background:#60a5fa"></div>
        <div class="tl-label">Entry</div>
        <div class="tl-val" id="m-entry"></div>
        <div class="tl-sub" id="m-entry-sub"></div>
      </div>
      <div class="tl-item">
        <div class="tl-dot" style="background:#39ff14"></div>
        <div class="tl-label">Peak</div>
        <div class="tl-val" id="m-peak"></div>
        <div class="tl-sub" id="m-peak-sub"></div>
      </div>
      <div class="tl-item">
        <div class="tl-dot" id="m-exit-dot" style="background:#39ff14"></div>
        <div class="tl-label">Exit</div>
        <div class="tl-val" id="m-exit"></div>
        <div class="tl-sub" id="m-exit-sub"></div>
      </div>
    </div>

    <div class="detail-grid">
      <div class="detail-box">
        <div class="lbl">PnL</div>
        <div class="val" id="m-pnl"></div>
      </div>
      <div class="detail-box">
        <div class="lbl">Hold Time</div>
        <div class="val" id="m-hold"></div>
      </div>
      <div class="detail-box">
        <div class="lbl">Bond % In</div>
        <div class="val" id="m-bond"></div>
      </div>
      <div class="detail-box">
        <div class="lbl">Bond % Peak</div>
        <div class="val" id="m-bond-high"></div>
      </div>
      <div class="detail-box">
        <div class="lbl">Trade Size</div>
        <div class="val" id="m-amount"></div>
      </div>
      <div class="detail-box">
        <div class="lbl">Exit Reason</div>
        <div class="val" id="m-reason"></div>
      </div>
    </div>

    <a id="m-solscan" href="#" target="_blank" class="solscan-btn">
      VIEW ON SOLSCAN →
    </a>
  </div>
</div>

<script>
const ALL = {trades_json};

function openModal(id) {{
  const t = ALL.find(x => x.id === id);
  if (!t) return;
  const won = t.pnl > 0;
  const sign = t.pnl >= 0 ? '+' : '';
  const peakPct = ((t.peak - t.entry) / Math.max(t.entry, 1e-12) * 100).toFixed(1);

  document.getElementById('m-symbol').textContent = t.symbol;
  document.getElementById('m-symbol').style.color = won ? '#39ff14' : '#ff006e';
  document.getElementById('m-sub').textContent =
    t.strategy.toUpperCase() + ' · ' + t.date + ' at ' + t.time;

  document.getElementById('m-entry').textContent = '$' + t.entry.toFixed(8);
  document.getElementById('m-entry-sub').textContent = 'Bond: ' + t.bond_entry.toFixed(1) + '%';

  document.getElementById('m-peak').textContent = '$' + t.peak.toFixed(8);
  document.getElementById('m-peak-sub').textContent = '+' + peakPct + '% from entry · Bond high: ' + (t.bond_high||0).toFixed(1) + '%';

  document.getElementById('m-exit').textContent = '$' + t.exit.toFixed(8);
  document.getElementById('m-exit-sub').textContent = t.result + ' · ' + sign + t.pnl_pct.toFixed(1) + '% move';
  document.getElementById('m-exit-dot').style.background = won ? '#39ff14' : '#ff006e';

  document.getElementById('m-pnl').textContent = sign + '$' + t.pnl.toFixed(4);
  document.getElementById('m-pnl').style.color = won ? '#39ff14' : '#ff006e';
  document.getElementById('m-hold').textContent = t.hold_m.toFixed(1) + ' min';
  document.getElementById('m-bond').textContent = (t.bond_entry||0).toFixed(1) + '%';
  document.getElementById('m-bond-high').textContent = (t.bond_high||0).toFixed(1) + '%';
  document.getElementById('m-amount').textContent = '$' + (t.amount||0).toFixed(2);
  document.getElementById('m-reason').textContent = t.result;

  document.getElementById('m-solscan').href =
    'https://solscan.io/token/' + (t.mint || '');

  document.getElementById('overlay').classList.add('open');
}}

function closeModal(e) {{
  if (!e || e.target === document.getElementById('overlay')) {{
    document.getElementById('overlay').classList.remove('open');
  }}
}}

document.addEventListener('keydown', e => {{ if(e.key==='Escape') closeModal(); }});
</script>
</body></html>"""
    return html, 200

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-100:]})

@app.route("/live/api", methods=["GET"])
def live_api():
    """Polled every 3s by the live page — returns open trades + recent events."""
    with trades_lock:
        open_now = []
        for t in open_trades.values():
            elapsed = round(time.time() - t["opened_at"], 1)
            cur_bond = 0
            pct = 0
            open_now.append({
                "symbol":     t["symbol"],
                "mint":       t["mint"],
                "strategy":   t["strategy"],
                "amount":     round(t["amount"], 2),
                "entry":      t["entry"],
                "bond_entry": round(t.get("bond_entry", 0), 1),
                "bond_high":  round(t.get("bond_high", 0), 1),
                "price_high": t.get("price_high", t["entry"]),
                "elapsed_s":  elapsed,
                "opened_at":  round(t["opened_at"]),
            })
    with capital_lock:
        cap = capital
    recent_closed = list(reversed(completed_trades[-30:]))
    with _scan_log_lock:
        sl = list(scan_log[:20])
    return jsonify({
        "ts":       round(time.time()),
        "capital":  round(cap, 2),
        "open":     open_now,
        "closed":   recent_closed,
        "scanning": scan_active,
        "paused":   _pause_until > time.time(),
        "today":    {"trades": _daily_trades, "wins": _daily_wins, "losses": _daily_losses},
        "scan_log": sl,
    })

@app.route("/live", methods=["GET"])
def live():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Live Feed — __BOT_NAME__</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#050a14;--bg2:#080f1e;--bg3:#0d1628;--cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--yellow:#ffee00;--text:#c8d8f0;--muted:#4a6080}
html,body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;-webkit-user-select:none;user-select:none;overflow-x:hidden}
body{max-width:430px;margin:0 auto}
nav{background:rgba(5,10,20,.97);border-bottom:1px solid rgba(0,229,255,.1);display:flex;overflow-x:auto;position:sticky;top:0;z-index:100}
nav::-webkit-scrollbar{display:none}
.nav-tab{font-family:'Bebas Neue',sans-serif;font-size:13px;letter-spacing:2px;padding:14px 18px;color:var(--muted);white-space:nowrap;border-bottom:2px solid transparent;flex-shrink:0;text-decoration:none}
.nav-tab.active{color:var(--cyan);border-bottom-color:var(--cyan)}
.page-hdr{padding:14px 16px 6px;display:flex;align-items:center;justify-content:space-between}
.page-title{font-family:'Bebas Neue',sans-serif;font-size:32px;letter-spacing:4px;color:var(--cyan)}
.status-strip{display:flex;flex-wrap:wrap;gap:5px;padding:0 16px 10px}
.s-pill{display:flex;align-items:center;gap:5px;padding:4px 9px;border-radius:20px;border:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.03);font-size:10px;font-family:'JetBrains Mono',monospace}
.s-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 1.8s ease infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.s-val{color:var(--cyan);font-weight:700}
.s-open{color:var(--yellow);font-weight:700}
.pos-section{margin:0 16px 10px;border:1px solid rgba(0,229,255,.12);border-radius:14px;overflow:hidden}
.sec-hdr{display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:var(--bg2);border-bottom:1px solid rgba(0,229,255,.08)}
.sec-title{font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--text);display:flex;align-items:center;gap:7px}
.s-dot2{width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 1.8s ease infinite}
.cbadge{font-family:'JetBrains Mono',monospace;font-size:8px;padding:2px 7px;border-radius:10px;font-weight:700;background:rgba(255,238,0,.12);color:var(--yellow);border:1px solid rgba(255,238,0,.25);transition:all .3s}
.sec-btn{font-family:'JetBrains Mono',monospace;font-size:8px;padding:3px 9px;border:1px solid rgba(0,229,255,.25);border-radius:6px;color:var(--cyan);background:rgba(0,229,255,.07);letter-spacing:1px;cursor:pointer;-webkit-tap-highlight-color:transparent}
.pos-stack-wrap{position:relative;transition:height .45s cubic-bezier(.22,.8,.36,1);overflow:hidden;background:var(--bg2)}
.pos-card{position:absolute;left:14px;right:14px;height:100px;border-radius:12px;overflow:hidden;transition:top .45s cubic-bezier(.22,.8,.36,1),transform .45s cubic-bezier(.22,.8,.36,1),opacity .45s,box-shadow .3s;will-change:transform,top,opacity}
.pos-card.neutral-card{border:1px solid rgba(0,229,255,.2);box-shadow:0 2px 14px rgba(0,229,255,.04)}
.pc-hide-bg{position:absolute;inset:0;background:linear-gradient(90deg,transparent 25%,rgba(0,80,100,.88));display:flex;align-items:center;justify-content:flex-end;padding-right:18px;font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:700;letter-spacing:1px;color:#fff;opacity:0;pointer-events:none}
.pc-inner{position:absolute;inset:0;background:var(--bg3);padding:11px 13px;display:flex;flex-direction:column;justify-content:space-between;will-change:transform}
.pc-row1{display:flex;align-items:baseline;justify-content:space-between}
.pc-sym{font-family:'Bebas Neue',sans-serif;font-size:26px;letter-spacing:2px;line-height:1;color:var(--cyan)}
.pc-bond{font-family:'Bebas Neue',sans-serif;font-size:16px;letter-spacing:1px;line-height:1;color:var(--green)}
.pc-row2{display:flex;align-items:center;justify-content:space-between}
.pc-bond-bar{flex:1;height:4px;background:rgba(255,255,255,.06);border-radius:2px;margin:0 8px;overflow:hidden}
.pc-bond-fill{height:4px;border-radius:2px;background:linear-gradient(90deg,var(--cyan),var(--green));transition:width .6s ease}
.pc-strat{font-family:'JetBrains Mono',monospace;font-size:8px;color:var(--muted)}
.pc-row3{display:flex;align-items:center;justify-content:space-between}
.pc-size{font-family:'JetBrains Mono',monospace;font-size:8px;color:var(--muted)}
.pc-time{font-family:'JetBrains Mono',monospace;font-size:8px;color:var(--muted)}
.pc-swipe-lbl{font-family:'JetBrains Mono',monospace;font-size:7px;color:rgba(74,96,128,.4);letter-spacing:1px}
.pos-collapse-hint{background:var(--bg2);border-top:1px solid rgba(0,229,255,.06);padding:8px 14px;text-align:center;font-family:'JetBrains Mono',monospace;font-size:8px;letter-spacing:2px;color:var(--muted);cursor:pointer;-webkit-tap-highlight-color:transparent}
.scan-section{margin:0 16px 10px;background:var(--bg2);border:1px solid rgba(0,229,255,.12);border-radius:14px;clip-path:inset(0 round 14px)}
.scan-now-bar{display:flex;align-items:center;gap:8px;padding:7px 14px;border-bottom:1px solid rgba(0,229,255,.08);background:var(--bg2)}
.sn-pulse{width:7px;height:7px;border-radius:50%;background:var(--cyan);animation:pulse .9s ease infinite;flex-shrink:0}
.sn-label{font-family:'JetBrains Mono',monospace;font-size:9px;letter-spacing:2px;color:var(--muted)}
.sn-coin{font-family:'Bebas Neue',sans-serif;font-size:14px;letter-spacing:2px;color:var(--cyan);margin-left:auto}
.sn-status{font-family:'JetBrains Mono',monospace;font-size:9px;font-weight:700;flex-shrink:0}
.sn-status.scan{color:var(--cyan);animation:blink .6s infinite}
.sn-status.pass{color:var(--green)}.sn-status.fail{color:var(--red)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.1}}
.deck-wrap{position:relative;height:390px;perspective:900px;perspective-origin:50% 50%;touch-action:none}
.scan-card{position:absolute;left:50%;top:50%;width:284px;margin-left:-142px;background:var(--bg3);border:1px solid rgba(0,229,255,.12);border-radius:14px;padding:11px 13px;will-change:transform,opacity;transition:transform .42s cubic-bezier(.22,.8,.36,1),opacity .42s,box-shadow .35s,border-color .35s;pointer-events:none;transform-origin:center center}
.scan-card.is-focus{pointer-events:auto;z-index:5!important}
.scan-card.state-pass{border-color:rgba(0,255,136,.5);box-shadow:0 0 26px rgba(0,255,136,.18)}
.scan-card.state-fail{border-color:rgba(255,51,85,.45);box-shadow:0 0 26px rgba(255,51,85,.13)}
.sc-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px}
.sc-sym{font-family:'Bebas Neue',sans-serif;font-size:22px;letter-spacing:2px;color:var(--cyan);line-height:1}
.sc-mint{font-family:'JetBrains Mono',monospace;font-size:7px;color:var(--muted);margin-top:2px}
.sc-right{display:flex;flex-direction:column;align-items:flex-end;gap:3px}
.sc-badge{font-family:'JetBrains Mono',monospace;font-size:7px;padding:2px 6px;border-radius:4px}
.b-bond{background:rgba(0,229,255,.1);border:1px solid rgba(0,229,255,.25);color:var(--cyan)}
.b-sig{background:rgba(255,238,0,.08);border:1px solid rgba(255,238,0,.22);color:var(--yellow)}
.sc-bar{height:2px;background:rgba(255,255,255,.05);border-radius:1px;margin-bottom:7px}
.sc-bar-fill{height:2px;border-radius:1px;background:linear-gradient(90deg,var(--cyan),var(--green))}
.sc-filters{display:grid;grid-template-columns:1fr 1fr;gap:2px 8px;margin-bottom:8px;min-height:48px}
.frow{display:flex;align-items:center;gap:4px;height:15px;opacity:0;transform:translateX(-5px);transition:opacity .2s,transform .2s}
.frow.vis{opacity:1;transform:translateX(0)}
.ficon{font-size:8px;width:11px;flex-shrink:0;text-align:center}
.fname{font-family:'JetBrains Mono',monospace;font-size:7px;color:var(--muted);flex:1}
.fval{font-family:'JetBrains Mono',monospace;font-size:7px;font-weight:700;flex-shrink:0}
.fval.ok{color:var(--green)}.fval.bad{color:var(--red)}.fval.chk{color:var(--cyan);animation:blink .55s infinite}
.sc-status{height:22px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-family:'Bebas Neue',sans-serif;font-size:11px;letter-spacing:2.5px;border:1px solid rgba(0,229,255,.15);background:rgba(0,229,255,.03);color:var(--muted);transition:all .3s;position:relative;overflow:hidden}
.sc-status.scanning{color:var(--cyan);border-color:rgba(0,229,255,.35);animation:spulse 1s ease infinite}
.sc-status.pass{background:rgba(0,255,136,.07);border-color:rgba(0,255,136,.4);color:var(--green)}
.sc-status.fail{background:rgba(255,51,85,.07);border-color:rgba(255,51,85,.35);color:var(--red)}
.sc-status.pass::after,.sc-status.fail::after{content:'';position:absolute;inset:0;background:linear-gradient(90deg,transparent,rgba(255,255,255,.1),transparent);animation:sweep .65s ease forwards}
@keyframes sweep{from{transform:translateX(-100%)}to{transform:translateX(100%)}}
@keyframes spulse{0%,100%{opacity:1}50%{opacity:.5}}
.sc-fig{position:absolute;bottom:10px;right:10px;width:20px;height:26px;opacity:.35;transition:opacity .3s;pointer-events:none}
.scan-card.is-focus .sc-fig{opacity:.8}
.deck-dots{position:absolute;right:5px;top:50%;transform:translateY(-50%);display:flex;flex-direction:column;gap:6px;z-index:20;pointer-events:none}
.ddot{width:5px;height:5px;border-radius:50%;background:rgba(0,229,255,.12);transition:all .22s}
.ddot.active{background:var(--cyan);transform:scale(1.6);box-shadow:0 0 5px var(--cyan)}
.ddot.dp{background:var(--green);opacity:.7}.ddot.df{background:var(--red);opacity:.7}
.live-btn{position:absolute;top:8px;right:32px;font-family:'JetBrains Mono',monospace;font-size:7px;letter-spacing:1.5px;padding:3px 9px;border-radius:10px;background:rgba(0,229,255,.12);border:1px solid rgba(0,229,255,.4);color:var(--cyan);cursor:pointer;z-index:20;animation:livePulse 1.5s ease infinite;display:none}
@keyframes livePulse{0%,100%{box-shadow:0 0 4px rgba(0,229,255,.2)}50%{box-shadow:0 0 10px rgba(0,229,255,.5)}}
.deck-hint{position:absolute;bottom:6px;left:50%;transform:translateX(-50%);display:flex;align-items:center;gap:4px;opacity:0;transition:opacity .4s;pointer-events:none;z-index:20}
.deck-hint.show{opacity:.35}
.dh-u{width:0;height:0;border-left:3px solid transparent;border-right:3px solid transparent;border-bottom:4px solid var(--cyan)}
.dh-d{width:0;height:0;border-left:3px solid transparent;border-right:3px solid transparent;border-top:4px solid var(--cyan)}
.dh-lbl{font-family:'JetBrains Mono',monospace;font-size:6px;letter-spacing:1.5px;color:var(--cyan);text-transform:uppercase}
.scan-stats{border-top:1px solid rgba(0,229,255,.07);padding:7px 14px;display:flex;align-items:center;gap:7px;background:var(--bg2);border-radius:0 0 14px 14px}
.st-dot{width:5px;height:5px;border-radius:50%;background:var(--cyan);animation:pulse 1s ease infinite;flex-shrink:0}
.st-counts{display:flex;gap:10px;font-family:'JetBrains Mono',monospace;font-size:8px;margin-left:auto}
.section2{margin:0 16px 18px}
.sec2-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.sec2-title{font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:700;letter-spacing:2px;color:var(--text);text-transform:uppercase}
.sec2-link{font-family:'JetBrains Mono',monospace;font-size:9px;color:var(--cyan);text-decoration:none}
.trade-row{display:flex;align-items:flex-start;gap:9px;padding:10px;background:var(--bg2);border:1px solid rgba(255,255,255,.04);border-radius:10px;margin-bottom:6px}
.tr-time{font-family:'JetBrains Mono',monospace;font-size:8px;color:var(--muted);white-space:nowrap;margin-top:2px}
.tr-icon{width:18px;height:18px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:10px;flex-shrink:0}
.tr-icon.win{background:rgba(0,255,136,.15);color:var(--green)}
.tr-icon.loss{background:rgba(255,51,85,.12);color:var(--red)}
.tr-icon.open{background:rgba(0,229,255,.12);color:var(--cyan)}
.tr-body{flex:1}
.tr-sym{font-family:'Bebas Neue',sans-serif;font-size:14px;letter-spacing:1.5px;line-height:1;margin-bottom:3px}
.tr-tags{display:flex;gap:4px;margin-bottom:2px}
.tag{font-family:'JetBrains Mono',monospace;font-size:7px;padding:2px 5px;border-radius:3px;font-weight:700}
.tag-win{background:rgba(0,255,136,.2);color:var(--green)}.tag-loss{background:rgba(255,51,85,.18);color:var(--red)}
.tag-open{background:rgba(0,229,255,.12);color:var(--cyan)}.tag-bond{background:rgba(0,229,255,.1);color:var(--cyan)}
.tr-detail{font-family:'JetBrains Mono',monospace;font-size:7px;color:var(--muted);line-height:1.5}
.tr-pnl{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:700;white-space:nowrap}
.tr-pnl.pos{color:var(--green)}.tr-pnl.neg{color:var(--red)}
.bot-name{text-align:center;font-size:9px;color:rgba(74,96,128,.4);padding-bottom:20px;letter-spacing:1px}
</style>
</head>
<body>
<nav>
  <a href="/" class="nav-tab">HOME</a>
  <a href="/live" class="nav-tab active">LIVE</a>
  <a href="/trades" class="nav-tab">TRADES</a>
  <a href="/status" class="nav-tab">STATUS</a>
  <a href="/learn" class="nav-tab">STRATEGY</a>
</nav>
<div class="page-hdr"><div class="page-title">LIVE FEED</div></div>
<div class="status-strip">
  <div class="s-pill"><span class="s-dot"></span><span id="scanStatus">Scanning</span></div>
  <div class="s-pill">Capital: <span class="s-val" id="capVal">--</span></div>
  <div class="s-pill">Today: <span style="color:var(--text)" id="todayVal">--</span></div>
  <div class="s-pill">Open: <span class="s-open" id="openCount">0</span></div>
</div>
<div class="pos-section">
  <div class="sec-hdr">
    <div class="sec-title"><span class="s-dot2"></span>OPEN POSITIONS<span class="cbadge" id="posBadge">0 OPEN</span></div>
    <div class="sec-btn" id="expandBtn" onclick="toggleExpand()" style="display:none">EXPAND &#x2195;</div>
  </div>
  <div class="pos-stack-wrap" id="posStackWrap"></div>
  <div class="pos-collapse-hint" id="collapseHint" onclick="toggleExpand()" style="display:none">&#x2191; COLLAPSE</div>
</div>
<div class="scan-section">
  <div class="scan-now-bar">
    <div class="sn-pulse"></div>
    <div class="sn-label">NOW SCANNING</div>
    <div class="sn-coin" id="snCoin">&#x2014;</div>
    <div class="sn-status scan" id="snStatus">WAITING</div>
  </div>
  <div class="deck-wrap" id="deckWrap">
    <div class="live-btn" id="liveBtn" onclick="jumpToLive()">&#x25B6; LIVE</div>
    <div class="deck-dots" id="deckDots"></div>
    <div class="deck-hint" id="deckHint">
      <div class="dh-u"></div><div class="dh-lbl">drag</div><div class="dh-d"></div>
    </div>
  </div>
  <div class="scan-stats">
    <div class="st-dot"></div>
    <span style="font-family:'JetBrains Mono',monospace;font-size:8px;color:var(--muted)">SCAN HISTORY</span>
    <div class="st-counts">
      <span style="color:var(--text)" id="st-n">0 scanned</span>
      <span style="color:var(--green)" id="st-p">0 &#x2713;</span>
      <span style="color:var(--red)" id="st-f">0 &#x2717;</span>
    </div>
  </div>
</div>
<div class="section2">
  <div class="sec2-hdr">
    <div class="sec2-title">TRADE EVENTS</div>
    <a href="/trades" class="sec2-link">FULL HISTORY &#x2192;</a>
  </div>
  <div id="eventsWrap"><div style="font-family:monospace;font-size:9px;color:var(--muted);text-align:center;padding:20px">Waiting for events...</div></div>
</div>
<div class="bot-name">__BOT_NAME__ &#xB7; Live</div>
<script>
// OPEN POSITIONS — iOS card stack
const CARD_H=100,PEEK=17,EXP_GAP=10,PAD=13;
let stackExpanded=false;
const dismissed=new Set();
const posCards={};
const posTimers={};
const posWrap=document.getElementById('posStackWrap');
const expandBtn=document.getElementById('expandBtn');
const badge=document.getElementById('posBadge');
const colHint=document.getElementById('collapseHint');
function fmt(s){s=Math.round(s);return s<60?s+'s':Math.floor(s/60)+'m '+String(s%60).padStart(2,'0')+'s';}
function makePos(t){
  var mint=t.mint;
  if(posCards[mint])return;
  var el=document.createElement('div');
  el.className='pos-card neutral-card';
  var bpct=Math.min(100,t.bond_entry||0);
  el.innerHTML=
    '<div class="pc-hide-bg">&#x2190; HIDE</div>'+
    '<div class="pc-inner">'+
      '<div class="pc-row1">'+
        '<div class="pc-sym">'+t.symbol+'</div>'+
        '<div class="pc-bond">'+t.bond_entry.toFixed(1)+'% &#x2192; '+t.bond_high.toFixed(1)+'%</div>'+
      '</div>'+
      '<div class="pc-row2">'+
        '<div class="pc-strat">BOND</div>'+
        '<div class="pc-bond-bar"><div class="pc-bond-fill" style="width:'+bpct+'%"></div></div>'+
        '<div class="pc-strat">'+t.strategy.toUpperCase()+'</div>'+
      '</div>'+
      '<div class="pc-row3">'+
        '<div class="pc-size">$'+t.amount.toFixed(2)+'</div>'+
        '<div class="pc-time" id="pct'+mint+'">'+fmt(t.elapsed_s)+'</div>'+
        '<div class="pc-swipe-lbl">&#x2190; HIDE</div>'+
      '</div>'+
    '</div>';
  posWrap.appendChild(el);
  posCards[mint]={card:el,timeEl:document.getElementById('pct'+mint)};
  posTimers[mint]=t.elapsed_s;
  initSwipe(el,mint);
}
function visMints(){return Object.keys(posCards).filter(function(m){return !dismissed.has(m)&&posCards[m].card.style.display!=='none';});}
function updateStack(){
  var vk=visMints();var n=vk.length;
  badge.textContent=n+' OPEN';
  document.getElementById('openCount').textContent=n;
  if(n===0){posWrap.style.height='0px';colHint.style.display='none';expandBtn.style.display='none';return;}
  expandBtn.style.display='';
  colHint.style.display=stackExpanded&&n>1?'':'none';
  expandBtn.textContent=stackExpanded?'COLLAPSE &#x2195;':'EXPAND &#x2195;';
  if(stackExpanded){
    posWrap.style.height=(PAD+n*CARD_H+(n-1)*EXP_GAP+PAD)+'px';
    vk.forEach(function(m,i){var c=posCards[m].card;c.style.top=(PAD+i*(CARD_H+EXP_GAP))+'px';c.style.transform='scale(1)';c.style.opacity='1';c.style.zIndex=n-i;c.style.pointerEvents='auto';});
  }else{
    posWrap.style.height=(PAD+CARD_H+(n-1)*PEEK+PAD)+'px';
    vk.forEach(function(m,i){var c=posCards[m].card;c.style.top=(PAD+i*PEEK)+'px';c.style.transform='scale('+(1-i*0.03)+')';c.style.opacity=i===0?'1':i===1?'0.78':'0.58';c.style.zIndex=n-i;c.style.pointerEvents='auto';});
  }
}
function toggleExpand(){if(visMints().length<=1)return;stackExpanded=!stackExpanded;updateStack();}
function initSwipe(card,mint){
  var inner=card.querySelector('.pc-inner');
  var bg=card.querySelector('.pc-hide-bg');
  var sx,sy,isH=null,moved=false;
  function snap(){inner.style.transition='transform .35s cubic-bezier(.22,.8,.36,1)';inner.style.transform='translateX(0)';bg.style.opacity='0';}
  card.addEventListener('touchstart',function(e){sx=e.touches[0].clientX;sy=e.touches[0].clientY;isH=null;moved=false;inner.style.transition='none';bg.style.transition='none';},{passive:true});
  card.addEventListener('touchmove',function(e){
    var dx=e.touches[0].clientX-sx,dy=e.touches[0].clientY-sy;
    if(isH===null&&(Math.abs(dx)>6||Math.abs(dy)>6))isH=Math.abs(dx)>Math.abs(dy);
    if(!isH)return;moved=true;if(dx>8)return;
    inner.style.transform='translateX('+Math.min(0,dx)+'px)';
    bg.style.opacity=Math.min(1,Math.abs(Math.min(0,dx))/88);
    e.preventDefault();
  },{passive:false});
  card.addEventListener('touchend',function(e){
    var dx=e.changedTouches[0].clientX-sx;
    if(!moved||isH===null){snap();if(!stackExpanded)toggleExpand();return;}
    if(!isH){snap();return;}
    if(dx<-80){
      inner.style.transition='transform .28s ease';inner.style.transform='translateX(-110%)';bg.style.opacity='1';
      setTimeout(function(){dismissCard(mint);},290);
    }else snap();
  },{passive:true});
}
function dismissCard(mint){
  var c=posCards[mint];if(!c)return;
  var card=c.card;
  card.style.transition='height .38s ease,opacity .28s ease';
  var h=card.offsetHeight;void card.offsetHeight;card.style.height=h+'px';
  requestAnimationFrame(function(){card.style.height='0';card.style.opacity='0';card.style.overflow='hidden';});
  setTimeout(function(){card.style.display='none';dismissed.add(mint);if(visMints().length<2&&stackExpanded)stackExpanded=false;updateStack();},400);
}
setInterval(function(){Object.keys(posTimers).forEach(function(m){if(dismissed.has(m))return;posTimers[m]+=1;var c=posCards[m];if(c&&c.timeEl)c.timeEl.textContent=fmt(posTimers[m]);});},1000);

// COIN SCANNER — ISO perspective deck + real scan_log
var FILTERS=[
  {label:'SOCIAL',icon:'&#x1F517;'},{label:'ACTIVE',icon:'&#x23F1;'},
  {label:'BOND%', icon:'&#x1F4C8;'},{label:'RUG',   icon:'&#x1F6E1;'},
  {label:'HOLDER',icon:'&#x1F465;'},{label:'DEV',   icon:'&#x1F50D;'},
];
var SLOTS=[
  {ty:-130,s:0.54,rx:-15,op:0.08,z:1},
  {ty: -65,s:0.78,rx: -7,op:0.40,z:2},
  {ty:   0,s:1.00,rx:  0,op:1.00,z:5},
  {ty:  65,s:0.78,rx:  7,op:0.40,z:2},
  {ty: 130,s:0.54,rx: 15,op:0.08,z:1},
];
var MAX_H=20;
var scanHistory=[],viewIdx=0,isAtLive=true,nPass=0,nFail=0;
var seenTs=new Set(),animQueue=[],isAnimating=false,firstPoll=true;
var deckWrap=document.getElementById('deckWrap');
var dotsEl=document.getElementById('deckDots');
var hintEl=document.getElementById('deckHint');
var liveBtn=document.getElementById('liveBtn');
function g(id){return document.getElementById(id);}
function makeCard(ci){
  var el=document.createElement('div');
  el.className='scan-card';el.id='card-'+ci;
  var fHTML=FILTERS.map(function(_,fi){
    return '<div class="frow" id="fr-'+ci+'-'+fi+'">'+
      '<div class="ficon" id="fi-'+ci+'-'+fi+'">&#x25FD;</div>'+
      '<div class="fname">'+FILTERS[fi].label+'</div>'+
      '<div class="fval" id="fv-'+ci+'-'+fi+'">&#x2014;</div>'+
    '</div>';
  }).join('');
  el.innerHTML=
    '<div class="sc-top">'+
      '<div><div class="sc-sym" id="csym-'+ci+'">&#x2014;</div><div class="sc-mint" id="cmint-'+ci+'"></div></div>'+
      '<div class="sc-right">'+
        '<div class="sc-badge b-bond" id="cbond-'+ci+'">BOND &#x2014;</div>'+
        '<div class="sc-badge b-sig" id="csig-'+ci+'">SIG &#x2014;</div>'+
      '</div>'+
    '</div>'+
    '<div class="sc-bar"><div class="sc-bar-fill" id="cbar-'+ci+'" style="width:0%"></div></div>'+
    '<div class="sc-filters">'+fHTML+'</div>'+
    '<div class="sc-status" id="csb-'+ci+'">QUEUED</div>'+
    '<svg class="sc-fig" viewBox="0 0 22 30" fill="none">'+
      '<circle cx="11" cy="3.5" r="3" stroke="#00e5ff" stroke-width="1.2"/>'+
      '<line x1="11" y1="6.5" x2="11" y2="19" stroke="#00e5ff" stroke-width="1.2"/>'+
      '<line x1="11" y1="10" x2="4" y2="15" stroke="#00e5ff" stroke-width="1.2">'+
        '<animateTransform id="arm-'+ci+'" attributeName="transform" type="rotate"'+
        ' from="0 11 10" to="-22 11 10" dur="0.5s" repeatCount="indefinite"'+
        ' calcMode="discrete" begin="indefinite"/>'+
      '</line>'+
      '<line x1="11" y1="10" x2="18" y2="15" stroke="#00e5ff" stroke-width="1.2"/>'+
      '<line x1="11" y1="19" x2="7" y2="28" stroke="#00e5ff" stroke-width="1.2"/>'+
      '<line x1="11" y1="19" x2="15" y2="28" stroke="#00e5ff" stroke-width="1.2"/>'+
    '</svg>';
  deckWrap.insertBefore(el,dotsEl);
  return el;
}
var cardEls=[];
for(var i=0;i<MAX_H;i++)cardEls.push(makeCard(i));
var dotEls=[];
for(var i=0;i<MAX_H;i++){var d=document.createElement('div');d.className='ddot';dotsEl.appendChild(d);dotEls.push(d);}
function layout(){
  cardEls.forEach(function(card,ci){
    var delta=ci-viewIdx,si=delta+2;
    if(si<0||si>4||ci>=scanHistory.length){
      card.style.opacity='0';card.style.pointerEvents='none';card.style.zIndex='0';
      card.style.transform='translateY('+(delta>0?180:-180)+'px) scale(0.4) rotateX('+(delta>0?20:-20)+'deg)';
    }else{
      var sl=SLOTS[si];
      card.style.opacity=sl.op;card.style.zIndex=sl.z;
      card.style.transform='translateY('+sl.ty+'px) scale('+sl.s+') rotateX('+sl.rx+'deg)';
      card.style.pointerEvents=si===2?'auto':'none';
      if(si===2)card.classList.add('is-focus');else card.classList.remove('is-focus');
    }
  });
  dotEls.forEach(function(dd,i){
    if(i>=scanHistory.length){dd.className='ddot';dd.style.display='none';return;}
    dd.style.display='block';
    var r=scanHistory[i].result;
    dd.className='ddot '+(r==='pass'?'dp':r==='fail'?'df':'dq');
    if(i===viewIdx)dd.classList.add('active');
  });
  isAtLive=viewIdx===0;
  liveBtn.style.display=isAtLive?'none':'block';
  hintEl.classList.toggle('show',scanHistory.length>1);
}
function entryToCoin(e){
  return {sym:e.sym,mint:e.mint,bond:e.bond,sig:Math.min(3,e.sig||0),failAt:e.fi,passMsg:e.msg||'',failMsg:e.msg||''};
}
function fillCard(ci){
  if(ci>=scanHistory.length)return;
  var coin=scanHistory[ci].coin;
  var cs=g('csym-'+ci);if(cs)cs.textContent=coin.sym;
  var cm=g('cmint-'+ci);if(cm)cm.textContent=coin.mint;
  var cb=g('cbond-'+ci);if(cb)cb.textContent='BOND '+coin.bond+'%';
  var csg=g('csig-'+ci);if(csg)csg.textContent='SIG '+coin.sig+'/3';
  var cbar=g('cbar-'+ci);if(cbar)cbar.style.width=Math.min(100,coin.bond)+'%';
  FILTERS.forEach(function(_,fi){
    var fr=g('fr-'+ci+'-'+fi);if(fr)fr.className='frow';
    var fic=g('fi-'+ci+'-'+fi);if(fic)fic.innerHTML='&#x25FD;';
    var fv=g('fv-'+ci+'-'+fi);if(fv){fv.className='fval';fv.innerHTML='&#x2014;';}
  });
  var sb=g('csb-'+ci);if(sb){sb.className='sc-status';sb.textContent='QUEUED';}
  cardEls[ci].className='scan-card';
}
function applyFinalState(ci){
  if(ci>=scanHistory.length)return;
  var coin=scanHistory[ci].coin,result=scanHistory[ci].result;
  var showUntil=result==='fail'?coin.failAt:FILTERS.length-1;
  for(var fi=0;fi<FILTERS.length;fi++){
    var row=g('fr-'+ci+'-'+fi);if(!row)continue;
    if(fi>showUntil){row.className='frow';continue;}
    row.className='frow vis';
    if(result==='fail'&&fi===coin.failAt){
      g('fi-'+ci+'-'+fi).innerHTML='&#x1F534;';
      g('fv-'+ci+'-'+fi).className='fval bad';
      g('fv-'+ci+'-'+fi).textContent='FAIL';
    }else{
      g('fi-'+ci+'-'+fi).innerHTML='&#x2705;';
      g('fv-'+ci+'-'+fi).className='fval ok';
      g('fv-'+ci+'-'+fi).textContent='OK';
    }
  }
  var sb=g('csb-'+ci);cardEls[ci].className='scan-card';
  if(result==='pass'){sb.className='sc-status pass';sb.innerHTML='&#x2713; '+(coin.passMsg||'TRADE ENTERED');cardEls[ci].classList.add('state-pass');}
  else{sb.className='sc-status fail';sb.innerHTML='&#x2717; '+(coin.failMsg||'REJECTED');cardEls[ci].classList.add('state-fail');}
}
function animateEntry(ci,onDone){
  var coin=scanHistory[ci].coin;
  var sb=g('csb-'+ci),card=cardEls[ci];
  g('snCoin').textContent=coin.sym;
  g('snStatus').className='sn-status scan';g('snStatus').textContent='CHECKING';
  try{g('arm-'+ci).beginElement();}catch(e){}
  sb.className='sc-status scanning';sb.textContent='SCANNING';
  var step=0;
  function tick(){
    if(step>=FILTERS.length){
      try{g('arm-'+ci).endElement();}catch(e){}
      sb.className='sc-status pass';sb.innerHTML='&#x2713; '+(coin.passMsg||'TRADE ENTERED');
      card.classList.add('state-pass');scanHistory[ci].result='pass';nPass++;
      g('snStatus').className='sn-status pass';g('snStatus').innerHTML='&#x2713; ENTERED';
      g('st-n').textContent=(nPass+nFail)+' scanned';g('st-p').innerHTML=nPass+' &#x2713;';
      setTimeout(onDone,2000);return;
    }
    var fr=g('fr-'+ci+'-'+step);if(fr)fr.className='frow vis';
    var fic=g('fi-'+ci+'-'+step);if(fic)fic.innerHTML=FILTERS[step].icon;
    var fv=g('fv-'+ci+'-'+step);if(fv){fv.className='fval chk';fv.textContent='...';}
    if(coin.failAt===step){
      setTimeout(function(){
        if(fic)fic.innerHTML='&#x1F534;';if(fv){fv.className='fval bad';fv.textContent='FAIL';}
        setTimeout(function(){
          try{g('arm-'+ci).endElement();}catch(e){}
          sb.className='sc-status fail';sb.innerHTML='&#x2717; '+(coin.failMsg||'REJECTED');
          card.classList.add('state-fail');scanHistory[ci].result='fail';nFail++;
          g('snStatus').className='sn-status fail';g('snStatus').innerHTML='&#x2717; REJECTED';
          g('st-n').textContent=(nPass+nFail)+' scanned';g('st-f').innerHTML=nFail+' &#x2717;';
          setTimeout(onDone,1600);
        },380);
      },340);return;
    }
    setTimeout(function(){
      if(fic)fic.innerHTML='&#x2705;';if(fv){fv.className='fval ok';fv.textContent='OK';}
      step++;setTimeout(tick,150);
    },340);
  }
  setTimeout(tick,200);
}
function drainQueue(){
  if(isAnimating||animQueue.length===0)return;
  isAnimating=true;
  var entry=animQueue.shift();
  var coin=entryToCoin(entry);
  scanHistory.unshift({coin:coin,result:'scanning'});
  if(scanHistory.length>MAX_H)scanHistory.pop();
  scanHistory.forEach(function(_,ci){fillCard(ci);});
  if(isAtLive)viewIdx=0;
  layout();
  animateEntry(0,function(){isAnimating=false;drainQueue();});
}
function jumpToLive(){viewIdx=0;isAtLive=true;layout();}
var ty0=0,dacc=0,ddrag=false;
deckWrap.addEventListener('touchstart',function(e){ty0=e.touches[0].clientY;dacc=0;ddrag=true;},{passive:true});
deckWrap.addEventListener('touchmove',function(e){if(ddrag)dacc=e.touches[0].clientY-ty0;},{passive:true});
deckWrap.addEventListener('touchend',function(){if(!ddrag)return;ddrag=false;if(Math.abs(dacc)>40){viewIdx=Math.max(0,Math.min(scanHistory.length-1,viewIdx+(dacc<0?1:-1)));isAtLive=viewIdx===0;layout();}});
var md=false,my0=0,ma=0;
deckWrap.addEventListener('mousedown',function(e){md=true;my0=e.clientY;ma=0;});
deckWrap.addEventListener('mousemove',function(e){if(md)ma=e.clientY-my0;});
deckWrap.addEventListener('mouseup',function(){if(!md)return;md=false;if(Math.abs(ma)>40){viewIdx=Math.max(0,Math.min(scanHistory.length-1,viewIdx+(ma<0?1:-1)));isAtLive=viewIdx===0;layout();}});

// TRADE EVENTS
function fmtP(p){if(!p||p===0)return '-';return p<0.0001?p.toFixed(10):p.toFixed(6);}
function elStr(s){s=Math.round(s||0);if(s<3600)return Math.floor(s/60)+'m '+String(s%60).padStart(2,'0')+'s';return Math.floor(s/3600)+'h '+(Math.floor(s/60)%60)+'m';}
function renderEvents(d){
  var wrap=g('eventsWrap');var rows=[];
  (d.open||[]).forEach(function(t){
    rows.push(
      '<div class="trade-row">'+
      '<div class="tr-time">'+elStr(t.elapsed_s)+'</div>'+
      '<div class="tr-icon open">&#x2192;</div>'+
      '<div class="tr-body">'+
        '<div class="tr-sym">'+t.symbol+'</div>'+
        '<div class="tr-tags"><span class="tag tag-open">OPEN</span><span class="tag tag-bond">'+t.strategy.toUpperCase()+'</span></div>'+
        '<div class="tr-detail">$'+t.amount.toFixed(2)+' &#xB7; BOND '+t.bond_entry.toFixed(1)+'%&#x2192;'+t.bond_high.toFixed(1)+'%</div>'+
      '</div>'+
      '<div class="tr-pnl" style="color:var(--muted)">OPEN</div>'+
      '</div>');
  });
  (d.closed||[]).slice(0,8).forEach(function(t){
    var win=(t.pnl||0)>0;
    rows.push(
      '<div class="trade-row">'+
      '<div class="tr-time">'+elStr(t.elapsed_s||0)+'</div>'+
      '<div class="tr-icon '+(win?'win':'loss')+'">'+(win?'&#x2713;':'&#x2717;')+'</div>'+
      '<div class="tr-body">'+
        '<div class="tr-sym">'+t.symbol+'</div>'+
        '<div class="tr-tags"><span class="tag '+(win?'tag-win':'tag-loss')+'">'+(win?'WIN':'LOSS')+'</span><span class="tag tag-bond">'+(t.strategy||'').toUpperCase()+'</span></div>'+
        '<div class="tr-detail">'+(t.exit_reason||t.result||'EXIT')+'</div>'+
      '</div>'+
      '<div class="tr-pnl '+(win?'pos':'neg')+'">'+(win?'+':'')+' $'+Math.abs(t.pnl||0).toFixed(2)+'</div>'+
      '</div>');
  });
  if(!rows.length)rows.push('<div style="font-family:monospace;font-size:9px;color:var(--muted);text-align:center;padding:20px">No events yet</div>');
  wrap.innerHTML=rows.join('');
}

// API POLL
function poll(){
  fetch('/live/api').then(function(r){return r.json();}).then(function(d){
    document.getElementById('capVal').textContent='$'+d.capital.toFixed(2);
    document.getElementById('todayVal').textContent=d.today.trades+'T '+d.today.wins+'W '+d.today.losses+'L';
    var sc=g('scanStatus');if(sc)sc.textContent=d.scanning?(d.paused?'Paused':'Scanning'):'Stopped';
    var mintSet=new Set((d.open||[]).map(function(t){return t.mint;}));
    (d.open||[]).forEach(function(t){makePos(t);});
    Object.keys(posCards).forEach(function(m){if(!mintSet.has(m)&&!dismissed.has(m)){var c=posCards[m];if(c&&c.card)c.card.style.display='none';}});
    (d.open||[]).forEach(function(t){posTimers[t.mint]=t.elapsed_s;});
    updateStack();
    var sl=d.scan_log||[];
    if(firstPoll){
      firstPoll=false;
      var seed=sl.slice(0,Math.min(4,sl.length));
      seed.forEach(function(e){seenTs.add(e.ts);});
      seed.slice().reverse().forEach(function(e){
        var coin=entryToCoin(e);
        scanHistory.unshift({coin:coin,result:e.result==='pass'?'pass':'fail'});
      });
      scanHistory.forEach(function(_,ci){fillCard(ci);applyFinalState(ci);});
      nPass=scanHistory.filter(function(s){return s.result==='pass';}).length;
      nFail=scanHistory.filter(function(s){return s.result==='fail';}).length;
      g('st-n').textContent=(nPass+nFail)+' scanned';
      g('st-p').innerHTML=nPass+' &#x2713;';
      g('st-f').innerHTML=nFail+' &#x2717;';
      if(sl.length>0&&sl[0].sym)g('snCoin').textContent=sl[0].sym;
      sl.forEach(function(e){seenTs.add(e.ts);});
      layout();
    }else{
      var fresh=sl.filter(function(e){return !seenTs.has(e.ts);});
      fresh.forEach(function(e){seenTs.add(e.ts);});
      fresh.slice().reverse().forEach(function(e){animQueue.push(e);});
      drainQueue();
    }
    renderEvents(d);
  }).catch(function(){});
}
poll();
setInterval(poll,3000);
</script>
</body></html>"""
    html = html.replace("__BOT_NAME__", BOT_NAME)
    return html, 200
@app.route("/learn/api", methods=["GET"])
def learn_api():
    try:
        with trades_lock:
            recent = list(completed_trades[-60:])
        wins   = [t for t in recent if t.get("pnl", 0) > 0]
        losses = [t for t in recent if t.get("pnl", 0) <= 0]
        bond_all   = [t for t in recent if t.get("strategy") == "bond"]
        spike_all  = [t for t in recent if t.get("strategy") == "spike"]
        copy_all   = [t for t in recent if t.get("strategy") == "copy"]
        bond_wins  = [t for t in bond_all  if t.get("pnl", 0) > 0]
        spike_wins = [t for t in spike_all if t.get("pnl", 0) > 0]
        copy_wins  = [t for t in copy_all  if t.get("pnl", 0) > 0]

        stats = None
        stats_file = LEARN_FILE.replace(".json", "_stats.json")
        if os.path.exists(stats_file):
            with open(stats_file) as f:
                stats = json.load(f)

        return jsonify({
            "params": {
                "bond_entry":      f"{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%",
                "bond_tp":         f"{BOND_TP}%",
                "bond_sl":         f"{BOND_SL_PCT}%",
                "bond_stale_secs": BOND_STALE_SECS,
                "bond_max_secs":   BOND_MAX_SECS,
                "spike_min_age_h": SPIKE_MIN_AGE_H,
                "spike_min_1h":    SPIKE_MIN_1H,
                "spike_tp":        f"{SPIKE_TP_PCT}%",
                "spike_sl":        f"{SPIKE_SL_PCT}%",
                "spike_max_secs":  SPIKE_MAX_SECS,
                "analyze_every":   ANALYZE_EVERY,
                "bundle_mode":     BUNDLE_MODE,
            },
            "live": {
                "bond_trades":  len(bond_all),
                "bond_wins":    len(bond_wins),
                "bond_wr":      round(len(bond_wins)/max(len(bond_all),1)*100,1),
                "spike_trades": len(spike_all),
                "spike_wins":   len(spike_wins),
                "spike_wr":     round(len(spike_wins)/max(len(spike_all),1)*100,1),
                "copy_trades":  len(copy_all),
                "copy_wins":    len(copy_wins),
                "copy_wr":      round(len(copy_wins)/max(len(copy_all),1)*100,1),
                "total_trades": len(recent),
                "total_wins":   len(wins),
                "overall_wr":   round(len(wins)/max(len(recent),1)*100,1),
            },
            "last_tune": stats,
            "paper_mode": PAPER_MODE,
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/learn", methods=["GET"])
def learn():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>Strategy — Boogey's Treasure Chest</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
  *{box-sizing:border-box;margin:0;padding:0}
  :root{--acc:#b44fff;--bg:#080010;--card:#0e0018;--border:#ffffff15}
  body{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}
  .bg-art{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;object-position:center;opacity:.35;pointer-events:none;z-index:0}
  .wrap{position:relative}
  nav{display:flex;gap:0;border-bottom:2px solid var(--acc);overflow-x:auto;scrollbar-width:none}
  nav::-webkit-scrollbar{display:none}
  nav a{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;padding:10px 14px;white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;border-right:1px solid var(--border);transition:all .15s}
  nav a:hover{background:var(--acc);color:#000}
  nav a.active{background:var(--acc);color:#000}
  .page-title{font-family:'Bebas Neue',sans-serif;font-size:3rem;color:var(--acc);text-shadow:0 0 24px #b44fff88;padding:18px 16px 8px;line-height:1;letter-spacing:.04em}
  .status-bar{display:flex;flex-wrap:wrap;gap:6px;padding:0 12px 12px}
  .pill{padding:5px 12px;font-size:.68rem;font-weight:700;border:1px solid var(--border);background:var(--card);display:inline-flex;align-items:center;gap:6px;letter-spacing:.04em}
  .pill.mode{border-color:#b44fff44;background:#b44fff10;color:var(--acc)}
  .dot{width:7px;height:7px;border-radius:50%}
  .dot.purple{background:var(--acc);box-shadow:0 0 8px var(--acc)}
  .dot.green{background:#39ff14;box-shadow:0 0 8px #39ff14}
  .dot.orange{background:#ff9500;box-shadow:0 0 8px #ff9500}
  .blink{animation:blink 2s infinite}
  @keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
  .section{background:var(--card);border-top:2px solid var(--border);padding:14px;margin:0 12px 12px}
  .section.accent{border-top-color:var(--acc)}
  .section-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
  .section-hdr h2{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}
  .tune-badge{background:#b44fff20;color:var(--acc);border:1px solid #b44fff40;font-size:.62rem;font-weight:700;padding:2px 8px;letter-spacing:.04em}
  /* Win rate bars */
  .strat-row{margin-bottom:16px}
  .strat-row:last-child{margin-bottom:0}
  .strat-meta{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:5px}
  .strat-name{font-size:.8rem;font-weight:900;letter-spacing:.06em;text-transform:uppercase}
  .strat-desc{font-size:.64rem;color:#888;margin-top:1px}
  .strat-wr{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:700}
  .bar-track{height:6px;background:#ffffff0d;border-radius:3px;overflow:hidden}
  .bar-fill{height:100%;border-radius:3px;transition:width 1s ease;background:var(--acc)}
  .bar-fill.green{background:#39ff14}
  .bar-fill.orange{background:#ff9500}
  .strat-counts{display:flex;gap:12px;margin-top:5px}
  .strat-counts span{font-size:.62rem;color:#888}
  .strat-counts strong{color:#fff}
  /* Param grid */
  .param-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
  .param-card{background:#ffffff05;border:1px solid var(--border);padding:10px 12px}
  .param-card .p-lbl{font-size:.58rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.08em;margin-bottom:3px}
  .param-card .p-val{font-family:'JetBrains Mono',monospace;font-size:.9rem;font-weight:700;color:var(--acc)}
  .param-card .p-desc{font-size:.6rem;color:#666;margin-top:2px}
  .param-card.highlight{border-color:#b44fff40;background:#b44fff08}
  /* Tune block */
  .tune-block{background:#ffffff05;border:1px solid var(--border);padding:12px;margin-top:8px}
  .tune-block .tune-lbl{font-size:.58rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px}
  .tune-row{display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid #ffffff08;font-size:.72rem}
  .tune-row:last-child{border-bottom:none}
  .tune-row .k{color:#888}
  .tune-row .v{font-family:'JetBrains Mono',monospace;font-weight:700;color:#fff}
  /* Strategy desc cards */
  .strat-card{border:1px solid var(--border);padding:12px;margin-bottom:8px;background:#ffffff03}
  .strat-card:last-child{margin-bottom:0}
  .strat-card-hdr{display:flex;align-items:center;gap:8px;margin-bottom:6px}
  .strat-card-hdr .tag{font-size:.58rem;font-weight:900;letter-spacing:.1em;padding:2px 7px;border:1px solid}
  .tag.bond{color:#b44fff;border-color:#b44fff40;background:#b44fff15}
  .tag.spike{color:#ff9500;border-color:#ff950040;background:#ff950015}
  .tag.copy{color:#00f5ff;border-color:#00f5ff40;background:#00f5ff15}
  .strat-card-body{font-size:.7rem;color:#aaa;line-height:1.5}
  .mono{font-family:'JetBrains Mono',monospace;font-size:.68rem}
  .purple{color:var(--acc)}
  footer{padding:14px 16px;text-align:center;font-size:.6rem;color:#444;border-top:1px solid var(--border)}
  #last-update{font-size:.62rem;color:#888}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<div class="wrap">
  <nav>
    <a href="/">HOME</a>
    <a href="/live">LIVE</a>
    <a href="/trades">TRADES</a>
    <a href="/status">STATUS</a>
    <a href="/learn" class="active">STRATEGY</a>
    <a href="/setup">SETUP</a>
  </nav>

  <div class="page-title">STRATEGY</div>

  <div class="status-bar">
    <div class="pill mode"><span class="dot purple blink"></span><span id="mode-pill">Loading...</span></div>
    <div class="pill">Tuned every <strong id="tune-every" class="purple">--</strong> trades</div>
  </div>

  <!-- Win Rates -->
  <div class="section accent">
    <div class="section-hdr">
      <h2>Win Rates (last 60 trades)</h2>
      <span class="tune-badge" id="overall-wr">--%</span>
    </div>

    <div class="strat-row">
      <div class="strat-meta">
        <div>
          <div class="strat-name purple">Bond Runner</div>
          <div class="strat-desc">Rides bonding curve momentum</div>
        </div>
        <div class="strat-wr purple" id="bond-wr">--%</div>
      </div>
      <div class="bar-track"><div class="bar-fill" id="bond-bar" style="width:0%"></div></div>
      <div class="strat-counts">
        <span><strong id="bond-wins">-</strong> wins</span>
        <span><strong id="bond-trades">-</strong> total</span>
      </div>
    </div>

    <div class="strat-row">
      <div class="strat-meta">
        <div>
          <div class="strat-name" style="color:#ff9500">Spike Detector</div>
          <div class="strat-desc">Catches volume spikes on older tokens</div>
        </div>
        <div class="strat-wr" style="color:#ff9500" id="spike-wr">--%</div>
      </div>
      <div class="bar-track"><div class="bar-fill orange" id="spike-bar" style="width:0%"></div></div>
      <div class="strat-counts">
        <span><strong id="spike-wins">-</strong> wins</span>
        <span><strong id="spike-trades">-</strong> total</span>
      </div>
    </div>

    <div class="strat-row">
      <div class="strat-meta">
        <div>
          <div class="strat-name" style="color:#00f5ff">Copy Trader</div>
          <div class="strat-desc">Mirrors whale wallet activity</div>
        </div>
        <div class="strat-wr" style="color:#00f5ff" id="copy-wr">--%</div>
      </div>
      <div class="bar-track"><div class="bar-fill green" id="copy-bar" style="width:0%"></div></div>
      <div class="strat-counts">
        <span><strong id="copy-wins">-</strong> wins</span>
        <span><strong id="copy-trades">-</strong> total</span>
      </div>
    </div>
  </div>

  <!-- Bond Runner Params -->
  <div class="section">
    <div class="section-hdr">
      <h2>Bond Runner Params</h2>
      <span class="tune-badge purple">AUTO-TUNED</span>
    </div>
    <div class="param-grid">
      <div class="param-card highlight">
        <div class="p-lbl">Entry Range</div>
        <div class="p-val" id="p-bond-entry">--</div>
        <div class="p-desc">Bond curve %</div>
      </div>
      <div class="param-card">
        <div class="p-lbl">Take Profit</div>
        <div class="p-val" id="p-bond-tp">--</div>
        <div class="p-desc">Exit on gain</div>
      </div>
      <div class="param-card">
        <div class="p-lbl">Stop Loss</div>
        <div class="p-val" id="p-bond-sl">--</div>
        <div class="p-desc">Max drawdown</div>
      </div>
      <div class="param-card highlight">
        <div class="p-lbl">Stale Exit</div>
        <div class="p-val" id="p-bond-stale">--s</div>
        <div class="p-desc">If bond stalls</div>
      </div>
      <div class="param-card">
        <div class="p-lbl">Hard Timeout</div>
        <div class="p-val" id="p-bond-max">--s</div>
        <div class="p-desc">Max hold time</div>
      </div>
    </div>
  </div>

  <!-- Spike Detector Params -->
  <div class="section">
    <div class="section-hdr">
      <h2>Spike Detector Params</h2>
    </div>
    <div class="param-grid">
      <div class="param-card">
        <div class="p-lbl">Min Token Age</div>
        <div class="p-val" id="p-spike-age">--h</div>
        <div class="p-desc">Hours old</div>
      </div>
      <div class="param-card">
        <div class="p-lbl">Min 1h Volume</div>
        <div class="p-val" id="p-spike-vol">--</div>
        <div class="p-desc">SOL in 1 hour</div>
      </div>
      <div class="param-card highlight">
        <div class="p-lbl">Take Profit</div>
        <div class="p-val" id="p-spike-tp">--</div>
        <div class="p-desc">Exit on gain</div>
      </div>
      <div class="param-card">
        <div class="p-lbl">Stop Loss</div>
        <div class="p-val" id="p-spike-sl">--</div>
        <div class="p-desc">Max drawdown</div>
      </div>
      <div class="param-card">
        <div class="p-lbl">Hard Timeout</div>
        <div class="p-val" id="p-spike-max">--s</div>
        <div class="p-desc">Max hold time</div>
      </div>
    </div>
  </div>

  <!-- Last Auto-Tune -->
  <div class="section" id="tune-section">
    <div class="section-hdr">
      <h2>Last Auto-Tune</h2>
      <span id="tune-time" class="tune-badge">Never</span>
    </div>
    <div id="tune-body">
      <div style="text-align:center;padding:20px;color:#555;font-size:.75rem">No tune data yet — need """ + str(ANALYZE_EVERY) + """ completed trades</div>
    </div>
  </div>

  <!-- How It Works -->
  <div class="section">
    <div class="section-hdr"><h2>How It Works</h2></div>

    <div class="strat-card">
      <div class="strat-card-hdr">
        <span class="tag bond">BOND</span>
        <span style="font-size:.75rem;font-weight:700">Bond Runner</span>
      </div>
      <div class="strat-card-body">
        Enters when a token's bonding curve hits <strong id="desc-bond-entry">--</strong>.
        Rides momentum toward 100% graduation. Exits on TP, SL, stale curve, or hard timeout.
        Parameters auto-tune every <strong id="desc-tune-every">--</strong> trades.
      </div>
    </div>

    <div class="strat-card">
      <div class="strat-card-hdr">
        <span class="tag spike">SPIKE</span>
        <span style="font-size:.75rem;font-weight:700">Spike Detector</span>
      </div>
      <div class="strat-card-body">
        Targets tokens older than <strong id="desc-spike-age">--</strong>h with sudden 1h volume above
        <strong id="desc-spike-vol">--</strong> SOL. Scalps the momentum burst. High TP, tighter timeout.
      </div>
    </div>

    <div class="strat-card">
      <div class="strat-card-hdr">
        <span class="tag copy">COPY</span>
        <span style="font-size:.75rem;font-weight:700">Copy Trader</span>
      </div>
      <div class="strat-card-body">
        Mirrors buys from tracked whale wallets. Enters on the same token within seconds of a whale buy.
        Exits with trailing stop loss once position gains.
      </div>
    </div>
  </div>

  <div style="padding:0 12px 8px;text-align:right"><span id="last-update"></span></div>
  <footer>Boogey's Treasure Chest · Strategy</footer>
</div>
<script>
async function load() {
  try {
    const d = await (await fetch('/learn/api')).json();

    document.getElementById('mode-pill').textContent = d.paper_mode ? 'Paper Mode' : 'Live Mode';
    document.getElementById('tune-every').textContent = d.params.analyze_every;
    document.getElementById('overall-wr').textContent = 'Overall ' + d.live.overall_wr + '%';

    // Win rate bars
    function setStrat(prefix, wr, wins, total) {
      document.getElementById(prefix+'-wr').textContent = wr + '%';
      document.getElementById(prefix+'-bar').style.width = Math.min(wr, 100) + '%';
      document.getElementById(prefix+'-wins').textContent = wins;
      document.getElementById(prefix+'-trades').textContent = total;
    }
    setStrat('bond',  d.live.bond_wr,  d.live.bond_wins,  d.live.bond_trades);
    setStrat('spike', d.live.spike_wr, d.live.spike_wins, d.live.spike_trades);
    setStrat('copy',  d.live.copy_wr,  d.live.copy_wins,  d.live.copy_trades);

    // Bond params
    document.getElementById('p-bond-entry').textContent = d.params.bond_entry;
    document.getElementById('p-bond-tp').textContent    = d.params.bond_tp;
    document.getElementById('p-bond-sl').textContent    = d.params.bond_sl;
    document.getElementById('p-bond-stale').textContent = d.params.bond_stale_secs + 's';
    document.getElementById('p-bond-max').textContent   = d.params.bond_max_secs + 's';

    // Spike params
    document.getElementById('p-spike-age').textContent = d.params.spike_min_age_h + 'h';
    document.getElementById('p-spike-vol').textContent = d.params.spike_min_1h;
    document.getElementById('p-spike-tp').textContent  = d.params.spike_tp;
    document.getElementById('p-spike-sl').textContent  = d.params.spike_sl;
    document.getElementById('p-spike-max').textContent = d.params.spike_max_secs + 's';

    // Descriptions
    document.getElementById('desc-bond-entry').textContent = d.params.bond_entry;
    document.getElementById('desc-spike-age').textContent  = d.params.spike_min_age_h;
    document.getElementById('desc-spike-vol').textContent  = d.params.spike_min_1h;
    document.getElementById('desc-tune-every').textContent = d.params.analyze_every;

    // Last tune
    if (d.last_tune) {
      document.getElementById('tune-time').textContent = d.last_tune.tuned_at || 'Recently';
      document.getElementById('tune-body').innerHTML = `
        <div class="tune-block">
          <div class="tune-row"><span class="k">Trades Analyzed</span><span class="v">${d.last_tune.trades_analyzed}</span></div>
          <div class="tune-row"><span class="k">Overall Win Rate</span><span class="v">${d.last_tune.overall_wr}</span></div>
          <div class="tune-row"><span class="k">Bond Win Rate</span><span class="v">${d.last_tune.bond_wr}</span></div>
          <div class="tune-row"><span class="k">Spike Win Rate</span><span class="v">${d.last_tune.spike_wr}</span></div>
          <div class="tune-row"><span class="k">Bond Entry</span><span class="v">${d.last_tune.bond_entry}</span></div>
          <div class="tune-row"><span class="k">Stale Exit</span><span class="v">${d.last_tune.bond_stale_secs}s</span></div>
          <div class="tune-row"><span class="k">Stop Loss</span><span class="v">${d.last_tune.bond_sl_pct}%</span></div>
          <div class="tune-row"><span class="k">Spike TP</span><span class="v">${d.last_tune.spike_tp_pct}%</span></div>
        </div>`;
    }

    document.getElementById('last-update').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) { console.error(e); }
}
load();
setInterval(load, 10000);
</script>
</body></html>"""
    html = html.replace("Boogey's Treasure Chest", BOT_NAME)
    return html

@app.route("/setup/test-telegram", methods=["POST"])
def setup_test_telegram():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return jsonify({"ok": False, "error": "TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set in env vars"})
    try:
        r = _session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"✅ {BOT_NAME} is connected and ready!"},
            timeout=8
        )
        if r.json().get("ok"):
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": r.json().get("description", "unknown")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/setup", methods=["GET"])
def setup():
    with capital_lock:
        cap = capital
    wallet_ok   = bool(WALLET)
    tg_ok       = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)
    pct, limit  = _cap_tier(cap)
    sol_price   = get_sol_price()
    sol_display = f"${sol_price:,.2f}" if sol_price else "unavailable"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>Setup — {BOT_NAME}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{--acc:#ff6b00;--bg:#0a0500;--card:#120900;--border:#ffffff15}}
  body{{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}}
  .bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;object-position:center;opacity:.35;pointer-events:none;z-index:0}}
  .wrap{{position:relative}}
  nav{{display:flex;gap:0;border-bottom:2px solid var(--acc);overflow-x:auto;scrollbar-width:none}}
  nav::-webkit-scrollbar{{display:none}}
  nav a{{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;padding:10px 14px;white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;border-right:1px solid var(--border);transition:all .15s}}
  nav a:hover{{background:var(--acc);color:#000}}
  nav a.active{{background:var(--acc);color:#000}}
  .page-title{{font-family:'Bebas Neue',sans-serif;font-size:3rem;color:var(--acc);text-shadow:0 0 24px #ff6b0088;padding:18px 16px 8px;line-height:1;letter-spacing:.04em}}
  .section{{background:var(--card);border-top:2px solid var(--border);padding:14px;margin:0 12px 12px}}
  .section.accent{{border-top-color:var(--acc)}}
  .section-hdr{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px}}
  /* Checklist */
  .check-row{{display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border)}}
  .check-row:last-child{{border-bottom:none}}
  .check-icon{{font-size:1.1rem;width:22px;text-align:center;flex-shrink:0}}
  .check-body{{flex:1}}
  .check-title{{font-size:.8rem;font-weight:700}}
  .check-sub{{font-size:.65rem;color:#888;margin-top:2px}}
  .check-badge{{font-size:.6rem;font-weight:900;padding:2px 8px;border:1px solid;letter-spacing:.06em;flex-shrink:0}}
  .badge-ok{{color:#39ff14;border-color:#39ff1440;background:#39ff1410}}
  .badge-warn{{color:#ff6b00;border-color:#ff6b0040;background:#ff6b0010}}
  .badge-err{{color:#ff006e;border-color:#ff006e40;background:#ff006e10}}
  /* Risk cards */
  .risk-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px}}
  .risk-card{{border:1px solid var(--border);padding:10px 8px;text-align:center;cursor:pointer;transition:all .15s;background:#ffffff03}}
  .risk-card.selected{{border-color:var(--acc);background:#ff6b0015}}
  .risk-card:hover{{border-color:#ff6b0066}}
  .risk-name{{font-size:.7rem;font-weight:900;letter-spacing:.06em;text-transform:uppercase}}
  .risk-detail{{font-size:.58rem;color:#888;margin-top:4px;line-height:1.4}}
  .risk-selected-label{{font-size:.62rem;color:var(--acc);margin-top:4px;font-weight:700;display:none}}
  .risk-card.selected .risk-selected-label{{display:block}}
  /* Env var reference */
  .env-block{{background:#0a0a0a;border:1px solid var(--border);padding:10px 12px;margin-top:6px}}
  .env-row{{display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #ffffff08;gap:8px}}
  .env-row:last-child{{border-bottom:none}}
  .env-key{{font-family:'JetBrains Mono',monospace;font-size:.65rem;color:var(--acc);flex-shrink:0}}
  .env-val{{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:#888;text-align:right;word-break:break-all}}
  .env-set{{color:#39ff14}}
  .env-unset{{color:#ff006e}}
  /* Buttons */
  .btn{{display:block;width:100%;padding:12px;font-size:.78rem;font-weight:900;letter-spacing:.08em;text-transform:uppercase;border:2px solid var(--acc);background:var(--acc);color:#000;cursor:pointer;transition:all .15s;margin-top:8px;text-decoration:none;text-align:center}}
  .btn:hover{{background:transparent;color:var(--acc)}}
  .btn-ghost{{background:transparent;color:var(--acc)}}
  .btn-ghost:hover{{background:var(--acc);color:#000}}
  .result-msg{{font-size:.72rem;padding:8px 12px;margin-top:6px;display:none}}
  .result-msg.ok{{background:#39ff1415;border:1px solid #39ff1440;color:#39ff14}}
  .result-msg.err{{background:#ff006e15;border:1px solid #ff006e40;color:#ff006e}}
  footer{{padding:14px 16px;text-align:center;font-size:.6rem;color:#444;border-top:1px solid var(--border)}}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<div class="wrap">
  <nav>
    <a href="/">HOME</a>
    <a href="/live">LIVE</a>
    <a href="/trades">TRADES</a>
    <a href="/status">STATUS</a>
    <a href="/learn">STRATEGY</a>
    <a href="/setup" class="active">SETUP</a>
  </nav>

  <div class="page-title">SETUP</div>

  <!-- Status Checklist -->
  <div class="section accent">
    <div class="section-hdr">Configuration Status</div>

    <div class="check-row">
      <span class="check-icon">{'✅' if not PAPER_MODE else '🟡'}</span>
      <div class="check-body">
        <div class="check-title">Trading Mode</div>
        <div class="check-sub">{'Live trading active' if not PAPER_MODE else 'Paper mode — set WALLET + WALLET_PRIVATE_KEY to go live'}</div>
      </div>
      <span class="check-badge {'badge-ok' if not PAPER_MODE else 'badge-warn'}">{'LIVE' if not PAPER_MODE else 'PAPER'}</span>
    </div>

    <div class="check-row">
      <span class="check-icon">{'✅' if wallet_ok else '❌'}</span>
      <div class="check-body">
        <div class="check-title">Phantom Wallet</div>
        <div class="check-sub">{'Connected: ' + WALLET[:8] + '...' + WALLET[-4:] if wallet_ok else 'Set WALLET env var in Railway'}</div>
      </div>
      <span class="check-badge {'badge-ok' if wallet_ok else 'badge-err'}">{'SET' if wallet_ok else 'MISSING'}</span>
    </div>

    <div class="check-row">
      <span class="check-icon">{'✅' if tg_ok else '❌'}</span>
      <div class="check-body">
        <div class="check-title">Telegram Notifications</div>
        <div class="check-sub">{'Token + Chat ID configured' if tg_ok else 'Set TELEGRAM_TOKEN + TELEGRAM_CHAT_ID'}</div>
      </div>
      <span class="check-badge {'badge-ok' if tg_ok else 'badge-err'}">{'SET' if tg_ok else 'MISSING'}</span>
    </div>

    <div class="check-row">
      <span class="check-icon">✅</span>
      <div class="check-body">
        <div class="check-title">Capital</div>
        <div class="check-sub">Starting: ${STARTING_CAPITAL:.2f} · Current: ${cap:.2f} · Goal: ${PROFIT_GOAL:,.0f}</div>
      </div>
      <span class="check-badge badge-ok">SET</span>
    </div>

    <div class="check-row">
      <span class="check-icon">✅</span>
      <div class="check-body">
        <div class="check-title">SOL Price</div>
        <div class="check-sub">Live feed: {sol_display}</div>
      </div>
      <span class="check-badge badge-ok">LIVE</span>
    </div>

    <div class="check-row">
      <span class="check-icon">{'✅' if GMGN_API_KEY else '🟡'}</span>
      <div class="check-body">
        <div class="check-title">GMGN API Key</div>
        <div class="check-sub">{'Smart money signals + KOL tracking enabled' if GMGN_API_KEY else 'Optional — set GMGN_API_KEY for KOL tracking; smart money signals work without it'}</div>
      </div>
      <span class="check-badge {'badge-ok' if GMGN_API_KEY else 'badge-warn'}">{'SET' if GMGN_API_KEY else 'OPTIONAL'}</span>
    </div>

    {'<button class="btn" style="margin-top:12px" onclick="testTelegram()">📱 Test Telegram Connection</button><div class="result-msg" id="tg-result"></div>' if tg_ok else '<div style="font-size:.68rem;color:#888;margin-top:10px;padding:8px;border:1px solid var(--border)">Add TELEGRAM_TOKEN and TELEGRAM_CHAT_ID to Railway env vars to enable notifications.</div>'}
  </div>

  <!-- Risk Level -->
  <div class="section">
    <div class="section-hdr">Risk Level — currently <span style="color:var(--acc);text-transform:uppercase">{RISK_LEVEL}</span></div>
    <div class="risk-grid">
      <div class="risk-card {'selected' if RISK_LEVEL == 'conservative' else ''}">
        <div class="risk-name" style="color:#39ff14">Conservative</div>
        <div class="risk-detail">5–12% per trade<br>8–15 trades/day<br>Slower growth, lower risk</div>
        <div class="risk-selected-label">▲ ACTIVE</div>
      </div>
      <div class="risk-card {'selected' if RISK_LEVEL == 'standard' else ''}">
        <div class="risk-name" style="color:var(--acc)">Standard</div>
        <div class="risk-detail">8–18% per trade<br>12–20 trades/day<br>Balanced default</div>
        <div class="risk-selected-label">▲ ACTIVE</div>
      </div>
      <div class="risk-card {'selected' if RISK_LEVEL == 'aggressive' else ''}">
        <div class="risk-name" style="color:#ff006e">Aggressive</div>
        <div class="risk-detail">12–22% per trade<br>15–25 trades/day<br>Faster growth, higher risk</div>
        <div class="risk-selected-label">▲ ACTIVE</div>
      </div>
    </div>
    <div style="font-size:.62rem;color:#888;margin-top:8px">Change by setting <span style="color:var(--acc);font-family:monospace">RISK_LEVEL</span> env var to <code>conservative</code>, <code>standard</code>, or <code>aggressive</code> in Railway.</div>
  </div>

  <!-- Railway Env Var Reference -->
  <div class="section">
    <div class="section-hdr">Railway Environment Variables</div>
    <div class="env-block">
      <div class="env-row">
        <span class="env-key">WALLET</span>
        <span class="env-val {'env-set' if wallet_ok else 'env-unset'}">{'set ✓' if wallet_ok else 'not set'}</span>
      </div>
      <div class="env-row">
        <span class="env-key">WALLET_PRIVATE_KEY</span>
        <span class="env-val {'env-set' if WALLET_PRIVATE_KEY else 'env-unset'}">{'set ✓' if WALLET_PRIVATE_KEY else 'not set'}</span>
      </div>
      <div class="env-row">
        <span class="env-key">PAPER_MODE</span>
        <span class="env-val">{'true' if PAPER_MODE else 'false'}</span>
      </div>
      <div class="env-row">
        <span class="env-key">STARTING_CAPITAL</span>
        <span class="env-val">{STARTING_CAPITAL:.2f}</span>
      </div>
      <div class="env-row">
        <span class="env-key">PROFIT_GOAL</span>
        <span class="env-val">{PROFIT_GOAL:.0f}</span>
      </div>
      <div class="env-row">
        <span class="env-key">RISK_LEVEL</span>
        <span class="env-val">{RISK_LEVEL}</span>
      </div>
      <div class="env-row">
        <span class="env-key">TELEGRAM_TOKEN</span>
        <span class="env-val {'env-set' if TELEGRAM_TOKEN else 'env-unset'}">{'set ✓' if TELEGRAM_TOKEN else 'not set'}</span>
      </div>
      <div class="env-row">
        <span class="env-key">TELEGRAM_CHAT_ID</span>
        <span class="env-val {'env-set' if TELEGRAM_CHAT_ID else 'env-unset'}">{'set ✓' if TELEGRAM_CHAT_ID else 'not set'}</span>
      </div>
      <div class="env-row">
        <span class="env-key">GMGN_API_KEY</span>
        <span class="env-val {'env-set' if GMGN_API_KEY else 'env-unset'}">{'set ✓' if GMGN_API_KEY else 'not set (optional)'}</span>
      </div>
    </div>
    <a href="https://railway.app" target="_blank" class="btn btn-ghost" style="margin-top:10px">Open Railway Dashboard →</a>
  </div>

  <!-- Quick Links -->
  <div class="section">
    <div class="section-hdr">Quick Links</div>
    <a href="/" class="btn btn-ghost" style="margin-bottom:6px">Dashboard →</a>
    <a href="/status" class="btn btn-ghost" style="margin-bottom:6px">Bot Status →</a>
    <a href="/learn" class="btn btn-ghost">Strategy →</a>
  </div>

  <footer>{BOT_NAME} · Setup</footer>
</div>
<script>
async function testTelegram() {{
  const btn = document.querySelector('button');
  const msg = document.getElementById('tg-result');
  btn.disabled = true;
  btn.textContent = 'Testing...';
  try {{
    const r = await fetch('/setup/test-telegram', {{method:'POST'}});
    const d = await r.json();
    msg.style.display = 'block';
    if (d.ok) {{
      msg.className = 'result-msg ok';
      msg.textContent = '✅ Telegram connected! Check your chat for a test message.';
    }} else {{
      msg.className = 'result-msg err';
      msg.textContent = '❌ ' + (d.error || 'Failed');
    }}
  }} catch(e) {{
    msg.style.display = 'block';
    msg.className = 'result-msg err';
    msg.textContent = '❌ Request failed';
  }}
  btn.disabled = false;
  btn.textContent = '📱 Test Telegram Connection';
}}
</script>
</body></html>"""
    return html

@app.route("/wallets", methods=["GET"])
def wallets_status():
    """Shows which wallets are being tracked — use this to confirm TRACKED_WALLETS is set correctly."""
    with _copy_lock:
        discovered = list(_copy_wallets)
    return jsonify({
        "tracked_wallets": {
            "count":     len(TRACKED_WALLETS),
            "addresses": TRACKED_WALLETS,
            "hint":      "Set TRACKED_WALLETS=addr1,addr2,addr3 in Railway env vars for THIS service (sniper bot)",
        },
        "gmgn_discovered": {
            "count":     len(discovered),
            "wallets":   [{"address": w["address"], "winrate": w["winrate"]} for w in discovered],
            "status":    "OK" if discovered else "No wallets fetched yet — GMGN rank may be rate-limited",
        },
        "total_watching": len(TRACKED_WALLETS) + len(discovered),
        "copy_trade_on":  COPY_TRADE,
    })

@app.route("/blacklist/<mint>", methods=["GET"])
def blacklist_route(mint):
    blacklisted_mints.add(mint)
    return jsonify({"blacklisted": mint})

@app.route("/telegram_setup", methods=["GET"])
def telegram_setup():
    result = {
        "token_set":   bool(TELEGRAM_TOKEN),
        "chat_id_set": bool(TELEGRAM_CHAT_ID),
        "token_preview": (TELEGRAM_TOKEN[:10] + "...") if TELEGRAM_TOKEN else "NOT SET",
        "chat_id": TELEGRAM_CHAT_ID or "NOT SET",
    }
    if not TELEGRAM_TOKEN:
        result["step"] = "Set TELEGRAM_TOKEN in Railway env vars. Get it from @BotFather → /mybots → your bot → API Token"
        return jsonify(result)
    # Test 1: verify token with getMe
    try:
        r = _session.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe", timeout=8)
        data = r.json()
        if not data.get("ok"):
            result["token_valid"] = False
            result["telegram_error"] = data.get("description", "unknown")
            result["step"] = "Token is INVALID. Go to @BotFather → /mybots → your bot → API Token and get a fresh token"
            return jsonify(result)
        result["token_valid"] = True
        result["bot_username"] = data["result"]["username"]
    except Exception as e:
        result["token_valid"] = False
        result["error"] = str(e)
        return jsonify(result)
    if not TELEGRAM_CHAT_ID:
        result["step"] = (
            f"Token is VALID! Bot username: @{result['bot_username']}. "
            "Now get your chat ID: open Telegram, send your bot any message (like 'hi'), "
            f"then visit: https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates "
            "and look for 'chat':{{'id': 123456789}} — set that number as TELEGRAM_CHAT_ID in Railway"
        )
        return jsonify(result)
    # Test 2: send a real test message
    try:
        r = _session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": "✅ Bot notifications are working!", "parse_mode": "Markdown"},
            timeout=8
        )
        data = r.json()
        if data.get("ok"):
            result["message_sent"] = True
            result["step"] = "SUCCESS! Check your Telegram — you should have a test message."
        else:
            result["message_sent"] = False
            result["telegram_error"] = data.get("description", "unknown")
            result["step"] = "Token valid but message failed. Chat ID is probably wrong — send your bot a message first, then re-check getUpdates for the correct chat id number."
    except Exception as e:
        result["message_sent"] = False
        result["error"] = str(e)
    return jsonify(result)

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

    _load_daily_state()

    threading.Thread(target=_notify_worker,    daemon=True).start()
    threading.Thread(target=monitor_loop,      daemon=True).start()
    threading.Thread(target=scanner_loop,      daemon=True).start()
    threading.Thread(target=daily_summary_loop, daemon=True).start()
    if COPY_TRADE:
        threading.Thread(target=copy_trade_loop, daemon=True).start()
    t_signals = threading.Thread(target=run_signal_refresh_loop, daemon=True)
    t_signals.start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", "=" * 55)
    log("ok", f"Mode      : {'PAPER' if PAPER_MODE else 'LIVE'}")
    log("ok", f"Wallet    : {WALLET[:8] if WALLET else 'NOT SET'}...")
    log("ok", f"Capital   : ${capital:.2f} USDC (trading budget)")
    log("ok", f"SOL wallet: ${SOL_ALLOCATED:.2f} funded for on-chain execution")
    log("ok", f"Trade size: ${trade_size():.2f} fixed")
    with capital_lock:
        _cap = capital
    _pct, _limit = _cap_tier(_cap)
    log("ok", f"Capital: ${_cap:.2f} | Trade size: {_pct*100:.0f}% (${trade_size():.2f}) | Daily cap: {_limit} trades | Max daily loss: {MAX_DAILY_LOSS_PCT:.0f}%")
    log("ok", f"Copy trade: {'ON' if COPY_TRADE else 'OFF'} | WR {COPY_WINRATE_MIN}-{COPY_WINRATE_MAX}% | top {COPY_MAX_WALLETS} wallets")
    if TRACKED_WALLETS:
        log("ok", f"Tracked wallets ({len(TRACKED_WALLETS)}): {', '.join(w[:8]+'...' for w in TRACKED_WALLETS)}")
    else:
        log("warn", "TRACKED_WALLETS not set — add wallet addresses in Railway env vars")
    log("ok", f"USDC lock : activates at ${USDC_LOCK_THRESHOLD:.0f} capital")
    log("ok", "=" * 55)
    app.run(host="0.0.0.0", port=port, use_reloader=False)
