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

# Position sizing — capital-tiered (protects small accounts)
MIN_TRADE         = float(os.environ.get("MIN_TRADE",   "3"))
MAX_TRADE         = float(os.environ.get("MAX_TRADE",   "500"))
FIXED_TRADE_SIZE  = float(os.environ.get("FIXED_TRADE_SIZE", "0"))  # 0 = use tiered %

# Capital tiers: (min_capital, trade_pct, daily_max_trades)
_CAP_TIERS = [
    (5_000, 0.18, 20),
    (  500, 0.15, 15),
    (  100, 0.12, 12),
    (    0, 0.08, 12),
]

MAX_DAILY_LOSS_PCT = float(os.environ.get("MAX_DAILY_LOSS_PCT", "20"))  # stop day if down >20% of start capital

# Risk limits
DAILY_LOSS_MAX    = int(os.environ.get("DAILY_LOSS_MAX",  "3"))   # retune after N consecutive losses
LOSS_COOLDOWN_HRS = float(os.environ.get("LOSS_COOLDOWN_HRS", "0.5")) # 30-min pause then resume
ANALYZE_EVERY     = int(os.environ.get("ANALYZE_EVERY",   "5"))   # retune every 5 trades for faster learning

# Bond Runner strategy
BOND_ENTRY_MIN  = float(os.environ.get("BOND_ENTRY_MIN", "58"))
BOND_ENTRY_MAX  = float(os.environ.get("BOND_ENTRY_MAX", "63"))
BOND_TP         = float(os.environ.get("BOND_TP",        "67"))
BOND_SL_PCT     = float(os.environ.get("BOND_SL_PCT",    "10"))
BOND_MAX_SECS   = int(os.environ.get("BOND_MAX_SECS",    "240"))   # 4 min hard cap
BOND_STALE_SECS = int(os.environ.get("BOND_STALE_SECS",  "120"))   # exit if bond hasn't moved in 2 min

# Dormant Spike strategy
SPIKE_MIN_AGE_H = float(os.environ.get("SPIKE_MIN_AGE_H", "12"))
SPIKE_MIN_1H    = float(os.environ.get("SPIKE_MIN_1H",    "100"))
SPIKE_TP_PCT    = float(os.environ.get("SPIKE_TP_PCT",    "40"))
SPIKE_SL_PCT    = float(os.environ.get("SPIKE_SL_PCT",    "15"))
SPIKE_MAX_SECS  = int(os.environ.get("SPIKE_MAX_SECS",    "180"))   # 3 min hard cap

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
COPY_MAX_AGE_SECS = int(os.environ.get("COPY_MAX_AGE_SECS",  "60"))   # ignore trades older than 60s
COPY_REFRESH_MINS = int(os.environ.get("COPY_REFRESH_MINS",  "60"))   # refresh wallet list hourly
COPY_TP_PCT       = float(os.environ.get("COPY_TP_PCT",       "40"))
COPY_SL_PCT       = float(os.environ.get("COPY_SL_PCT",       "15"))
COPY_MAX_SECS     = int(os.environ.get("COPY_MAX_SECS",       "180"))
GMGN_RANK         = "https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d"
GMGN_ACTIVITY     = "https://gmgn.ai/defi/quotation/v1/wallet_activity/sol"

# Notifications
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
NTFY_TOPIC       = os.environ.get("NTFY_TOPIC", "")

# Social / quality gates
MIN_REPLIES  = int(os.environ.get("MIN_REPLIES",  "10"))
MIN_LIQ      = float(os.environ.get("MIN_LIQ",    "500"))

# General
MAX_OPEN      = int(os.environ.get("MAX_OPEN",      "4"))
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", "10"))

SOL_RPC     = "https://api.mainnet-beta.solana.com"
PUMPPORTAL  = "https://pumpportal.fun/api/trade-local"
LEARN_FILE  = "/tmp/bot_learn.json"
STATE_FILE  = "/tmp/bot_state.json"
WEEK_FILE   = "/tmp/bot_week.json"

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
usdc_locked       = 0.0
usdc_lock         = threading.Lock()
# Daily tracking — resets at midnight
_daily_date       = ""
_daily_trades     = 0
_daily_wins       = 0
_daily_losses     = 0
_day_start_cap    = 0.0    # capital at start of day — used for daily loss % guard
_pause_until      = 0.0    # Unix timestamp — bot pauses trading until this time
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
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "date":         _daily_date,
                "trades":       _daily_trades,
                "wins":         _daily_wins,
                "losses":       _daily_losses,
                "pause_until":  _pause_until,
                "capital":      capital,
                "week_start":   _week_start_date,
                "week_logs":    _week_day_logs,
            }, f)
    except Exception:
        pass

def _load_daily_state():
    global _daily_date, _daily_trades, _daily_wins, _daily_losses
    global _pause_until, capital, _week_start_date, _week_day_logs
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                s = json.load(f)
            today = time.strftime("%Y-%m-%d")
            if s.get("date") == today:
                _daily_date   = s["date"]
                _daily_trades = s.get("trades",      0)
                _daily_wins   = s.get("wins",        0)
                _daily_losses = s.get("losses",      0)
                _pause_until  = s.get("pause_until", 0.0)
                capital       = s.get("capital",     capital)
            _week_start_date  = s.get("week_start", "")
            _week_day_logs    = s.get("week_logs",  [])
            paused_msg = f" | paused until {time.strftime('%H:%M', time.localtime(_pause_until))}" if _pause_until > time.time() else ""
            log("ok", f"Restored: {_daily_trades} trades | {_daily_wins}W {_daily_losses}L | cap=${capital:.2f}{paused_msg}")
    except Exception as e:
        log("warn", f"State load: {e}")

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
            _day_start_cap = capital  # snapshot for daily loss % guard
            limit = daily_trade_limit()
            with capital_lock:
                cap = capital
            pct, _ = _cap_tier(cap)
            log("ok", f"New day {today} | Day {len(_week_day_logs)+1} | cap=${cap:.2f} | trade={pct*100:.0f}% (${trade_size():.2f}) | limit={limit}/day")
            _save_daily_state()

def daily_limit_reached():
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
            return True
        # Max daily loss guard — stop if down >MAX_DAILY_LOSS_PCT% from today's open
        if _day_start_cap > 0:
            with capital_lock:
                cap_now = capital
            loss_pct = (_day_start_cap - cap_now) / _day_start_cap * 100
            if loss_pct >= MAX_DAILY_LOSS_PCT:
                log("warn", f"Daily loss guard: down {loss_pct:.1f}% today (${_day_start_cap - cap_now:.2f}) — stopping until tomorrow")
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
        total_pnl = sum(t["pnl"] for t in completed_trades)
        wr = round(_daily_wins / max(_daily_trades, 1) * 100, 1)
        exit_counts = {}
        for t in completed_trades:
            r = t.get("result", "?")
            exit_counts[r] = exit_counts.get(r, 0) + 1
        exit_str = " | ".join(f"{k}:{v}" for k, v in sorted(exit_counts.items(), key=lambda x: -x[1])[:4])
        day_num = len(_week_day_logs) + 1
        msg = (f"Day {day_num} running\n"
               f"Trades: {_daily_trades} | {_daily_wins}W {_daily_losses}L ({wr}% WR)\n"
               f"PnL today: ${total_pnl:+.2f}\n"
               f"Capital: ${cap:.2f} → goal $100,000\n"
               f"Exits: {exit_str or 'none yet'}\n"
               f"Bond range: {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%")
        log("ok", f"Daily summary: {_daily_wins}W/{_daily_losses}L cap=${cap:.2f}", "DAY")
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
        with open(LEARN_FILE, "w") as f:
            json.dump(history[-200:], f)
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
    global capital, _daily_trades
    if daily_limit_reached():
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

    tx = execute_buy(mint, symbol, amount)
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
            _gmgn_backoff = time.time() + 3600  # back off 1 hour on 403
            log("warn", "GMGN rank blocked (403) — copy trading paused 1h", "COPY")
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
                        market = get_market_data(mint)
                        if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                            continue
                        with _copy_lock:
                            _copied_mints[mint] = time.time()
                        amt = trade_size()
                        log("ok", f"COPY {addr[:8]}... WR:{w['winrate']}% | ${amt:.2f}", symbol)
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
                            enter_trade(mint, symbol, market["price"], amt, "bundle", bond, 0)
                            time.sleep(0.5)
                            continue

                # ── Bond Runner ────────────────────────────────────────
                if BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX:
                    details = get_bonding_details(mint)
                    if details:
                        bond = details["bond_pct"]
                        if details.get("complete"):
                            log("info", f"BOND SKIP: already graduated bond={bond:.1f}%", symbol)
                            continue
                    if not (BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX):
                        log("info", f"BOND SKIP: bond moved to {bond:.1f}% (range {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%)", symbol)
                        continue

                    rug = run_rugcheck(mint)
                    if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                        log("warn", f"BOND SKIP: mint/freeze auth rug={rug}", symbol)
                        blacklisted_mints.add(mint)
                        continue
                    if rug and rug.get("is_bundled") and BUNDLE_MODE == "avoid":
                        log("warn", f"BOND SKIP: bundled", symbol)
                        continue

                    market = get_market_data(mint)
                    if not market:
                        log("info", f"BOND SKIP: no market data (DexScreener not indexed yet)", symbol)
                        continue
                    if market["price"] <= 0:
                        log("info", f"BOND SKIP: price=0", symbol)
                        continue
                    # Skip liquidity check for bond runner — bonding curve IS the liquidity

                    amt = trade_size()
                    log("ok", f"BOND RUNNER | bond={bond:.1f}%", symbol)
                    enter_trade(mint, symbol, market["price"], amt, "bond", bond, 0)
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
                        enter_trade(mint, symbol, market["price"], amt, "spike", bond, 0)
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
<title>Boogey's Treasure Chest</title>
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
    <p class="tagline">Autonomous pump.fun sniper &nbsp;·&nbsp; goal $25,000</p>
    <div class="mode-pill"><span class="dot"></span>{mode} MODE</div>
  </header>

  <nav>
    <a href="/live">⚡ Live Feed</a>
    <a href="/trades">📋 All Trades</a>
    <a href="/status">📊 Status</a>
    <a href="/learn">🧠 Strategy</a>
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
      <div class="sub">Started at $39.67</div>
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
<title>Boogey's Treasure Chest</title>
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
    <a href="https://pump.fun" target="_blank">🚀 PUMP</a>
    <a href="https://solscan.io" target="_blank">🔍 SCAN</a>
  </nav>
  <div class="mode-strip">
    <span class="left">Refreshes 30s · Goal $25k</span>
    <span class="mode-pill">{mode}</span>
  </div>
  <div class="hero">
    <div class="hero-lbl">Current Capital</div>
    <div class="hero-cap">${cap:.2f}</div>
    <div class="hero-pnl">{sign}${pnl:.2f} total PnL</div>
    <div class="hero-sub">Started $39.67 &nbsp;·&nbsp; {total} trades closed</div>
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
  <footer>Boogey's Treasure Chest &nbsp;·&nbsp;
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
    return jsonify({
        "capital": round(cap, 2), "paper_mode": PAPER_MODE,
        "open_trades": len(open_trades), "total_trades": total,
        "wins": len(wins), "losses": total - len(wins),
        "win_rate": round(len(wins) / max(total, 1) * 100, 1),
        "total_pnl": round(pnl, 4),
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
        cap_c = "#4ade80" if d["capital"] >= 39.67 else "#f87171"
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
<title>Status — Boogey's Treasure Chest</title>
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
      <div class="sub">Started $39.67</div>
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
      <span class="row-val">{DAILY_LOSS_MAX} losses → {LOSS_COOLDOWN_HRS}h pause + retune</span></div>
    <div class="row"><span class="row-key">Bond Entry Range</span>
      <span class="row-val">{BOND_ENTRY_MIN}% – {BOND_ENTRY_MAX}%</span></div>
    <div class="row"><span class="row-key">Bond Take Profit</span>
      <span class="row-val" style="color:#39ff14">{BOND_TP}%</span></div>
    <div class="row"><span class="row-key">Stop Loss</span>
      <span class="row-val" style="color:#ff006e">{BOND_SL_PCT}% · Trailing SL at +{TSL_ACTIVATE_PCT}%</span></div>
    <div class="row"><span class="row-key">Retune Interval</span>
      <span class="row-val">Every {ANALYZE_EVERY} trades</span></div>
  </div>

  {"" if not _week_day_logs else f'''<div class="section" style="margin:0 12px 12px">
    <div class="section-hdr" style="font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px">DAY-BY-DAY LOG</div>
    <div class="table-wrap"><table>
      <thead><tr><th>Date</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Win Rate</th><th>Capital</th></tr></thead>
      <tbody>{day_rows}</tbody>
    </table></div>
  </div>'''}

  <footer>Boogey's Treasure Chest · Refreshes every 20s</footer>
</div></body></html>"""
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
<title>Trades — Boogey's Treasure Chest</title>
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

  <footer>Boogey's Treasure Chest · Refreshes every 30s</footer>
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
    return jsonify({
        "ts":       round(time.time()),
        "capital":  round(cap, 2),
        "open":     open_now,
        "closed":   recent_closed,
        "scanning": scan_active,
        "paused":   _pause_until > time.time(),
        "today":    {"trades": _daily_trades, "wins": _daily_wins, "losses": _daily_losses},
    })

@app.route("/live", methods=["GET"])
def live():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>Live Feed — Boogey's Treasure Chest</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
  *{box-sizing:border-box;margin:0;padding:0}
  :root{--acc:#00f5ff;--bg:#0a0008;--card:#110010;--border:#ffffff15}
  body{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}
  .bg-art{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;object-position:center;opacity:.35;pointer-events:none;z-index:0}
  .wrap{position:relative}
  nav{display:flex;gap:0;border-bottom:2px solid var(--acc);overflow-x:auto;scrollbar-width:none}
  nav::-webkit-scrollbar{display:none}
  nav a{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;padding:10px 14px;white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;border-right:1px solid var(--border);transition:all .15s}
  nav a:hover{background:var(--acc);color:#000}
  nav a.active{background:var(--acc);color:#000}
  .page-title{font-family:'Bebas Neue',sans-serif;font-size:3rem;color:var(--acc);text-shadow:0 0 24px #00f5ff88;padding:18px 16px 8px;line-height:1;letter-spacing:.04em}
  .status-bar{display:flex;flex-wrap:wrap;gap:6px;padding:0 12px 12px}
  .pill{padding:5px 12px;font-size:.68rem;font-weight:700;border:1px solid var(--border);background:var(--card);display:inline-flex;align-items:center;gap:6px;letter-spacing:.04em}
  .dot{width:7px;height:7px;border-radius:50%}
  .dot.green{background:#39ff14;box-shadow:0 0 8px #39ff14}
  .dot.red{background:#ff006e;box-shadow:0 0 8px #ff006e}
  .dot.cyan{background:var(--acc);box-shadow:0 0 8px var(--acc)}
  .blink{animation:blink 1s infinite}
  @keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
  .cap-display{text-align:center;padding:20px 16px;background:var(--card);border-top:2px solid var(--acc);border-bottom:2px solid var(--border);margin-bottom:12px}
  .cap-display .lbl{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.15em;margin-bottom:4px}
  .cap-display .amount{font-family:'Bebas Neue',sans-serif;font-size:4rem;color:var(--acc);text-shadow:0 0 30px #00f5ff66;letter-spacing:.02em;line-height:1}
  .cap-display .sub{font-size:.68rem;color:#888;margin-top:6px}
  .section{background:var(--card);border-top:2px solid var(--border);padding:14px;margin:0 12px 12px}
  .section-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
  .section-hdr h2{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}
  .count-badge{background:#00f5ff20;color:var(--acc);border:1px solid #00f5ff40;font-size:.62rem;font-weight:700;padding:2px 8px;letter-spacing:.04em}
  .live-dot{display:inline-block;width:6px;height:6px;background:#39ff14;border-radius:50%;margin-right:4px;animation:blink 1s infinite;box-shadow:0 0 6px #39ff14}
  .table-wrap{overflow-x:auto;border:1px solid var(--border)}
  table{width:100%;border-collapse:collapse;font-size:.72rem}
  th{padding:8px 10px;font-size:.56rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.08em;text-align:left;border-bottom:1px solid var(--border);white-space:nowrap;background:#0d000c}
  td{padding:9px 10px;border-bottom:1px solid #ffffff05;vertical-align:middle}
  tr:last-child td{border-bottom:none}
  tr.new-row{animation:flash .8s ease-out}
  @keyframes flash{0%{background:#00f5ff18}100%{background:transparent}}
  .timer{font-family:'JetBrains Mono',monospace;font-size:.8rem;color:var(--acc);font-weight:600}
  .mono{font-family:'JetBrains Mono',monospace;font-size:.68rem}
  .sym{font-weight:900;font-size:.8rem}
  .muted{color:#888}
  .green{color:#39ff14} .red{color:#ff006e} .cyan{color:var(--acc)}
  .badge{display:inline-block;padding:2px 6px;font-size:.56rem;font-weight:900;letter-spacing:.06em;border:1px solid}
  .badge.win{color:#39ff14;border-color:#39ff14;background:#39ff1415}
  .badge.loss{color:#ff006e;border-color:#ff006e;background:#ff006e15}
  .badge.strat{color:#00f5ff;border-color:#00f5ff;background:#00f5ff12}
  .badge.open{color:var(--acc);border-color:var(--acc);background:#00f5ff15}
  .feed{display:flex;flex-direction:column;gap:6px;max-height:400px;overflow-y:auto}
  .feed-item{display:flex;align-items:center;gap:10px;padding:10px 12px;border:1px solid var(--border);background:#0d000c;font-size:.75rem;animation:flash .8s ease-out}
  .feed-item.buy{border-color:#00f5ff30;background:#00f5ff08}
  .feed-item.win{border-color:#39ff1430;background:#39ff1408}
  .feed-item.loss{border-color:#ff006e30;background:#ff006e08}
  .feed-ts{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:#888;white-space:nowrap;min-width:56px}
  .feed-icon{font-size:1rem;width:20px;text-align:center}
  .feed-body{flex:1}
  .feed-sym{font-weight:900;margin-right:6px}
  .feed-detail{color:#888;font-size:.68rem;margin-top:1px}
  .feed-pnl{font-family:'JetBrains Mono',monospace;font-weight:700;white-space:nowrap}
  .empty{text-align:center;padding:28px;color:#888;font-size:.78rem}
  #last-update{font-size:.62rem;color:#888}
  footer{padding:14px 16px;text-align:center;font-size:.6rem;color:#444;border-top:1px solid var(--border)}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<div class="wrap">
  <nav>
    <a href="/">HOME</a>
    <a href="/live" class="active">LIVE</a>
    <a href="/trades">TRADES</a>
    <a href="/status">STATUS</a>
    <a href="/learn">STRATEGY</a>
  </nav>

  <div class="page-title">LIVE FEED</div>

  <div class="status-bar">
    <div class="pill"><span class="dot green blink" id="scan-dot"></span><span id="scan-label">Scanning</span></div>
    <div class="pill">Capital: <strong id="cap-pill" class="cyan">$--</strong></div>
    <div class="pill">Today: <span id="today-pill">--</span></div>
    <div class="pill">Open: <span id="open-count" class="cyan">0</span></div>
  </div>

  <div class="cap-display">
    <div class="lbl">Current Capital</div>
    <div class="amount" id="cap-big">$---.--</div>
    <div class="sub">Started at $39.67 &nbsp;·&nbsp; Goal: $25,000</div>
  </div>

  <div class="section" id="open-section">
    <div class="section-hdr">
      <h2><span class="live-dot"></span>Open Trades</h2>
      <span class="count-badge" id="open-badge">0 active</span>
    </div>
    <div id="open-body">
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>Symbol</th><th>Strategy</th><th>Size</th>
            <th>Bond In</th><th>Bond High</th><th>Elapsed</th>
          </tr></thead>
          <tbody id="open-rows"><tr><td colspan="6" class="empty">No open trades — scanning...</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-hdr">
      <h2>TRADE EVENTS</h2>
      <a href="/trades" style="font-size:.68rem;color:var(--acc);text-decoration:none;font-weight:700;letter-spacing:.06em">FULL HISTORY →</a>
    </div>
    <div class="feed" id="feed">
      <div class="empty">Waiting for trades...</div>
    </div>
  </div>

  <div style="padding:0 12px 8px;text-align:right"><span id="last-update"></span></div>
  <footer>Boogey's Treasure Chest · Live</footer>
</div>
<script>
let seenIds = new Set();
let openTimers = {};

function fmt(n, dec=4) { return (n>=0?'+':'')+n.toFixed(dec); }
function fmtTime(ts) {
  const d = new Date(ts*1000);
  return d.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});
}
function elapsed(s) {
  if (s < 60) return s.toFixed(0)+'s';
  return Math.floor(s/60)+'m '+(s%60).toFixed(0)+'s';
}

function tick() {
  // Update elapsed timers for open trades
  document.querySelectorAll('.elapsed-cell').forEach(el => {
    const start = parseInt(el.dataset.start);
    el.textContent = elapsed(Date.now()/1000 - start);
  });
}
setInterval(tick, 1000);

async function poll() {
  try {
    const r = await fetch('/live/api');
    const d = await r.json();

    // Capital
    document.getElementById('cap-big').textContent = '$'+d.capital.toFixed(2);
    document.getElementById('cap-pill').textContent = '$'+d.capital.toFixed(2);
    document.getElementById('today-pill').textContent =
      d.today.trades+'T '+d.today.wins+'W '+d.today.losses+'L';
    document.getElementById('open-count').textContent = d.open.length;
    document.getElementById('open-badge').textContent = d.open.length+' active';

    // Scan status
    const dot = document.getElementById('scan-dot');
    const lbl = document.getElementById('scan-label');
    if (d.paused) {
      dot.className='dot red blink'; lbl.textContent='Cooling Down';
    } else if (d.scanning) {
      dot.className='dot green blink'; lbl.textContent='Scanning';
    } else {
      dot.className='dot red'; lbl.textContent='Halted';
    }

    // Open trades table
    const tbody = document.getElementById('open-rows');
    if (d.open.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="empty">No open trades — scanning...</td></tr>';
    } else {
      tbody.innerHTML = d.open.map(t => `
        <tr>
          <td><span class="sym">${t.symbol}</span><br>
            <span class="muted" style="font-size:.65rem">${t.strategy.toUpperCase()}</span></td>
          <td><span class="badge strat">${t.strategy.toUpperCase()}</span></td>
          <td class="mono cyan">$${t.amount.toFixed(2)}</td>
          <td class="mono">${t.bond_entry.toFixed(1)}%</td>
          <td class="mono cyan">${t.bond_high.toFixed(1)}%</td>
          <td class="timer elapsed-cell" data-start="${t.opened_at}">${elapsed(Date.now()/1000 - t.opened_at)}</td>
        </tr>`).join('');
    }

    // Feed — closed trades, newest first
    const feed = document.getElementById('feed');
    let added = false;
    const sorted = [...d.closed].reverse();
    const newItems = [];

    sorted.forEach(t => {
      const key = t.id ?? (t.symbol+t.time);
      if (!seenIds.has(key)) {
        seenIds.add(key);
        added = true;
        const won = t.pnl > 0;
        const isBuy = false; // closed trades only here
        const sign = t.pnl >= 0 ? '+' : '';
        newItems.push(`
          <div class="feed-item ${won?'win':'loss'}">
            <span class="feed-ts">${t.time}</span>
            <span class="feed-icon">${won?'✅':'❌'}</span>
            <div class="feed-body">
              <span class="feed-sym">${t.symbol}</span>
              <span class="badge ${won?'win':'loss'}">${won?'WIN':'LOSS'}</span>
              &nbsp;<span class="badge strat">${t.strategy.toUpperCase()}</span>
              <div class="feed-detail">
                Entry $${t.entry.toFixed(6)} → Exit $${t.exit.toFixed(6)}
                &nbsp;·&nbsp; ${t.result} &nbsp;·&nbsp; held ${t.hold_m.toFixed(1)}m
              </div>
            </div>
            <span class="feed-pnl ${won?'green':'red'}">${sign}$${t.pnl.toFixed(4)}</span>
          </div>`);
      }
    });

    if (newItems.length > 0) {
      const existing = feed.innerHTML === '<div class="empty">Waiting for trades...</div>'
        ? '' : feed.innerHTML;
      feed.innerHTML = newItems.join('') + existing;
    }

    // Last updated
    document.getElementById('last-update').textContent =
      'Updated ' + new Date().toLocaleTimeString();

  } catch(e) { console.error(e); }
}

poll();
setInterval(poll, 3000);
</script>
</body></html>"""
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
    return html

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
    log("ok", f"USDC lock : activates at ${USDC_LOCK_THRESHOLD:.0f} capital")
    log("ok", "=" * 55)
    app.run(host="0.0.0.0", port=port, use_reloader=False)
