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
    (  100, 0.12, 10),
    (    0, 0.08,  6),
]

MAX_DAILY_LOSS_PCT = float(os.environ.get("MAX_DAILY_LOSS_PCT", "20"))  # stop day if down >20% of start capital

# Risk limits
DAILY_LOSS_MAX    = int(os.environ.get("DAILY_LOSS_MAX",  "3"))   # cooldown after N consecutive losses
LOSS_COOLDOWN_HRS = int(os.environ.get("LOSS_COOLDOWN_HRS", "4")) # hours to pause + retune
ANALYZE_EVERY     = int(os.environ.get("ANALYZE_EVERY",   "10"))  # retune after every N completed trades

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
MAX_OPEN      = int(os.environ.get("MAX_OPEN",      "3"))
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
            log("warn", f"{_daily_losses} losses — cooling down {LOSS_COOLDOWN_HRS}h. Resumes {resume_str}")
            notify("🔧 Cooling Down",
                   f"{_daily_losses} losses hit.\nPausing {LOSS_COOLDOWN_HRS}h to retune.\nResumes: {resume_str}")
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
            "bond_last_moved":   time.time(),  # tracks last time bond increased
            "bond_slip_start":   None,
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
                    open_trades[mint]["bond_high"]       = bond
                    open_trades[mint]["bond_last_moved"]  = time.time()
                bond_high       = open_trades[mint]["bond_high"]
                bond_prev       = open_trades[mint]["bond_prev"]
                bond_last_moved = open_trades[mint].get("bond_last_moved", time.time())
                slip_start      = open_trades[mint]["bond_slip_start"]
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

            # Copy trade exits (price-based, same params as spike)
            if strategy == "copy":
                move = ((price - trade["entry"]) / trade["entry"]) * 100
                if move >= COPY_TP_PCT:
                    exit_trade(mint, price, "COPY_TP", bond)
                    continue
                if move <= -COPY_SL_PCT:
                    exit_trade(mint, price, "COPY_SL", bond)
                    continue
                if elapsed >= COPY_MAX_SECS:
                    exit_trade(mint, price, "COPY_TIME", bond)
                    continue

            pct = ((price - trade["entry"]) / trade["entry"]) * 100
            log("info", f"[{strategy}] bond={bond:.1f}% price={pct:+.1f}% {elapsed/60:.1f}m", symbol)

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
        "copy_wallets":   [{"addr": w["address"][:8]+"...", "winrate": w["winrate"]} for w in _copy_wallets],
        "usdc_locked":    round(usdc_locked, 4),
        "usdc_threshold": USDC_LOCK_THRESHOLD,
        "milestones_hit": sorted(_milestones_hit),
        "next_milestone": next((m for m in MILESTONES if m > cap), None),
        "today": {
            "trades":        _daily_trades,
            "cap":           daily_trade_limit(),
            "wins":          _daily_wins,
            "losses":        _daily_losses,
            "day_start_cap": round(_day_start_cap, 2),
            "daily_loss_pct": round((_day_start_cap - cap) / max(_day_start_cap, 1) * 100, 1) if _day_start_cap > 0 else 0,
            "max_daily_loss": f"{MAX_DAILY_LOSS_PCT:.0f}%",
            "paused_until":  time.strftime("%H:%M", time.localtime(_pause_until)) if _pause_until > time.time() else None,
            "date":          _daily_date,
        },
        "progress": {
            "day":          len(_week_day_logs) + 1,
            "start":        _week_start_date,
            "analyze_every": f"every {ANALYZE_EVERY} trades",
            "day_logs":     _week_day_logs[-3:],
        },
        "settings": {
            "trade_size":    f"${FIXED_TRADE_SIZE:.0f} fixed" if FIXED_TRADE_SIZE > 0 else f"tiered {_cap_tier(cap)[0]*100:.0f}% (${trade_size():.2f})",
            "daily_cap":     f"{daily_trade_limit()} trades/day at current capital",
            "max_daily_loss": f"{MAX_DAILY_LOSS_PCT:.0f}% of start capital",
            "cooldown":      f"pause {LOSS_COOLDOWN_HRS}h + retune after {DAILY_LOSS_MAX} losses",
            "bond_entry":    f"{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%",
            "bond_tp":       f"{BOND_TP}%",
            "spike_min_age": f"{SPIKE_MIN_AGE_H}h",
            "spike_min_1h":  f"{SPIKE_MIN_1H}%",
            "bundle_mode":   BUNDLE_MODE,
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
