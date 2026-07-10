import os, time, threading, requests, json, re, csv, io
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, Response, request
from collections import defaultdict, deque

try:
    from solders.keypair import Keypair
    from solders.transaction import VersionedTransaction
    from solders.pubkey import Pubkey
    from solana.rpc.api import Client
    from solana.rpc.types import TxOpts
    _SOLANA_AVAILABLE = True
except ImportError:
    _SOLANA_AVAILABLE = False

_session = requests.Session()
_session.trust_env = False  # bypass Railway proxy env vars

app = Flask(__name__)

@app.errorhandler(500)
def handle_500(e):
    import traceback
    tb = traceback.format_exc()
    log("warn", f"500 error: {e}\n{tb[:500]}", "FLASK")
    return f"<h1>Internal Server Error</h1><pre style='font-size:12px;color:#aaa'>{str(e)}</pre>", 500

def _auth_required():
    """Returns a 401 Response if API_SECRET is set and the request doesn't provide it.
    Check: Authorization: Bearer <secret>  OR  X-API-Key: <secret>
    Returns None if auth passes, a Response object if it fails."""
    if not API_SECRET:
        return None
    provided = (
        request.headers.get("X-API-Key", "") or
        request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    if provided != API_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    return None

# ── REDIS PERSISTENCE (Upstash REST) ────────────────────────────
def _redis_cmd(*args):
    if not REDIS_URL or not REDIS_TOKEN:
        return None
    try:
        r = _session.post(
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
BOT_PAUSED        = os.environ.get("BOT_PAUSED", "false").lower() == "true"  # set true in Railway to halt all trading

# Position sizing — capital-tiered (protects small accounts)
MIN_TRADE         = float(os.environ.get("MIN_TRADE",   "3"))
MAX_TRADE         = float(os.environ.get("MAX_TRADE",   "500"))
FIXED_TRADE_SIZE  = float(os.environ.get("FIXED_TRADE_SIZE", "0"))   # 0 = use tiered % sizing

# Capital tiers per risk level: (min_capital, trade_pct, daily_max_trades)
# daily_max set to 9999 = no cap — bot runs 24/7
_RISK_TIERS = {
    "conservative": [(5_000,0.12,9999),(500,0.10,9999),(100,0.08,9999),(0,0.05,9999)],
    "standard":     [(5_000,0.18,9999),(500,0.15,9999),(100,0.12,9999),(0,0.08,9999)],
    "aggressive":   [(5_000,0.22,9999),(500,0.18,9999),(100,0.15,9999),(0,0.12,9999)],
}
_CAP_TIERS = _RISK_TIERS.get(RISK_LEVEL, _RISK_TIERS["standard"])

MAX_DAILY_LOSS_PCT  = float(os.environ.get("MAX_DAILY_LOSS_PCT",  "20"))  # stop day if down >20% of start capital
SOLD_COOLDOWN_SECS = int(os.environ.get("SOLD_COOLDOWN_SECS", "1800"))  # 30 min cooldown before re-buying a sold coin

# Risk limits
DAILY_LOSS_MAX    = int(os.environ.get("DAILY_LOSS_MAX",  "6"))   # retune after N consecutive losses
LOSS_COOLDOWN_HRS = float(os.environ.get("LOSS_COOLDOWN_HRS", "0.083")) # 5-min pause then resume
ANALYZE_EVERY     = int(os.environ.get("ANALYZE_EVERY",   "5"))   # kept for reference only — retune is weekly (Monday 07:00 UTC)

# Bond Runner strategy
BOND_ENTRY_MIN  = float(os.environ.get("BOND_ENTRY_MIN", "50"))  # 50%+ = confirmed momentum, less stall risk
BOND_ENTRY_MAX  = float(os.environ.get("BOND_ENTRY_MAX", "75"))
BOND_TP_PCT     = float(os.environ.get("BOND_TP_PCT",    "30"))  # 30% TP — lets partial scale-out run (TP1@20%, TP2@25%, full@30%)
BOND_SL_PCT     = float(os.environ.get("BOND_SL_PCT",    "8"))
BOND_GRAD_BOND  = float(os.environ.get("BOND_GRAD_BOND", "90"))  # graduation imminent — tighten TSL
BOND_GRAD_TSL   = float(os.environ.get("BOND_GRAD_TSL",  "3"))   # tight TSL % near graduation
BOND_MAX_SECS       = int(os.environ.get("BOND_MAX_SECS",       "600"))  # 10 min — early runners need time
BOND_STALE_SECS     = int(os.environ.get("BOND_STALE_SECS",     "180"))  # 3 min — allow brief consolidations
DEAD_PAIR_SECS      = int(os.environ.get("DEAD_PAIR_SECS",       "45"))  # exit if zero bond movement in 45s (buffer for transient API failures)
VOL_STALE_SECS      = int(os.environ.get("VOL_STALE_SECS",       "60"))  # exit if had volume but stalled 60s

# Dormant Spike strategy
SPIKE_MIN_AGE_H = float(os.environ.get("SPIKE_MIN_AGE_H", "6"))
SPIKE_MIN_1H    = float(os.environ.get("SPIKE_MIN_1H",    "30"))
SPIKE_TP_PCT    = float(os.environ.get("SPIKE_TP_PCT",    "35"))   # raised from 20 — spikes run 30-50% before fading
SPIKE_SL_PCT    = float(os.environ.get("SPIKE_SL_PCT",    "15"))
SPIKE_MAX_SECS  = int(os.environ.get("SPIKE_MAX_SECS",    "180"))   # 3 min hard cap

# Trench strategy — coins 85-97% bonded, about to graduate (fast pump at migration)
TRENCH_ENTRY_MIN = float(os.environ.get("TRENCH_ENTRY_MIN", "85"))
TRENCH_ENTRY_MAX = float(os.environ.get("TRENCH_ENTRY_MAX", "97"))
TRENCH_TP_PCT    = float(os.environ.get("TRENCH_TP_PCT",    "50"))  # raised from 35 — graduation pumps run hard
TRENCH_SL_PCT    = float(os.environ.get("TRENCH_SL_PCT",    "12"))
TRENCH_MAX_SECS  = int(os.environ.get("TRENCH_MAX_SECS",    "90"))  # 90s — very fast

# Migration bounce — coins that just graduated to Raydium (first 2 min momentum)
MIGRATE_MAX_AGE  = int(os.environ.get("MIGRATE_MAX_AGE",    "120")) # enter within 2 min of graduation
MIGRATE_TP_PCT   = float(os.environ.get("MIGRATE_TP_PCT",   "40"))  # realistic Raydium bounce (was 400 — fantasy)
MIGRATE_SL_PCT   = float(os.environ.get("MIGRATE_SL_PCT",   "12"))
MIGRATE_MAX_SECS = int(os.environ.get("MIGRATE_MAX_SECS",   "300"))
GRAD_THROUGH     = os.environ.get("GRAD_THROUGH", "true").lower() != "false"  # hold bond positions through graduation to Raydium

# Exit protection
SLIP_TRIGGER   = float(os.environ.get("SLIP_TRIGGER",  "90"))
SLIP_DROP_TO   = float(os.environ.get("SLIP_DROP_TO",  "85"))
SLIP_WAIT_SECS = int(os.environ.get("SLIP_WAIT_SECS",  "6"))

# Trailing stop loss — activates once trade is up TSL_ACTIVATE_PCT, then trails BOND_SL_PCT below peak
TSL_ACTIVATE_PCT = float(os.environ.get("TSL_ACTIVATE_PCT", "12"))  # trailing SL kicks in at +12% — locks gains before rug
SHARP_DROP_PCT = float(os.environ.get("SHARP_DROP_PCT", "4"))

# Partial take-profit — scale out to lock gains without killing the run
# TP1: +20% → sell 30%; TP2: +25% → sell 30% of remaining; final ~49% rides to BOND_TP at +30%
PARTIAL_TP1_PCT  = float(os.environ.get("PARTIAL_TP1_PCT",  "20"))
PARTIAL_TP2_PCT  = float(os.environ.get("PARTIAL_TP2_PCT",  "25"))

# Bundle mode: "avoid" or "ride"
BUNDLE_MODE    = os.environ.get("BUNDLE_MODE", "avoid").lower()
BUNDLE_RIDE_TP = float(os.environ.get("BUNDLE_RIDE_TP", "88"))

# USDC profit lock
USDC_LOCK_THRESHOLD = float(os.environ.get("USDC_LOCK_THRESHOLD", "80"))
USDC_MINT  = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT  = "So11111111111111111111111111111111111111112"
GMGN_ROUTE = "https://gmgn.ai/defi/router/v1/sol/tx/get_swap_route"

# Jupiter APIs (agent-skills suite)
JUPITER_API_KEY        = os.environ.get("JUPITER_API_KEY", "")
JUPITER_QUOTE_URL      = "https://api.jup.ag/swap/v1/quote"
JUP_TOKENS_URL         = "https://api.jup.ag/tokens/v2"
JUP_PRICE_V3_URL       = "https://api.jup.ag/price/v3/price"
JUP_IMPACT_MAX_PCT     = float(os.environ.get("JUP_IMPACT_MAX_PCT",     "3.0"))
JUP_SIGNAL_REFRESH_SECS= int(os.environ.get("JUP_SIGNAL_REFRESH_SECS", "120"))

# Copy trading via GMGN smart wallets
COPY_TRADE        = os.environ.get("COPY_TRADE", "true").lower() == "true"
COPY_WINRATE_MIN  = float(os.environ.get("COPY_WINRATE_MIN",  "65"))  # was 60 — only elite wallets
COPY_WINRATE_MAX  = float(os.environ.get("COPY_WINRATE_MAX",  "99"))
COPY_MAX_WALLETS  = int(os.environ.get("COPY_MAX_WALLETS",    "5"))
COPY_MAX_AGE_SECS    = int(os.environ.get("COPY_MAX_AGE_SECS",    "120"))   # ignore trades older than 2 min
COPY_MIN_WHALE_USD   = float(os.environ.get("COPY_MIN_WHALE_USD",  "100"))  # skip if whale spent <$100 (test nibbles)
# Manually tracked wallets — comma-separated Solana addresses; merged with GMGN auto-discovered wallets
TRACKED_WALLETS   = [w.strip() for w in os.environ.get("TRACKED_WALLETS", "").split(",") if w.strip()]
# Pinned wallets — always monitored, mirror their exact USD trade size, use bot's own TP/SL/exits
PINNED_WALLETS = [
    # add verified wallets here — mirror exact USD size, use bot's own TP/SL/exits
]
# Fast wallets — skip ALL safety filters, exit before the wallet does (tight TP/SL)
FAST_WALLETS = [
    "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
    "5t9xBNuDdGTGpjaPTx6hKd7sdRJbvtKS8Mhq6qVbo8Qz",
]
FAST_TP_PCT       = float(os.environ.get("FAST_TP_PCT",       "8"))
FAST_SL_PCT       = float(os.environ.get("FAST_SL_PCT",       "10"))
FAST_MAX_SECS     = int(os.environ.get("FAST_MAX_SECS",       "90"))
COPY_REFRESH_MINS = int(os.environ.get("COPY_REFRESH_MINS",  "60"))   # refresh wallet list hourly
COPY_TP_PCT       = float(os.environ.get("COPY_TP_PCT",       "30"))  # matched to bond — 15% SL was negative EV at <43% WR
COPY_SL_PCT       = float(os.environ.get("COPY_SL_PCT",        "8"))  # matched to bond — break-even WR now 21% not 43%
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

# DexScreener endpoints
DSC_BASE              = "https://api.dexscreener.com"
DSC_REFRESH_SECS      = int(os.environ.get("DSC_REFRESH_SECS", "120"))
DSC_META_MAX          = int(os.environ.get("DSC_META_MAX",     "3"))    # top N trending metas to expand

# Notifications
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
NTFY_TOPIC       = os.environ.get("NTFY_TOPIC", "")

# Social / quality gates
MIN_REPLIES      = int(os.environ.get("MIN_REPLIES",      "8"))
MIN_SOCIALS      = int(os.environ.get("MIN_SOCIALS",       "0"))
MIN_LIQ          = float(os.environ.get("MIN_LIQ",        "500"))
MIN_VOL_5M       = float(os.environ.get("MIN_VOL_5M",     "2000"))  # $2k 5-min volume — catches coins before viral ($10k was post-move)
MIN_SIGNAL_SCORE = int(os.environ.get("MIN_SIGNAL_SCORE", "2"))     # ≥2 signal points — 1 confirmation + organic is enough at early bonding stage
MAX_RUG_SCORE    = int(os.environ.get("MAX_RUG_SCORE",    "400"))   # rugcheck score ceiling (higher = riskier)

# General
MAX_OPEN      = int(os.environ.get("MAX_OPEN",      "10"))
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", "2"))

SOL_RPC         = os.environ.get("SOL_RPC", "https://api.mainnet-beta.solana.com")
HELIUS_API_KEY        = os.environ.get("HELIUS_API_KEY", "")
HELIUS_WEBHOOK_AUTH   = os.environ.get("HELIUS_WEBHOOK_AUTH", "")   # set same value in Helius dashboard → Auth Header
RAILWAY_PUBLIC_DOMAIN = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")  # e.g. your-app.up.railway.app (no https://)
BIRDEYE_API_KEY       = os.environ.get("BIRDEYE_API_KEY", "")
PUMPPORTAL      = "https://pumpportal.fun/api/trade-local"

def _rpc_endpoints():
    """Return ordered list of RPCs to try. Helius first if key is set."""
    rpcs = []
    if HELIUS_API_KEY:
        rpcs.append(f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}")
    rpcs.append(SOL_RPC)
    if "mainnet-beta.solana.com" in SOL_RPC:
        rpcs.append("https://rpc.ankr.com/solana")
        rpcs.append("https://solana-mainnet.rpc.extrnode.com")
    return list(dict.fromkeys(rpcs))  # deduplicate, preserve order

def _send_tx(tx_bytes, symbol=""):
    """Send signed tx across multiple RPCs with retry. Returns sig or None."""
    for rpc_url in _rpc_endpoints():
        rpc_name = rpc_url.split("/")[2].split("?")[0]
        for attempt in range(2):
            try:
                client = Client(rpc_url)
                result = client.send_raw_transaction(
                    tx_bytes, opts=TxOpts(skip_preflight=True, preflight_commitment="confirmed")
                )
                sig = str(result.value)
                if sig and len(sig) > 10:
                    log("ok", f"TX landed via {rpc_name}: {sig[:14]}...", symbol)
                    return sig
            except Exception as e:
                log("warn", f"RPC {rpc_name} attempt {attempt+1}: {e}", symbol)
                if attempt == 0:
                    time.sleep(0.5)
    log("err", "All RPCs failed — tx not sent", symbol)
    return None
DATA_DIR    = os.environ.get("DATA_DIR", "/tmp")
os.makedirs(DATA_DIR, exist_ok=True)
LEARN_FILE  = os.path.join(DATA_DIR, "bot_learn.json")
STATE_FILE  = os.path.join(DATA_DIR, "bot_state.json")
WEEK_FILE   = os.path.join(DATA_DIR, "bot_week.json")
REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
API_SECRET  = os.environ.get("API_SECRET", "")

MILESTONES = [100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000]

# ── STATE ────────────────────────────────────────────────────────
capital           = float(os.environ.get("STARTING_CAPITAL", "100"))
STARTING_CAPITAL  = capital  # snapshot of configured start, for UI display
SOL_ALLOCATED     = float(os.environ.get("SOL_ALLOCATED",     "19.67"))  # SOL wallet funded for trading
capital_lock      = threading.Lock()
open_trades       = {}
trades_lock       = threading.Lock()
blacklisted_mints = set()
_blacklist_ts     = {}   # mint -> timestamp added; for 24h TTL expiry
_bundle_deployers = {}   # deployer wallet -> {"count": int, "first_seen": float}
BUNDLE_DEPLOYER_THRESHOLD = 2  # block deployer after N bundled launches
trade_log         = []
completed_trades  = []
_trade_id_lock    = threading.Lock()
_trade_id_counter = 0
log_lock          = threading.Lock()
scan_active       = True
_milestones_hit   = set()
_milestone_lock   = threading.Lock()
usdc_locked       = 0.0
usdc_lock         = threading.Lock()
# Trading window — 0/0 = 24/7 (no gate). Set START/END to restrict hours (UTC).
TRADE_START_HOUR = int(os.environ.get("TRADE_START_HOUR", "0"))
TRADE_END_HOUR   = int(os.environ.get("TRADE_END_HOUR",   "0"))

# Daily tracking — resets at TRADE_START_HOUR
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
_webhook_queue    = deque(maxlen=500)  # Helius push events: (wallet_info_dict, act_dict)
_helius_wh_id     = ""                 # registered Helius webhook ID (persisted to Redis)
_helius_wh_lock   = threading.Lock()
_bond_prev        = {}   # mint -> (bond_pct, timestamp) — velocity check: skip stalled bonds
_bond_prev_lock   = threading.Lock()
_watchlist        = {}   # mint -> {"res": action_result, "added_at": float} — qualified but couldn't enter
_watchlist_lock   = threading.Lock()
WATCHLIST_TTL_SECS = int(os.environ.get("WATCHLIST_TTL_SECS", "300"))  # 5 min before watchlist entry expires
TUNE_PAUSED_UNTIL  = 0.0   # Unix timestamp — auto-tune blocked until this time (0 = use Monday schedule)
_gmgn_sm_signal_mints  = set()   # smart money buy signal mints (type 12)
_gmgn_surge_mints      = set()   # price surge signal mints (type 6)
_gmgn_kol_mints        = set()   # KOL buy mints
_gmgn_sm_sell_mints    = set()   # smart money sell mints (exit/skip filter)
_gmgn_trending_mints   = set()   # trending tokens (1h price movers on GMGN)
_gmgn_hot_mints        = set()   # hot search tokens (what people are searching on GMGN)
_gmgn_signal_time      = 0.0     # last signal refresh time
_signal_lock           = threading.Lock()
# DexScreener signal sets
_dsc_boosted_mints     = set()   # tokens currently receiving boost orders
_dsc_top_mints         = set()   # tokens with most total boost spend
_dsc_profile_mints     = set()   # tokens that have/recently updated a DSC profile
_dsc_ad_mints          = set()   # tokens running paid ads on DSC
_dsc_takeover_mints    = set()   # community takeover tokens
_dsc_meta_mints        = set()   # tokens in the top trending metas/narratives
_dsc_signal_time       = 0.0
_dsc_lock              = threading.Lock()
# Jupiter agent-skills signal sets
_jup_trending_mints    = set()   # toptrending/5m — actively pumping right now
_jup_organic_mints     = set()   # toporganicscore/5m — real volume, not bots
_jup_toptraded_mints   = set()   # toptraded/5m — highest volume tokens
_jup_verified_mints    = set()   # Jupiter-verified token list
_jup_token_cache       = {}      # mint -> (timestamp, token_data) for audit checks
_jup_signal_time       = 0.0
_jup_lock              = threading.Lock()
_JUP_TOKEN_CACHE_TTL   = 120
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
    with capital_lock:
        cap_snapshot = capital
    with usdc_lock:
        usdc_snap = usdc_locked
    with _copy_lock:
        sold_snapshot = dict(_sold_mints)
    with _watchlist_lock:
        wl_snapshot = {m: {"res": e["res"], "added_at": e["added_at"]} for m, e in _watchlist.items()}
    state = {
        "date":           _daily_date,
        "trades":         _daily_trades,
        "wins":           _daily_wins,
        "losses":         _daily_losses,
        "pause_until":    _pause_until,
        "capital":        cap_snapshot,
        "week_start":     _week_start_date,
        "week_logs":      _week_day_logs,
        "day_start_cap":       _day_start_cap,
        "tune_paused_until":   TUNE_PAUSED_UNTIL,
        "bond_entry_min":      BOND_ENTRY_MIN,
        "bond_entry_max":      BOND_ENTRY_MAX,
        "bond_tp_pct":         BOND_TP_PCT,
        "bond_sl_pct":         BOND_SL_PCT,
        "bond_stale_secs":     BOND_STALE_SECS,
        "bond_max_secs":       BOND_MAX_SECS,
        "spike_tp_pct":        SPIKE_TP_PCT,
        "trench_tp_pct":       TRENCH_TP_PCT,
        "blacklist":      list(blacklisted_mints),
        "blacklist_ts":   _blacklist_ts,
        "bundle_deployers": _bundle_deployers,
        "sold_mints":     sold_snapshot,
        "watchlist":      wl_snapshot,
        "usdc_locked":    usdc_snap,
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass
    redis_save("bot_state", state)
    # Persist open positions so they survive restarts
    with trades_lock:
        redis_save("bot_open_trades", list(open_trades.values()))

def _load_daily_state():
    global _daily_date, _daily_trades, _daily_wins, _daily_losses
    global _pause_until, capital, _week_start_date, _week_day_logs, completed_trades
    global _day_start_cap, TUNE_PAUSED_UNTIL, usdc_locked
    global BOND_ENTRY_MIN, BOND_ENTRY_MAX, BOND_TP_PCT, BOND_SL_PCT
    global BOND_STALE_SECS, BOND_MAX_SECS, SPIKE_TP_PCT, TRENCH_TP_PCT
    # Compute the same @HH day key used by _save_daily_state so the date comparison matches
    _now = time.gmtime()
    if _now.tm_hour >= TRADE_START_HOUR:
        today = time.strftime("%Y-%m-%d", _now) + f"@{TRADE_START_HOUR:02d}"
    else:
        _prev = time.gmtime(time.time() - 86400)
        today = time.strftime("%Y-%m-%d", _prev) + f"@{TRADE_START_HOUR:02d}"

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
            # Capital is always restored — it accumulates across days and must survive redeploys
            saved_cap = s.get("capital", None)
            if saved_cap is not None and float(saved_cap) > 0:
                capital = float(saved_cap)
            if s.get("date") == today:
                _daily_date   = s["date"]
                _daily_trades = s.get("trades",      0)
                _daily_wins   = s.get("wins",        0)
                _daily_losses = s.get("losses",      0)
                _pause_until  = s.get("pause_until", 0.0)
            # usdc_locked accumulates across trades — always restore regardless of date
            if "usdc_locked" in s:
                usdc_locked = float(s["usdc_locked"])
            _week_start_date = s.get("week_start", "")
            _week_day_logs   = s.get("week_logs",  [])
            if s.get("day_start_cap", 0) > 0:
                _day_start_cap = float(s["day_start_cap"])
            TUNE_PAUSED_UNTIL = float(s.get("tune_paused_until", _next_monday_7am()))
            # Restore tuner parameters — env vars are initial defaults only; Redis wins
            if "bond_entry_min"  in s: BOND_ENTRY_MIN  = float(s["bond_entry_min"])
            if "bond_entry_max"  in s: BOND_ENTRY_MAX  = float(s["bond_entry_max"])
            if "bond_tp_pct"     in s: BOND_TP_PCT     = float(s["bond_tp_pct"])
            if "bond_sl_pct"     in s: BOND_SL_PCT     = float(s["bond_sl_pct"])
            if "bond_stale_secs" in s: BOND_STALE_SECS = int(s["bond_stale_secs"])
            if "bond_max_secs"   in s: BOND_MAX_SECS   = int(s["bond_max_secs"])
            if "spike_tp_pct"    in s: SPIKE_TP_PCT    = float(s["spike_tp_pct"])
            if "trench_tp_pct"   in s: TRENCH_TP_PCT   = float(s["trench_tp_pct"])
            # Restore blacklist with 24h TTL
            saved_bl_ts = s.get("blacklist_ts", {})
            _now_bl = time.time()
            for m in s.get("blacklist", []):
                ts = float(saved_bl_ts.get(m, 0))
                if ts == 0 or _now_bl - ts < 86400:  # keep if <24h old or no timestamp
                    blacklisted_mints.add(m)
                    _blacklist_ts[m] = ts or _now_bl
            # Restore bundle deployer history
            for dev, info in s.get("bundle_deployers", {}).items():
                _bundle_deployers[dev] = info
            # Restore sold cooldowns still within window
            _now = time.time()
            with _copy_lock:
                for m, ts in s.get("sold_mints", {}).items():
                    if _now - ts < SOLD_COOLDOWN_SECS:
                        _sold_mints[m] = ts
            # Restore watchlist entries still within TTL
            with _watchlist_lock:
                for m, entry in s.get("watchlist", {}).items():
                    if _now - entry.get("added_at", 0) < WATCHLIST_TTL_SECS:
                        _watchlist[m] = entry
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
        # Seed monotonic counter above the highest existing ID to prevent collisions after redeploy
        with _trade_id_lock:
            global _trade_id_counter
            _trade_id_counter = max((t.get("id", 0) for t in trades_data), default=0)
        log("ok", f"Reloaded {len(completed_trades)} completed trades")

    # Ensure TUNE_PAUSED_UNTIL is always set to a future Monday — never left at 0.0
    if TUNE_PAUSED_UNTIL <= 0:
        TUNE_PAUSED_UNTIL = _next_monday_7am()
        log("ok", f"Tune schedule initialised: next retune {time.strftime('%a %b %d 07:00 UTC', time.gmtime(TUNE_PAUSED_UNTIL))}", "TUNE")

    # Restore open positions so bot doesn't re-buy after crash/redeploy
    saved_open = redis_load("bot_open_trades")
    if saved_open:
        needs_verify = []
        with trades_lock:
            for t in saved_open:
                mint = t.get("mint")
                if mint and mint not in open_trades:
                    # Clear sell-side flag — exit_trade will re-set it when it actually runs
                    t.pop("_exiting", None)
                    # Track which positions need buy verification restarted
                    if t.pop("_unverified", None):
                        needs_verify.append((mint, t.get("symbol", "?"), t.get("amount", 0)))
                    open_trades[mint] = t
        # Restart buy-verification threads outside the lock
        for mint, symbol, amount in needs_verify:
            threading.Thread(
                target=_verify_tx_landed,
                args=("RESTART", mint, symbol, amount),
                daemon=True
            ).start()
        log("ok", f"Restored {len(saved_open)} open position(s) from Redis"
            + (f" — {len(needs_verify)} awaiting buy confirm" if needs_verify else ""))

def _reset_daily_if_needed():
    global _daily_date, _daily_trades, _daily_wins, _daily_losses, _pause_until
    global _week_start_date, _week_day_logs, _day_start_cap, _daily_cap_notified
    # Rollover key = "YYYY-MM-DD@HH" where HH is TRADE_START_HOUR.
    # This means the "trading day" starts at TRADE_START_HOUR (not midnight).
    now = time.gmtime()
    if now.tm_hour >= TRADE_START_HOUR:
        day_key = time.strftime("%Y-%m-%d", now) + f"@{TRADE_START_HOUR:02d}"
    else:
        # Before today's start hour — still the previous trading day
        prev = time.gmtime(time.time() - 86400)
        day_key = time.strftime("%Y-%m-%d", prev) + f"@{TRADE_START_HOUR:02d}"
    today = day_key   # reuse variable name so the block below works unchanged
    with capital_lock:
        _cap_snap = capital  # snapshot before _daily_lock to avoid holding both simultaneously
    with _daily_lock:
        if _daily_date != today:
            if _daily_date:
                _week_day_logs.append({
                    "date":    _daily_date,
                    "trades":  _daily_trades,
                    "wins":    _daily_wins,
                    "losses":  _daily_losses,
                    "capital": _cap_snap,
                })
            if not _week_start_date:
                _week_start_date = today
            _daily_date    = today
            _daily_trades  = 0
            _daily_wins    = 0
            _daily_losses  = 0
            _daily_cap_notified = False
            # _pause_until intentionally NOT reset here — pause survives day rollover
            _day_start_cap = _cap_snap  # snapshot for daily loss % guard
            limit = daily_trade_limit()
            with capital_lock:
                cap = capital
            pct, _ = _cap_tier(cap)
            log("ok", f"New day {today} | Day {len(_week_day_logs)+1} | cap=${cap:.2f} | trade={pct*100:.0f}% (${trade_size():.2f}) | limit={limit}/day")
            _save_daily_state()
            if len(_week_day_logs) > 0:
                notify(
                    f"🌅 Boogeys Sniper — New Day",
                    f"Date: {today}\nCapital: ${cap:,.2f}\nDaily limits reset — sniping resumed."
                )

def daily_limit_reached():
    global _daily_cap_notified
    _reset_daily_if_needed()
    # Trading-hours gate. START==END==0 means 24/7 — no restriction.
    if TRADE_START_HOUR != 0 or TRADE_END_HOUR != 0:
        utc_hour = time.gmtime().tm_hour
        if TRADE_START_HOUR <= TRADE_END_HOUR:
            in_window = TRADE_START_HOUR <= utc_hour < TRADE_END_HOUR
        else:  # wraps midnight
            in_window = utc_hour >= TRADE_START_HOUR or utc_hour < TRADE_END_HOUR
        if not in_window:
            next_open = time.strftime("%H:%M UTC", time.gmtime(
                time.time() + ((TRADE_START_HOUR - utc_hour) % 24) * 3600
            ))
            log("info", f"Outside trading window (UTC {TRADE_START_HOUR:02d}–{TRADE_END_HOUR:02d}) — resumes {next_open}")
            return True
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
                if not _daily_cap_notified:
                    _daily_cap_notified = True
                    log("warn", f"Daily loss guard: down {loss_pct:.1f}% today (${_day_start_cap - cap_now:.2f}) — stopping until tomorrow")
                    notify(
                        f"🛑 *Boogeys Sniper* — Loss Guard\n"
                        f"Down {loss_pct:.1f}% today (${_day_start_cap - cap_now:.2f})\n"
                        f"Stopping to protect capital. Auto-resumes at midnight."
                    )
                return True
        return False

def record_daily_trade(won):
    global _daily_wins, _daily_losses, _pause_until
    trigger_retune = False
    with _daily_lock:
        if won:
            _daily_wins += 1
        else:
            _daily_losses += 1
            # Loss-streak guard: DAILY_LOSS_MAX consecutive losses → short cooldown + retune
            if _daily_losses >= DAILY_LOSS_MAX and _pause_until <= time.time():
                _pause_until   = time.time() + LOSS_COOLDOWN_HRS * 3600
                trigger_retune = True
                log("warn",
                    f"Loss streak {_daily_losses} — pausing {LOSS_COOLDOWN_HRS*60:.0f}min, retuning",
                    "RISK")
        log("ok" if won else "info",
            f"Daily: {_daily_trades} trades | {_daily_wins}W {_daily_losses}L")
    if trigger_retune:
        notify(f"⚠️ Loss Streak {_daily_losses}",
               f"Pausing {LOSS_COOLDOWN_HRS*60:.0f} min and retuning strategies.")
        threading.Thread(target=_retune_strategies, daemon=True).start()
    _save_daily_state()

def _record_bundle_deployer(dev_wallet: str, symbol: str):
    """Track wallets that repeatedly deploy bundled tokens. Blocks them after BUNDLE_DEPLOYER_THRESHOLD launches."""
    if not dev_wallet:
        return
    if dev_wallet not in _bundle_deployers:
        _bundle_deployers[dev_wallet] = {"count": 0, "first_seen": time.time()}
    _bundle_deployers[dev_wallet]["count"] += 1
    count = _bundle_deployers[dev_wallet]["count"]
    if count >= BUNDLE_DEPLOYER_THRESHOLD:
        log("warn", f"Bundle deployer flagged ({count} bundled launches) — {dev_wallet[:8]}...", symbol)

def _is_bundle_deployer(dev_wallet: str) -> bool:
    """Return True if this deployer has hit the bundle threshold."""
    if not dev_wallet:
        return False
    info = _bundle_deployers.get(dev_wallet)
    return bool(info and info.get("count", 0) >= BUNDLE_DEPLOYER_THRESHOLD)

def _blacklist_add(mint: str):
    """Add mint to blacklist with timestamp for 24h TTL."""
    blacklisted_mints.add(mint)
    _blacklist_ts[mint] = time.time()

def _next_monday_7am():
    """Returns Unix timestamp of next Monday at 07:00 UTC (pure arithmetic, no datetime import)."""
    now = time.time()
    t   = time.gmtime(now)
    secs_today       = t.tm_hour * 3600 + t.tm_min * 60 + t.tm_sec
    today_midnight   = now - secs_today
    days_until_mon   = (7 - t.tm_wday) % 7   # Monday = weekday 0
    if days_until_mon == 0 and t.tm_hour >= 7:
        days_until_mon = 7  # already past 7am Monday — aim for next week
    return today_midnight + days_until_mon * 86400 + 7 * 3600

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
    """Sends end-of-trading-day summary at TRADE_END_HOUR UTC. Weekly report every 7 days."""
    while True:
        now = time.gmtime()
        secs_to_end = ((TRADE_END_HOUR - now.tm_hour) % 24) * 3600 - now.tm_min * 60 - now.tm_sec
        if secs_to_end <= 0:
            secs_to_end += 86400
        time.sleep(secs_to_end + 5)
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
        # Caller already appended to completed_trades — read from in-memory list,
        # not the local file (ephemeral, gone after dyno restart)
        trimmed = list(completed_trades)[-200:]
        with open(LEARN_FILE, "w") as f:
            json.dump(trimmed, f)
        redis_save("bot_trades", trimmed)
    except Exception as e:
        log("warn", f"Learning record: {e}")

def auto_tune(history):
    global BOND_ENTRY_MIN, BOND_ENTRY_MAX, BOND_TP_PCT, SPIKE_TP_PCT, BOND_STALE_SECS, BOND_SL_PCT, BOND_MAX_SECS
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
            BOND_ENTRY_MIN = round(min(max(avg_win_entry - 2, 38), 65), 1)
            BOND_ENTRY_MAX = round(min(BOND_ENTRY_MIN + 6, 70), 1)
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

        # Tune BOND_TP_PCT toward where winners actually peaked
        bond_tp_wins = [t for t in bond_wins if t.get("pnl", 0) > 0]
        if bond_tp_wins:
            avg_win_pnl_pct = sum((t["pnl"] / max(t["amount"], 0.01)) * 100 for t in bond_tp_wins) / len(bond_tp_wins)
            # Nudge TP toward actual winner peak — stay in 10-35% range for scalping
            BOND_TP_PCT = round(max(10, min(35, avg_win_pnl_pct * 0.85)), 1)

        # Spike tuning — keep within scalping range
        if spike_wr > bond_wr + 0.2 and SPIKE_TP_PCT < 40:
            SPIKE_TP_PCT = round(SPIKE_TP_PCT * 1.1, 1)
        elif spike_wr < 0.3 and SPIKE_TP_PCT > 12:
            SPIKE_TP_PCT = round(SPIKE_TP_PCT * 0.9, 1)

        overall_wr = round(len(wins) / max(len(recent), 1) * 100, 1)
        stats = {
            "tuned_at":        time.strftime("%Y-%m-%d %H:%M:%S"),
            "trades_analyzed": len(recent),
            "overall_wr":      f"{overall_wr}%",
            "bond_wr":         f"{round(bond_wr*100,1)}%",
            "spike_wr":        f"{round(spike_wr*100,1)}%",
            "bond_entry":      f"{BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}%",
            "bond_tp_pct":     BOND_TP_PCT,
            "bond_stale_secs": BOND_STALE_SECS,
            "bond_max_secs":   BOND_MAX_SECS,
            "bond_sl_pct":     BOND_SL_PCT,
            "spike_tp_pct":    SPIKE_TP_PCT,
        }
        log("ok", f"Auto-tuned: bond={BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% tp={BOND_TP_PCT}% "
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
                        "replies":  int(coin.get("reply_count", 0) or coin.get("replies", 0) or 0),
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
            vtr   = float(data.get("virtual_token_reserves", 1) or 1)
            bond  = min((vsol / 85_000_000_000) * 100, 99.9)
            # Real price from constant-product curve: price_sol = vsol/vtr (lamports/raw_tokens)
            # Convert: (vsol/1e9 SOL) / (vtr/1e6 tokens) = vsol/vtr * 1e-3 SOL per token
            sol_price = get_sol_price() or 0
            price_usd = (vsol / vtr * 1e-3 * sol_price) if sol_price and vtr > 0 else 0
            return {
                "bond_pct":   round(bond, 1),
                "price_usd":  price_usd,
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
        if pairs:
            pair = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
            vol  = pair.get("volume", {})
            txns = pair.get("txns",   {}).get("m5", {})
            dex_price = float(pair.get("priceUsd", 0) or 0)
            # Fill a zero DexScreener price from Birdeye (coin may be indexed but price missing)
            if dex_price <= 0:
                dex_price = get_birdeye_price(mint) or 0
            return {
                "price":        dex_price,
                "liq":          float(pair.get("liquidity", {}).get("usd", 0) or 0),
                "change1h":     float(pair.get("priceChange", {}).get("h1", 0) or 0),
                "change5m":     float(pair.get("priceChange", {}).get("m5", 0) or 0),
                "vol_m5":       float(vol.get("m5", 0) or 0),
                "vol_h1":       float(vol.get("h1", 0) or 0),
                "buys_m5":      int(txns.get("buys", 0) or 0),
                "sells_m5":     int(txns.get("sells", 0) or 0),
                "pair_address": pair.get("pairAddress", ""),
                "age_h":        (time.time() - float(pair.get("pairCreatedAt", time.time() * 1000)) / 1000) / 3600,
            }
        # DexScreener has no pairs yet — try Birdeye for a price-only fallback
        be_price = get_birdeye_price(mint)
        if be_price:
            return {"price": be_price, "liq": 0, "change1h": 0, "change5m": 0,
                    "vol_m5": 0, "vol_h1": 0, "buys_m5": 0, "sells_m5": 0,
                    "pair_address": "", "age_h": 0}
        return None
    except Exception:
        return None

def is_1m_trending_up(pair_address, market=None) -> bool:
    """
    Returns True if price momentum is up or neutral.
    Uses change5m from already-fetched market data — DexScreener has no candles endpoint.
    Fails open (True) when no data so API downtime doesn't block all entries.
    Pass market dict from get_market_data to avoid a redundant fetch.
    """
    try:
        if market is None:
            return True  # caller should pass market — don't do a free fetch here
        change5m = float(market.get("change5m", 0) or 0)
        # Allow slight dips (-2%) to avoid filtering out brief consolidations
        return change5m >= -2.0
    except Exception:
        return True


_sol_price_cache = (None, 0.0)  # (price, fetched_at) — tuple assignment is atomic in CPython
_SOL_PRICE_TTL   = 30  # seconds — SOL price doesn't move >0.1% in 30s at normal volatility

_BIRDEYE_TTL    = 15  # seconds — shorter TTL than SOL cache; used for token prices too
_birdeye_cache  = {}  # mint -> (timestamp, price)
_WSOL_MINT      = "So11111111111111111111111111111111111111112"

def get_birdeye_price(mint):
    """Return USD price for any Solana token via Birdeye. Cached for _BIRDEYE_TTL seconds.
    Returns None if API key not set or request fails."""
    if not BIRDEYE_API_KEY:
        return None
    now = time.time()
    hit = _birdeye_cache.get(mint)
    if hit and now - hit[0] < _BIRDEYE_TTL:
        return hit[1]
    try:
        res = _session.get(
            "https://public-api.birdeye.so/defi/price",
            params={"address": mint},
            headers={"X-API-KEY": BIRDEYE_API_KEY, "x-chain": "solana"},
            timeout=6
        )
        if res.status_code == 200:
            price = float(res.json().get("data", {}).get("value", 0) or 0)
            if price > 0:
                _birdeye_cache[mint] = (now, price)
                return price
    except Exception:
        pass
    return None

def get_sol_price():
    global _sol_price_cache
    cached_price, cached_ts = _sol_price_cache
    if cached_price and time.time() - cached_ts < _SOL_PRICE_TTL:
        return cached_price
    try:
        res   = _session.get(
            "https://api.dexscreener.com/latest/dex/pairs/solana/8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        if pairs:
            price = float(pairs[0].get("priceUsd", 0))
            if price > 0:
                _sol_price_cache = (price, time.time())
                return price
    except Exception:
        pass
    # Birdeye fallback — faster than CoinGecko, no rate limits with API key
    be_price = get_birdeye_price(_WSOL_MINT)
    if be_price:
        _sol_price_cache = (be_price, time.time())
        return be_price
    try:
        res   = _session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=8
        )
        price = float(res.json()["solana"]["usd"])
        if price > 0:
            _sol_price_cache = (price, time.time())
            return price
    except Exception:
        pass
    # Return stale cache rather than None — stale SOL price beats a failed bond price calc
    return cached_price

# ── JUPITER PRICE IMPACT ────────────────────────────────────────
_jup_impact_cache = {}   # mint -> (timestamp, impact_pct)
_JUP_CACHE_TTL    = 30   # seconds

def jup_price_impact(mint: str, usd_amount: float) -> float:
    """
    GET /swap/v1/quote — returns priceImpactPct for buying `usd_amount` USD
    worth of `mint` using SOL as input.  Returns 0.0 on any failure so the
    caller never blocks an entry just because Jupiter has no route (e.g. a
    token still on the pump.fun bonding curve that isn't on Raydium yet).
    Returns a large number only when Jupiter explicitly quotes a high impact.
    """
    now = time.time()
    hit = _jup_impact_cache.get(mint)
    if hit and now - hit[0] < _JUP_CACHE_TTL:
        return hit[1]
    try:
        sol_price = get_sol_price()
        if not sol_price or sol_price <= 0:
            return 0.0
        lamports = int((usd_amount / sol_price) * 1_000_000_000)
        if lamports <= 0:
            return 0.0
        hdrs = {"Accept": "application/json"}
        if JUPITER_API_KEY:
            hdrs["x-api-key"] = JUPITER_API_KEY
        r = _session.get(
            JUPITER_QUOTE_URL,
            params={
                "inputMint":                 WSOL_MINT,
                "outputMint":                mint,
                "amount":                    lamports,
                "swapMode":                  "ExactIn",
                "slippageBps":               50,
                "restrictIntermediateTokens":"true",
                "maxAccounts":               64,
                "instructionVersion":        "V1",
            },
            headers=hdrs,
            timeout=6
        )
        if r.status_code == 200:
            impact = float(r.json().get("priceImpactPct", 0) or 0)
            _jup_impact_cache[mint] = (now, impact)
            return impact
    except Exception:
        pass
    _jup_impact_cache[mint] = (now, 0.0)
    return 0.0

# ── RUGCHECK ────────────────────────────────────────────────────
_rug_cache    = {}  # mint -> (timestamp, result)
_holder_cache = {}  # mint -> (timestamp, result)
_dev_cache    = {}  # dev_wallet -> (timestamp, result)
_CACHE_TTL    = 90  # seconds — rug/holder data doesn't change in 90s

def run_rugcheck(mint):
    now = time.time()
    hit = _rug_cache.get(mint)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]
    try:
        res   = _session.get(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary", timeout=10)
        data  = res.json()
        risks = [r.get("name", "").lower() for r in data.get("risks", [])]
        result = {
            "has_mint_auth":   any("mint" in r for r in risks),
            "has_freeze_auth": any("freeze" in r for r in risks),
            "is_bundled":      any("insider" in r or "bundle" in r for r in risks),
            "score":           int(data.get("score", 0) or 0),
        }
        _rug_cache[mint] = (now, result)
        return result
    except Exception:
        return None

def check_holder_concentration(mint) -> tuple:
    """(ok, reason) — ok=False if top-10 wallets hold >35% of supply."""
    now = time.time()
    hit = _holder_cache.get(mint)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]
    try:
        hdrs = {"User-Agent":"Mozilla/5.0","Referer":"https://gmgn.ai/","Origin":"https://gmgn.ai"}
        r = _session.get(f"{GMGN_TOP_HOLDERS}/{mint}", headers=hdrs, params={"limit":10}, timeout=8)
        if r.status_code != 200:
            result = (True, "")
            _holder_cache[mint] = (now, result)
            return result
        data = r.json().get("data") or {}
        holders = data.get("holders") or data if isinstance(data, list) else []
        top10_pct = sum(float(h.get("amount_percentage") or h.get("percent") or 0) for h in holders[:10])
        result = (False, f"top10={top10_pct:.0f}%") if top10_pct > 55 else (True, "")
        _holder_cache[mint] = (now, result)
        return result
    except Exception:
        return True, ""

def check_dev_history(dev_wallet) -> tuple:
    """(ok, reason) — ok=False if dev has 2+ tokens that rugged (>95% drop from ATH)."""
    if not dev_wallet:
        return True, ""
    now = time.time()
    hit = _dev_cache.get(dev_wallet)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]
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
        result = (False, f"dev rugged {rugs}x") if rugs >= 2 else (True, "")
        _dev_cache[dev_wallet] = (now, result)
        return result
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

# ── DEXSCREENER INTEGRATIONS ─────────────────────────────────────

def _refresh_dsc_signals():
    """Pull all DexScreener signal sets: boosts, profiles, ads, takeovers, trending metas."""
    global _dsc_boosted_mints, _dsc_top_mints, _dsc_profile_mints
    global _dsc_ad_mints, _dsc_takeover_mints, _dsc_meta_mints, _dsc_signal_time
    hdrs = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

    def _sol_addrs(data, addr_key="tokenAddress", chain_key="chainId"):
        items = data if isinstance(data, list) else (data or {}).get("pairs", [])
        return {
            item.get(addr_key) or item.get("baseToken", {}).get("address", "")
            for item in (items or [])
            if (item.get(chain_key, "solana") or "").lower() in ("solana", "")
            and (item.get(addr_key) or item.get("baseToken", {}).get("address", ""))
        }

    try:
        # /token-boosts/latest/v1 — tokens currently receiving boost payments
        r1 = _session.get(f"{DSC_BASE}/token-boosts/latest/v1", headers=hdrs, timeout=8)
        boosted = set()
        if r1.status_code == 200:
            for item in (r1.json() or []):
                if (item.get("chainId", "solana") or "").lower() in ("solana", ""):
                    a = item.get("tokenAddress", "")
                    if a:
                        boosted.add(a)

        # /token-boosts/top/v1 — tokens with most total boost spend
        r2 = _session.get(f"{DSC_BASE}/token-boosts/top/v1", headers=hdrs, timeout=8)
        top = set()
        if r2.status_code == 200:
            for item in (r2.json() or []):
                if (item.get("chainId", "solana") or "").lower() in ("solana", ""):
                    a = item.get("tokenAddress", "")
                    if a:
                        top.add(a)

        # /token-profiles/latest/v1 — tokens that just created a DSC profile
        r3 = _session.get(f"{DSC_BASE}/token-profiles/latest/v1", headers=hdrs, timeout=8)
        profiles = set()
        if r3.status_code == 200:
            for item in (r3.json() or []):
                if (item.get("chainId", "solana") or "").lower() in ("solana", ""):
                    a = item.get("tokenAddress", "")
                    if a:
                        profiles.add(a)

        # /token-profiles/recent-updates/v1 — tokens that recently updated their profile
        r4 = _session.get(f"{DSC_BASE}/token-profiles/recent-updates/v1", headers=hdrs, timeout=8)
        if r4.status_code == 200:
            for item in (r4.json() or []):
                if (item.get("chainId", "solana") or "").lower() in ("solana", ""):
                    a = item.get("tokenAddress", "")
                    if a:
                        profiles.add(a)

        # /ads/latest/v1 — tokens running paid DexScreener ads
        r5 = _session.get(f"{DSC_BASE}/ads/latest/v1", headers=hdrs, timeout=8)
        ads = set()
        if r5.status_code == 200:
            data5 = r5.json() or {}
            for item in (data5 if isinstance(data5, list) else data5.get("ads", [])):
                if (item.get("chainId", "solana") or "").lower() in ("solana", ""):
                    a = item.get("tokenAddress", "")
                    if a:
                        ads.add(a)

        # /community-takeovers/latest/v1 — community-revived tokens
        r6 = _session.get(f"{DSC_BASE}/community-takeovers/latest/v1", headers=hdrs, timeout=8)
        takeovers = set()
        if r6.status_code == 200:
            for item in (r6.json() or []):
                if (item.get("chainId", "solana") or "").lower() in ("solana", ""):
                    a = item.get("tokenAddress", "")
                    if a:
                        takeovers.add(a)

        # /metas/trending/v1 → expand top N metas into token mints
        r7 = _session.get(f"{DSC_BASE}/metas/trending/v1", headers=hdrs, timeout=8)
        meta_mints = set()
        if r7.status_code == 200:
            metas = r7.json() or []
            for meta in metas[:DSC_META_MAX]:
                slug = meta.get("slug", "")
                if not slug:
                    continue
                try:
                    rm = _session.get(f"{DSC_BASE}/metas/meta/v1/{slug}", headers=hdrs, timeout=8)
                    if rm.status_code == 200:
                        for p in (rm.json() or {}).get("pairs", []):
                            if p.get("chainId", "").lower() == "solana":
                                a = p.get("baseToken", {}).get("address", "")
                                if a:
                                    meta_mints.add(a)
                except Exception:
                    pass

        with _dsc_lock:
            _dsc_boosted_mints  = boosted
            _dsc_top_mints      = top
            _dsc_profile_mints  = profiles
            _dsc_ad_mints       = ads
            _dsc_takeover_mints = takeovers
            _dsc_meta_mints     = meta_mints
            _dsc_signal_time    = time.time()
        log("info",
            f"DSC: boost={len(boosted)} top={len(top)} profiles={len(profiles)} "
            f"ads={len(ads)} takeovers={len(takeovers)} meta={len(meta_mints)}", "DSC")
    except Exception as e:
        log("warn", f"DSC signal refresh: {e}", "DSC")

def run_dsc_refresh_loop():
    """Background thread: refresh DexScreener signals every DSC_REFRESH_SECS seconds."""
    time.sleep(20)  # stagger from GMGN refresh
    while True:
        if time.time() - _dsc_signal_time >= DSC_REFRESH_SECS:
            _refresh_dsc_signals()
        time.sleep(60)

def dsc_signal_score(mint) -> int:
    """
    Returns 0-6 signal score based on DexScreener paid/organic activity:
      +2 currently boosted (dev paying for promotion RIGHT NOW)
      +2 running paid ads
      +1 top boost list (most total spend)
      +1 has DSC profile (dev invested in presentation)
      +1 in a trending narrative meta
      +1 community takeover
    """
    with _dsc_lock:
        return (
            (2 if mint in _dsc_boosted_mints   else 0) +
            (2 if mint in _dsc_ad_mints         else 0) +
            (1 if mint in _dsc_top_mints        else 0) +
            (1 if mint in _dsc_profile_mints    else 0) +
            (1 if mint in _dsc_meta_mints       else 0) +
            (1 if mint in _dsc_takeover_mints   else 0)
        )

def dsc_has_orders(mint) -> bool:
    """/orders/v1 — True if this token has at least one approved paid DSC order."""
    try:
        r = _session.get(f"{DSC_BASE}/orders/v1/solana/{mint}",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
        if r.status_code == 200:
            return any(o.get("status") == "approved" for o in (r.json() or []))
    except Exception:
        pass
    return False

def dsc_get_pairs(mint) -> list:
    """/token-pairs/v1 — all Solana trading pairs for a given token address."""
    try:
        r = _session.get(f"{DSC_BASE}/token-pairs/v1/solana/{mint}",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if r.status_code == 200:
            return (r.json() or {}).get("pairs", [])
    except Exception:
        pass
    return []

def dsc_batch_tokens(mints: list) -> dict:
    """/tokens/v1 — batch-fetch up to 30 token addresses, return {mint: best_pair}."""
    if not mints:
        return {}
    addrs = ",".join(mints[:30])
    try:
        r = _session.get(f"{DSC_BASE}/tokens/v1/solana/{addrs}",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code == 200:
            result = {}
            for p in ((r.json() or {}).get("pairs", [])):
                addr = (p.get("baseToken") or {}).get("address", "")
                if addr and addr not in result:
                    result[addr] = p
            return result
    except Exception:
        pass
    return {}

def dsc_search(query: str) -> list:
    """/latest/dex/search — search by token name/symbol, returns Solana pairs only."""
    try:
        r = _session.get(f"{DSC_BASE}/latest/dex/search",
                         params={"q": query},
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if r.status_code == 200:
            return [p for p in ((r.json() or {}).get("pairs", []))
                    if p.get("chainId", "").lower() == "solana"]
    except Exception:
        pass
    return []

# ── JUPITER AGENT SKILLS ─────────────────────────────────────────

def _jup_headers() -> dict:
    h = {"Accept": "application/json"}
    if JUPITER_API_KEY:
        h["x-api-key"] = JUPITER_API_KEY
    return h

def jup_tokens_search(mints: list) -> dict:
    """
    /tokens/v2/search?query={mints} — batch fetch token data for up to 100 mints.
    Populates _jup_token_cache. Returns {mint: token_data}.
    Each token_data contains: organicScore, isVerified, audit.{mintAuthorityAddress,
    freezeAuthorityAddress, isTop10HoldersNotContract, isTop10HoldersNotBridge}.
    """
    if not mints:
        return {}
    now = time.time()
    result, to_fetch = {}, []
    for m in mints[:100]:
        hit = _jup_token_cache.get(m)
        if hit and now - hit[0] < _JUP_TOKEN_CACHE_TTL:
            result[m] = hit[1]
        else:
            to_fetch.append(m)
    if not to_fetch:
        return result
    try:
        r = _session.get(
            f"{JUP_TOKENS_URL}/search",
            params={"query": ",".join(to_fetch)},
            headers=_jup_headers(), timeout=8
        )
        if r.status_code == 200:
            for item in (r.json() if isinstance(r.json(), list) else []):
                m = item.get("mint", "")
                if m:
                    _jup_token_cache[m] = (now, item)
                    result[m] = item
    except Exception:
        pass
    return result

def jup_price_v3(mints: list) -> dict:
    """
    /price/v3/price?ids={mints} — batch prices for up to 50 mints.
    Returns {mint: {usdPrice, priceChange24h, liquidity}}.
    """
    if not mints:
        return {}
    try:
        r = _session.get(
            JUP_PRICE_V3_URL,
            params={"ids": ",".join(mints[:50])},
            headers=_jup_headers(), timeout=8
        )
        if r.status_code == 200:
            return r.json().get("data", {})
    except Exception:
        pass
    return {}

def _refresh_jup_signals():
    """Refresh Jupiter trending, organic score, and top-traded token sets (all /5m windows)."""
    global _jup_trending_mints, _jup_organic_mints, _jup_toptraded_mints
    global _jup_verified_mints, _jup_signal_time
    hdrs = _jup_headers()

    def _mints_from(url):
        try:
            r = _session.get(url, headers=hdrs, timeout=8)
            if r.status_code == 200:
                data = r.json() or []
                return {item.get("mint") if isinstance(item, dict) else item
                        for item in data
                        if (isinstance(item, dict) and item.get("mint"))
                        or isinstance(item, str)}
        except Exception:
            pass
        return set()

    try:
        trending  = _mints_from(f"{JUP_TOKENS_URL}/toptrending/5m")
        organic   = _mints_from(f"{JUP_TOKENS_URL}/toporganicscore/5m")
        toptraded = _mints_from(f"{JUP_TOKENS_URL}/toptraded/5m")
        verified  = _mints_from(f"{JUP_TOKENS_URL}/tag/verified")

        with _jup_lock:
            _jup_trending_mints  = trending
            _jup_organic_mints   = organic
            _jup_toptraded_mints = toptraded
            _jup_verified_mints  = verified
            _jup_signal_time     = time.time()
        log("info",
            f"JUP: trending={len(trending)} organic={len(organic)} "
            f"toptraded={len(toptraded)} verified={len(verified)}", "JUP")
    except Exception as e:
        log("warn", f"JUP signal refresh: {e}", "JUP")

def run_jup_refresh_loop():
    """Background thread: refresh Jupiter signals every JUP_SIGNAL_REFRESH_SECS seconds."""
    time.sleep(30)
    while True:
        if time.time() - _jup_signal_time >= JUP_SIGNAL_REFRESH_SECS:
            _refresh_jup_signals()
        time.sleep(60)

def jup_token_signal_score(mint: str) -> int:
    """
    0-5 signal score from Jupiter token signals:
      +2 in toptrending/5m  (actively pumping right now)
      +2 in toporganicscore/5m  (real traders, not bots)
      +1 in toptraded/5m  (highest volume)
      +1 Jupiter-verified
    """
    with _jup_lock:
        return (
            (2 if mint in _jup_trending_mints  else 0) +
            (2 if mint in _jup_organic_mints    else 0) +
            (1 if mint in _jup_toptraded_mints  else 0) +
            (1 if mint in _jup_verified_mints   else 0)
        )

def jup_audit_ok(mint: str) -> tuple:
    """
    Check Jupiter's audit data (from token cache).
    Returns (ok, reason). Never blocks if token not in cache.
    """
    hit = _jup_token_cache.get(mint)
    if not hit:
        return True, ""
    audit = (hit[1] or {}).get("audit") or {}
    if audit.get("mintAuthorityAddress"):
        return False, "mint authority active"
    if audit.get("freezeAuthorityAddress"):
        return False, "freeze authority active"
    return True, ""

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

# ── POOL DETECTION + BUY HELPER ──────────────────────────────────
def _detect_pool(mint, symbol=""):
    """Ask pump.fun which pool this coin is on. Falls back to pump-swap → pump."""
    try:
        r = _session.get(f"https://frontend-api.pump.fun/coins/{mint}", timeout=5)
        if r.status_code == 200:
            d = r.json()
            if d.get("raydium_pool"):
                log("info", f"Pool detect: raydium", symbol)
                return "raydium"
            if d.get("is_pump_swap") or d.get("pump_swap_pool") or d.get("is_cashback_enabled"):
                log("info", f"Pool detect: pump-swap", symbol)
                return "pump-swap"
            log("info", f"Pool detect: pump", symbol)
            return "pump"
    except Exception as e:
        log("warn", f"Pool detect failed ({e}) — trying pump-swap", symbol)
    return "pump-swap"  # safer default: pump-swap works for most recent coins

def _buy_with_pool(keypair, mint, symbol, sol_amount, pool):
    """Call PumpPortal, sign, and send. Retries with pump if pump-swap returns error."""
    pools_to_try = [pool]
    if pool == "pump-swap":
        pools_to_try.append("pump")
    elif pool == "pump":
        pools_to_try.append("pump-swap")

    for p in pools_to_try:
        try:
            res = _session.post(
                PUMPPORTAL,
                headers={"Content-Type": "application/json"},
                json={"publicKey": WALLET, "action": "buy", "mint": mint,
                      "denominatedInSol": "true", "amount": sol_amount,
                      "slippage": 50, "priorityFee": 0.005, "pool": p},
                timeout=15
            )
            if res.status_code != 200:
                log("warn", f"PumpPortal {p} pool {res.status_code} — trying next pool", symbol)
                continue
            tx  = VersionedTransaction(VersionedTransaction.from_bytes(res.content).message, [keypair])
            sig = _send_tx(bytes(tx), symbol)
            if sig:
                log("ok", f"Bought via {p} pool", symbol)
                return sig
        except Exception as e:
            log("warn", f"Buy attempt on {p} pool: {e}", symbol)
    return None

# ── TRADE EXECUTION ──────────────────────────────────────────────
def execute_buy(mint, symbol, amount, pump_swap=False, raydium=False):
    if PAPER_MODE:
        log("ok", f"[PAPER] Buy ${amount:.2f} -> {symbol}", symbol)
        return "PAPER_TX"
    try:
        # Validate private key format before any network calls
        try:
            keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        except Exception as ke:
            log("err", f"INVALID PRIVATE KEY: {ke} — check Railway WALLET_PRIVATE_KEY (must be Phantom base58, ~88 chars)", symbol)
            return None

        sol_price = get_sol_price()
        if not sol_price:
            log("err", "Cannot get SOL price — buy aborted", symbol)
            return None
        sol_amount = round(amount / sol_price, 6)

        # Check SOL balance before calling PumpPortal
        try:
            _rpc = Client(_rpc_endpoints()[0])
            bal_resp = _rpc.get_balance(Pubkey.from_string(WALLET))
            sol_balance = bal_resp.value / 1e9
            min_needed = sol_amount + 0.005  # tx cost ~0.001–0.005 SOL
            if sol_balance < min_needed:
                log("err", f"LOW SOL: have {sol_balance:.4f} SOL, need {min_needed:.4f} (${amount:.2f} buy + gas) — top up Phantom", symbol)
                return None
            log("info", f"Wallet: {sol_balance:.4f} SOL available", symbol)
        except Exception as be:
            log("warn", f"SOL balance check failed: {be}", symbol)

        # Detect correct pool from pump.fun API — avoids on-chain swap failure from pool mismatch
        if raydium:
            pool = "raydium"
        elif pump_swap:
            pool = "pump-swap"
        else:
            pool = _detect_pool(mint, symbol)

        sig = _buy_with_pool(keypair, mint, symbol, sol_amount, pool)
        if sig:
            log("ok", f"solscan.io/tx/{sig}", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Buy error [{type(e).__name__}]: {e}", symbol)
        return None

def execute_sell(tokens, mint, symbol, pump_swap=False, raydium=False):
    if PAPER_MODE:
        log("ok", f"[PAPER] Sell {symbol}", symbol)
        return "PAPER_TX"
    try:
        # Always detect pool fresh — stored flags from buy time can be stale/wrong
        detected = _detect_pool(mint, symbol)
        if detected == "raydium":
            pools_to_try = ["raydium"]
        elif detected == "pump-swap":
            pools_to_try = ["pump-swap", "pump"]
        else:
            pools_to_try = ["pump", "pump-swap"]

        keypair  = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        # PumpPortal expects integer token count — float can cause 400 rejection
        tok_int  = int(tokens)

        for pool in pools_to_try:
            for fetch_attempt in range(2):  # retry with fresh blockhash if broadcast fails
                try:
                    # Escalate priority fee on retry — exits during pumps need higher priority
                    priority = 0.005 if fetch_attempt == 0 else 0.01
                    res = _session.post(
                        PUMPPORTAL,
                        headers={"Content-Type": "application/json"},
                        json={"publicKey": WALLET, "action": "sell", "mint": mint,
                              "denominatedInSol": "false", "amount": tok_int,
                              "slippage": 50, "priorityFee": priority, "pool": pool},
                        timeout=15
                    )
                    if res.status_code != 200:
                        log("warn", f"PumpPortal sell [{pool}] {res.status_code}: {res.text[:120]}", symbol)
                        break  # 4xx/5xx — try next pool, not retry same pool
                    if len(res.content) < 100:
                        log("warn", f"PumpPortal [{pool}]: bad response ({len(res.content)}b): {res.text[:80]}", symbol)
                        break
                    tx  = VersionedTransaction(VersionedTransaction.from_bytes(res.content).message, [keypair])
                    sig = _send_tx(bytes(tx), symbol)
                    if sig:
                        log("ok", f"Sold via {pool} | solscan.io/tx/{sig}", symbol)
                        return sig
                    # Broadcast failed — refetch fresh tx (fresh blockhash) and retry once
                    log("warn", f"Sell broadcast failed [{pool}] attempt {fetch_attempt+1} — refetching tx", symbol)
                    time.sleep(0.4)
                except Exception as pe:
                    log("warn", f"Sell [{pool}] attempt {fetch_attempt+1}: {type(pe).__name__}: {pe}", symbol)
                    break  # exception on this pool — move to next pool
        log("err", f"All sell pools exhausted for {symbol} ({mint[:8]})", symbol)
        return None
    except Exception as e:
        log("err", f"Sell error: {type(e).__name__}: {e}", symbol)
        return None

# ── PARTIAL EXIT ────────────────────────────────────────────────
def _partial_exit(mint, price, fraction, label):
    """Sell `fraction` of remaining tokens; updates trade in-place, adds proceeds to capital."""
    global capital
    with trades_lock:
        if mint not in open_trades:
            return
        trade = open_trades[mint]
        # Truncate to int: execute_sell sends int to PumpPortal; state must match
        tokens_to_sell = int(trade["tokens"] * fraction)
        if tokens_to_sell <= 0:
            return
        entry     = trade["entry"]
        raw_proceeds = tokens_to_sell * price
        # Cap: this partial slice is worth at most fraction*amount*(1+5x) — blocks token-count inflation
        max_proceeds = trade["amount"] * fraction * 6.0
        proceeds = min(raw_proceeds, max_proceeds)
        trade["tokens"]            -= tokens_to_sell
        trade["partial_proceeds"]  += proceeds
        trade["partial_tp_done"]   += 1
        symbol    = trade["symbol"]
        pump_swap = trade.get("pump_swap", False)
        raydium   = trade.get("raydium", False)

    # Execute sell FIRST; only credit capital once the transaction is confirmed
    sell_ok = execute_sell(tokens_to_sell, mint, symbol, pump_swap=pump_swap, raydium=raydium)
    if not sell_ok:
        # Revert trade state changes since sell failed
        with trades_lock:
            if mint in open_trades:
                open_trades[mint]["tokens"]           += tokens_to_sell
                open_trades[mint]["partial_proceeds"] -= proceeds
                open_trades[mint]["partial_tp_done"]  -= 1
        log("err", f"[{label}] Partial sell failed — trade state reverted", symbol)
        return

    with capital_lock:
        capital += proceeds

    move_pct = ((price - entry) / max(entry, 1e-12)) * 100
    pct_sold = int(fraction * 100)
    log("ok", f"[{label}] Sold {pct_sold}% at +{move_pct:.1f}% → ${proceeds:.4f} locked (cap=${capital:.2f})", symbol)
    notify(f"📤 {label} {symbol}", f"Sold {pct_sold}% at +{move_pct:.1f}%\nProceeds: +${proceeds:.4f}\nCapital: ${capital:.2f}")


# ── GHOST POSITION CLEANUP ───────────────────────────────────────
def _check_token_balance(mint):
    """Return wallet's token balance for a mint. 0 if not found or error."""
    if not WALLET:
        return 0
    for rpc_url in _rpc_endpoints():
        try:
            resp = _session.post(rpc_url, json={
                "jsonrpc": "2.0", "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [WALLET, {"mint": mint}, {"encoding": "jsonParsed"}]
            }, timeout=8)
            if resp.status_code != 200:
                continue
            accounts = resp.json().get("result", {}).get("value", [])
            for acct in accounts:
                ui = (acct.get("account", {}).get("data", {})
                      .get("parsed", {}).get("info", {})
                      .get("tokenAmount", {}).get("uiAmount", 0) or 0)
                if ui > 0:
                    return ui
            return 0
        except Exception:
            continue
    return 0

def _verify_tx_landed(sig, mint, symbol, amount):
    """Background: check wallet token balance. If tokens never arrived, cleanup ghost."""
    for attempt, delay in [(1, 10), (2, 20), (3, 30)]:
        time.sleep(delay)
        try:
            bal = _check_token_balance(mint)
            if bal > 0:
                # Tokens confirmed — update actual count (slippage means estimate was wrong)
                with trades_lock:
                    if mint in open_trades:
                        open_trades[mint].pop("_unverified", None)
                        open_trades[mint]["tokens"] = bal
                log("ok", f"Buy confirmed: {bal:.0f} tokens in wallet ✓ (estimated {amount})", symbol)
                return
        except Exception as e:
            log("warn", f"Balance check attempt {attempt}: {e}", symbol)
        if attempt == 3:
            log("warn", f"No tokens found after {10+20+30}s — ghost cleanup", symbol)
            _cleanup_ghost(mint, amount, symbol)

def _cleanup_ghost(mint, amount, symbol):
    global capital, _daily_trades
    with trades_lock:
        if mint in open_trades:
            open_trades.pop(mint)
            with capital_lock:
                capital += amount
            with _daily_lock:
                _daily_trades = max(0, _daily_trades - 1)
            redis_save("bot_open_trades", list(open_trades.values()))
            log("ok", f"Ghost cleared — ${amount:.2f} refunded, daily trade counter corrected", symbol)
            notify(f"⚠️ Ghost Cleared {symbol}", f"TX failed on-chain. ${amount:.2f} refunded.")

# ── ENTER / EXIT ─────────────────────────────────────────────────
def enter_trade(mint, symbol, entry_price, amount, strategy, bond_entry=0, replies=0, pump_swap=False, raydium=False):
    global capital, _daily_trades
    if BOT_PAUSED or _pause_until > time.time():
        return False
    if daily_limit_reached():
        _log_scan(symbol, mint, bond_entry, 0, "cap", -1, "DAILY CAP / COOLDOWN")
        return False

    # Skip recently sold coins — check before any reservation
    with _copy_lock:
        sold_at = _sold_mints.get(mint, 0)
        if sold_at and time.time() - sold_at < SOLD_COOLDOWN_SECS:
            return False

    # Atomically reserve capital — check + deduct in one lock acquisition to prevent
    # two concurrent scanner threads from both passing the check and double-spending
    with capital_lock:
        if capital < amount:
            return False
        capital -= amount  # RESERVED: refunded below if buy fails

    # Atomically reserve trade slot — prevent exceeding MAX_OPEN under concurrent load.
    # Uses _unverified=True so monitor skips the placeholder until real data is written.
    # Lock order: trades_lock outer, capital_lock inner — consistent with _cleanup_ghost.
    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            with capital_lock:
                capital += amount  # Release reservation
            return False
        open_trades[mint] = {"symbol": symbol, "mint": mint, "_unverified": True}

    tx = execute_buy(mint, symbol, amount, pump_swap=pump_swap, raydium=raydium)
    if not tx:
        # Buy failed — release both reservations
        with trades_lock:
            open_trades.pop(mint, None)
        with capital_lock:
            capital += amount
        return False

    with _daily_lock:
        _daily_trades += 1

    # Capital already deducted during reservation — just write the full trade record
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
            "price_high":        entry_price,
            "replies":           replies,
            "pump_swap":         pump_swap,
            "raydium":           raydium,
            "partial_tp_done":   0,
            "partial_proceeds":  0.0,
            "tx":                tx if tx not in (None, "PAPER_TX") else "",
            "_unverified":       not PAPER_MODE and tx not in (None, "PAPER_TX"),
        }

    log("ok", f"ENTER [{strategy.upper()}] ${amount:.2f} | bond={bond_entry:.1f}% | tx={str(tx)[:12]}...", symbol)
    _log_scan(symbol, mint, bond_entry, 0, "pass", -1, f"ENTERED [{strategy.upper()}] ${amount:.2f}")
    notify(f"🟢 BUY {symbol}",
           f"Strategy: {strategy.upper()}\nAmount: ${amount:.2f}\nBond: {bond_entry:.1f}%\nReplies: {replies}")
    with trades_lock:
        redis_save("bot_open_trades", list(open_trades.values()))
    # Background: verify tokens landed in wallet, cleanup ghost if not
    if not PAPER_MODE and tx not in (None, "PAPER_TX"):
        threading.Thread(target=_verify_tx_landed, args=(tx, mint, symbol, amount), daemon=True).start()
    return True

def _verify_sell_and_retry(sig, trade, mint, clamped_return, reason):
    """Background: verify sell tx landed. Uses Helius first, balance check as ground truth."""
    symbol = trade["symbol"]
    for attempt, delay in [(1, 12), (2, 25)]:
        time.sleep(delay)
        for rpc_url in _rpc_endpoints():
            try:
                status = Client(rpc_url).get_signature_statuses([sig])
                tx_s   = status.value[0] if status.value else None
                if tx_s is not None and not tx_s.err:
                    return  # confirmed on-chain — done
                if tx_s is not None and tx_s.err:
                    break  # explicitly failed — stop checking this attempt
                break  # got response, tx pending — wait for next attempt
            except Exception:
                continue
    # Status inconclusive — use token balance as ground truth
    bal = _check_token_balance(mint)
    if bal == 0:
        log("ok", f"Sell confirmed via balance (tokens gone from wallet)", symbol)
        return  # tokens gone = sell worked even if status check uncertain
    # Tokens still in wallet — sell genuinely failed, retry once
    log("warn", f"Sell tx failed — {bal:.0f} tokens still in wallet — retrying", symbol)
    retry_sig = execute_sell(bal, mint, symbol,
                             pump_swap=trade.get("pump_swap", False),
                             raydium=trade.get("raydium", False))
    if retry_sig:
        log("ok", f"Sell retry submitted: {retry_sig[:12]}...", symbol)
        return
    # Retry also failed — tokens stuck, correct capital
    global capital
    with capital_lock:
        capital -= clamped_return
    log("err", f"Sell failed twice — tokens stuck. Capital -${clamped_return:.2f}. Sell manually in Phantom.", symbol)
    notify(f"⚠️ STUCK {symbol}", f"Sell failed twice. Tokens still in wallet.\nSell manually in Phantom.\nCapital corrected -${clamped_return:.2f}")

def exit_trade(mint, price, reason, bond=0):
    global capital
    with trades_lock:
        if mint not in open_trades:
            return
        if open_trades[mint].get("_exiting"):
            return
        open_trades[mint]["_exiting"] = True
        trade = open_trades.pop(mint)

    amount           = trade["amount"]
    partial_proceeds = trade.get("partial_proceeds", 0.0)
    hold_m           = (time.time() - trade["opened_at"]) / 60
    final_value      = trade["tokens"] * price if price > 0 else 0
    pnl              = (partial_proceeds + final_value) - amount
    pnl              = max(-amount, min(pnl, amount * 5))
    clamped_return   = max(0.0, amount - partial_proceeds + pnl)

    # Fire sell immediately — do NOT block waiting for on-chain confirmation
    sell_sig = execute_sell(trade["tokens"], mint, trade["symbol"],
                            pump_swap=trade.get("pump_swap", False),
                            raydium=trade.get("raydium", False))
    if not sell_sig:
        # Couldn't even submit — put position back for retry next tick
        with trades_lock:
            trade.pop("_exiting", None)
            open_trades[mint] = trade
        log("err", "Sell submit failed — position restored, retrying next tick", trade["symbol"])
        return

    # Optimistically credit capital immediately — position already closed fast
    with capital_lock:
        capital += clamped_return

    # Background verify — if it failed on-chain, retry once and correct capital
    threading.Thread(target=_verify_sell_and_retry,
                     args=(sell_sig, trade, mint, clamped_return, reason),
                     daemon=True).start()

    sign = "+" if pnl >= 0 else ""
    log("ok" if pnl >= 0 else "err",
        f"{'WIN' if pnl>=0 else 'LOSS'} {reason} | {sign}${pnl:.4f} | {hold_m:.1f}m | cap=${capital:.2f}",
        trade["symbol"])
    emoji = "✅" if pnl >= 0 else "❌"
    notify(f"{emoji} {'WIN' if pnl>=0 else 'LOSS'} {trade['symbol']}",
           f"Reason: {reason}\nPnL: {sign}${pnl:.4f}\nHeld: {hold_m:.1f} min\nCapital: ${capital:.2f}")

    with _copy_lock:
        _sold_mints[mint] = time.time()  # 30 min cooldown before re-buying
    record_daily_trade(won=(pnl > 0))

    pnl_pct = round(pnl / max(amount, 1e-12) * 100, 2)
    global _trade_id_counter
    with _trade_id_lock:
        _trade_id_counter += 1
        trade_id = _trade_id_counter
    rec = {
        "id":         trade_id,
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
def _check_one_position(mint):
    """Check a single open position — runs in parallel per-coin thread."""
    try:
        with trades_lock:
            if mint not in open_trades or open_trades[mint].get("_exiting"):
                return
            # Skip until token balance confirmed in wallet (~60s window)
            if open_trades[mint].get("_unverified"):
                return
            trade = dict(open_trades[mint])
        symbol   = trade["symbol"]
        strategy = trade["strategy"]
        elapsed  = time.time() - trade["opened_at"]

        details = get_bonding_details(mint)
        bond    = details["bond_pct"] if details else 0

        # Use real price from pump.fun virtual reserves — accurate constant-product AMM price
        # Falls back to DexScreener for graduated/raydium tokens
        market = None
        if strategy in ("bond", "copy", "bundle", "trench") and details:
            price = details.get("price_usd", 0) or 0
            if price <= 0:
                market = get_market_data(mint)
                price = market["price"] if market and market["price"] > 0 else trade["entry"]
        else:
            market = get_market_data(mint)
            price = market["price"] if market and market["price"] > 0 else trade["entry"]

        with trades_lock:
            if mint not in open_trades or open_trades[mint].get("_exiting"):
                return
            if bond > open_trades[mint]["bond_high"]:
                open_trades[mint]["bond_high"]      = bond
                open_trades[mint]["bond_last_moved"] = time.time()
            elif details is None:
                # Transient pump.fun API failure — preserve existing bond state so
                # DEAD/STALE timers don't expire on a network blip; use cached high
                bond = open_trades[mint]["bond_high"]
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

        if bond_high >= SLIP_TRIGGER and bond_drop <= -SHARP_DROP_PCT:
            log("warn", f"SHARP DROP bond={bond:.1f}% drop={bond_drop:.1f}%", symbol)
            exit_trade(mint, price, "SHARP_DROP", bond); return

        if bond_high >= SLIP_TRIGGER and bond <= SLIP_DROP_TO:
            with trades_lock:
                if mint not in open_trades or open_trades[mint].get("_exiting"):
                    return
                if open_trades[mint]["bond_slip_start"] is None:
                    open_trades[mint]["bond_slip_start"] = time.time()
                    log("warn", f"Bond slip {bond:.1f}% — watching {SLIP_WAIT_SECS}s", symbol)
                elif time.time() - open_trades[mint]["bond_slip_start"] >= SLIP_WAIT_SECS:
                    exit_trade(mint, price, "BOND_SLIP", bond); return
        else:
            with trades_lock:
                if mint in open_trades:
                    open_trades[mint]["bond_slip_start"] = None

        if strategy == "bundle" and bond >= BUNDLE_RIDE_TP:
            exit_trade(mint, price, "BUNDLE_TP", bond); return

        if strategy in ("bond", "bundle", "trench"):
            bond_moved = bond_last_moved > trade["opened_at"] + 5
            if not bond_moved and elapsed >= DEAD_PAIR_SECS:
                log("warn", f"Dead pair — no volume in {elapsed:.0f}s — exiting", symbol)
                exit_trade(mint, price, "DEAD", bond); return
            stale_secs = time.time() - bond_last_moved
            if bond_moved and stale_secs >= VOL_STALE_SECS:
                log("warn", f"Volume stale {stale_secs:.0f}s — exiting", symbol)
                exit_trade(mint, price, "STALE", bond); return
        else:
            stale_secs = time.time() - bond_last_moved

        if strategy in ("bond", "bundle") and elapsed > 30 and stale_secs >= BOND_STALE_SECS:
            log("warn", f"Bond stale {stale_secs:.0f}s — exiting", symbol)
            exit_trade(mint, price, "STALE", bond); return

        entry_gain_pct = ((price_high - trade["entry"]) / max(trade["entry"], 1e-12)) * 100
        _sl_pct = {
            "bond": BOND_SL_PCT, "bundle": BOND_SL_PCT, "trench": TRENCH_SL_PCT,
            "spike": SPIKE_SL_PCT, "copy": COPY_SL_PCT, "fast": FAST_SL_PCT,
            "migrate": MIGRATE_SL_PCT,
        }.get(strategy, BOND_SL_PCT)
        tsl_price = (price_high if entry_gain_pct >= TSL_ACTIVATE_PCT else trade["entry"]) * (1 - _sl_pct / 100)

        if strategy == "bond" and GRAD_THROUGH and details and details.get("complete"):
            with trades_lock:
                if mint in open_trades:
                    open_trades[mint]["strategy"]       = "migrate"
                    open_trades[mint]["raydium"]        = True
                    open_trades[mint]["grad_opened_at"] = time.time()
            strategy = "migrate"
            log("ok", f"GRADUATED → riding on Raydium (bond={bond:.1f}%)", symbol)

        move_pct     = ((price - trade["entry"]) / max(trade["entry"], 1e-12)) * 100
        partial_done = trade.get("partial_tp_done", 0)

        if partial_done == 0 and move_pct >= PARTIAL_TP1_PCT:
            _partial_exit(mint, price, 0.30, "PARTIAL_TP1")
            with trades_lock:
                if mint not in open_trades or open_trades[mint].get("_exiting"):
                    return
                trade = dict(open_trades[mint])
            partial_done = 1

        if partial_done == 1 and move_pct >= PARTIAL_TP2_PCT:
            _partial_exit(mint, price, 0.30, "PARTIAL_TP2")
            with trades_lock:
                if mint not in open_trades or open_trades[mint].get("_exiting"):
                    return
                trade = dict(open_trades[mint])

        if strategy == "bond":
            move = ((price - trade["entry"]) / max(trade["entry"], 1e-12)) * 100
            if move >= BOND_TP_PCT:
                exit_trade(mint, price, "BOND_TP", bond); return
            if bond >= BOND_GRAD_BOND and entry_gain_pct >= 3:
                tight_tsl = price_high * (1 - BOND_GRAD_TSL / 100)
                if price <= tight_tsl:
                    exit_trade(mint, price, "BOND_GRAD_TSL", bond); return
            if price <= tsl_price:
                exit_trade(mint, price, "BOND_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "BOND_SL", bond); return
            if elapsed >= BOND_MAX_SECS:
                exit_trade(mint, price, "BOND_TIME", bond); return

        elif strategy == "spike":
            move = ((price - trade["entry"]) / trade["entry"]) * 100
            if move >= SPIKE_TP_PCT:
                exit_trade(mint, price, "SPIKE_TP", bond); return
            if price <= tsl_price:
                exit_trade(mint, price, "SPIKE_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "SPIKE_SL", bond); return
            if elapsed >= SPIKE_MAX_SECS:
                exit_trade(mint, price, "SPIKE_TIME", bond); return

        elif strategy == "copy":
            # Bond-based exits — same as bond runner; price feed unreliable for new tokens
            move = ((price - trade["entry"]) / max(trade["entry"], 1e-12)) * 100
            if move >= BOND_TP_PCT:
                exit_trade(mint, price, "COPY_TP", bond); return
            if bond >= BOND_GRAD_BOND and entry_gain_pct >= 3:
                tight_tsl = price_high * (1 - BOND_GRAD_TSL / 100)
                if price <= tight_tsl:
                    exit_trade(mint, price, "COPY_GRAD_TSL", bond); return
            if price <= tsl_price:
                exit_trade(mint, price, "COPY_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "COPY_SL", bond); return
            if elapsed >= BOND_MAX_SECS:
                exit_trade(mint, price, "COPY_TIME", bond); return

        elif strategy == "fast":
            move = ((price - trade["entry"]) / trade["entry"]) * 100
            if move >= FAST_TP_PCT:
                exit_trade(mint, price, "FAST_TP", bond); return
            if price <= trade["entry"] * (1 - FAST_SL_PCT / 100):
                exit_trade(mint, price, "FAST_SL", bond); return
            if elapsed >= FAST_MAX_SECS:
                exit_trade(mint, price, "FAST_TIME", bond); return

        elif strategy == "trench":
            move = ((price - trade["entry"]) / trade["entry"]) * 100
            if bond >= 99:
                exit_trade(mint, price, "TRENCH_GRAD", bond); return
            if move >= TRENCH_TP_PCT:
                exit_trade(mint, price, "TRENCH_TP", bond); return
            if price <= tsl_price:
                exit_trade(mint, price, "TRENCH_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "TRENCH_SL", bond); return
            if elapsed >= TRENCH_MAX_SECS:
                exit_trade(mint, price, "TRENCH_TIME", bond); return

        elif strategy == "migrate":
            migrate_elapsed = time.time() - trade.get("grad_opened_at", trade["opened_at"])
            move = ((price - trade["entry"]) / trade["entry"]) * 100
            if move >= MIGRATE_TP_PCT:
                exit_trade(mint, price, "MIGRATE_TP", bond); return
            if price <= tsl_price:
                exit_trade(mint, price, "MIGRATE_TSL" if entry_gain_pct >= TSL_ACTIVATE_PCT else "MIGRATE_SL", bond); return
            if migrate_elapsed >= MIGRATE_MAX_SECS:
                exit_trade(mint, price, "MIGRATE_TIME", bond); return

        pct = ((price - trade["entry"]) / max(trade["entry"], 1e-12)) * 100
        tsl_info = f" TSL@{tsl_price:.6f}" if entry_gain_pct >= TSL_ACTIVATE_PCT else ""
        log("info", f"[{strategy}] bond={bond:.1f}% price={pct:+.1f}% peak={entry_gain_pct:+.1f}%{tsl_info} {elapsed/60:.1f}m", symbol)
    except Exception as e:
        log("warn", f"Monitor [{mint[:8]}]: {e}")

def monitor_loop():
    while True:
        try:
            time.sleep(1)
            with trades_lock:
                mints = list(open_trades.keys())
            if not mints:
                continue
            with ThreadPoolExecutor(max_workers=min(len(mints), 4)) as ex:
                ex.map(_check_one_position, mints)
        except Exception as e:
            log("warn", f"Monitor loop: {e}")

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
        # Re-register webhook so new wallet addresses are included
        threading.Thread(target=_register_helius_webhook, daemon=True).start()
    except Exception as e:
        log("warn", f"fetch_smart_wallets: {e}", "COPY")

def _parse_helius_enhanced_tx(tx, wallet_addr):
    """Parse one Helius enhanced tx object into a buy-event dict.
    Returns None if the tx is not a pump.fun buy into wallet_addr."""
    if "PUMP" not in tx.get("source", "").upper():
        return None
    addr_lc = wallet_addr.lower()
    mint = None
    for t in tx.get("tokenTransfers", []):
        if t.get("toUserAccount", "").lower() == addr_lc and t.get("mint"):
            mint = t["mint"]
            break
    if not mint:
        return None
    sol_lamports = sum(
        n.get("amount", 0)
        for n in tx.get("nativeTransfers", [])
        if n.get("fromUserAccount", "").lower() == addr_lc
    )
    sol_price = get_sol_price() or 150
    return {
        "token_address": mint,
        "token_symbol":  mint[:8],
        "timestamp":     tx.get("timestamp", 0),
        "cost":          (sol_lamports / 1e9) * sol_price,
    }

def _helius_wallet_buys(addr):
    """Fetch recent pump.fun buys for a wallet via Helius Enhanced Transactions API.
    Returns list of {token_address, token_symbol, timestamp, cost} dicts."""
    if not HELIUS_API_KEY:
        return []
    try:
        res = _session.get(
            f"https://api.helius.xyz/v0/addresses/{addr}/transactions",
            params={"api-key": HELIUS_API_KEY, "type": "SWAP", "limit": 10},
            timeout=8
        )
        if res.status_code != 200:
            log("warn", f"Helius {res.status_code} for {addr[:8]}", "COPY")
            return []
        acts = [a for tx in res.json() if (a := _parse_helius_enhanced_tx(tx, addr))]
        return acts
    except Exception as e:
        log("warn", f"Helius wallet buys ({addr[:8]}): {e}", "COPY")
        return []

def _all_tracked_addrs():
    """Return set of all wallet addresses currently being tracked for copy trading."""
    with _copy_lock:
        addrs = {w["address"] for w in _copy_wallets}
    addrs.update(TRACKED_WALLETS)
    addrs.update(PINNED_WALLETS)
    addrs.update(FAST_WALLETS)
    return addrs

def _register_helius_webhook():
    """Create or update the Helius webhook so all tracked wallets push to this Railway instance.
    No-op if HELIUS_API_KEY or RAILWAY_PUBLIC_DOMAIN are not set."""
    global _helius_wh_id
    if not HELIUS_API_KEY or not RAILWAY_PUBLIC_DOMAIN:
        return
    all_addrs = _all_tracked_addrs()
    if not all_addrs:
        return
    webhook_url = f"https://{RAILWAY_PUBLIC_DOMAIN}/webhook/helius"
    body = {
        "webhookURL":       webhook_url,
        "transactionTypes": ["SWAP"],
        "accountAddresses": list(all_addrs),
        "webhookType":      "enhanced",
    }
    if HELIUS_WEBHOOK_AUTH:
        body["authHeader"] = HELIUS_WEBHOOK_AUTH
    try:
        with _helius_wh_lock:
            wh_id = _helius_wh_id
        if wh_id:
            res = _session.put(
                f"https://api.helius.xyz/v0/webhooks/{wh_id}",
                params={"api-key": HELIUS_API_KEY},
                json=body, timeout=12
            )
        else:
            res = _session.post(
                "https://api.helius.xyz/v0/webhooks",
                params={"api-key": HELIUS_API_KEY},
                json=body, timeout=12
            )
        if res.status_code in (200, 201):
            new_id = res.json().get("webhookID", wh_id or "")
            with _helius_wh_lock:
                _helius_wh_id = new_id
            redis_save("helius_wh_id", new_id)
            verb = "updated" if wh_id else "registered"
            log("ok", f"Helius webhook {verb}: {len(all_addrs)} addrs → {webhook_url}", "WH")
        else:
            log("warn", f"Helius webhook register failed {res.status_code}: {res.text[:120]}", "WH")
    except Exception as e:
        log("warn", f"Helius webhook register error: {e}", "WH")

def _process_copy_act(w, act, source="POLL"):
    """Evaluate and enter one copy trade opportunity.
    w = wallet info dict (address, winrate, fast/pinned flags).
    act = buy event dict (token_address, token_symbol, timestamp, cost).
    source = "PUSH" (Helius webhook) or "POLL" (polling fallback).
    Returns True if a trade was attempted."""
    if daily_limit_reached():
        return False
    mint   = act.get("token_address", "")
    symbol = act.get("token_symbol", mint[:8] if mint else "?")
    if not mint:
        return False
    # Polling path: skip stale trades. Push events are always fresh.
    if source == "POLL":
        ts       = int(act.get("timestamp", 0) or 0)
        age_secs = time.time() - ts if ts > 0 else 9999
        if age_secs > COPY_MAX_AGE_SECS:
            return False
    with _copy_lock:
        if mint in _copied_mints:
            return False
    with trades_lock:
        if mint in open_trades:
            return False
    if mint in blacklisted_mints:
        return False
    with _copy_lock:
        sold_at = _sold_mints.get(mint, 0)
        if sold_at and time.time() - sold_at < SOLD_COOLDOWN_SECS:
            return False
    whale_cost = float(act.get("cost", act.get("cost_usd", act.get("usd_value", 0))) or 0)
    if whale_cost > 0 and whale_cost < COPY_MIN_WHALE_USD:
        log("info", f"COPY SKIP: whale only ${whale_cost:.0f}<{COPY_MIN_WHALE_USD:.0f}", symbol)
        return False
    is_fast = w.get("fast", False)
    if not is_fast:
        rug = run_rugcheck(mint)
        if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
            _blacklist_add(mint)
            return False
        if rug and rug.get("is_bundled") and BUNDLE_MODE == "avoid":
            return False
        holder_ok, holder_reason = check_holder_concentration(mint)
        if not holder_ok:
            log("warn", f"SKIP: {holder_reason}", symbol)
            return False
        dev_wallet = act.get("creator", "") or act.get("dev", "")
        dev_ok, dev_reason = check_dev_history(dev_wallet)
        if not dev_ok:
            log("warn", f"SKIP: {dev_reason}", symbol)
            return False
        if gmgn_smart_money_selling(mint):
            log("warn", "SKIP: smart money selling", symbol)
            return False
    sig_score = gmgn_signal_score(mint)
    market = get_market_data(mint)
    if not market or market["price"] <= 0:
        bond_det = get_bonding_details(mint)
        if not bond_det or bond_det.get("price_usd", 0) <= 0:
            return False
        market = {"price": bond_det["price_usd"], "liq": 0,
                  "vol_m5": 0, "buys_m5": 0, "sells_m5": 0,
                  "change5m": 0, "pair_address": ""}
    if market.get("vol_m5", 0) < MIN_VOL_5M and market.get("pair_address"):
        log("info", f"COPY SKIP: vol ${market.get('vol_m5',0):.0f}<{MIN_VOL_5M:.0f}", symbol)
        return False
    if not is_fast and market.get("pair_address") and market["liq"] < MIN_LIQ:
        return False
    with _copy_lock:
        _copied_mints[mint] = time.time()
    addr   = w["address"]
    prefix = "[PUSH] " if source == "PUSH" else ""
    amt    = trade_size()
    if is_fast:
        log("ok", f"{prefix}FAST {addr[:8]}... NO-FILTER | ${amt:.2f}", symbol)
        notify(f"⚡ {'PUSH ' if source=='PUSH' else ''}FAST {symbol}",
               f"Wallet: {addr[:8]}...\nAmount: ${amt:.2f}")
        enter_trade(mint, symbol, market["price"], amt, "fast", 0, 0)
    else:
        tag = "PINNED" if w.get("pinned") else "COPY"
        log("ok", f"{prefix}{tag} {addr[:8]}... WR:{w['winrate']}% 5m={market.get('change5m',0):+.1f}% | ${amt:.2f} | sig={sig_score}", symbol)
        notify(f"📋 {'PUSH ' if source=='PUSH' else ''}{tag} {symbol}",
               f"Wallet: {addr[:8]}...\nWin rate: {w['winrate']}%\nAmount: ${amt:.2f}")
        bond_now = get_bonding_details(mint)
        if not bond_now:
            log("warn", "COPY SKIP: bond API unavailable — no price tracking", symbol)
            with _copy_lock:
                _copied_mints.pop(mint, None)
            return False
        bond_entry_pct = bond_now["bond_pct"]
        entry_px = bond_now.get("price_usd", 0) or market["price"]
        if entry_px <= 0:
            log("warn", "COPY SKIP: no valid entry price", symbol)
            with _copy_lock:
                _copied_mints.pop(mint, None)
            return False
        enter_trade(mint, symbol, entry_px, amt, "copy", bond_entry_pct, 0)
    return True

def copy_trade_loop():
    time.sleep(15)
    fetch_smart_wallets()
    while scan_active:
        try:
            if BOT_PAUSED or _pause_until > time.time():
                time.sleep(5)
                continue
            # Refresh wallet list every hour (or retry after backoff expires)
            with _copy_lock:
                stale = time.time() - _copy_wallet_time > COPY_REFRESH_MINS * 60
            if stale or (_gmgn_backoff > 0 and time.time() >= _gmgn_backoff):
                fetch_smart_wallets()

            with _copy_lock:
                now = time.time()
                expired = [m for m, t in _copied_mints.items() if now - t > 1800]
                for m in expired:
                    _copied_mints.pop(m, None)

            # ── Drain Helius push queue first (instant, no polling lag) ──
            drained = 0
            while _webhook_queue:
                try:
                    w, act = _webhook_queue.popleft()
                except IndexError:
                    break
                try:
                    _process_copy_act(w, act, source="PUSH")
                    drained += 1
                except Exception as e:
                    log("warn", f"Webhook act process: {e}", "PUSH")
            if drained:
                log("info", f"Drained {drained} webhook event(s)", "PUSH")

            # ── Polling fallback — catches anything missed by webhook ──
            with _copy_lock:
                wallets = list(_copy_wallets)
            tracked_addrs = {w["address"] for w in wallets}
            for addr in TRACKED_WALLETS:
                if addr not in tracked_addrs:
                    tracked_addrs.add(addr)
                    wallets.append({"address": addr, "winrate": 100.0})
            for addr in PINNED_WALLETS:
                if addr not in tracked_addrs:
                    tracked_addrs.add(addr)
                    wallets.append({"address": addr, "winrate": 100.0, "pinned": True})
            for addr in FAST_WALLETS:
                if addr not in tracked_addrs:
                    tracked_addrs.add(addr)
                    wallets.append({"address": addr, "winrate": 100.0, "fast": True})

            if not wallets:
                time.sleep(60)
                continue

            for w in wallets:
                if daily_limit_reached():
                    break
                try:
                    acts = _helius_wallet_buys(w["address"])
                    for act in acts:
                        _process_copy_act(w, act, source="POLL")
                    time.sleep(0.5)
                except Exception as e:
                    log("warn", f"Wallet {w['address'][:8]} activity: {e}", "COPY")
        except Exception as e:
            log("err", f"Copy loop: {e}", "COPY")
        time.sleep(15)

# ── COIN EVALUATOR (runs in thread pool) ─────────────────────────
def _eval_coin(coin):
    """Evaluate one coin for all strategies. All blocking I/O runs here in a thread.
    Returns a trade action dict or None. Never calls enter_trade."""
    mint   = coin["mint"]
    symbol = coin["symbol"]
    bond   = coin.get("bond_pct", 0)
    _sig_pre = sum([bool(coin.get("twitter")), bool(coin.get("telegram")), bool(coin.get("website"))])

    # Bond Runner
    if BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX:
        details = get_bonding_details(mint)
        if details:
            bond = details["bond_pct"]
            if details.get("complete"):
                return None
        if not (BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX):
            return None
        dev_wallet = coin.get("creator", "") or coin.get("dev", "")
        if _is_bundle_deployer(dev_wallet):
            _log_scan(symbol, mint, bond, _sig_pre, "dev", 2, "SERIAL BUNDLER")
            return None
        rug = run_rugcheck(mint)
        if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
            return {"action": "blacklist", "mint": mint}
        if rug and rug.get("is_bundled"):
            _record_bundle_deployer(dev_wallet, symbol)
            if BUNDLE_MODE == "avoid":
                return None
        if rug and rug.get("score", 0) > MAX_RUG_SCORE:
            _log_scan(symbol, mint, bond, _sig_pre, "rug", 3, f"RUG SCORE {rug['score']}")
            return None
        jup_ok, jup_reason = jup_audit_ok(mint)
        if not jup_ok:
            _log_scan(symbol, mint, bond, _sig_pre, "jup", 3, jup_reason[:18].upper())
            return {"action": "blacklist", "mint": mint}
        holder_ok, holder_reason = check_holder_concentration(mint)
        if not holder_ok:
            _log_scan(symbol, mint, bond, _sig_pre, "holder", 4, holder_reason[:18].upper())
            return None
        dev_ok, dev_reason = check_dev_history(dev_wallet)
        if not dev_ok:
            _log_scan(symbol, mint, bond, _sig_pre, "dev", 5, dev_reason[:18].upper())
            return None
        if gmgn_smart_money_selling(mint):
            _log_scan(symbol, mint, bond, _sig_pre, "sm", 6, "SMART $ SELLING")
            return None
        sig_score = gmgn_signal_score(mint) + dsc_signal_score(mint) + jup_token_signal_score(mint)
        # Bond runner buys BEFORE smart money arrives — no signal floor here
        # Bond velocity: skip if bond hasn't moved ≥0.2% in last 30s (loosened from 0.5%/2s)
        _now_ts = time.time()
        with _bond_prev_lock:
            _prev_bond, _prev_ts = _bond_prev.get(mint, (None, 0))
            _bond_prev[mint] = (bond, _now_ts)
        if _prev_bond is not None and abs(bond - _prev_bond) < 0.2 and _now_ts - _prev_ts < 120:
            _log_scan(symbol, mint, bond, _sig_pre, "vel", 7, "BOND STALLED")
            return None
        market = get_market_data(mint)
        if not market or not market.get("pair_address") or market["price"] <= 0:
            # DexScreener hasn't indexed yet — derive price from bonding curve
            _sol_p = get_sol_price()
            _vtok  = coin.get("vtok", 0)
            _vsol  = coin.get("vsol", 0)
            _curve_price = (_vsol / _vtok * 1e6) * _sol_p if (_vtok > 0 and _sol_p) else 0
            if _curve_price <= 0:
                _log_scan(symbol, mint, bond, _sig_pre, "vol", 8, "NO PRICE DATA")
                return None
            # Skip DexScreener volume/momentum/impact gates — bonding curve is the exit
            _log_scan(symbol, mint, bond, _sig_pre, "vol", 8, f"CURVE ${_curve_price:.6f}")
            return {"action": "trade", "strategy": "bond", "mint": mint, "symbol": symbol,
                    "price": _curve_price, "bond": bond, "sig_score": sig_score,
                    "pump_swap": coin.get("pump_swap", False),
                    "market": {"price": _curve_price, "liq": 0, "vol_m5": 0}}
        if market.get("vol_m5", 0) < MIN_VOL_5M:
            _log_scan(symbol, mint, bond, _sig_pre, "vol", 8, f"VOL ${market['vol_m5']:.0f}<{MIN_VOL_5M:.0f}")
            return None
        if market.get("change5m", 0) < -3:
            _log_scan(symbol, mint, bond, _sig_pre, "mom", 8, f"5M DOWN {market['change5m']:.1f}%")
            return None
        impact = jup_price_impact(mint, trade_size())
        if impact > JUP_IMPACT_MAX_PCT:
            _log_scan(symbol, mint, bond, _sig_pre, "liq", 9, f"IMPACT {impact:.1f}%")
            return None
        return {"action": "trade", "strategy": "bond", "mint": mint, "symbol": symbol,
                "price": market["price"], "bond": bond, "sig_score": sig_score,
                "pump_swap": coin.get("pump_swap", False), "market": market}

    # Trench Runner
    if TRENCH_ENTRY_MIN <= bond <= TRENCH_ENTRY_MAX:
        _trench_dev = coin.get("creator", "") or coin.get("dev", "")
        if _is_bundle_deployer(_trench_dev):
            return None
        rug = run_rugcheck(mint)
        if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
            return {"action": "blacklist", "mint": mint}
        if rug and rug.get("is_bundled"):
            _record_bundle_deployer(_trench_dev, symbol)
            if BUNDLE_MODE == "avoid":
                return None
        if rug and rug.get("score", 0) > MAX_RUG_SCORE:
            _log_scan(symbol, mint, bond, _sig_pre, "rug", 3, f"RUG SCORE {rug['score']}")
            return None
        if gmgn_smart_money_selling(mint):
            _log_scan(symbol, mint, bond, _sig_pre, "sm", 6, "SMART $ SELLING")
            return None
        holder_ok, holder_reason = check_holder_concentration(mint)
        if not holder_ok:
            _log_scan(symbol, mint, bond, _sig_pre, "holder", 4, holder_reason[:18].upper())
            return None
        sig_score = gmgn_signal_score(mint) + dsc_signal_score(mint) + jup_token_signal_score(mint)
        # Trench runner near graduation — vol and price impact are the real gates, not signal score
        market = get_market_data(mint)
        if not market or not market.get("pair_address") or market["price"] <= 0:
            _log_scan(symbol, mint, bond, _sig_pre, "vol", 8, "NOT INDEXED YET")
            return None
        if market.get("vol_m5", 0) < MIN_VOL_5M:
            _log_scan(symbol, mint, bond, _sig_pre, "vol", 8, f"VOL ${market['vol_m5']:.0f}<{MIN_VOL_5M:.0f}")
            return None
        impact = jup_price_impact(mint, trade_size())
        if impact > JUP_IMPACT_MAX_PCT:
            _log_scan(symbol, mint, bond, _sig_pre, "liq", 9, f"IMPACT {impact:.1f}%")
            return None
        return {"action": "trade", "strategy": "trench", "mint": mint, "symbol": symbol,
                "price": market["price"], "bond": bond, "sig_score": sig_score,
                "pump_swap": coin.get("pump_swap", False), "market": market}

    # Dormant Spike
    created_at = coin.get("created_at", 0)
    age_h = (time.time() - created_at / 1000) / 3600 if created_at > 0 else 0
    if age_h >= SPIKE_MIN_AGE_H:
        if int(coin.get("replies", 0) or 0) < MIN_REPLIES:
            return None
        market = get_market_data(mint)
        if not market or not market.get("pair_address") or market["price"] <= 0:
            return None
        if (market["change1h"] >= SPIKE_MIN_1H and market["liq"] >= MIN_LIQ
                and market.get("vol_m5", 0) >= MIN_VOL_5M):
            dev_wallet = coin.get("creator", "") or coin.get("dev", "")
            if _is_bundle_deployer(dev_wallet):
                return None
            rug = run_rugcheck(mint)
            if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                return {"action": "blacklist", "mint": mint}
            if rug and rug.get("is_bundled"):
                _record_bundle_deployer(dev_wallet, symbol)
                if BUNDLE_MODE == "avoid":
                    return None
            if rug and rug.get("score", 0) > MAX_RUG_SCORE:
                return None
            holder_ok, holder_reason = check_holder_concentration(mint)
            if not holder_ok:
                return None
            dev_ok, _ = check_dev_history(dev_wallet)
            if not dev_ok:
                return None
            if gmgn_smart_money_selling(mint):
                return None
            sig_score = gmgn_signal_score(mint) + dsc_signal_score(mint) + jup_token_signal_score(mint)
            if sig_score < MIN_SIGNAL_SCORE:
                return None
            if not is_1m_trending_up(market.get("pair_address", ""), market):
                return None
            impact = jup_price_impact(mint, trade_size())
            if impact > JUP_IMPACT_MAX_PCT:
                return None
            return {"action": "trade", "strategy": "spike", "mint": mint, "symbol": symbol,
                    "price": market["price"], "bond": bond, "sig_score": sig_score,
                    "pump_swap": coin.get("pump_swap", False), "market": market}
    return None

# ── WATCHLIST ────────────────────────────────────────────────────
def _add_to_watchlist(res):
    """Save a qualified trade result that couldn't enter (paused/full) for later retry."""
    mint = res["mint"]
    with _watchlist_lock:
        if mint not in _watchlist:
            _watchlist[mint] = {"res": res, "added_at": time.time()}
            log("info", f"WATCHLIST [{res['strategy'].upper()}] saved for retry", res["symbol"])

def _drain_watchlist():
    """Re-verify watchlisted tokens and enter any that are still valid."""
    now = time.time()
    with _watchlist_lock:
        expired = [m for m, v in _watchlist.items() if now - v["added_at"] > WATCHLIST_TTL_SECS]
        for m in expired:
            _watchlist.pop(m, None)
        entries = list(_watchlist.items())

    for mint, entry in entries:
        if daily_limit_reached():
            break
        with trades_lock:
            if len(open_trades) >= MAX_OPEN:
                break
            if mint in open_trades:
                with _watchlist_lock:
                    _watchlist.pop(mint, None)
                continue
        if mint in blacklisted_mints:
            with _watchlist_lock:
                _watchlist.pop(mint, None)
            continue

        res = entry["res"]
        market = get_market_data(mint)
        if not market or not market.get("pair_address") or market["price"] <= 0:
            with _watchlist_lock:
                _watchlist.pop(mint, None)
            continue
        if market.get("vol_m5", 0) < MIN_VOL_5M:
            with _watchlist_lock:
                _watchlist.pop(mint, None)
            continue
        # Drop if price dumped >5% since we watchlisted it
        price_drift = (market["price"] - res["price"]) / max(res["price"], 1e-12) * 100
        if price_drift < -5:
            log("info", f"WATCHLIST DROP: price {price_drift:.1f}% since save", res["symbol"])
            with _watchlist_lock:
                _watchlist.pop(mint, None)
            continue

        with _watchlist_lock:
            _watchlist.pop(mint, None)
        waited = now - entry["added_at"]
        amt = trade_size()
        log("ok", f"WATCHLIST ENTER [{res['strategy'].upper()}] waited {waited:.0f}s | ${amt:.2f}", res["symbol"])
        enter_trade(mint, res["symbol"], market["price"], amt, res["strategy"],
                    res.get("bond", 0), 0, pump_swap=res.get("pump_swap", False))

# ── SCANNER LOOP ─────────────────────────────────────────────────
def scanner_loop():
    global TUNE_PAUSED_UNTIL
    log("ok", "=" * 55)
    log("ok", "PumpFun Sniper — Bond Runner + Dormant Spike")
    log("ok", f"Bond entry: {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% | TP: +{BOND_TP_PCT}% price")
    log("ok", f"Spike: {SPIKE_MIN_AGE_H}h+ dormant, {SPIKE_MIN_1H}%+ 1h move")
    with capital_lock:
        _sp, _ = _cap_tier(capital)
    log("ok", f"Trade size: ~{_sp*100:.0f}% of capital (min ${MIN_TRADE} max ${MAX_TRADE})")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else 'LIVE'}")
    log("ok", "=" * 55)

    while scan_active:
        try:
            # Pause gate — check before doing any work
            if BOT_PAUSED or _pause_until > time.time():
                time.sleep(5)
                continue

            # Weekly Monday 7am retune — run in background thread, never block scanner
            if TUNE_PAUSED_UNTIL > 0 and time.time() >= TUNE_PAUSED_UNTIL:
                TUNE_PAUSED_UNTIL = _next_monday_7am()  # reset first so loop can't re-fire
                _save_daily_state()
                log("ok", f"Monday 7am retune firing in background. Next: {time.strftime('%a %b %d', time.gmtime(TUNE_PAUSED_UNTIL))}", "TUNE")
                with trades_lock:
                    history_snap = list(completed_trades)
                def _do_weekly_tune(h):
                    auto_tune(h)
                    notify("🧠 Weekly Retune Complete",
                           f"Next retune: Monday {time.strftime('%b %d', time.gmtime(TUNE_PAUSED_UNTIL))} at 07:00 UTC\n"
                           f"Bond: {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% | TP: {BOND_TP_PCT}% | SL: {BOND_SL_PCT}%")
                threading.Thread(target=_do_weekly_tune, args=(history_snap or [],), daemon=True).start()

            _drain_watchlist()
            # Expire _copied_mints regardless of whether copy_trade_loop is running
            _now_ts = time.time()
            with _copy_lock:
                expired_c = [m for m, ts in _copied_mints.items() if _now_ts - ts > 1800]
                for m in expired_c:
                    _copied_mints.pop(m, None)
            # Expire blacklist entries older than 24h (TTL checked at startup from Redis, but
            # not during the lifetime of the process — blacklist would grow unbounded otherwise)
            _bl_expired = [m for m, ts in list(_blacklist_ts.items()) if _now_ts - ts > 86400]
            for m in _bl_expired:
                blacklisted_mints.discard(m)
                _blacklist_ts.pop(m, None)
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
            n_social = n_active = n_bond_range = n_spike_range = 0

            # ── Phase 1: cheap pre-filter (no I/O) ────────────────────
            with trades_lock:
                _open_snap = set(open_trades.keys())
            _black_snap = set(blacklisted_mints)
            candidates = []
            for coin in coins:
                if BOT_PAUSED or _pause_until > time.time():
                    break
                if daily_limit_reached():
                    break
                mint   = coin["mint"]
                symbol = coin["symbol"]
                bond   = coin.get("bond_pct", 0)
                if mint in _black_snap or mint in _open_snap:
                    continue
                _soc = coin.get("socials") or {}
                social_count = sum([
                    bool(coin.get("twitter") or coin.get("twitter_url") or _soc.get("twitter")),
                    bool(coin.get("telegram") or coin.get("telegram_url") or _soc.get("telegram")),
                    bool(coin.get("website") or coin.get("website_url") or _soc.get("website")),
                ])
                if social_count < MIN_SOCIALS:
                    _log_scan(symbol, mint, bond, social_count, "social", 0, f"ONLY {social_count}/{MIN_SOCIALS} SOCIALS")
                    continue
                n_social += 1
                last_trade = coin.get("last_trade", 0)
                if last_trade > 0 and time.time() - last_trade / 1000 > 300:
                    _log_scan(symbol, mint, bond, social_count, "active", 1, "LAST TRADE >5MIN")
                    continue
                n_active += 1

                # ── Bundle ride — handled inline (special path) ────────
                if BUNDLE_MODE == "ride" and 0 < bond < 75:
                    _br_dev = coin.get("creator", "") or coin.get("dev", "")
                    if _is_bundle_deployer(_br_dev):
                        continue
                    rug = run_rugcheck(mint)
                    if rug and rug.get("is_bundled") and not rug.get("has_mint_auth") and not rug.get("has_freeze_auth"):
                        _record_bundle_deployer(_br_dev, symbol)
                        holder_ok, holder_reason = check_holder_concentration(mint)
                        if not holder_ok:
                            log("warn", f"SKIP: {holder_reason}", symbol)
                            continue
                        dev_ok, dev_reason = check_dev_history(_br_dev)
                        if not dev_ok:
                            log("warn", f"SKIP: {dev_reason}", symbol)
                            continue
                        if gmgn_smart_money_selling(mint):
                            log("warn", "SKIP: smart money selling", symbol)
                            continue
                        sig_score = gmgn_signal_score(mint) + dsc_signal_score(mint) + jup_token_signal_score(mint)
                        if sig_score < MIN_SIGNAL_SCORE:
                            continue
                        market = get_market_data(mint)
                        if (market and market["price"] > 0 and market["liq"] >= MIN_LIQ
                                and market.get("vol_m5", 0) >= MIN_VOL_5M):
                            # enter_trade acquires trades_lock internally — don't hold it here
                            if len(open_trades) < MAX_OPEN and mint not in open_trades:
                                amt = trade_size()
                                log("ok", f"BUNDLE RIDE | bond={bond:.1f}% | sig={sig_score}", symbol)
                                enter_trade(mint, symbol, market["price"], amt, "bundle", bond, 0, pump_swap=coin.get("pump_swap", False))
                    continue

                if BOND_ENTRY_MIN <= bond <= BOND_ENTRY_MAX:
                    n_bond_range += 1
                age_h = (time.time() - coin.get("created_at", 0) / 1000) / 3600 if coin.get("created_at", 0) > 0 else 0
                if age_h >= SPIKE_MIN_AGE_H:
                    n_spike_range += 1
                candidates.append((len(candidates), coin))

            # ── Phase 2: parallel I/O evaluation ──────────────────────
            if candidates:
                # Warm Jupiter token cache for all candidates in one batch call
                # so jup_audit_ok() hits cache instead of making per-coin requests
                jup_tokens_search([coin["mint"] for _, coin in candidates])
                workers = min(len(candidates), 12)
                ordered_results = [None] * len(candidates)
                with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="scan") as pool:
                    future_map = {pool.submit(_eval_coin, coin): idx for idx, coin in candidates}
                    for future in as_completed(future_map):
                        idx = future_map[future]
                        try:
                            ordered_results[idx] = future.result()
                        except Exception as e:
                            log("warn", f"Coin eval error: {e}")

                # ── Phase 3: sequential trade decisions ────────────────
                for res in ordered_results:
                    if BOT_PAUSED or _pause_until > time.time():
                        break
                    if not res:
                        continue
                    if res.get("action") == "blacklist":
                        _blacklist_add(res["mint"])
                        continue
                    if res.get("action") != "trade":
                        continue
                    with trades_lock:
                        _already_open = res["mint"] in open_trades
                        _slots_full   = len(open_trades) >= MAX_OPEN
                    if _already_open:
                        continue
                    if daily_limit_reached() or _slots_full:
                        _add_to_watchlist(res)
                        continue
                    strategy = res["strategy"]
                    amt = trade_size()
                    # Re-fetch price — eval happened in a parallel pool, price may be stale
                    fresh = get_market_data(res["mint"])
                    if not fresh or fresh["price"] <= 0:
                        continue
                    m = fresh
                    if strategy == "bond":
                        log("ok", f"BOND RUNNER | bond={res['bond']:.1f}% 5m={m.get('change5m',0):+.1f}% | sig={res['sig_score']}", res["symbol"])
                    elif strategy == "trench":
                        log("ok", f"TRENCH | bond={res['bond']:.1f}% | sig={res['sig_score']}", res["symbol"])
                    elif strategy == "spike":
                        log("ok", f"DORMANT SPIKE | 1h={m.get('change1h',0):+.0f}% 5m={m.get('change5m',0):+.1f}% | sig={res['sig_score']}", res["symbol"])
                    enter_trade(res["mint"], res["symbol"], fresh["price"], amt, strategy,
                                res["bond"], 0, pump_swap=res.get("pump_swap", False))

            log("info",
                f"Filter summary: {len(coins)} coins | "
                f"{n_social} have-social | {n_active} active<5m | "
                f"{n_bond_range} in bond range | {n_spike_range} dormant")
            if n_social == 0:
                log("warn", "0 coins have Twitter or Telegram — market may be slow")
            elif n_active == 0:
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
                with _copy_lock:
                    _gmint_sold_at = _sold_mints.get(gmint, 0)
                if time.time() - _gmint_sold_at < SOLD_COOLDOWN_SECS:
                    continue
                if gc.get("replies", 0) < MIN_REPLIES:
                    log("info", f"MIGRATION SKIP: replies {gc.get('replies',0)}<{MIN_REPLIES}", gc["symbol"])
                    continue
                market = get_market_data(gmint)
                if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                    continue
                rug = run_rugcheck(gmint)
                if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                    _blacklist_add(gmint)
                    continue
                if gmgn_smart_money_selling(gmint):
                    continue
                dev_ok, dev_reason = check_dev_history(gc.get("dev", ""))
                if not dev_ok:
                    log("warn", f"SKIP: {dev_reason}", gc["symbol"])
                    continue
                sig_score = gmgn_signal_score(gmint) + dsc_signal_score(gmint)
                if sig_score < MIN_SIGNAL_SCORE:
                    log("info", f"MIGRATION SKIP: sig={sig_score}<{MIN_SIGNAL_SCORE}", gc["symbol"])
                    continue
                if not is_1m_trending_up(market.get("pair_address", ""), market):
                    log("info", f"MIGRATION SKIP: 1m not trending up", gc["symbol"])
                    continue
                with _copy_lock:
                    _copied_mints[gmint] = time.time()
                amt = trade_size()
                log("ok", f"MIGRATION | {gc['grad_age']}s ago | liq=${market['liq']:.0f} 5m={market.get('change5m',0):+.1f}% | sig={sig_score}", gc["symbol"])
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
                    _sig_sold_at = _sold_mints.get(sig_mint, 0)
                if time.time() - _sig_sold_at < SOLD_COOLDOWN_SECS:
                    continue
                if daily_limit_reached():
                    break
                market = get_market_data(sig_mint)
                if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                    continue
                _gmgn_dev = None
                rug = run_rugcheck(sig_mint)
                if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                    _blacklist_add(sig_mint)
                    continue
                if rug and rug.get("is_bundled"):
                    _gmgn_dev = rug.get("creator") or rug.get("dev") or ""
                    _record_bundle_deployer(_gmgn_dev, sig_mint[:8])
                    if BUNDLE_MODE == "avoid":
                        continue
                if _gmgn_dev and _is_bundle_deployer(_gmgn_dev):
                    log("info", f"GMGN SKIP: serial bundler {_gmgn_dev[:8]}", sig_mint[:8])
                    continue
                if gmgn_smart_money_selling(sig_mint):
                    continue
                if market.get("vol_m5", 0) < MIN_VOL_5M:
                    log("info", f"GMGN SKIP: vol ${market.get('vol_m5',0):.0f}<{MIN_VOL_5M}", sig_mint[:8])
                    continue
                sig_score = gmgn_signal_score(sig_mint) + dsc_signal_score(sig_mint)
                if sig_score < MIN_SIGNAL_SCORE:
                    log("info", f"GMGN SKIP: sig={sig_score}<{MIN_SIGNAL_SCORE}", sig_mint[:8])
                    continue
                if not is_1m_trending_up(market.get("pair_address", ""), market):
                    log("info", f"SIGNAL SKIP: 1m not trending up", sig_mint[:8])
                    continue
                with _copy_lock:
                    _copied_mints[sig_mint] = time.time()
                sig_sym   = sig_mint[:8]
                amt       = trade_size()
                log("ok", f"GMGN SIGNAL | liq=${market['liq']:.0f} 5m={market.get('change5m',0):+.1f}% | sig={sig_score}", sig_sym)
                notify(f"📡 SIGNAL {sig_sym}", f"GMGN signal entry\nLiq: ${market['liq']:.0f}\nSig score: {sig_score}\nAmount: ${amt:.2f}")
                enter_trade(sig_mint, sig_sym, market["price"], amt, "copy", 0, 0)
                n_signal_entered += 1
                time.sleep(0.5)
            if n_signal_entered:
                log("info", f"GMGN signal scan: entered {n_signal_entered} | pool={len(signal_mints)}")

            # ── DexScreener Boost / Ad / Meta Scan ───────────────────
            # Enter tokens where devs are actively spending money on DSC
            # (boosts, ads, trending meta). Stronger signal = dev backing it publicly.
            with _dsc_lock:
                dsc_signal_pool = list(
                    (_dsc_boosted_mints | _dsc_ad_mints | _dsc_meta_mints
                     | _dsc_top_mints | _dsc_takeover_mints) - blacklisted_mints
                )
            n_dsc_entered = 0
            for dsc_mint in dsc_signal_pool:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN:
                        break
                    if dsc_mint in open_trades:
                        continue
                with _copy_lock:
                    if dsc_mint in _copied_mints:
                        continue
                    _dsc_sold_at = _sold_mints.get(dsc_mint, 0)
                if time.time() - _dsc_sold_at < SOLD_COOLDOWN_SECS:
                    continue
                if daily_limit_reached():
                    break
                market = get_market_data(dsc_mint)
                if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                    continue
                _dsc_dev = None
                rug = run_rugcheck(dsc_mint)
                if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                    _blacklist_add(dsc_mint)
                    continue
                if rug and rug.get("is_bundled"):
                    _dsc_dev = rug.get("creator") or rug.get("dev") or ""
                    _record_bundle_deployer(_dsc_dev, dsc_mint[:8])
                    if BUNDLE_MODE == "avoid":
                        continue
                if _dsc_dev and _is_bundle_deployer(_dsc_dev):
                    log("info", f"DSC SKIP: serial bundler {_dsc_dev[:8]}", dsc_mint[:8])
                    continue
                if gmgn_smart_money_selling(dsc_mint):
                    continue
                if market.get("vol_m5", 0) < MIN_VOL_5M:
                    log("info", f"DSC SKIP: vol ${market.get('vol_m5',0):.0f}<{MIN_VOL_5M}", dsc_mint[:8])
                    continue
                sig_score = gmgn_signal_score(dsc_mint) + dsc_signal_score(dsc_mint)
                if sig_score < MIN_SIGNAL_SCORE:
                    log("info", f"DSC SKIP: sig={sig_score}<{MIN_SIGNAL_SCORE}", dsc_mint[:8])
                    continue
                if not is_1m_trending_up(market.get("pair_address", ""), market):
                    continue
                with _copy_lock:
                    _copied_mints[dsc_mint] = time.time()
                dsc_sym = dsc_mint[:8]
                amt = trade_size()
                # Label by which DSC signal triggered
                with _dsc_lock:
                    _why = ("BOOST" if dsc_mint in _dsc_boosted_mints else
                            "ADS"   if dsc_mint in _dsc_ad_mints       else
                            "META"  if dsc_mint in _dsc_meta_mints      else
                            "TOP"   if dsc_mint in _dsc_top_mints       else "TAKEOVER")
                log("ok", f"DSC {_why} | liq=${market['liq']:.0f} 5m={market.get('change5m',0):+.1f}% | sig={sig_score}", dsc_sym)
                notify(f"📊 DSC {_why} {dsc_sym}",
                       f"DexScreener signal entry\nLiq: ${market['liq']:.0f}\nSig: {sig_score}\nAmount: ${amt:.2f}")
                enter_trade(dsc_mint, dsc_sym, market["price"], amt, "copy", 0, 0)
                n_dsc_entered += 1
                time.sleep(0.5)
            if n_dsc_entered:
                log("info", f"DSC boost scan: entered {n_dsc_entered} | pool={len(dsc_signal_pool)}")

            # ── Jupiter Signal Scan ───────────────────────────────────
            # Tokens trending on Jupiter right now — real organic volume,
            # not bots. Most reliable "something is happening" signal.
            with _jup_lock:
                jup_signal_pool = list(
                    (_jup_trending_mints | _jup_organic_mints | _jup_toptraded_mints)
                    - blacklisted_mints
                )
            n_jup_entered = 0
            for jup_mint in jup_signal_pool:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN:
                        break
                    if jup_mint in open_trades:
                        continue
                with _copy_lock:
                    if jup_mint in _copied_mints:
                        continue
                    _jup_sold_at = _sold_mints.get(jup_mint, 0)
                if time.time() - _jup_sold_at < SOLD_COOLDOWN_SECS:
                    continue
                if daily_limit_reached():
                    break
                market = get_market_data(jup_mint)
                if not market or market["price"] <= 0 or market["liq"] < MIN_LIQ:
                    continue
                rug = run_rugcheck(jup_mint)
                if rug and (rug.get("has_mint_auth") or rug.get("has_freeze_auth")):
                    _blacklist_add(jup_mint)
                    continue
                jup_ok, _ = jup_audit_ok(jup_mint)
                if not jup_ok:
                    _blacklist_add(jup_mint)
                    continue
                if rug and rug.get("is_bundled"):
                    _jup_dev = rug.get("creator") or rug.get("dev") or ""
                    _record_bundle_deployer(_jup_dev, jup_mint[:8])
                    if BUNDLE_MODE == "avoid":
                        continue
                    if _is_bundle_deployer(_jup_dev):
                        log("info", f"JUP SKIP: serial bundler {_jup_dev[:8]}", jup_mint[:8])
                        continue
                if gmgn_smart_money_selling(jup_mint):
                    continue
                if market.get("vol_m5", 0) < MIN_VOL_5M:
                    log("info", f"JUP SKIP: vol ${market.get('vol_m5',0):.0f}<{MIN_VOL_5M}", jup_mint[:8])
                    continue
                sig_score = gmgn_signal_score(jup_mint) + dsc_signal_score(jup_mint) + jup_token_signal_score(jup_mint)
                if sig_score < MIN_SIGNAL_SCORE:
                    log("info", f"JUP SKIP: sig={sig_score}<{MIN_SIGNAL_SCORE}", jup_mint[:8])
                    continue
                if not is_1m_trending_up(market.get("pair_address", ""), market):
                    continue
                impact = jup_price_impact(jup_mint, trade_size())
                if impact > JUP_IMPACT_MAX_PCT:
                    continue
                with _copy_lock:
                    _copied_mints[jup_mint] = time.time()
                jup_sym = jup_mint[:8]
                amt = trade_size()
                with _jup_lock:
                    _why = ("TRENDING" if jup_mint in _jup_trending_mints else
                            "ORGANIC"  if jup_mint in _jup_organic_mints   else "TOP_VOL")
                log("ok", f"JUP {_why} | liq=${market['liq']:.0f} 5m={market.get('change5m',0):+.1f}% | sig={sig_score}", jup_sym)
                notify(f"⚡ JUP {_why} {jup_sym}",
                       f"Jupiter signal entry\nLiq: ${market['liq']:.0f}\nSig: {sig_score}\nAmount: ${amt:.2f}")
                enter_trade(jup_mint, jup_sym, market["price"], amt, "copy", 0, 0)
                n_jup_entered += 1
                time.sleep(0.5)
            if n_jup_entered:
                log("info", f"JUP signal scan: entered {n_jup_entered} | pool={len(jup_signal_pool)}")

        except Exception as e:
            log("err", f"Scanner: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ENDPOINTS ───────────────────────────────────────────────


@app.route("/", methods=["GET"])
def home():
    try:
        return _home_inner()
    except Exception as e:
        import traceback
        log("warn", f"/ render error: {traceback.format_exc()[:400]}", "HOME")
        return f"<h1>Home Error</h1><pre>{e}</pre>", 500

def _home_inner():
    from flask import request as _req
    theme = _req.args.get("theme", "classic")
    with capital_lock:
        cap = capital
    with trades_lock:
        open_list = list(open_trades.values())
    with usdc_lock:
        locked = usdc_locked
    wins  = [t for t in completed_trades if t.get("pnl", 0) > 0]
    total = len(completed_trades)
    wr    = round(len(wins) / max(total, 1) * 100, 1)
    pnl   = sum(t.get("pnl", 0) for t in completed_trades)
    mode  = "PAPER" if PAPER_MODE else "LIVE"
    pct, limit = _cap_tier(cap)
    next_m = next((m for m in MILESTONES if m > cap), None)
    next_m_str = f"{next_m:,}" if next_m else "MAX"
    progress_pct = min(round(cap / max(next_m, 1) * 100, 1), 100) if next_m else 100

    cap_points = [{"day": d.get("date","")[-5:], "cap": round(d.get("capital", 0) or 0, 2)} for d in _week_day_logs[-14:]]
    cap_points.append({"day": "Today", "cap": round(cap, 2)})
    cap_json = json.dumps(cap_points)

    recent = list(reversed(completed_trades[-20:]))
    rows = ""
    for t in recent:
        color = "#4ade80" if t.get("pnl", 0) >= 0 else "#f87171"
        icon  = "▲" if t.get("pnl", 0) >= 0 else "▼"
        sign  = "+" if t.get("pnl", 0) >= 0 else ""
        rows += (f'<tr>'
                 f'<td><span class="badge badge-strategy">{t.get("strategy","?").upper()}</span></td>'
                 f'<td class="sym">{t.get("symbol","?")}</td>'
                 f'<td style="color:{color};font-weight:700">{icon} {sign}${t.get("pnl",0):.4f}</td>'
                 f'<td><span class="badge">{t.get("result","?")}</span></td>'
                 f'<td class="muted">{t.get("hold_m",0):.1f}m</td>'
                 f'<td class="muted">{t.get("time","")}</td>'
                 f'</tr>')

    open_rows = ""
    for t in open_list:
        elapsed = round((time.time() - t.get("opened_at", time.time())) / 60, 1)
        _mint = t.get("mint", "")
        open_rows += (f'<tr class="open-trade-row" data-mint="{_mint}" onclick="openPosMini(this)" style="cursor:pointer">'
                      f'<td class="sym">{t.get("symbol","?")}</td>'
                      f'<td><span class="badge badge-strategy">{t.get("strategy","?").upper()}</span></td>'
                      f'<td class="gold">${t.get("amount",0):.2f}</td>'
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
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
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
    min-height:100vh;overflow-x:hidden;max-width:900px;margin:0 auto}}

  /* animated starfield */
  body::before{{content:'';position:fixed;inset:0;
    background:radial-gradient(ellipse at 20% 50%,#1a0a3a22 0%,transparent 60%),
               radial-gradient(ellipse at 80% 20%,#0a1a3a22 0%,transparent 60%);
    pointer-events:none;z-index:0}}

  .wrap{{padding:20px 16px;position:relative}}

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
  nav{{display:flex;flex-wrap:nowrap;overflow-x:auto;scrollbar-width:none;gap:8px;padding:0 12px;margin:20px 0 28px}}
  nav::-webkit-scrollbar{{display:none}}
  nav a{{color:var(--muted);font-size:.78rem;font-weight:500;text-decoration:none;
    padding:6px 14px;border-radius:8px;border:1px solid var(--border);white-space:nowrap;flex-shrink:0;
    background:var(--surface);transition:all .2s;letter-spacing:.03em}}
  nav a:hover{{color:var(--gold);border-color:#f5c54244;background:#f5c54210}}

  /* STAT CARDS */
  .cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:20px}}
  @media(max-width:540px){{.cards{{grid-template-columns:1fr 1fr}}}}
  @media(max-width:360px){{.cards{{grid-template-columns:1fr}}}}
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
  .tbl-wrap{{overflow-x:auto;-webkit-overflow-scrolling:touch}}
  table{{width:100%;border-collapse:collapse;font-size:.78rem;min-width:420px}}
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
  @media(max-width:480px){{.actions{{gap:6px}}.btn{{padding:8px 12px;font-size:.72rem}}}}
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
    <a href="/positions">📍 Positions</a>
    <a href="/trades">📋 All Trades</a>
    <a href="/status">📊 Status</a>
    <a href="/watchlist">👁 Watchlist</a>
    <a href="/learn">🧠 Strategy</a>
    <a href="/chart">📈 Chart</a>
    <a href="/setup">⚙️ Setup</a>
    <a href="https://pump.fun" target="_blank">🚀 Pump.fun</a>
    <a href="https://solscan.io" target="_blank">🔍 Solscan</a>
  </nav>

  <div class="actions">
    <a href="/" class="btn btn-gold">⚡ Refresh Now</a>
    <a href="/live" class="btn btn-ghost">📡 Live Feed</a>
    <a href="/trades" class="btn btn-ghost">💼 All Trades</a>
    <button class="btn btn-ghost" style="border-color:rgba(0,229,255,.4);color:#00e5ff" onclick="adminPost('/admin/tune-now',{{}},null)">🧠 Tune Now</button>
    <button class="btn btn-ghost" onclick="adminPost('/admin/reset-daily',{{}},null)">🔄 Reset Daily</button>
    <button class="btn btn-ghost" onclick="adminPost('/admin/reset-capital',{{}},null)">💰 Reset Capital</button>
    <button class="btn btn-ghost" style="border-color:#00ff88;color:#00ff88;font-weight:700" onclick="(function(){{var v=prompt('Set capital to:','75');if(v&&!isNaN(parseFloat(v)))adminPost('/admin/set-capital',{{amount:parseFloat(v)}},null)}})()">💵 Set Capital</button>
    <button class="btn btn-ghost" style="border-color:#00e5ff;color:#00e5ff;font-weight:700" onclick="adminPost('/admin/sync-capital',{{}},function(r){{alert('Synced: '+r.msg)}})">🔄 Sync Wallet</button>
    <button class="btn btn-ghost" style="border-color:#ff3355;color:#ff3355" onclick="adminPost('/admin/reset-all',{{}},null)">🗑️ Reset All</button>
    <button class="btn btn-ghost" style="border-color:#888;color:#aaa;font-size:0.68rem" onclick="setApiKey()">🔑 Set Key</button>
  </div>

  <div class="cards">
    <div class="card gold-card glow">
      <div class="lbl">Capital</div>
      <div id="h-cap" class="val gold">${cap:.2f}</div>
      <div class="sub">Started at ${STARTING_CAPITAL:.2f}</div>
    </div>
    <div class="card">
      <div class="lbl">Total PnL</div>
      <div id="h-pnl" class="val {'green' if pnl>=0 else 'red'}">{'+' if pnl>=0 else ''}${pnl:.2f}</div>
      <div id="h-pnl-sub" class="sub">{total} trades closed</div>
    </div>
    <div class="card">
      <div class="lbl">Win Rate <span style="font-size:.6rem;letter-spacing:.06em;color:var(--muted);font-weight:500">ALL TIME</span></div>
      <div id="h-wr" class="val {'green' if wr>=50 else 'red'}">{wr}%</div>
      <div id="h-wr-sub" class="sub">{len(wins)}W &nbsp;/&nbsp; {total-len(wins)}L</div>
    </div>
    <div class="card">
      <div class="lbl">Trade Size <span style="font-size:.6rem;letter-spacing:.06em;color:var(--muted);font-weight:500">TODAY</span></div>
      <div id="h-size" class="val blue">${trade_size():.2f}</div>
      <div id="h-size-sub" class="sub">{_daily_wins}W {_daily_losses}L · {_daily_trades}/{limit}</div>
    </div>
    <div class="card">
      <div class="lbl">Open Trades</div>
      <div id="h-open" class="val {'green' if open_list else ''}">{len(open_list)}<span style="font-size:1rem;font-weight:500;color:var(--muted)">/{MAX_OPEN}</span></div>
      <div id="h-open-sub" class="sub">{'🟢 Active' if open_list else '🔍 Scanning...'}</div>
    </div>
    <div class="card">
      <div class="lbl">USDC Locked</div>
      <div id="h-locked" class="val blue">${locked:.2f}</div>
      <div class="sub">Profit secured</div>
    </div>
  </div>

  <div class="section">
    <div class="section-hdr">
      <h2>🏆 Milestone Progress</h2>
      <span style="font-size:.72rem;color:var(--muted)">${cap:.2f} → ${next_m_str}</span>
    </div>
    <div class="prog-labels"><span>${cap:.2f}</span><span>${next_m_str}</span></div>
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
    <div class="tbl-wrap"><table>
      <thead><tr><th>Symbol</th><th>Strategy</th><th>Size</th><th>Bond In</th><th>Held</th></tr></thead>
      <tbody>{open_rows}</tbody>
    </table></div>
  </div>'''}

  <div class="section">
    <div class="section-hdr">
      <h2>📋 Recent Trades</h2>
      <a href="/trades">View All →</a>
    </div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Strategy</th><th>Symbol</th><th>PnL</th><th>Exit</th><th>Hold</th><th>Time</th></tr></thead>
      <tbody>{rows if rows else '<tr><td colspan="6" class="empty">No trades yet — bot is scanning...</td></tr>'}</tbody>
    </table></div>
  </div>

  <footer>
    Live updates every 5s &nbsp;·&nbsp; Retuning weekly Mon 07:00 UTC &nbsp;·&nbsp;
    Built by Boogey &nbsp;·&nbsp;
    <a href="https://github.com/BoogeyBlues/bot.py" target="_blank" style="color:var(--gold);text-decoration:none">GitHub ↗</a>
  </footer>

<!-- ── Position mini-drawer ─────────────────────────── -->
<div id="pm-backdrop" onclick="closeMiniDrawer()" style="display:none;position:fixed;inset:0;background:#000000b8;z-index:50;backdrop-filter:blur(3px)"></div>
<div id="pm-drawer" style="display:none;position:fixed;bottom:0;left:50%;transform:translateX(-50%);width:100%;max-width:430px;background:#10101a;border-radius:20px 20px 0 0;border-top:1px solid #ffffff0d;z-index:51;padding:0 0 env(safe-area-inset-bottom,16px)">
  <div style="display:flex;justify-content:center;padding:10px 0 4px"><div style="width:36px;height:4px;border-radius:2px;background:#ffffff14"></div></div>
  <div style="padding:12px 18px 6px;display:flex;justify-content:space-between;align-items:center">
    <div>
      <div id="pm-sym" style="font-size:1.5rem;font-weight:800;letter-spacing:-.01em;line-height:1">—</div>
      <div style="display:flex;gap:6px;margin-top:4px;align-items:center">
        <span id="pm-badge" style="font-size:.6rem;font-weight:700;letter-spacing:.06em;padding:2px 7px;border-radius:5px;background:#60a5fa18;color:#60a5fa;border:1px solid #60a5fa30;text-transform:uppercase">—</span>
        <span id="pm-held" style="font-size:.6rem;color:#5a5a7a">—</span>
        <a id="pm-tx-link" href="#" target="_blank" style="display:none;font-size:.6rem;color:#00e5ff;text-decoration:none;border:1px solid #00e5ff33;padding:1px 6px;border-radius:4px">Solscan ↗</a>
      </div>
    </div>
    <div style="text-align:right">
      <div id="pm-pnl" style="font-size:1.8rem;font-weight:800;line-height:1;letter-spacing:-.02em">—</div>
      <div style="font-size:.58rem;color:#5a5a7a;margin-top:3px"><span id="pm-price">—</span> &nbsp;·&nbsp; <span id="pm-bond">—</span> bond</div>
    </div>
  </div>
  <canvas id="pm-chart" height="80" style="display:block;width:calc(100% - 28px);margin:6px 14px 8px;border-radius:10px"></canvas>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;padding:0 14px 6px">
    <button onclick="miniAction('close')" style="background:#f8717114;color:#f87171;border:1px solid #f8717130;border-radius:12px;padding:14px 6px 10px;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:4px;font-family:inherit;transition:all .13s">
      <span style="font-size:1.1rem">✕</span><span style="font-size:.8rem;font-weight:700;letter-spacing:.04em">CLOSE</span><span style="font-size:.52rem;color:#f8717180">full exit</span>
    </button>
    <button onclick="miniAction('tp')" style="background:#f5c54214;color:#f5c542;border:1px solid #f5c54230;border-radius:12px;padding:14px 6px 10px;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:4px;font-family:inherit;transition:all .13s">
      <span style="font-size:1.1rem">💰</span><span style="font-size:.8rem;font-weight:700;letter-spacing:.04em">TAKE TP</span><span style="font-size:.52rem;color:#f5c54280">40% out</span>
    </button>
    <button onclick="miniAction('add')" style="background:#60a5fa12;color:#60a5fa;border:1px solid #60a5fa28;border-radius:12px;padding:14px 6px 10px;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:4px;font-family:inherit;transition:all .13s">
      <span style="font-size:1.1rem">＋</span><span style="font-size:.8rem;font-weight:700;letter-spacing:.04em">ADD</span><span style="font-size:.52rem;color:#60a5fa80">compound</span>
    </button>
  </div>
  <div style="padding:0 14px 14px">
    <button onclick="miniAction('force')" style="width:100%;background:#ff335514;color:#ff3355;border:1px solid #ff335530;border-radius:12px;padding:10px 6px;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:8px;font-family:inherit;font-size:.8rem;font-weight:700;letter-spacing:.06em;transition:all .13s">
      ⚡ FORCE SELL — bypass bot, sell wallet balance now
    </button>
  </div>
</div>
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

// ── Position mini-drawer ─────────────────────────────────
const _pm = {{mint:null,timer:null,hist:[],chartObj:null}};

function _fmtHeld(s){{
  if(s<60) return Math.round(s)+'s';
  if(s<3600) return Math.round(s/60)+'m';
  return Math.round(s/3600)+'h '+Math.round((s%3600)/60)+'m';
}}

function openPosMini(row){{
  _pm.mint = row.dataset.mint;
  document.querySelector('meta[http-equiv="refresh"]')?.remove();
  document.getElementById('pm-backdrop').style.display='block';
  const d=document.getElementById('pm-drawer');
  d.style.display='block';
  _pm.hist=[];
  fetchMiniPos();
  clearInterval(_pm.timer);
  _pm.timer=setInterval(fetchMiniPos,3000);
}}

function closeMiniDrawer(){{
  clearInterval(_pm.timer);
  _pm.mint=null;
  document.getElementById('pm-backdrop').style.display='none';
  document.getElementById('pm-drawer').style.display='none';
}}

async function fetchMiniPos(){{
  if(!_pm.mint) return;
  try{{
    const r=await fetch('/positions/api');
    const d=await r.json();
    const pos=(d.positions||[]).find(p=>p.mint===_pm.mint);
    if(!pos){{closeMiniDrawer();return;}}
    const pnl=pos.pnl||0;
    document.getElementById('pm-sym').textContent=pos.symbol||'—';
    document.getElementById('pm-badge').textContent=(pos.strategy||'?').toUpperCase();
    document.getElementById('pm-held').textContent=_fmtHeld(pos.held_s||0);
    const pe=document.getElementById('pm-pnl');
    pe.textContent=(pnl>=0?'+':'')+'\$'+Math.abs(pnl).toFixed(4);
    pe.style.color=pnl>=0?'#4ade80':'#f87171';
    document.getElementById('pm-price').textContent='\$'+(pos.price||pos.entry||0).toFixed(8);
    document.getElementById('pm-bond').textContent=(pos.bond_high||0).toFixed(1)+'%';
    const txLink=document.getElementById('pm-tx-link');
    if(txLink){{
      if(pos.tx){{txLink.href='https://solscan.io/tx/'+pos.tx;txLink.style.display='inline';}}
      else{{txLink.style.display='none';}}
    }}
    _pm.hist.push(pos.price||pos.entry||0);
    if(_pm.hist.length>40)_pm.hist.shift();
    _drawMiniChart(pos.entry||0);
  }}catch(e){{}}
}}

function _drawMiniChart(entry){{
  const cv=document.getElementById('pm-chart');
  const w=cv.offsetWidth||360,h=80;
  cv.width=w;cv.height=h;
  const ctx=cv.getContext('2d');
  ctx.clearRect(0,0,w,h);
  const pts=_pm.hist;
  if(pts.length<2) return;
  const mn=Math.min(...pts,entry),mx=Math.max(...pts,entry);
  const pad=(mx-mn)*0.1||entry*0.001;
  const lo=mn-pad,hi=mx+pad,rng=hi-lo;
  const x=i=>(i/(pts.length-1))*w;
  const y=v=>h-((v-lo)/rng)*h;
  ctx.strokeStyle='#ffffff20';ctx.lineWidth=1;ctx.setLineDash([4,4]);
  ctx.beginPath();ctx.moveTo(0,y(entry));ctx.lineTo(w,y(entry));ctx.stroke();
  ctx.setLineDash([]);
  const last=pts[pts.length-1];
  const col=last>=entry?'#4ade80':'#f87171';
  const grad=ctx.createLinearGradient(0,0,0,h);
  grad.addColorStop(0,last>=entry?'rgba(74,222,128,.18)':'rgba(248,113,113,.18)');
  grad.addColorStop(1,'rgba(0,0,0,0)');
  ctx.beginPath();ctx.moveTo(x(0),y(pts[0]));
  pts.forEach((v,i)=>i>0&&ctx.lineTo(x(i),y(v)));
  ctx.lineTo(w,h);ctx.lineTo(0,h);ctx.closePath();
  ctx.fillStyle=grad;ctx.fill();
  ctx.beginPath();ctx.moveTo(x(0),y(pts[0]));
  pts.forEach((v,i)=>i>0&&ctx.lineTo(x(i),y(v)));
  ctx.strokeStyle=col;ctx.lineWidth=2;ctx.stroke();
}}

async function miniAction(action){{
  const mint=_pm.mint;
  if(!mint) return;
  let url,body=null;
  if(action==='close') url=`/position/${{mint}}/close`;
  else if(action==='tp'){{url=`/position/${{mint}}/tp`;body={{fraction:0.4,label:'TP1'}};}}
  else if(action==='add'){{url=`/position/${{mint}}/compound`;body={{amount:5}};}}
  else if(action==='force') url=`/admin/force-sell/${{mint}}`;
  if(!url) return;
  try{{
    const s=localStorage.getItem('api_secret')||'';
    const hdrs={{'Content-Type':'application/json'}};
    if(s) hdrs['X-API-Key']=s;
    const r=await fetch(url,{{method:'POST',headers:hdrs,body:body?JSON.stringify(body):undefined}});
    const d=await r.json().catch(()=>({{}}));
    if(action==='close'||action==='force'){{
      if(d.ok===false) alert('Sell failed: '+(d.error||'check logs'));
      else closeMiniDrawer();
    }} else fetchMiniPos();
  }}catch(e){{alert('Error: '+e);}}
}}

// ── Live stat polling ────────────────────────────────────
async function pollStats(){{
  try{{
    const d=await(await fetch('/status/api')).json();
    const capEl=document.getElementById('h-cap');
    if(capEl) capEl.textContent='$'+d.capital.toFixed(2);
    const pnlEl=document.getElementById('h-pnl');
    if(pnlEl){{
      const sign=d.total_pnl>=0?'+':'';
      pnlEl.textContent=sign+'$'+Math.abs(d.total_pnl).toFixed(2);
      pnlEl.style.color=d.total_pnl>=0?'#4ade80':'#f87171';
    }}
    const pnlSub=document.getElementById('h-pnl-sub');
    if(pnlSub) pnlSub.textContent=d.total_trades+' trades closed';
    const wrEl=document.getElementById('h-wr');
    if(wrEl){{
      wrEl.textContent=d.win_rate+'%';
      wrEl.style.color=d.win_rate>=50?'#4ade80':'#f87171';
    }}
    const wrSub=document.getElementById('h-wr-sub');
    if(wrSub) wrSub.innerHTML=d.wins+'W &nbsp;/&nbsp; '+d.losses+'L';
    const sizeEl=document.getElementById('h-size');
    if(sizeEl) sizeEl.textContent='$'+d.trade_size.toFixed(2);
    const sizeSubEl=document.getElementById('h-size-sub');
    if(sizeSubEl&&d.today) sizeSubEl.textContent=d.today.wins+'W '+d.today.losses+'L · '+d.today.trades+'/'+(d.daily_limit||9999);
    const openEl=document.getElementById('h-open');
    if(openEl) openEl.innerHTML=d.open_trades+'<span style="font-size:1rem;font-weight:500;color:var(--muted)">'+'/'+d.max_open+'</span>';
    const openSub=document.getElementById('h-open-sub');
    if(openSub) openSub.textContent=d.scanning?(d.open_trades?'🟢 Active':'🔍 Scanning...'):'⏸ Paused';
    const lockedEl=document.getElementById('h-locked');
    if(lockedEl) lockedEl.textContent='$'+d.usdc_locked.toFixed(2);
  }}catch(e){{}}
}}
pollStats();
setInterval(pollStats,5000);
function _getKey(){{return localStorage.getItem('api_secret')||'';}}
function _setKey(k){{localStorage.setItem('api_secret',k||'');}}
function showToast(msg){{
  var t=document.getElementById('admin-toast');
  if(!t){{t=document.createElement('div');t.id='admin-toast';
    t.style.cssText='position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:#00e5ff;color:#050a14;padding:10px 20px;border-radius:8px;font-weight:700;font-size:0.9rem;z-index:9999;transition:opacity 0.4s';
    document.body.appendChild(t);}}
  t.textContent=msg;t.style.opacity='1';
  clearTimeout(t._hide);t._hide=setTimeout(()=>{{t.style.opacity='0';}},3000);
}}
function setApiKey(){{
  var cur=_getKey();
  var k=window.prompt?window.prompt('API secret (blank to clear):',cur):cur;
  if(k===null)return;_setKey(k);showToast(k?'Key saved':'Key cleared');
}}
async function adminPost(url,body,_unused){{
  var s=_getKey();
  var hdrs={{'Content-Type':'application/json'}};
  if(s) hdrs['X-API-Key']=s;
  try{{
    var r=await fetch(url,{{method:'POST',headers:hdrs,body:JSON.stringify(body)}});
    if(r.status===401){{
      showToast('Need API key — tap Set Key first');return;
    }}
    var d=await r.json();
    showToast(d.msg||d.error||'Done');
    pollStats();
  }}catch(e){{showToast('Error: '+e);}}
}}
async function togglePause(){{
  const btn=document.getElementById('pause-btn');
  const isPaused=btn.textContent.includes('RESUME');
  btn.disabled=true;btn.textContent='...';
  try{{await adminPost(isPaused?'/admin/resume':'/admin/pause',{{hours:24}},null);}}
  catch(e){{alert('Error: '+e);}}
  btn.disabled=false;pollStats();
}}
</script>
</body></html>"""
    return html, 200


def _home_punk(cap, open_list, locked, wins, total, wr, pnl, mode,
               pct, limit, next_m, progress_pct, cap_json, rows, open_rows):
    sign      = "+" if pnl >= 0 else ""
    pnl_color = "#39ff14" if pnl >= 0 else "#ff006e"
    wr_color  = "#39ff14" if wr >= 50 else ("#ffee00" if wr >= 35 else "#ff006e")
    mode_color= "#ffee00" if mode == "PAPER" else "#39ff14"
    next_m_str = f"{next_m:,}" if next_m else "MAX"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
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
    <a href="/positions">📍 POS</a>
    <a href="/trades">📋 TRADES</a>
    <a href="/status">📊 STATUS</a>
    <a href="/learn">🧠 STRAT</a>
    <a href="/chart">📈 CHART</a>
    <a href="/setup">⚙️ SETUP</a>
    <a href="https://pump.fun" target="_blank">🚀 PUMP</a>
    <a href="https://solscan.io" target="_blank">🔍 SCAN</a>
  </nav>
  <div class="mode-strip">
    <span class="left">Live updates · Goal ${PROFIT_GOAL:,.0f}</span>
    <span class="mode-pill">{mode}</span>
  </div>
  <div class="hero">
    <div class="hero-lbl">Current Capital</div>
    <div id="pk-cap" class="hero-cap">${cap:.2f}</div>
    <div id="pk-pnl" class="hero-pnl">{sign}${pnl:.2f} total PnL</div>
    <div id="pk-hero-sub" class="hero-sub">Started ${STARTING_CAPITAL:.2f} &nbsp;·&nbsp; {total} trades closed</div>
  </div>
  <div class="grid">
    <div class="stat">
      <div class="lbl">Win Rate <span style="font-size:.6rem;opacity:.5;letter-spacing:.05em">ALL TIME</span></div>
      <div id="pk-wr" class="val" style="color:{wr_color}">{wr}%</div>
      <div id="pk-wr-sub" class="sub">{len(wins)}W / {total-len(wins)}L</div>
    </div>
    <div class="stat">
      <div class="lbl">Trade Size <span style="font-size:.6rem;opacity:.5;letter-spacing:.05em">TODAY</span></div>
      <div id="pk-size" class="val cyan">${trade_size():.2f}</div>
      <div id="pk-size-sub" class="sub">{_daily_wins}W {_daily_losses}L · {_daily_trades}/{_cap_tier(cap)[1]}</div>
    </div>
    <div class="stat">
      <div class="lbl">Open Now</div>
      <div id="pk-open" class="val {'green' if open_list else 'yellow'}">{len(open_list)}<span style="font-size:1.1rem;color:#888">/{MAX_OPEN}</span></div>
      <div id="pk-open-sub" class="sub">{'active' if open_list else 'scanning'}</div>
    </div>
    <div class="stat">
      <div class="lbl">USDC Locked</div>
      <div id="pk-locked" class="val cyan">${locked:.2f}</div>
      <div class="sub">Secured</div>
    </div>
    <div class="stat">
      <div class="lbl">Today</div>
      <div class="val yellow">{_daily_trades}<span style="font-size:1.1rem;color:#888">/{limit}</span></div>
      <div class="sub">{_daily_wins}W {_daily_losses}L</div>
    </div>
    <div class="stat">
      <div class="lbl">Next Target</div>
      <div class="val pink">${next_m_str}</div>
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

async function pollPunk(){{
  try{{
    const d=await(await fetch('/status/api')).json();
    const capEl=document.getElementById('pk-cap');
    if(capEl) capEl.textContent='$'+d.capital.toFixed(2);
    const pnlEl=document.getElementById('pk-pnl');
    if(pnlEl){{
      const sign=d.total_pnl>=0?'+':'';
      pnlEl.textContent=sign+'$'+Math.abs(d.total_pnl).toFixed(2)+' total PnL';
      pnlEl.style.color=d.total_pnl>=0?'#39ff14':'#ff006e';
    }}
    const subEl=document.getElementById('pk-hero-sub');
    if(subEl) subEl.innerHTML='Started ${STARTING_CAPITAL:.2f} &nbsp;·&nbsp; '+d.total_trades+' trades closed';
    const wrEl=document.getElementById('pk-wr');
    if(wrEl){{
      wrEl.textContent=d.win_rate+'%';
      wrEl.style.color=d.win_rate>=50?'#39ff14':'#ff006e';
    }}
    const wrSub=document.getElementById('pk-wr-sub');
    if(wrSub) wrSub.textContent=d.wins+'W / '+d.losses+'L';
    const sizeEl=document.getElementById('pk-size');
    if(sizeEl) sizeEl.textContent='$'+d.trade_size.toFixed(2);
    const pkSizeSub=document.getElementById('pk-size-sub');
    if(pkSizeSub&&d.today) pkSizeSub.textContent=d.today.wins+'W '+d.today.losses+'L · '+d.today.trades+'/'+(d.daily_limit||9999);
    const openEl=document.getElementById('pk-open');
    if(openEl) openEl.innerHTML=d.open_trades+'<span style="font-size:1.1rem;color:#888">/{MAX_OPEN}</span>';
    const openSub=document.getElementById('pk-open-sub');
    if(openSub) openSub.textContent=d.scanning?(d.open_trades?'active':'scanning'):'paused';
    const lockedEl=document.getElementById('pk-locked');
    if(lockedEl) lockedEl.textContent='$'+d.usdc_locked.toFixed(2);
  }}catch(e){{}}
}}
pollPunk();
setInterval(pollPunk,5000);
</script>
</body></html>"""
    return html, 200


@app.route("/status/api", methods=["GET"])
def status_api():
    wins  = [t for t in completed_trades if t.get("pnl", 0) > 0]
    total = len(completed_trades)
    pnl   = sum(t.get("pnl", 0) for t in completed_trades)
    with capital_lock:
        cap = capital
    sol_price = get_sol_price()
    next_tune_str = time.strftime("Mon %b %d · 07:00 UTC", time.gmtime(TUNE_PAUSED_UNTIL)) if TUNE_PAUSED_UNTIL > 0 else "—"
    with usdc_lock:
        locked = usdc_locked
    return jsonify({
        "capital":      round(cap, 2),
        "paper_mode":   PAPER_MODE,
        "open_trades":  len(open_trades),
        "max_open":     MAX_OPEN,
        "total_trades": total,
        "wins":         len(wins),
        "losses":       total - len(wins),
        "win_rate":     round(len(wins) / max(total, 1) * 100, 1),
        "total_pnl":    round(pnl, 4),
        "trade_size":   round(trade_size(), 2),
        "usdc_locked":  round(locked, 2),
        "sol_price":    round(sol_price, 2) if sol_price else None,
        "next_tune_str": next_tune_str,
        "scanning":     scan_active and _pause_until <= time.time(),
        "daily_limit":  _cap_tier(cap)[1],
        "today": {"trades": _daily_trades, "wins": _daily_wins, "losses": _daily_losses,
                  "paused_until": time.strftime("%H:%M", time.localtime(_pause_until)) if _pause_until > time.time() else None},
    })

@app.route("/status", methods=["GET"])
def status():
    wins   = [t for t in completed_trades if t.get("pnl", 0) > 0]
    losses = [t for t in completed_trades if t.get("pnl", 0) <= 0]
    total  = len(completed_trades)
    pnl    = sum(t.get("pnl", 0) for t in completed_trades)
    with capital_lock:
        cap = capital
    wr = round(len(wins) / max(total, 1) * 100, 1)
    pct, limit = _cap_tier(cap)
    next_m = next((m for m in MILESTONES if m > cap), None)
    next_m_str = f"{next_m:,}" if next_m else "MAX"
    progress_pct = min(round(cap / max(next_m, 1) * 100, 1), 100) if next_m else 100
    paused = _pause_until > time.time()
    daily_loss_pct = round((_day_start_cap - cap) / max(_day_start_cap, 1) * 100, 1) if _day_start_cap > 0 else 0

    if BOT_PAUSED:
        health = ("#a78bfa", "🟣", "MAINTENANCE", "Set BOT_PAUSED=false in Railway to resume")
    elif not scan_active:
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
    scan_c = "#a78bfa" if BOT_PAUSED else ("#4ade80" if (not paused and scan_active) else ("#fbbf24" if paused else "#f87171"))
    scan_t = "MAINTENANCE" if BOT_PAUSED else ("SCANNING" if (not paused and scan_active) else ("PAUSED" if paused else "HALTED"))

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
      <button id="pause-btn" onclick="togglePause()" style="margin-top:8px;padding:5px 14px;border-radius:4px;border:1px solid {'#fbbf24' if not paused else '#4ade80'};background:transparent;color:{'#fbbf24' if not paused else '#4ade80'};font-size:0.72rem;font-weight:700;letter-spacing:.06em;cursor:pointer">{'⏸ PAUSE' if not paused else '▶ RESUME'}</button>
      <button id="paper-btn" onclick="togglePaperMode()" style="margin-top:4px;padding:5px 14px;border-radius:4px;border:1px solid {'#f5c542' if PAPER_MODE else '#ff3355'};background:transparent;color:{'#f5c542' if PAPER_MODE else '#ff3355'};font-size:0.72rem;font-weight:700;letter-spacing:.06em;cursor:pointer">{'📄 PAPER' if PAPER_MODE else '🔴 GO PAPER'}</button>
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
    <div class="section-hdr">MILESTONE PROGRESS — next ${next_m_str} ({progress_pct}%)</div>
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
    <div class="row"><span class="row-key">SOL Price</span>
      <span class="row-val" id="sol-price">...</span></div>
    <div class="row"><span class="row-key">Bond Entry Range</span>
      <span class="row-val">{BOND_ENTRY_MIN}% – {BOND_ENTRY_MAX}%</span></div>
    <div class="row"><span class="row-key">Bond Take Profit</span>
      <span class="row-val" style="color:#39ff14">{BOND_TP_PCT}%</span></div>
    <div class="row"><span class="row-key">Stop Loss</span>
      <span class="row-val" style="color:#ff006e">{BOND_SL_PCT}% · Trailing SL at +{TSL_ACTIVATE_PCT}%</span></div>
    <div class="row"><span class="row-key">Retune Schedule</span>
      <span class="row-val">Weekly · Monday 07:00 UTC</span></div>
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

  <footer>{BOT_NAME} · Live updates every 5s</footer>
</div>
<script>
function refreshStatus(){{
  fetch('/status/api').then(r=>r.json()).then(d=>{{
    if(d.sol_price) document.getElementById('sol-price').textContent='$'+d.sol_price.toLocaleString();
    const ntEl=document.getElementById('next-tune');
    if(ntEl && d.next_tune_str) ntEl.textContent=d.next_tune_str;
    const btn=document.getElementById('pause-btn');
    if(btn && d.scanning!==undefined){{
      const paused=!d.scanning;
      btn.textContent=paused?'▶ RESUME':'⏸ PAUSE';
      btn.style.color=paused?'#4ade80':'#fbbf24';
      btn.style.borderColor=paused?'#4ade80':'#fbbf24';
    }}
  }}).catch(()=>{{}});
}}
function _getKey(){{return localStorage.getItem('api_secret')||'';}}
function _setKey(k){{localStorage.setItem('api_secret',k||'');}}
function showToast(msg){{
  var t=document.getElementById('admin-toast');
  if(!t){{t=document.createElement('div');t.id='admin-toast';
    t.style.cssText='position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:#00e5ff;color:#050a14;padding:10px 20px;border-radius:8px;font-weight:700;font-size:0.9rem;z-index:9999;transition:opacity 0.4s';
    document.body.appendChild(t);}}
  t.textContent=msg;t.style.opacity='1';
  clearTimeout(t._hide);t._hide=setTimeout(()=>{{t.style.opacity='0';}},3000);
}}
async function adminPost(url,body,_unused){{
  var s=_getKey();
  var hdrs={{'Content-Type':'application/json'}};
  if(s) hdrs['X-API-Key']=s;
  try{{
    var r=await fetch(url,{{method:'POST',headers:hdrs,body:JSON.stringify(body)}});
    if(r.status===401){{showToast('Need API key — tap Set Key first');return;}}
    var d=await r.json();
    showToast(d.msg||d.error||'Done');
  refreshStatus();
}}
async function togglePause(){{
  const btn=document.getElementById('pause-btn');
  const isPaused=btn.textContent.includes('RESUME');
  btn.disabled=true;
  btn.textContent='...';
  try{{
    const url=isPaused?'/admin/resume':'/admin/pause';
    await adminPost(url,{{hours:24}},null);
  }} catch(e){{ alert('Error: '+e); }}
  btn.disabled=false;
  refreshStatus();
}}
async function togglePaperMode(){{
  const btn=document.getElementById('paper-btn');
  if(!btn) return;
  const isLive=btn.textContent.includes('GO PAPER');
  btn.disabled=true;
  btn.textContent='...';
  var s=_getKey();
  var hdrs={{'Content-Type':'application/json'}};
  if(s) hdrs['X-API-Key']=s;
  try{{
    var r=await fetch('/admin/paper-mode',{{method:'POST',headers:hdrs,body:JSON.stringify({{enabled:isLive}})}});
    var d=await r.json();
    if(d.ok){{
      const p=d.paper_mode;
      btn.textContent=p?'📄 PAPER':'🔴 GO PAPER';
      btn.style.color=p?'#f5c542':'#ff3355';
      btn.style.borderColor=p?'#f5c542':'#ff3355';
      showToast(p?'📄 PAPER MODE ON — no real trades':'🔴 LIVE MODE — real funds active');
    }}
  }}catch(e){{alert('Error: '+e);}}
  btn.disabled=false;
}}
refreshStatus();
setInterval(refreshStatus,5000);
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
    try:
        return _trades_inner()
    except Exception as e:
        import traceback
        log("warn", f"/trades render error: {traceback.format_exc()[:400]}", "TRADES")
        return f"<h1>Trades Error</h1><pre>{e}</pre>", 500

def _trades_inner():
    with trades_lock:
        open_list = list(open_trades.values())
    with capital_lock:
        cap = capital

    wins   = [t for t in completed_trades if t.get("pnl", 0) > 0]
    losses = [t for t in completed_trades if t.get("pnl", 0) <= 0]
    total  = len(completed_trades)
    wr     = round(len(wins) / max(total, 1) * 100, 1)
    total_pnl = round(sum(t.get("pnl", 0) for t in completed_trades), 4)
    avg_win   = round(sum(t.get("pnl", 0) for t in wins)   / max(len(wins),   1), 4)
    avg_loss  = round(sum(t.get("pnl", 0) for t in losses) / max(len(losses), 1), 4)
    best      = max(completed_trades, key=lambda t: t.get("pnl", 0), default=None)
    worst     = min(completed_trades, key=lambda t: t.get("pnl", 0), default=None)

    # Build table rows — newest first
    rows = ""
    for t in reversed(completed_trades):
        won   = t.get("pnl", 0) > 0
        color = "#4ade80" if won else "#f87171"
        badge = f'<span class="badge {"win" if won else "loss"}">{"WIN" if won else "LOSS"}</span>'
        sign  = "+" if t.get("pnl", 0) >= 0 else ""
        rows += f"""<tr class="trade-row" data-id="{t.get('id',0)}" onclick="openModal({t.get('id',0)})">
          <td class="td-num">#{t.get('id',0)}</td>
          <td>{t.get('date','')}<br><span class="muted">{t.get('time','')}</span></td>
          <td class="sym">{t.get('symbol','?')}</td>
          <td><span class="badge strat">{t.get('strategy','?').upper()}</span></td>
          <td class="mono">${t.get('entry',0):.6f}</td>
          <td class="mono">${t.get('exit',0):.6f}</td>
          <td class="mono" style="color:{color}">{sign}{t.get('pnl_pct',0):.1f}%</td>
          <td class="mono" style="color:{color};font-weight:700">{sign}${t.get('pnl',0):.4f}</td>
          <td>{t.get('hold_m',0):.1f}m</td>
          <td><span class="badge exit">{t.get('result','?')}</span></td>
          <td>{badge}</td>
        </tr>"""

    # Open trades rows
    open_rows = ""
    for t in open_list:
        elapsed = round((time.time() - t.get("opened_at", time.time())) / 60, 1)
        entry   = t.get("entry", 0)
        cur_pct = round((t.get("price_high", entry) - entry) / max(entry, 1e-12) * 100, 1)
        open_rows += f"""<tr>
          <td class="sym">{t.get('symbol','?')}</td>
          <td><span class="badge strat">{t.get('strategy','?').upper()}</span></td>
          <td class="mono">${t.get('amount',0):.2f}</td>
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
    <a href="/chart">CHART</a>
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

@app.route("/watchlist/api", methods=["GET"])
def watchlist_api():
    now = time.time()
    with _watchlist_lock:
        items = [
            {
                "mint":        mint,
                "symbol":      entry["res"]["symbol"],
                "strategy":    entry["res"]["strategy"],
                "bond":        round(entry["res"].get("bond", 0), 1),
                "sig_score":   entry["res"].get("sig_score", 0),
                "entry_price": entry["res"]["price"],
                "added_at":    entry["added_at"],
                "wait_secs":   round(now - entry["added_at"]),
                "vol_m5":      entry["res"]["market"].get("vol_m5", 0),
                "change5m":    entry["res"]["market"].get("change5m", 0),
            }
            for mint, entry in _watchlist.items()
        ]
    paused = _pause_until > now
    return jsonify({
        "items":           items,
        "paused":          paused,
        "pause_remaining": round(max(0, _pause_until - now)),
        "open_trades":     len(open_trades),
        "max_open":        MAX_OPEN,
    })

@app.route("/watchlist/<mint>/remove", methods=["POST"])
def watchlist_remove(mint):
    with _watchlist_lock:
        if mint in _watchlist:
            sym = _watchlist[mint]["res"]["symbol"]
            _watchlist.pop(mint)
            log("info", f"Removed {sym} from watchlist via UI")
            return jsonify({"ok": True})
    return jsonify({"ok": False, "msg": "Not found"})

@app.route("/watchlist/<mint>/greenlight", methods=["POST"])
def watchlist_greenlight(mint):
    with _watchlist_lock:
        entry = _watchlist.get(mint)
    if not entry:
        return jsonify({"ok": False, "msg": "Not on watchlist"})
    res = entry["res"]
    market = get_market_data(mint)
    if not market or market["price"] <= 0:
        return jsonify({"ok": False, "msg": "Can't fetch price"})
    amt = trade_size()
    ok = enter_trade(mint, res["symbol"], market["price"], amt, res["strategy"],
                     res.get("bond", 0), 0, pump_swap=res.get("pump_swap", False))
    if ok:
        with _watchlist_lock:
            _watchlist.pop(mint, None)
        return jsonify({"ok": True, "msg": f"Entered {res['symbol']}"})
    return jsonify({"ok": False, "msg": "Entry failed — paused, full, or low capital"})

@app.route("/watchlist/greenlight-all", methods=["POST"])
def watchlist_greenlight_all():
    with _watchlist_lock:
        entries = list(_watchlist.items())
    entered = 0
    for mint, entry in entries:
        res = entry["res"]
        market = get_market_data(mint)
        if not market or market["price"] <= 0:
            continue
        ok = enter_trade(mint, res["symbol"], market["price"], trade_size(), res["strategy"],
                         res.get("bond", 0), 0, pump_swap=res.get("pump_swap", False))
        if ok:
            with _watchlist_lock:
                _watchlist.pop(mint, None)
            entered += 1
    return jsonify({"ok": True, "entered": entered})

@app.route("/watchlist/clear", methods=["POST"])
def watchlist_clear():
    with _watchlist_lock:
        count = len(_watchlist)
        _watchlist.clear()
    return jsonify({"ok": True, "cleared": count})

@app.route("/watchlist", methods=["GET"])
def watchlist_page():
    mode = "PAPER" if PAPER_MODE else "LIVE"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Watchlist — {BOT_NAME}</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}}
:root{{--bg:#050a14;--bg2:#080f1e;--bg3:#0d1628;--cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--yellow:#ffee00;--text:#c8d8f0;--muted:#4a6080}}
html,body{{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;overflow-x:hidden}}
body{{max-width:430px;margin:0 auto}}
nav{{background:rgba(5,10,20,.97);border-bottom:1px solid rgba(0,229,255,.1);display:flex;overflow-x:auto;position:sticky;top:0;z-index:100}}
nav::-webkit-scrollbar{{display:none}}
.nav-tab{{font-family:'Bebas Neue',sans-serif;font-size:13px;letter-spacing:2px;padding:14px 18px;color:var(--muted);white-space:nowrap;border-bottom:2px solid transparent;flex-shrink:0;text-decoration:none}}
.nav-tab.active{{color:var(--cyan);border-bottom-color:var(--cyan)}}
.page{{padding:14px 14px 80px}}
/* STATUS BAR */
.status-bar{{background:var(--bg2);border:1px solid rgba(255,238,0,.2);border-radius:14px;padding:13px 15px;margin-bottom:12px}}
.status-bar.live{{border-color:rgba(0,255,136,.2)}}
.status-top{{display:flex;align-items:center;justify-content:space-between;margin-bottom:9px}}
.status-left{{display:flex;align-items:center;gap:9px}}
.pause-dot{{width:8px;height:8px;border-radius:50%;background:var(--yellow);flex-shrink:0;box-shadow:0 0 8px rgba(255,238,0,.6);animation:pdot 1.2s ease-in-out infinite}}
.pause-dot.live{{background:var(--green);box-shadow:0 0 8px rgba(0,255,136,.6)}}
@keyframes pdot{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.pause-lbl{{font-size:.65rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--yellow)}}
.pause-lbl.live{{color:var(--green)}}
.pause-sub{{font-size:.55rem;color:var(--muted);margin-top:1px}}
.countdown{{font-family:'Bebas Neue',sans-serif;font-size:1.8rem;letter-spacing:2px;color:var(--yellow);line-height:1}}
.countdown.live{{color:var(--green)}}
.meta-pills{{display:flex;gap:6px;flex-wrap:wrap}}
.mpill{{font-size:.58rem;font-weight:600;letter-spacing:.05em;font-family:'JetBrains Mono',monospace;padding:3px 8px;border-radius:20px;background:rgba(0,229,255,.07);border:1px solid rgba(0,229,255,.12);color:var(--cyan)}}
/* ACTIONS */
.act-row{{display:flex;gap:8px;margin-bottom:12px}}
.btn-gl-all{{flex:1;padding:11px;border-radius:12px;border:1px solid rgba(0,255,136,.25);background:rgba(0,255,136,.1);color:var(--green);font-family:'Bebas Neue',sans-serif;font-size:1rem;letter-spacing:2px;cursor:pointer;transition:all .2s}}
.btn-gl-all:hover{{background:rgba(0,255,136,.18);border-color:rgba(0,255,136,.45)}}
.btn-clr{{padding:11px 14px;border-radius:12px;border:1px solid rgba(255,51,85,.15);background:rgba(255,51,85,.07);color:var(--red);font-size:.68rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;cursor:pointer;transition:all .2s}}
.btn-clr:hover{{background:rgba(255,51,85,.15)}}
/* SECTION */
.sec-lbl{{font-size:.58rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:9px;display:flex;align-items:center;gap:8px}}
.sec-lbl::after{{content:'';flex:1;height:1px;background:rgba(255,255,255,.05)}}
/* CARD */
.wl-card{{background:var(--bg2);border:1px solid rgba(255,238,0,.15);border-radius:14px;margin-bottom:10px;overflow:hidden;animation:fadeUp .3s ease both}}
.wl-card.live{{border-color:rgba(0,255,136,.2)}}
@keyframes fadeUp{{from{{opacity:0;transform:translateY(10px)}}to{{opacity:1;transform:translateY(0)}}}}
.wl-card:nth-child(1){{animation-delay:.04s}}.wl-card:nth-child(2){{animation-delay:.08s}}.wl-card:nth-child(3){{animation-delay:.12s}}.wl-card:nth-child(4){{animation-delay:.16s}}.wl-card:nth-child(5){{animation-delay:.2s}}
.stripe{{height:2px;width:100%}}
.s-bond{{background:var(--cyan)}}.s-trench{{background:var(--yellow)}}.s-spike{{background:var(--green)}}.s-copy{{background:#a78bfa}}.s-migrate{{background:#f97316}}
.card-head{{display:flex;align-items:flex-start;justify-content:space-between;padding:11px 13px 5px}}
.card-sym{{font-family:'Bebas Neue',sans-serif;font-size:1.3rem;letter-spacing:2px;color:var(--text);line-height:1}}
.card-badges{{display:flex;gap:4px;flex-wrap:wrap;margin-top:4px}}
.cbadge{{font-size:.55rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;padding:2px 7px;border-radius:20px;font-family:'JetBrains Mono',monospace}}
.cb-bond{{background:rgba(0,229,255,.1);color:var(--cyan);border:1px solid rgba(0,229,255,.18)}}
.cb-trench{{background:rgba(255,238,0,.08);color:var(--yellow);border:1px solid rgba(255,238,0,.18)}}
.cb-spike{{background:rgba(0,255,136,.08);color:var(--green);border:1px solid rgba(0,255,136,.18)}}
.cb-copy{{background:rgba(167,139,250,.1);color:#a78bfa;border:1px solid rgba(167,139,250,.18)}}
.cb-migrate{{background:rgba(249,115,22,.1);color:#f97316;border:1px solid rgba(249,115,22,.18)}}
.cb-sig{{background:rgba(0,229,255,.05);color:var(--cyan);border:1px solid rgba(0,229,255,.1)}}
.cb-wait{{background:rgba(255,238,0,.08);color:var(--yellow);border:1px solid rgba(255,238,0,.15)}}
.cb-ready{{background:rgba(0,255,136,.08);color:var(--green);border:1px solid rgba(0,255,136,.15)}}
.card-x{{width:26px;height:26px;border-radius:7px;border:1px solid rgba(255,51,85,.12);background:rgba(255,51,85,.07);color:var(--red);font-size:.75rem;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;transition:background .15s}}
.card-x:hover{{background:rgba(255,51,85,.2)}}
/* CHART */
.chart-zone{{padding:3px 13px 3px;cursor:pointer;position:relative}}
.chart-hint{{position:absolute;right:18px;top:7px;font-size:.5rem;font-weight:600;letter-spacing:.06em;color:var(--muted);text-transform:uppercase;opacity:.7}}
canvas.mini{{width:100%;height:56px;display:block;border-radius:9px;background:rgba(255,255,255,.02)}}
/* STATS */
.card-stats{{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;padding:7px 13px 10px}}
.st{{display:flex;flex-direction:column;gap:2px}}
.st-v{{font-family:'JetBrains Mono',monospace;font-size:.74rem;font-variant-numeric:tabular-nums;color:var(--text)}}
.st-v.up{{color:var(--green)}}.st-v.dn{{color:var(--red)}}
.st-l{{font-size:.53rem;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}}
/* FOOTER */
.card-foot{{display:flex;gap:7px;padding:0 13px 12px}}
.btn-rm{{padding:8px 13px;border-radius:9px;border:1px solid rgba(255,51,85,.12);background:rgba(255,51,85,.06);color:var(--red);font-size:.64rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;cursor:pointer;transition:all .15s}}
.btn-rm:hover{{background:rgba(255,51,85,.16)}}
.btn-gl{{flex:1;padding:8px;border-radius:9px;font-family:'Bebas Neue',sans-serif;font-size:.9rem;letter-spacing:2px;cursor:pointer;transition:all .15s;border:1px solid rgba(0,255,136,.1);background:rgba(0,255,136,.04);color:rgba(0,255,136,.38)}}
.btn-gl.live{{color:var(--green);background:rgba(0,255,136,.1);border-color:rgba(0,255,136,.25);cursor:pointer}}
.btn-gl.live:hover{{background:rgba(0,255,136,.18);border-color:rgba(0,255,136,.45)}}
/* DRAWER */
.drawer-bd{{position:fixed;inset:0;z-index:200;background:rgba(0,0,0,.6);backdrop-filter:blur(4px);display:none}}
.drawer{{position:fixed;bottom:0;left:50%;transform:translateX(-50%);width:100%;max-width:430px;z-index:201;background:var(--bg2);border-radius:20px 20px 0 0;border:1px solid rgba(0,229,255,.12);border-bottom:none;padding:0 0 44px;display:none;animation:drawerUp .25s ease}}
@keyframes drawerUp{{from{{transform:translate(-50%,100%)}}to{{transform:translate(-50%,0)}}}}
.drawer-handle{{width:36px;height:4px;border-radius:2px;background:var(--muted);margin:12px auto 14px}}
.drawer-hdr{{padding:0 16px 12px;display:flex;align-items:flex-start;justify-content:space-between;border-bottom:1px solid rgba(255,255,255,.04)}}
.d-sym{{font-family:'Bebas Neue',sans-serif;font-size:1.5rem;letter-spacing:2px;color:var(--text)}}
.d-sub{{font-size:.6rem;color:var(--muted);margin-top:2px;font-family:'JetBrains Mono',monospace}}
.d-close{{background:rgba(255,255,255,.06);border:none;border-radius:8px;color:var(--muted);width:30px;height:30px;font-size:1rem;cursor:pointer;display:flex;align-items:center;justify-content:center}}
.d-chart-wrap{{padding:14px 14px 8px}}
canvas.dchart{{width:100%;height:130px;display:block;border-radius:11px;background:rgba(255,255,255,.02)}}
.d-stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;padding:10px 16px 0}}
.ds{{display:flex;flex-direction:column;gap:3px}}
.ds-v{{font-family:'JetBrains Mono',monospace;font-size:.8rem;font-variant-numeric:tabular-nums;color:var(--cyan)}}
.ds-l{{font-size:.54rem;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}}
/* EMPTY */
.empty{{text-align:center;padding:56px 20px;display:flex;flex-direction:column;align-items:center;gap:14px}}
.empty-icon{{font-size:2.2rem;opacity:.25}}
.empty-title{{font-family:'Bebas Neue',sans-serif;font-size:1.3rem;letter-spacing:2px;color:var(--muted)}}
.empty-sub{{font-size:.72rem;color:var(--muted);opacity:.55;max-width:220px;line-height:1.6}}
</style>
</head>
<body>
<nav>
  <a href="/" class="nav-tab">HOME</a>
  <a href="/live" class="nav-tab">LIVE</a>
  <a href="/trades" class="nav-tab">TRADES</a>
  <a href="/status" class="nav-tab">STATUS</a>
  <a href="/watchlist" class="nav-tab active">WATCHLIST</a>
  <a href="/learn" class="nav-tab">STRATEGY</a>
  <a href="/chart" class="nav-tab">CHART</a>
</nav>

<div class="page">

  <div class="status-bar" id="statusBar">
    <div class="status-top">
      <div class="status-left">
        <div class="pause-dot" id="pauseDot"></div>
        <div>
          <div class="pause-lbl" id="pauseLbl">LOSS COOLDOWN</div>
          <div class="pause-sub" id="pauseSub">resumes trading in</div>
        </div>
      </div>
      <div class="countdown" id="countdown">--:--</div>
    </div>
    <div class="meta-pills" id="metaPills">
      <span class="mpill" id="queuedPill">0 QUEUED</span>
      <span class="mpill">SIG ≥{MIN_SIGNAL_SCORE}</span>
      <span class="mpill">VOL ≥${MIN_VOL_5M/1000:.0f}K</span>
    </div>
  </div>

  <div class="act-row">
    <button class="btn-gl-all" id="btnGlAll" onclick="greenlightAll()">GREENLIGHT ALL</button>
    <button class="btn-clr" onclick="clearAll()">Clear All</button>
  </div>

  <div class="sec-lbl">Qualified Coins</div>
  <div id="coinList"></div>

</div>

<!-- DRAWER -->
<div class="drawer-bd" id="drawerBd" onclick="closeDrawer()"></div>
<div class="drawer" id="drawer">
  <div class="drawer-handle"></div>
  <div class="drawer-hdr">
    <div><div class="d-sym" id="dSym">—</div><div class="d-sub" id="dSub">—</div></div>
    <button class="d-close" onclick="closeDrawer()">✕</button>
  </div>
  <div class="d-chart-wrap"><canvas class="dchart" id="dChart"></canvas></div>
  <div class="d-stats">
    <div class="ds"><span class="ds-v" id="dEntry">—</span><span class="ds-l">Entry</span></div>
    <div class="ds"><span class="ds-v" id="dNow">—</span><span class="ds-l">Now</span></div>
    <div class="ds"><span class="ds-v" id="dDrift">—</span><span class="ds-l">Drift</span></div>
    <div class="ds"><span class="ds-v" id="dVol">—</span><span class="ds-l">Vol 5m</span></div>
  </div>
</div>

<script>
let _data={{}};
let _drawerMint=null;
let _priceHists={{}};

function fmt(p){{return p<0.001?'\$'+p.toExponential(3):'\$'+p.toFixed(p<0.01?6:p<1?4:2)}}
function fmtWait(s){{return s<60?s+'s':Math.floor(s/60)+'m '+s%60+'s'}}

function drawChart(canvas,hist,entry,h){{
  const w=canvas.offsetWidth||380;
  canvas.width=w;canvas.height=h;
  const ctx=canvas.getContext('2d');
  ctx.clearRect(0,0,w,h);
  if(hist.length<2)return;
  const mn=Math.min(...hist,entry),mx=Math.max(...hist,entry);
  const pad=(mx-mn)*0.12||entry*0.005;
  const lo=mn-pad,hi=mx+pad,rng=hi-lo||1;
  const xp=i=>(i/(hist.length-1))*w;
  const yp=v=>h-((v-lo)/rng)*h;
  ctx.strokeStyle='rgba(255,255,255,.1)';ctx.lineWidth=1;ctx.setLineDash([3,4]);
  ctx.beginPath();ctx.moveTo(0,yp(entry));ctx.lineTo(w,yp(entry));ctx.stroke();ctx.setLineDash([]);
  const last=hist[hist.length-1];
  const col=last>=entry?'#00ff88':'#ff3355';
  const grad=ctx.createLinearGradient(0,0,0,h);
  grad.addColorStop(0,last>=entry?'rgba(0,255,136,.18)':'rgba(255,51,85,.18)');
  grad.addColorStop(1,'rgba(0,0,0,0)');
  ctx.beginPath();ctx.moveTo(xp(0),yp(hist[0]));
  hist.forEach((v,i)=>i>0&&ctx.lineTo(xp(i),yp(v)));
  ctx.lineTo(w,h);ctx.lineTo(0,h);ctx.closePath();ctx.fillStyle=grad;ctx.fill();
  ctx.beginPath();ctx.moveTo(xp(0),yp(hist[0]));
  hist.forEach((v,i)=>i>0&&ctx.lineTo(xp(i),yp(v)));
  ctx.strokeStyle=col;ctx.lineWidth=h>80?2:1.5;ctx.stroke();
  ctx.beginPath();ctx.arc(w-3,yp(last),3,0,Math.PI*2);ctx.fillStyle=col;ctx.fill();
}}

function renderStatus(){{
  const d=_data;
  const paused=d.paused||false;
  const rem=d.pause_remaining||0;
  const bar=document.getElementById('statusBar');
  const dot=document.getElementById('pauseDot');
  const lbl=document.getElementById('pauseLbl');
  const sub=document.getElementById('pauseSub');
  const cd=document.getElementById('countdown');
  const pill=document.getElementById('queuedPill');
  const btn=document.getElementById('btnGlAll');
  if(pill)pill.textContent=(d.items||[]).length+' QUEUED';
  if(paused&&rem>0){{
    const m=Math.floor(rem/60),s=rem%60;
    cd.textContent=m+':'+(s<10?'0':'')+s;
    cd.className='countdown';dot.className='pause-dot';
    lbl.textContent='LOSS COOLDOWN';lbl.className='pause-lbl';
    sub.textContent='resumes trading in';
    bar.className='status-bar';
    if(btn)btn.textContent='WAITING…';
  }}else{{
    cd.textContent='LIVE';cd.className='countdown live';
    dot.className='pause-dot live';lbl.textContent='TRADING ACTIVE';lbl.className='pause-lbl live';
    sub.textContent='bot is scanning';bar.className='status-bar live';
    if(btn)btn.textContent='GREENLIGHT ALL';
  }}
}}

function stratClass(s){{return{{bond:'cb-bond',trench:'cb-trench',spike:'cb-spike',copy:'cb-copy',migrate:'cb-migrate'}}[s]||'cb-bond'}}
function stripeClass(s){{return{{bond:'s-bond',trench:'s-trench',spike:'s-spike',copy:'s-copy',migrate:'s-migrate'}}[s]||'s-bond'}}

function renderCoins(){{
  const list=document.getElementById('coinList');
  const items=_data.items||[];
  const paused=_data.paused||false;
  if(!items.length){{
    list.innerHTML=`<div class="empty">
      <div class="empty-icon">👁</div>
      <div class="empty-title">WATCHLIST CLEAR</div>
      <div class="empty-sub">Coins that pass all filters during a cooldown or when slots are full will appear here</div>
    </div>`;
    return;
  }}
  list.innerHTML=items.map((c,i)=>{{
    const drift=((_priceHists[c.mint]||[c.entry_price]).slice(-1)[0]-c.entry_price)/c.entry_price*100;
    const driftStr=(drift>=0?'+':'')+drift.toFixed(1)+'%';
    const driftCls=drift>=1?'up':drift<=-2?'dn':'';
    const waitBadge=paused
      ?`<span class="cbadge cb-wait">⏳ ${{fmtWait(c.wait_secs)}}</span>`
      :`<span class="cbadge cb-ready">✓ READY</span>`;
    return `<div class="wl-card ${{paused?'':'live'}}" id="card-${{c.mint}}">
      <div class="stripe ${{stripeClass(c.strategy)}}"></div>
      <div class="card-head">
        <div>
          <div class="card-sym">${{c.symbol}}</div>
          <div class="card-badges">
            <span class="cbadge ${{stratClass(c.strategy)}}">${{c.strategy.toUpperCase()}}</span>
            <span class="cbadge cb-sig">SIG ${{c.sig_score}}</span>
            ${{waitBadge}}
          </div>
        </div>
        <button class="card-x" onclick="removeCoin('${{c.mint}}')">✕</button>
      </div>
      <div class="chart-zone" onclick="openDrawer('${{c.mint}}')">
        <span class="chart-hint">tap to expand</span>
        <canvas class="mini" id="mc-${{c.mint}}" width="400" height="56"></canvas>
      </div>
      <div class="card-stats">
        <div class="st"><span class="st-v">${{fmt(c.entry_price)}}</span><span class="st-l">Entry Price</span></div>
        <div class="st"><span class="st-v ${{driftCls}}">${{driftStr}}</span><span class="st-l">Drift</span></div>
        <div class="st"><span class="st-v">\$${{(c.vol_m5/1000).toFixed(1)}}k</span><span class="st-l">Vol 5m</span></div>
      </div>
      <div class="card-foot">
        <button class="btn-rm" onclick="removeCoin('${{c.mint}}')">Remove</button>
        <button class="btn-gl ${{paused?'':'live'}}" onclick="greenlightOne('${{c.mint}}','${{c.symbol}}')">${{paused?'WAITING…':'GREENLIGHT'}}</button>
      </div>
    </div>`;
  }}).join('');

  items.forEach(c=>{{
    const hist=_priceHists[c.mint]||[c.entry_price];
    requestAnimationFrame(()=>{{
      const cv=document.getElementById('mc-'+c.mint);
      if(cv)drawChart(cv,hist,c.entry_price,56);
    }});
  }});
}}

function openDrawer(mint){{
  const c=(_data.items||[]).find(x=>x.mint===mint);
  if(!c)return;
  _drawerMint=mint;
  const hist=_priceHists[mint]||[c.entry_price];
  const last=hist[hist.length-1];
  const drift=(last-c.entry_price)/c.entry_price*100;
  document.getElementById('dSym').textContent=c.symbol;
  document.getElementById('dSub').textContent=c.strategy.toUpperCase()+' · '+c.mint.slice(0,8)+'…'+c.mint.slice(-4);
  document.getElementById('dEntry').textContent=fmt(c.entry_price);
  document.getElementById('dNow').textContent=fmt(last);
  document.getElementById('dDrift').textContent=(drift>=0?'+':'')+drift.toFixed(2)+'%';
  document.getElementById('dDrift').style.color=drift>=0?'var(--green)':'var(--red)';
  document.getElementById('dVol').textContent='\$'+(c.vol_m5/1000).toFixed(1)+'k';
  document.getElementById('drawerBd').style.display='block';
  document.getElementById('drawer').style.display='block';
  requestAnimationFrame(()=>{{
    const cv=document.getElementById('dChart');
    if(cv)drawChart(cv,hist,c.entry_price,130);
  }});
}}

function closeDrawer(){{
  _drawerMint=null;
  document.getElementById('drawerBd').style.display='none';
  document.getElementById('drawer').style.display='none';
}}

function removeCoin(mint){{
  fetch('/watchlist/'+mint+'/remove',{{method:'POST'}})
    .then(()=>poll()).catch(()=>{{}});
}}

function greenlightOne(mint,sym){{
  if(_data.paused){{alert('Bot is in cooldown — coin will auto-enter when pause lifts.');return;}}
  fetch('/watchlist/'+mint+'/greenlight',{{method:'POST'}})
    .then(r=>r.json()).then(d=>{{
      if(d.ok){{closeDrawer();poll();}}
      else alert(d.msg||'Entry failed');
    }}).catch(()=>{{}});
}}

function greenlightAll(){{
  if(_data.paused){{alert('Bot is in cooldown — coins will auto-enter when pause lifts.');return;}}
  fetch('/watchlist/greenlight-all',{{method:'POST'}})
    .then(r=>r.json()).then(d=>poll()).catch(()=>{{}});
}}

function clearAll(){{
  if(!(_data.items||[]).length)return;
  if(confirm('Remove all '+(_data.items||[]).length+' coins from watchlist?')){{
    fetch('/watchlist/clear',{{method:'POST'}}).then(()=>poll()).catch(()=>{{}});
  }}
}}

function tickHists(){{
  (_data.items||[]).forEach(c=>{{
    const h=_priceHists[c.mint];
    if(!h)return;
    const last=h[h.length-1];
    const next=last*(1+(Math.random()-.49)*0.015);
    h.push(next);
    if(h.length>80)h.shift();
  }});
  (_data.items||[]).forEach(c=>{{
    const cv=document.getElementById('mc-'+c.mint);
    if(cv)drawChart(cv,_priceHists[c.mint]||[c.entry_price],c.entry_price,56);
    if(_drawerMint===c.mint){{
      const dv=document.getElementById('dChart');
      const hist=_priceHists[c.mint]||[c.entry_price];
      const last=hist[hist.length-1];
      const drift=(last-c.entry_price)/c.entry_price*100;
      if(dv)drawChart(dv,hist,c.entry_price,130);
      document.getElementById('dNow').textContent=fmt(last);
      document.getElementById('dDrift').textContent=(drift>=0?'+':'')+drift.toFixed(2)+'%';
      document.getElementById('dDrift').style.color=drift>=0?'var(--green)':'var(--red)';
    }}
  }});
}}

async function poll(){{
  try{{
    const d=await fetch('/watchlist/api').then(r=>r.json());
    // seed price hists for new coins
    (d.items||[]).forEach(c=>{{
      if(!_priceHists[c.mint])_priceHists[c.mint]=[c.entry_price];
    }});
    // remove hists for removed coins
    const mints=new Set((d.items||[]).map(x=>x.mint));
    Object.keys(_priceHists).forEach(m=>{{if(!mints.has(m))delete _priceHists[m];}});
    _data=d;
    renderStatus();renderCoins();
  }}catch(e){{}}
}}

poll();
setInterval(poll,5000);
setInterval(tickHists,3000);
</script>
</body></html>"""
    return html, 200

@app.route("/webhook/helius", methods=["POST"])
def helius_webhook():
    """Receive real-time Helius push events for tracked wallet swaps.
    Helius POSTs an array of enhanced transaction objects the instant a swap lands."""
    if HELIUS_WEBHOOK_AUTH:
        provided = (request.headers.get("Authorization", "") or
                    request.headers.get("authHeader", ""))
        if provided != HELIUS_WEBHOOK_AUTH:
            return jsonify({"error": "Unauthorized"}), 401
    try:
        payload = request.get_json(silent=True) or []
        if not isinstance(payload, list):
            payload = [payload]

        # Build addr→wallet_info lookup from all tracked sources
        with _copy_lock:
            addr_map = {w["address"].lower(): w for w in _copy_wallets}
        for addr in TRACKED_WALLETS:
            addr_map.setdefault(addr.lower(), {"address": addr, "winrate": 100.0})
        for addr in PINNED_WALLETS:
            addr_map.setdefault(addr.lower(), {"address": addr, "winrate": 100.0, "pinned": True})
        for addr in FAST_WALLETS:
            addr_map.setdefault(addr.lower(), {"address": addr, "winrate": 100.0, "fast": True})

        enqueued = 0
        for tx in payload:
            # Identify which tracked wallet triggered this event (feePayer is the swapper)
            fee_payer = tx.get("feePayer", "").lower()
            wallet_info = addr_map.get(fee_payer)
            if not wallet_info:
                # Fallback: check token recipient fields
                for t in tx.get("tokenTransfers", []):
                    acct = t.get("toUserAccount", "").lower()
                    if acct in addr_map:
                        wallet_info = addr_map[acct]
                        fee_payer = acct
                        break
            if not wallet_info:
                continue
            act = _parse_helius_enhanced_tx(tx, fee_payer)
            if not act:
                continue
            _webhook_queue.append((wallet_info, act))
            enqueued += 1

        if enqueued:
            log("info", f"Helius webhook: {enqueued} buy event(s) queued", "WH")
        return jsonify({"ok": True, "enqueued": enqueued})
    except Exception as e:
        log("warn", f"Helius webhook handler: {e}", "WH")
        return jsonify({"ok": False}), 200  # always 200 so Helius doesn't retry

@app.route("/admin/tune-now", methods=["POST"])
def admin_tune_now():
    denied = _auth_required()
    if denied: return denied
    global TUNE_PAUSED_UNTIL
    with trades_lock:
        history_snap = list(completed_trades)
    auto_tune(history_snap or [])
    TUNE_PAUSED_UNTIL = _next_monday_7am()
    _save_daily_state()
    next_str = time.strftime("%a %b %d at 07:00 UTC", time.gmtime(TUNE_PAUSED_UNTIL))
    log("ok", f"Manual retune complete — next auto-retune {next_str}", "TUNE")
    return jsonify({"ok": True, "msg": f"Tuned. Next auto-retune: {next_str}. "
                                       f"Bond {BOND_ENTRY_MIN}-{BOND_ENTRY_MAX}% | TP {BOND_TP_PCT}% | SL {BOND_SL_PCT}%"})

@app.route("/admin/reset-daily", methods=["POST"])
def admin_reset_daily():
    denied = _auth_required()
    if denied: return denied
    global _daily_trades, _daily_wins, _daily_losses, _pause_until, _daily_cap_notified, _day_start_cap
    with capital_lock:
        cap_now = capital
    with _daily_lock:
        _daily_trades        = 0
        _daily_wins          = 0
        _daily_losses        = 0
        _pause_until         = 0.0
        _daily_cap_notified  = False
        _day_start_cap       = cap_now  # reset loss guard baseline to current capital
    with trades_lock:
        completed_trades.clear()
    _redis_cmd("DEL", "bot_trades")
    redis_save("bot_trades", [])
    _save_daily_state()
    log("ok", f"Daily state reset via /admin/reset-daily — loss guard baseline reset to ${cap_now:.2f}")
    return jsonify({"ok": True, "msg": "Daily counters reset — bot will resume trading"})

@app.route("/admin/reset-capital", methods=["POST"])
def admin_reset_capital():
    denied = _auth_required()
    if denied: return denied
    global capital, usdc_locked
    with capital_lock:
        capital = STARTING_CAPITAL
    with trades_lock:
        completed_trades.clear()
    with usdc_lock:
        usdc_locked = 0.0
    _redis_cmd("DEL", "bot_trades")
    redis_save("bot_trades", [])
    _save_daily_state()
    log("ok", f"Capital reset to ${STARTING_CAPITAL:.2f} via /admin/reset-capital")
    return jsonify({"ok": True, "msg": f"Capital reset to ${STARTING_CAPITAL:.2f} and win rate cleared"})

@app.route("/set-capital/<float:amount>")
def set_capital_get(amount):
    """Manually set capital to a specific value. Clears ghost open positions. Keeps trade history."""
    global capital, _day_start_cap, _daily_cap_notified
    if amount <= 0 or amount > 100000:
        return "Invalid amount", 400
    cleared = []
    with trades_lock:
        for mint, t in list(open_trades.items()):
            cleared.append(t["symbol"])
            del open_trades[mint]
        redis_save("bot_open_trades", [])
    with capital_lock:
        capital = float(amount)
    with _daily_lock:
        _day_start_cap      = float(amount)
        _daily_cap_notified = False
    with usdc_lock:
        global usdc_locked
        usdc_locked = 0.0
    _save_daily_state()
    ghost_msg = f" Cleared {len(cleared)} ghost(s): {', '.join(cleared)}." if cleared else ""
    log("ok", f"Capital manually set to ${amount:.2f}.{ghost_msg}")
    return f'<meta http-equiv="refresh" content="3;url=/">Capital set to ${amount:.2f}.{ghost_msg} Redirecting...'

@app.route("/admin/sync-capital", methods=["POST"])
def admin_sync_capital():
    """Read SOL + USDC balance from wallet and sync bot capital."""
    denied = _auth_required()
    if denied: return denied
    global capital, _day_start_cap, _daily_cap_notified
    if not WALLET or not _SOLANA_AVAILABLE:
        return jsonify({"error": "No wallet configured"}), 400
    sol_price = get_sol_price() or 0
    if not sol_price:
        return jsonify({"error": "Cannot fetch SOL price"}), 500
    USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    try:
        for rpc_url in _rpc_endpoints():
            try:
                client   = Client(rpc_url)
                wallet_pk = Pubkey.from_string(WALLET)
                sol_bal  = client.get_balance(wallet_pk).value / 1e9
                sol_usd  = sol_bal * sol_price
                # Read USDC balance using direct JSON-RPC with jsonParsed encoding
                # (SDK returns raw binary; hasattr(data, "parsed") is always False)
                usdc_usd = 0.0
                try:
                    usdc_resp = _session.post(rpc_url, json={
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getTokenAccountsByOwner",
                        "params": [WALLET, {"mint": USDC_MINT}, {"encoding": "jsonParsed"}]
                    }, timeout=8)
                    if usdc_resp.status_code == 200:
                        for acct in usdc_resp.json().get("result", {}).get("value", []):
                            ui = (acct.get("account", {}).get("data", {})
                                  .get("parsed", {}).get("info", {})
                                  .get("tokenAmount", {}).get("uiAmount", 0) or 0)
                            usdc_usd += float(ui)
                except Exception:
                    pass
                usd = round(sol_usd + usdc_usd, 2)
                old = capital
                with capital_lock:
                    capital = usd
                with _daily_lock:
                    _day_start_cap      = usd
                    _daily_cap_notified = False
                _save_daily_state()
                msg = (f"Synced to ${usd:.2f} "
                       f"({sol_bal:.4f} SOL=${sol_usd:.2f} + USDC=${usdc_usd:.2f} @ ${sol_price:.0f}/SOL)")
                log("ok", f"Capital synced: was ${old:.2f} → ${usd:.2f} | wallet={WALLET[:8]}...")
                return jsonify({"ok": True, "sol": sol_bal, "usdc": usdc_usd,
                                "usd": usd, "sol_price": sol_price, "wallet": WALLET, "msg": msg})
            except Exception:
                continue
        return jsonify({"error": "RPC unreachable"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/admin/set-capital", methods=["POST"])
def admin_set_capital():
    denied = _auth_required()
    if denied: return denied
    global capital, usdc_locked, _daily_trades, _daily_wins, _daily_losses
    body = request.json or {}
    try:
        amount = float(body.get("amount", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "invalid amount"}), 400
    if amount <= 0 or amount > 100000:
        return jsonify({"error": "amount out of range"}), 400

    # Clear ghost open positions
    cleared = []
    with trades_lock:
        for mint, t in list(open_trades.items()):
            cleared.append(t["symbol"])
            del open_trades[mint]
        redis_save("bot_open_trades", [])

    # Set capital and reset loss guard baseline
    global _day_start_cap, _daily_cap_notified
    with capital_lock:
        capital = float(amount)
    with _daily_lock:
        _day_start_cap      = float(amount)
        _daily_cap_notified = False
    with usdc_lock:
        usdc_locked = 0.0

    # Remove ghost completed trades if target_pnl provided
    removed_trade = None
    target_pnl = body.get("target_pnl")
    if target_pnl is not None:
        try:
            target_pnl = float(target_pnl)
            current_sum = sum(t.get("pnl", 0) for t in completed_trades)
            excess = current_sum - target_pnl
            if abs(excess) > 0.01 and completed_trades:
                # Find and remove the trade closest to the excess (the ghost)
                ghost = min(completed_trades, key=lambda t: abs(t.get("pnl", 0) - excess))
                completed_trades.remove(ghost)
                removed_trade = ghost.get("symbol", "?")
                # Fix daily counters
                with _daily_lock:
                    if ghost.get("pnl", 0) > 0:
                        _daily_wins   = max(0, _daily_wins - 1)
                    else:
                        _daily_losses = max(0, _daily_losses - 1)
                    _daily_trades = max(0, _daily_trades - 1)
                redis_save("bot_trades", list(completed_trades))
        except Exception as e:
            log("warn", f"target_pnl cleanup error: {e}")

    _save_daily_state()
    ghost_msg = f" Cleared open ghost(s): {', '.join(cleared)}." if cleared else ""
    pnl_msg   = f" Removed ghost trade ({removed_trade})." if removed_trade else ""
    log("ok", f"Capital set to ${amount:.2f} via admin.{ghost_msg}{pnl_msg}")
    return jsonify({"ok": True, "msg": f"Capital ${amount:.2f}.{ghost_msg}{pnl_msg} Refresh to confirm."})

@app.route("/clear-usdc")
def clear_usdc_get():
    global usdc_locked
    with usdc_lock:
        usdc_locked = 0.0
    _save_daily_state()
    return '<meta http-equiv="refresh" content="2;url=/">USDC locked cleared to $0. Redirecting...'

@app.route("/clear-ghosts")
def clear_ghosts_get():
    """Remove ghost open positions (bot recorded them but tx failed on-chain). Refunds capital."""
    global capital
    cleared = []
    with trades_lock:
        for mint, t in list(open_trades.items()):
            with capital_lock:
                capital += t["amount"]
            cleared.append(f"{t['symbol']} (${t['amount']:.2f})")
            del open_trades[mint]
        redis_save("bot_open_trades", list(open_trades.values()))
    _save_daily_state()
    msg = f"Cleared {len(cleared)} ghost(s): {', '.join(cleared)}. Capital refunded." if cleared else "No open positions to clear."
    return f'<meta http-equiv="refresh" content="3;url=/">{msg} Redirecting...'

@app.route("/admin/reset-all", methods=["POST"])
def admin_reset_all():
    denied = _auth_required()
    if denied: return denied
    global capital, usdc_locked
    global _daily_trades, _daily_wins, _daily_losses, _daily_cap_notified
    global _day_start_cap

    with capital_lock:
        capital = STARTING_CAPITAL
    with trades_lock:
        open_trades.clear()
        completed_trades.clear()
    with usdc_lock:
        usdc_locked = 0.0
    with _daily_lock:
        _daily_trades       = 0
        _daily_wins         = 0
        _daily_losses       = 0
        _daily_cap_notified = False
        _day_start_cap      = STARTING_CAPITAL

    _save_daily_state()
    redis_save("bot_trades", [])
    redis_save("bot_open_trades", [])

    log("ok", f"FULL RESET — capital=${STARTING_CAPITAL:.2f}, all history wiped")
    return jsonify({"ok": True, "msg": f"Full reset — capital restored to ${STARTING_CAPITAL:.2f}. Reload the page."})

def _persist_pause(until_ts: float):
    """Write pause timestamp to Redis so it survives restarts and daily resets."""
    global _pause_until
    _pause_until = until_ts
    redis_save("bot_pause_until", until_ts)
    _save_daily_state()

def _load_pause():
    """Restore pause state from Redis on startup."""
    global _pause_until
    val = redis_load("bot_pause_until")
    if val is not None:
        try:
            _pause_until = float(val)
        except (TypeError, ValueError):
            _pause_until = 0.0

@app.route("/admin/pause", methods=["POST"])
def admin_pause():
    denied = _auth_required()
    if denied: return denied
    hours = 24.0
    if request.is_json and request.json:
        hours = float(request.json.get("hours", 24))
    _persist_pause(time.time() + hours * 3600)
    until_str = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(_pause_until))
    log("warn", f"Scanner PAUSED for {hours:.0f}h (until {until_str}) via admin")
    return jsonify({"ok": True, "msg": f"Scanner paused for {hours:.0f}h until {until_str}"})

@app.route("/admin/resume", methods=["POST"])
def admin_resume():
    denied = _auth_required()
    if denied: return denied
    _persist_pause(0.0)
    log("ok", "Scanner RESUMED via admin endpoint")
    return jsonify({"ok": True, "msg": "Scanner resumed"})

@app.route("/admin/paper-mode", methods=["POST"])
def admin_set_paper_mode():
    global PAPER_MODE
    denied = _auth_required()
    if denied: return denied
    data = request.get_json(silent=True) or {}
    PAPER_MODE = bool(data.get("enabled", True))
    state = "PAPER" if PAPER_MODE else "LIVE"
    log("warn" if not PAPER_MODE else "ok", f"Mode switched to {state} via admin", "ADMIN")
    return jsonify({"ok": True, "paper_mode": PAPER_MODE, "mode": state})

@app.route("/admin/force-sell/<mint>", methods=["POST"])
def admin_force_sell(mint):
    """Emergency: sell all tokens for a given mint immediately, regardless of open_trades state."""
    global capital
    denied = _auth_required()
    if denied: return denied
    # Get actual wallet balance as source of truth
    bal = _check_token_balance(mint)
    symbol = "UNKNOWN"
    with trades_lock:
        if mint in open_trades:
            symbol = open_trades[mint].get("symbol", symbol)
    if bal <= 0 and mint not in open_trades:
        return jsonify({"ok": False, "error": "No tokens found in wallet and no open position"})
    sell_tokens = bal if bal > 0 else open_trades.get(mint, {}).get("tokens", 0)
    if sell_tokens <= 0:
        return jsonify({"ok": False, "error": "Token balance is zero"})
    log("warn", f"FORCE SELL {sell_tokens:.0f} tokens via admin", symbol)
    # Close open position if tracked
    trade = None
    with trades_lock:
        if mint in open_trades:
            open_trades[mint]["_exiting"] = True
            trade = open_trades.pop(mint)
    sig = execute_sell(sell_tokens, mint, symbol,
                       pump_swap=(trade.get("pump_swap", False) if trade else False),
                       raydium=(trade.get("raydium", False) if trade else False))
    if sig:
        if trade:
            price = trade.get("entry", 0)
            amount = trade.get("amount", 0)
            partial = trade.get("partial_proceeds", 0.0)
            clamped = max(0.0, amount - partial)
            with capital_lock:
                capital += clamped
            log("ok", f"Force sell succeeded: {sig[:12]}... Capital +${clamped:.2f}", symbol)
        return jsonify({"ok": True, "sig": sig, "tokens_sold": sell_tokens, "symbol": symbol})
    else:
        if trade:
            with trades_lock:
                trade.pop("_exiting", None)
                open_trades[mint] = trade
        return jsonify({"ok": False, "error": "Sell transaction failed — check logs. Position restored.", "symbol": symbol})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-100:]})

@app.route("/debug")
def debug_view():
    with capital_lock:
        cap = capital
    with _scan_log_lock:
        recent_scans = list(scan_log[:30])
    errs = [e for e in trade_log[-200:] if e.get("tag") in ("err","warn")][-20:]
    result_counts = {}
    for s in recent_scans:
        r = s.get("result","?")
        result_counts[r] = result_counts.get(r, 0) + 1

    rows = ""
    for s in recent_scans:
        color = {"trade":"#00ff88","pass":"#00e5ff"}.get(s.get("result",""), "#ff3355")
        rows += (f"<tr>"
                 f"<td>{s.get('sym','?')}</td>"
                 f"<td>{s.get('bond',0):.1f}%</td>"
                 f"<td>{s.get('fi',0)}</td>"
                 f"<td style='color:{color}'>{s.get('result','?').upper()}</td>"
                 f"<td style='color:#aaa'>{s.get('msg','')}</td>"
                 f"<td style='color:#555;font-size:.7rem'>{s.get('ts','')}</td>"
                 f"</tr>")

    err_rows = ""
    for e in reversed(errs):
        err_rows += f"<tr><td style='color:#888'>{e.get('time','')}</td><td style='color:#ff3355'>{e.get('symbol','')}</td><td>{e.get('msg','')}</td></tr>"

    summary = " | ".join(f"{k.upper()}: {v}" for k,v in sorted(result_counts.items(), key=lambda x:-x[1]))
    return f"""<!doctype html><html><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Debug — Scanner</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#050a14;color:#c8d8f0;font-family:monospace;font-size:13px;padding:16px}}
h2{{color:#00e5ff;margin-bottom:4px}}
.sub{{color:#555;font-size:.75rem;margin-bottom:16px}}
table{{width:100%;border-collapse:collapse;margin-bottom:24px}}
th{{color:#555;text-align:left;padding:4px 8px;border-bottom:1px solid #1a2340;font-size:.7rem;text-transform:uppercase}}
td{{padding:5px 8px;border-bottom:1px solid #0d1525;font-size:.8rem}}
.summary{{background:#0d1525;padding:10px 14px;border-radius:6px;color:#00e5ff;margin-bottom:20px;font-size:.8rem}}
a{{color:#00e5ff;text-decoration:none}}
</style></head><body>
<h2>Scanner Debug</h2>
<div class='sub'>Capital: ${cap:.2f} | Paper: {PAPER_MODE} | Scanning: {scan_active}</div>
<div class='summary'>{summary or "No scans yet — scanner may not be running"}</div>
<h2 style='color:#aaa;font-size:.85rem;margin-bottom:8px'>Last 30 Coin Evaluations</h2>
<table><thead><tr><th>Symbol</th><th>Bond</th><th>Filter#</th><th>Result</th><th>Reason</th><th>Time</th></tr></thead>
<tbody>{rows or "<tr><td colspan=6 style='color:#555;padding:16px'>No evaluations yet</td></tr>"}</tbody></table>
<h2 style='color:#ff3355;font-size:.85rem;margin-bottom:8px'>Recent Errors / Warnings</h2>
<table><thead><tr><th>Time</th><th>Symbol</th><th>Message</th></tr></thead>
<tbody>{err_rows or "<tr><td colspan=3 style='color:#555;padding:16px'>None</td></tr>"}</tbody></table>
<a href='/'>← Dashboard</a> | <a href='/debug'>Refresh</a>
</body></html>"""

@app.route("/wallet-check")
def wallet_check():
    lines = [
        "<pre style='font-family:monospace;font-size:13px;line-height:1.8;padding:20px;background:#050a14;color:#c8d8f0;min-height:100vh'>",
        f"<b style='color:#00e5ff'>WALLET DIAGNOSTIC</b>",
        "",
        f"PAPER_MODE  : {'YES (simulated — no real trades)' if PAPER_MODE else '<b style=color:#00ff88>NO — LIVE MODE</b>'}",
        f"WALLET      : {'<b style=color:#00ff88>' + WALLET[:8] + '...</b> (set)' if WALLET else '<b style=color:#ff3355>NOT SET</b> — add WALLET env var'}",
        f"PRIVATE KEY : {'SET (' + str(len(WALLET_PRIVATE_KEY)) + ' chars)' if WALLET_PRIVATE_KEY else '<b style=color:#ff3355>NOT SET</b> — add WALLET_PRIVATE_KEY env var'}",
    ]
    if WALLET_PRIVATE_KEY and not PAPER_MODE and _SOLANA_AVAILABLE:
        try:
            kp = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
            lines.append(f"KEY FORMAT  : <b style='color:#00ff88'>VALID</b> (derived pubkey={str(kp.pubkey())[:16]}...)")
        except Exception as ke:
            lines.append(f"KEY FORMAT  : <b style='color:#ff3355'>INVALID — {ke}</b>")
            lines.append(f"             → Must be Phantom base58 (~88 chars), NOT a JSON array")
    if WALLET and _SOLANA_AVAILABLE:
        try:
            _c = Client(SOL_RPC)
            br = _c.get_balance(Pubkey.from_string(WALLET))
            sol_bal = br.value / 1e9
            color = "#00ff88" if sol_bal >= 0.01 else "#ff3355"
            lines.append(f"SOL BALANCE : <b style='color:{color}'>{sol_bal:.6f} SOL</b> {'✓' if sol_bal >= 0.01 else '⚠ LOW — need ≥0.01 SOL for gas'}")
        except Exception as be:
            lines.append(f"SOL BALANCE : CHECK FAILED ({be})")
    lines.append("")
    lines.append(f"REDIS       : {'connected' if REDIS_URL else 'NOT SET (trades lost on restart)'}")
    lines.append(f"SOLANA LIB  : {'available' if _SOLANA_AVAILABLE else 'NOT INSTALLED'}")
    lines.append("")
    lines.append("<a style='color:#00e5ff' href='/'>← Back to dashboard</a>")
    lines.append("</pre>")
    return "\n".join(lines)

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
    recent_errors = [e for e in trade_log[-80:] if e.get("tag") in ("err", "warn")][-8:]
    return jsonify({
        "ts":       round(time.time()),
        "capital":  round(cap, 2),
        "open":     open_now,
        "closed":   recent_closed,
        "scanning": scan_active,
        "paused":   _pause_until > time.time(),
        "today":    {"trades": _daily_trades, "wins": _daily_wins, "losses": _daily_losses},
        "scan_log": sl,
        "errors":   recent_errors,
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
.lv-drop{overflow:hidden;max-height:0;transition:max-height .32s ease;background:var(--bg3);margin:0 14px;border-radius:0 0 12px 12px;border:1px solid rgba(0,229,255,.18);border-top:none}
.lv-drop-inner{padding:10px 13px 12px;display:flex;flex-direction:column;gap:8px}
.lvd-row{display:flex;justify-content:space-between;align-items:center}
.lvd-pnl{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:800;letter-spacing:-.01em;line-height:1}
.lvd-meta{font-family:'JetBrains Mono',monospace;font-size:.53rem;color:#4a6080;letter-spacing:.04em;margin-top:2px}
.lvd-act-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px}
.lvd-btn{font-family:'JetBrains Mono',monospace;font-size:.65rem;font-weight:700;padding:9px 4px 7px;border-radius:9px;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:3px;letter-spacing:.05em;-webkit-tap-highlight-color:transparent;transition:opacity .1s}
.lvd-btn .ico{font-size:.9rem}
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
  <a href="/watchlist" class="nav-tab">WATCHLIST</a>
  <a href="/learn" class="nav-tab">STRATEGY</a>
  <a href="/chart" class="nav-tab">CHART</a>
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
<div class="lv-drop" id="lv-drop">
  <div class="lv-drop-inner">
    <div class="lvd-row">
      <div>
        <div class="lvd-pnl" id="lvd-pnl">—</div>
        <div class="lvd-meta" id="lvd-price">price —</div>
      </div>
      <div style="text-align:right">
        <div class="lvd-meta" id="lvd-bond">bond —%</div>
        <div class="lvd-meta" id="lvd-held">held —</div>
      </div>
    </div>
    <canvas id="lvd-cv" height="88" style="display:block;width:100%;border-radius:8px;background:rgba(0,0,0,.3)"></canvas>
    <div class="lvd-act-grid">
      <button class="lvd-btn" id="lvd-close" style="background:rgba(255,51,85,.1);color:#ff3355;border:1px solid rgba(255,51,85,.25)"><span class="ico">&#x2715;</span>CLOSE</button>
      <button class="lvd-btn" id="lvd-tp" style="background:rgba(255,238,0,.08);color:#ffee00;border:1px solid rgba(255,238,0,.22)"><span class="ico">&#x1F4B0;</span>TAKE TP</button>
      <button class="lvd-btn" id="lvd-add" style="background:rgba(0,229,255,.08);color:#00e5ff;border:1px solid rgba(0,229,255,.22)"><span class="ico">&#xFF0B;</span>ADD</button>
    </div>
  </div>
</div>
<div class="scan-section">
  <div class="scan-now-bar">
    <div class="sn-pulse"></div>
    <div class="sn-label">NOW SCANNING</div>
    <div class="sn-coin" id="snCoin">&#x2014;</div>
    <div class="sn-status scan" id="snStatus">WAITING</div>
    <button id="pause-btn" onclick="togglePause()" style="margin-left:auto;padding:4px 12px;border-radius:4px;border:1px solid __PAUSE_COLOR__;background:transparent;color:__PAUSE_COLOR__;font-family:monospace;font-size:9px;font-weight:700;letter-spacing:.1em;cursor:pointer">__PAUSE_LABEL__</button>
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
    if(!moved||isH===null){snap();toggleCardDrop(mint);return;}
    if(!isH){snap();return;}
    if(dx<-80){
      inner.style.transition='transform .28s ease';inner.style.transform='translateX(-110%)';bg.style.opacity='1';
      setTimeout(function(){dismissCard(mint);},290);
    }else snap();
  },{passive:true});
}
function dismissCard(mint){
  if(_exMint===mint)_closeDrop();
  var c=posCards[mint];if(!c)return;
  var card=c.card;
  card.style.transition='height .38s ease,opacity .28s ease';
  var h=card.offsetHeight;void card.offsetHeight;card.style.height=h+'px';
  requestAnimationFrame(function(){card.style.height='0';card.style.opacity='0';card.style.overflow='hidden';});
  setTimeout(function(){card.style.display='none';dismissed.add(mint);if(visMints().length<2&&stackExpanded)stackExpanded=false;updateStack();},400);
}
// ── Dropdown chart panel ───────────────────────────────────
let _exMint=null;
const _cvData={};
function _fmtS(s){if(s<60)return Math.round(s)+'s';if(s<3600)return Math.round(s/60)+'m';return Math.round(s/3600)+'h '+Math.round((s%3600)/60)+'m';}
function toggleCardDrop(mint){
  if(_exMint===mint){_closeDrop();return;}
  _exMint=mint;
  if(!_cvData[mint])_cvData[mint]={hist:[],timer:null,fresh:true};
  else{_cvData[mint].hist=[];_cvData[mint].fresh=true;}
  clearInterval(_cvData[mint].timer);
  _fetchDrop(mint);
  _cvData[mint].timer=setInterval(function(){_fetchDrop(mint);},3000);
  var drop=document.getElementById('lv-drop');
  // Use scrollHeight for exact fit; fall back to 400px before data loads
  drop.style.maxHeight=(drop.scrollHeight||400)+'px';
}
function _closeDrop(){
  if(_exMint&&_cvData[_exMint]){clearInterval(_cvData[_exMint].timer);_cvData[_exMint].timer=null;}
  _exMint=null;
  document.getElementById('lv-drop').style.maxHeight='0';
}
async function _fetchDrop(mint){
  if(_exMint!==mint)return;
  try{
    const r=await fetch('/positions/api');
    const d=await r.json();
    const pos=(d.positions||[]).find(function(p){return p.mint===mint;});
    if(!pos){_closeDrop();return;}
    const pnl=pos.pnl||0;
    const pe=document.getElementById('lvd-pnl');
    if(pe){pe.textContent=(pnl>=0?'+':'')+'$'+Math.abs(pnl).toFixed(4);pe.style.color=pnl>=0?'#39ff14':'#ff3355';}
    const prEl=document.getElementById('lvd-price');
    if(prEl)prEl.textContent='price $'+(pos.price||pos.entry||0).toFixed(8);
    const bEl=document.getElementById('lvd-bond');
    if(bEl)bEl.textContent='bond '+(pos.bond_high||0).toFixed(1)+'%';
    const hEl=document.getElementById('lvd-held');
    var heldS=pos.opened_at?Math.round(Date.now()/1000-pos.opened_at):0;
    if(hEl)hEl.textContent='held '+_fmtS(heldS);
    const cd=_cvData[mint];
    cd.hist.push(pos.price||pos.entry||0);
    if(cd.hist.length>40)cd.hist.shift();
    const anim=cd.fresh&&cd.hist.length>=2;
    if(anim)cd.fresh=false;
    _drawDrop(pos.entry||0,anim);
    // Re-fit height now that content is populated
    var drop=document.getElementById('lv-drop');
    if(drop.style.maxHeight!=='0px')drop.style.maxHeight=drop.scrollHeight+'px';
  }catch(e){}
}
function _drawDrop(entry,animate){
  const cv=document.getElementById('lvd-cv');
  if(!cv)return;
  const w=cv.offsetWidth||320,h=88;
  cv.width=w;cv.height=h;
  const ctx=cv.getContext('2d');
  const pts=(_cvData[_exMint]||{}).hist||[];
  if(pts.length<2){ctx.clearRect(0,0,w,h);return;}
  const mn=Math.min(...pts,entry),mx=Math.max(...pts,entry);
  const pad=(mx-mn)*0.12||entry*0.001;
  const lo=mn-pad,hi=mx+pad,rng=hi-lo||1;
  const xx=function(i){return(i/(pts.length-1))*w;};
  const yy=function(v){return h-((v-lo)/rng)*(h-4);};
  const last=pts[pts.length-1],col=last>=entry?'#39ff14':'#ff3355';
  const grad=ctx.createLinearGradient(0,0,0,h);
  grad.addColorStop(0,last>=entry?'rgba(57,255,20,.22)':'rgba(255,51,85,.22)');
  grad.addColorStop(1,'rgba(0,0,0,0)');
  function _r(prog){
    ctx.clearRect(0,0,w,h);
    ctx.strokeStyle='rgba(255,255,255,.15)';ctx.lineWidth=1;ctx.setLineDash([4,4]);
    ctx.beginPath();ctx.moveTo(0,yy(entry));ctx.lineTo(w,yy(entry));ctx.stroke();ctx.setLineDash([]);
    ctx.save();
    ctx.beginPath();ctx.rect(0,0,xx(pts.length-1)*prog+2,h);ctx.clip();
    ctx.beginPath();ctx.moveTo(xx(0),yy(pts[0]));
    pts.forEach(function(v,i){if(i>0)ctx.lineTo(xx(i),yy(v));});
    ctx.lineTo(w,h);ctx.lineTo(0,h);ctx.closePath();ctx.fillStyle=grad;ctx.fill();
    ctx.beginPath();ctx.moveTo(xx(0),yy(pts[0]));
    for(var i=1;i<pts.length;i++){var cx=xx(i-.5);ctx.bezierCurveTo(cx,yy(pts[i-1]),cx,yy(pts[i]),xx(i),yy(pts[i]));}
    ctx.strokeStyle=col;ctx.lineWidth=2;ctx.lineJoin='round';ctx.stroke();
    ctx.restore();
    if(prog>=1){
      var lx=xx(pts.length-1),ly=yy(last);
      ctx.beginPath();ctx.arc(lx,ly,4,0,Math.PI*2);
      ctx.fillStyle=col;ctx.shadowColor=col;ctx.shadowBlur=12;ctx.fill();ctx.shadowBlur=0;
    }
  }
  if(animate){
    var start=null;var dur=500;
    function step(ts){if(!start)start=ts;var p=Math.min((ts-start)/dur,1);_r(1-(1-p)*(1-p)*(1-p));if(p<1)requestAnimationFrame(step);}
    requestAnimationFrame(step);
  }else{_r(1);}
}
function _lvFlash(id,color){
  var btn=document.getElementById(id);if(!btn)return;
  var ob=btn.style.background,oc=btn.style.color,obc=btn.style.borderColor;
  btn.style.background='rgba(0,255,136,.25)';btn.style.color='#00ff88';btn.style.borderColor='rgba(0,255,136,.55)';
  setTimeout(function(){btn.style.background=ob;btn.style.color=oc;btn.style.borderColor=obc;},1400);
}
function _lvSetBtns(disabled){
  ['lvd-close','lvd-tp','lvd-add'].forEach(function(id){var b=document.getElementById(id);if(b)b.disabled=disabled;b.style.opacity=disabled?'.4':'1';});
}
async function lvAct(action){
  if(!_exMint)return;
  var mint=_exMint;
  var url,body=null;
  if(action==='close'){
    url='/position/'+mint+'/close';
  } else if(action==='tp'){
    url='/position/'+mint+'/tp';body={fraction:0.4};
  } else if(action==='add'){
    var amtStr=prompt('Add funds ($):','5');
    if(!amtStr)return;
    var amt=parseFloat(amtStr);
    if(isNaN(amt)||amt<=0){alert('Enter a valid amount');return;}
    url='/position/'+mint+'/compound';body={amount:amt};
  }
  _lvSetBtns(true);
  try{
    var r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:body?JSON.stringify(body):null});
    if(!r.ok){var e=await r.json().catch(function(){return {};});alert('Failed: '+(e.error||('HTTP '+r.status)));return;}
    if(action==='close'){
      _closeDrop();
      dismissCard(mint);
    } else {
      _lvFlash(action==='tp'?'lvd-tp':'lvd-add');
      if(_cvData[mint]){_cvData[mint].hist=[];_cvData[mint].fresh=true;}
      _fetchDrop(mint);
    }
  }catch(e){alert('Network error — try again');}
  finally{_lvSetBtns(false);}
}
document.getElementById('lvd-close').addEventListener('click',function(){lvAct('close');});
document.getElementById('lvd-tp').addEventListener('click',function(){lvAct('tp');});
document.getElementById('lvd-add').addEventListener('click',function(){lvAct('add');});
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
  (d.errors||[]).forEach(function(e){
    rows.push(
      '<div class="trade-row" style="border-color:rgba(255,51,85,.2);background:rgba(255,51,85,.06)">'+
      '<div class="tr-time">'+e.time+'</div>'+
      '<div class="tr-icon loss">&#x26A0;</div>'+
      '<div class="tr-body">'+
        '<div class="tr-sym" style="color:#ff3355">'+(e.symbol||'BOT')+'</div>'+
        '<div class="tr-tags"><span class="tag tag-loss">'+(e.tag||'err').toUpperCase()+'</span></div>'+
        '<div class="tr-detail" style="font-size:8px;color:#ff8899">'+e.msg+'</div>'+
      '</div>'+
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
async function _punkAdminPost(url,body){
  var s=localStorage.getItem('api_secret')||'';
  if(!s){s=prompt('API secret:');if(!s)return null;localStorage.setItem('api_secret',s);}
  var r=await fetch(url,{method:'POST',headers:{'X-API-Key':s,'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(r.status===401){
    var k=prompt('Wrong API secret — enter correct key:');
    if(!k)return null;
    localStorage.setItem('api_secret',k);
    return _punkAdminPost(url,body);
  }
  return r.json();
}
async function togglePause(){
  var btn=document.getElementById('pause-btn');
  var isPaused=btn.textContent.includes('RESUME');
  btn.disabled=true; btn.textContent='...';
  try{
    var url=isPaused?'/admin/resume':'/admin/pause';
    var d=await _punkAdminPost(url,{hours:24});
    if(d){
      btn.textContent=isPaused?'⏸ PAUSE':'▶ RESUME';
      btn.style.color=isPaused?'#00e5ff':'#fbbf24';
      btn.style.borderColor=isPaused?'#00e5ff':'#fbbf24';
    }
  }catch(e){alert('Error: '+e);}
  btn.disabled=false;
}
</script>
</body></html>"""
    paused = _pause_until > time.time()
    html = html.replace("__BOT_NAME__", BOT_NAME)
    html = html.replace("__PAUSE_COLOR__", "#fbbf24" if paused else "#00e5ff")
    html = html.replace("__PAUSE_LABEL__", "▶ RESUME" if paused else "⏸ PAUSE")
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
                "bond_tp":         f"+{BOND_TP_PCT}%",
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
            "next_tune_str": time.strftime("Mon %b %d · 07:00 UTC", time.gmtime(TUNE_PAUSED_UNTIL)) if TUNE_PAUSED_UNTIL > 0 else "—",
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
    <a href="/chart">CHART</a>
    <a href="/setup">SETUP</a>
  </nav>

  <div class="page-title">STRATEGY</div>

  <div class="status-bar">
    <div class="pill mode"><span class="dot purple blink"></span><span id="mode-pill">Loading...</span></div>
    <div class="pill">Next retune: <strong id="tune-every" class="purple">--</strong></div>
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
        <div class="p-lbl">Min 1h Change</div>
        <div class="p-val" id="p-spike-vol">--%</div>
        <div class="p-desc">Price change in 1h</div>
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
        Parameters auto-tune weekly every Monday 07:00 UTC.
      </div>
    </div>

    <div class="strat-card">
      <div class="strat-card-hdr">
        <span class="tag spike">SPIKE</span>
        <span style="font-size:.75rem;font-weight:700">Spike Detector</span>
      </div>
      <div class="strat-card-body">
        Targets tokens older than <strong id="desc-spike-age">--</strong>h with a 1h price surge above
        <strong id="desc-spike-vol">--</strong>%. Scalps the momentum burst. High TP, tighter timeout.
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
    document.getElementById('tune-every').textContent = d.next_tune_str || 'Mon 07:00 UTC';
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
    <a href="/chart">CHART</a>
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
    _blacklist_add(mint)
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

# ── PROFIT PROJECTOR / CHART ─────────────────────────────────────

@app.route("/chart/api", methods=["GET"])
def chart_api():
    mint = request.args.get("mint", "").strip()
    resolution = int(request.args.get("resolution", "5"))  # minutes
    if not mint:
        return jsonify({"error": "mint required"}), 400
    try:
        # Get pair address + market snapshot
        market = get_market_data(mint)
        pair_address = (market or {}).get("pair_address", "")

        # Try bonding curve details for pre-grad coins
        bond_info = get_bonding_details(mint)

        # Fetch candles
        candles = []
        if pair_address:
            now     = int(time.time())
            from_ms = (now - 300 * resolution * 60) * 1000  # last 300 candles
            to_ms   = now * 1000
            try:
                r = _session.get(
                    f"https://api.dexscreener.com/latest/dex/candles/solana/{pair_address}",
                    params={"from": from_ms, "to": to_ms, "resolution": resolution},
                    timeout=10
                )
                if r.status_code == 200:
                    raw = r.json()
                    raw_candles = raw.get("candles", raw) if isinstance(raw, dict) else raw
                    for c in (raw_candles or []):
                        ts = c.get("time") or c.get("timestamp") or c.get("t") or 0
                        if ts > 1e12:
                            ts = int(ts / 1000)
                        candles.append({
                            "time":   int(ts),
                            "open":   float(c.get("open",  c.get("o", 0)) or 0),
                            "high":   float(c.get("high",  c.get("h", 0)) or 0),
                            "low":    float(c.get("low",   c.get("l", 0)) or 0),
                            "close":  float(c.get("close", c.get("c", 0)) or 0),
                            "volume": float(c.get("volume",c.get("v", 0)) or 0),
                        })
                    candles = [c for c in candles if c["close"] > 0]
                    candles.sort(key=lambda c: c["time"])
            except Exception as e:
                log("warn", f"Chart candles: {e}", "CHART")

        sig_score  = gmgn_signal_score(mint)
        sm_selling = gmgn_smart_money_selling(mint)

        with trades_lock:
            open_trade = dict(open_trades.get(mint, {}))

        return jsonify({
            "mint":         mint,
            "market":       market,
            "bond":         bond_info,
            "candles":      candles,
            "sig_score":    sig_score,
            "sm_selling":   sm_selling,
            "open_trade":   open_trade,
            "tp1_pct":      PARTIAL_TP1_PCT,
            "tp2_pct":      PARTIAL_TP2_PCT,
            "full_tp_pct":  MIGRATE_TP_PCT,
            "sl_pct":       BOND_SL_PCT,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/chart", methods=["GET"])
def chart_page():
    mint = request.args.get("mint", "")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>Profit Projector · {BOT_NAME}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;600;700;900&family=JetBrains+Mono:wght@500;700&display=swap');
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{--acc:#39ff14;--cyan:#00e5ff;--red:#ff3355;--yellow:#ffee00;--bg:#0a0008;--card:#110010;--border:#ffffff12;--muted:#66668a}}
body{{background:var(--bg);color:#e8e8f4;font-family:'Inter',sans-serif;min-height:100vh;overflow-x:hidden}}
nav{{position:sticky;top:0;z-index:200;display:flex;border-bottom:2px solid var(--acc);background:rgba(10,0,8,.97);backdrop-filter:blur(12px);overflow-x:auto;scrollbar-width:none}}
nav::-webkit-scrollbar{{display:none}}
nav a{{color:#aaa;text-decoration:none;font-size:.68rem;font-weight:700;padding:11px 13px;white-space:nowrap;letter-spacing:.07em;text-transform:uppercase;border-right:1px solid var(--border);transition:all .15s;flex-shrink:0}}
nav a:hover{{background:rgba(57,255,20,.12);color:var(--acc)}}
nav a.active{{background:var(--acc);color:#000}}
.wrap{{max-width:980px;margin:0 auto;padding:18px 12px 40px}}
.page-title{{font-family:'Bebas Neue',sans-serif;font-size:2rem;letter-spacing:.05em;color:var(--acc);text-shadow:0 0 28px rgba(57,255,20,.3);margin-bottom:3px}}
.page-sub{{font-size:.68rem;color:var(--muted);margin-bottom:14px}}
.search-card{{background:var(--card);border:1px solid rgba(57,255,20,.2);border-top:2px solid var(--acc);padding:14px;margin-bottom:14px}}
.search-label{{font-size:.58rem;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:7px}}
.search-row{{display:flex;gap:8px;flex-wrap:wrap}}
.mint-input{{flex:1;min-width:180px;background:#0a0008;border:1px solid rgba(57,255,20,.25);padding:11px 13px;color:#fff;font-family:'JetBrains Mono',monospace;font-size:.76rem;outline:none;transition:border-color .2s}}
.mint-input:focus{{border-color:var(--acc);box-shadow:0 0 0 2px rgba(57,255,20,.1)}}
.mint-input::placeholder{{color:#444}}
.btn-load{{background:var(--acc);color:#000;border:none;padding:11px 22px;font-weight:900;font-size:.76rem;letter-spacing:.06em;text-transform:uppercase;cursor:pointer;transition:opacity .15s;white-space:nowrap;font-family:'Inter',sans-serif}}
.btn-load:hover{{opacity:.85}}
.btn-load:disabled{{opacity:.4;cursor:not-allowed}}
.sig-banner{{display:none;margin-bottom:12px;background:var(--card);border:1px solid var(--border);border-top:3px solid var(--acc);padding:12px 14px}}
.sig-banner.show{{display:block}}
.sig-top{{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:10px}}
.coin-name{{font-weight:900;font-size:.95rem;letter-spacing:.02em}}
.coin-price{{font-family:'JetBrains Mono',monospace;font-size:.85rem;color:var(--cyan)}}
.sig-action{{font-family:'Bebas Neue',sans-serif;font-size:1.7rem;letter-spacing:.06em;padding:2px 16px;border:2px solid}}
.sig-BUY{{color:var(--acc);border-color:var(--acc);background:rgba(57,255,20,.08);text-shadow:0 0 18px rgba(57,255,20,.5)}}
.sig-SELL{{color:var(--red);border-color:var(--red);background:rgba(255,51,85,.08)}}
.sig-CAUTION{{color:var(--yellow);border-color:var(--yellow);background:rgba(255,238,0,.08)}}
.sig-WAIT{{color:var(--muted);border-color:var(--muted);background:rgba(255,255,255,.04)}}
.ind-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}}
@media(max-width:480px){{.ind-grid{{grid-template-columns:1fr 1fr}}}}
.ind{{background:#0a0008;padding:8px 10px;border-left:2px solid var(--border)}}
.ind-label{{font-size:.55rem;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}}
.ind-val{{font-family:'JetBrains Mono',monospace;font-size:.8rem;font-weight:700;margin-top:3px}}
.g{{color:var(--acc)}} .r{{color:var(--red)}} .y{{color:var(--yellow)}} .c{{color:var(--cyan)}}
.reasons{{display:flex;gap:5px;flex-wrap:wrap;margin-top:10px}}
.pill{{font-size:.6rem;font-weight:700;padding:3px 8px;border:1px solid;letter-spacing:.04em}}
.pg{{color:var(--acc);border-color:rgba(57,255,20,.35);background:rgba(57,255,20,.08)}}
.pr{{color:var(--red);border-color:rgba(255,51,85,.35);background:rgba(255,51,85,.08)}}
.py{{color:var(--yellow);border-color:rgba(255,238,0,.3);background:rgba(255,238,0,.06)}}
.pc{{color:var(--cyan);border-color:rgba(0,229,255,.3);background:rgba(0,229,255,.06)}}
.ot-banner{{display:none;margin-bottom:10px;background:rgba(57,255,20,.07);border:1px solid rgba(57,255,20,.3);border-left:3px solid var(--acc);padding:10px 14px;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}}
.ot-banner.show{{display:flex}}
.ot-dot{{width:8px;height:8px;border-radius:50%;background:var(--acc);animation:blink 1.2s infinite;flex-shrink:0}}
@keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.15}}}}
.ot-inner{{display:flex;align-items:center;gap:10px}}
.ot-title{{font-size:.74rem;font-weight:700;color:var(--acc);letter-spacing:.04em}}
.ot-meta{{font-size:.66rem;color:var(--muted)}} .ot-meta span{{color:#e8e8f4}}
.ot-pnl{{font-family:'Bebas Neue',sans-serif;font-size:1.5rem}}
.chart-card{{background:#000;border:1px solid var(--border);border-top:2px solid rgba(57,255,20,.35);margin-bottom:12px;overflow:hidden}}
.chart-topbar{{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;border-bottom:1px solid var(--border);font-size:.66rem}}
.chart-topbar-left{{display:flex;align-items:center;gap:10px}}
.chart-live-dot{{width:6px;height:6px;border-radius:50%;background:var(--acc);animation:blink 1.4s infinite}}
.chart-frame{{width:100%;height:430px;border:none;display:none}}
.chart-loading{{height:430px;display:none;align-items:center;justify-content:center;flex-direction:column;gap:12px;background:#0a0008}}
.chart-loading.show{{display:flex}}
.spin{{width:30px;height:30px;border:2px solid rgba(57,255,20,.2);border-top-color:var(--acc);border-radius:50%;animation:spin .7s linear infinite}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
.chart-empty{{height:430px;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:10px;background:#0a0008}}
.empty-icon{{font-size:2.2rem;filter:drop-shadow(0 0 14px rgba(57,255,20,.3))}}
.empty-title{{font-family:'Bebas Neue',sans-serif;font-size:1.3rem;color:var(--acc);letter-spacing:.05em}}
.empty-tips{{display:flex;flex-direction:column;gap:5px;margin-top:4px}}
.etip{{display:flex;align-items:center;gap:8px;font-size:.65rem;color:var(--muted);padding:5px 12px;background:rgba(57,255,20,.05);border-left:2px solid rgba(57,255,20,.2)}}
.chart-error{{height:90px;display:none;align-items:center;justify-content:center;flex-direction:column;gap:6px}}
.chart-error.show{{display:flex}}
.config-strip{{display:none;flex-wrap:wrap;gap:12px;padding:7px 12px;border-top:1px solid var(--border);font-size:.62rem;color:var(--muted);background:#0a0008}}
.config-strip.show{{display:flex}}
.panels{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px}}
@media(max-width:600px){{.panels{{grid-template-columns:1fr}}}}
.panel{{background:var(--card);border:1px solid var(--border);border-top:2px solid rgba(57,255,20,.2);padding:14px}}
.panel-title{{font-family:'Bebas Neue',sans-serif;font-size:1rem;color:var(--acc);letter-spacing:.05em;margin-bottom:11px}}
.inv-row{{display:flex;align-items:center;gap:8px;margin-bottom:10px}}
.inv-row label{{font-size:.6rem;color:var(--muted);white-space:nowrap;font-weight:700;text-transform:uppercase;letter-spacing:.06em}}
.inv-input{{flex:1;background:#0a0008;border:1px solid rgba(57,255,20,.2);padding:7px 10px;color:var(--acc);font-size:.85rem;outline:none;font-family:'JetBrains Mono',monospace;font-weight:700}}
.inv-input:focus{{border-color:var(--acc)}}
.proj-row{{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.05)}}
.proj-row:last-child{{border-bottom:none}}
.proj-label{{font-size:.68rem;color:var(--muted)}}
.proj-val{{font-size:.8rem;font-weight:700;font-family:'JetBrains Mono',monospace}}
.stat-grid{{display:grid;grid-template-columns:1fr 1fr;gap:6px}}
.stat-box{{background:#0a0008;padding:9px 10px;text-align:center}}
.stat-box .val{{font-family:'JetBrains Mono',monospace;font-size:.8rem;font-weight:700;color:var(--cyan)}}
.stat-box .lbl{{font-size:.56rem;color:var(--muted);margin-top:3px;text-transform:uppercase;letter-spacing:.07em}}
.no-data{{font-size:.7rem;color:var(--muted);text-align:center;padding:18px;grid-column:1/-1}}
@keyframes shake{{0%{{transform:translateX(0)}}20%{{transform:translateX(-5px)}}40%{{transform:translateX(5px)}}60%{{transform:translateX(-3px)}}80%{{transform:translateX(3px)}}100%{{transform:translateX(0)}}}}
</style>
</head>
<body>
<nav>
  <a href="/">HOME</a>
  <a href="/live">LIVE</a>
  <a href="/trades">TRADES</a>
  <a href="/status">STATUS</a>
  <a href="/learn">STRATEGY</a>
  <a href="/chart" class="active">CHART</a>
  <a href="/setup">SETUP</a>
</nav>
<div class="wrap">
  <div class="page-title">Profit Projector</div>
  <div class="page-sub">Paste any Solana token address · live chart + buy/sell signals + profit calculator</div>
  <div class="search-card">
    <div class="search-label">Token Address</div>
    <div class="search-row">
      <input id="mint-input" class="mint-input" placeholder="e.g. BuiakygrxVqV6me8sRD2WQrcZxLQyChR1sekXrRDpump" value="{mint}" autocomplete="off" spellcheck="false" onkeydown="if(event.key==='Enter')loadChart()">
      <button id="load-btn" class="btn-load" onclick="loadChart()">Load Chart</button>
    </div>
  </div>
  <div id="sig-banner" class="sig-banner">
    <div class="sig-top">
      <div><div class="coin-name" id="coin-name">—</div><div class="coin-price" id="coin-price">—</div></div>
      <div id="sig-action" class="sig-action sig-WAIT">WAIT</div>
    </div>
    <div class="ind-grid" id="ind-grid"></div>
    <div class="reasons" id="reasons"></div>
  </div>
  <div id="ot-banner" class="ot-banner">
    <div class="ot-inner"><div class="ot-dot"></div>
      <div><div class="ot-title">BOT HAS AN OPEN POSITION</div><div class="ot-meta" id="ot-meta"></div></div>
    </div>
    <div class="ot-pnl g" id="ot-pnl">—</div>
  </div>
  <div class="chart-card">
    <div class="chart-topbar">
      <div class="chart-topbar-left">
        <div class="chart-live-dot" id="live-dot" style="display:none"></div>
        <span style="color:var(--acc);font-weight:700;font-size:.7rem;letter-spacing:.04em" id="chart-label">CHART</span>
        <span style="color:var(--muted);font-size:.6rem">via DexScreener</span>
      </div>
      <span style="color:var(--muted);font-size:.6rem" id="chart-age"></span>
    </div>
    <div id="chart-empty" class="chart-empty">
      <div class="empty-icon">📈</div>
      <div class="empty-title">Enter a Token Address</div>
      <div class="empty-tips">
        <div class="etip"><span style="color:var(--acc);font-weight:900">▸</span>Green signal = strong buy pressure</div>
        <div class="etip"><span style="color:var(--acc);font-weight:900">▸</span>Red signal = smart money exiting</div>
        <div class="etip"><span style="color:var(--acc);font-weight:900">▸</span>Projector shows exact $ returns at each TP</div>
      </div>
    </div>
    <div id="chart-loading" class="chart-loading">
      <div class="spin"></div>
      <div style="font-size:.74rem;color:var(--muted);font-weight:600">Fetching live chart…</div>
    </div>
    <iframe id="chart-frame" class="chart-frame" src="" allowfullscreen></iframe>
    <div id="chart-error" class="chart-error">
      <div style="color:var(--red);font-weight:700;font-size:.8rem">Could not load chart</div>
      <div style="color:var(--muted);font-size:.68rem" id="error-msg">Token not found or not yet on DEX.</div>
    </div>
    <div id="config-strip" class="config-strip">
      <span>TP1 <b class="g" id="cs-tp1">+50%</b></span>
      <span>TP2 <b class="y" id="cs-tp2">+100%</b></span>
      <span>Full TP <b class="c" id="cs-ftp">+400%</b></span>
      <span>SL <b class="r" id="cs-sl">-8%</b></span>
      <span style="margin-left:auto;color:#555">sell 40% → 40% → hold 36%</span>
    </div>
  </div>
  <div class="panels">
    <div class="panel">
      <div class="panel-title">💰 Profit Projector</div>
      <div class="inv-row"><label>Trade $</label>
        <input id="inv-input" class="inv-input" type="number" value="10" min="1" oninput="updateProjection()">
      </div>
      <div id="proj-rows"><div class="no-data">Load a token to see projected returns</div></div>
    </div>
    <div class="panel">
      <div class="panel-title">📊 Market Data</div>
      <div id="stat-grid" class="stat-grid"><div class="no-data">Load a token to see market data</div></div>
    </div>
  </div>
</div>
<script>
let lastData=null;
function fmt(n){{if(n>=1e6)return(n/1e6).toFixed(2)+'M';if(n>=1000)return Math.round(n).toLocaleString();return n.toFixed(2);}}
function fmtPct(n){{return(n>=0?'+':'')+n.toFixed(1)+'%';}}
async function loadChart(){{
  const mint=document.getElementById('mint-input').value.trim();
  if(!mint||mint.length<30){{const el=document.getElementById('mint-input');el.style.animation='none';el.offsetHeight;el.style.animation='shake .3s';el.style.borderColor='var(--red)';setTimeout(()=>el.style.borderColor='',1200);return;}}
  const btn=document.getElementById('load-btn');
  btn.disabled=true;btn.textContent='Loading…';
  document.getElementById('chart-empty').style.display='none';
  document.getElementById('chart-loading').className='chart-loading show';
  document.getElementById('chart-frame').style.display='none';
  document.getElementById('chart-error').className='chart-error';
  document.getElementById('config-strip').className='config-strip';
  document.getElementById('sig-banner').className='sig-banner';
  document.getElementById('ot-banner').className='ot-banner';
  document.getElementById('live-dot').style.display='none';
  try{{
    const r=await fetch('/chart/api?mint='+encodeURIComponent(mint)+'&resolution=5');
    const d=await r.json();
    if(d.error){{showErr(d.error);return;}}
    lastData=d;
    const m=d.market||{{}};
    const pairAddr=m.pair_address||'';
    if(pairAddr){{
      const frame=document.getElementById('chart-frame');
      frame.src='https://dexscreener.com/solana/'+pairAddr+'?embed=1&theme=dark&trades=0&info=0';
      frame.style.display='block';
      document.getElementById('live-dot').style.display='block';
      document.getElementById('chart-label').textContent=(m.symbol||'LIVE')+' · LIVE';
    }}else{{showErr('No DEX pair — coin may still be on PumpFun bonding curve only.');return;}}
    document.getElementById('chart-loading').className='chart-loading';
    document.getElementById('config-strip').className='config-strip show';
    if(m.age_h!=null){{const ah=parseFloat(m.age_h)||0;document.getElementById('chart-age').textContent=ah<1?Math.round(ah*60)+'m old':ah<24?ah.toFixed(1)+'h old':Math.floor(ah/24)+'d old';}}
    document.getElementById('cs-tp1').textContent='+'+d.tp1_pct+'%';
    document.getElementById('cs-tp2').textContent='+'+d.tp2_pct+'%';
    document.getElementById('cs-ftp').textContent='+'+d.full_tp_pct+'%';
    document.getElementById('cs-sl').textContent='-'+d.sl_pct+'%';
    renderSignals(d);renderStats(m,d);updateProjection();
    try{{history.replaceState(null,'','/chart?mint='+encodeURIComponent(mint));}}catch(e){{}}
  }}catch(err){{showErr('Network error.');}}
  finally{{btn.disabled=false;btn.textContent='Load Chart';}}
}}
function showErr(msg){{
  document.getElementById('chart-loading').className='chart-loading';
  document.getElementById('chart-frame').style.display='none';
  document.getElementById('chart-error').className='chart-error show';
  document.getElementById('error-msg').textContent=msg;
}}
function renderSignals(d){{
  const m=d.market||{{}};
  const c5=parseFloat(m.change5m||0),c1h=parseFloat(m.change1h||0);
  const b5=parseInt(m.buys_m5||0),s5=parseInt(m.sells_m5||0);
  const liq=parseFloat(m.liq||0);
  const ratio5=(b5+s5)>0?b5/(b5+s5):0.5;
  let score=0;const reasons=[];
  if(c5>5){{score+=2;reasons.push({{t:'5m '+fmtPct(c5),c:'pg'}});}}else if(c5<-5){{score-=2;reasons.push({{t:'5m '+fmtPct(c5),c:'pr'}});}}
  if(c1h>20){{score+=2;reasons.push({{t:'1h '+fmtPct(c1h),c:'pg'}});}}else if(c1h<-10){{score-=2;reasons.push({{t:'1h '+fmtPct(c1h),c:'pr'}});}}
  if(ratio5>0.62){{score+=2;reasons.push({{t:'Buys '+b5+' / Sells '+s5,c:'pg'}});}}else if(ratio5<0.4){{score-=2;reasons.push({{t:'Sells dominating',c:'pr'}});}}
  if(liq>50000){{score+=1;reasons.push({{t:'Liq $'+fmt(liq),c:'pc'}});}}else if(liq<5000&&liq>0){{score-=1;reasons.push({{t:'Low liq $'+fmt(liq),c:'py'}});}}
  if(d.sig_score>0){{score+=1;reasons.push({{t:'GMGN sig='+d.sig_score,c:'py'}});}}
  if(d.sm_selling){{score-=2;reasons.push({{t:'Smart $ Selling',c:'pr'}});}}
  if(d.bond&&d.bond.bond_pct){{reasons.push({{t:'Bond '+d.bond.bond_pct.toFixed(1)+'%',c:'pc'}});}}
  let action='WAIT',acls='WAIT';
  if(score>=5){{action='STRONG BUY';acls='BUY';}}else if(score>=2){{action='BUY';acls='BUY';}}else if(score<=-4){{action='SELL';acls='SELL';}}else if(score<=-1){{action='CAUTION';acls='CAUTION';}}
  document.getElementById('sig-banner').className='sig-banner show';
  document.getElementById('coin-name').textContent=(m.symbol||'')+' · '+(m.name||'');
  document.getElementById('coin-price').textContent=parseFloat(m.price||0)>0?'$'+parseFloat(m.price).toPrecision(5):'—';
  const el=document.getElementById('sig-action');el.textContent=action;el.className='sig-action sig-'+acls;
  document.getElementById('ind-grid').innerHTML=[
    {{l:'5m',v:fmtPct(c5),c:c5>=0?'g':'r'}},
    {{l:'1h',v:fmtPct(c1h),c:c1h>=0?'g':'r'}},
    {{l:'Buys/Sells 5m',v:b5+' / '+s5,c:ratio5>0.5?'g':'r'}},
    {{l:'Liquidity',v:'$'+fmt(liq),c:liq>50000?'g':liq<5000?'r':'y'}},
    {{l:'GMGN Score',v:d.sig_score||'0',c:d.sig_score>0?'y':''}},
    {{l:'Momentum',v:score>=2?'BULLISH':score<=-2?'BEARISH':'NEUTRAL',c:score>=2?'g':score<=-2?'r':'y'}},
  ].map(x=>'<div class="ind"><div class="ind-label">'+x.l+'</div><div class="ind-val '+x.c+'">'+x.v+'</div></div>').join('');
  document.getElementById('reasons').innerHTML=reasons.map(r=>'<span class="pill '+r.c+'">'+r.t+'</span>').join('');
  const ot=d.open_trade;
  if(ot&&ot.entry){{
    document.getElementById('ot-banner').className='ot-banner show';
    const pnlPct=m.price?((parseFloat(m.price)-ot.entry)/ot.entry*100).toFixed(1):0;
    document.getElementById('ot-meta').innerHTML='Entry: <span>$'+ot.entry.toFixed(8)+'</span> · Size: <span>$'+ot.amount.toFixed(2)+'</span> · Strat: <span>'+(ot.strategy||'').toUpperCase()+'</span>';
    const pe=document.getElementById('ot-pnl');pe.textContent=(pnlPct>=0?'+':'')+pnlPct+'%';pe.className='ot-pnl '+(pnlPct>=0?'g':'r');
  }}
}}
function renderStats(m,d){{
  document.getElementById('stat-grid').innerHTML=[
    {{v:parseFloat(m.price||0)>0?'$'+parseFloat(m.price).toPrecision(5):'—',l:'Price'}},
    {{v:'$'+fmt(parseFloat(m.liq||0)),l:'Liquidity'}},
    {{v:'$'+fmt(parseFloat(m.vol_m5||0)),l:'5m Volume',c:parseFloat(m.vol_m5||0)>5000?'g':''}},
    {{v:'$'+fmt(parseFloat(m.vol_h1||0)),l:'1h Volume'}},
    {{v:(m.buys_m5||0)+' / '+(m.sells_m5||0),l:'Buys/Sells 5m',c:(m.buys_m5||0)>(m.sells_m5||0)?'g':'r'}},
    {{v:d.sig_score||'0',l:'GMGN Score',c:d.sig_score>0?'y':''}},
  ].map(s=>'<div class="stat-box"><div class="val '+(s.c||'')+'">'+s.v+'</div><div class="lbl">'+s.l+'</div></div>').join('');
}}
function updateProjection(){{
  const inv=parseFloat(document.getElementById('inv-input').value)||10;
  const tp1=(lastData&&lastData.tp1_pct)||50,tp2=(lastData&&lastData.tp2_pct)||100;
  const full=(lastData&&lastData.full_tp_pct)||400,sl=(lastData&&lastData.sl_pct)||8;
  const p1=(inv*.40)*(tp1/100),r1=inv*.60,p2=(r1*.40)*(tp2/100),r2=r1*.60,pf=r2*(full/100);
  const tot=p1+p2+pf,slv=inv*(sl/100),rr=(tot/slv).toFixed(1);
  document.getElementById('cs-tp1').textContent='+'+tp1+'%';
  document.getElementById('cs-tp2').textContent='+'+tp2+'%';
  document.getElementById('cs-ftp').textContent='+'+full+'%';
  document.getElementById('cs-sl').textContent='-'+sl+'%';
  document.getElementById('proj-rows').innerHTML=[
    {{l:'TP1 +'+tp1+'% · sell 40%',v:'+$'+p1.toFixed(2),c:'g'}},
    {{l:'TP2 +'+tp2+'% · sell 40% more',v:'+$'+p2.toFixed(2),c:'y'}},
    {{l:'Full exit +'+full+'%',v:'+$'+pf.toFixed(2),c:'c'}},
    {{l:'Total if all TPs hit',v:'+$'+tot.toFixed(2),c:'g',b:true}},
    {{l:'Stop loss -'+sl+'%',v:'-$'+slv.toFixed(2),c:'r'}},
    {{l:'Risk : Reward',v:'1 : '+rr,c:'c'}},
  ].map(x=>'<div class="proj-row"><span class="proj-label" style="'+(x.b?'color:#e8e8f4;font-weight:700':'')+'">'+x.l+'</span><span class="proj-val '+x.c+'" style="'+(x.b?'font-size:.88rem':'')+'">'+x.v+'</span></div>').join('');
}}
(function(){{const m=new URLSearchParams(window.location.search).get('mint');if(m){{document.getElementById('mint-input').value=m;loadChart();}}}})();
</script>
</body>
</html>"""



# ── POSITIONS FEATURE ────────────────────────────────────────────
def _pos_price(trade):
    """Best-effort current price: paper mode uses bond drift, live uses price_high."""
    if PAPER_MODE:
        bond_h = trade.get("bond_high", trade.get("bond_entry", 0))
        bond_e = trade.get("bond_entry", 0)
        entry  = trade["entry"]
        if bond_e > 0 and bond_h >= bond_e:
            return entry * (1 + (bond_h - bond_e) / 100)
    return trade.get("price_high", trade["entry"])

@app.route("/positions/api", methods=["GET"])
def positions_api():
    with trades_lock:
        rows = []
        for mint, t in open_trades.items():
            price   = _pos_price(t)
            tokens  = t["tokens"]
            amount  = t["amount"]
            partial = t.get("partial_proceeds", 0.0)
            final_v = tokens * price if price > 0 else 0
            pnl     = max(-amount, min((partial + final_v) - amount, amount * 5))
            rows.append({
                "mint":           mint,
                "symbol":         t["symbol"],
                "strategy":       t["strategy"],
                "entry":          t["entry"],
                "price":          price,
                "amount":         amount,
                "tokens":         tokens,
                "opened_at":      t["opened_at"],
                "bond_entry":     t.get("bond_entry", 0),
                "bond_high":      t.get("bond_high",  t.get("bond_entry", 0)),
                "sl_pct":         BOND_SL_PCT,
                "tp1_pct":        PARTIAL_TP1_PCT,
                "partial_tp_done": t.get("partial_tp_done", 0),
                "partial_proceeds": partial,
                "pnl":            round(pnl, 4),
                "pnl_pct":        round(pnl / max(amount, 1e-12) * 100, 2),
                "tx":             t.get("tx", ""),
            })
    with capital_lock:
        cap = capital
    return jsonify({"positions": rows, "capital": round(cap, 2)})

@app.route("/position/<mint>/close", methods=["POST"])
def position_close(mint):
    global capital
    with trades_lock:
        if mint not in open_trades:
            return jsonify({"error": "not found"}), 404
        trade = dict(open_trades[mint])
    if not PAPER_MODE:
        # Use actual wallet balance for sell amount — slippage means stored count may differ
        real_bal = _check_token_balance(mint)
        if real_bal > 0:
            with trades_lock:
                if mint in open_trades:
                    open_trades[mint]["tokens"] = real_bal
    price = _pos_price(trade)
    exit_trade(mint, price, "MANUAL_CLOSE", trade.get("bond_high", 0))
    return jsonify({"ok": True})

@app.route("/position/<mint>/tp", methods=["POST"])
def position_tp(mint):
    from flask import request as _req
    data     = _req.get_json(silent=True) or {}
    fraction = float(data.get("fraction", 0.40))
    with trades_lock:
        if mint not in open_trades:
            return jsonify({"error": "not found"}), 404
        trade = dict(open_trades[mint])
    price = _pos_price(trade)
    _partial_exit(mint, price, fraction, "MANUAL_TP")
    return jsonify({"ok": True})

@app.route("/position/<mint>/compound", methods=["POST"])
def position_compound(mint):
    global capital
    from flask import request as _req
    data       = _req.get_json(silent=True) or {}
    add_amount = float(data.get("amount", 0))
    if add_amount <= 0:
        return jsonify({"error": "invalid amount"}), 400
    with capital_lock:
        if capital < add_amount:
            return jsonify({"error": "insufficient capital"}), 400
    with trades_lock:
        if mint not in open_trades:
            return jsonify({"error": "not found"}), 404
        t          = open_trades[mint]
        price      = _pos_price(t)
        if price <= 0:
            return jsonify({"error": "invalid price"}), 400
        old_tok    = t["tokens"]
        new_tok    = add_amount / price
        new_total  = t["amount"] + add_amount
        t["tokens"] += new_tok
        t["entry"]  = new_total / max(old_tok + new_tok, 1e-12)
        t["amount"] = new_total
        sym = t["symbol"]
    with capital_lock:
        capital -= add_amount
    log("ok", f"[MANUAL_COMPOUND] +${add_amount:.2f} | new_entry={t['entry']:.8f}", sym)
    return jsonify({"ok": True})

@app.route("/positions", methods=["GET"])
def positions_page():
    mode = "PAPER" if PAPER_MODE else "LIVE"
    dot_col = "#f5c542" if PAPER_MODE else "#4ade80"
    pill_bg  = "background:#f5c54218;color:#f5c542;border-color:#f5c54244" if PAPER_MODE else "background:#4ade8018;color:#4ade80;border-color:#4ade8044"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Positions — {BOT_NAME}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}}
:root{{
  --gold:#f5c542;--gold2:#e8a800;--bg:#080810;--surface:#10101a;
  --surface2:#16162a;--border:#ffffff0d;--text:#e8e8f0;--muted:#5a5a7a;
  --green:#4ade80;--red:#f87171;--cyan:#60a5fa;
}}
html{{height:100%}}
body{{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;
  height:100%;overflow:hidden;max-width:430px;margin:0 auto;
  display:flex;flex-direction:column;position:relative}}
body::before{{content:'';position:fixed;inset:0;
  background:radial-gradient(ellipse at 20% 50%,#1a0a3a22 0%,transparent 60%),
             radial-gradient(ellipse at 80% 20%,#0a1a3a22 0%,transparent 60%);
  pointer-events:none;z-index:0}}
.bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;
  object-position:center;opacity:.35;pointer-events:none;z-index:0}}
nav.topnav{{height:56px;background:var(--surface);border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;padding:0 16px;
  flex-shrink:0;z-index:10;position:relative}}
.n-brand{{font-size:.95rem;font-weight:800;letter-spacing:-.01em;
  background:linear-gradient(135deg,var(--gold) 0%,#fff 50%,var(--gold2) 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
.n-mid{{display:flex;flex-direction:column;align-items:center;gap:1px}}
.n-cap{{font-size:.8rem;font-weight:800;color:var(--text);letter-spacing:-.01em}}
.n-pnl{{font-size:.6rem;color:var(--muted)}}
.mode-pill{{display:inline-flex;align-items:center;gap:5px;padding:4px 10px;
  border-radius:999px;font-size:.65rem;font-weight:700;letter-spacing:.05em;
  border:1px solid;{pill_bg}}}
.dot{{width:6px;height:6px;border-radius:50%;background:{dot_col};
  box-shadow:0 0 6px {dot_col};animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
.scroll{{flex:1;overflow-y:auto;padding:14px 16px;display:flex;flex-direction:column;
  gap:10px;position:relative;z-index:1}}
.scroll::-webkit-scrollbar{{display:none}}
.sec-lbl{{font-size:.68rem;font-weight:700;color:var(--muted);text-transform:uppercase;
  letter-spacing:.1em;padding:2px 0 8px;display:flex;align-items:center;gap:8px}}
.count-pill{{background:var(--gold);color:#000;font-size:.6rem;font-weight:900;
  padding:2px 7px;border-radius:5px;letter-spacing:.04em}}
.pos-card{{background:var(--surface);border:1px solid var(--border);border-radius:16px;
  padding:14px;cursor:pointer;transition:transform .2s;
  display:flex;align-items:center;gap:12px;position:relative;overflow:hidden}}
.pos-card::before{{content:'';position:absolute;inset:0;
  background:linear-gradient(135deg,#ffffff04 0%,transparent 100%);pointer-events:none}}
.pos-card:hover{{transform:translateY(-1px)}}
.pos-card:active{{transform:scale(.985)}}
.pos-card.win{{border-left:3px solid var(--green)}}
.pos-card.loss{{border-left:3px solid var(--red)}}
@keyframes exitCard{{to{{opacity:0;transform:scale(.92);max-height:0;padding:0;margin:0;border:0}}}}
.pos-card.exiting{{animation:exitCard .38s ease forwards;overflow:hidden}}
.tok-icon{{width:42px;height:42px;border-radius:50%;display:flex;align-items:center;
  justify-content:center;flex-shrink:0;font-size:.75rem;font-weight:800;letter-spacing:.02em}}
.tok-icon.win{{background:#4ade8018;color:var(--green);border:1px solid #4ade8030}}
.tok-icon.loss{{background:#f8717118;color:var(--red);border:1px solid #f8717130}}
.card-body{{flex:1;min-width:0}}
.card-top{{display:flex;align-items:center;gap:7px;margin-bottom:3px}}
.card-sym{{font-size:.92rem;font-weight:800;letter-spacing:.01em;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.strat-badge{{font-size:.6rem;font-weight:700;letter-spacing:.06em;padding:2px 7px;
  border-radius:5px;text-transform:uppercase;flex-shrink:0}}
.sb-bond{{background:#60a5fa18;color:var(--cyan);border:1px solid #60a5fa30}}
.sb-trench{{background:#f5c54218;color:var(--gold);border:1px solid #f5c54230}}
.sb-spike{{background:#a855f718;color:#c084fc;border:1px solid #a855f730}}
.sb-copy{{background:#4ade8018;color:var(--green);border:1px solid #4ade8030}}
.sb-migrate{{background:#60a5fa18;color:var(--cyan);border:1px solid #60a5fa30}}
.card-meta{{font-size:.66rem;color:var(--muted);display:flex;align-items:center;gap:5px}}
.card-right{{text-align:right;flex-shrink:0}}
.card-pnl{{font-size:1.45rem;font-weight:800;line-height:1;letter-spacing:-.01em}}
.card-pct{{font-size:.64rem;font-weight:700;margin-top:2px}}
@keyframes numFlash{{0%,100%{{opacity:1}}45%{{opacity:.35;transform:scale(1.04)}}}}
.flash{{animation:numFlash .3s ease}}
.spark-c{{width:54px;height:30px;flex-shrink:0}}
.tp-badge{{position:absolute;top:8px;right:8px;font-size:.55rem;font-weight:700;
  letter-spacing:.06em;padding:2px 6px;border-radius:5px;
  background:#f5c54218;color:var(--gold);border:1px solid #f5c54230;text-transform:uppercase}}
.exit-row{{background:var(--surface);border:1px solid var(--border);border-radius:14px;
  padding:12px 14px;display:flex;justify-content:space-between;align-items:center;opacity:.5}}
.exit-sym{{font-size:.88rem;font-weight:800;letter-spacing:.01em}}
.exit-meta{{font-size:.6rem;color:var(--muted);margin-top:2px}}
.exit-pnl{{font-size:1rem;font-weight:800;text-align:right}}
.exit-pct{{font-size:.6rem;text-align:right;margin-top:2px}}
.empty-state{{text-align:center;padding:48px 20px;color:var(--muted)}}
.empty-state .e-ico{{font-size:2.4rem;margin-bottom:12px;opacity:.4}}
.empty-state p{{font-size:.78rem}}
.backdrop{{position:fixed;inset:0;background:#000000b8;z-index:40;
  opacity:0;pointer-events:none;transition:opacity .28s;backdrop-filter:blur(3px)}}
.backdrop.on{{opacity:1;pointer-events:all}}
.drawer{{position:fixed;bottom:0;left:50%;transform:translateX(-50%) translateY(104%);
  width:100%;max-width:430px;height:90vh;background:var(--surface2);
  border-radius:22px 22px 0 0;z-index:41;border-top:1px solid var(--border);
  display:flex;flex-direction:column;transition:transform .35s cubic-bezier(.25,.72,0,1)}}
.drawer.on{{transform:translateX(-50%) translateY(0)}}
.handle-wrap{{padding:10px 0 2px;display:flex;justify-content:center;flex-shrink:0;
  cursor:grab;touch-action:none}}
.handle{{width:36px;height:4px;background:#ffffff14;border-radius:2px}}
.d-scroll{{flex:1;overflow-y:auto;display:flex;flex-direction:column}}
.d-scroll::-webkit-scrollbar{{display:none}}
.d-head{{padding:4px 18px 14px;display:flex;align-items:flex-start;
  justify-content:space-between;flex-shrink:0;border-bottom:1px solid var(--border)}}
.d-sym-grp{{display:flex;align-items:center;gap:10px}}
.d-sym{{font-size:1.9rem;font-weight:800;letter-spacing:-.01em;line-height:1}}
.d-badges{{display:flex;flex-direction:column;gap:5px}}
.d-live{{display:flex;align-items:center;gap:4px;font-size:.58rem;font-weight:700;
  color:var(--green);letter-spacing:.08em;text-transform:uppercase}}
.d-right{{text-align:right}}
.d-pnl-big{{font-size:2.2rem;font-weight:800;line-height:1;letter-spacing:-.02em;transition:color .25s}}
.d-sub{{font-size:.6rem;color:var(--muted);margin-top:4px;
  display:flex;flex-direction:column;gap:2px;align-items:flex-end}}
.d-price-live{{font-weight:700;font-size:.7rem;transition:color .25s}}
.chart-wrap{{padding:10px 14px 2px;flex-shrink:0}}
.chart-axis{{display:flex;justify-content:space-between;margin-bottom:5px;
  font-size:.56rem;color:var(--muted)}}
#d-chart{{display:block;width:100%;border-radius:10px}}
.chart-legend{{display:flex;gap:14px;padding:5px 14px 0;flex-shrink:0}}
.leg{{display:flex;align-items:center;gap:5px;font-size:.55rem;color:var(--muted)}}
.leg-line{{width:14px;height:2px;border-radius:1px}}
.stats-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;
  margin:10px 14px;background:var(--border);border:1px solid var(--border);
  border-radius:12px;overflow:hidden;flex-shrink:0}}
.stat-cell{{background:var(--surface);padding:9px 5px;text-align:center}}
.stat-lbl{{font-size:.52rem;font-weight:600;color:var(--muted);letter-spacing:.08em;
  text-transform:uppercase;margin-bottom:3px}}
.stat-val{{font-size:.72rem;font-weight:700;font-variant-numeric:tabular-nums}}
.bars-section{{padding:0 14px 2px;display:flex;flex-direction:column;gap:7px;flex-shrink:0}}
.bar-row{{display:flex;align-items:center;gap:8px}}
.bar-lbl{{font-size:.55rem;font-weight:700;color:var(--muted);letter-spacing:.08em;
  text-transform:uppercase;width:30px;flex-shrink:0}}
.bar-track{{flex:1;height:6px;background:#ffffff08;border-radius:3px;overflow:hidden}}
.bar-fill{{height:6px;border-radius:3px;transition:width .5s ease}}
.bf-g{{background:linear-gradient(90deg,var(--gold2),var(--gold))}}
.bf-r{{background:var(--red)}}
.bf-c{{background:var(--cyan)}}
.bar-val{{font-size:.6rem;font-weight:700;width:38px;text-align:right;
  flex-shrink:0;font-variant-numeric:tabular-nums}}
.actions-wrap{{padding:10px 14px 6px;flex-shrink:0}}
.primary-row{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:8px}}
.act-btn{{border:none;border-radius:12px;padding:15px 6px 11px;cursor:pointer;
  display:flex;flex-direction:column;align-items:center;gap:4px;
  transition:all .13s;font-weight:700;font-family:'Inter',system-ui,sans-serif}}
.act-btn:active{{transform:scale(.94)}}
.act-btn .a-icon{{font-size:1.15rem;line-height:1}}
.act-btn .a-label{{font-size:.82rem;letter-spacing:.04em}}
.act-btn .a-hint{{font-size:.54rem;font-weight:500;opacity:.55}}
.ab-close{{background:#f8717114;color:var(--red);border:1px solid #f8717130}}
.ab-add{{background:#60a5fa12;color:var(--cyan);border:1px solid #60a5fa28}}
.ab-tp{{background:#f5c54214;color:var(--gold);border:1px solid #f5c54230}}
.act-btn.active-btn{{border-color:currentColor;box-shadow:0 0 0 1px currentColor inset}}
.panel{{display:none;border-radius:12px;padding:12px;margin-bottom:8px;
  animation:panIn .18s ease both}}
.panel.open{{display:block}}
@keyframes panIn{{from{{opacity:0;transform:translateY(-4px)}}to{{opacity:1;transform:translateY(0)}}}}
.tp-panel{{background:#f5c54208;border:1px solid #f5c54220}}
.tp-row{{display:flex;justify-content:space-between;align-items:center;
  padding:11px 12px;border-radius:10px;cursor:pointer;
  border:1px solid #ffffff09;background:#ffffff05;
  transition:background .11s;margin-bottom:7px}}
.tp-row:last-child{{margin-bottom:0}}
.tp-row:active,.tp-row:hover{{background:#f5c54210;border-color:#f5c54228}}
.tp-name{{font-size:.88rem;font-weight:700;color:var(--gold);letter-spacing:.01em}}
.tp-desc{{font-size:.58rem;color:var(--muted);margin-top:1px}}
.tp-amt{{font-size:1rem;font-weight:800;text-align:right}}
.tp-note{{font-size:.57rem;color:var(--gold);text-align:right}}
.cmp-panel{{background:#60a5fa06;border:1px solid #60a5fa18}}
.cmp-lbl{{font-size:.6rem;font-weight:700;color:var(--muted);
  letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px}}
.preset-row{{display:flex;gap:7px;margin-bottom:10px}}
.preset-btn{{background:#60a5fa0c;border:1px solid #60a5fa22;color:var(--cyan);
  font-size:.8rem;font-weight:700;padding:9px 0;border-radius:10px;
  cursor:pointer;transition:all .11s;flex:1;text-align:center}}
.preset-btn.sel{{background:#60a5fa20;border-color:var(--cyan);box-shadow:0 0 10px #60a5fa18}}
.cmp-go{{width:100%;background:linear-gradient(135deg,var(--gold),var(--gold2));
  color:#000;border:none;font-size:1rem;font-weight:800;letter-spacing:.04em;
  padding:13px;border-radius:10px;cursor:pointer;transition:all .12s;
  box-shadow:0 0 20px #f5c54230}}
.cmp-go:hover{{box-shadow:0 0 28px #f5c54250}}
.cmp-go:disabled{{opacity:.45;cursor:default}}
.cmp-note{{font-size:.58rem;color:var(--muted);margin-top:7px;text-align:center}}
.cls-panel{{background:#f8717106;border:1px solid #f8717118}}
.cls-msg{{font-size:.72rem;font-weight:600;text-align:center;margin-bottom:10px;color:var(--text)}}
.cls-row{{display:grid;grid-template-columns:1fr 1fr;gap:8px}}
.cls-cancel{{background:#ffffff08;border:1px solid var(--border);color:var(--muted);
  font-size:.95rem;font-weight:700;letter-spacing:.04em;
  padding:12px;border-radius:10px;cursor:pointer;transition:all .11s}}
.cls-confirm{{background:var(--red);border:none;color:#fff;
  font-size:.95rem;font-weight:700;letter-spacing:.04em;
  padding:12px;border-radius:10px;cursor:pointer;transition:all .11s}}
.cls-confirm:disabled{{opacity:.45;cursor:default}}
.spin{{display:inline-block;width:12px;height:12px;border:2px solid #ffffff28;
  border-top-color:#fff;border-radius:50%;animation:spinr .55s linear infinite;
  vertical-align:middle;margin-right:4px}}
@keyframes spinr{{to{{transform:rotate(360deg)}}}}
.safe{{height:max(env(safe-area-inset-bottom,0px),16px);flex-shrink:0}}
.toast{{position:fixed;bottom:96px;left:50%;transform:translateX(-50%) translateY(6px);
  opacity:0;transition:all .22s;pointer-events:none;z-index:99;
  background:var(--surface2);border-radius:10px;padding:9px 17px;
  font-size:.67rem;border:1px solid var(--border);white-space:nowrap;
  box-shadow:0 6px 28px #00000090}}
.toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}
.toast.tok-success{{border-color:var(--green);color:var(--green)}}
.toast.tok-error{{border-color:var(--red);color:var(--red)}}
.toast.tok-info{{border-color:var(--cyan);color:var(--cyan)}}
.page-nav{{display:flex;gap:6px;padding:8px 14px;overflow-x:auto;flex-shrink:0;
  border-bottom:1px solid var(--border);position:relative;z-index:1;background:var(--surface)}}
.page-nav::-webkit-scrollbar{{display:none}}
.pn-link{{color:var(--muted);font-size:.7rem;font-weight:600;text-decoration:none;
  padding:5px 12px;border-radius:8px;border:1px solid var(--border);
  background:var(--surface2);white-space:nowrap;transition:all .18s;flex-shrink:0}}
.pn-link:active,.pn-link:hover{{color:var(--gold);border-color:#f5c54244;background:#f5c54210}}
.pn-link.cur{{color:var(--gold);border-color:#f5c54244;background:#f5c54214;font-weight:700}}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<nav class="topnav">
  <div class="n-brand">Boogey's Sniper</div>
  <div class="n-mid">
    <div class="n-cap" id="nav-cap">—</div>
    <div class="n-pnl"><span id="nav-open">0</span> open · <span id="nav-pnl">+$0.00</span></div>
  </div>
  <div class="mode-pill"><span class="dot"></span>{mode}</div>
</nav>
<div class="page-nav">
  <a href="/" class="pn-link">🏠 Home</a>
  <a href="/live" class="pn-link">⚡ Live</a>
  <a href="/positions" class="pn-link cur">📍 Positions</a>
  <a href="/trades" class="pn-link">📋 Trades</a>
  <a href="/status" class="pn-link">📊 Status</a>
  <a href="/chart" class="pn-link">📈 Chart</a>
  <a href="/learn" class="pn-link">🧠 Strategy</a>
  <a href="/setup" class="pn-link">⚙️ Setup</a>
</div>
<div class="scroll" id="main-scroll">
  <div class="sec-lbl">OPEN POSITIONS <span class="count-pill" id="open-count">0</span></div>
  <div id="positions-list"></div>
  <div class="sec-lbl" style="margin-top:6px">RECENT EXITS</div>
  <div id="exits-list"></div>
</div>
<div class="backdrop" id="backdrop"></div>
<div class="drawer" id="drawer">
  <div class="handle-wrap" id="handle-wrap"><div class="handle"></div></div>
  <div class="d-scroll">
    <div class="d-head">
      <div class="d-sym-grp">
        <div class="d-sym" id="d-sym">—</div>
        <div class="d-badges">
          <span class="strat-badge sb-bond" id="d-badge">bond</span>
          <div class="d-live"><span class="dot"></span>LIVE</div>
        </div>
      </div>
      <div class="d-right">
        <div class="d-pnl-big" id="d-pnl">—</div>
        <div class="d-sub">
          <span id="d-hold" style="color:var(--muted)">0m 0s</span>
          <span class="d-price-live" id="d-price-live">—</span>
        </div>
      </div>
    </div>
    <div class="chart-wrap">
      <div class="chart-axis">
        <span id="d-ax-l">3m ago</span>
        <span id="d-entry-lbl">entry</span>
        <span>now</span>
      </div>
      <canvas id="d-chart" height="148"></canvas>
    </div>
    <div class="chart-legend">
      <div class="leg"><div class="leg-line" style="border-top:2px dashed #ffffff28;background:transparent"></div>ENTRY</div>
      <div class="leg"><div class="leg-line" style="background:#ff335550"></div>SL</div>
      <div class="leg"><div class="leg-line" style="background:#00ff8850"></div>TP1</div>
    </div>
    <div class="stats-grid">
      <div class="stat-cell"><div class="stat-lbl">Entry</div><div class="stat-val" id="d-entry-v">—</div></div>
      <div class="stat-cell"><div class="stat-lbl">Current</div><div class="stat-val" id="d-cur-v" style="color:var(--green)">—</div></div>
      <div class="stat-cell"><div class="stat-lbl">PnL %</div><div class="stat-val" id="d-pct-v">—</div></div>
      <div class="stat-cell"><div class="stat-lbl">Size</div><div class="stat-val" id="d-size-v">—</div></div>
    </div>
    <div class="bars-section">
      <div class="bar-row">
        <span class="bar-lbl">Bond</span>
        <div class="bar-track"><div class="bar-fill bf-g" id="d-bond-fill" style="width:0%"></div></div>
        <span class="bar-val" id="d-bond-val" style="color:var(--gold)">—</span>
      </div>
      <div class="bar-row">
        <span class="bar-lbl">SL</span>
        <div class="bar-track"><div class="bar-fill bf-r" id="d-sl-fill" style="width:0%"></div></div>
        <span class="bar-val" id="d-sl-val" style="color:var(--red)">—</span>
      </div>
      <div class="bar-row">
        <span class="bar-lbl">TP1</span>
        <div class="bar-track"><div class="bar-fill bf-c" id="d-tp1-fill" style="width:0%"></div></div>
        <span class="bar-val" id="d-tp1-val" style="color:var(--cyan)">—</span>
      </div>
    </div>
    <div class="actions-wrap">
      <div class="panel tp-panel" id="panel-tp">
        <div class="tp-row" id="tp-row-1" onclick="doTP('tp1')">
          <div><div class="tp-name">TP1 — LOCK 40%</div><div class="tp-desc">sell 40% at current price</div></div>
          <div><div class="tp-amt" id="tp1-amt">—</div><div class="tp-note">returned to capital</div></div>
        </div>
        <div class="tp-row" id="tp-row-2" onclick="doTP('tp2')">
          <div><div class="tp-name">TP2 — LOCK 40% REM.</div><div class="tp-desc">sell 40% of remaining tokens</div></div>
          <div><div class="tp-amt" id="tp2-amt">—</div><div class="tp-note">returned to capital</div></div>
        </div>
        <div class="tp-row" onclick="doTP('full')">
          <div><div class="tp-name">FULL EXIT</div><div class="tp-desc">close entire remaining position</div></div>
          <div><div class="tp-amt" id="full-amt">—</div><div class="tp-note" id="full-pnl" style="color:var(--green)">—</div></div>
        </div>
      </div>
      <div class="panel cmp-panel" id="panel-cmp">
        <div class="cmp-lbl">Add to Position</div>
        <div class="preset-row">
          <button class="preset-btn sel" id="prs5"  onclick="setPreset(5)">$5</button>
          <button class="preset-btn"     id="prs10" onclick="setPreset(10)">$10</button>
          <button class="preset-btn"     id="prs15" onclick="setPreset(15)">$15</button>
          <button class="preset-btn"     id="prs25" onclick="setPreset(25)">$25</button>
        </div>
        <button class="cmp-go" id="cmp-go" onclick="doCompound()">COMPOUND $5</button>
        <div class="cmp-note">⚠ Increases risk · averages entry price</div>
      </div>
      <div class="panel cls-panel" id="panel-cls">
        <div class="cls-msg" id="cls-msg">Close at market? Return: <span id="cls-return" style="color:var(--green)">—</span></div>
        <div class="cls-row">
          <button class="cls-cancel" onclick="closePanel()">Cancel</button>
          <button class="cls-confirm" id="cls-confirm-btn" onclick="doClose()">CLOSE NOW</button>
        </div>
      </div>
      <div class="primary-row">
        <button class="act-btn ab-close" id="btn-close" onclick="togglePanel('close')">
          <span class="a-icon">✕</span><span class="a-label">CLOSE</span><span class="a-hint">market sell</span>
        </button>
        <button class="act-btn ab-add" id="btn-add" onclick="togglePanel('compound')">
          <span class="a-icon">⊕</span><span class="a-label">ADD</span><span class="a-hint">compound</span>
        </button>
        <button class="act-btn ab-tp" id="btn-tp" onclick="togglePanel('tp')">
          <span class="a-icon">◈</span><span class="a-label">TAKE</span><span class="a-hint">profit</span>
        </button>
      </div>
    </div>
    <div class="safe"></div>
  </div>
</div>
<div class="toast" id="toast"></div>
<script>
const TICK_MS  = 3000;
const HIST_LEN = 20;
let POSITIONS  = {{}};
let capital    = 0;
let activeId   = null;
let openPanel  = null;
let selPreset  = 5;
let toastTimer = null;
let tickTimer  = null;

function fmtPrice(v){{
  if(!v||v===0) return '$0';
  let s=v.toFixed(8);
  return '$'+s.replace(/(\\.\\.d*?[1-9])0+$/,'$1').replace(/\\.$/,'');
}}
// Fix: proper trailing-zero strip for fmtPrice
function fmtP(v){{
  if(!v||v===0) return '$0';
  const s=v.toFixed(8);
  let i=s.length-1;
  while(i>s.indexOf('.')+2&&s[i]==='0') i--;
  return '$'+s.slice(0,i+1);
}}
function fmtUSD(v){{return '$'+Math.abs(v).toFixed(2);}}
function sign(v){{return v>=0?'+':'−';}}
function col(v){{return v>=0?'var(--green)':'var(--red)';}}
function holdStr(openedAt){{
  const s=Math.floor((Date.now()/1000-openedAt));
  return Math.floor(s/60)+'m '+s%60+'s';
}}
function calcPnL(p){{
  const cur=p.price*p.remFrac*(p.amount/p.entry);
  return p.partialProceeds+cur-p.amount;
}}
function calcPct(p){{return ((p.price-p.entry)/p.entry)*100;}}

// ── FETCH & BUILD STATE ──────────────────────────────────────
async function fetchPositions(){{
  try{{
    const r=await fetch('/positions/api');
    const d=await r.json();
    capital=d.capital;
    // merge: update prices from server; keep browser history
    const newIds=new Set(d.positions.map(p=>p.mint));
    // remove closed positions
    for(const id of Object.keys(POSITIONS)){{
      if(!newIds.has(id)){{ delete POSITIONS[id]; }}
    }}
    d.positions.forEach(p=>{{
      if(POSITIONS[p.mint]){{
        // update server truth; keep browser history and vel
        POSITIONS[p.mint].price=p.price;
        POSITIONS[p.mint].amount=p.amount;
        POSITIONS[p.mint].entry=p.entry;
        POSITIONS[p.mint].partialProceeds=p.partial_proceeds;
        POSITIONS[p.mint].tpDone=p.partial_tp_done;
        POSITIONS[p.mint].remFrac=p.tokens>0?p.tokens/(p.amount/p.entry):0;
        POSITIONS[p.mint].bond=p.bond_high;
        POSITIONS[p.mint].bondEntry=p.bond_entry;
      }} else {{
        // new position
        const remFrac=p.tokens>0?p.tokens/(p.amount/p.entry):1;
        const pos={{
          id:p.mint,sym:p.symbol,strat:p.strategy,stratClass:stratClass(p.strategy),
          entry:p.entry,price:p.price,vel:(p.price-p.entry)*0.01,
          amount:p.amount,partialProceeds:p.partial_proceeds,
          tpDone:p.partial_tp_done,remFrac:remFrac,
          bond:p.bond_high,bondEntry:p.bond_entry,
          openedAt:p.opened_at,slPct:p.sl_pct,tp1Pct:p.tp1_pct,
          history:[],tpBadge:null
        }};
        // seed history from entry
        let px=p.entry;
        const drift=(p.price-p.entry)/Math.max(HIST_LEN,1);
        for(let i=0;i<HIST_LEN;i++){{
          px+=drift+(Math.random()-.5)*p.entry*.006;
          px=Math.max(px,p.entry*.3);
          pos.history.push(px);
        }}
        pos.price=pos.history[pos.history.length-1];
        POSITIONS[p.mint]=pos;
      }}
    }});
    buildCards();
    updateNav();
  }} catch(e){{console.error('fetch error',e);}}
}}

function stratClass(s){{
  if(s==='bond'||s==='migrate') return 'bond';
  if(s==='trench') return 'trench';
  if(s==='spike'||s==='bundle') return 'spike';
  return 'copy';
}}

// ── TICK ──────────────────────────────────────────────────
function tick(){{
  Object.values(POSITIONS).forEach(p=>{{
    p.vel+=(Math.random()-.5)*p.entry*.003;
    p.vel*=.92;
    p.vel=Math.max(-p.entry*.03,Math.min(p.entry*.03,p.vel));
    let nx=p.price+p.vel+(Math.random()-.49)*p.entry*.008;
    nx=Math.max(nx,p.entry*.3);
    p.price=nx;
    p.history.push(nx);
    if(p.history.length>HIST_LEN) p.history.shift();
    p.bond+=(Math.random()-.35)*.4;
    p.bond=Math.max(p.bondEntry,Math.min(99.9,p.bond));
  }});
  refreshCards();
  updateNav();
  if(activeId&&POSITIONS[activeId]) refreshDrawer(false);
}}

// ── CARDS ──────────────────────────────────────────────────
function buildCards(){{
  const list=document.getElementById('positions-list');
  list.querySelectorAll('[data-pos]').forEach(el=>{{
    if(!POSITIONS[el.dataset.pos]) el.remove();
  }});
  const ids=Object.keys(POSITIONS);
  if(ids.length===0){{
    if(!list.querySelector('.empty-state')){{
      list.innerHTML='<div class="empty-state"><div class="e-ico">📭</div><p>No open positions</p></div>';
    }}
    document.getElementById('open-count').textContent='0';
    document.getElementById('nav-open').textContent='0';
    return;
  }}
  const empty=list.querySelector('.empty-state');
  if(empty) empty.remove();
  ids.forEach(id=>{{
    if(list.querySelector(`[data-pos="${{id}}"]`)) return;
    const p=POSITIONS[id];
    const div=document.createElement('div');
    div.className='pos-card';
    div.dataset.pos=id;
    div.addEventListener('click',()=>openDrawer(id));
    div.innerHTML=`
      <div class="tok-icon" id="icon-${{id}}">${{p.sym.slice(0,4)}}</div>
      <div class="card-body">
        <div class="card-top">
          <span class="card-sym">${{p.sym}}</span>
          <span class="strat-badge sb-${{p.stratClass}}" id="badge-${{id}}">${{p.strat}}</span>
        </div>
        <div class="card-meta">
          <span id="c-sz-${{id}}">$${{p.amount.toFixed(2)}}</span>
          <span style="opacity:.3">·</span>
          <span id="c-hold-${{id}}">—</span>
          <span style="opacity:.3">·</span>
          <span id="c-bond-${{id}}">bond —%</span>
        </div>
      </div>
      <canvas class="spark-c" id="spark-${{id}}"></canvas>
      <div class="card-right">
        <div class="card-pnl" id="c-pnl-${{id}}">—</div>
        <div class="card-pct" id="c-pct-${{id}}">—</div>
      </div>`;
    list.appendChild(div);
  }});
  refreshCards();
  document.getElementById('open-count').textContent=ids.length;
  document.getElementById('nav-open').textContent=ids.length;
}}

function refreshCards(){{
  Object.values(POSITIONS).forEach(p=>{{
    const pnlEl=document.getElementById('c-pnl-'+p.id);
    if(!pnlEl) return;
    const pnl=calcPnL(p),pct=calcPct(p),win=pnl>=0;
    const newT=sign(pnl)+fmtUSD(pnl);
    if(pnlEl.textContent!==newT){{
      pnlEl.classList.remove('flash');void pnlEl.offsetWidth;pnlEl.classList.add('flash');
    }}
    pnlEl.textContent=newT;pnlEl.style.color=col(pnl);
    const pctEl=document.getElementById('c-pct-'+p.id);
    pctEl.textContent=sign(pct)+pct.toFixed(1)+'%';
    pctEl.style.color=win?'#00ff8877':'#ff335577';
    const card=document.querySelector(`[data-pos="${{p.id}}"]`);
    if(card) card.className='pos-card '+(win?'win':'loss');
    const icon=document.getElementById('icon-'+p.id);
    if(icon) icon.className='tok-icon '+(win?'win':'loss');
    document.getElementById('c-hold-'+p.id).textContent=holdStr(p.openedAt);
    document.getElementById('c-bond-'+p.id).textContent='bond '+p.bond.toFixed(0)+'%';
    drawSpark(p.id);
  }});
}}

function updateNav(){{
  document.getElementById('nav-cap').textContent='$'+capital.toFixed(2);
  let pnl=0;
  Object.values(POSITIONS).forEach(p=>pnl+=calcPnL(p));
  const el=document.getElementById('nav-pnl');
  el.textContent=sign(pnl)+fmtUSD(pnl);
  el.style.color=col(pnl);
}}

// ── SPARKLINE ──────────────────────────────────────────────
function drawSpark(id){{
  const c=document.getElementById('spark-'+id);
  if(!c) return;
  const p=POSITIONS[id];
  const dpr=window.devicePixelRatio||1;
  const W=52,H=28;
  c.width=W*dpr;c.height=H*dpr;
  c.style.width=W+'px';c.style.height=H+'px';
  const ctx=c.getContext('2d');
  ctx.scale(dpr,dpr);
  const pts=p.history.slice(-20);
  const mn=Math.min(...pts),mx=Math.max(...pts),rng=mx-mn||1;
  const x=i=>2+(i/(pts.length-1))*(W-4);
  const y=v=>H-2-((v-mn)/rng)*(H-4);
  const win=calcPnL(p)>=0,hue=win?'#00ff88':'#ff3355';
  ctx.beginPath();ctx.moveTo(x(0),y(pts[0]));
  pts.forEach((v,i)=>{{if(i)ctx.lineTo(x(i),y(v));}});
  ctx.lineTo(x(pts.length-1),H);ctx.lineTo(x(0),H);ctx.closePath();
  const g=ctx.createLinearGradient(0,0,0,H);
  g.addColorStop(0,hue+'44');g.addColorStop(1,hue+'00');
  ctx.fillStyle=g;ctx.fill();
  ctx.beginPath();ctx.moveTo(x(0),y(pts[0]));
  pts.forEach((v,i)=>{{if(i)ctx.lineTo(x(i),y(v));}});
  ctx.strokeStyle=hue;ctx.lineWidth=1.5;ctx.stroke();
  ctx.beginPath();ctx.arc(x(pts.length-1),y(pts[pts.length-1]),2.5,0,Math.PI*2);
  ctx.fillStyle=hue;ctx.shadowColor=hue;ctx.shadowBlur=6;ctx.fill();ctx.shadowBlur=0;
}}

// ── DRAWER ────────────────────────────────────────────────
function openDrawer(id){{
  activeId=id;closePanel();
  fillDrawer(id);
  document.getElementById('backdrop').classList.add('on');
  document.getElementById('drawer').classList.add('on');
}}
function closeDrawer(){{
  activeId=null;
  document.getElementById('backdrop').classList.remove('on');
  document.getElementById('drawer').classList.remove('on');
  closePanel();
}}
document.getElementById('backdrop').addEventListener('click',closeDrawer);

function fillDrawer(id){{
  const p=POSITIONS[id];
  document.getElementById('d-sym').textContent=p.sym;
  const badge=document.getElementById('d-badge');
  badge.textContent=p.strat;badge.className='strat-badge sb-'+p.stratClass;
  refreshDrawer(true);
  setTimeout(()=>drawChart(id,true),80);
}}

function refreshDrawer(full){{
  if(!activeId||!POSITIONS[activeId]) return;
  const p=POSITIONS[activeId];
  const pnl=calcPnL(p),pct=calcPct(p),win=pnl>=0;
  const pnlEl=document.getElementById('d-pnl');
  pnlEl.textContent=sign(pnl)+fmtUSD(pnl);pnlEl.style.color=col(pnl);
  document.getElementById('d-hold').textContent=holdStr(p.openedAt);
  const prEl=document.getElementById('d-price-live');
  prEl.textContent=fmtP(p.price);prEl.style.color=col(pnl);
  const curEl=document.getElementById('d-cur-v');
  curEl.textContent=fmtP(p.price);curEl.style.color=col(pnl);
  const pctEl=document.getElementById('d-pct-v');
  pctEl.textContent=sign(pct)+pct.toFixed(2)+'%';pctEl.style.color=col(pnl);
  const slP=p.entry*(1-p.slPct/100),tp1P=p.entry*(1+p.tp1Pct/100);
  const prng=tp1P-slP;
  const slProg=Math.max(0,Math.min(100,((p.price-slP)/prng)*100));
  const tp1Prog=Math.max(0,Math.min(100,((p.price-p.entry)/(tp1P-p.entry))*100));
  document.getElementById('d-bond-fill').style.width=p.bond+'%';
  document.getElementById('d-bond-val').textContent=p.bond.toFixed(0)+'%';
  document.getElementById('d-sl-fill').style.width=slProg+'%';
  document.getElementById('d-sl-val').textContent=p.slPct+'%';
  document.getElementById('d-tp1-fill').style.width=tp1Prog+'%';
  document.getElementById('d-tp1-val').textContent=p.tp1Pct+'%';
  if(full){{
    document.getElementById('d-entry-v').textContent=fmtP(p.entry);
    document.getElementById('d-size-v').textContent='$'+p.amount.toFixed(2);
  }}
  const tp1R=p.amount*0.4*(p.price/p.entry);
  const tp2R=p.amount*p.remFrac*0.4*(p.price/p.entry);
  const fullR=p.partialProceeds+p.price*p.remFrac*(p.amount/p.entry);
  document.getElementById('tp1-amt').textContent=fmtUSD(tp1R);
  document.getElementById('tp2-amt').textContent=fmtUSD(tp2R);
  document.getElementById('full-amt').textContent=fmtUSD(fullR);
  const fpEl=document.getElementById('full-pnl');
  fpEl.textContent=sign(pnl)+fmtUSD(pnl);fpEl.style.color=col(pnl);
  document.getElementById('cls-return').textContent=fmtUSD(fullR);
  document.getElementById('cls-return').style.color=col(pnl);
  const r1=document.getElementById('tp-row-1');
  const r2=document.getElementById('tp-row-2');
  if(r1)r1.style.opacity=p.tpDone>=1?'.35':'1';
  if(r2)r2.style.opacity=p.tpDone>=2?'.35':'1';
  if(!full)drawChart(activeId);
}}

// ── CHART ─────────────────────────────────────────────────
function drawChart(id,animate){{
  const p=POSITIONS[id];if(!p)return;
  const canvas=document.getElementById('d-chart');if(!canvas)return;
  const parent=canvas.parentElement;
  const dpr=window.devicePixelRatio||1;
  const W=parent.offsetWidth-28,H=148;
  canvas.width=W*dpr;canvas.height=H*dpr;
  canvas.style.width=W+'px';canvas.style.height=H+'px';
  const ctx=canvas.getContext('2d');
  ctx.scale(dpr,dpr);
  const pts=p.history;
  const slP=p.entry*(1-p.slPct/100),tp1P=p.entry*(1+p.tp1Pct/100);
  const allV=[...pts,slP,tp1P];
  const mn=Math.min(...allV)*.997,mx=Math.max(...allV)*1.003,rng=mx-mn;
  const pl=4,pr=4,pt=10,pb=10,fw=W-pl-pr,fh=H-pt-pb;
  const xx=i=>pl+(i/(pts.length-1))*fw;
  const yy=v=>pt+fh-((v-mn)/rng)*fh;
  const win=calcPnL(p)>=0,hue=win?'#00ff88':'#ff3355';
  const ey=yy(p.entry);
  document.getElementById('d-ax-l').textContent=Math.round(HIST_LEN*TICK_MS/60000)+'m ago';
  document.getElementById('d-entry-lbl').textContent='entry '+fmtP(p.entry);

  function _render(prog){{
    ctx.clearRect(0,0,W,H);
    // grid lines
    ctx.strokeStyle='#ffffff05';ctx.lineWidth=1;
    for(let i=0;i<=3;i++){{const gy=pt+(fh/3)*i;ctx.beginPath();ctx.moveTo(pl,gy);ctx.lineTo(W-pr,gy);ctx.stroke();}}
    // entry dashed
    ctx.setLineDash([4,4]);ctx.strokeStyle='#ffffff22';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(pl,ey);ctx.lineTo(W-pr,ey);ctx.stroke();ctx.setLineDash([]);
    ctx.font='500 9px monospace';ctx.fillStyle='#ffffff25';ctx.fillText('ENTRY',pl+4,ey-3);
    // SL / TP lines
    ctx.setLineDash([3,5]);ctx.strokeStyle='#ff335540';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(pl,yy(slP));ctx.lineTo(W-pr,yy(slP));ctx.stroke();
    ctx.strokeStyle='#00ff8840';
    ctx.beginPath();ctx.moveTo(pl,yy(tp1P));ctx.lineTo(W-pr,yy(tp1P));ctx.stroke();
    ctx.setLineDash([]);
    // clip to animated progress
    const clipW=pl+fw*prog;
    ctx.save();ctx.beginPath();ctx.rect(0,0,clipW,H);ctx.clip();
    // fill under curve
    ctx.beginPath();ctx.moveTo(xx(0),yy(pts[0]));
    pts.forEach((v,i)=>{{if(i)ctx.lineTo(xx(i),yy(v));}});
    ctx.lineTo(xx(pts.length-1),ey);ctx.lineTo(xx(0),ey);ctx.closePath();
    const g=ctx.createLinearGradient(0,pt,0,H-pb);
    if(win){{g.addColorStop(0,'#00ff8825');g.addColorStop(1,'#00ff8804');}}
    else{{g.addColorStop(0,'#ff335506');g.addColorStop(1,'#ff335525');}}
    ctx.fillStyle=g;ctx.fill();
    // price curve
    ctx.beginPath();ctx.moveTo(xx(0),yy(pts[0]));
    for(let i=1;i<pts.length;i++){{const cpx=xx(i-.5);ctx.bezierCurveTo(cpx,yy(pts[i-1]),cpx,yy(pts[i]),xx(i),yy(pts[i]));}}
    ctx.strokeStyle=hue;ctx.lineWidth=2;ctx.lineJoin='round';ctx.stroke();
    ctx.restore();
    // live dot at clip edge
    if(prog>=1){{
      const lx=xx(pts.length-1),ly=yy(pts[pts.length-1]);
      ctx.beginPath();ctx.arc(lx,ly,4,0,Math.PI*2);
      ctx.fillStyle=hue;ctx.shadowColor=hue;ctx.shadowBlur=12;ctx.fill();ctx.shadowBlur=0;
    }}
  }}

  if(animate){{
    let start=null;
    const dur=520;
    function step(ts){{
      if(!start)start=ts;
      const prog=Math.min((ts-start)/dur,1);
      // ease-out cubic
      _render(1-(1-prog)*(1-prog)*(1-prog));
      if(prog<1)requestAnimationFrame(step);
    }}
    requestAnimationFrame(step);
  }}else{{
    _render(1);
  }}
}}

// ── PANELS ────────────────────────────────────────────────
function togglePanel(name){{
  if(openPanel===name){{closePanel();return;}}
  closePanel();openPanel=name;
  if(name==='tp')      {{document.getElementById('panel-tp').classList.add('open'); document.getElementById('btn-tp').classList.add('active-btn');}}
  if(name==='compound'){{document.getElementById('panel-cmp').classList.add('open');document.getElementById('btn-add').classList.add('active-btn');}}
  if(name==='close')   {{document.getElementById('panel-cls').classList.add('open');document.getElementById('btn-close').classList.add('active-btn');}}
}}
function closePanel(){{
  ['panel-tp','panel-cmp','panel-cls'].forEach(id=>document.getElementById(id).classList.remove('open'));
  ['btn-tp','btn-add','btn-close'].forEach(id=>document.getElementById(id).classList.remove('active-btn'));
  openPanel=null;
}}
function setPreset(v){{
  selPreset=v;
  [5,10,15,25].forEach(n=>document.getElementById('prs'+n).classList.toggle('sel',n===v));
  document.getElementById('cmp-go').textContent='COMPOUND $'+v;
}}

// ── ACTIONS ───────────────────────────────────────────────
async function doTP(type){{
  const p=POSITIONS[activeId];if(!p)return;
  let fraction=0.40,label='';
  if(type==='tp1'){{
    if(p.tpDone>=1){{toast('TP1 already locked','tok-error');return;}}
    fraction=0.40;label='TP1 — 40% locked';
  }} else if(type==='tp2'){{
    if(p.tpDone>=2){{toast('TP2 already locked','tok-error');return;}}
    fraction=0.40;label='TP2 — 40% of remaining locked';
  }} else {{
    // full exit
    try{{
      const r=await fetch('/position/'+activeId+'/close',{{method:'POST'}});
      const d=await r.json();
      if(d.ok){{toast('Position closed','tok-success');exitPosition(activeId);await fetchPositions();}}
      else toast(d.error||'Error','tok-error');
    }}catch(e){{toast('Network error','tok-error');}}
    return;
  }}
  try{{
    const r=await fetch('/position/'+activeId+'/tp',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{fraction}})}});
    const d=await r.json();
    if(d.ok){{
      toast(label,'tok-success');
      closePanel();
      await fetchPositions();
      if(activeId&&POSITIONS[activeId])refreshDrawer(true);
    }} else toast(d.error||'Error','tok-error');
  }}catch(e){{toast('Network error','tok-error');}}
}}

async function doClose(){{
  const p=POSITIONS[activeId];if(!p)return;
  const btn=document.getElementById('cls-confirm-btn');
  btn.disabled=true;btn.innerHTML='<span class="spin"></span>CLOSING';
  try{{
    const r=await fetch('/position/'+activeId+'/close',{{method:'POST'}});
    const d=await r.json();
    if(d.ok){{
      toast('Position closed','tok-success');
      exitPosition(activeId);
      await fetchPositions();
    }} else {{toast(d.error||'Error','tok-error');btn.disabled=false;btn.textContent='CLOSE NOW';}}
  }}catch(e){{toast('Network error','tok-error');btn.disabled=false;btn.textContent='CLOSE NOW';}}
}}

async function doCompound(){{
  const p=POSITIONS[activeId];if(!p)return;
  const btn=document.getElementById('cmp-go');
  btn.disabled=true;btn.textContent='Adding...';
  try{{
    const r=await fetch('/position/'+activeId+'/compound',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{amount:selPreset}})}});
    const d=await r.json();
    if(d.ok){{
      toast('Compounded +$'+selPreset,'tok-info');
      closePanel();
      await fetchPositions();
      if(activeId&&POSITIONS[activeId])refreshDrawer(true);
    }} else toast(d.error||'Error','tok-error');
  }}catch(e){{toast('Network error','tok-error');}}
  btn.disabled=false;btn.textContent='COMPOUND $'+selPreset;
}}

function exitPosition(id){{
  const card=document.querySelector(`[data-pos="${{id}}"]`);
  if(card)card.classList.add('exiting');
  setTimeout(()=>{{delete POSITIONS[id];buildCards();updateNav();}},380);
  closeDrawer();
}}

// ── TOAST ─────────────────────────────────────────────────
function toast(msg,type='tok-info'){{
  const el=document.getElementById('toast');
  el.textContent=msg;el.className='toast '+type+' show';
  clearTimeout(toastTimer);
  toastTimer=setTimeout(()=>el.classList.remove('show'),2800);
}}

// ── DRAG TO CLOSE ─────────────────────────────────────────
let dragY0=0,dragging=false;
const hw=document.getElementById('handle-wrap');
const dr=document.getElementById('drawer');
hw.addEventListener('touchstart',e=>{{dragY0=e.touches[0].clientY;dragging=true;}},{{passive:true}});
document.addEventListener('touchmove',e=>{{
  if(!dragging)return;
  const dy=e.touches[0].clientY-dragY0;
  if(dy>0)dr.style.transform=`translateX(-50%) translateY(${{dy}}px)`;
}},{{passive:true}});
document.addEventListener('touchend',e=>{{
  if(!dragging)return;
  dragging=false;
  const dy=e.changedTouches[0].clientY-dragY0;
  dr.style.transform='';
  if(dy>80)closeDrawer();
}});

// ── EXITS LIST ─────────────────────────────────────────────
function buildExits(exits){{
  const list=document.getElementById('exits-list');
  if(!exits||exits.length===0){{list.innerHTML='<div style="font-size:.62rem;color:var(--muted);padding:8px 0">No exits yet this session</div>';return;}}
  list.innerHTML=exits.slice(0,5).map(t=>{{
    const win=t.pnl>=0;
    const s=win?'+':'';
    const ago=Math.round((Date.now()/1000-t.closed_ts)/60);
    return `<div class="exit-row">
      <div><div class="exit-sym">${{t.symbol}}</div>
      <div class="exit-meta">${{t.result}} · ${{t.strategy}} · ${{ago}}m ago</div></div>
      <div><div class="exit-pnl" style="color:${{win?'var(--green)':'var(--red)'}}">${{win?'+':'−'}}$${{Math.abs(t.pnl).toFixed(2)}}</div>
      <div class="exit-pct" style="color:${{win?'#00ff8866':'#ff335566'}}">${{s}}${{t.pnl_pct.toFixed(1)}}%</div></div>
    </div>`;
  }}).join('');
}}

// ── INIT ──────────────────────────────────────────────────
async function init(){{
  await fetchPositions();
  // also fetch recent exits from /trades/api
  try{{
    const r=await fetch('/trades/api');
    const d=await r.json();
    buildExits(d.completed?d.completed.slice(-5).reverse():[]);
  }}catch(e){{}}
  tickTimer=setInterval(tick,TICK_MS);
  // re-sync with server every 10s
  setInterval(fetchPositions,10000);
}}
init();
</script>
</body>
</html>"""
    return html, 200


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
    _load_pause()
    # Restore webhook ID so we update rather than create a duplicate on restart
    _saved_wh_id = redis_load("helius_wh_id")
    if _saved_wh_id:
        with _helius_wh_lock:
            _helius_wh_id = _saved_wh_id
    if not REDIS_URL or not REDIS_TOKEN:
        log("warn", "⚠️  No Redis configured — capital saved to /tmp only (wiped on redeploy). "
            "Set UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN in Railway for safe persistence.")

    threading.Thread(target=_notify_worker, daemon=True).start()

    if BOT_PAUSED:
        log("warn", "=" * 55)
        log("warn", "BOT IS PAUSED — set BOT_PAUSED=false in Railway to resume")
        log("warn", "UI is live. Zero trading activity.")
        log("warn", "=" * 55)
    else:
        threading.Thread(target=monitor_loop,       daemon=True).start()
        threading.Thread(target=scanner_loop,       daemon=True).start()
        threading.Thread(target=daily_summary_loop, daemon=True).start()
        if COPY_TRADE:
            threading.Thread(target=copy_trade_loop, daemon=True).start()
            threading.Thread(target=_register_helius_webhook, daemon=True).start()
        t_signals = threading.Thread(target=run_signal_refresh_loop, daemon=True)
        t_signals.start()
        threading.Thread(target=run_dsc_refresh_loop, daemon=True).start()
        threading.Thread(target=run_jup_refresh_loop, daemon=True).start()
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
