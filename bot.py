import os, time, threading, requests, json, re, csv, io, random
from flask import Flask, jsonify, Response, request as flask_request
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

# Position sizing — capital-tiered (protects small accounts)
MIN_TRADE         = float(os.environ.get("MIN_TRADE",   "3"))
MAX_TRADE         = float(os.environ.get("MAX_TRADE",   "500"))
FIXED_TRADE_SIZE  = float(os.environ.get("FIXED_TRADE_SIZE", "0"))  # 0 = use tiered %

# Capital tiers per risk level: (min_capital, trade_pct, daily_max_trades)
_RISK_TIERS = {
    "conservative": [(5_000,0.12,60),(500,0.10,50),(100,0.08,40),(0,0.05,30)],
    "standard":     [(5_000,0.18,80),(500,0.15,70),(100,0.12,60),(0,0.08,50)],
    "aggressive":   [(5_000,0.22,100),(500,0.18,90),(100,0.15,75),(0,0.12,60)],
}
_CAP_TIERS = _RISK_TIERS.get(RISK_LEVEL, _RISK_TIERS["standard"])

MAX_DAILY_LOSS_PCT = float(os.environ.get("MAX_DAILY_LOSS_PCT", "20"))  # stop day if down >20% of start capital

# Risk limits
DAILY_TRADE_MIN   = int(os.environ.get("DAILY_TRADE_MIN",  "3"))   # minimum trades before stop rules apply
DAILY_WIN_TARGET  = int(os.environ.get("DAILY_WIN_TARGET", "3"))   # once wins >= this, allow up to DAILY_LOSS_MAX
DAILY_LOSS_SOFT   = int(os.environ.get("DAILY_LOSS_SOFT",  "1"))   # default: stop after 1 loss (2W 1L target)
DAILY_LOSS_MAX    = int(os.environ.get("DAILY_LOSS_MAX",   "3"))   # hard cap: always stop at 3 losses
LOSS_COOLDOWN_HRS = float(os.environ.get("LOSS_COOLDOWN_HRS", "0.5")) # kept for retune timing
ANALYZE_EVERY     = int(os.environ.get("ANALYZE_EVERY",   "5"))   # retune every 5 trades for faster learning

# Bond Runner strategy
BOND_ENTRY_MIN  = float(os.environ.get("BOND_ENTRY_MIN", "25"))
BOND_ENTRY_MAX  = float(os.environ.get("BOND_ENTRY_MAX", "75"))
BOND_TP         = float(os.environ.get("BOND_TP",        "67"))
BOND_SL_PCT     = float(os.environ.get("BOND_SL_PCT",    "10"))
BOND_MAX_SECS   = int(os.environ.get("BOND_MAX_SECS",    "240"))   # 4 min hard cap
BOND_STALE_SECS = int(os.environ.get("BOND_STALE_SECS",  "120"))   # exit if bond hasn't moved in 2 min

# Dormant Spike strategy
SPIKE_MIN_AGE_H = float(os.environ.get("SPIKE_MIN_AGE_H", "12"))
SPIKE_MIN_1H    = float(os.environ.get("SPIKE_MIN_1H",    "20"))
SPIKE_TP_PCT    = float(os.environ.get("SPIKE_TP_PCT",    "40"))
SPIKE_SL_PCT    = float(os.environ.get("SPIKE_SL_PCT",    "15"))
SPIKE_MAX_SECS  = int(os.environ.get("SPIKE_MAX_SECS",    "180"))   # 3 min hard cap

# Raydium Runner strategy (post-graduation tokens on Raydium)
GRAD_MODE        = os.environ.get("GRAD_MODE", "true").lower() == "true"
GRAD_MAX_AGE_H   = float(os.environ.get("GRAD_MAX_AGE_H",   "4"))     # only tokens graduated in last 4h
GRAD_MIN_LIQ     = float(os.environ.get("GRAD_MIN_LIQ",     "15000")) # min $15k liquidity
GRAD_MIN_1H_PCT  = float(os.environ.get("GRAD_MIN_1H_PCT",  "5"))     # min +5% 1h momentum
GRAD_MIN_5M_PCT  = float(os.environ.get("GRAD_MIN_5M_PCT",  "-5"))    # reject if dumping >5% in last 5m
GRAD_MIN_VOL_24H = float(os.environ.get("GRAD_MIN_VOL_24H", "10000")) # min $10k 24h volume
GRAD_MIN_VOL_LIQ = float(os.environ.get("GRAD_MIN_VOL_LIQ", "0.5"))   # min volume/liq ratio
GRAD_TP_PCT      = float(os.environ.get("GRAD_TP_PCT",      "30"))
GRAD_SL_PCT      = float(os.environ.get("GRAD_SL_PCT",      "12"))
GRAD_MAX_SECS    = int(os.environ.get("GRAD_MAX_SECS",      "300"))   # 5 min hard cap
GRAD_POOL        = os.environ.get("GRAD_POOL", "raydium")
GRAD_SMC_MIN     = int(os.environ.get("GRAD_SMC_MIN", "1"))  # min SMC alignment score (0=off, 1-3)

# Hype Scalp — fires when a coin appears in 2+ GMGN feeds simultaneously
HYPE_MIN_FEEDS   = int(os.environ.get("HYPE_MIN_FEEDS",   "2"))    # min feeds to trigger
HYPE_MIN_LIQ     = float(os.environ.get("HYPE_MIN_LIQ",   "5000")) # lower bar — scalp is fast
HYPE_TP_PCT      = float(os.environ.get("HYPE_TP_PCT",    "15"))   # take profit at 15%
HYPE_SL_PCT      = float(os.environ.get("HYPE_SL_PCT",    "8"))    # stop loss at 8%
HYPE_MAX_SECS    = int(os.environ.get("HYPE_MAX_SECS",    "120"))  # 2 min max hold

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
COPY_PINNED_WALLETS = [a.strip() for a in os.environ.get("COPY_PINNED_WALLETS", "").split(",") if a.strip()]
GMGN_RANK         = "https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d"
GMGN_ACTIVITY     = "https://gmgn.ai/defi/quotation/v1/wallet_activity/sol"
GMGN_API_KEY       = os.environ.get("GMGN_API_KEY", "")
GMGN_TOP_HOLDERS   = "https://gmgn.ai/defi/quotation/v1/tokens/top_holders/sol"
GMGN_CREATED_TOKENS= "https://gmgn.ai/defi/quotation/v1/portfolio/sol"
GMGN_SIGNALS_URL   = "https://gmgn.ai/defi/quotation/v1/signals/sol"
GMGN_KOL_TRACK      = "https://gmgn.ai/defi/quotation/v1/tracks/kol/sol"
GMGN_SM_TRACK       = "https://gmgn.ai/defi/quotation/v1/tracks/smartmoney/sol"
GMGN_TRENDING_URL   = "https://gmgn.ai/defi/quotation/v1/tokens/trending/sol"
GMGN_HOT_SEARCH_URL = "https://gmgn.ai/defi/quotation/v1/tokens/hot_searches/sol"
GMGN_COMPLETING_URL = "https://gmgn.ai/defi/quotation/v1/tokens/completing/sol"
GMGN_NEW_PAIRS_URL  = "https://gmgn.ai/defi/quotation/v1/tokens/new_pairs/sol"
GMGN_TREND_SCAN_INTERVAL = int(os.environ.get("GMGN_TREND_SCAN_INTERVAL", "120"))  # seconds between trend scans

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
_daily_lock       = threading.Lock()
_tune_lock        = threading.Lock()
# Weekly tracking
_week_start_date  = ""
_week_day_logs    = []     # one entry per day: {date, trades, wins, losses, pnl, start_cap, end_cap}
_copy_wallets     = [{"address": a, "winrate": "pinned"} for a in COPY_PINNED_WALLETS]
_copy_wallet_time = 0.0
_copied_mints     = {}   # mint -> timestamp, to avoid double-copy
_copy_lock        = threading.Lock()
_gmgn_backoff     = 0    # epoch timestamp — retry GMGN rank after this time (0 = no backoff)
_sold_mints       = {}   # mint -> timestamp, cooldown after selling to prevent re-buy
_gmgn_sm_signal_mints  = set()   # smart money buy signal mints (type 12)
_gmgn_surge_mints      = set()   # price surge signal mints (type 6)
_gmgn_kol_mints        = set()   # KOL buy mints (entry signal)
_gmgn_sm_sell_mints    = set()   # smart money sell mints (exit/skip signal)
_gmgn_trending_mints   = set()   # trending tokens (1h/4h movers)
_gmgn_hot_mints        = set()   # hot search mints
_gmgn_completing_mints = set()   # bonding curve near completion
_gmgn_new_pair_mints   = set()   # newly listed pairs
_gmgn_signal_time      = 0.0     # last signal refresh time
_signal_lock           = threading.Lock()
_trend_scanned         = set()   # mints already evaluated by trend scanner (cleared hourly)
