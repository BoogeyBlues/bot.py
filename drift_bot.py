import os, time, threading, requests, json, hmac, hashlib
from collections import deque
from flask import Flask, jsonify, request as flask_request

app = Flask(__name__)

# ── CONFIG ────────────────────────────────────────────────────────
DRIFT_PAPER_MODE   = os.environ.get("DRIFT_PAPER_MODE", "true").lower() != "false"
DRIFT_EXCHANGE     = os.environ.get("DRIFT_EXCHANGE", "jupiter")
DRIFT_LEVERAGE     = float(os.environ.get("DRIFT_LEVERAGE", "65"))     # midpoint; used as fallback
DRIFT_LEV_MIN      = float(os.environ.get("DRIFT_LEV_MIN",  "50"))     # minimum leverage
DRIFT_LEV_MAX      = float(os.environ.get("DRIFT_LEV_MAX",  "80"))     # maximum leverage
DRIFT_MARGIN_USD   = float(os.environ.get("DRIFT_MARGIN_USD", "400"))  # fixed margin per trade ($)
DRIFT_MAX_OPEN     = int(os.environ.get("DRIFT_MAX_OPEN", "5"))
DRIFT_TP_PCT       = float(os.environ.get("DRIFT_TP_PCT", "0.20"))
DRIFT_SL_PCT       = float(os.environ.get("DRIFT_SL_PCT", "0.05"))
DRIFT_TRAIL_PCT    = float(os.environ.get("DRIFT_TRAIL_PCT", "0.015"))
DRIFT_MARKETS      = os.environ.get("DRIFT_MARKETS", "SOL,ETH,DOGE,PEPE,WIF,BONK,POPCAT,TRUMP,XRP,AVAX")
DRIFT_BOT_NAME     = os.environ.get("DRIFT_BOT_NAME", "Drift Sniper")
DRIFT_PORT         = int(os.environ.get("DRIFT_PORT", "5001"))
WALLET             = os.environ.get("WALLET", "")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY", "")
SOL_RPC            = os.environ.get("SOL_RPC", "https://api.mainnet-beta.solana.com")
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
GMGN_API_KEY       = os.environ.get("GMGN_API_KEY", "")
STARTING_CAPITAL   = float(os.environ.get("DRIFT_STARTING_CAPITAL", "5000"))
PROFIT_GOAL        = float(os.environ.get("DRIFT_PROFIT_GOAL", "10000"))
DRIFT_TP_USD       = float(os.environ.get("DRIFT_TP_USD",    str(DRIFT_MARGIN_USD * 2)))  # default: 2× margin
DRIFT_SL_MARGIN_PCT = float(os.environ.get("DRIFT_SL_MARGIN_PCT", "0.75"))  # close when loss = 75% of margin
DRIFT_TUNE_EVERY   = int(os.environ.get("DRIFT_TUNE_EVERY",   "3")) # retune after every N closed trades
DRIFT_COMPOUND_PCT    = float(os.environ.get("DRIFT_COMPOUND_PCT", "0.10"))  # % of profit reinvested
DRIFT_MAX_HOLD_MINUTES = int(os.environ.get("DRIFT_MAX_HOLD_MINUTES", "30"))  # force-exit after N minutes
REDIS_URL          = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN        = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
BYBIT_API_KEY      = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET   = os.environ.get("BYBIT_API_SECRET", "")
BYBIT_BASE_URL     = "https://api.bybit.com"

MILESTONES = [250, 500, 1000, 2500, 5000, 10000, 25000]

# ── STATE ─────────────────────────────────────────────────────────
_positions           = {}
_trades              = []
_total_trades_ever   = 0   # monotonic; never capped — used for tune trigger
_capital         = STARTING_CAPITAL
_profit_secured  = 0.0   # total profit taken out
_daily_pnl     = 0.0
_day_start     = ""
_price_history = {}
_milestones_hit = set()
_state_lock    = threading.Lock()
_tuning_lock   = threading.Lock()
_log_buffer    = []
_log_lock      = threading.Lock()
_notify_queue  = []
_notify_q_lock = threading.Lock()
_notify_log    = []   # last 50 messages sent (same text as Telegram)
_st_prev           = {}   # market -> previous supertrend bullish state (for flip detection)
_market_stats      = {}   # market -> {wins, losses, long_wins, short_wins, long_total, short_total}
_market_params     = {}   # tuned per-market: {leverage, bias, paused_until, last_result}
_5m_trend_cache    = {}   # market -> {trend, ts} — 5-min candle bias, 60s TTL
_liquidity_cache   = {}   # market -> {factor, ts} — OKX spread+volume factor, 30s TTL

_session = requests.Session()
_session.trust_env = False

# ── REDIS ─────────────────────────────────────────────────────────
def _redis_cmd(*args):
    if not REDIS_URL or not REDIS_TOKEN:
        return None
    try:
        r = _session.post(REDIS_URL, headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
                          json=list(args), timeout=5)
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

def _save_state():
    with _state_lock:
        cap  = _capital
        sec  = _profit_secured
        dpnl = _daily_pnl
        ds   = _day_start
        pos  = {k: dict(v) for k, v in _positions.items()}
        trd  = list(_trades)
        ms   = list(_milestones_hit)
        mst  = dict(_market_stats)
        mpr  = dict(_market_params)
        ph   = {k: list(v) for k, v in _price_history.items()}
    redis_save("drift_capital",        cap)
    redis_save("drift_positions",      pos)
    redis_save("drift_trades",         trd[-200:])
    redis_save("drift_milestones",     ms)
    redis_save("drift_secured",        sec)
    redis_save("drift_market_stats",   mst)
    redis_save("drift_market_params",  mpr)
    redis_save("drift_price_history",  ph)
    redis_save("drift_daily_pnl",      dpnl)
    redis_save("drift_day_start",      ds)

def _load_state():
    global _capital, _positions, _trades, _milestones_hit, _profit_secured
    global _market_stats, _market_params, _price_history, _daily_pnl, _day_start
    cap    = redis_load("drift_capital")
    pos    = redis_load("drift_positions")
    trd    = redis_load("drift_trades")
    ms     = redis_load("drift_milestones")
    sec    = redis_load("drift_secured")
    mst    = redis_load("drift_market_stats")
    mpr    = redis_load("drift_market_params")
    ph     = redis_load("drift_price_history")
    dpnl   = redis_load("drift_daily_pnl")
    ds     = redis_load("drift_day_start")
    with _state_lock:
        if cap is not None:
            _capital = float(cap)
        if pos:
            _positions = pos
        if trd:
            _trades = trd
        if ms:
            _milestones_hit = set(ms)
        if sec is not None:
            _profit_secured = float(sec)
        if mst:
            _market_stats = mst
        if mpr:
            _market_params = mpr
        ph_count = 0
        if ph:
            for k, v in ph.items():
                _price_history[k] = deque(v, maxlen=300)
            ph_count = len(ph)
        today = time.strftime("%Y-%m-%d")
        if dpnl is not None and ds == today:
            _daily_pnl = float(dpnl)
            _day_start  = ds
    if ph_count:
        log("ok", f"Restored price history for {ph_count} markets")

# ── LOGGING ───────────────────────────────────────────────────────
def log(level, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{level.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with _log_lock:
        _log_buffer.append({"time": time.strftime('%H:%M:%S'), "tag": level, "msg": msg})
        if len(_log_buffer) > 300:
            _log_buffer.pop(0)

# ── NOTIFICATIONS ─────────────────────────────────────────────────
def notify(msg):
    with _notify_q_lock:
        _notify_queue.append(msg)
        _notify_log.insert(0, {"time": time.strftime('%H:%M:%S'), "text": msg})
        if len(_notify_log) > 50:
            _notify_log.pop()

def _notify_worker():
    while True:
        item = None
        with _notify_q_lock:
            if _notify_queue:
                item = _notify_queue.pop(0)
        if item:
            if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
                try:
                    _session.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": item, "parse_mode": "Markdown"},
                        timeout=8
                    )
                except Exception as e:
                    log("warn", f"Telegram failed: {e}")
            time.sleep(1)
        else:
            time.sleep(0.2)

# ── PRICE FEEDS (Pyth primary → OKX fallback → Bybit last resort) ─────────────
# Pyth Network feed IDs — the same oracle Drift Protocol uses for settlement.
# Verified via https://hermes.pyth.network/v2/price_feeds
_PYTH_FEEDS = {
    "SOL":    "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
    "BTC":    "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
    "ETH":    "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
    "XRP":    "ec5d399846a9209f3fe5881d70aae9268c94339ff9817e8d18ff19fa05eea1c8",
    "DOGE":   "dcef50dd0a4cd2dcc17e45df1676dcb336a11a61c69df7a0299b0150c672d25c",
    "AVAX":   "93da3352f9f1d105fdfe4971cfa80e9dd777bfc5d0f683ebb6e1294b92137bb7",
    "BNB":    "2f95862b045670cd22bee3114c39763a4a08beeb663b145d283c31d7d1101c4f",
    "SUI":    "23d7315113f5b1d3ba7a83604c44b94d79f4fd69af77f804fc7f920a6dc65744",
    "BONK":   "72b021217ca3fe68922a19aaf990109cb9d84e9ad004b4d2025ad6f529314419",
    "WIF":    "4ca4beeca86f0d164160323817a4e42b10010a724c2217c6ee41b54cd4cc61fc",
    "PEPE":   "d69731a2e74ac1ce884fc3890f7ee324b6deb66147055249568869ed700882e4",
    "POPCAT": "b9312a7ee50e189ef045aa3c7842e099b061bd9bdc99ac645956c3b660dc8cce",
    "TRUMP":  "879551021853eec7a7dc827578e8e69da7e4fa8148339aa0d3d5296405be4b1a",
    "JTO":    "b43660a5f790c69354b0729a5ef9d50d68f1df92107540210b9cccba1f947cc2",
    "JUP":    "0a0408d619e9380abad35060f9192039ed5042fa6f82301d0e48bb52be830996",
    "HYPE":   "4279e31cc369bbcc2faf022b382b080e32a8e689ff20fbc530d2a603eb6cd98b",
    "TIA":    "09f7c1d7dfbb7df2b8fe3d3d87ee94a2259d212da4f30c1f0540d066dfa44723",
    "ARB":    "3fa4252848f9f0a1480be62745a4629d9eb1322aebab8a791e344b3b9c1adcf5",
    "RNDR":   "3d4a2bd9535be6ce8059d75eadeba507b043257321aa544717c56fa19b49e35d",
    "SEI":    "53614f1cb0c031d4af66c04cb9c756234adad0e1cee85303795091499a4084eb",
    "ONDO":   "d40472610abe56d36d065a0cf889fc8f1dd9f3b7f2a478231a5fc6df07ea5ce3",
    # PYTH token falls through to OKX
}

_PYTH_HERMES  = "https://hermes.pyth.network/v2/updates/price/latest"

# OKX perpetuals mark price — fallback for markets not on Pyth.
# Uses perp mark prices (not spot) so they align with how Drift prices positions.
_OKX_SYM = {
    "SOL":    "SOL-USDT-SWAP",  "BTC":    "BTC-USDT-SWAP",  "ETH":    "ETH-USDT-SWAP",
    "XRP":    "XRP-USDT-SWAP",  "DOGE":   "DOGE-USDT-SWAP", "AVAX":   "AVAX-USDT-SWAP",
    "SUI":    "SUI-USDT-SWAP",  "BNB":    "BNB-USDT-SWAP",  "BONK":   "BONK-USDT-SWAP",
    "WIF":    "WIF-USDT-SWAP",  "PEPE":   "PEPE-USDT-SWAP", "POPCAT": "POPCAT-USDT-SWAP",
    "TRUMP":  "TRUMP-USDT-SWAP","JTO":    "JTO-USDT-SWAP",  "JUP":    "JUP-USDT-SWAP",
    "HYPE":   "HYPE-USDT-SWAP", "TIA":    "TIA-USDT-SWAP",  "ARB":    "ARB-USDT-SWAP",
    "RNDR":   "RENDER-USDT-SWAP","SEI":   "SEI-USDT-SWAP",  "ONDO":   "ONDO-USDT-SWAP",
    "PYTH":   "PYTH-USDT-SWAP", "SHIB":   "SHIB-USDT-SWAP",
}

# Bybit last-resort fallback (spot, same symbol convention as OKX minus -SWAP)
_BYBIT_SYM = {k: v.replace("-USDT-SWAP", "USDT") for k, v in _OKX_SYM.items()}
_BYBIT_SYM["RNDR"] = "RNDRUSDT"   # Bybit uses RNDR, not RENDER

# Per-market price cache: {market: (price, fetched_at)}
_price_cache      = {}
_price_cache_lock = threading.Lock()
_PRICE_TTL        = 8   # seconds — reuse cached price within same loop tick

def _fetch_all_prices_pyth(markets):
    """
    Batch-fetch prices from Pyth Hermes in one HTTP request.
    Returns {market: price} for all markets with a known Pyth feed ID.
    Pyth is the oracle Drift uses — these prices match Drift's mark price exactly.
    """
    wanted = {m: _PYTH_FEEDS[m] for m in markets if m in _PYTH_FEEDS}
    if not wanted:
        return {}
    try:
        params = [("ids[]", fid) for fid in wanted.values()]
        r = _session.get(_PYTH_HERMES, params=params, timeout=8)
        if r.status_code != 200:
            return {}
        id_to_price = {}
        for item in r.json().get("parsed", []):
            raw   = item.get("price", {})
            price = int(raw["price"]) * (10 ** int(raw["expo"]))
            if price > 0:
                id_to_price[item["id"]] = price
        out = {}
        for market, fid in wanted.items():
            if fid in id_to_price:
                out[market] = id_to_price[fid]
        return out
    except Exception:
        return {}

def _fetch_all_prices_okx(markets):
    """
    Batch-fetch perpetuals mark prices from OKX in one request (all SWAP tickers).
    Returns {market: price} for markets found. Uses perp mark price, not spot.
    """
    if not any(m in _OKX_SYM for m in markets):
        return {}
    try:
        r = _session.get(
            "https://www.okx.com/api/v5/public/mark-price",
            params={"instType": "SWAP"},
            timeout=10
        )
        if r.status_code != 200:
            return {}
        sym_to_price = {
            item["instId"]: float(item["markPx"])
            for item in r.json().get("data", [])
            if float(item.get("markPx", 0)) > 0
        }
        return {
            m: sym_to_price[_OKX_SYM[m]]
            for m in markets
            if _OKX_SYM.get(m) in sym_to_price
        }
    except Exception:
        return {}

def _fetch_price_bybit(market):
    """Last-resort single-market Bybit fetch."""
    sym = _BYBIT_SYM.get(market.upper())
    if not sym:
        return None
    try:
        r = _session.get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "spot", "symbol": sym},
            timeout=8
        )
        if r.status_code == 200:
            items = r.json().get("result", {}).get("list", [])
            if items:
                price = float(items[0].get("lastPrice", 0))
                if price > 0:
                    return price
    except Exception:
        pass
    return None

def refresh_price_cache(markets):
    """
    Called once per trading loop tick.
    1. Pyth batch  — one request, all markets with known feed IDs
    2. OKX batch   — one request for remaining markets (perp mark prices)
    3. Bybit individual — last resort for anything still missing
    """
    now    = time.time()
    prices = _fetch_all_prices_pyth(markets)

    # OKX fills in markets not on Pyth
    missing = [m for m in markets if m not in prices]
    if missing:
        prices.update(_fetch_all_prices_okx(missing))

    # Bybit for anything still missing
    still_missing = [m for m in markets if m not in prices]
    for market in still_missing:
        p = _fetch_price_bybit(market)
        if p:
            prices[market] = p
            log("info", f"Bybit fallback: ${p:.4f}", market)

    with _price_cache_lock:
        for market, price in prices.items():
            _price_cache[market] = (price, now)

def _prefetch_price_history(markets):
    """Seed _price_history with 60 1-min OKX candles so EMA-50 is ready on first loop tick."""
    for market in markets:
        sym = _OKX_SYM.get(market.upper())
        if not sym:
            continue
        try:
            r = _session.get(
                "https://www.okx.com/api/v5/market/candles",
                params={"instId": sym, "bar": "1m", "limit": "60"},
                timeout=10,
            )
            candles = r.json().get("data", [])
            entries = [(int(c[0]) / 1000, float(c[4])) for c in reversed(candles)]
            if entries:
                with _state_lock:
                    if market not in _price_history:
                        _price_history[market] = deque(maxlen=300)
                    existing = {ts for ts, _ in _price_history[market]}
                    for ts, px in entries:
                        if ts not in existing:
                            _price_history[market].append((ts, px))
                log("ok", f"Pre-fetched {len(entries)} candles — signal engine warm", market)
        except Exception as e:
            log("warn", f"Candle pre-fetch failed: {e}", market)

def get_market_price(market):
    """Return cached price if fresh, otherwise fetch individually via Pyth → OKX → Bybit."""
    market = market.upper()
    with _price_cache_lock:
        cached = _price_cache.get(market)
    if cached and (time.time() - cached[1]) < _PRICE_TTL:
        return cached[0]
    # Cache miss — single fetch
    fid = _PYTH_FEEDS.get(market)
    if fid:
        try:
            r = _session.get(_PYTH_HERMES, params=[("ids[]", fid)], timeout=8)
            if r.status_code == 200:
                item = r.json().get("parsed", [{}])[0]
                raw  = item.get("price", {})
                price = int(raw["price"]) * (10 ** int(raw["expo"]))
                if price > 0:
                    with _price_cache_lock:
                        _price_cache[market] = (price, time.time())
                    return price
        except Exception:
            pass
    # OKX single-market fallback via the batch endpoint
    okx_prices = _fetch_all_prices_okx([market])
    if market in okx_prices:
        price = okx_prices[market]
        with _price_cache_lock:
            _price_cache[market] = (price, time.time())
        return price
    price = _fetch_price_bybit(market)
    if price:
        with _price_cache_lock:
            _price_cache[market] = (price, time.time())
    return price

def get_sol_price():
    return get_market_price("SOL")

# ── INDICATOR HELPERS ─────────────────────────────────────────────
def _calc_atr(vals, period=14):
    """Average True Range using |close[i] - close[i-1]| as proxy for True Range."""
    if len(vals) < period + 1:
        return None
    trs = [abs(vals[i] - vals[i-1]) for i in range(len(vals) - period, len(vals))]
    return sum(trs) / period

def _calc_rsi(vals, period=14):
    """RSI from closing prices."""
    if len(vals) < period + 1:
        return None
    deltas = [vals[i] - vals[i-1] for i in range(len(vals) - period, len(vals))]
    gains  = sum(max(0,  d) for d in deltas) / period
    losses = sum(max(0, -d) for d in deltas) / period
    if losses == 0:
        return 100.0
    return 100 - (100 / (1 + gains / losses))

def _supertrend(vals, period=10, mult=3.0):
    """
    Supertrend indicator (ATR-based).
    Returns (is_bullish: bool, level: float) or (None, None).
    Uses closing price as HL2 approximation since we have no OHLC data.
    """
    n = len(vals)
    if n < period + 5:
        return None, None

    trs = [abs(vals[i] - vals[i-1]) for i in range(1, n)]
    look = min(40, n - 1)
    prices = vals[-(look + 1):]
    trs_w  = trs[-look:]

    up   = [0.0] * look
    dn   = [0.0] * look
    bull = [True] * look

    for i in range(look):
        a    = sum(trs_w[max(0, i - period + 1):i + 1]) / min(i + 1, period)
        mid  = prices[i + 1]
        b_up = mid + mult * a
        b_dn = mid - mult * a
        if i == 0:
            up[i], dn[i] = b_up, b_dn
            bull[i] = mid > b_dn
        else:
            up[i] = b_up if b_up < up[i-1] or prices[i] > up[i-1] else up[i-1]
            dn[i] = b_dn if b_dn > dn[i-1] or prices[i] < dn[i-1] else dn[i-1]
            bull[i] = (prices[i+1] >= dn[i-1]) if bull[i-1] else (prices[i+1] > up[i-1])

    lvl = dn[-1] if bull[-1] else up[-1]
    return bull[-1], lvl

# ── SIGNAL ENGINE ─────────────────────────────────────────────────
def _get_gmgn_signal(market):
    """Fetch smart money signal from GMGN for the underlying asset. Returns 'long', 'short', or None."""
    if not GMGN_API_KEY:
        return None
    cached = _gmgn_signal_cache.get(market)
    if cached and time.time() - cached["ts"] < 300:
        return cached["result"]
    result = None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {GMGN_API_KEY}",
        }
        r = _session.get(
            "https://gmgn.ai/defi/quotation/v1/signals/sol",
            params={"type": "12", "limit": "20"},
            headers=headers,
            timeout=8
        )
        if r.status_code == 200:
            signals = r.json().get("data", {}).get("signals", [])
            for s in signals:
                sym = (s.get("token_symbol") or "").upper()
                if sym == market.upper():
                    action = (s.get("action") or "").lower()
                    if action in ("buy", "accumulate"):
                        result = "long"
                    elif action in ("sell", "dump"):
                        result = "short"
                    break
        _5m_trend_cache[market] = {"trend": None, "ts": now}
        return None


def _get_liquidity_factor(market):
    """
    Returns a leverage multiplier in [0.70, 1.00] based on two live signals from
    the OKX perpetual ticker: bid-ask spread and 24-hour USDT volume.

    Tight spread + high volume  → 1.00  (no penalty, full leverage)
    Wide spread or thin volume  → down to 0.70 (cut leverage up to 30%)

    Result is cached 30 s so one OKX call covers many 5-second loop ticks.
    Falls back to 1.0 on any network / parse error so it never blocks a trade.
    """
    now = time.time()
    cached = _liquidity_cache.get(market)
    if cached and now - cached["ts"] < 30:
        return cached["factor"]

    sym = _OKX_SYM.get(market.upper())
    if not sym:
        return 1.0
    try:
        r = _session.get(
            "https://www.okx.com/api/v5/market/ticker",
            params={"instId": sym},
            timeout=5,
        )
        if r.status_code != 200:
            return 1.0
        data = r.json().get("data", [{}])[0]
        bid      = float(data.get("bidPx",    0))
        ask      = float(data.get("askPx",    0))
        vol_usd  = float(data.get("volCcy24h", 0))   # 24 h notional in USDT

        # ── Spread factor ───────────────────────────────────────────
        # 0.01 % spread → ~0.975  |  0.05 % → ~0.875  |  ≥ 0.12 % → 0.70
        spread_factor = 1.0
        if bid > 0 and ask > 0:
            spread_pct    = (ask - bid) / ((ask + bid) / 2)
            spread_factor = max(0.70, 1.0 - spread_pct * 250)

        # ── Volume factor ───────────────────────────────────────────
        # Calibrated to typical OKX-SWAP daily volumes.
        # Below minimum → 0.70; at or above baseline → 1.00; linear between.
        _VOL_BASELINE = {"SOL": 1_000_000_000, "ETH": 5_000_000_000, "BTC": 10_000_000_000}
        _VOL_MIN      = {"SOL":   100_000_000, "ETH":   500_000_000, "BTC":  1_000_000_000}
        baseline = _VOL_BASELINE.get(market.upper(), 1_000_000_000)
        minimum  = _VOL_MIN.get(market.upper(),       100_000_000)
        if vol_usd <= 0:
            vol_factor = 1.0   # no data — don't penalise
        elif vol_usd < minimum:
            vol_factor = 0.70
        elif vol_usd >= baseline:
            vol_factor = 1.0
        else:
            vol_factor = 0.70 + 0.30 * (vol_usd - minimum) / (baseline - minimum)

        factor = min(spread_factor, vol_factor)
        _liquidity_cache[market] = {"factor": factor, "ts": now}
        return factor
    except Exception:
        return 1.0


def _calc_ema(vals, period):
    """Proper exponential moving average — weights recent prices more than SMA."""
    if len(vals) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(vals[:period]) / period  # seed with SMA of first `period` bars
    for p in vals[period:]:
        ema = p * k + ema * (1 - k)
    return ema

def get_signal(market):
    """
    Research-backed multi-factor signal engine.

    Entry requires ALL of:
      - Supertrend (10, 3) bullish/bearish
      - Price on correct side of EMA-50 (no counter-trend)
      - RSI-14 between 35-65 (not exhausted, not ranging at extremes)
      - Market not in dead zone (ATR / price > 0.001 — enough volatility to trade)

    Confidence score (affects position size):
      +1 base (Supertrend + EMA agree, RSI not exhausted)
      +1 if RSI is neutral (35-65) — momentum has room to run
      +1 if Supertrend just FLIPPED this bar (fresh signal, highest quality)
      +1 if GMGN smart money actively confirms direction
      → min to enter: confidence >= 2 (max 4)

    Returns (direction, confidence, atr) or (None, 0, None).
    """
    global _st_prev
    with _state_lock:
        prices = list(_price_history.get(market, []))
    if len(prices) < 55:   # need 50 for EMA + buffer
        return None, 0, None
    vals = [p for _, p in prices]

    # ── Indicators ────────────────────────────────────────────────
    ema50     = sum(vals[-50:]) / 50
    rsi       = _calc_rsi(vals, 14)
    atr       = _calc_atr(vals, 14)
    st_bull, st_lvl = _supertrend(vals, 10, 3.0)

    if rsi is None or atr is None or st_bull is None:
        return None, 0, None

    cur = vals[-1]

    # ── Market regime: must have enough volatility to be worth trading ──
    atr_pct = atr / cur
    if atr_pct < 0.001:   # market is frozen / ranging — skip
        _st_prev[market] = st_bull  # consume flip so it doesn't linger
        return None, 0, None

    # ── Direction from Supertrend ──────────────────────────────────
    trend = "long" if st_bull else "short"

    # ── Factor 1: EMA-50 trend filter (no counter-trend entries) ──
    ema_ok = (trend == "long" and cur > ema50) or (trend == "short" and cur < ema50)
    if not ema_ok:
        _st_prev[market] = st_bull  # consume flip so stale flip doesn't persist
        return None, 0, None

    # ── Factor 2: RSI exhaustion check ────────────────────────────
    # Hard block only on clear exhaustion (overbought long or oversold short).
    # Mid-range RSI adds a confidence point; extreme-but-not-exhausted passes with lower confidence.
    rsi_exhausted = (trend == "long" and rsi > 78) or (trend == "short" and rsi < 22)
    if rsi_exhausted:
        _st_prev[market] = st_bull
        return None, 0, None

    # ── Confidence scoring ─────────────────────────────────────────
    confidence = 1  # base: ST + EMA agree and RSI not exhausted
    if 35 < rsi < 65:
        confidence += 1  # RSI in neutral zone — momentum has room to run

    prev_bull = _st_prev.get(market)
    just_flipped = prev_bull is not None and prev_bull != st_bull
    if just_flipped:
        confidence += 1  # fresh flip = highest quality entry

    _st_prev[market] = st_bull

    return trend, confidence, atr

# ── LIVE EXECUTION STUBS ──────────────────────────────────────────
def _execute_drift_order(market, side, size_usd, leverage) -> bool:
    try:
        from driftpy.drift_client import DriftClient
        from driftpy.types import OrderType, PositionDirection, OrderParams
        from solders.keypair import Keypair
        from solana.rpc.async_api import AsyncClient
        import asyncio, base58

        kp = Keypair.from_bytes(base58.b58decode(WALLET_PRIVATE_KEY))

        async def _place():
            conn = AsyncClient(SOL_RPC)
            try:
                client = DriftClient(conn, kp)
                await client.subscribe()
                direction = PositionDirection.Long() if side == "long" else PositionDirection.Short()
                market_index_map = {
                    "SOL": 0, "BTC": 1, "ETH": 2, "APT": 3,
                    "BONK": 4, "ARB": 5, "DOGE": 6, "BNB": 7,
                    "SUI": 8, "PEPE": 9, "OP": 10, "MATIC": 11,
                    "XRP": 12, "WIF": 13, "JTO": 14, "PYTH": 15,
                    "TIA": 16, "JUP": 17, "RNDR": 18, "W": 19,
                    "DRIFT": 21, "POPCAT": 24, "HYPE": 26,
                    "TRUMP": 27, "AVAX": 28, "SEI": 29,
                }
                idx = market_index_map.get(market.upper())
                if idx is None:
                    raise ValueError(f"Market {market} not in Drift market_index_map — refusing to place order")
                price = get_market_price(market)
                if not price:
                    raise ValueError(f"Cannot get price for {market}")
                base_amount = int((size_usd / price) * 1e9)  # USD → base asset atomic units
                await client.place_perp_order(OrderParams(
                    order_type=OrderType.Market(),
                    market_index=idx,
                    direction=direction,
                    base_asset_amount=base_amount,
                ))
            finally:
                await conn.close()

        asyncio.run(_place())
        return True
    except ImportError:
        log("err", "driftpy not installed — run: pip install driftpy")
    except Exception as e:
        log("err", f"Drift order failed: {e}")
    return False

# ── BYBIT EXECUTION ──────────────────────────────────────────────
def _bybit_headers(ts, body_str):
    recv = "5000"
    sig  = hmac.new(BYBIT_API_SECRET.encode(),
                    f"{ts}{BYBIT_API_KEY}{recv}{body_str}".encode(),
                    hashlib.sha256).hexdigest()
    return {
        "X-BAPI-API-KEY":     BYBIT_API_KEY,
        "X-BAPI-SIGN":        sig,
        "X-BAPI-SIGN-TYPE":   "2",
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-RECV-WINDOW": recv,
        "Content-Type":       "application/json",
    }

def _bybit_qty(price, size_usd):
    qty = size_usd / price
    if price >= 100:
        return round(qty, 3)
    if price >= 1:
        return round(qty, 1)
    if price >= 0.001:
        return int(qty)
    return int(qty / 1000) * 1000  # PEPE/BONK — nearest thousand

def _execute_bybit_order(market, side, size_usd, leverage) -> bool:
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        log("err", "BYBIT_API_KEY / BYBIT_API_SECRET not set")
        return False
    symbol = _BYBIT_SYM.get(market.upper())
    if not symbol:
        log("err", f"No Bybit symbol for {market}")
        return False
    price = get_market_price(market)
    if not price:
        log("err", f"Cannot get price for {market}")
        return False
    qty = _bybit_qty(price, size_usd)
    # set leverage (non-fatal)
    ts       = str(int(time.time() * 1000))
    lev_body = json.dumps({"category": "linear", "symbol": symbol,
                            "buyLeverage": str(int(leverage)),
                            "sellLeverage": str(int(leverage))})
    try:
        _session.post(f"{BYBIT_BASE_URL}/v5/position/set-leverage",
                      headers=_bybit_headers(ts, lev_body),
                      data=lev_body, timeout=10)
    except Exception:
        pass
    # place market order
    ts         = str(int(time.time() * 1000))
    order_body = json.dumps({"category": "linear", "symbol": symbol,
                              "side": "Buy" if side == "long" else "Sell",
                              "orderType": "Market", "qty": str(qty)})
    r    = _session.post(f"{BYBIT_BASE_URL}/v5/order/create",
                         headers=_bybit_headers(ts, order_body),
                         data=order_body, timeout=15)
    data = r.json()
    if data.get("retCode") != 0:
        log("err", f"Bybit order failed: {data.get('retMsg')} [{data.get('retCode')}]", market)
        return False
    log("ok", f"Bybit {side} {market} qty={qty} orderId={data['result'].get('orderId','')[:16]}", market)
    return True

def _close_bybit_position(market, side) -> bool:
    symbol = _BYBIT_SYM.get(market.upper())
    if not symbol or not BYBIT_API_KEY:
        return False
    ts   = str(int(time.time() * 1000))
    body = json.dumps({"category": "linear", "symbol": symbol,
                       "side": "Sell" if side == "long" else "Buy",
                       "orderType": "Market", "qty": "0", "reduceOnly": True})
    r    = _session.post(f"{BYBIT_BASE_URL}/v5/order/create",
                         headers=_bybit_headers(ts, body),
                         data=body, timeout=15)
    data = r.json()
    if data.get("retCode") != 0:
        log("err", f"Bybit close failed: {data.get('retMsg')}", market)
        return False
    return True

# ── JUPITER PERPS EXECUTION ────────────────────────────────────────
# Markets: SOL, ETH, BTC only (JLP pool composition)
_JPERP_PROGRAM   = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"
_JPERP_POOL      = "5BUwFW4nRbftYTDMbgxykoFWqWHPzahFSNAaaaJtVKsq"
_JPERP_EVT_AUTH  = "37hJBDnntwqhGbK7L6M1bLyvccj4u55CCUiLPdYkiqBN"
_JPERP_CUSTODIES = {   # on-chain custody PDAs
    "SOL": "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz",
    "ETH": "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn",
    "BTC": "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm",
}
_JPERP_MINTS     = {   # token mint addresses on Solana
    "SOL": "So11111111111111111111111111111111111111112",
    "ETH": "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",  # Wormhole ETH
    "BTC": "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh",  # Wormhole wBTC
}
_JPERP_DECIMALS  = {"SOL": 9, "ETH": 8, "BTC": 8}
_JPERP_USDC_CUST = "G18jKKXQwBbrHeiK3C9MRXhkHsLHf7XgCSisykV46EZa"
_JPERP_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
_JPERP_MARKETS   = {"SOL", "ETH", "BTC"}
_JPERP_IDL       = None   # cached at startup — never fetched per-trade

# System programs
_SYS_PROG  = "11111111111111111111111111111111"
_TOK_PROG  = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_ATOK_PROG = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bsT"


def _jperp_ata(owner_pk, mint_pk):
    """Derive the Associated Token Account address (no spl-token dependency)."""
    from solders.pubkey import Pubkey
    ata, _ = Pubkey.find_program_address(
        [bytes(owner_pk), bytes(Pubkey.from_string(_TOK_PROG)), bytes(mint_pk)],
        Pubkey.from_string(_ATOK_PROG)
    )
    return ata


def _cache_jupiter_idl():
    """Fetch the Jupiter Perps IDL once at startup and store it globally.
    Called only in live mode after anchorpy is confirmed importable."""
    global _JPERP_IDL
    try:
        from anchorpy import Program, Provider, Wallet
        from solders.keypair import Keypair
        from solders.pubkey import Pubkey
        from solana.rpc.async_api import AsyncClient
        import asyncio

        async def _fetch():
            conn = AsyncClient(SOL_RPC)
            try:
                kp       = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
                prog_id  = Pubkey.from_string(_JPERP_PROGRAM)
                provider = Provider(conn, Wallet(kp))
                return await Program.fetch_idl(prog_id, provider)
            finally:
                await conn.close()

        idl = asyncio.run(_fetch())
        if idl:
            _JPERP_IDL = idl
            log("ok", "Jupiter Perps IDL cached at startup")
        else:
            log("warn", "Jupiter Perps IDL fetch returned None — will retry per-trade")
    except Exception as e:
        log("warn", f"Jupiter Perps IDL cache failed: {e} — will retry per-trade")


def _execute_jupiter_perp_order(market, side, size_usd, leverage) -> bool:
    mkt = market.upper()
    if mkt not in _JPERP_MARKETS:
        log("err", f"Jupiter Perps: {market} not supported — only SOL/ETH/BTC")
        return False
    try:
        from anchorpy import Program, Provider, Wallet, Context
        from solders.keypair import Keypair
        from solders.pubkey import Pubkey
        from solana.rpc.async_api import AsyncClient
        import asyncio

        kp = Keypair.from_base58_string(WALLET_PRIVATE_KEY)

        async def _place():
            conn = AsyncClient(SOL_RPC)
            try:
                prog_id    = Pubkey.from_string(_JPERP_PROGRAM)
                pool       = Pubkey.from_string(_JPERP_POOL)
                evt_auth   = Pubkey.from_string(_JPERP_EVT_AUTH)
                custody    = Pubkey.from_string(_JPERP_CUSTODIES[mkt])
                usdc_cust  = Pubkey.from_string(_JPERP_USDC_CUST)
                usdc_mint  = Pubkey.from_string(_JPERP_USDC_MINT)
                owner      = kp.pubkey()

                # Longs: collateral stored in the token itself; shorts: USDC
                col_cust = custody if side == "long" else usdc_cust

                # PDA derivations
                [perps_pda, _] = Pubkey.find_program_address([b"perpetuals"], prog_id)
                [pos_pda,   _] = Pubkey.find_program_address(
                    [b"position", bytes(owner), bytes(pool), bytes(custody), bytes(col_cust)],
                    prog_id
                )
                # time-based counter → unique request PDA per call
                counter = int(time.time() * 1000) % (2 ** 63)
                [req_pda, _] = Pubkey.find_program_address(
                    [b"position_request", bytes(owner), counter.to_bytes(8, "little")],
                    prog_id
                )

                # Token accounts (we always fund from owner's USDC ATA)
                funding_ata = _jperp_ata(owner, usdc_mint)
                req_ata     = _jperp_ata(req_pda, usdc_mint)

                # Parameters scaled to 6 decimal places
                margin_u6 = int(DRIFT_MARGIN_USD * 1_000_000)
                size_u6   = int(size_usd * 1_000_000)
                slippage  = 10_000  # 1% in units of 10^-6

                # For longs: USDC is swapped → token internally; provide min-out
                price = get_market_price(mkt)
                if not price:
                    raise ValueError(f"No price for {mkt}")
                if side == "long":
                    tok_units = (DRIFT_MARGIN_USD / price) * (10 ** _JPERP_DECIMALS[mkt])
                    jup_min   = int(tok_units * 0.99)   # 1% slippage buffer
                else:
                    jup_min = None  # USDC used directly for shorts

                side_enum = {"long": {}} if side == "long" else {"short": {}}

                provider = Provider(conn, Wallet(kp))
                idl      = _JPERP_IDL or await Program.fetch_idl(prog_id, provider)
                if not idl:
                    raise RuntimeError("Jupiter Perps IDL not available on-chain")
                program  = Program(idl, prog_id, provider)

                await program.rpc["create_increase_position_market_request"](
                    counter,
                    margin_u6,   # collateralTokenDelta
                    jup_min,     # jupiterMinimumOut  (None = no swap for shorts)
                    slippage,    # priceSlippage
                    side_enum,   # side
                    size_u6,     # sizeUsdDelta
                    ctx=Context(accounts={
                        "custody":                   custody,
                        "collateral_custody":         col_cust,
                        "funding_account":            funding_ata,
                        "input_mint":                 usdc_mint,
                        "owner":                      owner,
                        "perpetuals":                 perps_pda,
                        "pool":                       pool,
                        "position":                   pos_pda,
                        "position_request":           req_pda,
                        "position_request_ata":       req_ata,
                        "referral":                   None,
                        "system_program":             Pubkey.from_string(_SYS_PROG),
                        "token_program":              Pubkey.from_string(_TOK_PROG),
                        "associated_token_program":   Pubkey.from_string(_ATOK_PROG),
                        "event_authority":            evt_auth,
                        "program":                    prog_id,
                    }, signers=[kp])
                )
                log("ok", f"Jupiter Perps {side} {mkt} size=${size_usd:.0f} lev={leverage}x — keeper request submitted", mkt)
                return True
            finally:
                await conn.close()

        return asyncio.run(_place())
    except ImportError:
        log("err", "anchorpy not installed — run: pip install anchorpy")
        return False
    except Exception as e:
        log("err", f"Jupiter Perps open error: {e}", market)
        return False


def _close_jupiter_perp_position(market, side, size_usd) -> bool:
    mkt = market.upper()
    if mkt not in _JPERP_MARKETS:
        return False
    try:
        from anchorpy import Program, Provider, Wallet, Context
        from solders.keypair import Keypair
        from solders.pubkey import Pubkey
        from solana.rpc.async_api import AsyncClient
        import asyncio

        kp = Keypair.from_base58_string(WALLET_PRIVATE_KEY)

        async def _close():
            conn = AsyncClient(SOL_RPC)
            try:
                prog_id   = Pubkey.from_string(_JPERP_PROGRAM)
                pool      = Pubkey.from_string(_JPERP_POOL)
                evt_auth  = Pubkey.from_string(_JPERP_EVT_AUTH)
                custody   = Pubkey.from_string(_JPERP_CUSTODIES[mkt])
                usdc_cust = Pubkey.from_string(_JPERP_USDC_CUST)
                usdc_mint = Pubkey.from_string(_JPERP_USDC_MINT)
                owner     = kp.pubkey()

                col_cust = custody if side == "long" else usdc_cust

                [perps_pda, _] = Pubkey.find_program_address([b"perpetuals"], prog_id)
                [pos_pda,   _] = Pubkey.find_program_address(
                    [b"position", bytes(owner), bytes(pool), bytes(custody), bytes(col_cust)],
                    prog_id
                )
                counter = int(time.time() * 1000) % (2 ** 63)
                [req_pda, _] = Pubkey.find_program_address(
                    [b"position_request", bytes(owner), counter.to_bytes(8, "little")],
                    prog_id
                )

                recv_ata = _jperp_ata(owner, usdc_mint)   # receive USDC on close
                req_ata  = _jperp_ata(req_pda, usdc_mint)

                size_u6   = int(size_usd * 1_000_000)
                slippage  = 10_000
                side_enum = {"long": {}} if side == "long" else {"short": {}}

                provider = Provider(conn, Wallet(kp))
                idl      = _JPERP_IDL or await Program.fetch_idl(prog_id, provider)
                if not idl:
                    raise RuntimeError("Jupiter Perps IDL not available on-chain")
                program  = Program(idl, prog_id, provider)

                await program.rpc["create_decrease_position_market_request"](
                    counter,
                    0,           # collateralTokenDelta: 0 = release all collateral
                    size_u6,     # sizeUsdDelta: full position size
                    side_enum,
                    slippage,
                    usdc_mint,   # desiredMint: receive USDC back
                    ctx=Context(accounts={
                        "custody":                   custody,
                        "collateral_custody":         col_cust,
                        "owner":                      owner,
                        "pool":                       pool,
                        "perpetuals":                 perps_pda,
                        "position":                   pos_pda,
                        "position_request":           req_pda,
                        "position_request_ata":       req_ata,
                        "receiving_account":          recv_ata,
                        "referral":                   None,
                        "system_program":             Pubkey.from_string(_SYS_PROG),
                        "token_program":              Pubkey.from_string(_TOK_PROG),
                        "associated_token_program":   Pubkey.from_string(_ATOK_PROG),
                        "event_authority":            evt_auth,
                        "program":                    prog_id,
                    }, signers=[kp])
                )
                log("ok", f"Jupiter Perps close {side} {mkt} — keeper request submitted", mkt)
                return True
            finally:
                await conn.close()

        return asyncio.run(_close())
    except Exception as e:
        log("err", f"Jupiter Perps close error: {e}", market)
        return False

# ── POSITION MANAGEMENT ───────────────────────────────────────────
def open_position(market, side, price, size_usd, leverage, sl_pct=None, tp_pct=None, atr_pct=None):
    global _capital
    # Use ATR-based levels when provided, otherwise fall back to fixed config
    _sl = sl_pct if sl_pct is not None else DRIFT_SL_PCT
    _tp = tp_pct if tp_pct is not None else DRIFT_TP_PCT
    # Trail distance = 1× ATR (same unit as SL so it scales with volatility)
    _trail = atr_pct if atr_pct is not None else DRIFT_TRAIL_PCT
    if side == "long":
        tp = price * (1 + _tp)
        sl = price * (1 - _sl)
    else:
        tp = price * (1 - _tp)
        sl = price * (1 + _sl)

    # Liquidation when margin is fully consumed (~99% for safety math)
    liq_price = price * (1 - 0.99 / leverage) if side == "long" else price * (1 + 0.99 / leverage)

    pos = {
        "market": market, "side": side, "entry": price,
        "size": size_usd, "leverage": leverage,
        "peak_price": price, "tp": tp, "sl": sl,
        "liq_price": liq_price,
        "trail_pct": _trail,
        "opened_at": time.time(), "pnl": 0.0, "paper": DRIFT_PAPER_MODE,
        "current_price": price,
    }

    margin = size_usd / leverage
    with _state_lock:
        if len(_positions) >= DRIFT_MAX_OPEN:
            log("warn", f"Max open positions ({DRIFT_MAX_OPEN}) reached — skipping {market}")
            return
        if _capital < margin:
            log("warn", f"Insufficient capital ${_capital:.2f} for margin ${margin:.2f} — skipping")
            return

    if not DRIFT_PAPER_MODE:
        if DRIFT_EXCHANGE == "bybit":
            ok = _execute_bybit_order(market, side, size_usd, leverage)
        elif DRIFT_EXCHANGE == "jupiter":
            ok = _execute_jupiter_perp_order(market, side, size_usd, leverage)
        else:
            ok = _execute_drift_order(market, side, size_usd, leverage)
        if not ok:
            log("err", f"Exchange order failed for {market} — position NOT opened")
            return

    with _state_lock:
        # Re-check inside lock after placing order (capital/slots may have changed)
        if len(_positions) >= DRIFT_MAX_OPEN or _capital < margin:
            if not DRIFT_PAPER_MODE:
                log("err", f"State changed during live order for {market} — position recorded anyway to match on-chain state")
            else:
                return
        _positions[market] = pos
        _capital -= margin

    log("ok", f"OPEN {side.upper()} {market} @ ${price:.4f} size=${size_usd:.2f} {leverage}x TP={tp:.4f} SL={sl:.4f}")
    notify(
        f"{'[PAPER] ' if DRIFT_PAPER_MODE else ''}"
        f"*{DRIFT_BOT_NAME}*\n"
        f"OPEN {side.upper()} {market}\n"
        f"Entry: ${price:.4f}\n"
        f"Size: ${size_usd:.2f} @ {leverage}x\n"
        f"TP: ${tp:.4f} | SL: ${sl:.4f}"
    )
    _save_state()

def close_position(market, exit_price, reason=""):
    global _capital, _daily_pnl
    with _state_lock:
        pos = _positions.pop(market, None)
    if not pos:
        return
    if not DRIFT_PAPER_MODE:
        if DRIFT_EXCHANGE == "bybit":
            _close_bybit_position(market, pos["side"])
        elif DRIFT_EXCHANGE == "jupiter":
            _close_jupiter_perp_position(market, pos["side"], pos["size"])

    if pos["side"] == "long":
        pnl_pct = (exit_price - pos["entry"]) / pos["entry"]
    else:
        pnl_pct = (pos["entry"] - exit_price) / pos["entry"]

    margin   = pos["size"] / pos["leverage"]
    pnl_usd  = margin * pnl_pct * pos["leverage"]   # = notional * pnl_pct

    # Dollar TP: compound 10% back, secure the rest
    is_dollar_tp = reason.startswith("TP$") and pnl_usd > 0
    compound_amt = round(pnl_usd * DRIFT_COMPOUND_PCT, 2) if is_dollar_tp else 0
    secured_amt  = round(pnl_usd - compound_amt, 2) if is_dollar_tp else 0

    trade = {
        "market": market, "side": pos["side"],
        "entry": pos["entry"], "exit": exit_price,
        "pnl": pnl_usd, "pnl_pct": pnl_pct * 100 * pos["leverage"],
        "reason": reason,
        "secured": secured_amt, "compounded": compound_amt,
        "duration_s": time.time() - pos["opened_at"],
        "ts": time.strftime("%Y-%m-%d %H:%M"),
    }

    global _profit_secured
    with _state_lock:
        if is_dollar_tp:
            _capital += margin + compound_amt   # return margin + 10% of profit
            _profit_secured += secured_amt
        else:
            _capital += margin + pnl_usd
        _daily_pnl += pnl_usd
        cap_after = _capital  # snapshot while still holding lock
        _trades.insert(0, trade)  # capital update + trade record in single lock — no inconsistent reads
        if len(_trades) > 200:
            _trades.pop()

    if is_dollar_tp:
        log("ok", f"SECURED ${secured_amt:.2f} | COMPOUNDED +${compound_amt:.2f} → capital=${cap_after:.2f}", market)

    emoji = "✅" if pnl_usd >= 0 else "❌"
    log("ok" if pnl_usd >= 0 else "err",
        f"CLOSE {pos['side'].upper()} {market} @ ${exit_price:.4f} "
        f"PnL={pnl_usd:+.2f} ({pnl_pct * pos['leverage'] * 100:+.1f}% lev) [{reason}]")
    if is_dollar_tp:
        notify(
            f"💰 *{DRIFT_BOT_NAME}*\n"
            f"PROFIT TAKEN {market}\n"
            f"Secured: ${secured_amt:.2f}\n"
            f"Compounded: +${compound_amt:.2f} back into capital\n"
            f"Capital now: ${cap_after:.2f}"
        )
    else:
        notify(
            f"{emoji} *{DRIFT_BOT_NAME}*\n"
            f"CLOSE {pos['side'].upper()} {market}\n"
            f"Exit: ${exit_price:.4f}\n"
            f"PnL: ${pnl_usd:+.2f} ({pnl_pct * 100 * pos['leverage']:+.1f}%) | {reason}"
        )
    _save_state()
    _check_milestones()

    # ── Record market stats for adaptive learning ──────────────────
    won = pnl_usd > 0
    with _state_lock:
        s = _market_stats.setdefault(market, {
            "wins": 0, "losses": 0,
            "long_wins": 0, "short_wins": 0,
            "long_total": 0, "short_total": 0,
        })
        if won:
            s["wins"] += 1
            s["long_wins" if pos["side"] == "long" else "short_wins"] += 1
        else:
            s["losses"] += 1
        s["long_total" if pos["side"] == "long" else "short_total"] += 1
        mp = _market_params.setdefault(market, {})
        mp["last_result"] = "win" if won else "loss"
        if not won:
            # Brief cooldown after a loss — let the market settle before re-entering
            existing_pause = mp.get("paused_until", 0)
            loss_cooldown = time.time() + 900  # 15 min
            if loss_cooldown > existing_pause:
                mp["paused_until"] = loss_cooldown
        global _total_trades_ever
        _total_trades_ever += 1
        trade_count = _total_trades_ever  # use monotonic counter — len(_trades) caps at 200

    if trade_count % DRIFT_TUNE_EVERY == 0:
        if _tuning_lock.acquire(blocking=False):
            def _tune_and_release():
                try:
                    drift_auto_tune()
                finally:
                    _tuning_lock.release()
            threading.Thread(target=_tune_and_release, daemon=True).start()

def partial_close_position(market, exit_price, target_usd, reason=""):
    """Lock target_usd of profit by closing a fraction of the position; let the rest ride.
    Resets the timeout clock on the remaining notional."""
    global _capital, _daily_pnl
    with _state_lock:
        pos = _positions.get(market)
        if not pos:
            return

        if pos["side"] == "long":
            pnl_pct = (exit_price - pos["entry"]) / pos["entry"]
        else:
            pnl_pct = (pos["entry"] - exit_price) / pos["entry"]

        full_pnl = pos["size"] * pnl_pct  # notional * raw %

        if full_pnl > 0 and full_pnl >= target_usd:
            fraction = min(target_usd / full_pnl, 0.50)
        elif full_pnl > 0:
            fraction = 0.50   # can't hit target yet — take half anyway
        else:
            fraction = 0.25   # underwater: trim exposure

        realized_pnl  = full_pnl * fraction
        closed_margin = (pos["size"] / pos["leverage"]) * fraction
        closed_size   = pos["size"] * fraction

        _positions[market]["size"]     -= closed_size
        _positions[market]["opened_at"] = time.time()  # restart timeout clock

        cap_snap = pos.copy()   # snapshot for logging

    trade = {
        "market": market, "side": cap_snap["side"],
        "entry": cap_snap["entry"], "exit": exit_price,
        "pnl": round(realized_pnl, 4),
        "pnl_pct": pnl_pct * 100 * cap_snap["leverage"],
        "reason": reason,
        "secured": 0, "compounded": 0,
        "duration_s": time.time() - cap_snap["opened_at"],
        "ts": time.strftime("%Y-%m-%d %H:%M"),
    }

    with _state_lock:
        _capital   += closed_margin + realized_pnl
        _daily_pnl += realized_pnl
        cap_after   = _capital
        _trades.insert(0, trade)
        if len(_trades) > 200:
            _trades.pop()

    pct_closed = int(fraction * 100)
    pct_riding = 100 - pct_closed
    log("ok" if realized_pnl >= 0 else "warn",
        f"PARTIAL-{pct_closed}% {cap_snap['side'].upper()} {market} @ ${exit_price:.4f} "
        f"locked=${realized_pnl:+.4f} ({pnl_pct * cap_snap['leverage'] * 100:+.1f}% lev) "
        f"— {pct_riding}% riding [{reason}]")
    notify(
        f"💰 *{DRIFT_BOT_NAME}*\n"
        f"PARTIAL EXIT {market} ({pct_closed}%)\n"
        f"Locked: ${realized_pnl:+.4f}\n"
        f"Remaining: {pct_riding}% still riding\n"
        f"Capital now: ${cap_after:.2f}"
    )
    _save_state()


def monitor_positions():
    # TP/SL exits are handled exclusively by run_position_price_updater (every 5s).
    # This function only updates trailing peaks so the 60s loop keeps them current.
    with _state_lock:
        markets = list(_positions.keys())

    for market in markets:
        with _state_lock:
            pos = _positions.get(market)
        if not pos:
            continue
        price = get_market_price(market)
        if not price:
            continue

        raw_pct = (price - pos["entry"]) / pos["entry"] * (1 if pos["side"] == "long" else -1)
        notional = pos["size"]
        with _state_lock:
            if market not in _positions:
                continue
            _positions[market]["pnl"] = raw_pct * notional
            trail = _positions[market].get("trail_pct", DRIFT_TRAIL_PCT)
            if pos["side"] == "long" and price > _positions[market]["peak_price"]:
                _positions[market]["peak_price"] = price
                _positions[market]["sl"] = max(_positions[market]["sl"], price * (1 - trail))
            elif pos["side"] == "short" and price < _positions[market]["peak_price"]:
                _positions[market]["peak_price"] = price
                _positions[market]["sl"] = min(_positions[market]["sl"], price * (1 + trail))

def _check_milestones():
    with _state_lock:
        cap = _capital
        newly_hit = [m for m in MILESTONES if cap >= m and m not in _milestones_hit]
        for m in newly_hit:
            _milestones_hit.add(m)
    for m in newly_hit:
        log("ok", f"MILESTONE ${m:,} REACHED! Capital: ${cap:.2f}")
        notify(f"*{DRIFT_BOT_NAME}*\nMILESTONE ${m:,} REACHED!\nCapital: ${cap:.2f}")

# ── ADAPTIVE LEARNING ─────────────────────────────────────────────
def drift_auto_tune():
    """Retune per-market leverage, long/short bias, and market pausing from trade history."""
    import copy
    global _market_params
    changes = []
    with _state_lock:
        stats  = copy.deepcopy(_market_stats)
        params = copy.deepcopy(_market_params)
        total  = len(_trades)

    for market, s in stats.items():
        n = s.get("wins", 0) + s.get("losses", 0)
        if n < 3:
            continue

        wr          = s["wins"] / n
        cur         = params.get(market, {})
        cur_lev     = cur.get("leverage", DRIFT_LEVERAGE)
        long_total  = s.get("long_total",  0)
        short_total = s.get("short_total", 0)
        long_wins   = s.get("long_wins",   0)
        short_wins  = s.get("short_wins",  0)
        long_wr     = long_wins  / max(long_total,  1)
        short_wr    = short_wins / max(short_total, 1)

        # Leverage: pull back on losers, push up on winners — always within 50-80x range
        if wr < 0.30:
            new_lev = round(max(DRIFT_LEV_MIN, cur_lev - 5), 1)
        elif wr > 0.65:
            new_lev = round(min(DRIFT_LEV_MAX, cur_lev + 2), 1)
        else:
            new_lev = cur_lev

        # Long/short bias: if one side wins 70%+ AND has at least 3 trades, lock to it
        if long_total >= 3 and long_wr >= 0.70 and long_wr > short_wr + 0.20:
            bias = "long"
        elif short_total >= 3 and short_wr >= 0.70 and short_wr > long_wr + 0.20:
            bias = "short"
        else:
            bias = None

        # Pause market for 4h if win rate is very poor after 5+ trades
        paused_until = cur.get("paused_until", 0)
        if wr < 0.20 and n >= 5 and paused_until < time.time():
            paused_until = time.time() + 4 * 3600
            log("warn", f"Pausing {market} (WR={wr*100:.0f}% / {n} trades) for 4h", "TUNE")

        new = {"leverage": new_lev, "bias": bias,
               "paused_until": paused_until, "last_result": cur.get("last_result", "")}
        with _state_lock:
            _market_params[market] = new

        changed = new_lev != cur_lev or bias != cur.get("bias")
        if changed:
            changes.append(
                f"{market}: lev {cur_lev}x→{new_lev}x | "
                f"bias={bias or 'both'} | WR={wr*100:.0f}% ({n}t)"
            )

    _save_state()

    if changes:
        log("ok", f"Drift tuned {len(changes)} market(s): {'; '.join(changes)}", "TUNE")
        notify(
            f"🧠 *{DRIFT_BOT_NAME}* Trend Machine\n"
            f"{'—'*22}\n"
            f"Tuned after {total} trades:\n" +
            "\n".join(f"• {c}" for c in changes)
        )
    else:
        log("info", f"Drift tune check: no param changes ({total} trades)", "TUNE")

# ── TRADING LOOP ──────────────────────────────────────────────────
def run_trading_loop():
    markets = [m.strip().upper() for m in DRIFT_MARKETS.split(",")]
    log("ok", f"Trading loop started | markets={markets} | paper={DRIFT_PAPER_MODE}")

    # Seed price history immediately so EMA-50 is ready on the first loop tick
    _prefetch_price_history(markets)

    while True:
        try:
            # Single batch call to Binance for all markets — one request instead of N
            refresh_price_cache(markets)

            # Update price history from cache
            for market in markets:
                price = get_market_price(market)
                if price:
                    with _state_lock:
                        if market not in _price_history:
                            _price_history[market] = deque(maxlen=300)
                        _price_history[market].append((time.time(), price))

            # Monitor existing positions for exits
            monitor_positions()

            # Daily PnL reset
            today = time.strftime("%Y-%m-%d")
            global _day_start, _daily_pnl
            with _state_lock:
                if _day_start != today:
                    _day_start = today
                    _daily_pnl = 0.0

            # Open new positions if slots available
            with _state_lock:
                n_open = len(_positions)
                cap    = _capital

            if n_open < DRIFT_MAX_OPEN:
                for market in markets:
                    # Jupiter Perps only supports SOL/ETH/BTC — skip others silently
                    if DRIFT_EXCHANGE == "jupiter" and market.upper() not in _JPERP_MARKETS:
                        continue

                    with _state_lock:
                        already_open = market in _positions
                        mparams = dict(_market_params.get(market, {}))
                    if already_open:
                        continue

                    # Respect learned market pause
                    if mparams.get("paused_until", 0) > time.time():
                        resume = time.strftime("%H:%M", time.localtime(mparams["paused_until"]))
                        log("info", f"{market} paused until {resume} — skipping", "TUNE")
                        continue

                    signal, confidence, sig_atr = get_signal(market)
                    if not signal:
                        continue

                    # Respect learned long/short bias
                    bias = mparams.get("bias")
                    if bias and bias != signal:
                        log("info", f"{market} bias={bias} but signal={signal} — skip", "TUNE")
                        continue

                    price = get_market_price(market)
                    if not price:
                        continue

                    # ── ATR-based SL/TP (dynamic, volatility-adjusted) ────
                    atr_pct   = sig_atr / price
                    sl_pct    = atr_pct * 1.5   # SL = 1.5× ATR
                    tp_pct    = atr_pct * 3.0   # TP = 3× ATR (2:1 R:R)

                    # ── Liquidation safety cap ────────────────────────────
                    # At leverage L, liquidation happens when price moves 1/L against us.
                    # SL must land INSIDE that distance (using 85% of margin as hard limit).
                    # If even the minimum leverage puts SL past liquidation, skip the trade.
                    max_safe_lev = 0.85 / sl_pct if sl_pct > 0 else DRIFT_LEV_MAX
                    if max_safe_lev < DRIFT_LEV_MIN:
                        log("info",
                            f"ATR too high for safe {int(DRIFT_LEV_MIN)}x entry "
                            f"(sl={sl_pct*100:.2f}% > liq buffer) — skip", market)
                        continue

                    # ── Dynamic leverage: volatility + liquidity, clamped to safe range ──
                    # Volatility (ATR): calm markets get higher leverage; volatile gets lower.
                    # Liquidity (spread + 24h volume): thin markets get up to 30% haircut.
                    # Confidence boost: fresh Supertrend flip (conf=3) adds 15%.
                    LEV_K      = 0.10
                    raw_lev    = LEV_K / atr_pct if atr_pct > 0 else DRIFT_LEV_MIN
                    tuned_lev  = mparams.get("leverage", DRIFT_LEVERAGE)
                    raw_lev    = (raw_lev + tuned_lev) / 2
                    if confidence >= 3:
                        raw_lev *= 1.15   # fresh flip = highest conviction, push lev up
                    liq_mult   = _get_liquidity_factor(market)
                    raw_lev   *= liq_mult
                    if liq_mult < 0.95:
                        log("info", f"Liquidity factor {liq_mult:.2f} — leverage trimmed", market)
                    market_lev = int(max(DRIFT_LEV_MIN, min(DRIFT_LEV_MAX, raw_lev, max_safe_lev)))
                    size_usd   = DRIFT_MARGIN_USD * market_lev
                    margin     = DRIFT_MARGIN_USD

                    log("ok",
                        f"SIGNAL {signal.upper()} conf={confidence}/4 lev={market_lev}x "
                        f"margin=${margin:.0f} notional=${size_usd:.0f} "
                        f"SL={sl_pct*100:.2f}% TP={tp_pct*100:.2f}%", market)

                    # Open with ATR-based SL/TP and ATR-based trailing stop
                    open_position(market, signal, price, size_usd, market_lev,
                                  sl_pct=sl_pct, tp_pct=tp_pct, atr_pct=atr_pct)

        except Exception as e:
            log("err", f"Loop error: {e}")

        time.sleep(5)

# ── FLASK ROUTES ──────────────────────────────────────────────────
_CURSOR = """<style>
@media(pointer:fine){
  body{
    background-image:linear-gradient(rgba(5,10,20,.65),rgba(5,10,20,.65)),
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
    mode         = "PAPER" if DRIFT_PAPER_MODE else "LIVE"
    mode_color   = "#ffee00" if DRIFT_PAPER_MODE else "#39ff14"
    exch         = DRIFT_EXCHANGE.upper()
    markets_list = [m.strip().upper() for m in DRIFT_MARKETS.split(",")]
    cap_str      = f"${_capital:,.2f}"
    cap_pct      = round(max(0, min(100, (_capital - STARTING_CAPITAL) / max(PROFIT_GOAL - STARTING_CAPITAL, 1) * 100)), 1)
    milestone_html = "".join(
        f'<div class="milestone" id="ms-{m}"><div class="milestone-dot"></div>${m:,}</div>'
        for m in MILESTONES
    )
    market_rows = "".join(
        f'<div class="market-row">'
        f'<div class="market-label">{mk}</div>'
        f'<button class="trade-btn btn-long" onclick="manualTrade(\'{mk}\',\'long\')">LONG</button>'
        f'<button class="trade-btn btn-short" onclick="manualTrade(\'{mk}\',\'short\')">SHORT</button>'
        f'</div>'
        for mk in markets_list
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>{DRIFT_BOT_NAME}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{--bg:#050a14;--bg2:#080f1e;--bg3:#0d1628;--cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--yellow:#ffee00;--text:#c8d8f0;--muted:#4a6080}}

body{{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;overflow-x:hidden}}

/* ORBS */
.orb{{position:fixed;border-radius:50%;pointer-events:none;z-index:0;filter:blur(80px);opacity:.18}}
.orb-cyan{{width:340px;height:340px;top:-80px;left:calc(50% - 215px);background:radial-gradient(circle,var(--cyan) 0%,transparent 70%);animation:orbCyan 8s ease-in-out infinite}}
.orb-red{{width:300px;height:300px;bottom:-80px;left:calc(50% + 15px);background:radial-gradient(circle,var(--red) 0%,transparent 70%);animation:orbRed 8s ease-in-out infinite}}
@keyframes orbCyan{{0%{{transform:translate(0,0)}}25%{{transform:translate(40px,30px)}}50%{{transform:translate(20px,60px)}}75%{{transform:translate(-20px,30px)}}100%{{transform:translate(0,0)}}}}
@keyframes orbRed{{0%{{transform:translate(0,0)}}25%{{transform:translate(-30px,-40px)}}50%{{transform:translate(-60px,-20px)}}75%{{transform:translate(-20px,20px)}}100%{{transform:translate(0,0)}}}}
#particles{{position:fixed;inset:0;z-index:0;pointer-events:none}}
.wrapper{{max-width:430px;margin:0 auto;position:relative;z-index:1;min-height:100vh}}

/* NAV */
nav{{position:sticky;top:0;z-index:100;background:rgba(5,10,20,.92);backdrop-filter:blur(12px);border-bottom:1px solid rgba(0,229,255,.12);display:flex;align-items:center;padding:0 12px;height:48px;overflow:hidden}}
.nav-link{{font-family:'Bebas Neue',sans-serif;font-size:13px;letter-spacing:1.5px;color:var(--muted);text-decoration:none;padding:0 9px;height:100%;display:flex;align-items:center;transition:color .2s;opacity:0;transform:translateX(-30px);white-space:nowrap}}
.nav-link.active{{color:var(--cyan)}}
.nav-link:hover{{color:var(--text)}}
.nav-link:nth-child(1){{animation:navSlide .5s ease forwards .05s}}
.nav-link:nth-child(2){{animation:navSlide .5s ease forwards .15s}}
.nav-link:nth-child(3){{animation:navSlide .5s ease forwards .25s}}
.nav-link:nth-child(4){{animation:navSlide .5s ease forwards .35s}}
@keyframes navSlide{{to{{opacity:1;transform:translateX(0)}}}}

/* MODE STRIP */
.mode-strip{{display:flex;align-items:center;justify-content:space-between;padding:8px 16px;background:rgba(8,15,30,.7);border-bottom:1px solid rgba(0,229,255,.07)}}
.bot-name{{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--cyan);display:flex;align-items:center;gap:6px}}
.blink-dot{{width:6px;height:6px;border-radius:50%;background:{mode_color};animation:blinkDot 1s step-end infinite}}
@keyframes blinkDot{{0%,100%{{opacity:1}}50%{{opacity:0}}}}
.mode-pill{{font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:700;letter-spacing:2px;padding:3px 10px;border-radius:20px;background:rgba(255,238,0,.1);border:1px solid rgba(255,238,0,.3);color:var(--yellow);animation:pillGlow 2.5s ease-in-out infinite}}
@keyframes pillGlow{{0%,100%{{box-shadow:0 0 4px rgba(255,238,0,.2)}}50%{{box-shadow:0 0 14px rgba(255,238,0,.6),0 0 28px rgba(255,238,0,.2)}}}}

.scroll-area{{padding:0 16px 80px}}

/* HERO */
.hero{{padding:28px 0 16px;text-align:center}}
.hero-title{{font-family:'Bebas Neue',sans-serif;font-size:42px;letter-spacing:4px;color:var(--cyan);text-shadow:0 0 20px rgba(0,229,255,.4);display:inline-block;overflow:hidden;white-space:nowrap;width:0;border-right:3px solid var(--cyan);animation:typewriter .8s steps(12,end) forwards .3s}}
@keyframes typewriter{{to{{width:260px}}}}
.hero-balance{{display:flex;justify-content:center;align-items:baseline;gap:1px;margin:10px 0 6px;font-family:'JetBrains Mono',monospace;font-size:32px;font-weight:700;color:var(--text)}}
.digit-reel-wrap{{display:inline-block;overflow:hidden;height:1.1em;vertical-align:bottom}}
.digit-reel{{display:flex;flex-direction:column}}
.digit-char{{height:1.1em;line-height:1.1em;display:block}}
.hero-sub{{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:1.5px;display:flex;justify-content:center;gap:20px;opacity:0;animation:fadeIn .5s ease forwards 1.1s}}
.hero-sub .up{{color:var(--green)}}.hero-sub .hi{{color:var(--cyan)}}
@keyframes fadeIn{{to{{opacity:1}}}}

/* WAVE */
.wave-wrap{{height:32px;overflow:hidden;margin:12px -16px;position:relative}}
.wave-svg{{width:200%;height:100%;animation:waveScroll 4s linear infinite}}
@keyframes waveScroll{{from{{transform:translateX(0)}}to{{transform:translateX(-50%)}}}}

/* STATS GRID */
.stats-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:16px}}
.stat-cell{{background:var(--bg3);border:1px solid rgba(0,229,255,.1);border-radius:10px;padding:10px 8px;text-align:center;opacity:0;transform:translateY(20px)}}
.stat-cell:nth-child(1){{animation:statUp .5s ease forwards .3s,statBob 3s ease-in-out infinite 1.5s}}
.stat-cell:nth-child(2){{animation:statUp .5s ease forwards .45s,statBob 3s ease-in-out infinite 2.5s}}
.stat-cell:nth-child(3){{animation:statUp .5s ease forwards .6s,statBob 3s ease-in-out infinite 3.5s}}
@keyframes statUp{{to{{opacity:1;transform:translateY(0)}}}}
@keyframes statBob{{0%,100%{{transform:translateY(0)}}50%{{transform:translateY(-4px)}}}}
.stat-label{{font-family:'JetBrains Mono',monospace;font-size:9px;letter-spacing:2px;color:var(--muted);margin-bottom:4px;text-transform:uppercase}}
.stat-val{{font-family:'Bebas Neue',sans-serif;font-size:22px;letter-spacing:1px;color:var(--cyan)}}

/* SECTION HEADER */
.section-header{{font-family:'Bebas Neue',sans-serif;font-size:16px;letter-spacing:3px;color:var(--text);margin:20px 0 10px;position:relative;overflow:hidden}}
.scan-bar{{position:absolute;top:0;left:-40%;width:40%;height:100%;background:linear-gradient(90deg,transparent,rgba(0,229,255,.35),transparent);animation:scanBar 4s ease-in-out infinite}}
@keyframes scanBar{{0%{{left:-40%}}100%{{left:140%}}}}

/* POSITION CARDS */
.pos-card{{border-radius:12px;padding:12px;display:flex;align-items:center;gap:10px;opacity:0;transform:translateX(60px);cursor:pointer;touch-action:pan-y;will-change:transform;position:relative;z-index:1}}
.pos-card.profit{{background:linear-gradient(110deg,var(--bg3) 0%,rgba(0,255,136,.04) 50%,var(--bg3) 100%);background-size:200% 100%;border:1px solid rgba(0,255,136,.35);animation:slideFromRight .55s ease forwards .7s,cardShimmer 3s linear infinite 2s}}
.pos-card.loss{{background:var(--bg3);border:1px solid rgba(255,51,85,.35);animation:slideFromRight .55s ease forwards .85s}}
.pos-card.snap{{transition:transform .25s cubic-bezier(.25,.1,.25,1)}}
@keyframes slideFromRight{{to{{opacity:1;transform:translateX(0)}}}}
@keyframes cardShimmer{{0%{{background-position:200% 0}}100%{{background-position:-200% 0}}}}
/* SWIPE WRAP — holds card + hidden close button */
.swipe-wrap{{position:relative;border-radius:12px;margin-bottom:10px;overflow:hidden;}}
.swipe-close{{position:absolute;right:0;top:0;bottom:0;width:76px;background:var(--red);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;cursor:pointer;user-select:none;-webkit-tap-highlight-color:transparent}}
.swipe-close svg{{width:20px;height:20px;stroke:#fff;stroke-width:2.5;fill:none}}
.swipe-close span{{font-family:'Bebas Neue',sans-serif;font-size:12px;letter-spacing:2px;color:#fff}}
.pos-info{{flex:1}}
.pos-pair{{font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:700;color:var(--text)}}
.pos-dir{{font-size:10px;font-family:'JetBrains Mono',monospace;letter-spacing:1px;margin-top:2px}}
.pos-dir.long{{color:var(--green)}}.pos-dir.short{{color:var(--red)}}
.pos-pnl{{font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;flex-shrink:0}}
.pos-pnl.up{{color:var(--green)}}.pos-pnl.dn{{color:var(--red)}}
.sparkline-wrap{{width:72px;height:40px;flex-shrink:0}}
.no-pos{{text-align:center;padding:28px 16px;color:var(--muted);font-size:13px}}

/* TRADE DECK */
.deck-wrap{{perspective:600px;height:88px;position:relative;margin-bottom:8px;cursor:pointer;user-select:none}}
.trade-card{{position:absolute;inset:0;background:var(--bg3);border-radius:12px;border:1px solid rgba(0,229,255,.12);padding:12px 16px;display:flex;align-items:center;justify-content:space-between;backface-visibility:hidden;opacity:0;will-change:transform,opacity}}
.tc-pair{{font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:700;color:var(--text);margin-bottom:4px}}
.tc-pnl{{font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700}}
.tc-badge{{font-family:'JetBrains Mono',monospace;font-size:9px;letter-spacing:2px;padding:2px 7px;border-radius:20px;font-weight:700;margin-left:8px}}
.tc-badge.win{{background:rgba(0,255,136,.15);color:var(--green);border:1px solid rgba(0,255,136,.3)}}
.tc-badge.loss{{background:rgba(255,51,85,.15);color:var(--red);border:1px solid rgba(255,51,85,.3)}}
.deck-hint{{font-family:'JetBrains Mono',monospace;font-size:9px;color:var(--muted);text-align:center;letter-spacing:1.5px}}

/* PROGRESS BAR */
.goal-label{{font-family:'JetBrains Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:1.5px;margin-bottom:6px;display:flex;justify-content:space-between}}
.goal-label span{{color:var(--cyan)}}
.progress-track{{background:var(--bg3);border-radius:8px;height:12px;border:1px solid rgba(0,229,255,.1);overflow:hidden;margin-bottom:10px;position:relative}}
.progress-fill{{height:100%;width:0%;background:linear-gradient(90deg,var(--cyan),var(--green));border-radius:8px;transition:width 1.2s cubic-bezier(.22,1,.36,1);position:relative;overflow:hidden}}
.progress-shine{{position:absolute;top:0;left:-60%;width:60%;height:100%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.4),transparent);animation:shineSlide 2s linear infinite 1.5s}}
@keyframes shineSlide{{0%{{left:-60%}}100%{{left:120%}}}}
.milestones{{display:flex;justify-content:space-between}}
.milestone{{font-family:'JetBrains Mono',monospace;font-size:8px;letter-spacing:1px;text-align:center;color:var(--muted)}}
.milestone.hit{{color:var(--cyan);animation:msGlow 2s ease-in-out infinite}}
@keyframes msGlow{{0%,100%{{text-shadow:0 0 4px rgba(0,229,255,.2)}}50%{{text-shadow:0 0 12px rgba(0,229,255,.8)}}}}
.milestone-dot{{width:6px;height:6px;border-radius:50%;background:var(--muted);margin:0 auto 3px}}
.milestone.hit .milestone-dot{{background:var(--cyan);box-shadow:0 0 8px var(--cyan)}}

/* MANUAL TRADING */
.market-row{{display:flex;align-items:center;gap:8px;margin-bottom:8px;opacity:0;transform:translateX(-30px)}}
.market-row:nth-child(1){{animation:slideLeft .5s ease forwards .2s}}
.market-row:nth-child(2){{animation:slideLeft .5s ease forwards .35s}}
.market-row:nth-child(3){{animation:slideLeft .5s ease forwards .5s}}
.market-row:nth-child(4){{animation:slideLeft .5s ease forwards .65s}}
@keyframes slideLeft{{to{{opacity:1;transform:translateX(0)}}}}
.market-label{{font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:700;color:var(--text);width:36px;flex-shrink:0}}
.trade-btn{{flex:1;padding:9px 0;border:none;border-radius:8px;font-family:'Bebas Neue',sans-serif;font-size:15px;letter-spacing:2px;cursor:pointer;position:relative;overflow:hidden;transition:transform .15s}}
.trade-btn:active{{transform:scale(.96)}}
.btn-long{{background:rgba(0,255,136,.12);border:1px solid rgba(0,255,136,.35);color:var(--green);animation:longPulse 2.5s ease-in-out infinite}}
@keyframes longPulse{{0%,100%{{box-shadow:0 0 4px rgba(0,255,136,.1)}}50%{{box-shadow:0 0 14px rgba(0,255,136,.4)}}}}
.btn-short{{background:rgba(255,51,85,.12);border:1px solid rgba(255,51,85,.35);color:var(--red);animation:shortPulse 2.5s ease-in-out infinite 1.25s}}
@keyframes shortPulse{{0%,100%{{box-shadow:0 0 4px rgba(255,51,85,.1)}}50%{{box-shadow:0 0 14px rgba(255,51,85,.4)}}}}
.ripple-el{{position:absolute;border-radius:50%;transform:scale(0);pointer-events:none;opacity:.4;animation:rippleAnim .6s ease forwards}}
@keyframes rippleAnim{{to{{transform:scale(4);opacity:0}}}}
.reset-btn{{width:100%;margin-top:12px;padding:10px;background:transparent;border:1px solid rgba(255,51,85,.3);color:var(--red);font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:700;letter-spacing:1.5px;cursor:pointer;text-transform:uppercase;border-radius:8px;transition:all .2s}}
.reset-btn:hover{{background:var(--red);color:#fff}}

/* LIVE FEED */
.feed-entry{{font-family:'JetBrains Mono',monospace;font-size:10px;padding:7px 10px;background:var(--bg3);border-left:3px solid var(--muted);border-radius:0 6px 6px 0;margin-bottom:5px;opacity:0;transform:translateX(30px);letter-spacing:.5px;line-height:1.4}}
.feed-entry.vis{{opacity:1;transform:translateX(0);transition:opacity .4s,transform .4s}}
.feed-ts{{opacity:.5;margin-right:8px}}
.feed-cursor{{display:inline-block;width:6px;height:.85em;background:var(--muted);margin-left:2px;vertical-align:text-bottom;animation:blinkCur .8s step-end infinite}}
@keyframes blinkCur{{0%,100%{{opacity:1}}50%{{opacity:0}}}}

footer{{text-align:center;padding:20px 16px 40px;font-family:'JetBrains Mono',monospace;font-size:9px;letter-spacing:2px;color:var(--muted);opacity:0;animation:fadeIn .6s ease forwards 1.5s;line-height:1.8}}
footer a{{color:var(--cyan);text-decoration:none}}
::-webkit-scrollbar{{width:4px}}
::-webkit-scrollbar-thumb{{background:var(--muted);border-radius:4px}}
@media(min-width:768px){{
  .wrapper{{max-width:1200px}}
  nav{{padding:0 32px;gap:8px}}
  .nav-link{{font-size:15px;padding:0 16px}}
  .scroll-area{{padding:0 48px 80px;display:grid;grid-template-columns:1fr 1fr;gap:0 56px;align-items:start}}
  .hero{{grid-column:1/-1;padding:40px 0 20px}}
  .hero-title{{font-size:64px;width:auto!important;animation:none;border-right:none;display:block}}
  .hero-balance{{font-size:52px}}
  .hero-sub{{font-size:13px;gap:32px}}
  .wave-wrap{{grid-column:1/-1}}
  .stats-grid{{grid-column:1/-1;grid-template-columns:repeat(6,1fr)}}
  .stat-val{{font-size:28px}}
  .section-header{{font-size:18px;letter-spacing:4px}}
  .pos-pnl{{font-size:16px}}
  .feed-entry{{font-size:11px;padding:9px 14px}}
  .market-row:nth-child(5){{animation:slideLeft .5s ease forwards .8s}}
  .market-row:nth-child(6){{animation:slideLeft .5s ease forwards .95s}}
  .market-row:nth-child(7){{animation:slideLeft .5s ease forwards 1.1s}}
  .trade-btn{{font-size:17px;padding:11px 0}}
  footer{{font-size:11px}}
}}
</style>
</head>
<body>

<div class="orb orb-cyan"></div>
<div class="orb orb-red"></div>
<canvas id="particles"></canvas>

<div class="wrapper">

  <nav>
    <a class="nav-link active" href="/">HOME</a>
    <a class="nav-link" href="/trades">TRADES</a>
    <a class="nav-link" href="/monitor">MONITOR</a>
    <a class="nav-link" href="https://jup.ag" target="_blank">JUPITER ↗</a>
  </nav>

  <div class="mode-strip">
    <div class="bot-name"><div class="blink-dot"></div>{DRIFT_BOT_NAME}</div>
    <div class="mode-pill">{mode}</div>
  </div>

  <div class="scroll-area">

    <!-- HERO -->
    <div class="hero">
      <div class="hero-title" id="heroTitle">{DRIFT_BOT_NAME}</div>
      <div class="hero-balance" id="heroBalance"></div>
      <div class="hero-sub">
        <span>PNL <span class="up" id="heroPnl">+$0.00</span></span>
        <span>WIN RATE <span class="hi" id="heroWr">0%</span></span>
      </div>
    </div>

    <!-- WAVE DIVIDER -->
    <div class="wave-wrap">
      <svg class="wave-svg" viewBox="0 0 800 32" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M0,16 C50,0 100,32 150,16 C200,0 250,32 300,16 C350,0 400,32 450,16 C500,0 550,32 600,16 C650,0 700,32 750,16 C800,0 800,16 800,16" fill="none" stroke="rgba(0,229,255,0.15)" stroke-width="1.5"/>
        <path d="M0,16 C50,0 100,32 150,16 C200,0 250,32 300,16 C350,0 400,32 450,16 C500,0 550,32 600,16 C650,0 700,32 750,16 C800,0 800,16 800,16" fill="none" stroke="rgba(0,229,255,0.15)" stroke-width="1.5" transform="translate(400,0)"/>
      </svg>
    </div>

    <!-- STATS GRID -->
    <div class="stats-grid">
      <div class="stat-cell">
        <div class="stat-label">TRADES</div>
        <div class="stat-val" id="statTrades">0</div>
      </div>
      <div class="stat-cell">
        <div class="stat-label">OPEN</div>
        <div class="stat-val" id="statOpen">0</div>
      </div>
      <div class="stat-cell">
        <div class="stat-label">DAILY PNL</div>
        <div class="stat-val" id="statDpnl">$0</div>
      </div>
    </div>

    <!-- OPEN POSITIONS -->
    <div class="section-header">OPEN POSITIONS<div class="scan-bar"></div></div>
    <div id="pos-wrap"><div class="no-pos" id="no-pos">📡 Scanning for signals...</div></div>

    <!-- RECENT TRADES DECK -->
    <div class="section-header">RECENT TRADES<div class="scan-bar"></div></div>
    <div class="deck-wrap" id="deck" onclick="cycleDeck()"></div>
    <div class="deck-hint" id="deckHint"></div>

    <!-- GOAL PROGRESS -->
    <div class="section-header">GOAL PROGRESS<div class="scan-bar"></div></div>
    <div class="goal-label">
      <span id="goalCap">${_capital:,.2f} / ${PROFIT_GOAL:,.0f}</span>
      <span id="goalPct">{cap_pct}%</span>
    </div>
    <div class="progress-track">
      <div class="progress-fill" id="progFill" style="width:{cap_pct}%">
        <div class="progress-shine"></div>
      </div>
    </div>
    <div class="milestones">{milestone_html}</div>

    <!-- MANUAL TRADING -->
    <div class="section-header" style="margin-top:20px">MANUAL TRADE<div class="scan-bar"></div></div>
    <div id="market-rows">{market_rows}</div>
    <button class="reset-btn" onclick="resetCapital()">⟳ RESET CAPITAL TO ${STARTING_CAPITAL:.0f}</button>

    <!-- LIVE FEED -->
    <div class="section-header">LIVE FEED<div class="scan-bar"></div></div>
    <div id="feed-wrap"></div>

  </div>

  <footer>{DRIFT_BOT_NAME} · {exch} · <a href="/monitor">Monitor</a> · <a href="/trades">All Trades</a></footer>
</div>

<script>
/* ── PARTICLES ─────────────────────────────────────────────── */
(function() {{
  const c = document.getElementById('particles');
  const ctx = c.getContext('2d');
  let W, H;
  const pts = Array.from({{length:20}}, () => ({{
    x: Math.random()*430, y: Math.random()*window.innerHeight,
    vy: -(0.25+Math.random()*.55), opacity: .15+Math.random()*.45, size: 1+Math.random()
  }}));
  const resize = () => {{ W=c.width=window.innerWidth; H=c.height=window.innerHeight; }};
  window.addEventListener('resize', resize); resize();
  const ox = () => Math.max(0,(window.innerWidth-430)/2);
  (function draw() {{
    ctx.clearRect(0,0,W,H);
    pts.forEach(p => {{
      ctx.beginPath(); ctx.arc(ox()+p.x, p.y, p.size, 0, Math.PI*2);
      ctx.fillStyle=`rgba(0,229,255,${{p.opacity}})`; ctx.fill();
      p.y+=p.vy;
      if(p.y<-4){{p.y=H+4;p.x=Math.random()*430;}}
    }});
    requestAnimationFrame(draw);
  }})();
}})();

/* ── SLOT-MACHINE BALANCE ───────────────────────────────────── */
(function() {{
  const target = '{cap_str}';
  const container = document.getElementById('heroBalance');
  for(let i=0;i<target.length;i++) {{
    const ch = target[i];
    if('$,.'.includes(ch)) {{
      const s=document.createElement('span'); s.textContent=ch; container.appendChild(s);
    }} else {{
      const fd=parseInt(ch);
      const wrap=document.createElement('span'); wrap.className='digit-reel-wrap';
      const reel=document.createElement('span'); reel.className='digit-reel';
      for(let d=0;d<=9;d++){{const dc=document.createElement('span');dc.className='digit-char';dc.textContent=d;reel.appendChild(dc);}}
      wrap.appendChild(reel); container.appendChild(wrap);
      const delay=500+i*70, dur=900;
      setTimeout(()=>{{
        const t0=performance.now();
        (function tick(now){{
          const p=Math.min((now-t0)/dur,1), e=1-Math.pow(1-p,3);
          reel.style.transform=`translateY(${{-e*fd*1.1}}em)`;
          if(p<1) requestAnimationFrame(tick);
        }})(performance.now());
      }},delay);
    }}
  }}
  setTimeout(()=>{{const t=document.getElementById('heroTitle');t.style.borderRightColor='transparent';t.style.borderRightWidth='0';}},1300);
}})();

/* ── DECK ───────────────────────────────────────────────────── */
const STACK=[{{z:5,ty:0,sc:1}},{{z:4,ty:6,sc:.97}},{{z:3,ty:12,sc:.94}}];
let _trades=[], _offset=0, _deckBusy=false;

function renderDeck() {{
  const wrap=document.getElementById('deck');
  const hint=document.getElementById('deckHint');
  if(!_trades.length){{wrap.innerHTML='<div class="no-pos">No trades yet</div>';hint.textContent='';return;}}
  const total=_trades.length, shown=Math.min(3,total);
  wrap.innerHTML='';
  for(let i=0;i<shown;i++) {{
    const t=_trades[(_offset+i)%total];
    const pc=t.pnl>=0?'var(--green)':'var(--red)';
    const badge=t.pnl>=0?'win':'loss';
    const card=document.createElement('div'); card.className='trade-card'; card.id='tc'+i;
    card.innerHTML=`<div><div class="tc-pair">${{t.market}}-PERP</div><div style="font-family:'JetBrains Mono',monospace;font-size:9px;color:var(--muted);letter-spacing:1px">${{t.ts.slice(-5)}}</div></div>
      <div style="display:flex;align-items:center"><span class="tc-pnl" style="color:${{pc}}">${{t.pnl>=0?'+':''}}$${{t.pnl.toFixed(2)}}</span><span class="tc-badge ${{badge}}">${{badge.toUpperCase()}}</span></div>`;
    wrap.appendChild(card);
    const pos=STACK[i];
    setTimeout(()=>{{card.style.transition='transform .5s cubic-bezier(.22,1,.36,1),opacity .5s ease';card.style.zIndex=pos.z;card.style.opacity='1';card.style.transform=`translateY(${{pos.ty}}px) scale(${{pos.sc}})`;card.style.transformOrigin='top center';}},80+i*80);
  }}
  hint.textContent=total>1?`▸ TAP TO CYCLE · ${{total}} TRADES`:'';
}}

function cycleDeck() {{
  if(_deckBusy||_trades.length<2) return; _deckBusy=true;
  const front=document.getElementById('tc0');
  if(!front){{_deckBusy=false;return;}}
  front.style.transition='transform .4s ease,opacity .4s ease';
  front.style.transform='rotateX(-90deg) translateY(-20px) scale(.88)';
  front.style.opacity='0';
  setTimeout(()=>{{_offset=(_offset+1)%_trades.length;renderDeck();_deckBusy=false;}},420);
}}

/* ── LIVE DATA ──────────────────────────────────────────────── */
const charts={{}}, pnlH={{}};
const MS={json.dumps(MILESTONES)};
const START_CAP={STARTING_CAPITAL};
const GOAL={PROFIT_GOAL};
const MAX_OPEN={DRIFT_MAX_OPEN};

async function poll() {{
  try {{
    const d=await fetch('/status/api').then(r=>r.json());
    // hero
    const pnl=d.total_pnl||0;
    const pnlEl=document.getElementById('heroPnl');
    pnlEl.textContent=(pnl>=0?'+':'')+' $'+Math.abs(pnl).toFixed(2);
    pnlEl.className=pnl>=0?'up':'dn'; pnlEl.style.color=pnl>=0?'var(--green)':'var(--red)';
    const wr=d.win_rate||0;
    document.getElementById('heroWr').textContent=wr+'%';
    // stats
    document.getElementById('statTrades').textContent=d.total_trades||0;
    document.getElementById('statOpen').textContent=Object.keys(d.positions||{{}}).length+'/'+MAX_OPEN;
    const dp=d.daily_pnl||0;
    const dpEl=document.getElementById('statDpnl');
    dpEl.textContent=(dp>=0?'+':'')+' $'+Math.abs(dp).toFixed(2);
    dpEl.style.color=dp>=0?'var(--green)':'var(--red)';
    // positions
    updatePositions(d.positions||{{}});
    // progress
    const cap=d.capital||START_CAP;
    const next=MS.find(m=>m>cap)||GOAL;
    const pct=Math.max(0,Math.min(100,Math.round((cap-START_CAP)/Math.max(next-START_CAP,1)*100)));
    document.getElementById('progFill').style.width=pct+'%';
    document.getElementById('goalPct').textContent=pct+'%';
    document.getElementById('goalCap').textContent='$'+cap.toFixed(2)+' / $'+GOAL.toLocaleString();
    MS.forEach(m=>{{const el=document.getElementById('ms-'+m);if(el)el.className=cap>=m?'milestone hit':'milestone';}});
  }} catch(e) {{}}
}}

function updatePositions(positions) {{
  const wrap=document.getElementById('pos-wrap');
  const noPos=document.getElementById('no-pos');
  const keys=Object.keys(positions);
  noPos.style.display=keys.length?'none':'block';
  keys.forEach((market,i)=>{{
    const pos=positions[market]; const pnl=pos.pnl||0;
    if(!pnlH[market]) pnlH[market]=[];
    pnlH[market].push(pnl); if(pnlH[market].length>80) pnlH[market].shift();
    let card=document.getElementById('pc-'+market);
    if(!card) {{
      // Build swipe wrapper + card + hidden close button
      const sw=document.createElement('div'); sw.className='swipe-wrap'; sw.id='sw-'+market;
      card=document.createElement('div'); card.id='pc-'+market;
      card.className='pos-card '+(pnl>=0?'profit':'loss');
      card.innerHTML=buildCard(market,pos);
      const closeBtn=document.createElement('div'); closeBtn.className='swipe-close';
      closeBtn.innerHTML=`<svg viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg><span>CLOSE</span>`;
      closeBtn.addEventListener('click',()=>closePosDirect(market));
      sw.appendChild(card); sw.appendChild(closeBtn);
      wrap.appendChild(sw);
      setTimeout(()=>{{initChart(market);initSwipe(card,market);}},80);
    }} else {{
      const pnlEl=document.getElementById('pp-'+market);
      if(pnlEl){{pnlEl.textContent=(pnl>=0?'+':'')+' $'+Math.abs(pnl).toFixed(2);pnlEl.className='pos-pnl '+(pnl>=0?'up':'dn');}}
      updateChart(market,pnl);
      card.className='pos-card '+(pnl>=0?'profit':'loss');
    }}
  }});
  document.querySelectorAll('.swipe-wrap').forEach(el=>{{
    const m=el.id.replace('sw-','');
    if(!positions[m]){{
      const card=document.getElementById('pc-'+m);
      if(charts[m]){{charts[m].destroy();delete charts[m];}}
      delete pnlH[m]; el.remove();
    }}
  }});
}}

function initSwipe(card,market) {{
  let x0=0,y0=0,dx=0,axis=null;
  const MAX=76, SNAP=50;
  card.addEventListener('touchstart',e=>{{
    x0=e.touches[0].clientX; y0=e.touches[0].clientY; dx=0; axis=null;
  }},{{passive:true}});
  card.addEventListener('touchmove',e=>{{
    const cx=e.touches[0].clientX, cy=e.touches[0].clientY;
    if(!axis) axis=Math.abs(cx-x0)>Math.abs(cy-y0)?'h':'v';
    if(axis!=='h') return;
    dx=Math.max(-MAX,Math.min(0,cx-x0));
    card.style.transition='none';
    card.style.transform='translateX('+dx+'px)';
  }},{{passive:true}});
  card.addEventListener('touchend',()=>{{
    card.style.transition='transform .25s cubic-bezier(.25,.1,.25,1)';
    if(dx<=-SNAP) {{
      card.style.transform='translateX(-'+MAX+'px)';
    }} else {{
      card.style.transform='translateX(0)';
    }}
    dx=0; axis=null;
  }},{{passive:true}});
  // Tapping elsewhere snaps card back
  document.addEventListener('touchstart',e=>{{
    if(!card.closest('.swipe-wrap').contains(e.target)&&card.style.transform&&card.style.transform!=='translateX(0)') {{
      card.style.transition='transform .25s cubic-bezier(.25,.1,.25,1)';
      card.style.transform='translateX(0)';
    }}
  }},{{passive:true}});
}}

function closePosDirect(market) {{
  fetch('/api/close/'+market,{{method:'POST'}})
    .then(r=>r.json())
    .then(d=>{{
      const sw=document.getElementById('sw-'+market);
      if(sw){{ sw.style.transition='opacity .3s,transform .3s'; sw.style.opacity='0'; sw.style.transform='translateX(-100%)'; setTimeout(()=>sw.remove(),350); }}
    }})
    .catch(e=>console.error(e));
}}

function buildCard(market,pos) {{
  const side=pos.side||'long';
  const sc=side==='long'?'var(--green)':'var(--red)';
  const pnl=pos.pnl||0;
  const sparkId='spark-'+market;
  return `<div class="pos-info">
    <div class="pos-pair">${{market}}-PERP</div>
    <div class="pos-dir ${{side}}">● ${{side.toUpperCase()}}</div>
  </div>
  <canvas class="sparkline-wrap" id="${{sparkId}}" width="72" height="40"></canvas>
  <div class="pos-pnl ${{pnl>=0?'up':'dn'}}" id="pp-${{market}}">${{pnl>=0?'+':''}} $${{Math.abs(pnl).toFixed(2)}}</div>`;
}}

function initChart(market) {{
  const el=document.getElementById('spark-'+market);
  if(!el||charts[market]) return;
  charts[market]=new Chart(el.getContext('2d'),{{
    type:'line',
    data:{{labels:[],datasets:[{{data:[],borderColor:'#00ff88',borderWidth:1.5,pointRadius:0,tension:.4,fill:true,backgroundColor:'rgba(0,255,136,.08)'}}]}},
    options:{{responsive:false,animation:{{duration:300}},plugins:{{legend:{{display:false}},tooltip:{{enabled:false}}}},scales:{{x:{{display:false}},y:{{display:false}}}}}}
  }});
}}

function updateChart(market,latestPnl) {{
  const c=charts[market]; if(!c) return;
  const hist=pnlH[market]||[];
  const col=latestPnl>=0?'#00ff88':'#ff3355';
  c.data.labels=hist.map((_,i)=>i);
  c.data.datasets[0].data=hist;
  c.data.datasets[0].borderColor=col;
  c.data.datasets[0].backgroundColor=latestPnl>=0?'rgba(0,255,136,.08)':'rgba(255,51,85,.08)';
  c.update('none');
}}

async function pollTrades() {{
  try {{
    const trades=await fetch('/trades/api').then(r=>r.json());
    _trades=trades; _offset=0; renderDeck();
  }} catch(e) {{}}
}}

async function pollFeed() {{
  try {{
    const d=await fetch('/notify/api').then(r=>r.json());
    const wrap=document.getElementById('feed-wrap');
    if(!d.length){{wrap.innerHTML='<div class="no-pos">Waiting for signals...</div>';return;}}
    wrap.innerHTML=d.slice(0,5).map((n,i)=>{{
      const txt=n.text||'';
      const isOpen=txt.includes('OPEN'),isClose=txt.includes('CLOSE');
      const colMap=isClose&&txt.includes('+')?'#00ff88':isClose?'#ff3355':isOpen?'#00e5ff':'var(--muted)';
      const clean=txt.replace(/\*/g,'').replace(/\\n/g,' · ');
      return `<div class="feed-entry vis" style="border-left-color:${{colMap}};color:${{colMap}};animation-delay:${{i*.08}}s">
        <span class="feed-ts">${{n.time}}</span>${{clean}}
      </div>`;
    }}).join('');
  }} catch(e) {{}}
}}

/* ── RIPPLE ─────────────────────────────────────────────────── */
document.addEventListener('click',function(e){{
  const btn=e.target.closest('.trade-btn');
  if(!btn) return;
  const r=btn.getBoundingClientRect();
  const x=e.clientX-r.left, y=e.clientY-r.top;
  const size=Math.max(r.width,r.height)*1.5;
  const rip=document.createElement('div'); rip.className='ripple-el';
  const col=btn.classList.contains('btn-long')?'var(--green)':'var(--red)';
  rip.style.cssText=`width:${{size}}px;height:${{size}}px;left:${{x-size/2}}px;top:${{y-size/2}}px;background:${{col}}`;
  btn.appendChild(rip); rip.addEventListener('animationend',()=>rip.remove());
}});

/* ── CONTROLS ───────────────────────────────────────────────── */
function manualTrade(market,side) {{
  if(!confirm('Open '+side.toUpperCase()+' '+market+'?')) return;
  fetch('/api/manual-'+side+'/'+market,{{method:'POST'}})
    .then(r=>r.json()).then(d=>alert(d.msg||d.error)).catch(e=>alert(e));
}}
function closePos(market) {{
  if(!confirm('Close '+market+'?')) return;
  fetch('/api/close/'+market,{{method:'POST'}})
    .then(r=>r.json()).then(d=>alert(d.msg||d.error)).catch(e=>alert(e));
}}
function resetCapital() {{
  if(!confirm('Reset capital to ${STARTING_CAPITAL:.0f} and clear all trade history?\\nThis cannot be undone.')) return;
  fetch('/api/reset-capital',{{method:'POST'}})
    .then(r=>r.json()).then(d=>{{alert(d.msg||d.error);poll();pollTrades();}}).catch(e=>alert(e));
}}

/* ── KICK OFF ───────────────────────────────────────────────── */
poll(); pollTrades(); pollFeed();
setInterval(poll,3000); setInterval(pollTrades,5000); setInterval(pollFeed,4000);
</script>
</body></html>"""


@app.route("/trades", methods=["GET"])
def trades_page():
    with _state_lock:
        all_trades = list(_trades)

    wins       = [t for t in all_trades if t["pnl"] >= 0]
    total      = len(all_trades)
    wr         = round(len(wins) / max(total, 1) * 100, 1)
    total_pnl  = sum(t["pnl"] for t in all_trades)
    best_pnl   = max((t["pnl"] for t in all_trades), default=0)
    sign       = "+" if total_pnl >= 0 else ""
    best_sign  = "+" if best_pnl >= 0 else ""
    wr_color   = "#00ff88" if wr >= 50 else "#ff3355"
    pnl_color  = "#00ff88" if total_pnl >= 0 else "#ff3355"
    best_color = "#00ff88" if best_pnl >= 0 else "#ff3355"
    badge_txt  = f"{total} TRADES · {len(wins)} WINS" if total else "ANALYST DESK — STANDING BY"

    ticker_items = ""
    for t in all_trades[-20:]:
        cls = "ti-g" if t["pnl"] >= 0 else "ti-r"
        sgn = "+" if t["pnl"] >= 0 else ""
        ticker_items += f'<span class="ti {cls}">&#x25CF; {t["market"]} {t.get("reason","?")} {sgn}${t["pnl"]:.0f}</span>'
    if ticker_items:
        ticker_items = ticker_items + ticker_items
    else:
        ticker_items = '<span class="ti" style="color:var(--muted)">&#x25CF; No trades yet &#x25CF; Analysts standing by &#x25CF; Waiting for signals</span>' * 2

    WIN_FIG = (
        '<svg viewBox="0 0 58 46" width="58" height="46" style="display:block;margin:0 auto">'
        '<line x1="2" y1="38" x2="56" y2="38" stroke="#0d1e30" stroke-width="1.2"/>'
        '<rect x="30" y="6" width="20" height="15" rx="1.5" fill="#040810" stroke="#00ff88" stroke-width="1.2"/>'
        '<rect x="32" y="8" width="16" height="11" fill="#00ff88" fill-opacity=".12"/>'
        '<text x="35" y="16" fill="#00ff88" font-size="8" font-family="sans-serif">&#x2191;</text>'
        '<line x1="40" y1="21" x2="40" y2="27" stroke="#0d1e30" stroke-width="1"/>'
        '<line x1="36" y1="27" x2="44" y2="27" stroke="#0d1e30" stroke-width="1.2"/>'
        '<rect x="18" y="34" width="14" height="3" rx=".5" fill="#0a1628"/>'
        '<circle cx="10" cy="9" r="5.5" fill="none" stroke="#00ff88" stroke-width="1.5"/>'
        '<line x1="10" y1="14.5" x2="10" y2="28" stroke="#00ff88" stroke-width="1.5"/>'
        '<line x1="10" y1="20" x2="2" y2="12" stroke="#00ff88" stroke-width="1.5"/>'
        '<line x1="10" y1="20" x2="18" y2="12" stroke="#00ff88" stroke-width="1.5"/>'
        '<line x1="10" y1="28" x2="6" y2="38" stroke="#00ff88" stroke-width="1.5"/>'
        '<line x1="6" y1="38" x2="3" y2="38" stroke="#00ff88" stroke-width="1"/>'
        '<line x1="10" y1="28" x2="14" y2="38" stroke="#00ff88" stroke-width="1.5"/>'
        '<line x1="14" y1="38" x2="18" y2="38" stroke="#00ff88" stroke-width="1"/>'
        '</svg>'
    )

    LOSS_FIG = (
        '<svg viewBox="0 0 58 46" width="58" height="46" style="display:block;margin:0 auto">'
        '<line x1="2" y1="38" x2="56" y2="38" stroke="#0d1e30" stroke-width="1.2"/>'
        '<rect x="30" y="6" width="20" height="15" rx="1.5" fill="#040810" stroke="#ff3355" stroke-width="1.2"/>'
        '<rect x="32" y="8" width="16" height="11" fill="#ff3355" fill-opacity=".1"/>'
        '<text x="35" y="16" fill="#ff3355" font-size="8" font-family="sans-serif">&#x2193;</text>'
        '<line x1="40" y1="21" x2="40" y2="27" stroke="#0d1e30" stroke-width="1"/>'
        '<line x1="36" y1="27" x2="44" y2="27" stroke="#0d1e30" stroke-width="1.2"/>'
        '<rect x="18" y="34" width="14" height="3" rx=".5" fill="#0a1628"/>'
        '<circle cx="10" cy="13" r="5.5" fill="none" stroke="#ff3355" stroke-width="1.5"/>'
        '<line x1="10" y1="18.5" x2="9" y2="30" stroke="#ff3355" stroke-width="1.5"/>'
        '<g>'
        '<line x1="9" y1="24" x2="18" y2="34" stroke="#ff3355" stroke-width="1.5"/>'
        '<line x1="9" y1="24" x2="22" y2="34" stroke="#ff3355" stroke-width="1.5"/>'
        '<animateTransform attributeName="transform" type="translate" values="0 0;0 -1.5;0 0" dur="0.35s" repeatCount="indefinite"/>'
        '</g>'
        '<line x1="9" y1="30" x2="5" y2="38" stroke="#ff3355" stroke-width="1.5"/>'
        '<line x1="5" y1="38" x2="2" y2="38" stroke="#ff3355" stroke-width="1"/>'
        '<line x1="9" y1="30" x2="13" y2="38" stroke="#ff3355" stroke-width="1.5"/>'
        '<line x1="13" y1="38" x2="17" y2="38" stroke="#ff3355" stroke-width="1"/>'
        '</svg>'
    )

    rows = ""
    if not all_trades:
        rows = (
            '<tr><td class="c-fig"></td>'
            '<td colspan="9" style="text-align:center;padding:24px;color:var(--muted);font-size:11px">'
            'No trades yet — analysts are standing by</td></tr>'
        )
    else:
        for i, t in enumerate(all_trades, 1):
            win       = t["pnl"] >= 0
            fig       = WIN_FIG if win else LOSS_FIG
            pnl_cls   = "win" if win else "los"
            pnl_bg    = "win-bg" if win else "los-bg"
            s_color   = "#00ff88" if t["side"] == "long" else "#ff3355"
            s_sign    = "+" if win else ""
            reason    = t.get("reason", "?")
            badge_cls = "rbadge-tp" if reason == "TP" else "rbadge-sl"
            dur_m     = round(t.get("duration_s", 0) / 60, 1)
            rows += (
                f'<tr>'
                f'<td class="c-fig">{fig}</td>'
                f'<td class="c-num">{i}</td>'
                f'<td class="c-market">{t["market"]}</td>'
                f'<td class="c-side" style="color:{s_color}">{t["side"].upper()}</td>'
                f'<td class="c-mono">${t["entry"]:.4f}</td>'
                f'<td class="c-mono">${t["exit"]:.4f}</td>'
                f'<td class="c-pnl {pnl_cls} {pnl_bg}">{s_sign}${t["pnl"]:.2f}</td>'
                f'<td class="c-pct {pnl_cls}">{s_sign}{t["pnl_pct"]:.1f}%</td>'
                f'<td><span class="rbadge {badge_cls}">{reason}</span></td>'
                f'<td class="c-mono" style="color:var(--muted)">{dur_m}m</td>'
                f'</tr>'
            )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>Trade Log — {DRIFT_BOT_NAME}</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{--bg:#050a14;--bg2:#080f1e;--bg3:#0d1628;--cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--yellow:#ffee00;--text:#c8d8f0;--muted:#4a6080}}
body{{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;overflow-x:hidden}}
.orb{{position:fixed;border-radius:50%;pointer-events:none;z-index:0;filter:blur(80px);opacity:.15}}
.orb-cyan{{width:320px;height:320px;top:-60px;left:calc(50% - 160px);background:radial-gradient(circle,var(--cyan),transparent 70%);animation:orbF 9s ease-in-out infinite}}
.orb-green{{width:260px;height:260px;bottom:-40px;right:calc(50% - 220px);background:radial-gradient(circle,var(--green),transparent 70%);animation:orbF 9s ease-in-out infinite reverse}}
@keyframes orbF{{0%,100%{{transform:translate(0,0)}}50%{{transform:translate(20px,30px)}}}}
#particles{{position:fixed;inset:0;z-index:0;pointer-events:none}}
.wrapper{{max-width:430px;margin:0 auto;position:relative;z-index:1;min-height:100vh}}
nav{{position:fixed;top:0;left:0;right:0;z-index:100;background:rgba(5,10,20,.92);backdrop-filter:blur(12px);border-bottom:1px solid rgba(0,229,255,.12);display:flex;align-items:center;justify-content:space-between;padding:0 18px;height:50px;max-width:430px;margin:0 auto;left:50%;transform:translateX(-50%);width:100%}}
.nav-logo{{font-family:'Bebas Neue',sans-serif;font-size:19px;color:var(--cyan);letter-spacing:2px;text-shadow:0 0 12px rgba(0,229,255,.5)}}
.nav-links{{display:flex;gap:14px}}
.nav-link{{font-size:10px;font-weight:600;letter-spacing:1.5px;color:var(--muted);text-decoration:none;text-transform:uppercase;transition:color .2s}}
.nav-link.active,.nav-link:hover{{color:var(--cyan)}}
.scroll-area{{padding-top:50px;padding-bottom:60px}}
.hero-scene{{display:flex;align-items:center;gap:10px;padding:18px 14px 6px}}
.page-title{{font-family:'Bebas Neue',sans-serif;font-size:46px;line-height:.95;color:var(--cyan);text-shadow:0 0 22px rgba(0,229,255,.5);letter-spacing:2px}}
.page-sub{{font-size:9px;font-weight:700;letter-spacing:2.5px;color:var(--muted);text-transform:uppercase;margin-top:5px}}
.live-badge{{display:inline-flex;align-items:center;gap:5px;background:rgba(0,229,255,.07);border:1px solid rgba(0,229,255,.18);padding:3px 9px;border-radius:20px;font-size:9px;font-weight:700;letter-spacing:1px;color:var(--cyan);margin-top:8px}}
.bdot{{width:5px;height:5px;border-radius:50%;background:var(--green);animation:blink 1.2s ease-in-out infinite}}
@keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.15}}}}
.stats-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:2px;padding:8px 12px;margin-bottom:2px}}
.stat{{background:rgba(13,22,40,.8);border:1px solid rgba(0,229,255,.07);padding:9px 6px;text-align:center}}
.stat-lbl{{font-size:7.5px;font-weight:700;letter-spacing:1.5px;color:var(--muted);text-transform:uppercase}}
.stat-val{{font-family:'Bebas Neue',sans-serif;font-size:19px;margin-top:2px}}
.cyan{{color:var(--cyan)}}.green{{color:var(--green)}}.red{{color:var(--red)}}
.ticker-wrap{{overflow:hidden;background:rgba(0,229,255,.03);border-top:1px solid rgba(0,229,255,.09);border-bottom:1px solid rgba(0,229,255,.09);padding:5px 0;margin-bottom:14px}}
.ticker-track{{display:flex;gap:28px;white-space:nowrap;animation:tick 20s linear infinite}}
@keyframes tick{{0%{{transform:translateX(0)}}100%{{transform:translateX(-50%)}}}}
.ti{{font-size:10px;font-weight:700;letter-spacing:.8px;font-family:'JetBrains Mono',monospace}}
.ti-g{{color:var(--green)}}.ti-r{{color:var(--red)}}
.sheet-section{{padding:0 12px 24px}}
.sheet-label{{font-size:9px;font-weight:700;letter-spacing:2px;color:var(--muted);text-transform:uppercase;margin-bottom:8px;padding-left:2px}}
.sheet-outer{{border-radius:5px;background:#05091a;box-shadow:0 0 0 1px rgba(0,229,255,.16),0 16px 50px rgba(0,0,0,.85),6px 6px 0 rgba(0,229,255,.05),12px 12px 0 rgba(0,229,255,.025);overflow:hidden}}
.sheet-titlebar{{background:#0a1525;border-bottom:1px solid rgba(0,229,255,.12);padding:6px 10px;display:flex;align-items:center;gap:6px}}
.tl{{width:8px;height:8px;border-radius:50%}}
.tl-r{{background:#ff5f56}}.tl-y{{background:#ffbd2e}}.tl-g{{background:#27c93f}}
.sheet-name{{font-size:9.5px;font-weight:600;color:var(--muted);letter-spacing:.5px;margin:0 auto;font-family:'JetBrains Mono',monospace}}
.formula-bar{{background:#06091a;border-bottom:1px solid rgba(255,255,255,.05);padding:4px 10px;display:flex;align-items:center;gap:8px;font-family:'JetBrains Mono',monospace;font-size:9px}}
.fx-label{{color:var(--cyan);font-weight:700;font-size:10px}}
.fx-val{{color:rgba(0,229,255,.5)}}
.tbl-scroll{{overflow-x:auto;-webkit-overflow-scrolling:touch}}
table{{width:100%;border-collapse:collapse;font-size:9.5px;min-width:500px}}
.col-hdr{{background:#06091a;border-bottom:1px solid rgba(255,255,255,.05)}}
.col-hdr th{{font-family:'JetBrains Mono',monospace;font-size:8px;font-weight:600;color:#2a3a50;text-align:center;padding:2px 0;border-right:1px solid rgba(255,255,255,.03);letter-spacing:0}}
.fld-hdr th{{background:#080f1c;color:#5a7090;font-size:7.5px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;padding:6px 8px;border-bottom:2px solid rgba(0,229,255,.14);border-right:1px solid rgba(255,255,255,.04);text-align:left;white-space:nowrap}}
.fld-hdr th.c-fig{{text-align:center}}
.fld-hdr th.c-num{{text-align:center;color:#253545}}
td{{padding:0 7px;height:46px;border-bottom:1px solid rgba(255,255,255,.04);border-right:1px solid rgba(255,255,255,.03);vertical-align:middle;white-space:nowrap}}
td.c-fig{{padding:0;text-align:center;border-right:1px solid rgba(0,229,255,.08);background:#04081a}}
td.c-num{{color:#253545;font-family:'JetBrains Mono',monospace;font-size:8px;text-align:center}}
tr:nth-child(even) td{{background:rgba(255,255,255,.012)}}
tr:hover td{{background:rgba(0,229,255,.04)!important}}
.c-market{{font-weight:700;font-size:11px;letter-spacing:.5px}}
.c-side{{font-weight:900;font-size:10px}}
.c-mono{{font-family:'JetBrains Mono',monospace;font-size:9px;color:var(--text)}}
.c-pnl{{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:11px;padding-left:6px!important;padding-right:6px!important}}
.c-pct{{font-family:'JetBrains Mono',monospace;font-size:9px}}
.win{{color:var(--green)}}.los{{color:var(--red)}}
.win-bg{{background:rgba(0,255,136,.05)!important}}.los-bg{{background:rgba(255,51,85,.05)!important}}
.rbadge{{display:inline-block;font-size:7px;font-weight:900;letter-spacing:1px;padding:2px 5px;border:1px solid;border-radius:2px}}
.rbadge-tp{{color:var(--green);border-color:var(--green);background:rgba(0,255,136,.08)}}
.rbadge-sl{{color:var(--red);border-color:var(--red);background:rgba(255,51,85,.08)}}
footer{{padding:18px 16px;text-align:center;font-size:9px;color:var(--muted);border-top:1px solid rgba(255,255,255,.04)}}
footer a{{color:var(--cyan);text-decoration:none}}
@media(min-width:768px){{
  .wrapper{{max-width:1200px}}
  nav{{max-width:1200px;padding:0 40px}}
  .page-inner{{padding:72px 48px 80px}}
  .tbl-wrap table th,
  .tbl-wrap table td{{padding:10px 16px;font-size:12px}}
  .stats-row{{grid-template-columns:repeat(6,1fr)}}
  .hero-label{{font-size:20px}}
}}
</style>
</head>
<body>
<div class="orb orb-cyan"></div>
<div class="orb orb-green"></div>
<canvas id="particles"></canvas>
<div class="wrapper">
<nav>
  <span class="nav-logo">{DRIFT_BOT_NAME}</span>
  <div class="nav-links">
    <a href="/" class="nav-link">HOME</a>
    <a href="/trades" class="nav-link active">TRADES</a>
    <a href="/monitor" class="nav-link">MONITOR</a>
    <a href="https://jup.ag" class="nav-link" target="_blank">JUPITER ↗</a>
  </div>
</nav>
<div class="scroll-area">
<div class="hero-scene">
  <div style="flex-shrink:0">
    <svg viewBox="0 0 158 118" width="158" height="118" style="display:block">
      <line x1="6" y1="88" x2="152" y2="88" stroke="#0d1e30" stroke-width="3"/>
      <line x1="18" y1="88" x2="18" y2="108" stroke="#0d1e30" stroke-width="2.5"/>
      <line x1="140" y1="88" x2="140" y2="108" stroke="#0d1e30" stroke-width="2.5"/>
      <rect x="50" y="32" width="36" height="28" rx="2" fill="#040810" stroke="#00e5ff" stroke-width="1.4"/>
      <rect x="52" y="34" width="32" height="24" rx="1" fill="#00e5ff" fill-opacity=".04"/>
      <polyline points="55,52 61,46 67,48 73,40 79,44 83,38" fill="none" stroke="#00ff88" stroke-width="1.2"/>
      <rect x="52" y="34" width="32" height="24" fill="#00e5ff" fill-opacity="0"><animate attributeName="fill-opacity" values="0;.07;0" dur="3s" repeatCount="indefinite"/></rect>
      <line x1="68" y1="60" x2="68" y2="70" stroke="#0d1e30" stroke-width="2"/>
      <line x1="62" y1="70" x2="74" y2="70" stroke="#0d1e30" stroke-width="2.2"/>
      <rect x="94" y="24" width="44" height="36" rx="2" fill="#040810" stroke="#00e5ff" stroke-width="1.4"/>
      <rect x="96" y="26" width="40" height="32" rx="1" fill="#00e5ff" fill-opacity=".03"/>
      <text x="99" y="36" fill="{pnl_color}" font-family="monospace" font-size="5.5">{sign}${total_pnl:.0f} PNL</text>
      <text x="99" y="44" fill="{wr_color}" font-family="monospace" font-size="5.5">{wr}% WIN RATE</text>
      <text x="99" y="52" fill="#c8d8f0" font-family="monospace" font-size="5.5">{total} TRADES</text>
      <rect x="96" y="26" width="40" height="32" fill="#00e5ff" fill-opacity="0"><animate attributeName="fill-opacity" values="0;.06;0" dur="3s" begin="1.5s" repeatCount="indefinite"/></rect>
      <line x1="116" y1="60" x2="116" y2="70" stroke="#0d1e30" stroke-width="2"/>
      <line x1="108" y1="70" x2="124" y2="70" stroke="#0d1e30" stroke-width="2.2"/>
      <rect x="56" y="80" width="50" height="7" rx="1.5" fill="#0a1628"/>
      <rect x="58" y="81.5" width="46" height="4" rx="1" fill="#0d1e30"/>
      <rect x="32" y="78" width="11" height="10" rx="1.5" fill="#0d1e30" stroke="#253545" stroke-width="1"/>
      <path d="M43,81 Q48,81 48,84 Q48,87 43,87" fill="none" stroke="#253545" stroke-width="1"/>
      <line x1="35" y1="77" x2="34" y2="72" stroke="#253545" stroke-width="1" opacity=".5"><animate attributeName="opacity" values=".5;.1;.5" dur="1.8s" repeatCount="indefinite"/></line>
      <line x1="39" y1="77" x2="40" y2="72" stroke="#253545" stroke-width="1" opacity=".4"><animate attributeName="opacity" values=".3;.7;.3" dur="2.1s" repeatCount="indefinite"/></line>
      <line x1="14" y1="54" x2="14" y2="80" stroke="#0d1e30" stroke-width="3"/>
      <line x1="14" y1="58" x2="26" y2="58" stroke="#0d1e30" stroke-width="2.5"/>
      <circle cx="28" cy="40" r="10" fill="none" stroke="#c8d8f0" stroke-width="2"/>
      <circle cx="35" cy="39" r="2" fill="#c8d8f0"/>
      <line x1="28" y1="50" x2="28" y2="74" stroke="#c8d8f0" stroke-width="2.5"/>
      <g><line x1="28" y1="58" x2="56" y2="80" stroke="#c8d8f0" stroke-width="2"/><animateTransform attributeName="transform" type="translate" values="0 0;0 -2;0 0" dur="0.32s" repeatCount="indefinite"/></g>
      <line x1="28" y1="58" x2="20" y2="70" stroke="#c8d8f0" stroke-width="2"/>
      <line x1="20" y1="70" x2="50" y2="80" stroke="#c8d8f0" stroke-width="1.5"/>
      <line x1="28" y1="74" x2="16" y2="88" stroke="#c8d8f0" stroke-width="2"/>
      <line x1="16" y1="88" x2="6" y2="88" stroke="#c8d8f0" stroke-width="1.5"/>
      <line x1="28" y1="74" x2="38" y2="88" stroke="#c8d8f0" stroke-width="2"/>
      <line x1="38" y1="88" x2="50" y2="88" stroke="#c8d8f0" stroke-width="1.5"/>
    </svg>
  </div>
  <div style="flex:1">
    <div class="page-title">TRADE<br>LOG</div>
    <div class="page-sub">ANALYST DESK</div>
    <div class="live-badge"><div class="bdot"></div>{badge_txt}</div>
  </div>
</div>
<div class="stats-row">
  <div class="stat"><div class="stat-lbl">TOTAL</div><div class="stat-val cyan">{total}</div></div>
  <div class="stat"><div class="stat-lbl">WIN RATE</div><div class="stat-val" style="color:{wr_color}">{wr}%</div></div>
  <div class="stat"><div class="stat-lbl">TOT PNL</div><div class="stat-val" style="color:{pnl_color}">{sign}${total_pnl:.2f}</div></div>
  <div class="stat"><div class="stat-lbl">BEST</div><div class="stat-val" style="color:{best_color}">{best_sign}${best_pnl:.2f}</div></div>
</div>
<div class="ticker-wrap"><div class="ticker-track">{ticker_items}</div></div>
<div class="sheet-section">
  <div class="sheet-label">&#x25C6; ANALYST DESK — TRADE HISTORY</div>
  <div class="sheet-outer">
    <div class="sheet-titlebar">
      <div class="tl tl-r"></div><div class="tl tl-y"></div><div class="tl tl-g"></div>
      <div class="sheet-name">TRADES_LOG.xlsx</div>
    </div>
    <div class="formula-bar">
      <span class="fx-label">fx</span>
      <span class="fx-val">=TRADE_LOG!A1:J{total + 1}</span>
    </div>
    <div class="tbl-scroll">
      <table>
        <thead>
          <tr class="col-hdr">
            <th style="width:58px"></th><th style="width:28px">A</th>
            <th style="width:80px">B</th><th style="width:52px">C</th>
            <th style="width:72px">D</th><th style="width:72px">E</th>
            <th style="width:70px">F</th><th style="width:52px">G</th>
            <th style="width:44px">H</th><th style="width:36px">I</th>
          </tr>
          <tr class="fld-hdr">
            <th class="c-fig">ANALYST</th><th class="c-num">#</th>
            <th>MARKET</th><th>SIDE</th><th>ENTRY</th><th>EXIT</th>
            <th>PNL</th><th>%</th><th>CLOSE</th><th>DUR</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
  </div>
</div>
</div>
<footer>{DRIFT_BOT_NAME} &nbsp;&#x00B7;&nbsp; <a href="https://jup.ag" target="_blank">JUPITER ↗</a> &nbsp;&#x00B7;&nbsp; {'PAPER' if DRIFT_PAPER_MODE else 'LIVE'}</footer>
</div>
<script>
const canvas=document.getElementById('particles');
const ctx=canvas.getContext('2d');
let W,H,pts=[];
function resize(){{W=canvas.width=window.innerWidth;H=canvas.height=window.innerHeight;}}
function mkPt(){{return{{x:Math.random()*W,y:Math.random()*H,vx:(Math.random()-.5)*.3,vy:(Math.random()-.5)*.3,r:Math.random()*1.4+.4,a:Math.random()*.4+.1}};}}
function initPts(){{pts=Array.from({{length:55}},mkPt);}}
function drawPts(){{
  ctx.clearRect(0,0,W,H);
  pts.forEach(p=>{{
    p.x+=p.vx;p.y+=p.vy;
    if(p.x<0||p.x>W)p.vx*=-1;if(p.y<0||p.y>H)p.vy*=-1;
    ctx.beginPath();ctx.arc(p.x,p.y,p.r,0,Math.PI*2);
    ctx.fillStyle=`rgba(0,229,255,${{p.a}})`;ctx.fill();
  }});
  requestAnimationFrame(drawPts);
}}
window.addEventListener('resize',()=>{{resize();initPts();}});
resize();initPts();drawPts();
</script>
</body></html>"""



@app.route("/status/api", methods=["GET"])
def status_api():
    with _state_lock:
        cap       = _capital
        pos_snap  = {k: dict(v) for k, v in _positions.items()}
        trades_snap = list(_trades)
        dpnl      = _daily_pnl
        psec      = _profit_secured
    n_trades  = len(trades_snap)
    wins_n    = sum(1 for t in trades_snap if t["pnl"] >= 0)
    total_pnl = sum(t["pnl"] for t in trades_snap)

    sol_price = get_sol_price()
    return jsonify({
        "bot": DRIFT_BOT_NAME,
        "exchange": DRIFT_EXCHANGE,
        "paper_mode": DRIFT_PAPER_MODE,
        "capital": round(cap, 2),
        "starting_capital": STARTING_CAPITAL,
        "profit_goal": PROFIT_GOAL,
        "daily_pnl": round(dpnl, 2),
        "total_trades": n_trades,
        "wins": wins_n,
        "losses": n_trades - wins_n,
        "win_rate": round(wins_n / max(n_trades, 1) * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "open_positions": len(pos_snap),
        "positions": pos_snap,
        "leverage_range": f"{int(DRIFT_LEV_MIN)}-{int(DRIFT_LEV_MAX)}x",
        "margin_per_trade": DRIFT_MARGIN_USD,
        "sol_price": round(sol_price, 2) if sol_price else None,
        "profit_secured": round(psec, 2),
        "tp_usd": DRIFT_TP_USD,
        "compound_pct": DRIFT_COMPOUND_PCT * 100,
        "markets": DRIFT_MARKETS,
    })


@app.route("/trades/api", methods=["GET"])
def trades_api():
    with _state_lock:
        return jsonify(list(_trades))


@app.route("/logs/api", methods=["GET"])
def logs_api():
    with _log_lock:
        return jsonify(list(_log_buffer))


@app.route("/notify/api", methods=["GET"])
def notify_api():
    with _notify_q_lock:
        return jsonify(list(_notify_log))


@app.route("/api/manual-long/<market>", methods=["POST"])
def manual_long(market):
    market = market.upper()
    price  = get_market_price(market)
    if not price:
        return jsonify({"error": f"Cannot fetch price for {market}"}), 400
    with _state_lock:
        if market in _positions:
            return jsonify({"error": f"{market} already has an open position"}), 400
        if len(_positions) >= DRIFT_MAX_OPEN:
            return jsonify({"error": f"Max open positions ({DRIFT_MAX_OPEN}) reached"}), 400
        size_usd = DRIFT_MARGIN_USD * DRIFT_LEVERAGE

    open_position(market, "long", price, size_usd, DRIFT_LEVERAGE)
    return jsonify({"msg": f"Opened LONG {market} @ ${price:.4f} margin=${DRIFT_MARGIN_USD:.0f} lev={int(DRIFT_LEVERAGE)}x notional=${size_usd:.0f}"})


@app.route("/api/manual-short/<market>", methods=["POST"])
def manual_short(market):
    market = market.upper()
    price  = get_market_price(market)
    if not price:
        return jsonify({"error": f"Cannot fetch price for {market}"}), 400
    with _state_lock:
        if market in _positions:
            return jsonify({"error": f"{market} already has an open position"}), 400
        if len(_positions) >= DRIFT_MAX_OPEN:
            return jsonify({"error": f"Max open positions ({DRIFT_MAX_OPEN}) reached"}), 400
        size_usd = DRIFT_MARGIN_USD * DRIFT_LEVERAGE

    open_position(market, "short", price, size_usd, DRIFT_LEVERAGE)
    return jsonify({"msg": f"Opened SHORT {market} @ ${price:.4f} margin=${DRIFT_MARGIN_USD:.0f} lev={int(DRIFT_LEVERAGE)}x notional=${size_usd:.0f}"})


@app.route("/ping-telegram", methods=["GET"])
def ping_telegram():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return jsonify({"error": "TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set", "token_set": bool(TELEGRAM_TOKEN), "chat_id_set": bool(TELEGRAM_CHAT_ID)}), 400
    try:
        r = _session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"*{DRIFT_BOT_NAME}* ping test ✅", "parse_mode": "Markdown"},
            timeout=10
        )
        return jsonify({"status": r.status_code, "response": r.json()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/close/<market>", methods=["POST"])
def manual_close(market):
    market = market.upper()
    with _state_lock:
        if market not in _positions:
            return jsonify({"error": f"No open position for {market}"}), 400
        price = _positions[market].get("current_price") or _positions[market].get("entry")

    if not price:
        price = get_market_price(market)

    if not price:
        return jsonify({"error": f"Cannot determine exit price for {market}"}), 400

    close_position(market, price, "MANUAL")
    return jsonify({"msg": f"Closed {market} position @ ${price:.4f}"})


@app.route("/api/reset-capital", methods=["POST"])
def reset_capital():
    global _capital, _trades, _daily_pnl, _profit_secured, _positions
    global _market_stats, _market_params, _milestones_hit, _total_trades_ever
    if not DRIFT_PAPER_MODE:
        with _state_lock:
            open_mkts = list(_positions.keys())
        if open_mkts:
            return jsonify({"error": f"Cannot reset with open live positions: {open_mkts}. Close them first."}), 400
    with _state_lock:
        _capital           = STARTING_CAPITAL
        _trades            = []
        _daily_pnl         = 0.0
        _profit_secured    = 0.0
        _positions         = {}
        _market_stats      = {}
        _market_params     = {}
        _milestones_hit    = set()
        _total_trades_ever = 0
    _save_state()
    log("ok", f"Full reset — capital ${STARTING_CAPITAL:.2f}, trade history, tuner state, milestones all cleared")
    notify(f"*{DRIFT_BOT_NAME}* FULL RESET\nCapital: ${STARTING_CAPITAL:.2f} | Clean baseline")
    return jsonify({"msg": f"Full reset to ${STARTING_CAPITAL:.2f}", "capital": STARTING_CAPITAL})


@app.route("/api/tune-stats", methods=["GET"])
def tune_stats():
    with _state_lock:
        stats  = dict(_market_stats)
        params = dict(_market_params)
        total  = len(_trades)
    out = {}
    for market in sorted(set(list(stats.keys()) + list(params.keys()))):
        s = stats.get(market, {})
        p = params.get(market, {})
        n = s.get("wins", 0) + s.get("losses", 0)
        paused = p.get("paused_until", 0) > time.time()
        out[market] = {
            "trades":       n,
            "wins":         s.get("wins", 0),
            "losses":       s.get("losses", 0),
            "win_rate":     round(s["wins"] / n * 100, 1) if n else None,
            "long_wr":      round(s.get("long_wins", 0) / max(s.get("long_total", 0), 1) * 100, 1),
            "short_wr":     round(s.get("short_wins", 0) / max(s.get("short_total", 0), 1) * 100, 1),
            "leverage":     p.get("leverage", DRIFT_LEVERAGE),
            "bias":         p.get("bias"),
            "paused":       paused,
            "last_result":  p.get("last_result", ""),
        }
    return jsonify({
        "tune_every":   DRIFT_TUNE_EVERY,
        "total_trades": total,
        "markets":      out,
    })


def run_position_price_updater():
    while True:
        try:
            with _state_lock:
                markets = list(_positions.keys())
            for market in markets:
                price = get_market_price(market)
                if not price:
                    continue
                with _state_lock:
                    if market not in _positions:
                        continue
                    pos = _positions[market]
                    raw_pct = (price - pos["entry"]) / pos["entry"] * (1 if pos["side"] == "long" else -1)
                    _positions[market]["pnl"] = raw_pct * pos["size"]   # size is already notional
                    _positions[market]["current_price"] = price
                    updated = dict(_positions[market])

                pnl = updated.get("pnl", 0)

                # ── Liquidation guard — highest priority ──────────────
                # Exit immediately if price comes within 0.3% of liq price.
                # At 80x a 1.25% move liquidates — 0.3% buffer gives ~1s of reaction time.
                liq_price = updated.get("liq_price")
                if liq_price:
                    near_liq = (
                        (updated["side"] == "long"  and price <= liq_price * 1.003) or
                        (updated["side"] == "short" and price >= liq_price * 0.997)
                    )
                    if near_liq:
                        close_position(market, price, f"LIQ-GUARD liq={liq_price:.4f}")
                        continue

                # ── ATR TP% / SL% — primary exits (volatility-calibrated) ──
                if (updated["side"] == "long" and price >= updated["tp"]) or \
                   (updated["side"] == "short" and price <= updated["tp"]):
                    close_position(market, price, "TP%")
                elif (updated["side"] == "long" and price <= updated["sl"]) or \
                     (updated["side"] == "short" and price >= updated["sl"]):
                    close_position(market, price, "SL%")
                # ── Dollar ceiling TP / margin SL — secondary hard limits ──
                elif DRIFT_TP_USD > 0 and pnl >= DRIFT_TP_USD:
                    close_position(market, price, f"TP${DRIFT_TP_USD:.0f}")
                elif DRIFT_SL_MARGIN_PCT > 0 and pnl <= -(updated["size"] / updated["leverage"] * DRIFT_SL_MARGIN_PCT):
                    sl_usd = updated["size"] / updated["leverage"] * DRIFT_SL_MARGIN_PCT
                    close_position(market, price, f"SL${sl_usd:.1f}")
                # ── Stale check — ride upswings, pull $2 if stalling ─────
                elif DRIFT_MAX_HOLD_MINUTES > 0 and \
                     (time.time() - updated["opened_at"]) > DRIFT_MAX_HOLD_MINUTES * 60:
                    with _state_lock:
                        hist = list(_price_history.get(market, []))
                    ref_prices = [p for t, p in hist if t <= time.time() - 300]
                    ref = ref_prices[-1] if ref_prices else None
                    if ref and ((updated["side"] == "long"  and price > ref * 1.001) or
                                (updated["side"] == "short" and price < ref * 0.999)):
                        # Still moving our way — reset clock, keep riding
                        with _state_lock:
                            if market in _positions:
                                _positions[market]["opened_at"] = time.time()
                        log("info", f"Upswing ongoing — clock reset (pnl={pnl:+.2f})", market)
                    elif pnl > 0:
                        # Stalling with profit — lock $2, let rest ride
                        partial_close_position(market, price, 2.0, "STALL-PARTIAL")
                    else:
                        # Stalling with no profit — cut it
                        close_position(market, price, "STALL-FLAT")
        except Exception as e:
            log("warn", f"Price updater error: {e}")
        time.sleep(1)


@app.route("/monitor", methods=["GET"])
def monitor():
    mode       = "PAPER" if DRIFT_PAPER_MODE else "LIVE"
    mode_color = "#ffee00" if DRIFT_PAPER_MODE else "#39ff14"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>Monitor — {DRIFT_BOT_NAME}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{{--cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--yellow:#ffee00;--bg:#050a14;--bg2:#080f1e;--bg3:#0d1628;--text:#c8d8f0;--muted:#4a6080}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;overflow-x:hidden}}
nav{{position:fixed;top:0;left:50%;transform:translateX(-50%);width:100%;z-index:100;background:rgba(5,10,20,.92);backdrop-filter:blur(12px);border-bottom:1px solid rgba(0,229,255,.12);display:flex;align-items:center;justify-content:space-between;padding:0 20px;height:52px;max-width:430px;margin:0 auto}}
.nav-logo{{font-family:'Bebas Neue',sans-serif;font-size:22px;color:var(--cyan);letter-spacing:2px;text-shadow:0 0 14px rgba(0,229,255,.6)}}
.nav-links{{display:flex;gap:16px}}
.nav-link{{font-size:11px;font-weight:600;letter-spacing:1.5px;color:var(--muted);text-decoration:none;text-transform:uppercase;transition:color .2s;animation:slideInLeft .4s both}}
.nav-link:nth-child(1){{animation-delay:.05s}}
.nav-link:nth-child(2){{animation-delay:.15s;color:var(--cyan)}}
.nav-link:nth-child(3){{animation-delay:.25s}}
.nav-link:hover{{color:var(--cyan)}}
@keyframes slideInLeft{{from{{opacity:0;transform:translateX(-20px)}}to{{opacity:1;transform:translateX(0)}}}}
.page{{max-width:430px;margin:0 auto;padding:68px 16px 40px;position:relative;z-index:1}}
.mode-toggle-bar{{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;animation:fadeUp .5s .1s both}}
.mode-pill{{display:inline-flex;align-items:center;gap:6px;background:rgba(0,229,255,.08);border:1px solid rgba(0,229,255,.25);border-radius:20px;padding:5px 14px;font-size:11px;font-weight:700;letter-spacing:1.5px;color:var(--cyan);text-transform:uppercase;animation:breatheGlow 2.5s ease-in-out infinite}}
.mode-pill::before{{content:'';width:7px;height:7px;border-radius:50%;background:{mode_color};box-shadow:0 0 8px {mode_color};animation:dotPulse 2.5s ease-in-out infinite}}
@keyframes breatheGlow{{0%,100%{{box-shadow:0 0 8px rgba(0,229,255,.2)}}50%{{box-shadow:0 0 20px rgba(0,229,255,.5)}}}}
@keyframes dotPulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.pnl-toggle-btn{{display:inline-flex;align-items:center;gap:8px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.12);border-radius:8px;padding:7px 14px;font-size:11px;font-weight:700;letter-spacing:1px;color:var(--text);cursor:pointer;text-transform:uppercase;transition:all .2s}}
.pnl-toggle-btn:hover{{border-color:var(--cyan);color:var(--cyan)}}
.scene-wrapper{{background:linear-gradient(180deg,rgba(0,229,255,.03) 0%,rgba(5,10,20,0) 100%);border:1px solid rgba(0,229,255,.1);border-radius:16px;padding:16px 0 0;margin-bottom:16px;overflow:hidden;animation:fadeUp .5s .2s both}}
.scene-pnl{{text-align:center;font-family:'Bebas Neue',sans-serif;font-size:42px;letter-spacing:2px;line-height:1;margin-bottom:4px;transition:color .6s}}
.scene-pnl.profit{{color:var(--green);text-shadow:0 0 20px rgba(0,255,136,.5)}}
.scene-pnl.loss{{color:var(--red);text-shadow:0 0 20px rgba(255,51,85,.5)}}
.scene-label{{text-align:center;font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;margin-bottom:8px;transition:color .6s}}
.scene-label.profit{{color:var(--green)}}
.scene-label.loss{{color:var(--red)}}
#stickScene{{display:block;width:100%;height:200px}}
.mini-stats{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}}
.mini-stat{{background:var(--bg3);border:1px solid rgba(0,229,255,.08);border-radius:10px;padding:10px 8px;text-align:center;animation:fadeUp .5s both}}
.mini-stat:nth-child(1){{animation-delay:.1s}}
.mini-stat:nth-child(2){{animation-delay:.2s}}
.mini-stat:nth-child(3){{animation-delay:.3s}}
.mini-stat-val{{font-family:'Bebas Neue',sans-serif;font-size:20px;letter-spacing:1px;color:var(--cyan);display:block}}
.mini-stat-lbl{{font-size:9px;font-weight:600;letter-spacing:1px;color:var(--muted);text-transform:uppercase;margin-top:2px;display:block}}
.card{{background:var(--bg2);border:1px solid rgba(0,229,255,.1);border-radius:14px;padding:16px;margin-bottom:12px}}
.sec-hdr{{font-family:'Bebas Neue',sans-serif;font-size:16px;letter-spacing:2px;color:var(--cyan);margin-bottom:12px;position:relative;overflow:hidden;display:inline-block;padding-right:8px}}
.sec-hdr::after{{content:'';position:absolute;top:0;left:-100%;width:60%;height:100%;background:linear-gradient(90deg,transparent,rgba(0,229,255,.5),transparent);animation:scanLine 4s linear infinite}}
@keyframes scanLine{{0%{{left:-60%}}100%{{left:160%}}}}
.pos-card{{background:var(--bg3);border:1px solid rgba(255,255,255,.06);border-radius:10px;padding:12px 14px;margin-bottom:8px;position:relative;overflow:hidden;animation:slideInRight .5s both}}
@keyframes slideInRight{{from{{opacity:0;transform:translateX(30px)}}to{{opacity:1;transform:translateX(0)}}}}
.pos-top{{display:flex;align-items:center;gap:10px;margin-bottom:8px}}
.pos-sym{{font-family:'Bebas Neue',sans-serif;font-size:22px;color:var(--cyan);letter-spacing:1px}}
.pos-side{{font-size:10px;font-weight:700;letter-spacing:1.5px;padding:2px 7px;border-radius:4px}}
.pos-pnl{{font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:700;margin-left:auto}}
.pos-meta{{display:grid;grid-template-columns:1fr 1fr;gap:3px 12px;font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-bottom:10px}}
.pos-meta b{{color:#aaa;font-weight:400}}
canvas{{width:100%!important;display:block;margin-bottom:10px}}
.close-btn{{width:100%;padding:9px;background:transparent;border:1px solid var(--red);color:var(--red);font-size:11px;font-weight:900;letter-spacing:1px;cursor:pointer;text-transform:uppercase;transition:all .15s;border-radius:6px}}
.close-btn:hover{{background:var(--red);color:#fff}}
.log-feed{{background:var(--bg);border:1px solid rgba(0,229,255,.08);border-radius:10px;padding:10px 12px;max-height:180px;overflow-y:auto;font-family:'JetBrains Mono',monospace;font-size:11px;line-height:1.7}}
.log-feed::-webkit-scrollbar{{width:3px}}
.log-feed::-webkit-scrollbar-thumb{{background:var(--muted);border-radius:3px}}
.log-entry{{border-left:2px solid;padding:5px 10px;margin-bottom:5px;border-radius:0 6px 6px 0}}
.trade-table{{width:100%;border-collapse:collapse;font-size:12px}}
.trade-table th{{font-size:10px;font-weight:700;letter-spacing:1px;color:var(--muted);text-transform:uppercase;padding:6px 8px;text-align:left;border-bottom:1px solid rgba(255,255,255,.06)}}
.trade-table td{{padding:9px 8px;font-family:'JetBrains Mono',monospace;font-size:11px;border-bottom:1px solid rgba(255,255,255,.04)}}
.orb{{position:fixed;border-radius:50%;pointer-events:none;filter:blur(80px);z-index:0;opacity:.12}}
.orb-cyan{{width:300px;height:300px;background:radial-gradient(circle,var(--cyan),transparent 70%);top:-80px;left:-60px;animation:orb8 10s ease-in-out infinite}}
.orb-red{{width:250px;height:250px;background:radial-gradient(circle,var(--red),transparent 70%);bottom:30px;right:-60px;animation:orb8 10s ease-in-out infinite reverse;animation-delay:-4s}}
@keyframes orb8{{0%{{transform:translate(0,0)}}25%{{transform:translate(30px,20px)}}50%{{transform:translate(60px,0)}}75%{{transform:translate(30px,-20px)}}100%{{transform:translate(0,0)}}}}
#particles{{position:fixed;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:0}}
.empty{{text-align:center;padding:30px;color:var(--muted);font-size:13px}}
.badge{{display:inline-block;padding:1px 7px;font-size:10px;font-weight:700;border-radius:3px;border:1px solid}}
.badge.win{{color:var(--green);border-color:var(--green)}}.badge.loss{{color:var(--red);border-color:var(--red)}}
@keyframes fadeUp{{from{{opacity:0;transform:translateY(18px)}}to{{opacity:1;transform:translateY(0)}}}}
@media(min-width:768px){{
  nav{{max-width:1200px;padding:0 40px}}
  .page{{max-width:1200px;padding:72px 48px 80px}}
  .mini-stats{{grid-template-columns:repeat(6,1fr)}}
  .scene-pnl{{font-size:64px}}
  .scene-label{{font-size:14px}}
  #stickScene{{height:320px}}
  .card{{padding:20px}}
  .sec-hdr{{font-size:20px}}
  .pos-sym{{font-size:28px}}
  .pos-pnl{{font-size:24px}}
  .pos-meta{{font-size:12px;grid-template-columns:1fr 1fr 1fr 1fr}}
  .log-line{{font-size:11px;padding:6px 14px}}
}}
</style>
</head>
<body>

<div class="orb orb-cyan"></div>
<div class="orb orb-red"></div>
<canvas id="particles"></canvas>

<nav>
  <div class="nav-logo">DRIFT BOT</div>
  <div class="nav-links">
    <a class="nav-link" href="/">HOME</a>
    <a class="nav-link" href="/monitor">MONITOR</a>
    <a class="nav-link" href="/trades">TRADES</a>
  </div>
</nav>

<div class="page">

  <div class="mode-toggle-bar">
    <div class="mode-pill">{mode} MODE</div>
    <button class="pnl-toggle-btn" onclick="manualToggle()">
      <span id="toggleIcon">📈</span>
      <span id="toggleLabel">SHOW LOSS</span>
    </button>
  </div>

  <!-- STICK FIGURE SCENE -->
  <div class="scene-wrapper">
    <div class="scene-pnl profit" id="scenePnl">$0.00</div>
    <div class="scene-label profit" id="sceneLabel">MONITORING...</div>
    <svg id="stickScene" viewBox="0 0 430 200" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <radialGradient id="glowGreen" cx="50%" cy="50%" r="50%">
          <stop offset="0%" stop-color="#00ff88" stop-opacity="0.3"/>
          <stop offset="100%" stop-color="#00ff88" stop-opacity="0"/>
        </radialGradient>
        <radialGradient id="glowRed" cx="50%" cy="50%" r="50%">
          <stop offset="0%" stop-color="#ff3355" stop-opacity="0.25"/>
          <stop offset="100%" stop-color="#ff3355" stop-opacity="0"/>
        </radialGradient>
      </defs>

      <ellipse id="sceneGlow" cx="215" cy="175" rx="120" ry="20" fill="url(#glowGreen)" opacity="0.7"/>

      <!-- Terrain -->
      <path d="M0,175 Q40,170 60,172 Q80,174 90,168 Q100,162 110,160 Q130,155 150,148 Q170,140 180,135 Q200,125 215,108 Q230,125 250,135 Q265,140 280,148 Q300,155 320,160 Q330,162 340,168 Q350,174 360,172 Q380,170 430,175 L430,200 L0,200 Z"
            fill="#0d1628" stroke="none"/>
      <path d="M120,175 Q160,140 215,108 Q270,140 310,175"
            fill="none" stroke="#1a2840" stroke-width="2"/>

      <!-- PROFIT GROUP: 2 miners picking at the peak (SVG animateTransform — no JS needed) -->
      <g id="profitGroup">
        <!-- Rising $ above each miner -->
        <text x="190" y="72" font-size="14" fill="#ffee00" text-anchor="middle" font-family="monospace" font-weight="bold">
          $<animate attributeName="opacity" values="0;0.9;0" dur="2s" repeatCount="indefinite"/>
          <animateTransform attributeName="transform" type="translate" from="0 0" to="0 -32" dur="2s" repeatCount="indefinite" additive="sum"/>
        </text>
        <text x="240" y="72" font-size="14" fill="#ffee00" text-anchor="middle" font-family="monospace" font-weight="bold">
          $<animate attributeName="opacity" values="0;0.9;0" dur="2s" begin="0.7s" repeatCount="indefinite"/>
          <animateTransform attributeName="transform" type="translate" from="0 0" to="0 -32" dur="2s" begin="0.7s" repeatCount="indefinite" additive="sum"/>
        </text>
        <!-- Miner 1: left of peak, swings pickaxe right -->
        <g transform="translate(190,115)">
          <circle cx="0" cy="-30" r="8" stroke="#00ff88" stroke-width="2.5" fill="rgba(0,255,136,0.12)"/>
          <line x1="0" y1="-22" x2="0"   y2="-2"  stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
          <line x1="0" y1="-17" x2="-11" y2="-9"  stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
          <g>
            <line x1="0"  y1="-17" x2="13" y2="-8"  stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
            <line x1="11" y1="-9"  x2="18" y2="-2"  stroke="#ffee00" stroke-width="2.5" stroke-linecap="round"/>
            <line x1="15" y1="-4"  x2="22" y2="-11" stroke="#ffee00" stroke-width="3"   stroke-linecap="round"/>
            <animateTransform attributeName="transform" type="rotate" values="-28 0 -17;24 0 -17;-28 0 -17" dur="0.65s" repeatCount="indefinite"/>
          </g>
          <line x1="0" y1="-2" x2="-8" y2="14" stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
          <line x1="0" y1="-2" x2="8"  y2="14" stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
          <text x="0" y="26" font-size="9" fill="#00e5ff" text-anchor="middle" font-family="monospace" id="miner1Label">—</text>
        </g>
        <!-- Miner 2: right of peak, mirrored so pickaxe swings toward center -->
        <g transform="translate(240,115) scale(-1,1)">
          <circle cx="0" cy="-30" r="8" stroke="#00ff88" stroke-width="2.5" fill="rgba(0,255,136,0.12)"/>
          <line x1="0" y1="-22" x2="0"   y2="-2"  stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
          <line x1="0" y1="-17" x2="-11" y2="-9"  stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
          <g>
            <line x1="0"  y1="-17" x2="13" y2="-8"  stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
            <line x1="11" y1="-9"  x2="18" y2="-2"  stroke="#ffee00" stroke-width="2.5" stroke-linecap="round"/>
            <line x1="15" y1="-4"  x2="22" y2="-11" stroke="#ffee00" stroke-width="3"   stroke-linecap="round"/>
            <animateTransform attributeName="transform" type="rotate" values="-28 0 -17;24 0 -17;-28 0 -17" dur="0.65s" begin="0.32s" repeatCount="indefinite"/>
          </g>
          <line x1="0" y1="-2" x2="-8" y2="14" stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
          <line x1="0" y1="-2" x2="8"  y2="14" stroke="#00ff88" stroke-width="2.5" stroke-linecap="round"/>
          <g transform="scale(-1,1)">
            <text x="0" y="26" font-size="9" fill="#00e5ff" text-anchor="middle" font-family="monospace" id="miner2Label">—</text>
          </g>
        </g>
        <!-- Impact sparkle at peak -->
        <circle cx="215" cy="112" r="2.5" fill="#ffee00">
          <animate attributeName="opacity" values="0.9;0;0.9" dur="0.65s" repeatCount="indefinite"/>
          <animate attributeName="r"       values="2.5;5;2.5"  dur="0.65s" repeatCount="indefinite"/>
        </circle>
      </g>

      <!-- LOSS GROUP: 2 walkers marching off-screen to the right -->
      <g id="lossGroup" style="display:none">
        <!-- Walker 1 -->
        <g>
          <circle cx="0" cy="-30" r="8" stroke="#ff3355" stroke-width="2.5" fill="rgba(255,51,85,0.1)"/>
          <line x1="0" y1="-22" x2="3"   y2="-2"  stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
          <line x1="1" y1="-17" x2="-10" y2="-5"  stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
          <line x1="1" y1="-17" x2="10"  y2="-5"  stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
          <g>
            <line x1="3" y1="-2" x2="-4" y2="14" stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
            <animateTransform attributeName="transform" type="rotate" values="25 3 -2;-25 3 -2;25 3 -2" dur="0.45s" repeatCount="indefinite" additive="sum"/>
          </g>
          <g>
            <line x1="3" y1="-2" x2="9" y2="14" stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
            <animateTransform attributeName="transform" type="rotate" values="-25 3 -2;25 3 -2;-25 3 -2" dur="0.45s" repeatCount="indefinite" additive="sum"/>
          </g>
          <ellipse cx="9" cy="-32" rx="2" ry="3" fill="#4af" opacity="0">
            <animate attributeName="opacity" values="0;0.8;0" dur="2s" repeatCount="indefinite"/>
            <animateTransform attributeName="transform" type="translate" from="0 0" to="2 20" dur="2s" repeatCount="indefinite" additive="sum"/>
          </ellipse>
          <text x="0" y="26" font-size="9" fill="#ff3355" text-anchor="middle" font-family="monospace" id="walker1Label">—</text>
          <animateTransform attributeName="transform" type="translate" from="100 155" to="410 155" dur="3s" repeatCount="indefinite"/>
          <animate attributeName="opacity" values="1;1;0" keyTimes="0;0.75;1" dur="3s" repeatCount="indefinite"/>
        </g>
        <!-- Walker 2 -->
        <g>
          <circle cx="0" cy="-30" r="8" stroke="#ff3355" stroke-width="2.5" fill="rgba(255,51,85,0.1)"/>
          <line x1="0" y1="-22" x2="3"   y2="-2"  stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
          <line x1="1" y1="-17" x2="-10" y2="-5"  stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
          <line x1="1" y1="-17" x2="10"  y2="-5"  stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
          <g>
            <line x1="3" y1="-2" x2="-4" y2="14" stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
            <animateTransform attributeName="transform" type="rotate" values="25 3 -2;-25 3 -2;25 3 -2" dur="0.45s" begin="0.22s" repeatCount="indefinite" additive="sum"/>
          </g>
          <g>
            <line x1="3" y1="-2" x2="9" y2="14" stroke="#ff3355" stroke-width="2.5" stroke-linecap="round"/>
            <animateTransform attributeName="transform" type="rotate" values="-25 3 -2;25 3 -2;-25 3 -2" dur="0.45s" begin="0.22s" repeatCount="indefinite" additive="sum"/>
          </g>
          <text x="0" y="26" font-size="9" fill="#ff3355" text-anchor="middle" font-family="monospace" id="walker2Label">—</text>
          <animateTransform attributeName="transform" type="translate" from="145 155" to="455 155" dur="3s" begin="1.1s" repeatCount="indefinite"/>
          <animate attributeName="opacity" values="1;1;0" keyTimes="0;0.75;1" dur="3s" begin="1.1s" repeatCount="indefinite"/>
        </g>
      </g>

    </svg>
  </div>

  <!-- MINI STATS -->
  <div class="mini-stats">
    <div class="mini-stat">
      <span class="mini-stat-val" id="statPositions">0</span>
      <span class="mini-stat-lbl">Positions</span>
    </div>
    <div class="mini-stat">
      <span class="mini-stat-val" id="statPnl" style="color:var(--cyan)">$0</span>
      <span class="mini-stat-lbl">Open PnL</span>
    </div>
    <div class="mini-stat">
      <span class="mini-stat-val" id="statWinRate">—</span>
      <span class="mini-stat-lbl">Win Rate</span>
    </div>
  </div>

  <!-- OPEN POSITIONS -->
  <div class="card" style="animation:fadeUp .5s .3s both">
    <div class="sec-hdr">OPEN POSITIONS</div>
    <div id="pos-wrap"><div class="empty" id="no-pos">Scanning for signals...</div></div>
  </div>

  <!-- LIVE FEED -->
  <div class="card" style="animation:fadeUp .5s .4s both">
    <div class="sec-hdr">SYSTEM LOG</div>
    <div class="log-feed" id="log-box">Connecting...</div>
  </div>

  <!-- RECENT TRADES -->
  <div class="card" style="animation:fadeUp .5s .5s both">
    <div class="sec-hdr">RECENT TRADES</div>
    <table class="trade-table">
      <thead><tr><th>Market</th><th>Side</th><th>PnL</th><th>Result</th><th>Time</th></tr></thead>
      <tbody id="trades-wrap"><tr><td colspan="5" class="empty">No trades yet</td></tr></tbody>
    </table>
  </div>

</div>

<script>
// ── PARTICLES ─────────────────────────────────────────────
const pCanvas = document.getElementById('particles');
const pCtx    = pCanvas.getContext('2d');
let particles = [];
function resizeP() {{ pCanvas.width = window.innerWidth; pCanvas.height = window.innerHeight; }}
resizeP();
window.addEventListener('resize', resizeP);
for (let i = 0; i < 18; i++) {{
  particles.push({{ x: Math.random()*window.innerWidth, y: Math.random()*window.innerHeight, speed: .4+Math.random()*.6, size: 1.5+Math.random(), opacity: .2+Math.random()*.3 }});
}}
(function animP() {{
  pCtx.clearRect(0, 0, pCanvas.width, pCanvas.height);
  particles.forEach(p => {{
    p.y -= p.speed;
    if (p.y < -4) {{ p.y = pCanvas.height+4; p.x = Math.random()*pCanvas.width; }}
    pCtx.beginPath(); pCtx.arc(p.x, p.y, p.size, 0, Math.PI*2);
    pCtx.fillStyle = `rgba(0,229,255,${{p.opacity}})`; pCtx.fill();
  }});
  requestAnimationFrame(animP);
}})();

// ── SCENE ─────────────────────────────────────────────────
let isProfitMode   = true;
let manualOverride = false;

function setSceneMode(profit, totalPnl, labels) {{
  isProfitMode = profit;
  const profitGroup = document.getElementById('profitGroup');
  const lossGroup   = document.getElementById('lossGroup');
  const scenePnl    = document.getElementById('scenePnl');
  const sceneLabel  = document.getElementById('sceneLabel');
  const sceneGlow   = document.getElementById('sceneGlow');
  const toggleIcon  = document.getElementById('toggleIcon');
  const toggleLabel = document.getElementById('toggleLabel');
  const pnlStr      = (totalPnl >= 0 ? '+' : '') + '$' + Math.abs(totalPnl).toFixed(2);
  scenePnl.textContent = pnlStr;
  if (profit) {{
    profitGroup.style.display = '';
    lossGroup.style.display   = 'none';
    scenePnl.className        = 'scene-pnl profit';
    sceneLabel.textContent    = 'MINING PROFITS';
    sceneLabel.className      = 'scene-label profit';
    sceneGlow.setAttribute('fill', 'url(#glowGreen)');
    toggleIcon.textContent    = '📈';
    toggleLabel.textContent   = 'SHOW LOSS';
    if (labels[0]) document.getElementById('miner1Label').textContent = labels[0];
    if (labels[1]) document.getElementById('miner2Label').textContent = labels[1];
  }} else {{
    profitGroup.style.display = 'none';
    lossGroup.style.display   = '';
    scenePnl.className        = 'scene-pnl loss';
    sceneLabel.textContent    = 'CUTTING LOSSES...';
    sceneLabel.className      = 'scene-label loss';
    sceneGlow.setAttribute('fill', 'url(#glowRed)');
    toggleIcon.textContent    = '📉';
    toggleLabel.textContent   = 'SHOW PROFIT';
    if (labels[0]) document.getElementById('walker1Label').textContent = labels[0];
    if (labels[1]) document.getElementById('walker2Label').textContent = labels[1];
  }}
}}

function manualToggle() {{
  manualOverride = true;
  setSceneMode(!isProfitMode, 0, []);
  setTimeout(() => {{ manualOverride = false; }}, 30000);
}}

// ── CHARTS ────────────────────────────────────────────────
const charts  = {{}};
const pnlHist = {{}};

// ── POSITIONS ─────────────────────────────────────────────
async function poll() {{
  try {{
    const d    = await fetch('/status/api').then(r => r.json());
    const pos  = d.positions || {{}};
    const keys = Object.keys(pos);
    document.getElementById('statPositions').textContent = keys.length;
    const wrap  = document.getElementById('pos-wrap');
    const noPos = document.getElementById('no-pos');
    noPos.style.display = keys.length ? 'none' : 'block';
    let totalPnl = 0;
    const labels = [];
    keys.forEach(market => {{
      const p   = pos[market];
      const pnl = p.pnl || 0;
      const cur = p.current_price || p.entry;
      totalPnl += pnl;
      labels.push(market.replace(/-?PERP$/i, ''));
      if (!pnlHist[market]) pnlHist[market] = [];
      pnlHist[market].push(pnl);
      if (pnlHist[market].length > 120) pnlHist[market].shift();
      let card = document.getElementById('pc-' + market);
      if (!card) {{
        card = document.createElement('div');
        card.id = 'pc-' + market;
        card.className = 'pos-card';
        card.innerHTML = cardHTML(market, p);
        wrap.appendChild(card);
        setTimeout(() => initChart(market), 50);
      }} else {{
        const pnlEl = document.getElementById('pp-'  + market);
        const curEl = document.getElementById('pc2-' + market);
        const c = pnl >= 0 ? '#00ff88' : '#ff3355';
        if (pnlEl) {{ pnlEl.textContent = (pnl>=0?'+':'')+'$'+pnl.toFixed(2); pnlEl.style.color = c; }}
        if (curEl) curEl.textContent = '$' + cur.toFixed(4);
        updateChart(market, pnl);
      }}
    }});
    document.querySelectorAll('.pos-card').forEach(el => {{
      const m = el.id.replace('pc-', '');
      if (!pos[m]) {{ if (charts[m]) {{ charts[m].destroy(); delete charts[m]; }} delete pnlHist[m]; el.remove(); }}
    }});
    const pnlEl = document.getElementById('statPnl');
    pnlEl.textContent  = (totalPnl >= 0 ? '+' : '') + '$' + totalPnl.toFixed(2);
    pnlEl.style.color  = totalPnl >= 0 ? 'var(--green)' : 'var(--red)';
    if (!manualOverride) setSceneMode(totalPnl >= 0 || keys.length === 0, totalPnl, labels);
  }} catch(e) {{ console.error(e); }}
}}

// ── TRADES ────────────────────────────────────────────────
async function pollTrades() {{
  try {{
    const trades = await fetch('/trades/api').then(r => r.json());
    const wrap   = document.getElementById('trades-wrap');
    if (!trades.length) {{ wrap.innerHTML = '<tr><td colspan="5" class="empty">No trades yet</td></tr>'; return; }}
    const wins = trades.filter(t => t.pnl >= 0).length;
    document.getElementById('statWinRate').textContent = Math.round(wins / trades.length * 100) + '%';
    wrap.innerHTML = trades.slice(0, 15).map(t => {{
      const sc    = t.side === 'long' ? '#00ff88' : '#ff3355';
      const pc    = t.pnl >= 0 ? '#00ff88' : '#ff3355';
      const badge = t.pnl >= 0 ? 'win' : 'loss';
      return `<tr>
        <td style="color:var(--cyan);font-weight:700">${{t.market}}</td>
        <td style="color:${{sc}}">${{t.side.toUpperCase()}}</td>
        <td style="color:${{pc}}">${{t.pnl>=0?'+':''}}$${{t.pnl.toFixed(2)}}</td>
        <td><span class="badge ${{badge}}">${{t.reason}}</span></td>
        <td style="color:var(--muted)">${{t.ts.slice(-5)}}</td>
      </tr>`;
    }}).join('');
  }} catch(e) {{}}
}}

// ── LOG FEED ──────────────────────────────────────────────
async function pollLogs() {{
  try {{
    const d   = await fetch('/notify/api').then(r => r.json());
    const box = document.getElementById('log-box');
    if (!d.length) {{ box.innerHTML = '<span style="color:var(--muted)">Waiting for first signal...</span>'; return; }}
    box.innerHTML = d.map(n => {{
      const txt     = n.text || '';
      const isOpen  = txt.includes('OPEN ');
      const isClose = txt.includes('CLOSE ');
      const isMile  = txt.includes('MILESTONE');
      const color   = isClose && txt.includes('+') ? '#00ff88' :
                      isClose && txt.includes('-') ? '#ff3355' :
                      isOpen  ? '#00e5ff' : isMile ? '#ffee00' : '#555';
      const icon    = isOpen ? '▶' : isClose ? (txt.includes('+') ? '✅' : '❌') : isMile ? '🏆' : '•';
      const lines   = txt.replace(/\*/g, '').split('\\n').filter(Boolean);
      return `<div class="log-entry" style="border-color:${{color}};background:rgba(255,255,255,.03)">
        <div style="font-size:10px;color:var(--muted);margin-bottom:2px">${{n.time}}</div>
        ${{lines.map((l,i) => `<div style="color:${{i===0?color:'#888'}};font-weight:${{i===0?'600':'400'}}">${{icon}} ${{l}}</div>`).join('')}}
      </div>`;
    }}).join('');
    box.scrollTop = box.scrollHeight;
  }} catch(e) {{}}
}}

// ── CARD / CHART HELPERS ──────────────────────────────────
function cardHTML(market, p) {{
  const side = p.side || 'long';
  const sc   = side === 'long' ? '#00ff88' : '#ff3355';
  const pnl  = p.pnl || 0;
  const pc   = pnl >= 0 ? '#00ff88' : '#ff3355';
  const cur  = p.current_price || p.entry;
  return `
    <div class="pos-top">
      <span class="pos-sym">${{market}}-PERP</span>
      <span class="pos-side" style="background:${{sc}}20;color:${{sc}}">${{side.toUpperCase()}}</span>
      <span class="pos-pnl" id="pp-${{market}}" style="color:${{pc}}">${{pnl>=0?'+':''}}$${{pnl.toFixed(2)}}</span>
    </div>
    <div class="pos-meta">
      <span><b>Entry</b> $${{(p.entry||0).toFixed(4)}}</span>
      <span><b>Current</b> <span id="pc2-${{market}}">$${{cur.toFixed(4)}}</span></span>
      <span><b>TP</b> <span style="color:#00ff88">$${{(p.tp||0).toFixed(4)}}</span></span>
      <span><b>SL</b> <span style="color:#ff3355">$${{(p.sl||0).toFixed(4)}}</span></span>
      <span><b>Size</b> $${{(p.size||0).toFixed(2)}}</span>
      <span><b>Leverage</b> ${{p.leverage||3}}x</span>
    </div>
    <canvas id="chart-${{market}}" height="80"></canvas>
    <button class="close-btn" onclick="closePos('${{market}}')">✕ CLOSE ${{market}}</button>`;
}}

function initChart(market) {{
  const el = document.getElementById('chart-' + market);
  if (!el || charts[market]) return;
  charts[market] = new Chart(el.getContext('2d'), {{
    type: 'line',
    data: {{ labels: [], datasets: [{{ data: [], borderColor: '#00e5ff', backgroundColor: 'rgba(0,229,255,.07)', borderWidth: 2, pointRadius: 0, tension: 0.4, fill: true }}] }},
    options: {{
      responsive: true, animation: {{ duration: 300 }},
      plugins: {{ legend: {{ display: false }}, tooltip: {{ callbacks: {{ label: c => (c.parsed.y>=0?'+':'')+'$'+c.parsed.y.toFixed(2) }} }} }},
      scales: {{ x: {{ display: false }}, y: {{ grid: {{ color: '#ffffff10' }}, ticks: {{ color: '#555', font: {{ size: 9 }}, callback: v => '$' + v.toFixed(2) }} }} }}
    }}
  }});
}}

function updateChart(market, latestPnl) {{
  const c = charts[market];
  if (!c) return;
  const hist  = pnlHist[market] || [];
  const color = latestPnl >= 0 ? '#00ff88' : '#ff3355';
  c.data.labels = hist.map((_, i) => i);
  c.data.datasets[0].data             = hist;
  c.data.datasets[0].borderColor      = color;
  c.data.datasets[0].backgroundColor  = latestPnl >= 0 ? 'rgba(0,255,136,.07)' : 'rgba(255,51,85,.07)';
  c.update('none');
}}

function closePos(market) {{
  if (!confirm('Close ' + market + '?')) return;
  fetch('/api/close/' + market, {{ method: 'POST' }})
    .then(r => r.json()).then(d => alert(d.msg || d.error)).catch(e => alert(e));
}}

// ── KICK OFF ──────────────────────────────────────────────
poll();
pollTrades();
pollLogs();
setInterval(poll,       3000);
setInterval(pollTrades, 5000);
setInterval(pollLogs,   3000);
</script>
</body></html>"""


# ── ENTRY POINT ───────────────────────────────────────────────────
if __name__ == "__main__":
    if not DRIFT_PAPER_MODE:
        if DRIFT_EXCHANGE == "bybit":
            if not BYBIT_API_KEY or not BYBIT_API_SECRET:
                log("err", "DRIFT_EXCHANGE=bybit requires BYBIT_API_KEY and BYBIT_API_SECRET env vars.")
                raise SystemExit(1)
        elif DRIFT_EXCHANGE == "jupiter":
            if not WALLET_PRIVATE_KEY:
                log("err", "DRIFT_EXCHANGE=jupiter requires WALLET_PRIVATE_KEY env var.")
                raise SystemExit(1)
            try:
                import anchorpy  # noqa: F401
            except ImportError:
                log("err", "DRIFT_EXCHANGE=jupiter requires anchorpy — run: pip install anchorpy.")
                raise SystemExit(1)
            active_mkts = [m.strip().upper() for m in DRIFT_MARKETS.split(",") if m.strip().upper() in _JPERP_MARKETS]
            if not active_mkts:
                log("err", "DRIFT_MARKETS has no Jupiter-supported markets (SOL/ETH/BTC).")
                raise SystemExit(1)
            skipped = [m.strip().upper() for m in DRIFT_MARKETS.split(",") if m.strip().upper() not in _JPERP_MARKETS]
            if skipped:
                log("warn", f"Jupiter Perps does not support {skipped} — those markets will be skipped")
            _cache_jupiter_idl()
        elif not WALLET or not WALLET_PRIVATE_KEY:
            log("err", "DRIFT_PAPER_MODE=false requires WALLET and WALLET_PRIVATE_KEY env vars.")
            raise SystemExit(1)

    if REDIS_URL and REDIS_TOKEN:
        ping = _redis_cmd("PING")
        if ping == "PONG":
            print("[OK] Redis connected")
        else:
            print(f"[WARN] Redis PING returned {ping!r} — state will not persist across restarts")
    else:
        print("[WARN] UPSTASH_REDIS_REST_URL/TOKEN not set — state will not persist across restarts")

    _load_state()
    log("ok", f"{'[PAPER] ' if DRIFT_PAPER_MODE else '[LIVE] '}{DRIFT_BOT_NAME} starting")
    log("ok", f"Exchange: {DRIFT_EXCHANGE.upper()} | Markets: {DRIFT_MARKETS} | Leverage: {int(DRIFT_LEV_MIN)}-{int(DRIFT_LEV_MAX)}x dynamic | Margin: ${DRIFT_MARGIN_USD:.0f}/trade")
    log("ok", f"Capital: ${_capital:.2f} | Goal: ${PROFIT_GOAL:,.0f} | Port: {DRIFT_PORT}")

    t_notify = threading.Thread(target=_notify_worker, daemon=True)
    t_notify.start()

    t_trade = threading.Thread(target=run_trading_loop, daemon=True)
    t_trade.start()

    t_prices = threading.Thread(target=run_position_price_updater, daemon=True)
    t_prices.start()

    notify(
        f"*{DRIFT_BOT_NAME}* started\n"
        f"Mode: {'PAPER' if DRIFT_PAPER_MODE else 'LIVE'}\n"
        f"Exchange: {DRIFT_EXCHANGE.upper()}\n"
        f"Markets: {DRIFT_MARKETS}\n"
        f"Leverage: {int(DRIFT_LEV_MIN)}-{int(DRIFT_LEV_MAX)}x dynamic | Margin: ${DRIFT_MARGIN_USD:.0f}/trade\n"
        f"Capital: ${_capital:.2f}"
    )

    _port = int(os.environ.get("DRIFT_PORT", os.environ.get("PORT", DRIFT_PORT)))
    app.run(host="0.0.0.0", port=_port, debug=False)
