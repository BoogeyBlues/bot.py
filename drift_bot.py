import os, time, threading, requests, json
from collections import deque
from flask import Flask, jsonify, request as flask_request

app = Flask(__name__)

# ── CONFIG ────────────────────────────────────────────────────────
DRIFT_PAPER_MODE   = os.environ.get("DRIFT_PAPER_MODE", "true").lower() != "false"
DRIFT_EXCHANGE     = os.environ.get("DRIFT_EXCHANGE", "drift")
DRIFT_LEVERAGE     = float(os.environ.get("DRIFT_LEVERAGE", "3"))
DRIFT_TRADE_PCT    = float(os.environ.get("DRIFT_TRADE_PCT", "0.10"))
DRIFT_MAX_OPEN     = int(os.environ.get("DRIFT_MAX_OPEN", "3"))
DRIFT_TP_PCT       = float(os.environ.get("DRIFT_TP_PCT", "0.20"))
DRIFT_SL_PCT       = float(os.environ.get("DRIFT_SL_PCT", "0.05"))
DRIFT_TRAIL_PCT    = float(os.environ.get("DRIFT_TRAIL_PCT", "0.05"))
DRIFT_MARKETS      = os.environ.get("DRIFT_MARKETS", "SOL,BTC,ETH,DOGE,XRP,BONK,WIF,PEPE,HYPE,POPCAT,TRUMP")
DRIFT_BOT_NAME     = os.environ.get("DRIFT_BOT_NAME", "Drift Sniper")
DRIFT_PORT         = int(os.environ.get("PORT", os.environ.get("DRIFT_PORT", "5001")))
WALLET             = os.environ.get("WALLET", "")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY", "")
SOL_RPC            = os.environ.get("SOL_RPC", "https://api.mainnet-beta.solana.com")
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
GMGN_API_KEY       = os.environ.get("GMGN_API_KEY", "")
STARTING_CAPITAL   = float(os.environ.get("DRIFT_STARTING_CAPITAL", "100"))
PROFIT_GOAL        = float(os.environ.get("DRIFT_PROFIT_GOAL", "10000"))
DRIFT_TP_USD       = float(os.environ.get("DRIFT_TP_USD", "100"))   # close when PnL hits this $
DRIFT_SL_USD       = float(os.environ.get("DRIFT_SL_USD", "2"))     # close when loss hits this $
DRIFT_TUNE_EVERY   = int(os.environ.get("DRIFT_TUNE_EVERY",   "3")) # retune after every N closed trades
DRIFT_COMPOUND_PCT = float(os.environ.get("DRIFT_COMPOUND_PCT", "0.10"))  # % of profit reinvested
REDIS_URL          = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN        = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

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
_signals_cache     = {}   # market -> {signal, ts}
_st_prev           = {}   # market -> previous supertrend bullish state (for flip detection)
_market_stats      = {}   # market -> {wins, losses, long_wins, short_wins, long_total, short_total}
_market_params     = {}   # tuned per-market: {leverage, bias, paused_until, last_result}
_gmgn_signal_cache = {}   # market -> {result, ts} — 5-min TTL to avoid per-tick API calls

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
    except Exception:
        pass
    _gmgn_signal_cache[market] = {"result": result, "ts": time.time()}
    return result

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

    gmgn_bias = _get_gmgn_signal(market)
    if gmgn_bias == trend:
        confidence += 1
    elif gmgn_bias and gmgn_bias != trend:
        log("info", f"{market} ST={trend} but GMGN={gmgn_bias} — suppressed", "SIG")
        _st_prev[market] = st_bull
        return None, 0, None

    _st_prev[market] = st_bull

    if confidence < 2:
        return None, 0, None

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

def _execute_zeta_order(market, side, size_usd, leverage) -> bool:
    try:
        price = get_market_price(market)
        if not price:
            log("err", f"Cannot get price for {market} — Zeta order skipped")
            return False
        quantity = (size_usd * leverage) / price
        r = _session.post(
            "https://dex.zeta.markets/api/v2/order",
            json={
                "wallet": WALLET,
                "market": f"{market.upper()}-PERP",
                "side": side,
                "quantity": quantity,
                "orderType": "market",
                "privateKey": WALLET_PRIVATE_KEY,
            },
            timeout=15
        )
        if r.status_code != 200:
            log("err", f"Zeta order failed: {r.status_code} {r.text[:80]}")
            return False
        return True
    except Exception as e:
        log("err", f"Zeta order error: {e}")
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

    pos = {
        "market": market, "side": side, "entry": price,
        "size": size_usd, "leverage": leverage,
        "peak_price": price, "tp": tp, "sl": sl,
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
        ok = _execute_zeta_order(market, side, size_usd, leverage) if DRIFT_EXCHANGE == "zeta" \
             else _execute_drift_order(market, side, size_usd, leverage)
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
        f"PnL={pnl_usd:+.2f} ({pnl_pct*100:+.1f}%) [{reason}]")
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

        # Leverage: pull back on losers, push up on winners
        if wr < 0.30:
            new_lev = round(max(1.0, cur_lev - 1.0), 1)
        elif wr > 0.65:
            new_lev = round(min(DRIFT_LEVERAGE * 2, cur_lev + 0.5), 1)
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

                    # ── ATR-based position sizing ─────────────────────────
                    # Size so that the ATR stop-out equals exactly DRIFT_SL_USD
                    market_lev     = mparams.get("leverage", DRIFT_LEVERAGE)
                    conf_mult      = 1.25 if confidence == 3 else 1.0
                    with _state_lock:
                        cap = _capital  # re-read per market so prior openings are reflected
                    ideal_notional = (DRIFT_SL_USD / sl_pct) if sl_pct > 0 else cap * DRIFT_TRADE_PCT * market_lev
                    max_notional   = cap * DRIFT_TRADE_PCT * market_lev * conf_mult
                    size_usd       = min(ideal_notional, max_notional)
                    margin         = size_usd / market_lev
                    if margin < 1.0:
                        log("warn", f"Margin too small: ${margin:.2f}")
                        continue

                    log("ok",
                        f"SIGNAL {signal.upper()} conf={confidence}/3 lev={market_lev}x "
                        f"size=${size_usd:.2f} SL={sl_pct*100:.2f}% TP={tp_pct*100:.2f}%", market)

                    # Cache signal for display
                    with _state_lock:
                        _signals_cache[market] = {
                            "signal": signal, "confidence": confidence,
                            "ts": time.strftime("%H:%M:%S"), "price": price,
                        }

                    # Open with ATR-based SL/TP and ATR-based trailing stop
                    open_position(market, signal, price, size_usd, market_lev,
                                  sl_pct=sl_pct, tp_pct=tp_pct, atr_pct=atr_pct)

        except Exception as e:
            log("err", f"Loop error: {e}")

        time.sleep(60)

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
    mode       = "PAPER" if DRIFT_PAPER_MODE else "LIVE"
    mode_color = "#ffee00" if DRIFT_PAPER_MODE else "#39ff14"
    exch       = DRIFT_EXCHANGE.upper()
    markets_list = [m.strip().upper() for m in DRIFT_MARKETS.split(",")]

    # SHOES + LIQUID redesign

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>{DRIFT_BOT_NAME}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{--cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--yellow:#ffee00;--bg:#050a14;--card:#0a1220;--border:#ffffff15}}

/* LIQUID BACKGROUND */
body{{
  background:var(--bg);
  background-image:
    radial-gradient(ellipse 70% 50% at 15% 15%,#00e5ff09 0%,transparent 60%),
    radial-gradient(ellipse 50% 70% at 85% 85%,#ff335508 0%,transparent 60%);
  color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;
  min-height:100vh;overflow-x:hidden
}}
.bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;opacity:.22;pointer-events:none;z-index:0}}
.wrap{{position:relative;z-index:1}}

/* NAV */
nav{{display:flex;border-bottom:2px solid var(--cyan);overflow-x:auto;scrollbar-width:none;
  background:linear-gradient(90deg,#020d1c,#030f1e)}}
nav::-webkit-scrollbar{{display:none}}
nav a{{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;padding:10px 14px;
  white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;
  border-right:1px solid var(--border);transition:all .2s}}
nav a:hover,nav a.active{{background:var(--cyan);color:#000}}

/* MODE STRIP */
.strip{{background:var(--card);border-bottom:2px solid var(--border);padding:5px 16px;
  display:flex;justify-content:space-between;align-items:center}}
.strip-left{{font-size:.6rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
.mode-pill{{font-size:.65rem;font-weight:900;padding:3px 10px;background:{mode_color};color:#000;letter-spacing:.08em}}

/* HERO */
.hero{{padding:18px 16px 20px;
  background:linear-gradient(180deg,#020d1c 0%,rgba(0,229,255,.04) 60%,var(--bg) 100%);
  text-align:center;position:relative;overflow:hidden}}
.hero::after{{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,var(--cyan),var(--green),transparent);
  animation:liquidWave 3s ease-in-out infinite}}
@keyframes liquidWave{{0%,100%{{opacity:.6;transform:scaleX(1)}}50%{{opacity:1;transform:scaleX(1.05)}}}}
.hero-title{{font-family:'Bebas Neue',sans-serif;font-size:1.6rem;color:var(--cyan);letter-spacing:.12em;text-shadow:0 0 20px #00e5ff55}}
.hero-cap{{font-family:'Bebas Neue',sans-serif;font-size:3.8rem;color:var(--cyan);
  text-shadow:0 0 40px #00e5ff66,4px 4px 0 #ff3355;line-height:1.05;animation:glow 3s ease-in-out infinite}}
.hero-row{{display:flex;justify-content:center;gap:18px;margin-top:8px;font-size:.68rem;color:#555}}
.hero-row span{{color:#888;font-family:'JetBrains Mono',monospace}}

/* WAVE DIVIDER */
.wave-div{{overflow:hidden;line-height:0}}
.wave-div svg{{display:block;width:100%}}

/* STATS */
.stats{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:1px;
  background:linear-gradient(90deg,var(--cyan),var(--green))}}
.stat{{background:#0a1220;padding:12px 10px;text-align:center}}
.stat .lbl{{font-size:.54rem;font-weight:700;color:#555;text-transform:uppercase;letter-spacing:.08em}}
.stat .val{{font-family:'Bebas Neue',sans-serif;font-size:1.55rem;line-height:1.1;margin-top:2px}}

/* LIQUID SECTION */
.sec{{background:linear-gradient(180deg,#0a1220 0%,#07101e 100%);margin-bottom:1px;position:relative}}
.sec-hdr{{display:flex;align-items:center;justify-content:space-between;
  padding:10px 16px;border-bottom:1px solid #ffffff0a}}
.sec-hdr h2{{font-size:.6rem;font-weight:700;color:#555;text-transform:uppercase;letter-spacing:.12em}}
.sec-hdr a{{font-size:.6rem;color:var(--cyan);text-decoration:none;font-weight:700}}
.sec-hdr span{{font-size:.6rem;color:var(--cyan);font-weight:700}}

/* POSITION CARDS — tap to reveal timestamp */
.pos-card{{
  margin:10px 12px;
  background:linear-gradient(135deg,#0d1825,#091420);
  border:1px solid #ffffff12;border-radius:4px;
  position:relative;overflow:hidden;cursor:pointer;
  transition:box-shadow .3s ease
}}
.pos-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,var(--cyan),transparent);
  animation:shimmer 2.5s ease-in-out infinite}}
.pos-card.profit{{border-color:#00ff8828;box-shadow:0 0 16px #00ff8806}}
.pos-card.profit::before{{background:linear-gradient(90deg,transparent,var(--green),transparent)}}
.pos-card.loss{{border-color:#ff335528;box-shadow:0 0 16px #ff335506}}
.pos-card.loss::before{{background:linear-gradient(90deg,transparent,var(--red),transparent)}}
.pos-card:active{{transform:scale(.99)}}
.pos-body{{padding:12px 14px 4px}}
.pos-top{{display:flex;align-items:center;gap:8px;margin-bottom:8px}}
.pos-sym{{font-family:'Bebas Neue',sans-serif;font-size:1.3rem;color:var(--cyan)}}
.pos-side{{font-size:.63rem;font-weight:900;padding:2px 7px;border:1px solid;letter-spacing:.06em}}
.pos-pnl{{font-family:'JetBrains Mono',monospace;font-size:.95rem;font-weight:700;margin-left:auto;transition:color .4s}}
.pos-meta{{display:grid;grid-template-columns:1fr 1fr;gap:3px 10px;
  font-size:.6rem;color:#444;font-family:'JetBrains Mono',monospace;margin-bottom:8px}}
.pos-meta b{{color:#666;font-weight:400}}
canvas.mini{{width:100%!important;display:block;border-radius:2px}}
/* expandable timestamp detail */
.pos-detail{{max-height:0;overflow:hidden;transition:max-height .35s cubic-bezier(.4,0,.2,1),padding .35s ease;
  background:linear-gradient(180deg,transparent,#00e5ff05);padding:0 14px}}
.pos-card.expanded .pos-detail{{max-height:72px;padding:8px 14px 10px}}
.pos-ts-row{{font-size:.58rem;font-family:'JetBrains Mono',monospace;color:#444;
  display:flex;justify-content:space-between;margin-bottom:4px}}
.pos-ts-row span:last-child{{color:#666}}
.pos-notional{{font-size:.56rem;color:#2a3a4a;font-family:'JetBrains Mono',monospace}}
.pos-hint{{display:flex;justify-content:center;align-items:center;gap:4px;
  padding:5px 0 6px;font-size:.48rem;color:#1e2d3a;letter-spacing:.12em;text-transform:uppercase}}
.pos-chevron{{display:inline-block;transition:transform .35s ease;font-size:.6rem;line-height:1}}
.pos-card.expanded .pos-chevron{{transform:rotate(180deg)}}
.close-btn{{width:calc(100% - 28px);margin:0 14px 12px;display:block;padding:7px;background:transparent;
  border:1px solid #ff335530;color:#ff3355;font-size:.6rem;font-weight:900;
  letter-spacing:.08em;cursor:pointer;text-transform:uppercase;transition:all .2s;border-radius:2px}}
.close-btn:hover,.close-btn:active{{background:var(--red);color:#fff}}
.no-pos{{text-align:center;padding:28px 16px;color:#1e2d3a;font-size:.72rem}}
.no-pos-icon{{font-size:1.4rem;margin-bottom:6px;opacity:.4}}

/* WAVE SPACER */
.wave-spacer{{height:18px;background:var(--bg);position:relative;overflow:hidden}}
.wave-spacer::after{{content:'';position:absolute;bottom:0;left:-10%;width:120%;height:100%;
  background:linear-gradient(180deg,transparent,#00e5ff07);
  clip-path:ellipse(60% 100% at 50% 100%)}}

/* SHOES PINWHEEL */
.pinwheel-wrap{{perspective:700px;height:160px;position:relative;margin:14px 16px 4px;cursor:pointer}}
.pw-card{{
  position:absolute;top:0;left:0;right:0;
  padding:12px 14px;
  background:linear-gradient(135deg,#0d1825,#0a1520);
  border:1px solid #ffffff0e;border-radius:3px;
  transform-origin:top center;
  transition:transform .5s cubic-bezier(.34,1.56,.64,1),opacity .4s ease;
  backface-visibility:hidden;will-change:transform,opacity
}}
.pw-card[data-idx="0"]{{transform:rotateX(0deg) translateZ(0) scale(1);z-index:5;opacity:1}}
.pw-card[data-idx="1"]{{transform:rotateX(5deg) translateZ(-18px) scale(.96);z-index:4;opacity:.78}}
.pw-card[data-idx="2"]{{transform:rotateX(10deg) translateZ(-34px) scale(.92);z-index:3;opacity:.58}}
.pw-card[data-idx="3"]{{transform:rotateX(14deg) translateZ(-46px) scale(.88);z-index:2;opacity:.40}}
.pw-card[data-idx="4"]{{transform:rotateX(18deg) translateZ(-56px) scale(.84);z-index:1;opacity:.24}}
.pw-card.fly-off{{
  transform:rotateX(-45deg) translateZ(60px) translateY(-50px) scale(.75)!important;
  opacity:0!important;
  transition:transform .3s ease-in,opacity .25s ease-in!important;
  z-index:10!important
}}
.pw-top{{display:flex;align-items:center;gap:8px;margin-bottom:5px}}
.pw-sym{{font-family:'Bebas Neue',sans-serif;font-size:1.25rem;color:var(--cyan)}}
.pw-side{{font-size:.6rem;font-weight:900;padding:1px 6px;border:1px solid;letter-spacing:.05em}}
.pw-pnl{{font-family:'JetBrains Mono',monospace;font-size:.88rem;font-weight:700;margin-left:auto}}
.pw-detail{{font-size:.56rem;color:#2a3a4a;font-family:'JetBrains Mono',monospace}}
.pw-hint{{text-align:center;font-size:.48rem;color:#1a2535;letter-spacing:.14em;
  text-transform:uppercase;padding:4px 0 10px}}

/* LIVE FEED */
.feed-item{{padding:7px 16px;border-bottom:1px solid #ffffff04;font-size:.6rem;
  font-family:'JetBrains Mono',monospace;display:flex;gap:8px;align-items:flex-start}}
.feed-time{{color:#1e2d3a;font-size:.54rem;flex-shrink:0;margin-top:1px}}
.feed-text{{color:#555;line-height:1.5}}
.feed-text.open{{color:var(--cyan)}}.feed-text.win{{color:var(--green)}}.feed-text.loss{{color:var(--red)}}

/* PROGRESS */
.prog-wrap{{padding:12px 16px;background:linear-gradient(180deg,#0a1220,#07101e)}}
.prog-lbl{{font-size:.56rem;font-weight:700;color:#555;text-transform:uppercase;letter-spacing:.1em;
  margin-bottom:6px;display:flex;justify-content:space-between}}
.prog-track{{background:#ffffff07;height:3px;overflow:hidden;border-radius:2px}}
.prog-fill{{background:linear-gradient(90deg,var(--cyan),var(--green),#ffee00);height:3px;
  transition:width .8s ease;box-shadow:0 0 8px var(--cyan)}}
.milestones{{display:flex;flex-wrap:wrap;gap:3px;margin-top:8px}}
.ms{{font-size:.52rem;padding:2px 6px;font-weight:700;border:1px solid #ffffff08;color:#2a3a4a;background:#ffffff02}}
.ms.hit{{color:var(--cyan);border-color:#00e5ff35;background:#00e5ff0a;box-shadow:0 0 5px #00e5ff20}}

/* MANUAL */
.mkt-row{{display:flex;align-items:center;gap:5px;padding:6px 12px;border-bottom:1px solid #ffffff05}}
.mkt-lbl{{font-family:'Bebas Neue',sans-serif;font-size:.95rem;color:var(--cyan);width:58px;flex-shrink:0}}
.btn{{padding:5px 11px;font-size:.58rem;font-weight:900;letter-spacing:.06em;border:none;cursor:pointer;
  text-transform:uppercase;transition:all .15s;flex-shrink:0}}
.btn-long{{background:var(--green);color:#000}}.btn-short{{background:var(--red);color:#fff}}
.btn-x{{background:#0d1825;color:#444;border:1px solid #1a2535;padding:5px 9px}}
.btn-x:active{{color:#fff}}

footer{{padding:12px 16px;text-align:center;font-size:.56rem;color:#1e2d3a;
  border-top:1px solid #ffffff07;background:#050a14}}
footer a{{color:#00e5ff60;text-decoration:none}}

/* ANIMATIONS */
@keyframes glow{{
  0%,100%{{text-shadow:0 0 30px #00e5ff55,4px 4px 0 #ff3355}}
  50%{{text-shadow:0 0 60px #00e5ffaa,4px 4px 0 #ff3355,0 0 100px #00e5ff22}}
}}
@keyframes shimmer{{0%{{transform:translateX(-100%)}}100%{{transform:translateX(100%)}}}}
@keyframes slideUp{{from{{opacity:0;transform:translateY(16px)}}to{{opacity:1;transform:translateY(0)}}}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.dot{{display:inline-block;width:5px;height:5px;border-radius:50%;
  background:{mode_color};margin-right:5px;animation:pulse 2s infinite}}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<div class="wrap">

  <nav>
    <a href="/" class="active">HOME</a>
    <a href="/monitor">MONITOR</a>
    <a href="/trades">TRADES</a>
    <a href="/status/api" target="_blank">API ↗</a>
    <a href="https://drift.trade" target="_blank">DRIFT ↗</a>
  </nav>
  <div class="strip">
    <span class="strip-left"><span class="dot"></span>{exch} · ${PROFIT_GOAL:,.0f} goal</span>
    <span class="mode-pill">{mode}</span>
  </div>

  <div class="hero">
    <div class="hero-title">{DRIFT_BOT_NAME}</div>
    <div id="hero-cap" class="hero-cap">${_capital:.2f}</div>
    <div class="hero-row">
      <span id="hero-pnl">$0.00 PnL</span>
      <span id="hero-trades">0 trades</span>
      <span id="hero-secured">$0 secured</span>
    </div>
  </div>

  <div class="wave-div" style="background:#0a1220">
    <svg viewBox="0 0 430 16" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none">
      <path d="M0,16 C80,2 180,12 260,5 C340,-2 400,10 430,6 L430,16 Z" fill="#050a14"/>
    </svg>
  </div>

  <div class="stats">
    <div class="stat">
      <div class="lbl">Win Rate</div>
      <div class="val" id="stat-wr" style="color:var(--green)">0%</div>
    </div>
    <div class="stat">
      <div class="lbl">Open</div>
      <div class="val" id="stat-open" style="color:var(--cyan)">0/{DRIFT_MAX_OPEN}</div>
    </div>
    <div class="stat">
      <div class="lbl">Daily PnL</div>
      <div class="val" id="stat-dpnl" style="color:var(--green)">$0</div>
    </div>
  </div>

  <!-- LIVE POSITIONS -->
  <div class="sec">
    <div class="sec-hdr">
      <h2>LIVE POSITIONS</h2>
      <span id="pos-count">0</span>
    </div>
    <div id="pos-wrap">
      <div class="no-pos" id="no-pos">
        <div class="no-pos-icon">📡</div>
        Scanning for signals...
      </div>
    </div>
  </div>

  <div class="wave-spacer"></div>

  <!-- RECENT TRADES — SHOES PINWHEEL -->
  <div class="sec">
    <div class="sec-hdr"><h2>RECENT TRADES</h2><a href="/trades">ALL →</a></div>
    <div id="pinwheel" class="pinwheel-wrap" onclick="cyclePinwheel()">
      <div class="no-pos">No trades yet</div>
    </div>
    <div class="pw-hint" id="pw-hint"></div>
  </div>

  <!-- LIVE FEED -->
  <div class="sec">
    <div class="sec-hdr"><h2>LIVE FEED</h2><span id="feed-ts"></span></div>
    <div id="feed-wrap"><div class="no-pos">Waiting for signals...</div></div>
  </div>

  <!-- PROGRESS -->
  <div class="prog-wrap">
    <div class="prog-lbl"><span>GOAL PROGRESS</span><span style="color:var(--cyan)" id="prog-pct">0%</span></div>
    <div class="prog-track"><div class="prog-fill" id="prog-fill" style="width:0%"></div></div>
    <div class="milestones">
      {"".join(f'<span class="ms" id="ms-{m}">${m:,}</span>' for m in MILESTONES)}
    </div>
  </div>

  <!-- MANUAL CONTROLS -->
  <div class="sec">
    <div class="sec-hdr"><h2>MANUAL CONTROLS</h2></div>
    <div style="padding:12px 12px 4px">
      <select id="mkt-sel" style="
        width:100%;padding:9px 12px;background:#0d1825;color:var(--cyan);
        border:1px solid #00e5ff30;font-size:.8rem;font-weight:700;
        font-family:'Bebas Neue',sans-serif;letter-spacing:.08em;
        border-radius:2px;outline:none;margin-bottom:8px;
        -webkit-appearance:none;cursor:pointer">
        {"".join(f'<option value="{mk}">{mk}-PERP</option>' for mk in markets_list)}
      </select>
      <div style="display:flex;gap:6px">
        <button onclick="manualTrade(document.getElementById('mkt-sel').value,'long')" style="
          flex:1;padding:10px;background:var(--green);color:#000;border:none;
          font-size:.65rem;font-weight:900;letter-spacing:.08em;cursor:pointer;
          text-transform:uppercase;border-radius:2px">▲ LONG</button>
        <button onclick="manualTrade(document.getElementById('mkt-sel').value,'short')" style="
          flex:1;padding:10px;background:var(--red);color:#fff;border:none;
          font-size:.65rem;font-weight:900;letter-spacing:.08em;cursor:pointer;
          text-transform:uppercase;border-radius:2px">▼ SHORT</button>
        <button onclick="closePos(document.getElementById('mkt-sel').value)" style="
          padding:10px 14px;background:#0d1825;color:#555;border:1px solid #1a2535;
          font-size:.65rem;font-weight:900;cursor:pointer;border-radius:2px">✕</button>
      </div>
    </div>
    <div style="padding:10px 12px 12px;border-top:1px solid #ffffff06;margin-top:8px">
      <button onclick="resetCapital()" style="
        width:100%;padding:10px;background:transparent;
        border:1px solid #ff335540;color:#ff3355;
        font-size:.62rem;font-weight:900;letter-spacing:.1em;
        cursor:pointer;text-transform:uppercase;border-radius:2px">
        ⟳ RESET CAPITAL TO ${STARTING_CAPITAL:.0f}
      </button>
    </div>
  </div>

  <footer>{DRIFT_BOT_NAME} · {exch} · <a href="/monitor">Monitor</a> · <a href="/trades">All Trades</a></footer>
</div>

<script>
const charts = {{}}, pnlH = {{}};
const MS = {json.dumps(MILESTONES)};
const START_CAP = {STARTING_CAPITAL};
const GOAL = {PROFIT_GOAL};
const MAX_OPEN = {DRIFT_MAX_OPEN};

// ── SHOES PINWHEEL ──
let _allTrades = [], _tradeOffset = 0;

function buildPinwheel(tradeList) {{
  _allTrades = tradeList;
  renderPinwheel();
}}

function renderPinwheel() {{
  const wrap = document.getElementById('pinwheel');
  const hint = document.getElementById('pw-hint');
  if (!_allTrades.length) {{
    wrap.innerHTML = '<div class="no-pos">No trades yet</div>';
    if (hint) hint.textContent = '';
    return;
  }}
  const total = _allTrades.length;
  const shown = Math.min(5, total);
  let html = '';
  for (let i = 0; i < shown; i++) {{
    const t = _allTrades[(_tradeOffset + i) % total];
    const sc = t.side === 'long' ? '#00ff88' : '#ff3355';
    const pc = t.pnl >= 0 ? '#00ff88' : '#ff3355';
    const dur = t.duration_s ? Math.floor(t.duration_s / 60) + 'm' : '';
    html += `<div class="pw-card" data-idx="${{i}}">
      <div class="pw-top">
        <span class="pw-sym">${{t.market}}</span>
        <span class="pw-side" style="color:${{sc}};border-color:${{sc}}">${{t.side.toUpperCase()}}</span>
        <span class="pw-pnl" style="color:${{pc}}">${{t.pnl>=0?'+':''}}$${{t.pnl.toFixed(2)}}</span>
      </div>
      <div class="pw-detail">$${{t.entry.toFixed(4)}} → $${{t.exit.toFixed(4)}} · ${{t.reason}}</div>
      <div class="pw-detail" style="color:#1a2535;margin-top:2px">${{t.ts}}${{dur?' · '+dur:''}}</div>
    </div>`;
  }}
  wrap.innerHTML = html;
  if (hint) hint.textContent = total > 1 ? '▸ TAP TO CYCLE · ' + total + ' TRADES' : '';
}}

function cyclePinwheel() {{
  if (_allTrades.length <= 1) return;
  const top = document.querySelector('#pinwheel .pw-card[data-idx="0"]');
  if (!top) return;
  top.classList.add('fly-off');
  setTimeout(() => {{
    _tradeOffset = (_tradeOffset + 1) % _allTrades.length;
    renderPinwheel();
  }}, 300);
}}

// ── POSITION CARD TAP TO EXPAND ──
function toggleCard(el, event) {{
  if (event.target.classList.contains('close-btn')) return;
  el.classList.toggle('expanded');
}}

// ── POLLS ──
async function poll() {{
  try {{
    const d = await fetch('/status/api').then(r => r.json());
    updateHero(d);
    updatePositions(d.positions || {{}});
    updateProgress(d.capital);
  }} catch(e) {{}}
}}

async function pollTrades() {{
  try {{
    const trades = await fetch('/trades/api').then(r => r.json());
    buildPinwheel(trades);
  }} catch(e) {{}}
}}

async function pollFeed() {{
  try {{
    const d = await fetch('/notify/api').then(r => r.json());
    document.getElementById('feed-ts').textContent = new Date().toLocaleTimeString();
    const wrap = document.getElementById('feed-wrap');
    if (!d.length) {{ wrap.innerHTML = '<div class="no-pos">Waiting for signals...</div>'; return; }}
    wrap.innerHTML = d.slice(0,5).map(n => {{
      const txt = n.text || '';
      const cls = txt.includes('OPEN') ? 'open'
        : txt.includes('+$') && txt.includes('CLOSE') ? 'win'
        : txt.includes('CLOSE') ? 'loss' : '';
      const clean = txt.replace(/\*/g,'').replace(/\\n/g,' · ');
      return `<div class="feed-item">
        <span class="feed-time">${{n.time}}</span>
        <span class="feed-text ${{cls}}">${{clean}}</span>
      </div>`;
    }}).join('');
  }} catch(e) {{}}
}}

function updateHero(d) {{
  document.getElementById('hero-cap').textContent = '$' + d.capital.toFixed(2);
  const pnl = d.total_pnl || 0;
  const pnlEl = document.getElementById('hero-pnl');
  pnlEl.textContent = (pnl >= 0 ? '+' : '') + '$' + pnl.toFixed(2) + ' PnL';
  pnlEl.style.color = pnl >= 0 ? '#00ff88' : '#ff3355';
  document.getElementById('hero-trades').textContent = d.total_trades + ' trades';
  document.getElementById('hero-secured').textContent = '$' + (d.profit_secured || 0).toFixed(0) + ' secured';
  const wr = d.win_rate || 0;
  const wrEl = document.getElementById('stat-wr');
  wrEl.textContent = wr + '%';
  wrEl.style.color = wr >= 50 ? '#00ff88' : '#ff3355';
  document.getElementById('stat-open').textContent = Object.keys(d.positions || {{}}).length + '/' + MAX_OPEN;
  const dp = d.daily_pnl || 0;
  const dpEl = document.getElementById('stat-dpnl');
  dpEl.textContent = (dp >= 0 ? '+' : '') + '$' + dp.toFixed(2);
  dpEl.style.color = dp >= 0 ? '#00ff88' : '#ff3355';
}}

function updatePositions(positions) {{
  const wrap = document.getElementById('pos-wrap');
  const noPos = document.getElementById('no-pos');
  const keys = Object.keys(positions);
  document.getElementById('pos-count').textContent = keys.length;
  noPos.style.display = keys.length ? 'none' : 'block';

  keys.forEach((market, i) => {{
    const pos = positions[market];
    const pnl = pos.pnl || 0;
    if (!pnlH[market]) pnlH[market] = [];
    pnlH[market].push(pnl);
    if (pnlH[market].length > 80) pnlH[market].shift();

    let card = document.getElementById('pc-' + market);
    if (!card) {{
      card = document.createElement('div');
      card.id = 'pc-' + market;
      card.className = 'pos-card';
      card.setAttribute('onclick', 'toggleCard(this, event)');
      card.innerHTML = buildCard(market, pos);
      card.style.animation = 'slideUp .4s ease ' + (i * 0.08) + 's both';
      wrap.appendChild(card);
      setTimeout(() => initChart(market), 80);
    }} else {{
      const pnlEl = document.getElementById('pp-' + market);
      const c = pnl >= 0 ? '#00ff88' : '#ff3355';
      const margin = (pos.size || 0) / (pos.leverage || 1);
      const pct = margin > 0 ? (pnl / margin * 100).toFixed(1) : '0.0';
      if (pnlEl) {{
        pnlEl.textContent = (pnl >= 0 ? '+' : '') + '$' + pnl.toFixed(2) + ' (' + (pnl >= 0 ? '+' : '') + pct + '%)';
        pnlEl.style.color = c;
      }}
      const curEl = document.getElementById('cur-' + market);
      if (curEl) curEl.textContent = '$' + (pos.current_price || pos.entry || 0).toFixed(4);
      const durEl = document.getElementById('dur-' + market);
      if (durEl && pos.opened_at) {{
        durEl.textContent = Math.floor((Date.now() / 1000 - pos.opened_at) / 60) + 'm open';
      }}
      const isExpanded = card.classList.contains('expanded');
      card.className = 'pos-card' + (isExpanded ? ' expanded' : '') + (pnl >= 0 ? ' profit' : ' loss');
      updateChart(market, pnl);
    }}
  }});

  document.querySelectorAll('.pos-card').forEach(el => {{
    const m = el.id.replace('pc-', '');
    if (!positions[m]) {{
      if (charts[m]) {{ charts[m].destroy(); delete charts[m]; }}
      delete pnlH[m];
      el.remove();
    }}
  }});
}}

function buildCard(market, pos) {{
  const side = pos.side || 'long';
  const sc = side === 'long' ? '#00ff88' : '#ff3355';
  const pnl = pos.pnl || 0;
  const pc = pnl >= 0 ? '#00ff88' : '#ff3355';
  const margin = (pos.size || 0) / (pos.leverage || 1);
  const pct = margin > 0 ? (pnl / margin * 100).toFixed(1) : '0.0';
  const cur = pos.current_price || pos.entry || 0;
  const openedAt = pos.opened_at || 0;
  const openedStr = openedAt ? new Date(openedAt * 1000).toLocaleTimeString() : '—';
  const mins = openedAt ? Math.floor((Date.now() / 1000 - openedAt) / 60) : 0;
  return `
  <div class="pos-body">
    <div class="pos-top">
      <span class="pos-sym">${{market}}-PERP</span>
      <span class="pos-side" style="color:${{sc}};border-color:${{sc}}">${{side.toUpperCase()}}</span>
      <span class="pos-pnl" id="pp-${{market}}" style="color:${{pc}}">${{pnl>=0?'+':''}}$${{pnl.toFixed(2)}} (${{pnl>=0?'+':''}}${{pct}}%)</span>
    </div>
    <div class="pos-meta">
      <span><b>Entry</b> $${{(pos.entry||0).toFixed(4)}}</span>
      <span><b>Now</b> <span id="cur-${{market}}">$${{cur.toFixed(4)}}</span></span>
      <span><b>TP</b> <span style="color:#00ff8870">$${{(pos.tp||0).toFixed(4)}}</span></span>
      <span><b>SL</b> <span style="color:#ff335570">$${{(pos.sl||0).toFixed(4)}}</span></span>
    </div>
    <canvas id="chart-${{market}}" class="mini" height="60"></canvas>
  </div>
  <div class="pos-detail">
    <div class="pos-ts-row">
      <span>Opened ${{openedStr}}</span>
      <span id="dur-${{market}}">${{mins}}m open</span>
    </div>
    <div class="pos-notional">$${{(pos.size||0).toFixed(2)}} @ ${{pos.leverage||1}}x leverage</div>
  </div>
  <div class="pos-hint">TAP FOR DETAILS <span class="pos-chevron">▾</span></div>
  <button class="close-btn" onclick="closePos('${{market}}')">✕ CLOSE ${{market}}</button>`;
}}

function initChart(market) {{
  const el = document.getElementById('chart-' + market);
  if (!el || charts[market]) return;
  charts[market] = new Chart(el.getContext('2d'), {{
    type: 'line',
    data: {{ labels: [], datasets: [{{
      data: [], borderColor: '#00e5ff',
      backgroundColor: 'rgba(0,229,255,0.1)',
      borderWidth: 1.5, pointRadius: 0, tension: 0.45, fill: true
    }}]}},
    options: {{
      responsive: true, animation: {{ duration: 350 }},
      plugins: {{ legend: {{ display: false }},
        tooltip: {{ callbacks: {{ label: c => (c.parsed.y>=0?'+':'')+'$'+c.parsed.y.toFixed(2) }} }} }},
      scales: {{
        x: {{ display: false }},
        y: {{ grid: {{ color: '#ffffff06' }}, ticks: {{ color: '#333', font: {{ size: 8 }}, callback: v => '$'+v.toFixed(1) }} }}
      }}
    }}
  }});
  updateChart(market, pnlH[market]?.[pnlH[market].length-1] || 0);
}}

function updateChart(market, latestPnl) {{
  const c = charts[market]; if (!c) return;
  const hist = pnlH[market] || [];
  const col = latestPnl >= 0 ? '#00ff88' : '#ff3355';
  const ctx = c.canvas.getContext('2d');
  const g = ctx.createLinearGradient(0, 0, 0, 60);
  g.addColorStop(0, latestPnl >= 0 ? 'rgba(0,255,136,0.22)' : 'rgba(255,51,85,0.22)');
  g.addColorStop(1, 'rgba(0,0,0,0)');
  c.data.labels = hist.map((_,i) => i);
  c.data.datasets[0].data = hist;
  c.data.datasets[0].borderColor = col;
  c.data.datasets[0].backgroundColor = g;
  c.update('none');
}}

function updateProgress(cap) {{
  const next = MS.find(m => m > cap) || GOAL;
  const pct = Math.max(0, Math.min(100, Math.round((cap - START_CAP) / Math.max(next - START_CAP, 1) * 100)));
  document.getElementById('prog-fill').style.width = pct + '%';
  document.getElementById('prog-pct').textContent = pct + '%';
  MS.forEach(m => {{
    const el = document.getElementById('ms-' + m);
    if (el) el.className = cap >= m ? 'ms hit' : 'ms';
  }});
}}

function manualTrade(market, side) {{
  if (!confirm('Open ' + side.toUpperCase() + ' ' + market + '?')) return;
  fetch('/api/manual-' + side + '/' + market, {{ method: 'POST' }})
    .then(r => r.json()).then(d => alert(d.msg || d.error)).catch(e => alert(e));
}}
function closePos(market) {{
  if (!confirm('Close ' + market + '?')) return;
  fetch('/api/close/' + market, {{ method: 'POST' }})
    .then(r => r.json()).then(d => alert(d.msg || d.error)).catch(e => alert(e));
}}
function resetCapital() {{
  if (!confirm('Reset capital to ${STARTING_CAPITAL:.0f} and clear all trade history?\\nThis cannot be undone.')) return;
  fetch('/api/reset-capital', {{ method: 'POST' }})
    .then(r => r.json()).then(d => {{ alert(d.msg || d.error); poll(); pollTrades(); }})
    .catch(e => alert(e));
}}

poll(); pollTrades(); pollFeed();
setInterval(poll, 3000);
setInterval(pollTrades, 5000);
setInterval(pollFeed, 4000);
</script>
</body></html>"""


@app.route("/trades", methods=["GET"])
def trades_page():
    with _state_lock:
        all_trades = list(_trades)

    wins   = [t for t in all_trades if t["pnl"] >= 0]
    total  = len(all_trades)
    wr     = round(len(wins) / max(total, 1) * 100, 1)
    total_pnl = sum(t["pnl"] for t in all_trades)
    sign        = "+" if total_pnl >= 0 else ""
    wr_color    = "#00ff88" if wr >= 50 else "#ff3355"
    pnl_color   = "#00ff88" if total_pnl >= 0 else "#ff3355"
    rows_empty  = '<tr><td colspan="9" class="empty">No trades yet</td></tr>'

    rows = ""
    for t in all_trades:
        t_color    = "#00ff88" if t["pnl"] >= 0 else "#ff3355"
        t_icon     = "▲" if t["pnl"] >= 0 else "▼"
        t_sign     = "+" if t["pnl"] >= 0 else ""
        t_side_clr = "#00ff88" if t["side"] == "long" else "#ff3355"
        t_badge    = "win" if t["pnl"] >= 0 else "loss"
        dur_m      = round(t.get("duration_s", 0) / 60, 1)
        rows += (
            f'<tr>'
            f'<td class="sym">{t["market"]}</td>'
            f'<td style="color:{t_side_clr};font-weight:900">{t["side"].upper()}</td>'
            f'<td class="mono">${t["entry"]:.4f}</td>'
            f'<td class="mono">${t["exit"]:.4f}</td>'
            f'<td class="mono" style="color:{t_color}">{t_icon} {t_sign}${t["pnl"]:.2f}</td>'
            f'<td class="mono" style="color:{t_color}">{t_sign}{t["pnl_pct"]:.1f}%</td>'
            f'<td><span class="badge {t_badge}">{t["reason"]}</span></td>'
            f'<td class="muted">{dur_m}m</td>'
            f'<td class="muted">{t["ts"]}</td>'
            f'</tr>'
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<meta http-equiv="refresh" content="30">
<title>Trades — {DRIFT_BOT_NAME}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{--cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--bg:#050a14;--card:#0a1220;--border:#ffffff15}}
  body{{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}}
  .bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;object-position:center;opacity:.30;pointer-events:none;z-index:0}}
  .wrap{{position:relative;z-index:1}}
  nav{{display:flex;gap:0;border-bottom:2px solid var(--cyan);overflow-x:auto;scrollbar-width:none}}
  nav::-webkit-scrollbar{{display:none}}
  nav a{{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;padding:10px 14px;
    white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;border-right:1px solid var(--border);transition:all .15s}}
  nav a:hover{{background:var(--cyan);color:#000}}
  .page-title{{font-family:'Bebas Neue',sans-serif;font-size:3rem;color:var(--cyan);
    text-shadow:0 0 24px #00e5ff88;padding:18px 16px 8px;line-height:1;letter-spacing:.04em}}
  .stats-bar{{display:flex;gap:2px;background:var(--cyan);margin:0 0 2px}}
  .stat-item{{background:var(--card);padding:12px 16px;flex:1}}
  .stat-item .lbl{{font-size:.56rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
  .stat-item .val{{font-family:'Bebas Neue',sans-serif;font-size:1.6rem;margin-top:2px}}
  .cyan{{color:var(--cyan)}} .green{{color:var(--green)}} .red{{color:var(--red)}} .muted{{color:#888}}
  .section{{background:var(--card);border-bottom:2px solid var(--border)}}
  .tbl-wrap{{overflow-x:auto}}
  table{{width:100%;border-collapse:collapse;font-size:.68rem}}
  th{{padding:8px 10px;font-size:.54rem;font-weight:700;color:#888;text-transform:uppercase;
    letter-spacing:.08em;text-align:left;border-bottom:1px solid var(--border);
    white-space:nowrap;background:#080f1a}}
  td{{padding:8px 10px;border-bottom:1px solid #ffffff05;vertical-align:middle}}
  .sym{{font-weight:900;font-size:.78rem}}
  .mono{{font-family:'JetBrains Mono',monospace;font-size:.64rem}}
  .badge{{display:inline-block;padding:2px 6px;font-size:.54rem;font-weight:900;letter-spacing:.06em;border:1px solid}}
  .badge.win{{color:var(--green);border-color:var(--green);background:#00ff8815}}
  .badge.loss{{color:var(--red);border-color:var(--red);background:#ff335515}}
  .empty{{text-align:center;padding:24px;color:#444;font-size:.75rem}}
  footer{{padding:14px 16px;text-align:center;font-size:.6rem;color:#444;border-top:1px solid var(--border)}}
  footer a{{color:var(--cyan);text-decoration:none}}
</style>
</head>
<body>
<img src="/static/tankgirl.png" class="bg-art" alt="">
<div class="wrap">
  <nav>
    <a href="/">HOME</a>
    <a href="/trades">TRADES</a>
    <a href="/status/api">API</a>
    <a href="https://drift.trade" target="_blank">DRIFT</a>
  </nav>
  <div class="page-title">TRADE HISTORY</div>
  <div class="stats-bar">
    <div class="stat-item">
      <div class="lbl">Total</div>
      <div class="val cyan">{total}</div>
    </div>
    <div class="stat-item">
      <div class="lbl">Win Rate</div>
      <div class="val" style="color:{wr_color}">{wr}%</div>
    </div>
    <div class="stat-item">
      <div class="lbl">Total PnL</div>
      <div class="val" style="color:{pnl_color}">{sign}${total_pnl:.2f}</div>
    </div>
  </div>
  <div class="section">
    <div class="tbl-wrap"><table>
      <thead><tr><th>Market</th><th>Side</th><th>Entry</th><th>Exit</th><th>PnL $</th><th>PnL %</th><th>Reason</th><th>Dur</th><th>Time</th></tr></thead>
      <tbody>{rows if rows else rows_empty}</tbody>
    </table></div>
  </div>
  <footer>{DRIFT_BOT_NAME} &nbsp;·&nbsp; <a href="/">← Back</a></footer>
</div>
</body></html>"""
    return html, 200


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
        "leverage": DRIFT_LEVERAGE,
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
        size_usd = _capital * DRIFT_TRADE_PCT * DRIFT_LEVERAGE

    open_position(market, "long", price, size_usd, DRIFT_LEVERAGE)
    return jsonify({"msg": f"Opened LONG {market} @ ${price:.4f} size=${size_usd:.2f}"})


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
        size_usd = _capital * DRIFT_TRADE_PCT * DRIFT_LEVERAGE

    open_position(market, "short", price, size_usd, DRIFT_LEVERAGE)
    return jsonify({"msg": f"Opened SHORT {market} @ ${price:.4f} size=${size_usd:.2f}"})


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
    if not DRIFT_PAPER_MODE:
        with _state_lock:
            open_mkts = list(_positions.keys())
        if open_mkts:
            return jsonify({"error": f"Cannot reset with open live positions: {open_mkts}. Close them first."}), 400
    with _state_lock:
        _capital         = STARTING_CAPITAL
        _trades          = []
        _daily_pnl       = 0.0
        _profit_secured  = 0.0
        _positions       = {}
    _save_state()
    log("ok", f"Capital reset to ${STARTING_CAPITAL:.2f} — all trades/positions cleared")
    notify(f"*{DRIFT_BOT_NAME}* CAPITAL RESET\nCapital: ${STARTING_CAPITAL:.2f}")
    return jsonify({"msg": f"Reset to ${STARTING_CAPITAL:.2f}", "capital": STARTING_CAPITAL})


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

                # SL/TP checked every 5s
                pnl = updated.get("pnl", 0)
                if DRIFT_TP_USD > 0 and pnl >= DRIFT_TP_USD:
                    close_position(market, price, f"TP${DRIFT_TP_USD:.0f}")
                elif DRIFT_SL_USD > 0 and pnl <= -DRIFT_SL_USD:
                    close_position(market, price, f"SL${DRIFT_SL_USD:.0f}")
                elif (updated["side"] == "long" and price >= updated["tp"]) or \
                     (updated["side"] == "short" and price <= updated["tp"]):
                    close_position(market, price, "TP")
                elif (updated["side"] == "long" and price <= updated["sl"]) or \
                     (updated["side"] == "short" and price >= updated["sl"]):
                    close_position(market, price, "SL")
        except Exception as e:
            log("warn", f"Price updater error: {e}")
        time.sleep(5)


@app.route("/monitor", methods=["GET"])
def monitor():
    mode = "PAPER" if DRIFT_PAPER_MODE else "LIVE"
    mode_color = "#ffee00" if DRIFT_PAPER_MODE else "#39ff14"
    markets_list = [m.strip().upper() for m in DRIFT_MARKETS.split(",")]
    close_btns = "".join(
        f'<option value="{m}">{m}</option>' for m in markets_list
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>Monitor — {DRIFT_BOT_NAME}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{--cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--yellow:#ffee00;--bg:#050a14;--card:#0a1220;--border:#ffffff15}}
body{{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;min-height:100vh}}
nav{{display:flex;border-bottom:2px solid var(--cyan);overflow-x:auto;scrollbar-width:none}}
nav a{{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;padding:10px 14px;white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;border-right:1px solid var(--border);transition:background .15s}}
nav a:hover,nav a.active{{background:var(--cyan);color:#000}}
.strip{{background:var(--card);border-bottom:2px solid var(--border);padding:5px 16px;display:flex;justify-content:space-between;align-items:center}}
.strip-left{{font-size:.6rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
.pill{{font-size:.65rem;font-weight:900;padding:3px 10px;background:{mode_color};color:#000}}
.dot{{display:inline-block;width:6px;height:6px;border-radius:50%;background:{mode_color};margin-right:5px;animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
.section{{margin-bottom:2px;background:var(--card)}}
.sec-hdr{{padding:10px 16px;border-bottom:1px solid var(--border);font-size:.6rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em;display:flex;justify-content:space-between;align-items:center}}
.sec-hdr span{{color:var(--cyan)}}
.pos-card{{padding:14px 16px;border-bottom:1px solid var(--border)}}
.pos-top{{display:flex;align-items:baseline;gap:10px;margin-bottom:6px}}
.pos-sym{{font-family:'Bebas Neue',sans-serif;font-size:1.4rem;color:var(--cyan)}}
.pos-side{{font-size:.68rem;font-weight:900;padding:2px 7px;border:1px solid}}
.pos-pnl{{font-family:'JetBrains Mono',monospace;font-size:1.1rem;font-weight:700;margin-left:auto}}
.pos-meta{{display:grid;grid-template-columns:1fr 1fr;gap:3px 12px;font-size:.62rem;color:#666;font-family:'JetBrains Mono',monospace;margin-bottom:10px}}
.pos-meta b{{color:#aaa;font-weight:400}}
canvas{{width:100%!important;display:block;margin-bottom:10px}}
.close-btn{{width:100%;padding:9px;background:transparent;border:1px solid var(--red);color:var(--red);font-size:.65rem;font-weight:900;letter-spacing:.08em;cursor:pointer;text-transform:uppercase;transition:all .15s}}
.close-btn:hover{{background:var(--red);color:#fff}}
.empty{{text-align:center;padding:30px 16px;color:#444;font-size:.75rem}}
.log-box{{height:220px;overflow-y:auto;padding:10px 16px;font-family:'JetBrains Mono',monospace;font-size:.62rem;line-height:1.7;background:#040810}}
.log-ok{{color:#00ff88}}.log-err{{color:#ff3355}}.log-warn{{color:#ffee00}}.log-info{{color:#888}}
.trade-row{{display:grid;grid-template-columns:2fr 1fr 1fr 1.2fr 1fr;gap:4px;padding:8px 16px;border-bottom:1px solid var(--border);font-size:.65rem;font-family:'JetBrains Mono',monospace;align-items:center}}
.trade-row.hdr{{font-size:.56rem;color:#666;font-weight:700;text-transform:uppercase;letter-spacing:.06em;padding:7px 16px;background:#080f1a}}
.badge{{display:inline-block;padding:1px 5px;font-size:.54rem;font-weight:900;border:1px solid}}
.badge.win{{color:var(--green);border-color:var(--green)}}.badge.loss{{color:var(--red);border-color:var(--red)}}
</style>
</head>
<body>
<nav>
  <a href="/">HOME</a>
  <a href="/monitor" class="active">MONITOR</a>
  <a href="/trades">TRADES</a>
  <a href="/status/api" target="_blank">API ↗</a>
  <a href="https://drift.trade" target="_blank">DRIFT ↗</a>
</nav>
<div class="strip">
  <span class="strip-left"><span class="dot"></span>LIVE MONITOR</span>
  <span class="pill">{mode}</span>
</div>

<div class="section">
  <div class="sec-hdr">OPEN POSITIONS <span id="pos-count">0</span></div>
  <div id="pos-wrap"><div class="empty" id="no-pos">Scanning for signals...</div></div>
</div>

<div class="section">
  <div class="sec-hdr">RECENT TRADES <span id="trade-count">0</span></div>
  <div class="trade-row hdr"><span>MARKET</span><span>SIDE</span><span>PNL</span><span>RESULT</span><span>TIME</span></div>
  <div id="trades-wrap"><div class="empty">No trades yet</div></div>
</div>

<div class="section">
  <div class="sec-hdr">LIVE FEED <span id="log-ts"></span></div>
  <div class="log-box" id="log-box">Connecting...</div>
</div>

<script>
const charts = {{}};
const pnlHist = {{}};

async function poll() {{
  try {{
    const d = await fetch('/status/api').then(r => r.json());
    const pos = d.positions || {{}};
    const keys = Object.keys(pos);

    document.getElementById('pos-count').textContent = keys.length;

    const wrap = document.getElementById('pos-wrap');
    const noPos = document.getElementById('no-pos');
    noPos.style.display = keys.length ? 'none' : 'block';

    // Add / update position cards
    keys.forEach(market => {{
      const p = pos[market];
      const pnl = p.pnl || 0;
      const cur = p.current_price || p.entry;

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
        const pnlEl = document.getElementById('pp-' + market);
        const curEl = document.getElementById('pc2-' + market);
        const c = pnl >= 0 ? '#00ff88' : '#ff3355';
        if (pnlEl) {{ pnlEl.textContent = (pnl>=0?'+':'') + '$' + pnl.toFixed(2); pnlEl.style.color = c; }}
        if (curEl) curEl.textContent = '$' + cur.toFixed(4);
        updateChart(market, pnl);
      }}
    }});

    // Remove closed
    document.querySelectorAll('.pos-card').forEach(el => {{
      const m = el.id.replace('pc-','');
      if (!pos[m]) {{ if(charts[m]){{charts[m].destroy();delete charts[m];}} delete pnlHist[m]; el.remove(); }}
    }});

  }} catch(e) {{ console.error(e); }}
}}

async function pollTrades() {{
  try {{
    const trades = await fetch('/trades/api').then(r => r.json());
    document.getElementById('trade-count').textContent = trades.length;
    const wrap = document.getElementById('trades-wrap');
    if (!trades.length) {{ wrap.innerHTML = '<div class="empty">No trades yet</div>'; return; }}
    wrap.innerHTML = trades.slice(0,15).map(t => {{
      const sc = t.side==='long' ? '#00ff88' : '#ff3355';
      const pc = t.pnl>=0 ? '#00ff88' : '#ff3355';
      const badge = t.pnl>=0 ? 'win' : 'loss';
      return `<div class="trade-row">
        <span style="color:var(--cyan);font-weight:900">${{t.market}}</span>
        <span style="color:${{sc}}">${{t.side.toUpperCase()}}</span>
        <span style="color:${{pc}}">${{t.pnl>=0?'+':''}}$${{t.pnl.toFixed(2)}}</span>
        <span><span class="badge ${{badge}}">${{t.reason}}</span></span>
        <span style="color:#666">${{t.ts.slice(-5)}}</span>
      </div>`;
    }}).join('');
  }} catch(e) {{}}
}}

async function pollLogs() {{
  try {{
    const d = await fetch('/notify/api').then(r => r.json());
    const box = document.getElementById('log-box');
    document.getElementById('log-ts').textContent = new Date().toLocaleTimeString();
    if (!d.length) {{ box.innerHTML = '<span style="color:#444">Waiting for first signal...</span>'; return; }}
    box.innerHTML = d.map(n => {{
      const txt = n.text || '';
      const isOpen = txt.includes('OPEN ');
      const isClose = txt.includes('CLOSE ');
      const isMile = txt.includes('MILESTONE');
      const isStart = txt.includes('started');
      const color = isClose && txt.includes('+') ? '#00ff88' :
                    isClose && txt.includes('-') ? '#ff3355' :
                    isOpen ? '#00e5ff' :
                    isMile ? '#ffee00' : '#888';
      const icon = isOpen ? '▶' : isClose ? (txt.includes('+') ? '✅' : '❌') : isMile ? '🏆' : '•';
      const lines = txt.replace(/\*/g,'').split('\\n').filter(Boolean);
      return `<div style="border-left:2px solid ${{color}};padding:6px 10px;margin-bottom:6px;background:#ffffff05">
        <div style="font-size:.58rem;color:#555;margin-bottom:3px">[${{n.time}}]</div>
        ${{lines.map((l,i) => `<div style="color:${{i===0?color:'#aaa'}};font-weight:${{i===0?'700':'400'}}">${{icon}} ${{l}}</div>`).join('')}}
      </div>`;
    }}).join('');
  }} catch(e) {{}}
}}

function cardHTML(market, p) {{
  const side = p.side || 'long';
  const sc = side==='long' ? '#00ff88' : '#ff3355';
  const pnl = p.pnl || 0;
  const pc = pnl>=0 ? '#00ff88' : '#ff3355';
  const cur = p.current_price || p.entry;
  return `
    <div class="pos-top">
      <span class="pos-sym">${{market}}-PERP</span>
      <span class="pos-side" style="color:${{sc}};border-color:${{sc}}">${{side.toUpperCase()}}</span>
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
    <button class="close-btn" onclick="closePos('${{market}}')">✕ CLOSE ${{market}}</button>
  `;
}}

function initChart(market) {{
  const el = document.getElementById('chart-' + market);
  if (!el || charts[market]) return;
  charts[market] = new Chart(el.getContext('2d'), {{
    type: 'line',
    data: {{
      labels: [],
      datasets: [{{
        data: [],
        borderColor: '#00e5ff',
        backgroundColor: 'rgba(0,229,255,0.07)',
        borderWidth: 2, pointRadius: 0, tension: 0.4, fill: true
      }}]
    }},
    options: {{
      responsive: true, animation: {{duration:300}},
      plugins: {{ legend: {{display:false}}, tooltip: {{ callbacks: {{ label: c => (c.parsed.y>=0?'+':'')+'$'+c.parsed.y.toFixed(2) }} }} }},
      scales: {{
        x: {{display:false}},
        y: {{ grid:{{color:'#ffffff10'}}, ticks:{{color:'#555',font:{{size:9}},callback:v=>'$'+v.toFixed(2)}} }}
      }}
    }}
  }});
  updateChart(market, pnlHist[market]?.slice(-1)[0] || 0);
}}

function updateChart(market, latestPnl) {{
  const c = charts[market];
  if (!c) return;
  const hist = pnlHist[market] || [];
  const color = latestPnl >= 0 ? '#00ff88' : '#ff3355';
  c.data.labels = hist.map((_,i) => i);
  c.data.datasets[0].data = hist;
  c.data.datasets[0].borderColor = color;
  c.data.datasets[0].backgroundColor = latestPnl>=0 ? 'rgba(0,255,136,0.07)' : 'rgba(255,51,85,0.07)';
  c.update('none');
}}

function closePos(market) {{
  if (!confirm('Close ' + market + '?')) return;
  fetch('/api/close/' + market, {{method:'POST'}})
    .then(r=>r.json()).then(d=>alert(d.msg||d.error)).catch(e=>alert(e));
}}

poll();
pollTrades();
pollLogs();
setInterval(poll, 3000);
setInterval(pollTrades, 5000);
setInterval(pollLogs, 3000);
</script>
</body></html>"""


# ── ENTRY POINT ───────────────────────────────────────────────────
if __name__ == "__main__":
    if not DRIFT_PAPER_MODE:
        if not WALLET or not WALLET_PRIVATE_KEY:
            print("[FATAL] DRIFT_PAPER_MODE=false requires WALLET and WALLET_PRIVATE_KEY. Exiting.")
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
    log("ok", f"Exchange: {DRIFT_EXCHANGE.upper()} | Markets: {DRIFT_MARKETS} | Leverage: {DRIFT_LEVERAGE}x")
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
        f"Leverage: {DRIFT_LEVERAGE}x | Capital: ${_capital:.2f}"
    )

    app.run(host="0.0.0.0", port=DRIFT_PORT, debug=False)
