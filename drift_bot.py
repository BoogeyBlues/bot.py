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
DRIFT_SL_PCT       = float(os.environ.get("DRIFT_SL_PCT", "0.10"))
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
DRIFT_COMPOUND_PCT = float(os.environ.get("DRIFT_COMPOUND_PCT", "0.10"))  # % of profit reinvested
REDIS_URL          = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN        = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

MILESTONES = [250, 500, 1000, 2500, 5000, 10000, 25000]

# ── STATE ─────────────────────────────────────────────────────────
_positions       = {}
_trades          = []
_capital         = STARTING_CAPITAL
_profit_secured  = 0.0   # total profit taken out
_daily_pnl     = 0.0
_day_start     = ""
_price_history = {}
_milestones_hit = set()
_state_lock    = threading.Lock()
_log_buffer    = []
_log_lock      = threading.Lock()
_notify_queue  = []
_notify_q_lock = threading.Lock()
_notify_log    = []   # last 50 messages sent (same text as Telegram)
_signals_cache = {}   # market -> {signal, ts}

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
    global _profit_secured
    with _state_lock:
        cap  = _capital
        pos  = {k: dict(v) for k, v in _positions.items()}
        trd  = list(_trades)
        ms   = list(_milestones_hit)
    redis_save("drift_capital",    cap)
    redis_save("drift_positions",  pos)
    redis_save("drift_trades",     trd[-200:])
    redis_save("drift_milestones", ms)
    redis_save("drift_secured",    _profit_secured)

def _load_state():
    global _capital, _positions, _trades, _milestones_hit, _profit_secured
    cap = redis_load("drift_capital")
    pos = redis_load("drift_positions")
    trd = redis_load("drift_trades")
    ms  = redis_load("drift_milestones")
    sec = redis_load("drift_secured")
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

# ── PRICE FEEDS ───────────────────────────────────────────────────
_CG_ID_MAP = {
    # Large caps
    "SOL":    "solana",
    "BTC":    "bitcoin",
    "ETH":    "ethereum",
    "XRP":    "ripple",
    "DOGE":   "dogecoin",
    "AVAX":   "avalanche-2",
    "SUI":    "sui",
    "BNB":    "binancecoin",
    # Meme coins
    "BONK":   "bonk",
    "WIF":    "dogwifhat",
    "PEPE":   "pepe",
    "POPCAT": "popcat",
    "TRUMP":  "official-trump",
    "SHIB":   "shiba-inu",      # price feed only — not on Drift (ETH token)
    # Solana ecosystem
    "JTO":    "jito-governance",
    "JUP":    "jupiter-exchange-solana",
    "PYTH":   "pyth-network",
    "TIA":    "celestia",
    "RNDR":   "render-token",
    "WEN":    "wen-4",
    # DeFi / perp platforms
    "HYPE":   "hyperliquid",
    "ARB":    "arbitrum",
    "ONDO":   "ondo-finance",
    "SEI":    "sei-network",
}

def get_market_price(market):
    cg_id = _CG_ID_MAP.get(market.upper())
    if not cg_id:
        return None
    try:
        r = _session.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd"},
            timeout=8
        )
        if r.status_code == 200:
            return float(r.json()[cg_id]["usd"])
    except Exception:
        pass
    # Fallback: DexScreener for SOL
    if market.upper() == "SOL":
        try:
            r = _session.get(
                "https://api.dexscreener.com/latest/dex/pairs/solana/8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj",
                timeout=8
            )
            pairs = r.json().get("pairs", [])
            if pairs:
                return float(pairs[0].get("priceUsd", 0))
        except Exception:
            pass
    return None

def get_sol_price():
    return get_market_price("SOL")

# ── SIGNAL ENGINE ─────────────────────────────────────────────────
def _get_gmgn_signal(market):
    """Fetch smart money signal from GMGN for the underlying asset. Returns 'long', 'short', or None."""
    if not GMGN_API_KEY:
        return None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        }
        r = _session.get(
            "https://gmgn.ai/defi/quotation/v1/signals/sol",
            params={"type": "12", "limit": "20"},
            headers=headers,
            timeout=8
        )
        if r.status_code != 200:
            return None
        signals = r.json().get("data", {}).get("signals", [])
        # Check if smart money is loading up on the underlying asset symbol
        for s in signals:
            sym = (s.get("token_symbol") or "").upper()
            if sym == market.upper():
                action = (s.get("action") or "").lower()
                if action in ("buy", "accumulate"):
                    return "long"
                elif action in ("sell", "dump"):
                    return "short"
    except Exception:
        pass
    return None

def get_signal(market):
    """Returns 'long', 'short', or None. EMA crossover + optional GMGN smart money bias."""
    prices = list(_price_history.get(market, []))
    if len(prices) < 10:
        return None
    vals = [p for _, p in prices]

    ema8  = sum(vals[-8:]) / 8
    ema21 = sum(vals[-21:]) / 21 if len(vals) >= 21 else None
    if ema21 is None:
        return None

    trend = None
    if ema8 > ema21 * 1.002:
        trend = "long"
    elif ema8 < ema21 * 0.998:
        trend = "short"

    if trend is None:
        return None

    # GMGN override: if smart money is going opposite direction, suppress signal
    gmgn_bias = _get_gmgn_signal(market)
    if gmgn_bias and gmgn_bias != trend:
        log("info", f"{market} EMA={trend} but GMGN bias={gmgn_bias} — skipping")
        return None

    return trend

# ── LIVE EXECUTION STUBS ──────────────────────────────────────────
def _execute_drift_order(market, side, size_usd, leverage):
    try:
        from driftpy.drift_client import DriftClient
        from driftpy.types import OrderType, PositionDirection, OrderParams
        from solders.keypair import Keypair
        from solana.rpc.async_api import AsyncClient
        import asyncio, base58

        kp = Keypair.from_bytes(base58.b58decode(WALLET_PRIVATE_KEY))

        async def _place():
            conn = AsyncClient(SOL_RPC)
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
            idx = market_index_map.get(market.upper(), 0)
            await client.place_perp_order(OrderParams(
                order_type=OrderType.Market(),
                market_index=idx,
                direction=direction,
                base_asset_amount=int(size_usd * 1e6),
            ))
            await conn.close()

        asyncio.run(_place())
    except ImportError:
        log("err", "driftpy not installed — run: pip install driftpy")
    except Exception as e:
        log("err", f"Drift order failed: {e}")

def _execute_zeta_order(market, side, size_usd, leverage):
    try:
        price = get_market_price(market)
        if not price:
            return
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
    except Exception as e:
        log("err", f"Zeta order error: {e}")

# ── POSITION MANAGEMENT ───────────────────────────────────────────
def open_position(market, side, price, size_usd, leverage):
    global _capital
    if side == "long":
        tp = price * (1 + DRIFT_TP_PCT)
        sl = price * (1 - DRIFT_SL_PCT)
    else:
        tp = price * (1 - DRIFT_TP_PCT)
        sl = price * (1 + DRIFT_SL_PCT)

    pos = {
        "market": market, "side": side, "entry": price,
        "size": size_usd, "leverage": leverage,
        "peak_price": price, "tp": tp, "sl": sl,
        "opened_at": time.time(), "pnl": 0.0, "paper": DRIFT_PAPER_MODE,
    }

    if not DRIFT_PAPER_MODE:
        if DRIFT_EXCHANGE == "zeta":
            _execute_zeta_order(market, side, size_usd, leverage)
        else:
            _execute_drift_order(market, side, size_usd, leverage)

    with _state_lock:
        _positions[market] = pos
        _capital -= size_usd / leverage  # margin used

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

    pnl_usd = pos["size"] * pnl_pct * pos["leverage"]
    margin   = pos["size"] / pos["leverage"]

    # Dollar TP: compound 10% back, secure the rest
    is_dollar_tp = reason.startswith("TP$") and pnl_usd > 0
    compound_amt = round(pnl_usd * DRIFT_COMPOUND_PCT, 2) if is_dollar_tp else 0
    secured_amt  = round(pnl_usd - compound_amt, 2) if is_dollar_tp else 0

    global _profit_secured
    with _state_lock:
        if is_dollar_tp:
            _capital += margin + compound_amt   # return margin + 10% of profit
            _profit_secured += secured_amt
        else:
            _capital += margin + pnl_usd
        _daily_pnl += pnl_usd

    if is_dollar_tp:
        log("ok", f"SECURED ${secured_amt:.2f} | COMPOUNDED +${compound_amt:.2f} → capital=${_capital:.2f}", market)

    trade = {
        "market": market, "side": pos["side"],
        "entry": pos["entry"], "exit": exit_price,
        "pnl": pnl_usd, "pnl_pct": pnl_pct * 100 * pos["leverage"],
        "reason": reason,
        "secured": secured_amt, "compounded": compound_amt,
        "duration_s": time.time() - pos["opened_at"],
        "ts": time.strftime("%Y-%m-%d %H:%M"),
    }

    with _state_lock:
        _trades.insert(0, trade)
        if len(_trades) > 200:
            _trades.pop()

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
            f"Capital now: ${_capital:.2f}"
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

def monitor_positions():
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
        with _state_lock:
            if market not in _positions:
                continue
            _positions[market]["pnl"] = raw_pct * pos["size"] * pos["leverage"]

            # Update trailing peak
            if pos["side"] == "long" and price > _positions[market]["peak_price"]:
                _positions[market]["peak_price"] = price
                _positions[market]["sl"] = max(pos["sl"], price * (1 - DRIFT_TRAIL_PCT))
            elif pos["side"] == "short" and price < _positions[market]["peak_price"]:
                _positions[market]["peak_price"] = price
                _positions[market]["sl"] = min(pos["sl"], price * (1 + DRIFT_TRAIL_PCT))

            updated_pos = dict(_positions[market])

        # Dollar TP — close when profit hits threshold
        if DRIFT_TP_USD > 0 and updated_pos.get("pnl", 0) >= DRIFT_TP_USD:
            close_position(market, price, f"TP${DRIFT_TP_USD:.0f}")
        # Percentage TP hit
        elif (updated_pos["side"] == "long" and price >= updated_pos["tp"]) or \
             (updated_pos["side"] == "short" and price <= updated_pos["tp"]):
            close_position(market, price, "TP")
        # SL hit
        elif (updated_pos["side"] == "long" and price <= updated_pos["sl"]) or \
             (updated_pos["side"] == "short" and price >= updated_pos["sl"]):
            close_position(market, price, "SL")

def _check_milestones():
    with _state_lock:
        cap = _capital
    for m in MILESTONES:
        if cap >= m and m not in _milestones_hit:
            _milestones_hit.add(m)
            log("ok", f"MILESTONE ${m:,} REACHED! Capital: ${cap:.2f}")
            notify(f"*{DRIFT_BOT_NAME}*\nMILESTONE ${m:,} REACHED!\nCapital: ${cap:.2f}")

# ── TRADING LOOP ──────────────────────────────────────────────────
def run_trading_loop():
    markets = [m.strip().upper() for m in DRIFT_MARKETS.split(",")]
    log("ok", f"Trading loop started | markets={markets} | paper={DRIFT_PAPER_MODE}")

    while True:
        try:
            # Update price history for all markets
            for market in markets:
                price = get_market_price(market)
                if price:
                    if market not in _price_history:
                        _price_history[market] = deque(maxlen=30)
                    _price_history[market].append((time.time(), price))

            # Monitor existing positions for exits
            monitor_positions()

            # Daily PnL reset
            today = time.strftime("%Y-%m-%d")
            global _day_start, _daily_pnl
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
                    if already_open:
                        continue

                    signal = get_signal(market)
                    if not signal:
                        continue

                    price = get_market_price(market)
                    if not price:
                        continue

                    size_usd = cap * DRIFT_TRADE_PCT * DRIFT_LEVERAGE
                    margin   = cap * DRIFT_TRADE_PCT
                    if margin < 1.0:
                        log("warn", f"Insufficient capital for trade: margin=${margin:.2f}")
                        continue

                    # Cache signal for display
                    _signals_cache[market] = {
                        "signal": signal,
                        "ts": time.strftime("%H:%M:%S"),
                        "price": price,
                    }

                    open_position(market, signal, price, size_usd, DRIFT_LEVERAGE)
                    break  # one new position per cycle

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

    manual_html = ""
    for mk in markets_list:
        manual_html += (
            f'<div class="mkt-row">'
            f'<span class="mkt-lbl">{mk}</span>'
            f'<button class="btn btn-long" onclick="manualTrade(\'{mk}\',\'long\')">▲ LONG</button>'
            f'<button class="btn btn-short" onclick="manualTrade(\'{mk}\',\'short\')">▼ SHORT</button>'
            f'<button class="btn btn-x" onclick="closePos(\'{mk}\')">✕</button>'
            f'</div>'
        )

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
body{{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}}
.bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;opacity:.25;pointer-events:none;z-index:0}}
.wrap{{position:relative;z-index:1}}

/* NAV */
nav{{display:flex;border-bottom:2px solid var(--cyan);overflow-x:auto;scrollbar-width:none}}
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
.hero{{padding:18px 16px 14px;background:linear-gradient(180deg,#020d1c,var(--bg));
  border-bottom:3px solid var(--cyan);text-align:center}}
.hero-title{{font-family:'Bebas Neue',sans-serif;font-size:1.6rem;color:var(--cyan);
  letter-spacing:.1em;text-shadow:0 0 20px #00e5ff55}}
.hero-cap{{font-family:'Bebas Neue',sans-serif;font-size:3.6rem;color:var(--cyan);
  text-shadow:0 0 30px #00e5ff66,3px 3px 0 #ff3355;line-height:1.1;
  animation:glow 3s ease-in-out infinite}}
.hero-row{{display:flex;justify-content:center;gap:20px;margin-top:6px;font-size:.68rem;color:#666}}
.hero-row span{{color:#aaa;font-family:'JetBrains Mono',monospace}}

/* STATS */
.stats{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:2px;background:var(--cyan);
  border:2px solid var(--cyan);margin-bottom:2px}}
.stat{{background:var(--card);padding:12px 10px;text-align:center}}
.stat .lbl{{font-size:.54rem;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.08em}}
.stat .val{{font-family:'Bebas Neue',sans-serif;font-size:1.6rem;line-height:1.1;margin-top:2px}}

/* SECTION */
.sec{{background:var(--card);margin-bottom:2px}}
.sec-hdr{{display:flex;align-items:center;justify-content:space-between;
  padding:10px 16px;border-bottom:1px solid var(--border)}}
.sec-hdr h2{{font-size:.6rem;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.1em}}
.sec-hdr a{{font-size:.6rem;color:var(--cyan);text-decoration:none;font-weight:700}}
.sec-hdr span{{font-size:.6rem;color:var(--cyan);font-weight:700}}

/* POSITION CARDS — floating animated */
.pos-card{{
  margin:10px 12px;
  padding:14px;
  background:linear-gradient(135deg,#0d1825,#0a1520);
  border:1px solid var(--border);
  border-radius:2px;
  animation:slideUp .4s ease forwards, float 4s ease-in-out infinite;
  animation-delay:calc(var(--i,0) * .1s), calc(var(--i,0) * .5s);
  opacity:0;
  position:relative;
  overflow:hidden;
}}
.pos-card::before{{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,var(--cyan),transparent);
  animation:shimmer 2s ease-in-out infinite;
}}
.pos-card.profit{{border-color:#00ff8840;box-shadow:0 0 20px #00ff8810}}
.pos-card.loss{{border-color:#ff335540;box-shadow:0 0 20px #ff335510}}
.pos-top{{display:flex;align-items:center;gap:8px;margin-bottom:8px}}
.pos-sym{{font-family:'Bebas Neue',sans-serif;font-size:1.3rem;color:var(--cyan)}}
.pos-side{{font-size:.65rem;font-weight:900;padding:2px 7px;border:1px solid;letter-spacing:.06em}}
.pos-pnl{{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:700;margin-left:auto;
  transition:color .4s ease}}
.pos-meta{{display:grid;grid-template-columns:1fr 1fr;gap:3px 12px;
  font-size:.6rem;color:#555;font-family:'JetBrains Mono',monospace;margin-bottom:10px}}
.pos-meta b{{color:#888;font-weight:400}}
canvas.mini{{width:100%!important;display:block;margin-bottom:8px;border-radius:2px}}
.close-btn{{width:100%;padding:8px;background:transparent;border:1px solid var(--red);
  color:var(--red);font-size:.62rem;font-weight:900;letter-spacing:.08em;cursor:pointer;
  text-transform:uppercase;transition:all .2s;border-radius:1px}}
.close-btn:hover{{background:var(--red);color:#fff;box-shadow:0 0 12px #ff335560}}
.no-pos{{text-align:center;padding:28px 16px;color:#333;font-size:.75rem}}
.no-pos-icon{{font-size:1.5rem;margin-bottom:6px;opacity:.3}}

/* SPACER */
.spacer{{height:10px;background:var(--bg)}}

/* TRADES */
.trade-row{{display:grid;grid-template-columns:1.8fr 1fr 1fr 1.4fr 1fr;gap:4px;
  padding:9px 16px;border-bottom:1px solid #ffffff08;font-size:.65rem;align-items:center}}
.trade-row.hdr{{font-size:.54rem;color:#444;font-weight:700;text-transform:uppercase;
  letter-spacing:.06em;padding:7px 16px;background:#060d18;border-bottom:1px solid var(--border)}}
.badge{{display:inline-block;padding:2px 5px;font-size:.52rem;font-weight:900;border:1px solid;letter-spacing:.04em}}
.badge.win{{color:var(--green);border-color:var(--green);background:#00ff8810}}
.badge.loss{{color:var(--red);border-color:var(--red);background:#ff335510}}
.badge.sl{{color:#ff6600;border-color:#ff6600;background:#ff660010}}

/* MANUAL */
.mkt-row{{display:flex;align-items:center;gap:5px;padding:6px 12px;border-bottom:1px solid #ffffff08}}
.mkt-lbl{{font-family:'Bebas Neue',sans-serif;font-size:.95rem;color:var(--cyan);width:58px;flex-shrink:0}}
.btn{{padding:5px 11px;font-size:.58rem;font-weight:900;letter-spacing:.06em;border:none;cursor:pointer;text-transform:uppercase;transition:all .15s;flex-shrink:0}}
.btn-long{{background:var(--green);color:#000}}.btn-long:hover{{box-shadow:0 0 8px var(--green)}}
.btn-short{{background:var(--red);color:#fff}}.btn-short:hover{{box-shadow:0 0 8px var(--red)}}
.btn-x{{background:#1a2535;color:#666;border:1px solid #333;padding:5px 9px}}
.btn-x:hover{{color:#fff;border-color:#666}}

/* LIVE FEED */
.feed-item{{padding:8px 16px;border-bottom:1px solid #ffffff06;font-size:.62rem;
  font-family:'JetBrains Mono',monospace;display:flex;gap:8px;align-items:flex-start}}
.feed-time{{color:#333;font-size:.56rem;flex-shrink:0;margin-top:1px}}
.feed-text{{color:#888;line-height:1.5}}
.feed-text.open{{color:var(--cyan)}}.feed-text.win{{color:var(--green)}}.feed-text.loss{{color:var(--red)}}

/* PROGRESS */
.prog-wrap{{padding:12px 16px;background:var(--card)}}
.prog-lbl{{font-size:.58rem;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:.1em;
  margin-bottom:6px;display:flex;justify-content:space-between}}
.prog-track{{background:#ffffff08;height:4px;overflow:hidden}}
.prog-fill{{background:linear-gradient(90deg,var(--cyan),var(--green),#ffee00);height:4px;
  transition:width .6s ease;box-shadow:0 0 8px var(--cyan)}}
.milestones{{display:flex;flex-wrap:wrap;gap:3px;margin-top:8px}}
.ms{{font-size:.54rem;padding:2px 6px;font-weight:700;border:1px solid #ffffff10;color:#444;background:#ffffff04}}
.ms.hit{{color:var(--cyan);border-color:var(--cyan);background:#00e5ff10;box-shadow:0 0 5px #00e5ff30}}

footer{{padding:12px 16px;text-align:center;font-size:.58rem;color:#333;border-top:1px solid var(--border)}}
footer a{{color:var(--cyan);text-decoration:none}}

/* ANIMATIONS */
@keyframes float{{
  0%,100%{{transform:translateY(0)}}
  50%{{transform:translateY(-5px)}}
}}
@keyframes slideUp{{
  from{{opacity:0;transform:translateY(20px)}}
  to{{opacity:1;transform:translateY(0)}}
}}
@keyframes glow{{
  0%,100%{{text-shadow:0 0 30px #00e5ff66,3px 3px 0 #ff3355}}
  50%{{text-shadow:0 0 50px #00e5ffaa,3px 3px 0 #ff3355,0 0 80px #00e5ff33}}
}}
@keyframes shimmer{{
  0%{{transform:translateX(-100%)}}
  100%{{transform:translateX(100%)}}
}}
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
      <span id="hero-pnl">+$0.00 PnL</span>
      <span id="hero-trades">0 trades</span>
      <span id="hero-secured">$0 secured</span>
    </div>
  </div>

  <div class="stats">
    <div class="stat">
      <div class="lbl">Win Rate</div>
      <div class="val" id="stat-wr" style="color:var(--green)">0%</div>
    </div>
    <div class="stat">
      <div class="lbl">Open</div>
      <div class="val cyan" id="stat-open">0/{DRIFT_MAX_OPEN}</div>
    </div>
    <div class="stat">
      <div class="lbl">Daily PnL</div>
      <div class="val" id="stat-dpnl" style="color:var(--green)">$0</div>
    </div>
  </div>

  <!-- LIVE POSITIONS WIDGET -->
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

  <div class="spacer"></div>

  <!-- RECENT TRADES -->
  <div class="sec">
    <div class="sec-hdr"><h2>RECENT TRADES</h2><a href="/trades">ALL TRADES →</a></div>
    <div class="trade-row hdr"><span>MARKET</span><span>SIDE</span><span>PNL</span><span>RESULT</span><span>TIME</span></div>
    <div id="trades-wrap"><div class="no-pos">No trades yet</div></div>
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
    <div style="padding:4px 0">{manual_html}</div>
  </div>

  <footer>{DRIFT_BOT_NAME} · {exch} · <a href="/monitor">Monitor</a> · <a href="/trades">Trades</a> · <a href="/status/api" target="_blank">API ↗</a></footer>
</div>

<script>
const charts = {{}}, pnlH = {{}};
const MS = {json.dumps(MILESTONES)};
const START_CAP = {STARTING_CAPITAL};
const GOAL = {PROFIT_GOAL};
const MAX_OPEN = {DRIFT_MAX_OPEN};

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
    const wrap = document.getElementById('trades-wrap');
    if (!trades.length) {{ wrap.innerHTML = '<div class="no-pos">No trades yet</div>'; return; }}
    wrap.innerHTML = trades.slice(0,8).map(t => {{
      const sc = t.side==='long'?'#00ff88':'#ff3355';
      const pc = t.pnl>=0?'#00ff88':'#ff3355';
      const b = t.reason==='SL'?'sl':t.pnl>=0?'win':'loss';
      return `<div class="trade-row">
        <span style="color:var(--cyan);font-weight:900">${{t.market}}</span>
        <span style="color:${{sc}}">${{t.side.toUpperCase()}}</span>
        <span style="color:${{pc}};font-family:monospace">${{t.pnl>=0?'+':''}}$${{t.pnl.toFixed(2)}}</span>
        <span><span class="badge ${{b}}">${{t.reason}}</span></span>
        <span style="color:#444">${{t.ts.slice(-5)}}</span>
      </div>`;
    }}).join('');
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
      const cls = txt.includes('OPEN')?'open':txt.includes('+$')&&txt.includes('CLOSE')?'win':txt.includes('CLOSE')?'loss':'';
      const clean = txt.replace(/\*/g,'').replace(/\\n/g,' · ');
      return `<div class="feed-item"><span class="feed-time">${{n.time}}</span><span class="feed-text ${{cls}}">${{clean}}</span></div>`;
    }}).join('');
  }} catch(e) {{}}
}}

function updateHero(d) {{
  document.getElementById('hero-cap').textContent = '$' + d.capital.toFixed(2);
  const pnl = d.total_pnl||0;
  const pnlEl = document.getElementById('hero-pnl');
  pnlEl.textContent = (pnl>=0?'+':'')+'$'+pnl.toFixed(2)+' PnL';
  pnlEl.style.color = pnl>=0?'#00ff88':'#ff3355';
  document.getElementById('hero-trades').textContent = d.total_trades+' trades';
  document.getElementById('hero-secured').textContent = '$'+(d.profit_secured||0).toFixed(0)+' secured';
  const wr = d.win_rate||0;
  const wrEl = document.getElementById('stat-wr');
  wrEl.textContent = wr+'%'; wrEl.style.color = wr>=50?'#00ff88':'#ff3355';
  const keys = Object.keys(d.positions||{{}});
  document.getElementById('stat-open').textContent = keys.length+'/'+MAX_OPEN;
  const dp = d.daily_pnl||0;
  const dpEl = document.getElementById('stat-dpnl');
  dpEl.textContent = (dp>=0?'+':'')+'$'+dp.toFixed(2);
  dpEl.style.color = dp>=0?'#00ff88':'#ff3355';
}}

function updatePositions(positions) {{
  const wrap = document.getElementById('pos-wrap');
  const noPos = document.getElementById('no-pos');
  const keys = Object.keys(positions);
  document.getElementById('pos-count').textContent = keys.length;
  noPos.style.display = keys.length?'none':'block';

  keys.forEach((market, i) => {{
    const pos = positions[market];
    const pnl = pos.pnl||0;
    if (!pnlH[market]) pnlH[market] = [];
    pnlH[market].push(pnl);
    if (pnlH[market].length > 80) pnlH[market].shift();

    let card = document.getElementById('pc-'+market);
    if (!card) {{
      card = document.createElement('div');
      card.id = 'pc-'+market;
      card.className = 'pos-card';
      card.style.setProperty('--i', i);
      card.innerHTML = buildCard(market, pos);
      wrap.appendChild(card);
      setTimeout(() => initChart(market), 80);
    }} else {{
      const pnlEl = document.getElementById('pp-'+market);
      const c = pnl>=0?'#00ff88':'#ff3355';
      const margin = (pos.size||0)/(pos.leverage||1);
      const pct = margin>0?(pnl/margin*100).toFixed(1):'0.0';
      if (pnlEl) {{ pnlEl.textContent=(pnl>=0?'+':'')+'$'+pnl.toFixed(2)+' ('+(pnl>=0?'+':'')+pct+'%)'; pnlEl.style.color=c; }}
      card.className = 'pos-card '+(pnl>=0?'profit':'loss');
      updateChart(market, pnl);
    }}
  }});

  document.querySelectorAll('.pos-card').forEach(el => {{
    const m = el.id.replace('pc-','');
    if (!positions[m]) {{ if(charts[m]){{charts[m].destroy();delete charts[m];}} delete pnlH[m]; el.remove(); }}
  }});
}}

function buildCard(market, pos) {{
  const side = pos.side||'long';
  const sc = side==='long'?'#00ff88':'#ff3355';
  const pnl = pos.pnl||0; const pc = pnl>=0?'#00ff88':'#ff3355';
  const margin = (pos.size||0)/(pos.leverage||1);
  const pct = margin>0?(pnl/margin*100).toFixed(1):'0.0';
  const cur = pos.current_price||pos.entry||0;
  return `<div class="pos-top">
    <span class="pos-sym">${{market}}-PERP</span>
    <span class="pos-side" style="color:${{sc}};border-color:${{sc}}">${{side.toUpperCase()}}</span>
    <span class="pos-pnl" id="pp-${{market}}" style="color:${{pc}}">${{pnl>=0?'+':''}}$${{pnl.toFixed(2)}} (${{pnl>=0?'+':''}}${{pct}}%)</span>
  </div>
  <div class="pos-meta">
    <span><b>Entry</b> $${{(pos.entry||0).toFixed(4)}}</span>
    <span><b>Now</b> $${{cur.toFixed(4)}}</span>
    <span><b>TP</b> <span style="color:#00ff88">$${{(pos.tp||0).toFixed(4)}}</span></span>
    <span><b>SL</b> <span style="color:#ff3355">$${{(pos.sl||0).toFixed(4)}}</span></span>
  </div>
  <canvas id="chart-${{market}}" class="mini" height="70"></canvas>
  <button class="close-btn" onclick="closePos('${{market}}')">✕ CLOSE ${{market}}</button>`;
}}

function initChart(market) {{
  const el = document.getElementById('chart-'+market);
  if (!el||charts[market]) return;
  charts[market] = new Chart(el.getContext('2d'), {{
    type:'line',
    data:{{ labels:[], datasets:[{{
      data:[], borderColor:'#00e5ff',
      backgroundColor: createGradient(el.getContext('2d'), '#00e5ff'),
      borderWidth:2, pointRadius:0, tension:0.4, fill:true
    }}]}},
    options:{{
      responsive:true, animation:{{duration:400}},
      plugins:{{ legend:{{display:false}}, tooltip:{{ callbacks:{{ label: c=>(c.parsed.y>=0?'+':'')+'$'+c.parsed.y.toFixed(2) }} }} }},
      scales:{{
        x:{{display:false}},
        y:{{ grid:{{color:'#ffffff08'}}, ticks:{{color:'#444',font:{{size:9}},callback:v=>'$'+v.toFixed(1)}} }}
      }}
    }}
  }});
  updateChart(market, pnlH[market]?.slice(-1)[0]||0);
}}

function createGradient(ctx, color) {{
  const g = ctx.createLinearGradient(0,0,0,80);
  g.addColorStop(0, color.replace(')',',0.25)').replace('rgb','rgba'));
  g.addColorStop(1, color.replace(')',',0.0)').replace('rgb','rgba'));
  return g;
}}

function updateChart(market, latestPnl) {{
  const c = charts[market]; if(!c) return;
  const hist = pnlH[market]||[];
  const color = latestPnl>=0?'#00ff88':'#ff3355';
  const ctx = c.canvas.getContext('2d');
  const g = ctx.createLinearGradient(0,0,0,70);
  g.addColorStop(0, latestPnl>=0?'rgba(0,255,136,0.2)':'rgba(255,51,85,0.2)');
  g.addColorStop(1, 'rgba(0,0,0,0)');
  c.data.labels = hist.map((_,i)=>i);
  c.data.datasets[0].data = hist;
  c.data.datasets[0].borderColor = color;
  c.data.datasets[0].backgroundColor = g;
  c.update('none');
}}

function updateProgress(cap) {{
  const next = MS.find(m=>m>cap)||GOAL;
  const pct = Math.max(0,Math.min(100,Math.round((cap-START_CAP)/Math.max(next-START_CAP,1)*100)));
  document.getElementById('prog-fill').style.width = pct+'%';
  document.getElementById('prog-pct').textContent = pct+'%';
  MS.forEach(m => {{
    const el = document.getElementById('ms-'+m);
    if(el) el.className = cap>=m?'ms hit':'ms';
  }});
}}

function manualTrade(market, side) {{
  if (!confirm('Open '+side.toUpperCase()+' '+market+'?')) return;
  fetch('/api/manual-'+side+'/'+market,{{method:'POST'}})
    .then(r=>r.json()).then(d=>alert(d.msg||d.error)).catch(e=>alert(e));
}}
function closePos(market) {{
  if (!confirm('Close '+market+'?')) return;
  fetch('/api/close/'+market,{{method:'POST'}})
    .then(r=>r.json()).then(d=>alert(d.msg||d.error)).catch(e=>alert(e));
}}

poll(); pollTrades(); pollFeed();
setInterval(poll, 3000);
setInterval(pollTrades, 5000);
setInterval(pollFeed, 4000);
</script>
</body></html>"""



    wins   = [t for t in _trades if t["pnl"] >= 0]
    total  = len(_trades)
    wr     = round(len(wins) / max(total, 1) * 100, 1)
    total_pnl = sum(t["pnl"] for t in _trades)
    mode   = "PAPER" if DRIFT_PAPER_MODE else "LIVE"
    exch   = DRIFT_EXCHANGE.upper()
    next_m = next((m for m in MILESTONES if m > cap), None)
    progress_pct = min(round((cap - STARTING_CAPITAL) / max((next_m or PROFIT_GOAL) - STARTING_CAPITAL, 1) * 100, 1), 100) if next_m else 100
    mode_color  = "#ffee00" if DRIFT_PAPER_MODE else "#39ff14"
    pnl_color   = "#00ff88" if total_pnl >= 0 else "#ff3355"
    sign        = "+" if total_pnl >= 0 else ""
    wr_color    = "#00ff88" if wr >= 50 else "#ff3355"
    dpnl_color  = "#00ff88" if _daily_pnl >= 0 else "#ff3355"
    open_cls    = "green" if pos_list else "yellow"
    open_status = "active" if pos_list else "scanning"
    next_m_str  = f"${next_m:,}" if next_m else "DONE"
    ms_html     = "".join(
        '<span class="ms{}">${:,}</span>'.format(" hit" if cap >= m else "", m)
        for m in MILESTONES
    )
    open_tbl    = (
        '<div class="tbl-wrap"><table>'
        '<thead><tr><th>Market</th><th>Side</th><th>Entry</th><th>Current</th>'
        '<th>PnL%</th><th>TP</th><th>SL</th><th>Held</th></tr></thead>'
        '<tbody>' + (pos_rows if pos_list else "") + '</tbody>'
        '</table></div>'
        if pos_list else
        '<div class="empty">No open positions — scanning for signals...</div>'
    )
    sig_fallback   = '<tr><td colspan="4" class="empty">Warming up price history...</td></tr>'
    trade_fallback = '<tr><td colspan="7" class="empty">No trades yet</td></tr>'
    today_str      = time.strftime("%Y-%m-%d")
    manual_btns_html = "".join(
        '<button class="btn btn-long" onclick="manualTrade(\'{mk}\',\'long\')">LONG {mk}</button>'
        '<button class="btn btn-short" onclick="manualTrade(\'{mk}\',\'short\')">SHORT {mk}</button>'
        '<button class="btn btn-close" onclick="closePos(\'{mk}\')">CLOSE {mk}</button>'.format(mk=m.strip().upper())
        for m in DRIFT_MARKETS.split(",")
    )

    # Build positions table rows
    pos_rows = ""
    for p in pos_list:
        cur_price = get_market_price(p["market"])
        if cur_price:
            if p["side"] == "long":
                live_pct = (cur_price - p["entry"]) / p["entry"] * 100 * p["leverage"]
            else:
                live_pct = (p["entry"] - cur_price) / p["entry"] * 100 * p["leverage"]
            live_pnl = p["size"] * (live_pct / 100)
        else:
            cur_price = p["entry"]
            live_pct  = 0.0
            live_pnl  = p.get("pnl", 0.0)

        side_color = "#00ff88" if p["side"] == "long" else "#ff3355"
        pct_color  = "#00ff88" if live_pct >= 0 else "#ff3355"
        elapsed    = round((time.time() - p["opened_at"]) / 60, 1)
        pos_rows += (
            f'<tr>'
            f'<td class="sym">{p["market"]}-PERP</td>'
            f'<td style="color:{side_color};font-weight:900">{p["side"].upper()}</td>'
            f'<td class="mono">${p["entry"]:.4f}</td>'
            f'<td class="mono">${cur_price:.4f}</td>'
            f'<td class="mono" style="color:{pct_color}">{live_pct:+.2f}%</td>'
            f'<td class="mono">${p["tp"]:.4f}</td>'
            f'<td class="mono">${p["sl"]:.4f}</td>'
            f'<td class="muted">{elapsed}m</td>'
            f'</tr>'
        )

    # Build signals rows
    sig_rows = ""
    for market in [m.strip().upper() for m in DRIFT_MARKETS.split(",")]:
        sig_info = _signals_cache.get(market, {})
        sig      = sig_info.get("signal", "—")
        sig_ts   = sig_info.get("ts", "—")
        hist     = list(_price_history.get(market, []))
        if len(hist) >= 21:
            vals = [p for _, p in hist]
            ema8  = sum(vals[-8:]) / 8
            ema21 = sum(vals[-21:]) / 21
            strength = round(abs(ema8 - ema21) / ema21 * 100, 3) if ema21 else 0
        else:
            strength = 0
        sig_color = "#00ff88" if sig == "long" else ("#ff3355" if sig == "short" else "#888")
        sig_rows += (
            f'<tr>'
            f'<td class="sym">{market}</td>'
            f'<td style="color:{sig_color};font-weight:900">{sig.upper() if sig != "—" else sig}</td>'
            f'<td class="mono">{strength:.3f}%</td>'
            f'<td class="muted">{sig_ts}</td>'
            f'</tr>'
        )

    # Build recent trades rows
    trade_rows = ""
    for t in trades:
        t_color    = "#00ff88" if t["pnl"] >= 0 else "#ff3355"
        t_icon     = "▲" if t["pnl"] >= 0 else "▼"
        t_sign     = "+" if t["pnl"] >= 0 else ""
        t_side_clr = "#00ff88" if t["side"] == "long" else "#ff3355"
        t_badge    = "win" if t["pnl"] >= 0 else "loss"
        trade_rows += (
            f'<tr>'
            f'<td class="sym">{t["market"]}</td>'
            f'<td style="color:{t_side_clr};font-weight:900">{t["side"].upper()}</td>'
            f'<td class="mono">${t["entry"]:.4f}</td>'
            f'<td class="mono">${t["exit"]:.4f}</td>'
            f'<td class="mono" style="color:{t_color}">{t_icon} {t_sign}${t["pnl"]:.2f}</td>'
            f'<td><span class="badge {t_badge}">{t["reason"]}</span></td>'
            f'<td class="muted">{t["ts"]}</td>'
            f'</tr>'
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<meta http-equiv="refresh" content="30">
<title>{DRIFT_BOT_NAME}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;700;900&family=JetBrains+Mono:wght@600&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{
    --cyan:#00e5ff;--green:#00ff88;--red:#ff3355;--yellow:#ffee00;
    --bg:#050a14;--card:#0a1220;--border:#ffffff15;
  }}
  body{{background:var(--bg);color:#fff;font-family:'Inter',sans-serif;
    max-width:430px;margin:0 auto;min-height:100vh;overflow-x:hidden}}
  .bg-art{{position:fixed;top:0;left:0;width:100%;height:100%;object-fit:cover;
    object-position:center;opacity:.30;pointer-events:none;z-index:0}}
  .wrap{{position:relative;z-index:1}}
  nav{{display:flex;gap:0;border-bottom:2px solid var(--cyan);overflow-x:auto;scrollbar-width:none}}
  nav::-webkit-scrollbar{{display:none}}
  nav a{{color:#fff;text-decoration:none;font-size:.72rem;font-weight:700;
    padding:10px 14px;white-space:nowrap;letter-spacing:.06em;text-transform:uppercase;
    border-right:1px solid var(--border);transition:all .15s}}
  nav a:hover{{background:var(--cyan);color:#000}}
  .mode-strip{{background:var(--card);border-bottom:2px solid var(--border);
    padding:6px 16px;display:flex;align-items:center;justify-content:space-between}}
  .mode-strip .left{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
  .mode-pill{{font-size:.68rem;font-weight:900;padding:3px 12px;letter-spacing:.08em;
    background:{mode_color};color:#000}}
  .hero{{padding:20px 16px 16px;background:linear-gradient(180deg,#020d1c,var(--bg));
    border-bottom:3px solid var(--cyan);text-align:center}}
  .hero-lbl{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;
    letter-spacing:.15em;margin-bottom:4px}}
  .hero-title{{font-family:'Bebas Neue',sans-serif;font-size:2rem;color:var(--cyan);
    letter-spacing:.08em;text-shadow:0 0 20px #00e5ff66;margin-bottom:4px}}
  .hero-cap{{font-family:'Bebas Neue',sans-serif;font-size:4rem;letter-spacing:.02em;
    color:var(--cyan);text-shadow:0 0 30px #00e5ff66,3px 3px 0 #ff3355;line-height:1}}
  .hero-pnl{{font-family:'JetBrains Mono',monospace;font-size:1rem;font-weight:600;
    margin-top:8px;color:{pnl_color}}}
  .hero-sub{{font-size:.7rem;color:#888;margin-top:6px}}
  .grid{{display:grid;grid-template-columns:1fr 1fr;gap:2px;background:var(--cyan);border:2px solid var(--cyan)}}
  .stat{{background:var(--card);padding:14px 16px}}
  .stat .lbl{{font-size:.58rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
  .stat .val{{font-family:'Bebas Neue',sans-serif;font-size:2rem;margin-top:2px;line-height:1}}
  .stat .sub{{font-size:.62rem;color:#888;margin-top:3px}}
  .cyan{{color:var(--cyan)}} .green{{color:var(--green)}} .red{{color:var(--red)}}
  .yellow{{color:var(--yellow)}} .muted{{color:#888}}
  .prog-wrap{{padding:14px 16px;background:var(--card);border-bottom:2px solid var(--border)}}
  .prog-lbl{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;
    letter-spacing:.1em;margin-bottom:8px;display:flex;justify-content:space-between}}
  .prog-track{{background:#ffffff10;height:6px;overflow:hidden}}
  .prog-fill{{background:linear-gradient(90deg,var(--cyan),var(--green),#ffee00);
    height:6px;width:{progress_pct}%;box-shadow:0 0 10px var(--cyan)}}
  .milestones{{display:flex;flex-wrap:wrap;gap:4px;margin-top:10px}}
  .ms{{font-size:.58rem;padding:3px 8px;font-weight:700;letter-spacing:.04em;
    border:1px solid #ffffff15;color:#666;background:#ffffff05}}
  .ms.hit{{color:var(--cyan);border-color:var(--cyan);background:#00e5ff15;box-shadow:0 0 6px #00e5ff40}}
  .section{{background:var(--card);border-bottom:2px solid var(--border)}}
  .section-hdr{{display:flex;align-items:center;justify-content:space-between;
    padding:12px 16px;border-bottom:1px solid var(--border)}}
  .section-hdr h2{{font-size:.62rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.1em}}
  .section-hdr a{{font-size:.62rem;color:var(--cyan);text-decoration:none;font-weight:700;letter-spacing:.06em}}
  .tbl-wrap{{overflow-x:auto}}
  table{{width:100%;border-collapse:collapse;font-size:.72rem}}
  th{{padding:8px 10px;font-size:.56rem;font-weight:700;color:#888;text-transform:uppercase;
    letter-spacing:.08em;text-align:left;border-bottom:1px solid var(--border);
    white-space:nowrap;background:#080f1a}}
  td{{padding:9px 10px;border-bottom:1px solid #ffffff05;vertical-align:middle}}
  .sym{{font-weight:900;font-size:.8rem}}
  .mono{{font-family:'JetBrains Mono',monospace;font-size:.68rem}}
  .badge{{display:inline-block;padding:2px 6px;font-size:.56rem;font-weight:900;letter-spacing:.06em;border:1px solid}}
  .badge.win{{color:var(--green);border-color:var(--green);background:#00ff8815}}
  .badge.loss{{color:var(--red);border-color:var(--red);background:#ff335515}}
  .manual-btns{{display:flex;gap:6px;padding:10px 16px;background:var(--card);
    border-bottom:1px solid var(--border);flex-wrap:wrap}}
  .btn{{padding:7px 13px;font-size:.65rem;font-weight:900;letter-spacing:.06em;border:none;
    cursor:pointer;text-transform:uppercase;transition:all .15s}}
  .btn-long{{background:var(--green);color:#000}}
  .btn-long:hover{{box-shadow:0 0 12px var(--green)}}
  .btn-short{{background:var(--red);color:#fff}}
  .btn-short:hover{{box-shadow:0 0 12px var(--red)}}
  .btn-close{{background:#333;color:#fff;border:1px solid #666}}
  .btn-close:hover{{background:#555}}
  .empty{{text-align:center;padding:24px;color:#444;font-size:.75rem}}
  footer{{padding:14px 16px;text-align:center;font-size:.6rem;color:#444;border-top:1px solid var(--border)}}
  footer a{{color:var(--cyan);text-decoration:none}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
  .dot{{display:inline-block;width:6px;height:6px;border-radius:50%;
    background:{mode_color};box-shadow:0 0 6px {mode_color};animation:pulse 2s infinite;margin-right:6px}}
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
    <a href="https://solscan.io" target="_blank">SCAN</a>
  </nav>
  <div class="mode-strip">
    <span class="left"><span class="dot"></span>{exch} · Goal ${PROFIT_GOAL:,.0f}</span>
    <span class="mode-pill">{mode}</span>
  </div>
  <div class="hero">
    <div class="hero-title">DRIFT SNIPER</div>
    <div class="hero-lbl">Current Capital</div>
    <div class="hero-cap">${cap:.2f}</div>
    <div class="hero-pnl">{sign}${total_pnl:.2f} total PnL</div>
    <div class="hero-sub">Started ${STARTING_CAPITAL:.2f} &nbsp;·&nbsp; {total} trades closed</div>
  </div>
  <div class="grid">
    <div class="stat">
      <div class="lbl">Win Rate</div>
      <div class="val" style="color:{wr_color}">{wr}%</div>
      <div class="sub">{len(wins)}W / {total-len(wins)}L</div>
    </div>
    <div class="stat">
      <div class="lbl">Open Positions</div>
      <div class="val {open_cls}">{len(pos_list)}<span style="font-size:1.1rem;color:#888">/{DRIFT_MAX_OPEN}</span></div>
      <div class="sub">{open_status}</div>
    </div>
    <div class="stat">
      <div class="lbl">Leverage</div>
      <div class="val cyan">{DRIFT_LEVERAGE}x</div>
      <div class="sub">{DRIFT_TRADE_PCT*100:.0f}% per trade</div>
    </div>
    <div class="stat">
      <div class="lbl">Daily PnL</div>
      <div class="val" style="color:{dpnl_color}">${_daily_pnl:+.2f}</div>
      <div class="sub">{today_str}</div>
    </div>
    <div class="stat">
      <div class="lbl">TP / SL</div>
      <div class="val green">{DRIFT_TP_PCT*100:.0f}%</div>
      <div class="sub">SL {DRIFT_SL_PCT*100:.0f}% · Trail {DRIFT_TRAIL_PCT*100:.0f}%</div>
    </div>
    <div class="stat">
      <div class="lbl">Next Target</div>
      <div class="val cyan">${next_m:,}</div>
      <div class="sub">{progress_pct:.0f}% there</div>
    </div>
  </div>
  <div class="prog-wrap">
    <div class="prog-lbl"><span>GOAL PROGRESS</span><span style="color:var(--cyan)">{progress_pct:.0f}%</span></div>
    <div class="prog-track"><div class="prog-fill"></div></div>
    <div class="milestones">
      {ms_html}
    </div>
  </div>
  <div class="section">
    <div class="section-hdr"><h2>OPEN POSITIONS ({len(pos_list)})</h2></div>
    {open_tbl}
  </div>
  <div class="section">
    <div class="section-hdr"><h2>SIGNAL STATUS</h2></div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Market</th><th>Signal</th><th>Strength</th><th>Updated</th></tr></thead>
      <tbody>{sig_rows if sig_rows else sig_fallback}</tbody>
    </table></div>
  </div>
  <div class="section">
    <div class="section-hdr"><h2>MANUAL CONTROLS</h2></div>
    <div class="manual-btns">
      {manual_btns_html}
    </div>
  </div>
  <div class="section">
    <div class="section-hdr"><h2>RECENT TRADES</h2><a href="/trades">ALL →</a></div>
    <div class="tbl-wrap"><table>
      <thead><tr><th>Market</th><th>Side</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Reason</th><th>Time</th></tr></thead>
      <tbody>{trade_rows if trade_rows else trade_fallback}</tbody>
    </table></div>
  </div>
  <footer>{DRIFT_BOT_NAME} &nbsp;·&nbsp; {exch} &nbsp;·&nbsp;
    <a href="/status/api">API →</a>
  </footer>
</div>
<script>
function manualTrade(market, side) {{
  if (!confirm('Open ' + side.toUpperCase() + ' ' + market + '?')) return;
  fetch('/api/manual-' + side + '/' + market, {{method:'POST'}})
    .then(r => r.json()).then(d => {{ alert(d.msg || d.error); location.reload(); }})
    .catch(e => alert('Error: ' + e));
}}
function closePos(market) {{
  if (!confirm('Close ' + market + ' position?')) return;
  fetch('/api/close/' + market, {{method:'POST'}})
    .then(r => r.json()).then(d => {{ alert(d.msg || d.error); location.reload(); }})
    .catch(e => alert('Error: ' + e));
}}
</script>
</body></html>"""
    return html, 200


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
        n_trades  = len(_trades)
        wins_n    = sum(1 for t in _trades if t["pnl"] >= 0)
        total_pnl = sum(t["pnl"] for t in _trades)

    sol_price = get_sol_price()
    return jsonify({
        "bot": DRIFT_BOT_NAME,
        "exchange": DRIFT_EXCHANGE,
        "paper_mode": DRIFT_PAPER_MODE,
        "capital": round(cap, 2),
        "starting_capital": STARTING_CAPITAL,
        "profit_goal": PROFIT_GOAL,
        "daily_pnl": round(_daily_pnl, 2),
        "total_trades": n_trades,
        "wins": wins_n,
        "losses": n_trades - wins_n,
        "win_rate": round(wins_n / max(n_trades, 1) * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "open_positions": len(pos_snap),
        "positions": pos_snap,
        "leverage": DRIFT_LEVERAGE,
        "sol_price": round(sol_price, 2) if sol_price else None,
        "profit_secured": round(_profit_secured, 2),
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
        cap = _capital

    size_usd = cap * DRIFT_TRADE_PCT * DRIFT_LEVERAGE
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
        cap = _capital

    size_usd = cap * DRIFT_TRADE_PCT * DRIFT_LEVERAGE
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
        # Use live cached price first — always accurate
        price = _positions[market].get("current_price") or _positions[market].get("entry")

    # Try fresh API price as override if cached is missing
    if not price:
        price = get_market_price(market)

    close_position(market, price, "MANUAL")
    return jsonify({"msg": f"Closed {market} position @ ${price:.4f}"})


def run_position_price_updater():
    while True:
        try:
            with _state_lock:
                markets = list(_positions.keys())
            for market in markets:
                price = get_market_price(market)
                if price:
                    with _state_lock:
                        if market in _positions:
                            pos = _positions[market]
                            raw_pct = (price - pos["entry"]) / pos["entry"] * (1 if pos["side"] == "long" else -1)
                            _positions[market]["pnl"] = raw_pct * pos["size"] * pos["leverage"]
                            _positions[market]["current_price"] = price
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
