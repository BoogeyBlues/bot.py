import os, time, threading, requests, re
from flask import Flask, jsonify
from collections import defaultdict

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────────
CLAUDE_KEY         = os.environ.get("CLAUDE_KEY", "")
WALLET             = os.environ.get("WALLET", "")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY", "")
PAPER_MODE         = os.environ.get("PAPER_MODE", "true").lower() == "true"
TRADE_AMOUNT       = float(os.environ.get("TRADE_AMOUNT", "5"))
TP_LOW             = float(os.environ.get("TP_LOW", "10"))
TP_HIGH            = float(os.environ.get("TP_HIGH", "20"))
SL_PCT             = float(os.environ.get("SL_PCT", "5"))
MAX_HOLD_MINS      = int(os.environ.get("MAX_HOLD_MINS", "10"))
MAX_OPEN           = int(os.environ.get("MAX_OPEN", "2"))
MAX_PER_COIN       = int(os.environ.get("MAX_PER_COIN", "2"))
SCAN_INTERVAL      = int(os.environ.get("SCAN_INTERVAL", "30"))
MIN_MCAP           = int(os.environ.get("MIN_MCAP", "5000"))
MAX_MCAP           = int(os.environ.get("MAX_MCAP", "500000"))
MIN_LIQ            = int(os.environ.get("MIN_LIQ", "1000"))
MIN_CHANGE_5M      = float(os.environ.get("MIN_CHANGE_5M", "3"))
MIN_BUYS_5M        = int(os.environ.get("MIN_BUYS_5M", "5"))

SOL_RPC    = "https://api.mainnet-beta.solana.com"
PUMPPORTAL = "https://pumpportal.fun/api/trade-local"

# KOL names to watch for in social sources
KOLS = ["elonmusk","elon","ansem","murad","cobie","hsaka",
        "gainzy","kaleo","pentoshi","blknoiz06","lookonchain",
        "notthreadguy","inversebrah","wublockchain","degen"]

# ── STATE ────────────────────────────────────────────────────────
capital          = float(os.environ.get("STARTING_CAPITAL", "54.86"))
capital_lock     = threading.Lock()
open_trades      = {}
trades_lock      = threading.Lock()
coin_trade_count = defaultdict(int)
trade_log        = []
completed_trades = []
log_lock         = threading.Lock()
scan_active      = True
social_signals   = []
social_lock      = threading.Lock()

# ── LOGGING ─────────────────────────────────────────────────────
def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 300:
            trade_log.pop(0)

# ── SOL PRICE ────────────────────────────────────────────────────
def get_sol_price():
    try:
        res = requests.get(
            "https://api.dexscreener.com/latest/dex/pairs/solana/8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        if pairs:
            return float(pairs[0].get("priceUsd", 0))
    except:
        pass
    return None

# ── COIN DATA FROM DEXSCREENER ───────────────────────────────────
def get_coin_data(mint):
    try:
        res = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            return None
        pair = max(sol_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        return {
            "price":      float(pair.get("priceUsd", 0) or 0),
            "mcap":       float(pair.get("marketCap", 0) or 0),
            "liq":        float(pair.get("liquidity", {}).get("usd", 0) or 0),
            "vol5m":      float(pair.get("volume", {}).get("m5", 0) or 0),
            "vol1h":      float(pair.get("volume", {}).get("h1", 0) or 0),
            "change5m":   float(pair.get("priceChange", {}).get("m5", 0) or 0),
            "change1h":   float(pair.get("priceChange", {}).get("h1", 0) or 0),
            "buys5m":     int((pair.get("txns", {}).get("m5", {}) or {}).get("buys", 0)),
            "symbol":     pair.get("baseToken", {}).get("symbol", ""),
            "name":       pair.get("baseToken", {}).get("name", ""),
            "pair_addr":  pair.get("pairAddress", ""),
        }
    except:
        return None

# ── GET TRENDING PUMP.FUN COINS ──────────────────────────────────
def get_trending_coins():
    coins = []
    try:
        res = requests.get(
            "https://api.dexscreener.com/token-boosts/latest/v1",
            timeout=10
        )
        data = res.json()
        if isinstance(data, list):
            for t in data[:30]:
                if t.get("chainId") == "solana":
                    mint = t.get("tokenAddress", "")
                    if mint:
                        coins.append({
                            "mint":    mint,
                            "symbol":  t.get("description", mint[:8])[:12],
                            "source":  "boost",
                            "twitter": any("twitter" in str(l).lower() or "x.com" in str(l).lower()
                                          for l in t.get("links", [])),
                        })
    except Exception as e:
        log("warn", f"Boost fetch: {e}")

    try:
        res = requests.get(
            "https://api.dexscreener.com/token-boosts/top/v1",
            timeout=10
        )
        data = res.json()
        if isinstance(data, list):
            mints_seen = {c["mint"] for c in coins}
            for t in data[:20]:
                if t.get("chainId") == "solana":
                    mint = t.get("tokenAddress", "")
                    if mint and mint not in mints_seen:
                        coins.append({
                            "mint":    mint,
                            "symbol":  t.get("description", mint[:8])[:12],
                            "source":  "top_boost",
                            "twitter": any("twitter" in str(l).lower() or "x.com" in str(l).lower()
                                          for l in t.get("links", [])),
                        })
                        mints_seen.add(mint)
    except Exception as e:
        log("warn", f"Top boost fetch: {e}")

    return coins[:50]

# ── SOCIAL SIGNAL SCAN ───────────────────────────────────────────
def scan_social():
    """
    Scans Reddit, CoinGecko trending, and DexScreener for
    KOL mentions and viral meme coins. Runs every 5 minutes.
    Twitter API costs $200/mo so we use these free alternatives
    which catch the same signals within minutes.
    """
    signals = []

    # Reddit cryptomoonshots
    try:
        res = requests.get(
            "https://www.reddit.com/r/cryptomoonshots/new.json?limit=25",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        posts = res.json().get("data", {}).get("children", [])
        for post in posts:
            p        = post["data"]
            title    = p.get("title", "")
            text     = p.get("selftext", "")
            ups      = p.get("ups", 0)
            combined = (title + " " + text).upper()
            kol_hit  = any(k.upper() in combined for k in KOLS)
            is_sol   = any(w in combined for w in ["SOLANA", "SOL", "PUMP.FUN", "PUMPFUN"])
            tickers  = re.findall(r'\$([A-Z]{2,10})', combined)
            if is_sol and (kol_hit or ups > 30) and tickers:
                for t in tickers[:2]:
                    signals.append({
                        "ticker":  t,
                        "source":  "reddit",
                        "kol":     kol_hit,
                        "score":   ups + (100 if kol_hit else 0),
                        "context": title[:80],
                    })
                    log("info", f"Reddit: ${t} | KOL:{kol_hit} ups:{ups}")
    except Exception as e:
        log("warn", f"Reddit: {e}")

    # CoinGecko trending meme coins
    try:
        res = requests.get(
            "https://api.coingecko.com/api/v3/search/trending",
            timeout=8
        )
        coins = res.json().get("coins", [])
        meme_words = ["DOGE","PEPE","SHIB","MEME","INU","FLOKI","WIF",
                      "BONK","CAT","FROG","MOON","PUMP","APE","BABY"]
        for c in coins[:10]:
            item   = c.get("item", {})
            symbol = item.get("symbol", "").upper()
            name   = item.get("name", "").upper()
            rank   = item.get("score", 9)
            if any(w in symbol or w in name for w in meme_words):
                signals.append({
                    "ticker":  symbol,
                    "source":  "coingecko_trending",
                    "kol":     False,
                    "score":   80 - rank * 5,
                    "context": f"#{rank+1} trending on CoinGecko",
                })
                log("info", f"CoinGecko trending meme: {symbol}")
    except Exception as e:
        log("warn", f"CoinGecko: {e}")

    with social_lock:
        social_signals.clear()
        social_signals.extend(signals)

    log("info", f"Social scan done — {len(signals)} signals")
    return signals

# ── SCORE COIN ───────────────────────────────────────────────────
def score_coin(mint, symbol, data, twitter_linked):
    """Score a coin 0-100 based on all available signals."""
    score = 0
    context = ""

    # Momentum score
    change5m = data.get("change5m", 0)
    buys5m   = data.get("buys5m", 0)
    liq      = data.get("liq", 0)

    score += min(change5m * 2, 30)   # up to 30 for price move
    score += min(buys5m * 2, 20)     # up to 20 for buy count
    score += min(liq / 500, 10)      # up to 10 for liquidity

    # Twitter link on DexScreener = social attention
    if twitter_linked:
        score += 25
        context = "Twitter linked on DexScreener"

    # Social signal match
    with social_lock:
        for sig in social_signals:
            t = sig.get("ticker", "").upper()
            if t and (t in symbol.upper() or symbol.upper() in t or t[:4] in symbol.upper()):
                boost = 50 if sig.get("kol") else 20
                score += boost
                context = sig.get("context", sig.get("source", ""))
                log("ok", f"Social match: {symbol} in {sig['source']} +{boost}pts", symbol)
                break

    return score, context

# ── CLAUDE FILTER ────────────────────────────────────────────────
def ask_claude(symbol, data, context, score):
    try:
        if not CLAUDE_KEY or CLAUDE_KEY in ["none", ""]:
            strength = "strong" if score >= 60 else "medium" if score >= 30 else "weak"
            return True, strength, "Local filter"

        prompt = f"""Pump.fun meme coin sniper — quick analysis.

Coin: {symbol}
Score: {score}/100
Market Cap: ${data.get('mcap', 0):,.0f}
Liquidity: ${data.get('liq', 0):,.0f}
5min Volume: ${data.get('vol5m', 0):,.0f}
5min Change: {data.get('change5m', 0):+.1f}%
5min Buys: {data.get('buys5m', 0)}
1h Change: {data.get('change1h', 0):+.1f}%
Signal context: {context or 'Trending on DexScreener'}

Trade: ${TRADE_AMOUNT} fixed | TP:{TP_LOW}-{TP_HIGH}% | SL:{SL_PCT}% | Max:{MAX_HOLD_MINS}min
Profit target: $1 min — $10 max (2x)

APPROVE or REJECT + reason.
If APPROVE: rate as STRONG, MEDIUM, or WEAK."""

        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 80,
                "messages": [{"role": "user", "content": prompt}]
            },
            headers={
                "x-api-key": CLAUDE_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            timeout=10
        )
        text = res.json()["content"][0]["text"].upper()
        log("ai", text[:80], symbol)

        if "REJECT" in text:
            return False, "none", text
        strength = "strong" if "STRONG" in text else "medium" if "MEDIUM" in text else "weak"
        return True, strength, text

    except Exception as e:
        log("warn", f"Claude: {e}")
        strength = "strong" if score >= 60 else "medium" if score >= 30 else "weak"
        return True, strength, "Local filter"

# ── SWAP EXECUTION ───────────────────────────────────────────────
def execute_buy(mint, symbol):
    if PAPER_MODE:
        log("ok", f"[PAPER] Buy ${TRADE_AMOUNT} -> {symbol}", symbol)
        return "PAPER_TX"
    try:
        from solders.keypair import Keypair
        from solders.transaction import VersionedTransaction
        from solana.rpc.api import Client
        from solana.rpc.types import TxOpts
        from solana.rpc.commitment import Confirmed

        sol_price = get_sol_price()
        if not sol_price:
            log("err", "No SOL price", symbol)
            return None

        sol_amount = round(TRADE_AMOUNT / sol_price, 6)
        log("info", f"${TRADE_AMOUNT} = {sol_amount} SOL -> {symbol}", symbol)

        res = requests.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={
                "publicKey":        WALLET,
                "action":           "buy",
                "mint":             mint,
                "denominatedInSol": "true",
                "amount":           sol_amount,
                "slippage":         20,
                "priorityFee":      0.001,
                "pool":             "pump"
            },
            timeout=15
        )
        if res.status_code != 200:
            log("err", f"PumpPortal {res.status_code}: {res.text[:80]}", symbol)
            return None

        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx = VersionedTransaction(
            VersionedTransaction.from_bytes(res.content).message,
            [keypair]
        )
        client = Client(SOL_RPC)
        result = client.send_raw_transaction(
            bytes(tx),
            opts=TxOpts(skip_preflight=True, preflight_commitment=Confirmed)
        )
        sig = str(result.value)
        if sig and len(sig) > 10:
            log("ok", f"Bought! Tx:{sig[:20]}...", symbol)
            log("ok", f"https://solscan.io/tx/{sig}", symbol)
            log("ok", "Check Phantom — balance updated!", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Buy error: {e}", symbol)
        return None

def execute_sell(tokens, mint, symbol):
    if PAPER_MODE:
        log("ok", f"[PAPER] Sell {tokens:.6f} {symbol}", symbol)
        return "PAPER_TX"
    try:
        from solders.keypair import Keypair
        from solders.transaction import VersionedTransaction
        from solana.rpc.api import Client
        from solana.rpc.types import TxOpts
        from solana.rpc.commitment import Confirmed

        res = requests.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={
                "publicKey":        WALLET,
                "action":           "sell",
                "mint":             mint,
                "denominatedInSol": "false",
                "amount":           tokens,
                "slippage":         20,
                "priorityFee":      0.001,
                "pool":             "pump"
            },
            timeout=15
        )
        if res.status_code != 200:
            return None

        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx = VersionedTransaction(
            VersionedTransaction.from_bytes(res.content).message,
            [keypair]
        )
        client = Client(SOL_RPC)
        result = client.send_raw_transaction(
            bytes(tx),
            opts=TxOpts(skip_preflight=True, preflight_commitment=Confirmed)
        )
        sig = str(result.value)
        if sig and len(sig) > 10:
            log("ok", f"Sold! Tx:{sig[:20]}...", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Sell error: {e}", symbol)
        return None

# ── ENTER TRADE ──────────────────────────────────────────────────
def enter_trade(mint, symbol, data, strength, context):
    global capital

    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            return False
    if coin_trade_count[mint] >= MAX_PER_COIN:
        return False
    with capital_lock:
        if capital < TRADE_AMOUNT:
            log("warn", f"Low capital ${capital:.2f}")
            return False

    price = data["price"]
    if price <= 0:
        return False

    # Dynamic TP based on signal strength
    tp_pct = TP_HIGH if strength == "strong" else (TP_LOW + TP_HIGH) / 2 if strength == "medium" else TP_LOW
    sl_pct = SL_PCT

    tp_price = price * (1 + tp_pct / 100)
    sl_price = price * (1 - sl_pct / 100)
    exp_profit = round(TRADE_AMOUNT * tp_pct / 100, 2)
    exp_profit = max(1.0, min(exp_profit, TRADE_AMOUNT))  # $1 min, 2x max

    log("ok", f"[{strength.upper()}] ${TRADE_AMOUNT} | TP:+{tp_pct}% (${exp_profit}) | SL:-{sl_pct}%", symbol)

    tx = execute_buy(mint, symbol)
    if not tx:
        return False

    with trades_lock:
        open_trades[mint] = {
            "symbol":     symbol,
            "mint":       mint,
            "entry":      price,
            "tp":         tp_price,
            "tp_pct":     tp_pct,
            "sl":         sl_price,
            "sl_pct":     sl_pct,
            "amount":     TRADE_AMOUNT,
            "tokens":     TRADE_AMOUNT / price,
            "exp_profit": exp_profit,
            "strength":   strength,
            "context":    context[:80],
            "opened_at":  time.time(),
            "time_str":   time.strftime("%H:%M:%S"),
        }

    coin_trade_count[mint] += 1
    with capital_lock:
        capital -= TRADE_AMOUNT
    return True

# ── EXIT TRADE ───────────────────────────────────────────────────
def exit_trade(mint, current_price, reason):
    global capital
    with trades_lock:
        if mint not in open_trades:
            return
        trade = open_trades.pop(mint)

    symbol   = trade["symbol"]
    entry    = trade["entry"]
    amount   = trade["amount"]
    tokens   = trade["tokens"]
    tp_pct   = trade["tp_pct"]
    hold_m   = (time.time() - trade["opened_at"]) / 60

    if reason == "TP":
        pnl = amount * (tp_pct / 100)
    elif reason == "SL":
        pnl = -(amount * SL_PCT / 100)
    else:
        pnl = ((current_price - entry) / entry) * amount if entry > 0 else 0

    pnl = max(-amount, min(pnl, amount))  # cap between -$5 and +$5 (2x)

    with capital_lock:
        capital += amount + pnl

    emoji = "✅" if pnl >= 0 else "❌"
    log("ok" if pnl >= 0 else "err",
        f"{emoji} {reason} | Entry:${entry:.8f} Exit:${current_price:.8f} | PnL:{'+' if pnl>=0 else ''}${pnl:.4f} | {hold_m:.1f}m | Capital:${capital:.2f}",
        symbol)

    execute_sell(tokens, mint, symbol)

    completed_trades.append({
        "symbol":   symbol,
        "entry":    entry,
        "exit":     current_price,
        "result":   reason,
        "pnl":      round(pnl, 4),
        "hold_m":   round(hold_m, 1),
        "strength": trade["strength"],
        "context":  trade["context"],
        "time":     time.strftime("%H:%M:%S"),
    })

    if capital >= 100:
        log("ok", "GOAL REACHED — $100!")
    if capital < 2:
        global scan_active
        scan_active = False
        log("err", "Capital too low — stopping")

# ── MONITOR ──────────────────────────────────────────────────────
def monitor_loop():
    while True:
        time.sleep(10)
        with trades_lock:
            mints = list(open_trades.keys())
        for mint in mints:
            with trades_lock:
                if mint not in open_trades:
                    continue
                trade = open_trades[mint]

            hold_m = (time.time() - trade["opened_at"]) / 60
            symbol = trade["symbol"]

            # Force time exit
            if hold_m >= MAX_HOLD_MINS:
                data = get_coin_data(mint)
                price = data["price"] if data and data["price"] > 0 else trade["entry"]
                log("warn", f"TIME EXIT after {hold_m:.1f}m", symbol)
                exit_trade(mint, price, "TIME")
                continue

            data = get_coin_data(mint)
            if not data or data["price"] <= 0:
                continue

            price = data["price"]
            move  = ((price - trade["entry"]) / trade["entry"]) * 100
            log("info", f"${price:.8f} | {move:+.2f}% | TP:+{trade['tp_pct']}% SL:-{trade['sl_pct']}% | {hold_m:.1f}/{MAX_HOLD_MINS}m", symbol)

            if price >= trade["tp"]:
                exit_trade(mint, price, "TP")
            elif price <= trade["sl"]:
                exit_trade(mint, price, "SL")

# ── SOCIAL SCAN LOOP ─────────────────────────────────────────────
def social_loop():
    time.sleep(5)  # let main scanner start first
    while scan_active:
        try:
            scan_social()
        except Exception as e:
            log("err", f"Social loop: {e}")
        time.sleep(300)  # every 5 minutes

# ── MAIN SCANNER ─────────────────────────────────────────────────
def scanner_loop():
    global scan_active
    log("ok", "=" * 50)
    log("ok", "PumpFun KOL Sniper — ready")
    log("ok", f"Trade: ${TRADE_AMOUNT} | TP:{TP_LOW}-{TP_HIGH}% | SL:{SL_PCT}% | Max:{MAX_HOLD_MINS}m")
    log("ok", f"Profit: $1 min — ${TRADE_AMOUNT} max (2x)")
    log("ok", f"Social: Reddit + CoinGecko + DexScreener (every 5m)")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else 'LIVE -> Phantom'}")
    log("ok", "=" * 50)

    # Initial social scan
    scan_social()

    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)

            if num_open >= MAX_OPEN:
                log("info", f"Max trades open ({num_open}/{MAX_OPEN})")
                time.sleep(SCAN_INTERVAL)
                continue

            log("info", f"--- Scanning pump.fun | Open:{num_open}/{MAX_OPEN} ---")

            trending = get_trending_coins()
            log("info", f"Checking {len(trending)} trending coins...")

            candidates = []

            for coin in trending:
                mint    = coin["mint"]
                twitter = coin.get("twitter", False)

                if coin_trade_count[mint] >= MAX_PER_COIN:
                    continue
                with trades_lock:
                    if mint in open_trades:
                        continue

                data = get_coin_data(mint)
                if not data or data["price"] <= 0:
                    continue

                symbol = data["symbol"] or data["name"] or mint[:8]

                # Hard filters — must pass all
                if data["mcap"] < MIN_MCAP:       continue
                if data["mcap"] > MAX_MCAP:       continue
                if data["liq"] < MIN_LIQ:         continue
                if data["change5m"] < MIN_CHANGE_5M: continue
                if data["buys5m"] < MIN_BUYS_5M:  continue

                score, context = score_coin(mint, symbol, data, twitter)

                if score >= 8:
                    candidates.append({
                        "mint":    mint,
                        "symbol":  symbol,
                        "data":    data,
                        "score":   score,
                        "context": context,
                    })
                    log("info", f"Candidate: {symbol} score:{score} +{data['change5m']:.1f}% buys:{data['buys5m']}", symbol)

                time.sleep(0.3)

            candidates.sort(key=lambda c: c["score"], reverse=True)
            log("info", f"Qualified: {len(candidates)} candidates")

            for c in candidates[:3]:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN:
                        break
                    if c["mint"] in open_trades:
                        continue

                approved, strength, reason = ask_claude(c["symbol"], c["data"], c["context"], c["score"])
                if not approved:
                    log("ai", f"Rejected: {reason[:50]}", c["symbol"])
                    continue

                enter_trade(c["mint"], c["symbol"], c["data"], strength, c["context"])

            if not candidates:
                log("info", "No qualifying pump.fun coins this scan")

        except Exception as e:
            log("err", f"Scanner: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ─────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    with trades_lock:
        n = len(open_trades)
    return f"KOL Sniper | Capital:${capital:.2f} | Open:{n}/{MAX_OPEN} | {'PAPER' if PAPER_MODE else 'LIVE'}", 200

@app.route("/status", methods=["GET"])
def status():
    wins      = len([t for t in completed_trades if t["result"] == "TP"])
    losses    = len([t for t in completed_trades if t["result"] == "SL"])
    times     = len([t for t in completed_trades if t["result"] == "TIME"])
    total_pnl = sum(t["pnl"] for t in completed_trades)
    wr        = round(wins / max(wins + losses, 1) * 100, 1)
    return jsonify({
        "capital":       round(capital, 2),
        "goal":          100,
        "paper_mode":    PAPER_MODE,
        "open_trades":   len(open_trades),
        "wins":          wins,
        "losses":        losses,
        "time_exits":    times,
        "win_rate":      wr,
        "total_pnl":     round(total_pnl, 4),
        "total_trades":  len(completed_trades),
        "social_signals": len(social_signals),
        "settings": {
            "trade_amount":  TRADE_AMOUNT,
            "tp_range":      f"{TP_LOW}-{TP_HIGH}%",
            "sl_pct":        SL_PCT,
            "max_hold_mins": MAX_HOLD_MINS,
        }
    })

@app.route("/trades", methods=["GET"])
def trades():
    return jsonify({
        "open":      [{k: v for k, v in t.items() if k != "opened_at"} for t in open_trades.values()],
        "completed": completed_trades[-50:]
    })

@app.route("/signals", methods=["GET"])
def signals():
    with social_lock:
        return jsonify({"social_signals": social_signals})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-50:]})

# ── START ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=social_loop,  daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", f"Starting | Wallet:{WALLET[:8] if WALLET else 'NOT SET'}...{WALLET[-4:] if WALLET else ''}")
    log("ok", f"{'PAPER MODE — no real money' if PAPER_MODE else 'LIVE MODE — real trades to Phantom'}")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
