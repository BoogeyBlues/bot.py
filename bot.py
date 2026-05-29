import os, time, threading, requests, re
from flask import Flask, jsonify
from collections import deque, defaultdict

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────────
CLAUDE_KEY         = os.environ.get("CLAUDE_KEY", "")
WALLET             = os.environ.get("WALLET", "")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY", "")
PAPER_MODE         = os.environ.get("PAPER_MODE", "true").lower() == "true"

# Trade settings — $5 per trade, 10-20% TP, tight SL
TRADE_AMOUNT   = float(os.environ.get("TRADE_AMOUNT", "5"))       # $5 fixed per trade
TP_LOW         = float(os.environ.get("TP_LOW", "10"))            # 10% min TP
TP_HIGH        = float(os.environ.get("TP_HIGH", "20"))           # 20% max TP
SL_PCT         = float(os.environ.get("SL_PCT", "5"))             # 5% tight SL
MAX_HOLD_MINS  = int(os.environ.get("MAX_HOLD_MINS", "10"))       # 10 min max hold
MAX_OPEN       = int(os.environ.get("MAX_OPEN", "2"))             # 2 trades at once
MAX_PER_COIN   = int(os.environ.get("MAX_PER_COIN", "2"))         # max 2 trades per coin
SCAN_INTERVAL  = int(os.environ.get("SCAN_INTERVAL", "30"))       # scan every 30s

# Profit targets
MIN_PROFIT_USD = float(os.environ.get("MIN_PROFIT", "1.0"))       # minimum $1 profit target
MAX_PROFIT_USD = float(os.environ.get("MAX_PROFIT", "10.0"))      # maximum 2x = $10 on $5

SOL_RPC    = "https://api.mainnet-beta.solana.com"
PUMPPORTAL = "https://pumpportal.fun/api/trade-local"

# ── KOL WATCH LIST ───────────────────────────────────────────────
# These are the accounts we track mentions of
# We use Reddit + RSS instead of Twitter since X API costs $200/mo
KOL_NAMES = [
    "elonmusk", "elon", "ansem", "murad", "cobie",
    "hsaka", "gainzy", "degen", "kaleo", "pentoshi",
    "cryptokaleo", "blknoiz06", "notthreadguy",
    "inversebrah", "lookonchain", "wublockchain",
]

# Coins that KOLs often move
KOL_COINS = [
    "DOGE", "SHIB", "PEPE", "WIF", "BONK", "FLOKI",
    "BABYDOGE", "MEME", "WOJAK", "TURBO", "MOG", "BRETT"
]

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
kol_signals      = []   # signals from KOL/social scanning
kol_lock         = threading.Lock()

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

# ── COIN DATA ────────────────────────────────────────────────────
def get_coin_data(mint):
    try:
        res = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        if not pairs:
            return None
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            return None
        pair = max(sol_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        return {
            "price":     float(pair.get("priceUsd", 0) or 0),
            "mcap":      float(pair.get("marketCap", 0) or 0),
            "liq":       float(pair.get("liquidity", {}).get("usd", 0) or 0),
            "vol5m":     float(pair.get("volume", {}).get("m5", 0) or 0),
            "vol1h":     float(pair.get("volume", {}).get("h1", 0) or 0),
            "change5m":  float(pair.get("priceChange", {}).get("m5", 0) or 0),
            "change1h":  float(pair.get("priceChange", {}).get("h1", 0) or 0),
            "txns5m_buy": int((pair.get("txns", {}).get("m5", {}) or {}).get("buys", 0)),
            "symbol":    pair.get("baseToken", {}).get("symbol", ""),
            "name":      pair.get("baseToken", {}).get("name", ""),
            "pair_addr": pair.get("pairAddress", ""),
        }
    except:
        return None

# ── TRENDING PUMP.FUN COINS ──────────────────────────────────────
def get_trending_coins():
    """Get trending pump.fun coins from DexScreener."""
    coins = []
    try:
        # Boosted/trending tokens
        res = requests.get("https://api.dexscreener.com/token-boosts/latest/v1", timeout=10)
        data = res.json()
        if isinstance(data, list):
            for t in data[:30]:
                if t.get("chainId") == "solana":
                    coins.append({
                        "mint":   t.get("tokenAddress", ""),
                        "symbol": t.get("description", "")[:12],
                        "source": "dexscreener_boost"
                    })
    except Exception as e:
        log("warn", f"Trending fetch error: {e}")

    # New pairs search
    try:
        res = requests.get("https://api.dexscreener.com/latest/dex/search?q=pump.fun", timeout=10)
        pairs = res.json().get("pairs", [])
        for p in pairs[:20]:
            if p.get("chainId") != "solana":
                continue
            mint = p.get("baseToken", {}).get("address", "")
            sym  = p.get("baseToken", {}).get("symbol", "")
            if mint and mint not in [c["mint"] for c in coins]:
                coins.append({"mint": mint, "symbol": sym, "source": "dexscreener_search"})
    except:
        pass

    return [c for c in coins if c["mint"]][:50]

# ── SOCIAL SIGNAL SCANNER ────────────────────────────────────────
def scan_social_signals():
    """
    Scans free social sources for KOL mentions and coin signals.
    Uses Reddit, RSS feeds, and DexScreener social data.
    No Twitter API needed.
    """
    signals = []

    # 1. Reddit r/cryptomoonshots — often has pump.fun coins before they moon
    try:
        res = requests.get(
            "https://www.reddit.com/r/cryptomoonshots/new.json?limit=25",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        posts = res.json().get("data", {}).get("children", [])
        for post in posts:
            p     = post["data"]
            title = p.get("title", "").upper()
            text  = p.get("selftext", "").upper()
            ups   = p.get("ups", 0)
            combined = title + " " + text

            # Check if any KOL mentioned
            kol_mentioned = any(kol.upper() in combined for kol in KOL_NAMES)

            # Check if Solana/pump.fun coin
            is_solana = any(w in combined for w in ["SOLANA", "SOL", "PUMP.FUN", "PUMPFUN", "SPL"])

            # Extract potential coin tickers ($TICKER format)
            tickers = re.findall(r'\$([A-Z]{2,10})', combined)

            if (kol_mentioned or ups > 50) and is_solana and tickers:
                for ticker in tickers[:2]:
                    signals.append({
                        "source":  "reddit",
                        "ticker":  ticker,
                        "context": title[:100],
                        "kol":     kol_mentioned,
                        "score":   ups,
                    })
                    log("info", f"Reddit signal: ${ticker} | KOL:{kol_mentioned} | Ups:{ups}")

    except Exception as e:
        log("warn", f"Reddit scan error: {e}")

    # 2. DexScreener trending — coins getting social attention
    try:
        res = requests.get("https://api.dexscreener.com/token-boosts/top/v1", timeout=10)
        data = res.json()
        if isinstance(data, list):
            for t in data[:10]:
                if t.get("chainId") == "solana":
                    desc = t.get("description", "")
                    links = t.get("links", [])
                    has_twitter = any("twitter" in str(l).lower() or "x.com" in str(l).lower() for l in links)
                    signals.append({
                        "source":  "dexscreener_top",
                        "mint":    t.get("tokenAddress", ""),
                        "ticker":  desc[:10],
                        "kol":     has_twitter,
                        "score":   t.get("amount", 0),
                    })
                    if has_twitter:
                        log("info", f"DexScreener top boost with Twitter: {desc[:20]}")
    except Exception as e:
        log("warn", f"DexScreener boost error: {e}")

    # 3. CoinGecko trending — catches coins going viral
    try:
        res = requests.get(
            "https://api.coingecko.com/api/v3/search/trending",
            timeout=8
        )
        coins = res.json().get("coins", [])
        for c in coins[:7]:
            item   = c.get("item", {})
            symbol = item.get("symbol", "").upper()
            name   = item.get("name", "")
            score  = item.get("score", 0)
            # Only care about meme-like coins
            meme_words = ["DOGE", "PEPE", "SHIB", "MEME", "INU", "FLOKI", "WIF", "BONK", "CAT", "FROG", "MOON"]
            is_meme = any(w in symbol or w in name.upper() for w in meme_words)
            if is_meme:
                signals.append({
                    "source":  "coingecko_trending",
                    "ticker":  symbol,
                    "context": f"Trending #{score+1} on CoinGecko",
                    "kol":     False,
                    "score":   100 - score,
                })
                log("info", f"CoinGecko trending meme: {symbol}")
    except Exception as e:
        log("warn", f"CoinGecko trending error: {e}")

    with kol_lock:
        kol_signals.clear()
        kol_signals.extend(signals)

    log("info", f"Social scan complete — {len(signals)} signals found")
    return signals

# ── CALCULATE TAKE PROFIT ─────────────────────────────────────────
def calc_tp(price, signal_strength):
    """
    Dynamic TP between 10-20% based on signal strength.
    Strong KOL signal = 20% TP
    Normal signal = 10% TP
    """
    if signal_strength == "strong":
        tp_pct = TP_HIGH   # 20%
    elif signal_strength == "medium":
        tp_pct = (TP_LOW + TP_HIGH) / 2  # 15%
    else:
        tp_pct = TP_LOW    # 10%

    tp_price = price * (1 + tp_pct / 100)
    profit   = TRADE_AMOUNT * (tp_pct / 100)

    # Ensure minimum $1 profit, cap at 2x ($10 on $5)
    profit = max(MIN_PROFIT_USD, min(profit, MAX_PROFIT_USD))

    return tp_price, tp_pct, profit

# ── CLAUDE SIGNAL ANALYSIS ───────────────────────────────────────
def ask_claude(symbol, mint, coin_data, social_context=""):
    try:
        if not CLAUDE_KEY or CLAUDE_KEY in ["none", ""]:
            return True, "strong", "Local filter"

        prompt = f"""You are a pump.fun meme coin sniper analyzing a trade opportunity.

Coin: {symbol}
Market Cap: ${coin_data.get('mcap', 0):,.0f}
Liquidity: ${coin_data.get('liq', 0):,.0f}
5min Volume: ${coin_data.get('vol5m', 0):,.0f}
5min Price Change: {coin_data.get('change5m', 0):+.1f}%
5min Buy Transactions: {coin_data.get('txns5m_buy', 0)}
1h Change: {coin_data.get('change1h', 0):+.1f}%
Social Context: {social_context or 'Trending on DexScreener'}

Trade Parameters:
- Fixed trade size: ${TRADE_AMOUNT}
- Take Profit: {TP_LOW}-{TP_HIGH}% (${TRADE_AMOUNT * TP_LOW/100:.2f}-${TRADE_AMOUNT * TP_HIGH/100:.2f} profit)
- Stop Loss: {SL_PCT}% (${TRADE_AMOUNT * SL_PCT/100:.2f} max loss)
- Max hold: {MAX_HOLD_MINS} minutes then force exit

APPROVE or REJECT + one reason.
If APPROVE, also rate signal strength: STRONG, MEDIUM, or WEAK."""

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
        log("warn", f"Claude error: {e}")
        return True, "medium", "Local filter"

# ── EXECUTE SWAP ─────────────────────────────────────────────────
def execute_swap(mint, symbol):
    """Execute $5 buy on pump.fun via PumpPortal."""
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
        log("info", f"${TRADE_AMOUNT} USDC = {sol_amount} SOL -> {symbol}", symbol)

        response = requests.post(
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

        if response.status_code != 200:
            log("err", f"PumpPortal {response.status_code}: {response.text[:100]}", symbol)
            return None

        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx = VersionedTransaction(
            VersionedTransaction.from_bytes(response.content).message,
            [keypair]
        )
        client = Client(SOL_RPC)
        opts = TxOpts(skip_preflight=True, preflight_commitment=Confirmed)
        result = client.send_raw_transaction(bytes(tx), opts=opts)
        tx_sig = str(result.value)

        if tx_sig and len(tx_sig) > 10:
            log("ok", f"Bought {symbol}! Tx: {tx_sig[:20]}...", symbol)
            log("ok", f"Solscan: https://solscan.io/tx/{tx_sig}", symbol)
            return tx_sig
        return None
    except Exception as e:
        log("err", f"Swap error: {e}", symbol)
        return None

def execute_sell(tokens, mint, symbol):
    """Sell tokens back via PumpPortal."""
    if PAPER_MODE:
        log("ok", f"[PAPER] Sell {tokens:.6f} {symbol}", symbol)
        return "PAPER_TX"
    try:
        from solders.keypair import Keypair
        from solders.transaction import VersionedTransaction
        from solana.rpc.api import Client
        from solana.rpc.types import TxOpts
        from solana.rpc.commitment import Confirmed

        response = requests.post(
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
        if response.status_code != 200:
            return None

        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx = VersionedTransaction(
            VersionedTransaction.from_bytes(response.content).message,
            [keypair]
        )
        client = Client(SOL_RPC)
        opts = TxOpts(skip_preflight=True, preflight_commitment=Confirmed)
        result = client.send_raw_transaction(bytes(tx), opts=opts)
        tx_sig = str(result.value)
        if tx_sig and len(tx_sig) > 10:
            log("ok", f"Sold {symbol}! Tx: {tx_sig[:20]}...", symbol)
            return tx_sig
        return None
    except Exception as e:
        log("err", f"Sell error: {e}", symbol)
        return None

# ── ENTER TRADE ──────────────────────────────────────────────────
def enter_trade(mint, symbol, coin_data, signal_strength, social_context):
    global capital

    if coin_trade_count[mint] >= MAX_PER_COIN:
        return False
    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            return False

    if capital < TRADE_AMOUNT:
        log("warn", f"Insufficient capital ${capital:.2f} for ${TRADE_AMOUNT} trade")
        return False

    price = coin_data["price"]
    if price <= 0:
        return False

    tp_price, tp_pct, expected_profit = calc_tp(price, signal_strength)
    sl_price = price * (1 - SL_PCT / 100)

    log("ok", f"[{signal_strength.upper()}] Entry ${TRADE_AMOUNT} | TP:+{tp_pct}% (${expected_profit:.2f} profit) | SL:-{SL_PCT}%", symbol)

    tx_sig = execute_swap(mint, symbol)
    if not tx_sig:
        return False

    tokens = TRADE_AMOUNT / price

    with trades_lock:
        open_trades[mint] = {
            "symbol":    symbol,
            "mint":      mint,
            "entry":     price,
            "tp":        tp_price,
            "tp_pct":    tp_pct,
            "sl":        sl_price,
            "sl_pct":    SL_PCT,
            "amount":    TRADE_AMOUNT,
            "tokens":    tokens,
            "exp_profit": expected_profit,
            "strength":  signal_strength,
            "context":   social_context[:80],
            "opened_at": time.time(),
            "time_str":  time.strftime("%H:%M:%S"),
            "live":      not PAPER_MODE,
        }

    coin_trade_count[mint] += 1
    with capital_lock:
        capital -= TRADE_AMOUNT  # reserve capital

    return True

# ── EXIT TRADE ───────────────────────────────────────────────────
def exit_trade(mint, current_price, reason):
    global capital

    with trades_lock:
        if mint not in open_trades:
            return
        trade = open_trades.pop(mint)

    symbol    = trade["symbol"]
    entry     = trade["entry"]
    amount    = trade["amount"]
    tokens    = trade["tokens"]
    tp_pct    = trade["tp_pct"]
    hold_m    = (time.time() - trade["opened_at"]) / 60

    if reason == "TP":
        pnl = amount * (tp_pct / 100)
    elif reason == "SL":
        pnl = -(amount * SL_PCT / 100)
    else:  # TIME exit
        pnl = ((current_price - entry) / entry) * amount if entry > 0 else 0

    with capital_lock:
        capital += amount + pnl  # return capital + profit/loss

    emoji = "✅" if pnl >= 0 else "❌"
    log("ok" if pnl >= 0 else "err",
        f"{emoji} {reason} | Entry:${entry:.8f} Exit:${current_price:.8f} | PnL:{'+' if pnl>=0 else ''}${pnl:.4f} | Held:{hold_m:.1f}m | Capital:${capital:.2f}", symbol)

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
    if capital < 1:
        global scan_active
        scan_active = False

# ── MONITOR LOOP ─────────────────────────────────────────────────
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

            if hold_m >= MAX_HOLD_MINS:
                data = get_coin_data(mint)
                price = data["price"] if data and data["price"] > 0 else trade["entry"]
                log("warn", f"TIME EXIT after {hold_m:.1f}m", trade["symbol"])
                exit_trade(mint, price, f"TIME")
                continue

            data = get_coin_data(mint)
            if not data or data["price"] <= 0:
                continue

            price = data["price"]
            move  = ((price - trade["entry"]) / trade["entry"]) * 100
            log("info", f"${price:.8f} | Move:{move:+.2f}% | TP:{trade['tp_pct']}% SL:{SL_PCT}% | {hold_m:.1f}/{MAX_HOLD_MINS}m", trade["symbol"])

            if price >= trade["tp"]:
                exit_trade(mint, price, "TP")
            elif price <= trade["sl"]:
                exit_trade(mint, price, "SL")

# ── SOCIAL SCANNER THREAD ────────────────────────────────────────
def social_scanner_loop():
    """Runs every 5 minutes to refresh social signals."""
    while scan_active:
        try:
            log("info", "--- Social scan (Reddit + CoinGecko + DexScreener) ---")
            scan_social_signals()
        except Exception as e:
            log("err", f"Social scanner error: {e}")
        time.sleep(300)  # every 5 minutes

# ── MAIN SCANNER ─────────────────────────────────────────────────
def scanner_loop():
    global scan_active
    log("ok", "=" * 55)
    log("ok", "PumpFun KOL Sniper — starting")
    log("ok", f"Trade size: ${TRADE_AMOUNT} fixed | TP: {TP_LOW}-{TP_HIGH}% | SL: {SL_PCT}%")
    log("ok", f"Profit targets: ${MIN_PROFIT_USD:.0f} min — ${MAX_PROFIT_USD:.0f} max (2x)")
    log("ok", f"Max hold: {MAX_HOLD_MINS} mins | Mode: {'PAPER' if PAPER_MODE else 'LIVE'}")
    log("ok", "Social sources: Reddit + CoinGecko Trending + DexScreener")
    log("ok", "=" * 55)

    # Initial social scan
    scan_social_signals()

    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)

            if num_open >= MAX_OPEN:
                log("info", f"Max trades open ({num_open}/{MAX_OPEN})")
                time.sleep(SCAN_INTERVAL)
                continue

            log("info", f"--- Scanning | Open:{num_open}/{MAX_OPEN} ---")

            # Get trending coins
            trending = get_trending_coins()
            log("info", f"Found {len(trending)} trending pump.fun coins")

            # Get current social signals
            with kol_lock:
                current_signals = list(kol_signals)

            # Score each trending coin
            candidates = []

            for coin in trending[:40]:
                mint   = coin["mint"]
                if not mint:
                    continue
                if coin_trade_count[mint] >= MAX_PER_COIN:
                    continue
                with trades_lock:
                    if mint in open_trades:
                        continue

                data = get_coin_data(mint)
                if not data or data["price"] <= 0:
                    continue

                symbol = data["symbol"] or data["name"] or mint[:8]

                # Base filters
                if data["mcap"] < 5000 or data["mcap"] > 500000:
                    continue
                if data["liq"] < 1000:
                    continue
                if data["change5m"] < 3:
                    continue
                if data["txns5m_buy"] < 5:
                    continue

                # Score the coin
                score = 0
                social_context = ""

                # Check if mentioned in social signals
                for sig in current_signals:
                    ticker = sig.get("ticker", "").upper()
                    if ticker and (ticker in symbol.upper() or symbol.upper() in ticker):
                        score += 50 if sig.get("kol") else 20
                        social_context = sig.get("context", sig.get("source", ""))
                        log("ok", f"SOCIAL MATCH: {symbol} in {sig['source']} score+{score}", symbol)

                # Momentum score
                score += min(data["change5m"] * 2, 30)    # up to 30 pts for momentum
                score += min(data["txns5m_buy"], 20)       # up to 20 pts for buys
                score += min(data["liq"] / 1000, 10)       # up to 10 pts for liquidity

                if score >= 10:
                    candidates.append({
                        "mint":    mint,
                        "symbol":  symbol,
                        "data":    data,
                        "score":   score,
                        "context": social_context or f"+{data['change5m']:.1f}% on pump.fun",
                    })

            # Sort by score
            candidates.sort(key=lambda c: c["score"], reverse=True)
            log("info", f"Candidates: {len(candidates)} | Top score: {candidates[0]['score'] if candidates else 0}")

            for c in candidates[:3]:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN:
                        break
                    if c["mint"] in open_trades:
                        continue

                symbol = c["symbol"]
                score  = c["score"]
                signal_strength = "strong" if score >= 60 else "medium" if score >= 30 else "weak"

                log("ok", f"Candidate: {symbol} score:{score} strength:{signal_strength} | {c['context'][:50]}", symbol)

                # Claude filter
                approved, strength, reason = ask_claude(symbol, c["mint"], c["data"], c["context"])
                if not approved:
                    log("ai", f"Rejected: {reason[:50]}", symbol)
                    continue

                enter_trade(c["mint"], symbol, c["data"], strength, c["context"])

            if not candidates:
                log("info", "No qualifying candidates this scan")

        except Exception as e:
            log("err", f"Scanner error: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ─────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    with trades_lock:
        n = len(open_trades)
    return f"KOL Sniper | Capital: ${capital:.2f} | Open: {n}/{MAX_OPEN} | {'PAPER' if PAPER_MODE else 'LIVE'}", 200

@app.route("/status", methods=["GET"])
def status():
    wins      = len([t for t in completed_trades if t["result"] == "TP"])
    losses    = len([t for t in completed_trades if t["result"] == "SL"])
    time_exits = len([t for t in completed_trades if t["result"] == "TIME"])
    total_pnl = sum(t["pnl"] for t in completed_trades)
    wr        = round(wins / max(wins + losses, 1) * 100, 1)
    return jsonify({
        "capital":       round(capital, 2),
        "goal":          100,
        "paper_mode":    PAPER_MODE,
        "open_trades":   len(open_trades),
        "wins":          wins,
        "losses":        losses,
        "time_exits":    time_exits,
        "win_rate":      wr,
        "total_pnl":     round(total_pnl, 4),
        "total_trades":  len(completed_trades),
        "social_signals": len(kol_signals),
        "settings": {
            "trade_amount":  TRADE_AMOUNT,
            "tp_range":      f"{TP_LOW}-{TP_HIGH}%",
            "sl_pct":        SL_PCT,
            "max_hold_mins": MAX_HOLD_MINS,
            "profit_range":  f"${MIN_PROFIT_USD}-${MAX_PROFIT_USD}",
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
    with kol_lock:
        return jsonify({"social_signals": kol_signals})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-50:]})

# ── START ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=monitor_loop,       daemon=True).start()
    threading.Thread(target=social_scanner_loop, daemon=True).start()
    threading.Thread(target=scanner_loop,        daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", f"KOL Sniper | Wallet: {WALLET[:8]}...{WALLET[-4:] if WALLET else 'NOT SET'}")
    log("ok", f"{'PAPER MODE' if PAPER_MODE else 'LIVE -> Phantom'}")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
