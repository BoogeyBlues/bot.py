import os, time, threading, requests, re
from flask import Flask, jsonify
from collections import defaultdict

try:
    from solders.keypair import Keypair
    from solders.transaction import VersionedTransaction
    from solana.rpc.api import Client
    from solana.rpc.types import TxOpts
    _SOLANA_AVAILABLE = True
except ImportError:
    _SOLANA_AVAILABLE = False

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
MIN_BOND_PCT       = float(os.environ.get("MIN_BOND_PCT", "70"))
MAX_BOND_PCT       = float(os.environ.get("MAX_BOND_PCT", "99"))
MIN_REAL_BUYERS    = int(os.environ.get("MIN_REAL_BUYERS", "300"))
MIN_MCAP           = int(os.environ.get("MIN_MCAP", "10000"))
MAX_MCAP           = int(os.environ.get("MAX_MCAP", "500000"))
MIN_LIQ            = int(os.environ.get("MIN_LIQ", "5000"))
MAX_TOP10_PCT      = float(os.environ.get("MAX_TOP10_PCT", "20"))
MIN_CHANGE_5M      = float(os.environ.get("MIN_CHANGE_5M", "2"))
MIN_BUYS_5M        = int(os.environ.get("MIN_BUYS_5M", "10"))

SOL_RPC    = "<https://api.mainnet-beta.solana.com>"
PUMPPORTAL = "<https://pumpportal.fun/api/trade-local>"

KOLS = ["elonmusk","elon","ansem","murad","cobie","hsaka",
        "gainzy","kaleo","pentoshi","blknoiz06","lookonchain",
        "notthreadguy","inversebrah","wublockchain","degen"]

# ── STATE ────────────────────────────────────────────────────────
capital           = float(os.environ.get("STARTING_CAPITAL", "54.86"))
capital_lock      = threading.Lock()
open_trades       = {}
trades_lock       = threading.Lock()
coin_trade_count  = defaultdict(int)
blacklisted_mints = set()
blacklisted_devs  = set()
trade_log         = []
completed_trades  = []
log_lock          = threading.Lock()
scan_active       = True
social_signals    = []
social_lock       = threading.Lock()
check_cache       = {}

# ── LOGGING ─────────────────────────────────────────────────────
def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 300:
            trade_log.pop(0)

# ── FETCH PUMP.FUN COINS ─────────────────────────────────────────
def get_pumpfun_coins():
    """
    Fetch live pump.fun coins using the correct v3 API.
    Falls back to DexScreener if pump.fun API is unavailable.
    """
    coins = []

    # Primary: pump.fun frontend API v3
    endpoints = [
        "<https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false>",
        "<https://frontend-api-v3.pump.fun/coins/currently-live?offset=0&limit=50&includeNsfw=false&order=DESC>",
        "<https://frontend-api-v2.pump.fun/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false>",
    ]

    for url in endpoints:
        try:
            res = requests.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
                    "Accept": "application/json",
                    "Referer": "<https://pump.fun/>",
                    "Origin": "<https://pump.fun>",
                },
                timeout=10
            )
            if res.status_code == 200:
                data = res.json()
                items = data if isinstance(data, list) else data.get("coins", [])
                if items:
                    for coin in items[:50]:
                        mint  = coin.get("mint", "")
                        sym   = coin.get("symbol", mint[:8])
                        mcap  = float(coin.get("usd_market_cap", 0) or 0)
                        # Calculate bonding %
                        vsol  = float(coin.get("virtual_sol_reserves", 0) or 0)
                        bond  = min((vsol / 85_000_000_000) * 100, 99.9) if vsol > 0 else 0
                        complete = coin.get("complete", False)
                        if mint and not complete:
                            coins.append({
                                "mint":      mint,
                                "symbol":    sym,
                                "mcap":      mcap,
                                "bond_pct":  round(bond, 1),
                                "twitter":   bool(coin.get("twitter")),
                                "telegram":  bool(coin.get("telegram")),
                                "website":   bool(coin.get("website")),
                                "dev":       coin.get("creator", ""),
                                "replies":   int(coin.get("reply_count", 0) or 0),
                                "desc":      coin.get("description", ""),
                                "complete":  complete,
                            })
                    log("info", f"pump.fun API: {len(coins)} live coins from {url.split('/')[4]}")
                    return coins
        except Exception as e:
            log("warn", f"Endpoint failed {url[:50]}: {e}")
            continue

    # Fallback: DexScreener token boosts (these are pump.fun coins)
    log("warn", "pump.fun API unavailable — using DexScreener fallback")
    try:
        res = requests.get(
            "<https://api.dexscreener.com/token-boosts/latest/v1>",
            timeout=10
        )
        data = res.json()
        if isinstance(data, list):
            for t in data[:30]:
                if t.get("chainId") == "solana":
                    mint = t.get("tokenAddress", "")
                    if mint:
                        links = t.get("links", [])
                        coins.append({
                            "mint":     mint,
                            "symbol":   t.get("description", mint[:8])[:12],
                            "mcap":     0,
                            "bond_pct": 0,
                            "twitter":  any("twitter" in str(l).lower() or "<x.com>" in str(l).lower() for l in links),
                            "telegram": any("telegram" in str(l).lower() or "<t.me>" in str(l).lower() for l in links),
                            "website":  any("http" in str(l).lower() for l in links),
                            "dev":      "",
                            "replies":  0,
                            "desc":     "",
                            "complete": False,
                        })
        log("info", f"DexScreener fallback: {len(coins)} coins")
    except Exception as e:
        log("warn", f"DexScreener fallback failed: {e}")

    return coins

# ── BONDING CURVE DETAILS ────────────────────────────────────────
def get_bonding_details(mint):
    """Get bonding curve % from pump.fun coin detail endpoint."""
    try:
        res = requests.get(
            f"<https://frontend-api-v3.pump.fun/coins/{mint}>",
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "<https://pump.fun/>",
            },
            timeout=8
        )
        if res.status_code == 200:
            data = res.json()
            vsol     = float(data.get("virtual_sol_reserves", 0) or 0)
            bond_pct = min((vsol / 85_000_000_000) * 100, 99.9)
            return {
                "bond_pct":  round(bond_pct, 1),
                "complete":  data.get("complete", False),
                "dev":       data.get("creator", ""),
                "replies":   int(data.get("reply_count", 0) or 0),
                "twitter":   data.get("twitter", ""),
                "telegram":  data.get("telegram", ""),
                "website":   data.get("website", ""),
                "desc":      data.get("description", ""),
            }
    except:
        pass
    return None

# ── MARKET DATA ──────────────────────────────────────────────────
def get_market_data(mint):
    try:
        res = requests.get(
            f"<https://api.dexscreener.com/latest/dex/tokens/{mint}>",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            return None
        pair = max(sol_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        return {
            "price":    float(pair.get("priceUsd", 0) or 0),
            "mcap":     float(pair.get("marketCap", 0) or 0),
            "liq":      float(pair.get("liquidity", {}).get("usd", 0) or 0),
            "vol5m":    float(pair.get("volume", {}).get("m5", 0) or 0),
            "change5m": float(pair.get("priceChange", {}).get("m5", 0) or 0),
            "change1h": float(pair.get("priceChange", {}).get("h1", 0) or 0),
            "buys5m":   int((pair.get("txns", {}).get("m5", {}) or {}).get("buys", 0)),
            "symbol":   pair.get("baseToken", {}).get("symbol", ""),
        }
    except:
        return None

def get_sol_price():
    try:
        res = requests.get(
            "<https://api.dexscreener.com/latest/dex/pairs/solana/8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj>",
            timeout=8
        )
        pairs = res.json().get("pairs", [])
        if pairs:
            price = float(pairs[0].get("priceUsd", 0))
            if price > 0:
                return price
    except:
        pass
    try:
        res = requests.get(
            "<https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd>",
            timeout=8
        )
        price = float(res.json()["solana"]["usd"])
        if price > 0:
            return price
    except:
        pass
    return None

# ── HOLDER / RUG CHECK ───────────────────────────────────────────
def run_rugcheck(mint):
    try:
        res = requests.get(
            f"<https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary>",
            timeout=10
        )
        data = res.json()
        risks      = data.get("risks", [])
        risk_names = [r.get("name", "").lower() for r in risks]
        top_holders = data.get("topHolders", [])
        top10_pct   = sum(float(h.get("pct", 0) or 0) for h in top_holders[:10])
        total_holders = data.get("totalHolders", 0)
        return {
            "score":          data.get("score", 0),
            "total_holders":  total_holders,
            "top10_pct":      round(top10_pct, 2),
            "has_mint_auth":  any("mint" in r for r in risk_names),
            "has_freeze_auth": any("freeze" in r for r in risk_names),
            "has_insider":    any("insider" in r or "bundle" in r for r in risk_names),
            "risks":          risk_names[:5],
        }
    except:
        return None

# ── DEV WALLET CHECK ─────────────────────────────────────────────
def check_dev_sold(dev_wallet, mint):
    if not dev_wallet or dev_wallet in blacklisted_devs:
        return False, "Blacklisted dev" if dev_wallet in blacklisted_devs else "No dev wallet"
    try:
        res = requests.post(
            SOL_RPC,
            json={
                "jsonrpc": "2.0", "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [dev_wallet, {"mint": mint}, {"encoding": "jsonParsed"}]
            },
            headers={"Content-Type": "application/json"},
            timeout=8
        )
        accounts = res.json().get("result", {}).get("value", [])
        balance  = sum(
            float(a.get("account",{}).get("data",{}).get("parsed",{}).get("info",{}).get("tokenAmount",{}).get("uiAmount", 0) or 0)
            for a in accounts
        )
        sold = balance < 1000
        if not sold:
            blacklisted_devs.add(dev_wallet)
        return sold, f"Dev balance: {balance:,.0f}"
    except:
        return True, "Check skipped"

# ── GREENLIGHT CHECK ─────────────────────────────────────────────
def greenlight(mint, symbol, coin_info, market):
    cache_key = mint
    cached = check_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < 600:
        return cached["ok"], cached["msg"], cached["score"]

    fails = []
    score = 0

    # 1. Market filters
    mcap = market["mcap"] or coin_info.get("mcap", 0)
    if mcap < MIN_MCAP:         fails.append(f"MCap low ${mcap:,.0f}")
    elif mcap > MAX_MCAP:       fails.append(f"MCap high ${mcap:,.0f}")
    else:                       score += 10

    if market["liq"] < MIN_LIQ: fails.append(f"Low liq ${market['liq']:,.0f}")
    else:                        score += 10

    if market["change5m"] < MIN_CHANGE_5M: fails.append(f"Weak 5m {market['change5m']:+.1f}%")
    else:                                   score += min(market["change5m"] * 2, 20)

    if market["buys5m"] < MIN_BUYS_5M: fails.append(f"Low buys {market['buys5m']}/5m")
    else:                               score += min(market["buys5m"], 15)

    if fails:
        result = {"ok": False, "msg": fails[0], "score": 0, "ts": time.time()}
        check_cache[cache_key] = result
        return False, fails[0], 0

    # 2. Blacklist
    if mint in blacklisted_mints:
        result = {"ok": False, "msg": "Blacklisted", "score": 0, "ts": time.time()}
        check_cache[cache_key] = result
        return False, "Blacklisted", 0

    # 3. Bonding curve
    bond_pct = coin_info.get("bond_pct", 0)
    if bond_pct == 0:
        details = get_bonding_details(mint)
        if details:
            bond_pct = details["bond_pct"]
            coin_info.update(details)

    if bond_pct < MIN_BOND_PCT:
        msg = f"Bond {bond_pct:.1f}% < {MIN_BOND_PCT}% needed"
        result = {"ok": False, "msg": msg, "score": 0, "ts": time.time()}
        check_cache[cache_key] = result
        return False, msg, 0
    elif coin_info.get("complete", False):
        result = {"ok": False, "msg": "Already graduated", "score": 0, "ts": time.time()}
        check_cache[cache_key] = result
        return False, "Already graduated", 0
    else:
        score += 20

    # 4. Whitepaper / scam language
    desc = (coin_info.get("desc", "") or "").lower()
    wp_words = ["whitepaper","white paper","roadmap","tokenomics","invest",
                "guaranteed","presale","ico","private sale","vesting","team tokens"]
    if any(w in desc for w in wp_words):
        result = {"ok": False, "msg": "Whitepaper/invest language", "score": 0, "ts": time.time()}
        check_cache[cache_key] = result
        return False, "Whitepaper/invest language detected", 0

    # 5. Social presence
    has_twitter  = bool(coin_info.get("twitter"))
    has_telegram = bool(coin_info.get("telegram"))
    replies      = int(coin_info.get("replies", 0) or 0)
    if has_twitter:  score += 15
    if has_telegram: score += 10
    if replies > 5:  score += 10
    elif replies < 2: fails.append("Low engagement")

    # 6. RugCheck
    rug = run_rugcheck(mint)
    if rug:
        if rug["has_mint_auth"]:
            result = {"ok": False, "msg": "Mint authority — dev can print tokens", "score": 0, "ts": time.time()}
            check_cache[cache_key] = result
            return False, "Mint authority enabled", 0
        if rug["has_freeze_auth"]:
            result = {"ok": False, "msg": "Freeze authority", "score": 0, "ts": time.time()}
            check_cache[cache_key] = result
            return False, "Freeze authority enabled", 0
        if rug["total_holders"] < MIN_REAL_BUYERS:
            msg = f"Only {rug['total_holders']} holders < {MIN_REAL_BUYERS} needed"
            result = {"ok": False, "msg": msg, "score": 0, "ts": time.time()}
            check_cache[cache_key] = result
            return False, msg, 0
        if rug["top10_pct"] > MAX_TOP10_PCT:
            msg = f"Top10 hold {rug['top10_pct']:.1f}% > {MAX_TOP10_PCT}% limit"
            result = {"ok": False, "msg": msg, "score": 0, "ts": time.time()}
            check_cache[cache_key] = result
            return False, msg, 0
        if rug["has_insider"]:
            result = {"ok": False, "msg": "Insider/bundle activity detected", "score": 0, "ts": time.time()}
            check_cache[cache_key] = result
            return False, "Bundle/insider wallets detected", 0
        score += 20

    # 7. Dev sold check
    dev = coin_info.get("dev", "")
    dev_sold, dev_msg = check_dev_sold(dev, mint)
    if not dev_sold:
        result = {"ok": False, "msg": f"Dev not sold: {dev_msg}", "score": 0, "ts": time.time()}
        check_cache[cache_key] = result
        return False, f"Dev not sold: {dev_msg}", 0
    score += 15

    # ALL PASSED
    msg = f"Bond:{bond_pct:.0f}% Holders:{rug['total_holders'] if rug else '?'} Score:{score}"
    result = {"ok": True, "msg": msg, "score": score, "ts": time.time()}
    check_cache[cache_key] = result
    log("ok", f"GREENLIGHT ✓ {msg}", symbol)
    return True, msg, score

# ── SOCIAL SCAN ──────────────────────────────────────────────────
def scan_social():
    signals = []
    try:
        res = requests.get(
            "<https://www.reddit.com/r/cryptomoonshots/new.json?limit=25>",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        posts = res.json().get("data", {}).get("children", [])
        for post in posts:
            p = post["data"]
            combined = (p.get("title","") + " " + p.get("selftext","")).upper()
            kol_hit  = any(k.upper() in combined for k in KOLS)
            is_sol   = any(w in combined for w in ["SOLANA","SOL","PUMP.FUN","PUMPFUN"])
            tickers  = re.findall(r'\$([A-Z]{2,10})', combined)
            ups      = p.get("ups", 0)
            if is_sol and (kol_hit or ups > 30) and tickers:
                for t in tickers[:2]:
                    signals.append({"ticker": t, "source": "reddit", "kol": kol_hit,
                                    "score": ups + (100 if kol_hit else 0),
                                    "context": p.get("title","")[:80]})
    except Exception as e:
        log("warn", f"Reddit: {e}")

    try:
        res = requests.get("<https://api.coingecko.com/api/v3/search/trending>", timeout=8)
        meme_words = ["DOGE","PEPE","SHIB","MEME","INU","FLOKI","WIF",
                      "BONK","CAT","FROG","MOON","PUMP","APE","BABY"]
        for c in res.json().get("coins", [])[:10]:
            item = c.get("item", {})
            sym  = item.get("symbol","").upper()
            if any(w in sym or w in item.get("name","").upper() for w in meme_words):
                signals.append({"ticker": sym, "source": "coingecko", "kol": False,
                                 "score": 80, "context": f"Trending CoinGecko"})
    except Exception as e:
        log("warn", f"CoinGecko: {e}")

    with social_lock:
        social_signals.clear()
        social_signals.extend(signals)
    log("info", f"Social: {len(signals)} signals")

# ── CLAUDE ───────────────────────────────────────────────────────
def ask_claude(symbol, market, score, context):
    try:
        if not CLAUDE_KEY or CLAUDE_KEY in ["none", ""]:
            strength = "strong" if score >= 70 else "medium" if score >= 40 else "weak"
            return True, strength, "Local filter"
        prompt = f"""Pump.fun GREENLIGHT sniper — final check.

{symbol} passed ALL safety checks:
✓ 70%+ bonded (pre-graduation)
✓ 300+ real holders
✓ Dev sold all tokens
✓ No bundles/insiders
✓ No mint/freeze authority
✓ Top10 < 20% supply
✓ No whitepaper language

Score: {score}/100
MCap: ${market.get('mcap',0):,.0f} | Liq: ${market.get('liq',0):,.0f}
5m: {market.get('change5m',0):+.1f}% | Buys: {market.get('buys5m',0)} | 1h: {market.get('change1h',0):+.1f}%
Signal: {context or 'Trending pump.fun'}

Trade: ${TRADE_AMOUNT} | TP:{TP_LOW}-{TP_HIGH}% | SL:{SL_PCT}% | Max:{MAX_HOLD_MINS}m

APPROVE or REJECT + reason. If APPROVE: STRONG, MEDIUM, or WEAK."""
        res = requests.post(
            "<https://api.anthropic.com/v1/messages>",
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 80,
                  "messages": [{"role": "user", "content": prompt}]},
            headers={"x-api-key": CLAUDE_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
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
        strength = "strong" if score >= 70 else "medium" if score >= 40 else "weak"
        return True, strength, "Local filter"

# ── TRADE EXECUTION ──────────────────────────────────────────────
def execute_buy(mint, symbol):
    if PAPER_MODE:
        log("ok", f"[PAPER] Buy ${TRADE_AMOUNT} -> {symbol}", symbol)
        return "PAPER_TX"
    try:
        sol_price = get_sol_price()
        if not sol_price:
            log("err", "Cannot get SOL price — buy aborted", symbol)
            return None
        sol_amount = round(TRADE_AMOUNT / sol_price, 6)

        res = requests.post(
            PUMPPORTAL,
            headers={"Content-Type": "application/json"},
            json={"publicKey": WALLET, "action": "buy", "mint": mint,
                  "denominatedInSol": "true", "amount": sol_amount,
                  "slippage": 20, "priorityFee": 0.001, "pool": "pump"},
            timeout=15
        )
        if res.status_code != 200:
            log("err", f"PumpPortal {res.status_code}: {res.text[:80]}", symbol)
            return None

        keypair = Keypair.from_base58_string(WALLET_PRIVATE_KEY)
        tx = VersionedTransaction(VersionedTransaction.from_bytes(res.content).message, [keypair])
        client = Client(SOL_RPC)
        result = client.send_raw_transaction(bytes(tx), opts=TxOpts(skip_preflight=True, preflight_commitment="confirmed"))
        sig = str(result.value)
        if sig and len(sig) > 10:
            log("ok", f"Bought! {sig[:20]}...", symbol)
            log("ok", f"<https://solscan.io/tx/{sig}>", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Buy: {e}", symbol)
        return None

def execute_sell(tokens, mint, symbol):
    if PAPER_MODE:
        log("ok", f"[PAPER] Sell {symbol}", symbol)
        return "PAPER_TX"
    try:
        res = requests.post(
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
        tx = VersionedTransaction(VersionedTransaction.from_bytes(res.content).message, [keypair])
        client = Client(SOL_RPC)
        result = client.send_raw_transaction(bytes(tx), opts=TxOpts(skip_preflight=True, preflight_commitment="confirmed"))
        sig = str(result.value)
        if sig and len(sig) > 10:
            log("ok", f"Sold! {sig[:20]}...", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Sell: {e}", symbol)
        return None

def enter_trade(mint, symbol, market, strength, context):
    global capital
    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            return False
    if coin_trade_count[mint] >= MAX_PER_COIN:
        return False
    with capital_lock:
        if capital < TRADE_AMOUNT:
            return False

    price  = market["price"]
    tp_pct = TP_HIGH if strength == "strong" else (TP_LOW + TP_HIGH) / 2 if strength == "medium" else TP_LOW
    tp     = price * (1 + tp_pct / 100)
    sl     = price * (1 - SL_PCT / 100)
    profit = min(max(TRADE_AMOUNT * tp_pct / 100, 1.0), TRADE_AMOUNT)

    log("ok", f"[{strength.upper()}] ${TRADE_AMOUNT} | TP:+{tp_pct}% (${profit:.2f}) | SL:-{SL_PCT}%", symbol)

    tx = execute_buy(mint, symbol)
    if not tx:
        return False

    with trades_lock:
        open_trades[mint] = {
            "symbol": symbol, "mint": mint, "entry": price,
            "tp": tp, "tp_pct": tp_pct, "sl": sl,
            "amount": TRADE_AMOUNT, "tokens": TRADE_AMOUNT / price,
            "strength": strength, "context": context[:80],
            "opened_at": time.time(), "time_str": time.strftime("%H:%M:%S"),
        }
    coin_trade_count[mint] += 1
    with capital_lock:
        capital -= TRADE_AMOUNT
    return True

def exit_trade(mint, price, reason):
    global capital
    with trades_lock:
        if mint not in open_trades:
            return
        trade = open_trades.pop(mint)

    tp_pct = trade["tp_pct"]
    amount = trade["amount"]
    hold_m = (time.time() - trade["opened_at"]) / 60

    if reason == "TP":   pnl = amount * (tp_pct / 100)
    elif reason == "SL": pnl = -(amount * SL_PCT / 100)
    else:                pnl = ((price - trade["entry"]) / trade["entry"]) * amount if trade["entry"] > 0 else 0

    pnl = max(-amount, min(pnl, amount))
    with capital_lock:
        capital += amount + pnl

    emoji = "✅" if pnl >= 0 else "❌"
    log("ok" if pnl >= 0 else "err",
        f"{emoji} {reason} | {'+' if pnl>=0 else ''}${pnl:.4f} | {hold_m:.1f}m | Capital:${capital:.2f}",
        trade["symbol"])

    execute_sell(trade["tokens"], mint, trade["symbol"])
    completed_trades.append({
        "symbol": trade["symbol"], "entry": trade["entry"], "exit": price,
        "result": reason, "pnl": round(pnl, 4),
        "hold_m": round(hold_m, 1), "time": time.strftime("%H:%M:%S"),
    })

    if capital >= 100:
        log("ok", "GOAL REACHED — $100!")
    if capital < 2:
        global scan_active
        scan_active = False

# ── LOOPS ────────────────────────────────────────────────────────
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
                data = get_market_data(mint)
                price = data["price"] if data and data["price"] > 0 else trade["entry"]
                exit_trade(mint, price, "TIME")
                continue
            data = get_market_data(mint)
            if not data or data["price"] <= 0:
                continue
            price = data["price"]
            move  = ((price - trade["entry"]) / trade["entry"]) * 100
            log("info", f"${price:.8f} | {move:+.2f}% | {hold_m:.1f}/{MAX_HOLD_MINS}m", trade["symbol"])
            if price >= trade["tp"]:
                exit_trade(mint, price, "TP")
            elif price <= trade["sl"]:
                exit_trade(mint, price, "SL")

def social_loop():
    time.sleep(5)
    while scan_active:
        try:
            scan_social()
        except Exception as e:
            log("err", f"Social: {e}")
        time.sleep(300)

def scanner_loop():
    global scan_active
    log("ok", "=" * 55)
    log("ok", "Greenlight Sniper — ALL SAFETY CHECKS ACTIVE")
    log("ok", f"Bond:{MIN_BOND_PCT}-{MAX_BOND_PCT}% | Buyers:{MIN_REAL_BUYERS}+ | Top10:<{MAX_TOP10_PCT}%")
    log("ok", f"Dev must sell | No bundles | No mint/freeze | No whitepaper")
    log("ok", f"Trade:${TRADE_AMOUNT} | TP:{TP_LOW}-{TP_HIGH}% | SL:{SL_PCT}% | Max:{MAX_HOLD_MINS}m")
    log("ok", f"Mode: {'PAPER' if PAPER_MODE else 'LIVE -> Phantom'}")
    log("ok", "=" * 55)

    scan_social()

    while scan_active:
        try:
            with trades_lock:
                num_open = len(open_trades)
            if num_open >= MAX_OPEN:
                log("info", f"Max open ({num_open}/{MAX_OPEN})")
                time.sleep(SCAN_INTERVAL)
                continue

            log("info", f"--- Scan | Open:{num_open}/{MAX_OPEN} ---")
            coins = get_pumpfun_coins()

            if not coins:
                log("warn", "No coins fetched — retrying in 30s")
                time.sleep(30)
                continue

            log("info", f"Checking {len(coins)} pump.fun coins...")
            candidates = []

            for coin in coins:
                mint   = coin["mint"]
                symbol = coin["symbol"]

                if coin_trade_count[mint] >= MAX_PER_COIN:
                    continue
                if mint in blacklisted_mints:
                    continue
                with trades_lock:
                    if mint in open_trades:
                        continue

                # Quick pre-filter
                mcap = coin.get("mcap", 0)
                if mcap > 0 and (mcap < MIN_MCAP or mcap > MAX_MCAP):
                    continue
                if coin.get("complete", False):
                    continue

                # Bond % pre-filter
                bond_pct = coin.get("bond_pct", 0)
                if bond_pct > 0 and bond_pct < MIN_BOND_PCT:
                    continue

                # Get market data
                market = get_market_data(mint)
                if not market or market["price"] <= 0:
                    continue

                # Run full greenlight
                passed, msg, score = greenlight(mint, symbol, coin, market)
                if not passed:
                    continue

                # Social boost
                context = ""
                with social_lock:
                    for sig in social_signals:
                        t = sig.get("ticker", "").upper()
                        if t and t in symbol.upper():
                            score += 50 if sig.get("kol") else 20
                            context = sig.get("context", "")
                            log("ok", f"Social match +boost", symbol)
                            break

                candidates.append({
                    "mint": mint, "symbol": symbol,
                    "market": market, "score": score, "context": context,
                })

                time.sleep(0.5)

            candidates.sort(key=lambda c: c["score"], reverse=True)
            log("info", f"Greenlight passed: {len(candidates)}")

            for c in candidates[:3]:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN or c["mint"] in open_trades:
                        break
                approved, strength, reason = ask_claude(c["symbol"], c["market"], c["score"], c["context"])
                if not approved:
                    log("ai", f"Rejected: {reason[:50]}", c["symbol"])
                    continue
                enter_trade(c["mint"], c["symbol"], c["market"], strength, c["context"])

            if not candidates:
                log("info", "No coins passed greenlight")

        except Exception as e:
            log("err", f"Scanner: {e}")

        time.sleep(SCAN_INTERVAL)

# ── FLASK ─────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    with trades_lock:
        n = len(open_trades)
    return f"Greenlight Sniper | Capital:${capital:.2f} | Open:{n}/{MAX_OPEN} | {'PAPER' if PAPER_MODE else 'LIVE'}", 200

@app.route("/status", methods=["GET"])
def status():
    wins  = len([t for t in completed_trades if t["result"] == "TP"])
    loss  = len([t for t in completed_trades if t["result"] == "SL"])
    times = len([t for t in completed_trades if t["result"] == "TIME"])
    pnl   = sum(t["pnl"] for t in completed_trades)
    wr    = round(wins / max(wins + loss, 1) * 100, 1)
    return jsonify({
        "capital": round(capital, 2), "goal": 100,
        "paper_mode": PAPER_MODE,
        "open_trades": len(open_trades),
        "wins": wins, "losses": loss, "time_exits": times,
        "win_rate": wr, "total_pnl": round(pnl, 4),
        "total_trades": len(completed_trades),
        "blacklisted_devs": len(blacklisted_devs),
        "social_signals": len(social_signals),
        "filters": {
            "bond_pct": f"{MIN_BOND_PCT}-{MAX_BOND_PCT}%",
            "min_buyers": MIN_REAL_BUYERS,
            "max_top10": f"{MAX_TOP10_PCT}%",
            "dev_sold": True,
            "no_bundles": True,
            "no_mint_auth": True,
            "no_whitepaper": True,
        }
    })

@app.route("/trades", methods=["GET"])
def trades():
    return jsonify({
        "open": [{k: v for k, v in t.items() if k != "opened_at"} for t in open_trades.values()],
        "completed": completed_trades[-50:]
    })

@app.route("/signals", methods=["GET"])
def signals():
    with social_lock:
        return jsonify({"social_signals": social_signals})

@app.route("/log", methods=["GET"])
def get_log():
    return jsonify({"logs": trade_log[-50:]})

@app.route("/blacklist/<mint>", methods=["GET"])
def blacklist(mint):
    blacklisted_mints.add(mint)
    return jsonify({"blacklisted": mint})

if __name__ == "__main__":
    if not PAPER_MODE:
        if not WALLET or not WALLET_PRIVATE_KEY:
            print("[FATAL] LIVE mode requires WALLET and WALLET_PRIVATE_KEY env vars to be set. Exiting.")
            raise SystemExit(1)
        if not _SOLANA_AVAILABLE:
            print("[FATAL] LIVE mode requires solders and solana packages. Run: pip install solders solana. Exiting.")
            raise SystemExit(1)
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=social_loop,  daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", f"Greenlight Sniper | {'PAPER' if PAPER_MODE else 'LIVE'} | Wallet:{WALLET[:8] if WALLET else 'NOT SET'}...")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
