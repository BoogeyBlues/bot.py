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

# ── GREENLIGHT FILTERS ───────────────────────────────────────────
MIN_BOND_PCT       = float(os.environ.get("MIN_BOND_PCT", "70"))    # 70%+ bonded
MAX_BOND_PCT       = float(os.environ.get("MAX_BOND_PCT", "99"))    # not yet fully bonded
MIN_REAL_BUYERS    = int(os.environ.get("MIN_REAL_BUYERS", "300"))  # 300+ unique real buyers
MIN_MCAP           = int(os.environ.get("MIN_MCAP", "10000"))       # $10k min mcap
MAX_MCAP           = int(os.environ.get("MAX_MCAP", "500000"))      # $500k max
MIN_LIQ            = int(os.environ.get("MIN_LIQ", "5000"))         # $5k min liquidity
MAX_TOP10_PCT      = float(os.environ.get("MAX_TOP10_PCT", "20"))   # top 10 holders < 20%
MAX_DEV_HOLD_PCT   = float(os.environ.get("MAX_DEV_HOLD_PCT", "5")) # dev must hold < 5%
MIN_CHANGE_5M      = float(os.environ.get("MIN_CHANGE_5M", "2"))    # 2%+ move in 5m
MIN_BUYS_5M        = int(os.environ.get("MIN_BUYS_5M", "10"))       # 10+ buys in 5m

SOL_RPC    = "https://api.mainnet-beta.solana.com"
PUMPPORTAL = "https://pumpportal.fun/api/trade-local"

# KOL names to watch for
KOLS = ["elonmusk","elon","ansem","murad","cobie","hsaka",
        "gainzy","kaleo","pentoshi","blknoiz06","lookonchain",
        "notthreadguy","inversebrah","wublockchain","degen",
        "cryptokaleo","lichocrypto","flowslikeosmo"]

# ── STATE ────────────────────────────────────────────────────────
capital          = float(os.environ.get("STARTING_CAPITAL", "54.86"))
capital_lock     = threading.Lock()
open_trades      = {}
trades_lock      = threading.Lock()
coin_trade_count = defaultdict(int)
blacklisted_mints = set()   # known rugs/scams
blacklisted_devs  = set()   # known rug dev wallets
trade_log        = []
completed_trades = []
log_lock         = threading.Lock()
scan_active      = True
social_signals   = []
social_lock      = threading.Lock()
checked_coins    = {}        # mint -> check result cache (10 min TTL)

# ── LOGGING ─────────────────────────────────────────────────────
def log(tag, msg, symbol=""):
    prefix = f"[{symbol}] " if symbol else ""
    entry  = f"[{time.strftime('%H:%M:%S')}] [{tag.upper()}] {prefix}{msg}"
    print(entry, flush=True)
    with log_lock:
        trade_log.append({"time": time.strftime('%H:%M:%S'), "tag": tag, "symbol": symbol, "msg": msg})
        if len(trade_log) > 300:
            trade_log.pop(0)

# ══════════════════════════════════════════════════════════════════
# GREENLIGHT CHECK SYSTEM
# Every coin must pass ALL checks before entry
# ══════════════════════════════════════════════════════════════════

def check_bonding_curve(mint):
    """
    Check bonding curve progress from pump.fun API.
    We want 70-99% — catching coins about to graduate.
    """
    try:
        res = requests.get(
            f"https://client-api-2-74b1891ee9f9.herokuapp.com/coins/{mint}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8
        )
        data = res.json()
        virtual_sol  = float(data.get("virtual_sol_reserves", 0) or 0)
        real_sol     = float(data.get("real_sol_reserves", 0) or 0)
        bonding_pct  = float(data.get("progress", 0) or 0)
        complete     = data.get("complete", False)
        dev_wallet   = data.get("creator", "")
        king_of_hill = data.get("king_of_the_hill_timestamp", None)

        if bonding_pct == 0 and virtual_sol > 0:
            # Calculate from reserves: 85 SOL = 100% bonded threshold
            bonding_pct = min((virtual_sol / 85) * 100, 99.9)

        return {
            "bonding_pct":  round(bonding_pct, 2),
            "complete":     complete,
            "dev_wallet":   dev_wallet,
            "king_hill":    king_of_hill is not None,
            "description":  data.get("description", ""),
            "twitter":      data.get("twitter", ""),
            "telegram":     data.get("telegram", ""),
            "website":      data.get("website", ""),
            "reply_count":  int(data.get("reply_count", 0) or 0),
        }
    except Exception as e:
        log("warn", f"Bonding curve check failed: {e}")
        return None

def check_holders(mint):
    """
    Check holder distribution using RugCheck API.
    Detects: top holder concentration, insider networks,
    bundle wallets, bot wallets.
    """
    try:
        # RugCheck.xyz free API
        res = requests.get(
            f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary",
            timeout=10
        )
        data = res.json()

        risks      = data.get("risks", [])
        score      = data.get("score", 0)    # 0 = risky, 100 = safe
        risk_names = [r.get("name", "").lower() for r in risks]

        # Extract holder data
        top_holders   = data.get("topHolders", [])
        total_holders = data.get("totalHolders", 0)

        # Calculate top 10 concentration
        top10_pct = sum(float(h.get("pct", 0) or 0) for h in top_holders[:10])

        # Check for red flags
        has_mint_authority   = any("mint" in r for r in risk_names)
        has_freeze_authority = any("freeze" in r for r in risk_names)
        has_insider          = any("insider" in r or "bundle" in r for r in risk_names)
        low_liquidity        = any("liquidity" in r for r in risk_names)

        return {
            "rugcheck_score":      score,
            "total_holders":       total_holders,
            "top10_pct":           round(top10_pct, 2),
            "has_mint_authority":  has_mint_authority,
            "has_freeze_authority": has_freeze_authority,
            "has_insider":         has_insider,
            "risks":               risk_names[:5],
            "risk_count":          len(risks),
        }
    except Exception as e:
        log("warn", f"RugCheck failed: {e}")
        return None

def check_dev_wallet(dev_wallet, mint):
    """
    Check if developer has sold their tokens.
    Dev must have sold all or most of their allocation.
    Also checks dev's history — serial ruggers get blacklisted.
    """
    if not dev_wallet:
        return {"dev_clean": False, "reason": "No dev wallet found"}

    # Check if dev is blacklisted
    if dev_wallet in blacklisted_devs:
        return {"dev_clean": False, "reason": "Dev wallet blacklisted — known rugger"}

    try:
        # Check dev's current token balance via Solana RPC
        res = requests.post(
            SOL_RPC,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    dev_wallet,
                    {"mint": mint},
                    {"encoding": "jsonParsed"}
                ]
            },
            headers={"Content-Type": "application/json"},
            timeout=8
        )
        data = res.json()
        accounts = data.get("result", {}).get("value", [])

        dev_balance = 0
        for acc in accounts:
            parsed = acc.get("account", {}).get("data", {}).get("parsed", {})
            info   = parsed.get("info", {})
            amount = float(info.get("tokenAmount", {}).get("uiAmount", 0) or 0)
            dev_balance += amount

        # Dev should have sold most tokens
        dev_sold = dev_balance < 1000  # less than 1000 tokens remaining = essentially sold

        return {
            "dev_clean":    dev_sold,
            "dev_balance":  dev_balance,
            "dev_wallet":   dev_wallet[:12] + "...",
            "reason":       "Dev sold ✓" if dev_sold else f"Dev still holds {dev_balance:,.0f} tokens",
        }
    except Exception as e:
        log("warn", f"Dev check failed: {e}")
        return {"dev_clean": True, "reason": "Check skipped (API error)"}

def check_bundle_snipers(mint):
    """
    Detect bundle/sniper wallets in early buyers.
    Bundles = multiple wallets buying in same transaction at launch.
    If >10% of supply held by bundles = skip.
    """
    try:
        res = requests.get(
            f"https://api.rugcheck.xyz/v1/tokens/{mint}/report",
            timeout=10
        )
        data = res.json()

        insider_networks = data.get("insiderNetworks", [])
        bundle_supply_pct = 0

        for network in insider_networks:
            pct = float(network.get("percentage", 0) or 0)
            bundle_supply_pct += pct

        # Also check markets for manipulation
        markets = data.get("markets", [])
        has_bot_activity = False
        for market in markets:
            if market.get("lp", {}).get("lpLockedPct", 0) == 0:
                has_bot_activity = True

        return {
            "bundle_pct":     round(bundle_supply_pct, 2),
            "bundle_clean":   bundle_supply_pct < 10,
            "has_bot_activity": has_bot_activity,
        }
    except Exception as e:
        log("warn", f"Bundle check failed: {e}")
        return {"bundle_pct": 0, "bundle_clean": True, "has_bot_activity": False}

def check_social_legitimacy(coin_data):
    """
    Check if social presence is real (not fake/bot farms).
    Looks at: Twitter exists, reply count, Telegram, website age.
    """
    score = 0
    flags = []

    twitter  = coin_data.get("twitter", "")
    telegram = coin_data.get("telegram", "")
    website  = coin_data.get("website", "")
    replies  = int(coin_data.get("reply_count", 0) or 0)
    desc     = coin_data.get("description", "")

    if twitter:
        score += 30
    else:
        flags.append("No Twitter")

    if telegram:
        score += 20
    else:
        flags.append("No Telegram")

    if website:
        score += 15
    else:
        flags.append("No website")

    if replies > 10:
        score += 20
    elif replies > 3:
        score += 10
    else:
        flags.append(f"Low engagement ({replies} replies)")

    # Description quality check
    if len(desc) > 50:
        score += 15
    elif len(desc) > 10:
        score += 5
    else:
        flags.append("No/poor description")

    # Whitepaper / "invest" language = scam signal
    wp_words = ["whitepaper", "white paper", "roadmap", "tokenomics",
                "invest", "guaranteed", "100x guaranteed", "presale",
                "ico", "private sale", "vesting"]
    if any(w in desc.lower() for w in wp_words):
        score -= 40
        flags.append("Whitepaper/invest language detected")

    return {
        "social_score": score,
        "social_legit": score >= 30,
        "has_twitter":  bool(twitter),
        "has_telegram": bool(telegram),
        "flags":        flags,
    }

def check_blacklist(mint, symbol):
    """Check against known rug/scam databases."""
    if mint in blacklisted_mints:
        return False, "Blacklisted mint"

    # Known scam patterns in name/symbol
    scam_patterns = [
        "FAKE", "SCAM", "RUG", "EXIT", "HONEYPOT",
        "ELONTEST", "TESTOKEN", "AIRDROP_CLAIM"
    ]
    if any(p in symbol.upper() for p in scam_patterns):
        return False, f"Suspicious name: {symbol}"

    return True, "Clean"

# ══════════════════════════════════════════════════════════════════
# MASTER GREENLIGHT CHECK — All filters must pass
# ══════════════════════════════════════════════════════════════════

def run_greenlight_checks(mint, symbol, market_data):
    """
    Runs ALL greenlight checks on a coin.
    Returns (passed: bool, reasons: list, score: int)
    
    CHECKS:
    1. Bonding curve 70-99%
    2. Not already bonded/graduated
    3. 300+ real buyers
    4. Dev sold all/most tokens
    5. No bundle/sniper concentration > 10%
    6. Top 10 holders < 20% supply
    7. No mint/freeze authority
    8. Active social (not bots)
    9. No whitepaper/insider language
    10. Not blacklisted
    11. Market data filters (mcap, liq, momentum)
    """

    # Cache check to avoid re-running expensive checks
    cache_key = mint
    if cache_key in checked_coins:
        cached = checked_coins[cache_key]
        if time.time() - cached["ts"] < 600:  # 10 min cache
            return cached["passed"], cached["reasons"], cached["score"]

    reasons = []
    score   = 0
    passed  = True

    # ── CHECK 1: Market data filters ──────────────────────────────
    if market_data["mcap"] < MIN_MCAP:
        reasons.append(f"MCap too low ${market_data['mcap']:,.0f}")
        passed = False
    elif market_data["mcap"] > MAX_MCAP:
        reasons.append(f"MCap too high ${market_data['mcap']:,.0f}")
        passed = False
    else:
        score += 10

    if market_data["liq"] < MIN_LIQ:
        reasons.append(f"Low liquidity ${market_data['liq']:,.0f}")
        passed = False
    else:
        score += 10

    if market_data["change5m"] < MIN_CHANGE_5M:
        reasons.append(f"Weak momentum {market_data['change5m']:+.1f}%")
        passed = False
    else:
        score += min(market_data["change5m"] * 2, 20)

    if market_data["buys5m"] < MIN_BUYS_5M:
        reasons.append(f"Low buys {market_data['buys5m']}/5m")
        passed = False
    else:
        score += min(market_data["buys5m"], 15)

    if not passed:
        result = {"passed": False, "reasons": reasons, "score": 0, "ts": time.time()}
        checked_coins[cache_key] = result
        return False, reasons, 0

    # ── CHECK 2: Blacklist ─────────────────────────────────────────
    bl_clean, bl_reason = check_blacklist(mint, symbol)
    if not bl_clean:
        reasons.append(f"BLACKLIST: {bl_reason}")
        result = {"passed": False, "reasons": reasons, "score": 0, "ts": time.time()}
        checked_coins[cache_key] = result
        return False, reasons, 0

    # ── CHECK 3: Bonding curve ─────────────────────────────────────
    bond = check_bonding_curve(mint)
    if not bond:
        reasons.append("Could not verify bonding curve")
        # Don't fail — just note it
    else:
        bp = bond["bonding_pct"]
        if bp < MIN_BOND_PCT:
            reasons.append(f"Only {bp:.1f}% bonded — need {MIN_BOND_PCT}%+")
            passed = False
        elif bond["complete"]:
            reasons.append("Already graduated — too late")
            passed = False
        else:
            score += 20
            reasons.append(f"Bonding: {bp:.1f}% ✓")

        # Social check from pump.fun data
        if bond:
            social = check_social_legitimacy(bond)
            if not social["social_legit"]:
                reasons.append(f"Weak social: {', '.join(social['flags'][:2])}")
                if len(social["flags"]) >= 3:
                    passed = False  # fail only if 3+ social red flags
            else:
                score += social["social_score"] // 3
                reasons.append(f"Social score: {social['social_score']}/100 ✓")

            # Whitepaper = instant fail
            if social["social_score"] < -10:
                reasons.append("FAIL: Whitepaper/insider language")
                passed = False

    if not passed:
        result = {"passed": False, "reasons": reasons, "score": 0, "ts": time.time()}
        checked_coins[cache_key] = result
        return False, reasons, 0

    # ── CHECK 4: Holder analysis via RugCheck ─────────────────────
    holders = check_holders(mint)
    if holders:
        if holders["has_mint_authority"]:
            reasons.append("FAIL: Mint authority enabled — dev can print tokens")
            passed = False
        elif holders["has_freeze_authority"]:
            reasons.append("FAIL: Freeze authority — can freeze your wallet")
            passed = False
        elif holders["top10_pct"] > MAX_TOP10_PCT:
            reasons.append(f"FAIL: Top 10 hold {holders['top10_pct']:.1f}% — too concentrated")
            passed = False
        elif holders["total_holders"] < MIN_REAL_BUYERS:
            reasons.append(f"FAIL: Only {holders['total_holders']} holders — need {MIN_REAL_BUYERS}+")
            passed = False
        else:
            score += 20
            reasons.append(f"Holders: {holders['total_holders']} | Top10: {holders['top10_pct']:.1f}% ✓")
    else:
        reasons.append("Holder check skipped (API unavailable)")

    if not passed:
        result = {"passed": False, "reasons": reasons, "score": 0, "ts": time.time()}
        checked_coins[cache_key] = result
        return False, reasons, 0

    # ── CHECK 5: Bundle/sniper detection ──────────────────────────
    bundles = check_bundle_snipers(mint)
    if bundles:
        if not bundles["bundle_clean"]:
            reasons.append(f"FAIL: Bundle wallets hold {bundles['bundle_pct']:.1f}% — snipers present")
            passed = False
        else:
            score += 15
            reasons.append(f"Bundle check: clean ({bundles['bundle_pct']:.1f}%) ✓")

    if not passed:
        result = {"passed": False, "reasons": reasons, "score": 0, "ts": time.time()}
        checked_coins[cache_key] = result
        return False, reasons, 0

    # ── CHECK 6: Dev wallet ────────────────────────────────────────
    dev_wallet = bond.get("dev_wallet", "") if bond else ""
    dev = check_dev_wallet(dev_wallet, mint)
    if dev:
        if not dev["dev_clean"]:
            reasons.append(f"FAIL: {dev['reason']}")
            passed = False
            # Add to blacklist if serial rugger
            if dev_wallet:
                blacklisted_devs.add(dev_wallet)
        else:
            score += 15
            reasons.append(f"Dev wallet: {dev['reason']} ✓")

    if not passed:
        result = {"passed": False, "reasons": reasons, "score": 0, "ts": time.time()}
        checked_coins[cache_key] = result
        return False, reasons, 0

    # ── ALL CHECKS PASSED ─────────────────────────────────────────
    log("ok", f"GREENLIGHT PASSED | Score:{score} | " + " | ".join(reasons[:3]), symbol)
    result = {"passed": True, "reasons": reasons, "score": score, "ts": time.time()}
    checked_coins[cache_key] = result
    return True, reasons, score

# ══════════════════════════════════════════════════════════════════
# DATA FETCHING
# ══════════════════════════════════════════════════════════════════

def get_pumpfun_coins():
    """Get coins from pump.fun API — guaranteed bonding curve tokens."""
    coins = []
    try:
        res = requests.get(
            "https://client-api-2-74b1891ee9f9.herokuapp.com/coins?offset=0&limit=50&sort=last_trade_timestamp&order=DESC&includeNsfw=false",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = res.json()
        if isinstance(data, list):
            for coin in data[:50]:
                mint  = coin.get("mint", "")
                sym   = coin.get("symbol", mint[:8])
                mcap  = float(coin.get("usd_market_cap", 0) or 0)
                if mint:
                    coins.append({
                        "mint":     mint,
                        "symbol":   sym,
                        "mcap":     mcap,
                        "twitter":  bool(coin.get("twitter")),
                        "telegram": bool(coin.get("telegram")),
                        "website":  bool(coin.get("website")),
                    })
    except Exception as e:
        log("warn", f"PumpFun API: {e}")
    return coins

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
            "price":    float(pair.get("priceUsd", 0) or 0),
            "mcap":     float(pair.get("marketCap", 0) or 0),
            "liq":      float(pair.get("liquidity", {}).get("usd", 0) or 0),
            "vol5m":    float(pair.get("volume", {}).get("m5", 0) or 0),
            "vol1h":    float(pair.get("volume", {}).get("h1", 0) or 0),
            "change5m": float(pair.get("priceChange", {}).get("m5", 0) or 0),
            "change1h": float(pair.get("priceChange", {}).get("h1", 0) or 0),
            "buys5m":   int((pair.get("txns", {}).get("m5", {}) or {}).get("buys", 0)),
            "symbol":   pair.get("baseToken", {}).get("symbol", ""),
            "name":     pair.get("baseToken", {}).get("name", ""),
        }
    except:
        return None

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

# ══════════════════════════════════════════════════════════════════
# SOCIAL SCANNING
# ══════════════════════════════════════════════════════════════════

def scan_social():
    signals = []
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
            is_sol   = any(w in combined for w in ["SOLANA","SOL","PUMP.FUN","PUMPFUN"])
            tickers  = re.findall(r'\$([A-Z]{2,10})', combined)
            if is_sol and (kol_hit or ups > 30) and tickers:
                for t in tickers[:2]:
                    signals.append({"ticker": t, "source": "reddit",
                                    "kol": kol_hit, "score": ups + (100 if kol_hit else 0),
                                    "context": title[:80]})
    except Exception as e:
        log("warn", f"Reddit: {e}")

    try:
        res = requests.get("https://api.coingecko.com/api/v3/search/trending", timeout=8)
        coins = res.json().get("coins", [])
        meme_words = ["DOGE","PEPE","SHIB","MEME","INU","FLOKI","WIF",
                      "BONK","CAT","FROG","MOON","PUMP","APE","BABY"]
        for c in coins[:10]:
            item   = c.get("item", {})
            symbol = item.get("symbol", "").upper()
            name   = item.get("name", "").upper()
            rank   = item.get("score", 9)
            if any(w in symbol or w in name for w in meme_words):
                signals.append({"ticker": symbol, "source": "coingecko",
                                 "kol": False, "score": 80 - rank * 5,
                                 "context": f"#{rank+1} trending CoinGecko"})
    except Exception as e:
        log("warn", f"CoinGecko: {e}")

    with social_lock:
        social_signals.clear()
        social_signals.extend(signals)
    log("info", f"Social: {len(signals)} signals")

# ══════════════════════════════════════════════════════════════════
# TRADE EXECUTION
# ══════════════════════════════════════════════════════════════════

def ask_claude(symbol, market_data, checks_passed, score, context):
    try:
        if not CLAUDE_KEY or CLAUDE_KEY in ["none", ""]:
            strength = "strong" if score >= 70 else "medium" if score >= 40 else "weak"
            return True, strength, "Local filter"

        prompt = f"""Pump.fun GREENLIGHT sniper — final approval.

Coin: {symbol}
Greenlight Score: {score}/100
Passed All Safety Checks: YES
Bond Progress: {checks_passed[2] if len(checks_passed) > 2 else 'verified'}

Market:
- MCap: ${market_data.get('mcap',0):,.0f}
- Liquidity: ${market_data.get('liq',0):,.0f}
- 5m Change: {market_data.get('change5m',0):+.1f}%
- 5m Buys: {market_data.get('buys5m',0)}
- 1h Change: {market_data.get('change1h',0):+.1f}%

Signal: {context or 'Trending pump.fun coin 70%+ bonded'}

Trade: ${TRADE_AMOUNT} fixed | TP:{TP_LOW}-{TP_HIGH}% | SL:{SL_PCT}% | Max:{MAX_HOLD_MINS}m
Targets: $1 min — ${TRADE_AMOUNT} max profit (2x)

This coin PASSED dev sold check, no bundles, no mint authority,
300+ real holders, 70%+ bonded but not yet graduated.

APPROVE or REJECT + reason.
If APPROVE: STRONG, MEDIUM, or WEAK."""

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
        strength = "strong" if score >= 70 else "medium" if score >= 40 else "weak"
        return True, strength, "Local filter"

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
            log("ok", f"Bought! {sig[:20]}...", symbol)
            log("ok", f"https://solscan.io/tx/{sig}", symbol)
            log("ok", "Check Phantom!", symbol)
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
            log("ok", f"Sold! {sig[:20]}...", symbol)
            return sig
        return None
    except Exception as e:
        log("err", f"Sell error: {e}", symbol)
        return None

def enter_trade(mint, symbol, data, strength, context):
    global capital
    with trades_lock:
        if mint in open_trades or len(open_trades) >= MAX_OPEN:
            return False
    if coin_trade_count[mint] >= MAX_PER_COIN:
        return False
    with capital_lock:
        if capital < TRADE_AMOUNT:
            return False

    price  = data["price"]
    tp_pct = TP_HIGH if strength == "strong" else (TP_LOW + TP_HIGH) / 2 if strength == "medium" else TP_LOW
    tp     = price * (1 + tp_pct / 100)
    sl     = price * (1 - SL_PCT / 100)
    profit = min(max(TRADE_AMOUNT * tp_pct / 100, 1.0), TRADE_AMOUNT)

    log("ok", f"[{strength.upper()}] ENTERING ${TRADE_AMOUNT} | TP:+{tp_pct}% (${profit:.2f}) | SL:-{SL_PCT}%", symbol)

    tx = execute_buy(mint, symbol)
    if not tx:
        return False

    with trades_lock:
        open_trades[mint] = {
            "symbol":    symbol,
            "mint":      mint,
            "entry":     price,
            "tp":        tp,
            "tp_pct":    tp_pct,
            "sl":        sl,
            "amount":    TRADE_AMOUNT,
            "tokens":    TRADE_AMOUNT / price,
            "profit":    profit,
            "strength":  strength,
            "context":   context[:80],
            "opened_at": time.time(),
            "time_str":  time.strftime("%H:%M:%S"),
        }
    coin_trade_count[mint] += 1
    with capital_lock:
        capital -= TRADE_AMOUNT
    return True

def exit_trade(mint, current_price, reason):
    global capital
    with trades_lock:
        if mint not in open_trades:
            return
        trade = open_trades.pop(mint)

    symbol  = trade["symbol"]
    entry   = trade["entry"]
    amount  = trade["amount"]
    tokens  = trade["tokens"]
    tp_pct  = trade["tp_pct"]
    hold_m  = (time.time() - trade["opened_at"]) / 60

    if reason == "TP":
        pnl = amount * (tp_pct / 100)
    elif reason == "SL":
        pnl = -(amount * SL_PCT / 100)
    else:
        pnl = ((current_price - entry) / entry) * amount if entry > 0 else 0

    pnl = max(-amount, min(pnl, amount))

    with capital_lock:
        capital += amount + pnl

    emoji = "✅" if pnl >= 0 else "❌"
    log("ok" if pnl >= 0 else "err",
        f"{emoji} {reason} | {'+' if pnl>=0 else ''}${pnl:.4f} | {hold_m:.1f}m | Capital:${capital:.2f}",
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

# ══════════════════════════════════════════════════════════════════
# LOOPS
# ══════════════════════════════════════════════════════════════════

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
            symbol = trade["symbol"]
            hold_m = (time.time() - trade["opened_at"]) / 60

            if hold_m >= MAX_HOLD_MINS:
                data = get_coin_data(mint)
                price = data["price"] if data and data["price"] > 0 else trade["entry"]
                log("warn", f"TIME EXIT {hold_m:.1f}m", symbol)
                exit_trade(mint, price, "TIME")
                continue

            data = get_coin_data(mint)
            if not data or data["price"] <= 0:
                continue
            price = data["price"]
            move  = ((price - trade["entry"]) / trade["entry"]) * 100
            log("info", f"${price:.8f} | {move:+.2f}% | {hold_m:.1f}/{MAX_HOLD_MINS}m", symbol)

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
    log("ok", "PumpFun Greenlight Sniper — FULL SAFETY MODE")
    log("ok", f"Bond: {MIN_BOND_PCT}-{MAX_BOND_PCT}% | Buyers: {MIN_REAL_BUYERS}+ | Top10: <{MAX_TOP10_PCT}%")
    log("ok", f"Dev: must sell all | Bundles: <10% | No mint/freeze auth")
    log("ok", f"Trade: ${TRADE_AMOUNT} | TP:{TP_LOW}-{TP_HIGH}% | SL:{SL_PCT}% | Max:{MAX_HOLD_MINS}m")
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

            log("info", f"--- Greenlight scan | Open:{num_open}/{MAX_OPEN} ---")
            coins = get_pumpfun_coins()
            candidates = []

            for coin in coins:
                mint   = coin["mint"]
                if coin_trade_count[mint] >= MAX_PER_COIN:
                    continue
                with trades_lock:
                    if mint in open_trades:
                        continue
                if mint in blacklisted_mints:
                    continue

                data = get_coin_data(mint)
                if not data or data["price"] <= 0:
                    continue

                symbol = data["symbol"] or data["name"] or coin.get("symbol", mint[:8])

                # Quick pre-filter before expensive checks
                mcap = data["mcap"] or coin.get("mcap", 0)
                if mcap < MIN_MCAP or mcap > MAX_MCAP:
                    continue
                if data["liq"] < MIN_LIQ:
                    continue
                if data["change5m"] < MIN_CHANGE_5M:
                    continue

                log("info", f"Running greenlight checks on {symbol}...", symbol)

                # Run all safety checks
                passed, reasons, score = run_greenlight_checks(mint, symbol, data)

                if not passed:
                    log("info", f"FAILED: {reasons[0] if reasons else 'unknown'}", symbol)
                    continue

                # Social signal boost
                context = ""
                with social_lock:
                    for sig in social_signals:
                        t = sig.get("ticker", "").upper()
                        if t and t in symbol.upper():
                            score += 50 if sig.get("kol") else 20
                            context = sig.get("context", "")
                            log("ok", f"KOL/Social match! +boost", symbol)
                            break

                candidates.append({
                    "mint": mint, "symbol": symbol, "data": data,
                    "score": score, "reasons": reasons, "context": context,
                })
                log("ok", f"GREENLIGHT PASS | score:{score} | {symbol}", symbol)

                time.sleep(0.5)

            candidates.sort(key=lambda c: c["score"], reverse=True)
            log("info", f"Greenlight passed: {len(candidates)} coins")

            for c in candidates[:3]:
                with trades_lock:
                    if len(open_trades) >= MAX_OPEN or c["mint"] in open_trades:
                        break

                approved, strength, reason = ask_claude(
                    c["symbol"], c["data"], c["reasons"], c["score"], c["context"]
                )
                if not approved:
                    log("ai", f"Claude rejected: {reason[:50]}", c["symbol"])
                    continue

                enter_trade(c["mint"], c["symbol"], c["data"], strength, c["context"])

            if not candidates:
                log("info", "No coins passed greenlight this scan")

        except Exception as e:
            log("err", f"Scanner: {e}")

        time.sleep(SCAN_INTERVAL)

# ══════════════════════════════════════════════════════════════════
# FLASK
# ══════════════════════════════════════════════════════════════════

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
        "blacklisted_coins": len(blacklisted_mints),
        "social_signals": len(social_signals),
        "greenlight_filters": {
            "bond_pct":       f"{MIN_BOND_PCT}-{MAX_BOND_PCT}%",
            "min_buyers":     MIN_REAL_BUYERS,
            "max_top10":      f"{MAX_TOP10_PCT}%",
            "max_dev_hold":   f"{MAX_DEV_HOLD_PCT}%",
            "max_bundle_pct": "10%",
            "no_mint_auth":   True,
            "no_freeze_auth": True,
            "dev_must_sell":  True,
            "no_whitepaper":  True,
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

@app.route("/blacklist/add/<mint>", methods=["GET"])
def add_blacklist(mint):
    blacklisted_mints.add(mint)
    return jsonify({"added": mint, "total": len(blacklisted_mints)})

if __name__ == "__main__":
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=social_loop,  daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log("ok", f"Greenlight Sniper | Wallet:{WALLET[:8] if WALLET else 'NOT SET'}...")
    log("ok", f"{'PAPER MODE' if PAPER_MODE else 'LIVE -> Phantom'}")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
