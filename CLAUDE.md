# Project Memory

## Repo — TWO BOTS, STRICT BRANCH SEPARATION
- **boogeyblues/bot.py** repo contains two completely separate bots
- **NEVER** commit `bot.py` changes to `claude/pumpfun-sniper-bot-dVuGg`
- **NEVER** commit `drift_bot.py` changes to `main`

## Two Bots in One Repo
| File | What it is | Branch | Deployed on |
|---|---|---|---|
| `bot.py` | PumpFun sniper (memecoin) | `main` | Railway (production) |
| `drift_bot.py` | Leverage/perps bot (Jupiter) | `claude/pumpfun-sniper-bot-dVuGg` | separate Railway service |

## Rules
- Any task touching `bot.py` → checkout `main`, commit, push `main`
- Any task touching `drift_bot.py` → checkout `claude/pumpfun-sniper-bot-dVuGg`, commit, push that branch
- Never mix files from both bots in the same commit
- Check `git branch --show-current` before every commit

## Completed This Session
- `DRIFT_EXCHANGE` default → `"jupiter"` (Drift Protocol is shut down)
- `home()` fully redesigned — slot-machine balance hero, sparkline position cards, 3D trade deck, goal progress bar, manual trading panel, live feed
- `monitor()` fully redesigned — animated SVG stick figures (miners for profit, walkers for loss), orbs, particle canvas, position cards, log feed

## Pending
- **Implement `/tmp/mockup_trades.html` into `trades_page()` in `drift_bot.py`**
  - Floating Excel spreadsheet with Mac-style titlebar (`TRADES_LOG.xlsx`), formula bar, column letters A-I
  - Per-row stick figure analysts: green arms-up for wins, red head-drooping-still-typing for losses
  - Big dual-monitor analyst in hero header with animated typing arm + coffee steam
  - Activity ticker, stats bar, particle canvas
- 6 live-mode bugs in `bot.py` — plan at `/root/.claude/plans/import-os-time-threading-quiet-summit.md`

## Design Rules
- **Always mock up webpages before implementing** into drift_bot.py
- User must approve mockup before implementation
- Mockup files live in `/tmp/mockup_*.html`

## Mockup Files
- `/tmp/mockup_landing.html` — home() (already implemented)
- `/tmp/mockup_monitor.html` — monitor() (already implemented)
- `/tmp/mockup_trades.html` — trades_page() (PENDING implementation)

## Key Config (drift_bot.py line 1–40)
- `DRIFT_EXCHANGE` default = `"jupiter"`
- `DRIFT_PAPER_MODE` default = `true`
- `STARTING_CAPITAL` env = `DRIFT_STARTING_CAPITAL`
- `MILESTONES = [250, 500, 1000, 2500, 5000, 10000, 25000]`
- API endpoints: `/status/api`, `/trades/api`, `/notify/api`

## Design Language
- Dark bg `#050a14`, cyan `#00e5ff`, green `#00ff88`, red `#ff3355`, yellow `#ffee00`
- Fonts: Bebas Neue (headings), Inter, JetBrains Mono
- Fixed nav (50-52px), orb divs (blur 80px), particles canvas
- Mobile-first 430px max-width
- SVG stick figures use native SMIL `animateTransform` (NOT CSS @keyframes on SVG)
- f-string templates: `{{` and `}}` for literal CSS/JS braces

---

# bot.py — Strategy & Ops Memory (read this before touching strategy code)

**Ground truth, always:** don't trust conversation memory of "what the strategy is doing" —
pull it live. `GET /status/api` (capital, career_pnl/career_win_rate = TRUE all-time,
never resettable; pools = which feeds are actually populated) and `GET /trades/archive`
(the permanent, never-wiped trade ledger, capped at 5000). Verify claims against these
before making changes, and after every deploy.

## The one fact that matters most
**`dsc_signal` is ~94% of every trade the bot has ever made.** The other 6 designed
strategies (`birdeye`, `dsc_organic`, `gmgn_signal`, `bond`, `spike`, `trench`, `copy`,
`fast`) are effectively dead — their pools read 0 in `/status/api`. Root causes:
- `birdeye`/`dsc_organic`/`gmgn_signal` pools empty → `BIRDEYE_API_KEY` was never set in
  Railway, and GMGN's signal endpoints (gmgn.ai) return nothing usable (likely scraping
  protection — this was never conclusively fixed, just worked around)
- `bond`/`trench` starved by `MIN_SIGNAL_SCORE` requiring GMGN/DSC signal points that
  don't exist without those feeds — this is a recurring bug: it was found and fixed once
  (commit 992319f, Aug 8), then the Aug-8 strategy *revert* re-broke it by restoring
  `MIN_SIGNAL_SCORE=2`, then fixed again. **If bond/trench show 0 trades again, check
  `MIN_SIGNAL_SCORE` first before assuming anything else is wrong.**

## Adaptive learning (built to address "the bot has no personality / doesn't learn")
Before this, `auto_tune()` only adjusted `bond`/`spike` parameters — zero learning logic
touched `dsc_signal`, the strategy actually running. Added:
- Every `dsc_signal` entry now tags *which* DSC sub-signal triggered it (`signal_tag`:
  BOOST/ADS/META/TOP/TAKEOVER), persisted on the trade record
- `dsc_signal_type_stats()` / `dsc_signal_type_avoid()`: computed from the permanent
  archive, no bias until a sub-type hits `DSC_LEARN_MIN_SAMPLE` (15) trades, then any
  sub-type below `DSC_LEARN_AVOID_WR` (30%) win rate gets skipped going forward — a real
  trial-and-error loop grounded in the bot's own outcomes, not a hand-tuned guess
- Check what it's currently avoiding via `/status/api` → `dsc_signal_learning`
- **This only covers `dsc_signal`.** If the other feeds ever get fixed and start
  producing real trade volume, they need the same per-signal-type learning built for
  them — right now they'd inherit none of this.

## Performance reality check (as of this session, 836 trades)
Net **-$78, 40.2% win rate**. Not a small sample, not noise. The Aug-8 "clean exit"
profile everyone remembered as profitable (a separate, smaller, unverifiable earlier run)
does not hold up at real scale. A from-scratch "ride to a double, bank half, let half
ride" momentum redesign was deployed on top of it (see `MOMENTUM_DOUBLE_PCT` etc.) —
unproven as of this writing; check `/trades/archive` `by_strategy` for real dsc_signal
results since that deploy before trusting it.

## Reset behavior (do not regress this)
No reset endpoint should ever delete trade history, USDC-locked tracking, wallet
activity, or combat stats — this was audited and fixed across `/admin/reset-daily`,
`/admin/reset-capital`, `/admin/reset-all`, `/set-capital`, `/admin/set-capital`. Only
`/clear-usdc` (explicit, by name) and `/admin/reset-all`'s open-position clear are
allowed to touch anything beyond capital + today's session counters. If a new
reset/admin endpoint is ever added, it must not silently wipe anything beyond what its
name promises — this exact mistake happened 4 separate times in one session before being
caught, each time discovered from a live screenshot, not from reading the code.

## Known unresolved
- `BIRDEYE_API_KEY` still not confirmed set in Railway — check `/status/api` boot log /
  API key audit before assuming this is fixed
- GMGN signal endpoints unreliable — no confirmed fix, just worked around via
  `MIN_SIGNAL_SCORE` tuning
- The momentum "double" strategy needs a real sample before anyone trusts it
- `paper_mode` has been `true` for this entire session — nothing here has been proven
  under real execution conditions (slippage, fills, latency)
- **`exit_trade()`'s pnl/capital math uses the SL/TP *trigger* price (from the market-data
  feed), not the actual Jupiter swap fill price.** `_verify_sell_and_retry()` only
  corrects capital when a sell fails outright (tokens stuck) — it never reconciles a
  *successful* sell that filled worse than the trigger price due to slippage. Root-caused
  but NOT fixed — touching the capital-crediting formula itself needs explicit sign-off
  before going live, this is flagged here so it isn't rediscovered as a surprise once
  `paper_mode` is turned off and real slippage starts showing up as PnL that doesn't match
  the wallet.
- **USDC Locked vs. lifetime PnL can show a large positive locked balance next to a
  negative all-time PnL** — this is a real design question (is "locked" profit still
  counted against career PnL once swept to USDC, or is it separate?), not something to
  silently decide in code. Needs user input before changing.
- Daily-loss-guard (`MAX_DAILY_LOSS_PCT`) baselines to `_day_start_cap`, which every
  capital-reset endpoint updates — so resetting capital frequently during iteration also
  resets how much room the guard thinks is left today. Acceptable/expected today (paper
  mode, active iteration) but worth remembering once live: frequent manual resets = the
  daily guard effectively never trips.
- Long-held momentum "ride" positions (`RIDE_MAX_SECS` up to 30 min) go a while between
  reconciler passes — a wide-enough Redis/state gap during that window and the position
  reconciler could disagree with the in-memory `open_trades` state briefly. Flagged, not
  reproduced.

## Fixed this pass (see commit history on `main` for exact diffs)
- Bond TP auto-tune floor formula (`PARTIAL_TP2_PCT+2` → `BOND_SL_PCT+2`) that was
  computing 101% once partials got disabled; added restore-time sanity clamps for
  `BOND_TP_PCT` (5–50%) and `BOND_ENTRY_MIN/MAX` (40–65% / 45–85%) so a broken formula
  can't get stuck forever again.
- `/positions/api` was showing the same hardcoded TP target for every open position
  regardless of strategy — now per-strategy via `_position_display_targets()`.
- `/trades` and `/status` computed win-rate/PnL from the recent ~200-trade window while
  Home already used the permanent all-time archive — same bot, different numbers on
  different pages. Both now show all-time figures (from `_archive_stats()`), with the
  recent-window numbers kept and explicitly labeled as recent.
- `/export/wins` and `/export/all` silently only exported the recent ~200-trade window
  despite their names implying a complete export — now source from the permanent archive.
- DSC signal tagging only recorded the highest-priority matching category
  (BOOST > ADS > META > TOP > TAKEOVER) when a coin matched more than one — the adaptive
  learning loop was silently losing that combo information. Now records every matching
  category as a `+`-joined tag (e.g. `BOOST+ADS`) so combos are tracked as their own
  bucket instead of collapsing into whichever category happened to be checked first.
- Wallet copy-trade polling ran unconditionally every 15s per tracked wallet even after
  the Helius push webhook was registered and successfully draining events — this was the
  actual source of wallets sitting permanently in 429 backoff. Polling now only runs as a
  5-minute safety-net sweep once the webhook is confirmed live; falls back to full-speed
  polling if the webhook was never registered (no public domain / key).
