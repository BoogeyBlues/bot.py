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
**LIVE mode (`PAPER_MODE=false`):** no reset endpoint should ever delete trade history,
USDC-locked tracking, wallet activity, or combat stats — this was audited and fixed
across `/admin/reset-daily`, `/admin/reset-capital`, `/admin/reset-all`, `/set-capital`,
`/admin/set-capital`. Only `/clear-usdc` (explicit, by name) and `/admin/reset-all`'s
open-position clear are allowed to touch anything beyond capital + today's session
counters. If a new reset/admin endpoint is ever added, it must not silently wipe
anything beyond what its name promises — this exact mistake happened 4 separate times in
one session before being caught, each time discovered from a live screenshot, not from
reading the code.

**PAPER mode (`PAPER_MODE=true`), `/admin/reset-capital` and `/admin/reset-all` only:**
deliberate exception, per explicit user request — paper-mode trade history isn't real
trading history, and accumulating it forever across strategy iterations was polluting
the one number meant to answer "is this worth trusting with real money" (screenshot:
853 stale paper trades / 40.1% WR / $110.73 USDC Locked survived a reset the user
expected to be a fresh start). While `PAPER_MODE` is on, both buttons call
`_paper_fresh_start()`, which wipes `completed_trades`, the permanent
`bot_trades_archive`, `usdc_locked`, `_pending_lock_usd`, `_week_day_logs`, and resets
the trade-id counter and `_milestones_hit`. DSC signal learning isn't stored separately
— it's derived live from the archive by `dsc_signal_type_stats()` — so wiping the
archive resets it for free. The instant `PAPER_MODE` flips to `false`, these same two
buttons silently revert to the live-mode never-wipe behavior above — `_paper_fresh_start`
is gated on `if PAPER_MODE:` and is simply never called once real capital is on the
line. Wallet activity/combat stats (the Wallet Arena feature) are NOT touched by this —
that's a separate subsystem from trade PnL and wasn't part of what was asked.

## Known unresolved
- `BIRDEYE_API_KEY` still not confirmed set in Railway — check `/status/api` boot log /
  API key audit before assuming this is fixed
- GMGN signal endpoints unreliable — no confirmed fix, just worked around via
  `MIN_SIGNAL_SCORE` tuning
- The momentum "double" strategy needs a real sample before anyone trusts it
- `paper_mode` has been `true` for this entire session — nothing here has been proven
  under real execution conditions (slippage, fills, latency)
- Long-held momentum "ride" positions (`RIDE_MAX_SECS` up to 30 min) go a while between
  reconciler passes — a wide-enough Redis/state gap during that window and the position
  reconciler could disagree with the in-memory `open_trades` state briefly. Flagged, not
  reproduced.
- Partial exits (`_partial_exit`, the momentum "bank half at the double" step) still credit
  capital from the SL/TP trigger price like full exits used to — they were left out of the
  slippage-reconciliation fix below (smaller, separate follow-up if it matters in practice).
- `execute_sell`'s PumpPortal/pump-swap/raydium path (bond/spike/trench/copy/fast — all
  low/zero-volume strategies per the dsc_signal-dominance finding above) has no equivalent
  slippage reconciliation — only the Jupiter path does. Not fixed, since Jupiter covers
  ~94%+ of real trade volume and PumpPortal doesn't return quote data the same way.

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
- **`capital` was never decremented when profit got swept to `usdc_locked`** — real SOL
  left the wallet to become USDC but the internal capital counter kept counting it as
  still-tradeable, silently overstating buying power by everything ever locked. Fixed in
  both the paper-mode path and the live Jupiter-swap path (including its refund/failure
  branches). **User decision:** USDC Locked and career PnL should be "netted together" —
  added a `Net Worth` figure (`capital + usdc_locked`, vs. `STARTING_CAPITAL`) to Home,
  Status, and `/status/api` as the reconciled all-time number. Kept `_archive_stats()`'s
  true all-time trade-sum PnL as the separate headline (reset-immune — see the epoch-stats
  history above for why that's load-bearing); relabeled `USDC Locked` to make clear it's a
  cumulative secured-profit counter that doesn't drop on later losses, since that (not an
  actual conflict) is why it could look bigger than net PnL.
- **User decision:** the daily-loss-guard baseline (`_day_start_cap`) should only move at
  the real day-boundary rollover, not on manual capital resets. Decoupled from
  `/admin/reset-capital`, `/set-capital`, `/admin/sync-capital`, `/admin/set-capital`, and
  `/admin/reset-all` — none of them touch it now. Left `/admin/reset-daily` alone since
  forcing a fresh day is literally its purpose. Known tradeoff the user accepted: a large
  manual capital correction (e.g. fixing a bad number) can now look like a sudden loss and
  briefly trip the guard, since the baseline won't have moved with it.
- **User decision: build the real fix, not just visibility, for trigger-price-vs-actual-fill
  slippage.** Jupiter sells now capture what the swap's own quote promised at submit time;
  once `_verify_sell_and_retry` confirms the sell landed, it reconciles capital and the
  already-recorded trade (working view + permanent archive) against that quote via
  `_apply_slippage_correction`, sanity-clamped to the estimate's own size. Only covers the
  Jupiter path (dsc_signal + other Jupiter-routed strategies, ~94%+ of real volume) — see
  Known unresolved above for what's still out of scope (partial exits, PumpPortal sells).
