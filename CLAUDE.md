# Project Memory — Leverage Bot (drift_bot.py)

## Repo
- **boogeyblues/bot.py** — branch `claude/pumpfun-sniper-bot-dVuGg`
- **NEVER** touch `main` branch — it runs `bot.py` (PumpFun sniper), separate service
- All drift_bot.py changes push to `claude/pumpfun-sniper-bot-dVuGg` only

## Two Bots in One Repo
| File | What it is | Branch |
|---|---|---|
| `bot.py` | PumpFun sniper | `main` |
| `drift_bot.py` | Leverage/perps bot (Jupiter) | `claude/pumpfun-sniper-bot-dVuGg` |

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
