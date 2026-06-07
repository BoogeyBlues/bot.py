# PumpFun Sniper Bot

> Autonomous Solana memecoin trading bot with real-time scanning, multi-strategy execution, and self-tuning parameter optimization.

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-REST%20API-000000?style=flat&logo=flask&logoColor=white)
![Solana](https://img.shields.io/badge/Solana-On--Chain-9945FF?style=flat&logo=solana&logoColor=white)
![Railway](https://img.shields.io/badge/Deployed%20on-Railway-0B0D0E?style=flat&logo=railway&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

---

## Overview

PumpFun Sniper Bot is a fully autonomous trading system built for the [pump.fun](https://pump.fun) Solana memecoin launchpad. It runs 24/7 in the cloud, scanning for trade opportunities across three independent strategies, managing open positions with multi-layered exit logic, and continuously retuning its own parameters based on historical performance.

This project was built from scratch as a portfolio piece to demonstrate end-to-end Python engineering — from on-chain transaction signing to cloud deployment, REST APIs, multi-threaded concurrency, and self-improving feedback loops.

---

## Features

### Trading
- **Three independent strategies** running concurrently (Bond Runner, Dormant Spike, Copy Trading)
- **Paper mode** for fully simulated trading with no real funds at risk
- **Live mode** with real on-chain execution via Solana versioned transactions
- **Multi-layered exit logic**: sharp drop guard, bonding curve slip detection, stale position timeout
- **Daily trade limits**: maximum 10 trades/day with automatic reset at midnight

### Risk Management
- **3-loss cooldown**: after 3 consecutive losses, trading pauses for 4 hours, triggers parameter reanalysis, then resumes with adjusted settings
- **USDC profit locking**: when capital exceeds $80, profits are swapped to USDC via the GMGN router to protect gains
- **Milestone alerts**: Telegram notifications at $100, $250, $500, $1k, $5k, $10k, $25k, $50k, $100k

### Self-Learning
- Analyzes full trade history every 24 hours
- Auto-tunes four parameters based on what is actually winning:
  - Bond entry range (58-63% default)
  - Stop loss percentage
  - Stale position timeout
  - Spike take-profit target
- Midnight daily summary and weekly deep analysis report sent to Telegram

### Infrastructure
- Deployed on Railway cloud — always-on, no local machine required
- State persisted to disk so redeploys never lose daily progress or open positions
- Flask REST API with 6 endpoints for monitoring and data export
- Queued Telegram notification worker — never blocks trading threads

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        PumpFun Sniper Bot                       │
│                     (Railway Cloud Process)                     │
└─────────────────────┬───────────────────────────────────────────┘
                      │
        ┌─────────────┼──────────────┬─────────────────┐
        │             │              │                  │
        ▼             ▼              ▼                  ▼
┌──────────────┐ ┌──────────┐ ┌───────────┐ ┌──────────────────┐
│   SCANNER    │ │ MONITOR  │ │   COPY    │ │  NOTIFICATION    │
│   THREAD     │ │  THREAD  │ │  TRADER   │ │    WORKER        │
│              │ │          │ │  THREAD   │ │    THREAD        │
│ Scans pump   │ │ Manages  │ │ Mirrors   │ │ Queued Telegram  │
│ .fun in real │ │ open     │ │ GMGN      │ │ delivery —       │
│ time for new │ │ positions│ │ smart     │ │ non-blocking     │
│ opportunities│ │ + exits  │ │ wallets   │ │                  │
└──────┬───────┘ └────┬─────┘ └─────┬─────┘ └────────┬─────────┘
       │              │             │                 │
       └──────────────┴─────────────┴─────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │   Shared State     │
                    │  (Thread-Safe /    │
                    │   Disk Persisted)  │
                    └─────────┬──────────┘
                              │
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
       ┌────────────┐  ┌────────────┐  ┌─────────────┐
       │ Flask API  │  │  CSV Logs  │  │  Telegram   │
       │ (6 routes) │  │ (trades,   │  │    Bot      │
       │            │  │  wins)     │  │             │
       └────────────┘  └────────────┘  └─────────────┘


External APIs:
  pump.fun ──── DexScreener ──── PumpPortal ──── GMGN
  CoinGecko ──── rugcheck.xyz ──── Solana RPC
```

---

## Strategies

### Bond Runner
Targets tokens in the early bonding curve phase before they graduate to Raydium.

- **Entry**: token is between 58-63% of its bonding curve (auto-tuned daily)
- **Exit**: bonding curve reaches 67%, or stop loss / stale timeout triggers
- **Edge**: most pump.fun traders enter too early or too late — this targets the momentum window just before graduation pressure kicks in

### Dormant Spike
Identifies older tokens that are waking up with unusual volume.

- **Entry**: token is 12+ hours old AND has seen a 100%+ price move in the last 1 hour
- **Exit**: configurable take-profit (auto-tuned), stop loss, or stale exit
- **Edge**: filters out new launch noise — a token that survived 12 hours and is now spiking has organic demand behind it

### Copy Trading
Mirrors wallets that have a demonstrated edge on GMGN.

- **Entry**: when a tracked smart wallet (60-99% historical win rate) makes a buy, the bot follows within seconds
- **Exit**: mirrors the wallet's sell signal, or falls back to internal stop loss
- **Edge**: delegates strategy entirely to wallets with proven track records rather than relying on technical signals alone

---

## Self-Learning System

Every 24 hours at midnight, the bot runs a full analysis pass over its trade history:

```
Daily Reanalysis Loop
─────────────────────
1. Load all completed trades from disk
2. Segment by strategy and outcome (win/loss)
3. Identify parameter ranges that correlate with wins
4. Adjust four parameters toward the winning ranges:
     - bond_entry_min / bond_entry_max  (default: 58-63%)
     - stop_loss_pct                    (default: configurable)
     - stale_timeout_minutes            (default: configurable)
     - spike_take_profit_pct            (default: configurable)
5. Persist new parameters to disk
6. Send Telegram report with before/after values
7. Resume trading with updated config
```

After a 3-loss streak, the same reanalysis runs immediately rather than waiting for midnight — the bot does not resume until it has reviewed what went wrong.

Weekly deep analysis (every 7 days) generates a more detailed breakdown by strategy, time-of-day, and market conditions.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Concurrency | `threading`, `queue`, `Lock` |
| Web Framework | Flask |
| On-Chain | `solders`, `solana-py` (VersionedTransaction signing) |
| HTTP | `requests` |
| Data | `json`, `csv` (disk persistence) |
| Notifications | Telegram Bot API |
| Deployment | Railway (cloud, always-on) |
| Version Control | Git / GitHub |

**External APIs integrated:**

| API | Purpose |
|---|---|
| pump.fun | Real-time token scanning, bonding curve data |
| PumpPortal | WebSocket stream for new token events |
| DexScreener | Price feeds, volume, liquidity data |
| GMGN | Smart wallet tracking, swap router (USDC locking) |
| CoinGecko | SOL/USD price reference |
| rugcheck.xyz | Token contract risk scoring |
| Telegram Bot API | All notifications and reports |

---

## REST API Endpoints

The bot exposes a Flask API for external monitoring and data access.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/status` | Current bot state: mode, active trades, daily P&L, cooldown status, current parameters |
| `GET` | `/trades` | All trades from the current session (open and closed) |
| `GET` | `/log` | Recent bot activity log entries |
| `POST` | `/learn` | Manually trigger a parameter reanalysis pass |
| `GET` | `/export/wins` | Download CSV of all winning trades |
| `GET` | `/export/all` | Download CSV of all trades (wins and losses) |

---

## Setup

### Prerequisites

- Python 3.11+
- A funded Solana wallet (for live mode)
- A Telegram bot token and chat ID
- Railway account (or any always-on host)

### Installation

```bash
git clone https://github.com/your-username/pumpfun-sniper-bot.git
cd pumpfun-sniper-bot
pip install -r requirements.txt
```

### Environment Variables

Configure the following variables in Railway (or a local `.env` file — never commit this):

| Variable | Required | Description |
|---|---|---|
| `WALLET_PRIVATE_KEY` | Yes (live) | Base58 Solana private key for transaction signing |
| `TELEGRAM_BOT_TOKEN` | Yes | Token from [@BotFather](https://t.me/botfather) |
| `TELEGRAM_CHAT_ID` | Yes | Your Telegram user or group chat ID |
| `MODE` | Yes | `paper` or `live` |
| `INITIAL_CAPITAL_USD` | Yes | Starting capital in USD (used for position sizing) |
| `PUMPPORTAL_API_KEY` | No | PumpPortal API key (increases rate limits) |
| `GMGN_API_KEY` | No | GMGN API key for smart wallet tracking |
| `RUGCHECK_API_KEY` | No | rugcheck.xyz API key |
| `MAX_TRADES_PER_DAY` | No | Daily trade limit (default: `10`) |
| `STOP_LOSS_PCT` | No | Stop loss percentage (default: `15`) |
| `BOND_ENTRY_MIN` | No | Min bonding curve % for Bond Runner (default: `58`) |
| `BOND_ENTRY_MAX` | No | Max bonding curve % for Bond Runner (default: `63`) |
| `STALE_TIMEOUT_MINUTES` | No | Minutes before exiting a stale position (default: `30`) |
| `COOLDOWN_HOURS` | No | Cooldown after 3 losses (default: `4`) |
| `PROFIT_LOCK_THRESHOLD_USD` | No | Swap profits to USDC above this amount (default: `80`) |

### Running Locally

```bash
# Paper mode (safe — no real funds)
MODE=paper python bot.py

# Live mode
MODE=live python bot.py
```

### Deploying to Railway

1. Push the repo to GitHub
2. Create a new Railway project and connect the repo
3. Add all environment variables in the Railway dashboard
4. Railway auto-deploys on every push — the bot starts immediately

---

## Paper Mode vs Live Mode

| Feature | Paper Mode | Live Mode |
|---|---|---|
| Trade execution | Simulated (no on-chain txns) | Real Solana transactions |
| P&L tracking | Yes (virtual) | Yes (real) |
| Telegram alerts | Yes | Yes |
| Self-learning | Yes | Yes |
| API endpoints | Yes | Yes |
| Risk | None | Real capital at risk |

Paper mode is fully functional and recommended for validating strategy performance before switching to live.

---

## Project Structure

```
pumpfun-sniper-bot/
├── bot.py                  # Main entry point — spins up all threads
├── strategies/
│   ├── bond_runner.py      # Bond curve entry/exit logic
│   ├── dormant_spike.py    # Aged token spike detection
│   └── copy_trader.py      # GMGN wallet mirroring
├── core/
│   ├── scanner.py          # pump.fun real-time scanning
│   ├── monitor.py          # Open position management
│   ├── executor.py         # On-chain transaction signing (solders)
│   ├── learner.py          # Daily self-analysis and parameter tuning
│   └── state.py            # Thread-safe shared state + disk persistence
├── integrations/
│   ├── dexscreener.py
│   ├── gmgn.py
│   ├── rugcheck.py
│   ├── coingecko.py
│   └── telegram.py
├── api/
│   └── routes.py           # Flask REST API
├── data/
│   ├── trades.csv          # Persisted trade history
│   └── state.json          # Bot state (survives redeploys)
├── requirements.txt
└── README.md
```

---

## Built By

**Christian Daniels** — self-taught Python developer.

This bot is a portfolio project built to demonstrate practical, production-grade engineering across multiple domains:

- **Systems programming**: multi-threaded Python with proper lock discipline, queue-based worker patterns, and disk-persisted state across process restarts
- **Web3 / blockchain**: real on-chain transaction construction and signing using `solders` and Solana versioned transactions — not just calling a swap widget
- **API integration**: seven external APIs integrated with rate limiting, error handling, and fallback logic
- **Self-improving systems**: a feedback loop that ingests historical trade data and adjusts live operating parameters — a lightweight form of the same pattern used in production ML systems
- **Cloud deployment**: Railway-hosted, environment-variable-configured, always-on service with no manual intervention required after deploy

The strategies, architecture, and all code were designed and written independently. No course, template, or existing bot was used as a base.

---

> **Disclaimer**: This software is for educational and portfolio purposes. Cryptocurrency trading involves substantial risk of loss. Nothing in this project constitutes financial advice. Use paper mode until you fully understand the system's behavior.
