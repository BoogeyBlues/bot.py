"""
Monte Carlo simulation: How long to reach $100,000?

Uses the bot's actual parameters to project growth across 10,000 simulated runs.
"""
import random
import statistics

# ── Bot Parameters (match bot.py defaults) ───────────────────────
STARTING_CAPITAL  = 39.67
TARGET            = 100_000
TRADE_PCT         = 18        # % of capital per trade
MIN_TRADE         = 5.0
MAX_TRADE         = 500.0
DAILY_MAX         = 10        # max trades per day
DAILY_LOSS_MAX    = 3         # losses before 4-hour cooldown
COOLDOWN_TRADES   = 4         # trades skipped after hitting loss limit (≈4 hrs of scanning)

# ── Strategy Win/Loss Assumptions ────────────────────────────────
# Bond Runner: entry 58-63%, TP at 67% bond → ~8-15% gain on trade size
# SL at 10% of trade size. Stale exit (no move in 2 min) → ~3% loss avg.
#
# Scenarios: pessimistic / base / optimistic
SCENARIOS = {
    "Pessimistic (35% WR)": {"win_rate": 0.35, "avg_win_pct": 0.18, "avg_loss_pct": 0.10},
    "Base Case  (48% WR)":  {"win_rate": 0.48, "avg_win_pct": 0.22, "avg_loss_pct": 0.10},
    "Optimistic (58% WR)":  {"win_rate": 0.58, "avg_win_pct": 0.28, "avg_loss_pct": 0.09},
    "Strong     (65% WR)":  {"win_rate": 0.65, "avg_win_pct": 0.32, "avg_loss_pct": 0.08},
}

SIMULATIONS = 10_000
BUST_FLOOR   = 2.0   # bot stops trading below $2

def run_simulation(win_rate, avg_win_pct, avg_loss_pct):
    capital = STARTING_CAPITAL
    day = 0
    while capital < TARGET and capital >= BUST_FLOOR and day < 10_000:
        day += 1
        daily_losses = 0
        daily_trades = 0
        cooldown_remaining = 0

        for _ in range(DAILY_MAX):
            if cooldown_remaining > 0:
                cooldown_remaining -= 1
                continue
            if capital < BUST_FLOOR:
                break

            trade_size = capital * TRADE_PCT / 100
            trade_size = max(MIN_TRADE, min(MAX_TRADE, trade_size))
            if trade_size > capital:
                trade_size = capital

            won = random.random() < win_rate
            # Add variance: wins range 50%-150% of avg, losses range 80%-120%
            if won:
                pct = avg_win_pct * random.uniform(0.5, 1.5)
                capital += trade_size * pct
            else:
                pct = avg_loss_pct * random.uniform(0.8, 1.2)
                capital -= trade_size * pct
                daily_losses += 1

            daily_trades += 1

            if daily_losses >= DAILY_LOSS_MAX:
                cooldown_remaining = COOLDOWN_TRADES  # skip next ~4 trades (4-hr cooldown)
                daily_losses = 0  # reset after cooldown

            if capital >= TARGET:
                return day, capital

    return day, capital

def format_days(days):
    if days < 7:
        return f"{days}d"
    elif days < 30:
        return f"{days//7}w {days%7}d"
    elif days < 365:
        months = days // 30
        rem = days % 30
        return f"{months}mo {rem}d"
    else:
        years = days // 365
        rem = (days % 365) // 30
        return f"{years}y {rem}mo"

print(f"\n{'='*62}")
print(f"  Monte Carlo Simulation: ${STARTING_CAPITAL:.2f} → $100,000")
print(f"  {SIMULATIONS:,} runs per scenario | {DAILY_MAX} trades/day max")
print(f"{'='*62}\n")

all_results = {}

for name, params in SCENARIOS.items():
    results = []
    busted  = 0
    reached = 0

    for _ in range(SIMULATIONS):
        days, final = run_simulation(**params)
        if final >= TARGET:
            reached += 1
            results.append(days)
        elif final < BUST_FLOOR:
            busted += 1

    all_results[name] = results

    if results:
        med    = statistics.median(results)
        p25    = sorted(results)[int(len(results) * 0.25)]
        p75    = sorted(results)[int(len(results) * 0.75)]
        p10    = sorted(results)[int(len(results) * 0.10)]
        p90    = sorted(results)[int(len(results) * 0.90)]
        pct    = reached / SIMULATIONS * 100
        bust_p = busted / SIMULATIONS * 100

        print(f"  {name}")
        print(f"    Win rate: {params['win_rate']*100:.0f}%  |  Avg win: +{params['avg_win_pct']*100:.0f}%  |  Avg loss: -{params['avg_loss_pct']*100:.0f}%")
        print(f"    Reached $100k:    {pct:.1f}% of runs ({reached:,}/{SIMULATIONS:,})")
        print(f"    Busted (<$2):     {bust_p:.1f}% of runs")
        if reached > 0:
            print(f"    Fastest path:     {format_days(int(p10))}  (top 10%)")
            print(f"    Median time:      {format_days(int(med))}")
            print(f"    Slower path:      {format_days(int(p90))}  (bottom 10%)")
            print(f"    Middle 50% range: {format_days(int(p25))} – {format_days(int(p75))}")
        print()

# ── Capital milestone checkpoints (base case) ────────────────────
print(f"{'='*62}")
print(f"  Milestone Checkpoints — Base Case (48% WR)")
print(f"{'='*62}")

milestones = [100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000]
base = SCENARIOS["Base Case  (48% WR)"]
milestone_days = {m: [] for m in milestones}

for _ in range(SIMULATIONS):
    capital = STARTING_CAPITAL
    day = 0
    hit = set()
    while capital < TARGET and capital >= BUST_FLOOR and day < 10_000:
        day += 1
        daily_losses = 0
        cooldown_remaining = 0
        for _ in range(DAILY_MAX):
            if cooldown_remaining > 0:
                cooldown_remaining -= 1
                continue
            if capital < BUST_FLOOR:
                break
            trade_size = max(MIN_TRADE, min(MAX_TRADE, capital * TRADE_PCT / 100))
            won = random.random() < base["win_rate"]
            if won:
                capital += trade_size * base["avg_win_pct"] * random.uniform(0.5, 1.5)
            else:
                capital -= trade_size * base["avg_loss_pct"] * random.uniform(0.8, 1.2)
                daily_losses += 1
            if daily_losses >= DAILY_LOSS_MAX:
                cooldown_remaining = COOLDOWN_TRADES
                daily_losses = 0
            for m in milestones:
                if capital >= m and m not in hit:
                    hit.add(m)
                    milestone_days[m].append(day)

print(f"  {'Milestone':<12} {'Median time':<20} {'% of runs that hit it'}")
print(f"  {'-'*55}")
for m in milestones:
    days_list = milestone_days[m]
    if days_list:
        med = int(statistics.median(days_list))
        pct = len(days_list) / SIMULATIONS * 100
        print(f"  ${m:<11,} {format_days(med):<20} {pct:.1f}%")
    else:
        print(f"  ${m:<11,} {'never':<20} 0%")

print(f"\n{'='*62}")
print("  Key takeaway: small daily edge + compounding = exponential growth")
print("  The bot's 4-hour cooldown after 3 losses protects against drawdown")
print(f"{'='*62}\n")
