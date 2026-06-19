#!/bin/bash
set -e

# When running both bots together, give drift_bot its own port (DRIFT_PORT)
# so it doesn't clash with bot.py's Railway PORT.
echo "[start] Launching drift_bot (perp trader) on port ${DRIFT_PORT:-5001}..."
PORT=${DRIFT_PORT:-5001} python drift_bot.py &

echo "[start] Launching bot (sniper) on port ${PORT:-5000}..."
exec python bot.py
