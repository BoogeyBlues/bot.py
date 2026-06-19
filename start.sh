#!/bin/bash
set -e

echo "[start] Launching drift_bot (perp trader) on port ${DRIFT_PORT:-5001}..."
python drift_bot.py &

echo "[start] Launching bot (sniper) on port ${PORT:-5000}..."
python bot.py
