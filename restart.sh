#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

pkill -f '\.venv/bin/python run\.py' 2>/dev/null || true
sleep 1

KOOPAKREW_CONFIG=production nohup .venv/bin/python run.py >> /tmp/koopakrew.log 2>&1 &
echo "Started PID $! — tailing log..."
sleep 2
tail -6 /tmp/koopakrew.log
