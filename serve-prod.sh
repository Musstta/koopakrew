#!/usr/bin/env bash
set -euo pipefail

# Always run from repo root
cd "$(dirname "$0")"

VENV_DIR=".venv"
VENV_BIN="$VENV_DIR/bin"
VENV_PY="$VENV_BIN/python"
VENV_PIP="$VENV_BIN/pip"

if [[ ! -x "$VENV_PY" ]]; then
  if ! command -v python3 >/dev/null 2>&1; then
    echo "Error: python3 not found on PATH."
    exit 1
  fi
  echo "Creating virtual environment in $VENV_DIR ..."
  python3 -m venv "$VENV_DIR"
fi

if [[ -f "requirements.txt" ]]; then
  "$VENV_PIP" install -r requirements.txt
else
  "$VENV_PIP" install --upgrade pip >/dev/null
  "$VENV_PIP" install flask >/dev/null
fi

export KOOPAKREW_CONFIG="${KOOPAKREW_CONFIG:-production}"
export KOOPAKREW_PORT="${KOOPAKREW_PORT:-5000}"

echo "Starting Koopa Krew production-style server on port ${KOOPAKREW_PORT}..."
exec "$VENV_PY" run.py
