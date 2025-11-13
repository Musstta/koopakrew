#!/usr/bin/env bash
set -euo pipefail

# Always run from this script's folder
cd "$(dirname "$0")"

# ---- venv bootstrap ---------------------------------------------------------
VENV_DIR=".venv"
VENV_BIN="$VENV_DIR/bin"
VENV_PY="$VENV_BIN/python"
VENV_PIP="$VENV_BIN/pip"

if [[ ! -x "$VENV_PY" ]]; then
  # Create venv if it doesn't exist
  if ! command -v python3 >/dev/null 2>&1; then
    echo "Error: python3 not found on PATH."
    exit 1
  fi
  echo "Creating virtual environment in $VENV_DIR ..."
  python3 -m venv "$VENV_DIR"
fi

# Ensure Flask (and any requirements) are installed
if [[ -f "requirements.txt" ]]; then
  "$VENV_PIP" install -r requirements.txt
else
  "$VENV_PIP" install --upgrade pip >/dev/null
  # Minimal deps for this app
  "$VENV_PIP" install flask >/dev/null
fi

# ---- Environment knobs (safe defaults) --------------------------------------
export KOOPAKREW_DB="${KOOPAKREW_DB:-koopakrew.db}"
export KOOPAKREW_S2_CSV="${KOOPAKREW_S2_CSV:-MK8TracksS2.csv}"
export KOOPAKREW_TZ="${KOOPAKREW_TZ:-America/Costa_Rica}"
export KOOPAKREW_SECRET="${KOOPAKREW_SECRET:-change-me-in-prod}"

# Season 2 (2025 Q4) seed metadata
export KOOPAKREW_SEASON_LABEL="${KOOPAKREW_SEASON_LABEL:-Season 2 — 2025 Q4}"
export KOOPAKREW_SEASON_ID="${KOOPAKREW_SEASON_ID:-2}"           # seeder pre-check convenience
export KOOPAKREW_SEASON_START="${KOOPAKREW_SEASON_START:-2025-10-01}"
export KOOPAKREW_SEASON_END="${KOOPAKREW_SEASON_END:-2026-01-01}"

# Players
export KOOPAKREW_PLAYERS="${KOOPAKREW_PLAYERS:-Salim,Sergio,Fabian,Sebas}"

# Optional: force reseed (idempotent). Default 0
: "${KOOPAKREW_FORCE_SEED:=0}"

# ---- Sanity checks ----------------------------------------------------------
if [[ ! -f "$KOOPAKREW_S2_CSV" ]]; then
  echo "Warning: CSV '$KOOPAKREW_S2_CSV' not found. If this is a fresh setup, place it in the project root."
fi

# ---- Seed if needed ---------------------------------------------------------
if [[ ! -f "$KOOPAKREW_DB" || "$KOOPAKREW_FORCE_SEED" == "1" ]]; then
  echo "Seeding Season 2 from ${KOOPAKREW_S2_CSV}..."
  exec 3>&1
  "$VENV_PY" db_init.py | tee /dev/fd/3
else
  echo "Database present — skipping seed."
fi

# ---- Run the app ------------------------------------------------------------
echo "Starting Koopa Krew app..."
exec "$VENV_PY" app.py
