import os
import csv
import sqlite3

DB_PATH = os.environ.get("KOOPAKREW_DB", "koopakrew.db")
LOCAL_TZ = os.environ.get("KOOPAKREW_TZ", "America/Costa_Rica")
CSV_PATH = os.environ.get("KOOPAKREW_S2_CSV", "MK8TracksS2.csv")

# Season 2 meta (2025 Q4)
SEASON_LABEL = os.environ.get("KOOPAKREW_SEASON_LABEL", "Season 2 — 2025 Q4")
SEASON_ID = int(os.environ.get("KOOPAKREW_SEASON_ID", "2"))
SEASON_START = os.environ.get("KOOPAKREW_SEASON_START", "2025-10-01")
SEASON_END_EXCLUSIVE = os.environ.get("KOOPAKREW_SEASON_END", "2026-01-01")

PLAYERS = [name.strip() for name in os.environ.get("KOOPAKREW_PLAYERS", "Salim,Sergio,Fabian,Sebas").split(",")]


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def create_schema(db):
    db.executescript("""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS players (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS season_meta (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  label TEXT NOT NULL,
  start_date TEXT NOT NULL,     -- ISO yyyy-mm-dd
  end_date   TEXT NOT NULL      -- exclusive
);

CREATE TABLE IF NOT EXISTS cups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,
  en   TEXT NOT NULL,
  es   TEXT NOT NULL,
  [order] INTEGER NOT NULL      -- <— quoted because 'order' is a keyword
);

CREATE TABLE IF NOT EXISTS tracks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL,                 -- stable id for the track
  cup_id INTEGER NOT NULL REFERENCES cups(id),
  en TEXT NOT NULL,
  es TEXT NOT NULL,
  order_in_cup INTEGER NOT NULL CHECK (order_in_cup BETWEEN 1 AND 4),

  owner_id INTEGER REFERENCES players(id),
  state INTEGER NOT NULL CHECK (state IN (-1,0,1)),  -- -1 At Risk, 0 Default, 1 Locked
  threatened_by_id INTEGER REFERENCES players(id),

  season INTEGER NOT NULL,

  -- Only At Risk tracks have a mark; Default/Locked must not.
  CHECK (
    (state = -1 AND threatened_by_id IS NOT NULL)
    OR
    (state IN (0,1) AND threatened_by_id IS NULL)
  ),

  UNIQUE (season, code)
);

CREATE INDEX IF NOT EXISTS idx_tracks_season_cup   ON tracks(season, cup_id);
CREATE INDEX IF NOT EXISTS idx_tracks_season_owner ON tracks(season, owner_id);
CREATE INDEX IF NOT EXISTS idx_tracks_season_code  ON tracks(season, code);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  track_id INTEGER REFERENCES tracks(id),
  winner_id INTEGER REFERENCES players(id),
  occurred_at TEXT NOT NULL,          -- ISO timestamp

  -- race pre/post snapshot (for is_sweep = 0)
  pre_owner_id INTEGER,
  pre_state INTEGER,
  pre_threatened_by_id INTEGER,
  post_owner_id INTEGER,
  post_state INTEGER,
  post_threatened_by_id INTEGER,

  -- JSON array of side effects (each has track_id, pre_state, pre_threatened_by_id, post_state, post_threatened_by_id)
  side_effects_json TEXT,

  -- sweep metadata (for separate sweep events)
  is_sweep INTEGER NOT NULL DEFAULT 0,
  sweep_cup_id INTEGER REFERENCES cups(id),
  sweep_owner_id INTEGER REFERENCES players(id)
);

CREATE INDEX IF NOT EXISTS idx_events_time     ON events(occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_is_sweep ON events(is_sweep);
""")



def seed_players(db):
    for name in PLAYERS:
        db.execute("INSERT OR IGNORE INTO players (name, active) VALUES (?, 1)", (name,))
    db.commit()


def seed_season_meta(db):
    # Check if this exact season row exists; if not, insert
    row = db.execute(
        "SELECT id FROM season_meta WHERE label = ? AND start_date = ? AND end_date = ?",
        (SEASON_LABEL, SEASON_START, SEASON_END_EXCLUSIVE)
    ).fetchone()
    if not row:
        db.execute(
            "INSERT INTO season_meta (label, start_date, end_date) VALUES (?, ?, ?)",
            (SEASON_LABEL, SEASON_START, SEASON_END_EXCLUSIVE)
        )
        db.commit()
    # Return the id for the row matching the dates
    row = db.execute(
        "SELECT id FROM season_meta WHERE start_date = ? AND end_date = ?",
        (SEASON_START, SEASON_END_EXCLUSIVE)
    ).fetchone()
    return row["id"]


def read_csv_rows():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")
    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader]
    # Expected columns:
    expected = {"track_code","cup_code","cup_en","cup_es","track_en","track_es","owner","state","threat"}
    missing = expected - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"CSV missing columns: {sorted(missing)}")
    return rows


def seed_cups_tracks(db, season_id):
    rows = read_csv_rows()

    # Build players dict
    players = {r["name"]: r["id"] for r in db.execute("SELECT id, name FROM players").fetchall()}

    # Insert cups in first-seen order
    cup_order = {}
    for r in rows:
        code = r["cup_code"].strip()
        if code not in cup_order:
            cup_order[code] = len(cup_order) + 1

    for code, order in cup_order.items():
        # Find a representative row for names
        rep = next((r for r in rows if r["cup_code"].strip() == code), None)
        if rep is None:
            raise ValueError(f"No representative row found for cup_code '{code}' in CSV rows.")
        en = rep["cup_en"].strip()
        es = rep["cup_es"].strip()
        db.execute(
            'INSERT OR IGNORE INTO cups (code, en, es, "order") VALUES (?, ?, ?, ?)',
            (code, en, es, order)
        )
    db.commit()

    # Cup id map
    cups = {r["code"]: r["id"] for r in db.execute("SELECT id, code FROM cups").fetchall()}

    # Insert tracks; compute order_in_cup by encounter within each cup
    order_in_cup_map = {}
    for r in rows:
        cup_code = r["cup_code"].strip()
        order_in_cup_map.setdefault(cup_code, 0)
        order_in_cup_map[cup_code] += 1
        order_in_cup = order_in_cup_map[cup_code]

        track_code = r["track_code"].strip()
        en = r["track_en"].strip()
        es = r["track_es"].strip()

        owner_name = (r.get("owner") or "").strip() or None
        state_val = int(r.get("state") or 0)
        threat_name = (r.get("threat") or "").strip() or None

        owner_id = players.get(owner_name) if owner_name else None
        threat_id = players.get(threat_name) if threat_name else None

        db.execute(
            """INSERT OR IGNORE INTO tracks
(code, cup_id, en, es, order_in_cup, owner_id, state, threatened_by_id, season)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
            (track_code, cups[cup_code], en, es, order_in_cup, owner_id, state_val, threat_id, season_id)
        )
    db.commit()


def main():
    db = get_db()
    create_schema(db)

    # If tracks already exist for this season, do nothing (idempotent import)
    row = db.execute("SELECT COUNT(*) AS n FROM tracks WHERE season = ?", (SEASON_ID,)).fetchone()
    if row and row["n"] > 0:
        print(f"Season {SEASON_ID} already has {row['n']} tracks. Skipping import.")
        return

    seed_players(db)
    season_id = seed_season_meta(db)
    seed_cups_tracks(db, season_id)
    print(f"Seeded Season {season_id} from {CSV_PATH}.")


if __name__ == "__main__":
    main()
