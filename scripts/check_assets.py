#!/usr/bin/env python3
"""List cup and track images that are missing local files."""
import os
from pathlib import Path

from koopakrew import create_app
from koopakrew.db import connect_database

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static" / "images"
CUP_DIR = STATIC / "cups"
TRACK_DIR = STATIC / "tracks"
EXTS = ("png", "jpg", "jpeg", "webp", "gif", "svg")


def has_asset(dir_path: Path, code: str) -> bool:
    return any((dir_path / f"{code}.{ext}").exists() for ext in EXTS)


def resolve_db_path() -> Path:
    override = os.environ.get("KOOPAKREW_DB_PATH")
    if override:
        return Path(override)
    app = create_app()
    return Path(app.config["DATABASE_PATH"])


conn = connect_database(resolve_db_path())

missing_cups = [
    f"{row['code']} – {row['en']}"
    for row in conn.execute("SELECT code,en FROM cups ORDER BY [order]")
    if not has_asset(CUP_DIR, str(row["code"]))
]
missing_tracks = [
    f"{row['code']} – {row['en']}"
    for row in conn.execute("SELECT code,en FROM tracks ORDER BY code")
    if not has_asset(TRACK_DIR, str(row["code"]))
]

if missing_cups:
    print("Missing cup logos:")
    for entry in missing_cups:
        print("  ", entry)
else:
    print("All cup logos found.")

print()
if missing_tracks:
    print("Missing track art (drop files like static/images/tracks/<code>.png):")
    for entry in missing_tracks:
        print("  ", entry)
else:
    print("All track arts found.")
