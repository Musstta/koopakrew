#!/usr/bin/env python3
"""List cup and track images that are missing local files."""
import sqlite3
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static" / "images"
CUP_DIR = STATIC / "cups"
TRACK_DIR = STATIC / "tracks"
EXTS = ("png","jpg","jpeg","webp","gif","svg")

def has_asset(dir_path: Path, code: str) -> bool:
    return any((dir_path / f"{code}.{ext}").exists() for ext in EXTS)

conn = sqlite3.connect(ROOT / "koopakrew.db")
conn.row_factory = sqlite3.Row

missing_cups = [f"{row['code']} – {row['en']}" for row in conn.execute("SELECT code,en FROM cups ORDER BY [order]") if not has_asset(CUP_DIR, str(row['code']))]
missing_tracks = [f"{row['code']} – {row['en']}" for row in conn.execute("SELECT code,en FROM tracks ORDER BY code") if not has_asset(TRACK_DIR, str(row['code']))]

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
