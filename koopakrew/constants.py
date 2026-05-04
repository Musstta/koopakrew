import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # project root

STATIC_IMAGE_EXTS = ("png", "jpg", "jpeg", "webp", "gif", "svg")

ONLINE_TIMEOUT_SECONDS = 300
PRESENCE_FRESH_SECONDS = 90
PRESENCE_WARMING_SECONDS = 210

SEASONAL_LOGOS = {
    9: "KoopaKrewPatriot.png",
    10: "KoopaKrewSpooky.png",
    12: "KoopaKrewXmas.png",
}

ARCHIVE_ENTRIES = [
    {
        "id": "season1",
        "label": "Season 1 — 2025 Q4",
        "type": "db",
        "season_label": "Season 1 — 2025 Q4",
        "notes": "Live data snapshot pulled from the Koopa Krew database.",
    },
    {
        "id": "season1",
        "label": "Season 1 — 2025 Q3",
        "type": "csv",
        "file": os.fspath(BASE_DIR / "archive" / "snapshots" / "season1_tracks.csv"),
        "notes": "Legacy export (track names in Spanish).",
    },
]

STATE_ALIASES = {
    "locked": "locked",
    "default": "default",
    "at-risk": "at-risk",
    "atrisk": "at-risk",
    "at_risk": "at-risk",
}
STATE_VALUE_MAP = {"locked": 1, "default": 0, "at-risk": -1}
