import csv
import os
from collections import defaultdict

from koopakrew.constants import ARCHIVE_ENTRIES
from koopakrew.helpers.assets import decorate_standings_with_art
from koopakrew.queries.seasons import current_local_date
from koopakrew.queries.standings import fetch_standings, fetch_totals_overall
from koopakrew.services import core as services_core


def load_archive_rows(path: str) -> list[list[str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return [row for row in reader if any(cell.strip() for cell in row)]


def load_legacy_archive_data(path: str) -> dict:
    if not os.path.exists(path):
        return {"cups": [], "totals": []}
    cups: dict[str, list] = defaultdict(list)
    totals: dict[str, int] = defaultdict(int)
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            cup = (row.get("Pistas") or "Cup").strip()
            track = (row.get("Circuto") or "").strip()
            owner = (row.get("Dueño") or "").strip()
            state = (row.get("Seguro - Peligro") or "").strip()
            if not track:
                continue
            cups[cup].append({"track": track, "owner": owner, "state": state})
            if owner:
                totals[owner] += 1
    ordered_cups = [
        {"cup_name": cup, "tracks": tracks}
        for cup, tracks in sorted(cups.items(), key=lambda item: item[0])
    ]
    totals_list = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    return {"cups": ordered_cups, "totals": totals_list}


def resolve_season_id(entry: dict, db) -> int | None:
    if entry.get("season_id"):
        return entry["season_id"]
    if entry.get("season_label"):
        row = db.execute(
            "SELECT id FROM season_meta WHERE label = ?", (entry["season_label"],)
        ).fetchone()
        if row:
            return row["id"]
    return None


def build_csv_archive(entry: dict) -> dict:
    path = entry.get("file")
    if not path:
        return {
            "mode": "csv",
            "label": entry["label"],
            "notes": entry.get("notes"),
            "totals": [],
            "champions": [],
            "cups": [],
        }
    legacy = load_legacy_archive_data(path)
    totals_list = legacy.get("totals", [])
    champions = []
    if totals_list:
        top_count = totals_list[0][1]
        champions = [name for name, count in totals_list if count == top_count]
    return {
        "mode": "csv",
        "label": entry["label"],
        "notes": entry.get("notes"),
        "totals": totals_list,
        "champions": champions,
        "cups": legacy.get("cups", []),
    }


def build_db_archive(db, entry: dict) -> dict:
    season_id = resolve_season_id(entry, db)
    if not season_id:
        return {
            "mode": "db",
            "label": entry["label"],
            "notes": entry.get("notes"),
            "standings": [],
            "totals": [],
            "champions": [],
            "stats": None,
            "highlights": [],
        }
    standings = fetch_standings(db, season_id)
    decorate_standings_with_art(standings)
    totals = fetch_totals_overall(db, season_id)
    stats_payload = services_core.compute_stats_data(db, [season_id])
    champions = []
    if totals:
        top_count = totals[0][1]
        champions = [name for name, count in totals if count == top_count]

    def best_player(key, label, as_pct=False):
        valid = [p for p in stats_payload["player_stats"] if p[key] is not None]
        if not valid:
            return None
        best = max(valid, key=lambda p: p[key])
        val = best[key]
        display = f"{val:.1f}%" if as_pct else str(val)
        return {"label": label, "player": best["name"], "value": display}

    highlights = [
        h
        for h in [
            best_player("wins", "Most wins"),
            best_player("tracks_owned", "Tracks controlled"),
            best_player("defense_success_rate", "Best defense %", as_pct=True),
        ]
        if h
    ]
    return {
        "mode": "db",
        "label": entry["label"],
        "notes": entry.get("notes"),
        "standings": standings,
        "totals": totals,
        "champions": champions,
        "stats": stats_payload,
        "highlights": highlights,
    }


def build_archive_entries(db) -> list[dict]:
    today_iso = current_local_date().isoformat()
    entries = list(ARCHIVE_ENTRIES)
    skip_ids = {e.get("season_id") for e in entries if e.get("season_id")}
    skip_labels = {e.get("season_label") for e in entries if e.get("season_label")}
    rows = db.execute(
        """
        SELECT id, label, end_date
        FROM season_meta
        WHERE end_date <= ?
        ORDER BY end_date DESC, id DESC
        """,
        (today_iso,),
    ).fetchall()
    for row in rows:
        if row["id"] in skip_ids or row["label"] in skip_labels:
            continue
        entries.append(
            {
                "id": f"season{row['id']}",
                "label": row["label"],
                "type": "db",
                "season_id": row["id"],
                "notes": (
                    f"Archived automatically (ended {row['end_date']})" if row["end_date"] else None
                ),
            }
        )
    return entries
