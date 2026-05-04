import logging
from collections import defaultdict
from datetime import date, datetime
from zoneinfo import ZoneInfo

from flask import current_app

from koopakrew.services import core as services_core

logger = logging.getLogger(__name__)

_stats_cache: dict[tuple, dict] = {}


def _all_seasons_closed(db, season_ids: list[int]) -> bool:
    if not season_ids:
        return False
    tz_name = current_app.config.get("LOCAL_TIMEZONE", "America/Costa_Rica")
    today = datetime.now(ZoneInfo(tz_name)).date().isoformat()
    placeholders = ",".join("?" * len(season_ids))
    rows = db.execute(
        f"SELECT end_date FROM season_meta WHERE id IN ({placeholders})",
        season_ids,
    ).fetchall()
    if len(rows) != len(season_ids):
        return False
    return all(r["end_date"] and r["end_date"] <= today for r in rows)


def invalidate_stats_cache():
    """Clear the closed-season stats cache. Call after any player change."""
    _stats_cache.clear()


def get_stats(
    db,
    season_ids,
    sort_metric_id: str = "wins",
    selected_track_id: int | None = None,
    active_players_only: bool = False,
) -> dict:
    """Wrapper around services_core.compute_stats_data with closed-season caching.

    Results for seasons whose end_date is in the past are cached in-memory for
    the lifetime of the process. Cache is invalidated on any player change via
    invalidate_stats_cache().
    """
    ids = [season_ids] if isinstance(season_ids, int) else list(season_ids)
    cache_key = (frozenset(ids), sort_metric_id, selected_track_id, active_players_only)

    if cache_key in _stats_cache and _all_seasons_closed(db, ids):
        return _stats_cache[cache_key]

    result = services_core.compute_stats_data(
        db,
        ids,
        sort_metric_id,
        selected_track_id,
        active_players_only,
    )

    if _all_seasons_closed(db, ids):
        _stats_cache[cache_key] = result

    return result


def compute_player_streaks(db, season_id: int) -> dict[int, dict]:
    player_rows = db.execute("SELECT id, name FROM players").fetchall()
    names = {r["id"]: r["name"] for r in player_rows}

    def make_entry(pid):
        return {
            "name": names.get(pid, f"Player {pid}"),
            "current_win_streak": 0,
            "current_defense_streak": 0,
        }

    streaks: dict[int, dict] = {}

    def ensure(pid):
        if pid is None:
            return None
        if pid not in streaks:
            streaks[pid] = make_entry(pid)
        return streaks[pid]

    event_rows = db.execute(
        """
        SELECT e.*
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        WHERE t.season = ? AND e.is_sweep = 0
        ORDER BY e.occurred_at ASC, e.id ASC
        """,
        (season_id,),
    ).fetchall()

    for e in event_rows:
        winner_stats = ensure(e["winner_id"])
        pre_owner_stats = ensure(e["pre_owner_id"])

        if winner_stats:
            winner_stats["current_win_streak"] += 1

        if pre_owner_stats and e["pre_owner_id"] and e["pre_owner_id"] != e["winner_id"]:
            pre_owner_stats["current_win_streak"] = 0
            pre_owner_stats["current_defense_streak"] = 0

        if (
            pre_owner_stats
            and e["pre_owner_id"]
            and e["winner_id"] == e["pre_owner_id"]
            and e["post_owner_id"] == e["pre_owner_id"]
        ):
            pre_owner_stats["current_defense_streak"] += 1

    return streaks


def build_player_highlights(db, season_id: int) -> dict[str, dict]:
    stats_payload = services_core.compute_stats_data(db, [season_id], "wins", None)
    highlights: dict[str, dict] = {}
    highlights_by_id: dict[int, dict] = {}

    def format_capture_timestamp(value: str | None) -> str:
        if not value:
            return ""
        try:
            dt = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            return value or ""
        tz_name = current_app.config.get("LOCAL_TIMEZONE", "America/Costa_Rica")
        try:
            localized = dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            localized = dt
        return localized.strftime("%Y-%m-%d %H:%M")

    for ps in stats_payload.get("player_stats", []):
        entry = {
            "risk_rate": ps.get("defense_at_risk_rate"),
            "win_rate": ps.get("win_rate"),
            "tracks_taken": ps.get("tracks_taken"),
            "current_win_streak": ps.get("current_win_streak"),
            "locks_applied": ps.get("locks_applied"),
            "wins": ps.get("wins"),
            "last_capture": None,
            "last_result": None,
        }
        highlights[ps["name"]] = entry
        highlights_by_id[ps["id"]] = entry

    if not highlights_by_id:
        return highlights

    recent_rows = db.execute(
        """
        SELECT
            e.winner_id, e.pre_owner_id, e.post_owner_id, e.occurred_at,
            t.en AS track_en, t.es AS track_es, c.en AS cup_en, c.es AS cup_es
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        JOIN cups c ON c.id = t.cup_id
        WHERE t.season = ? AND e.is_sweep = 0
        ORDER BY e.occurred_at DESC, e.id DESC
        """,
        (season_id,),
    ).fetchall()

    for row in recent_rows:
        participants = []
        if row["winner_id"]:
            participants.append(row["winner_id"])
        if row["pre_owner_id"]:
            participants.append(row["pre_owner_id"])
        seen_ids: set[int] = set()
        for pid in participants:
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            entry = highlights_by_id.get(pid)
            if not entry or entry.get("last_result"):
                continue
            if pid == row["winner_id"] and pid == row["pre_owner_id"]:
                outcome, role = "defended", "owner"
            elif pid == row["winner_id"]:
                outcome, role = "captured", "challenger"
            elif pid == row["pre_owner_id"]:
                role = "owner"
                outcome = "saved" if row["post_owner_id"] == pid else "lost"
            else:
                continue
            entry["last_result"] = {
                "track_en": row["track_en"],
                "track_es": row["track_es"],
                "cup_en": row["cup_en"],
                "cup_es": row["cup_es"],
                "occurred_at": format_capture_timestamp(row["occurred_at"]),
                "outcome": outcome,
                "role": role,
            }

    capture_rows = db.execute(
        """
        SELECT
            e.winner_id, e.pre_owner_id, e.post_owner_id, e.occurred_at,
            t.en AS track_en, t.es AS track_es, c.en AS cup_en, c.es AS cup_es
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        JOIN cups c ON c.id = t.cup_id
        WHERE t.season = ? AND e.is_sweep = 0 AND e.winner_id IS NOT NULL
        ORDER BY e.occurred_at DESC, e.id DESC
        """,
        (season_id,),
    ).fetchall()

    for row in capture_rows:
        winner_id = row["winner_id"]
        entry = highlights_by_id.get(winner_id)
        if not entry or entry.get("last_capture"):
            continue
        if row["post_owner_id"] != winner_id or row["pre_owner_id"] == winner_id:
            continue
        entry["last_capture"] = {
            "track_en": row["track_en"],
            "track_es": row["track_es"],
            "cup_en": row["cup_en"],
            "cup_es": row["cup_es"],
            "occurred_at": format_capture_timestamp(row["occurred_at"]),
        }

    return highlights
