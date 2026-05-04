"""
Background worker that pre-generates AI profiles for all players sequentially.

Runs once at startup in a daemon thread (if GEMINI_API_KEY is set), then
re-checks every POLL_INTERVAL seconds in case new events make profiles stale.

Rate-limiting: INTER_CALL_DELAY seconds between each player's generation to
stay comfortably under Gemini Flash free tier (15 RPM).
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)

INTER_REQUEST_DELAY = 15.0  # seconds between the 2 Gemini calls per player
INTER_PLAYER_DELAY = 15.0   # additional seconds after each player (~2 RPM, well under free tier 15 RPM)
STARTUP_DELAY = 30.0        # wait for any pending rate-limit windows to clear
POLL_INTERVAL = 1800.0      # re-check for stale profiles every 30 minutes


def _run_once(app) -> int:
    """Generate all stale profiles. Returns the number of profiles generated."""
    generated = 0
    with app.app_context():
        from koopakrew.db import get_db
        from koopakrew.queries.seasons import get_current_season_row
        from koopakrew.services.ai_profiles import get_or_generate_profile, _current_event_count
        from koopakrew.stats.attributes import compute_profile_attributes
        from koopakrew.stats.compute import get_stats
        from flask import current_app

        gemini_key = current_app.config.get("GEMINI_API_KEY")
        mistral_key = current_app.config.get("MISTRAL_API_KEY")
        if not gemini_key and not mistral_key:
            return 0

        db = get_db()

        season_meta_rows = db.execute(
            "SELECT id, label FROM season_meta ORDER BY start_date DESC"
        ).fetchall()
        all_season_labels = [r["label"] for r in season_meta_rows]
        all_season_ids = [r["id"] for r in season_meta_rows]

        season_row = get_current_season_row(db)
        if not season_row:
            return 0
        season_id = season_row["id"]

        players = db.execute(
            "SELECT id, name FROM players WHERE active = 1 ORDER BY id"
        ).fetchall()

        stats_data = get_stats(db, [season_id], "wins", None, active_players_only=False)
        all_player_stats = stats_data.get("player_stats") or []

        clutch_rows = db.execute(
            """
            SELECT e.winner_id, e.pre_owner_id, e.pre_state, e.pre_threatened_by_id
            FROM events e JOIN tracks t ON t.id = e.track_id
            WHERE t.season = ? AND e.is_sweep = 0
            ORDER BY e.occurred_at ASC, e.id ASC
            """,
            (season_id,),
        ).fetchall()
        clutch_events = [dict(r) for r in clutch_rows]

        for player_row in players:
            player_id = player_row["id"]
            player_name = player_row["name"]

            # Check if this player actually needs generation before sleeping
            cached = db.execute(
                "SELECT events_count_at_generation, regen_threshold "
                "FROM player_profiles WHERE player_id = ?",
                (player_id,),
            ).fetchone()
            current_count = _current_event_count(db, player_id)

            if cached:
                delta = current_count - cached["events_count_at_generation"]
                if delta < cached["regen_threshold"]:
                    continue  # still fresh, skip

            logger.info("profile_worker: generating profile for %s (season %d)", player_name, season_id)

            # Build attribute scores for the prompt
            player_stats = next(
                (ps for ps in all_player_stats if ps["id"] == player_id),
                {"id": player_id, "name": player_name},
            )
            last10_rows = db.execute(
                """
                SELECT e.winner_id, e.pre_owner_id
                FROM events e JOIN tracks t ON t.id = e.track_id
                WHERE t.season = ? AND e.is_sweep = 0
                  AND (e.winner_id = ? OR e.pre_owner_id = ?)
                ORDER BY e.occurred_at DESC, e.id DESC LIMIT 10
                """,
                (season_id, player_id, player_id),
            ).fetchall()
            total_events = db.execute(
                "SELECT COUNT(*) AS n FROM events e "
                "JOIN tracks t ON t.id=e.track_id WHERE t.season=? AND e.is_sweep=0",
                (season_id,),
            ).fetchone()["n"]
            player_events = db.execute(
                "SELECT COUNT(*) AS n FROM events e "
                "JOIN tracks t ON t.id=e.track_id "
                "WHERE t.season=? AND e.is_sweep=0 AND (e.winner_id=? OR e.pre_owner_id=?)",
                (season_id, player_id, player_id),
            ).fetchone()["n"]

            attributes = compute_profile_attributes(
                player_id=player_id,
                player_stats=player_stats,
                all_player_stats=all_player_stats,
                clutch_events=clutch_events,
                last_10_events=[dict(r) for r in last10_rows],
                presence_player_events=player_events,
                presence_total_events=total_events,
            )

            all_time_data = get_stats(db, all_season_ids, "wins", None, active_players_only=False)
            all_time_ps = next(
                (ps for ps in (all_time_data.get("player_stats") or []) if ps["id"] == player_id),
                player_stats,
            )

            result = get_or_generate_profile(
                db=db,
                player_id=player_id,
                player_name=player_name,
                current_season_stats=player_stats,
                current_season_label=season_row["label"],
                all_time_stats=all_time_ps,
                season_labels=all_season_labels,
                attribute_scores=attributes["scores"],
                gemini_key=gemini_key,
                mistral_key=mistral_key,
                inter_request_delay=INTER_REQUEST_DELAY,
            )

            if result.get("generated"):
                generated += 1
                logger.info("profile_worker: generated profile for %s", player_name)
            elif not result.get("from_cache"):
                logger.warning("profile_worker: generation failed for %s — will retry next cycle", player_name)

            time.sleep(INTER_PLAYER_DELAY)

    return generated


def start(app) -> None:
    """Start the background profile-generation daemon thread."""

    def _loop():
        time.sleep(STARTUP_DELAY)
        while True:
            try:
                n = _run_once(app)
                if n:
                    logger.info("profile_worker: generated %d profile(s) this cycle", n)
            except Exception:
                logger.exception("profile_worker: unexpected error")
            time.sleep(POLL_INTERVAL)

    t = threading.Thread(target=_loop, name="profile-worker", daemon=True)
    t.start()
    logger.info(
        "profile_worker: started (startup delay %.0fs, %.0fs between requests, %.0fs between players, poll every %.0fs)",
        STARTUP_DELAY, INTER_REQUEST_DELAY, INTER_PLAYER_DELAY, POLL_INTERVAL,
    )
