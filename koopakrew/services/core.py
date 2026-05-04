import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict
from zoneinfo import ZoneInfo

from flask import current_app, has_app_context

logger = logging.getLogger(__name__)

_BOUND_APP = None


def bind_app(app):
    """Remember the original Flask app for config lookups outside contexts."""
    global _BOUND_APP
    _BOUND_APP = app


def _config_value(key, default=None):
    if has_app_context():
        return current_app.config.get(key, default)
    if _BOUND_APP is not None:
        return _BOUND_APP.config.get(key, default)
    return default


class PlayerStatsDict(TypedDict):
    id: int
    name: str
    tracks_owned: int
    locked_tracks: int
    locks_applied: int
    wins: int
    wins_as_owner: int
    wins_as_non_owner: int
    sweeps: int
    cups_owned_count: int
    tracks_taken: int
    tracks_lost: int
    net_tracks: int
    races_as_owner: int
    defense_successes: int
    defense_success_rate: float | None
    defense_at_risk_attempts: int
    defense_at_risk_successes: int
    defense_at_risk_rate: float | None
    wins_on_risk: int
    steals_from_risk: int
    hunter_marks: int
    wins_with_hunter_mark: int
    races_played: int
    win_rate: float | None
    current_win_streak: int
    best_win_streak: int
    current_defense_streak: int
    best_defense_streak: int


@dataclass
class TrackPlayerStats:
    name: str = ""
    wins: int = 0
    defenses: int = 0
    takes: int = 0
    losses: int = 0


METRIC_DEFS = [
    {
        "id": "tracks_owned",
        "label": "Tracks",
        "type": "value",
        "value_key": "tracks_owned",
        "group": "control",
        "sort_mode": "value",
        "help": "Tracks currently controlled.",
    },
    {
        "id": "locked_tracks",
        "label": "Locked",
        "type": "value",
        "value_key": "locked_tracks",
        "group": "control",
        "sort_mode": "value",
        "help": "Owned tracks that are locked.",
    },
    {
        "id": "locks_applied",
        "label": "Locks applied",
        "type": "value",
        "value_key": "locks_applied",
        "group": "control",
        "sort_mode": "value",
        "help": "Times you locked a track (sweeps or owner wins on default).",
    },
    {
        "id": "cups_owned_count",
        "label": "Cups owned",
        "type": "value",
        "value_key": "cups_owned_count",
        "group": "control",
        "sort_mode": "value",
        "help": "Cups where you currently hold every track.",
    },
    {
        "id": "races_played",
        "label": "Tracked races",
        "type": "value",
        "value_key": "races_played",
        "group": "performance",
        "sort_mode": "value",
        "help": "Races where the system recorded you as owner or winner.",
    },
    {
        "id": "wins",
        "label": "Wins",
        "type": "value",
        "value_key": "wins",
        "group": "performance",
        "sort_mode": "value",
        "help": "Total race wins.",
    },
    {
        "id": "win_rate",
        "label": "Win %",
        "type": "percent",
        "value_key": "win_rate",
        "group": "performance",
        "sort_mode": "value",
        "help": "Recorded wins divided by tracked races (owner/winner only).",
    },
    {
        "id": "wins_as_owner",
        "label": "Owner wins",
        "type": "value",
        "value_key": "wins_as_owner",
        "group": "performance",
        "sort_mode": "value",
        "help": "Wins while already owning the track.",
    },
    {
        "id": "wins_as_non_owner",
        "label": "Challenger wins",
        "type": "value",
        "value_key": "wins_as_non_owner",
        "group": "performance",
        "sort_mode": "value",
        "help": "Wins taken as challenger.",
    },
    {
        "id": "sweeps",
        "label": "Cup sweeps",
        "type": "value",
        "value_key": "sweeps",
        "group": "performance",
        "sort_mode": "value",
        "help": "Total cup sweeps triggered.",
    },
    {
        "id": "tracks_taken",
        "label": "Tracks taken",
        "type": "value",
        "value_key": "tracks_taken",
        "group": "performance",
        "sort_mode": "value",
        "help": "Tracks gained from others.",
    },
    {
        "id": "tracks_lost",
        "label": "Tracks lost",
        "type": "value",
        "value_key": "tracks_lost",
        "group": "performance",
        "sort_mode": "value",
        "help": "Tracks lost to challengers.",
    },
    {
        "id": "net_tracks",
        "label": "Net gain",
        "type": "value",
        "value_key": "net_tracks",
        "group": "performance",
        "sort_mode": "value",
        "help": "Tracks taken minus lost.",
    },
    {
        "id": "best_win_streak",
        "label": "🔥 Longest hot hand",
        "type": "value",
        "value_key": "best_win_streak",
        "group": "performance",
        "sort_mode": "value",
        "help": "Best win streak recorded this season.",
    },
    {
        "id": "best_defense_streak",
        "label": "🛡️ Longest shield wall",
        "type": "value",
        "value_key": "best_defense_streak",
        "group": "performance",
        "sort_mode": "value",
        "help": "Best defense streak recorded this season.",
    },
    {
        "id": "wins_on_risk",
        "label": "Risk wins",
        "type": "value",
        "value_key": "wins_on_risk",
        "group": "risk",
        "sort_mode": "value",
        "help": "Wins on tracks that began the race at risk.",
    },
    {
        "id": "steals_from_risk",
        "label": "Risk steals",
        "type": "value",
        "value_key": "steals_from_risk",
        "group": "risk",
        "sort_mode": "value",
        "help": "At-risk wins that stole the track.",
    },
    {
        "id": "hunter_marks",
        "label": "Hunter marks",
        "type": "value",
        "value_key": "hunter_marks",
        "group": "risk",
        "sort_mode": "value",
        "help": "Times you marked someone at risk.",
    },
    {
        "id": "wins_with_hunter_mark",
        "label": "Hunter closes",
        "type": "value",
        "value_key": "wins_with_hunter_mark",
        "group": "risk",
        "sort_mode": "value",
        "help": "Wins when already tagged as hunter.",
    },
    {
        "id": "defense_succ_att",
        "label": "Defense saves",
        "type": "pair",
        "num_key": "defense_successes",
        "den_key": "races_as_owner",
        "group": "defense",
        "sort_mode": "ratio",
        "help": "Successful defenses vs. owner races.",
    },
    {
        "id": "defense_success_rate",
        "label": "Defense %",
        "type": "percent",
        "value_key": "defense_success_rate",
        "group": "defense",
        "sort_mode": "value",
        "help": "Defense success rate on owner races.",
    },
    {
        "id": "risk_defense_pair",
        "label": "At-risk saves",
        "type": "pair",
        "num_key": "defense_at_risk_successes",
        "den_key": "defense_at_risk_attempts",
        "group": "defense",
        "sort_mode": "ratio",
        "help": "Successful saves when the track was already at risk.",
    },
    {
        "id": "defense_at_risk_rate",
        "label": "At-risk %",
        "type": "percent",
        "value_key": "defense_at_risk_rate",
        "group": "defense",
        "sort_mode": "value",
        "help": "Defense success rate on at-risk tracks.",
    },
]


def apply_result(db, season_id, track_id, winner_id):
    # Load pre-state
    row = db.execute(
        "SELECT * FROM tracks WHERE id = ? AND season = ?", (track_id, season_id)
    ).fetchone()
    if not row:
        raise ValueError("Track not found for this season")

    pre_owner_id = row["owner_id"]
    pre_state = row["state"]
    pre_threat_id = row["threatened_by_id"]
    cup_id = row["cup_id"]

    # --- Pre-counts BEFORE the update (for sweep detection) ---
    pre_count_winner = (
        db.execute(
            "SELECT COUNT(*) AS n FROM tracks WHERE season = ? AND cup_id = ? AND owner_id = ?",
            (season_id, cup_id, winner_id),
        ).fetchone()["n"]
        if winner_id
        else 0
    )

    pre_count_prev_owner = (
        db.execute(
            "SELECT COUNT(*) AS n FROM tracks WHERE season = ? AND cup_id = ? AND owner_id = ?",
            (season_id, cup_id, pre_owner_id),
        ).fetchone()["n"]
        if pre_owner_id
        else 0
    )

    # --- Race transition (pre -> post) ---
    post_owner_id = pre_owner_id
    post_state = pre_state
    post_threat_id = pre_threat_id

    if pre_owner_id is None:
        # Season start: immediate claim
        post_owner_id = winner_id
        post_state = 0
        post_threat_id = None
    else:
        if pre_owner_id == winner_id:
            # Owner won
            if pre_state == -1:
                post_state = 0
                post_threat_id = None
            else:
                post_state = 1
                post_threat_id = None
        else:
            # Challenger won
            if pre_state == 1:
                post_state = 0
                post_threat_id = None
            elif pre_state == 0:
                post_state = -1
                post_threat_id = winner_id  # cosmetic mark
            elif pre_state == -1:
                post_owner_id = winner_id  # free-for-all claim
                post_state = 0
                post_threat_id = None

    tz_name = _config_value("LOCAL_TIMEZONE", "America/Costa_Rica")
    occurred_at = datetime.now(ZoneInfo(tz_name)).isoformat(timespec="seconds")

    # --- Write normal race event (is_sweep=0) ---
    cur = db.execute(
        """
        INSERT INTO events
          (track_id, winner_id, occurred_at,
           pre_owner_id, pre_state, pre_threatened_by_id,
           post_owner_id, post_state, post_threatened_by_id,
           side_effects_json, is_sweep, sweep_cup_id, sweep_owner_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, NULL, NULL)
        """,
        (
            track_id,
            winner_id,
            occurred_at,
            pre_owner_id,
            pre_state,
            pre_threat_id,
            post_owner_id,
            post_state,
            post_threat_id,
        ),
    )
    event_id = cur.lastrowid

    # --- Apply the track change once ---
    db.execute(
        "UPDATE tracks SET owner_id = ?, state = ?, threatened_by_id = ? WHERE id = ?",
        (post_owner_id, post_state, post_threat_id, track_id),
    )

    # --- Sweep detection: did the post-owner cross from <4 to 4? ---
    post_owner_now = post_owner_id
    if post_owner_now:
        post_count_for_post_owner = db.execute(
            "SELECT COUNT(*) AS n FROM tracks WHERE season = ? AND cup_id = ? AND owner_id = ?",
            (season_id, cup_id, post_owner_now),
        ).fetchone()["n"]

        pre_count_for_post_owner = (
            pre_count_winner if post_owner_now == winner_id else pre_count_prev_owner
        )

        if pre_count_for_post_owner < 4 and post_count_for_post_owner == 4:
            # Lock any of that owner's 4 that aren't locked yet, record side-effects
            affected = db.execute(
                """
                SELECT id, state, threatened_by_id
                FROM tracks
                WHERE season = ? AND cup_id = ? AND owner_id = ?
                """,
                (season_id, cup_id, post_owner_now),
            ).fetchall()

            to_lock_ids = [r["id"] for r in affected if r["state"] != 1]
            if to_lock_ids:
                side_effects = []
                for r in affected:
                    if r["id"] in to_lock_ids:
                        side_effects.append(
                            {
                                "track_id": r["id"],
                                "pre_state": r["state"],
                                "pre_threatened_by_id": r["threatened_by_id"],
                                "post_state": 1,
                                "post_threatened_by_id": None,
                            }
                        )
                qmarks = ",".join("?" for _ in to_lock_ids)
                db.execute(
                    f"UPDATE tracks SET state = 1, threatened_by_id = NULL WHERE id IN ({qmarks})",
                    to_lock_ids,
                )
                db.execute(
                    """
                    INSERT INTO events
                      (track_id, winner_id, occurred_at,
                       pre_owner_id, pre_state, pre_threatened_by_id,
                       post_owner_id, post_state, post_threatened_by_id,
                       side_effects_json, is_sweep, sweep_cup_id, sweep_owner_id)
                    VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, 1, ?, ?)
                    """,
                    (
                        track_id,
                        post_owner_now,
                        occurred_at,
                        json.dumps(side_effects),
                        cup_id,
                        post_owner_now,
                    ),
                )

    db.commit()
    return event_id


def undo_last_event(db) -> bool:
    """Undo the most recent event atomically. Returns True on success, False if nothing to undo.

    All DB operations run inside a single transaction. Any error (including malformed
    side_effects_json or a post-undo state mismatch) triggers a full rollback so no
    partial state is ever committed.
    """
    try:
        result = _undo_step(db)
        db.commit()
        return result
    except Exception as exc:
        logger.error("Undo failed, rolling back: %s", exc)
        db.rollback()
        return False


def _undo_step(db) -> bool:
    ev = db.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
    if not ev:
        return False

    side_json = ev["side_effects_json"]
    deactivate_payload = None
    side_effect_list: list = []
    if side_json:
        try:
            effects = json.loads(side_json)
            if isinstance(effects, dict) and effects.get("action") == "deactivate_player":
                deactivate_payload = effects
            elif isinstance(effects, list):
                side_effect_list = effects
            else:
                raise ValueError(f"Unexpected side_effects_json shape: {type(effects)}")
        except (json.JSONDecodeError, ValueError) as exc:
            if ev["is_sweep"]:
                raise RuntimeError(
                    f"Cannot safely undo sweep: malformed side_effects_json on event {ev['id']}: {exc}"
                ) from exc
            logger.warning(
                "Malformed side_effects_json on event %s, treating as empty: %s", ev["id"], exc
            )

    # Restore tracks that were side-effected (e.g. locked by a sweep event)
    for eff in side_effect_list:
        track_id = eff.get("track_id")
        if not track_id:
            continue
        if "pre_owner_id" in eff:
            db.execute(
                "UPDATE tracks SET owner_id = ? WHERE id = ?",
                (eff.get("pre_owner_id"), track_id),
            )
        db.execute(
            """
            UPDATE tracks
            SET state = COALESCE(?, 0),
                threatened_by_id = ?
            WHERE id = ?
            """,
            (eff.get("pre_state", 0), eff.get("pre_threatened_by_id"), track_id),
        )

    if deactivate_payload:
        for eff in deactivate_payload.get("tracks", []):
            track_id = eff.get("track_id")
            if not track_id:
                continue
            db.execute(
                """
                UPDATE tracks
                SET owner_id = ?,
                    state = COALESCE(?, 0),
                    threatened_by_id = ?
                WHERE id = ?
                """,
                (
                    eff.get("pre_owner_id"),
                    eff.get("pre_state", 0),
                    eff.get("pre_threatened_by_id"),
                    track_id,
                ),
            )
        player_id = deactivate_payload.get("player_id")
        if player_id:
            db.execute("UPDATE players SET active = 1 WHERE id = ?", (player_id,))
        db.execute("DELETE FROM events WHERE id = ?", (ev["id"],))
        return True

    # Sweep event: delete it and recurse to undo the race that triggered it.
    # No commit here — both deletions happen in the same outer transaction.
    if ev["is_sweep"]:
        db.execute("DELETE FROM events WHERE id = ?", (ev["id"],))
        return _undo_step(db)

    # Normal race: restore the main track's pre-state
    db.execute(
        "UPDATE tracks SET owner_id = ?, state = COALESCE(?, 0), threatened_by_id = ? WHERE id = ?",
        (ev["pre_owner_id"], ev["pre_state"], ev["pre_threatened_by_id"], ev["track_id"]),
    )
    db.execute("DELETE FROM events WHERE id = ?", (ev["id"],))

    # Verify the restored state matches the snapshot before committing
    if ev["track_id"] is not None:
        restored = db.execute(
            "SELECT owner_id, state, threatened_by_id FROM tracks WHERE id = ?",
            (ev["track_id"],),
        ).fetchone()
        expected_state = ev["pre_state"] if ev["pre_state"] is not None else 0
        if restored and (
            restored["owner_id"] != ev["pre_owner_id"]
            or restored["state"] != expected_state
            or restored["threatened_by_id"] != ev["pre_threatened_by_id"]
        ):
            raise AssertionError(
                f"Post-undo state mismatch on track {ev['track_id']}: "
                f"got ({restored['owner_id']}, {restored['state']}, {restored['threatened_by_id']}) "
                f"expected ({ev['pre_owner_id']}, {expected_state}, {ev['pre_threatened_by_id']})"
            )

    return True


def deactivate_player(db, player_id: int) -> bool:
    row = db.execute("SELECT id, name, active FROM players WHERE id = ?", (player_id,)).fetchone()
    if not row or row["active"] == 0:
        return False
    track_rows = db.execute(
        "SELECT id, state, threatened_by_id FROM tracks WHERE owner_id = ?",
        (player_id,),
    ).fetchall()
    payload = {
        "action": "deactivate_player",
        "player_id": player_id,
        "tracks": [
            {
                "track_id": tr["id"],
                "pre_owner_id": player_id,
                "pre_state": tr["state"],
                "pre_threatened_by_id": tr["threatened_by_id"],
            }
            for tr in track_rows
        ],
    }
    tz_name = _config_value("LOCAL_TIMEZONE", "America/Costa_Rica")
    occurred_at = datetime.now(ZoneInfo(tz_name)).isoformat(timespec="seconds")
    db.execute(
        """
        INSERT INTO events
          (track_id, winner_id, occurred_at,
           pre_owner_id, pre_state, pre_threatened_by_id,
           post_owner_id, post_state, post_threatened_by_id,
           side_effects_json, is_sweep, sweep_cup_id, sweep_owner_id)
        VALUES (NULL, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, 0, NULL, NULL)
        """,
        (player_id, occurred_at, json.dumps(payload)),
    )
    db.execute(
        "UPDATE tracks SET owner_id = NULL, state = 0, threatened_by_id = NULL WHERE owner_id = ?",
        (player_id,),
    )
    db.execute("UPDATE players SET active = 0 WHERE id = ?", (player_id,))
    db.commit()
    return True


def compute_stats_data(
    db, season_ids, sort_metric_id="wins", selected_track_id=None, active_players_only=False
):
    if isinstance(season_ids, int):
        season_ids = [season_ids]
    season_ids = [sid for sid in season_ids if sid is not None]
    if not season_ids:
        return {
            "player_stats": [],
            "track_activity": [],
            "most_defended": None,
            "most_contested": None,
            "track_selector": [],
            "selected_track": None,
            "track_player_stats": [],
            "track_events": [],
            "metric_rows": [],
            "streak_badges": {},
            "player_spotlights": [],
            "track_insights_enabled": False,
        }
    season_clause = ",".join("?" for _ in season_ids)
    allow_track_insights = True
    player_rows = db.execute("SELECT id, name, active FROM players ORDER BY name").fetchall()
    player_meta = {r["id"]: {"name": r["name"], "active": bool(r["active"])} for r in player_rows}
    player_names = {pid: meta["name"] for pid, meta in player_meta.items()}

    def make_empty_player_stats(pid: int, name: str) -> PlayerStatsDict:
        return {
            "id": pid,
            "name": name,
            "tracks_owned": 0,
            "locked_tracks": 0,
            "locks_applied": 0,
            "wins": 0,
            "wins_as_owner": 0,
            "wins_as_non_owner": 0,
            "sweeps": 0,
            "cups_owned_count": 0,
            "tracks_taken": 0,
            "tracks_lost": 0,
            "net_tracks": 0,
            "races_as_owner": 0,
            "defense_successes": 0,
            "defense_success_rate": None,
            "defense_at_risk_attempts": 0,
            "defense_at_risk_successes": 0,
            "defense_at_risk_rate": None,
            "wins_on_risk": 0,
            "steals_from_risk": 0,
            "hunter_marks": 0,
            "wins_with_hunter_mark": 0,
            "races_played": 0,
            "win_rate": None,
            "current_win_streak": 0,
            "best_win_streak": 0,
            "current_defense_streak": 0,
            "best_defense_streak": 0,
            "current_mark_targets": 0,
        }

    stats = {pid: make_empty_player_stats(pid, name) for pid, name in player_names.items()}

    def ensure_player(pid: int | None):
        if pid is None:
            return None
        if pid not in stats:
            fallback_name = player_names.get(pid, f"Player {pid}")
            stats[pid] = make_empty_player_stats(pid, fallback_name)
            if pid not in player_names:
                player_names[pid] = fallback_name
            player_meta.setdefault(pid, {"name": fallback_name, "active": True})
        return stats[pid]

    track_rows = db.execute(
        f"""
        SELECT t.id, t.code AS track_code, t.en AS track_en, t.es AS track_es,
               c.id AS cup_id, c.code AS cup_code,
               c.en AS cup_en, c.es AS cup_es,
               c."order" AS cup_order, t.order_in_cup,
               t.owner_id AS final_owner_id,
               t.state AS final_state,
               t.threatened_by_id AS final_threat,
               t.season AS season_id
        FROM tracks t
        JOIN cups c ON c.id = t.cup_id
        WHERE t.season IN ({season_clause})
        """,
        season_ids,
    ).fetchall()

    track_info = {}
    cup_track_totals = defaultdict(int)
    cup_owner_counts = defaultdict(lambda: defaultdict(int))
    cup_entries = {}
    cup_meta_by_id = {}
    for r in track_rows:
        tid = r["id"]
        track_info[tid] = {
            "track_code": r["track_code"],
            "track_en": r["track_en"],
            "track_es": r["track_es"],
            "cup_id": r["cup_id"],
            "cup_code": r["cup_code"],
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
            "cup_order": r["cup_order"],
            "order_in_cup": r["order_in_cup"],
            "season_id": r["season_id"],
            "final_owner_id": r["final_owner_id"],
            "final_state": r["final_state"],
            "final_threat": r["final_threat"],
        }

        owner_id = r["final_owner_id"]
        state = r["final_state"]
        threat = r["final_threat"]
        cup_key = (r["season_id"], r["cup_id"])
        cup_track_totals[cup_key] += 1
        cup_entries[cup_key] = {
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
            "cup_code": r["cup_code"],
            "season_id": r["season_id"],
            "cup_id": r["cup_id"],
        }
        cup_meta_by_id[r["cup_id"]] = {
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
            "cup_code": r["cup_code"],
        }
        if threat:
            ensure_player(threat)
        if threat and state == -1:
            ps_mark = ensure_player(threat)
            if ps_mark:
                ps_mark["current_mark_targets"] += 1
        if owner_id is not None:
            ps = ensure_player(owner_id)
            if ps:
                ps["tracks_owned"] += 1
                if state == 1:
                    ps["locked_tracks"] += 1
                cup_owner_counts[cup_key][owner_id] += 1

    cup_ownerships = defaultdict(list)
    for cup_key, owners in cup_owner_counts.items():
        total = cup_track_totals.get(cup_key)
        if not total:
            continue
        for owner_id, count in owners.items():
            if count == total:
                info = cup_entries.get(cup_key)
                if info:
                    cup_ownerships[owner_id].append(info)

    track_race_count = defaultdict(int)
    track_ownership_changes = defaultdict(int)
    track_defenses = defaultdict(int)
    player_track_profiles = defaultdict(
        lambda: defaultdict(lambda: {"wins": 0, "defenses": 0, "attacks": 0})
    )
    player_cup_wins = defaultdict(lambda: defaultdict(int))

    sweep_rows = db.execute(
        f"""
        SELECT e.sweep_owner_id, e.side_effects_json
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        WHERE t.season IN ({season_clause}) AND e.is_sweep = 1
        """,
        season_ids,
    ).fetchall()
    for r in sweep_rows:
        sweeper_id = r["sweep_owner_id"]
        ps = ensure_player(sweeper_id)
        if ps:
            ps["sweeps"] += 1
            side_json = r["side_effects_json"]
            lock_count = 0
            if side_json:
                try:
                    effects = json.loads(side_json)
                except Exception:
                    effects = []
                if isinstance(effects, list):
                    for eff in effects:
                        if not isinstance(eff, dict):
                            continue
                        if eff.get("post_state") == 1 and eff.get("pre_state") != 1:
                            lock_count += 1
            ps["locks_applied"] += lock_count

    event_rows = db.execute(
        f"""
        SELECT e.*, t.id AS track_id, t.season AS track_season
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        WHERE t.season IN ({season_clause}) AND e.is_sweep = 0
        ORDER BY t.season ASC, e.occurred_at ASC, e.id ASC
        """,
        season_ids,
    ).fetchall()

    for e in event_rows:
        track_id = e["track_id"]
        winner_id = e["winner_id"]
        pre_owner_id = e["pre_owner_id"]
        post_owner_id = e["post_owner_id"]
        pre_state = e["pre_state"]
        post_state = e["post_state"]
        pre_threat = e["pre_threatened_by_id"]
        track_meta = track_info.get(track_id)
        cup_id = track_meta["cup_id"] if track_meta else None

        track_race_count[track_id] += 1

        winner_stats = ensure_player(winner_id)
        pre_owner_stats = ensure_player(pre_owner_id)
        post_owner_stats = ensure_player(post_owner_id)

        if winner_stats:
            winner_stats["wins"] += 1
            winner_stats["races_played"] += 1
            if pre_owner_id == winner_id:
                winner_stats["wins_as_owner"] += 1
            else:
                winner_stats["wins_as_non_owner"] += 1
            winner_stats["current_win_streak"] += 1
            if winner_stats["current_win_streak"] > winner_stats["best_win_streak"]:
                winner_stats["best_win_streak"] = winner_stats["current_win_streak"]
            if (
                pre_owner_id == winner_id
                and pre_state == 0
                and post_state == 1
                and post_owner_id == winner_id
            ):
                winner_stats["locks_applied"] += 1
        if winner_id:
            profile = player_track_profiles[winner_id][track_id]
            profile["wins"] += 1
            if pre_owner_id == winner_id:
                profile["defenses"] += 1
            elif pre_owner_id:
                profile["attacks"] += 1
            if cup_id:
                player_cup_wins[winner_id][cup_id] += 1

        if pre_owner_stats:
            pre_owner_stats["races_played"] += 1
            pre_owner_stats["races_as_owner"] += 1

        if pre_owner_id and post_owner_id and pre_owner_id != post_owner_id:
            track_ownership_changes[track_id] += 1
            if pre_owner_stats:
                pre_owner_stats["tracks_lost"] += 1
            if post_owner_stats:
                post_owner_stats["tracks_taken"] += 1

        if (
            pre_owner_id
            and winner_id == pre_owner_id
            and post_owner_id == pre_owner_id
            and pre_owner_stats
        ):
            pre_owner_stats["defense_successes"] += 1
            pre_owner_stats["current_defense_streak"] += 1
            if pre_owner_stats["current_defense_streak"] > pre_owner_stats["best_defense_streak"]:
                pre_owner_stats["best_defense_streak"] = pre_owner_stats["current_defense_streak"]
            track_defenses[track_id] += 1

        if pre_owner_id and pre_state == -1 and pre_owner_stats:
            pre_owner_stats["defense_at_risk_attempts"] += 1
            if winner_id == pre_owner_id and post_owner_id == pre_owner_id:
                pre_owner_stats["defense_at_risk_successes"] += 1

        if winner_id and pre_state == -1 and winner_stats:
            winner_stats["wins_on_risk"] += 1
            if pre_owner_id and winner_id != pre_owner_id:
                winner_stats["steals_from_risk"] += 1

        if (
            winner_id
            and pre_state == 0
            and pre_owner_id
            and winner_id != pre_owner_id
            and post_state == -1
            and winner_stats
        ):
            winner_stats["hunter_marks"] += 1

        if winner_id and pre_state == -1 and pre_threat == winner_id and winner_stats:
            winner_stats["wins_with_hunter_mark"] += 1

        if pre_owner_stats and pre_owner_id and pre_owner_id != winner_id:
            pre_owner_stats["current_win_streak"] = 0
            pre_owner_stats["current_defense_streak"] = 0

    for ps in stats.values():
        pid = ps["id"]
        ps["net_tracks"] = ps["tracks_taken"] - ps["tracks_lost"]
        ps["cups_owned_count"] = len(cup_ownerships.get(pid, []))

        if ps["races_as_owner"] > 0:
            ps["defense_success_rate"] = ps["defense_successes"] / ps["races_as_owner"] * 100.0
        else:
            ps["defense_success_rate"] = None

        if ps["defense_at_risk_attempts"] > 0:
            ps["defense_at_risk_rate"] = (
                ps["defense_at_risk_successes"] / ps["defense_at_risk_attempts"] * 100.0
            )
        else:
            ps["defense_at_risk_rate"] = None
        if ps["races_played"] > 0:
            ps["win_rate"] = ps["wins"] / ps["races_played"] * 100.0
        else:
            ps["win_rate"] = None

    def metric_sort_value(player, descriptor):
        if descriptor["type"] in ("value", "percent"):
            val = player.get(descriptor.get("value_key"))
            return val if val is not None else 0
        if descriptor["type"] == "pair":
            num = player.get(descriptor.get("num_key"), 0) or 0
            den = player.get(descriptor.get("den_key"), 0) or 0
            return (num / den) if den else 0
        return 0

    metric_def_map = {d["id"]: d for d in METRIC_DEFS}
    sort_descriptor = metric_def_map.get(sort_metric_id, metric_def_map.get("wins"))

    player_stats = sorted(
        stats.values(),
        key=lambda s: (
            -metric_sort_value(s, sort_descriptor),
            -s["tracks_owned"],
            s["name"].lower(),
        ),
    )

    if active_players_only:
        player_stats = [
            ps for ps in player_stats if player_meta.get(ps["id"], {}).get("active", True)
        ]

    if len(season_ids) > 1:
        code_to_canonical: dict[str, int] = {}
        for tid, info in track_info.items():
            code = info.get("track_code")
            if not code:
                continue
            prev = code_to_canonical.get(code)
            if prev is None or info["season_id"] > track_info[prev]["season_id"]:
                code_to_canonical[code] = tid

        def _merge(d: defaultdict) -> defaultdict:
            code_totals: defaultdict = defaultdict(int)
            for tid, val in d.items():
                code = track_info.get(tid, {}).get("track_code")
                if code:
                    code_totals[code] += val
            result: defaultdict = defaultdict(int)
            for code, total in code_totals.items():
                can = code_to_canonical.get(code)
                if can is not None:
                    result[can] = total
            return result

        track_race_count = _merge(track_race_count)
        track_defenses = _merge(track_defenses)
        track_ownership_changes = _merge(track_ownership_changes)
    else:
        code_to_canonical = {}

    track_activity = []
    for tid, count in track_race_count.items():
        info = track_info.get(tid)
        if not info:
            continue
        track_activity.append(
            {
                "id": tid,
                "track_code": info.get("track_code"),
                "track_en": info["track_en"],
                "track_es": info["track_es"],
                "cup_en": info["cup_en"],
                "cup_es": info["cup_es"],
                "cup_code": info.get("cup_code"),
                "races": count,
                "owner_id": info.get("final_owner_id"),
                "state": info.get("final_state"),
            }
        )
    track_activity.sort(key=lambda x: (-x["races"], x["track_en"]))
    track_activity = track_activity[:10]

    most_defended = None
    if track_defenses:
        tid = max(track_defenses, key=lambda t: track_defenses[t])
        info = track_info.get(tid)
        if info:
            most_defended = {
                "id": tid,
                "track_code": info.get("track_code"),
                "track_en": info["track_en"],
                "track_es": info["track_es"],
                "cup_en": info["cup_en"],
                "cup_es": info["cup_es"],
                "cup_code": info.get("cup_code"),
                "defenses": track_defenses[tid],
            }

    most_contested = None
    if track_ownership_changes:
        tid = max(track_ownership_changes, key=lambda t: track_ownership_changes[t])
        info = track_info.get(tid)
        if info:
            most_contested = {
                "id": tid,
                "track_code": info.get("track_code"),
                "track_en": info["track_en"],
                "track_es": info["track_es"],
                "cup_en": info["cup_en"],
                "cup_es": info["cup_es"],
                "cup_code": info.get("cup_code"),
                "changes": track_ownership_changes[tid],
            }

    track_selector = []
    selected_track = None
    track_player_stats = []
    track_events = []

    if allow_track_insights:
        if code_to_canonical:
            selector_tids = set(code_to_canonical.values())
        else:
            selector_tids = set(track_info.keys())
        for tid in selector_tids:
            info = track_info[tid]
            track_selector.append(
                {
                    "id": tid,
                    "track_code": info.get("track_code"),
                    "track_en": info["track_en"],
                    "track_es": info["track_es"],
                    "cup_en": info["cup_en"],
                    "cup_es": info["cup_es"],
                    "cup_code": info.get("cup_code"),
                    "cup_order": info.get("cup_order", 0),
                    "order_in_cup": info.get("order_in_cup", 0),
                    "owner_id": info.get("final_owner_id"),
                    "state": info.get("final_state"),
                }
            )
        track_selector.sort(key=lambda x: (x["cup_order"], x["order_in_cup"]))

        if selected_track_id:
            selected_track = db.execute(
                """
                SELECT t.*, t.code AS track_code, c.en AS cup_en, c.es AS cup_es, c.code AS cup_code
                FROM tracks t
                JOIN cups c ON c.id = t.cup_id
                WHERE t.id = ?
                """,
                (selected_track_id,),
            ).fetchone()

        if selected_track:
            track_code_for_events = selected_track["track_code"] if hasattr(selected_track, "keys") else None
            if len(season_ids) > 1 and track_code_for_events:
                all_tids_for_code = [
                    tid for tid, inf in track_info.items()
                    if inf.get("track_code") == track_code_for_events
                ]
                ev_clause = ",".join("?" for _ in all_tids_for_code)
                events = db.execute(
                    f"""
                    SELECT e.*,
                        pw.name   AS winner_name,
                        preo.name AS pre_owner_name,
                        posto.name AS post_owner_name
                    FROM events e
                    JOIN tracks t ON t.id = e.track_id
                    LEFT JOIN players pw   ON pw.id   = e.winner_id
                    LEFT JOIN players preo ON preo.id = e.pre_owner_id
                    LEFT JOIN players posto ON posto.id = e.post_owner_id
                    WHERE t.id IN ({ev_clause}) AND e.is_sweep = 0
                    ORDER BY e.occurred_at ASC, e.id ASC
                    """,
                    all_tids_for_code,
                ).fetchall()
            else:
                events = db.execute(
                    """
                    SELECT e.*,
                        pw.name   AS winner_name,
                        preo.name AS pre_owner_name,
                        posto.name AS post_owner_name
                    FROM events e
                    JOIN tracks t ON t.id = e.track_id
                    LEFT JOIN players pw   ON pw.id   = e.winner_id
                    LEFT JOIN players preo ON preo.id = e.pre_owner_id
                    LEFT JOIN players posto ON posto.id = e.post_owner_id
                    WHERE t.id = ? AND e.is_sweep = 0
                    ORDER BY e.occurred_at ASC, e.id ASC
                    """,
                    (selected_track_id,),
                ).fetchall()

            per_player_track: defaultdict[int, TrackPlayerStats] = defaultdict(TrackPlayerStats)

            for ev in events:
                winner_id = ev["winner_id"]
                pre_owner_id = ev["pre_owner_id"]
                post_owner_id = ev["post_owner_id"]

                if winner_id:
                    ps = per_player_track[winner_id]
                    ps.name = player_names.get(winner_id, f"Player {winner_id}")
                    ps.wins += 1

                if pre_owner_id and post_owner_id and pre_owner_id != post_owner_id:
                    ps_old = per_player_track[pre_owner_id]
                    ps_old.name = player_names.get(pre_owner_id, f"Player {pre_owner_id}")
                    ps_old.losses += 1

                    ps_new = per_player_track[post_owner_id]
                    ps_new.name = player_names.get(post_owner_id, f"Player {post_owner_id}")
                    ps_new.takes += 1

                if pre_owner_id and winner_id == pre_owner_id and post_owner_id == pre_owner_id:
                    ps_def = per_player_track[pre_owner_id]
                    ps_def.name = player_names.get(pre_owner_id, f"Player {pre_owner_id}")
                    ps_def.defenses += 1

            track_player_stats = sorted(
                per_player_track.values(),
                key=lambda s: (-s.wins, -s.defenses, s.name.lower()),
            )
            track_events = events

    def pick_track_detail(player_id: int, metric: str):
        profile = player_track_profiles.get(player_id)
        if not profile:
            return None
        best_track_id = None
        best_value_tuple = None
        for tid, values in profile.items():
            val = values.get(metric, 0) or 0
            if val <= 0:
                continue
            info = track_info.get(tid)
            name = info["track_en"] if info else ""
            key = (val, values.get("wins", 0), name.lower())
            if best_value_tuple is None or key > best_value_tuple:
                best_track_id = tid
                best_value_tuple = key
        if best_track_id is None:
            return None
        best_value = best_value_tuple[0] if best_value_tuple else 0
        info = track_info.get(best_track_id)
        if not info:
            return None
        return {
            "track_en": info["track_en"],
            "track_es": info["track_es"],
            "cup_en": info["cup_en"],
            "cup_es": info["cup_es"],
            "count": best_value,
        }

    favorite_cups = {}
    for pid, cup_counts in player_cup_wins.items():
        best_cup_id = None
        best_val = 0
        best_name = ""
        for cup_id, wins in cup_counts.items():
            wins = wins or 0
            info = cup_meta_by_id.get(cup_id)
            if not info or wins <= 0:
                continue
            name = info["cup_en"]
            candidate = (wins, name.lower())
            current = (best_val, best_name.lower())
            if best_cup_id is None or candidate > current:
                best_cup_id = cup_id
                best_val = wins
                best_name = name
        if best_cup_id:
            info = cup_meta_by_id.get(best_cup_id)
            if info:
                favorite_cups[pid] = {
                    "cup_en": info["cup_en"],
                    "cup_es": info["cup_es"],
                    "wins": best_val,
                }

    player_spotlights = []
    for ps in player_stats:
        pid = ps["id"]
        owned_cups = sorted(
            cup_ownerships.get(pid, []),
            key=lambda c: (c.get("season_id", 0), c.get("cup_en", "")),
        )
        player_spotlights.append(
            {
                "id": pid,
                "name": ps["name"],
                "locks_applied": ps["locks_applied"],
                "hunter_marks": ps["hunter_marks"],
                "locked_tracks": ps["locked_tracks"],
                "current_mark_targets": ps["current_mark_targets"],
                "tracks_owned": ps["tracks_owned"],
                "cups_owned": owned_cups,
                "best_track": pick_track_detail(pid, "wins"),
                "most_defended_track": pick_track_detail(pid, "defenses"),
                "most_attacked_track": pick_track_detail(pid, "attacks"),
                "favorite_cup": favorite_cups.get(pid),
            }
        )

    def build_cell(player_row, descriptor):
        if descriptor["type"] == "value":
            val = player_row.get(descriptor["value_key"], 0) or 0
            return {"value": val, "highlight": val}
        if descriptor["type"] == "percent":
            val = player_row.get(descriptor["value_key"])
            return {"percent": val, "highlight": val if val is not None else None}
        if descriptor["type"] == "pair":
            num = player_row.get(descriptor["num_key"], 0) or 0
            den = player_row.get(descriptor["den_key"], 0) or 0
            ratio = (num / den) if den else None
            return {"num": num, "den": den, "highlight": ratio}
        return {"value": 0, "highlight": 0}

    metric_rows = []
    for descriptor in METRIC_DEFS:
        row = {
            "id": descriptor["id"],
            "label": descriptor["label"],
            "type": descriptor["type"],
            "group": descriptor["group"],
            "cells": [],
            "help": descriptor.get("help"),
        }
        max_highlight = None
        for ps in player_stats:
            cell = build_cell(ps, descriptor)
            row["cells"].append(cell)
            highlight = cell.get("highlight")
            if highlight is not None and (max_highlight is None or highlight > max_highlight):
                max_highlight = highlight
        row["max_highlight"] = max_highlight
        metric_rows.append(row)

    def all_current(field: str, threshold: int = 2):
        candidates = sorted(
            [ps for ps in player_stats if ps[field] >= threshold],
            key=lambda item: (-item[field], -item["wins"]),
        )
        return [{"player": c["name"], "streak": c[field]} for c in candidates]

    streak_badges = {
        "hot_hand": all_current("current_win_streak"),
        "shield_wall": all_current("current_defense_streak"),
    }

    return {
        "player_stats": player_stats,
        "track_activity": track_activity,
        "most_defended": most_defended,
        "most_contested": most_contested,
        "track_selector": track_selector,
        "selected_track": selected_track,
        "track_player_stats": track_player_stats,
        "track_events": track_events,
        "metric_rows": metric_rows,
        "streak_badges": streak_badges,
        "player_spotlights": player_spotlights,
        "track_insights_enabled": allow_track_insights,
    }


__all__ = [
    "TrackPlayerStats",
    "PlayerStatsDict",
    "METRIC_DEFS",
    "apply_result",
    "undo_last_event",
    "deactivate_player",
    "compute_stats_data",
    "bind_app",
]
