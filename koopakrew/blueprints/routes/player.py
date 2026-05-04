from flask import abort, request

from koopakrew.db import get_db
from koopakrew.helpers.rendering import render_page
from koopakrew.queries.seasons import get_current_season_row, get_season_row
from koopakrew.stats.attributes import compute_profile_attributes
from koopakrew.stats.compute import get_stats


def _attrs_for_player_season(db, player_id: int, season_id: int) -> dict:
    """Compute pentagon attributes for a player in a specific season."""
    stats_data = get_stats(db, [season_id], "wins", None, active_players_only=False)
    all_ps: list[dict] = stats_data.get("player_stats") or []

    ps = next((s for s in all_ps if s["id"] == player_id), None)
    if not ps:
        ps = {k: 0 for k in (
            "tracks_taken", "hunter_marks", "wins_as_non_owner",
            "defense_success_rate", "defense_at_risk_rate", "best_defense_streak",
            "tracks_owned", "locked_tracks", "cups_owned_count",
            "win_rate", "best_win_streak",
        )}
        ps["id"] = player_id

    clutch_rows = db.execute(
        """
        SELECT e.winner_id, e.pre_owner_id, e.pre_state, e.pre_threatened_by_id
        FROM events e JOIN tracks t ON t.id = e.track_id
        WHERE t.season = ? AND e.is_sweep = 0
        ORDER BY e.occurred_at ASC, e.id ASC
        """,
        (season_id,),
    ).fetchall()

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

    return compute_profile_attributes(
        player_id=player_id,
        player_stats=ps,
        all_player_stats=all_ps,
        clutch_events=[dict(r) for r in clutch_rows],
        last_10_events=[dict(r) for r in last10_rows],
        presence_player_events=player_events,
        presence_total_events=total_events,
    )


def player_profile(player_id: int):
    db = get_db()

    player_row = db.execute(
        "SELECT id, name FROM players WHERE id = ?", (player_id,)
    ).fetchone()
    if not player_row:
        abort(404)

    player_name = player_row["name"]

    # Season selection (query-param ?season=N or current season)
    season_param = request.args.get("season")
    season_meta_rows = db.execute(
        "SELECT id, label, start_date FROM season_meta ORDER BY start_date DESC"
    ).fetchall()
    season_options = [{"id": str(r["id"]), "label": r["label"]} for r in season_meta_rows]

    if season_param:
        try:
            season_row = get_season_row(db, int(season_param))
        except (TypeError, ValueError):
            season_row = get_current_season_row(db)
    else:
        season_row = get_current_season_row(db)

    if not season_row:
        abort(500, "No season configured.")

    season_id: int = season_row["id"]
    season_label: str = season_row["label"]
    selected_season = str(season_id)

    # --- Stats for this season ---
    stats_data = get_stats(db, [season_id], "wins", None, active_players_only=False)
    all_player_stats: list[dict] = stats_data.get("player_stats") or []

    player_stats = next(
        (ps for ps in all_player_stats if ps["id"] == player_id), None
    )
    if not player_stats:
        player_stats = {k: 0 for k in (
            "id", "name", "tracks_owned", "locked_tracks", "cups_owned_count",
            "tracks_taken", "wins_as_non_owner", "hunter_marks",
            "defense_success_rate", "defense_at_risk_rate", "best_defense_streak",
            "win_rate", "best_win_streak",
            "steals_from_risk", "wins_with_hunter_mark", "wins_on_risk",
            "wins", "races_played", "tracks_lost", "net_tracks",
        )}
        player_stats["id"] = player_id
        player_stats["name"] = player_name

    # --- Pentagon attributes ---
    attributes = _attrs_for_player_season(db, player_id, season_id)

    # --- AI profile: read from cache only (worker generates in background) ---
    cached_row = db.execute(
        "SELECT historical_paragraph, session_notes FROM player_profiles WHERE player_id = ?",
        (player_id,),
    ).fetchone()
    ai_profile = {
        "historical_paragraph": (cached_row["historical_paragraph"] or "") if cached_row else "",
        "session_notes": (cached_row["session_notes"] or "") if cached_row else "",
        "from_cache": bool(cached_row),
    }

    # --- Active players for navigation ---
    all_players = db.execute(
        "SELECT id, name FROM players WHERE active = 1 ORDER BY name"
    ).fetchall()
    all_players = [{"id": r["id"], "name": r["name"]} for r in all_players]

    # --- Compare feature ---
    compare_player_id = request.args.get("compare", type=int)
    compare_season_id = request.args.get("compare_season", type=int)

    compare_attributes = None
    compare_label = None

    if compare_player_id and compare_player_id != player_id:
        cmp_row = db.execute(
            "SELECT id, name FROM players WHERE id = ?", (compare_player_id,)
        ).fetchone()
        if cmp_row:
            compare_attributes = _attrs_for_player_season(db, compare_player_id, season_id)
            compare_label = cmp_row["name"]
    elif compare_season_id and str(compare_season_id) != selected_season:
        cmp_season = get_season_row(db, compare_season_id)
        if cmp_season:
            compare_attributes = _attrs_for_player_season(db, player_id, compare_season_id)
            compare_label = cmp_season["label"]

    return render_page(
        "player_profile.html",
        db,
        season_label=season_label,
        season_options=season_options,
        selected_season=selected_season,
        player_id=player_id,
        player_name=player_name,
        player_stats=player_stats,
        attributes=attributes,
        ai_profile=ai_profile,
        all_players=all_players,
        compare_player_id=compare_player_id,
        compare_season_id=compare_season_id,
        compare_attributes=compare_attributes,
        compare_label=compare_label,
        page_title=f"Koopa Krew — {player_name}",
    )
