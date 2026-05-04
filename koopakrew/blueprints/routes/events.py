from flask import abort, redirect, request, url_for

from koopakrew.db import get_db
from koopakrew.helpers.rendering import csv_response, render_page
from koopakrew.i18n import flash_message
from koopakrew.queries.players import fetch_players, get_default_player
from koopakrew.queries.seasons import get_season_row
from koopakrew.queries.standings import fetch_cups_for_season, state_label
from koopakrew.services import core as services_core


def update_result(track_id):
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured. Seed the database.")
    season_id = season_row["id"]
    season_label = season_row["label"]

    from koopakrew.queries.standings import fetch_track_detail
    track = fetch_track_detail(db, track_id)
    if not track or track["season"] != season_id:
        abort(404)

    players = fetch_players(db)
    default_player = get_default_player(db)
    prefill_winner = None
    if request.args.get("quick", default=0, type=int) == 1 and default_player:
        prefill_winner = default_player["name"]
    elif request.args.get("winner"):
        prefill_winner = request.args.get("winner")

    recent_events = db.execute(
        """
        SELECT
            e.occurred_at,
            pw.name AS winner_name,
            preo.name AS pre_owner_name,
            posto.name AS post_owner_name,
            e.pre_state,
            e.post_state
        FROM events e
        JOIN tracks t ON t.id = e.track_id
        LEFT JOIN players pw   ON pw.id   = e.winner_id
        LEFT JOIN players preo ON preo.id = e.pre_owner_id
        LEFT JOIN players posto ON posto.id = e.post_owner_id
        WHERE t.id = ? AND e.is_sweep = 0
        ORDER BY e.occurred_at DESC, e.id DESC
        LIMIT 5
        """,
        (track_id,),
    ).fetchall()

    if request.method == "POST":
        winner_name = request.form.get("winner")
        row = db.execute(
            "SELECT id FROM players WHERE name = ? AND active = 1", (winner_name,)
        ).fetchone()
        if not row:
            flash_message("Unknown player", "error")
            return redirect(url_for("update_result", track_id=track_id))
        try:
            services_core.apply_result(db, season_id, track_id, row["id"])
            flash_message("Result saved.", "success")
        except Exception as e:
            flash_message("Error saving result: {error}", "error", error=e)
        return redirect(url_for("index"))

    track_dict = dict(track)
    track_dict["owner"] = track["owner"]
    track_dict["threatened_by"] = track["threatened_by"]
    return render_page(
        "update.html",
        db,
        track=track_dict,
        players=[p["name"] for p in players],
        season_label=season_label,
        prefill_winner=prefill_winner,
        default_player=default_player,
        recent_events=recent_events,
    )


def undo():
    db = get_db()
    ok = services_core.undo_last_event(db)
    if ok:
        flash_message("Last change undone.", "success")
    else:
        flash_message("Nothing to undo.", "info")
    next_url = request.form.get("next")
    if next_url:
        return redirect(next_url)
    return redirect(url_for("index"))


def events_log():
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured.")
    season_id = season_row["id"]
    season_label = season_row["label"]

    player_filter = request.args.get("player", type=int)
    cup_filter = request.args.get("cup", type=int)
    track_filter = request.args.get("track", type=int)
    event_type_filter = request.args.get("event_type", default="all")
    me_filter = request.args.get("me", default=0, type=int)

    default_player = get_default_player(db)
    if me_filter == 1 and default_player and not player_filter:
        player_filter = default_player["id"]

    player_rows = db.execute("SELECT id, name FROM players ORDER BY name").fetchall()
    players = [dict(id=r["id"], name=r["name"]) for r in player_rows]
    cups = fetch_cups_for_season(db, season_id)
    track_rows = db.execute(
        """
        SELECT t.id, t.en AS track_en, t.es AS track_es, c.en AS cup_en, c.es AS cup_es
        FROM tracks t
        JOIN cups c ON c.id = t.cup_id
        WHERE t.season = ?
        ORDER BY c.[order] ASC, t.order_in_cup ASC
        """,
        (season_id,),
    ).fetchall()
    tracks = [
        {
            "id": r["id"],
            "track_en": r["track_en"],
            "track_es": r["track_es"],
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
        }
        for r in track_rows
    ]

    where_clauses = [
        """
        (
            (t.season = ?)
            OR (
                t.id IS NULL
                AND e.is_sweep = 1
                AND e.sweep_cup_id IN (
                    SELECT DISTINCT ts.cup_id FROM tracks ts WHERE ts.season = ?
                )
            )
        )
        """
    ]
    sql_params = [season_id, season_id]

    if event_type_filter == "race":
        where_clauses.append("e.is_sweep = 0")
    elif event_type_filter == "sweep":
        where_clauses.append("e.is_sweep = 1")

    if player_filter:
        where_clauses.append(
            "(pw.id = ? OR preo.id = ? OR posto.id = ? OR (e.is_sweep = 1 AND so.id = ?))"
        )
        sql_params.extend([player_filter, player_filter, player_filter, player_filter])

    if cup_filter:
        where_clauses.append(
            "((e.is_sweep = 0 AND c.id = ?) OR (e.is_sweep = 1 AND c2.id = ?))"
        )
        sql_params.extend([cup_filter, cup_filter])

    if track_filter:
        where_clauses.append("t.id = ?")
        sql_params.append(track_filter)

    sql = f"""
    SELECT
        e.id AS event_id, e.occurred_at, e.is_sweep,
        c2.code AS sweep_cup_code, c2.en AS sweep_cup_en, c2.es AS sweep_cup_es,
        so.name AS sweep_owner,
        c.code AS cup_code, c.en AS cup_en, c.es AS cup_es,
        t.code AS track_code, t.en AS track_en, t.es AS track_es,
        pw.name AS winner, preo.name AS pre_owner,
        e.pre_state, pret.name AS pre_mark,
        posto.name AS post_owner, e.post_state, postt.name AS post_mark
    FROM events e
    LEFT JOIN tracks  t    ON t.id  = e.track_id
    LEFT JOIN cups    c    ON c.id  = t.cup_id
    LEFT JOIN cups    c2   ON c2.id = e.sweep_cup_id
    LEFT JOIN players so   ON so.id = e.sweep_owner_id
    LEFT JOIN players pw   ON pw.id = e.winner_id
    LEFT JOIN players preo ON preo.id = e.pre_owner_id
    LEFT JOIN players pret ON pret.id = e.pre_threatened_by_id
    LEFT JOIN players posto ON posto.id = e.post_owner_id
    LEFT JOIN players postt ON postt.id = e.post_threatened_by_id
    WHERE {" AND ".join(where_clauses)}
    ORDER BY e.occurred_at DESC, e.id DESC
    """
    rows = db.execute(sql, sql_params).fetchall()
    events = [
        {
            "id": r["event_id"],
            "occurred_at": r["occurred_at"],
            "is_sweep": bool(r["is_sweep"]),
            "sweep_cup_en": r["sweep_cup_en"],
            "sweep_cup_es": r["sweep_cup_es"],
            "sweep_owner": r["sweep_owner"],
            "cup_en": r["cup_en"],
            "cup_es": r["cup_es"],
            "track_en": r["track_en"],
            "track_es": r["track_es"],
            "winner": r["winner"],
            "pre_owner": r["pre_owner"],
            "pre_state": r["pre_state"],
            "pre_mark": r["pre_mark"],
            "post_owner": r["post_owner"],
            "post_state": r["post_state"],
            "post_mark": r["post_mark"],
        }
        for r in rows
    ]

    return render_page(
        "events.html",
        db,
        events=events,
        season_label=season_label,
        players=players,
        cups=cups,
        tracks=tracks,
        default_player=default_player,
        selected_filters={
            "player": player_filter,
            "cup": cup_filter,
            "track": track_filter,
            "event_type": event_type_filter,
            "me": me_filter,
        },
        page_title="Koopa Krew - Events",
    )


def export_events():
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured.")
    season_id = season_row["id"]
    season_label = season_row["label"]
    label_safe = season_label.replace(" — ", " ").replace("—", "-").replace(" ", "_")

    season_scope_clause = """
        (
            (t.season = ?)
            OR (
                t.id IS NULL
                AND e.is_sweep = 1
                AND e.sweep_cup_id IN (
                    SELECT DISTINCT ts.cup_id FROM tracks ts WHERE ts.season = ?
                )
            )
        )
    """
    sql = f"""
    SELECT
        e.id AS event_id, e.occurred_at, e.is_sweep,
        c2.code AS sweep_cup_code, c2.en AS sweep_cup_en, c2.es AS sweep_cup_es,
        so.name AS sweep_owner,
        c.code AS cup_code, c.en AS cup_en, c.es AS cup_es,
        t.code AS track_code, t.en AS track_en, t.es AS track_es,
        pw.name AS winner, preo.name AS pre_owner,
        e.pre_state, pret.name AS pre_mark,
        posto.name AS post_owner, e.post_state, postt.name AS post_mark
    FROM events e
    LEFT JOIN tracks  t    ON t.id  = e.track_id
    LEFT JOIN cups    c    ON c.id  = t.cup_id
    LEFT JOIN cups    c2   ON c2.id = e.sweep_cup_id
    LEFT JOIN players so   ON so.id = e.sweep_owner_id
    LEFT JOIN players pw   ON pw.id = e.winner_id
    LEFT JOIN players preo ON preo.id = e.pre_owner_id
    LEFT JOIN players pret ON pret.id = e.pre_threatened_by_id
    LEFT JOIN players posto ON posto.id = e.post_owner_id
    LEFT JOIN players postt ON postt.id = e.post_threatened_by_id
    WHERE {season_scope_clause}
    ORDER BY e.occurred_at ASC, e.id ASC
    """
    rows = db.execute(sql, (season_id, season_id)).fetchall()

    header = [
        "event_id", "occurred_at", "is_sweep",
        "sweep_cup_code", "sweep_cup_en", "sweep_cup_es", "sweep_owner",
        "cup_code", "cup_en", "cup_es", "track_code", "track_en", "track_es",
        "winner", "pre_owner", "pre_state", "pre_state_label", "pre_mark",
        "post_owner", "post_state", "post_state_label", "post_mark",
    ]
    out = [
        [
            r["event_id"], r["occurred_at"], r["is_sweep"],
            r["sweep_cup_code"], r["sweep_cup_en"], r["sweep_cup_es"], r["sweep_owner"],
            r["cup_code"], r["cup_en"], r["cup_es"],
            r["track_code"], r["track_en"], r["track_es"],
            r["winner"], r["pre_owner"], r["pre_state"],
            state_label(r["pre_state"]) if r["pre_state"] is not None else "",
            r["pre_mark"], r["post_owner"], r["post_state"],
            state_label(r["post_state"]) if r["post_state"] is not None else "",
            r["post_mark"],
        ]
        for r in rows
    ]
    filename = f"events_{label_safe}.csv"
    return csv_response(filename, header, out)
