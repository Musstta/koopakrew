from flask import abort, request, url_for

from koopakrew.db import get_db
from koopakrew.helpers.assets import decorate_standings_with_art
from koopakrew.helpers.rendering import csv_response, render_page
from koopakrew.queries.filters import normalize_owner_filters, normalize_state_filters
from koopakrew.queries.players import fetch_players, get_default_player
from koopakrew.queries.seasons import get_season_row
from koopakrew.queries.filters import FilterBuilder
from koopakrew.queries.standings import (
    fetch_cups_for_season,
    fetch_standings,
    fetch_totals_filtered,
    fetch_totals_overall,
    state_label,
)
from koopakrew.stats.compute import build_player_highlights, compute_player_streaks


def index():
    db = get_db()
    season_param = request.args.get("season", type=int)
    season_row = get_season_row(db, season_param)
    if not season_row:
        abort(500, "No season configured. Seed the database.")
    season_id = season_row["id"]
    season_label = season_row["label"]

    owner_filters = normalize_owner_filters(request.args.getlist("owner"))
    cup_filter = request.args.get("cup", default="all")
    state_filters = request.args.getlist("state")
    filters_active = bool(
        owner_filters or state_filters or (cup_filter and cup_filter.lower() != "all")
    )

    standings = fetch_standings(
        db, season_id, owner_names=owner_filters, cup_code=cup_filter, state_filters=state_filters
    )
    decorate_standings_with_art(standings)
    for cup in standings:
        cup["is_dlc"] = bool(cup.get("is_dlc", 0))

    totals_overall = fetch_totals_overall(db, season_id)
    totals_filtered = fetch_totals_filtered(
        db, season_id, owner_names=owner_filters, cup_code=cup_filter, state_filters=state_filters
    )
    medals = ["🥇", "🥈", "🥉"]

    player_options = fetch_players(db)
    players = [p["name"] for p in player_options]
    cups = fetch_cups_for_season(db, season_id)
    default_player = get_default_player(db)
    streak_entries = compute_player_streaks(db, season_id)
    streaks_by_name = {info["name"]: info for info in streak_entries.values()}
    player_highlights = build_player_highlights(db, season_id)

    return render_page(
        "index.html",
        db,
        standings=standings,
        totals_overall=totals_overall,
        totals_filtered=totals_filtered,
        medals=medals,
        players=players,
        player_options=player_options,
        cups=cups,
        selected_filters={"owners": owner_filters, "cup": cup_filter, "states": state_filters},
        filters_active=filters_active,
        season_label=season_label,
        default_player=default_player,
        streaks_by_name=streaks_by_name,
        player_highlights=player_highlights,
        page_title="Koopa Krew - Standings",
    )


def export_standings():
    db = get_db()
    season_row = get_season_row(db, request.args.get("season", type=int))
    if not season_row:
        abort(500, "No season configured.")
    season_id = season_row["id"]
    season_label = season_row["label"]
    label_safe = season_label.replace(" — ", " ").replace("—", "-").replace(" ", "_")

    # Support multiple owners consistent with the main standings view
    owner_f = request.args.getlist("owner")
    cup_f = request.args.get("cup", default="all")
    state_f = request.args.getlist("state")

    where_str, args = (
        FilterBuilder(season_id)
        .with_owners(owner_f, alias="po")
        .with_cup(cup_f)
        .with_states(state_f)
        .build()
    )

    sql = f"""
    SELECT
        c.code AS cup_code, c.en AS cup_en, c.es AS cup_es, c.[order] AS cup_order,
        t.code AS track_code, t.en AS track_en, t.es AS track_es, t.order_in_cup,
        po.name AS owner, t.state, pt.name AS mark
    FROM tracks t
    JOIN cups c ON c.id = t.cup_id
    LEFT JOIN players po ON po.id = t.owner_id
    LEFT JOIN players pt ON pt.id = t.threatened_by_id
    WHERE {where_str}
    ORDER BY c.[order] ASC, t.order_in_cup ASC
    """
    rows = db.execute(sql, args).fetchall()

    header = [
        "cup_code", "cup_en", "cup_es", "track_code", "track_en", "track_es",
        "order_in_cup", "owner", "state", "state_label", "mark",
    ]
    out = [
        [
            r["cup_code"], r["cup_en"], r["cup_es"],
            r["track_code"], r["track_en"], r["track_es"],
            r["order_in_cup"], r["owner"], r["state"], state_label(r["state"]), r["mark"],
        ]
        for r in rows
    ]

    owners_norm = normalize_owner_filters(owner_f)
    states_norm = normalize_state_filters(state_f)
    fbits = []
    if owners_norm:
        fbits.append("owner-" + "+".join(owners_norm))
    if cup_f and cup_f.lower() != "all":
        fbits.append(f"cup-{cup_f}")
    if states_norm:
        fbits.append("state-" + "+".join(str(s) for s in states_norm))
    suffix = ("_" + "_".join(fbits)) if fbits else ""
    filename = f"standings_{label_safe}{suffix}.csv"
    return csv_response(filename, header, out)
