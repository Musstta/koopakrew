from flask import abort, request

from koopakrew.db import get_db
from koopakrew.helpers.assets import cup_image_url, track_image_url
from koopakrew.helpers.rendering import render_page
from koopakrew.queries.seasons import get_current_season_row, get_season_row
from koopakrew.queries.standings import state_label
from koopakrew.stats.compute import get_stats


def stats_page():
    db = get_db()
    season_param = request.args.get("season")
    sort_metric_id = request.args.get("sort", default="wins")
    track_param = request.args.get("track_id", type=int)

    season_meta_rows = db.execute(
        "SELECT id, label FROM season_meta ORDER BY start_date DESC"
    ).fetchall()
    season_options = [{"id": str(row["id"]), "label": row["label"]} for row in season_meta_rows]
    season_options.append({"id": "all", "label": "All seasons"})

    selected_ids = []
    season_label = "Koopa Krew"
    selected_option = None

    if season_param == "all":
        selected_ids = [row["id"] for row in season_meta_rows]
        season_label = "All seasons"
        selected_option = "all"
    else:
        target_id = None
        if season_param:
            try:
                target_id = int(season_param)
            except (TypeError, ValueError):
                target_id = None
        season_row = get_season_row(db, target_id)
        if not season_row:
            abort(500, "No season configured.")
        season_label = season_row["label"]
        selected_ids = [season_row["id"]]
        selected_option = str(season_row["id"])

    if not selected_option and season_meta_rows:
        selected_option = str(season_meta_rows[0]["id"])

    restrict_active_players = False
    if selected_option != "all" and len(selected_ids) == 1:
        current_row = get_current_season_row(db)
        if current_row and current_row["id"] == selected_ids[0]:
            restrict_active_players = True

    stats_data = get_stats(
        db, selected_ids, sort_metric_id, track_param, active_players_only=restrict_active_players
    )

    track_summary = None
    track_quick_picks: list[dict] = []
    selected_track_row = stats_data.get("selected_track")
    if selected_track_row:
        owner_id = selected_track_row["owner_id"]
        threat_id = selected_track_row["threatened_by_id"]
        ids = [pid for pid in (owner_id, threat_id) if pid]
        name_map = {}
        if ids:
            qmarks = ",".join("?" for _ in ids)
            rows = db.execute(
                f"SELECT id, name FROM players WHERE id IN ({qmarks})", ids
            ).fetchall()
            name_map = {r["id"]: r["name"] for r in rows}
        track_summary = {
            "owner": name_map.get(owner_id),
            "owner_id": owner_id,
            "state": selected_track_row["state"],
            "state_label": state_label(selected_track_row["state"]),
            "threat": name_map.get(threat_id),
            "threat_id": threat_id,
        }

    def _add_pick(source, label_key):
        if not source or not source.get("id"):
            return
        track_quick_picks.append(
            {
                "id": source["id"],
                "label": label_key,
                "track_en": source.get("track_en"),
                "track_es": source.get("track_es"),
                "cup_en": source.get("cup_en"),
                "cup_es": source.get("cup_es"),
                "track_code": source.get("track_code"),
                "cup_code": source.get("cup_code"),
                "image_src": track_image_url(source) if source.get("track_code") else None,
                "cup_logo_src": cup_image_url(source) if source.get("cup_code") else None,
                "races": source.get("races"),
                "defenses": source.get("defenses"),
                "changes": source.get("changes"),
            }
        )

    _add_pick(stats_data.get("most_defended"), "most_defended")
    _add_pick(stats_data.get("most_contested"), "most_contested")
    for entry in (stats_data.get("track_activity") or [])[:2]:
        _add_pick(entry, "most_active")

    player_rows = stats_data.get("player_stats") or []
    extra_highlights = {}

    def _best(key, threshold_key=None, threshold_min=0):
        best_row = None
        for row in player_rows:
            if threshold_key and row.get(threshold_key, 0) < threshold_min:
                continue
            val = row.get(key)
            if val is None:
                continue
            if best_row is None or val > best_row.get(key):
                best_row = row
        return best_row

    best_win_rate = _best("win_rate", "races_played", 5)
    if best_win_rate:
        extra_highlights["best_win_rate"] = {
            "name": best_win_rate["name"],
            "win_rate": best_win_rate["win_rate"],
            "races": best_win_rate.get("races_played"),
        }
    most_tracks_taken = _best("tracks_taken")
    if most_tracks_taken and most_tracks_taken.get("tracks_taken"):
        extra_highlights["tracks_taken"] = {
            "name": most_tracks_taken["name"],
            "count": most_tracks_taken["tracks_taken"],
        }
    best_net_gain = _best("net_tracks")
    if best_net_gain and best_net_gain.get("net_tracks"):
        extra_highlights["net_gain"] = {
            "name": best_net_gain["name"],
            "count": best_net_gain["net_tracks"],
        }
    best_defense_rate = _best("defense_success_rate", "races_as_owner", 5)
    if best_defense_rate and best_defense_rate.get("defense_success_rate") is not None:
        extra_highlights["defense_rate"] = {
            "name": best_defense_rate["name"],
            "rate": best_defense_rate["defense_success_rate"],
            "races": best_defense_rate.get("races_as_owner"),
        }
    most_locks = _best("locks_applied")
    if most_locks and most_locks.get("locks_applied"):
        extra_highlights["locks_applied"] = {
            "name": most_locks["name"],
            "count": most_locks["locks_applied"],
        }

    return render_page(
        "stats.html",
        db,
        season_label=season_label,
        selected_sort=sort_metric_id,
        season_options=season_options,
        selected_season=selected_option,
        selected_track_id=track_param,
        track_summary=track_summary,
        track_quick_picks=track_quick_picks,
        extra_highlights=extra_highlights,
        page_title="Koopa Krew - Stats",
        **stats_data,
    )


def track_stats_page():
    db = get_db()
    season_param = request.args.get("season")
    track_param = request.args.get("track_id", type=int)
    owner_filter = request.args.get("owner", type=int)
    state_filter = request.args.get("state")

    season_meta_rows = db.execute(
        "SELECT id, label FROM season_meta ORDER BY start_date DESC"
    ).fetchall()
    if not season_meta_rows:
        abort(500, "No season configured. Seed the database.")

    season_options = [{"id": str(r["id"]), "label": r["label"]} for r in season_meta_rows]
    season_options.append({"id": "all", "label": "All seasons"})

    if season_param == "all":
        season_ids = [r["id"] for r in season_meta_rows]
        season_label = "All seasons"
        selected_season = "all"
    else:
        target_id = None
        if season_param:
            try:
                target_id = int(season_param)
            except (TypeError, ValueError):
                pass
        season_row = get_season_row(db, target_id)
        if not season_row:
            abort(500, "No season configured.")
        season_ids = [season_row["id"]]
        season_label = season_row["label"]
        selected_season = str(season_row["id"])

    stats_data = get_stats(db, season_ids, "wins", track_param, active_players_only=False)
    track_selector = stats_data.get("track_selector") or []

    def matches_filters(track_entry):
        if owner_filter and track_entry.get("owner_id") != owner_filter:
            return False
        if state_filter and state_filter != "any":
            try:
                state_val = int(state_filter)
            except (TypeError, ValueError):
                state_val = None
            if state_val is not None and track_entry.get("state") != state_val:
                return False
        return True

    filtered_selector = [t for t in track_selector if matches_filters(t)]
    if track_param and all(t["id"] != track_param for t in filtered_selector):
        track_param = None

    filtered_activity = [t for t in (stats_data.get("track_activity") or []) if matches_filters(t)]

    track_summary = None
    selected_track_row = stats_data.get("selected_track")
    if selected_track_row:
        owner_id = selected_track_row["owner_id"]
        threat_id = selected_track_row["threatened_by_id"]
        ids = [pid for pid in (owner_id, threat_id) if pid]
        name_map = {}
        if ids:
            qmarks = ",".join("?" for _ in ids)
            rows = db.execute(
                f"SELECT id, name FROM players WHERE id IN ({qmarks})", ids
            ).fetchall()
            name_map = {r["id"]: r["name"] for r in rows}
        track_summary = {
            "owner": name_map.get(owner_id),
            "owner_id": owner_id,
            "state": selected_track_row["state"],
            "state_label": state_label(selected_track_row["state"]),
            "threat": name_map.get(threat_id),
            "threat_id": threat_id,
        }

    track_quick_picks: list[dict] = []

    def _add_pick(source, label_key):
        if not source or not source.get("id"):
            return
        track_quick_picks.append(
            {
                "id": source["id"],
                "label": label_key,
                "track_en": source.get("track_en"),
                "track_es": source.get("track_es"),
                "cup_en": source.get("cup_en"),
                "cup_es": source.get("cup_es"),
                "track_code": source.get("track_code"),
                "cup_code": source.get("cup_code"),
                "image_src": track_image_url(source) if source.get("track_code") else None,
                "cup_logo_src": cup_image_url(source) if source.get("cup_code") else None,
                "races": source.get("races"),
                "defenses": source.get("defenses"),
                "changes": source.get("changes"),
            }
        )

    _add_pick(stats_data.get("most_defended"), "most_defended")
    _add_pick(stats_data.get("most_contested"), "most_contested")
    for entry in filtered_activity[:3]:
        _add_pick(entry, "most_active")

    most_defended = stats_data.get("most_defended")
    most_contested = stats_data.get("most_contested")
    if most_defended:
        most_defended = dict(most_defended)
        most_defended["image_src"] = track_image_url(most_defended) if most_defended.get("track_code") else None
        most_defended["cup_logo_src"] = cup_image_url(most_defended) if most_defended.get("cup_code") else None
    if most_contested:
        most_contested = dict(most_contested)
        most_contested["image_src"] = track_image_url(most_contested) if most_contested.get("track_code") else None
        most_contested["cup_logo_src"] = cup_image_url(most_contested) if most_contested.get("cup_code") else None

    filtered_activity_with_art = []
    for t in filtered_activity:
        t2 = dict(t)
        t2["image_src"] = track_image_url(t2) if t2.get("track_code") else None
        t2["cup_logo_src"] = cup_image_url(t2) if t2.get("cup_code") else None
        filtered_activity_with_art.append(t2)

    selected_track_art = None
    if selected_track_row:
        st_dict = dict(selected_track_row)
        selected_track_art = {
            "image_src": track_image_url(st_dict),
            "cup_logo_src": cup_image_url(st_dict),
        }

    payload = dict(stats_data)
    payload["track_selector"] = filtered_selector
    payload["track_activity"] = filtered_activity_with_art
    payload["most_defended"] = most_defended
    payload["most_contested"] = most_contested

    return render_page(
        "track_stats.html",
        db,
        season_label=season_label,
        season_options=season_options,
        selected_season=selected_season,
        selected_track_id=track_param,
        track_summary=track_summary,
        track_quick_picks=track_quick_picks,
        track_art=selected_track_art,
        owner_options=db.execute("SELECT id, name FROM players ORDER BY name").fetchall(),
        owner_filter=owner_filter,
        state_filter=state_filter or "any",
        page_title="Koopa Krew - Track Stats",
        **payload,
    )
