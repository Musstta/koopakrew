import sqlite3
import time

from flask import jsonify, redirect, request, session, url_for

from koopakrew.db import get_db
from koopakrew.helpers.rendering import render_page
from koopakrew.i18n import flash_message
from koopakrew.queries.players import fetch_all_players, get_default_player
from koopakrew.queries.seasons import get_current_season_row
from koopakrew.services import core as services_core
from koopakrew.stats.compute import invalidate_stats_cache


def _presence_service():
    from flask import current_app
    return current_app.extensions["presence"]


def admin_players():
    db = get_db()
    season_row = get_current_season_row(db)
    season_label = season_row["label"] if season_row else "Koopa Krew"
    show_mode = request.args.get("show", default="active")

    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = (request.form.get("name") or "").strip()
            if not name:
                flash_message("Name is required.", "error")
            else:
                try:
                    db.execute("INSERT INTO players (name, active) VALUES (?, 1)", (name,))
                    db.commit()
                    invalidate_stats_cache()
                    flash_message("Added player {name}.", "success", name=name)
                except sqlite3.IntegrityError:
                    flash_message("That name already exists.", "error")
        elif action == "rename":
            player_id = request.form.get("player_id", type=int)
            if player_id is None:
                flash_message("Invalid player id.", "error")
                return redirect(url_for("admin_players"))
            name = (request.form.get("name") or "").strip()
            if not name:
                flash_message("Name is required.", "error")
            else:
                try:
                    db.execute("UPDATE players SET name = ? WHERE id = ?", (name, player_id))
                    db.commit()
                    invalidate_stats_cache()
                    flash_message("Name updated.", "success")
                except sqlite3.IntegrityError:
                    flash_message("That name already exists.", "error")
        elif action == "toggle":
            player_id = request.form.get("player_id", type=int)
            if player_id is None:
                flash_message("Invalid player id.", "error")
            else:
                row = db.execute(
                    "SELECT active FROM players WHERE id = ?", (player_id,)
                ).fetchone()
                if not row:
                    flash_message("Player not found.", "error")
                elif row["active"] == 1:
                    if services_core.deactivate_player(db, player_id):
                        invalidate_stats_cache()
                        flash_message("Player deactivated and tracks cleared.", "success")
                    else:
                        flash_message("Player could not be deactivated.", "error")
                else:
                    db.execute("UPDATE players SET active = 1 WHERE id = ?", (player_id,))
                    db.commit()
                    invalidate_stats_cache()
                    flash_message("Player reactivated.", "success")
        else:
            flash_message("Unknown action.", "error")
        return redirect(url_for("admin_players"))

    players = fetch_all_players(db)
    filtered_players = players if show_mode == "all" else [p for p in players if p["active"]]
    active_count = sum(1 for p in players if p["active"])
    inactive_count = len(players) - active_count
    return render_page(
        "admin_players.html",
        db,
        season_label=season_label,
        players=filtered_players,
        counts={"active": active_count, "inactive": inactive_count},
        show_mode=show_mode,
        default_player=get_default_player(db),
    )


def set_default_player():
    db = get_db()
    player_id = request.form.get("player_id", type=int)
    previous_default = session.get("default_player_id")
    row = db.execute(
        "SELECT id, name, active FROM players WHERE id = ?", (player_id,)
    ).fetchone()
    if not row:
        flash_message("Player not found.", "error")
    elif not row["active"]:
        flash_message("Activate the player before setting as default.", "error")
    else:
        if previous_default != row["id"]:
            _presence_service().disconnect()
        session["default_player_id"] = row["id"]
        flash_message("{name} is now your default player.", "success", name=row["name"])
    show_mode = request.form.get("show_mode")
    next_view = request.form.get("next")
    if next_view == "index":
        return redirect(url_for("index"))
    return redirect(
        url_for("admin_players", show=show_mode) if show_mode else url_for("admin_players")
    )


def clear_default_player():
    _presence_service().disconnect()
    session.pop("default_player_id", None)
    flash_message("Cleared your default player.", "info")
    show_mode = request.form.get("show_mode")
    return redirect(
        url_for("admin_players", show=show_mode) if show_mode else url_for("admin_players")
    )


def presence_ping():
    db = get_db()
    default_player = get_default_player(db)
    service = _presence_service()
    if not default_player:
        service.disconnect()
        return jsonify({"status": "ignored"})
    service.ping(default_player["id"], now=time.time())
    return jsonify({"status": "ok"})
