import csv
import io
import time

from flask import current_app, make_response, render_template

from koopakrew.helpers.assets import get_current_logo_background_filename, get_current_logo_filename
from koopakrew.queries.players import get_default_player


def _presence_service():
    return current_app.extensions["presence"]


def get_online_players(db) -> list[str]:
    meta = _presence_service().online_players(now=time.time())
    if not meta:
        return []
    ids = [pid for pid, _ in meta]
    qmarks = ",".join("?" for _ in ids)
    rows = db.execute(
        f"SELECT id, name FROM players WHERE id IN ({qmarks})",
        ids,
    ).fetchall()
    names = {r["id"]: r["name"] for r in rows}
    return [names[pid] for pid in ids if pid in names]


def get_online_presence(db) -> list[dict]:
    service = _presence_service()
    states = service.presence_states(now=time.time())
    if not states:
        return []
    ids = {entry["player_id"] for entry in states if entry.get("player_id")}
    if not ids:
        return []
    id_list = sorted(ids)
    qmarks = ",".join("?" for _ in id_list)
    rows = db.execute(
        f"SELECT id, name FROM players WHERE id IN ({qmarks})",
        id_list,
    ).fetchall()
    names = {r["id"]: r["name"] for r in rows}
    presence = []
    for entry in states:
        pid = entry.get("player_id")
        if not pid:
            continue
        name = names.get(pid)
        if not name:
            continue
        presence.append({"name": name, "status": entry.get("status", "cooling")})
    return presence


def render_page(template_name: str, db, **context):
    context.setdefault("default_player", get_default_player(db))
    context.setdefault("online_players", get_online_players(db))
    context.setdefault("online_presence", get_online_presence(db))
    context.setdefault("hero_logo_filename", get_current_logo_filename())
    context.setdefault("hero_background_filename", get_current_logo_background_filename())
    context.setdefault("streak_badges", {})
    context.setdefault("streaks_by_name", {})
    if "page_title" not in context:
        season_label = context.get("season_label", "Koopa Krew")
        context["page_title"] = f"Koopa Krew — {season_label}"
    return render_template(template_name, **context)


def csv_response(filename: str, header: list[str], rows: list[list]):
    sio = io.StringIO()
    writer = csv.writer(sio)
    writer.writerow(header)
    for r in rows:
        writer.writerow(r)
    resp = make_response(sio.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp
