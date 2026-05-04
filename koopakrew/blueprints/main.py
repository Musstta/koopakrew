from flask import Blueprint

from koopakrew.blueprints.routes.admin import (
    admin_players,
    clear_default_player,
    presence_ping,
    set_default_player,
)
from koopakrew.blueprints.routes.archive import archive_page
from koopakrew.blueprints.routes.events import events_log, export_events, undo, update_result
from koopakrew.blueprints.routes.pages import rules_page
from koopakrew.blueprints.routes.standings import export_standings, index
from koopakrew.blueprints.routes.player import player_profile
from koopakrew.blueprints.routes.stats import stats_page, track_stats_page

bp = Blueprint("main", __name__)

_ROUTES: tuple[dict, ...] = (
    {"rule": "/",                             "endpoint": "index",              "view": index},
    {"rule": "/update/<int:track_id>",        "endpoint": "update_result",      "view": update_result,      "methods": ["GET", "POST"]},
    {"rule": "/undo",                         "endpoint": "undo",               "view": undo,               "methods": ["POST"]},
    {"rule": "/events",                       "endpoint": "events_log",         "view": events_log},
    {"rule": "/export/events.csv",            "endpoint": "export_events",      "view": export_events},
    {"rule": "/export/standings.csv",         "endpoint": "export_standings",   "view": export_standings},
    {"rule": "/stats",                        "endpoint": "stats_page",         "view": stats_page},
    {"rule": "/track-stats",                  "endpoint": "track_stats_page",   "view": track_stats_page},
    {"rule": "/archive",                      "endpoint": "archive_page",       "view": archive_page},
    {"rule": "/rules",                        "endpoint": "rules_page",         "view": rules_page},
    {"rule": "/admin/players",                "endpoint": "admin_players",      "view": admin_players,      "methods": ["GET", "POST"]},
    {"rule": "/admin/players/set-default",    "endpoint": "set_default_player", "view": set_default_player, "methods": ["POST"]},
    {"rule": "/admin/players/clear-default",  "endpoint": "clear_default_player","view": clear_default_player,"methods": ["POST"]},
    {"rule": "/presence/ping",                "endpoint": "presence_ping",      "view": presence_ping,      "methods": ["POST"]},
    {"rule": "/player/<int:player_id>",       "endpoint": "player_profile",     "view": player_profile},
)

for _route in _ROUTES:
    bp.add_url_rule(
        _route["rule"],
        endpoint=_route["endpoint"],
        view_func=_route["view"],
        **({} if "methods" not in _route else {"methods": _route["methods"]}),
    )


def register_route_aliases(app):
    for route in _ROUTES:
        endpoint = route["endpoint"]
        original = f"{bp.name}.{endpoint}"
        view_func = app.view_functions[original]
        options = {}
        if "methods" in route:
            options["methods"] = route["methods"]
        app.add_url_rule(route["rule"], endpoint=endpoint, view_func=view_func, **options)


__all__ = ["bp", "register_route_aliases"]
