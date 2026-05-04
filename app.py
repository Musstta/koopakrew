"""Thin shim — the real application lives in the koopakrew package.

Re-exports here keep tests and run.py working without modification.
"""
import logging
import os
import time  # re-exported for tests

from koopakrew import create_app
from koopakrew.constants import ONLINE_TIMEOUT_SECONDS
from koopakrew.db import get_db
from koopakrew.helpers.archive import build_archive_entries
from koopakrew.i18n import translate_text
from koopakrew.helpers.rendering import get_online_players, get_online_presence
from koopakrew.queries.seasons import create_season_for_today, get_current_season_row
from koopakrew.services.core import apply_result, deactivate_player, undo_last_event
from koopakrew.stats.compute import compute_player_streaks

app = create_app()
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    debug_flag = os.environ.get("KOOPAKREW_DEBUG", "true").lower() in ("1", "true", "yes")
    host = os.environ.get("KOOPAKREW_HOST", "0.0.0.0")
    port = int(os.environ.get("KOOPAKREW_PORT", os.environ.get("PORT", "5000")))
    app.run(host=host, port=port, debug=debug_flag)
