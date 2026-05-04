import os
import sqlite3
from typing import Any

from flask import current_app, g, has_app_context


def connect_database(path: Any):
    """Create a new sqlite connection with row dicts."""
    db = sqlite3.connect(os.fspath(path))
    db.row_factory = sqlite3.Row
    return db


def get_db():
    """Return the per-request sqlite handle, creating it on demand."""
    if "db" not in g:
        if not has_app_context():
            raise RuntimeError("No application context available for get_db().")
        db_path = current_app.config.get("DATABASE_PATH")
        if not db_path:
            raise RuntimeError("DATABASE_PATH is not configured.")
        g.db = connect_database(db_path)
    return g.db


def close_db(exception=None):
    """Close and drop the connection stored on g."""
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_app(app):
    """Register teardown handlers on the given Flask app."""
    app.teardown_appcontext(close_db)
