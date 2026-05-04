import os
from pathlib import Path
import sqlite3

import app as kk_app
from koopakrew.db import connect_database
from db_init import create_schema, run_migrations


def ensure_schema(app):
    db_path = Path(app.config["DATABASE_PATH"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = connect_database(db_path)
    try:
        db.execute("SELECT 1 FROM season_meta LIMIT 1")
    except sqlite3.OperationalError:
        create_schema(db)
    finally:
        db.close()


def ensure_migrations(app):
    db_path = Path(app.config["DATABASE_PATH"])
    db = connect_database(db_path)
    try:
        run_migrations(db)
    finally:
        db.close()


app = kk_app.app
ensure_schema(app)
ensure_migrations(app)

from koopakrew.services import profile_worker
profile_worker.start(app)

if __name__ == "__main__":
    host = os.environ.get("KOOPAKREW_HOST", "0.0.0.0")
    port = int(os.environ.get("KOOPAKREW_PORT", os.environ.get("PORT", "5000")))
    debug_flag = os.environ.get("KOOPAKREW_DEBUG", "false").lower() in ("1", "true", "yes")
    app.run(host=host, port=port, debug=debug_flag)
