import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from flask import Flask

from .blueprints.main import bp as main_bp
from .blueprints.main import register_route_aliases
from .config import CONFIG_MAP, DevelopmentConfig
from .constants import BASE_DIR, ONLINE_TIMEOUT_SECONDS, PRESENCE_FRESH_SECONDS, PRESENCE_WARMING_SECONDS

INSTANCE_PATH = BASE_DIR / "instance"


def _read_env_file(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    if not path.exists():
        return data
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            data[key] = value
    return data


def _resolve_config(config_obj: Any):
    if config_obj is None:
        config_obj = os.environ.get("KOOPAKREW_CONFIG", "development")
    if isinstance(config_obj, str):
        key = config_obj.lower()
        if key not in CONFIG_MAP:
            raise KeyError(f"Unknown config '{config_obj}'.")
        return CONFIG_MAP[key]
    return config_obj


def _finalize_database_path(app: Flask):
    existing = app.config.get("DATABASE_PATH")
    if existing:
        path = Path(existing)
    else:
        filename = app.config.get("DATABASE_FILENAME", "koopakrew.db")
        path = Path(filename)
        if not path.is_absolute():
            path = Path(app.instance_path) / path
    path.parent.mkdir(parents=True, exist_ok=True)
    app.config["DATABASE_PATH"] = str(path)


def create_app(config_obj=None):
    """Application factory that builds the Koopa Krew Flask app."""
    INSTANCE_PATH.mkdir(parents=True, exist_ok=True)
    app = Flask(
        "koopakrew",
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
        instance_path=str(INSTANCE_PATH),
        instance_relative_config=True,
    )
    resolved_config = _resolve_config(config_obj)
    app.config.from_object(resolved_config or DevelopmentConfig)
    app.config.from_pyfile("config.py", silent=True)
    env_name = app.config.get("ENV")
    if env_name == "development":
        env_map = _read_env_file(BASE_DIR / ".env.development")
        if env_map:
            app.config.from_mapping(env_map)
    elif env_name == "production":
        env_map = _read_env_file(BASE_DIR / ".env.production")
        if env_map:
            app.config.from_mapping(env_map)

    secret = (
        app.config.get("SECRET_KEY")
        or app.config.get("KOOPAKREW_SECRET")
        or os.environ.get("KOOPAKREW_SECRET")
    )
    if app.config.get("ENV") == "production" and not secret:
        raise RuntimeError("SECRET_KEY must be set when running in production.")
    if not secret:
        secret = "koopakrew-dev-secret"
    app.config["SECRET_KEY"] = secret
    app.secret_key = secret

    _finalize_database_path(app)

    # Presence service
    from .infra.presence import PresenceService
    app.extensions["presence"] = PresenceService(
        ONLINE_TIMEOUT_SECONDS, PRESENCE_FRESH_SECONDS, PRESENCE_WARMING_SECONDS
    )

    # Database teardown
    from .db import init_app as init_db
    init_db(app)

    # Bind services
    from .services import core as services_core
    services_core.bind_app(app)

    # i18n hooks
    from .i18n import inject_i18n, set_language
    app.before_request(set_language)
    app.context_processor(inject_i18n)

    # Jinja filters
    @app.template_filter("localdt")
    def localdt_filter(value):
        if not value:
            return value
        try:
            dt = datetime.fromisoformat(str(value))
            tz = app.config.get("LOCAL_TIMEZONE", "America/Costa_Rica")
            local = dt.astimezone(ZoneInfo(tz))
            return local.strftime("%b %-d, %Y — %-I:%M %p")
        except Exception:
            return value

    # CSS cache-busting version
    _css_path = BASE_DIR / "static" / "css" / "app.css"
    if _css_path.exists():
        _css_hash = hashlib.md5(_css_path.read_bytes()).hexdigest()[:10]
    else:
        _css_hash = "1"
    app.config["CSS_VERSION"] = _css_hash

    # Blueprints & route aliases
    app.register_blueprint(main_bp)
    register_route_aliases(app)

    return app
