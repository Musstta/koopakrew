import os
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import current_app, url_for

from koopakrew.constants import SEASONAL_LOGOS, STATIC_IMAGE_EXTS


def resolve_asset_path(subdir: str, code: str | None) -> str | None:
    if not code or not current_app.static_folder:
        return None
    slug = code.lower()
    for ext in STATIC_IMAGE_EXTS:
        rel = os.path.join("images", subdir, f"{slug}.{ext}")
        abs_path = os.path.join(current_app.static_folder, rel)
        if os.path.exists(abs_path):
            return rel.replace("\\", "/")
    return None


def cup_image_path(code: str | None) -> str | None:
    return resolve_asset_path("cups", code)


def track_image_path(code: str | None) -> str | None:
    return resolve_asset_path("tracks", code)


def cup_image_url(cup_dict: dict) -> str | None:
    local = cup_image_path(cup_dict.get("cup_code"))
    if local:
        return url_for("static", filename=local)
    return None


def track_image_url(track_dict: dict) -> str | None:
    local = track_image_path(track_dict.get("track_code"))
    if local:
        return url_for("static", filename=local)
    return None


def decorate_standings_with_art(standings: list[dict]) -> list[dict]:
    for cup in standings:
        cup["logo_src"] = cup_image_url(cup)
        for track in cup["tracks"]:
            track["image_src"] = track_image_url(track)
    return standings


def get_current_logo_filename() -> str:
    tz_name = current_app.config.get("LOCAL_TIMEZONE", "America/Costa_Rica")
    month = datetime.now(ZoneInfo(tz_name)).month
    return SEASONAL_LOGOS.get(month, "KoopaKrew.png")


def get_current_logo_background_filename() -> str:
    tz_name = current_app.config.get("LOCAL_TIMEZONE", "America/Costa_Rica")
    month = datetime.now(ZoneInfo(tz_name)).month
    if month == 12:
        return "Wreath.png"
    return "KoopalingsMK8.png"
