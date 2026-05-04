from flask import request

from koopakrew.db import get_db
from koopakrew.helpers.archive import build_archive_entries, build_csv_archive, build_db_archive
from koopakrew.helpers.rendering import render_page
from koopakrew.queries.seasons import get_current_season_row


def archive_page():
    db = get_db()
    season_row = get_current_season_row(db)
    selected_id = request.args.get("season")
    current_label = season_row["label"] if season_row else None
    archive_entries = build_archive_entries(db)
    filtered_entries = [
        entry
        for entry in archive_entries
        if not (entry.get("season_label") == current_label and entry.get("type") == "db")
    ]
    entry = None
    if filtered_entries:
        entry = next(
            (item for item in filtered_entries if item["id"] == selected_id),
            filtered_entries[0],
        )
    archive_data = None
    if entry:
        if entry.get("type") == "csv":
            archive_data = build_csv_archive(entry)
        else:
            archive_data = build_db_archive(db, entry)
    active_label = (
        entry["label"] if entry else (season_row["label"] if season_row else "Koopa Krew")
    )
    return render_page(
        "archive.html",
        db,
        archive_data=archive_data,
        archive_entries=filtered_entries,
        selected_archive_id=entry["id"] if entry else None,
        season_label=active_label,
        page_title=f"Koopa Krew - Archive ({active_label})",
    )
