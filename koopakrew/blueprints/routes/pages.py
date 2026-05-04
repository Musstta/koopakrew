from koopakrew.db import get_db
from koopakrew.helpers.rendering import render_page
from koopakrew.queries.seasons import get_current_season_row


def rules_page():
    db = get_db()
    season_row = get_current_season_row(db)
    season_label = season_row["label"] if season_row else "Koopa Krew"
    return render_page(
        "rules.html",
        db,
        season_label=season_label,
        page_title="Koopa Krew - Rules",
    )
