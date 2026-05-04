"""
Compute the 5 pentagon-chart attribute scores and 2 trend pills for a player profile.

Attributes (normalised against fixed absolute ceilings, not group-max):
  Aggression  — offensive pressure (tracks taken, hunter marks, non-owner wins)
  Defense     — defensive solidity (success rate, at-risk rate, best streak)
  Dominance   — territorial control (tracks owned, locked, cups)
  Consistency — reliable performance (win rate, best win streak)
  Clutch      — high-stakes wins with recency decay (λ=0.95)

Ceilings represent "exceptional but achievable" performance in a typical season
(~300 events, 4 players). Calibrated against Season 3 data where the dominant
player scored ~80% across attributes rather than 100%.

Trend pills (informational, not scaled against ceilings):
  Momentum  — win ratio over the player's last 10 events scaled to 0–10
  Presence  — session-day attendance fraction scaled to 0–100
"""

from __future__ import annotations

_DECAY = 0.95

# Absolute ceilings: "a truly exceptional season" in each attribute.
# Calibrated so the current dominant player lands ~75-85 rather than 100.
_ATTR_CEILINGS: dict[str, float] = {
    # tracks_taken=40×2 + hunter_marks=25×1.5 + wins_as_non_owner=80
    "aggression": 200.0,
    # defense_success_rate=80%×0.5 + at_risk_rate=65%×0.3 + best_streak=8×5
    "defense": 65.0,
    # tracks_owned=40×3 + locked=15×2 + cups=3×10
    "dominance": 180.0,
    # win_rate=90%×0.6 + best_win_streak=55×4
    "consistency": 274.0,
    # ~20 clutch events with full recency decay (decay sum ≈ 18.5 for n=20)
    "clutch": 18.0,
}


def _safe(value, default: float = 0.0) -> float:
    return float(value) if value is not None else default


def _raw_scores(ps: dict, clutch_raw: float) -> dict[str, float]:
    aggression = (
        _safe(ps.get("tracks_taken")) * 2.0
        + _safe(ps.get("hunter_marks")) * 1.5
        + _safe(ps.get("wins_as_non_owner"))
    )
    defense = (
        _safe(ps.get("defense_success_rate")) * 0.5
        + _safe(ps.get("defense_at_risk_rate")) * 0.3
        + _safe(ps.get("best_defense_streak")) * 5.0
    )
    dominance = (
        _safe(ps.get("tracks_owned")) * 3.0
        + _safe(ps.get("locked_tracks")) * 2.0
        + _safe(ps.get("cups_owned_count")) * 10.0
    )
    consistency = (
        _safe(ps.get("win_rate")) * 0.6
        + _safe(ps.get("best_win_streak")) * 4.0
    )
    return {
        "aggression": aggression,
        "defense": defense,
        "dominance": dominance,
        "consistency": consistency,
        "clutch": clutch_raw,
    }


def _clutch_raw(clutch_events: list[dict], player_id: int) -> float:
    """Apply per-event recency decay (λ=0.95) to clutch-qualifying wins."""
    relevant = [
        e for e in clutch_events
        if e.get("winner_id") == player_id
        and (
            e.get("pre_state") == -1
            or e.get("pre_threatened_by_id") == player_id
        )
    ]
    n = len(relevant)
    if n == 0:
        return 0.0
    return sum(_DECAY ** (n - 1 - i) for i in range(n))


def compute_profile_attributes(
    player_id: int,
    player_stats: dict,
    all_player_stats: list[dict],  # kept for API compatibility; not used for scoring
    clutch_events: list[dict],
    last_10_events: list[dict],
    presence_player_events: int,
    presence_total_events: int,
) -> dict:
    """
    Returns:
      {
        "scores":    {"aggression": 0-100, ...},
        "breakdown": {"aggression": {"raw": float, "ceiling": float}, ...},
        "trend":     {"momentum": 0-10, "presence": 0-100, ...},
      }
    """
    clutch_raw_me = _clutch_raw(clutch_events, player_id)
    my_raw = _raw_scores(player_stats, clutch_raw_me)

    scores: dict[str, float] = {}
    breakdown: dict[str, dict] = {}
    for attr, ceiling in _ATTR_CEILINGS.items():
        raw = my_raw[attr]
        scores[attr] = min(100.0, raw / ceiling * 100.0)
        breakdown[attr] = {"raw": raw, "ceiling": ceiling}

    # --- Momentum pill: win ratio over last 10 involved events, scaled 0-10 ---
    wins_in_10 = sum(
        1 for e in last_10_events if e.get("winner_id") == player_id
    )
    total_in_10 = len(last_10_events)
    momentum_10 = (wins_in_10 / total_in_10 * 10.0) if total_in_10 else 0.0

    # --- Presence pill: event participation fraction, 0-100 ---
    if presence_total_events > 0:
        presence = min(100.0, presence_player_events / presence_total_events * 100.0)
    else:
        presence = 0.0

    return {
        "scores": scores,
        "breakdown": breakdown,
        "trend": {
            "momentum": momentum_10,          # 0-10
            "presence": presence,             # 0-100
            "presence_player_events": presence_player_events,
            "presence_total_events": presence_total_events,
            "momentum_wins": wins_in_10,
            "momentum_total": total_in_10,
        },
    }


__all__ = ["compute_profile_attributes"]
