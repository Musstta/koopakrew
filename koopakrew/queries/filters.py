from koopakrew.constants import STATE_ALIASES, STATE_VALUE_MAP


def _iter_filter_values(values):
    for value in values or []:
        if not value:
            continue
        text = value.strip()
        if not text:
            continue
        if "," in text:
            for chunk in text.split(","):
                cleaned = chunk.strip()
                if cleaned:
                    yield cleaned
        else:
            yield text


def normalize_owner_filters(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in _iter_filter_values(values):
        lowered = value.lower()
        if lowered == "all":
            return []
        if lowered in seen:
            continue
        normalized.append(value)
        seen.add(lowered)
    return normalized


def normalize_state_filters(values: list[str]) -> list[int]:
    """Return a list of state integers (from STATE_VALUE_MAP) after resolving aliases."""
    resolved: list[int] = []
    seen: set[int] = set()
    for value in _iter_filter_values(values):
        lowered = value.lower()
        if lowered == "any":
            return []
        canonical = STATE_ALIASES.get(lowered)
        if not canonical:
            continue
        val = STATE_VALUE_MAP.get(canonical)
        if val is None:
            continue
        if val in seen:
            continue
        resolved.append(val)
        seen.add(val)
    return resolved


class FilterBuilder:
    """Builds a WHERE clause + args list for track queries.

    Usage:
        where_str, args = (
            FilterBuilder(season_id)
            .with_owners(owner_names, alias="po")
            .with_cup(cup_code)
            .with_states(state_filters)
            .build()
        )

    The caller may append extra conditions after `build()` returns the parts.
    Each integration passes its own player table alias since different queries
    join players under different aliases.
    """

    def __init__(self, season_id: int):
        self._where: list[str] = ["t.season = ?"]
        self._args: list = [season_id]

    def with_owners(self, owner_names, *, alias: str = "p") -> "FilterBuilder":
        owners = normalize_owner_filters(
            [owner_names] if isinstance(owner_names, str) else (owner_names or [])
        )
        if owners:
            ph = ",".join("?" * len(owners))
            self._where.append(f"{alias}.name IN ({ph})")
            self._args.extend(owners)
        return self

    def with_cup(self, cup_code) -> "FilterBuilder":
        if cup_code and cup_code.lower() != "all":
            self._where.append("c.code = ?")
            self._args.append(cup_code)
        return self

    def with_states(self, state_filters) -> "FilterBuilder":
        if isinstance(state_filters, str):
            state_filters = [state_filters]
        states = normalize_state_filters(state_filters or [])
        if states:
            ph = ",".join("?" * len(states))
            self._where.append(f"t.state IN ({ph})")
            self._args.extend(states)
        return self

    def build(self) -> tuple[str, list]:
        return " AND ".join(self._where), list(self._args)
