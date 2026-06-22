from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

DEFAULT_PARTITION_KEY = "default"
PARTITION_KEY_SEPARATOR = "__"
SELECTOR_SEPARATOR = "="
RELATIVE_TIME_PATTERN = re.compile(r"^\+(?P<count>\d+)(?P<unit>[smhd])$")


class TargetSelector(BaseModel):
    key: str
    value: str

    @classmethod
    def parse(cls, value: str) -> TargetSelector:
        if SELECTOR_SEPARATOR not in value:
            msg = f"Invalid selector {value!r}; expected key=value"
            raise ValueError(msg)
        key, selector_value = value.split(SELECTOR_SEPARATOR, 1)
        key = key.strip()
        selector_value = selector_value.strip()
        if not key or not selector_value:
            msg = f"Invalid selector {value!r}; expected non-empty key=value"
            raise ValueError(msg)
        return cls(key=key, value=selector_value)

    def matches(self, tags: dict[str, str]) -> bool:
        return tags.get(self.key) == self.value


def parse_selectors(
    values: list[str] | tuple[str, ...],
) -> list[TargetSelector]:
    return sorted(
        [TargetSelector.parse(value) for value in values if value.strip()],
        key=lambda selector: (selector.key, selector.value),
    )


def target_matches(
    tags: dict[str, str],
    *,
    include: list[TargetSelector] | None = None,
    exclude: list[TargetSelector] | None = None,
) -> bool:
    include = include or []
    exclude = exclude or []
    if include and not all(selector.matches(tags) for selector in include):
        return False
    return not any(selector.matches(tags) for selector in exclude)


def derive_partition_key(tags: dict[str, str]) -> str:
    if not tags:
        return DEFAULT_PARTITION_KEY
    if quota_pool := tags.get("quota_pool"):
        return sanitize_partition_key(quota_pool)
    parts = [
        f"{sanitize_partition_key(key)}{PARTITION_KEY_SEPARATOR}"
        f"{sanitize_partition_key(value)}"
        for key, value in sorted(tags.items())
    ]
    return PARTITION_KEY_SEPARATOR.join(parts) or DEFAULT_PARTITION_KEY


def sanitize_partition_key(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return sanitized.strip("_") or DEFAULT_PARTITION_KEY


def parse_blocked_until(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    match = RELATIVE_TIME_PATTERN.match(value)
    if match is not None:
        count = int(match.group("count"))
        unit = match.group("unit")
        delta = {
            "s": timedelta(seconds=count),
            "m": timedelta(minutes=count),
            "h": timedelta(hours=count),
            "d": timedelta(days=count),
        }[unit]
        return (datetime.now(tz=UTC) + delta).isoformat()
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def is_due(value: str | None) -> bool:
    if value is None:
        return True
    due_at = datetime.fromisoformat(value)
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=UTC)
    return due_at <= datetime.now(tz=UTC)
