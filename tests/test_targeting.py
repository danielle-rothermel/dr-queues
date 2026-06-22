from datetime import UTC, datetime

from dr_queues.targeting import (
    derive_partition_key,
    parse_blocked_until,
    parse_selectors,
    target_matches,
)


def test_parse_selectors_and_match_tags() -> None:
    selectors = parse_selectors(["provider=openai", "model=nano"])

    assert target_matches(
        {"provider": "openai", "model": "nano"},
        include=selectors,
    )
    assert not target_matches(
        {"provider": "openai", "model": "other"},
        include=selectors,
    )


def test_exclude_selector_rejects_matching_tags() -> None:
    assert not target_matches(
        {"provider": "gemini"},
        exclude=parse_selectors(["provider=gemini"]),
    )


def test_partition_key_prefers_quota_pool() -> None:
    assert (
        derive_partition_key(
            {"provider": "gemini", "quota_pool": "gemini-flash"}
        )
        == "gemini-flash"
    )


def test_parse_blocked_until_accepts_relative_duration() -> None:
    blocked_until = parse_blocked_until("+30m")

    assert blocked_until is not None
    assert datetime.fromisoformat(blocked_until) > datetime.now(tz=UTC)
