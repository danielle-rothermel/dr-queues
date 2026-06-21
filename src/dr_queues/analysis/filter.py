from __future__ import annotations

from typing import Any

from dr_queues.events.schema import PipelineEvent


def filter_run_events(
    events: list[PipelineEvent],
    run_id: str,
) -> list[PipelineEvent]:
    return [event for event in events if event.run_id == run_id]


def filter_run_event_dicts(
    events: list[dict[str, Any]],
    run_id: str,
) -> list[dict[str, Any]]:
    return [event for event in events if event.get("run_id") == run_id]
