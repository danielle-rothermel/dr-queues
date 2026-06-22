from __future__ import annotations

from dr_queues.events.schema import PipelineEvent


def filter_run_events(
    events: list[PipelineEvent],
    run_id: str,
) -> list[PipelineEvent]:
    return [event for event in events if event.run_id == run_id]
