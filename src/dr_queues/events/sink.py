from __future__ import annotations

from typing import Protocol, runtime_checkable

from dr_queues.events.schema import PipelineEvent


@runtime_checkable
class EventSink(Protocol):
    def append(self, event: PipelineEvent) -> None: ...

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]: ...

    def close(self) -> None: ...
