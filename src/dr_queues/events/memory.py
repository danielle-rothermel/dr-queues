from __future__ import annotations

from threading import Lock

from dr_queues.events.schema import PipelineEvent
from dr_queues.events.sink import EventSink


class MemoryEventSink:
    def __init__(self) -> None:
        self._events: list[PipelineEvent] = []
        self._lock = Lock()

    def append(self, event: PipelineEvent) -> None:
        with self._lock:
            self._events.append(event)

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        with self._lock:
            return [event for event in self._events if event.run_id == run_id]

    def read_all(self) -> list[PipelineEvent]:
        with self._lock:
            return list(self._events)

    def close(self) -> None:
        return None


class CompositeEventSink:
    def __init__(self, sinks: list[EventSink]) -> None:
        self._sinks = sinks

    def append(self, event: PipelineEvent) -> None:
        for sink in self._sinks:
            sink.append(event)

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        if not self._sinks:
            return []
        return self._sinks[0].read_by_run_id(run_id)

    def close(self) -> None:
        for sink in self._sinks:
            sink.close()
