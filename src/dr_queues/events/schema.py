from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class EventKind(StrEnum):
    STAGE_STARTED = "stage_started"
    STAGE_OUTPUT = "stage_output"
    TERMINAL = "terminal"


class PipelineEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    job_id: str
    lane: str
    stage: str
    event: EventKind
    timestamp: str = Field(
        default_factory=lambda: datetime.now(tz=UTC).isoformat(),
    )
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> PipelineEvent:
        return cls.model_validate_json(payload)


def filter_run_events(
    events: list[PipelineEvent],
    run_id: str,
) -> list[PipelineEvent]:
    return [event for event in events if event.run_id == run_id]
