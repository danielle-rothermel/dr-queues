from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

from dr_queues.events.schema import PipelineEvent
from dr_queues.manifest.manifest import RunManifest


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


class SeedBatchStatus(StrEnum):
    CREATED = "created"
    PUBLISHED = "published"
    FAILED = "failed"


class WorkerStatus(StrEnum):
    RUNNING = "running"
    STOP_REQUESTED = "stop_requested"
    STOPPED = "stopped"
    STALE = "stale"


class WaitTarget(StrEnum):
    TERMINAL = "terminal"


class SeedBatch(BaseModel):
    batch_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    job_ids: list[str]
    count: int
    status: SeedBatchStatus = SeedBatchStatus.CREATED
    created_at: str = Field(default_factory=utc_now_iso)
    published_at: str | None = None
    failed_at: str | None = None
    failure_detail: str | None = None


class WorkerProcessRecord(BaseModel):
    worker_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    stage: str
    pid: int
    host: str
    workers: int
    handlers_module: str
    status: WorkerStatus = WorkerStatus.RUNNING
    started_at: str = Field(default_factory=utc_now_iso)
    last_heartbeat_at: str = Field(default_factory=utc_now_iso)
    stop_requested_at: str | None = None
    stopped_at: str | None = None
    command: list[str] = Field(default_factory=list)


class QueueSnapshot(BaseModel):
    name: str
    ready_messages: int
    consumers: int


class StageRunStatus(BaseModel):
    stage: str
    expected_jobs: int
    started_jobs: int
    completed_jobs: int
    in_flight_jobs: int
    input_queue: QueueSnapshot
    output_queue: QueueSnapshot
    workers: list[WorkerProcessRecord] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return self.completed_jobs >= self.expected_jobs


class RunStatus(BaseModel):
    run_id: str
    manifest: RunManifest
    expected_jobs: int
    terminal_jobs: int
    stages: list[StageRunStatus]
    workers: list[WorkerProcessRecord] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return self.terminal_jobs >= self.expected_jobs


class EventProgress(BaseModel):
    stage_started: dict[str, set[str]] = Field(default_factory=dict)
    stage_completed: dict[str, set[str]] = Field(default_factory=dict)
    terminal_jobs: set[str] = Field(default_factory=set)

    @classmethod
    def from_events(cls, events: list[PipelineEvent]) -> EventProgress:
        progress = cls()
        for event in events:
            match event.event:
                case "stage_started":
                    progress.stage_started.setdefault(event.stage, set()).add(
                        event.job_id,
                    )
                case "stage_output":
                    progress.stage_completed.setdefault(
                        event.stage, set()
                    ).add(
                        event.job_id,
                    )
                case "terminal":
                    progress.terminal_jobs.add(event.job_id)
        return progress
