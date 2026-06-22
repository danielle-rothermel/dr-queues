from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from dr_queues.events.schema import PipelineEvent
from dr_queues.manifest.manifest import RunManifest
from dr_queues.targeting import TargetSelector


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


class WorkerRuntime(StrEnum):
    IN_PROCESS = "in_process"
    DETACHED = "detached"


class WaitTarget(StrEnum):
    TERMINAL = "terminal"


class JobStateStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    RETRY_WAITING = "retry_waiting"
    HELD = "held"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"
    TERMINAL = "terminal"


class JobAttemptAction(StrEnum):
    RETRY_WAITING = "retry_waiting"
    DEAD_LETTERED = "dead_lettered"


class RunHealth(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETE = "complete"
    RETRY_WAITING = "retry_waiting"
    HELD = "held"
    STALE_WORKERS = "stale_workers"
    DEAD_LETTERED = "dead_lettered"


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


class WorkerRecord(BaseModel):
    worker_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    stage: str
    pid: int
    host: str
    concurrency: int
    runtime: WorkerRuntime
    handlers_module: str
    status: WorkerStatus = WorkerStatus.RUNNING
    started_at: str = Field(default_factory=utc_now_iso)
    last_heartbeat_at: str = Field(default_factory=utc_now_iso)
    stop_requested_at: str | None = None
    stopped_at: str | None = None
    command: list[str] = Field(default_factory=list)
    include_selectors: list[TargetSelector] = Field(default_factory=list)
    exclude_selectors: list[TargetSelector] = Field(default_factory=list)


class JobState(BaseModel):
    run_id: str
    job_id: str
    stage: str
    status: JobStateStatus
    partition_key: str
    target_tags: dict[str, str] = Field(default_factory=dict)
    queue_name: str
    job: dict[str, Any]
    attempt_count: int = 0
    not_before: str | None = None
    held_until: str | None = None
    hold_id: str | None = None
    failure_detail: str | None = None
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)


class JobAttempt(BaseModel):
    attempt_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    job_id: str
    stage: str
    partition_key: str
    target_tags: dict[str, str] = Field(default_factory=dict)
    attempt_number: int
    action: JobAttemptAction
    error_type: str
    error_message: str
    worker_id: str | None = None
    created_at: str = Field(default_factory=utc_now_iso)


class TargetHold(BaseModel):
    hold_id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    selectors: list[TargetSelector]
    reason: str | None = None
    blocked_until: str | None = None
    created_at: str = Field(default_factory=utc_now_iso)
    cleared_at: str | None = None

    @property
    def is_active(self) -> bool:
        return self.cleared_at is None


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
    workers: list[WorkerRecord] = Field(default_factory=list)
    job_state_counts: dict[JobStateStatus, int] = Field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        return self.completed_jobs >= self.expected_jobs


class RunStatus(BaseModel):
    run_id: str
    manifest: RunManifest
    expected_jobs: int
    terminal_jobs: int
    stages: list[StageRunStatus]
    workers: list[WorkerRecord] = Field(default_factory=list)
    job_state_counts: dict[JobStateStatus, int] = Field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        return self.terminal_jobs >= self.expected_jobs


class RunRecord(BaseModel):
    run_id: str
    manifest: RunManifest
    created_at: str
    updated_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunSummary(BaseModel):
    run_id: str
    pipeline_id: str
    created_at: str
    updated_at: str
    expected_jobs: int
    terminal_jobs: int
    stage_names: list[str]
    partitions: list[str]
    job_state_counts: dict[JobStateStatus, int] = Field(default_factory=dict)
    active_workers: int
    stale_workers: int
    health: RunHealth
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunObservation(BaseModel):
    run_id: str
    summary: RunSummary
    status: RunStatus
    partitions: list[str]
    active_holds: list[TargetHold] = Field(default_factory=list)
    blocked_jobs: list[JobState] = Field(default_factory=list)
    recent_attempts: list[JobAttempt] = Field(default_factory=list)
    recent_events: list[PipelineEvent] = Field(default_factory=list)


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


def count_job_states(states: list[JobState]) -> dict[JobStateStatus, int]:
    counts = dict.fromkeys(JobStateStatus, 0)
    for state in states:
        counts[state.status] += 1
    return counts
