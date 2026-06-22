from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, Protocol, runtime_checkable

from dr_queues.events.schema import EventKind, PipelineEvent
from dr_queues.events.sink import EventSink
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.runtime.models import TargetHold

JobHandler = Callable[[JobEnvelope], JobEnvelope]
FAILURE_NOT_RECORDED: Final = object()


class StageExecutionAction(StrEnum):
    COMPLETED = "completed"
    FAILED_RECORDED = "failed_recorded"
    HELD = "held"
    REDELIVER = "redeliver"


@dataclass(frozen=True)
class StageExecutionResult:
    action: StageExecutionAction
    job: JobEnvelope

    @property
    def should_ack(self) -> bool:
        return self.action in {
            StageExecutionAction.COMPLETED,
            StageExecutionAction.FAILED_RECORDED,
            StageExecutionAction.HELD,
        }

    @property
    def should_forward(self) -> bool:
        return self.action == StageExecutionAction.COMPLETED


@runtime_checkable
class StageExecutionStore(Protocol):
    def append(self, event: PipelineEvent) -> None: ...

    def active_hold_for_tags(
        self,
        *,
        run_id: str,
        tags: dict[str, str],
    ) -> TargetHold | None: ...

    def hold_job(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
        hold: TargetHold,
    ) -> object | None: ...

    def mark_job_running(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object | None: ...

    def mark_job_completed(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object | None: ...

    def mark_job_terminal(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object | None: ...

    def record_job_failure(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
        error: Exception,
        worker_id: str | None,
        max_attempts: int,
        retry_delay_seconds: float,
    ) -> object: ...


class EventSinkStageExecutionStore:
    def __init__(self, sink: EventSink) -> None:
        self.sink = sink

    def append(self, event: PipelineEvent) -> None:
        self.sink.append(event)

    def active_hold_for_tags(
        self,
        *,
        run_id: str,
        tags: dict[str, str],
    ) -> TargetHold | None:
        del run_id, tags
        return None

    def hold_job(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
        hold: TargetHold,
    ) -> object | None:
        del job, stage, queue_name, hold
        return None

    def mark_job_running(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object | None:
        del job, stage, queue_name
        return None

    def mark_job_completed(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object | None:
        del job, stage, queue_name
        return None

    def mark_job_terminal(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object | None:
        del job, stage, queue_name
        return None

    def record_job_failure(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
        error: Exception,
        worker_id: str | None,
        max_attempts: int,
        retry_delay_seconds: float,
    ) -> object:
        del job, stage, queue_name, error, worker_id
        del max_attempts, retry_delay_seconds
        return FAILURE_NOT_RECORDED


class WorkerLogStates(StrEnum):
    STARTED = "started"
    FAILED = "failed"
    COMPLETED = "completed"


class StageExecution:
    def __init__(
        self,
        *,
        store: StageExecutionStore,
        stage_name: str,
        worker_id: str | None = None,
        max_attempts: int = 3,
        retry_delay_seconds: float = 300.0,
    ) -> None:
        self.store = store
        self.stage_name = stage_name
        self.worker_id = worker_id
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds

    @classmethod
    def from_event_sink(
        cls,
        *,
        event_sink: EventSink,
        stage_name: str,
        worker_id: str | None = None,
        max_attempts: int = 3,
        retry_delay_seconds: float = 300.0,
    ) -> StageExecution:
        store = (
            event_sink
            if isinstance(event_sink, StageExecutionStore)
            else EventSinkStageExecutionStore(event_sink)
        )
        return cls(
            store=store,
            stage_name=stage_name,
            worker_id=worker_id,
            max_attempts=max_attempts,
            retry_delay_seconds=retry_delay_seconds,
        )

    def run(
        self,
        *,
        job: JobEnvelope,
        queue_name: str,
        handler: JobHandler,
    ) -> StageExecutionResult:
        job.resolve_partition_key()
        hold = self.store.active_hold_for_tags(
            run_id=job.run_id,
            tags=job.target_tags,
        )
        if hold is not None:
            self.store.hold_job(
                job=job,
                stage=self.stage_name,
                queue_name=queue_name,
                hold=hold,
            )
            return StageExecutionResult(
                action=StageExecutionAction.HELD,
                job=job,
            )

        self.store.mark_job_running(
            job=job,
            stage=self.stage_name,
            queue_name=queue_name,
        )
        _log(self.stage_name, WorkerLogStates.STARTED, job)
        self.store.append(
            PipelineEvent(
                run_id=job.run_id,
                job_id=job.job_id,
                lane=job.lane,
                stage=self.stage_name,
                event=EventKind.STAGE_STARTED,
                payload={"step_index": job.step_index},
            ),
        )

        try:
            updated = handler(job)
        except Exception as error:
            _log(self.stage_name, WorkerLogStates.FAILED, job)
            failure_record = self.store.record_job_failure(
                job=job,
                stage=self.stage_name,
                queue_name=queue_name,
                error=error,
                worker_id=self.worker_id,
                max_attempts=self.max_attempts,
                retry_delay_seconds=self.retry_delay_seconds,
            )
            return StageExecutionResult(
                action=(
                    StageExecutionAction.REDELIVER
                    if failure_record is FAILURE_NOT_RECORDED
                    else StageExecutionAction.FAILED_RECORDED
                ),
                job=job,
            )

        _log(self.stage_name, WorkerLogStates.COMPLETED, updated)
        self.store.append(
            PipelineEvent(
                run_id=updated.run_id,
                job_id=updated.job_id,
                lane=updated.lane,
                stage=self.stage_name,
                event=EventKind.STAGE_OUTPUT,
                payload={
                    "step_index": updated.step_index,
                    "step_outputs": updated.step_outputs,
                    "step_record": updated.step_records.get(self.stage_name),
                },
            ),
        )
        return StageExecutionResult(
            action=StageExecutionAction.COMPLETED,
            job=updated,
        )

    def record_completed(
        self,
        *,
        job: JobEnvelope,
        queue_name: str,
    ) -> None:
        self.store.mark_job_completed(
            job=job,
            stage=self.stage_name,
            queue_name=queue_name,
        )

    def record_terminal(
        self,
        *,
        job: JobEnvelope,
        queue_name: str,
    ) -> None:
        self.store.append(
            PipelineEvent(
                run_id=job.run_id,
                job_id=job.job_id,
                lane=job.lane,
                stage=self.stage_name,
                event=EventKind.TERMINAL,
                payload=job.model_dump(),
            ),
        )
        self.store.mark_job_terminal(
            job=job,
            stage=self.stage_name,
            queue_name=queue_name,
        )

    def record_terminal_batch(
        self,
        *,
        jobs: list[tuple[JobEnvelope, str]],
    ) -> None:
        batch_recorder = getattr(self.store, "record_terminal_batch", None)
        if callable(batch_recorder):
            batch_recorder(jobs=jobs, stage=self.stage_name)
            return
        for job, queue_name in jobs:
            self.record_terminal(job=job, queue_name=queue_name)


def _log(stage: str, event: str, job: JobEnvelope) -> None:
    timestamp = datetime.now(tz=UTC).isoformat()
    print(
        f"{timestamp} stage={stage} event={event} "
        f"job_id={job.job_id} lane={job.lane}",
        flush=True,
    )
