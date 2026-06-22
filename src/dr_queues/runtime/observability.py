from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from dr_queues.events.schema import PipelineEvent
from dr_queues.runtime.models import (
    JobAttempt,
    JobState,
    JobStateStatus,
    RunHealth,
    RunObservation,
    RunRecord,
    RunStatus,
    RunSummary,
    WorkerRecord,
    WorkerStatus,
    count_job_states,
)
from dr_queues.runtime.status import get_run_status

if TYPE_CHECKING:
    from dr_queues.runtime.store import MongoRunStore

DEFAULT_RUN_LIMIT = 50
DEFAULT_DETAIL_LIMIT = 100
ACTIVE_WORKER_STATUSES = {
    WorkerStatus.RUNNING,
    WorkerStatus.STOP_REQUESTED,
}
BLOCKED_JOB_STATUSES = {
    JobStateStatus.RETRY_WAITING,
    JobStateStatus.HELD,
    JobStateStatus.FAILED,
    JobStateStatus.DEAD_LETTERED,
}

type RunStatusGetter = Callable[
    [str],
    RunStatus,
]


def list_run_summaries(
    run_store: MongoRunStore,
    *,
    limit: int = DEFAULT_RUN_LIMIT,
) -> list[RunSummary]:
    return [
        build_run_summary(
            record=record,
            job_states=run_store.list_job_states(record.run_id),
            workers=run_store.list_workers(record.run_id),
            partitions=run_store.list_run_partitions(record.run_id),
            expected_jobs=run_store.expected_job_count(record.run_id),
        )
        for record in run_store.list_runs(limit=limit)
    ]


def get_run_observation(
    run_id: str,
    *,
    run_store: MongoRunStore,
    status_getter: RunStatusGetter | None = None,
    detail_limit: int = DEFAULT_DETAIL_LIMIT,
) -> RunObservation:
    record = run_store.get_run_record(run_id)
    job_states = run_store.list_job_states(run_id)
    workers = run_store.list_workers(run_id)
    partitions = run_store.list_run_partitions(run_id)
    summary = build_run_summary(
        record=record,
        job_states=job_states,
        workers=workers,
        partitions=partitions,
        expected_jobs=run_store.expected_job_count(run_id),
    )
    status = (
        status_getter(run_id)
        if status_getter is not None
        else get_run_status(run_id, run_store=run_store)
    )
    return RunObservation(
        run_id=run_id,
        summary=summary,
        status=status,
        partitions=partitions,
        active_holds=run_store.list_target_holds(run_id),
        blocked_jobs=blocked_job_states(job_states, limit=detail_limit),
        recent_attempts=run_store.list_job_attempts(
            run_id,
            limit=detail_limit,
        ),
        recent_events=run_store.list_recent_events(
            run_id,
            limit=detail_limit,
        ),
    )


def build_run_summary(
    *,
    record: RunRecord,
    job_states: list[JobState],
    workers: list[WorkerRecord],
    partitions: list[str],
    expected_jobs: int,
) -> RunSummary:
    job_state_counts = count_job_states(job_states)
    terminal_jobs = job_state_counts[JobStateStatus.TERMINAL]
    stale_workers = _count_workers(workers, WorkerStatus.STALE)
    active_workers = sum(
        1 for worker in workers if worker.status in ACTIVE_WORKER_STATUSES
    )
    return RunSummary(
        run_id=record.run_id,
        pipeline_id=record.manifest.pipeline_id,
        created_at=record.created_at,
        updated_at=record.updated_at,
        expected_jobs=expected_jobs,
        terminal_jobs=terminal_jobs,
        stage_names=[stage.name for stage in record.manifest.stages],
        partitions=partitions,
        job_state_counts=job_state_counts,
        active_workers=active_workers,
        stale_workers=stale_workers,
        health=derive_run_health(
            expected_jobs=expected_jobs,
            terminal_jobs=terminal_jobs,
            job_state_counts=job_state_counts,
            stale_workers=stale_workers,
            active_workers=active_workers,
        ),
        metadata=record.metadata,
    )


def derive_run_health(
    *,
    expected_jobs: int,
    terminal_jobs: int,
    job_state_counts: dict[JobStateStatus, int],
    stale_workers: int,
    active_workers: int,
) -> RunHealth:
    if job_state_counts[JobStateStatus.DEAD_LETTERED]:
        return RunHealth.DEAD_LETTERED
    if job_state_counts[JobStateStatus.FAILED]:
        return RunHealth.DEAD_LETTERED
    if job_state_counts[JobStateStatus.HELD]:
        return RunHealth.HELD
    if job_state_counts[JobStateStatus.RETRY_WAITING]:
        return RunHealth.RETRY_WAITING
    if stale_workers:
        return RunHealth.STALE_WORKERS
    if expected_jobs > 0 and terminal_jobs >= expected_jobs:
        return RunHealth.COMPLETE
    if active_workers or any(count for count in job_state_counts.values()):
        return RunHealth.RUNNING
    return RunHealth.CREATED


def blocked_job_states(
    job_states: list[JobState],
    *,
    limit: int = DEFAULT_DETAIL_LIMIT,
) -> list[JobState]:
    blocked = [
        state for state in job_states if state.status in BLOCKED_JOB_STATUSES
    ]
    return sorted(
        blocked,
        key=lambda state: state.updated_at,
        reverse=True,
    )[:limit]


def _count_workers(
    workers: list[WorkerRecord],
    status: WorkerStatus,
) -> int:
    return sum(1 for worker in workers if worker.status == status)
