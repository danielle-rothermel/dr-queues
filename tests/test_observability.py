from __future__ import annotations

import pytest

from dr_queues.manifest import RunManifest, RunStageManifest
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.runtime.models import (
    JobState,
    JobStateStatus,
    RunHealth,
    RunRecord,
    WorkerProcessRecord,
    WorkerStatus,
)
from dr_queues.runtime.observability import (
    blocked_job_states,
    build_run_summary,
    derive_run_health,
)
from dr_queues.workflow.definition import (
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
)


def _manifest() -> RunManifest:
    return RunManifest(
        run_id="run-1",
        pipeline_definition=PipelineDefinition(
            id="demo",
            lanes=[PipelineLane(id="lane-a")],
            steps=[PipelineStep(name="parse", handler_key="parse")],
        ),
        expected_jobs=2,
        queue_prefix="run.run-1",
        stages=[
            RunStageManifest(
                name="parse",
                step_index=0,
                handler_key="parse",
                input_queue="run.run-1.s1.pending",
                output_queue="run.run-1.s1.completed",
                default_workers=1,
            ),
        ],
    )


def _job_state(
    *,
    job_id: str,
    status: JobStateStatus,
    updated_at: str = "2026-01-01T00:00:00+00:00",
) -> JobState:
    job = JobEnvelope(
        run_id="run-1",
        job_id=job_id,
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
    )
    return JobState(
        run_id="run-1",
        job_id=job_id,
        stage="parse",
        status=status,
        partition_key="default",
        queue_name="run.run-1.s1.pending",
        job=job.model_dump(),
        updated_at=updated_at,
    )


@pytest.mark.parametrize(
    ("counts", "stale_workers", "active_workers", "expected"),
    [
        ({JobStateStatus.DEAD_LETTERED: 1}, 0, 0, RunHealth.DEAD_LETTERED),
        ({JobStateStatus.FAILED: 1}, 0, 0, RunHealth.DEAD_LETTERED),
        ({JobStateStatus.HELD: 1}, 0, 0, RunHealth.HELD),
        (
            {JobStateStatus.RETRY_WAITING: 1},
            0,
            0,
            RunHealth.RETRY_WAITING,
        ),
        ({}, 1, 0, RunHealth.STALE_WORKERS),
        ({JobStateStatus.TERMINAL: 2}, 0, 0, RunHealth.COMPLETE),
        ({JobStateStatus.PENDING: 1}, 0, 0, RunHealth.RUNNING),
        ({}, 0, 1, RunHealth.RUNNING),
        ({}, 0, 0, RunHealth.CREATED),
    ],
)
def test_derive_run_health_prioritizes_operator_visible_states(
    counts: dict[JobStateStatus, int],
    stale_workers: int,
    active_workers: int,
    expected: RunHealth,
) -> None:
    all_counts = dict.fromkeys(JobStateStatus, 0)
    all_counts.update(counts)

    health = derive_run_health(
        expected_jobs=2,
        terminal_jobs=all_counts[JobStateStatus.TERMINAL],
        job_state_counts=all_counts,
        stale_workers=stale_workers,
        active_workers=active_workers,
    )

    assert health == expected


def test_build_run_summary_counts_terminal_and_workers() -> None:
    record = RunRecord(
        run_id="run-1",
        manifest=_manifest(),
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:01+00:00",
        metadata={"owner": "demo"},
    )
    workers = [
        WorkerProcessRecord(
            run_id="run-1",
            stage="parse",
            pid=100,
            host="local",
            workers=2,
            handlers_module="handlers",
        ),
        WorkerProcessRecord(
            run_id="run-1",
            stage="parse",
            pid=101,
            host="local",
            workers=1,
            handlers_module="handlers",
            status=WorkerStatus.STALE,
        ),
    ]

    summary = build_run_summary(
        record=record,
        job_states=[
            _job_state(job_id="job-1", status=JobStateStatus.TERMINAL),
            _job_state(job_id="job-2", status=JobStateStatus.TERMINAL),
        ],
        workers=workers,
        partitions=["default"],
    )

    assert summary.pipeline_id == "demo"
    assert summary.terminal_jobs == 2
    assert summary.active_workers == 1
    assert summary.stale_workers == 1
    assert summary.metadata == {"owner": "demo"}


def test_blocked_job_states_returns_newest_blocked_states() -> None:
    states = [
        _job_state(
            job_id="pending",
            status=JobStateStatus.PENDING,
            updated_at="2026-01-01T00:00:03+00:00",
        ),
        _job_state(
            job_id="old-held",
            status=JobStateStatus.HELD,
            updated_at="2026-01-01T00:00:01+00:00",
        ),
        _job_state(
            job_id="new-dead-letter",
            status=JobStateStatus.DEAD_LETTERED,
            updated_at="2026-01-01T00:00:02+00:00",
        ),
    ]

    blocked = blocked_job_states(states, limit=1)

    assert [state.job_id for state in blocked] == ["new-dead-letter"]
