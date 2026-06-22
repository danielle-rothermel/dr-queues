from __future__ import annotations

import os

import pytest

from dr_queues.events.schema import EventKind, PipelineEvent
from dr_queues.manifest import RunManifest, RunStageManifest
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.runtime import (
    DuplicateSeedError,
    JobAttemptAction,
    JobStateStatus,
    MongoRunStore,
    RunAlreadyExistsError,
    WorkerProcessRecord,
    WorkerStatus,
)
from dr_queues.runtime.models import EventProgress
from dr_queues.targeting import parse_blocked_until, parse_selectors
from dr_queues.workflow.definition import (
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
)


def _definition() -> PipelineDefinition:
    return PipelineDefinition(
        id="demo",
        lanes=[PipelineLane(id="lane-a")],
        steps=[PipelineStep(name="parse", handler_key="parse")],
    )


def _manifest(run_id: str = "run-1") -> RunManifest:
    return RunManifest(
        run_id=run_id,
        pipeline_definition=_definition(),
        expected_jobs=2,
        queue_prefix=f"run.{run_id}",
        stages=[
            RunStageManifest(
                name="parse",
                step_index=0,
                handler_key="parse",
                input_queue=f"run.{run_id}.s1.pending",
                output_queue=f"run.{run_id}.s1.completed",
                default_workers=1,
            ),
        ],
    )


def test_event_progress_deduplicates_retried_events() -> None:
    events = [
        PipelineEvent(
            run_id="run-1",
            job_id="job-1",
            lane="lane-a",
            stage="parse",
            event=EventKind.STAGE_STARTED,
        ),
        PipelineEvent(
            run_id="run-1",
            job_id="job-1",
            lane="lane-a",
            stage="parse",
            event=EventKind.STAGE_STARTED,
        ),
        PipelineEvent(
            run_id="run-1",
            job_id="job-1",
            lane="lane-a",
            stage="parse",
            event=EventKind.STAGE_OUTPUT,
        ),
    ]

    progress = EventProgress.from_events(events)

    assert progress.stage_started["parse"] == {"job-1"}
    assert progress.stage_completed["parse"] == {"job-1"}


@pytest.mark.integration
def test_mongo_run_store_refuses_duplicate_run(
    mongo_run_store: MongoRunStore,
) -> None:
    manifest = _manifest()
    mongo_run_store.create_run(manifest)

    with pytest.raises(RunAlreadyExistsError):
        mongo_run_store.create_run(manifest)


@pytest.mark.integration
def test_mongo_run_store_lists_run_records(
    mongo_run_store: MongoRunStore,
) -> None:
    first = _manifest("run-1")
    second = _manifest("run-2")
    mongo_run_store.create_run(first, metadata={"owner": "alpha"})
    mongo_run_store.create_run(second, metadata={"owner": "bravo"})

    records = mongo_run_store.list_runs(limit=10)

    assert {record.run_id for record in records} == {"run-1", "run-2"}
    assert records[0].manifest.pipeline_id == "demo"
    assert {record.metadata["owner"] for record in records} == {
        "alpha",
        "bravo",
    }


@pytest.mark.integration
def test_mongo_run_store_refuses_duplicate_seed_batch(
    mongo_run_store: MongoRunStore,
) -> None:
    mongo_run_store.create_seed_batch(run_id="run-1", job_ids=["job-1"])

    with pytest.raises(DuplicateSeedError):
        mongo_run_store.create_seed_batch(run_id="run-1", job_ids=["job-2"])


@pytest.mark.integration
def test_mongo_run_store_worker_stop_transition(
    mongo_run_store: MongoRunStore,
) -> None:
    record = mongo_run_store.register_worker(
        WorkerProcessRecord(
            run_id="run-1",
            stage="parse",
            pid=os.getpid(),
            host="localhost",
            workers=2,
            handlers_module="handlers",
        ),
    )

    records = mongo_run_store.request_worker_stop(
        run_id="run-1",
        worker_id=record.worker_id,
    )
    stopped = mongo_run_store.get_worker(record.worker_id)

    assert len(records) == 1
    assert stopped is not None
    assert stopped.status == WorkerStatus.STOP_REQUESTED


@pytest.mark.integration
def test_mongo_run_store_records_retry_and_dead_letter_attempts(
    mongo_run_store: MongoRunStore,
) -> None:
    job = JobEnvelope(
        run_id="run-1",
        job_id="job-1",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
        target_tags={"quota_pool": "gemini-flash"},
    )
    job.resolve_partition_key()

    first = mongo_run_store.record_job_failure(
        job=job,
        stage="parse",
        queue_name="queue",
        error=RuntimeError("rate limited"),
        max_attempts=2,
        retry_delay_seconds=60,
    )
    second = mongo_run_store.record_job_failure(
        job=job,
        stage="parse",
        queue_name="queue",
        error=RuntimeError("still limited"),
        max_attempts=2,
        retry_delay_seconds=60,
    )
    states = mongo_run_store.list_job_states("run-1", stage="parse")

    assert first.action == JobAttemptAction.RETRY_WAITING
    assert second.action == JobAttemptAction.DEAD_LETTERED
    assert len(states) == 1
    assert states[0].status == JobStateStatus.DEAD_LETTERED
    assert states[0].attempt_count == 2
    assert states[0].partition_key == "gemini-flash"


@pytest.mark.integration
def test_mongo_run_store_filters_job_states_and_recent_attempts(
    mongo_run_store: MongoRunStore,
) -> None:
    gemini = JobEnvelope(
        run_id="run-1",
        job_id="job-gemini",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
        target_tags={"quota_pool": "gemini-flash"},
    )
    openai = JobEnvelope(
        run_id="run-1",
        job_id="job-openai",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
        target_tags={"quota_pool": "openai-nano"},
    )
    for job in [gemini, openai]:
        job.resolve_partition_key()
        mongo_run_store.mark_job_pending(
            job=job,
            stage="parse",
            queue_name=f"queue.{job.partition_key}",
        )
    mongo_run_store.record_job_failure(
        job=gemini,
        stage="parse",
        queue_name="queue.gemini-flash",
        error=RuntimeError("first"),
        max_attempts=2,
    )
    second = mongo_run_store.record_job_failure(
        job=gemini,
        stage="parse",
        queue_name="queue.gemini-flash",
        error=RuntimeError("second"),
        max_attempts=2,
    )

    gemini_states = mongo_run_store.list_job_states(
        "run-1",
        partition_key="gemini-flash",
    )
    pending_states = mongo_run_store.list_job_states(
        "run-1",
        status=JobStateStatus.PENDING,
        limit=1,
    )
    recent_attempts = mongo_run_store.list_job_attempts("run-1", limit=1)

    assert [state.job_id for state in gemini_states] == ["job-gemini"]
    assert len(pending_states) == 1
    assert recent_attempts == [second]


@pytest.mark.integration
def test_mongo_run_store_target_holds_and_partition_selection(
    mongo_run_store: MongoRunStore,
) -> None:
    openai = JobEnvelope(
        run_id="run-1",
        job_id="job-openai",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
        target_tags={"provider": "openai", "model": "nano"},
    )
    gemini = JobEnvelope(
        run_id="run-1",
        job_id="job-gemini",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
        target_tags={"provider": "gemini", "model": "flash"},
    )
    for job in [openai, gemini]:
        job.resolve_partition_key()
        mongo_run_store.mark_job_pending(
            job=job,
            stage="parse",
            queue_name=f"queue.{job.partition_key}",
        )

    hold = mongo_run_store.set_target_hold(
        run_id="run-1",
        selectors=parse_selectors(["provider=gemini"]),
        blocked_until=parse_blocked_until("+30m"),
    )
    partitions = mongo_run_store.list_stage_partitions(
        run_id="run-1",
        stage="parse",
        include=parse_selectors(["provider=openai"]),
    )
    missing_partitions = mongo_run_store.list_stage_partitions(
        run_id="run-1",
        stage="parse",
        include=parse_selectors(["provider=anthropic"]),
    )

    assert hold.is_active
    assert mongo_run_store.active_hold_for_tags(
        run_id="run-1",
        tags=gemini.target_tags,
    )
    assert partitions == [openai.partition_key]
    assert missing_partitions == []
