from __future__ import annotations

import os

import pytest

from dr_queues.events.schema import EventKind, PipelineEvent
from dr_queues.manifest import RunManifest, RunStageManifest
from dr_queues.runtime import (
    DuplicateSeedError,
    MongoRunStore,
    RunAlreadyExistsError,
    WorkerProcessRecord,
    WorkerStatus,
)
from dr_queues.runtime.models import EventProgress
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
