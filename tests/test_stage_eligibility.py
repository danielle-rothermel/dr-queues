from __future__ import annotations

import pytest

from dr_queues.manifest import RunManifest, RunStageManifest
from dr_queues.pipeline.eligibility import (
    replay_stage_eligible_jobs,
    seed_stage_eligible_jobs,
)
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.runtime.models import JobState, JobStateStatus, SeedBatch
from dr_queues.targeting import parse_selectors
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
            steps=[
                PipelineStep(name="parse", handler_key="parse"),
                PipelineStep(name="score", handler_key="score"),
            ],
        ),
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
            RunStageManifest(
                name="score",
                step_index=1,
                handler_key="score",
                input_queue="run.run-1.s1.completed",
                output_queue="run.run-1.s2.completed",
                default_workers=1,
            ),
        ],
    )


def _job(job_id: str, *, run_id: str = "run-1") -> JobEnvelope:
    return JobEnvelope(
        run_id=run_id,
        job_id=job_id,
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
        target_tags={"quota_pool": "gemini-flash"},
    )


class FakeRunStore:
    def __init__(self, replay_states: list[JobState] | None = None) -> None:
        self.actions: list[tuple[str, str]] = []
        self.replay_states = replay_states or []
        self.replay_call: dict[str, object] | None = None
        self.failed_batch_detail: str | None = None

    def create_seed_batch(
        self, *, run_id: str, job_ids: list[str]
    ) -> SeedBatch:
        self.actions.append(("seed_batch", ",".join(job_ids)))
        return SeedBatch(run_id=run_id, job_ids=job_ids, count=len(job_ids))

    def mark_seed_batch_published(self, batch_id: str) -> None:
        self.actions.append(("batch_published", batch_id))

    def mark_seed_batch_failed(self, batch_id: str, detail: str) -> None:
        self.actions.append(("batch_failed", batch_id))
        self.failed_batch_detail = detail

    def mark_job_pending(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> None:
        self.actions.append(("pending", f"{stage}:{queue_name}:{job.job_id}"))

    def replayable_job_states(
        self,
        run_id: str,
        *,
        job_id: str | None,
        status: JobStateStatus | None,
        include: object,
        force: bool,
    ) -> list[JobState]:
        self.replay_call = {
            "run_id": run_id,
            "job_id": job_id,
            "status": status,
            "include": include,
            "force": force,
        }
        return self.replay_states


def test_seed_stage_eligible_jobs_marks_pending_before_publish() -> None:
    manifest = _manifest()
    run_store = FakeRunStore()
    declared: list[str] = []

    def publisher(queue_name: str, jobs: list[JobEnvelope]) -> None:
        run_store.actions.append(("publish", f"{queue_name}:{jobs[0].job_id}"))

    seed_stage_eligible_jobs(
        manifest,
        [_job("job-1")],
        run_store=run_store,
        publisher=publisher,
        queue_declarer=lambda _manifest, partition: declared.append(partition),
    )

    assert declared == ["gemini-flash"]
    assert run_store.actions[0] == ("seed_batch", "job-1")
    assert run_store.actions[1] == (
        "pending",
        "parse:run.run-1.s1.pending.partition.gemini-flash:job-1",
    )
    assert run_store.actions[2] == (
        "publish",
        "run.run-1.s1.pending.partition.gemini-flash:job-1",
    )
    assert run_store.actions[3][0] == "batch_published"


def test_seed_batch_failed_on_publish_error() -> None:
    run_store = FakeRunStore()

    def failing_publisher(_queue_name: str, _jobs: list[JobEnvelope]) -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        seed_stage_eligible_jobs(
            _manifest(),
            [_job("job-1")],
            run_store=run_store,
            publisher=failing_publisher,
            queue_declarer=lambda _manifest, _partition: None,
        )

    assert run_store.failed_batch_detail == "boom"
    assert run_store.actions[-1][0] == "batch_failed"


def test_replay_stage_eligible_jobs_selects_and_publishes_stage_work() -> None:
    manifest = _manifest()
    job = _job("job-1")
    job.resolve_partition_key()
    state = JobState(
        run_id="run-1",
        job_id="job-1",
        stage="score",
        status=JobStateStatus.HELD,
        partition_key=job.partition_key,
        target_tags=job.target_tags,
        queue_name="run.run-1.s1.completed.partition.gemini-flash",
        job=job.model_dump(),
    )
    run_store = FakeRunStore([state])
    published: list[tuple[str, list[str]]] = []
    selectors = parse_selectors(["quota_pool=gemini-flash"])

    replayed = replay_stage_eligible_jobs(
        manifest,
        run_store=run_store,
        status=JobStateStatus.HELD,
        include_selectors=selectors,
        force=True,
        publisher=lambda queue, jobs: published.append(
            (queue, [job.job_id for job in jobs])
        ),
        queue_declarer=lambda _manifest, _partition: None,
    )

    assert replayed == 1
    assert run_store.replay_call == {
        "run_id": "run-1",
        "job_id": None,
        "status": JobStateStatus.HELD,
        "include": selectors,
        "force": True,
    }
    assert run_store.actions == [
        (
            "pending",
            "score:run.run-1.s1.completed.partition.gemini-flash:job-1",
        )
    ]
    assert published == [
        ("run.run-1.s1.completed.partition.gemini-flash", ["job-1"])
    ]
