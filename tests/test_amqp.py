from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import dr_queues.amqp.connection as connection_mod
import dr_queues.amqp.session as session_mod
import dr_queues.pipeline.runner as runner_mod
import dr_queues.runtime.status as status_mod
from dr_queues.amqp.session import broker_session
from dr_queues.amqp.topology import declare_durable_queues
from dr_queues.manifest import RunManifest, RunStageManifest
from dr_queues.pipeline.eligibility import seed_stage_eligible_jobs
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.runtime.models import SeedBatch
from dr_queues.runtime.store import RunNotFoundError
from dr_queues.workflow.definition import (
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
)


class FakeChannel:
    def __init__(self) -> None:
        self.is_open = True
        self.declared: list[tuple[str, bool]] = []

    def close(self) -> None:
        self.is_open = False

    def queue_declare(self, *, queue: str, durable: bool) -> None:
        self.declared.append((queue, durable))


class FakeConnection:
    def __init__(self) -> None:
        self.is_open = True
        self.channel_instance = FakeChannel()

    def channel(self) -> FakeChannel:
        return self.channel_instance

    def close(self) -> None:
        self.is_open = False


def test_open_connection_uses_current_amqp_url(monkeypatch) -> None:
    hosts: list[str] = []

    def _blocking_connection(params):
        hosts.append(params.host)
        return object()

    monkeypatch.setattr(
        connection_mod.pika,
        "BlockingConnection",
        _blocking_connection,
    )

    monkeypatch.setenv("AMQP_URL", "amqp://guest:guest@rabbit-one:5672/")
    connection_mod.open_connection()
    monkeypatch.setenv("AMQP_URL", "amqp://guest:guest@rabbit-two:5672/")
    connection_mod.open_connection()

    assert hosts == ["rabbit-one", "rabbit-two"]


def test_broker_session_closes_channel_and_connection(monkeypatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(session_mod, "open_connection", lambda: connection)

    with broker_session() as broker:
        assert broker.connection is connection
        assert broker.channel is connection.channel_instance
        assert broker.channel.is_open
        assert broker.connection.is_open

    assert not connection.channel_instance.is_open
    assert not connection.is_open


def test_declare_durable_queues_dedupes_names() -> None:
    channel = FakeChannel()

    declare_durable_queues(channel, ["q1", "q2", "q1"])

    assert channel.declared == [("q1", True), ("q2", True)]


def test_setup_run_queues_declares_base_queues_in_one_session(monkeypatch):
    connection = FakeConnection()

    @contextmanager
    def _broker_session():
        yield SimpleNamespace(
            channel=connection.channel_instance,
            connection=connection,
        )

    class _Store:
        def close(self) -> None:
            return None

        def get_manifest(self, _run_id: str) -> RunManifest:
            raise RunNotFoundError("missing")

        def create_run(
            self,
            manifest: RunManifest,
            *,
            overwrite: bool,
            metadata: dict[str, Any] | None,
        ) -> RunManifest:
            del overwrite, metadata
            return manifest

    class _Pipeline:
        definition = PipelineDefinition(
            id="demo",
            lanes=[PipelineLane(id="lane-a")],
            steps=[
                PipelineStep(name="parse", handler_key="parse"),
                PipelineStep(name="score", handler_key="score"),
            ],
        )

    monkeypatch.setattr(runner_mod, "broker_session", _broker_session)

    manifest = runner_mod.setup_run_queues(
        pipeline=_Pipeline(),
        run_id="run-1",
        workers_by_stage={},
        run_store=_Store(),
    )

    assert [
        queue for queue, _durable in connection.channel_instance.declared
    ] == [
        "run.run-1.s1.pending",
        "run.run-1.s1.completed",
        "run.run-1.s2.completed",
    ]
    assert manifest.stages[0].output_queue == manifest.stages[1].input_queue


def test_build_run_status_snapshots_queues_in_one_session(monkeypatch):
    sessions = 0
    declared: list[str] = []

    class _Channel:
        def queue_declare(self, *, queue: str, passive: bool):
            assert passive
            declared.append(queue)
            return SimpleNamespace(
                method=SimpleNamespace(message_count=3, consumer_count=1)
            )

    @contextmanager
    def _broker_session():
        nonlocal sessions
        sessions += 1
        yield SimpleNamespace(channel=_Channel())

    class _Store:
        def read_by_run_id(self, _run_id: str) -> list[object]:
            return []

        def list_workers(self, _run_id: str) -> list[object]:
            return []

        def list_job_states(self, _run_id: str) -> list[object]:
            return []

        def list_run_partitions(self, _run_id: str) -> list[str]:
            return ["default", "gpu"]

        def expected_job_count(self, _run_id: str) -> int:
            return 0

    manifest = RunManifest(
        run_id="run-1",
        pipeline_definition=PipelineDefinition(
            id="demo",
            lanes=[PipelineLane(id="lane-a")],
            steps=[PipelineStep(name="parse", handler_key="parse")],
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
            )
        ],
    )

    monkeypatch.setattr(status_mod, "broker_session", _broker_session)

    status = status_mod.build_run_status(manifest=manifest, run_store=_Store())

    assert sessions == 1
    assert declared == [
        "run.run-1.s1.pending",
        "run.run-1.s1.pending.partition.gpu",
        "run.run-1.s1.completed",
        "run.run-1.s1.completed.partition.gpu",
    ]
    assert status.stages[0].input_queue.ready_messages == 6
    assert status.stages[0].output_queue.consumers == 2


def test_seed_stage_eligible_jobs_declares_each_partition_once() -> None:
    manifest = RunManifest(
        run_id="run-1",
        pipeline_definition=PipelineDefinition(
            id="demo",
            lanes=[PipelineLane(id="lane-a")],
            steps=[PipelineStep(name="parse", handler_key="parse")],
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
            )
        ],
    )
    declared: list[str] = []
    published: list[tuple[str, list[str]]] = []

    class _Store:
        def create_seed_batch(
            self,
            *,
            run_id: str,
            job_ids: list[str],
        ) -> SeedBatch:
            return SeedBatch(
                run_id=run_id, job_ids=job_ids, count=len(job_ids)
            )

        def mark_seed_batch_published(self, _batch_id: str) -> None:
            return None

        def mark_seed_batch_failed(
            self,
            _batch_id: str,
            _detail: str,
        ) -> None:
            return None

        def mark_job_pending(
            self,
            *,
            job: JobEnvelope,
            stage: str,
            queue_name: str,
        ) -> None:
            del job, stage, queue_name

    jobs = [
        JobEnvelope(
            run_id="run-1",
            job_id="job-1",
            lane="lane-a",
            repeat=0,
            pipeline_id="demo",
            target_tags={"quota_pool": "openai"},
        ),
        JobEnvelope(
            run_id="run-1",
            job_id="job-2",
            lane="lane-a",
            repeat=0,
            pipeline_id="demo",
            target_tags={"quota_pool": "openai"},
        ),
    ]

    seed_stage_eligible_jobs(
        manifest,
        jobs,
        run_store=_Store(),
        publisher=lambda queue, queued_jobs: published.append(
            (queue, [job.job_id for job in queued_jobs])
        ),
        queue_declarer=lambda _manifest, partition: declared.append(partition),
    )

    assert declared == ["openai"]
    assert published == [
        ("run.run-1.s1.pending.partition.openai", ["job-1", "job-2"])
    ]
