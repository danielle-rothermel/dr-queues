from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import dr_queues.pipeline.tap as tap_mod
from dr_queues.events.schema import EventKind, PipelineEvent
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.pipeline.tap import TerminalTap
from dr_queues.pipeline.workers import WorkerPool
from dr_queues.runtime.models import TargetHold


class FakeMethod:
    delivery_tag = 1
    routing_key = "run.r.s1.pending"


class FakeTerminalMethod:
    delivery_tag = 1
    routing_key = "run.r.s3.completed"


class FakeChannel:
    def __init__(self) -> None:
        self.is_open = True
        self.acked: list[int] = []
        self.nacked: list[tuple[int, bool]] = []
        self.published: list[dict[str, Any]] = []

    def basic_ack(self, *, delivery_tag: int) -> None:
        self.acked.append(delivery_tag)

    def basic_nack(self, *, delivery_tag: int, requeue: bool) -> None:
        self.nacked.append((delivery_tag, requeue))

    def basic_publish(self, **kwargs: Any) -> None:
        self.published.append(kwargs)


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[PipelineEvent] = []
        self.failures: list[dict[str, Any]] = []
        self.running: list[dict[str, str]] = []
        self.completed: list[dict[str, str]] = []
        self.terminal: list[dict[str, str]] = []

    def append(self, event: PipelineEvent) -> None:
        self.events.append(event)

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
    ) -> object:
        return {
            "job_id": job.job_id,
            "stage": stage,
            "queue_name": queue_name,
            "hold_id": hold.hold_id,
        }

    def mark_job_running(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object:
        self.running.append(
            {
                "job_id": job.job_id,
                "stage": stage,
                "queue_name": queue_name,
            }
        )
        return self.running[-1]

    def mark_job_completed(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object:
        self.completed.append(
            {
                "job_id": job.job_id,
                "stage": stage,
                "queue_name": queue_name,
            }
        )
        return self.completed[-1]

    def mark_job_terminal(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> object:
        self.terminal.append(
            {
                "job_id": job.job_id,
                "stage": stage,
                "queue_name": queue_name,
            }
        )
        return self.terminal[-1]

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
        self.failures.append(
            {
                "job_id": job.job_id,
                "stage": stage,
                "queue_name": queue_name,
                "error": str(error),
                "worker_id": worker_id,
                "max_attempts": max_attempts,
                "retry_delay_seconds": retry_delay_seconds,
            }
        )
        return self.failures[-1]


class TerminalRecordingStore(RecordingSink):
    def __init__(self, *, expected_jobs: int = 1) -> None:
        super().__init__()
        self.expected_jobs = expected_jobs
        self.read_calls = 0

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        self.read_calls += 1
        return [event for event in self.events if event.run_id == run_id]

    def expected_job_count(self, run_id: str) -> int:
        assert run_id == "run-1"
        return self.expected_jobs


class PlainEventSink:
    def __init__(self) -> None:
        self.events: list[PipelineEvent] = []

    def append(self, event: PipelineEvent) -> None:
        self.events.append(event)

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        return [event for event in self.events if event.run_id == run_id]

    def close(self) -> None:
        return None


def _job(job_id: str = "job-1") -> JobEnvelope:
    return JobEnvelope(
        run_id="run-1",
        job_id=job_id,
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
    )


def test_worker_records_failure_and_acks_message() -> None:
    def failing_handler(_job: JobEnvelope) -> JobEnvelope:
        raise RuntimeError("rate limited")

    sink = RecordingSink()
    pool = WorkerPool(
        input_queue="run.r.s1.pending",
        output_queue="run.r.s1.completed",
        handler=failing_handler,
        event_sink=sink,
        stage_name="score",
        worker_id="worker-1",
        max_attempts=2,
        retry_delay_seconds=30,
    )
    channel = FakeChannel()

    pool._on_message(channel, FakeMethod(), None, _job().to_json())

    assert channel.acked == [1]
    assert channel.nacked == []
    assert sink.failures == [
        {
            "job_id": "job-1",
            "stage": "score",
            "queue_name": "run.r.s1.pending",
            "error": "rate limited",
            "worker_id": "worker-1",
            "max_attempts": 2,
            "retry_delay_seconds": 30,
        }
    ]


def test_worker_nacks_when_failure_is_not_recorded() -> None:
    def failing_handler(_job: JobEnvelope) -> JobEnvelope:
        raise RuntimeError("rate limited")

    sink = PlainEventSink()
    pool = WorkerPool(
        input_queue="run.r.s1.pending",
        output_queue="run.r.s1.completed",
        handler=failing_handler,
        event_sink=sink,
        stage_name="score",
    )
    channel = FakeChannel()

    pool._on_message(channel, FakeMethod(), None, _job().to_json())

    assert channel.acked == []
    assert channel.nacked == [(1, True)]
    assert [event.event for event in sink.events] == [EventKind.STAGE_STARTED]


def test_worker_forwards_without_declaring_queue() -> None:
    sink = RecordingSink()
    pool = WorkerPool(
        input_queue="run.r.s1.pending",
        output_queue="run.r.s1.completed",
        handler=lambda job: job,
        event_sink=sink,
        stage_name="score",
    )
    channel = FakeChannel()

    pool._on_message(channel, FakeMethod(), None, _job().to_json())

    assert channel.acked == [1]
    assert channel.nacked == []
    assert [publish["routing_key"] for publish in channel.published] == [
        "run.r.s1.completed"
    ]
    assert sink.completed == [
        {
            "job_id": "job-1",
            "stage": "score",
            "queue_name": "run.r.s1.pending",
        }
    ]


def test_terminal_tap_records_terminal_through_stage_execution() -> None:
    store = TerminalRecordingStore()
    tap = TerminalTap(
        completed_queue="run.r.s3.completed",
        run_id="run-1",
        run_store=store,
        batch_size=1,
    )
    channel = FakeChannel()

    tap._on_message(channel, FakeTerminalMethod(), None, _job().to_json())

    assert channel.acked == [1]
    assert channel.nacked == []
    assert [event.event for event in store.events] == [EventKind.TERMINAL]
    assert store.terminal == [
        {
            "job_id": "job-1",
            "stage": "terminal",
            "queue_name": "run.r.s3.completed",
        }
    ]
    assert tap.wait_for_completion(timeout=0)


def test_terminal_tap_tracks_completion_without_rereading_events() -> None:
    store = TerminalRecordingStore(expected_jobs=3)
    tap = TerminalTap(
        completed_queue="run.r.s3.completed",
        run_id="run-1",
        run_store=store,
        batch_size=3,
    )
    channel = FakeChannel()

    tap._on_message(
        channel, FakeTerminalMethod(), None, _job("job-1").to_json()
    )
    tap._on_message(
        channel, FakeTerminalMethod(), None, _job("job-2").to_json()
    )

    assert store.read_calls == 1
    assert not tap.wait_for_completion(timeout=0)

    tap._on_message(
        channel, FakeTerminalMethod(), None, _job("job-3").to_json()
    )

    assert store.read_calls == 1
    assert tap.wait_for_completion(timeout=0)


def test_terminal_tap_prefetches_batch_size(monkeypatch) -> None:
    class _Channel:
        is_open = True

        def __init__(self) -> None:
            self.prefetch_count: int | None = None

        def basic_qos(self, *, prefetch_count: int) -> None:
            self.prefetch_count = prefetch_count

        def basic_consume(self, **_kwargs) -> None:
            return None

        def close(self) -> None:
            self.is_open = False

    class _Connection:
        is_open = True

        def __init__(self) -> None:
            self.channel_instance = _Channel()

        def channel(self) -> _Channel:
            return self.channel_instance

        def process_data_events(self, *, time_limit: float) -> None:
            del time_limit

        def close(self) -> None:
            self.is_open = False

    connection = _Connection()

    @contextmanager
    def _broker_session():
        yield SimpleNamespace(
            channel=connection.channel_instance,
            connection=connection,
        )

    monkeypatch.setattr(tap_mod, "broker_session", _broker_session)
    store = TerminalRecordingStore(expected_jobs=0)
    tap = TerminalTap(
        completed_queue="run.r.s3.completed",
        run_id="run-1",
        run_store=store,
        batch_size=37,
    )

    tap._run()

    assert connection.channel_instance.prefetch_count == 37
