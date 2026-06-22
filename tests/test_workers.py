from __future__ import annotations

from typing import Any

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
        self.acked: list[int] = []
        self.nacked: list[tuple[int, bool]] = []

    def basic_ack(self, *, delivery_tag: int) -> None:
        self.acked.append(delivery_tag)

    def basic_nack(self, *, delivery_tag: int, requeue: bool) -> None:
        self.nacked.append((delivery_tag, requeue))


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
    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        return [event for event in self.events if event.run_id == run_id]

    def expected_job_count(self, run_id: str) -> int:
        assert run_id == "run-1"
        return 1


class PlainEventSink:
    def __init__(self) -> None:
        self.events: list[PipelineEvent] = []

    def append(self, event: PipelineEvent) -> None:
        self.events.append(event)

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        return [event for event in self.events if event.run_id == run_id]

    def close(self) -> None:
        return None


def _job() -> JobEnvelope:
    return JobEnvelope(
        run_id="run-1",
        job_id="job-1",
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


def test_terminal_tap_records_terminal_through_stage_execution() -> None:
    store = TerminalRecordingStore()
    tap = TerminalTap(
        completed_queue="run.r.s3.completed",
        run_id="run-1",
        run_store=store,
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
