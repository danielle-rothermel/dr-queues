from __future__ import annotations

from typing import Any

from dr_queues.events.schema import PipelineEvent
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.pipeline.workers import WorkerPool


class FakeMethod:
    delivery_tag = 1
    routing_key = "run.r.s1.pending"


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

    def append(self, event: PipelineEvent) -> None:
        self.events.append(event)

    def mark_job_running(
        self,
        *,
        job: JobEnvelope,
        stage: str,
        queue_name: str,
    ) -> None:
        self.running.append(
            {
                "job_id": job.job_id,
                "stage": stage,
                "queue_name": queue_name,
            }
        )

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
    ) -> None:
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
    job = JobEnvelope(
        run_id="run-1",
        job_id="job-1",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
    )

    pool._on_message(channel, FakeMethod(), None, job.to_json())

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
