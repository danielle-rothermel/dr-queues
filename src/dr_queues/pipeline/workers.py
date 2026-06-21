from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from threading import Event, Thread
from typing import Any

from dr_queues.amqp.connection import (
    PikaBlockingChannel,
    PikaDeliveryMethod,
    PikaDeliveryMode,
    delivery_tag,
    open_connection,
    publish_job,
)
from dr_queues.events.schema import EventKind, PipelineEvent
from dr_queues.events.sink import EventSink
from dr_queues.pipeline.job import JobEnvelope

JobHandler = Callable[[JobEnvelope], JobEnvelope]
DEFAULT_STAGE_NAME = "stage"


class WorkerLogStates(StrEnum):
    STARTED = "started"
    FAILED = "failed"
    COMPLETED = "completed"


def _log(stage: str, event: str, job: JobEnvelope) -> None:
    timestamp = datetime.now(tz=UTC).isoformat()
    print(
        f"{timestamp} stage={stage} event={event} "
        f"job_id={job.job_id} lane={job.lane}",
        flush=True,
    )


class WorkerPool:
    def __init__(
        self,
        *,
        input_queue: str,
        output_queue: str | None,
        handler: JobHandler,
        event_sink: EventSink,
        workers: int = 1,
        stage_name: str = DEFAULT_STAGE_NAME,
        delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    ) -> None:
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.handler = handler
        self.event_sink = event_sink
        self.workers = workers
        self.stage_name = stage_name
        self.delivery_mode = delivery_mode
        self._stop = Event()
        self._threads: list[Thread] = []

    def start(self) -> None:
        for index in range(self.workers):
            thread = Thread(
                target=self._run_worker,
                args=(index,),
                daemon=True,
                name=f"worker-{self.stage_name}-{index}",
            )
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float | None = None) -> None:
        for thread in self._threads:
            thread.join(timeout=timeout)

    def _run_worker(self, _index: int) -> None:
        connection = open_connection()
        channel = connection.channel()

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.input_queue,
            on_message_callback=self._on_message,
            auto_ack=False,
        )
        while not self._stop.is_set():
            connection.process_data_events(time_limit=0.5)
        if channel.is_open:
            channel.close()
        if connection.is_open:
            connection.close()

    def _on_message(
        self,
        channel: PikaBlockingChannel,
        method: PikaDeliveryMethod,
        _properties: Any,
        body: bytes,
    ) -> None:
        tag = delivery_tag(method)
        if self._stop.is_set():
            channel.basic_nack(delivery_tag=tag, requeue=True)
            return

        job = JobEnvelope.from_json(body)
        _log(self.stage_name, WorkerLogStates.STARTED, job)
        # Append before ack/forward: durable event log precedes propagation.
        self.event_sink.append(
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
            job = self.handler(job)
        except Exception:
            _log(self.stage_name, WorkerLogStates.FAILED, job)
            channel.basic_nack(delivery_tag=tag, requeue=True)
            return

        _log(self.stage_name, WorkerLogStates.COMPLETED, job)

        self.event_sink.append(
            PipelineEvent(
                run_id=job.run_id,
                job_id=job.job_id,
                lane=job.lane,
                stage=self.stage_name,
                event=EventKind.STAGE_OUTPUT,
                payload={
                    "step_index": job.step_index,
                    "step_outputs": job.step_outputs,
                    "step_record": job.step_records.get(self.stage_name),
                },
            ),
        )

        if self.output_queue is not None:
            publish_job(
                channel=channel,
                queue_name=self.output_queue,
                body=job.to_json(),
                delivery_mode=self.delivery_mode,
            )

        channel.basic_ack(delivery_tag=tag)
