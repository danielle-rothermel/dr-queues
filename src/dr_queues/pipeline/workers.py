from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from threading import Event, Thread
from typing import Any

from dr_queues.amqp.connection import (
    ChannelSession,
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
OutputQueueResolver = Callable[[JobEnvelope], str | None]
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
        input_queues: list[str] | None = None,
        output_queue: str | None,
        output_queue_for_job: OutputQueueResolver | None = None,
        handler: JobHandler,
        event_sink: EventSink,
        workers: int = 1,
        stage_name: str = DEFAULT_STAGE_NAME,
        delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
        worker_id: str | None = None,
        max_attempts: int = 3,
        retry_delay_seconds: float = 300.0,
    ) -> None:
        self.input_queue = input_queue
        self.input_queues = input_queues or [input_queue]
        self.output_queue = output_queue
        self.output_queue_for_job = output_queue_for_job
        self.handler = handler
        self.event_sink = event_sink
        self.workers = workers
        self.stage_name = stage_name
        self.delivery_mode = delivery_mode
        self.worker_id = worker_id
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds
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

    @property
    def is_stopped(self) -> bool:
        return self._stop.is_set()

    def join(self, timeout: float | None = None) -> None:
        for thread in self._threads:
            thread.join(timeout=timeout)

    def _run_worker(self, _index: int) -> None:
        connection = open_connection()
        channel = connection.channel()

        channel.basic_qos(prefetch_count=1)
        for queue_name in self.input_queues:
            channel.basic_consume(
                queue=queue_name,
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
        job.resolve_partition_key()
        queue_name = getattr(method, "routing_key", self.input_queue)
        hold = self._active_hold_for_job(job)
        if hold is not None:
            self._hold_job(job, queue_name=queue_name, hold=hold)
            channel.basic_ack(delivery_tag=tag)
            return

        self._mark_job_running(job, queue_name=queue_name)
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
        except Exception as error:
            _log(self.stage_name, WorkerLogStates.FAILED, job)
            if self._record_job_failure(
                job,
                queue_name=queue_name,
                error=error,
            ):
                channel.basic_ack(delivery_tag=tag)
            else:
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

        output_queue = self._output_queue(job)
        if output_queue is not None:
            ChannelSession.declare_durable_queue(
                queue_name=output_queue,
                channel=channel,
                delivery_mode=self.delivery_mode,
            )
            publish_job(
                channel=channel,
                queue_name=output_queue,
                body=job.to_json(),
                delivery_mode=self.delivery_mode,
            )

        self._mark_job_completed(job, queue_name=queue_name)
        channel.basic_ack(delivery_tag=tag)

    def _output_queue(self, job: JobEnvelope) -> str | None:
        if self.output_queue_for_job is not None:
            return self.output_queue_for_job(job)
        return self.output_queue

    def _active_hold_for_job(self, job: JobEnvelope) -> Any | None:
        method = getattr(self.event_sink, "active_hold_for_tags", None)
        if method is None:
            return None
        return method(run_id=job.run_id, tags=job.target_tags)

    def _hold_job(
        self,
        job: JobEnvelope,
        *,
        queue_name: str,
        hold: Any,
    ) -> None:
        method = getattr(self.event_sink, "hold_job", None)
        if method is not None:
            method(
                job=job,
                stage=self.stage_name,
                queue_name=queue_name,
                hold=hold,
            )

    def _mark_job_running(
        self,
        job: JobEnvelope,
        *,
        queue_name: str,
    ) -> None:
        method = getattr(self.event_sink, "mark_job_running", None)
        if method is not None:
            method(job=job, stage=self.stage_name, queue_name=queue_name)

    def _mark_job_completed(
        self,
        job: JobEnvelope,
        *,
        queue_name: str,
    ) -> None:
        method = getattr(self.event_sink, "mark_job_completed", None)
        if method is not None:
            method(job=job, stage=self.stage_name, queue_name=queue_name)

    def _record_job_failure(
        self,
        job: JobEnvelope,
        *,
        queue_name: str,
        error: Exception,
    ) -> bool:
        method = getattr(self.event_sink, "record_job_failure", None)
        if method is None:
            return False
        method(
            job=job,
            stage=self.stage_name,
            queue_name=queue_name,
            error=error,
            worker_id=self.worker_id,
            max_attempts=self.max_attempts,
            retry_delay_seconds=self.retry_delay_seconds,
        )
        return True
