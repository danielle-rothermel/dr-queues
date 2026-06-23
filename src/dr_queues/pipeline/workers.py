from __future__ import annotations

from collections.abc import Callable
from threading import Event, Thread
from typing import Any

from dr_queues.amqp.connection import (
    PikaBlockingChannel,
    PikaDeliveryMethod,
    PikaDeliveryMode,
    delivery_tag,
)
from dr_queues.amqp.publish import publish_job
from dr_queues.amqp.session import broker_session
from dr_queues.events.sink import EventSink
from dr_queues.pipeline.execution import (
    JobHandler,
    StageExecution,
    StageExecutionAction,
)
from dr_queues.pipeline.job import JobEnvelope

OutputQueueResolver = Callable[[JobEnvelope], str | None]
DEFAULT_STAGE_NAME = "stage"


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
        self.stage_execution = StageExecution.from_event_sink(
            event_sink=event_sink,
            stage_name=stage_name,
            worker_id=worker_id,
            max_attempts=max_attempts,
            retry_delay_seconds=retry_delay_seconds,
        )
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

    @property
    def is_stopped(self) -> bool:
        return self._stop.is_set()

    def join(self, timeout: float | None = None) -> None:
        for thread in self._threads:
            thread.join(timeout=timeout)

    def _run_worker(self, _index: int) -> None:
        with broker_session() as broker:
            channel = broker.channel
            connection = broker.connection

            channel.basic_qos(prefetch_count=1)
            for queue_name in self.input_queues:
                channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=self._on_message,
                    auto_ack=False,
                )
            while not self._stop.is_set():
                connection.process_data_events(time_limit=0.5)

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
        queue_name = getattr(method, "routing_key", self.input_queue)
        result = self.stage_execution.run(
            job=job,
            queue_name=queue_name,
            handler=self.handler,
        )

        if result.action == StageExecutionAction.REDELIVER:
            channel.basic_nack(delivery_tag=tag, requeue=True)
            return

        if result.should_forward:
            output_queue = self._output_queue(result.job)
            if output_queue is not None:
                publish_job(
                    channel=channel,
                    queue_name=output_queue,
                    body=result.job.to_json(),
                    delivery_mode=self.delivery_mode,
                )
            self.stage_execution.record_completed(
                job=result.job,
                queue_name=queue_name,
            )

        if result.should_ack:
            channel.basic_ack(delivery_tag=tag)

    def _output_queue(self, job: JobEnvelope) -> str | None:
        if self.output_queue_for_job is not None:
            return self.output_queue_for_job(job)
        return self.output_queue
