from __future__ import annotations

from threading import Event, Thread
from typing import Any

from dr_queues.amqp.connection import (
    PikaBlockingChannel,
    PikaDeliveryMethod,
    delivery_tag,
    open_connection,
)
from dr_queues.events.schema import EventKind, PipelineEvent
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.runtime.store import MongoRunStore

STAGE_NAME = "terminal"
THREAD_NAME = f"{STAGE_NAME}-tap"


class TerminalTap:
    def __init__(
        self,
        *,
        completed_queue: str,
        completed_queues: list[str] | None = None,
        run_id: str,
        run_store: MongoRunStore,
    ) -> None:
        self.completed_queue = completed_queue
        self.completed_queues = completed_queues or [completed_queue]
        self.run_id = run_id
        self.run_store = run_store
        self._stop = Event()
        self._thread: Thread | None = None
        self._done = Event()
        if self._terminal_count() >= self._expected_count():
            self._done.set()

    def start(self) -> None:
        self._thread = Thread(
            target=self._run,
            daemon=True,
            name=THREAD_NAME,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def wait_for_completion(self, timeout: float | None = None) -> bool:
        if timeout is None:
            self._done.wait()
            return True
        return self._done.wait(timeout=timeout)

    def _run(self) -> None:
        connection = open_connection()
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        for queue_name in self.completed_queues:
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._on_message,
                auto_ack=False,
            )
        while not self._stop.is_set() and not self._done.is_set():
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
        job = JobEnvelope.from_json(body)
        tag = delivery_tag(method)
        if job.run_id != self.run_id:
            channel.basic_ack(delivery_tag=tag)
            return

        self.run_store.append_event(
            PipelineEvent(
                run_id=job.run_id,
                job_id=job.job_id,
                lane=job.lane,
                stage=STAGE_NAME,
                event=EventKind.TERMINAL,
                payload=job.model_dump(),
            ),
        )
        self.run_store.mark_job_terminal(
            job=job,
            stage=STAGE_NAME,
            queue_name=getattr(method, "routing_key", self.completed_queue),
        )
        channel.basic_ack(delivery_tag=tag)
        if self._terminal_count() >= self._expected_count():
            self._done.set()

    def _terminal_count(self) -> int:
        return len(
            {
                event.job_id
                for event in self.run_store.read_by_run_id(self.run_id)
                if event.event == EventKind.TERMINAL
            },
        )

    def _expected_count(self) -> int:
        return self.run_store.expected_job_count(self.run_id)
