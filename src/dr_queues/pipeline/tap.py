from __future__ import annotations

import time
from threading import Event, Thread
from typing import Any

from dr_queues.amqp.connection import (
    PikaBlockingChannel,
    PikaDeliveryMethod,
    delivery_tag,
    open_connection,
)
from dr_queues.events.schema import EventKind
from dr_queues.pipeline.execution import StageExecution
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.runtime.store import MongoRunStore

STAGE_NAME = "terminal"
THREAD_NAME = f"{STAGE_NAME}-tap"
DEFAULT_BATCH_SIZE = 100
DEFAULT_FLUSH_INTERVAL_SECONDS = 0.5


class TerminalTap:
    def __init__(
        self,
        *,
        completed_queue: str,
        completed_queues: list[str] | None = None,
        run_id: str,
        run_store: MongoRunStore,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS,
    ) -> None:
        self.completed_queue = completed_queue
        self.completed_queues = completed_queues or [completed_queue]
        self.run_id = run_id
        self.run_store = run_store
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.stage_execution = StageExecution.from_event_sink(
            event_sink=run_store,
            stage_name=STAGE_NAME,
        )
        self._stop = Event()
        self._thread: Thread | None = None
        self._done = Event()
        self._pending: list[
            tuple[PikaBlockingChannel, int, JobEnvelope, str]
        ] = []
        self._terminal_job_ids = self._read_terminal_job_ids()
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
        channel.basic_qos(prefetch_count=self.batch_size)
        for queue_name in self.completed_queues:
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._on_message,
                auto_ack=False,
            )
        next_flush_at = time.monotonic() + self.flush_interval_seconds
        while not self._stop.is_set() and not self._done.is_set():
            connection.process_data_events(time_limit=0.5)
            if self._pending and time.monotonic() >= next_flush_at:
                self._flush_pending()
                next_flush_at = time.monotonic() + self.flush_interval_seconds
        self._flush_pending()
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
        if job.job_id in self._terminal_job_ids:
            channel.basic_ack(delivery_tag=tag)
            return

        self._pending.append(
            (
                channel,
                tag,
                job,
                getattr(method, "routing_key", self.completed_queue),
            )
        )
        if len(self._pending) >= self.batch_size:
            self._flush_pending()

    def _flush_pending(self) -> None:
        if not self._pending:
            return
        pending = self._pending
        self._pending = []
        self.stage_execution.record_terminal_batch(
            jobs=[
                (job, queue_name)
                for _channel, _tag, job, queue_name in pending
            ]
        )
        for channel, tag, job, _queue_name in pending:
            self._terminal_job_ids.add(job.job_id)
            channel.basic_ack(delivery_tag=tag)
        if self._terminal_count() >= self._expected_count():
            self._done.set()

    def _terminal_count(self) -> int:
        return len(self._terminal_job_ids)

    def _read_terminal_job_ids(self) -> set[str]:
        return {
            event.job_id
            for event in self.run_store.read_by_run_id(self.run_id)
            if event.event == EventKind.TERMINAL
        }

    def _expected_count(self) -> int:
        return self.run_store.expected_job_count(self.run_id)
