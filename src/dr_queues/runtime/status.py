from __future__ import annotations

import time
from typing import TYPE_CHECKING

from dr_queues.amqp.connection import ChannelSession
from dr_queues.manifest.manifest import RunManifest
from dr_queues.runtime.models import (
    EventProgress,
    QueueSnapshot,
    RunStatus,
    StageRunStatus,
    WaitTarget,
)
from dr_queues.runtime.store import MongoRunStore

if TYPE_CHECKING:
    from dr_queues.pipeline.tap import TerminalTap

POLL_INTERVAL_SECONDS = 1.0


def queue_snapshot(queue_name: str) -> QueueSnapshot:
    session = ChannelSession.open_session()
    try:
        method = session.channel.queue_declare(
            queue=queue_name,
            passive=True,
        )
        ready_messages = method.method.message_count
        consumers = method.method.consumer_count
        if ready_messages is None or consumers is None:
            msg = f"RabbitMQ did not return counts for queue {queue_name!r}."
            raise RuntimeError(msg)
        return QueueSnapshot(
            name=queue_name,
            ready_messages=ready_messages,
            consumers=consumers,
        )
    finally:
        session.close()


def get_run_status(
    run_id: str,
    *,
    run_store: MongoRunStore | None = None,
) -> RunStatus:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    try:
        manifest = store.get_manifest(run_id)
        return build_run_status(manifest=manifest, run_store=store)
    finally:
        if close_store:
            store.close()


def build_run_status(
    *,
    manifest: RunManifest,
    run_store: MongoRunStore,
) -> RunStatus:
    events = run_store.read_by_run_id(manifest.run_id)
    progress = EventProgress.from_events(events)
    workers = run_store.list_workers(manifest.run_id)
    stages: list[StageRunStatus] = []
    for stage in manifest.stages:
        started = progress.stage_started.get(stage.name, set())
        completed = progress.stage_completed.get(stage.name, set())
        stage_workers = [
            worker for worker in workers if worker.stage == stage.name
        ]
        stages.append(
            StageRunStatus(
                stage=stage.name,
                expected_jobs=manifest.expected_jobs,
                started_jobs=len(started),
                completed_jobs=len(completed),
                in_flight_jobs=max(0, len(started) - len(completed)),
                input_queue=queue_snapshot(stage.input_queue),
                output_queue=queue_snapshot(stage.output_queue),
                workers=stage_workers,
            ),
        )
    return RunStatus(
        run_id=manifest.run_id,
        manifest=manifest,
        expected_jobs=manifest.expected_jobs,
        terminal_jobs=len(progress.terminal_jobs),
        stages=stages,
        workers=workers,
    )


def wait_for_run(
    run_id: str,
    *,
    target: str = WaitTarget.TERMINAL,
    timeout: float | None = None,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    run_store: MongoRunStore | None = None,
) -> RunStatus:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    started = time.monotonic()
    tap: TerminalTap | None = None
    try:
        while True:
            status = get_run_status(run_id, run_store=store)
            if _target_complete(status, target):
                return status
            if target == WaitTarget.TERMINAL and tap is None:
                from dr_queues.pipeline.tap import TerminalTap

                final_stage = status.manifest.stages[-1]
                tap = TerminalTap(
                    completed_queue=final_stage.output_queue,
                    run_id=run_id,
                    expected_count=status.expected_jobs,
                    run_store=store,
                )
                tap.start()
            if timeout is not None and time.monotonic() - started >= timeout:
                return status
            time.sleep(poll_interval)
    finally:
        if tap is not None:
            tap.stop()
            tap.join(timeout=5)
        if close_store:
            store.close()


def _target_complete(status: RunStatus, target: str) -> bool:
    if target == WaitTarget.TERMINAL:
        return status.is_complete
    stage = next(
        (
            stage_status
            for stage_status in status.stages
            if stage_status.stage == target
        ),
        None,
    )
    if stage is None:
        msg = f"Unknown wait target {target!r}."
        raise ValueError(msg)
    return stage.is_complete
