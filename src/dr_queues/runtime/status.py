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
    count_job_states,
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


def aggregate_queue_snapshot(
    queue_names: list[str],
    *,
    label: str,
) -> QueueSnapshot:
    snapshots = [queue_snapshot(queue_name) for queue_name in queue_names]
    return QueueSnapshot(
        name=label,
        ready_messages=sum(snapshot.ready_messages for snapshot in snapshots),
        consumers=sum(snapshot.consumers for snapshot in snapshots),
    )


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
    job_states = run_store.list_job_states(manifest.run_id)
    partitions = run_store.list_run_partitions(manifest.run_id)
    expected_jobs = run_store.expected_job_count(manifest.run_id)
    stages: list[StageRunStatus] = []
    for stage in manifest.stages:
        started = progress.stage_started.get(stage.name, set())
        completed = progress.stage_completed.get(stage.name, set())
        stage_states = [
            state for state in job_states if state.stage == stage.name
        ]
        stage_workers = [
            worker for worker in workers if worker.stage == stage.name
        ]
        stages.append(
            StageRunStatus(
                stage=stage.name,
                expected_jobs=expected_jobs,
                started_jobs=len(started),
                completed_jobs=len(completed),
                in_flight_jobs=max(0, len(started) - len(completed)),
                input_queue=aggregate_queue_snapshot(
                    [
                        manifest.stage_input_queue(stage.name, partition)
                        for partition in partitions
                    ],
                    label=stage.input_queue,
                ),
                output_queue=aggregate_queue_snapshot(
                    [
                        manifest.stage_output_queue(stage.name, partition)
                        for partition in partitions
                    ],
                    label=stage.output_queue,
                ),
                workers=stage_workers,
                job_state_counts=count_job_states(stage_states),
            ),
        )
    return RunStatus(
        run_id=manifest.run_id,
        manifest=manifest,
        expected_jobs=expected_jobs,
        terminal_jobs=len(progress.terminal_jobs),
        stages=stages,
        workers=workers,
        job_state_counts=count_job_states(job_states),
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
                partitions = store.list_run_partitions(run_id)
                tap = TerminalTap(
                    completed_queue=final_stage.output_queue,
                    completed_queues=[
                        status.manifest.stage_output_queue(
                            final_stage.name,
                            partition,
                        )
                        for partition in partitions
                    ],
                    run_id=run_id,
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
