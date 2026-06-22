from __future__ import annotations

import subprocess
import time

from dr_queues.amqp.connection import ChannelSession, PikaDeliveryMode
from dr_queues.amqp.queues import build_stage_queues
from dr_queues.cli import stage_worker_command_prefix
from dr_queues.manifest.manifest import (
    RunManifest,
    RunStageManifest,
)
from dr_queues.pipeline.job import JobEnvelope, seed_jobs
from dr_queues.pipeline.tap import TerminalTap
from dr_queues.pipeline.workers import WorkerPool
from dr_queues.runtime.lifecycle import WorkerHeartbeat, register_worker
from dr_queues.runtime.models import SeedBatch, WorkerRecord, WorkerRuntime
from dr_queues.runtime.status import wait_for_run
from dr_queues.runtime.store import (
    InvalidSeedJobError,
    MongoRunStore,
    RunAlreadyExistsError,
    RunNotFoundError,
)
from dr_queues.targeting import TargetSelector
from dr_queues.workflow.pipeline import Pipeline

RUNNER_QUEUE_PREFIX = "run"


def setup_run_queues(
    *,
    pipeline: Pipeline,
    run_id: str,
    workers_by_stage: dict[str, int],
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    queue_prefix: str | None = None,
    run_store: MongoRunStore | None = None,
    overwrite: bool = False,
) -> RunManifest:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    try:
        if not overwrite:
            try:
                store.get_manifest(run_id)
            except RunNotFoundError:
                pass
            else:
                msg = f"Run {run_id!r} already exists."
                raise RunAlreadyExistsError(msg)
        prefix = queue_prefix or f"{RUNNER_QUEUE_PREFIX}.{run_id}"
        stage_count = len(pipeline.definition.steps)
        stage_queues_list = []
        previous_completed: str | None = None

        for index in range(stage_count):
            stage_prefix = f"{prefix}.s{index + 1}"
            if index == 0:
                queues = build_stage_queues(
                    prefix=stage_prefix,
                    delivery_mode=delivery_mode,
                )
            else:
                queues = build_stage_queues(
                    prefix=stage_prefix,
                    pending=previous_completed,
                    delivery_mode=delivery_mode,
                )
            stage_queues_list.append(queues)
            previous_completed = queues.completed_name

        stages: list[RunStageManifest] = []
        for index, step in enumerate(pipeline.definition.steps):
            queues = stage_queues_list[index]
            stages.append(
                RunStageManifest(
                    name=step.name,
                    step_index=index,
                    handler_key=step.handler_key,
                    input_queue=queues.pending_name,
                    output_queue=queues.completed_name,
                    default_workers=workers_by_stage.get(step.name, 10),
                ),
            )

        manifest = RunManifest(
            run_id=run_id,
            pipeline_definition=pipeline.definition,
            queue_prefix=prefix,
            stages=stages,
        )
        return store.create_run(manifest, overwrite=overwrite)
    finally:
        if close_store:
            store.close()


def attach_run_queues(
    *,
    run_id: str,
    pipeline: Pipeline,
    run_store: MongoRunStore | None = None,
) -> RunManifest:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    try:
        return store.attach_run(
            run_id=run_id,
            pipeline_definition=pipeline.definition,
        )
    finally:
        if close_store:
            store.close()


def _build_pools(
    *,
    manifest: RunManifest,
    pipeline: Pipeline,
    workers_by_stage: dict[str, int],
    run_store: MongoRunStore,
    include_selectors: list[TargetSelector] | None = None,
    exclude_selectors: list[TargetSelector] | None = None,
    handlers_module: str = "in-process",
) -> list[tuple[WorkerPool, WorkerRecord]]:
    pools: list[tuple[WorkerPool, WorkerRecord]] = []
    partitions = run_store.list_run_partitions(manifest.run_id)
    for stage in manifest.stages:
        workers = workers_by_stage.get(stage.name, stage.default_workers)
        handler = pipeline.make_handler(stage.step_index)
        stage_partitions = [
            partition
            for partition in partitions
            if partition
            in run_store.list_stage_partitions(
                run_id=manifest.run_id,
                stage=stage.name,
                include=include_selectors,
                exclude=exclude_selectors,
            )
        ]
        if not stage_partitions:
            continue
        input_queues = [
            manifest.stage_input_queue(stage.name, partition)
            for partition in stage_partitions
        ]
        record = register_worker(
            run_store=run_store,
            run_id=manifest.run_id,
            stage=stage.name,
            concurrency=workers,
            runtime=WorkerRuntime.IN_PROCESS,
            handlers_module=handlers_module,
            include_selectors=include_selectors,
            exclude_selectors=exclude_selectors,
        )
        pool = WorkerPool(
            input_queue=input_queues[0],
            input_queues=input_queues,
            output_queue=stage.output_queue,
            output_queue_for_job=lambda job, stage_name=stage.name: (
                manifest.stage_output_queue(stage_name, job.partition_key)
            ),
            handler=handler,
            event_sink=run_store,
            workers=workers,
            stage_name=stage.name,
            worker_id=record.worker_id,
        )
        pools.append((pool, record))
    return pools


def run_in_process(
    *,
    manifest: RunManifest,
    pipeline: Pipeline,
    workers_by_stage: dict[str, int],
    run_store: MongoRunStore | None = None,
    completion_timeout: float,
    tap: TerminalTap | None = None,
    handlers_module: str = "in-process",
) -> None:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    pools = _build_pools(
        manifest=manifest,
        pipeline=pipeline,
        workers_by_stage=workers_by_stage,
        run_store=store,
        handlers_module=handlers_module,
    )
    heartbeats = [
        WorkerHeartbeat(
            run_store=store,
            worker_id=record.worker_id,
            stop_worker=pool.stop,
        )
        for pool, record in pools
    ]
    owned_tap = tap is None
    if tap is None:
        final_stage = manifest.stages[-1]
        partitions = store.list_run_partitions(manifest.run_id)
        tap = TerminalTap(
            completed_queue=final_stage.output_queue,
            completed_queues=[
                manifest.stage_output_queue(final_stage.name, partition)
                for partition in partitions
            ],
            run_id=manifest.run_id,
            run_store=store,
        )

    try:
        for heartbeat in heartbeats:
            heartbeat.start()
        for pool, _record in reversed(pools):
            pool.start()
        if owned_tap:
            tap.start()

        if not tap.wait_for_completion(timeout=completion_timeout):
            msg = "Timed out waiting for pipeline completion."
            raise TimeoutError(msg)
        status = wait_for_run(
            manifest.run_id,
            timeout=0,
            run_store=store,
        )
        if not status.is_complete:
            msg = "Timed out waiting for persisted pipeline completion."
            raise TimeoutError(msg)
    finally:
        for pool, _record in pools:
            pool.stop()
        if owned_tap:
            tap.stop()

        for heartbeat in heartbeats:
            heartbeat.stop()
        for pool, record in pools:
            pool.join(timeout=5)
            store.mark_worker_stopped(record.worker_id)
        if owned_tap:
            tap.join(timeout=5)

        time.sleep(0.5)
        if close_store:
            store.close()


def spawn_stage_worker_process(
    *,
    run_id: str,
    stage: str,
    workers: int,
    handlers_module: str,
    include_selectors: list[TargetSelector] | None = None,
    exclude_selectors: list[TargetSelector] | None = None,
) -> subprocess.Popen[bytes]:
    cmd = [
        *stage_worker_command_prefix(),
        "--run-id",
        run_id,
        "--stage",
        stage,
        "--workers",
        str(workers),
        "--handlers-module",
        handlers_module,
    ]
    for selector in include_selectors or []:
        cmd.extend(["--include", f"{selector.key}={selector.value}"])
    for selector in exclude_selectors or []:
        cmd.extend(["--exclude", f"{selector.key}={selector.value}"])
    return subprocess.Popen(cmd)


def spawn_all_stage_workers(
    *,
    manifest: RunManifest,
    workers_by_stage: dict[str, int],
    handlers_module: str,
) -> list[subprocess.Popen[bytes]]:
    processes: list[subprocess.Popen[bytes]] = []
    for stage in reversed(manifest.stages):
        workers = workers_by_stage.get(stage.name, stage.default_workers)
        processes.append(
            spawn_stage_worker_process(
                run_id=manifest.run_id,
                stage=stage.name,
                workers=workers,
                handlers_module=handlers_module,
            ),
        )
    return processes


def first_stage_input(manifest: RunManifest) -> str:
    return manifest.stages[0].input_queue


def declare_partition_queues(
    manifest: RunManifest,
    partition_key: str,
    *,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> None:
    session = ChannelSession.open_session(delivery_mode=delivery_mode)
    try:
        for stage in manifest.stages:
            ChannelSession.declare_durable_queue(
                queue_name=stage.input_queue_for_partition(partition_key),
                channel=session.channel,
                delivery_mode=delivery_mode,
            )
            ChannelSession.declare_durable_queue(
                queue_name=stage.output_queue_for_partition(partition_key),
                channel=session.channel,
                delivery_mode=delivery_mode,
            )
    finally:
        session.close()


def seed_run(
    manifest: RunManifest,
    jobs: list[JobEnvelope],
    *,
    run_store: MongoRunStore | None = None,
) -> None:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    batch: SeedBatch | None = None
    try:
        _validate_seed_jobs(manifest, jobs)
        batch = store.create_seed_batch(
            run_id=manifest.run_id,
            job_ids=[job.job_id for job in jobs],
        )
        jobs_by_queue: dict[str, list[JobEnvelope]] = {}
        first_stage = manifest.stages[0]
        for job in jobs:
            job.resolve_partition_key()
            declare_partition_queues(manifest, job.partition_key)
            queue_name = manifest.stage_input_queue(
                first_stage.name,
                job.partition_key,
            )
            store.mark_job_pending(
                job=job,
                stage=first_stage.name,
                queue_name=queue_name,
            )
            jobs_by_queue.setdefault(queue_name, []).append(job)
        for queue_name, queued_jobs in jobs_by_queue.items():
            seed_jobs(
                queue_name=queue_name,
                jobs=queued_jobs,
                delivery_mode=PikaDeliveryMode.PERSISTENT,
            )
    except Exception as error:
        if batch is not None:
            store.mark_seed_batch_failed(batch.batch_id, str(error))
        raise
    else:
        store.mark_seed_batch_published(batch.batch_id)
    finally:
        if close_store:
            store.close()


def _validate_seed_jobs(
    manifest: RunManifest,
    jobs: list[JobEnvelope],
) -> None:
    mismatched_run_ids = sorted(
        {job.run_id for job in jobs if job.run_id != manifest.run_id}
    )
    if mismatched_run_ids:
        msg = (
            f"Seed jobs for run {manifest.run_id!r} include other run IDs: "
            f"{', '.join(mismatched_run_ids)}."
        )
        raise InvalidSeedJobError(msg)
    mismatched_pipeline_ids = sorted(
        {
            job.pipeline_id
            for job in jobs
            if job.pipeline_id != manifest.pipeline_id
        }
    )
    if mismatched_pipeline_ids:
        msg = (
            f"Seed jobs for pipeline {manifest.pipeline_id!r} include other "
            f"pipeline IDs: {', '.join(mismatched_pipeline_ids)}."
        )
        raise InvalidSeedJobError(msg)
