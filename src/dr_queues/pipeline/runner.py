from __future__ import annotations

import subprocess
import time
from pathlib import Path

from dr_queues.amqp.connection import PikaDeliveryMode
from dr_queues.amqp.queues import build_stage_queues
from dr_queues.cli import stage_worker_command_prefix
from dr_queues.events.sink import EventSink
from dr_queues.manifest.manifest import (
    RunManifest,
    RunStageManifest,
    manifest_path,
    write_run_manifest,
)
from dr_queues.pipeline.job import JobEnvelope, seed_jobs
from dr_queues.pipeline.tap import TerminalTap
from dr_queues.pipeline.workers import WorkerPool
from dr_queues.workflow.pipeline import Pipeline

RUNNER_QUEUE_PREFIX = "run"


def setup_run_queues(
    *,
    pipeline: Pipeline,
    run_id: str,
    workers_by_stage: dict[str, int],
    expected_jobs: int,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    queue_prefix: str | None = None,
) -> RunManifest:
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
        pipeline_id=pipeline.definition.id,
        pipeline_definition=pipeline.definition,
        expected_jobs=expected_jobs,
        queue_prefix=prefix,
        stages=stages,
    )
    write_run_manifest(manifest_path(run_id), manifest)
    return manifest


def _build_pools(
    *,
    manifest: RunManifest,
    pipeline: Pipeline,
    workers_by_stage: dict[str, int],
    event_sink: EventSink,
) -> list[WorkerPool]:
    pools: list[WorkerPool] = []
    for stage in manifest.stages:
        workers = workers_by_stage.get(stage.name, stage.default_workers)
        handler = pipeline.make_handler(stage.step_index)
        pools.append(
            WorkerPool(
                input_queue=stage.input_queue,
                output_queue=stage.output_queue,
                handler=handler,
                event_sink=event_sink,
                workers=workers,
                stage_name=stage.name,
            ),
        )
    return pools


def run_in_process(
    *,
    manifest: RunManifest,
    pipeline: Pipeline,
    workers_by_stage: dict[str, int],
    event_sink: EventSink,
    completion_timeout: float,
    tap: TerminalTap | None = None,
) -> None:
    pools = _build_pools(
        manifest=manifest,
        pipeline=pipeline,
        workers_by_stage=workers_by_stage,
        event_sink=event_sink,
    )
    owned_tap = tap is None
    if tap is None:
        final_stage = manifest.stages[-1]
        tap = TerminalTap(
            completed_queue=final_stage.output_queue,
            run_id=manifest.run_id,
            expected_count=manifest.expected_jobs,
            event_sink=event_sink,
        )

    for pool in reversed(pools):
        pool.start()
    if owned_tap:
        tap.start()

    if not tap.wait_for_completion(timeout=completion_timeout):
        msg = "Timed out waiting for pipeline completion."
        raise TimeoutError(msg)

    for pool in pools:
        pool.stop()
    if owned_tap:
        tap.stop()

    for pool in pools:
        pool.join(timeout=5)
    if owned_tap:
        tap.join(timeout=5)

    time.sleep(0.5)


def spawn_stage_worker_process(
    *,
    manifest_path: Path,
    stage: str,
    workers: int,
    handlers_module: str,
    replace: bool = True,
) -> subprocess.Popen[bytes]:
    cmd = [
        *stage_worker_command_prefix(),
        "--manifest",
        str(manifest_path),
        "--stage",
        stage,
        "--workers",
        str(workers),
        "--handlers-module",
        handlers_module,
    ]
    if replace:
        cmd.append("--replace")
    return subprocess.Popen(cmd)


def spawn_all_stage_workers(
    *,
    manifest: RunManifest,
    workers_by_stage: dict[str, int],
    handlers_module: str,
) -> list[subprocess.Popen[bytes]]:
    path = manifest_path(manifest.run_id)
    processes: list[subprocess.Popen[bytes]] = []
    for stage in reversed(manifest.stages):
        workers = workers_by_stage.get(stage.name, stage.default_workers)
        processes.append(
            spawn_stage_worker_process(
                manifest_path=path,
                stage=stage.name,
                workers=workers,
                handlers_module=handlers_module,
                replace=True,
            ),
        )
    return processes


def first_stage_input(manifest: RunManifest) -> str:
    return manifest.stages[0].input_queue


def seed_manifest_jobs(manifest: RunManifest, jobs: list[JobEnvelope]) -> None:
    seed_jobs(
        queue_name=first_stage_input(manifest),
        jobs=jobs,
        delivery_mode=PikaDeliveryMode.PERSISTENT,
    )
