from __future__ import annotations

import importlib
import os
import signal
import time
from threading import Event, Thread

import typer

from dr_queues.pipeline.workers import WorkerPool
from dr_queues.runtime.lifecycle import current_host
from dr_queues.runtime.models import WorkerProcessRecord, WorkerStatus
from dr_queues.runtime.store import MongoRunStore
from dr_queues.workflow.pipeline import Pipeline
from dr_queues.workflow.registry import HandlerRegistry

app = typer.Typer(add_completion=False)
HEARTBEAT_INTERVAL_SECONDS = 2.0


def _load_registry(module_path: str) -> HandlerRegistry:
    module = importlib.import_module(module_path)
    registry = getattr(module, "registry", None)
    if registry is None:
        msg = f"Module {module_path!r} has no registry attribute."
        raise typer.BadParameter(msg)
    return registry


@app.command()
def main(
    run_id: str = typer.Option(..., "--run-id"),
    stage: str = typer.Option(..., "--stage"),
    workers: int = typer.Option(..., "--workers"),
    handlers_module: str = typer.Option(
        "dr_queues.demo_handlers",
        "--handlers-module",
    ),
) -> None:
    run_store = MongoRunStore()
    run_manifest = run_store.get_manifest(run_id)
    stage_entry = next(
        (item for item in run_manifest.stages if item.name == stage),
        None,
    )
    if stage_entry is None:
        typer.echo(f"Unknown stage {stage!r} in manifest.", err=True)
        raise typer.Exit(code=1)

    registry = _load_registry(handlers_module)
    pipeline = Pipeline(run_manifest.pipeline_definition, registry)
    handler = pipeline.make_handler(stage_entry.step_index)
    pool = WorkerPool(
        input_queue=stage_entry.input_queue,
        output_queue=stage_entry.output_queue,
        handler=handler,
        event_sink=run_store,
        workers=workers,
        stage_name=stage_entry.name,
    )

    record = run_store.register_worker(
        WorkerProcessRecord(
            run_id=run_id,
            stage=stage,
            pid=os.getpid(),
            host=current_host(),
            workers=workers,
            handlers_module=handlers_module,
        ),
    )
    typer.echo(
        f"worker_id={record.worker_id} stage={stage} "
        f"workers={workers} input={stage_entry.input_queue}",
    )
    heartbeat_stop = Event()
    heartbeat = Thread(
        target=_heartbeat_loop,
        args=(run_store, record.worker_id, pool, heartbeat_stop),
        daemon=True,
        name=f"heartbeat-{record.worker_id}",
    )
    heartbeat.start()

    def _shutdown(_signum: int, _frame: object) -> None:
        typer.echo(f"Stopping stage {stage}...")
        pool.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    pool.start()
    try:
        while not pool.is_stopped:
            time.sleep(0.5)
    finally:
        heartbeat_stop.set()
        pool.stop()
        pool.join(timeout=5)
        run_store.mark_worker_stopped(record.worker_id)
        run_store.close()


def _heartbeat_loop(
    run_store: MongoRunStore,
    worker_id: str,
    pool: WorkerPool,
    stop: Event,
) -> None:
    while not stop.wait(HEARTBEAT_INTERVAL_SECONDS):
        record = run_store.heartbeat_worker(worker_id)
        if record is not None and record.status == WorkerStatus.STOP_REQUESTED:
            pool.stop()
            return


def run() -> None:
    app()


if __name__ == "__main__":
    run()
