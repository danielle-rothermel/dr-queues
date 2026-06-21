from __future__ import annotations

import importlib
import os
import signal
import time
from pathlib import Path

import typer

from dr_queues.events.mongo import MongoEventSink
from dr_queues.manifest import (
    load_run_manifest,
    manifest_path,
    read_pid,
    remove_pid,
    stage_pid_path,
    write_pid,
)
from dr_queues.pipeline.workers import WorkerPool
from dr_queues.workflow.pipeline import Pipeline
from dr_queues.workflow.registry import HandlerRegistry

app = typer.Typer(add_completion=False)


def _stop_pid(pid: int, *, timeout: float = 30.0) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.2)


def _load_registry(module_path: str) -> HandlerRegistry:
    module = importlib.import_module(module_path)
    registry = getattr(module, "registry", None)
    if registry is None:
        msg = f"Module {module_path!r} has no registry attribute."
        raise typer.BadParameter(msg)
    return registry


@app.command()
def main(
    run_id: str | None = typer.Option(None, "--run-id"),
    stage: str = typer.Option(..., "--stage"),
    workers: int = typer.Option(..., "--workers"),
    manifest: Path | None = typer.Option(None, "--manifest"),
    handlers_module: str = typer.Option(
        "dr_queues.demo_handlers",
        "--handlers-module",
    ),
    replace: bool = typer.Option(False, "--replace"),
) -> None:
    resolved_manifest_path = manifest or (
        manifest_path(run_id) if run_id else None
    )
    if resolved_manifest_path is None or not resolved_manifest_path.exists():
        typer.echo(
            "Manifest not found. Pass --manifest or --run-id.",
            err=True,
        )
        raise typer.Exit(code=1)

    run_manifest = load_run_manifest(resolved_manifest_path)
    stage_entry = next(
        (item for item in run_manifest.stages if item.name == stage),
        None,
    )
    if stage_entry is None:
        typer.echo(f"Unknown stage {stage!r} in manifest.", err=True)
        raise typer.Exit(code=1)

    pid_path = stage_pid_path(run_manifest.run_id, stage)
    if replace:
        existing_pid = read_pid(pid_path)
        if existing_pid is not None:
            typer.echo(f"Stopping existing worker pid={existing_pid}...")
            _stop_pid(existing_pid)
            remove_pid(pid_path)

    registry = _load_registry(handlers_module)
    pipeline = Pipeline(run_manifest.pipeline_definition, registry)
    handler = pipeline.make_handler(stage_entry.step_index)
    event_sink = MongoEventSink()
    pool = WorkerPool(
        input_queue=stage_entry.input_queue,
        output_queue=stage_entry.output_queue,
        handler=handler,
        event_sink=event_sink,
        workers=workers,
        stage_name=stage_entry.name,
    )

    write_pid(pid_path, os.getpid())
    typer.echo(
        f"stage={stage} workers={workers} input={stage_entry.input_queue}",
    )

    def _shutdown(_signum: int, _frame: object) -> None:
        typer.echo(f"Stopping stage {stage}...")
        pool.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    pool.start()
    try:
        while not pool._stop.is_set():
            time.sleep(0.5)
    finally:
        pool.stop()
        pool.join(timeout=5)
        event_sink.close()
        remove_pid(pid_path)


if __name__ == "__main__":
    app()
