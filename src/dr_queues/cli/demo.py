from __future__ import annotations

import importlib
import shutil
import sys
from uuid import uuid4

import typer

from dr_queues import (
    EventKind,
    MongoRunStore,
    Pipeline,
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
    filter_run_events,
    parse_workers_arg,
    run_in_process,
    seed_run,
    setup_run_queues,
)

DEFAULT_WORKERS = "slow=4,transform=4,finalize=2"
HANDLERS_MODULE = "dr_queues.demo_handlers"

app = typer.Typer(add_completion=False)


def _load_registry(module_path: str):
    module = importlib.import_module(module_path)
    if hasattr(module, "registry"):
        return module.registry
    msg = f"Module {module_path!r} has no registry attribute."
    raise typer.BadParameter(msg)


def _build_pipeline(
    *,
    lanes: int,
    handlers_module: str,
) -> Pipeline:
    registry = _load_registry(handlers_module)
    definition = PipelineDefinition(
        id="demo_pipeline",
        lanes=[PipelineLane(id=f"lane-{index}") for index in range(lanes)],
        steps=[
            PipelineStep(name="slow", handler_key="sleep_ms"),
            PipelineStep(name="transform", handler_key="add_prefix"),
            PipelineStep(name="finalize", handler_key="record_artifact"),
        ],
    )
    return Pipeline(definition, registry)


@app.command()
def main(
    repeats: int = typer.Option(2, "--repeats"),
    lanes: int = typer.Option(2, "--lanes"),
    workers: str = typer.Option(DEFAULT_WORKERS, "--workers"),
    run_id: str | None = typer.Option(None, "--run-id"),
    handlers_module: str = typer.Option(
        HANDLERS_MODULE,
        "--handlers-module",
    ),
    completion_timeout: float = typer.Option(120.0, "--completion-timeout"),
) -> None:
    resolved_run_id = run_id or f"demo-{uuid4().hex[:8]}"
    pipeline = _build_pipeline(lanes=lanes, handlers_module=handlers_module)
    workers_by_stage = parse_workers_arg(
        workers,
        pipeline.step_names(),
        default=2,
    )
    expected = pipeline.expected_job_count(repeats)
    run_store = MongoRunStore()

    manifest = setup_run_queues(
        pipeline=pipeline,
        run_id=resolved_run_id,
        workers_by_stage=workers_by_stage,
        expected_jobs=expected,
        run_store=run_store,
    )
    jobs = pipeline.make_seed_jobs(run_id=resolved_run_id, repeats=repeats)
    seed_run(manifest, jobs, run_store=run_store)

    typer.echo(f"run_id={resolved_run_id}")
    typer.echo(f"expected_jobs={expected} store=mongo")

    run_in_process(
        manifest=manifest,
        pipeline=pipeline,
        workers_by_stage=workers_by_stage,
        run_store=run_store,
        completion_timeout=completion_timeout,
    )

    events = filter_run_events(
        run_store.read_by_run_id(resolved_run_id),
        resolved_run_id,
    )
    run_store.close()
    terminals = [
        event for event in events if event.event == EventKind.TERMINAL
    ]
    typer.echo(f"events={len(events)} terminals={len(terminals)}")
    if terminals:
        sample = terminals[0].payload
        typer.echo(
            "sample_terminal="
            f"lane={sample.get('lane')} "
            f"transform={sample.get('step_outputs', {}).get('transform')}",
        )

    if len(terminals) != expected:
        typer.echo("Terminal count mismatch.", err=True)
        raise typer.Exit(code=1)


def run() -> None:
    app()


if __name__ == "__main__":
    run()
