from __future__ import annotations

import importlib
from uuid import uuid4

import typer

from dr_queues import (
    AmqpEventSink,
    CompositeEventSink,
    EventKind,
    MongoEventSink,
    Pipeline,
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
    filter_run_events,
    manifest_path,
    parse_workers_arg,
    run_in_process,
    seed_manifest_jobs,
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


def _build_event_sink(sink: str):
    if sink == "mongo":
        return MongoEventSink()
    if sink == "amqp":
        return AmqpEventSink()
    if sink == "both":
        return CompositeEventSink(
            [MongoEventSink(), AmqpEventSink()],
        )
    msg = f"Unknown sink {sink!r}; expected mongo, amqp, or both."
    raise typer.BadParameter(msg)


@app.command()
def main(
    repeats: int = typer.Option(2, "--repeats"),
    lanes: int = typer.Option(2, "--lanes"),
    workers: str = typer.Option(DEFAULT_WORKERS, "--workers"),
    run_id: str | None = typer.Option(None, "--run-id"),
    sink: str = typer.Option("mongo", "--sink"),
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
    event_sink = _build_event_sink(sink)

    manifest = setup_run_queues(
        pipeline=pipeline,
        run_id=resolved_run_id,
        workers_by_stage=workers_by_stage,
        expected_jobs=expected,
    )
    jobs = pipeline.make_seed_jobs(run_id=resolved_run_id, repeats=repeats)
    seed_manifest_jobs(manifest, jobs)

    typer.echo(f"run_id={resolved_run_id}")
    typer.echo(f"manifest={manifest_path(resolved_run_id)}")
    typer.echo(f"expected_jobs={expected} sink={sink}")

    run_in_process(
        manifest=manifest,
        pipeline=pipeline,
        workers_by_stage=workers_by_stage,
        event_sink=event_sink,
        completion_timeout=completion_timeout,
    )

    events = filter_run_events(
        event_sink.read_by_run_id(resolved_run_id),
        resolved_run_id,
    )
    event_sink.close()
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


if __name__ == "__main__":
    app()
