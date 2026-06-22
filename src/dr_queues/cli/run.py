from __future__ import annotations

from pathlib import Path

import typer

from dr_queues.manifest import parse_workers_arg
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.pipeline.runner import seed_run, setup_run_queues
from dr_queues.runtime import (
    MongoRunStore,
    get_run_status,
    list_workers,
    replace_stage_workers,
    start_stage_workers,
    stop_workers,
    wait_for_run,
)
from dr_queues.workflow.definition import PipelineDefinition
from dr_queues.workflow.pipeline import Pipeline
from dr_queues.workflow.registry import HandlerRegistry

app = typer.Typer(add_completion=False)
DEFAULT_HANDLERS_MODULE = "dr_queues.demo_handlers"


@app.command()
def init(
    run_id: str = typer.Option(..., "--run-id"),
    definition_json: Path = typer.Option(..., "--definition-json"),
    expected_jobs: int = typer.Option(..., "--expected-jobs"),
    workers: str = typer.Option("", "--workers"),
    overwrite: bool = typer.Option(False, "--overwrite"),
) -> None:
    definition = PipelineDefinition.model_validate_json(
        definition_json.read_text(encoding="utf-8"),
    )
    pipeline = Pipeline(definition, HandlerRegistry())
    workers_by_stage = parse_workers_arg(
        workers,
        pipeline.step_names(),
        default=1,
    )
    manifest = setup_run_queues(
        pipeline=pipeline,
        run_id=run_id,
        workers_by_stage=workers_by_stage,
        expected_jobs=expected_jobs,
        overwrite=overwrite,
    )
    typer.echo(manifest.model_dump_json())


@app.command()
def seed(
    run_id: str = typer.Option(..., "--run-id"),
    jobs_jsonl: Path = typer.Option(..., "--jobs-jsonl"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    store = MongoRunStore()
    try:
        manifest = store.get_manifest(run_id)
        jobs = [
            JobEnvelope.model_validate_json(line)
            for line in jobs_jsonl.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        seed_run(manifest, jobs, run_store=store, force=force)
        typer.echo(f"seeded={len(jobs)} run_id={run_id}")
    finally:
        store.close()


@app.command()
def status(
    run_id: str = typer.Option(..., "--run-id"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    run_status = get_run_status(run_id)
    if json_output:
        typer.echo(run_status.model_dump_json())
        return
    typer.echo(
        f"run_id={run_id} terminals={run_status.terminal_jobs}/"
        f"{run_status.expected_jobs}",
    )
    for stage in run_status.stages:
        typer.echo(
            f"stage={stage.stage} completed={stage.completed_jobs}/"
            f"{stage.expected_jobs} input_depth={stage.input_queue.ready_messages} "
            f"output_depth={stage.output_queue.ready_messages} "
            f"workers={len(stage.workers)}",
        )


@app.command()
def wait(
    run_id: str = typer.Option(..., "--run-id"),
    target: str = typer.Option("terminal", "--target"),
    timeout: float | None = typer.Option(None, "--timeout"),
) -> None:
    run_status = wait_for_run(run_id, target=target, timeout=timeout)
    if target == "terminal" and not run_status.is_complete:
        raise typer.Exit(code=1)
    typer.echo(
        f"run_id={run_id} target={target} terminals="
        f"{run_status.terminal_jobs}/{run_status.expected_jobs}",
    )


@app.command()
def start(
    run_id: str = typer.Option(..., "--run-id"),
    stage: str = typer.Option(..., "--stage"),
    workers: int = typer.Option(..., "--workers"),
    handlers_module: str = typer.Option(
        DEFAULT_HANDLERS_MODULE,
        "--handlers-module",
    ),
) -> None:
    process = start_stage_workers(
        run_id=run_id,
        stage=stage,
        workers=workers,
        handlers_module=handlers_module,
    )
    typer.echo(f"started pid={process.pid} run_id={run_id} stage={stage}")


@app.command()
def replace(
    run_id: str = typer.Option(..., "--run-id"),
    stage: str = typer.Option(..., "--stage"),
    workers: int = typer.Option(..., "--workers"),
    handlers_module: str = typer.Option(
        DEFAULT_HANDLERS_MODULE,
        "--handlers-module",
    ),
) -> None:
    process = replace_stage_workers(
        run_id=run_id,
        stage=stage,
        workers=workers,
        handlers_module=handlers_module,
    )
    typer.echo(f"replaced pid={process.pid} run_id={run_id} stage={stage}")


@app.command()
def stop(
    run_id: str = typer.Option(..., "--run-id"),
    stage: str | None = typer.Option(None, "--stage"),
    worker_id: str | None = typer.Option(None, "--worker-id"),
) -> None:
    records = stop_workers(run_id=run_id, stage=stage, worker_id=worker_id)
    typer.echo(f"stop_requested={len(records)} run_id={run_id}")


@app.command()
def workers(
    run_id: str = typer.Option(..., "--run-id"),
    stage: str | None = typer.Option(None, "--stage"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    records = list_workers(run_id, stage=stage)
    if json_output:
        typer.echo(
            f"[{','.join(record.model_dump_json() for record in records)}]"
        )
        return
    for record in records:
        typer.echo(
            f"worker_id={record.worker_id} stage={record.stage} "
            f"status={record.status} pid={record.pid} host={record.host} "
            f"workers={record.workers}",
        )


def run() -> None:
    app()


if __name__ == "__main__":
    run()
