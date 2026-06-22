from __future__ import annotations

from pathlib import Path

import typer

from dr_queues.manifest import parse_workers_arg
from dr_queues.pipeline.eligibility import replay_stage_eligible_jobs
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.pipeline.runner import (
    seed_run,
    setup_run_queues,
)
from dr_queues.runtime import (
    MongoRunStore,
    WorkerStartError,
    get_run_status,
    list_workers,
    replace_stage_workers,
    start_stage_workers,
    stop_workers,
    wait_for_run,
)
from dr_queues.runtime.models import JobStateStatus, WorkerStatus
from dr_queues.targeting import (
    parse_blocked_until,
    parse_selectors,
)
from dr_queues.workflow.definition import PipelineDefinition
from dr_queues.workflow.pipeline import Pipeline
from dr_queues.workflow.registry import HandlerRegistry

app = typer.Typer(add_completion=False)
holds_app = typer.Typer(add_completion=False)
app.add_typer(holds_app, name="holds")
DEFAULT_HANDLERS_MODULE = "dr_queues.demo_handlers"
ACTIVE_WORKER_STATUSES = {
    WorkerStatus.RUNNING,
    WorkerStatus.STOP_REQUESTED,
}


@app.command()
def init(
    run_id: str = typer.Option(..., "--run-id"),
    definition_json: Path = typer.Option(..., "--definition-json"),
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
        overwrite=overwrite,
    )
    typer.echo(manifest.model_dump_json())


@app.command()
def seed(
    run_id: str = typer.Option(..., "--run-id"),
    jobs_jsonl: Path = typer.Option(..., "--jobs-jsonl"),
) -> None:
    store = MongoRunStore()
    try:
        manifest = store.get_manifest(run_id)
        jobs = [
            JobEnvelope.model_validate_json(line)
            for line in jobs_jsonl.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        seed_run(manifest, jobs, run_store=store)
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
    if run_status.job_state_counts:
        counts = " ".join(
            f"{status}={count}"
            for status, count in run_status.job_state_counts.items()
            if count
        )
        if counts:
            typer.echo(f"job_states {counts}")
    for stage in run_status.stages:
        active_workers = [
            worker
            for worker in stage.workers
            if worker.status in ACTIVE_WORKER_STATUSES
        ]
        typer.echo(
            f"stage={stage.stage} completed={stage.completed_jobs}/"
            f"{stage.expected_jobs} input_depth={stage.input_queue.ready_messages} "
            f"output_depth={stage.output_queue.ready_messages} "
            f"worker_records={len(active_workers)}/{len(stage.workers)} "
            f"worker_concurrency="
            f"{sum(worker.concurrency for worker in active_workers)}",
        )


@app.command()
def wait(
    run_id: str = typer.Option(..., "--run-id"),
    target: str = typer.Option("terminal", "--target"),
    timeout: float | None = typer.Option(None, "--timeout"),
) -> None:
    run_status = wait_for_run(run_id, target=target, timeout=timeout)
    if target == "terminal" and not run_status.is_complete:
        typer.echo(
            f"run_id={run_id} target={target} incomplete terminals="
            f"{run_status.terminal_jobs}/{run_status.expected_jobs}",
            err=True,
        )
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
    include: list[str] = typer.Option([], "--include"),
    exclude: list[str] = typer.Option([], "--exclude"),
) -> None:
    try:
        process = start_stage_workers(
            run_id=run_id,
            stage=stage,
            workers=workers,
            handlers_module=handlers_module,
            include_selectors=parse_selectors(include),
            exclude_selectors=parse_selectors(exclude),
        )
    except WorkerStartError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1) from error
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
    include: list[str] = typer.Option([], "--include"),
    exclude: list[str] = typer.Option([], "--exclude"),
) -> None:
    try:
        process = replace_stage_workers(
            run_id=run_id,
            stage=stage,
            workers=workers,
            handlers_module=handlers_module,
            include_selectors=parse_selectors(include),
            exclude_selectors=parse_selectors(exclude),
        )
    except WorkerStartError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1) from error
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
            f"runtime={record.runtime} concurrency={record.concurrency}",
        )


@holds_app.command("set")
def holds_set(
    run_id: str = typer.Option(..., "--run-id"),
    selector: list[str] = typer.Option(..., "--selector"),
    until: str | None = typer.Option(None, "--until"),
    reason: str | None = typer.Option(None, "--reason"),
) -> None:
    selectors = parse_selectors(selector)
    store = MongoRunStore()
    try:
        hold = store.set_target_hold(
            run_id=run_id,
            selectors=selectors,
            blocked_until=parse_blocked_until(until),
            reason=reason,
        )
        typer.echo(f"hold_id={hold.hold_id} run_id={run_id}")
    finally:
        store.close()


@holds_app.command("clear")
def holds_clear(
    run_id: str = typer.Option(..., "--run-id"),
    selector: list[str] = typer.Option(..., "--selector"),
) -> None:
    store = MongoRunStore()
    try:
        count = store.clear_target_holds(
            run_id=run_id,
            selectors=parse_selectors(selector),
        )
        typer.echo(f"cleared={count} run_id={run_id}")
    finally:
        store.close()


@holds_app.command("list")
def holds_list(
    run_id: str = typer.Option(..., "--run-id"),
    include_cleared: bool = typer.Option(False, "--include-cleared"),
) -> None:
    store = MongoRunStore()
    try:
        holds = store.list_target_holds(
            run_id, active_only=not include_cleared
        )
        for hold in holds:
            selectors = ",".join(
                f"{selector.key}={selector.value}"
                for selector in hold.selectors
            )
            typer.echo(
                f"hold_id={hold.hold_id} selectors={selectors} "
                f"blocked_until={hold.blocked_until} cleared_at={hold.cleared_at}",
            )
    finally:
        store.close()


@app.command()
def failures(
    run_id: str = typer.Option(..., "--run-id"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    store = MongoRunStore()
    try:
        states = [
            state
            for status in [
                JobStateStatus.RETRY_WAITING,
                JobStateStatus.HELD,
                JobStateStatus.FAILED,
                JobStateStatus.DEAD_LETTERED,
            ]
            for state in store.list_job_states(run_id, status=status)
        ]
        if json_output:
            typer.echo(
                f"[{','.join(state.model_dump_json() for state in states)}]"
            )
            return
        for state in states:
            typer.echo(
                f"job_id={state.job_id} stage={state.stage} "
                f"status={state.status} partition={state.partition_key} "
                f"attempts={state.attempt_count} detail={state.failure_detail}",
            )
    finally:
        store.close()


@app.command()
def attempts(
    run_id: str = typer.Option(..., "--run-id"),
    job_id: str | None = typer.Option(None, "--job-id"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    store = MongoRunStore()
    try:
        records = store.list_job_attempts(run_id, job_id=job_id)
        if json_output:
            typer.echo(
                f"[{','.join(record.model_dump_json() for record in records)}]"
            )
            return
        for record in records:
            typer.echo(
                f"job_id={record.job_id} stage={record.stage} "
                f"attempt={record.attempt_number} action={record.action} "
                f"error={record.error_type}: {record.error_message}",
            )
    finally:
        store.close()


@app.command()
def replay(
    run_id: str = typer.Option(..., "--run-id"),
    job_id: str | None = typer.Option(None, "--job-id"),
    selector: list[str] = typer.Option([], "--selector"),
    status: JobStateStatus | None = typer.Option(None, "--status"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    include = parse_selectors(selector)
    if job_id is None and not include and status is None:
        raise typer.BadParameter(
            "Provide --job-id, --selector, or --status to select replay jobs."
        )
    store = MongoRunStore()
    try:
        manifest = store.get_manifest(run_id)
        replayed = replay_stage_eligible_jobs(
            manifest,
            run_store=store,
            job_id=job_id,
            status=status,
            include_selectors=include,
            force=force,
        )
        typer.echo(f"replayed={replayed} run_id={run_id}")
    finally:
        store.close()


def run() -> None:
    app()


if __name__ == "__main__":
    run()
