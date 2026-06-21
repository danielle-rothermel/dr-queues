from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from dr_queues.workflow.definition import PipelineDefinition


class RunStageManifest(BaseModel):
    name: str
    step_index: int
    handler_key: str
    input_queue: str
    output_queue: str
    default_workers: int


class RunManifest(BaseModel):
    run_id: str
    pipeline_id: str
    pipeline_definition: PipelineDefinition
    expected_jobs: int
    queue_prefix: str
    stages: list[RunStageManifest]


def run_dir(run_id: str) -> Path:
    return Path(".runs") / run_id


def manifest_path(run_id: str) -> Path:
    return run_dir(run_id) / "manifest.json"


def stage_pid_path(run_id: str, stage_name: str) -> Path:
    return run_dir(run_id) / "pids" / f"{stage_name}.pid"


def write_run_manifest(path: Path, manifest: RunManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )


def load_run_manifest(path: Path) -> RunManifest:
    return RunManifest.model_validate_json(path.read_text(encoding="utf-8"))


def parse_workers_arg(
    value: str,
    step_names: list[str],
    *,
    default: int = 10,
) -> dict[str, int]:
    workers_by_stage = dict.fromkeys(step_names, default)
    if not value.strip():
        return workers_by_stage

    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            msg = f"Invalid workers spec {part!r}; expected name=count"
            raise ValueError(msg)
        name, count_text = part.split("=", 1)
        name = name.strip()
        if name not in workers_by_stage:
            msg = f"Unknown stage {name!r}; expected one of {step_names}"
            raise ValueError(msg)
        workers_by_stage[name] = int(count_text.strip())
    return workers_by_stage


def read_pid(path: Path) -> int | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    return int(text)


def write_pid(path: Path, pid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(pid), encoding="utf-8")


def remove_pid(path: Path) -> None:
    if path.exists():
        path.unlink()


def format_worker_commands(manifest: RunManifest) -> list[str]:
    commands: list[str] = []
    for stage in manifest.stages:
        commands.append(
            "uv run python scripts/run_stage_workers.py "
            f"--run-id {manifest.run_id} "
            f"--stage {stage.name} "
            f"--workers {stage.default_workers} "
            "--handlers-module dr_queues.demo_handlers "
            "--replace",
        )
    return commands
