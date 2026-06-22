from __future__ import annotations

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
    pipeline_definition: PipelineDefinition
    expected_jobs: int
    queue_prefix: str
    stages: list[RunStageManifest]

    @property
    def pipeline_id(self) -> str:
        return self.pipeline_definition.id


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
