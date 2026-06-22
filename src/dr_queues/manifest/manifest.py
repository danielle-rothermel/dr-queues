from __future__ import annotations

from pydantic import BaseModel, Field

from dr_queues.targeting import DEFAULT_PARTITION_KEY, sanitize_partition_key
from dr_queues.workflow.definition import PipelineDefinition

PARTITION_QUEUE_SEGMENT = "partition"


class StagePartitionQueues(BaseModel):
    partition_key: str
    input_queue: str
    output_queue: str


class RunStageManifest(BaseModel):
    name: str
    step_index: int
    handler_key: str
    input_queue: str
    output_queue: str
    default_workers: int
    partition_queues: dict[str, StagePartitionQueues] = Field(
        default_factory=dict,
    )

    def input_queue_for_partition(self, partition_key: str) -> str:
        if partition_key == DEFAULT_PARTITION_KEY:
            return self.input_queue
        partition = self.partition_queues.get(partition_key)
        if partition is not None:
            return partition.input_queue
        return partition_queue_name(self.input_queue, partition_key)

    def output_queue_for_partition(self, partition_key: str) -> str:
        if partition_key == DEFAULT_PARTITION_KEY:
            return self.output_queue
        partition = self.partition_queues.get(partition_key)
        if partition is not None:
            return partition.output_queue
        return partition_queue_name(self.output_queue, partition_key)

    def with_partition(self, partition_key: str) -> RunStageManifest:
        if partition_key == DEFAULT_PARTITION_KEY:
            return self
        if partition_key in self.partition_queues:
            return self
        updated = self.model_copy(deep=True)
        updated.partition_queues[partition_key] = StagePartitionQueues(
            partition_key=partition_key,
            input_queue=partition_queue_name(self.input_queue, partition_key),
            output_queue=partition_queue_name(
                self.output_queue, partition_key
            ),
        )
        return updated


class RunManifest(BaseModel):
    run_id: str
    pipeline_definition: PipelineDefinition
    expected_jobs: int
    queue_prefix: str
    stages: list[RunStageManifest]

    @property
    def pipeline_id(self) -> str:
        return self.pipeline_definition.id

    def stage_index(self, stage_name: str) -> int:
        for index, stage in enumerate(self.stages):
            if stage.name == stage_name:
                return index
        msg = f"Unknown stage {stage_name!r}."
        raise ValueError(msg)

    def stage_input_queue(
        self,
        stage_name: str,
        partition_key: str,
    ) -> str:
        index = self.stage_index(stage_name)
        if index == 0:
            return self.stages[index].input_queue_for_partition(partition_key)
        return self.stages[index - 1].output_queue_for_partition(partition_key)

    def stage_output_queue(
        self,
        stage_name: str,
        partition_key: str,
    ) -> str:
        stage = self.stages[self.stage_index(stage_name)]
        return stage.output_queue_for_partition(partition_key)

    def with_partition(self, partition_key: str) -> RunManifest:
        if partition_key == DEFAULT_PARTITION_KEY:
            return self
        updated = self.model_copy(deep=True)
        updated.stages = [
            stage.with_partition(partition_key) for stage in self.stages
        ]
        return updated


def partition_queue_name(base_queue: str, partition_key: str) -> str:
    return (
        f"{base_queue}.{PARTITION_QUEUE_SEGMENT}."
        f"{sanitize_partition_key(partition_key)}"
    )


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
