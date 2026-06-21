from __future__ import annotations

from dr_queues.pipeline.job import JobEnvelope
from dr_queues.workflow.definition import PipelineDefinition, PipelineStep
from dr_queues.workflow.registry import HandlerRegistry, StepHandler


class Pipeline:
    def __init__(
        self,
        definition: PipelineDefinition,
        registry: HandlerRegistry,
    ) -> None:
        self.definition = definition
        self.registry = registry

    def lane_ids(self) -> list[str]:
        return [lane.id for lane in self.definition.lanes]

    def step_names(self) -> list[str]:
        return [step.name for step in self.definition.steps]

    def expected_job_count(self, repeats: int) -> int:
        return len(self.definition.lanes) * repeats

    def step_name(self, step_index: int) -> str:
        return self.definition.steps[step_index].name

    def make_seed_jobs(
        self,
        *,
        run_id: str,
        repeats: int,
    ) -> list[JobEnvelope]:
        jobs: list[JobEnvelope] = []
        for lane in self.definition.lanes:
            for repeat in range(repeats):
                jobs.append(
                    JobEnvelope(
                        run_id=run_id,
                        lane=lane.id,
                        repeat=repeat,
                        step_index=0,
                        pipeline_id=self.definition.id,
                    ),
                )
        return jobs

    def _step(self, step_index: int) -> PipelineStep:
        return self.definition.steps[step_index]

    def make_handler(self, step_index: int) -> StepHandler:
        step = self._step(step_index)
        handler_fn = self.registry.get(step.handler_key)

        def handler(job: JobEnvelope) -> JobEnvelope:
            updated = handler_fn(job)
            updated.step_index = step_index + 1
            return updated

        return handler
