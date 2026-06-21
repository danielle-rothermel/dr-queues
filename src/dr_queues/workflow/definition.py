from __future__ import annotations

from pydantic import BaseModel, Field


class PipelineStep(BaseModel):
    name: str
    handler_key: str


class PipelineLane(BaseModel):
    id: str


class PipelineDefinition(BaseModel):
    id: str
    steps: list[PipelineStep]
    lanes: list[PipelineLane] = Field(
        default_factory=lambda: [PipelineLane(id="default")]
    )
