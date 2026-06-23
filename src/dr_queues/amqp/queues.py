from __future__ import annotations

from pydantic import BaseModel


class StageQueueNames(BaseModel):
    prefix: str
    pending_name: str
    completed_name: str

    @property
    def queue_names(self) -> list[str]:
        return list(dict.fromkeys([self.pending_name, self.completed_name]))


def build_stage_queue_names(
    *,
    prefix: str,
    pending: str | None = None,
    completed: str | None = None,
) -> StageQueueNames:
    pending_name = pending or f"{prefix}.pending"
    completed_name = completed or f"{prefix}.completed"
    return StageQueueNames(
        prefix=prefix,
        pending_name=pending_name,
        completed_name=completed_name,
    )
