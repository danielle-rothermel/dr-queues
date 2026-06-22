from __future__ import annotations

from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from dr_queues.amqp.connection import (
    ChannelSession,
    PikaDeliveryMode,
)
from dr_queues.targeting import (
    DEFAULT_PARTITION_KEY,
    derive_partition_key,
)


class JobEnvelope(BaseModel):
    run_id: str
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    lane: str
    repeat: int
    step_index: int = 0
    pipeline_id: str
    target_tags: dict[str, str] = Field(default_factory=dict)
    partition_key: str = DEFAULT_PARTITION_KEY
    payload: dict[str, Any] = Field(default_factory=dict)
    step_outputs: dict[str, Any] = Field(default_factory=dict)
    step_records: dict[str, Any] = Field(default_factory=dict)

    def resolve_partition_key(self) -> None:
        if self.partition_key == DEFAULT_PARTITION_KEY:
            self.partition_key = derive_partition_key(self.target_tags)

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> JobEnvelope:
        return cls.model_validate_json(payload)


def seed_jobs(
    *,
    queue_name: str,
    jobs: list[JobEnvelope],
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> None:
    seed_session = ChannelSession.open_session(delivery_mode=delivery_mode)
    try:
        for job in jobs:
            seed_session.publish_job(
                queue_name=queue_name,
                body=job.to_json(),
            )
    finally:
        seed_session.close()
