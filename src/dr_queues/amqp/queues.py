from __future__ import annotations

from pydantic import BaseModel

from dr_queues.amqp.connection import (
    ChannelSession,
    PikaBlockingChannel,
    PikaDeliveryMode,
)


class StageQueues(BaseModel):
    prefix: str
    delivery_mode: PikaDeliveryMode
    pending_name: str
    completed_name: str

    @classmethod
    def get_queue_name(cls, prefix: str, role: str) -> str:
        return f"{prefix}.{role}"

    def declare_queues(
        self,
        *,
        channel: PikaBlockingChannel | None = None,
    ) -> None:
        build_queue_session, channel = ChannelSession.ensure_channel(
            channel=channel,
            delivery_mode=self.delivery_mode,
        )
        try:
            ChannelSession.declare_durable_queue(
                queue_name=self.pending_name,
                channel=channel,
                delivery_mode=self.delivery_mode,
            )
            if self.completed_name != self.pending_name:
                ChannelSession.declare_durable_queue(
                    queue_name=self.completed_name,
                    channel=channel,
                    delivery_mode=self.delivery_mode,
                )
        finally:
            if build_queue_session is not None:
                build_queue_session.close()


def build_stage_queues(
    *,
    prefix: str,
    pending: str | None = None,
    completed: str | None = None,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> StageQueues:
    pending_name = pending or StageQueues.get_queue_name(prefix, "pending")
    completed_name = completed or StageQueues.get_queue_name(
        prefix, "completed"
    )
    stage_queues = StageQueues(
        prefix=prefix,
        delivery_mode=delivery_mode,
        pending_name=pending_name,
        completed_name=completed_name,
    )
    stage_queues.declare_queues()
    return stage_queues
