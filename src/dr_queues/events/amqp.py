from __future__ import annotations

from dr_queues.amqp.connection import (
    ChannelSession,
    PikaBasicProperties,
    PikaBlockingChannel,
    PikaDeliveryMethod,
    PikaDeliveryMode,
    ReceivedMessage,
    delivery_tag,
    make_delivery_props,
    publish_job,
)
from dr_queues.events.schema import PipelineEvent
from dr_queues.utils import load_json_body

EVENTS_QUEUE = "dr.events"


def ensure_events_queue(
    *,
    delivery_mode: PikaDeliveryMode,
    channel: PikaBlockingChannel | None = None,
    queue_name: str = EVENTS_QUEUE,
) -> None:
    ChannelSession.ensure_durable_queue(
        queue_name=queue_name,
        channel=channel,
        delivery_mode=delivery_mode,
    )


class AmqpEventSink:
    def __init__(
        self,
        *,
        queue_name: str = EVENTS_QUEUE,
        delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    ) -> None:
        self._queue_name = queue_name
        self._delivery_mode = delivery_mode
        ensure_events_queue(
            queue_name=queue_name,
            delivery_mode=delivery_mode,
        )

    def append(self, event: PipelineEvent) -> None:
        session = ChannelSession.open_session(
            delivery_mode=self._delivery_mode,
        )
        try:
            publish_job(
                channel=session.channel,
                queue_name=self._queue_name,
                body=event.to_json(),
                delivery_mode=self._delivery_mode,
            )
        finally:
            session.close()

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        events = _peek_events(
            queue_name=self._queue_name,
            delivery_mode=self._delivery_mode,
        )
        return [
            PipelineEvent.model_validate(raw)
            for raw in events
            if raw.get("run_id") == run_id
        ]

    def close(self) -> None:
        return None


def _peek_events(
    *,
    queue_name: str,
    delivery_mode: PikaDeliveryMode,
) -> list[dict]:
    drain_session, channel = ChannelSession.ensure_channel(
        delivery_mode=delivery_mode,
    )
    ensure_events_queue(
        channel=channel,
        queue_name=queue_name,
        delivery_mode=delivery_mode,
    )

    events: list[dict] = []
    payloads: list[tuple[bytes, PikaBasicProperties | None]] = []
    while True:
        message_obj = ReceivedMessage.from_get_tuple(
            *channel.basic_get(
                queue=queue_name,
                auto_ack=False,
            )
        )
        if not message_obj.has_messages:
            break
        method = message_obj.method
        if method is None:
            break
        events.append(load_json_body(message_obj.body))
        payloads.append(message_obj.payload)
        channel.basic_ack(delivery_tag=delivery_tag(method))

    default_props = make_delivery_props(delivery_mode=delivery_mode)
    for body, properties in payloads:
        publish_job(
            channel=channel,
            queue_name=queue_name,
            body=body,
            properties=properties or default_props,
        )
    if drain_session is not None:
        drain_session.close()
    return events
