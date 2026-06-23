from __future__ import annotations

from collections.abc import Iterable

import pika

from dr_queues.amqp.connection import (
    PikaBasicProperties,
    PikaBlockingChannel,
    PikaDeliveryMode,
)


def make_delivery_props(
    delivery_mode: PikaDeliveryMode,
) -> pika.BasicProperties:
    return pika.BasicProperties(delivery_mode=delivery_mode)


def publish_job(
    channel: PikaBlockingChannel,
    queue_name: str,
    body: bytes,
    *,
    exchange: str = "",
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    properties: PikaBasicProperties | None = None,
) -> None:
    props = properties or make_delivery_props(delivery_mode=delivery_mode)
    if not channel.is_open:
        raise RuntimeError("Channel is closed")
    channel.basic_publish(
        exchange=exchange,
        routing_key=queue_name,
        body=body,
        properties=props,
    )


def publish_messages(
    channel: PikaBlockingChannel,
    queue_name: str,
    bodies: Iterable[bytes],
    *,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> None:
    properties = make_delivery_props(delivery_mode=delivery_mode)
    for body in bodies:
        publish_job(
            channel=channel,
            queue_name=queue_name,
            body=body,
            properties=properties,
        )
