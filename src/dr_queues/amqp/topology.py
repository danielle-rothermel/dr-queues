from __future__ import annotations

from collections.abc import Iterable

from dr_queues.amqp.connection import PikaBlockingChannel


def declare_durable_queue(
    channel: PikaBlockingChannel,
    queue_name: str,
) -> None:
    channel.queue_declare(queue=queue_name, durable=True)


def declare_durable_queues(
    channel: PikaBlockingChannel,
    queue_names: Iterable[str],
) -> None:
    for queue_name in dict.fromkeys(queue_names):
        declare_durable_queue(channel, queue_name)
