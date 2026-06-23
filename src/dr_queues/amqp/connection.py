from __future__ import annotations

import os
from enum import IntEnum

import pika
import pika.adapters.blocking_connection as pika_blocking

DEFAULT_AMQP_URL = "amqp://guest:guest@localhost:5672/"

type PikaBlockingChannel = pika_blocking.BlockingChannel
type PikaBasicProperties = pika.spec.BasicProperties
type PikaDeliveryTag = int
type PikaDeliveryMethod = pika.spec.Basic.Deliver
type PikaGetOkMethod = pika.spec.Basic.GetOk


class PikaDeliveryMode(IntEnum):
    TRANSIENT = pika.spec.TRANSIENT_DELIVERY_MODE
    PERSISTENT = pika.spec.PERSISTENT_DELIVERY_MODE


def delivery_tag(
    method: PikaDeliveryMethod | PikaGetOkMethod,
) -> PikaDeliveryTag:
    tag = method.delivery_tag
    if tag is None:
        msg = "Missing delivery tag on message."
        raise RuntimeError(msg)
    return tag


def amqp_url() -> str:
    return os.environ.get("AMQP_URL", DEFAULT_AMQP_URL)


def open_connection() -> pika_blocking.BlockingConnection:
    return pika.BlockingConnection(pika.URLParameters(amqp_url()))
