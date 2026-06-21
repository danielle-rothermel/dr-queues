from __future__ import annotations

import os
from enum import IntEnum
from functools import lru_cache
from typing import Any

import pika
import pika.adapters.blocking_connection as pika_blocking
from pydantic import BaseModel, ConfigDict

DEFAULT_AMQP_URL = "amqp://guest:guest@localhost:5672/"

type PikaBlockingChannel = pika_blocking.BlockingChannel
type PikaBasicProperties = pika.spec.BasicProperties
type PikaDeliveryTag = int
type PikaDeliveryMethod = pika.spec.Basic.Deliver
type PikaGetOkMethod = pika.spec.Basic.GetOk


class PikaDeliveryMode(IntEnum):
    TRANSIENT = pika.spec.TRANSIENT_DELIVERY_MODE
    PERSISTENT = pika.spec.PERSISTENT_DELIVERY_MODE


class ReceivedMessage(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    method: PikaGetOkMethod | PikaDeliveryMethod | None
    body: bytes = rb""
    properties: PikaBasicProperties | None

    @classmethod
    def from_get_tuple(
        cls,
        method: Any,
        properties: Any,
        body: Any,
    ) -> ReceivedMessage:
        return ReceivedMessage(
            method=method,
            body=body or b"",
            properties=properties,
        )

    @property
    def has_messages(self) -> bool:
        return self.method is not None

    @property
    def payload(self) -> tuple[bytes, PikaBasicProperties | None]:
        return (self.body, self.properties)


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


@lru_cache(maxsize=1)
def _parameters() -> pika.URLParameters:
    return pika.URLParameters(amqp_url())


def open_connection() -> pika_blocking.BlockingConnection:
    return pika.BlockingConnection(_parameters())


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
    properties: pika.spec.BasicProperties | None = None,
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


class ChannelSession:
    def __init__(
        self,
        connection: pika.BlockingConnection,
        channel: PikaBlockingChannel,
        delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    ):
        self.connection = connection
        self.channel = channel
        self._delivery_mode = delivery_mode

    @classmethod
    def open_session(
        cls,
        *,
        delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    ) -> ChannelSession:
        connection = open_connection()
        return cls(
            connection=connection,
            channel=connection.channel(),
            delivery_mode=delivery_mode,
        )

    @classmethod
    def ensure_channel(
        cls,
        *,
        channel: PikaBlockingChannel | None = None,
        delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    ) -> tuple[ChannelSession | None, PikaBlockingChannel]:
        session = None
        if channel is None:
            session = cls.open_session(delivery_mode=delivery_mode)
            channel = session.channel
        return session, channel

    @classmethod
    def declare_durable_queue(
        cls,
        *,
        queue_name: str,
        channel: PikaBlockingChannel | None = None,
        delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    ) -> None:
        session = None
        if channel is None:
            session = cls.open_session(delivery_mode=delivery_mode)
            channel = session.channel

        try:
            channel.queue_declare(queue=queue_name, durable=True)
        finally:
            if session is not None:
                session.close()

    @classmethod
    def ensure_durable_queue(
        cls,
        *,
        queue_name: str,
        channel: PikaBlockingChannel | None = None,
        delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
    ) -> None:
        ChannelSession.declare_durable_queue(
            queue_name=queue_name,
            channel=channel,
            delivery_mode=delivery_mode,
        )

    @property
    def channel_props(self) -> pika.BasicProperties:
        return make_delivery_props(delivery_mode=self._delivery_mode)

    def publish_job(
        self,
        queue_name: str,
        body: bytes,
    ) -> None:
        publish_job(
            channel=self.channel,
            queue_name=queue_name,
            body=body,
            properties=self.channel_props,
        )

    def close(self) -> None:
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()
