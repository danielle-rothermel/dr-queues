from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pika
from pydantic import BaseModel, ConfigDict, SkipValidation

from dr_queues.amqp.connection import (
    PikaBlockingChannel,
    open_connection,
)


class BrokerSession(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    connection: SkipValidation[pika.BlockingConnection]
    channel: SkipValidation[PikaBlockingChannel]

    def close(self) -> None:
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()


@contextmanager
def broker_session() -> Iterator[BrokerSession]:
    connection = open_connection()
    session = BrokerSession(
        connection=connection,
        channel=connection.channel(),
    )
    try:
        yield session
    finally:
        session.close()
