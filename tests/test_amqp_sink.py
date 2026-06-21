from uuid import uuid4

import pytest

from dr_queues.events.amqp import AmqpEventSink
from dr_queues.events.schema import EventKind, PipelineEvent


@pytest.mark.integration
def test_amqp_sink_roundtrip(rabbitmq_available) -> None:
    if not rabbitmq_available:
        pytest.skip("RabbitMQ not available")
    run_id = f"run-amqp-{uuid4().hex[:8]}"
    sink = AmqpEventSink()
    event = PipelineEvent(
        run_id=run_id,
        job_id="job-1",
        lane="lane-a",
        stage="slow",
        event=EventKind.STAGE_STARTED,
    )
    sink.append(event)
    results = sink.read_by_run_id(run_id)
    assert len(results) == 1
    assert results[0].job_id == "job-1"
