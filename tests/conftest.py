from __future__ import annotations

import socket
from uuid import uuid4

import pytest

from dr_queues.amqp.connection import open_connection
from dr_queues.demo_handlers import registry as demo_registry
from dr_queues.events.memory import MemoryEventSink
from dr_queues.runtime import MongoRunStore
from dr_queues.workflow.definition import (
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
)
from dr_queues.workflow.pipeline import Pipeline


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture
def rabbitmq_available() -> bool:
    return _port_open("localhost", 5672)


@pytest.fixture
def rabbitmq_connection(rabbitmq_available: bool) -> None:
    if not rabbitmq_available:
        pytest.skip("RabbitMQ not available")
    try:
        connection = open_connection()
        connection.close()
    except Exception:
        pytest.skip("RabbitMQ not reachable")


@pytest.fixture
def mongodb_available() -> bool:
    return _port_open("localhost", 27017)


@pytest.fixture
def memory_sink() -> MemoryEventSink:
    return MemoryEventSink()


@pytest.fixture
def mongo_run_store(mongodb_available: bool) -> MongoRunStore:
    if not mongodb_available:
        pytest.skip("MongoDB not available")
    suffix = uuid4().hex[:8]
    store = MongoRunStore(
        events_collection_name=f"test_events_{suffix}",
        manifests_collection_name=f"test_manifests_{suffix}",
        seed_batches_collection_name=f"test_seed_batches_{suffix}",
        workers_collection_name=f"test_workers_{suffix}",
        job_states_collection_name=f"test_job_states_{suffix}",
        job_attempts_collection_name=f"test_job_attempts_{suffix}",
        target_holds_collection_name=f"test_target_holds_{suffix}",
    )
    yield store
    store._events.drop()
    store._manifests.drop()
    store._seed_batches.drop()
    store._workers.drop()
    store._job_states.drop()
    store._job_attempts.drop()
    store._target_holds.drop()
    store.close()


@pytest.fixture
def unique_run_id() -> str:
    return f"test-{uuid4().hex[:8]}"


@pytest.fixture
def demo_pipeline() -> Pipeline:
    definition = PipelineDefinition(
        id="demo_pipeline",
        lanes=[
            PipelineLane(id="lane-a"),
            PipelineLane(id="lane-b"),
        ],
        steps=[
            PipelineStep(name="slow", handler_key="sleep_ms"),
            PipelineStep(name="transform", handler_key="add_prefix"),
            PipelineStep(name="finalize", handler_key="record_artifact"),
        ],
    )
    return Pipeline(definition, demo_registry)


@pytest.fixture
def tiny_pipeline() -> Pipeline:
    definition = PipelineDefinition(
        id="tiny_pipeline",
        lanes=[PipelineLane(id="only")],
        steps=[
            PipelineStep(name="slow", handler_key="sleep_ms"),
            PipelineStep(name="transform", handler_key="add_prefix"),
            PipelineStep(name="finalize", handler_key="record_artifact"),
        ],
    )
    return Pipeline(definition, demo_registry)
