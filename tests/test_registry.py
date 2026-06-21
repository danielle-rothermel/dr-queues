import pytest

from dr_queues.pipeline.job import JobEnvelope
from dr_queues.workflow.registry import HandlerRegistry


def test_register_and_get() -> None:
    registry = HandlerRegistry()

    @registry.register("echo")
    def echo(job: JobEnvelope) -> JobEnvelope:
        job.step_outputs["echo"] = "ok"
        return job

    handler = registry.get("echo")
    job = JobEnvelope(
        run_id="run-1",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
    )
    updated = handler(job)
    assert updated.step_outputs["echo"] == "ok"


def test_unknown_handler_raises() -> None:
    registry = HandlerRegistry()
    with pytest.raises(ValueError, match="Unknown handler"):
        registry.get("missing")
