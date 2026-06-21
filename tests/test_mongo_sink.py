import pytest

from dr_queues.events.schema import EventKind, PipelineEvent


@pytest.mark.integration
def test_mongo_sink_roundtrip(mongo_sink) -> None:
    event = PipelineEvent(
        run_id="run-mongo",
        job_id="job-1",
        lane="lane-a",
        stage="slow",
        event=EventKind.STAGE_STARTED,
        payload={"step_index": 0},
    )
    mongo_sink.append(event)
    results = mongo_sink.read_by_run_id("run-mongo")
    assert len(results) == 1
    assert results[0].event == EventKind.STAGE_STARTED
    assert results[0].payload["step_index"] == 0
