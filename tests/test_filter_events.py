from dr_queues.analysis.filter import filter_run_events
from dr_queues.events.schema import EventKind, PipelineEvent


def test_filter_run_events() -> None:
    events = [
        PipelineEvent(
            run_id="run-a",
            job_id="job-1",
            lane="lane-a",
            stage="slow",
            event=EventKind.STAGE_STARTED,
        ),
        PipelineEvent(
            run_id="run-b",
            job_id="job-2",
            lane="lane-a",
            stage="slow",
            event=EventKind.STAGE_STARTED,
        ),
        PipelineEvent(
            run_id="run-a",
            job_id="job-1",
            lane="lane-a",
            stage="slow",
            event=EventKind.STAGE_OUTPUT,
        ),
    ]
    filtered = filter_run_events(events, "run-a")
    assert len(filtered) == 2
    assert all(event.run_id == "run-a" for event in filtered)
