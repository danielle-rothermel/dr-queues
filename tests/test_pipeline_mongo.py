import pytest

from dr_queues.analysis.filter import filter_run_events
from dr_queues.events.schema import EventKind
from dr_queues.manifest.manifest import parse_workers_arg
from dr_queues.pipeline.runner import (
    run_in_process,
    seed_manifest_jobs,
    setup_run_queues,
)


@pytest.mark.integration
@pytest.mark.usefixtures("rabbitmq_connection")
def test_pipeline_with_mongo_sink(
    mongodb_available,
    mongo_sink,
    tiny_pipeline,
    unique_run_id,
) -> None:
    if not mongodb_available:
        pytest.skip("MongoDB not available")

    repeats = 1
    workers_by_stage = parse_workers_arg(
        "slow=1,transform=1,finalize=1",
        tiny_pipeline.step_names(),
        default=1,
    )
    expected = tiny_pipeline.expected_job_count(repeats)
    manifest = setup_run_queues(
        pipeline=tiny_pipeline,
        run_id=unique_run_id,
        workers_by_stage=workers_by_stage,
        expected_jobs=expected,
    )
    jobs = tiny_pipeline.make_seed_jobs(run_id=unique_run_id, repeats=repeats)
    seed_manifest_jobs(manifest, jobs)

    run_in_process(
        manifest=manifest,
        pipeline=tiny_pipeline,
        workers_by_stage=workers_by_stage,
        event_sink=mongo_sink,
        completion_timeout=60.0,
    )

    events = filter_run_events(
        mongo_sink.read_by_run_id(unique_run_id),
        unique_run_id,
    )
    assert (
        len([event for event in events if event.event == EventKind.TERMINAL])
        == expected
    )
