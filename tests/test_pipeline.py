import pytest

from dr_queues.events.schema import EventKind, filter_run_events
from dr_queues.manifest.manifest import parse_workers_arg
from dr_queues.pipeline.runner import (
    run_in_process,
    seed_manifest_jobs,
    setup_run_queues,
)


@pytest.mark.integration
@pytest.mark.usefixtures("rabbitmq_connection")
def test_pipeline_in_process(
    memory_sink,
    demo_pipeline,
    unique_run_id,
) -> None:
    repeats = 2
    workers_by_stage = parse_workers_arg(
        "slow=2,transform=2,finalize=1",
        demo_pipeline.step_names(),
        default=1,
    )
    expected = demo_pipeline.expected_job_count(repeats)
    manifest = setup_run_queues(
        pipeline=demo_pipeline,
        run_id=unique_run_id,
        workers_by_stage=workers_by_stage,
        expected_jobs=expected,
    )
    jobs = demo_pipeline.make_seed_jobs(run_id=unique_run_id, repeats=repeats)
    seed_manifest_jobs(manifest, jobs)

    run_in_process(
        manifest=manifest,
        pipeline=demo_pipeline,
        workers_by_stage=workers_by_stage,
        event_sink=memory_sink,
        completion_timeout=60.0,
    )

    events = filter_run_events(
        memory_sink.read_by_run_id(unique_run_id),
        unique_run_id,
    )
    terminals = [
        event for event in events if event.event == EventKind.TERMINAL
    ]
    assert len(terminals) == expected

    terminal = terminals[0]
    assert terminal.payload["step_outputs"]["transform"].startswith(
        "transformed:"
    )


@pytest.mark.integration
@pytest.mark.usefixtures("rabbitmq_connection")
def test_runner_setup_chains_queues(
    demo_pipeline,
    unique_run_id,
) -> None:
    workers_by_stage = parse_workers_arg(
        "",
        demo_pipeline.step_names(),
        default=2,
    )
    manifest = setup_run_queues(
        pipeline=demo_pipeline,
        run_id=unique_run_id,
        workers_by_stage=workers_by_stage,
        expected_jobs=4,
    )
    assert len(manifest.stages) == 3
    assert manifest.stages[0].output_queue == manifest.stages[1].input_queue
    assert manifest.stages[1].output_queue == manifest.stages[2].input_queue
