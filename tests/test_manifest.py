from dr_queues.manifest.manifest import (
    parse_workers_arg,
    partition_queue_name,
)
from dr_queues.workflow.definition import (
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
)


def test_parse_workers_arg() -> None:
    result = parse_workers_arg(
        "slow=4,transform=2",
        ["slow", "transform", "finalize"],
        default=10,
    )
    assert result == {"slow": 4, "transform": 2, "finalize": 10}


def test_manifest_roundtrip_json() -> None:
    from dr_queues.manifest.manifest import RunManifest, RunStageManifest

    definition = PipelineDefinition(
        id="demo",
        lanes=[PipelineLane(id="lane-a")],
        steps=[PipelineStep(name="slow", handler_key="sleep_ms")],
    )
    manifest = RunManifest(
        run_id="run-abc",
        pipeline_definition=definition,
        expected_jobs=2,
        queue_prefix="run.run-abc",
        stages=[
            RunStageManifest(
                name="slow",
                step_index=0,
                handler_key="sleep_ms",
                input_queue="run.run-abc.s1.pending",
                output_queue="run.run-abc.s1.completed",
                default_workers=4,
            ),
        ],
    )
    loaded = RunManifest.model_validate_json(manifest.model_dump_json())
    assert loaded == manifest
    assert loaded.pipeline_id == "demo"


def test_manifest_resolves_partition_queues_across_stages() -> None:
    from dr_queues.manifest.manifest import RunManifest, RunStageManifest

    definition = PipelineDefinition(
        id="demo",
        lanes=[PipelineLane(id="lane-a")],
        steps=[
            PipelineStep(name="parse", handler_key="parse"),
            PipelineStep(name="score", handler_key="score"),
        ],
    )
    manifest = RunManifest(
        run_id="run-abc",
        pipeline_definition=definition,
        expected_jobs=1,
        queue_prefix="run.run-abc",
        stages=[
            RunStageManifest(
                name="parse",
                step_index=0,
                handler_key="parse",
                input_queue="run.run-abc.s1.pending",
                output_queue="run.run-abc.s1.completed",
                default_workers=1,
            ),
            RunStageManifest(
                name="score",
                step_index=1,
                handler_key="score",
                input_queue="run.run-abc.s1.completed",
                output_queue="run.run-abc.s2.completed",
                default_workers=1,
            ),
        ],
    )

    assert manifest.stage_input_queue("parse", "gemini-flash") == (
        "run.run-abc.s1.pending.partition.gemini-flash"
    )
    assert manifest.stage_input_queue("score", "gemini-flash") == (
        "run.run-abc.s1.completed.partition.gemini-flash"
    )
    assert partition_queue_name("queue.name", "provider/gemini") == (
        "queue.name.partition.provider_gemini"
    )
