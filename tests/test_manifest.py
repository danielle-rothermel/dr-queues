from dr_queues.manifest.manifest import (
    parse_workers_arg,
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
