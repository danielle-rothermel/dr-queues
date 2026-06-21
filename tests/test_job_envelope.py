from dr_queues.pipeline.job import JobEnvelope


def test_job_envelope_roundtrip() -> None:
    job = JobEnvelope(
        run_id="run-1",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
        payload={"counter": 1},
        step_outputs={"slow": "slept_100ms"},
        step_records={"finalize": {"counter": 1}},
    )
    restored = JobEnvelope.from_json(job.to_json())
    assert restored == job


def test_job_envelope_generates_job_id() -> None:
    job = JobEnvelope(
        run_id="run-1",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
    )
    assert job.job_id
