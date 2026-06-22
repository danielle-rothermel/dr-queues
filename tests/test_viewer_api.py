from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient
from pymongo.errors import PyMongoError

from dr_queues.events.schema import EventKind, PipelineEvent
from dr_queues.manifest import RunManifest, RunStageManifest
from dr_queues.pipeline.job import JobEnvelope
from dr_queues.runtime.models import (
    JobAttempt,
    JobAttemptAction,
    JobState,
    JobStateStatus,
    QueueSnapshot,
    RunRecord,
    RunStatus,
    StageRunStatus,
    TargetHold,
    WorkerProcessRecord,
)
from dr_queues.runtime.store import RunNotFoundError
from dr_queues.targeting import TargetSelector
from dr_queues.viewer.app import create_app
from dr_queues.workflow.definition import (
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
)


def _manifest() -> RunManifest:
    return RunManifest(
        run_id="run-1",
        pipeline_definition=PipelineDefinition(
            id="demo",
            lanes=[PipelineLane(id="lane-a")],
            steps=[PipelineStep(name="parse", handler_key="parse")],
        ),
        expected_jobs=1,
        queue_prefix="run.run-1",
        stages=[
            RunStageManifest(
                name="parse",
                step_index=0,
                handler_key="parse",
                input_queue="run.run-1.s1.pending",
                output_queue="run.run-1.s1.completed",
                default_workers=1,
            ),
        ],
    )


def _job() -> JobEnvelope:
    return JobEnvelope(
        run_id="run-1",
        job_id="job-1",
        lane="lane-a",
        repeat=0,
        pipeline_id="demo",
    )


def _job_state(
    status: JobStateStatus = JobStateStatus.DEAD_LETTERED,
) -> JobState:
    job = _job()
    return JobState(
        run_id="run-1",
        job_id=job.job_id,
        stage="parse",
        status=status,
        partition_key="default",
        queue_name="run.run-1.s1.pending",
        job=job.model_dump(),
        attempt_count=1,
        failure_detail="rate limited",
    )


def _run_status() -> RunStatus:
    manifest = _manifest()
    queue = QueueSnapshot(
        name="run.run-1.s1.pending",
        ready_messages=0,
        consumers=1,
    )
    return RunStatus(
        run_id="run-1",
        manifest=manifest,
        expected_jobs=1,
        terminal_jobs=0,
        stages=[
            StageRunStatus(
                stage="parse",
                expected_jobs=1,
                started_jobs=1,
                completed_jobs=0,
                in_flight_jobs=1,
                input_queue=queue,
                output_queue=queue,
                workers=[],
                job_state_counts=dict.fromkeys(JobStateStatus, 0),
            )
        ],
        workers=[],
        job_state_counts=dict.fromkeys(JobStateStatus, 0),
    )


class FakeStore:
    def __init__(self) -> None:
        self.closed = False
        self.job_state_calls: list[dict[str, Any]] = []
        self.attempt_calls: list[dict[str, Any]] = []

    def close(self) -> None:
        self.closed = True

    def get_run_record(self, run_id: str) -> RunRecord:
        if run_id == "missing":
            raise RunNotFoundError("missing")
        return RunRecord(
            run_id=run_id,
            manifest=_manifest(),
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:01+00:00",
            metadata={"source": "test"},
        )

    def list_runs(self, *, limit: int = 50) -> list[RunRecord]:
        return [self.get_run_record("run-1")][:limit]

    def list_job_states(
        self,
        run_id: str,
        *,
        stage: str | None = None,
        status: JobStateStatus | None = None,
        partition_key: str | None = None,
        limit: int | None = None,
    ) -> list[JobState]:
        self.job_state_calls.append(
            {
                "run_id": run_id,
                "stage": stage,
                "status": status,
                "partition_key": partition_key,
                "limit": limit,
            }
        )
        states = [_job_state()]
        if status is not None:
            states = [state for state in states if state.status == status]
        return states[:limit]

    def list_workers(
        self,
        run_id: str,
        *,
        stage: str | None = None,
        include_stopped: bool = True,
    ) -> list[WorkerProcessRecord]:
        _ = include_stopped
        return [
            WorkerProcessRecord(
                run_id=run_id,
                stage=stage or "parse",
                pid=123,
                host="local",
                workers=1,
                handlers_module="handlers",
            )
        ]

    def list_run_partitions(self, run_id: str) -> list[str]:
        assert run_id
        return ["default"]

    def list_target_holds(
        self,
        run_id: str,
        *,
        active_only: bool = True,
    ) -> list[TargetHold]:
        _ = active_only
        return [
            TargetHold(
                run_id=run_id,
                selectors=[TargetSelector(key="provider", value="gemini")],
            )
        ]

    def list_job_attempts(
        self,
        run_id: str,
        *,
        job_id: str | None = None,
        limit: int | None = None,
    ) -> list[JobAttempt]:
        self.attempt_calls.append(
            {"run_id": run_id, "job_id": job_id, "limit": limit}
        )
        return [
            JobAttempt(
                run_id=run_id,
                job_id=job_id or "job-1",
                stage="parse",
                partition_key="default",
                attempt_number=1,
                action=JobAttemptAction.DEAD_LETTERED,
                error_type="RuntimeError",
                error_message="rate limited",
            )
        ][:limit]

    def list_recent_events(
        self,
        run_id: str,
        *,
        limit: int = 100,
    ) -> list[PipelineEvent]:
        return [
            PipelineEvent(
                run_id=run_id,
                job_id="job-1",
                lane="lane-a",
                stage="parse",
                event=EventKind.STAGE_STARTED,
            )
        ][:limit]


@pytest.fixture
def fake_store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def client(fake_store: FakeStore) -> TestClient:
    app = create_app(
        run_store_factory=lambda: fake_store,
        status_getter=lambda _run_id: _run_status(),
    )
    return TestClient(app)


def test_viewer_lists_runs(client: TestClient) -> None:
    response = client.get("/api/runs?limit=1")

    assert response.status_code == 200
    [run] = response.json()
    assert run["run_id"] == "run-1"
    assert run["health"] == "dead_lettered"


def test_viewer_snapshot_composes_runtime_observation(
    client: TestClient,
) -> None:
    response = client.get("/api/runs/run-1/snapshot?limit=5")

    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["run_id"] == "run-1"
    assert body["blocked_jobs"][0]["status"] == "dead_lettered"
    assert body["active_holds"][0]["selectors"] == [
        {"key": "provider", "value": "gemini"}
    ]
    assert body["recent_attempts"][0]["error_message"] == "rate limited"


def test_viewer_jobs_passes_query_filters(
    client: TestClient,
    fake_store: FakeStore,
) -> None:
    response = client.get(
        "/api/runs/run-1/jobs"
        "?stage=parse&status=dead_lettered&partition=default&limit=7"
    )

    assert response.status_code == 200
    assert fake_store.job_state_calls[-1] == {
        "run_id": "run-1",
        "stage": "parse",
        "status": JobStateStatus.DEAD_LETTERED,
        "partition_key": "default",
        "limit": 7,
    }


def test_viewer_attempts_passes_query_filters(
    client: TestClient,
    fake_store: FakeStore,
) -> None:
    response = client.get("/api/runs/run-1/attempts?job_id=job-1&limit=3")

    assert response.status_code == 200
    assert fake_store.attempt_calls[-1] == {
        "run_id": "run-1",
        "job_id": "job-1",
        "limit": 3,
    }


def test_viewer_unknown_run_returns_404(client: TestClient) -> None:
    response = client.get("/api/runs/missing/snapshot")

    assert response.status_code == 404
    assert response.json()["detail"] == "missing"


def test_viewer_service_error_returns_503() -> None:
    app = create_app(
        run_store_factory=lambda: (_ for _ in ()).throw(
            PyMongoError("mongo unavailable")
        )
    )
    client = TestClient(app)

    response = client.get("/api/runs")

    assert response.status_code == 503
    assert response.json()["detail"] == "mongo unavailable"


def test_viewer_serves_static_assets(client: TestClient) -> None:
    index = client.get("/")
    script = client.get("/assets/app.js")

    assert index.status_code == 200
    assert "dr-queues viewer" in index.text
    assert script.status_code == 200
