from __future__ import annotations

import pytest

import dr_queues.runtime.lifecycle as lifecycle
from dr_queues.runtime.lifecycle import WorkerStartError


class FakeStartedProcess:
    pid = 12345
    returncode = None

    def poll(self) -> None:
        return None


class FakeExitedProcess:
    pid = 12345
    returncode = 1

    def poll(self) -> int:
        return self.returncode


class FakeRunStore:
    def __init__(self, partitions: list[str]) -> None:
        self.partitions = partitions
        self.closed = False
        self.calls: list[dict[str, object]] = []

    def list_stage_partitions(
        self,
        *,
        run_id: str,
        stage: str,
        include: object,
        exclude: object,
    ) -> list[str]:
        self.calls.append(
            {
                "run_id": run_id,
                "stage": stage,
                "include": include,
                "exclude": exclude,
            }
        )
        return self.partitions

    def close(self) -> None:
        self.closed = True


def test_start_stage_workers_returns_running_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = FakeStartedProcess()
    run_store = FakeRunStore(["default"])

    def fake_popen(*_args: object, **_kwargs: object) -> FakeStartedProcess:
        return process

    monkeypatch.setattr(lifecycle.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(lifecycle.time, "sleep", lambda _seconds: None)

    result = lifecycle.start_stage_workers(
        run_id="run-1",
        stage="score",
        workers=1,
        handlers_module="handlers",
        run_store=run_store,
    )

    assert result is process
    assert run_store.calls == [
        {
            "run_id": "run-1",
            "stage": "score",
            "include": None,
            "exclude": None,
        }
    ]


def test_start_stage_workers_raises_when_child_exits_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = FakeExitedProcess()

    def fake_popen(*_args: object, **_kwargs: object) -> FakeExitedProcess:
        return process

    monkeypatch.setattr(lifecycle.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(lifecycle.time, "sleep", lambda _seconds: None)

    with pytest.raises(WorkerStartError) as exc_info:
        lifecycle.start_stage_workers(
            run_id="run-1",
            stage="score",
            workers=1,
            handlers_module="handlers",
            run_store=FakeRunStore(["default"]),
        )

    assert (
        str(exc_info.value) == "Stage worker for run_id='run-1' stage='score' "
        "exited immediately with code 1."
    )


def test_start_stage_workers_raises_when_selectors_match_no_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_popen(*_args: object, **_kwargs: object) -> None:
        pytest.fail("worker process should not be spawned")

    monkeypatch.setattr(lifecycle.subprocess, "Popen", fake_popen)

    with pytest.raises(WorkerStartError) as exc_info:
        lifecycle.start_stage_workers(
            run_id="run-1",
            stage="score",
            workers=1,
            handlers_module="handlers",
            run_store=FakeRunStore([]),
        )

    assert (
        str(exc_info.value)
        == "No matching partitions for run_id='run-1' stage='score' "
        "and selectors."
    )
