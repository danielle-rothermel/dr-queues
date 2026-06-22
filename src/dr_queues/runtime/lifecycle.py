from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from threading import Event, Thread

from dr_queues.cli import stage_worker_command_prefix
from dr_queues.runtime.models import WorkerRecord, WorkerRuntime, WorkerStatus
from dr_queues.runtime.store import MongoRunStore
from dr_queues.targeting import TargetSelector

DEFAULT_STOP_SIGNAL = signal.SIGTERM
WORKER_STARTUP_GRACE_SECONDS = 0.5
HEARTBEAT_INTERVAL_SECONDS = 2.0


class WorkerStartError(RuntimeError):
    pass


def current_host() -> str:
    return socket.gethostname()


def register_worker(
    *,
    run_store: MongoRunStore,
    run_id: str,
    stage: str,
    concurrency: int,
    runtime: WorkerRuntime,
    handlers_module: str,
    command: list[str] | None = None,
    include_selectors: list[TargetSelector] | None = None,
    exclude_selectors: list[TargetSelector] | None = None,
) -> WorkerRecord:
    return run_store.register_worker(
        WorkerRecord(
            run_id=run_id,
            stage=stage,
            pid=os.getpid(),
            host=current_host(),
            concurrency=concurrency,
            runtime=runtime,
            handlers_module=handlers_module,
            command=command or [],
            include_selectors=include_selectors or [],
            exclude_selectors=exclude_selectors or [],
        ),
    )


class WorkerHeartbeat:
    def __init__(
        self,
        *,
        run_store: MongoRunStore,
        worker_id: str,
        stop_worker: Callable[[], None],
    ) -> None:
        self.run_store = run_store
        self.worker_id = worker_id
        self.stop_worker = stop_worker
        self._stop = Event()
        self._thread = Thread(
            target=self._run,
            daemon=True,
            name=f"heartbeat-{worker_id}",
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=HEARTBEAT_INTERVAL_SECONDS)

    def _run(self) -> None:
        while not self._stop.wait(HEARTBEAT_INTERVAL_SECONDS):
            record = self.run_store.heartbeat_worker(self.worker_id)
            if (
                record is not None
                and record.status == WorkerStatus.STOP_REQUESTED
            ):
                self.stop_worker()
                return


def start_stage_workers(
    *,
    run_id: str,
    stage: str,
    workers: int,
    handlers_module: str,
    include_selectors: list[TargetSelector] | None = None,
    exclude_selectors: list[TargetSelector] | None = None,
    run_store: MongoRunStore | None = None,
) -> subprocess.Popen[bytes]:
    _validate_stage_worker_partitions(
        run_id=run_id,
        stage=stage,
        include_selectors=include_selectors,
        exclude_selectors=exclude_selectors,
        run_store=run_store,
    )
    cmd = [
        *stage_worker_command_prefix(),
        "--run-id",
        run_id,
        "--stage",
        stage,
        "--workers",
        str(workers),
        "--handlers-module",
        handlers_module,
    ]
    for selector in include_selectors or []:
        cmd.extend(["--include", f"{selector.key}={selector.value}"])
    for selector in exclude_selectors or []:
        cmd.extend(["--exclude", f"{selector.key}={selector.value}"])
    process = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    time.sleep(WORKER_STARTUP_GRACE_SECONDS)
    if process.poll() is not None:
        msg = (
            f"Stage worker for run_id={run_id!r} stage={stage!r} "
            f"exited immediately with code {process.returncode}."
        )
        raise WorkerStartError(msg)
    return process


def replace_stage_workers(
    *,
    run_id: str,
    stage: str,
    workers: int,
    handlers_module: str,
    include_selectors: list[TargetSelector] | None = None,
    exclude_selectors: list[TargetSelector] | None = None,
    run_store: MongoRunStore | None = None,
) -> subprocess.Popen[bytes]:
    stop_workers(run_id=run_id, stage=stage, run_store=run_store)
    return start_stage_workers(
        run_id=run_id,
        stage=stage,
        workers=workers,
        handlers_module=handlers_module,
        include_selectors=include_selectors,
        exclude_selectors=exclude_selectors,
        run_store=run_store,
    )


def list_workers(
    run_id: str,
    *,
    stage: str | None = None,
    run_store: MongoRunStore | None = None,
) -> list[WorkerRecord]:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    try:
        return store.list_workers(run_id, stage=stage)
    finally:
        if close_store:
            store.close()


def stop_workers(
    *,
    run_id: str,
    worker_id: str | None = None,
    stage: str | None = None,
    run_store: MongoRunStore | None = None,
) -> list[WorkerRecord]:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    try:
        records = store.request_worker_stop(
            run_id=run_id,
            worker_id=worker_id,
            stage=stage,
        )
        for record in records:
            if record.host == current_host():
                _signal_worker(record)
        return records
    finally:
        if close_store:
            store.close()


def _signal_worker(record: WorkerRecord) -> None:
    if record.status == WorkerStatus.STOPPED:
        return
    try:
        os.kill(record.pid, DEFAULT_STOP_SIGNAL)
    except ProcessLookupError:
        return
    except PermissionError as error:
        print(
            f"Could not signal worker_id={record.worker_id} pid={record.pid}: {error}",
            file=sys.stderr,
        )


def _validate_stage_worker_partitions(
    *,
    run_id: str,
    stage: str,
    include_selectors: list[TargetSelector] | None,
    exclude_selectors: list[TargetSelector] | None,
    run_store: MongoRunStore | None,
) -> None:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    try:
        partitions = store.list_stage_partitions(
            run_id=run_id,
            stage=stage,
            include=include_selectors,
            exclude=exclude_selectors,
        )
    finally:
        if close_store:
            store.close()
    if partitions:
        return
    msg = (
        f"No matching partitions for run_id={run_id!r} stage={stage!r} "
        "and selectors."
    )
    raise WorkerStartError(msg)
