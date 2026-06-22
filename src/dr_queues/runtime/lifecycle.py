from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys

from dr_queues.cli import stage_worker_command_prefix
from dr_queues.runtime.models import WorkerProcessRecord, WorkerStatus
from dr_queues.runtime.store import MongoRunStore

DEFAULT_STOP_SIGNAL = signal.SIGTERM


def current_host() -> str:
    return socket.gethostname()


def start_stage_workers(
    *,
    run_id: str,
    stage: str,
    workers: int,
    handlers_module: str,
) -> subprocess.Popen[bytes]:
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
    return subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def replace_stage_workers(
    *,
    run_id: str,
    stage: str,
    workers: int,
    handlers_module: str,
    run_store: MongoRunStore | None = None,
) -> subprocess.Popen[bytes]:
    stop_workers(run_id=run_id, stage=stage, run_store=run_store)
    return start_stage_workers(
        run_id=run_id,
        stage=stage,
        workers=workers,
        handlers_module=handlers_module,
    )


def list_workers(
    run_id: str,
    *,
    stage: str | None = None,
    run_store: MongoRunStore | None = None,
) -> list[WorkerProcessRecord]:
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
) -> list[WorkerProcessRecord]:
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


def _signal_worker(record: WorkerProcessRecord) -> None:
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
