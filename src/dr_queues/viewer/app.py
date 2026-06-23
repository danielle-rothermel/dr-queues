from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pika.exceptions
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo.errors import PyMongoError

from dr_queues.events.schema import PipelineEvent
from dr_queues.runtime.models import (
    JobAttempt,
    JobState,
    JobStateStatus,
    RunObservation,
    RunStatus,
    RunSummary,
    TargetHold,
)
from dr_queues.runtime.observability import (
    DEFAULT_DETAIL_LIMIT,
    DEFAULT_RUN_LIMIT,
    RunStatusGetter,
    get_run_observation,
    list_run_summaries,
)
from dr_queues.runtime.status import get_run_status
from dr_queues.runtime.store import MongoRunStore, RunNotFoundError

STATIC_DIR = Path(__file__).with_name("static")
VIEWER_RUN_ID_ENV = "DR_QUEUES_VIEWER_RUN_ID"
SERVICE_UNAVAILABLE_ERRORS = (
    OSError,
    PyMongoError,
    pika.exceptions.AMQPError,
)

RunStoreFactory = Callable[[], Any]


def create_app(
    *,
    run_id: str | None = None,
    run_store_factory: RunStoreFactory = MongoRunStore,
    status_getter: RunStatusGetter | None = None,
) -> FastAPI:
    configured_run_id = run_id or os.environ.get(VIEWER_RUN_ID_ENV)
    app = FastAPI(title="dr-queues viewer")
    app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")

    def with_store(handler: Callable[[Any], Any]) -> Any:
        store: Any | None = None
        try:
            store = run_store_factory()
            return handler(store)
        except RunNotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except SERVICE_UNAVAILABLE_ERRORS as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        finally:
            close = getattr(store, "close", None)
            if close is not None:
                close()

    @app.get("/api/config")
    def config() -> dict[str, str | None]:
        return {"run_id": configured_run_id}

    @app.get("/api/runs", response_model=list[RunSummary])
    def runs(
        limit: int = Query(DEFAULT_RUN_LIMIT, ge=1, le=500),
    ) -> list[RunSummary]:
        return with_store(
            lambda store: list_run_summaries(store, limit=limit),
        )

    @app.get("/api/runs/{target_run_id}/snapshot")
    def snapshot(
        target_run_id: str,
        limit: int = Query(DEFAULT_DETAIL_LIMIT, ge=1, le=500),
    ) -> RunObservation:
        def read(store: MongoRunStore) -> RunObservation:
            getter = _status_getter_for_store(
                store=store,
                status_getter=status_getter,
            )
            return get_run_observation(
                target_run_id,
                run_store=store,
                status_getter=getter,
                detail_limit=limit,
            )

        return with_store(read)

    @app.get("/api/runs/{target_run_id}/status")
    def status(target_run_id: str) -> RunStatus:
        def read(store: MongoRunStore) -> RunStatus:
            getter = _status_getter_for_store(
                store=store,
                status_getter=status_getter,
            )
            return getter(target_run_id)

        return with_store(read)

    @app.get("/api/runs/{target_run_id}/jobs")
    def jobs(
        target_run_id: str,
        stage: str | None = None,
        status: JobStateStatus | None = None,
        partition: str | None = None,
        limit: int = Query(DEFAULT_DETAIL_LIMIT, ge=1, le=500),
    ) -> list[JobState]:
        return with_store(
            lambda store: store.list_job_states(
                target_run_id,
                stage=stage,
                status=status,
                partition_key=partition,
                limit=limit,
            ),
        )

    @app.get("/api/runs/{target_run_id}/attempts")
    def attempts(
        target_run_id: str,
        job_id: str | None = None,
        limit: int = Query(DEFAULT_DETAIL_LIMIT, ge=1, le=500),
    ) -> list[JobAttempt]:
        return with_store(
            lambda store: store.list_job_attempts(
                target_run_id,
                job_id=job_id,
                limit=limit,
            ),
        )

    @app.get("/api/runs/{target_run_id}/events")
    def events(
        target_run_id: str,
        limit: int = Query(DEFAULT_DETAIL_LIMIT, ge=1, le=500),
    ) -> list[PipelineEvent]:
        return with_store(
            lambda store: store.list_recent_events(
                target_run_id,
                limit=limit,
            ),
        )

    @app.get("/api/runs/{target_run_id}/holds")
    def holds(
        target_run_id: str,
        include_cleared: bool = False,
    ) -> list[TargetHold]:
        return with_store(
            lambda store: store.list_target_holds(
                target_run_id,
                active_only=not include_cleared,
            ),
        )

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/{_path:path}")
    def app_shell(_path: str) -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


def _status_getter_for_store(
    *,
    store: MongoRunStore,
    status_getter: RunStatusGetter | None,
) -> RunStatusGetter:
    if status_getter is not None:
        return status_getter

    def read_status(target_run_id: str) -> RunStatus:
        return get_run_status(target_run_id, run_store=store)

    return read_status
