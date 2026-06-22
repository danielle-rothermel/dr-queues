from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from dr_queues.events.mongo import (
    DEFAULT_COLLECTION,
    _database_name,
    mongodb_url,
)
from dr_queues.events.schema import PipelineEvent
from dr_queues.manifest.manifest import RunManifest
from dr_queues.runtime.models import (
    SeedBatch,
    SeedBatchStatus,
    WorkerProcessRecord,
    WorkerStatus,
    utc_now_iso,
)
from dr_queues.workflow.definition import PipelineDefinition

RUN_MANIFESTS_COLLECTION = "run_manifests"
SEED_BATCHES_COLLECTION = "seed_batches"
WORKER_PROCESSES_COLLECTION = "worker_processes"
STALE_HEARTBEAT_SECONDS = 30.0


class RunAlreadyExistsError(RuntimeError):
    pass


class RunNotFoundError(RuntimeError):
    pass


class DuplicateSeedError(RuntimeError):
    pass


class ManifestMismatchError(RuntimeError):
    pass


class MongoRunStore:
    def __init__(
        self,
        *,
        url: str | None = None,
        client: MongoClient | None = None,
        events_collection_name: str = DEFAULT_COLLECTION,
        manifests_collection_name: str = RUN_MANIFESTS_COLLECTION,
        seed_batches_collection_name: str = SEED_BATCHES_COLLECTION,
        workers_collection_name: str = WORKER_PROCESSES_COLLECTION,
    ) -> None:
        resolved_url = url or mongodb_url()
        self._owns_client = client is None
        self._client = client or MongoClient(resolved_url)
        database = self._client.get_database(_database_name(resolved_url))
        self._events: Collection = database[events_collection_name]
        self._manifests: Collection = database[manifests_collection_name]
        self._seed_batches: Collection = database[seed_batches_collection_name]
        self._workers: Collection = database[workers_collection_name]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._events.create_index(
            [("run_id", ASCENDING), ("timestamp", ASCENDING)],
        )
        self._events.create_index([("event_id", ASCENDING)], unique=True)
        self._manifests.create_index([("run_id", ASCENDING)], unique=True)
        self._seed_batches.create_index(
            [("run_id", ASCENDING), ("batch_id", ASCENDING)],
            unique=True,
        )
        self._workers.create_index([("worker_id", ASCENDING)], unique=True)
        self._workers.create_index(
            [("run_id", ASCENDING), ("stage", ASCENDING)]
        )

    def create_run(
        self,
        manifest: RunManifest,
        *,
        overwrite: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> RunManifest:
        document = {
            "run_id": manifest.run_id,
            "manifest": manifest.model_dump(),
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "metadata": metadata or {},
        }
        if overwrite:
            self._manifests.replace_one(
                {"run_id": manifest.run_id},
                document,
                upsert=True,
            )
            return manifest
        try:
            self._manifests.insert_one(document)
        except DuplicateKeyError as error:
            msg = f"Run {manifest.run_id!r} already exists."
            raise RunAlreadyExistsError(msg) from error
        return manifest

    def get_manifest(self, run_id: str) -> RunManifest:
        document = self._manifests.find_one({"run_id": run_id})
        if document is None:
            msg = f"Run {run_id!r} does not exist."
            raise RunNotFoundError(msg)
        return RunManifest.model_validate(document["manifest"])

    def attach_run(
        self,
        *,
        run_id: str,
        pipeline_definition: PipelineDefinition,
        expected_jobs: int | None = None,
    ) -> RunManifest:
        manifest = self.get_manifest(run_id)
        validate_manifest(
            manifest=manifest,
            pipeline_definition=pipeline_definition,
            expected_jobs=expected_jobs,
        )
        return manifest

    def append(self, event: PipelineEvent) -> None:
        self._events.insert_one(event.model_dump())

    def append_event(self, event: PipelineEvent) -> None:
        self.append(event)

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        cursor = self._events.find({"run_id": run_id}).sort(
            "timestamp",
            ASCENDING,
        )
        return [PipelineEvent.model_validate(document) for document in cursor]

    def create_seed_batch(
        self,
        *,
        run_id: str,
        job_ids: list[str],
        force: bool = False,
    ) -> SeedBatch:
        if not force:
            existing = self._seed_batches.find_one(
                {
                    "run_id": run_id,
                    "status": {
                        "$in": [
                            SeedBatchStatus.CREATED,
                            SeedBatchStatus.PUBLISHED,
                        ]
                    },
                },
            )
            if existing is not None:
                msg = f"Run {run_id!r} already has a seed batch."
                raise DuplicateSeedError(msg)
        batch = SeedBatch(run_id=run_id, job_ids=job_ids, count=len(job_ids))
        self._seed_batches.insert_one(batch.model_dump())
        return batch

    def mark_seed_batch_published(self, batch_id: str) -> None:
        self._seed_batches.update_one(
            {"batch_id": batch_id},
            {
                "$set": {
                    "status": SeedBatchStatus.PUBLISHED,
                    "published_at": utc_now_iso(),
                },
            },
        )

    def mark_seed_batch_failed(self, batch_id: str, detail: str) -> None:
        self._seed_batches.update_one(
            {"batch_id": batch_id},
            {
                "$set": {
                    "status": SeedBatchStatus.FAILED,
                    "failed_at": utc_now_iso(),
                    "failure_detail": detail,
                },
            },
        )

    def register_worker(
        self,
        record: WorkerProcessRecord,
    ) -> WorkerProcessRecord:
        self._workers.insert_one(record.model_dump())
        return record

    def heartbeat_worker(self, worker_id: str) -> WorkerProcessRecord | None:
        self._workers.update_one(
            {"worker_id": worker_id, "status": WorkerStatus.RUNNING},
            {"$set": {"last_heartbeat_at": utc_now_iso()}},
        )
        return self.get_worker(worker_id)

    def get_worker(self, worker_id: str) -> WorkerProcessRecord | None:
        document = self._workers.find_one({"worker_id": worker_id})
        if document is None:
            return None
        return WorkerProcessRecord.model_validate(document)

    def list_workers(
        self,
        run_id: str,
        *,
        stage: str | None = None,
        include_stopped: bool = True,
    ) -> list[WorkerProcessRecord]:
        self.mark_stale_workers(run_id)
        query: dict[str, Any] = {"run_id": run_id}
        if stage is not None:
            query["stage"] = stage
        if not include_stopped:
            query["status"] = {"$ne": WorkerStatus.STOPPED}
        cursor = self._workers.find(query).sort("started_at", ASCENDING)
        return [
            WorkerProcessRecord.model_validate(document) for document in cursor
        ]

    def request_worker_stop(
        self,
        *,
        run_id: str,
        worker_id: str | None = None,
        stage: str | None = None,
    ) -> list[WorkerProcessRecord]:
        query: dict[str, Any] = {
            "run_id": run_id,
            "status": {"$in": [WorkerStatus.RUNNING, WorkerStatus.STALE]},
        }
        if worker_id is not None:
            query["worker_id"] = worker_id
        if stage is not None:
            query["stage"] = stage
        records = [
            WorkerProcessRecord.model_validate(document)
            for document in self._workers.find(query)
        ]
        now = utc_now_iso()
        self._workers.update_many(
            query,
            {
                "$set": {
                    "status": WorkerStatus.STOP_REQUESTED,
                    "stop_requested_at": now,
                },
            },
        )
        return records

    def mark_worker_stopped(self, worker_id: str) -> None:
        self._workers.update_one(
            {"worker_id": worker_id},
            {
                "$set": {
                    "status": WorkerStatus.STOPPED,
                    "stopped_at": utc_now_iso(),
                },
            },
        )

    def mark_stale_workers(
        self,
        run_id: str,
        *,
        stale_after_seconds: float = STALE_HEARTBEAT_SECONDS,
    ) -> None:
        cutoff = datetime.now(tz=UTC) - timedelta(seconds=stale_after_seconds)
        self._workers.update_many(
            {
                "run_id": run_id,
                "status": WorkerStatus.RUNNING,
                "last_heartbeat_at": {"$lt": cutoff.isoformat()},
            },
            {"$set": {"status": WorkerStatus.STALE}},
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


def validate_manifest(
    *,
    manifest: RunManifest,
    pipeline_definition: PipelineDefinition,
    expected_jobs: int | None = None,
) -> None:
    if manifest.pipeline_definition != pipeline_definition:
        msg = (
            f"Run {manifest.run_id!r} uses pipeline "
            f"{manifest.pipeline_definition.id!r}, not {pipeline_definition.id!r}."
        )
        raise ManifestMismatchError(msg)
    if expected_jobs is not None and manifest.expected_jobs != expected_jobs:
        msg = (
            f"Run {manifest.run_id!r} expected_jobs mismatch: "
            f"{manifest.expected_jobs} != {expected_jobs}."
        )
        raise ManifestMismatchError(msg)
