from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pymongo import ASCENDING, DESCENDING, MongoClient
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
    JobAttempt,
    JobAttemptAction,
    JobState,
    JobStateStatus,
    RunRecord,
    SeedBatch,
    SeedBatchStatus,
    TargetHold,
    WorkerProcessRecord,
    WorkerStatus,
    utc_now_iso,
)
from dr_queues.targeting import TargetSelector, is_due, target_matches
from dr_queues.workflow.definition import PipelineDefinition

RUN_MANIFESTS_COLLECTION = "run_manifests"
SEED_BATCHES_COLLECTION = "seed_batches"
WORKER_PROCESSES_COLLECTION = "worker_processes"
JOB_STATES_COLLECTION = "job_states"
JOB_ATTEMPTS_COLLECTION = "job_attempts"
TARGET_HOLDS_COLLECTION = "target_holds"
STALE_HEARTBEAT_SECONDS = 30.0
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_SECONDS = 300.0


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
        job_states_collection_name: str = JOB_STATES_COLLECTION,
        job_attempts_collection_name: str = JOB_ATTEMPTS_COLLECTION,
        target_holds_collection_name: str = TARGET_HOLDS_COLLECTION,
    ) -> None:
        resolved_url = url or mongodb_url()
        self._owns_client = client is None
        self._client = client or MongoClient(resolved_url)
        database = self._client.get_database(_database_name(resolved_url))
        self._events: Collection = database[events_collection_name]
        self._manifests: Collection = database[manifests_collection_name]
        self._seed_batches: Collection = database[seed_batches_collection_name]
        self._workers: Collection = database[workers_collection_name]
        self._job_states: Collection = database[job_states_collection_name]
        self._job_attempts: Collection = database[job_attempts_collection_name]
        self._target_holds: Collection = database[target_holds_collection_name]
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
        self._job_states.create_index(
            [
                ("run_id", ASCENDING),
                ("job_id", ASCENDING),
                ("stage", ASCENDING),
            ],
            unique=True,
        )
        self._job_states.create_index(
            [
                ("run_id", ASCENDING),
                ("stage", ASCENDING),
                ("status", ASCENDING),
            ],
        )
        self._job_states.create_index(
            [
                ("run_id", ASCENDING),
                ("stage", ASCENDING),
                ("partition_key", ASCENDING),
            ],
        )
        self._job_attempts.create_index(
            [
                ("run_id", ASCENDING),
                ("job_id", ASCENDING),
                ("stage", ASCENDING),
            ],
        )
        self._target_holds.create_index([("run_id", ASCENDING)])

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
        return self.get_run_record(run_id).manifest

    def get_run_record(self, run_id: str) -> RunRecord:
        document = self._manifests.find_one({"run_id": run_id})
        if document is None:
            msg = f"Run {run_id!r} does not exist."
            raise RunNotFoundError(msg)
        return RunRecord(
            run_id=document["run_id"],
            manifest=RunManifest.model_validate(document["manifest"]),
            created_at=document["created_at"],
            updated_at=document["updated_at"],
            metadata=document.get("metadata", {}),
        )

    def list_runs(self, *, limit: int = 50) -> list[RunRecord]:
        cursor = (
            self._manifests.find({})
            .sort("created_at", DESCENDING)
            .limit(limit)
        )
        return [
            RunRecord(
                run_id=document["run_id"],
                manifest=RunManifest.model_validate(document["manifest"]),
                created_at=document["created_at"],
                updated_at=document["updated_at"],
                metadata=document.get("metadata", {}),
            )
            for document in cursor
        ]

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

    def list_recent_events(
        self,
        run_id: str,
        *,
        limit: int = 100,
    ) -> list[PipelineEvent]:
        cursor = (
            self._events.find({"run_id": run_id})
            .sort("timestamp", DESCENDING)
            .limit(limit)
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

    def mark_job_pending(
        self,
        *,
        job: Any,
        stage: str,
        queue_name: str,
    ) -> JobState:
        return self._upsert_job_state(
            job=job,
            stage=stage,
            status=JobStateStatus.PENDING,
            queue_name=queue_name,
            clear_fields=[
                "not_before",
                "held_until",
                "hold_id",
                "failure_detail",
            ],
        )

    def mark_job_running(
        self,
        *,
        job: Any,
        stage: str,
        queue_name: str,
    ) -> JobState:
        return self._upsert_job_state(
            job=job,
            stage=stage,
            status=JobStateStatus.RUNNING,
            queue_name=queue_name,
            clear_fields=[
                "not_before",
                "held_until",
                "hold_id",
                "failure_detail",
            ],
        )

    def mark_job_completed(
        self,
        *,
        job: Any,
        stage: str,
        queue_name: str,
    ) -> JobState:
        return self._upsert_job_state(
            job=job,
            stage=stage,
            status=JobStateStatus.COMPLETED,
            queue_name=queue_name,
            clear_fields=["not_before", "held_until", "hold_id"],
        )

    def mark_job_terminal(
        self,
        *,
        job: Any,
        stage: str,
        queue_name: str,
    ) -> JobState:
        return self._upsert_job_state(
            job=job,
            stage=stage,
            status=JobStateStatus.TERMINAL,
            queue_name=queue_name,
            clear_fields=["not_before", "held_until", "hold_id"],
        )

    def hold_job(
        self,
        *,
        job: Any,
        stage: str,
        queue_name: str,
        hold: TargetHold,
    ) -> JobState:
        return self._upsert_job_state(
            job=job,
            stage=stage,
            status=JobStateStatus.HELD,
            queue_name=queue_name,
            extra_fields={
                "held_until": hold.blocked_until,
                "hold_id": hold.hold_id,
            },
            clear_fields=["not_before", "failure_detail"],
        )

    def record_job_failure(
        self,
        *,
        job: Any,
        stage: str,
        queue_name: str,
        error: Exception,
        worker_id: str | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
    ) -> JobAttempt:
        attempt_number = (
            self._job_attempts.count_documents(
                {
                    "run_id": job.run_id,
                    "job_id": job.job_id,
                    "stage": stage,
                },
            )
            + 1
        )
        action = (
            JobAttemptAction.DEAD_LETTERED
            if attempt_number >= max_attempts
            else JobAttemptAction.RETRY_WAITING
        )
        attempt = JobAttempt(
            run_id=job.run_id,
            job_id=job.job_id,
            stage=stage,
            partition_key=job.partition_key,
            target_tags=job.target_tags,
            attempt_number=attempt_number,
            action=action,
            error_type=type(error).__name__,
            error_message=str(error),
            worker_id=worker_id,
        )
        self._job_attempts.insert_one(attempt.model_dump())

        status = (
            JobStateStatus.DEAD_LETTERED
            if action == JobAttemptAction.DEAD_LETTERED
            else JobStateStatus.RETRY_WAITING
        )
        not_before = None
        if status == JobStateStatus.RETRY_WAITING:
            not_before_dt = datetime.now(tz=UTC) + timedelta(
                seconds=retry_delay_seconds,
            )
            not_before = not_before_dt.isoformat()
        self._upsert_job_state(
            job=job,
            stage=stage,
            status=status,
            queue_name=queue_name,
            extra_fields={
                "attempt_count": attempt_number,
                "not_before": not_before,
                "failure_detail": attempt.error_message,
            },
            clear_fields=["held_until", "hold_id"],
        )
        return attempt

    def list_job_states(
        self,
        run_id: str,
        *,
        stage: str | None = None,
        status: JobStateStatus | None = None,
        partition_key: str | None = None,
        limit: int | None = None,
    ) -> list[JobState]:
        query: dict[str, Any] = {"run_id": run_id}
        if stage is not None:
            query["stage"] = stage
        if status is not None:
            query["status"] = status
        if partition_key is not None:
            query["partition_key"] = partition_key
        cursor = self._job_states.find(query).sort(
            [("stage", ASCENDING), ("job_id", ASCENDING)]
        )
        if limit is not None:
            cursor = cursor.limit(limit)
        return [JobState.model_validate(document) for document in cursor]

    def list_job_attempts(
        self,
        run_id: str,
        *,
        job_id: str | None = None,
        limit: int | None = None,
    ) -> list[JobAttempt]:
        query: dict[str, Any] = {"run_id": run_id}
        if job_id is not None:
            query["job_id"] = job_id
        direction = DESCENDING if limit is not None else ASCENDING
        cursor = self._job_attempts.find(query).sort("created_at", direction)
        if limit is not None:
            cursor = cursor.limit(limit)
        return [JobAttempt.model_validate(document) for document in cursor]

    def list_run_partitions(self, run_id: str) -> list[str]:
        partitions = self._job_states.distinct(
            "partition_key",
            {"run_id": run_id},
        )
        return sorted(str(partition) for partition in partitions) or [
            "default"
        ]

    def list_stage_partitions(
        self,
        *,
        run_id: str,
        stage: str,
        include: list[TargetSelector] | None = None,
        exclude: list[TargetSelector] | None = None,
    ) -> list[str]:
        stage_states = self.list_job_states(run_id, stage=stage)
        run_states = self.list_job_states(run_id)
        if not run_states:
            return self.list_run_partitions(run_id)
        states = (
            run_states if include or exclude else stage_states or run_states
        )
        partitions = {
            state.partition_key
            for state in states
            if target_matches(
                state.target_tags,
                include=include,
                exclude=exclude,
            )
        }
        return sorted(partitions)

    def set_target_hold(
        self,
        *,
        run_id: str,
        selectors: list[TargetSelector],
        blocked_until: str | None = None,
        reason: str | None = None,
    ) -> TargetHold:
        hold = TargetHold(
            run_id=run_id,
            selectors=selectors,
            blocked_until=blocked_until,
            reason=reason,
        )
        self._target_holds.insert_one(hold.model_dump())
        return hold

    def clear_target_holds(
        self,
        *,
        run_id: str,
        selectors: list[TargetSelector],
    ) -> int:
        selector_documents = [selector.model_dump() for selector in selectors]
        result = self._target_holds.update_many(
            {
                "run_id": run_id,
                "selectors": selector_documents,
                "cleared_at": None,
            },
            {"$set": {"cleared_at": utc_now_iso()}},
        )
        return result.modified_count

    def list_target_holds(
        self,
        run_id: str,
        *,
        active_only: bool = True,
    ) -> list[TargetHold]:
        query: dict[str, Any] = {"run_id": run_id}
        if active_only:
            query["cleared_at"] = None
        cursor = self._target_holds.find(query).sort("created_at", ASCENDING)
        return [TargetHold.model_validate(document) for document in cursor]

    def active_hold_for_tags(
        self,
        *,
        run_id: str,
        tags: dict[str, str],
    ) -> TargetHold | None:
        for hold in self.list_target_holds(run_id):
            if hold.blocked_until is not None and is_due(hold.blocked_until):
                continue
            if target_matches(tags, include=hold.selectors):
                return hold
        return None

    def replayable_job_states(
        self,
        run_id: str,
        *,
        job_id: str | None = None,
        status: JobStateStatus | None = None,
        include: list[TargetSelector] | None = None,
        exclude: list[TargetSelector] | None = None,
        force: bool = False,
    ) -> list[JobState]:
        if status is not None:
            states = self.list_job_states(run_id, status=status)
        else:
            states = [
                state
                for replay_status in [
                    JobStateStatus.RETRY_WAITING,
                    JobStateStatus.HELD,
                    JobStateStatus.FAILED,
                    JobStateStatus.DEAD_LETTERED,
                ]
                for state in self.list_job_states(run_id, status=replay_status)
            ]
        replayable = []
        for state in states:
            if job_id is not None and state.job_id != job_id:
                continue
            if not target_matches(
                state.target_tags,
                include=include,
                exclude=exclude,
            ):
                continue
            if (
                not force
                and state.status == JobStateStatus.RETRY_WAITING
                and not is_due(state.not_before)
            ):
                continue
            replayable.append(state)
        return replayable

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
            "status": WorkerStatus.RUNNING,
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

    def _upsert_job_state(
        self,
        *,
        job: Any,
        stage: str,
        status: JobStateStatus,
        queue_name: str,
        extra_fields: dict[str, Any] | None = None,
        clear_fields: list[str] | None = None,
    ) -> JobState:
        existing = self._job_states.find_one(
            {"run_id": job.run_id, "job_id": job.job_id, "stage": stage}
        )
        now = utc_now_iso()
        set_fields: dict[str, Any] = {
            "run_id": job.run_id,
            "job_id": job.job_id,
            "stage": stage,
            "status": status,
            "partition_key": job.partition_key,
            "target_tags": job.target_tags,
            "queue_name": queue_name,
            "job": job.model_dump(),
            "updated_at": now,
        }
        if existing is None:
            set_fields["created_at"] = now
        if extra_fields:
            set_fields.update(extra_fields)
        update: dict[str, Any] = {"$set": set_fields}
        if clear_fields:
            update["$unset"] = {field: "" for field in clear_fields}
        self._job_states.update_one(
            {"run_id": job.run_id, "job_id": job.job_id, "stage": stage},
            update,
            upsert=True,
        )
        document = self._job_states.find_one(
            {"run_id": job.run_id, "job_id": job.job_id, "stage": stage}
        )
        if document is None:
            msg = f"Job state for job {job.job_id!r} was not persisted."
            raise RuntimeError(msg)
        return JobState.model_validate(document)


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
