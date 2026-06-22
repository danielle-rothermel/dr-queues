from __future__ import annotations

from collections.abc import Callable

from dr_queues.amqp.connection import ChannelSession, PikaDeliveryMode
from dr_queues.manifest.manifest import RunManifest
from dr_queues.pipeline.job import JobEnvelope, seed_jobs
from dr_queues.runtime.models import JobStateStatus
from dr_queues.runtime.store import InvalidSeedJobError, MongoRunStore
from dr_queues.targeting import TargetSelector

JobPublisher = Callable[[str, list[JobEnvelope]], None]
PartitionQueueDeclarer = Callable[[RunManifest, str], None]


def publish_jobs_to_queue(queue_name: str, jobs: list[JobEnvelope]) -> None:
    seed_jobs(queue_name=queue_name, jobs=jobs)


def declare_partition_queues(
    manifest: RunManifest,
    partition_key: str,
    *,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> None:
    session = ChannelSession.open_session(delivery_mode=delivery_mode)
    try:
        for stage in manifest.stages:
            ChannelSession.declare_durable_queue(
                queue_name=stage.input_queue_for_partition(partition_key),
                channel=session.channel,
                delivery_mode=delivery_mode,
            )
            ChannelSession.declare_durable_queue(
                queue_name=stage.output_queue_for_partition(partition_key),
                channel=session.channel,
                delivery_mode=delivery_mode,
            )
    finally:
        session.close()


def seed_stage_eligible_jobs(
    manifest: RunManifest,
    jobs: list[JobEnvelope],
    *,
    run_store: MongoRunStore,
    publisher: JobPublisher = publish_jobs_to_queue,
    queue_declarer: PartitionQueueDeclarer = declare_partition_queues,
) -> None:
    _validate_seed_jobs(manifest, jobs)
    batch = run_store.create_seed_batch(
        run_id=manifest.run_id,
        job_ids=[job.job_id for job in jobs],
    )
    try:
        first_stage = manifest.stages[0]
        _mark_pending_and_publish(
            manifest=manifest,
            jobs_by_stage=[(first_stage.name, job) for job in jobs],
            run_store=run_store,
            publisher=publisher,
            queue_declarer=queue_declarer,
        )
    except Exception as error:
        run_store.mark_seed_batch_failed(batch.batch_id, str(error))
        raise
    else:
        run_store.mark_seed_batch_published(batch.batch_id)


def replay_stage_eligible_jobs(
    manifest: RunManifest,
    *,
    run_store: MongoRunStore,
    job_id: str | None = None,
    status: JobStateStatus | None = None,
    include_selectors: list[TargetSelector] | None = None,
    force: bool = False,
    publisher: JobPublisher = publish_jobs_to_queue,
    queue_declarer: PartitionQueueDeclarer = declare_partition_queues,
) -> int:
    states = run_store.replayable_job_states(
        manifest.run_id,
        job_id=job_id,
        status=status,
        include=include_selectors,
        force=force,
    )
    _mark_pending_and_publish(
        manifest=manifest,
        jobs_by_stage=[
            (state.stage, JobEnvelope.model_validate(state.job))
            for state in states
        ],
        run_store=run_store,
        publisher=publisher,
        queue_declarer=queue_declarer,
    )
    return len(states)


def _mark_pending_and_publish(
    *,
    manifest: RunManifest,
    jobs_by_stage: list[tuple[str, JobEnvelope]],
    run_store: MongoRunStore,
    publisher: JobPublisher,
    queue_declarer: PartitionQueueDeclarer,
) -> None:
    jobs_by_queue: dict[str, list[JobEnvelope]] = {}
    for stage_name, job in jobs_by_stage:
        job.resolve_partition_key()
        queue_declarer(manifest, job.partition_key)
        queue_name = manifest.stage_input_queue(stage_name, job.partition_key)
        run_store.mark_job_pending(
            job=job,
            stage=stage_name,
            queue_name=queue_name,
        )
        jobs_by_queue.setdefault(queue_name, []).append(job)
    for queue_name, queued_jobs in jobs_by_queue.items():
        publisher(queue_name, queued_jobs)


def _validate_seed_jobs(
    manifest: RunManifest,
    jobs: list[JobEnvelope],
) -> None:
    mismatched_run_ids = sorted(
        {job.run_id for job in jobs if job.run_id != manifest.run_id}
    )
    if mismatched_run_ids:
        msg = (
            f"Seed jobs for run {manifest.run_id!r} include other run IDs: "
            f"{', '.join(mismatched_run_ids)}."
        )
        raise InvalidSeedJobError(msg)
    mismatched_pipeline_ids = sorted(
        {
            job.pipeline_id
            for job in jobs
            if job.pipeline_id != manifest.pipeline_id
        }
    )
    if mismatched_pipeline_ids:
        msg = (
            f"Seed jobs for pipeline {manifest.pipeline_id!r} include other "
            f"pipeline IDs: {', '.join(mismatched_pipeline_ids)}."
        )
        raise InvalidSeedJobError(msg)
