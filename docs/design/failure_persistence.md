# Failure Persistence

This document describes the repository's current behavior for failed runs,
failed jobs, retries, and related operational state.

## Queue Topology

Runs are represented in RabbitMQ as chained stage queues. Each stage has a
pending queue and a completed queue, named from the run-specific prefix, for
example:

- `run.<run_id>.s1.pending`
- `run.<run_id>.s1.completed`
- `run.<run_id>.s2.completed`

Later stages consume from the previous stage's completed queue. Queue
declaration is handled by `StageQueues` and `build_stage_queues` in
`src/dr_queues/amqp/queues.py`.

There is no dedicated failed-run queue, failed-job queue, dead-letter queue, or
retry queue declared by the library today.

## Persistent Runtime State

MongoDB stores several durable runtime records:

- `run_manifests`: run definitions, expected job count, stage queue names, and
  pipeline metadata.
- `pipeline_events`: append-only progress events for stage starts, stage
  outputs, and terminal completion.
- `seed_batches`: records of jobs seeded into a run, including seed publishing
  failures.
- `worker_processes`: detached worker process records and lifecycle state.

These collections are managed by `MongoRunStore` in
`src/dr_queues/runtime/store.py`.

## Pipeline Events

Pipeline events are persisted only for successful progress points:

- `stage_started`
- `stage_output`
- `terminal`

There is no persisted pipeline event type for handler failure, retry attempt,
dead-lettering, or run failure. The event schema is defined in
`src/dr_queues/events/schema.py`.

Because `stage_started` is written before a handler runs, a job that repeatedly
fails and is requeued can produce multiple start events for the same stage and
job. Runtime status deduplicates these by job id when computing progress.

## Worker Handler Failures

When a worker receives a job, it appends a `stage_started` event before calling
the stage handler. If the handler raises an exception, `WorkerPool`:

1. prints a `failed` worker log line to stdout,
2. negatively acknowledges the RabbitMQ delivery with `requeue=True`,
3. returns without appending a durable failure event.

The failed job remains in RabbitMQ and can be delivered again. The exception
message, traceback, attempt count, and failure timestamp are not persisted in
MongoDB.

## Retry Behavior

Retry support exists only as RabbitMQ redelivery after
`basic_nack(..., requeue=True)`.

The retry behavior is:

- implicit,
- immediate,
- unbounded,
- not backoff-based,
- not attempt-counted,
- not classified by transient versus permanent failure,
- not visible as a structured persisted record.

There is no configured maximum attempt count and no dead-letter transition after
repeated failures.

## Seed Publishing Failures

Seed publishing has explicit persistence. `seed_run` creates a seed batch before
publishing jobs. If publishing raises an exception, the seed batch is updated to:

- `status = failed`
- `failed_at = <timestamp>`
- `failure_detail = <exception string>`

This records failures while placing the initial jobs onto RabbitMQ. It does not
cover failures that happen later inside stage handlers.

## Worker Process State

Detached worker processes are stored in `worker_processes` with these statuses:

- `running`
- `stop_requested`
- `stopped`
- `stale`

Workers heartbeat while running. If a running worker has not heartbeat recently,
the store can mark it `stale`.

There is no `failed` worker status. A crashed or unreachable worker is inferred
through stale heartbeat state rather than a persisted crash/failure record.

## Run Status

Run status is derived from persisted events and RabbitMQ queue snapshots.
`RunStatus.is_complete` returns true when the number of terminal job events
meets or exceeds the run's expected job count.

There is no run-level status field such as:

- `created`
- `running`
- `completed`
- `failed`
- `timed_out`

As a result, a run with a permanently failing job remains incomplete rather than
being marked failed. `wait_for_run` returns the latest status when its timeout
expires, but it does not persist a timeout or failure state.

## Operational Interpretation

The current system is durable for queued work and successful progress telemetry.
It can answer questions such as:

- Which run manifest was created?
- Which jobs reached each stage output?
- Which jobs reached terminal completion?
- How many messages are ready in each queue?
- Which worker records are running, stopped, stop-requested, or stale?
- Did initial seed publishing fail?

It cannot directly answer, from persisted structured state alone:

- Which jobs failed in a handler?
- How many attempts has a job made?
- What exception caused a job to retry?
- Which failures are permanent versus transient?
- Which jobs have exhausted retries?
- Which runs are failed?
- Which failed jobs are waiting in a failed-job queue?

## Conclusion

The current failure model is best described as durable progress tracking with
implicit RabbitMQ redelivery. Failed handler attempts are retried by requeueing
the same message, but they are not persisted as first-class failure records.
Seed publishing failures and worker lifecycle state are persisted, but run-level
failure, job-level failure history, bounded retries, and dead-letter handling are
not implemented.
