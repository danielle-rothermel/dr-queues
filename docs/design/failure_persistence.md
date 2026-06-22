# Failure Persistence

This document describes the current failure-tracking, retry, hold, and
target-aware queue behavior.

## Queue Topology

Runs still use chained RabbitMQ stage queues. Untagged jobs use the original
stage queue names, for example:

- `run.<run_id>.s1.pending`
- `run.<run_id>.s1.completed`
- `run.<run_id>.s2.completed`

Jobs can also carry generic `target_tags` and a derived `partition_key`. Tagged
jobs are routed through partition-specific queues derived from the stage queue
name, for example:

- `run.<run_id>.s1.pending.partition.gemini-flash`
- `run.<run_id>.s1.completed.partition.gemini-flash`

Later stages consume from the previous stage's partition-specific completed
queue. This lets workers target only matching partitions instead of pulling
unwanted jobs and requeueing them.

## Target Metadata

The core runtime stays domain-free. It understands generic string tags and
selectors, not provider-specific concepts.

Examples:

- `provider=gemini`
- `model=flash`
- `quota_pool=gemini-flash`

If `quota_pool` is present, it becomes the partition key. Otherwise the
partition key is derived from the sorted tag set. Jobs without tags use the
`default` partition and preserve the previous queue behavior.

## Persistent Runtime State

MongoDB stores the original runtime collections:

- `run_manifests`
- `pipeline_events`
- `seed_batches`
- `worker_processes`

It also stores operational failure/control state:

- `job_states`: latest state per run/job/stage.
- `job_attempts`: append-only failure attempt ledger.
- `target_holds`: active or cleared manual holds keyed by selectors.

`pipeline_events` remains the successful progress event log. Failure and
control-plane state lives in the dedicated runtime collections.

## Job State

`job_states` tracks operational states such as:

- `pending`
- `running`
- `completed`
- `retry_waiting`
- `held`
- `failed`
- `dead_lettered`
- `terminal`

Run status includes job-state counts so an incomplete run can show whether work
is held, retry-waiting, or dead-lettered.

## Worker Handler Failures

When a worker receives a job, it records the job as running and appends the
existing `stage_started` event. If the handler raises an exception, the worker:

1. records a `job_attempts` entry with error type/message, attempt number,
   worker id, and selected action,
2. updates `job_states` to `retry_waiting` or `dead_lettered`,
3. acknowledges the RabbitMQ delivery after persistence succeeds.

This replaces the old unbounded immediate `basic_nack(..., requeue=True)` path
when the event sink supports the durable runtime methods.

## Retry Behavior

Retries are explicit and bounded. A failed attempt is moved to
`retry_waiting` until it reaches the configured maximum attempt count. Once the
attempt limit is reached, the job is moved to `dead_lettered`.

The v1 implementation records `not_before` for retry-waiting jobs and provides
manual replay tooling. It does not yet include an always-on scheduler or
automatic token-bucket throttling.

## Target Holds

Operators can set a hold for matching target tags, optionally until a timestamp
or relative duration. For example:

```bash
dr-queues-run holds set \
  --run-id RUN_ID \
  --selector quota_pool=gemini-flash \
  --until +30m
```

Workers check active holds before invoking handlers. If a job matches an active
hold, the worker stores it as `held` and acknowledges the RabbitMQ delivery so it
leaves the hot queue path.

Clearing a hold removes the target block. Held jobs can then be replayed with
the replay command.

## Selective Workers

Detached workers can be started with include/exclude selectors:

```bash
dr-queues-run start \
  --run-id RUN_ID \
  --stage score \
  --workers 4 \
  --include provider=openai
```

The worker process resolves matching partitions from MongoDB job state and
consumes only those partition queues. If no partition matches, the stage worker
exits instead of falling back to the default queue.

## Manual Remediation

The CLI exposes manual inspection and remediation commands:

- `dr-queues-run failures`
- `dr-queues-run attempts`
- `dr-queues-run holds set`
- `dr-queues-run holds clear`
- `dr-queues-run holds list`
- `dr-queues-run replay`

Replay republishes selected held, retry-waiting, failed, or dead-lettered jobs
to the correct stage partition queue and marks them pending.

## Remaining Limits

The current implementation does not yet provide:

- automatic background release of retry-waiting jobs,
- automatic replay after a hold expires,
- token-bucket or lease-based provider throttling,
- provider-specific rate-limit classification,
- a run-level failed status field.

Those can be layered on top of the new durable job state, attempt ledger, target
holds, and partitioned queues.

## Conclusion

The failure model is now durable and operator-visible. RabbitMQ carries eligible
work; MongoDB owns the operational truth for job state, attempts, target holds,
and replay decisions. This supports manual rate-limit response and targeted
workers while keeping provider-specific policy outside the core queue runtime.
