# AMQP Refactor Manual Testing

This log tracks manual validation for the AMQP lifecycle/topology/publishing
refactor. Each scenario records what was tested, why it was tested, what
happened, and any fix or follow-up action taken.

## Completion Criteria

Manual testing is complete when all of these are true:

- Automated tests and `scripts/pre-check.sh` pass.
- Every scenario below has a recorded result.
- At least one in-process run reaches terminal completion.
- At least one detached-worker run reaches terminal completion.
- Status and viewer queue depths agree with completed runs, with no unexpected
  ready messages left behind.
- Partitioned seed/replay sends jobs to the expected partition queues.
- Failure/retry behavior still matches the pre-refactor semantics.
- Any bug found during manual testing has a documented fix and retest result.

## Scenarios

### Connectivity and Config Freshness

- **What to test:** Connect to RabbitMQ with the default `AMQP_URL`, then run a
  separate command with a changed `AMQP_URL`.
- **Why:** Confirms new connections read current environment configuration
  instead of reusing cached URL parameters.
- **What happened:** `open_connection()` succeeded with the default URL and
  closed cleanly. A second command with
  `AMQP_URL=amqp://guest:guest@localhost:5672/` printed that URL and connected
  successfully.
- **Actions taken:** None.

### In-Process Happy Path

- **What to test:** Run `uv run dr-queues-demo --repeats 2 --lanes 1`.
- **Why:** Exercises setup, seed, worker forwarding, terminal tap persistence,
  acking, and final status end to end.
- **What happened:** Run `demo-e37383b2` completed with `events=14` and
  `terminals=2`.
- **Actions taken:** None.

### Status and Viewer Queue Snapshots

- **What to test:** Run `dr-queues-run status --run-id <run_id>` repeatedly
  during and after a run, and optionally open `dr-queues-viewer`.
- **Why:** Exercises the batched passive queue snapshot path and viewer refresh
  behavior.
- **What happened:** Two `dr-queues-run status --run-id demo-e37383b2` calls
  reported `terminals=2/2` and zero input/output depth for all stages. A
  `TestClient` request to `/api/runs/demo-e37383b2/status` returned HTTP 200,
  `terminals=2/2`, and zero input/output depth for all stages.
- **Actions taken:** None.

### Detached Worker Path

- **What to test:** Initialize and seed a run, start detached stage workers,
  wait for terminal completion, then list workers.
- **Why:** Verifies long-lived worker loops outside in-process orchestration.
- **What happened:** Run `manual-amqp-detached-20260623` was initialized and
  seeded with one job. Detached `finalize`, `transform`, and `slow` workers
  started successfully. `dr-queues-run wait --target terminal --timeout 60`
  reported `terminals=1/1`. Status showed zero input/output depth for all
  stages.
- **Actions taken:** Ran `dr-queues-run stop --run-id
  manual-amqp-detached-20260623`; after a short wait, all three worker records
  were `stopped`.

### Partitioned Queues and Replay

- **What to test:** Seed jobs with multiple target tags, confirm partition
  queues are created, apply a hold/clear or replay flow, and verify jobs return
  to the correct stage partition queue.
- **Why:** Covers deduped partition declaration and seed/replay topology.
- **What happened:** Run `manual-amqp-partition-20260623` was seeded with
  `quota_pool=openai` and `quota_pool=gemini` jobs. RabbitMQ passive declares
  showed one ready message in each first-stage partition queue. Replaying with
  `--selector quota_pool=openai --status pending --force` reported
  `replayed=1`; after replay, the openai partition had two ready messages and
  the gemini partition still had one.
- **Actions taken:** Deleted 12 RabbitMQ queues for the manual partition run
  after observing the expected routing.

### Failure and Retry Behavior

- **What to test:** Run a handler-failure scenario, then verify nack, retry, or
  dead-letter state remains correct.
- **Why:** Ensures lifecycle cleanup did not change ack/nack ordering or
  Mongo write-before-ack behavior.
- **What happened:** Run `manual-amqp-failure-20260623` used a real
  `WorkerPool` with a handler that raises `RuntimeError("manual failure")` and
  `max_attempts=1`. Mongo recorded one dead-lettered job, terminal progress
  stayed `0/1`, and status showed zero input/output queue depth for the failing
  stage.
- **Actions taken:** Deleted the two RabbitMQ queues for the manual failure run
  after observing the expected dead-letter behavior.
