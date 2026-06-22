# dr-queues

RabbitMQ multi-stage pipeline runtime with MongoDB-backed run state.

dr-queues is a domain-free library for running jobs through chained RabbitMQ
stage queues, scaling worker pools per stage, and recording run state in
MongoDB. It is the execution substrate for experiment applications such as
[dr-bottleneck](https://github.com/danielle-rothermel/dr-bottleneck).

## Install

```bash
pip install dr-queues
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add dr-queues
```

RabbitMQ and MongoDB are **not** bundled with the package — run them locally
(Docker Compose) or point `AMQP_URL` / `MONGODB_URL` at your infrastructure.

After install, try the demo CLI:

```bash
dr-queues-demo --repeats 2 --lanes 1
```

## What dr-queues is

- AMQP staged pipeline: per-stage pending/completed queue pairs chained together
- Slim `JobEnvelope` for job state on the wire
- `WorkerPool` and `TerminalTap` with pluggable step handlers
- MongoDB-backed manifests, events, seed batches, worker records, job states,
  failure attempts, and target holds
- RabbitMQ durable job transport for queued and in-flight work
- Minimal workflow engine: ordered steps + `HandlerRegistry`

## Runtime model

dr-queues uses RabbitMQ and MongoDB for different jobs:

- **RabbitMQ** is the durable queue transport. It owns pending messages,
  completed-stage messages, delivery acknowledgements, redelivery, and queue
  depth. Jobs with target tags can be routed through partition-specific queues
  so workers can consume only matching target subsets.
- **MongoDB** is the persistence and query layer. It owns run manifests,
  seed-batch records, pipeline events, detached worker process records, latest
  per-job runtime state, failure attempt history, and target holds.

There is no filesystem-backed runtime store. New runs should not create
`.runs/<run_id>` state.

## What dr-queues is not

- LLM calls, prompts, or model profiles
- Dataset loading, HumanEval, or experiment metrics
- JSONL report assembly
- General domain EventBus / webhook dispatch (deferred to a future layer)

Workers append pipeline events **before** acking and forwarding jobs to the
next stage. That append-before-forward invariant matches an event-sourced
write-ahead log: durable telemetry precedes propagation.

## Requirements

- Python 3.13+ with [uv](https://docs.astral.sh/uv/)
- Docker Compose for local RabbitMQ and MongoDB

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ job transport |
| `MONGODB_URL` | `mongodb://localhost:27017/dr_queues` | MongoDB runtime store |

## Local services

```bash
docker compose up -d
```

- RabbitMQ management UI: http://localhost:15672 (guest/guest)
- MongoDB: `mongodb://localhost:27017/dr_queues`

## Quick start

From a git checkout:

```bash
uv sync
docker compose up -d
uv run dr-queues-demo \
  --repeats 5 \
  --workers slow=4,transform=4,finalize=2
```

Or use the repo script wrapper (same CLI):

```bash
uv run python scripts/run_pipeline_demo.py \
  --repeats 5 \
  --workers slow=4,transform=4,finalize=2
```

The demo runs a 3-stage dummy pipeline (`sleep_ms` → `add_prefix` →
`record_artifact`) with MongoDB as the runtime store.

Each run stores its manifest and events in MongoDB. The demo prints
`run_id=...` at the start; use that value when querying MongoDB or running
`dr-queues-run status`.

On success you should see output like `events=70 terminals=10` for
`--repeats 5` with the default 2 lanes (10 jobs × 7 events per job).

### Inspect MongoDB runtime state

Run manifests live in `run_manifests`, events in `pipeline_events`, seed
batches in `seed_batches`, worker records in `worker_processes`, latest job
state in `job_states`, failure attempt history in `job_attempts`, and target
holds in `target_holds`. Replace `YOUR_RUN_ID` with the `run_id` printed by the
demo.

Count events for one run:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.countDocuments({run_id: "YOUR_RUN_ID"})'
```

List all run IDs that have events:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.distinct("run_id")'
```

Count all events in the collection (across every run):

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.countDocuments({})'
```

Preview a few events from a run:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.find({run_id: "YOUR_RUN_ID"}).limit(3).pretty()'
```

Use the operational CLI for run status:

```bash
dr-queues-run status --run-id YOUR_RUN_ID
dr-queues-run wait --run-id YOUR_RUN_ID --target terminal --timeout 120
```

`status` combines Mongo progress records with RabbitMQ queue snapshots. Stage
lines report active worker process records separately from total persisted
records:

```text
stage=transform completed=10/10 input_depth=0 output_depth=0 workers=1 records=3
```

If counts are zero, check that MongoDB is running and that you used the actual
`run_id` from demo output, not the placeholder text.

## Package layout

| Module | Role |
|--------|------|
| `amqp/` | Connection helpers, stage queue pairs |
| `pipeline/` | `JobEnvelope`, `WorkerPool`, `TerminalTap`, runner |
| `events/` | `PipelineEvent`, local test sink, event filters |
| `manifest/` | Run manifest models and worker spec parsing |
| `runtime/` | Mongo run store, status, wait, worker lifecycle |
| `targeting.py` | Target selectors and partition-key derivation |
| `workflow/` | `PipelineDefinition`, `HandlerRegistry`, `Pipeline` |

## Public API

Import from `dr_queues`:

- **Setup / run:** `setup_run_queues`, `seed_run`, `run_in_process`
- **Runtime:** `MongoRunStore`, `get_run_status`, `wait_for_run`, `WorkerPool`, `TerminalTap`, `JobEnvelope`
- **Failure controls:** `JobState`, `JobStateStatus`, `JobAttempt`, `JobAttemptAction`, `TargetHold`, `TargetSelector`
- **Workflow:** `PipelineDefinition`, `HandlerRegistry`, `Pipeline`
- **Events:** `PipelineEvent`, `filter_run_events`

## Detached stage workers

Detached workers are controlled through Mongo worker records plus OS process
signals on the local host. Start a single stage in a separate process:

```bash
dr-queues-stage-worker \
  --run-id demo-abc123 \
  --stage transform \
  --workers 5
```

Handlers must be registered in the worker process via `--handlers-module`
(default: `dr_queues.demo_handlers`).

The operational CLI can start, replace, stop, list, and wait on detached
workers:

```bash
dr-queues-run start \
  --run-id demo-abc123 \
  --stage transform \
  --workers 5

dr-queues-run workers --run-id demo-abc123

dr-queues-run stop \
  --run-id demo-abc123 \
  --stage transform
```

Use `replace` to stop current running workers for a stage and start a new worker
process:

```bash
dr-queues-run replace \
  --run-id demo-abc123 \
  --stage transform \
  --workers 5
```

Workers can also include or exclude target subsets. This is useful when one
provider, model, or quota pool is paused but unrelated work can keep moving:

```bash
dr-queues-run start \
  --run-id demo-abc123 \
  --stage transform \
  --workers 5 \
  --include provider=openai
```

If a selector matches no known partitions, `start` and `replace` exit nonzero
instead of reporting a started worker.

`wait --target terminal` also consumes final-stage completed messages through a
terminal tap, so detached runs can reach terminal completion without an
in-process runner.

## Failure controls and replay

Handler failures are persisted in MongoDB. The worker records an append-only
attempt in `job_attempts`, updates the latest `job_states` record, and then
acknowledges the RabbitMQ delivery after persistence succeeds. Retryable
failures move to `retry_waiting`; jobs that exhaust their attempt budget move
to `dead_lettered`.

Inspect failed, held, retry-waiting, and dead-lettered jobs:

```bash
dr-queues-run failures --run-id demo-abc123
dr-queues-run attempts --run-id demo-abc123
```

Set a temporary target hold, for example when a provider quota pool is
rate-limited:

```bash
dr-queues-run holds set \
  --run-id demo-abc123 \
  --selector quota_pool=gemini-flash \
  --until +30m \
  --reason rate-limit
```

Workers persist matching jobs as `held` and remove them from the hot queue path.
After clearing a hold or fixing a failed handler, replay selected work back to
the correct stage partition queue:

```bash
dr-queues-run holds clear \
  --run-id demo-abc123 \
  --selector quota_pool=gemini-flash

dr-queues-run replay \
  --run-id demo-abc123 \
  --selector quota_pool=gemini-flash \
  --status held \
  --force
```

Replay is manual in this version. There is no background scheduler for
`retry_waiting` jobs, automatic replay after hold expiry, or token-bucket
provider throttling yet.

See [`docs/manual_runtime_testing.md`](docs/manual_runtime_testing.md) for the
manual operational test log covering detached startup, scale up/down,
kill/restart recovery, duplicate seed protection, filesystem persistence
checks, target-scoped workers, holds, retries, dead letters, and replay.
See [`docs/design/failure_persistence.md`](docs/design/failure_persistence.md)
for the current failure persistence design.

## Future layers

A general EventBus, domain EventAdapter, and webhook/hook dispatch will likely
live in a separate package or in dr-bottleneck, built on top of dr-queues.
Pipeline lifecycle events may eventually map to versioned domain event types;
dr-queues stays focused on queue-based execution and pipeline telemetry.

## Development

```bash
uv sync
docker compose up -d
scripts/pre-check.sh              # ruff, ty, pytest
uv run pytest -m integration      # integration tests only; needs docker compose
```

Full manual smoke test:

```bash
dr-queues-demo \
  --repeats 5 \
  --workers slow=4,transform=4,finalize=2
```

For failure-control scenarios, follow the tested flows in
[`docs/manual_runtime_testing.md`](docs/manual_runtime_testing.md).

Build a wheel locally before publishing:

```bash
uv build
tar -tzf dist/dr_queues-*.tar.gz | head
unzip -l dist/dr_queues-*.whl
```
