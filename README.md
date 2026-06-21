# dr-queues

RabbitMQ multi-stage pipeline runtime with append-only event sinks.

dr-queues is a domain-free library for running jobs through chained stage
queues, scaling worker pools per stage, and recording pipeline lifecycle
events to durable storage. It is the execution substrate for experiment
applications such as [dr-bottleneck](https://github.com/danielle-rothermel/dr-bottleneck).

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
- Append-only `EventSink` implementations (MongoDB happy path, AMQP optional)
- Run manifest for multi-process worker coordination
- Minimal workflow engine: ordered steps + `HandlerRegistry`

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
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection |
| `MONGODB_URL` | `mongodb://localhost:27017/dr_queues` | MongoDB event store |

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
`record_artifact`) with MongoDB as the default event sink.

Options:

```bash
dr-queues-demo --sink amqp --repeats 2
dr-queues-demo --sink both --lanes 1 --repeats 1
```

Each run writes a manifest to `.runs/{run_id}/manifest.json`. The demo prints
`run_id=...` at the start — use that value when querying MongoDB (do not use the
literal placeholder `demo-...`).

On success you should see output like `events=70 terminals=10` for
`--repeats 5` with the default 2 lanes (10 jobs × 7 events per job).

### Inspect events in MongoDB

Events are stored in the `pipeline_events` collection. Replace
`YOUR_RUN_ID` with the `run_id` printed by the demo (e.g. `demo-56bd0ce5`).

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

If counts are zero, check:

- You used the **actual** `run_id` from demo output, not the placeholder text.
- The demo used the default Mongo sink (`--sink mongo`). AMQP-only runs
  (`--sink amqp`) do not write to MongoDB.
- MongoDB is running (`docker compose up -d`) and reachable at `MONGODB_URL`.
- You re-ran the demo after starting Mongo if the first attempt failed to connect.

## Package layout

| Module | Role |
|--------|------|
| `amqp/` | Connection helpers, stage queue pairs |
| `pipeline/` | `JobEnvelope`, `WorkerPool`, `TerminalTap`, runner |
| `events/` | `PipelineEvent`, `EventSink`, Mongo/AMQP/memory sinks |
| `manifest/` | Run manifest read/write, worker CLI helpers |
| `workflow/` | `PipelineDefinition`, `HandlerRegistry`, `Pipeline` |
| `analysis/` | `filter_run_events` |

## Public API

Import from `dr_queues`:

- **Setup / run:** `setup_run_queues`, `run_in_process`, `seed_jobs`, `seed_manifest_jobs`
- **Runtime:** `WorkerPool`, `TerminalTap`, `JobEnvelope`
- **Workflow:** `PipelineDefinition`, `HandlerRegistry`, `Pipeline`
- **Events:** `PipelineEvent`, `EventSink`, `MongoEventSink`, `AmqpEventSink`, `MemoryEventSink`
- **Analysis:** `filter_run_events`

## Detached stage workers

Resize or run a single stage in a separate process:

```bash
dr-queues-stage-worker \
  --run-id demo-abc123 \
  --stage transform \
  --workers 5 \
  --replace
```

Handlers must be registered in the worker process via `--handlers-module`
(default: `dr_queues.demo_handlers`).

## Future layers

A general EventBus, domain EventAdapter, and webhook/hook dispatch will likely
live in a separate package or in dr-bottleneck, built on top of dr-queues.
Pipeline lifecycle events may eventually map to versioned domain event types;
dr-queues stays focused on queue-based execution and pipeline telemetry.

## Development

```bash
uv sync
docker compose up -d
scripts/pre-check.sh              # ruff, ty, pytest (14 tests)
uv run pytest -m integration      # integration tests only; needs docker compose
```

Full manual smoke test:

```bash
dr-queues-demo \
  --repeats 5 \
  --workers slow=4,transform=4,finalize=2

# optional: exercise AMQP event sink instead of Mongo
dr-queues-demo --sink amqp --repeats 2
```

Build a wheel locally before publishing:

```bash
uv build
tar -tzf dist/dr_queues-*.tar.gz | head
unzip -l dist/dr_queues-*.whl
```
