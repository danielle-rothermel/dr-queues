# dr-queues

RabbitMQ multi-stage pipeline runtime with append-only event sinks.

dr-queues is a domain-free library for running jobs through chained stage
queues, scaling worker pools per stage, and recording pipeline lifecycle
events to durable storage. It is the execution substrate for experiment
applications such as [dr-bottleneck](https://github.com/danielle-rothermel/dr-bottleneck).

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

```bash
uv sync
docker compose up -d
uv run python scripts/run_pipeline_demo.py \
  --repeats 5 \
  --workers slow=4,transform=4,finalize=2
```

The demo runs a 3-stage dummy pipeline (`sleep_ms` → `add_prefix` →
`record_artifact`) with MongoDB as the default event sink.

Options:

```bash
uv run python scripts/run_pipeline_demo.py --sink amqp --repeats 2
uv run python scripts/run_pipeline_demo.py --sink both --lanes 1 --repeats 1
```

Each run writes a manifest to `.runs/{run_id}/manifest.json`.

Inspect events in MongoDB:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.countDocuments({run_id: "demo-..."})'
```

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
uv run python scripts/run_stage_workers.py \
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
scripts/pre-check.sh   # ruff, ty, pytest
uv run pytest -m integration   # requires docker compose up
```
