REVIEW REQUEST: can you review our implementation of hte connection pieces and the places where we temporarily make channels for quick things, the Classvar helpers that support this, the encapsulation of different semantic concepts in objects, etc, and look for any place that we're not really using connections vs. channels vs. something lese correctly, we're not respecting how long we should keep a channel open for (potentially by sharing it) or we're building a channel or connection etc in a strange way.  identify things that don't really make sense with the mental model of these networking setups.  report what we might want to improve to make the code base read more natrual in the context of hte pieces its using and the job its doing.



## Findings

### 1. `ChannelSession` overloads three different concepts

`ChannelSession` is doing too much at once:

- A **lifecycle object** (`open_session`, `close`, `publish_job`)
- A **static broker utility namespace** (`declare_durable_queue`, `ensure_channel` as classmethods)
- A **misleading name** — it owns both a connection and a channel, but reads like “one channel”

That makes call sites hard to parse. When a worker does this:

```130:134:src/dr_queues/pipeline/workers.py
                ChannelSession.declare_durable_queue(
                    queue_name=output_queue,
                    channel=channel,
                    delivery_mode=self.delivery_mode,
                )
```

…nothing about “session” is happening. It is just `queue_declare` on an already-open consumer channel. The API shape fights the AMQP mental model: **connection → channel → operation**.

### 2. Dual connection patterns with no shared vocabulary

Long-lived workers bypass `ChannelSession` entirely:

```85:87:src/dr_queues/pipeline/workers.py
    def _run_worker(self, _index: int) -> None:
        connection = open_connection()
        channel = connection.channel()
```

Ephemeral work goes through `ChannelSession.open_session()`. Both are valid, but the codebase never states the rule:

| Pattern | Lifetime | Used for |
|---------|----------|----------|
| `open_connection()` + manual close | Process/thread lifetime | `WorkerPool`, `TerminalTap` |
| `ChannelSession.open_session()` | Function scope | seed, declare, status |

A reader has to infer that split. A clearer model would be two explicit abstractions, e.g. `BrokerSession` (context manager for short ops) and `WorkerBroker` / plain loop setup for long-lived consume/publish loops.

### 3. Per-message `queue_declare` on the worker hot path

On every successful forward, the worker re-declares the output queue on the **same channel that is consuming**:

```127:140:src/dr_queues/pipeline/workers.py
        if result.should_forward:
            output_queue = self._output_queue(result.job)
            if output_queue is not None:
                ChannelSession.declare_durable_queue(
                    queue_name=output_queue,
                    channel=channel,
                    delivery_mode=self.delivery_mode,
                )
                publish_job(
                    channel=channel,
                    queue_name=output_queue,
                    body=result.job.to_json(),
                    delivery_mode=self.delivery_mode,
                )
```

Queues are already declared at run setup (`build_stage_queues`) and partition creation (`declare_partition_queues`). Re-declaring per job is:

- Extra broker round-trips on the critical path
- Semantically odd — declare is topology setup, not message handling
- A sign the “borrowed channel” classmethod API is being used as a generic helper rather than as session management

If the goal is defensive “ensure exists before publish,” that belongs at setup/replay/partition-declaration time, not inside `_on_message`.

### 4. `queue_snapshot` opens one TCP connection per queue

```24:42:src/dr_queues/runtime/status.py
def queue_snapshot(queue_name: str) -> QueueSnapshot:
    session = ChannelSession.open_session()
    try:
        method = session.channel.queue_declare(
            queue=queue_name,
            passive=True,
        )
        ...
    finally:
        session.close()
```

`build_run_status` aggregates snapshots across every stage × every partition. A 3-stage run with 4 partitions can mean **24 connect/handshake/close cycles** per status call. The viewer auto-refreshes on that path, so this is structurally expensive and reads unlike normal AMQP usage, where one channel performs many passive declares.

Not incorrect, but it does not match how people usually use connections/channels for observability.

### 5. `setup_run_queues` opens one connection per stage

Each `build_stage_queues(...)` call declares queues in its own session:

```62:74:src/dr_queues/pipeline/runner.py
        for index in range(stage_count):
            ...
                queues = build_stage_queues(
                    prefix=stage_prefix,
                    ...
                )
```

A 5-stage pipeline means 5 separate broker sessions for setup. Harmless at setup frequency, but inconsistent with `declare_partition_queues`, which correctly batches many declares on one channel:

```26:38:src/dr_queues/pipeline/eligibility.py
    session = ChannelSession.open_session(delivery_mode=delivery_mode)
    try:
        for stage in manifest.stages:
            ChannelSession.declare_durable_queue(
                queue_name=stage.input_queue_for_partition(partition_key),
                channel=session.channel,
                ...
            )
```

The good pattern already exists; setup just does not use it.

### 6. Nested helper calls in `StageQueues.declare_queues`

```23:41:src/dr_queues/amqp/queues.py
        build_queue_session, channel = ChannelSession.ensure_channel(...)
        try:
            ChannelSession.declare_durable_queue(..., channel=channel, ...)
            ...
        finally:
            if build_queue_session is not None:
                build_queue_session.close()
```

This works, but it is awkward:

- `ensure_channel` opens a session
- `declare_durable_queue(..., channel=channel)` is called with that channel, so the inner “open ephemeral session if channel is None” branch is skipped
- The outer session is closed in `finally`

So you get **session → borrow channel → call classmethod that sometimes opens another session → close outer session**. The layering is harder to follow than:

```python
with broker_session() as session:
    declare_durable_queue(session.channel, pending_name)
    declare_durable_queue(session.channel, completed_name)
```

Also, `build_queue_session` is a confusing name for “maybe-owned session.”

### 7. Misleading parameters and dead API surface

**`delivery_mode` on declare helpers** — passed through `declare_durable_queue` / `ensure_channel`, but queue declaration never uses it. It only matters when the helper opens its own session for publishing later. That parameter suggests declare affects durability of the queue; it does not (`durable=True` is hardcoded).

**`ensure_durable_queue`** — pure alias for `declare_durable_queue`; adds no semantics.

**`ReceivedMessage` / `from_get_tuple`** — dead code. Nothing uses `basic_get` or this model; the runtime is entirely `basic_consume`-based. That reads like a leftover from an alternate design and clutters the connection module.

### 8. Domain models mixed with broker mutation

`StageQueues` is a Pydantic model of queue names, but it also performs broker I/O via `declare_queues()`. `build_stage_queues()` both constructs the object **and** declares on RabbitMQ. That blurs:

- **Naming / manifest data** (`StageQueues`, `RunManifest`)
- **Broker topology setup** (declare)
- **Job transport** (publish/consume)

In AMQP terms, declaring queues is admin/setup; publishing is transport; consuming is processing. Collapsing setup into a “build” factory makes the codebase harder to navigate.

### 9. Cached URL parameters never refresh

```69:71:src/dr_queues/amqp/connection.py
@lru_cache(maxsize=1)
def _parameters() -> pika.URLParameters:
    return pika.URLParameters(amqp_url())
```

The first connection pins `AMQP_URL` for the process lifetime. Unusual for tests or tooling that might repoint the broker mid-process. Minor, but it breaks the expectation that `amqp_url()` is live.

---

## What is actually correct

Worth keeping as-is or with only naming cleanup:

- **One connection per worker thread** — correct for Pika’s blocking adapter (connections/channels are not thread-safe).
- **Manual ack + prefetch on long-lived channels** — correct consumer pattern.
- **Publish and ack on the same consumer channel** — normal for single-threaded blocking usage.
- **`TerminalTap` storing `(channel, delivery_tag)` until batch ack** — required; acks must happen on the receiving channel.
- **`declare_partition_queues` and `seed_jobs`** — one session, many ops, then close. This is the natural ephemeral pattern.
- **Passive `queue_declare` for metrics** — correct AMQP usage; the issue is session granularity, not the declare itself.

---

## Improvements that would make the code read more naturally

### A. Split responsibilities in `amqp/`

A layout that mirrors AMQP layers:

| Module | Responsibility |
|--------|----------------|
| `connection.py` | `open_connection()`, URL params, type aliases |
| `session.py` | Context-managed `BrokerSession` (connection + channel + close) |
| `topology.py` | `declare_durable_queue(channel, name)`, batch declare helpers |
| `publish.py` | `publish_job(channel, ...)` |

Then classmethods on a session type are not the home for queue topology.

### B. Use a context manager for ephemeral work

Replace `session = ChannelSession.open_session(); try/finally: session.close()` with:

```python
with broker_session() as broker:
    declare_durable_queue(broker.channel, name)
    publish_job(broker.channel, ...)
```

That matches Python I/O idioms and AMQP lifetimes: open connection, open channel, do work, close channel, close connection.

### C. Make long-lived loops explicit

Extract a small shared setup for workers/tap:

```python
connection = open_connection()
channel = connection.channel()
channel.basic_qos(...)
# consume loop
# finally: close channel, close connection
```

Or a `WorkerBrokerLoop` helper. The point is to document that these are **not** “sessions” in the ephemeral sense.

### D. Batch broker setup and observability

- **`setup_run_queues`**: one session, declare all stage queues.
- **`get_run_status`**: one session, passive-declare all queue names, return snapshots.
- **`declare_partition_queues`**: already good; make it the canonical pattern.

### E. Remove per-message declare from workers

Trust setup + partition declaration. If you want belt-and-suspenders, declare output queues once when the worker starts (same long-lived channel), not per job.

### F. Rename and trim API surface

- `ChannelSession` → `BrokerSession` (or similar)
- Drop `ensure_durable_queue`, `ensure_channel`, `ReceivedMessage`
- Move `delivery_mode` off declare helpers; keep it on publish/session only
- Split `StageQueues` (names) from `declare_stage_queues(channel, ...)` (I/O)

### G. Optional: separate “topology” from “transport” in call-site language

Call sites would read closer to the job:

- `ensure_run_topology(manifest)` — declares everything needed for a run
- `publish_jobs(channel, queue, jobs)` — seed/replay
- `WorkerPool` — consume/ack/forward on a long-lived channel

---

## Suggested priority

1. **High impact, low risk:** batch status/setup declares; remove per-message declare in workers; add `broker_session` context manager.
2. **Readability:** split declare/publish/session helpers; stop using `ChannelSession` classmethods for borrowed-channel ops.
3. **Cleanup:** remove dead `ReceivedMessage` API; drop `ensure_durable_queue`; fix or document `_parameters()` caching.

None of these look like correctness bugs today — the current code mostly works within Pika’s blocking model. The main issue is **conceptual friction**: session/classmethod layering obscures the simple AMQP story the repo is actually telling (short setup/publish bursts vs long-lived consume loops).

I can turn any of the above into a concrete refactor plan or a focused PR if you want to tackle one slice first.

QUESTION: "for "2. Dual connection patterns with no shared vocabulary" do you think it makes sense to keep the separate pathways or should we standardize them on one?"


Short answer: **keep two lifetimes, unify one vocabulary** — don’t merge them into a single call pattern that treats a worker thread and a status poll the same way.

## They are genuinely different lifetimes

| | Ephemeral ops | Long-lived loops |
|---|---|---|
| **Examples** | seed, replay declare, `queue_snapshot` | `WorkerPool`, `TerminalTap` |
| **Duration** | milliseconds | thread/process lifetime |
| **Channel role** | declare and/or publish (or passive declare) | consume + ack + publish on one channel |
| **Close when** | function returns | worker stops / thread exits |

That split is normal in AMQP client code. A consume loop is not the same job as “open, passive-declare 12 queues, close.” Collapsing those into one generic “connection helper” usually either:

- wraps the worker loop in something that *looks* short-lived, or  
- makes ephemeral code hold connections open longer than it should.

So I would **not** standardize on “everyone calls `open_connection()` + manual close” — you’d repeat lifecycle boilerplate and lose the benefit of a scoped helper.

I would also **not** hide the long-lived case behind the same tiny helper API as declare/publish without making lifetime obvious.

## What *should* be standardized: one type, two scopes

The problem today isn’t two pathways — it’s that they **look accidental** (`open_connection()` vs `ChannelSession.open_session()` with no stated rule).

A cleaner model:

```python
# Low-level (tests, conftest, maybe internal only)
open_connection() -> BlockingConnection

# Shared vocabulary for all app code
@contextmanager
def broker_session(*, delivery_mode=...) -> BrokerSession:
    conn = open_connection()
    ch = conn.channel()
    try:
        yield BrokerSession(conn, ch, delivery_mode)
    finally:
        ch.close(); conn.close()
```

Then:

```python
# Ephemeral — short `with`
with broker_session() as broker:
    declare_durable_queue(broker.channel, name)
    publish_job(broker.channel, ...)

# Long-lived — same `with`, longer scope
def _run_worker(...):
    with broker_session() as broker:
        channel = broker.channel
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(...)
        while not self._stop.is_set():
            broker.connection.process_data_events(time_limit=0.5)
```

Same abstraction, same open/close rules, different **how long you stay inside the block**. That reads naturally in AMQP terms: one connection, one channel, explicit lifetime.

Module-level helpers (`declare_durable_queue(channel, ...)`, `publish_job(channel, ...)`) stay channel-first, so borrowed-channel use in workers doesn’t go through faux “session” classmethods.

## Recommendation

| Approach | Verdict |
|----------|---------|
| Keep separate lifetimes (ephemeral vs worker loop) | **Yes** |
| Keep separate low-level primitive (`open_connection`) | **Yes**, for tests and as the implementation detail |
| Standardize app code on one session type + context manager | **Yes** |
| Standardize on only `ChannelSession` classmethods | **No** — that’s what created the confusion |
| One code path that ignores lifetime differences | **No** |

So: **standardize the vocabulary, not the lifetime.** Rename/replace `ChannelSession` with something like `BrokerSession` + `broker_session()`, migrate `WorkerPool` and `TerminalTap` onto it, and stop calling `open_connection()` directly outside `amqp/` (except tests).

The only reason to leave workers on raw `open_connection()` after that refactor would be stylistic preference inside `amqp/` itself — not because they need a fundamentally different pathway.
