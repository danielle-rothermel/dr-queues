import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    from dr_queues.events.mongo import mongodb_url
    from dr_queues.runtime.models import EventProgress, count_job_states
    from dr_queues.runtime.status import queue_snapshot
    from dr_queues.runtime.store import MongoRunStore

    CONNECT_TIMEOUT_MS = 2000


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # dr-queues — quickstart

    A living introduction to `dr-queues`. The prose and the
    define/run sections are **illustrative** (copy them into your own
    code). Every cell that touches MongoDB or RabbitMQ below is
    **runnable read-only**, so you can point this notebook at a real
    deployment and watch live numbers.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## What it is, and why it exists

    `dr-queues` is the execution substrate for multi-stage jobs: it
    pushes work through a chain of **RabbitMQ stage queues**, scales a
    **worker pool per stage**, and records everything durably in
    **MongoDB**. It is domain-free — applications such as
    `dr-bottleneck` supply the handlers; the library supplies the
    plumbing, the state, and the audit trail.

    The defining idea is the **append-before-ack invariant**: a worker
    writes its event to MongoDB *before* it acks the RabbitMQ message.
    If a worker crashes mid-job, the message is redelivered and the log
    already records what happened. **The MongoDB log is the source of
    truth**, not the queue.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## The mental model

    ```
    seed → [ slow ]→queue→[ transform ]→queue→[ finalize ]→ TerminalTap
              │WorkerPool      │WorkerPool        │WorkerPool
              └────────────────┴─────────┬────────┘
                                         ▼
                                   MongoRunStore
    ```

    Jobs (`JobEnvelope`s) flow stage→stage through RabbitMQ. Each stage's
    `WorkerPool` consumes, runs a handler, records an event, and publishes
    downstream. `TerminalTap` drains the last stage and marks jobs
    terminal. `MongoRunStore` persists it all.

    **Knowing which collection answers which question is the whole skill.**
    There are three *lenses* plus two *operational* collections:

    | Question | Collection | Lens |
    |---|---|---|
    | Where is each job *now*? | `job_states` | current state |
    | What *happened*, in order? | `pipeline_events` | history |
    | What *went wrong*? | `job_attempts` | failures |
    | Who is processing? | `worker_processes` | operational |
    | What is paused? | `target_holds` | operational |

    Live queue depth (ready messages, consumers) comes from **RabbitMQ**,
    not MongoDB.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## The standard way to use it: define → run → observe

    **1. Define** a pipeline (steps + lanes) and register handlers.
    This is illustrative — it mirrors `dr-queues-demo`:

    ```python
    from dr_queues import (
        Pipeline, PipelineDefinition, PipelineLane, PipelineStep,
    )
    from dr_queues.workflow.registry import HandlerRegistry

    registry = HandlerRegistry()

    @registry.register("sleep_ms")
    def sleep_ms(job, ctx): ...

    definition = PipelineDefinition(
        id="demo_pipeline",
        lanes=[PipelineLane(id="lane-0"), PipelineLane(id="lane-1")],
        steps=[
            PipelineStep(name="slow", handler_key="sleep_ms"),
            PipelineStep(name="transform", handler_key="add_prefix"),
            PipelineStep(name="finalize", handler_key="record_artifact"),
        ],
    )
    pipeline = Pipeline(definition, registry)
    ```

    **2. Run** it — CLI on the left, the helpers it wraps on the right:

    ```text
    # CLI                              # Programmatic
    dr-queues-run init  --run-id R     setup_run_queues(pipeline, run_id="R", ...)
    dr-queues-run seed  --run-id R     seed_run(manifest, jobs, run_store=store)
    dr-queues-run start --run-id R     # detached: start_stage_workers(...)
    dr-queues-run status --run-id R    get_run_status("R")
    dr-queues-run wait  --run-id R     wait_for_run("R")
    ```

    Or run everything in one process (what the demo does):

    ```python
    manifest = setup_run_queues(pipeline=pipeline, run_id="R", ...)
    seed_run(manifest, pipeline.make_seed_jobs(run_id="R", repeats=2))
    run_in_process(manifest=manifest, pipeline=pipeline, ...)
    ```

    **3. Observe** — that's the runnable section below.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Connect (runnable, read-only)

    One shared `MongoClient` feeds both the **typed** `MongoRunStore`
    helpers and a **raw** pymongo handle for server-side aggregations.
    Honors `MONGODB_URL` (default `mongodb://localhost:27017/dr_queues`).
    """)
    return


@app.cell(hide_code=True)
def _():
    connect_error = None
    store = None
    mongo_db = None
    try:
        _client = MongoClient(
            mongodb_url(),
            serverSelectionTimeoutMS=CONNECT_TIMEOUT_MS,
        )
        store = MongoRunStore(client=_client)
        mongo_db = _client.get_default_database()
    except PyMongoError as error:
        connect_error = str(error)

    mo.md(
        f"❌ **MongoDB unavailable** — `{connect_error}`"
        if connect_error
        else f"✅ Connected to `{mongodb_url()}`"
    )
    return mongo_db, store


@app.cell(hide_code=True)
def _(store):
    if store is None:
        run_selector = None
        _out = mo.md("_Connect to MongoDB above to choose a run._")
    else:
        _runs = store.list_runs(limit=10)
        if not _runs:
            run_selector = None
            _out = mo.md(
                "_No runs in this database yet. Run `dr-queues-demo` first._"
            )
        else:
            run_selector = mo.ui.dropdown(
                options={record.run_id: record.run_id for record in _runs},
                value=_runs[0].run_id,
                label="run_id",
            )
            _out = mo.vstack([mo.md("**Pick a run to inspect:**"), run_selector])
    _out
    return (run_selector,)


@app.cell(hide_code=True)
def _(run_selector):
    run_id = run_selector.value if run_selector is not None else None
    mo.md(f"Selected run: `{run_id}`" if run_id else "_No run selected._")
    return (run_id,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Which collection answers which question?

    ### "Is it done?" → terminal progress (`pipeline_events`)

    Pure read of the event log. `EventProgress` folds the events into
    per-stage started/completed sets and the set of terminal jobs.
    """)
    return


@app.cell(hide_code=True)
def _(run_id, store):
    if store is None or not run_id:
        _out = mo.md("_Select a run to see terminal progress._")
    else:
        _events = store.read_by_run_id(run_id)
        _progress = EventProgress.from_events(_events)
        _manifest = store.get_manifest(run_id)
        _rows = [
            {
                "stage": _stage.name,
                "started": len(_progress.stage_started.get(_stage.name, set())),
                "completed": len(
                    _progress.stage_completed.get(_stage.name, set())
                ),
            }
            for _stage in _manifest.stages
        ]
        _out = mo.vstack(
            [
                mo.md(
                    f"### `{run_id}` — "
                    f"{len(_progress.terminal_jobs)}/"
                    f"{_manifest.expected_jobs} jobs terminal"
                ),
                mo.ui.table(_rows, selection=None),
            ]
        )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### "Where is everything?" → state counts (`job_states`)

    Two ways to the same numbers: the **typed helper** pulls models into
    Python, while the **raw aggregation** groups server-side in MongoDB
    (cheaper, and the pattern you reach for when slicing by stage).
    """)
    return


@app.cell(hide_code=True)
def _(mongo_db, run_id, store):
    if store is None or not run_id:
        _out = mo.md("_Select a run to see job-state counts._")
    else:
        _typed = count_job_states(store.list_job_states(run_id))
        _typed_rows = [
            {"status": _status.value, "count": _count}
            for _status, _count in _typed.items()
            if _count
        ]
        _raw = list(
            mongo_db["job_states"].aggregate(
                [
                    {"$match": {"run_id": run_id}},
                    {
                        "$group": {
                            "_id": {"stage": "$stage", "status": "$status"},
                            "count": {"$sum": 1},
                        }
                    },
                    {"$sort": {"_id.stage": 1, "_id.status": 1}},
                ]
            )
        )
        _raw_rows = [
            {
                "stage": _doc["_id"]["stage"],
                "status": _doc["_id"]["status"],
                "count": _doc["count"],
            }
            for _doc in _raw
        ]
        _out = mo.vstack(
            [
                mo.md("**Typed** — `count_job_states(store.list_job_states(run_id))`"),
                mo.ui.table(_typed_rows or [{"status": "—", "count": 0}], selection=None),
                mo.md("**Raw** — server-side `$group` by (stage, status)"),
                mo.ui.table(
                    _raw_rows or [{"stage": "—", "status": "—", "count": 0}],
                    selection=None,
                ),
            ]
        )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### "What failed and why?" → attempts (`job_attempts`)

    Each retry or dead-letter is one `job_attempts` document. Grouping by
    `error_type` and `action` (`retry_waiting` vs `dead_lettered`) is the
    first move when a run looks unhealthy.
    """)
    return


@app.cell(hide_code=True)
def _(mongo_db, run_id, store):
    if store is None or not run_id:
        _out = mo.md("_Select a run to inspect failures and retries._")
    else:
        _by_error = list(
            mongo_db["job_attempts"].aggregate(
                [
                    {"$match": {"run_id": run_id}},
                    {
                        "$group": {
                            "_id": {
                                "error_type": "$error_type",
                                "action": "$action",
                            },
                            "count": {"$sum": 1},
                        }
                    },
                    {"$sort": {"count": -1}},
                ]
            )
        )
        if not _by_error:
            _out = mo.md(f"### `{run_id}` — no failures recorded 🎉")
        else:
            _rows = [
                {
                    "error_type": _doc["_id"]["error_type"],
                    "action": _doc["_id"]["action"],
                    "count": _doc["count"],
                }
                for _doc in _by_error
            ]
            _out = mo.vstack(
                [
                    mo.md(f"### `{run_id}` — failures by type"),
                    mo.ui.table(_rows, selection=None),
                ]
            )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### "Who's working?" → workers (`worker_processes`)

    `list_workers` also performs light housekeeping — it flips workers
    whose heartbeat is older than 30s to `stale`. That is the only write
    in this notebook, and it is the same maintenance the dashboard does.
    """)
    return


@app.cell(hide_code=True)
def _(run_id, store):
    if store is None or not run_id:
        _out = mo.md("_Select a run to list workers._")
    else:
        _workers = store.list_workers(run_id)
        if not _workers:
            _out = mo.md(f"### `{run_id}` — no worker processes registered")
        else:
            _rows = [
                {
                    "worker_id": _worker.worker_id[:8],
                    "stage": _worker.stage,
                    "pid": _worker.pid,
                    "host": _worker.host,
                    "status": _worker.status.value,
                    "last_heartbeat": _worker.last_heartbeat_at,
                }
                for _worker in _workers
            ]
            _out = mo.vstack(
                [
                    mo.md(f"### `{run_id}` — workers"),
                    mo.ui.table(_rows, selection=None),
                ]
            )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### "What's paused?" → holds (`target_holds`)

    A hold blocks every job whose tags match its selectors, until cleared
    (or until `blocked_until` passes). Set them with
    `dr-queues-run holds set` and clear with `holds clear`.
    """)
    return


@app.cell(hide_code=True)
def _(run_id, store):
    if store is None or not run_id:
        _out = mo.md("_Select a run to list active holds._")
    else:
        _holds = store.list_target_holds(run_id, active_only=True)
        if not _holds:
            _out = mo.md(f"### `{run_id}` — no active holds")
        else:
            _rows = [
                {
                    "hold_id": _hold.hold_id[:8],
                    "selectors": ", ".join(
                        f"{selector.key}={selector.value}"
                        for selector in _hold.selectors
                    ),
                    "blocked_until": _hold.blocked_until or "(manual)",
                    "reason": _hold.reason or "",
                }
                for _hold in _holds
            ]
            _out = mo.vstack(
                [
                    mo.md(f"### `{run_id}` — active holds"),
                    mo.ui.table(_rows, selection=None),
                ]
            )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### "How deep are the queues?" → live depth (RabbitMQ)

    `queue_snapshot` asks the broker directly. Stage *N*'s input queue is
    stage *N-1*'s output queue, so the depths chain together. Guarded —
    if RabbitMQ is down or a queue was never declared, you get a note.
    """)
    return


@app.cell(hide_code=True)
def _(run_id, store):
    if store is None or not run_id:
        _out = mo.md("_Select a run to snapshot its queues._")
    else:
        _manifest = store.get_manifest(run_id)
        _partitions = store.list_run_partitions(run_id)
        try:
            _rows = []
            for _stage in _manifest.stages:
                for _partition in _partitions:
                    _queue = _manifest.stage_input_queue(_stage.name, _partition)
                    _snap = queue_snapshot(_queue)
                    _rows.append(
                        {
                            "stage": _stage.name,
                            "partition": _partition,
                            "queue": _queue,
                            "ready": _snap.ready_messages,
                            "consumers": _snap.consumers,
                        }
                    )
            _final = _manifest.stages[-1]
            for _partition in _partitions:
                _queue = _manifest.stage_output_queue(_final.name, _partition)
                _snap = queue_snapshot(_queue)
                _rows.append(
                    {
                        "stage": f"{_final.name} (out)",
                        "partition": _partition,
                        "queue": _queue,
                        "ready": _snap.ready_messages,
                        "consumers": _snap.consumers,
                    }
                )
            _out = mo.vstack(
                [
                    mo.md(f"### `{run_id}` — live queue depths"),
                    mo.ui.table(_rows, selection=None),
                ]
            )
        except Exception as error:  # noqa: BLE001 - broker down or queue absent
            _out = mo.md(f"_RabbitMQ unavailable or queues absent — {error}_")
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Playbooks

    Scenarios the library is built to support. Each is one CLI command
    plus the read-only query above that confirms it.

    **Check progress.** `dr-queues-run status --run-id R` →
    confirm with *"Is it done?"* and *"Where is everything?"*.

    **Diagnose failures.** A stuck run? *"What failed and why?"* groups
    the `job_attempts`; anything in `dead_lettered` exceeded `max_attempts`.

    **Pause a subset.** `dr-queues-run holds set --run-id R --select
    quota_pool=openai` pauses matching jobs; *"What's paused?"* shows the
    active hold; `holds clear` lifts it.

    **Scale a stage.** `dr-queues-run start --run-id R --stage transform
    --workers 8` (or `replace`) adds workers; *"Who's working?"* and the
    queue-depth cell show the effect.

    The shared `MongoClient` closes when the kernel shuts down; call
    `store.close()` if you want to release it sooner.
    """)
    return


if __name__ == "__main__":
    app.run()
