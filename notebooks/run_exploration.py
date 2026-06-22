import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium")

with app.setup:
    import statistics
    from collections import defaultdict
    from datetime import datetime

    import marimo as mo
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    from dr_queues.events.mongo import mongodb_url
    from dr_queues.runtime.status import get_run_status, queue_snapshot
    from dr_queues.runtime.store import MongoRunStore, RunNotFoundError

    CONNECT_TIMEOUT_MS = 2000


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Single-run exploration

    Set a `run_id` below, then read down. Every block is a copyable,
    read-only query answering one analytical question about that run.
    **Copy this notebook and tweak the blocks** to build a custom
    analysis. All queries hit MongoDB; the last block snapshots RabbitMQ.
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
        _default = ""
    else:
        _recent = store.list_runs(limit=1)
        _default = _recent[0].run_id if _recent else ""
    run_id_input = mo.ui.text(value=_default, label="run_id", full_width=True)
    mo.vstack([mo.md("## Set the run to explore"), run_id_input])
    return (run_id_input,)


@app.cell(hide_code=True)
def _(run_id_input):
    run_id = run_id_input.value.strip() or None
    mo.md(f"Exploring `{run_id}`" if run_id else "_Enter a run_id above._")
    return (run_id,)


@app.cell(hide_code=True)
def _(run_id, store):
    manifest = None
    manifest_error = None
    expected_jobs = None
    if store is not None and run_id:
        try:
            manifest = store.get_manifest(run_id)
            expected_jobs = store.expected_job_count(run_id)
        except RunNotFoundError as error:
            manifest_error = str(error)
    mo.md(
        f"❌ {manifest_error}"
        if manifest_error
        else (
            f"Pipeline `{manifest.pipeline_id}`, "
            f"{len(manifest.stages)} stages, "
            f"expecting {expected_jobs} jobs."
            if manifest is not None
            else "_No run resolved yet._"
        )
    )
    return (manifest,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 1. Run overview

    `get_run_status` is the all-in-one helper: it folds the event log,
    counts job states, and snapshots every stage queue. It needs RabbitMQ
    (for the queue depths) and performs light stale-worker housekeeping.
    Guarded — if the broker is down, fall back to the Mongo-only blocks
    below.
    """)
    return


@app.cell(hide_code=True)
def _(manifest, run_id, store):
    if manifest is None:
        _out = mo.md("_Resolve a run first._")
    else:
        try:
            _status = get_run_status(run_id, run_store=store)
            _rows = [
                {
                    "stage": _stage.stage,
                    "expected": _stage.expected_jobs,
                    "started": _stage.started_jobs,
                    "completed": _stage.completed_jobs,
                    "in_flight": _stage.in_flight_jobs,
                    "in_ready": _stage.input_queue.ready_messages,
                    "consumers": _stage.input_queue.consumers,
                }
                for _stage in _status.stages
            ]
            _out = mo.vstack(
                [
                    mo.md(
                        f"### `{run_id}` — "
                        f"{_status.terminal_jobs}/{_status.expected_jobs} "
                        f"terminal · {len(_status.workers)} worker records · "
                        f"{sum(_worker.concurrency for _worker in _status.workers)} "
                        "concurrency"
                    ),
                    mo.ui.table(_rows, selection=None),
                ]
            )
        except Exception as error:  # noqa: BLE001 - broker down or queue absent
            _out = mo.md(
                f"_RabbitMQ unavailable — overview needs the broker. {error}_"
            )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 2. State breakdown

    The workhorse view. Every job's current `job_states` row, plus a
    server-side count by (stage, status).
    """)
    return


@app.cell(hide_code=True)
def _(manifest, mongo_db, run_id, store):
    if manifest is None:
        _out = mo.md("_Resolve a run first._")
    else:
        _states = store.list_job_states(run_id)
        _rows = [
            {
                "job_id": _state.job_id,
                "stage": _state.stage,
                "status": _state.status.value,
                "partition": _state.partition_key,
                "attempts": _state.attempt_count,
                "updated_at": _state.updated_at,
            }
            for _state in _states
        ]
        _counts = list(
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
        _count_rows = [
            {
                "stage": _doc["_id"]["stage"],
                "status": _doc["_id"]["status"],
                "count": _doc["count"],
            }
            for _doc in _counts
        ]
        _out = mo.vstack(
            [
                mo.md(f"**Counts by (stage, status)** — {len(_states)} job rows"),
                mo.ui.table(
                    _count_rows or [{"stage": "—", "status": "—", "count": 0}],
                    selection=None,
                ),
                mo.md("**All job states**"),
                mo.ui.table(
                    _rows or [{"job_id": "—"}],
                    selection=None,
                    pagination=True,
                ),
            ]
        )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 3. Failure & retry analysis

    Where debugging time goes. Every attempt is a `job_attempts` row;
    `dead_lettered` means the job exhausted `max_attempts`.
    """)
    return


@app.cell(hide_code=True)
def _(manifest, run_id, store):
    if manifest is None:
        _out = mo.md("_Resolve a run first._")
    else:
        _attempts = store.list_job_attempts(run_id)
        if not _attempts:
            _out = mo.md(f"### `{run_id}` — no failures recorded 🎉")
        else:
            _by_type: dict[str, int] = defaultdict(int)
            _dead = []
            for _attempt in _attempts:
                _by_type[_attempt.error_type] += 1
                if _attempt.action == "dead_lettered":
                    _dead.append(
                        {
                            "job_id": _attempt.job_id,
                            "stage": _attempt.stage,
                            "attempt": _attempt.attempt_number,
                            "error_type": _attempt.error_type,
                            "error_message": _attempt.error_message[:80],
                        }
                    )
            _type_rows = [
                {"error_type": _name, "count": _count}
                for _name, _count in sorted(
                    _by_type.items(), key=lambda item: item[1], reverse=True
                )
            ]
            _out = mo.vstack(
                [
                    mo.md(
                        f"### `{run_id}` — {len(_attempts)} attempts, "
                        f"{len(_dead)} dead-lettered"
                    ),
                    mo.md("**By error type**"),
                    mo.ui.table(_type_rows, selection=None),
                    mo.md("**Dead-lettered jobs**"),
                    mo.ui.table(
                        _dead or [{"job_id": "(none)"}], selection=None
                    ),
                ]
            )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 4. Single-job timeline

    Trace one job end-to-end through the `pipeline_events` log: its
    `stage_started → stage_output` march across stages, ending in
    `terminal`.
    """)
    return


@app.cell(hide_code=True)
def _(manifest, mongo_db, run_id):
    if manifest is None:
        job_selector = None
        _out = mo.md("_Resolve a run first._")
    else:
        _job_ids = sorted(
            mongo_db["pipeline_events"].distinct("job_id", {"run_id": run_id})
        )
        if not _job_ids:
            job_selector = None
            _out = mo.md("_No events for this run yet._")
        else:
            job_selector = mo.ui.dropdown(
                options={_jid: _jid for _jid in _job_ids},
                value=_job_ids[0],
                label="job_id",
            )
            _out = mo.vstack([mo.md("**Pick a job:**"), job_selector])
    _out
    return (job_selector,)


@app.cell(hide_code=True)
def _(job_selector, mongo_db, run_id):
    if job_selector is None or not job_selector.value:
        _out = mo.md("_Pick a job above._")
    else:
        _events = list(
            mongo_db["pipeline_events"]
            .find({"run_id": run_id, "job_id": job_selector.value})
            .sort("timestamp", 1)
        )
        _rows = [
            {
                "timestamp": _event["timestamp"],
                "stage": _event["stage"],
                "event": _event["event"],
            }
            for _event in _events
        ]
        _out = mo.vstack(
            [
                mo.md(f"### Timeline for `{job_selector.value}`"),
                mo.ui.table(_rows, selection=None),
            ]
        )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 5. Per-stage timing

    Derived from event timestamps: for each job, the gap between
    `stage_started` and `stage_output` is its time in that stage. The most
    copy-and-tweak-able cell — swap in p95, group by partition, etc.
    """)
    return


@app.cell(hide_code=True)
def _(manifest, run_id, store):
    if manifest is None:
        _out = mo.md("_Resolve a run first._")
    else:
        _events = store.read_by_run_id(run_id)
        _starts: dict[tuple[str, str], datetime] = {}
        _durations: dict[str, list[float]] = defaultdict(list)
        for _event in _events:
            _key = (_event.job_id, _event.stage)
            if _event.event == "stage_started":
                _starts[_key] = datetime.fromisoformat(_event.timestamp)
            elif _event.event == "stage_output" and _key in _starts:
                _delta = datetime.fromisoformat(_event.timestamp) - _starts[_key]
                _durations[_event.stage].append(_delta.total_seconds())
        _rows = [
            {
                "stage": _stage.name,
                "samples": len(_durations.get(_stage.name, [])),
                "mean_s": round(statistics.mean(_durations[_stage.name]), 3),
                "p50_s": round(statistics.median(_durations[_stage.name]), 3),
                "max_s": round(max(_durations[_stage.name]), 3),
            }
            for _stage in manifest.stages
            if _durations.get(_stage.name)
        ]
        _out = mo.vstack(
            [
                mo.md(f"### `{run_id}` — seconds per stage"),
                mo.ui.table(
                    _rows or [{"stage": "(no completed stages yet)"}],
                    selection=None,
                ),
            ]
        )
    _out
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 6. Live queue depths (RabbitMQ)

    Current ready-message and consumer counts for this run's stage queues.
    """)
    return


@app.cell(hide_code=True)
def _(manifest, run_id, store):
    if manifest is None:
        _out = mo.md("_Resolve a run first._")
    else:
        _partitions = store.list_run_partitions(run_id)
        try:
            _rows = []
            for _stage in manifest.stages:
                for _partition in _partitions:
                    _queue = manifest.stage_input_queue(_stage.name, _partition)
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
            _final = manifest.stages[-1]
            for _partition in _partitions:
                _queue = manifest.stage_output_queue(_final.name, _partition)
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


if __name__ == "__main__":
    app.run()
