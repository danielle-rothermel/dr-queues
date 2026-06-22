# Dashboard Demo Verification Notes

These demos exercise the local observability dashboard from progressively more
operational angles. The first two were run during dashboard development. The
third is the next designed verification scenario for Holds and Attempts.

## Demo 1: In-process pipeline progress

Status: run successfully.

Representative run IDs:

- `viewer-demo-flow-083203`
- `viewer-demo-live-083558`

Representative command:

```bash
uv run dr-queues-demo \
  --run-id viewer-demo-live-083558 \
  --repeats 240 \
  --lanes 3 \
  --workers slow=1,transform=1,finalize=1
```

Reason:

This demo verifies that the dashboard can observe a normal run without adding
any control-plane behavior. It is intentionally the simplest user-facing story:
load a run ID, watch stage progress, and confirm terminal completion.

What to look for:

- Overview starts as running and ends complete.
- Overall progress climbs to `720/720`.
- Stage progress shows `slow`, then `transform`, then `finalize` advancing.
- Events fill with stage started, stage completed, and terminal activity.
- Queue depths briefly move and then drain.
- Blocked jobs, Holds, and Attempts remain empty.
- Workers remains empty.

What this taught:

The dashboard can read real Mongo and RabbitMQ runtime state while a run is in
flight. It also made an important model distinction visible: in-process demo
workers drain real queues and emit real events, but they do not create detached
worker process records. An empty Workers panel is therefore expected in this
mode and is not a dashboard bug.

## Demo 2: Detached workers

Status: run successfully.

Run ID:

- `viewer-demo-detached-084244`

Observed parameters:

- Expected jobs: `720`
- Workers: `slow=1,transform=1,finalize=1`
- Worker PIDs: `40549`, `40550`, `40551`

Reason:

This demo verifies the dashboard's worker-process view. Unlike demo 1, it
starts one detached worker process per stage so the runtime store has worker
records, heartbeats, PIDs, stages, and stop transitions to display.

What to look for:

- Workers shows one row each for `slow`, `transform`, and `finalize`.
- Worker rows include stage, status, PID, and worker count.
- Overview active worker count is nonzero while the run is active.
- Overall progress climbs to `720/720`.
- Stage progress advances while worker records remain visible.
- Events and stage counts behave like demo 1.
- After completion, stop is requested for all three workers and their records
  transition to stopped.

What this taught:

The Workers panel is tied to detached worker registration, not queue activity
alone. It also showed that the dashboard can combine run progress and worker
lifecycle state in one snapshot: the run completed at `720/720`, the worker
records were visible while running, and all three workers were stopped cleanly.

## Demo 3: Holds and attempts

Status: designed, not yet run.

Reason:

This demo should intentionally leave a run partially blocked so the dashboard
can show the control and failure surfaces together. Demo 1 and demo 2 are happy
path demos; this one should make Holds, Attempts, Blocked jobs, and partial
progress obvious.

Suggested scenario:

- Seed one run with three target groups:
  - `control` jobs that should complete normally.
  - `held` jobs tagged `quota_pool=gemini-flash`.
  - `failing` jobs tagged `quota_pool=openai-nano`.
- Before workers start, set an active hold:

```bash
uv run dr-queues-run holds set \
  --run-id YOUR_RUN_ID \
  --selector quota_pool=gemini-flash \
  --reason "demo quota hold"
```

- Run detached workers using a demo handler module where the `transform` stage
  raises for jobs tagged `quota_pool=openai-nano`.

What to look for:

- Holds shows one active hold for `quota_pool=gemini-flash`.
- Blocked jobs includes held jobs with status `held`.
- Attempts shows failed transform attempts for `quota_pool=openai-nano`.
- Blocked jobs also includes failed jobs with status `retry_waiting` or
  `dead_lettered`, depending on max attempts.
- Overall progress climbs for control jobs and then stalls below 100%.
- Stage progress makes the stopping point visible.
- Workers remains populated if detached workers are used.
- Events show normal activity for control jobs and partial activity for failing
  jobs.

What this should teach:

Holds are intentional operational pauses, while Attempts are evidence of actual
handler failures. Seeing both in one run should make the dashboard useful for
triage: a user can distinguish work that was deliberately paused from work that
failed and needs retry, replay, or handler repair.
