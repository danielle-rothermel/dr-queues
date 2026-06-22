# Manual Runtime Testing

Date: 2026-06-22

This log covers manual operational testing for the Mongo-backed runtime state
and detached worker controls. The goal was to verify that RabbitMQ remains the
queue transport, Mongo remains the persistence/query layer for run state, and
the removed `.runs` path is not used for new manual runs.

## Completion Criteria

Manual testing is complete when all of these are true:

- In-process and detached runs can reach their expected terminal counts.
- `status`, `wait`, `start`, `replace`, `stop`, and `workers` operate from
  Mongo state plus RabbitMQ queue state.
- Detached workers survive the parent CLI process exiting.
- Terminal completion can be collected for detached runs.
- Scaling up, replacing, stopping, killing, and restarting workers leave
  inspectable Mongo worker records.
- Completed manual runs have zero active `running` or `stop_requested` workers.
- New manual runs do not create `.runs/<run_id>` filesystem state.

## Environment

- Started the local dependencies with `docker compose up -d`.
- Verified RabbitMQ connectivity through `open_connection()`.
- Used the local Mongo database at `mongodb://localhost:27017/dr_queues`.

## In-Process Baseline

What tested:

- Ran `dr-queues-demo` for `manual-20260622-inproc` with 6 expected terminal
  jobs.

Why:

- Establish a baseline that the Mongo-backed manifest, seed batch, event
  persistence, and queue flow still work in the simplest execution mode.

What happened:

- The run completed with `events=42 terminals=6`.
- `dr-queues-run status` reported all three stages at `6/6` with empty queues.
- Mongo contained one manifest, one seed batch, 42 events, and 6 terminal
  events for the run.
- No `.runs/manual-20260622-inproc` path was created.

Actions taken:

- None.

## Detached Startup And Seeding

What tested:

- Created `manual-20260622-detached` in Mongo with 12 expected jobs.
- Started one detached worker process for each stage before seeding.
- Seeded the run after workers were already running.

Why:

- Verify detached worker orchestration is not dependent on process-local state,
  startup order, or filesystem persistence.

What happened:

- The first detached worker subprocesses exited after the parent CLI exited.
- Mongo eventually marked those first worker records `stale`.
- After restarting workers, all stages reached `12/12`, but final outputs
  remained in the final RabbitMQ queue and terminal events stayed at `0/12`.

Actions taken:

- Updated detached worker startup to detach stdio and start a new session with
  `stdin`, `stdout`, and `stderr` redirected to `subprocess.DEVNULL`.
- Updated terminal waits to start a `TerminalTap` when waiting for terminal
  completion on detached runs.
- Moved the `TerminalTap` import into the terminal-wait branch to avoid a
  runtime circular import.
- Added an error line when `dr-queues-run wait --target terminal` times out
  before terminal completion.

Retest result:

- `dr-queues-run wait --run-id manual-20260622-detached --target terminal`
  completed with `terminals=12/12`.
- Final status showed all queues empty.

## Stage Wait

What tested:

- Ran `dr-queues-run wait --run-id manual-20260622-detached --target slow`.

Why:

- Verify stage-specific waits still work after terminal wait starts a tap only
  for terminal completion.

What happened:

- The command exited successfully for the already-complete `slow` stage.

Actions taken:

- None.

## Replace, Scale, And Stop

What tested:

- Replaced the `transform` stage on `manual-20260622-detached` with a new
  worker process configured for two worker threads.
- Added a second `slow` worker process configured for two worker threads.
- Stopped all active workers.

Why:

- Verify manual scale-up/down operations are reflected in Mongo and affect only
  live worker processes.

What happened:

- `replace` stopped the active old `transform` record and started a new running
  `transform` record.
- `start` added a second running `slow` record.
- `status` originally counted stale and stopped records as `workers`, which made
  active capacity look higher than it was.
- `stop` requested exactly the four active worker records and all live PIDs
  exited.

Actions taken:

- Updated `status` output to show active worker count separately from total
  persisted worker records: `workers=<active> records=<total>`.
- Updated stop requests to target only `running` records, leaving `stale`
  records as stale history instead of converting them to `stop_requested`.

Retest result:

- Final `manual-20260622-detached` status showed `workers=0` for every stage.
- Mongo worker records were 3 `stale` and 5 `stopped`.

## Kill And Restart

What tested:

- Created `manual-20260622-kill` with 400 expected jobs.
- Started one worker process per stage.
- Killed the `slow` worker with `SIGKILL` while 324 jobs remained queued.
- Waited past the 30 second stale heartbeat threshold.
- Started replacement `slow` capacity and waited for terminal completion.
- Stopped remaining live workers.

Why:

- Verify crash recovery behavior: killed workers become stale, the run remains
  queryable, replacement workers can continue from RabbitMQ queues, and final
  terminal completion is captured in Mongo.

What happened:

- Immediately after `SIGKILL`, the OS process was gone but Mongo still showed
  the last known worker status as `running`.
- After the stale threshold, `workers` showed the killed `slow` worker as
  `stale`, and `status` showed `slow workers=0 records=1`.
- Replacement `slow` capacity completed the remaining backlog.
- `wait --target terminal --timeout 120` returned `terminals=400/400`.
- Final status showed all stages at `400/400`, all queues empty, and no active
  workers after `stop`.
- Mongo contained one manifest, one seed batch, 2801 events, 400 terminal
  events, and four worker records for the kill run.

Actions taken:

- None beyond the fixes already listed above.

## Duplicate Seed Guard

What tested:

- Attempted to seed `manual-20260622-kill` a second time without `force=True`.

Why:

- Verify Mongo seed-batch state prevents accidental duplicate publishing.

What happened:

- The second seed attempt failed with
  `DuplicateSeedError Run 'manual-20260622-kill' already has a seed batch.`

Actions taken:

- None.

## Filesystem Persistence Check

What tested:

- Checked for `.runs/manual-20260622-inproc`,
  `.runs/manual-20260622-detached`, and `.runs/manual-20260622-kill`.

Why:

- Verify new runtime behavior does not recreate the removed `.runs` persistence
  path.

What happened:

- None of those manual run paths existed.
- The working tree initially contained pre-existing `.runs/*` local artifacts
  from earlier runs.

Actions taken:

- Deleted the pre-existing `.runs` directory at the user's request.
- Verified there are no `.runs*` paths left under the repository root.

## Post-Test Cleanup

What tested:

- Removed local manual test artifacts for run ids matching
  `manual-20260622-*`.

Why:

- Leave the local development environment clean after recording the manual test
  evidence in this document.

What happened:

- Deleted 3 run manifests, 3 seed batches, 2927 pipeline events, and 12 worker
  records from Mongo.
- Deleted the RabbitMQ queues from the persisted manual run manifests.
- Verified Mongo has zero `manual-20260622-*` manifests, seed batches, events,
  or worker records.
- Verified there are no `.runs*` paths under the repository root.

Actions taken:

- None beyond the cleanup described above.
