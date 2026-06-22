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

## Duplicate Job Guard

What tested:

- Attempted to seed `manual-20260622-kill` again with a job ID already present
  in an active seed batch.

Why:

- Verify Mongo seed-batch state prevents accidental duplicate job publishing
  while still allowing additional seed batches for new jobs.

What happened:

- The duplicate job attempt failed with `DuplicateJobError`.

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

## Target-Aware Failure Controls

Completion criteria for this pass:

- A default, non-targeted run still completes using the default partition.
- Workers started with a target selector only process matching pending jobs.
- A selector that matches no pending or known jobs exits nonzero and does not
  report a fake started worker.
- Target holds persist affected jobs as `held`, keep them out of normal worker
  processing, and allow manual replay after the hold clears.
- Failed attempts persist in `job_attempts`, retryable failures can be replayed,
  retry exhaustion moves the job to `dead_lettered`, and a dead-lettered job can
  be manually replayed with a fixed handler.
- All manual workers for these runs are stopped at the end.

### Default Partition Regression

What tested:

- Ran `dr-queues-demo` for `manual-20260622-baseline` with one expected job.

Why:

- Verify the target-aware changes did not break the original default-partition
  path for jobs without target tags.

What happened:

- The run completed with `events=7 terminals=1`.
- `dr-queues-run status` reported `terminals=1/1`.
- Job state counts showed `completed=3 terminal=1`.

Actions taken:

- None.

### Selector-Scoped Workers

What tested:

- Created `manual-20260622-targets` with four jobs: two tagged
  `provider=openai` and two tagged `provider=gemini`.
- Started `classify` and `finalize` workers with `--include provider=openai`.
- Waited briefly for terminal completion.

Why:

- Verify queue workers can target only a subset of pending jobs, which supports
  continuing on one provider while another provider is paused or rate-limited.

What happened:

- The terminal wait intentionally exited nonzero because only the OpenAI subset
  was eligible for those workers.
- Status reported `terminals=2/4`.
- Job states showed the two OpenAI jobs completed and terminal, while the two
  Gemini jobs remained pending in their partition.

Actions taken:

- None for the selector-scoped processing path.

### No-Match Worker Start

What tested:

- Tried to start a `classify` worker on `manual-20260622-targets` with
  `--include provider=anthropic`, which matched no run partitions.

Why:

- Verify operator mistakes or temporarily empty target selections fail clearly
  instead of creating misleading worker records.

What happened:

- The first attempt printed `started pid=...` and exited zero, but the child
  process exited shortly afterward and no worker record was created.

Actions taken:

- Added parent-side partition preflight in `start_stage_workers`, so selector
  combinations that match no known partitions fail before spawning a worker.
- Kept the immediate child-exit check for other early startup failures.
- Added focused regression tests for successful starts, no-match selector
  starts, and immediate child exits.

Retest result:

- The no-match command now prints
  `No matching partitions for run_id='manual-20260622-targets' stage='classify' and selectors.`
  and exits with code `1`.

### Target Holds And Replay

What tested:

- Created `manual-20260622-holds` with one OpenAI job and two Gemini jobs.
- Set a hold on `quota_pool=gemini-flash`.
- Started a Gemini `classify` worker while the hold was active.
- Cleared the hold, replayed held Gemini jobs, started matching downstream
  workers, and started OpenAI workers for the remaining job.

Why:

- Verify rate-limit style holds persist blocked target work and allow manual
  recovery without dropping or silently requeuing jobs.

What happened:

- While held, `failures` listed both Gemini jobs as `status=held` with
  `attempts=0`.
- Status showed `pending=1 held=2`, with the OpenAI job still pending.
- After clearing the hold and replaying, the run completed with
  `terminals=3/3`.
- Final job states showed `completed=6 terminal=3`.

Actions taken:

- None.

### Retry, Dead Letter, And Manual Recovery

What tested:

- Created `manual-20260622-failures` with one Gemini job and a handler that
  raises `RuntimeError("manual rate limit")`.
- Replayed the `retry_waiting` job twice to exhaust the default retry budget.
- Stopped the failing worker, started a successful handler, replayed the
  `dead_lettered` job, and waited for terminal completion.

Why:

- Verify failures are durably inspectable, retries are explicit operator
  actions, retry exhaustion is visible, and manual recovery can reuse the same
  persisted job state.

What happened:

- After the first failure, `attempts` showed attempt `1` with action
  `retry_waiting`, and `failures` showed the job in `retry_waiting`.
- After two more replays, `attempts` showed three records: attempts `1` and `2`
  as `retry_waiting`, and attempt `3` as `dead_lettered`.
- `failures` showed the job as `dead_lettered` with `attempts=3`.
- After switching to the successful handler and replaying dead-lettered work,
  `wait --target terminal` returned `terminals=1/1`.
- The historical failed attempt records remained visible after successful
  recovery.

Actions taken:

- None.

## Post-Test Cleanup

What tested:

- Removed local manual test artifacts for run ids matching
  `manual-20260622-*`.

Why:

- Leave the local development environment clean after recording the manual test
  evidence in this document.

What happened:

- Deleted 4 run manifests, 4 seed batches, 38 pipeline events, 8 worker
  records, 23 job states, 3 job attempts, and 1 target hold from Mongo for
  the target-aware manual runs.
- Deleted the RabbitMQ queues from the persisted manual run manifests.
- Verified Mongo has zero `manual-20260622-*` manifests, seed batches, events,
  worker records, job states, job attempts, or target holds.
- Verified there are no `.runs*` paths under the repository root.

Actions taken:

- None beyond the cleanup described above.
