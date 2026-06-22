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

## Runtime Observability And Append Retest

Completion criteria for this pass:

- In-process and detached executions both leave worker records visible to
  `dr-queues-run workers` and the viewer snapshot API.
- Active worker counts distinguish currently running workers from historical
  stopped worker records.
- A run can start with zero expected jobs, receive an initial seed batch, reach
  completion, receive an additional seed batch under the same run ID, and update
  expected and terminal counts from seed-batch state.
- Duplicate job IDs are rejected without changing the run's expected count.
- All manually started detached workers are stopped at the end.

### Environment Note

What tested:

- Tried to start local dependencies with `docker compose up -d`.
- Reused the available local RabbitMQ and MongoDB services after checking the
  port conflict.

Why:

- Verify the manual run used real queue and store services while avoiding
  disruption to another local stack already bound to RabbitMQ ports.

What happened:

- The repo Mongo container was already running on port 27017.
- RabbitMQ port 5672 was already owned by
  `dr-bottleneckqueues-rabbitmq-1`, so the repo RabbitMQ container could not
  bind the same host port.
- The existing RabbitMQ service accepted connections, and Mongo ping succeeded.

Actions taken:

- Used the existing localhost RabbitMQ plus the running repo MongoDB for this
  manual pass.

### In-Process Worker Parity

What tested:

- Ran `dr-queues-demo` for
  `manual-20260622-runtime-v2-inproc-7df18d` with 6 terminal jobs and one
  worker for each stage.
- Queried `dr-queues-run status`, `dr-queues-run workers`, and the viewer
  snapshot API.

Why:

- Verify that in-process demo workers now write the same runtime worker records
  that detached workers write, so the dashboard does not show an empty Workers
  panel for a real in-process run.

What happened:

- The demo completed with `events=42 terminals=6`.
- `status` reported `terminals=6/6`; all three stages were `6/6` with empty
  queues.
- `workers` reported one stopped `runtime=in_process` worker for each stage,
  all with `concurrency=1`.
- The viewer snapshot API returned HTTP 200 with `expected_jobs=6`,
  `terminal_jobs=6`, and three stopped in-process worker records.

Actions taken:

- None.

### Detached Empty Start And First Seed

What tested:

- Initialized `manual-20260622-runtime-v2-detached-7df18d` from a manifest
  without seeding any jobs.
- Started one detached worker for each stage before adding input work.
- Seeded a first batch of 4 jobs and waited for terminal completion.

Why:

- Verify that worker observability does not depend on seeding order and that a
  run can intentionally exist at `0/0` before input work is added.

What happened:

- Initial status after `init` was `terminals=0/0`, with zero worker records.
- After starting detached workers, status stayed `0/0` but each stage showed
  `worker_records=1/1` and `worker_concurrency=1`.
- `workers` reported one running `runtime=detached` worker for each stage.
- Seeding the first batch reported `seeded=4`.
- `wait --target terminal` completed with `terminals=4/4`.
- Status reported `expected_jobs=4`, `terminal_jobs=4`, all three stages
  `4/4`, and active detached worker records.

Actions taken:

- None.

### Append After Completed Idle Run

What tested:

- Stopped the first detached worker set after the run reached `4/4`.
- Seeded a second batch of 2 new jobs into the same run ID.
- Restarted detached workers and waited for terminal completion.

Why:

- Verify that appending work is a first-class seed-batch operation and that
  completion ratios update from cumulative seed batches instead of a fixed
  manifest field.

What happened:

- After stopping workers, status stayed `terminals=4/4` with no active worker
  concurrency and one stopped worker record per stage.
- Seeding the second batch reported `seeded=2`.
- Status moved to `terminals=4/6`, with 2 pending jobs in the first stage input
  queue and `worker_records=0/1` for every stage.
- Restarting one detached worker per stage drained the appended work.
- `wait --target terminal` completed with `terminals=6/6`.
- Final status reported all stages `6/6`, empty queues, no active worker
  concurrency, and two stopped detached worker records per stage.

Actions taken:

- None.

### Duplicate Seed Guard

What tested:

- Attempted to seed the detached run again with the first batch's job IDs.

Why:

- Verify append support does not allow duplicate job publication or inflate the
  derived expected count.

What happened:

- The duplicate seed command exited 1 with `DuplicateJobError`.
- The error named the conflicting job IDs from the first batch.
- A follow-up status check stayed at `terminals=6/6`; expected jobs did not
  increase.

Actions taken:

- None.

### Viewer Snapshot API

What tested:

- Queried `/api/runs/{run_id}/snapshot` through `fastapi.testclient.TestClient`
  for both the in-process and detached retest runs.

Why:

- Verify the dashboard data source sees the same worker parity and derived
  expected-job counts as the CLI status commands.

What happened:

- The in-process snapshot returned HTTP 200 with `expected_jobs=6`,
  `terminal_jobs=6`, and three stopped `runtime=in_process` worker records.
- The detached snapshot returned HTTP 200 with `expected_jobs=6`,
  `terminal_jobs=6`, and six stopped `runtime=detached` worker records.
- Each stage in both snapshots reported expected and completed counts of 6.

Actions taken:

- None.

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

## Stage Execution Runtime Seam

Completion criteria for this pass:

- Normal in-process execution reaches the expected terminal count and leaves
  empty queues.
- Held work is persisted as `held`, acknowledged out of the hot queue path,
  and can complete after hold clear plus replay.
- Handler failures create durable attempts and latest job state before the
  delivery is acknowledged.
- A store that cannot record failures causes the worker to nack and requeue.
- Terminal tap recording still writes terminal events and terminal job state.
- All detached manual workers are stopped at the end.

### Environment And Fixtures

What tested:

- Started local RabbitMQ and MongoDB with `docker compose up -d`.
- Used ignored helper files under `.cache/manual-stage-execution/` for a
  two-stage `classify -> finalize` pipeline and custom manual handlers.
- Used run IDs prefixed with `manual-20260622-stage-exec-*`.

Why:

- Verify the new `StageExecution` abstraction against real local runtime
  services while keeping reusable scratch fixtures out of the tracked tree.

What happened:

- MongoDB was already running.
- RabbitMQ started successfully.
- The helper handler module raised `RuntimeError("manual rate limit")` only
  when `DR_QUEUES_MANUAL_FAIL_CLASSIFY=1` and the job target tag was
  `quota_pool=openai-nano`.

Actions taken:

- None.

### In-Process Happy Path

What tested:

- Ran `dr-queues-demo` for `manual-20260622-stage-exec-happy-094136` with
  4 expected jobs and one worker for each demo stage.
- Checked `status`, `failures`, `attempts`, and terminal event counts.

Why:

- Verify `StageExecution.run` still records started/output events, marks jobs
  running and completed, forwards successful jobs, and lets the terminal tap
  record completion in the normal Mongo-backed path.

What happened:

- The demo completed with `events=28 terminals=4`.
- `status` reported `terminals=4/4`, `completed=12 terminal=4`, and empty
  input/output queues for all three stages.
- `failures` and `attempts` returned no rows.

Actions taken:

- None.

### Target Hold And Replay

What tested:

- Created `manual-20260622-stage-exec-hold-094136` with one control job and
  one job tagged `quota_pool=gemini-flash`.
- Set a hold on `quota_pool=gemini-flash`.
- Started detached `finalize` and `classify` workers with the manual handler
  module.
- Waited for terminal completion before and after clearing the hold and
  replaying the held job.

Why:

- Verify `StageExecution` checks holds before calling the handler, persists the
  held state, acknowledges the held delivery, and does not forward held work
  until an operator clears and replays it.

What happened:

- An initial parallel setup attempt launched `seed` before `init` had finished,
  so `seed` exited with `RunNotFoundError`.
- After rerunning `seed` sequentially, the run behaved as expected.
- Before clearing the hold, `wait --target terminal --timeout 5` exited
  nonzero with `terminals=1/2`.
- `status` reported `completed=2 held=1 terminal=1`; both stage queues were
  empty.
- `failures` listed `hold-gemini-1` as `status=held`, `attempts=0`, and
  `detail=None`.
- `attempts` returned no rows.
- After `holds clear` and `replay --status held --force`, the run completed
  with `terminals=2/2` and final job states `completed=4 terminal=2`.
- Terminal event counting reported `events=10 terminals=2`.
- The detached workers were stopped; `workers` showed both records as
  `status=stopped`.

Actions taken:

- Reran the seed step sequentially after the manual setup race.
- No code changes were needed.

### Recorded Failure And Recovery

What tested:

- Created `manual-20260622-stage-exec-failure-094136` with one job tagged
  `quota_pool=openai-nano`.
- Started a detached `classify` worker with
  `DR_QUEUES_MANUAL_FAIL_CLASSIFY=1`.
- Replayed the `retry_waiting` job twice to exhaust the default retry budget.
- Stopped the failing worker, started successful `classify` and `finalize`
  workers, replayed the `dead_lettered` job, and waited for terminal
  completion.

Why:

- Verify handler exceptions go through durable failure recording before ack,
  retry/dead-letter state is inspectable, and a fixed handler can recover the
  same persisted job through manual replay.

What happened:

- After the first failure, `failures` listed `failure-openai-1` as
  `status=retry_waiting`, `attempts=1`, and `detail=manual rate limit`.
- `status` reported `terminals=0/1`, `retry_waiting=1`, and zero queue depth,
  showing the failed delivery was acknowledged after persistence.
- After two forced replays, `attempts` showed attempt `1` and `2` as
  `retry_waiting`, and attempt `3` as `dead_lettered`.
- `failures` showed the job as `dead_lettered` with `attempts=3`.
- After restarting successful workers and replaying the dead-lettered job,
  `wait --target terminal` returned `terminals=1/1`.
- Final `status` reported `completed=2 terminal=1` and empty queues.
- Historical failed attempts remained visible after successful recovery.
- Terminal event counting reported `events=8 terminals=1`.
- All three detached worker records for the run ended as `status=stopped`.

Actions taken:

- None.

### Failure Persistence Fallback Redelivery

What tested:

- Ran a small `uv run python` probe that constructed a `WorkerPool` with
  `MemoryEventSink`, a failing handler, and fake channel/method objects.
- Called `_on_message` directly with one `JobEnvelope`.

Why:

- Verify the `EventSinkStageExecutionStore` adapter returns the
  failure-not-recorded sentinel for sinks that cannot persist attempts, causing
  the worker to nack and requeue instead of acknowledging an unrecorded
  failure.

What happened:

- The probe printed `acked=[]`.
- The probe printed `nacked=[(7, True)]`.
- The memory sink contained only `STAGE_STARTED`.

Actions taken:

- None.
