# dr-queues Context

dr-queues is a domain-free runtime for moving jobs through staged pipelines while recording operational state.

## Language

**Run**:
A single execution of a pipeline over submitted jobs.
_Avoid_: execution, workflow run

**Pipeline**:
An ordered set of stages that jobs pass through.
_Avoid_: workflow, graph

**Stage**:
One ordered step in a pipeline where workers process jobs before eligible work moves onward.
_Avoid_: phase, layer

**Stage execution**:
The processing of one job at one stage, including whether the job runs, waits, is held, fails, completes, or reaches terminal state.
_Avoid_: handler call, worker event

**Stage eligibility**:
The condition that a job may be queued for workers at a stage, either from initial seed work or from replay after a held, failed, retry-waiting, or dead-lettered state.
_Avoid_: enqueue helper, intake

**Job state**:
The latest operational position of a job for a run and stage.
_Avoid_: status row, progress record

**Target tags**:
Generic key-value metadata used to select or partition jobs.
_Avoid_: provider fields, labels

**Target hold**:
An operator-controlled pause for jobs whose target tags match selected values.
_Avoid_: block, pause flag

**Attempt**:
A durable record of a stage execution failure and the selected retry or dead-letter action.
_Avoid_: retry log, error event
