# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-06-22

### Changed

- Simplified low-risk pipeline primitives and demo sink selection.
- Moved `filter_run_events` into the events module and removed the empty analysis package.

### Removed

- Removed unused worker command formatter, dict event filter, and empty notebook template.

## [0.1.0] - 2026-06-21

### Added

- RabbitMQ multi-stage pipeline runtime (`WorkerPool`, `TerminalTap`, queue chaining)
- Slim `JobEnvelope` for job state on the wire
- `PipelineDefinition`, `HandlerRegistry`, and `Pipeline` workflow engine
- Append-only event sinks: `MongoEventSink` (default), `AmqpEventSink`, `MemoryEventSink`
- Run manifest for multi-process worker coordination
- `filter_run_events` analysis helper
- Console entry points: `dr-queues-demo`, `dr-queues-stage-worker`
- Reference dummy handlers in `dr_queues.demo_handlers`

[0.1.1]: https://github.com/danielle-rothermel/dr-queues/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/danielle-rothermel/dr-queues/releases/tag/v0.1.0
