# Changelog

All notable changes to Bulklink will be documented in this file.

## Unreleased

No changes yet.

## 0.2.0rc1 - 2026-06-07

### Added

- `slot_now()` and `execute_now()` for admission without waiting-room entry.
- `slot_within()` and `execute_within()` for shorter per-call queue wait limits.
- `wait_closed()` and `close_and_wait()` for graceful asynchronous draining.
- Immutable lifecycle events with synchronous isolated handlers.
- Immutable capacity reports with conservative diagnostic findings.
- Dynamic execution capacity with FIFO-safe `resize()`.
- Optional named bulkhead registry with collective lifecycle management.
- Exact admission and waiting-room accounting metrics.
- Release verification for wheel contents, clean installation, runtime behavior, and
  consumer-facing typing.
- CI coverage for Python 3.10 through 3.14.

### Changed

- Split closed-state and cancellation metrics by lifecycle stage.
- Hardened slot context lifecycle against concurrent reuse during admission and release.
- Sanitized collective registry failure metadata and removed original exception chaining.
- Modernized package metadata to use the SPDX license expression.

## 0.1.0 - 2026-06-07

### Added

- `AsyncBulkhead` with bounded parallel execution.
- Bounded FIFO waiting room.
- Optional waiting deadline.
- Immediate saturation rejection.
- Cancellation-safe slot handoff.
- Graceful `close()` behavior.
- `slot()`, `execute()`, decorator, and `status()` APIs.
- Unit, contract, integration, race-oriented, and stress tests.
- Documentation for architecture and Relinker composition.
