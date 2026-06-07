# Changelog

All notable changes to Bulklink will be documented in this file.

## Unreleased

### Added

- `slot_now()` and `execute_now()` for admission without waiting-room entry.
- `slot_within()` and `execute_within()` for shorter per-call queue wait limits.
- `wait_closed()` and `close_and_wait()` for graceful asynchronous draining.
- Immutable lifecycle events with synchronous isolated handlers.
- Immutable capacity reports with conservative diagnostic findings.
- Dynamic execution capacity with FIFO-safe `resize()`.
- Optional named bulkhead registry with collective lifecycle management.
- Exact admission and waiting-room accounting metrics.

### Changed

- Split closed-state and cancellation metrics by lifecycle stage.
- Hardened slot context lifecycle against concurrent reuse during admission and release.
- Added official CI coverage for Python 3.10 through 3.14.
- Modernized package license metadata to the SPDX-based packaging standard.
- Added clean-wheel installation, runtime smoke, archive safety, and consumer typing checks.

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
