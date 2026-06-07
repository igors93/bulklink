# Changelog

All notable changes to Bulklink will be documented in this file.

## Unreleased

### Added

- `slot_now()` and `execute_now()` for admission without waiting-room entry.
- `slot_within()` and `execute_within()` for shorter per-call queue wait limits.
- Exact admission and waiting-room accounting metrics.

### Changed

- Split closed-state and cancellation metrics by lifecycle stage.
- Hardened slot context lifecycle against concurrent reuse during admission and release.

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
