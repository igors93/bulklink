# Changelog

All notable changes to Bulklink will be documented in this file.

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
