# Changelog

All notable changes to Bulklink will be documented in this file.

## Unreleased

### Fixed

- Interval comparisons now verify an opaque bulkhead instance identity and a strictly
  increasing snapshot sequence, rejecting same-label cross-instance comparisons and
  reversed snapshots even when cumulative counters are unchanged.

## 0.4.0 - 2026-06-08

### Added

- `BulkheadStatus.since(previous)` for immutable interval metrics calculated from two
  snapshots without resetting counters or creating background work.
- `BulkheadInterval` with interval admissions, queue outcomes, rejections, completions,
  wait totals, derived counts, and access to both endpoint snapshots.
- Executable interval-metrics example and installed-wheel contract verification.

### Changed

- Promoted the documented compatibility contract to the `0.4.x` line.

## 0.3.0 - 2026-06-08

### Added

- `slot_before(deadline)` and `execute_before(deadline, operation, ...)` for admission
  bounded by an absolute `asyncio` event-loop deadline.
- `BulkheadStatus.expired_before_queue_total` for attempts rejected because their
  absolute deadline had already elapsed before queue entry.

### Changed

- `BulkheadStatus.rejected_total` now includes pre-queue deadline expirations.
- Installed-wheel release verification now exercises absolute-deadline admission.

## 0.2.0 - 2026-06-08

### Changed

- Promoted the `0.2.0rc1` behavior and public API to the stable `0.2.0` release.
- Declared the documented `0.2.x` root exports, enum values, immutable record fields,
  exception hierarchy, and primary call signatures as patch-release compatibility contracts.

### Added

- Installed-wheel verification of the stable public contract.
- Contract tests that reject accidental public API drift before packaging or publication.

### Security

- Kept release publication restricted to verified artifacts and PyPI Trusted Publishing.

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
- Deterministic model-oriented admission, resize, cancellation, and shutdown tests.
- Cross-platform validation on Windows and macOS.
- Executable examples and dependency-free performance and waiter-memory benchmarks.

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
