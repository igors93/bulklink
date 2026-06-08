# Changelog

All notable changes to Bulklink will be documented in this file.

## Unreleased

### Fixed

- `PartitionedBulkhead` eviction no longer races: the task that selects and removes
  an idle victim holds a logical reservation slot, preventing any other task from
  claiming the freed capacity before the replacement partition is created.
- Reservation rollback under repeated cancellation is now safe: the rollback acquires
  the manager lock inside `complete_cleanup()`, so a second `cancel()` arriving while
  the rollback waits for the lock cannot leave `_reserved_slots` permanently elevated.
- `close_and_wait()` now waits for all pending eviction reservations before signalling
  drain; previously the manager could declare shutdown complete while a mid-eviction
  replacement was still pending.
- `close_and_wait()` now waits for `cleanup_idle()` and `discard()` child closures
  that were already in progress; previously the manager could report fully closed while
  removed children were still tearing down outside the partition map.
- `slot_now()` and `execute_now()` no longer block on victim closure when the
  partition limit is reached with an idle-but-evictable partition; they now raise
  `PartitionLimitError` immediately without modifying the partition map.
- Admission deadlines (`slot_within`, `slot_before`, configured `wait_limit`) now
  cover the full admission path — manager resolution and eviction time are deducted
  from the caller's budget before the child bulkhead sees the remaining limit.  When a
  deadline expires during victim closure, the caller receives `BulkheadQueueTimeoutError`
  immediately; the victim close continues in the background under manager ownership and
  is awaited by the drain signal.
- `PartitionLimitError` message no longer attributes the rejection solely to active
  partitions; the wording is neutral so that rejections caused by pending eviction
  reservations are not described as "0 active partitions."
- `discard()` and `cleanup_idle()` now raise `BulkheadClosedError` when called after
  `close()` has been initiated, preventing maintenance operations from being registered
  after the manager has entered the CLOSING lifecycle state.
- Multiple concurrent `close()` calls are now idempotent; the manager enters CLOSING
  only once and subsequent calls return without duplicating child close operations.

### Changed

- `PartitionCoordinator` now tracks all pending operations (evictions, idle cleanup,
  discard, and shutdown-child closes) in a single `_pending_ops` dictionary, replacing
  the previous pair of anonymous counters (`_reserved_slots`, `_pending_child_closures`).
  Every pending op has exactly one owner, is released exactly once, and participates in
  drain accounting.
- The manager lifecycle is now modeled as an explicit three-state enum (`OPEN`,
  `CLOSING`, `CLOSED`) rather than a boolean flag, making impossible states
  unrepresentable and preventing late maintenance registration during shutdown.
- Victim close during eviction is now bounded by the caller's admission deadline.
  The caller receives `BulkheadQueueTimeoutError` at the deadline; the victim close
  task continues under manager ownership so capacity is correctly restored.

### Security

- GitHub Actions workflow steps are now pinned to verified commit SHAs rather than
  mutable version tags, preventing tag-redirect supply-chain attacks.

## 0.6.0 - 2026-06-08

### Added

- `PartitionedBulkhead` for bounded per-key isolation with independent FIFO concurrency,
  relative limits, absolute deadlines, cancellation safety, and graceful shutdown.
- Hard `max_partitions` cardinality protection, least-recently-used idle reclamation under
  pressure, explicit idle cleanup, and safe idle-only discard.
- Immutable partition-manager status and interval metrics that never expose partition keys.
- Executable partition-isolation example, adversarial stress coverage, typing checks, and
  installed-wheel release verification.

### Security

- Partition keys are never included in public errors, manager status, or internal child
  labels, and retained keys are released after graceful shutdown.

### Changed

- Promoted the documented compatibility contract to the `0.6.x` line.

## 0.5.0 - 2026-06-08

### Added

- `WeightedBulkhead` for strict-FIFO admission where each operation consumes a positive
  integer capacity cost.
- Weighted status, interval metrics, lifecycle events, saturation errors, deadlines,
  cancellation-safe handoff, resizing, and graceful shutdown.
- Executable weighted-capacity example, stress coverage, typing checks, and installed-wheel
  release verification.

### Fixed

- Interval comparisons now verify an opaque bulkhead instance identity and a strictly
  increasing snapshot sequence, rejecting same-label cross-instance comparisons and
  reversed snapshots even when cumulative counters are unchanged.

### Changed

- Promoted the documented compatibility contract to the `0.5.x` line.

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
