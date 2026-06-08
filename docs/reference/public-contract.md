# Stable public contract

This document records the compatibility surface promoted with Bulklink `0.6.0`. The
repository enforces the same contract with automated tests and installed-wheel release
verification.

## Root exports

The supported import surface is exactly the ordered list exposed by
`bulklink.__all__`. Applications should import public objects from `bulklink`, not from
`bulklink._internal`.

## Enum values

Public enum string values are machine-readable contracts. Dashboards, logs, and stored
telemetry may depend on them, so patch releases must not rename or repurpose them.

The protected enums are:

- `BulkheadEventKind`;
- `CapacityFindingCode`;
- `CapacitySeverity`.

## Immutable records

The fields of these frozen dataclasses are protected for `0.6.x` patch compatibility:

- `BulkheadStatus`;
- `BulkheadInterval`;
- `BulkheadEvent`;
- `CapacityFinding`;
- `CapacityReport`;
- `BulkheadRegistryFailure`;
- `PartitionedBulkheadStatus`;
- `PartitionedBulkheadInterval`;
- `WeightedBulkheadStatus`;
- `WeightedBulkheadInterval`;
- `WeightedBulkheadEvent`.

Patch releases may fix the values produced by these records, but must not silently
remove, rename, reorder, or change the meaning of their documented fields.

## Exceptions

All Bulklink-generated operational errors inherit from `BulklinkError`.
`BulkheadQueueTimeoutError` deliberately does not inherit from `TimeoutError`, preventing
generic network-timeout retry policies from treating local overload as a remote timeout.
`WeightedBulkheadSaturatedError` is the weighted immediate/full-queue overload signal.
`PartitionLimitError` reports bounded partition-cardinality exhaustion. Both inherit from
`BulklinkError` and neither exposes operation or partition-key data.

## Calling conventions

The constructor and primary methods preserve their documented positional-only,
keyword-only, variadic, and regular parameters. This includes admission methods,
relative and absolute per-call limits, resize, shutdown, and registry creation.

`slot_before(deadline)` and `execute_before(deadline, ...)` use the owning event loop's
monotonic clock. They constrain admission only and do not cancel a protected operation
after it has started.

## Evolution before 1.0

The `0.6.x` patch line is stable. A later minor release may add or intentionally revise
pre-1.0 APIs, but changes must be explicit, tested, and documented. Runtime dependencies
remain zero unless a future design review demonstrates a compelling need.


## Interval comparison

`BulkheadStatus.since(previous)` is a pure comparison. It does not reset metrics or mutate
either snapshot. Every status contains an opaque `instance_id` and a strictly increasing
`snapshot_index`; these fields establish instance identity and chronology without storing
operation data. The result contains nonnegative counter changes and both endpoint statuses.
Invalid chronology or incompatible snapshots raise `ValueError`.


## Weighted admission

`WeightedBulkhead` uses positive integer capacity and costs. FIFO order is strict and does
not allow smaller requests to overtake. Reducing capacity below active usage is allowed and
drains naturally; reducing it below the largest queued cost is rejected. Weighted status,
interval, and event records contain only capacity and timing metadata.


## Partitioned admission

`PartitionedBulkhead` owns a bounded set of lazily created `AsyncBulkhead` children. Keys
are hashable application values but are never present in public status, errors, or child
labels. `max_partitions` is a hard limit. Only idle partitions may be reclaimed, using LRU
selection under pressure or explicit idle cleanup. No permanent cleanup task is created.

Admission deadlines bound the caller's total wait across both manager resolution (including
any eviction victim close) and child admission. When a deadline expires during victim close,
the caller receives `BulkheadQueueTimeoutError` immediately; the victim close continues in
the background and is awaited by `close_and_wait()`. Protected user code is never started
after the deadline.

`discard()` and `cleanup_idle()` raise `BulkheadClosedError` once `close()` has been
initiated. `close()` is idempotent. Multiple `wait_closed()` callers are independent;
cancelling one does not affect the lifecycle or other waiters.
