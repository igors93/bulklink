# Partitioned isolation

`PartitionedBulkhead` gives each hashable key an independent `AsyncBulkhead` while placing a
hard upper bound on how many partitions may be retained.

```python
from bulklink import PartitionedBulkhead

customers = PartitionedBulkhead(
    label="customers",
    parallelism=3,
    waiting_room=10,
    wait_limit=1.0,
    max_partitions=1_000,
    idle_timeout=300.0,
)

result = await customers.execute(customer_id, call_remote_service)
```

A noisy customer can fill only its own execution capacity and FIFO waiting room. Other
customer keys continue using their own partitions.

## Cardinality protection

`max_partitions` is mandatory. When a new key arrives at the logical capacity limit,
Bulklink removes the least-recently-used idle partition. A new key is rejected with
`PartitionLimitError` when no logical capacity is available — this happens when every
partition is occupied by active callers, when a pending eviction replacement already
holds the freed slot, or when any other supported transient condition fills the logical
limit.

The rejection does not include the partition key.

## Idle lifecycle

Bulklink creates no cleanup thread or background task. Idle partitions are reclaimed in
three explicit ways:

- automatically under partition-limit pressure;
- `await manager.cleanup_idle()` after `idle_timeout`;
- `await manager.discard(key)` when one known partition is idle.

`discard()` returns `False` when the key is missing or currently in use. Active and waiting
operations are never removed to make room.

## Key privacy

Keys are retained only while their partitions exist. They are not copied into child labels,
public errors, manager status records, or interval metrics. `close_and_wait()` drains all
children and releases the retained key mapping.

Keys must be hashable and have stable equality and hash behavior while retained.

## Admission behavior

The selected child uses the normal `AsyncBulkhead` guarantees:

- bounded parallel execution;
- bounded FIFO waiting;
- immediate admission;
- relative and absolute admission limits;
- cancellation-safe cleanup;
- graceful shutdown.

Deadlines control admission only and never cancel work after it starts.

## Admission budget and eviction

The admission deadline (`slot_before`, `execute_before`, `wait_limit`, `slot_within`)
covers the full admission path: both the time spent waiting for manager-level eviction
and the time spent waiting for the child bulkhead to grant a slot.

When a victim partition must be closed to make room:

- The victim close is started as a background task.
- The caller waits for it under the deadline.
- If the deadline expires before the close finishes, the caller receives
  `BulkheadQueueTimeoutError` immediately — the close task continues in the background
  under manager ownership.
- When the background task finishes, the reserved capacity slot is released and the drain
  signal is re-evaluated.
- Protected user code is never started after the deadline expires.

`execute_now()` and `slot_now()` never cause eviction. They raise `PartitionLimitError`
immediately if no partition is available without waiting.

## Maintenance and shutdown

`cleanup_idle()` and `discard()` raise `BulkheadClosedError` once `close()` has been
called. This prevents new maintenance from being registered after the manager begins its
shutdown sequence.

`close()` is idempotent. Multiple calls do not duplicate child close operations.

`close_and_wait()` waits for all in-flight evictions, cleanup closures, and discard
closures to finish before declaring the manager fully drained. The drain signal is set only
when all pending operations are complete.

## Concurrency envelope

`parallelism` and `waiting_room` are **per-partition** limits.  The global worst-case
simultaneous workload across all retained partitions is:

```
maximum concurrent operations  = parallelism  × max_partitions (conservative upper bound)
maximum queued operations       = waiting_room × max_partitions (conservative upper bound)
```

`max_partitions` controls cardinality and the memory footprint of the partition map.
It does not bound the total number of simultaneous operations by itself.  When a shared
downstream resource has a fixed concurrency limit — for example a connection pool or a
worker pool with a fixed number of slots — set `parallelism` small enough that
`parallelism × max_partitions` stays within that budget.

Bulklink is a concurrency limiter, not a rate limiter.  It does not enforce
requests per second; it enforces how many operations may execute or wait
simultaneously inside each partition.

`PartitionedBulkhead` is not a drop-in replacement for a single global bulkhead.
Use it when isolation between partition keys matters more than a strict global ceiling.

## Status snapshots and pending evictions

`status()` reports `partition_count` as the number of materialized child bulkheads.
During an LRU eviction the victim is removed from the map before its replacement is
created.  In that brief window `partition_count` may transiently read lower than the
logical count used by admission control.

The derived properties `available_partition_slots`, `is_at_limit`, and
`partition_utilization` are all computed from `partition_count`.  They reflect the
snapshot state and do not include any pending replacement reservations.

## Manager metrics

`status()` reports cardinality and lifecycle data without enumerating keys:

```python
status = await customers.status()

print(status.partition_count)
print(status.active_partitions)
print(status.leased_operations)
print(status.evicted_total)
print(status.limit_rejected_total)
```

Two snapshots can be compared with `since()`.
