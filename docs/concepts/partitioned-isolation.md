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

`max_partitions` is mandatory. When a new key arrives at the limit, Bulklink removes the
least-recently-used idle partition. If every retained partition currently has admitted or
waiting callers, the new key is rejected with `PartitionLimitError`.

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

## Concurrency envelope

`parallelism` and `waiting_room` are **per-partition** limits.  The global worst-case
throughput across all retained partitions is:

```
maximum concurrent operations  = parallelism  × max_partitions (conservative upper bound)
maximum queued operations       = waiting_room × max_partitions (conservative upper bound)
```

`max_partitions` controls cardinality and the memory footprint of the partition map.
It does not limit total throughput by itself.  When a shared downstream resource has a
fixed capacity — for example a connection pool or an upstream rate limit — set
`parallelism` small enough that `parallelism × max_partitions` stays within that budget.

`PartitionedBulkhead` is not a drop-in replacement for a single global bulkhead.
Use it when isolation between partition keys matters more than a strict global ceiling.

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
