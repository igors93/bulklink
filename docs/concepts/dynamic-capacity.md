# Dynamic capacity

```python
await payments.resize(20)
```

`resize()` changes the execution capacity of an open bulkhead without replacing the
object, clearing metrics, or cancelling protected operations.

## Increasing capacity

When capacity grows, available slots are assigned immediately to the oldest queued
operations in FIFO order. New arrivals cannot overtake those waiters.

```text
parallelism: 2 -> 5
in_flight: 2
waiting: 4

result:
in_flight: 5
waiting: 1
```

## Reducing capacity

A reduction never cancels work that already holds a slot. The bulkhead may temporarily
have more active operations than its new capacity. During that period, completed
operations reduce the excess and queued work is not admitted until active work reaches
the new limit.

```text
parallelism: 5 -> 2
in_flight: 5

three operations must finish before replacement admission resumes
```

`BulkheadStatus.is_over_capacity` and `capacity_excess` expose this temporary state.
`utilization` may be greater than `1.0` while the reduction drains.

## Validation and lifecycle

The new capacity must be a positive integer. Resizing to the current value is an
idempotent no-op. A closed bulkhead cannot be resized and raises `BulkheadClosedError`.
Bulkheads are terminal after closing and are never reopened through resizing.

## Events

A successful change emits `BulkheadEventKind.RESIZED`. The event contains the new
`parallelism`, the `previous_parallelism`, and the number of queued operations admitted
by the increase in `affected_waiters`.

When an increase admits queued work, the `RESIZED` event is delivered before the FIFO
`ADMITTED` events. All events created by that atomic resize share the same timestamp.
