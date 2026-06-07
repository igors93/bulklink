# Status and metrics

```python
current = await payments.status()
```

The status is immutable and represents one instant. Bulklink does not create a
background metrics task; applications decide how and when to export it.

## Capacity

- `in_flight` is the number of execution slots currently allocated;
- `waiting` is the number of operations currently in the FIFO waiting room;
- `free_slots` is the immediately available execution capacity;
- `capacity_excess` is active work above a newly reduced capacity;
- `is_over_capacity` identifies the temporary drain after shrinking;
- `utilization` is `in_flight / parallelism` and may exceed `1.0` after shrinking;
- `queue_utilization` is `waiting / waiting_room`, or zero when waiting is disabled;
- `peak_in_flight` and `peak_waiting` are historical high-water marks;
- `is_drained` is true only after closing when no active or queued work remains.

## Admissions

- `admitted_total` counts every allocation of an execution slot;
- `admitted_from_queue_total` counts allocations that first waited in the queue;
- `direct_admitted_total` is derived from the two admission counters;
- `finished_total` counts protected operations that released their slot normally,
  including exits caused by user exceptions or cancellation inside the protected block;
- `abandoned_after_admission_total` counts slots returned after queue admission but
  before the protected block began.

The counters obey this accounting identity:

```text
admitted_total = finished_total + abandoned_after_admission_total + in_flight
```

## Waiting-room outcomes

- `queued_total` counts operations that entered the waiting room;
- `cancelled_while_waiting_total` counts callers that withdrew while waiting;
- `expired_total` counts waiting deadlines that expired;
- `closed_while_waiting_total` counts queued operations rejected during closing;
- `settled_waiting_total` combines all operations that have left the waiting room.

The queue obeys this accounting identity:

```text
queued_total = settled_waiting_total + waiting
```

## Rejections

- `saturated_total` counts immediate capacity rejections;
- `closed_before_queue_total` counts calls made after the bulkhead was closed;
- `closed_total` combines both closed-state counters;
- `rejected_total` combines saturation, expiration, and closed-state rejections.

Caller cancellation is not a rejection and is therefore excluded from
`rejected_total`.

## Waiting time

- `average_wait_seconds` is the average queue wait of operations eventually admitted;
- `longest_wait_seconds` is the longest admitted queue wait;
- `cumulative_wait_seconds` is the sum used to calculate the average.

## Events

Bulklink can emit immutable lifecycle events without depending on a metrics or tracing
framework:

```python
from bulklink import BulkheadEvent


def observe(event: BulkheadEvent) -> None:
    metrics.increment(f"bulklink.{event.kind.value}")


payments.add_event_handler(observe)
```

Event kinds include admission, queue entry, saturation, expiration, cancellation,
abandonment after admission, release, resizing, closing, closed-state rejection, and draining.
Each event contains only bulkhead metadata:

- label and event kind;
- wall-clock occurrence time;
- configured and current capacity;
- whether the operation came from the queue;
- queue wait duration when applicable;
- number of queued operations affected by closing or resizing;
- previous capacity for resize events.

Operation arguments, return values, and exceptions are never included.

Handlers must be synchronous and should return quickly. They are invoked in registration
order outside the coordinator lock. Use a non-blocking queue operation such as
`asyncio.Queue.put_nowait()` when forwarding events to asynchronous processing.

Adding the same handler object twice is idempotent. Removing a missing handler is also
idempotent. All handler failures are sent to `loop.call_exception_handler()` and do not
change admission, queue, or capacity state.


## Capacity reports

`capacity_report()` interprets the same immutable status values using conservative,
documented thresholds. Reports are useful for human diagnostics, while status values
and lifecycle events remain the preferred inputs for application-specific monitoring.

See [Capacity diagnostics](capacity-diagnostics.md).
