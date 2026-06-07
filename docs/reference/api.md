# Public API

Stable root imports:

```python
from bulklink import (
    AsyncBulkhead,
    CapacityFinding,
    CapacityFindingCode,
    CapacityReport,
    CapacitySeverity,
    BulkheadClosedError,
    BulkheadEvent,
    BulkheadEventHandler,
    BulkheadEventKind,
    BulkheadQueueTimeoutError,
    BulkheadSaturatedError,
    BulkheadStatus,
    BulklinkError,
)
```

## AsyncBulkhead

```python
AsyncBulkhead(
    *,
    label: str,
    parallelism: int,
    waiting_room: int = 0,
    wait_limit: float | None = None,
)
```

Methods:

- `slot()` returns a context manager that may wait for capacity;
- `slot_now()` returns a context manager that rejects instead of waiting;
- `slot_within(limit)` returns a context manager with a stricter queue wait limit;
- `execute(operation, *args, **kwargs)` protects one async call and may wait;
- `execute_now(operation, *args, **kwargs)` protects one async call without queueing;
- `execute_within(limit, operation, *args, **kwargs)` uses a stricter queue wait limit;
- decorating an async function protects each invocation;
- `status()` returns `BulkheadStatus`;
- `capacity_report()` returns an immutable `CapacityReport`;
- `resize(parallelism)` changes open-bulkhead capacity without cancelling active work;
- `close()` rejects queued and future operations;
- `wait_closed()` waits for closing and active-work drainage;
- `close_and_wait()` performs both shutdown steps;
- `add_event_handler(handler)` registers one synchronous observer;
- `remove_event_handler(handler)` removes an observer by identity.

An instance binds to the first event loop that uses it. Per-call limits cannot
extend the configured `wait_limit` and apply only to waiting-room time.

`wait_closed()` may be started before `close()` and completes only after the bulkhead
is closed and has no active work. Cancelling one waiter does not cancel active work or
prevent other waiters from completing.

`resize()` accepts a positive integer. Increases admit queued work in FIFO order.
Reductions never cancel active work and may temporarily make `utilization` exceed 1.0.
Resizing to the current value is a no-op, and resizing after closing raises
`BulkheadClosedError`.

## Private modules

Modules under `bulklink._internal` are implementation details without compatibility
guarantees.

## Events

`BulkheadEvent` is immutable. `BulkheadEventKind` identifies the lifecycle transition,
and `BulkheadEventHandler` describes a synchronous callback that returns `None`.
Resize events expose `previous_parallelism` and the current `parallelism`.

Handlers execute outside internal locks and in registration order. Duplicate
registration and removal of missing handlers are idempotent. Unsupported asynchronous
handlers are rejected. Handler failures are reported through the running event loop's
exception handler and do not alter bulkhead state.


## Capacity diagnostics

`CapacityReport` contains the status snapshot, configured wait limit, assessment time,
and an immutable tuple of `CapacityFinding` values. Its derived properties include
severity, rejection and queueing ratios, wait-limit ratios, and a short summary.

`CapacityFindingCode` is the stable machine-readable category. `CapacitySeverity`
contains `ok`, `notice`, `warning`, and `critical`. Reports never alter capacity and do
not include protected operation arguments, results, or exceptions.
