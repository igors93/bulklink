# Public API

Stable root imports:

```python
from bulklink import (
    AsyncBulkhead,
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

## Private modules

Modules under `bulklink._internal` are implementation details without compatibility
guarantees.

## Events

`BulkheadEvent` is immutable. `BulkheadEventKind` identifies the lifecycle transition,
and `BulkheadEventHandler` describes a synchronous callback that returns `None`.

Handlers execute outside internal locks and in registration order. Duplicate
registration and removal of missing handlers are idempotent. Unsupported asynchronous
handlers are rejected. Handler failures are reported through the running event loop's
exception handler and do not alter bulkhead state.
