# Public API

Stable root imports:

```python
from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
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
- `close()` rejects queued and future operations.

An instance binds to the first event loop that uses it. Per-call limits cannot
extend the configured `wait_limit` and apply only to waiting-room time.

## Private modules

Modules under `bulklink._internal` are implementation details without compatibility
guarantees.
