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

- `slot()` returns an async context manager;
- `execute(operation, *args, **kwargs)` protects one async call;
- decorating an async function protects each invocation;
- `status()` returns `BulkheadStatus`;
- `close()` rejects queued and future operations.

An instance binds to the first event loop that uses it.

## Private modules

Modules under `bulklink._internal` are implementation details without compatibility
guarantees.
