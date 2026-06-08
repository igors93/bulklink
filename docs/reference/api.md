# Public API

The `0.5.x` compatibility surface is recorded in [Stable public contract](public-contract.md).

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
    BulkheadInterval,
    BulkheadQueueTimeoutError,
    BulkheadRegistry,
    BulkheadRegistryFailure,
    BulkheadRegistryOperationError,
    BulkheadSaturatedError,
    BulkheadStatus,
    BulklinkError,
    WeightedBulkhead,
    WeightedBulkheadEvent,
    WeightedBulkheadEventHandler,
    WeightedBulkheadInterval,
    WeightedBulkheadSaturatedError,
    WeightedBulkheadStatus,
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
- `slot_before(deadline)` returns a context manager bounded by an absolute loop deadline;
- `execute(operation, *args, **kwargs)` protects one async call and may wait;
- `execute_now(operation, *args, **kwargs)` protects one async call without queueing;
- `execute_within(limit, operation, *args, **kwargs)` uses a stricter queue wait limit;
- `execute_before(deadline, operation, *args, **kwargs)` uses an absolute loop deadline;
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
extend the configured `wait_limit` and apply only to waiting-room time. Absolute deadlines
use `asyncio.get_running_loop().time()` and also apply only to admission. A past deadline
raises `BulkheadQueueTimeoutError` without entering the FIFO queue.

`wait_closed()` may be started before `close()` and completes only after the bulkhead
is closed and has no active work. Cancelling one waiter does not cancel active work or
prevent other waiters from completing.

`resize()` accepts a positive integer. Increases admit queued work in FIFO order.
Reductions never cancel active work and may temporarily make `utilization` exceed 1.0.
Resizing to the current value is a no-op, and resizing after closing raises
`BulkheadClosedError`.


## WeightedBulkhead

```python
WeightedBulkhead(
    *,
    label: str,
    capacity: int,
    waiting_room: int = 0,
    wait_limit: float | None = None,
)
```

Primary operations:

- `slot(cost=1)` waits for the requested integer capacity cost;
- `slot_now(cost=1)` rejects instead of queueing;
- `slot_within(limit, cost=1)` applies a stricter relative queue limit;
- `slot_before(deadline, cost=1)` applies an absolute event-loop deadline;
- `execute(cost, operation, *args, **kwargs)` protects one async call;
- `execute_now`, `execute_within`, and `execute_before` provide matching admission modes;
- `status()` returns `WeightedBulkheadStatus`;
- `resize(capacity)` changes capacity without cancelling admitted work;
- `close()`, `wait_closed()`, and `close_and_wait()` provide graceful shutdown.

Costs and capacity are positive integers. A cost greater than the current capacity is
invalid. Queueing is strict FIFO: smaller requests never overtake earlier larger requests.
A resize reduction below the largest queued cost is rejected so queued work cannot become
permanently impossible to admit.

`WeightedBulkheadEvent` uses `BulkheadEventKind` and exposes only capacity, cost, queue, and
timing metadata. `WeightedBulkheadInterval` compares two immutable weighted status snapshots.

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


## BulkheadRegistry

```python
registry = BulkheadRegistry()
bulkhead = registry.create(
    "payments",
    parallelism=10,
    waiting_room=20,
    wait_limit=1.0,
)
```

Public operations:

- `create()` adds one unique named bulkhead;
- `get()` returns a registered instance;
- `remove()` closes, drains, and then removes one instance;
- `labels` returns an immutable creation-ordered tuple;
- `statuses()` and `capacity_reports()` return creation-ordered tuples;
- `close_all()` closes all current members and prevents future creation;
- `wait_closed()` waits after collective shutdown has started;
- `close_and_wait()` performs cancellation-safe collective shutdown.

A collective operation attempts every selected member. If any fail,
`BulkheadRegistryOperationError` contains immutable `BulkheadRegistryFailure` metadata
for each failed label.


## BulkheadInterval

Create an interval from a later status snapshot:

```python
interval = current.since(previous)
```

The immutable result contains both endpoint snapshots and counter differences such as
`admitted`, `queued`, `rejected`, `finished`, and `average_wait_seconds`. Comparing
different instances, incompatible configurations, reversed snapshot order, conflicting
snapshot identities, or a reopened lifecycle raises `ValueError`.
