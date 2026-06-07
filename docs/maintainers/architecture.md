# Architecture

Bulklink keeps the public facade small and places synchronization behind focused
private components.

## Package map

```text
src/bulklink/
├── __init__.py             stable root imports
├── bulkhead.py             public facade
├── errors.py               public exception hierarchy
├── events.py               immutable event contract
├── status.py               immutable observable state
├── typing.py               shared type parameters
└── _internal/
    ├── cancellation.py     protected critical cleanup
    ├── coordinator.py      admission, FIFO handoff, counters, closing
    ├── events.py           synchronous isolated event dispatch
    ├── models.py           private waiter and counter models
    ├── slot.py             context-manager lifecycle
    └── validation.py       deterministic validation
```

## Responsibilities

### AsyncBulkhead

Exposes user-facing usage styles but stores no mutable concurrency state directly.

### AdmissionCoordinator

Owns the lock, FIFO queue, in-flight count, event-loop binding, slot handoff, closing,
draining signal, and counters.

### EventDispatcher

Stores a stable tuple of synchronous handlers. The coordinator creates immutable event
snapshots while holding its lock, then dispatches them only after the lock has been
released. Handler failures are isolated and reported to the event loop.

### SlotContext

Owns exactly one admission/release lifecycle through injected actions. Internal
states prevent concurrent reuse while admission or release is still running.

### BulkheadStatus

Contains immutable observable values and never exposes locks, futures, or queue nodes.

## Invariants

1. `in_flight` never exceeds `parallelism`.
2. waiting count never exceeds `waiting_room`.
3. queued work is admitted FIFO.
4. new arrivals, including immediate admission, never overtake existing waiters.
5. each granted slot is released or transferred exactly once.
6. timeout and cancellation cannot leak slots.
7. protected user code is invoked at most once per Bulklink call.
8. `close()` does not cancel active user code.
9. public imports remain intentionally small.
10. Bulklink naming remains independent from Relinker naming.
11. Per-call wait limits can tighten but never extend the configured limit.
12. One slot context cannot run overlapping admission or release lifecycles.
13. Drain completion is signalled only after closing and when `in_flight` reaches zero.
14. Cancelling one shutdown waiter cannot affect active work or other shutdown waiters.
15. Event handlers never execute while the coordinator lock is held.
16. Event payloads never contain protected operation arguments, results, or exceptions.
17. Handler failures cannot change capacity, queue state, or admission outcomes.

## Why direct slot transfer?

On release, the coordinator transfers the slot to the oldest waiter while holding the
lock. It does not decrement capacity and make the waiter race to acquire it. This
prevents queue jumping and keeps accounting atomic.

## Why an event for draining?

The coordinator owns one event-loop-bound signal that is set exactly once, after
admission has closed and all allocated slots have been returned. Waiting callers use
that signal directly, so shutdown requires no polling and cancellation of one waiter
does not alter shared state.

## Why synchronous event handlers?

Synchronous handlers avoid background task ownership, shutdown races, and unobserved
coroutine failures inside the library. Applications that need asynchronous export can
forward immutable events into their own queue with `put_nowait()`, keeping lifecycle
control with the application.
