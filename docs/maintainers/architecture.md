# Architecture

Bulklink keeps the public facade small and places synchronization behind focused
private components.

## Package map

```text
src/bulklink/
├── __init__.py             stable root imports
├── bulkhead.py             public facade
├── capacity.py             immutable diagnostic contracts
├── errors.py               public exception hierarchy
├── events.py               immutable event contract
├── partitioned.py          bounded per-key isolation facade
├── partitioned_status.py   partition cardinality snapshots
├── registry.py             named ownership and collective lifecycle
├── status.py               immutable observable state
├── typing.py               shared type parameters
└── _internal/
    ├── cancellation.py     protected critical cleanup
    ├── coordinator.py      admission, FIFO handoff, counters, closing
    ├── diagnostics.py      pure capacity assessment rules
    ├── events.py           synchronous isolated event dispatch
    ├── models.py           private waiter and counter models
    ├── partitioned_coordinator.py bounded keyed ownership and cleanup
    ├── partitioned_models.py private partition entries and counters
    ├── slot.py             context-manager lifecycle
    └── validation.py       deterministic validation
```

## Responsibilities

### AsyncBulkhead

Exposes user-facing usage styles but stores no mutable concurrency state directly.

### BulkheadRegistry

Owns only named `AsyncBulkhead` references and registry lifecycle state. A short
threading lock protects synchronous membership changes. No registry lock is held while
awaiting a bulkhead operation. Collective methods operate on stable ordered snapshots.

### PartitionedBulkhead

Owns a bounded, lazily created set of `AsyncBulkhead` children. A parent asyncio lock
serializes key membership, lease counts, LRU idle reclamation, shutdown, and aggregate
cardinality metrics. The key is never copied into child labels or public records.

### AdmissionCoordinator

Owns the lock, FIFO queue, in-flight count, dynamic capacity, event-loop binding, slot
handoff, closing, draining signal, and counters.

### EventDispatcher

Stores a stable tuple of synchronous handlers. The coordinator creates immutable event
snapshots while holding its lock, then dispatches them only after the lock has been
released. Handler failures are isolated and reported to the event loop.

### Capacity diagnostics

`capacity_report()` reads one immutable status snapshot and passes it to pure assessment
rules. The rules use documented minimum sample sizes and never acquire coordinator
locks, modify counters, or change capacity.

### SlotContext

Owns exactly one admission/release lifecycle through injected actions. Internal
states prevent concurrent reuse while admission or release is still running.

### BulkheadStatus

Contains immutable observable values and never exposes locks, futures, or queue nodes.

## Invariants

1. `in_flight` may exceed `parallelism` only while a reduction drains existing work.
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
18. Capacity assessment is read-only and deterministic for the same snapshot.
19. Capacity increases admit existing waiters before new arrivals.
20. Capacity reductions never cancel admitted work or hand off replacements early.
21. Registry names are unique and never silently replace an existing bulkhead.
22. Registry shutdown prevents new members before taking the shutdown snapshot.
23. Collective failure cannot skip remaining selected bulkheads.
24. Partition cardinality never exceeds `max_partitions`.
25. A partition with admitted or waiting callers is never reclaimed.
26. Partition keys never appear in public manager errors or status.
27. Graceful partitioned shutdown releases the retained key mapping.

## Why gradual capacity reduction?

Revoking a slot from running user code would require cancelling work the bulkhead does
not own. After shrinking, releases reduce excess active work until the new limit is
reached. Only then can a release transfer capacity to the next FIFO waiter.

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

## Weighted admission

`WeightedBulkhead` uses a separate `WeightedAdmissionCoordinator` because capacity units and
operation counts are distinct invariants. It reuses the same slot lifecycle, cancellation
cleanup, event-dispatch isolation, validation helpers, wait-state model, and exception base
hierarchy as `AsyncBulkhead`.

The weighted coordinator owns:

- current used units and active operation count;
- strict-FIFO entries carrying one immutable integer cost;
- current and cumulative waiting units;
- weighted operation and unit counters;
- opaque snapshot identity and sequence;
- graceful close and drain state.

A queued entry is admitted only when it is at the head and its complete cost fits. Partial
allocation is forbidden. Resize-down is rejected when it would make an existing queued cost
larger than total capacity. This prevents permanent head-of-line impossibility while keeping
active work cancellation-free.


## Partitioned admission

A manager reference is acquired before child admission and released only after the complete
child slot lifecycle. This prevents eviction while an operation is waiting or executing.
Under cardinality pressure, only the least-recently-used entry with zero references may be
removed. Normal TTL cleanup is explicit and runs no permanent task.
