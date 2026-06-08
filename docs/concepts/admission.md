# Admission and FIFO queueing

Each operation follows one path:

```text
operation arrives
      |
      +-- free slot and no older waiter --> execute
      |
      +-- no free slot, queue has room --> wait in FIFO order
      |
      +-- queue full --------------------> saturation error
```

When active work leaves, its slot is transferred directly to the oldest valid waiter.
A newly arriving operation cannot overtake queued work.

`waiting_room=0` disables waiting. An operation is rejected whenever all execution
slots are occupied.

`wait_limit=None` permits indefinite queue waiting. Request/response systems should
usually use a finite limit.


## Immediate admission

`slot_now()` and `execute_now()` never enter the waiting room. They either acquire a
slot immediately or raise `BulkheadSaturatedError`. Existing FIFO waiters always keep
priority, so immediate admission cannot jump ahead of queued work.

## Per-call wait limits

`slot_within(limit)` and `execute_within(limit, operation, ...)` use a stricter limit
for one admission. The effective queue wait is the shorter of the configured default
and the requested value. A per-call limit can never extend the bulkhead default.

The limit starts after the operation enters the waiting room and ends when a slot is
admitted. Time spent inside the protected operation is not included.

## Absolute admission deadlines

`slot_before(deadline)` and `execute_before(deadline, operation, ...)` accept an absolute
deadline measured by `asyncio.get_running_loop().time()`. This lets a caller propagate the
time remaining in a larger request budget without resetting that budget at each layer.

```python
loop = asyncio.get_running_loop()
deadline = loop.time() + 0.5
result = await gate.execute_before(deadline, operation)
```

The effective queue limit is the earlier of the absolute deadline and the configured
`wait_limit`. A deadline that has already elapsed is rejected before queue entry. Closed
state still has priority over deadline expiration. The deadline controls admission only;
a protected operation is not cancelled after it starts.


## Dynamic execution capacity

`resize(new_parallelism)` changes capacity while preserving FIFO order. Increases
immediately admit the oldest waiters when new slots become available. Reductions do
not cancel active work; releases first drain any temporary excess above the new limit.

See [Dynamic capacity](dynamic-capacity.md).
