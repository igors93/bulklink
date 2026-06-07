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
