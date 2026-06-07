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
