# Exception behavior

## BulkheadSaturatedError

Raised immediately when all execution slots are occupied and the waiting room is
full.

## BulkheadQueueTimeoutError

Raised when a queued operation exceeds `wait_limit`. It deliberately does not inherit
from Python's `TimeoutError`.

## BulkheadClosedError

Raised for new operations after `close()` and queued operations removed during close.

## User exceptions

Bulklink does not wrap exceptions from protected user code. The original exception
propagates unchanged after the slot is released.
