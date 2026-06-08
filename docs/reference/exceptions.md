# Exception behavior

## BulkheadSaturatedError

Raised immediately when all execution slots are occupied and the waiting room is
full.

## BulkheadQueueTimeoutError

Raised when a queued operation exceeds its effective wait limit or when an absolute
admission deadline has already expired. It deliberately does not inherit from Python's
`TimeoutError`.

## BulkheadClosedError

Raised for new operations after `close()` and queued operations removed during close.

## User exceptions

Bulklink does not wrap exceptions from protected user code. The original exception
propagates unchanged after the slot is released.

## Registry membership errors

`BulkheadRegistry.create()` raises `ValueError` for duplicate or invalid labels and
invalid bulkhead configuration. `get()` and `remove()` raise `KeyError` when the label
is not registered. Creating after collective shutdown or calling `wait_closed()` before
shutdown raises `RuntimeError`.

## BulkheadRegistryOperationError

Raised only after every selected bulkhead has been attempted and at least one
collective status, report, or lifecycle operation failed. The exception contains an
immutable tuple of `BulkheadRegistryFailure` values with the label, exception type name,
and exception message. It never contains protected operation arguments or results.
