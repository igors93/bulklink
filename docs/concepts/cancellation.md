# Cancellation safety

Cancellation can happen:

- while an operation waits;
- at the exact moment a slot is transferred;
- while protected user code is running.

Bulklink handles each case:

1. queued cancellation removes the waiter;
2. cancellation racing with admission returns the granted slot;
3. cancellation inside `async with bulkhead.slot()` triggers release;
4. slot handoff skips cancelled waiters.

These rules prevent permanent capacity loss.
