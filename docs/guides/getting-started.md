# Getting started

## Create a bulkhead

```python
from bulklink import AsyncBulkhead

payments = AsyncBulkhead(
    label="payments",
    parallelism=10,
    waiting_room=50,
    wait_limit=2.0,
)
```

This means:

- at most 10 payment operations execute simultaneously;
- at most 50 payment operations wait;
- a queued operation waits at most 2 seconds.

## Protect a block

```python
async with payments.slot():
    response = await payment_client.send(order)
```

The slot is released whether the call succeeds, raises, or the task is cancelled.

## Protect a callable

```python
response = await payments.execute(payment_client.send, order)
```

## Reject instead of waiting

Use immediate admission when the caller should never enter the waiting room:

```python
response = await payments.execute_now(payment_client.send, order)
```

For a protected block:

```python
async with payments.slot_now():
    response = await payment_client.send(order)
```

Both forms raise `BulkheadSaturatedError` when a slot is not immediately available.
They do not consume waiting-room capacity and never overtake existing waiters.

## Use a shorter limit for one call

A caller can choose a stricter queue wait limit without changing the bulkhead:

```python
response = await payments.execute_within(
    0.25,
    payment_client.send,
    order,
)
```

For a protected block:

```python
async with payments.slot_within(0.25):
    response = await payment_client.send(order)
```

The effective limit is the shorter of the bulkhead default and the per-call value.
The limit applies only while waiting for admission; it does not time out the protected
operation itself.

## Shut down gracefully

Close admission and wait for active work to finish:

```python
await payments.close_and_wait()
```

For separate lifecycle steps:

```python
await payments.close()
await payments.wait_closed()
```

`close()` rejects queued and future operations but never cancels work that already
holds a slot. Multiple tasks may await `wait_closed()` independently.


## Observe lifecycle events

```python
from bulklink import BulkheadEvent


def log_event(event: BulkheadEvent) -> None:
    print(event.kind.value, event.in_flight, event.waiting)


payments.add_event_handler(log_event)
```

Handlers are synchronous and run outside the internal lock. Keep them fast, or forward
the event to a queue with a non-blocking operation. Remove a handler by identity:

```python
payments.remove_event_handler(log_event)
```

Events contain no arguments, results, or exceptions from the protected operation.

## Diagnose capacity pressure

```python
report = await payments.capacity_report()

print(report.summary)
for finding in report.findings:
    print(finding.code.value, finding.recommendation)
```

The report is a read-only interpretation of current state and cumulative counters. It
does not resize the bulkhead or create monitoring tasks.

## Change capacity at runtime

```python
await payments.resize(20)
```

Increasing capacity admits the oldest queued operations immediately. Reducing capacity
keeps active operations running and temporarily pauses replacement admission until the
new limit is reached. Resizing a closed bulkhead raises `BulkheadClosedError`.

## Protect a function

```python
@payments
async def send_payment(order: Order) -> Receipt:
    return await payment_client.send(order)
```

## Handle overload

```python
from bulklink import (
    BulkheadQueueTimeoutError,
    BulkheadSaturatedError,
)

try:
    return await payments.execute(payment_client.send, order)
except BulkheadSaturatedError:
    return {"status": "busy"}
except BulkheadQueueTimeoutError:
    return {"status": "try-later"}
```

These errors represent local admission decisions, not failures returned by the
payment service.


## Manage several named bulkheads

```python
from bulklink import BulkheadRegistry

registry = BulkheadRegistry()
payments = registry.create("payments", parallelism=10, waiting_room=20)
reports = registry.create("reports", parallelism=2)

await registry.close_and_wait()
```

Use a registry when one application owns several bulkheads and needs ordered status
collection or one coordinated shutdown. Direct `AsyncBulkhead` construction remains the
simplest option for isolated usage.


## Absolute admission deadline

Use the event loop's monotonic clock when a request already has a total time budget:

```python
loop = asyncio.get_running_loop()
deadline = loop.time() + 0.5
result = await payments.execute_before(deadline, send_payment, order)
```

The deadline limits only how long the call may wait for admission.

## Compare activity between two moments

```python
before = await payments.status()

# Run or observe application work.

after = await payments.status()
interval = after.since(before)

print(interval.admitted)
print(interval.rejected)
print(interval.finished)
```

The comparison does not reset counters or start a background sampler. Store the earlier
snapshot in the application and compare it with a later snapshot from the same bulkhead.
