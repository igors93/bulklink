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
