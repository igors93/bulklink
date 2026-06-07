# Using Bulklink with Relinker

Bulklink and Relinker are complementary:

- Relinker controls attempts after failure.
- Bulklink controls admission for each attempt.

```python
from bulklink import AsyncBulkhead
from relinker import RetryPolicy

payments = AsyncBulkhead(
    label="payments",
    parallelism=10,
    waiting_room=50,
    wait_limit=2.0,
)

retry_plan = (
    RetryPolicy()
    .attempts(3)
    .on(ConnectionError)
    .exponential_delay(base=1, maximum=10)
)

async def one_payment_attempt(order: object) -> object:
    return await payments.execute(payment_api.send, order)

result = await retry_plan.run_async(one_payment_attempt, order)
```

This order is intentional. The Bulklink slot is held only during the external call.
Relinker performs backoff outside the slot, so sleeping retries do not consume
execution capacity.

Bulklink queue expiration does not inherit from `TimeoutError`. A Relinker policy
that retries network timeouts therefore does not automatically retry local overload.

Retrying overload can increase overload, so applications must opt in explicitly.
