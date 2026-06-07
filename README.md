<div align="center">

# Bulklink

**Simple admission control. Strong isolation. Predictable behavior under load.**

Bulklink is a small, typed, zero-dependency library for bulkhead isolation and
bounded concurrency in Python `asyncio` applications.

</div>

## What problem does it solve?

Without admission control, one slow dependency can attract hundreds of concurrent
operations, consume connections and memory, and damage unrelated parts of an
application.

Bulklink creates independent compartments:

```python
from bulklink import AsyncBulkhead

payments = AsyncBulkhead(
    label="payments",
    parallelism=10,
    waiting_room=50,
    wait_limit=2.0,
)

reports = AsyncBulkhead(
    label="reports",
    parallelism=2,
    waiting_room=5,
)
```

Slow reports can use at most two execution slots. They cannot consume the ten slots
reserved for payments.

## Quick start

```python
async def send_payment(order: object) -> object:
    async with payments.slot():
        return await payment_api.send(order)
```

Or:

```python
result = await payments.execute(payment_api.send, order)
```

Reject instead of waiting when immediate capacity is required:

```python
result = await payments.execute_now(payment_api.send, order)
```

Use a shorter limit for one call without extending the bulkhead default:

```python
result = await payments.execute_within(0.25, payment_api.send, order)
```

Or decorate an async function:

```python
@payments
async def send_payment(order: object) -> object:
    return await payment_api.send(order)
```

## Behavior

For each bulkhead:

1. up to `parallelism` operations may execute;
2. up to `waiting_room` operations may wait in FIFO order;
3. an operation is rejected immediately when both areas are full;
4. a waiting operation is rejected when `wait_limit` expires;
5. exceptions and task cancellation release capacity safely;
6. `close()` rejects queued and future operations without interrupting active work;
7. `wait_closed()` waits until all active operations have released their slots.

## Graceful shutdown

```python
await payments.close_and_wait()
```

`close_and_wait()` stops new admission, rejects queued work, and waits for operations
already running to finish. Cancelling the caller does not cancel protected operations.

## Designed to coexist with Relinker

Bulklink and Relinker solve different stages:

- **Bulklink** decides whether one operation may start;
- **Relinker** decides whether a failed operation should be attempted again.

Bulklink deliberately uses `AsyncBulkhead`, `execute()`, `slot()`, and `status()`,
rather than Relinker's policy, retry, result, budget, `run_async()`, and `snapshot()`
terminology.

See [Using Bulklink with Relinker](docs/guides/with-relinker.md).

## Non-goals

Bulklink does not provide retries, backoff, jitter, circuit breakers, HTTP-specific
behavior, requests-per-second limits, or distributed coordination.

## Development

```bash
python -m pip install -e ".[dev]"
./scripts/ci.sh
```

## Documentation

- [Documentation index](docs/README.md)
- [Getting started](docs/guides/getting-started.md)
- [Using Bulklink with Relinker](docs/guides/with-relinker.md)
- [Production checklist](docs/guides/production-checklist.md)
- [Architecture](docs/maintainers/architecture.md)

## License

MIT.
