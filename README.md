<div align="center">

# Bulklink

**Simple admission control. Strong isolation. Predictable behavior under load.**

Bulklink is a small, typed, zero-dependency library for bulkhead isolation and
bounded concurrency in Python `asyncio` applications.

Current package version: **0.6.0**. The documented `0.6.x` public contract is stable.

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

Respect an absolute request deadline measured with the event-loop clock:

```python
loop = asyncio.get_running_loop()
result = await payments.execute_before(
    loop.time() + 0.25,
    payment_api.send,
    order,
)
```

The deadline limits admission only. Once admitted, Bulklink does not cancel the protected
operation when the deadline passes.

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
7. `wait_closed()` waits until all active operations have released their slots;
8. `resize()` changes capacity without cancelling active work or bypassing FIFO order;
9. `execute_before()` rejects work whose absolute admission deadline has expired.

## Weighted capacity

Use `WeightedBulkhead` when operations have different known costs:

```python
from bulklink import WeightedBulkhead

reports = WeightedBulkhead(
    label="reports",
    capacity=10,
    waiting_room=20,
    wait_limit=1.0,
)

async with reports.slot(4):
    await generate_report()

result = await reports.execute(2, load_summary)
```

Capacity and cost are positive integers. The waiting room is still measured in operations,
and queued work remains strict FIFO: a smaller request never overtakes an earlier larger
request. Reducing capacity below the largest queued cost is rejected so queued work cannot
become impossible to admit.

`AsyncBulkhead` remains unchanged and should be preferred when every operation consumes one
slot.

## Graceful shutdown

```python
await payments.close_and_wait()
```

`close_and_wait()` stops new admission, rejects queued work, and waits for operations
already running to finish. Cancelling the caller does not cancel protected operations.


## Observe state transitions

```python
from bulklink import BulkheadEvent


def record_event(event: BulkheadEvent) -> None:
    print(event.kind.value, event.in_flight, event.waiting)


payments.add_event_handler(record_event)
```

Handlers are synchronous, run outside the coordinator lock, and receive immutable
metadata only. They never receive operation arguments, results, or exceptions. Handler
failures are reported through the event loop exception handler without changing
bulkhead state.

## Diagnose capacity pressure

```python
report = await payments.capacity_report()

print(report.summary)
for finding in report.findings:
    print(finding.severity.value, finding.message)
```

The report combines the current snapshot with cumulative admission history. It is
immutable, conservative with small samples, and never changes the bulkhead
configuration.

## Measure activity between snapshots

```python
before = await payments.status()

# Later
after = await payments.status()
interval = after.since(before)

print(interval.admitted)
print(interval.rejected)
print(interval.average_wait_seconds)
```

The interval is computed locally from immutable cumulative snapshots. Every snapshot
carries an opaque instance identity and a strictly increasing sequence number, so
cross-instance and reversed comparisons are rejected. Bulklink does not reset counters,
retain historical windows, or create a background metrics task.

## Isolate concurrency by customer or resource key

```python
from bulklink import PartitionedBulkhead

customers = PartitionedBulkhead(
    label="customers",
    parallelism=3,
    waiting_room=10,
    max_partitions=1_000,
    idle_timeout=300.0,
)

result = await customers.execute(customer_id, call_remote_service)
```

Each key receives an independent `AsyncBulkhead`. Retained cardinality is strictly bounded,
idle partitions are reclaimed without background tasks, and partition keys are never
placed in public status or errors.

## Change capacity safely

```python
await payments.resize(20)
```

Increasing capacity admits queued operations in FIFO order. Reducing capacity never
cancels active work; existing operations drain naturally before admission resumes at
the new limit.

## Manage named bulkheads together

```python
from bulklink import BulkheadRegistry

registry = BulkheadRegistry()
payments = registry.create("payments", parallelism=10, waiting_room=20)
reports = registry.create("reports", parallelism=2)

await registry.close_and_wait()
```

The registry is optional. It enforces unique names, returns immutable ordered snapshots,
and coordinates shutdown without replacing direct `AsyncBulkhead` usage.

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
- [Capacity diagnostics](docs/concepts/capacity-diagnostics.md)
- [Interval metrics](docs/concepts/interval-metrics.md)
- [Partitioned isolation](docs/concepts/partitioned-isolation.md)
- [Weighted capacity](docs/concepts/weighted-capacity.md)
- [Dynamic capacity](docs/concepts/dynamic-capacity.md)
- [Named bulkhead registry](docs/concepts/registry.md)
- [Architecture](docs/maintainers/architecture.md)

## License

MIT.

## Validation and benchmarks

Bulklink is checked on Python 3.10 through 3.14 on Linux, with additional
Windows and macOS validation. The suite includes deterministic race tests, generated
model-oriented sequences, adversarial stress, executable examples, clean-wheel
installation, and consumer-facing typing checks.

Run the complete local verification on Linux or macOS:

```bash
./scripts/ci.sh
```

Record a local performance baseline without enforcing unstable timing thresholds:

```bash
python -m benchmarks.run --output benchmark-results.json
```
