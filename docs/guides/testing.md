# Testing applications that use Bulklink

Prefer behavioral tests over assertions against private modules.

## Verify successful execution

```python
async def test_payment_is_executed() -> None:
    payments = AsyncBulkhead(label="payments", parallelism=1)

    async def send(value: int) -> int:
        return value * 2

    assert await payments.execute(send, 3) == 6
```

## Verify overload handling

Occupy all slots with an event, fill the waiting room, and assert the next operation
raises `BulkheadSaturatedError`.

## Verify no retries

Use a failing callable with a call counter. Bulklink must invoke it exactly once.

## Timing tests

Use events to coordinate tasks. Keep real deadlines small but not so small that normal
scheduler variation makes tests unreliable.

## Maintainer test layers

The repository uses complementary layers:

- unit tests for individual transitions;
- deterministic race tests with observable synchronization;
- generated model-oriented sequences for resize, cancellation, and admission;
- adversarial stress tests for repeated handoff and drainage;
- executable examples as documentation contracts;
- wheel installation and consumer-typing verification;
- benchmarks that record performance without unstable pass/fail thresholds.

Run the complete local verification with:

```bash
./scripts/ci.sh
```

On Windows PowerShell, execute the individual Python commands documented in
`CONTRIBUTING.md` because `ci.sh` is a POSIX shell script.


## Deterministic plugin loading

`scripts/ci.sh` disables automatic discovery of unrelated globally installed pytest
plugins and explicitly loads only `pytest-asyncio` and `pytest-cov`. This prevents a local
IDE, tracing agent, or unrelated test plugin from changing Bulklink's test behavior.
