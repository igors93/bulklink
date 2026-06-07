from __future__ import annotations

import asyncio
import dataclasses

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadRegistry,
    BulkheadRegistryFailure,
    BulkheadRegistryOperationError,
    CapacitySeverity,
)
from tests.helpers import eventually


def test_registry_creates_normalized_unique_bulkheads() -> None:
    registry = BulkheadRegistry()

    payments = registry.create(
        " payments ",
        parallelism=3,
        waiting_room=5,
        wait_limit=1.0,
    )
    reports = registry.create("reports", parallelism=1)

    assert registry.labels == ("payments", "reports")
    assert len(registry) == 2
    assert "payments" in registry
    assert " payments " in registry
    assert 42 not in registry
    assert registry.get("payments") is payments
    assert registry.get(" reports ") is reports
    assert payments.label == "payments"

    with pytest.raises(ValueError, match="already registered"):
        registry.create("payments", parallelism=1)

    with pytest.raises(KeyError, match="not registered"):
        registry.get("missing")


def test_registry_rejects_new_entries_after_shutdown_starts() -> None:
    registry = BulkheadRegistry()
    registry.create("existing", parallelism=1)

    asyncio.run(registry.close_all())

    assert registry.is_closed
    assert registry.labels == ("existing",)
    with pytest.raises(RuntimeError, match="registry is closed"):
        registry.create("late", parallelism=1)


async def test_statuses_and_reports_preserve_creation_order() -> None:
    registry = BulkheadRegistry()
    registry.create("payments", parallelism=2, waiting_room=2, wait_limit=1.0)
    registry.create("reports", parallelism=1)

    statuses = await registry.statuses()
    reports = await registry.capacity_reports()

    assert isinstance(statuses, tuple)
    assert isinstance(reports, tuple)
    assert [status.label for status in statuses] == ["payments", "reports"]
    assert [report.status.label for report in reports] == ["payments", "reports"]
    assert reports[0].severity is CapacitySeverity.OK
    assert reports[1].severity is CapacitySeverity.OK


async def test_remove_closes_drains_and_releases_the_name() -> None:
    registry = BulkheadRegistry()
    removed = registry.create("temporary", parallelism=1)

    result = await registry.remove("temporary")

    assert result is removed
    assert len(registry) == 0
    assert "temporary" not in registry
    status = await removed.status()
    assert status.is_closed
    assert status.is_drained

    replacement = registry.create("temporary", parallelism=2)
    assert replacement is not removed
    assert replacement.parallelism == 2


async def test_remove_waits_for_active_work_before_deleting_entry() -> None:
    registry = BulkheadRegistry()
    gate = registry.create("active", parallelism=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    removal = asyncio.create_task(registry.remove("active"))
    await eventually(lambda: gate_is_closed(gate))

    assert "active" in registry
    assert not removal.done()

    release.set()
    await active
    assert await removal is gate
    assert "active" not in registry


async def test_cancelled_remove_finishes_draining_and_deletes_membership() -> None:
    registry = BulkheadRegistry()
    gate = registry.create("cancel-remove", parallelism=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    removal = asyncio.create_task(registry.remove("cancel-remove"))
    await eventually(lambda: gate_is_closed(gate))

    removal.cancel()
    await asyncio.sleep(0)
    assert not removal.done()

    release.set()
    await active
    with pytest.raises(asyncio.CancelledError):
        await removal

    assert "cancel-remove" not in registry
    assert (await gate.status()).is_drained


async def test_close_all_closes_every_bulkhead_without_waiting_for_active_work() -> None:
    registry = BulkheadRegistry()
    first = registry.create("first", parallelism=1)
    second = registry.create("second", parallelism=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with first.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(first, 1))

    await registry.close_all()

    first_status = await first.status()
    second_status = await second.status()
    assert registry.is_closed
    assert first_status.is_closed
    assert not first_status.is_drained
    assert second_status.is_drained

    release.set()
    await active
    await registry.wait_closed()
    assert (await first.status()).is_drained


async def test_wait_closed_requires_collective_shutdown() -> None:
    registry = BulkheadRegistry()
    registry.create("open", parallelism=1)

    with pytest.raises(RuntimeError, match="close_all"):
        await registry.wait_closed()


async def test_close_and_wait_is_cancellation_safe_for_the_whole_registry() -> None:
    registry = BulkheadRegistry()
    first = registry.create("first", parallelism=1)
    second = registry.create("second", parallelism=1)
    releases = [asyncio.Event(), asyncio.Event()]

    async def hold(gate: AsyncBulkhead, release: asyncio.Event) -> None:
        async with gate.slot():
            await release.wait()

    active = [
        asyncio.create_task(hold(first, releases[0])),
        asyncio.create_task(hold(second, releases[1])),
    ]
    await eventually(lambda: both_in_flight(first, second))

    shutdown = asyncio.create_task(registry.close_and_wait())
    await eventually(lambda: both_closed(first, second))
    shutdown.cancel()
    await asyncio.sleep(0)
    assert not shutdown.done()

    for release in releases:
        release.set()
    await asyncio.gather(*active)

    with pytest.raises(asyncio.CancelledError):
        await shutdown

    assert registry.is_closed
    assert all(status.is_drained for status in await registry.statuses())


async def test_collective_failure_does_not_skip_other_bulkheads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = BulkheadRegistry()
    broken = registry.create("broken", parallelism=1)
    healthy = registry.create("healthy", parallelism=1)

    async def fail_close() -> None:
        raise RuntimeError("simulated close failure")

    monkeypatch.setattr(broken, "close", fail_close)

    with pytest.raises(BulkheadRegistryOperationError) as captured:
        await registry.close_all()

    error = captured.value
    assert error.operation == "close_all"
    assert len(error.failures) == 1
    assert error.failures[0].label == "broken"
    assert error.failures[0].error_type == "RuntimeError"
    assert error.failures[0].message == "simulated close failure"
    assert registry.is_closed
    assert (await healthy.status()).is_drained
    assert not (await broken.status()).is_closed


def test_registry_failure_metadata_is_immutable_and_bounded() -> None:
    failure = BulkheadRegistryFailure(
        label="payments",
        error_type="RuntimeError",
        message="failed",
    )
    fields = {field.name for field in dataclasses.fields(failure)}

    assert fields == {"label", "error_type", "message"}
    assert "operation" not in fields
    assert "args" not in fields
    assert "result" not in fields
    assert "exception" not in fields
    with pytest.raises(dataclasses.FrozenInstanceError):
        failure.message = "changed"  # type: ignore[misc]


async def test_empty_registry_collective_operations_are_idempotent() -> None:
    registry = BulkheadRegistry()

    assert await registry.statuses() == ()
    assert await registry.capacity_reports() == ()
    await registry.close_all()
    await registry.wait_closed()
    await registry.close_and_wait()

    assert registry.is_closed
    assert registry.labels == ()


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def gate_is_closed(gate: AsyncBulkhead) -> bool:
    return (await gate.status()).is_closed


async def both_in_flight(first: AsyncBulkhead, second: AsyncBulkhead) -> bool:
    first_status, second_status = await asyncio.gather(
        first.status(),
        second.status(),
    )
    return first_status.in_flight == 1 and second_status.in_flight == 1


async def both_closed(first: AsyncBulkhead, second: AsyncBulkhead) -> bool:
    first_status, second_status = await asyncio.gather(
        first.status(),
        second.status(),
    )
    return first_status.is_closed and second_status.is_closed
