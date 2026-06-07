from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead, BulkheadClosedError, BulkheadQueueTimeoutError
from tests.helpers import eventually
from tests.invariants import assert_bulkhead_consistent


async def test_execute_within_uses_a_shorter_limit_than_the_default() -> None:
    gate = AsyncBulkhead(
        label="shorter-limit",
        parallelism=1,
        waiting_room=1,
        wait_limit=1.0,
    )
    release = asyncio.Event()
    calls = 0

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    async def should_not_run() -> None:
        nonlocal calls
        calls += 1

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    with pytest.raises(BulkheadQueueTimeoutError) as caught:
        await gate.execute_within(0.02, should_not_run)

    assert caught.value.wait_limit == 0.02
    assert calls == 0
    current = await gate.status()
    assert current.expired_total == 1
    assert current.rejected_total == 1
    assert current.waiting == 0

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_execute_within_cannot_extend_the_default_limit() -> None:
    gate = AsyncBulkhead(
        label="bounded-limit",
        parallelism=1,
        waiting_room=1,
        wait_limit=0.02,
    )
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    with pytest.raises(BulkheadQueueTimeoutError) as caught:
        await gate.execute_within(1.0, asyncio.sleep, 0)

    assert caught.value.wait_limit == 0.02

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_execute_within_uses_the_requested_limit_when_default_is_unbounded() -> None:
    gate = AsyncBulkhead(
        label="requested-limit",
        parallelism=1,
        waiting_room=1,
    )
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    with pytest.raises(BulkheadQueueTimeoutError) as caught:
        await gate.execute_within(0.02, asyncio.sleep, 0)

    assert caught.value.wait_limit == 0.02

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_per_call_limit_applies_only_while_waiting_for_admission() -> None:
    gate = AsyncBulkhead(label="body-runtime", parallelism=1)

    async def slow_body() -> str:
        await asyncio.sleep(0.02)
        return "completed"

    assert await gate.execute_within(0.001, slow_body) == "completed"

    current = await gate.status()
    assert current.finished_total == 1
    assert current.expired_total == 0
    await assert_bulkhead_consistent(gate)


async def test_execute_within_does_not_consume_operation_wait_limit_keyword() -> None:
    gate = AsyncBulkhead(label="keyword-boundary", parallelism=1)

    async def operation(*, wait_limit: float) -> float:
        return wait_limit

    result = await gate.execute_within(0.5, operation, wait_limit=7.0)

    assert result == 7.0
    await assert_bulkhead_consistent(gate)


async def test_slot_within_after_close_is_a_closed_rejection() -> None:
    gate = AsyncBulkhead(label="closed-limit", parallelism=1)
    await gate.close()

    with pytest.raises(BulkheadClosedError):
        async with gate.slot_within(0.5):
            pass

    current = await gate.status()
    assert current.closed_before_queue_total == 1
    assert current.expired_total == 0
    await assert_bulkhead_consistent(gate)


async def test_slot_within_releases_capacity_after_success() -> None:
    gate = AsyncBulkhead(label="limited-slot", parallelism=1)

    async with gate.slot_within(0.5):
        assert (await gate.status()).in_flight == 1

    current = await gate.status()
    assert current.in_flight == 0
    assert current.finished_total == 1
    await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected
