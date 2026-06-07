from __future__ import annotations

import asyncio

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
    BulkheadSaturatedError,
)
from tests.helpers import (
    eventually,
    install_observable_lock,
    wait_for_lock_waiters,
)
from tests.invariants import assert_bulkhead_consistent


async def test_execute_now_runs_without_using_the_waiting_room() -> None:
    gate = AsyncBulkhead(label="immediate-execution", parallelism=1, waiting_room=4)

    async def double(value: int) -> int:
        return value * 2

    assert await gate.execute_now(double, 4) == 8

    current = await gate.status()
    assert current.admitted_total == 1
    assert current.direct_admitted_total == 1
    assert current.queued_total == 0
    assert current.waiting == 0
    assert current.finished_total == 1
    await assert_bulkhead_consistent(gate)


async def test_slot_now_releases_capacity_after_the_protected_block() -> None:
    gate = AsyncBulkhead(label="immediate-slot", parallelism=1)

    async with gate.slot_now():
        current = await gate.status()
        assert current.in_flight == 1
        assert current.queued_total == 0

    current = await gate.status()
    assert current.in_flight == 0
    assert current.finished_total == 1
    await assert_bulkhead_consistent(gate)


async def test_execute_now_rejects_without_invoking_user_code_or_queueing() -> None:
    gate = AsyncBulkhead(label="immediate-rejection", parallelism=1, waiting_room=5)
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

    with pytest.raises(BulkheadSaturatedError):
        await gate.execute_now(should_not_run)

    assert calls == 0
    current = await gate.status()
    assert current.saturated_total == 1
    assert current.rejected_total == 1
    assert current.queued_total == 0
    assert current.waiting == 0

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_immediate_admission_never_overtakes_an_existing_waiter() -> None:
    gate = AsyncBulkhead(label="immediate-fairness", parallelism=1, waiting_room=1)
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()
    release_waiter = asyncio.Event()
    waiter_entered = asyncio.Event()
    immediate_calls = 0

    async def hold_active_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    async def queued_operation() -> None:
        waiter_entered.set()
        await release_waiter.wait()

    async def immediate_operation() -> None:
        nonlocal immediate_calls
        immediate_calls += 1

    active = asyncio.create_task(hold_active_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(queued_operation))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        release_active.set()
        await wait_for_lock_waiters(lock, 1)

        immediate = asyncio.create_task(gate.execute_now(immediate_operation))
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    await active
    await waiter_entered.wait()

    with pytest.raises(BulkheadSaturatedError):
        await immediate

    assert immediate_calls == 0
    current = await gate.status()
    assert current.in_flight == 1
    assert current.waiting == 0
    assert current.admitted_from_queue_total == 1
    assert current.saturated_total == 1

    release_waiter.set()
    await queued
    await assert_bulkhead_consistent(gate)


async def test_execute_now_after_close_is_a_closed_rejection() -> None:
    gate = AsyncBulkhead(label="immediate-closed", parallelism=1)
    calls = 0

    async def should_not_run() -> None:
        nonlocal calls
        calls += 1

    await gate.close()

    with pytest.raises(BulkheadClosedError):
        await gate.execute_now(should_not_run)

    assert calls == 0
    current = await gate.status()
    assert current.closed_before_queue_total == 1
    assert current.saturated_total == 0
    assert current.rejected_total == 1
    await assert_bulkhead_consistent(gate)


async def test_execute_now_propagates_user_exception_and_releases_the_slot() -> None:
    gate = AsyncBulkhead(label="immediate-error", parallelism=1)
    original = LookupError("missing")

    async def fail() -> None:
        raise original

    with pytest.raises(LookupError) as caught:
        await gate.execute_now(fail)

    assert caught.value is original
    current = await gate.status()
    assert current.admitted_total == 1
    assert current.finished_total == 1
    assert current.in_flight == 0
    await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
