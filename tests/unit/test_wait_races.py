from __future__ import annotations

import asyncio

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
    BulkheadQueueTimeoutError,
)
from tests.helpers import (
    eventually,
    install_observable_lock,
    wait_for_lock_waiters,
)
from tests.invariants import assert_bulkhead_consistent


async def test_repeated_cancellation_while_queued_does_not_leak_capacity() -> None:
    gate = AsyncBulkhead(
        label="repeated-cancellation",
        parallelism=1,
        waiting_room=1,
    )
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    active = asyncio.create_task(hold_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        queued.cancel()
        await wait_for_lock_waiters(lock, 1)

        for _ in range(4):
            queued.cancel()
            await asyncio.sleep(0)

        assert not queued.done()
    finally:
        lock.release()

    with pytest.raises(asyncio.CancelledError):
        await queued

    release_active.set()
    await active

    current = await gate.status()
    assert current.in_flight == 0
    assert current.waiting == 0
    assert current.cancelled_total == 1
    assert terminal_wait_outcomes(current) == 1
    await assert_bulkhead_consistent(gate)


async def test_cancellation_after_slot_handoff_returns_transferred_capacity() -> None:
    gate = AsyncBulkhead(
        label="handoff-cancellation",
        parallelism=1,
        waiting_room=1,
    )
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    active = asyncio.create_task(hold_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        release_active.set()
        await wait_for_lock_waiters(lock, 1)

        queued.cancel()
        await wait_for_lock_waiters(lock, 2)

        for _ in range(3):
            queued.cancel()
            await asyncio.sleep(0)
    finally:
        lock.release()

    await active
    with pytest.raises(asyncio.CancelledError):
        await queued

    current = await gate.status()
    assert current.in_flight == 0
    assert current.waiting == 0
    assert current.admitted_total == 2
    assert current.finished_total == 1
    assert current.cancelled_total == 0
    await assert_bulkhead_consistent(gate)


async def test_close_wins_timeout_race_without_double_counting() -> None:
    gate = AsyncBulkhead(
        label="close-before-timeout",
        parallelism=1,
        waiting_room=1,
        wait_limit=0.03,
    )
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    active = asyncio.create_task(hold_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        closing = asyncio.create_task(gate.close())
        await wait_for_lock_waiters(lock, 1)
        await asyncio.sleep(0.05)
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    await closing
    with pytest.raises(BulkheadClosedError):
        await queued

    release_active.set()
    await active

    current = await gate.status()
    assert current.closed_total == 1
    assert current.expired_total == 0
    assert current.rejected_total == 1
    assert terminal_wait_outcomes(current) == 1
    await assert_bulkhead_consistent(gate)


async def test_timeout_wins_close_race_without_double_counting() -> None:
    gate = AsyncBulkhead(
        label="timeout-before-close",
        parallelism=1,
        waiting_room=1,
        wait_limit=0.03,
    )
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    active = asyncio.create_task(hold_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        await asyncio.sleep(0.05)
        await wait_for_lock_waiters(lock, 1)

        closing = asyncio.create_task(gate.close())
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    with pytest.raises(BulkheadQueueTimeoutError):
        await queued
    await closing

    release_active.set()
    await active

    current = await gate.status()
    assert current.expired_total == 1
    assert current.closed_total == 0
    assert current.rejected_total == 1
    assert terminal_wait_outcomes(current) == 1
    await assert_bulkhead_consistent(gate)


async def test_close_wins_cancellation_race_without_double_counting() -> None:
    gate = AsyncBulkhead(
        label="close-cancellation",
        parallelism=1,
        waiting_room=1,
    )
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    active = asyncio.create_task(hold_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        closing = asyncio.create_task(gate.close())
        await wait_for_lock_waiters(lock, 1)

        queued.cancel()
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    await closing
    with pytest.raises(asyncio.CancelledError):
        await queued

    release_active.set()
    await active

    current = await gate.status()
    assert current.closed_total == 1
    assert current.cancelled_total == 0
    assert current.rejected_total == 1
    assert terminal_wait_outcomes(current) == 1
    await assert_bulkhead_consistent(gate)


async def test_cancellation_wins_close_race_without_double_counting() -> None:
    gate = AsyncBulkhead(
        label="cancellation-close",
        parallelism=1,
        waiting_room=1,
    )
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    active = asyncio.create_task(hold_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        queued.cancel()
        await wait_for_lock_waiters(lock, 1)

        closing = asyncio.create_task(gate.close())
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    with pytest.raises(asyncio.CancelledError):
        await queued
    await closing

    release_active.set()
    await active

    current = await gate.status()
    assert current.cancelled_total == 1
    assert current.closed_total == 0
    assert current.rejected_total == 0
    assert terminal_wait_outcomes(current) == 1
    await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected


def terminal_wait_outcomes(status: object) -> int:
    return status.cancelled_total + status.expired_total + status.closed_total


async def test_handoff_wins_timeout_race_without_expiration() -> None:
    gate = AsyncBulkhead(
        label="handoff-before-timeout",
        parallelism=1,
        waiting_room=1,
        wait_limit=0.03,
    )
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    active = asyncio.create_task(hold_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        release_active.set()
        await wait_for_lock_waiters(lock, 1)
        await asyncio.sleep(0.05)
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    await asyncio.gather(active, queued)

    current = await gate.status()
    assert current.admitted_total == 2
    assert current.expired_total == 0
    assert current.in_flight == 0
    await assert_bulkhead_consistent(gate)


async def test_timeout_wins_handoff_race_without_capacity_leak() -> None:
    gate = AsyncBulkhead(
        label="timeout-before-handoff",
        parallelism=1,
        waiting_room=1,
        wait_limit=0.03,
    )
    lock = install_observable_lock(gate)
    release_active = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_active.wait()

    active = asyncio.create_task(hold_slot())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        await asyncio.sleep(0.05)
        await wait_for_lock_waiters(lock, 1)
        release_active.set()
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    with pytest.raises(BulkheadQueueTimeoutError):
        await queued
    await active

    current = await gate.status()
    assert current.admitted_total == 1
    assert current.expired_total == 1
    assert current.finished_total == 1
    assert current.in_flight == 0
    await assert_bulkhead_consistent(gate)


async def test_repeated_cancellation_during_slot_release_cannot_leak_capacity() -> None:
    gate = AsyncBulkhead(label="release-cancellation", parallelism=1)
    lock = install_observable_lock(gate)
    entered = asyncio.Event()
    finish_body = asyncio.Event()

    async def operation() -> None:
        async with gate.slot():
            entered.set()
            await finish_body.wait()

    running = asyncio.create_task(operation())
    await entered.wait()

    await lock.acquire()
    try:
        finish_body.set()
        await wait_for_lock_waiters(lock, 1)

        for _ in range(5):
            running.cancel()
            await asyncio.sleep(0)

        assert not running.done()
    finally:
        lock.release()

    with pytest.raises(asyncio.CancelledError):
        await running

    current = await gate.status()
    assert current.in_flight == 0
    assert current.finished_total == 1
    await assert_bulkhead_consistent(gate)
