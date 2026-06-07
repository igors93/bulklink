from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead, BulkheadClosedError
from tests.helpers import eventually, install_observable_lock, wait_for_lock_waiters
from tests.invariants import assert_bulkhead_consistent


async def test_close_rejects_queued_and_future_work_but_not_active_work() -> None:
    gate = AsyncBulkhead(label="shutdown", parallelism=1, waiting_room=2)
    release = asyncio.Event()
    active_completed = False

    async def hold() -> None:
        nonlocal active_completed
        async with gate.slot():
            await release.wait()
            active_completed = True

    active = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: waiting(gate, 1))

    await gate.close()

    with pytest.raises(BulkheadClosedError):
        await queued

    with pytest.raises(BulkheadClosedError):
        async with gate.slot():
            pass

    assert not active.done()
    current = await gate.status()
    assert current.is_closed
    assert not current.is_drained

    release.set()
    await active
    assert active_completed
    await gate.wait_closed()

    current = await gate.status()
    assert current.closed_while_waiting_total == 1
    assert current.closed_before_queue_total == 1
    assert current.closed_total == 2
    assert current.rejected_total == 2
    assert current.in_flight == 0
    assert current.is_drained
    await assert_bulkhead_consistent(gate)


async def test_close_is_idempotent_and_empty_bulkhead_drains_immediately() -> None:
    gate = AsyncBulkhead(label="idempotent", parallelism=1)

    await gate.close()
    await gate.close()
    await gate.wait_closed()
    await gate.wait_closed()

    current = await gate.status()
    assert current.is_closed
    assert current.is_drained
    assert current.closed_before_queue_total == 0
    assert current.closed_while_waiting_total == 0
    assert current.closed_total == 0
    await assert_bulkhead_consistent(gate)


async def test_wait_closed_started_before_close_blocks_until_closing() -> None:
    gate = AsyncBulkhead(label="pre-close-wait", parallelism=1)

    waiter = asyncio.create_task(gate.wait_closed())
    await asyncio.sleep(0)
    assert not waiter.done()

    await gate.close()
    await asyncio.wait_for(waiter, timeout=0.5)

    assert (await gate.status()).is_drained
    await assert_bulkhead_consistent(gate)


async def test_wait_closed_blocks_until_active_work_finishes() -> None:
    gate = AsyncBulkhead(label="active-drain", parallelism=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))

    await gate.close()
    waiter = asyncio.create_task(gate.wait_closed())
    await asyncio.sleep(0)

    assert not waiter.done()
    assert not active.done()

    release.set()
    await active
    await asyncio.wait_for(waiter, timeout=0.5)

    current = await gate.status()
    assert current.is_drained
    assert current.finished_total == 1
    await assert_bulkhead_consistent(gate)


async def test_multiple_wait_closed_callers_are_independent() -> None:
    gate = AsyncBulkhead(label="many-drain-waiters", parallelism=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))
    await gate.close()

    waiters = [asyncio.create_task(gate.wait_closed()) for _ in range(3)]
    await asyncio.sleep(0)
    assert all(not waiter.done() for waiter in waiters)

    waiters[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiters[0]

    assert not waiters[1].done()
    assert not waiters[2].done()

    release.set()
    await active
    await asyncio.gather(*waiters[1:])

    assert (await gate.status()).is_drained
    await assert_bulkhead_consistent(gate)


async def test_close_and_wait_closes_admission_and_waits_for_active_work() -> None:
    gate = AsyncBulkhead(label="close-and-wait", parallelism=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))

    closing = asyncio.create_task(gate.close_and_wait())
    await eventually(lambda: is_closed(gate))

    assert not closing.done()
    with pytest.raises(BulkheadClosedError):
        await gate.execute_now(asyncio.sleep, 0)

    release.set()
    await active
    await asyncio.wait_for(closing, timeout=0.5)

    current = await gate.status()
    assert current.is_closed
    assert current.is_drained
    await assert_bulkhead_consistent(gate)


async def test_cancelling_close_and_wait_does_not_cancel_active_work() -> None:
    gate = AsyncBulkhead(label="cancel-close-wait", parallelism=1)
    release = asyncio.Event()
    active_finished = False

    async def hold() -> None:
        nonlocal active_finished
        async with gate.slot():
            await release.wait()
            active_finished = True

    active = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))

    closing = asyncio.create_task(gate.close_and_wait())
    await eventually(lambda: is_closed(gate))
    closing.cancel()

    with pytest.raises(asyncio.CancelledError):
        await closing

    assert not active.done()
    current = await gate.status()
    assert current.is_closed
    assert not current.is_drained

    release.set()
    await active
    await gate.wait_closed()

    assert active_finished
    assert (await gate.status()).is_drained
    await assert_bulkhead_consistent(gate)


async def test_close_and_wait_finishes_closing_before_propagating_cancellation() -> None:
    gate = AsyncBulkhead(label="cancel-during-close", parallelism=1)
    lock = install_observable_lock(gate)

    await lock.acquire()
    closing = asyncio.create_task(gate.close_and_wait())
    await wait_for_lock_waiters(lock, 1)

    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done()

    lock.release()

    with pytest.raises(asyncio.CancelledError):
        await closing

    current = await gate.status()
    assert current.is_closed
    assert current.is_drained
    await assert_bulkhead_consistent(gate)


async def in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected


async def is_closed(gate: AsyncBulkhead) -> bool:
    return (await gate.status()).is_closed
