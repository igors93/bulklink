from __future__ import annotations

import asyncio

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
    BulkheadQueueTimeoutError,
    BulkheadSaturatedError,
)
from tests.helpers import eventually, install_observable_lock, wait_for_lock_waiters
from tests.invariants import assert_bulkhead_consistent


async def test_saturation_is_an_immediate_rejection_not_a_queue_outcome() -> None:
    gate = AsyncBulkhead(label="saturation-metrics", parallelism=1, waiting_room=0)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    with pytest.raises(BulkheadSaturatedError):
        async with gate.slot():
            pass

    current = await gate.status()
    assert current.saturated_total == 1
    assert current.queued_total == 0
    assert current.settled_waiting_total == 0
    assert current.rejected_total == 1

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_closed_metrics_distinguish_queue_and_pre_queue_rejections() -> None:
    gate = AsyncBulkhead(label="closed-metrics", parallelism=1, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await gate.close()

    with pytest.raises(BulkheadClosedError):
        await queued
    with pytest.raises(BulkheadClosedError):
        await gate.execute(asyncio.sleep, 0)

    current = await gate.status()
    assert current.closed_while_waiting_total == 1
    assert current.closed_before_queue_total == 1
    assert current.closed_total == 2
    assert current.settled_waiting_total == 1
    assert current.rejected_total == 2

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_queue_cancellation_is_withdrawal_not_rejection() -> None:
    gate = AsyncBulkhead(label="withdrawal-metrics", parallelism=1, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))
    queued.cancel()

    with pytest.raises(asyncio.CancelledError):
        await queued

    current = await gate.status()
    assert current.cancelled_while_waiting_total == 1
    assert current.settled_waiting_total == 1
    assert current.rejected_total == 0

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_timeout_is_a_settled_queue_rejection() -> None:
    gate = AsyncBulkhead(
        label="expiration-metrics",
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

    with pytest.raises(BulkheadQueueTimeoutError):
        await gate.execute(asyncio.sleep, 0)

    current = await gate.status()
    assert current.expired_total == 1
    assert current.settled_waiting_total == 1
    assert current.rejected_total == 1

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_cancelled_handoff_is_accounted_as_abandoned_admission() -> None:
    gate = AsyncBulkhead(label="abandoned-metrics", parallelism=1, waiting_room=1)
    lock = install_observable_lock(gate)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        release.set()
        await wait_for_lock_waiters(lock, 1)
        queued.cancel()
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    await active
    with pytest.raises(asyncio.CancelledError):
        await queued

    current = await gate.status()
    assert current.admitted_total == 2
    assert current.admitted_from_queue_total == 1
    assert current.abandoned_after_admission_total == 1
    assert current.finished_total == 1
    assert current.in_flight == 0
    assert current.cancelled_while_waiting_total == 0
    await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected


async def test_user_exception_still_finishes_the_admitted_operation() -> None:
    gate = AsyncBulkhead(label="exception-accounting", parallelism=1)

    async def fail() -> None:
        raise LookupError("missing")

    with pytest.raises(LookupError):
        await gate.execute(fail)

    current = await gate.status()
    assert current.admitted_total == 1
    assert current.finished_total == 1
    assert current.abandoned_after_admission_total == 0
    assert current.in_flight == 0
    await assert_bulkhead_consistent(gate)


async def test_cancellation_inside_the_protected_block_counts_as_finished() -> None:
    gate = AsyncBulkhead(label="body-cancellation-accounting", parallelism=1)
    entered = asyncio.Event()

    async def block() -> None:
        async with gate.slot():
            entered.set()
            await asyncio.Event().wait()

    running = asyncio.create_task(block())
    await entered.wait()
    running.cancel()

    with pytest.raises(asyncio.CancelledError):
        await running

    current = await gate.status()
    assert current.admitted_total == 1
    assert current.finished_total == 1
    assert current.abandoned_after_admission_total == 0
    assert current.in_flight == 0
    await assert_bulkhead_consistent(gate)
