from __future__ import annotations

import asyncio

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
    BulkheadEvent,
    BulkheadEventKind,
)
from tests.helpers import eventually, install_observable_lock, wait_for_lock_waiters
from tests.invariants import assert_bulkhead_consistent


@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "2", None])
async def test_resize_rejects_invalid_capacity(value: object) -> None:
    gate = AsyncBulkhead(label="resize-invalid", parallelism=2)

    with pytest.raises(ValueError, match="positive integer"):
        await gate.resize(value)  # type: ignore[arg-type]

    assert gate.parallelism == 2
    await assert_bulkhead_consistent(gate)


async def test_increasing_capacity_admits_waiters_in_fifo_order() -> None:
    gate = AsyncBulkhead(label="resize-up", parallelism=1, waiting_room=3)
    active_release = asyncio.Event()
    worker_releases = [asyncio.Event() for _ in range(3)]
    entered = [asyncio.Event() for _ in range(3)]

    async def hold_active() -> None:
        async with gate.slot():
            await active_release.wait()

    async def queued_worker(index: int) -> None:
        async with gate.slot():
            entered[index].set()
            await worker_releases[index].wait()

    active = asyncio.create_task(hold_active())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = []
    for index in range(3):
        queued.append(asyncio.create_task(queued_worker(index)))
        await eventually(lambda index=index: has_waiting(gate, index + 1))

    await gate.resize(3)

    await eventually(lambda: entered[0].is_set() and entered[1].is_set())
    assert not entered[2].is_set()

    current = await gate.status()
    assert current.parallelism == 3
    assert current.in_flight == 3
    assert current.waiting == 1

    active_release.set()
    await active
    await eventually(entered[2].is_set)

    for release in worker_releases:
        release.set()
    await asyncio.gather(*queued)

    final = await gate.status()
    assert final.in_flight == 0
    assert final.waiting == 0
    assert final.admitted_from_queue_total == 3
    await assert_bulkhead_consistent(gate)


async def test_reducing_capacity_drains_without_cancelling_active_work() -> None:
    gate = AsyncBulkhead(label="resize-down", parallelism=3, waiting_room=1)
    releases = [asyncio.Event() for _ in range(3)]
    queued_entered = asyncio.Event()
    queued_release = asyncio.Event()

    async def hold(index: int) -> None:
        async with gate.slot():
            await releases[index].wait()

    async def queued_worker() -> None:
        async with gate.slot():
            queued_entered.set()
            await queued_release.wait()

    active = [asyncio.create_task(hold(index)) for index in range(3)]
    await eventually(lambda: has_in_flight(gate, 3))

    queued = asyncio.create_task(queued_worker())
    await eventually(lambda: has_waiting(gate, 1))

    await gate.resize(1)

    reduced = await gate.status()
    assert reduced.parallelism == 1
    assert reduced.in_flight == 3
    assert reduced.capacity_excess == 2
    assert reduced.is_over_capacity
    assert reduced.utilization == 3.0
    assert not queued_entered.is_set()

    releases[0].set()
    await active[0]
    await eventually(lambda: has_in_flight(gate, 2))
    assert not queued_entered.is_set()

    releases[1].set()
    await active[1]
    await eventually(lambda: has_in_flight(gate, 1))
    assert not queued_entered.is_set()

    releases[2].set()
    await active[2]
    await eventually(queued_entered.is_set)

    admitted = await gate.status()
    assert admitted.in_flight == 1
    assert admitted.waiting == 0
    assert not admitted.is_over_capacity

    queued_release.set()
    await queued
    await assert_bulkhead_consistent(gate)


async def test_resize_emits_one_summary_event_before_fifo_admissions() -> None:
    gate = AsyncBulkhead(label="resize-events", parallelism=1, waiting_room=2)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)
    active_release = asyncio.Event()
    worker_release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await active_release.wait()

    async def worker() -> None:
        async with gate.slot():
            await worker_release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    queued = asyncio.create_task(worker())
    await eventually(lambda: has_waiting(gate, 1))

    events.clear()
    await gate.resize(2)
    await eventually(lambda: has_waiting(gate, 0))

    assert [event.kind for event in events] == [
        BulkheadEventKind.RESIZED,
        BulkheadEventKind.ADMITTED,
    ]
    resized, admitted = events
    assert resized.previous_parallelism == 1
    assert resized.parallelism == 2
    assert resized.affected_waiters == 1
    assert admitted.from_queue
    assert resized.occurred_at == admitted.occurred_at

    worker_release.set()
    active_release.set()
    await asyncio.gather(active, queued)
    await assert_bulkhead_consistent(gate)


async def test_resizing_to_current_capacity_is_idempotent() -> None:
    gate = AsyncBulkhead(label="resize-same", parallelism=2)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)

    await gate.resize(2)

    assert events == []
    assert gate.parallelism == 2
    await assert_bulkhead_consistent(gate)


async def test_resize_after_close_is_rejected_without_changing_metrics() -> None:
    gate = AsyncBulkhead(label="resize-closed", parallelism=2)
    await gate.close_and_wait()
    before = await gate.status()

    with pytest.raises(BulkheadClosedError):
        await gate.resize(4)

    after = await gate.status()
    assert after == before
    assert gate.parallelism == 2
    await assert_bulkhead_consistent(gate)


async def test_cancellation_after_resize_handoff_returns_the_new_slot() -> None:
    gate = AsyncBulkhead(label="resize-cancel", parallelism=1, waiting_room=1)
    active_release = asyncio.Event()
    calls = 0

    async def hold() -> None:
        async with gate.slot():
            await active_release.wait()

    async def should_not_run() -> None:
        nonlocal calls
        calls += 1

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    queued = asyncio.create_task(gate.execute(should_not_run))
    await eventually(lambda: has_waiting(gate, 1))

    def cancel_on_resize(event: BulkheadEvent) -> None:
        if event.kind is BulkheadEventKind.RESIZED:
            queued.cancel()

    gate.add_event_handler(cancel_on_resize)
    await gate.resize(2)

    with pytest.raises(asyncio.CancelledError):
        await queued

    current = await gate.status()
    assert calls == 0
    assert current.parallelism == 2
    assert current.in_flight == 1
    assert current.waiting == 0
    assert current.abandoned_after_admission_total == 1

    active_release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_cancelled_resize_waiting_for_lock_does_not_change_capacity() -> None:
    gate = AsyncBulkhead(label="resize-cancelled", parallelism=2)
    lock = install_observable_lock(gate)

    await lock.acquire()
    resize = asyncio.create_task(gate.resize(5))
    try:
        await wait_for_lock_waiters(lock, 1)
        resize.cancel()
    finally:
        lock.release()

    with pytest.raises(asyncio.CancelledError):
        await resize

    assert gate.parallelism == 2
    await assert_bulkhead_consistent(gate)


async def test_close_winning_the_lock_rejects_a_concurrent_resize() -> None:
    gate = AsyncBulkhead(label="resize-close-race", parallelism=2)
    lock = install_observable_lock(gate)

    await lock.acquire()
    closing = asyncio.create_task(gate.close())
    await wait_for_lock_waiters(lock, 1)
    resizing = asyncio.create_task(gate.resize(4))
    await wait_for_lock_waiters(lock, 2)
    lock.release()

    await closing
    with pytest.raises(BulkheadClosedError):
        await resizing

    current = await gate.status()
    assert current.is_closed
    assert current.is_drained
    assert current.parallelism == 2
    await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
