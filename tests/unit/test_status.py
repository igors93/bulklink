from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead
from tests.helpers import eventually


async def test_initial_status() -> None:
    gate = AsyncBulkhead(
        label="payments",
        parallelism=2,
        waiting_room=3,
        wait_limit=1.0,
    )

    current = await gate.status()

    assert current.label == "payments"
    assert current.parallelism == 2
    assert current.waiting_room == 3
    assert current.in_flight == 0
    assert current.waiting == 0
    assert current.free_slots == 2
    assert not current.is_saturated
    assert current.utilization == 0.0
    assert current.queue_utilization == 0.0
    assert current.direct_admitted_total == 0
    assert current.closed_total == 0
    assert current.rejected_total == 0
    assert current.settled_waiting_total == 0
    assert current.average_wait_seconds == 0.0
    assert not current.is_closed


async def test_status_tracks_active_and_finished_operations() -> None:
    gate = AsyncBulkhead(label="workers", parallelism=2)

    async with gate.slot():
        active = await gate.status()
        assert active.in_flight == 1
        assert active.admitted_total == 1
        assert active.direct_admitted_total == 1
        assert active.peak_in_flight == 1
        assert active.utilization == 0.5
        assert not active.is_saturated

    finished = await gate.status()
    assert finished.in_flight == 0
    assert finished.finished_total == 1
    assert finished.abandoned_after_admission_total == 0
    assert finished.free_slots == 2


async def test_status_measures_admitted_queue_wait() -> None:
    gate = AsyncBulkhead(label="metrics", parallelism=1, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    first = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    second = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    queued = await gate.status()
    assert queued.queue_utilization == 1.0
    assert queued.settled_waiting_total == 0

    await asyncio.sleep(0.005)
    release.set()
    await asyncio.gather(first, second)

    current = await gate.status()
    assert current.admitted_from_queue_total == 1
    assert current.settled_waiting_total == 1
    assert current.cumulative_wait_seconds > 0
    assert current.longest_wait_seconds > 0
    assert current.average_wait_seconds > 0


async def test_queue_utilization_is_zero_without_a_waiting_room() -> None:
    gate = AsyncBulkhead(label="no-queue", parallelism=1, waiting_room=0)

    assert (await gate.status()).queue_utilization == 0.0


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
