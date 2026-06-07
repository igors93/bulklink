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
    assert current.rejected_total == 0
    assert current.average_wait_seconds == 0.0
    assert not current.is_closed


async def test_status_tracks_active_and_finished_operations() -> None:
    gate = AsyncBulkhead(label="workers", parallelism=1)

    async with gate.slot():
        active = await gate.status()
        assert active.in_flight == 1
        assert active.admitted_total == 1
        assert active.peak_in_flight == 1
        assert active.is_saturated

    finished = await gate.status()
    assert finished.in_flight == 0
    assert finished.finished_total == 1
    assert finished.free_slots == 1


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
    await asyncio.sleep(0.005)
    release.set()
    await asyncio.gather(first, second)

    current = await gate.status()
    assert current.admitted_from_queue_total == 1
    assert current.cumulative_wait_seconds > 0
    assert current.longest_wait_seconds > 0
    assert current.average_wait_seconds > 0


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
