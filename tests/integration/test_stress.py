from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead


async def test_stress_many_operations_preserves_capacity_accounting() -> None:
    gate = AsyncBulkhead(
        label="stress",
        parallelism=7,
        waiting_room=500,
        wait_limit=5.0,
    )
    active = 0
    highest = 0
    mutex = asyncio.Lock()

    async def work(item: int) -> int:
        nonlocal active, highest
        async with gate.slot():
            async with mutex:
                active += 1
                highest = max(highest, active)
            await asyncio.sleep(0)
            async with mutex:
                active -= 1
            return item

    results = await asyncio.gather(*(work(item) for item in range(300)))

    assert results == list(range(300))
    assert highest == 7

    current = await gate.status()
    assert current.in_flight == 0
    assert current.waiting == 0
    assert current.admitted_total == 300
    assert current.finished_total == 300
    assert current.peak_in_flight == 7
    assert current.peak_waiting <= 500
