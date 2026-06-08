from __future__ import annotations

import asyncio

from bulklink import WeightedBulkhead


async def test_weighted_mixed_cost_stress_does_not_leak_capacity() -> None:
    gate = WeightedBulkhead(
        label="weighted-stress",
        capacity=12,
        waiting_room=80,
        wait_limit=2.0,
    )
    active_units = 0
    peak_units = 0
    lock = asyncio.Lock()

    async def work(cost: int) -> int:
        nonlocal active_units, peak_units
        async with gate.slot(cost):
            async with lock:
                active_units += cost
                peak_units = max(peak_units, active_units)
                assert active_units <= 12
            await asyncio.sleep(0)
            async with lock:
                active_units -= cost
            return cost

    costs = tuple((index % 4) + 1 for index in range(60))
    results = await asyncio.wait_for(
        asyncio.gather(*(work(cost) for cost in costs)),
        timeout=10.0,
    )

    assert tuple(results) == costs
    assert peak_units <= 12
    current = await gate.status()
    assert current.used == 0
    assert current.in_flight == 0
    assert current.waiting == 0
    assert current.waiting_units == 0
    assert current.admitted_units_total == sum(costs)
    assert current.finished_units_total == sum(costs)
