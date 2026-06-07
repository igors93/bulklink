from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead
from tests.invariants import assert_bulkhead_consistent


async def test_repeated_resizing_preserves_capacity_and_queue_invariants() -> None:
    for round_index in range(20):
        await run_resize_round(round_index)


async def run_resize_round(round_index: int) -> None:
    gate = AsyncBulkhead(
        label=f"resize-stress-{round_index}",
        parallelism=3,
        waiting_room=60,
    )
    completed: list[int] = []

    async def work(index: int) -> None:
        async with gate.slot():
            await asyncio.sleep(0)
            completed.append(index)

    async def resize_repeatedly() -> None:
        for capacity in (1, 5, 2, 6, 3, 1, 4):
            await gate.resize(capacity)
            await asyncio.sleep(0)

    workers = [asyncio.create_task(work(index)) for index in range(50)]
    resizer = asyncio.create_task(resize_repeatedly())
    await asyncio.gather(*workers, resizer)

    current = await gate.status()
    assert sorted(completed) == list(range(50))
    assert current.in_flight == 0
    assert current.waiting == 0
    assert current.admitted_total == 50
    assert current.finished_total == 50
    await assert_bulkhead_consistent(gate)
