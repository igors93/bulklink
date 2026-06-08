from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead
from tests.invariants import assert_bulkhead_consistent


@pytest.mark.stress
async def test_resize_cancellation_and_handoff_stress_leaves_no_work_behind() -> None:
    async def scenario() -> None:
        gate = AsyncBulkhead(
            label="adversarial-stress",
            parallelism=7,
            waiting_room=160,
        )
        started = 0
        completed = 0

        async def work() -> None:
            nonlocal completed, started
            started += 1
            for _ in range(4):
                await asyncio.sleep(0)
            completed += 1

        tasks = [asyncio.create_task(gate.execute(work)) for _ in range(160)]

        async def resize_repeatedly() -> None:
            for capacity in (1, 12, 3, 8, 2, 10, 4, 7):
                await gate.resize(capacity)
                await asyncio.sleep(0)
                await assert_bulkhead_consistent(gate)

        resizer = asyncio.create_task(resize_repeatedly())
        for _ in range(3):
            await asyncio.sleep(0)

        for task in tasks[::11]:
            task.cancel()

        results = await asyncio.gather(*tasks, return_exceptions=True)
        await resizer
        await gate.close_and_wait()

        assert all(
            result is None or isinstance(result, asyncio.CancelledError) for result in results
        )
        cancelled = sum(isinstance(result, asyncio.CancelledError) for result in results)
        assert completed <= started
        assert started + cancelled >= len(tasks)

        status = await gate.status()
        assert status.in_flight == 0
        assert status.waiting == 0
        assert status.is_drained
        await assert_bulkhead_consistent(gate)

    await asyncio.wait_for(scenario(), timeout=10.0)
