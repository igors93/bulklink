from __future__ import annotations

import asyncio

import pytest

from bulklink import PartitionedBulkhead


@pytest.mark.stress
async def test_partitioned_mixed_key_stress_drains_without_leaks() -> None:
    gate = PartitionedBulkhead(
        label="partitioned-stress",
        parallelism=3,
        waiting_room=20,
        wait_limit=2.0,
        max_partitions=12,
        idle_timeout=0.01,
    )

    async def work(index: int) -> int:
        await asyncio.sleep((index % 5) * 0.0005)
        return index

    tasks = [
        asyncio.create_task(gate.execute(f"tenant-{index % 12}", work, index))
        for index in range(240)
    ]
    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10.0)

    assert sorted(results) == list(range(240))
    current = await gate.status()
    assert current.partition_count <= current.max_partitions
    assert current.leased_operations == 0
    assert current.active_partitions == 0

    await asyncio.sleep(0.02)
    removed = await gate.cleanup_idle()
    assert removed == current.partition_count
    assert (await gate.status()).partition_count == 0

    await gate.close_and_wait()
