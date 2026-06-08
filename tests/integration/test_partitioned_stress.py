from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest

from bulklink import PartitionedBulkhead, PartitionLimitError


@pytest.mark.stress
async def test_partitioned_eviction_pressure_drains_without_leaks() -> None:
    """Many distinct keys against a small partition cap forces concurrent evictions.

    Validates:
    - partition_count <= max_partitions at all observable points
    - leased_operations == 0 after all work settles
    - active_partitions == 0 after all work settles
    - close_and_wait() completes without hanging
    - _reserved_slots == 0 after shutdown
    """
    max_p = 3
    num_keys = 20
    concurrency = 40

    gate = PartitionedBulkhead(
        label="eviction-stress",
        parallelism=2,
        waiting_room=5,
        wait_limit=2.0,
        max_partitions=max_p,
        idle_timeout=0.01,
    )

    async def work(key: int) -> None:
        with suppress(PartitionLimitError):
            await gate.execute(f"key-{key % num_keys}", asyncio.sleep, 0)

    tasks = [asyncio.create_task(work(i)) for i in range(concurrency)]
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=15.0)

    # Allow idle cleanup between bursts.
    await asyncio.sleep(0.02)
    await gate.cleanup_idle()

    status = await gate.status()
    assert status.partition_count <= max_p
    assert status.leased_operations == 0
    assert status.active_partitions == 0

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._reserved_slots == 0

    await asyncio.wait_for(gate.close_and_wait(), timeout=5.0)


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
