from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead, BulkheadSaturatedError
from tests.helpers import eventually
from tests.invariants import assert_bulkhead_consistent


async def test_invariants_hold_with_active_and_waiting_operations() -> None:
    gate = AsyncBulkhead(label="active-waiting", parallelism=2, waiting_room=3)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = [asyncio.create_task(hold()) for _ in range(2)]
    await eventually(lambda: has_in_flight(gate, 2))

    queued = [asyncio.create_task(hold()) for _ in range(3)]
    await eventually(lambda: has_waiting(gate, 3))

    await assert_bulkhead_consistent(gate)

    with pytest.raises(BulkheadSaturatedError):
        async with gate.slot():
            pass

    await assert_bulkhead_consistent(gate)

    release.set()
    await asyncio.gather(*active, *queued)
    await assert_bulkhead_consistent(gate)


async def test_invariants_hold_after_queue_cancellations() -> None:
    gate = AsyncBulkhead(label="cancelled-waiters", parallelism=1, waiting_room=20)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = [asyncio.create_task(gate.execute(asyncio.sleep, 0)) for _ in range(20)]
    await eventually(lambda: has_waiting(gate, 20))

    for task in queued[::2]:
        task.cancel()

    await asyncio.gather(*queued[::2], return_exceptions=True)
    await eventually(lambda: has_waiting(gate, 10))
    await assert_bulkhead_consistent(gate)

    release.set()
    await asyncio.gather(active, *queued[1::2])
    await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
