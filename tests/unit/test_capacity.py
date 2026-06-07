from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead, BulkheadSaturatedError
from tests.helpers import eventually


async def test_never_exceeds_configured_parallelism() -> None:
    gate = AsyncBulkhead(label="workers", parallelism=3, waiting_room=50)
    active = 0
    highest = 0
    mutex = asyncio.Lock()

    async def work() -> None:
        nonlocal active, highest
        async with gate.slot():
            async with mutex:
                active += 1
                highest = max(highest, active)
            await asyncio.sleep(0.002)
            async with mutex:
                active -= 1

    await asyncio.gather(*(work() for _ in range(40)))

    assert highest == 3
    current = await gate.status()
    assert current.in_flight == 0
    assert current.finished_total == 40
    assert current.peak_in_flight == 3


async def test_zero_waiting_room_rejects_immediately() -> None:
    gate = AsyncBulkhead(label="no-queue", parallelism=1, waiting_room=0)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))

    with pytest.raises(BulkheadSaturatedError) as caught:
        async with gate.slot():
            pass

    assert caught.value.label == "no-queue"
    assert caught.value.in_flight == 1
    assert caught.value.waiting == 0

    release.set()
    await active


async def test_full_waiting_room_rejects_next_operation() -> None:
    gate = AsyncBulkhead(label="bounded", parallelism=1, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    first = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))

    second = asyncio.create_task(hold())
    await eventually(lambda: waiting(gate, 1))

    with pytest.raises(BulkheadSaturatedError):
        async with gate.slot():
            pass

    release.set()
    await asyncio.gather(first, second)

    current = await gate.status()
    assert current.saturated_total == 1
    assert current.rejected_total == 1


async def in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
