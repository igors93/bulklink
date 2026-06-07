from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead
from tests.helpers import eventually


async def test_waiters_are_admitted_in_fifo_order() -> None:
    gate = AsyncBulkhead(label="fifo", parallelism=1, waiting_room=5)
    release_first = asyncio.Event()
    order: list[int] = []

    async def first() -> None:
        async with gate.slot():
            await release_first.wait()

    async def queued(item: int) -> None:
        async with gate.slot():
            order.append(item)
            await asyncio.sleep(0)

    active = asyncio.create_task(first())
    await eventually(lambda: in_flight(gate, 1))

    queued_tasks = []
    for item in range(4):
        queued_tasks.append(asyncio.create_task(queued(item)))
        await eventually(lambda expected=item + 1: waiting(gate, expected))

    release_first.set()
    await asyncio.gather(active, *queued_tasks)

    assert order == [0, 1, 2, 3]


async def in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
