from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead
from tests.helpers import eventually


async def test_cancelling_queued_operation_removes_it() -> None:
    gate = AsyncBulkhead(label="cancel-queue", parallelism=1, waiting_room=2)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: waiting(gate, 1))

    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued

    await eventually(lambda: waiting(gate, 0))
    release.set()
    await active

    async with gate.slot():
        pass

    current = await gate.status()
    assert current.cancelled_total == 1
    assert current.in_flight == 0


async def test_cancellation_inside_protected_code_releases_slot() -> None:
    gate = AsyncBulkhead(label="cancel-body", parallelism=1)
    entered = asyncio.Event()

    async def body() -> None:
        async with gate.slot():
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(body())
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    async with gate.slot():
        pass

    assert (await gate.status()).in_flight == 0


async def test_many_cancellation_races_do_not_leak_slots() -> None:
    gate = AsyncBulkhead(label="race", parallelism=2, waiting_room=100)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    holders = [asyncio.create_task(hold()) for _ in range(2)]
    await eventually(lambda: in_flight(gate, 2))

    waiters = [asyncio.create_task(gate.execute(asyncio.sleep, 0)) for _ in range(40)]
    await eventually(lambda: waiting(gate, 40))

    for task in waiters[::2]:
        task.cancel()

    release.set()
    results = await asyncio.gather(*holders, *waiters, return_exceptions=True)
    assert sum(isinstance(item, asyncio.CancelledError) for item in results) == 20

    current = await gate.status()
    assert current.in_flight == 0
    assert current.cancelled_total == 20


async def in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
