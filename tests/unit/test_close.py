from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead, BulkheadClosedError
from tests.helpers import eventually


async def test_close_rejects_queued_and_future_work_but_not_active_work() -> None:
    gate = AsyncBulkhead(label="shutdown", parallelism=1, waiting_room=2)
    release = asyncio.Event()
    active_completed = False

    async def hold() -> None:
        nonlocal active_completed
        async with gate.slot():
            await release.wait()
            active_completed = True

    active = asyncio.create_task(hold())
    await eventually(lambda: in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: waiting(gate, 1))

    await gate.close()

    with pytest.raises(BulkheadClosedError):
        await queued

    with pytest.raises(BulkheadClosedError):
        async with gate.slot():
            pass

    assert not active.done()
    release.set()
    await active
    assert active_completed

    current = await gate.status()
    assert current.is_closed
    assert current.closed_total == 2
    assert current.in_flight == 0


async def test_close_is_idempotent() -> None:
    gate = AsyncBulkhead(label="idempotent", parallelism=1)

    await gate.close()
    await gate.close()

    current = await gate.status()
    assert current.is_closed
    assert current.closed_total == 0


async def in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
