from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead, BulkheadQueueTimeoutError
from tests.helpers import eventually


async def test_wait_deadline_expires_without_leaking_capacity() -> None:
    gate = AsyncBulkhead(
        label="reports",
        parallelism=1,
        waiting_room=1,
        wait_limit=0.03,
    )
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: one_active(gate))

    with pytest.raises(BulkheadQueueTimeoutError) as caught:
        async with gate.slot():
            pass

    assert caught.value.label == "reports"
    assert not isinstance(caught.value, TimeoutError)

    release.set()
    await active

    async with gate.slot():
        pass

    current = await gate.status()
    assert current.in_flight == 0
    assert current.expired_total == 1
    assert current.rejected_total == 1


async def one_active(gate: AsyncBulkhead) -> bool:
    return (await gate.status()).in_flight == 1
