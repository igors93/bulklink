from __future__ import annotations

import asyncio
from contextlib import suppress

from bulklink import AsyncBulkhead


async def test_waiting_between_attempts_happens_outside_bulkhead_slot() -> None:
    gate = AsyncBulkhead(label="dependency", parallelism=1)
    calls = 0

    async def one_attempt() -> str:
        nonlocal calls
        calls += 1
        async with gate.slot():
            if calls == 1:
                raise ConnectionError("temporary")
            return "ok"

    with suppress(ConnectionError):
        await one_attempt()

    between_attempts = await gate.status()
    assert between_attempts.in_flight == 0

    await asyncio.sleep(0)
    assert await one_attempt() == "ok"
    assert calls == 2
