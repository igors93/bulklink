from __future__ import annotations

import asyncio

import pytest

from bulklink._internal.cancellation import complete_cleanup


async def test_critical_cleanup_finishes_before_repeated_cancellation_propagates() -> None:
    started = asyncio.Event()
    allow_finish = asyncio.Event()
    finished = False

    async def cleanup() -> None:
        nonlocal finished
        started.set()
        await allow_finish.wait()
        finished = True

    task = asyncio.create_task(complete_cleanup(cleanup()))
    await started.wait()

    for _ in range(5):
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

    allow_finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert finished


async def test_cleanup_returns_its_result_without_cancellation() -> None:
    async def cleanup() -> str:
        await asyncio.sleep(0)
        return "finished"

    assert await complete_cleanup(cleanup()) == "finished"
