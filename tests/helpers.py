from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable


async def eventually(
    predicate: Callable[[], bool | Awaitable[bool]],
    *,
    timeout: float = 1.0,
) -> None:
    """Wait until a synchronous or asynchronous predicate becomes true."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while True:
        value = predicate()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return
        if loop.time() >= deadline:
            raise AssertionError("condition was not reached before timeout")
        await asyncio.sleep(0)
