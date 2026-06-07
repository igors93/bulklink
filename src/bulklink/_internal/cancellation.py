"""Helpers for completing critical cleanup during task cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


async def complete_cleanup(action: Coroutine[Any, Any, T]) -> T:
    """Finish one cleanup coroutine despite repeated cancellation requests.

    Cancellation is remembered and propagated only after the cleanup task has
    finished. An exception raised by the cleanup itself takes precedence because
    it indicates that internal state may not have been restored.
    """
    cleanup_task = asyncio.create_task(action)
    cancellation: asyncio.CancelledError | None = None

    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error

    result = cleanup_task.result()

    if cancellation is not None:
        raise cancellation

    return result
