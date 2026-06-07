"""Helpers for completing critical cleanup during task cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any


async def complete_cleanup(action: Coroutine[Any, Any, None]) -> None:
    """Finish a cleanup coroutine before propagating a new cancellation.

    Normal cancellation inside protected user code already allows ``__aexit__`` to
    run. This helper also protects slot release from a second cancellation arriving
    while cleanup is waiting for the coordinator lock.
    """
    task = asyncio.create_task(action)
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise
