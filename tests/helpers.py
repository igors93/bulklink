from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any


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


class ObservableLock:
    """Async lock that exposes its own waiter count for deterministic tests."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._waiting = 0

    @property
    def waiting(self) -> int:
        return self._waiting

    def locked(self) -> bool:
        return self._lock.locked()

    async def acquire(self) -> bool:
        if self._lock.locked():
            self._waiting += 1
            try:
                return await self._lock.acquire()
            finally:
                self._waiting -= 1
        return await self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    async def __aenter__(self) -> ObservableLock:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.release()


async def wait_for_lock_waiters(
    lock: ObservableLock,
    expected: int,
    *,
    timeout: float = 1.0,
) -> None:
    """Wait until the observable lock has at least the requested waiters."""
    await eventually(lambda: lock.waiting >= expected, timeout=timeout)


def install_observable_lock(bulkhead: Any) -> ObservableLock:
    """Replace one coordinator lock before concurrent work starts."""
    coordinator = bulkhead._coordinator
    if coordinator._owner_loop is not None:
        raise RuntimeError("observable lock must be installed before the bulkhead is used")
    lock = ObservableLock()
    coordinator._mutex = lock
    return lock
