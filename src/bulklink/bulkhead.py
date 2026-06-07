"""Public asynchronous bulkhead facade."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import wraps

from bulklink._internal.coordinator import AdmissionCoordinator
from bulklink._internal.slot import SlotContext
from bulklink.status import BulkheadStatus
from bulklink.typing import P, T


class AsyncBulkhead:
    """Bound concurrent async work and isolate it from unrelated workloads.

    Args:
        label: Human-readable name used in diagnostics and status reports.
        parallelism: Maximum operations executing at the same time.
        waiting_room: Maximum operations waiting in FIFO order.
        wait_limit: Maximum seconds one operation may wait, or ``None``.

    One instance binds to the first event loop that uses it.
    """

    def __init__(
        self,
        *,
        label: str,
        parallelism: int,
        waiting_room: int = 0,
        wait_limit: float | None = None,
    ) -> None:
        self._coordinator = AdmissionCoordinator(
            label=label,
            parallelism=parallelism,
            waiting_room=waiting_room,
            wait_limit=wait_limit,
        )

    @property
    def label(self) -> str:
        """Return the human-readable label."""
        return self._coordinator.label

    @property
    def parallelism(self) -> int:
        """Return the execution capacity."""
        return self._coordinator.parallelism

    @property
    def waiting_room(self) -> int:
        """Return the waiting capacity."""
        return self._coordinator.waiting_room

    @property
    def wait_limit(self) -> float | None:
        """Return the waiting deadline in seconds."""
        return self._coordinator.wait_limit

    def slot(self) -> SlotContext:
        """Return an async context manager for one execution slot."""
        return SlotContext(self._coordinator)

    async def execute(
        self,
        operation: Callable[P, Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute one async callable while holding one slot."""
        async with self.slot():
            return await operation(*args, **kwargs)

    def __call__(
        self,
        operation: Callable[P, Awaitable[T]],
    ) -> Callable[P, Awaitable[T]]:
        """Decorate one async callable with this bulkhead."""

        @wraps(operation)
        async def guarded(*args: P.args, **kwargs: P.kwargs) -> T:
            return await self.execute(operation, *args, **kwargs)

        return guarded

    async def status(self) -> BulkheadStatus:
        """Return an immutable point-in-time status report."""
        return await self._coordinator.status()

    async def close(self) -> None:
        """Reject queued and future operations without interrupting active work."""
        await self._coordinator.close()
