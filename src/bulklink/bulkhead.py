"""Public asynchronous bulkhead facade."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import wraps

from bulklink._internal.coordinator import AdmissionCoordinator
from bulklink._internal.diagnostics import assess_capacity
from bulklink._internal.slot import SlotContext
from bulklink.capacity import CapacityReport
from bulklink.events import BulkheadEventHandler
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

    def add_event_handler(self, handler: BulkheadEventHandler) -> None:
        """Register one synchronous observability handler."""
        self._coordinator.add_event_handler(handler)

    def remove_event_handler(self, handler: BulkheadEventHandler) -> None:
        """Remove one previously registered observability handler."""
        self._coordinator.remove_event_handler(handler)

    def slot(self) -> SlotContext:
        """Return a context manager that may wait for one execution slot."""
        return SlotContext(
            admit=self._coordinator.enter,
            release=self._coordinator.release,
        )

    def slot_now(self) -> SlotContext:
        """Return a context manager that rejects instead of entering the queue."""
        return SlotContext(
            admit=self._coordinator.enter_now,
            release=self._coordinator.release,
        )

    def slot_within(self, wait_limit: float, /) -> SlotContext:
        """Return a context manager with a shorter per-call queue wait limit."""
        effective_limit = self._coordinator.effective_wait_limit(wait_limit)
        return SlotContext(
            admit=lambda: self._coordinator.enter_within(effective_limit),
            release=self._coordinator.release,
        )

    async def execute(
        self,
        operation: Callable[P, Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute one async callable after waiting for a slot when necessary."""
        async with self.slot():
            return await operation(*args, **kwargs)

    async def execute_now(
        self,
        operation: Callable[P, Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute one async callable only when a slot is immediately available."""
        async with self.slot_now():
            return await operation(*args, **kwargs)

    async def execute_within(
        self,
        wait_limit: float,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute one async callable using a shorter per-call queue wait limit."""
        async with self.slot_within(wait_limit):
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

    async def capacity_report(self) -> CapacityReport:
        """Return an immutable diagnosis of current and cumulative capacity pressure."""
        current = await self.status()
        return assess_capacity(current, wait_limit=self.wait_limit)

    async def close(self) -> None:
        """Reject queued and future operations without interrupting active work."""
        await self._coordinator.close()

    async def wait_closed(self) -> None:
        """Wait until the bulkhead is closed and active work has drained."""
        await self._coordinator.wait_closed()

    async def close_and_wait(self) -> None:
        """Close admission and wait until every active operation has left."""
        await self._coordinator.close_and_wait()
