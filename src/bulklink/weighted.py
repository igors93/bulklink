"""Public weighted asynchronous bulkhead facade."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from bulklink._internal.slot import SlotContext
from bulklink._internal.weighted_coordinator import WeightedAdmissionCoordinator
from bulklink.typing import P, T
from bulklink.weighted_events import WeightedBulkheadEventHandler
from bulklink.weighted_status import WeightedBulkheadStatus


class WeightedBulkhead:
    """Bound async work by integer capacity cost while preserving FIFO admission.

    Args:
        label: Human-readable name used in events and status reports.
        capacity: Maximum capacity units allocated at the same time.
        waiting_room: Maximum operations waiting in strict FIFO order.
        wait_limit: Maximum seconds one operation may wait, or ``None``.

    One instance binds to the first event loop that uses it. Each operation must request a
    positive integer cost no greater than the current capacity.
    """

    def __init__(
        self,
        *,
        label: str,
        capacity: int,
        waiting_room: int = 0,
        wait_limit: float | None = None,
    ) -> None:
        self._coordinator = WeightedAdmissionCoordinator(
            label=label,
            capacity=capacity,
            waiting_room=waiting_room,
            wait_limit=wait_limit,
        )

    @property
    def label(self) -> str:
        """Return the human-readable label."""
        return self._coordinator.label

    @property
    def capacity(self) -> int:
        """Return total weighted capacity."""
        return self._coordinator.capacity

    @property
    def waiting_room(self) -> int:
        """Return the waiting capacity measured in operations."""
        return self._coordinator.waiting_room

    @property
    def wait_limit(self) -> float | None:
        """Return the configured queue wait limit in seconds."""
        return self._coordinator.wait_limit

    def add_event_handler(self, handler: WeightedBulkheadEventHandler) -> None:
        """Register one synchronous observability handler."""
        self._coordinator.add_event_handler(handler)

    def remove_event_handler(self, handler: WeightedBulkheadEventHandler) -> None:
        """Remove one previously registered observability handler."""
        self._coordinator.remove_event_handler(handler)

    def slot(self, cost: int = 1, /) -> SlotContext:
        """Return a context manager that may wait for the requested capacity cost."""
        validated = self._coordinator.validated_cost(cost)
        return SlotContext(
            admit=lambda: self._coordinator.enter(validated),
            release=lambda: self._coordinator.release(validated),
        )

    def slot_now(self, cost: int = 1, /) -> SlotContext:
        """Return a context manager that rejects instead of entering the queue."""
        validated = self._coordinator.validated_cost(cost)
        return SlotContext(
            admit=lambda: self._coordinator.enter_now(validated),
            release=lambda: self._coordinator.release(validated),
        )

    def slot_within(self, wait_limit: float, cost: int = 1, /) -> SlotContext:
        """Return a context manager with a shorter per-call queue wait limit."""
        validated_cost = self._coordinator.validated_cost(cost)
        effective_limit = self._coordinator.effective_wait_limit(wait_limit)
        return SlotContext(
            admit=lambda: self._coordinator.enter_within(validated_cost, effective_limit),
            release=lambda: self._coordinator.release(validated_cost),
        )

    def slot_before(self, deadline: float, cost: int = 1, /) -> SlotContext:
        """Return a context manager admitted before an absolute event-loop deadline."""
        validated_cost = self._coordinator.validated_cost(cost)
        validated_deadline = self._coordinator.validated_deadline(deadline)
        return SlotContext(
            admit=lambda: self._coordinator.enter_before(validated_cost, validated_deadline),
            release=lambda: self._coordinator.release(validated_cost),
        )

    async def execute(
        self,
        cost: int,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute one async callable after acquiring the requested capacity cost."""
        async with self.slot(cost):
            return await operation(*args, **kwargs)

    async def execute_now(
        self,
        cost: int,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute only when the requested capacity is immediately available."""
        async with self.slot_now(cost):
            return await operation(*args, **kwargs)

    async def execute_within(
        self,
        wait_limit: float,
        cost: int,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute using a shorter per-call queue wait limit."""
        async with self.slot_within(wait_limit, cost):
            return await operation(*args, **kwargs)

    async def execute_before(
        self,
        deadline: float,
        cost: int,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute after admission before an absolute event-loop deadline."""
        async with self.slot_before(deadline, cost):
            return await operation(*args, **kwargs)

    async def status(self) -> WeightedBulkheadStatus:
        """Return an immutable point-in-time weighted status report."""
        return await self._coordinator.status()

    async def resize(self, capacity: int, /) -> None:
        """Change weighted capacity without cancelling active or queued operations."""
        await self._coordinator.resize(capacity)

    async def close(self) -> None:
        """Reject queued and future operations without interrupting active work."""
        await self._coordinator.close()

    async def wait_closed(self) -> None:
        """Wait until the bulkhead is closed and active work has drained."""
        await self._coordinator.wait_closed()

    async def close_and_wait(self) -> None:
        """Close admission and wait until every active operation has left."""
        await self._coordinator.close_and_wait()
