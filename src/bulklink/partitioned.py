"""Public bounded partition-isolation facade."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Hashable

from bulklink._internal.cancellation import complete_cleanup
from bulklink._internal.partitioned_coordinator import PartitionCoordinator
from bulklink._internal.partitioned_models import PartitionEntry
from bulklink._internal.slot import SlotContext
from bulklink._internal.validation import require_finite_number, resolve_wait_limit
from bulklink.bulkhead import AsyncBulkhead
from bulklink.partitioned_status import PartitionedBulkheadStatus
from bulklink.typing import P, T

# Factory that receives the child bulkhead and an absolute event-loop deadline
# (or None when no deadline is in effect) and returns a configured SlotContext.
_ChildContextFactory = Callable[[AsyncBulkhead, float | None], SlotContext]


def _child_slot_with_deadline(bulkhead: AsyncBulkhead, deadline: float | None) -> SlotContext:
    """Return the appropriate child context depending on whether a deadline applies."""
    if deadline is not None:
        return bulkhead.slot_before(deadline)
    return bulkhead.slot()


def _child_slot_now(bulkhead: AsyncBulkhead, deadline: float | None) -> SlotContext:
    """Return an immediate child context regardless of any deadline."""
    return bulkhead.slot_now()


class _PartitionAdmission:
    """Bridge one manager reference to one child slot lifecycle.

    The admission budget (deadline) is computed at __aenter__ time so that relative
    limits (wait_limit, slot_within) start when the caller actually begins waiting,
    not when the SlotContext object was constructed.  The same absolute deadline is
    forwarded to child admission so manager resolution time is deducted from the
    caller's budget.
    """

    def __init__(
        self,
        owner: PartitionCoordinator,
        key: Hashable,
        immediate: bool,
        relative_limit: float | None,
        absolute_deadline: float | None,
        context_factory: _ChildContextFactory,
    ) -> None:
        self._owner = owner
        self._key = key
        # Exactly one of (immediate, relative_limit, absolute_deadline) governs
        # how the effective deadline is computed at admit() time.
        self._immediate = immediate
        self._relative_limit = relative_limit
        self._absolute_deadline = absolute_deadline
        self._context_factory = context_factory
        self._entry: PartitionEntry | None = None
        self._context: SlotContext | None = None

    async def admit(self) -> None:
        loop = asyncio.get_running_loop()

        # Compute the effective absolute deadline at the moment __aenter__ begins.
        # For relative limits this defers the start time to actual admission, not
        # SlotContext construction.
        if self._immediate:
            deadline: float | None = None
            budget_for_error: float | None = None
        elif self._absolute_deadline is not None:
            deadline = self._absolute_deadline
            budget_for_error = max(0.0, self._absolute_deadline - loop.time())
        elif self._relative_limit is not None:
            deadline = loop.time() + self._relative_limit
            budget_for_error = self._relative_limit
        else:
            deadline = None
            budget_for_error = None

        entry = await self._owner.acquire(
            self._key,
            deadline=deadline,
            immediate=self._immediate,
            budget_for_error=budget_for_error,
        )

        # Pass the same absolute deadline to child admission.  The child computes the
        # remaining time as (deadline - loop.time()), automatically deducting whatever
        # the manager layer consumed.
        context = self._context_factory(entry.bulkhead, deadline)
        try:
            await context.__aenter__()
        except BaseException:
            await complete_cleanup(self._owner.release_reference(entry))
            raise

        self._entry = entry
        self._context = context

    async def release(self) -> None:
        entry = self._entry
        context = self._context
        if entry is None or context is None:
            raise RuntimeError("partition admission released before entering")

        await context.__aexit__(None, None, None)
        await self._owner.release_reference(entry)
        self._entry = None
        self._context = None


class PartitionedBulkhead:
    """Isolate async concurrency independently for a bounded set of partition keys.

    Args:
        label: Human-readable manager name used in errors and status reports.
        parallelism: Maximum operations executing inside each partition.
        max_partitions: Hard upper bound for retained partition instances.
        waiting_room: Maximum operations waiting inside each partition.
        wait_limit: Maximum seconds one operation may wait, or ``None``.
        idle_timeout: Seconds an idle partition must remain unused before cleanup.

    Partition keys must be hashable and stable. Keys are retained only while their
    partitions exist and are never included in public errors, status snapshots, or child
    labels. No cleanup task runs in the background.
    """

    def __init__(
        self,
        *,
        label: str,
        parallelism: int,
        max_partitions: int,
        waiting_room: int = 0,
        wait_limit: float | None = None,
        idle_timeout: float = 300.0,
    ) -> None:
        self._coordinator = PartitionCoordinator(
            label=label,
            parallelism=parallelism,
            waiting_room=waiting_room,
            wait_limit=wait_limit,
            max_partitions=max_partitions,
            idle_timeout=idle_timeout,
        )

    @property
    def label(self) -> str:
        """Return the human-readable manager label."""
        return self._coordinator.label

    @property
    def parallelism(self) -> int:
        """Return execution capacity configured for every partition."""
        return self._coordinator.parallelism

    @property
    def waiting_room(self) -> int:
        """Return waiting capacity configured for every partition."""
        return self._coordinator.waiting_room

    @property
    def wait_limit(self) -> float | None:
        """Return the queue wait limit configured for every partition."""
        return self._coordinator.wait_limit

    @property
    def max_partitions(self) -> int:
        """Return the hard retained-partition limit."""
        return self._coordinator.max_partitions

    @property
    def idle_timeout(self) -> float:
        """Return seconds required before normal idle cleanup."""
        return self._coordinator.idle_timeout

    def slot(self, partition: Hashable, /) -> SlotContext:
        """Return a context manager using one isolated partition."""
        return self._slot(
            partition,
            immediate=False,
            relative_limit=self._coordinator.wait_limit,
            absolute_deadline=None,
            context_factory=_child_slot_with_deadline,
        )

    def slot_now(self, partition: Hashable, /) -> SlotContext:
        """Return a partition context that rejects instead of waiting."""
        return self._slot(
            partition,
            immediate=True,
            relative_limit=None,
            absolute_deadline=None,
            context_factory=_child_slot_now,
        )

    def slot_within(self, wait_limit: float, partition: Hashable, /) -> SlotContext:
        """Return a partition context with a shorter relative wait limit."""
        effective = resolve_wait_limit(self.wait_limit, wait_limit)
        return self._slot(
            partition,
            immediate=False,
            relative_limit=effective,
            absolute_deadline=None,
            context_factory=_child_slot_with_deadline,
        )

    def slot_before(self, deadline: float, partition: Hashable, /) -> SlotContext:
        """Return a partition context admitted before an absolute loop deadline."""
        validated = require_finite_number("deadline", deadline)
        return self._slot(
            partition,
            immediate=False,
            relative_limit=None,
            absolute_deadline=validated,
            context_factory=_child_slot_with_deadline,
        )

    async def execute(
        self,
        partition: Hashable,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute one async callable inside the selected partition."""
        async with self.slot(partition):
            return await operation(*args, **kwargs)

    async def execute_now(
        self,
        partition: Hashable,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute only when the selected partition has immediate capacity."""
        async with self.slot_now(partition):
            return await operation(*args, **kwargs)

    async def execute_within(
        self,
        wait_limit: float,
        partition: Hashable,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute inside one partition with a shorter queue wait limit."""
        async with self.slot_within(wait_limit, partition):
            return await operation(*args, **kwargs)

    async def execute_before(
        self,
        deadline: float,
        partition: Hashable,
        operation: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Execute after partition admission before an absolute deadline."""
        async with self.slot_before(deadline, partition):
            return await operation(*args, **kwargs)

    async def cleanup_idle(self) -> int:
        """Remove partitions idle for at least ``idle_timeout`` seconds."""
        return await self._coordinator.cleanup_idle()

    async def discard(self, partition: Hashable, /) -> bool:
        """Remove one partition when it exists and has no current callers."""
        return await self._coordinator.discard(partition)

    async def status(self) -> PartitionedBulkheadStatus:
        """Return an immutable manager-level cardinality snapshot."""
        return await self._coordinator.status()

    async def close(self) -> None:
        """Stop partition creation and close every retained child."""
        await self._coordinator.close()

    async def wait_closed(self) -> None:
        """Wait until every retained partition has drained after closing."""
        await self._coordinator.wait_closed()

    async def close_and_wait(self) -> None:
        """Close the manager and wait until all retained partitions drain."""
        await self._coordinator.close_and_wait()

    def _slot(
        self,
        partition: Hashable,
        immediate: bool,
        relative_limit: float | None,
        absolute_deadline: float | None,
        context_factory: _ChildContextFactory,
    ) -> SlotContext:
        key = self._coordinator.validated_key(partition)
        admission = _PartitionAdmission(
            self._coordinator,
            key,
            immediate=immediate,
            relative_limit=relative_limit,
            absolute_deadline=absolute_deadline,
            context_factory=context_factory,
        )
        return SlotContext(admit=admission.admit, release=admission.release)
