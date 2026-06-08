"""Bounded ownership and lifecycle coordination for keyed async bulkheads."""

from __future__ import annotations

import asyncio
from collections.abc import Hashable
from dataclasses import dataclass, field
from enum import Enum, auto
from secrets import token_hex

from bulklink._internal.cancellation import complete_cleanup
from bulklink._internal.partitioned_models import PartitionEntry, PartitionRuntimeCounters
from bulklink._internal.validation import (
    require_label,
    require_non_negative_integer,
    require_optional_positive_number,
    require_positive_integer,
    require_positive_number,
)
from bulklink.bulkhead import AsyncBulkhead
from bulklink.errors import BulkheadClosedError, BulkheadQueueTimeoutError, PartitionLimitError
from bulklink.partitioned_status import PartitionedBulkheadStatus


class _PartitionLifecycle(Enum):
    """Explicit lifecycle states for the partitioned bulkhead manager."""

    OPEN = auto()
    # New admissions and maintenance are rejected; pending ops continue.
    CLOSING = auto()
    # All work done, map cleared, drain signaled.
    CLOSED = auto()


class _PendingOpKind(Enum):
    EVICTION = auto()  # holds a capacity reservation
    MAINTENANCE = auto()  # cleanup_idle / discard / shutdown-child close


@dataclass(slots=True)
class _PendingOp:
    """Tracks one pending background operation with explicit ownership."""

    kind: _PendingOpKind
    # True when this op holds one logical capacity slot (eviction).
    owns_capacity: bool
    # The asyncio.Task driving this op's close work, if already spawned.
    task: asyncio.Task[None] | None = field(default=None)


class PartitionCoordinator:
    """Own a bounded set of lazily created per-key bulkheads."""

    def __init__(
        self,
        *,
        label: str,
        parallelism: int,
        waiting_room: int,
        wait_limit: float | None,
        max_partitions: int,
        idle_timeout: float,
    ) -> None:
        self._label = require_label(label)
        self._parallelism = require_positive_integer("parallelism", parallelism)
        self._waiting_room = require_non_negative_integer("waiting_room", waiting_room)
        self._wait_limit = require_optional_positive_number("wait_limit", wait_limit)
        self._max_partitions = require_positive_integer("max_partitions", max_partitions)
        self._idle_timeout = require_positive_number("idle_timeout", idle_timeout)

        self._instance_id = token_hex(16)
        self._snapshot_index = 0
        self._mutex = asyncio.Lock()
        self._partitions: dict[Hashable, PartitionEntry] = {}
        self._leased_operations = 0

        # Single source of truth for all pending background operations.
        # Key: id(op) — unique per _PendingOp instance.
        self._pending_ops: dict[int, _PendingOp] = {}

        self._lifecycle = _PartitionLifecycle.OPEN
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._drained_event: asyncio.Event | None = None
        self._counters = PartitionRuntimeCounters()

    # ------------------------------------------------------------------
    # Derived backwards-compatibility properties used by existing tests.
    # ------------------------------------------------------------------

    @property
    def _closed(self) -> bool:
        """Backward-compatibility shim: True when lifecycle is CLOSING or CLOSED."""
        return self._lifecycle is not _PartitionLifecycle.OPEN

    @property
    def _reserved_slots(self) -> int:
        """Backward-compatibility shim: count of ops that own a capacity slot."""
        return sum(op.owns_capacity for op in self._pending_ops.values())

    @property
    def _pending_child_closures(self) -> int:
        """Backward-compatibility shim: count of all pending background ops."""
        return len(self._pending_ops)

    @property
    def label(self) -> str:
        return self._label

    @property
    def parallelism(self) -> int:
        return self._parallelism

    @property
    def waiting_room(self) -> int:
        return self._waiting_room

    @property
    def wait_limit(self) -> float | None:
        return self._wait_limit

    @property
    def max_partitions(self) -> int:
        return self._max_partitions

    @property
    def idle_timeout(self) -> float:
        return self._idle_timeout

    def _bind_to_running_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = loop
            self._drained_event = asyncio.Event()
        elif self._owner_loop is not loop:
            raise RuntimeError(
                f"partitioned bulkhead {self._label!r} cannot be shared across event loops"
            )
        return loop

    @staticmethod
    def validated_key(key: Hashable) -> Hashable:
        """Return one hashable partition key without rendering or exposing it."""
        try:
            hash(key)
        except Exception:
            raise TypeError("partition must be hashable") from None
        return key

    async def acquire(
        self,
        key: Hashable,
        *,
        deadline: float | None = None,
        immediate: bool = False,
        budget_for_error: float | None = None,
    ) -> PartitionEntry:
        """Borrow one partition, creating or reclaiming capacity when necessary.

        Args:
            key: Partition key to acquire.
            deadline: Absolute event-loop time after which the attempt is abandoned.
                Checked before each blocking operation. When a victim close is needed
                and a deadline exists, the caller is shielded behind the deadline so
                the caller cannot wait longer than their budget.
            immediate: When True (slot_now semantics), reject instead of waiting for
                victim closure.  The mutex itself is still acquired briefly.
            budget_for_error: Original wait budget in seconds, used in the
                BulkheadQueueTimeoutError when the deadline expires.
        """
        normalized = self.validated_key(key)
        loop = self._bind_to_running_loop()

        # Pending op registered for this task's eviction reservation, if any.
        op: _PendingOp | None = None

        try:
            while True:
                victim: PartitionEntry | None = None
                async with self._mutex:
                    if self._lifecycle is not _PartitionLifecycle.OPEN:
                        raise BulkheadClosedError(label=self._label)

                    # Check the admission deadline before doing any work.
                    if deadline is not None and loop.time() >= deadline:
                        raise BulkheadQueueTimeoutError(
                            label=self._label,
                            wait_limit=budget_for_error if budget_for_error is not None else 0.0,
                        )

                    entry = self._partitions.get(normalized)
                    if entry is not None:
                        if op is not None:
                            # Our reservation is consumed — convert it to final state.
                            self._release_pending_op_locked(op)
                            op = None
                        self._borrow_locked(entry)
                        return entry

                    # op.owns_capacity means _reserved_slots already accounts for
                    # this task's slot; do not compete for capacity a second time.
                    logical = len(self._partitions) + self._reserved_slots
                    if op is not None or logical < self._max_partitions:
                        if op is not None:
                            self._release_pending_op_locked(op)
                            op = None
                        entry = self._create_locked(normalized, now=loop.time())
                        self._borrow_locked(entry)
                        return entry

                    victim = self._least_recent_idle_locked()
                    if victim is None:
                        self._counters.limit_rejected_total += 1
                        raise PartitionLimitError(
                            label=self._label,
                            max_partitions=self._max_partitions,
                            active_partitions=self._active_partitions_locked(),
                        )

                    if immediate:
                        # slot_now() semantics: cannot wait for victim closure.
                        self._counters.limit_rejected_total += 1
                        raise PartitionLimitError(
                            label=self._label,
                            max_partitions=self._max_partitions,
                            active_partitions=self._active_partitions_locked(),
                        )

                    # Register the reservation before releasing the lock.
                    del self._partitions[victim.key]
                    self._counters.evicted_total += 1
                    op = _PendingOp(kind=_PendingOpKind.EVICTION, owns_capacity=True)
                    self._pending_ops[id(op)] = op

                # Victim close outside the lock.  When a deadline is present we
                # shield the caller: spawn a task and wait with a timeout.  If the
                # deadline fires, the task continues under manager ownership and the
                # caller receives BulkheadQueueTimeoutError.
                if deadline is not None:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        # Already expired — attach a callback so the op is released
                        # when the underlying task finishes, then raise immediately.
                        # We must not hold op here, so we transfer ownership first.
                        close_task_now: asyncio.Task[None] = asyncio.create_task(
                            victim.bulkhead.close_and_wait()
                        )
                        async with self._mutex:
                            op.task = close_task_now
                        self._attach_release_callback(op, close_task_now)
                        op = None
                        raise BulkheadQueueTimeoutError(
                            label=self._label,
                            wait_limit=budget_for_error if budget_for_error is not None else 0.0,
                        )
                    # Start the close task and wait with deadline timeout.
                    close_task = asyncio.create_task(victim.bulkhead.close_and_wait())
                    async with self._mutex:
                        op.task = close_task
                    timed_out = False
                    cancelled_error: asyncio.CancelledError | None = None
                    try:
                        await asyncio.wait_for(asyncio.shield(close_task), timeout=remaining)
                    except asyncio.TimeoutError:
                        timed_out = True
                    except asyncio.CancelledError as err:
                        cancelled_error = err

                    if timed_out or cancelled_error is not None:
                        # Transfer task ownership — caller is released, task continues.
                        # Use complete_cleanup so a second cancel cannot interrupt this.
                        await complete_cleanup(
                            self._transfer_victim_close_to_manager(op, close_task)
                        )
                        op = None
                        if cancelled_error is not None:
                            raise cancelled_error
                        raise BulkheadQueueTimeoutError(
                            label=self._label,
                            wait_limit=budget_for_error if budget_for_error is not None else 0.0,
                        )
                    # Close completed within deadline — clear task reference.
                    async with self._mutex:
                        op.task = None
                else:
                    await complete_cleanup(victim.bulkhead.close_and_wait())

                # Loop continues: next iteration creates the replacement entry.

        except BaseException:
            if op is not None:
                # Release the reservation so drain/capacity accounting stays correct.
                await complete_cleanup(self._release_reserved_op(op))
            raise

    async def _transfer_victim_close_to_manager(
        self,
        op: _PendingOp,
        close_task: asyncio.Task[None],
    ) -> None:
        """Hand off an already-started victim close task to manager ownership.

        The close task continues running.  When it finishes, the pending op is
        released so the drain signal can fire.  The caller must set op=None after
        this returns.
        """
        self._attach_release_callback(op, close_task)

    def _attach_release_callback(
        self,
        op: _PendingOp,
        task: asyncio.Task[None],
    ) -> None:
        """Attach a done callback that releases the pending op when the task ends."""
        captured_op = op

        def _on_close_done(t: asyncio.Task[None]) -> None:
            asyncio.ensure_future(complete_cleanup(self._release_reserved_op(captured_op)))

        task.add_done_callback(_on_close_done)

    async def release_reference(self, entry: PartitionEntry) -> None:
        """Return one caller reference after child admission has fully ended."""
        loop = self._bind_to_running_loop()
        async with self._mutex:
            if entry.borrowers <= 0 or self._leased_operations <= 0:
                raise RuntimeError("partition reference released without a matching borrow")
            if self._partitions.get(entry.key) is not entry:
                raise RuntimeError("borrowed partition disappeared before release")

            entry.borrowers -= 1
            self._leased_operations -= 1
            if entry.borrowers == 0:
                entry.last_idle_at = loop.time()
            self._signal_drained_locked()

    async def cleanup_idle(self) -> int:
        """Remove all drained partitions idle for at least the configured timeout."""
        loop = self._bind_to_running_loop()
        now = loop.time()

        ops: list[tuple[_PendingOp, PartitionEntry]] = []

        async with self._mutex:
            if self._lifecycle is not _PartitionLifecycle.OPEN:
                raise BulkheadClosedError(label=self._label)

            victims = tuple(
                entry
                for entry in self._partitions.values()
                if entry.borrowers == 0 and now - entry.last_idle_at >= self._idle_timeout
            )
            for entry in victims:
                del self._partitions[entry.key]
                mop = _PendingOp(kind=_PendingOpKind.MAINTENANCE, owns_capacity=False)
                self._pending_ops[id(mop)] = mop
                ops.append((mop, entry))
            self._counters.evicted_total += len(victims)

        if not ops:
            return 0

        # Close each victim independently so each one releases its own op slot.
        results: list[BaseException | None] = list(
            await asyncio.gather(
                *(self._close_child_and_release_op(mop, entry.bulkhead) for mop, entry in ops),
                return_exceptions=True,
            )
        )
        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            raise errors[0]
        return len(ops)

    async def discard(self, key: Hashable) -> bool:
        """Remove one partition only when it is absent or currently idle."""
        normalized = self.validated_key(key)
        self._bind_to_running_loop()

        mop: _PendingOp | None = None
        entry: PartitionEntry | None = None

        async with self._mutex:
            if self._lifecycle is not _PartitionLifecycle.OPEN:
                raise BulkheadClosedError(label=self._label)

            entry = self._partitions.get(normalized)
            if entry is None or entry.borrowers != 0:
                return False
            del self._partitions[normalized]
            self._counters.discarded_total += 1
            mop = _PendingOp(kind=_PendingOpKind.MAINTENANCE, owns_capacity=False)
            self._pending_ops[id(mop)] = mop

        await complete_cleanup(self._close_child_and_release_op(mop, entry.bulkhead))
        return True

    async def status(self) -> PartitionedBulkheadStatus:
        """Build an immutable cardinality snapshot under the manager lock."""
        self._bind_to_running_loop()

        async with self._mutex:
            self._snapshot_index += 1
            partition_count = len(self._partitions)
            active_partitions = self._active_partitions_locked()
            counters = self._counters
            is_closed = self._lifecycle is not _PartitionLifecycle.OPEN
            return PartitionedBulkheadStatus(
                instance_id=self._instance_id,
                snapshot_index=self._snapshot_index,
                label=self._label,
                parallelism=self._parallelism,
                waiting_room=self._waiting_room,
                wait_limit=self._wait_limit,
                max_partitions=self._max_partitions,
                idle_timeout=self._idle_timeout,
                partition_count=partition_count,
                active_partitions=active_partitions,
                leased_operations=self._leased_operations,
                created_total=counters.created_total,
                evicted_total=counters.evicted_total,
                discarded_total=counters.discarded_total,
                limit_rejected_total=counters.limit_rejected_total,
                peak_partitions=counters.peak_partitions,
                peak_leased_operations=counters.peak_leased_operations,
                is_closed=is_closed,
            )

    async def close(self) -> None:
        """Close child admission and prevent creation of new partitions."""
        await complete_cleanup(self._close())

    async def wait_closed(self) -> None:
        """Wait for all children and release retained partition keys after shutdown."""
        self._bind_to_running_loop()
        async with self._mutex:
            if self._lifecycle is _PartitionLifecycle.OPEN:
                raise RuntimeError("close() must be called before wait_closed()")
            entries = tuple(self._partitions.values())
            drained = self._drain_signal()

        await drained.wait()
        await _wait_entries(entries)

        async with self._mutex:
            if self._leased_operations != 0:
                raise RuntimeError("partitioned bulkhead drained with active references")
            self._partitions.clear()

    async def close_and_wait(self) -> None:
        """Close the manager and wait until every borrowed child has drained."""
        await complete_cleanup(self._close_and_wait())

    async def _close(self) -> None:
        self._bind_to_running_loop()
        async with self._mutex:
            if self._lifecycle is not _PartitionLifecycle.OPEN:
                # Idempotent — already closing or closed.
                return
            self._lifecycle = _PartitionLifecycle.CLOSING
            entries = tuple(self._partitions.values())
            self._signal_drained_locked()
        await _close_entries_without_wait(entries)

    async def _close_and_wait(self) -> None:
        await self._close()
        await self.wait_closed()

    def _drain_signal(self) -> asyncio.Event:
        event = self._drained_event
        if event is None:
            raise RuntimeError("partitioned drain signal is unavailable before loop binding")
        return event

    def _signal_drained_locked(self) -> None:
        # All four conditions must hold before the manager is considered drained:
        # - not OPEN: no new work accepted
        # - leased_operations == 0: no outstanding admission slots
        # - no pending ops with capacity: no pending eviction replacements
        # - no pending ops at all: no children still being torn down
        if (
            self._lifecycle is not _PartitionLifecycle.OPEN
            and self._leased_operations == 0
            and not self._pending_ops
        ):
            self._drain_signal().set()

    async def _release_reserved_op(self, op: _PendingOp) -> None:
        """Release exactly one pending op and re-evaluate the drain condition."""
        async with self._mutex:
            if id(op) not in self._pending_ops:
                raise RuntimeError(
                    "pending op released without a matching registration: "
                    "invariant violation in PartitionCoordinator"
                )
            del self._pending_ops[id(op)]
            self._signal_drained_locked()

    def _release_pending_op_locked(self, op: _PendingOp) -> None:
        """Release a pending op while already holding the lock (inline path).

        The drain signal is NOT checked here because the caller is still inside
        the lock; the caller is responsible for calling _signal_drained_locked()
        if appropriate, or the natural flow re-evaluates it on the next operation.
        This is only called when the reservation is consumed by creating a partition
        (not a terminal error path), so drain is not relevant there.
        """
        if id(op) not in self._pending_ops:
            raise RuntimeError(
                "pending op released without a matching registration (locked path): "
                "invariant violation in PartitionCoordinator"
            )
        del self._pending_ops[id(op)]

    async def _close_child_and_release_op(self, op: _PendingOp, bulkhead: AsyncBulkhead) -> None:
        """Close one removed child and release its maintenance op when done."""
        try:
            await bulkhead.close_and_wait()
        finally:
            async with self._mutex:
                if id(op) not in self._pending_ops:
                    raise RuntimeError(
                        "maintenance op released without a matching registration: "
                        "invariant violation in PartitionCoordinator"
                    )
                del self._pending_ops[id(op)]
                self._signal_drained_locked()

    def _create_locked(self, key: Hashable, *, now: float) -> PartitionEntry:
        entry = PartitionEntry(
            key=key,
            bulkhead=AsyncBulkhead(
                label=self._label,
                parallelism=self._parallelism,
                waiting_room=self._waiting_room,
                wait_limit=self._wait_limit,
            ),
            borrowers=0,
            last_idle_at=now,
        )
        self._partitions[key] = entry
        self._counters.created_total += 1
        self._counters.peak_partitions = max(
            self._counters.peak_partitions,
            len(self._partitions),
        )
        return entry

    def _borrow_locked(self, entry: PartitionEntry) -> None:
        entry.borrowers += 1
        self._leased_operations += 1
        self._counters.peak_leased_operations = max(
            self._counters.peak_leased_operations,
            self._leased_operations,
        )

    def _least_recent_idle_locked(self) -> PartitionEntry | None:
        idle = (entry for entry in self._partitions.values() if entry.borrowers == 0)
        return min(idle, key=lambda entry: entry.last_idle_at, default=None)

    def _active_partitions_locked(self) -> int:
        return sum(entry.borrowers > 0 for entry in self._partitions.values())


async def _close_entries_without_wait(entries: tuple[PartitionEntry, ...]) -> None:
    if entries:
        await asyncio.gather(*(entry.bulkhead.close() for entry in entries))


async def _wait_entries(entries: tuple[PartitionEntry, ...]) -> None:
    if entries:
        await asyncio.gather(*(entry.bulkhead.wait_closed() for entry in entries))
