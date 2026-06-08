"""Bounded ownership and lifecycle coordination for keyed async bulkheads."""

from __future__ import annotations

import asyncio
from collections.abc import Hashable
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
        # Counts logical slots held by tasks that evicted a victim and are
        # closing it before creating the replacement.  Included in capacity
        # accounting so no other task can claim the slot being reclaimed.
        self._reserved_slots = 0
        # Counts children that have been removed from _partitions but whose
        # close_and_wait() has not yet finished.  cleanup_idle() and discard()
        # increment this under the lock before releasing it, and each close
        # decrements it in a finally block so the drain signal is not raised
        # while child teardown is still in flight.
        self._pending_child_closures = 0
        self._closed = False
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._drained_event: asyncio.Event | None = None
        self._counters = PartitionRuntimeCounters()

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
                Checked before each blocking operation; not enforced mid-close because
                complete_cleanup() must run to maintain invariants.
            immediate: When True (slot_now semantics), reject instead of waiting for
                victim closure.  The mutex itself is still acquired briefly.
            budget_for_error: Original wait budget in seconds, used in the
                BulkheadQueueTimeoutError when the deadline expires.
        """
        normalized = self.validated_key(key)
        loop = self._bind_to_running_loop()
        # True once this task has removed a victim and incremented _reserved_slots.
        # The reservation is the logical slot for the replacement; it prevents any
        # other task from claiming the capacity we freed while we close the victim.
        has_reservation = False

        try:
            while True:
                victim: PartitionEntry | None = None
                async with self._mutex:
                    if self._closed:
                        raise BulkheadClosedError(label=self._label)

                    # Check the admission deadline before doing any work.
                    if deadline is not None and loop.time() >= deadline:
                        raise BulkheadQueueTimeoutError(
                            label=self._label,
                            wait_limit=budget_for_error if budget_for_error is not None else 0.0,
                        )

                    entry = self._partitions.get(normalized)
                    if entry is not None:
                        if has_reservation:
                            self._reserved_slots -= 1
                            has_reservation = False
                        self._borrow_locked(entry)
                        return entry

                    # has_reservation means _reserved_slots already accounts for this
                    # task's slot; do not compete for capacity a second time.
                    logical = len(self._partitions) + self._reserved_slots
                    if has_reservation or logical < self._max_partitions:
                        if has_reservation:
                            self._reserved_slots -= 1
                            has_reservation = False
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
                        # Reject at the limit so callers get the standard error.
                        self._counters.limit_rejected_total += 1
                        raise PartitionLimitError(
                            label=self._label,
                            max_partitions=self._max_partitions,
                            active_partitions=self._active_partitions_locked(),
                        )

                    # Reserve the freed slot before releasing the lock.  Any task
                    # that checks capacity while we close the victim will see logical
                    # at max_partitions and cannot steal the slot we are reclaiming.
                    del self._partitions[victim.key]
                    self._counters.evicted_total += 1
                    self._reserved_slots += 1
                    has_reservation = True

                await complete_cleanup(victim.bulkhead.close_and_wait())
                # After victim close, re-check deadline before creating the replacement.
                if deadline is not None and loop.time() >= deadline:
                    raise BulkheadQueueTimeoutError(
                        label=self._label,
                        wait_limit=budget_for_error if budget_for_error is not None else 0.0,
                    )
                # Loop continues: next iteration creates the replacement entry.

        except BaseException:
            if has_reservation:
                # complete_cleanup guarantees the release runs to completion even
                # if a second cancellation arrives while we wait for the mutex.
                await complete_cleanup(self._release_reserved_slot())
            raise

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

        async with self._mutex:
            victims = tuple(
                entry
                for entry in self._partitions.values()
                if entry.borrowers == 0 and now - entry.last_idle_at >= self._idle_timeout
            )
            for entry in victims:
                del self._partitions[entry.key]
            self._counters.evicted_total += len(victims)
            # Register maintenance before releasing the lock so the drain condition
            # cannot fire while these children are still being torn down.
            self._pending_child_closures += len(victims)

        if not victims:
            return 0

        # Close each victim independently so each one releases its own maintenance
        # slot.  return_exceptions prevents one failure from orphaning remaining
        # tasks (and their maintenance releases).
        await complete_cleanup(_close_with_tracking(self, victims))
        return len(victims)

    async def discard(self, key: Hashable) -> bool:
        """Remove one partition only when it is absent or currently idle."""
        normalized = self.validated_key(key)
        self._bind_to_running_loop()

        async with self._mutex:
            entry = self._partitions.get(normalized)
            if entry is None or entry.borrowers != 0:
                return False
            del self._partitions[normalized]
            self._counters.discarded_total += 1
            # Register maintenance before releasing the lock.
            self._pending_child_closures += 1

        await complete_cleanup(self._close_child_and_release_maintenance(entry.bulkhead))
        return True

    async def status(self) -> PartitionedBulkheadStatus:
        """Build an immutable cardinality snapshot under the manager lock."""
        self._bind_to_running_loop()

        async with self._mutex:
            self._snapshot_index += 1
            partition_count = len(self._partitions)
            active_partitions = self._active_partitions_locked()
            counters = self._counters
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
                is_closed=self._closed,
            )

    async def close(self) -> None:
        """Close child admission and prevent creation of new partitions."""
        await complete_cleanup(self._close())

    async def wait_closed(self) -> None:
        """Wait for all children and release retained partition keys after shutdown."""
        self._bind_to_running_loop()
        async with self._mutex:
            if not self._closed:
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
            self._closed = True
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
        # - closed: no new work is accepted
        # - leased_operations == 0: no outstanding admission slots
        # - reserved_slots == 0: no pending eviction replacements
        # - pending_child_closures == 0: no children still being torn down by
        #   cleanup_idle() or discard()
        if (
            self._closed
            and self._leased_operations == 0
            and self._reserved_slots == 0
            and self._pending_child_closures == 0
        ):
            self._drain_signal().set()

    async def _release_reserved_slot(self) -> None:
        """Release exactly one reservation slot and re-evaluate the drain condition.

        Must be called exactly once per reservation, even under cancellation.
        Wrapping with complete_cleanup() ensures a second cancel() cannot interrupt
        the mutex acquisition and leave _reserved_slots permanently elevated.
        """
        async with self._mutex:
            if self._reserved_slots <= 0:
                raise RuntimeError(
                    "reserved slot released without a matching reservation: "
                    "invariant violation in PartitionCoordinator"
                )
            self._reserved_slots -= 1
            self._signal_drained_locked()

    async def _close_child_and_release_maintenance(self, bulkhead: AsyncBulkhead) -> None:
        """Close one removed child and release its maintenance slot when done.

        The slot is registered under the mutex before the child is removed, so the
        drain signal cannot fire while the child is still tearing down.  The finally
        block ensures the counter is always decremented, even when the close raises
        or the task is cancelled via complete_cleanup().
        """
        try:
            await bulkhead.close_and_wait()
        finally:
            async with self._mutex:
                if self._pending_child_closures <= 0:
                    raise RuntimeError(
                        "maintenance slot released without a matching registration: "
                        "invariant violation in PartitionCoordinator"
                    )
                self._pending_child_closures -= 1
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


async def _close_entries(entries: tuple[PartitionEntry, ...]) -> None:
    if entries:
        await asyncio.gather(*(entry.bulkhead.close_and_wait() for entry in entries))


async def _close_entries_without_wait(entries: tuple[PartitionEntry, ...]) -> None:
    if entries:
        await asyncio.gather(*(entry.bulkhead.close() for entry in entries))


async def _wait_entries(entries: tuple[PartitionEntry, ...]) -> None:
    if entries:
        await asyncio.gather(*(entry.bulkhead.wait_closed() for entry in entries))


async def _close_with_tracking(
    coordinator: PartitionCoordinator,
    victims: tuple[PartitionEntry, ...],
) -> None:
    """Close each victim independently and release its maintenance slot when done.

    Uses return_exceptions so a failure in one close operation does not abandon the
    remaining tasks and their maintenance releases.  The first error is re-raised
    after all closures have settled.
    """
    results: list[BaseException | None] = list(
        await asyncio.gather(
            *(coordinator._close_child_and_release_maintenance(e.bulkhead) for e in victims),
            return_exceptions=True,
        )
    )
    errors = [r for r in results if isinstance(r, BaseException)]
    if errors:
        raise errors[0]
