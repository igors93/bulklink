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
from bulklink.errors import BulkheadClosedError, PartitionLimitError
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

    async def acquire(self, key: Hashable) -> PartitionEntry:
        """Borrow one partition, creating or reclaiming capacity when necessary."""
        normalized = self.validated_key(key)
        loop = self._bind_to_running_loop()

        while True:
            victim: PartitionEntry | None = None
            async with self._mutex:
                if self._closed:
                    raise BulkheadClosedError(label=self._label)

                entry = self._partitions.get(normalized)
                if entry is not None:
                    self._borrow_locked(entry)
                    return entry

                if len(self._partitions) < self._max_partitions:
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

                del self._partitions[victim.key]
                self._counters.evicted_total += 1

            await complete_cleanup(victim.bulkhead.close_and_wait())

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

        await complete_cleanup(_close_entries(victims))
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

        await complete_cleanup(entry.bulkhead.close_and_wait())
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
        if self._closed and self._leased_operations == 0:
            self._drain_signal().set()

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
