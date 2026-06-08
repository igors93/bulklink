"""Immutable status and interval metrics for partitioned bulkheads."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PartitionedBulkheadStatus:
    """Read-only point-in-time description of one partitioned bulkhead."""

    instance_id: str = field(compare=False, repr=False)
    snapshot_index: int = field(compare=False)
    label: str
    parallelism: int
    waiting_room: int
    wait_limit: float | None
    max_partitions: int
    idle_timeout: float
    partition_count: int
    active_partitions: int
    leased_operations: int
    created_total: int
    evicted_total: int
    discarded_total: int
    limit_rejected_total: int
    peak_partitions: int
    peak_leased_operations: int
    is_closed: bool

    @property
    def idle_partitions(self) -> int:
        """Return partitions that currently have no admitted or waiting callers."""
        return self.partition_count - self.active_partitions

    @property
    def available_partition_slots(self) -> int:
        """Return how many new partitions fit before reaching the hard limit."""
        return max(0, self.max_partitions - self.partition_count)

    @property
    def is_at_limit(self) -> bool:
        """Return True when the current partition count reached the configured limit."""
        return self.partition_count >= self.max_partitions

    @property
    def partition_utilization(self) -> float:
        """Return current partition count divided by the configured maximum."""
        return self.partition_count / self.max_partitions

    @property
    def reclaimed_total(self) -> int:
        """Return all automatic and explicit partition removals."""
        return self.evicted_total + self.discarded_total

    def since(self, previous: PartitionedBulkheadStatus, /) -> PartitionedBulkheadInterval:
        """Return immutable lifecycle changes since an earlier manager snapshot."""
        if not isinstance(previous, PartitionedBulkheadStatus):
            raise TypeError("previous must be a PartitionedBulkheadStatus")
        return PartitionedBulkheadInterval._between(previous, self)


@dataclass(frozen=True, slots=True)
class PartitionedBulkheadInterval:
    """Immutable partition lifecycle activity between two status snapshots."""

    start: PartitionedBulkheadStatus
    end: PartitionedBulkheadStatus
    created: int
    evicted: int
    discarded: int
    limit_rejected: int

    @classmethod
    def _between(
        cls,
        start: PartitionedBulkheadStatus,
        end: PartitionedBulkheadStatus,
    ) -> PartitionedBulkheadInterval:
        _validate_partitioned_interval_snapshots(start, end)
        return cls(
            start=start,
            end=end,
            created=end.created_total - start.created_total,
            evicted=end.evicted_total - start.evicted_total,
            discarded=end.discarded_total - start.discarded_total,
            limit_rejected=end.limit_rejected_total - start.limit_rejected_total,
        )

    @property
    def reclaimed(self) -> int:
        """Return all partitions removed during the interval."""
        return self.evicted + self.discarded

    @property
    def has_activity(self) -> bool:
        """Return True when at least one cumulative lifecycle metric changed."""
        return any((self.created, self.evicted, self.discarded, self.limit_rejected))


def _validate_partitioned_interval_snapshots(
    start: PartitionedBulkheadStatus,
    end: PartitionedBulkheadStatus,
) -> None:
    if start.instance_id != end.instance_id:
        raise ValueError("status snapshots must belong to the same partitioned bulkhead instance")
    if end.snapshot_index < start.snapshot_index:
        raise ValueError("status snapshots must be provided in chronological order")
    if end.snapshot_index == start.snapshot_index and end != start:
        raise ValueError("snapshots with the same index must be identical")

    stable_fields = (
        "label",
        "parallelism",
        "waiting_room",
        "wait_limit",
        "max_partitions",
        "idle_timeout",
    )
    for field_name in stable_fields:
        if getattr(end, field_name) != getattr(start, field_name):
            raise ValueError(f"partitioned status changed stable field {field_name!r}")

    if start.is_closed and not end.is_closed:
        raise ValueError("a later status cannot reopen a closed partitioned bulkhead")

    monotonic_fields = (
        "created_total",
        "evicted_total",
        "discarded_total",
        "limit_rejected_total",
        "peak_partitions",
        "peak_leased_operations",
    )
    for field_name in monotonic_fields:
        if getattr(end, field_name) < getattr(start, field_name):
            raise ValueError(f"later status decreased cumulative field {field_name!r}")
