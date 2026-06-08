"""Immutable status and interval metrics for weighted bulkheads."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class WeightedBulkheadStatus:
    """Read-only point-in-time description of one weighted bulkhead."""

    instance_id: str = field(compare=False, repr=False)
    snapshot_index: int = field(compare=False)
    label: str
    capacity: int
    waiting_room: int
    used: int
    in_flight: int
    waiting: int
    waiting_units: int
    admitted_total: int
    admitted_units_total: int
    admitted_from_queue_total: int
    admitted_from_queue_units_total: int
    abandoned_after_admission_total: int
    abandoned_units_total: int
    queued_total: int
    queued_units_total: int
    saturated_total: int
    expired_total: int
    expired_before_queue_total: int
    cancelled_while_waiting_total: int
    closed_before_queue_total: int
    closed_while_waiting_total: int
    finished_total: int
    finished_units_total: int
    peak_used: int
    peak_in_flight: int
    peak_waiting: int
    peak_waiting_units: int
    cumulative_wait_seconds: float
    longest_wait_seconds: float
    is_closed: bool

    @property
    def is_drained(self) -> bool:
        """Return True after closing when no active or queued work remains."""
        return self.is_closed and self.used == 0 and self.in_flight == 0 and self.waiting == 0

    @property
    def available(self) -> int:
        """Return capacity units currently available for immediate allocation."""
        return max(0, self.capacity - self.used)

    @property
    def capacity_excess(self) -> int:
        """Return active units above the current resized capacity."""
        return max(0, self.used - self.capacity)

    @property
    def is_over_capacity(self) -> bool:
        """Return True while a capacity reduction drains excess active units."""
        return self.capacity_excess > 0

    @property
    def is_saturated(self) -> bool:
        """Return True when no capacity unit is immediately available."""
        return self.available == 0

    @property
    def utilization(self) -> float:
        """Return used units divided by current capacity; it may exceed one after shrinking."""
        return self.used / self.capacity

    @property
    def queue_utilization(self) -> float:
        """Return the fraction of waiting-room entries currently occupied."""
        if self.waiting_room == 0:
            return 0.0
        return self.waiting / self.waiting_room

    @property
    def direct_admitted_total(self) -> int:
        """Return admissions that did not wait in the FIFO queue."""
        return self.admitted_total - self.admitted_from_queue_total

    @property
    def direct_admitted_units_total(self) -> int:
        """Return admitted units that did not wait in the FIFO queue."""
        return self.admitted_units_total - self.admitted_from_queue_units_total

    @property
    def closed_total(self) -> int:
        """Return all operations rejected because the bulkhead was closed."""
        return self.closed_before_queue_total + self.closed_while_waiting_total

    @property
    def rejected_total(self) -> int:
        """Return all operations rejected by capacity, deadline, or closing."""
        return (
            self.saturated_total
            + self.expired_total
            + self.expired_before_queue_total
            + self.closed_total
        )

    @property
    def settled_waiting_total(self) -> int:
        """Return queued operations that have left the waiting room."""
        return (
            self.admitted_from_queue_total
            + self.cancelled_while_waiting_total
            + self.expired_total
            + self.closed_while_waiting_total
        )

    @property
    def average_wait_seconds(self) -> float:
        """Return average queue wait for operations eventually admitted."""
        if self.admitted_from_queue_total == 0:
            return 0.0
        return self.cumulative_wait_seconds / self.admitted_from_queue_total

    @property
    def average_admitted_cost(self) -> float:
        """Return average cost among admitted operations."""
        if self.admitted_total == 0:
            return 0.0
        return self.admitted_units_total / self.admitted_total

    def since(self, previous: WeightedBulkheadStatus, /) -> WeightedBulkheadInterval:
        """Return immutable counter changes since an earlier status from this instance."""
        if not isinstance(previous, WeightedBulkheadStatus):
            raise TypeError("previous must be a WeightedBulkheadStatus")
        return WeightedBulkheadInterval._between(previous, self)


@dataclass(frozen=True, slots=True)
class WeightedBulkheadInterval:
    """Immutable weighted activity measured between two status snapshots."""

    start: WeightedBulkheadStatus
    end: WeightedBulkheadStatus
    admitted: int
    admitted_units: int
    admitted_from_queue: int
    admitted_from_queue_units: int
    abandoned_after_admission: int
    abandoned_units: int
    queued: int
    queued_units: int
    saturated: int
    expired: int
    expired_before_queue: int
    cancelled_while_waiting: int
    closed_before_queue: int
    closed_while_waiting: int
    finished: int
    finished_units: int
    cumulative_wait_seconds: float

    @classmethod
    def _between(
        cls,
        start: WeightedBulkheadStatus,
        end: WeightedBulkheadStatus,
    ) -> WeightedBulkheadInterval:
        _validate_weighted_interval_snapshots(start, end)
        return cls(
            start=start,
            end=end,
            admitted=end.admitted_total - start.admitted_total,
            admitted_units=end.admitted_units_total - start.admitted_units_total,
            admitted_from_queue=(end.admitted_from_queue_total - start.admitted_from_queue_total),
            admitted_from_queue_units=(
                end.admitted_from_queue_units_total - start.admitted_from_queue_units_total
            ),
            abandoned_after_admission=(
                end.abandoned_after_admission_total - start.abandoned_after_admission_total
            ),
            abandoned_units=end.abandoned_units_total - start.abandoned_units_total,
            queued=end.queued_total - start.queued_total,
            queued_units=end.queued_units_total - start.queued_units_total,
            saturated=end.saturated_total - start.saturated_total,
            expired=end.expired_total - start.expired_total,
            expired_before_queue=(
                end.expired_before_queue_total - start.expired_before_queue_total
            ),
            cancelled_while_waiting=(
                end.cancelled_while_waiting_total - start.cancelled_while_waiting_total
            ),
            closed_before_queue=(end.closed_before_queue_total - start.closed_before_queue_total),
            closed_while_waiting=(
                end.closed_while_waiting_total - start.closed_while_waiting_total
            ),
            finished=end.finished_total - start.finished_total,
            finished_units=end.finished_units_total - start.finished_units_total,
            cumulative_wait_seconds=(end.cumulative_wait_seconds - start.cumulative_wait_seconds),
        )

    @property
    def direct_admitted(self) -> int:
        """Return interval admissions that did not wait in the queue."""
        return self.admitted - self.admitted_from_queue

    @property
    def direct_admitted_units(self) -> int:
        """Return interval units admitted without waiting in the queue."""
        return self.admitted_units - self.admitted_from_queue_units

    @property
    def closed(self) -> int:
        """Return interval rejections caused by closing."""
        return self.closed_before_queue + self.closed_while_waiting

    @property
    def rejected(self) -> int:
        """Return all interval rejections caused by capacity, deadlines, or closing."""
        return self.saturated + self.expired + self.expired_before_queue + self.closed

    @property
    def settled_waiting(self) -> int:
        """Return queued operations that left the waiting room during the interval."""
        return (
            self.admitted_from_queue
            + self.cancelled_while_waiting
            + self.expired
            + self.closed_while_waiting
        )

    @property
    def average_wait_seconds(self) -> float:
        """Return average queue wait for interval admissions that waited."""
        if self.admitted_from_queue == 0:
            return 0.0
        return self.cumulative_wait_seconds / self.admitted_from_queue

    @property
    def average_admitted_cost(self) -> float:
        """Return average cost among interval admissions."""
        if self.admitted == 0:
            return 0.0
        return self.admitted_units / self.admitted

    @property
    def has_activity(self) -> bool:
        """Return True when at least one cumulative metric changed."""
        return any(
            (
                self.admitted,
                self.abandoned_after_admission,
                self.queued,
                self.saturated,
                self.expired,
                self.expired_before_queue,
                self.cancelled_while_waiting,
                self.closed_before_queue,
                self.closed_while_waiting,
                self.finished,
            )
        )


def _validate_weighted_interval_snapshots(
    start: WeightedBulkheadStatus,
    end: WeightedBulkheadStatus,
) -> None:
    if start.instance_id != end.instance_id:
        raise ValueError("status snapshots must belong to the same weighted bulkhead instance")
    if end.snapshot_index < start.snapshot_index:
        raise ValueError("status snapshots must be provided in chronological order")
    if end.snapshot_index == start.snapshot_index and end != start:
        raise ValueError("snapshots with the same index must be identical")
    if start.label != end.label:
        raise ValueError("status snapshots from one instance must keep the same label")
    if start.waiting_room != end.waiting_room:
        raise ValueError("status snapshots must use the same waiting-room capacity")
    if start.is_closed and not end.is_closed:
        raise ValueError("a later status cannot reopen a closed weighted bulkhead")

    monotonic_fields = (
        "admitted_total",
        "admitted_units_total",
        "admitted_from_queue_total",
        "admitted_from_queue_units_total",
        "abandoned_after_admission_total",
        "abandoned_units_total",
        "queued_total",
        "queued_units_total",
        "saturated_total",
        "expired_total",
        "expired_before_queue_total",
        "cancelled_while_waiting_total",
        "closed_before_queue_total",
        "closed_while_waiting_total",
        "finished_total",
        "finished_units_total",
        "peak_used",
        "peak_in_flight",
        "peak_waiting",
        "peak_waiting_units",
        "cumulative_wait_seconds",
        "longest_wait_seconds",
    )
    for field_name in monotonic_fields:
        if getattr(end, field_name) < getattr(start, field_name):
            raise ValueError(f"later status decreased cumulative field {field_name!r}")
