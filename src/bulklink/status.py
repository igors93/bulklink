"""Immutable runtime status for one bulkhead."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BulkheadStatus:
    """Read-only point-in-time description of one bulkhead."""

    label: str
    parallelism: int
    waiting_room: int
    in_flight: int
    waiting: int
    admitted_total: int
    admitted_from_queue_total: int
    queued_total: int
    saturated_total: int
    expired_total: int
    cancelled_total: int
    closed_total: int
    finished_total: int
    peak_in_flight: int
    peak_waiting: int
    cumulative_wait_seconds: float
    longest_wait_seconds: float
    is_closed: bool

    @property
    def free_slots(self) -> int:
        """Return execution slots currently available for immediate admission."""
        return max(0, self.parallelism - self.in_flight)

    @property
    def is_saturated(self) -> bool:
        """Return True when no execution slot is immediately available."""
        return self.free_slots == 0

    @property
    def rejected_total(self) -> int:
        """Return all capacity, deadline, and closed-state rejections."""
        return self.saturated_total + self.expired_total + self.closed_total

    @property
    def average_wait_seconds(self) -> float:
        """Return average wait for operations admitted from the queue."""
        if self.admitted_from_queue_total == 0:
            return 0.0
        return self.cumulative_wait_seconds / self.admitted_from_queue_total
