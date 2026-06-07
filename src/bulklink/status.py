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
    abandoned_after_admission_total: int
    queued_total: int
    saturated_total: int
    expired_total: int
    cancelled_while_waiting_total: int
    closed_before_queue_total: int
    closed_while_waiting_total: int
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
    def utilization(self) -> float:
        """Return the fraction of execution capacity currently allocated."""
        return self.in_flight / self.parallelism

    @property
    def queue_utilization(self) -> float:
        """Return the fraction of waiting-room capacity currently occupied."""
        if self.waiting_room == 0:
            return 0.0
        return self.waiting / self.waiting_room

    @property
    def direct_admitted_total(self) -> int:
        """Return admissions that did not wait in the FIFO queue."""
        return self.admitted_total - self.admitted_from_queue_total

    @property
    def closed_total(self) -> int:
        """Return all operations rejected because the bulkhead was closed."""
        return self.closed_before_queue_total + self.closed_while_waiting_total

    @property
    def rejected_total(self) -> int:
        """Return all operations rejected by capacity, deadline, or closing."""
        return self.saturated_total + self.expired_total + self.closed_total

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
