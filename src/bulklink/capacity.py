"""Immutable capacity diagnostics for one bulkhead snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from bulklink.status import BulkheadStatus


class CapacitySeverity(str, Enum):
    """Severity assigned to capacity findings and reports."""

    OK = "ok"
    NOTICE = "notice"
    WARNING = "warning"
    CRITICAL = "critical"


class CapacityFindingCode(str, Enum):
    """Stable machine-readable categories of capacity findings."""

    CLOSED_WITH_ACTIVE_WORK = "closed_with_active_work"
    EXECUTION_FULL = "execution_full"
    ACTIVE_WORK_ABOVE_CAPACITY = "active_work_above_capacity"
    WAITING_ROOM_NEAR_CAPACITY = "waiting_room_near_capacity"
    WAITING_ROOM_FULL = "waiting_room_full"
    UNBOUNDED_QUEUE_WAIT = "unbounded_queue_wait"
    LARGE_WAITING_ROOM = "large_waiting_room"
    FREQUENT_QUEUEING = "frequent_queueing"
    ELEVATED_REJECTION_RATE = "elevated_rejection_rate"
    ELEVATED_EXPIRATION_RATE = "elevated_expiration_rate"
    WAIT_TIME_NEAR_LIMIT = "wait_time_near_limit"


@dataclass(frozen=True, slots=True)
class CapacityFinding:
    """One immutable diagnostic observation and its recommended response."""

    code: CapacityFindingCode
    severity: CapacitySeverity
    message: str
    recommendation: str


_SEVERITY_RANK = {
    CapacitySeverity.OK: 0,
    CapacitySeverity.NOTICE: 1,
    CapacitySeverity.WARNING: 2,
    CapacitySeverity.CRITICAL: 3,
}


@dataclass(frozen=True, slots=True)
class CapacityReport:
    """Immutable interpretation of one status snapshot and cumulative history."""

    assessed_at: float
    status: BulkheadStatus
    wait_limit: float | None
    findings: tuple[CapacityFinding, ...]

    @property
    def severity(self) -> CapacitySeverity:
        """Return the highest severity in the report."""
        return max(
            (finding.severity for finding in self.findings),
            key=_SEVERITY_RANK.__getitem__,
            default=CapacitySeverity.OK,
        )

    @property
    def requires_attention(self) -> bool:
        """Return True when at least one warning or critical finding exists."""
        return _SEVERITY_RANK[self.severity] >= _SEVERITY_RANK[CapacitySeverity.WARNING]

    @property
    def is_healthy(self) -> bool:
        """Return True when no warning or critical finding exists."""
        return not self.requires_attention

    @property
    def has_critical_findings(self) -> bool:
        """Return True when at least one critical finding exists."""
        return self.severity is CapacitySeverity.CRITICAL

    @property
    def capacity_decisions_total(self) -> int:
        """Return admissions and capacity-related rejections while open."""
        return self.status.admitted_total + self.status.saturated_total + self.status.expired_total

    @property
    def capacity_rejected_total(self) -> int:
        """Return saturation and waiting-deadline rejections."""
        return self.status.saturated_total + self.status.expired_total

    @property
    def rejection_ratio(self) -> float:
        """Return capacity-related rejections divided by capacity decisions."""
        if self.capacity_decisions_total == 0:
            return 0.0
        return self.capacity_rejected_total / self.capacity_decisions_total

    @property
    def open_admission_total(self) -> int:
        """Return direct admissions, queued arrivals, and saturation rejections."""
        return (
            self.status.direct_admitted_total
            + self.status.queued_total
            + self.status.saturated_total
        )

    @property
    def queue_entry_ratio(self) -> float:
        """Return the fraction of open admission attempts that entered the queue."""
        if self.open_admission_total == 0:
            return 0.0
        return self.status.queued_total / self.open_admission_total

    @property
    def expiration_ratio(self) -> float:
        """Return the fraction of queued operations that expired while waiting."""
        if self.status.queued_total == 0:
            return 0.0
        return self.status.expired_total / self.status.queued_total

    @property
    def average_wait_limit_ratio(self) -> float | None:
        """Return average admitted wait as a fraction of the configured limit."""
        if self.wait_limit is None:
            return None
        return self.status.average_wait_seconds / self.wait_limit

    @property
    def longest_wait_limit_ratio(self) -> float | None:
        """Return longest admitted wait as a fraction of the configured limit."""
        if self.wait_limit is None:
            return None
        return self.status.longest_wait_seconds / self.wait_limit

    @property
    def summary(self) -> str:
        """Return a short human-readable interpretation of the report."""
        if self.status.is_drained:
            return "The bulkhead is closed and fully drained."
        if self.status.is_closed:
            return "The bulkhead is closed and is draining active work."
        if self.severity is CapacitySeverity.CRITICAL:
            return "Critical capacity pressure is present."
        if self.severity is CapacitySeverity.WARNING:
            return "Capacity pressure requires attention."
        if self.severity is CapacitySeverity.NOTICE:
            return "No immediate pressure is present, but advisory findings exist."
        return "No capacity pressure was detected in this snapshot."
