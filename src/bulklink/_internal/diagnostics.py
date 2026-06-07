"""Pure capacity assessment rules for immutable status snapshots."""

from __future__ import annotations

from time import time

from bulklink.capacity import (
    CapacityFinding,
    CapacityFindingCode,
    CapacityReport,
    CapacitySeverity,
)
from bulklink.status import BulkheadStatus

_MIN_CAPACITY_DECISIONS = 20
_MIN_QUEUE_HISTORY = 10
_QUEUE_NEAR_CAPACITY = 0.80
_REJECTION_WARNING = 0.05
_REJECTION_CRITICAL = 0.20
_EXPIRATION_WARNING = 0.10
_EXPIRATION_CRITICAL = 0.30
_FREQUENT_QUEUEING = 0.50
_AVERAGE_WAIT_NEAR_LIMIT = 0.75
_LONGEST_WAIT_NEAR_LIMIT = 0.95
_LARGE_WAITING_ROOM_MULTIPLIER = 20
_LARGE_WAITING_ROOM_MINIMUM = 100


def assess_capacity(
    status: BulkheadStatus,
    *,
    wait_limit: float | None,
    assessed_at: float | None = None,
) -> CapacityReport:
    """Build one deterministic report without mutating bulkhead state."""
    findings: list[CapacityFinding] = []

    if status.is_closed and not status.is_drained:
        findings.append(
            CapacityFinding(
                code=CapacityFindingCode.CLOSED_WITH_ACTIVE_WORK,
                severity=CapacitySeverity.NOTICE,
                message="The bulkhead is closed while active work is still draining.",
                recommendation="Wait for wait_closed() before completing application shutdown.",
            )
        )

    if not status.is_closed:
        _add_current_pressure_findings(status, findings)

    if status.waiting_room > 0 and wait_limit is None:
        findings.append(
            CapacityFinding(
                code=CapacityFindingCode.UNBOUNDED_QUEUE_WAIT,
                severity=CapacitySeverity.NOTICE,
                message="Queued operations have no waiting deadline.",
                recommendation=(
                    "Consider a finite wait_limit for request-response workloads so stale "
                    "operations do not wait indefinitely."
                ),
            )
        )

    if (
        status.waiting_room >= _LARGE_WAITING_ROOM_MINIMUM
        and status.waiting_room >= status.parallelism * _LARGE_WAITING_ROOM_MULTIPLIER
    ):
        findings.append(
            CapacityFinding(
                code=CapacityFindingCode.LARGE_WAITING_ROOM,
                severity=CapacitySeverity.NOTICE,
                message="The waiting room is much larger than execution capacity.",
                recommendation=(
                    "Confirm that the application can tolerate the memory use and latency "
                    "created by a large backlog."
                ),
            )
        )

    report = CapacityReport(
        assessed_at=time() if assessed_at is None else assessed_at,
        status=status,
        wait_limit=wait_limit,
        findings=(),
    )

    _add_historical_findings(report, findings)

    return CapacityReport(
        assessed_at=report.assessed_at,
        status=status,
        wait_limit=wait_limit,
        findings=tuple(findings),
    )


def _add_current_pressure_findings(
    status: BulkheadStatus,
    findings: list[CapacityFinding],
) -> None:
    if not status.is_saturated:
        return

    if status.is_over_capacity:
        findings.append(
            CapacityFinding(
                code=CapacityFindingCode.ACTIVE_WORK_ABOVE_CAPACITY,
                severity=(
                    CapacitySeverity.WARNING if status.waiting > 0 else CapacitySeverity.NOTICE
                ),
                message=("Active work is above the current capacity after a reduction."),
                recommendation=(
                    "Allow existing operations to finish; Bulklink will not admit replacements "
                    "until active work reaches the resized limit."
                ),
            )
        )

    if status.waiting_room > 0 and status.waiting >= status.waiting_room:
        findings.append(
            CapacityFinding(
                code=CapacityFindingCode.WAITING_ROOM_FULL,
                severity=CapacitySeverity.CRITICAL,
                message="Execution capacity and the waiting room are both full.",
                recommendation=(
                    "Reduce incoming work, shorten operation latency, or review whether "
                    "parallelism matches the protected dependency capacity."
                ),
            )
        )
        return

    if not status.is_over_capacity:
        findings.append(
            CapacityFinding(
                code=CapacityFindingCode.EXECUTION_FULL,
                severity=(
                    CapacitySeverity.WARNING
                    if status.waiting > 0 or status.waiting_room == 0
                    else CapacitySeverity.NOTICE
                ),
                message="All execution slots are currently allocated.",
                recommendation=(
                    "Observe whether the condition persists before increasing parallelism; "
                    "the protected dependency may already be at its safe limit."
                ),
            )
        )

    if status.waiting_room > 0 and status.queue_utilization >= _QUEUE_NEAR_CAPACITY:
        findings.append(
            CapacityFinding(
                code=CapacityFindingCode.WAITING_ROOM_NEAR_CAPACITY,
                severity=CapacitySeverity.WARNING,
                message="The waiting room is close to full.",
                recommendation=(
                    "Investigate operation latency and incoming load before the queue starts "
                    "rejecting new work."
                ),
            )
        )


def _add_historical_findings(
    report: CapacityReport,
    findings: list[CapacityFinding],
) -> None:
    if (
        report.open_admission_total >= _MIN_CAPACITY_DECISIONS
        and report.queue_entry_ratio >= _FREQUENT_QUEUEING
    ):
        findings.append(
            CapacityFinding(
                code=CapacityFindingCode.FREQUENT_QUEUEING,
                severity=CapacitySeverity.NOTICE,
                message="At least half of open admission attempts have entered the queue.",
                recommendation=(
                    "Review sustained operation latency and whether upstream concurrency is "
                    "higher than the dependency can safely handle."
                ),
            )
        )

    if report.capacity_decisions_total >= _MIN_CAPACITY_DECISIONS:
        severity = _rate_severity(
            report.rejection_ratio,
            warning=_REJECTION_WARNING,
            critical=_REJECTION_CRITICAL,
        )
        if severity is not None:
            findings.append(
                CapacityFinding(
                    code=CapacityFindingCode.ELEVATED_REJECTION_RATE,
                    severity=severity,
                    message="Capacity-related rejections are elevated in cumulative history.",
                    recommendation=(
                        "Check saturation and queue expiration separately before changing "
                        "parallelism or waiting-room size."
                    ),
                )
            )

    if report.status.queued_total >= _MIN_QUEUE_HISTORY:
        severity = _rate_severity(
            report.expiration_ratio,
            warning=_EXPIRATION_WARNING,
            critical=_EXPIRATION_CRITICAL,
        )
        if severity is not None:
            findings.append(
                CapacityFinding(
                    code=CapacityFindingCode.ELEVATED_EXPIRATION_RATE,
                    severity=severity,
                    message="A significant share of queued operations expired while waiting.",
                    recommendation=(
                        "Reduce queueing delay or route less work to this dependency; increasing "
                        "the deadline alone may only hide overload."
                    ),
                )
            )

    if report.wait_limit is None or report.status.admitted_from_queue_total < 5:
        return

    average_ratio = report.average_wait_limit_ratio
    longest_ratio = report.longest_wait_limit_ratio
    if average_ratio is None or longest_ratio is None:
        return
    if average_ratio < _AVERAGE_WAIT_NEAR_LIMIT and longest_ratio < _LONGEST_WAIT_NEAR_LIMIT:
        return

    findings.append(
        CapacityFinding(
            code=CapacityFindingCode.WAIT_TIME_NEAR_LIMIT,
            severity=CapacitySeverity.WARNING,
            message="Admitted queue waits are approaching the configured waiting deadline.",
            recommendation=(
                "Investigate dependency latency and queue pressure before extending wait_limit."
            ),
        )
    )


def _rate_severity(
    ratio: float,
    *,
    warning: float,
    critical: float,
) -> CapacitySeverity | None:
    if ratio >= critical:
        return CapacitySeverity.CRITICAL
    if ratio >= warning:
        return CapacitySeverity.WARNING
    return None
