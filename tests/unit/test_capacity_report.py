from __future__ import annotations

import asyncio
import dataclasses

import pytest

from bulklink import (
    AsyncBulkhead,
    CapacityFinding,
    CapacityFindingCode,
    CapacityReport,
    CapacitySeverity,
)
from bulklink._internal.diagnostics import assess_capacity
from bulklink.status import BulkheadStatus
from tests.helpers import eventually


async def test_capacity_report_is_immutable_and_does_not_mutate_state() -> None:
    gate = AsyncBulkhead(
        label="capacity-report",
        parallelism=2,
        waiting_room=4,
        wait_limit=1.0,
    )

    before = await gate.status()
    report = await gate.capacity_report()
    after = await gate.status()

    assert report.status == before == after
    assert report.wait_limit == 1.0
    assert report.findings == ()
    assert report.severity is CapacitySeverity.OK
    assert report.is_healthy
    assert not report.requires_attention
    assert not report.has_critical_findings
    assert report.summary == "No capacity pressure was detected in this snapshot."
    assert report.assessed_at > 0

    with pytest.raises(dataclasses.FrozenInstanceError):
        report.assessed_at = 0.0  # type: ignore[misc]


async def test_capacity_report_captures_current_queue_pressure() -> None:
    gate = AsyncBulkhead(label="current-pressure", parallelism=1, waiting_room=2)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    report = await gate.capacity_report()
    codes = {finding.code for finding in report.findings}

    assert CapacityFindingCode.EXECUTION_FULL in codes
    assert report.severity is CapacitySeverity.WARNING
    assert report.requires_attention

    release.set()
    await asyncio.gather(active, queued)


def test_unbounded_wait_is_advisory_not_capacity_failure() -> None:
    report = assess_capacity(
        make_status(waiting_room=10),
        wait_limit=None,
        assessed_at=1.0,
    )

    assert finding_codes(report) == [CapacityFindingCode.UNBOUNDED_QUEUE_WAIT]
    assert report.severity is CapacitySeverity.NOTICE
    assert report.is_healthy
    assert not report.requires_attention


def test_full_waiting_room_is_critical() -> None:
    report = assess_capacity(
        make_status(
            parallelism=2,
            waiting_room=4,
            in_flight=2,
            waiting=4,
        ),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    assert finding_codes(report) == [CapacityFindingCode.WAITING_ROOM_FULL]
    assert report.severity is CapacitySeverity.CRITICAL
    assert report.has_critical_findings
    assert report.summary == "Critical capacity pressure is present."


def test_near_full_waiting_room_is_reported_without_duplicate_full_finding() -> None:
    report = assess_capacity(
        make_status(
            parallelism=2,
            waiting_room=10,
            in_flight=2,
            waiting=8,
        ),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    assert finding_codes(report) == [
        CapacityFindingCode.EXECUTION_FULL,
        CapacityFindingCode.WAITING_ROOM_NEAR_CAPACITY,
    ]
    assert report.severity is CapacitySeverity.WARNING


def test_rate_findings_require_enough_history() -> None:
    report = assess_capacity(
        make_status(
            admitted_total=1,
            saturated_total=1,
            queued_total=1,
            expired_total=1,
        ),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    assert CapacityFindingCode.ELEVATED_REJECTION_RATE not in finding_codes(report)
    assert CapacityFindingCode.ELEVATED_EXPIRATION_RATE not in finding_codes(report)


def test_elevated_rejection_rate_uses_capacity_decisions_only() -> None:
    warning = assess_capacity(
        make_status(
            admitted_total=19,
            saturated_total=1,
            closed_before_queue_total=100,
        ),
        wait_limit=1.0,
        assessed_at=1.0,
    )
    critical = assess_capacity(
        make_status(admitted_total=16, saturated_total=4),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    warning_finding = find(warning, CapacityFindingCode.ELEVATED_REJECTION_RATE)
    critical_finding = find(critical, CapacityFindingCode.ELEVATED_REJECTION_RATE)

    assert warning.rejection_ratio == pytest.approx(0.05)
    assert warning_finding.severity is CapacitySeverity.WARNING
    assert critical.rejection_ratio == pytest.approx(0.20)
    assert critical_finding.severity is CapacitySeverity.CRITICAL


def test_elevated_expiration_rate_uses_queued_history() -> None:
    warning = assess_capacity(
        make_status(queued_total=10, expired_total=1),
        wait_limit=1.0,
        assessed_at=1.0,
    )
    critical = assess_capacity(
        make_status(queued_total=10, expired_total=3),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    assert warning.expiration_ratio == pytest.approx(0.10)
    assert (
        find(
            warning,
            CapacityFindingCode.ELEVATED_EXPIRATION_RATE,
        ).severity
        is CapacitySeverity.WARNING
    )
    assert critical.expiration_ratio == pytest.approx(0.30)
    assert (
        find(
            critical,
            CapacityFindingCode.ELEVATED_EXPIRATION_RATE,
        ).severity
        is CapacitySeverity.CRITICAL
    )


def test_frequent_queueing_is_advisory() -> None:
    report = assess_capacity(
        make_status(
            admitted_total=20,
            admitted_from_queue_total=10,
            queued_total=10,
        ),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    assert report.queue_entry_ratio == pytest.approx(0.50)
    finding = find(report, CapacityFindingCode.FREQUENT_QUEUEING)
    assert finding.severity is CapacitySeverity.NOTICE


def test_wait_time_near_limit_requires_representative_admitted_history() -> None:
    report = assess_capacity(
        make_status(
            admitted_total=5,
            admitted_from_queue_total=5,
            queued_total=5,
            cumulative_wait_seconds=3.75,
            longest_wait_seconds=0.95,
        ),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    assert report.average_wait_limit_ratio == pytest.approx(0.75)
    assert report.longest_wait_limit_ratio == pytest.approx(0.95)
    assert (
        find(
            report,
            CapacityFindingCode.WAIT_TIME_NEAR_LIMIT,
        ).severity
        is CapacitySeverity.WARNING
    )


def test_large_waiting_room_is_reported_as_advisory() -> None:
    report = assess_capacity(
        make_status(parallelism=5, waiting_room=100),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    assert finding_codes(report) == [CapacityFindingCode.LARGE_WAITING_ROOM]
    assert report.severity is CapacitySeverity.NOTICE


def test_closed_reports_distinguish_draining_from_drained() -> None:
    draining = assess_capacity(
        make_status(is_closed=True, in_flight=1),
        wait_limit=1.0,
        assessed_at=1.0,
    )
    drained = assess_capacity(
        make_status(is_closed=True),
        wait_limit=1.0,
        assessed_at=1.0,
    )

    assert (
        find(
            draining,
            CapacityFindingCode.CLOSED_WITH_ACTIVE_WORK,
        ).severity
        is CapacitySeverity.NOTICE
    )
    assert draining.summary == "The bulkhead is closed and is draining active work."
    assert drained.findings == ()
    assert drained.summary == "The bulkhead is closed and fully drained."


def test_report_ratios_are_zero_without_history() -> None:
    report = assess_capacity(
        make_status(),
        wait_limit=None,
        assessed_at=1.0,
    )

    assert report.capacity_decisions_total == 0
    assert report.capacity_rejected_total == 0
    assert report.rejection_ratio == 0.0
    assert report.open_admission_total == 0
    assert report.queue_entry_ratio == 0.0
    assert report.expiration_ratio == 0.0
    assert report.average_wait_limit_ratio is None
    assert report.longest_wait_limit_ratio is None


def test_capacity_contract_contains_no_operation_data() -> None:
    finding_fields = {field.name for field in dataclasses.fields(CapacityFinding)}
    report_fields = {field.name for field in dataclasses.fields(CapacityReport)}

    prohibited = {"operation", "args", "kwargs", "result", "exception"}
    assert finding_fields.isdisjoint(prohibited)
    assert report_fields.isdisjoint(prohibited)


def make_status(**overrides: object) -> BulkheadStatus:
    values: dict[str, object] = {
        "label": "diagnostics",
        "parallelism": 1,
        "waiting_room": 0,
        "in_flight": 0,
        "waiting": 0,
        "admitted_total": 0,
        "admitted_from_queue_total": 0,
        "abandoned_after_admission_total": 0,
        "queued_total": 0,
        "saturated_total": 0,
        "expired_total": 0,
        "cancelled_while_waiting_total": 0,
        "closed_before_queue_total": 0,
        "closed_while_waiting_total": 0,
        "finished_total": 0,
        "peak_in_flight": 0,
        "peak_waiting": 0,
        "cumulative_wait_seconds": 0.0,
        "longest_wait_seconds": 0.0,
        "is_closed": False,
    }
    values.update(overrides)
    return BulkheadStatus(**values)  # type: ignore[arg-type]


def finding_codes(report: CapacityReport) -> list[CapacityFindingCode]:
    return [finding.code for finding in report.findings]


def find(report: CapacityReport, code: CapacityFindingCode) -> CapacityFinding:
    return next(finding for finding in report.findings if finding.code is code)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
