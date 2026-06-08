from __future__ import annotations

import inspect
from dataclasses import fields

from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
    BulkheadEvent,
    BulkheadEventKind,
    BulkheadInterval,
    BulkheadQueueTimeoutError,
    BulkheadRegistry,
    BulkheadRegistryFailure,
    BulkheadRegistryOperationError,
    BulkheadSaturatedError,
    BulkheadStatus,
    BulklinkError,
    CapacityFinding,
    CapacityFindingCode,
    CapacityReport,
    CapacitySeverity,
)


def _field_names(record: type[object]) -> tuple[str, ...]:
    return tuple(field.name for field in fields(record))


def _parameter_contract(callable_object: object) -> tuple[tuple[str, inspect._ParameterKind], ...]:
    return tuple(
        (parameter.name, parameter.kind)
        for parameter in inspect.signature(callable_object).parameters.values()
    )


def test_public_enum_values_are_stable() -> None:
    assert {member.name: member.value for member in BulkheadEventKind} == {
        "ADMITTED": "admitted",
        "QUEUED": "queued",
        "SATURATED": "saturated",
        "EXPIRED": "expired",
        "CANCELLED": "cancelled",
        "ABANDONED": "abandoned",
        "RELEASED": "released",
        "CLOSED": "closed",
        "CLOSED_REJECTION": "closed_rejection",
        "DRAINED": "drained",
        "RESIZED": "resized",
    }
    assert {member.name: member.value for member in CapacitySeverity} == {
        "OK": "ok",
        "NOTICE": "notice",
        "WARNING": "warning",
        "CRITICAL": "critical",
    }
    assert {member.name: member.value for member in CapacityFindingCode} == {
        "CLOSED_WITH_ACTIVE_WORK": "closed_with_active_work",
        "EXECUTION_FULL": "execution_full",
        "ACTIVE_WORK_ABOVE_CAPACITY": "active_work_above_capacity",
        "WAITING_ROOM_NEAR_CAPACITY": "waiting_room_near_capacity",
        "WAITING_ROOM_FULL": "waiting_room_full",
        "UNBOUNDED_QUEUE_WAIT": "unbounded_queue_wait",
        "LARGE_WAITING_ROOM": "large_waiting_room",
        "FREQUENT_QUEUEING": "frequent_queueing",
        "ELEVATED_REJECTION_RATE": "elevated_rejection_rate",
        "ELEVATED_EXPIRATION_RATE": "elevated_expiration_rate",
        "WAIT_TIME_NEAR_LIMIT": "wait_time_near_limit",
    }


def test_public_immutable_record_fields_are_stable() -> None:
    assert _field_names(BulkheadInterval) == (
        "start",
        "end",
        "admitted",
        "admitted_from_queue",
        "abandoned_after_admission",
        "queued",
        "saturated",
        "expired",
        "expired_before_queue",
        "cancelled_while_waiting",
        "closed_before_queue",
        "closed_while_waiting",
        "finished",
        "cumulative_wait_seconds",
    )
    assert _field_names(BulkheadStatus) == (
        "label",
        "parallelism",
        "waiting_room",
        "in_flight",
        "waiting",
        "admitted_total",
        "admitted_from_queue_total",
        "abandoned_after_admission_total",
        "queued_total",
        "saturated_total",
        "expired_total",
        "expired_before_queue_total",
        "cancelled_while_waiting_total",
        "closed_before_queue_total",
        "closed_while_waiting_total",
        "finished_total",
        "peak_in_flight",
        "peak_waiting",
        "cumulative_wait_seconds",
        "longest_wait_seconds",
        "is_closed",
    )
    assert _field_names(BulkheadEvent) == (
        "kind",
        "label",
        "occurred_at",
        "parallelism",
        "waiting_room",
        "in_flight",
        "waiting",
        "is_closed",
        "from_queue",
        "waited_seconds",
        "affected_waiters",
        "previous_parallelism",
    )
    assert _field_names(CapacityFinding) == (
        "code",
        "severity",
        "message",
        "recommendation",
    )
    assert _field_names(CapacityReport) == (
        "assessed_at",
        "status",
        "wait_limit",
        "findings",
    )
    assert _field_names(BulkheadRegistryFailure) == (
        "label",
        "error_type",
        "message",
    )


def test_public_exception_hierarchy_is_stable() -> None:
    assert issubclass(BulkheadClosedError, BulklinkError)
    assert issubclass(BulkheadSaturatedError, BulklinkError)
    assert issubclass(BulkheadQueueTimeoutError, BulklinkError)
    assert issubclass(BulkheadRegistryOperationError, BulklinkError)
    assert not issubclass(BulkheadQueueTimeoutError, TimeoutError)


def test_primary_calling_conventions_are_stable() -> None:
    assert _parameter_contract(AsyncBulkhead.__init__) == (
        ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("label", inspect.Parameter.KEYWORD_ONLY),
        ("parallelism", inspect.Parameter.KEYWORD_ONLY),
        ("waiting_room", inspect.Parameter.KEYWORD_ONLY),
        ("wait_limit", inspect.Parameter.KEYWORD_ONLY),
    )
    assert _parameter_contract(AsyncBulkhead.execute) == (
        ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("operation", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("args", inspect.Parameter.VAR_POSITIONAL),
        ("kwargs", inspect.Parameter.VAR_KEYWORD),
    )
    assert _parameter_contract(AsyncBulkhead.execute_now) == _parameter_contract(
        AsyncBulkhead.execute
    )
    assert _parameter_contract(AsyncBulkhead.execute_within) == (
        ("self", inspect.Parameter.POSITIONAL_ONLY),
        ("wait_limit", inspect.Parameter.POSITIONAL_ONLY),
        ("operation", inspect.Parameter.POSITIONAL_ONLY),
        ("args", inspect.Parameter.VAR_POSITIONAL),
        ("kwargs", inspect.Parameter.VAR_KEYWORD),
    )
    assert _parameter_contract(AsyncBulkhead.slot_within) == (
        ("self", inspect.Parameter.POSITIONAL_ONLY),
        ("wait_limit", inspect.Parameter.POSITIONAL_ONLY),
    )
    assert _parameter_contract(AsyncBulkhead.execute_before) == (
        ("self", inspect.Parameter.POSITIONAL_ONLY),
        ("deadline", inspect.Parameter.POSITIONAL_ONLY),
        ("operation", inspect.Parameter.POSITIONAL_ONLY),
        ("args", inspect.Parameter.VAR_POSITIONAL),
        ("kwargs", inspect.Parameter.VAR_KEYWORD),
    )
    assert _parameter_contract(AsyncBulkhead.slot_before) == (
        ("self", inspect.Parameter.POSITIONAL_ONLY),
        ("deadline", inspect.Parameter.POSITIONAL_ONLY),
    )
    assert _parameter_contract(AsyncBulkhead.resize) == (
        ("self", inspect.Parameter.POSITIONAL_ONLY),
        ("parallelism", inspect.Parameter.POSITIONAL_ONLY),
    )
    assert _parameter_contract(BulkheadRegistry.create) == (
        ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("label", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("parallelism", inspect.Parameter.KEYWORD_ONLY),
        ("waiting_room", inspect.Parameter.KEYWORD_ONLY),
        ("wait_limit", inspect.Parameter.KEYWORD_ONLY),
    )


def test_interval_calling_convention_is_stable() -> None:
    assert _parameter_contract(BulkheadStatus.since) == (
        ("self", inspect.Parameter.POSITIONAL_ONLY),
        ("previous", inspect.Parameter.POSITIONAL_ONLY),
    )
