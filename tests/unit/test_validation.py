from __future__ import annotations

import math

import pytest

from bulklink import AsyncBulkhead


@pytest.mark.parametrize("value", ["", "   ", 12, None])
def test_label_must_be_non_empty_string(value: object) -> None:
    with pytest.raises(ValueError):
        AsyncBulkhead(label=value, parallelism=1)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "2"])
def test_parallelism_must_be_positive_integer(value: object) -> None:
    with pytest.raises(ValueError):
        AsyncBulkhead(label="x", parallelism=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [-1, True, 1.5, "2"])
def test_waiting_room_must_be_non_negative_integer(value: object) -> None:
    with pytest.raises(ValueError):
        AsyncBulkhead(label="x", parallelism=1, waiting_room=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, -1, True, math.inf, math.nan, "1"])
def test_wait_limit_must_be_positive_finite_number(value: object) -> None:
    with pytest.raises(ValueError):
        AsyncBulkhead(label="x", parallelism=1, wait_limit=value)  # type: ignore[arg-type]


def test_label_is_normalized_and_properties_are_exposed() -> None:
    gate = AsyncBulkhead(
        label="  payments  ",
        parallelism=2,
        waiting_room=3,
        wait_limit=1,
    )

    assert gate.label == "payments"
    assert gate.parallelism == 2
    assert gate.waiting_room == 3
    assert gate.wait_limit == 1.0


@pytest.mark.parametrize("value", [0, -1, True, math.inf, math.nan, "1"])
def test_slot_within_requires_a_positive_finite_number(value: object) -> None:
    gate = AsyncBulkhead(label="per-call-validation", parallelism=1)

    with pytest.raises(ValueError):
        gate.slot_within(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, -1, True, math.inf, math.nan, "1"])
async def test_execute_within_requires_a_positive_finite_number(value: object) -> None:
    gate = AsyncBulkhead(label="execute-validation", parallelism=1)

    with pytest.raises(ValueError):
        await gate.execute_within(value, asyncio_noop)  # type: ignore[arg-type]


async def asyncio_noop() -> None:
    return None


@pytest.mark.parametrize("value", [True, math.inf, -math.inf, math.nan, "1", None])
def test_slot_before_requires_a_finite_number(value: object) -> None:
    gate = AsyncBulkhead(label="deadline-validation", parallelism=1)

    with pytest.raises(ValueError):
        gate.slot_before(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [True, math.inf, -math.inf, math.nan, "1", None])
async def test_execute_before_requires_a_finite_number(value: object) -> None:
    gate = AsyncBulkhead(label="execute-deadline-validation", parallelism=1)

    with pytest.raises(ValueError):
        await gate.execute_before(value, asyncio_noop)  # type: ignore[arg-type]
