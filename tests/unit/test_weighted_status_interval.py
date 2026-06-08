from __future__ import annotations

import asyncio
import dataclasses

import pytest

from bulklink import WeightedBulkhead, WeightedBulkheadInterval, WeightedBulkheadStatus


async def test_weighted_status_exposes_capacity_and_interval_units() -> None:
    gate = WeightedBulkhead(label="weighted-interval", capacity=5)
    before = await gate.status()

    await gate.execute(3, asyncio.sleep, 0)

    after = await gate.status()
    interval = after.since(before)

    assert isinstance(interval, WeightedBulkheadInterval)
    assert interval.admitted == 1
    assert interval.admitted_units == 3
    assert interval.direct_admitted == 1
    assert interval.direct_admitted_units == 3
    assert interval.finished == 1
    assert interval.finished_units == 3
    assert interval.average_admitted_cost == 3.0
    assert interval.has_activity


async def test_weighted_interval_rejects_cross_instance_and_reversed_snapshots() -> None:
    first_gate = WeightedBulkhead(label="same", capacity=4)
    second_gate = WeightedBulkhead(label="same", capacity=4)

    first = await first_gate.status()
    second = await second_gate.status()

    with pytest.raises(ValueError, match="same weighted bulkhead instance"):
        second.since(first)

    await first_gate.resize(5)
    later = await first_gate.status()
    with pytest.raises(ValueError, match="chronological order"):
        first.since(later)


async def test_weighted_status_identity_is_stable_and_sequence_is_contiguous() -> None:
    gate = WeightedBulkhead(label="weighted-sequence", capacity=2)

    snapshots = await asyncio.gather(*(gate.status() for _ in range(10)))
    indexes = sorted(snapshot.snapshot_index for snapshot in snapshots)

    assert len({snapshot.instance_id for snapshot in snapshots}) == 1
    assert indexes == list(range(indexes[0], indexes[0] + len(indexes)))


def test_weighted_interval_is_immutable_and_requires_a_status() -> None:
    status = WeightedBulkheadStatus(
        instance_id="weighted-test",
        snapshot_index=1,
        label="weighted-test",
        capacity=2,
        waiting_room=0,
        used=0,
        in_flight=0,
        waiting=0,
        waiting_units=0,
        admitted_total=0,
        admitted_units_total=0,
        admitted_from_queue_total=0,
        admitted_from_queue_units_total=0,
        abandoned_after_admission_total=0,
        abandoned_units_total=0,
        queued_total=0,
        queued_units_total=0,
        saturated_total=0,
        expired_total=0,
        expired_before_queue_total=0,
        cancelled_while_waiting_total=0,
        closed_before_queue_total=0,
        closed_while_waiting_total=0,
        finished_total=0,
        finished_units_total=0,
        peak_used=0,
        peak_in_flight=0,
        peak_waiting=0,
        peak_waiting_units=0,
        cumulative_wait_seconds=0.0,
        longest_wait_seconds=0.0,
        is_closed=False,
    )
    interval = status.since(status)

    with pytest.raises(dataclasses.FrozenInstanceError):
        interval.admitted = 1  # type: ignore[misc]
    with pytest.raises(TypeError, match="WeightedBulkheadStatus"):
        status.since(object())  # type: ignore[arg-type]


async def test_weighted_status_and_interval_derived_properties_cover_queue_outcomes() -> None:
    gate = WeightedBulkhead(
        label="weighted-derived",
        capacity=2,
        waiting_room=1,
        wait_limit=0.02,
    )
    initial = await gate.status()
    assert initial.available == 2
    assert initial.capacity_excess == 0
    assert not initial.is_over_capacity
    assert not initial.is_saturated
    assert initial.utilization == 0.0
    assert initial.queue_utilization == 0.0
    assert initial.direct_admitted_total == 0
    assert initial.direct_admitted_units_total == 0
    assert initial.closed_total == 0
    assert initial.rejected_total == 0
    assert initial.settled_waiting_total == 0
    assert initial.average_wait_seconds == 0.0
    assert initial.average_admitted_cost == 0.0

    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot(2):
            await release.wait()

    active = asyncio.create_task(hold())
    while (await gate.status()).used != 2:
        await asyncio.sleep(0)
    before = await gate.status()

    with pytest.raises(Exception) as captured:
        await gate.execute(1, asyncio.sleep, 0)
    assert captured.type.__name__ == "BulkheadQueueTimeoutError"

    release.set()
    await active
    await gate.close()
    with pytest.raises(Exception) as closed:
        await gate.execute(1, asyncio.sleep, 0)
    assert closed.type.__name__ == "BulkheadClosedError"

    after = await gate.status()
    interval = after.since(before)
    assert after.available == 2
    assert after.closed_total == 1
    assert after.rejected_total == 2
    assert interval.expired == 1
    assert interval.closed_before_queue == 1
    assert interval.closed == 1
    assert interval.rejected == 2
    assert interval.settled_waiting == 1
    assert interval.average_wait_seconds == 0.0
    assert interval.average_admitted_cost == 0.0
