from __future__ import annotations

import asyncio
import dataclasses

import pytest

from bulklink import PartitionedBulkhead, PartitionedBulkheadInterval


async def test_status_reports_cardinality_without_exposing_keys() -> None:
    gate = PartitionedBulkhead(
        label="status",
        parallelism=2,
        max_partitions=4,
        waiting_room=3,
        wait_limit=1.0,
        idle_timeout=10.0,
    )

    await gate.execute("customer-secret", asyncio.sleep, 0)
    current = await gate.status()

    assert current.label == "status"
    assert current.parallelism == 2
    assert current.waiting_room == 3
    assert current.wait_limit == 1.0
    assert current.max_partitions == 4
    assert current.idle_timeout == 10.0
    assert current.partition_count == 1
    assert current.active_partitions == 0
    assert current.idle_partitions == 1
    assert current.available_partition_slots == 3
    assert current.partition_utilization == 0.25
    assert current.created_total == 1
    assert "customer-secret" not in repr(current)


async def test_interval_reports_partition_lifecycle_changes() -> None:
    gate = PartitionedBulkhead(
        label="interval",
        parallelism=1,
        max_partitions=1,
    )
    before = await gate.status()

    await gate.execute("alpha", asyncio.sleep, 0)
    await gate.execute("beta", asyncio.sleep, 0)
    assert await gate.discard("beta")

    after = await gate.status()
    interval = after.since(before)

    assert isinstance(interval, PartitionedBulkheadInterval)
    assert interval.created == 2
    assert interval.evicted == 1
    assert interval.discarded == 1
    assert interval.reclaimed == 2
    assert interval.limit_rejected == 0
    assert interval.has_activity


async def test_interval_rejects_different_instances_and_reversed_order() -> None:
    first_gate = PartitionedBulkhead(label="same", parallelism=1, max_partitions=1)
    second_gate = PartitionedBulkhead(label="same", parallelism=1, max_partitions=1)
    first = await first_gate.status()
    second = await second_gate.status()

    with pytest.raises(ValueError, match="same partitioned bulkhead instance"):
        second.since(first)

    later = await first_gate.status()
    with pytest.raises(ValueError, match="chronological order"):
        first.since(later)


async def test_status_sequence_is_unique_under_concurrency() -> None:
    gate = PartitionedBulkhead(label="sequence", parallelism=1, max_partitions=2)

    snapshots = await asyncio.gather(*(gate.status() for _ in range(20)))
    indexes = sorted(status.snapshot_index for status in snapshots)

    assert len({status.instance_id for status in snapshots}) == 1
    assert indexes == list(range(indexes[0], indexes[0] + len(indexes)))


def test_interval_rejects_same_index_with_conflicting_state() -> None:
    status = make_status()
    conflicting = dataclasses.replace(status, partition_count=1)

    with pytest.raises(ValueError, match="same index"):
        conflicting.since(status)


def test_status_and_interval_are_immutable() -> None:
    status = make_status()
    interval = status.since(status)

    with pytest.raises(dataclasses.FrozenInstanceError):
        status.partition_count = 2  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        interval.created = 1  # type: ignore[misc]


def make_status() -> object:
    from bulklink import PartitionedBulkheadStatus

    return PartitionedBulkheadStatus(
        instance_id="test-instance",
        snapshot_index=1,
        label="test",
        parallelism=1,
        waiting_room=0,
        wait_limit=None,
        max_partitions=2,
        idle_timeout=10.0,
        partition_count=0,
        active_partitions=0,
        leased_operations=0,
        created_total=0,
        evicted_total=0,
        discarded_total=0,
        limit_rejected_total=0,
        peak_partitions=0,
        peak_leased_operations=0,
        is_closed=False,
    )
