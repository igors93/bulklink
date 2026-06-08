from __future__ import annotations

import asyncio
import dataclasses
from typing import Any

import pytest

from bulklink import PartitionedBulkhead, PartitionedBulkheadInterval
from tests.unit.test_partitioned_bulkhead import _pause_victim_close


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


async def test_derived_properties_reflect_materialized_partitions_only() -> None:
    """available_partition_slots, is_at_limit, and partition_utilization reflect
    partition_count (materialized children), not the logical count that includes
    pending eviction reservations.  This documents the known transient behaviour.
    """
    gate = PartitionedBulkhead(label="obs", parallelism=1, max_partitions=1)
    # Use keys that cannot appear as substrings in class or field names.
    old_key = "tenant-xqz-orig"
    new_key = "tenant-xqz-new"

    await gate.execute(old_key, asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, old_key)
    t1 = asyncio.create_task(gate.execute(new_key, asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # At this point: old_key removed from map, reservation held, new_key not yet created.
    # The snapshot sees partition_count == 0 even though the slot is logically occupied.
    mid_status = await gate.status()

    # Documented behaviour: these properties reflect materialized children only.
    assert mid_status.partition_count == 0
    assert mid_status.available_partition_slots == 1  # snapshot-based, not logical
    assert not mid_status.is_at_limit  # snapshot-based, not logical
    assert mid_status.partition_utilization == 0.0  # snapshot-based, not logical

    # Keys must never appear in the public status representation.
    status_repr = repr(mid_status)
    assert old_key not in status_repr
    assert new_key not in status_repr

    allow_close.set()
    await asyncio.wait_for(t1, timeout=2.0)

    # After replacement: exactly one partition, counters coherent.
    final = await gate.status()
    assert final.partition_count == 1
    assert final.available_partition_slots == 0
    assert final.is_at_limit
    assert final.partition_utilization == 1.0
    assert final.created_total == 2
    assert final.evicted_total == 1

    await gate.close_and_wait()


async def test_status_counters_are_monotonically_non_decreasing() -> None:
    """Cumulative counters must never decrease between two sequential snapshots."""
    gate = PartitionedBulkhead(label="mono", parallelism=1, max_partitions=1)

    snapshots = []
    await gate.execute("A", asyncio.sleep, 0)
    snapshots.append(await gate.status())
    await gate.execute("B", asyncio.sleep, 0)
    snapshots.append(await gate.status())
    await gate.execute("C", asyncio.sleep, 0)
    snapshots.append(await gate.status())

    monotonic = (
        "created_total",
        "evicted_total",
        "peak_partitions",
        "peak_leased_operations",
    )
    for field_name in monotonic:
        values = [getattr(s, field_name) for s in snapshots]
        assert values == sorted(values), (
            f"{field_name} is not monotonically non-decreasing: {values}"
        )

    await gate.close_and_wait()


async def test_partition_limit_error_does_not_expose_keys() -> None:
    """PartitionLimitError message must not contain the rejected partition key."""
    from bulklink import PartitionLimitError

    gate = PartitionedBulkhead(label="key-privacy", parallelism=1, max_partitions=1)
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("secret-tenant-id", release.wait))
    await asyncio.wait_for(asyncio.create_task(_wait_leased(gate, 1)), timeout=1.0)

    with pytest.raises(PartitionLimitError) as exc_info:
        await gate.execute("another-secret", asyncio.sleep, 0)

    err = exc_info.value
    assert "secret-tenant-id" not in str(err)
    assert "another-secret" not in str(err)

    release.set()
    await active
    await gate.close_and_wait()


async def _wait_leased(gate: Any, expected: int) -> None:
    from tests.helpers import eventually

    await eventually(lambda: _has_leased(gate, expected))


async def _has_leased(gate: Any, expected: int) -> bool:
    return (await gate.status()).leased_operations == expected


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
