from __future__ import annotations

import asyncio
from typing import Any

import pytest

from bulklink import (
    BulkheadClosedError,
    BulkheadSaturatedError,
    PartitionedBulkhead,
    PartitionLimitError,
)
from tests.helpers import eventually


async def test_different_keys_receive_independent_execution_capacity() -> None:
    gate = PartitionedBulkhead(
        label="tenants",
        parallelism=1,
        max_partitions=2,
    )
    first_entered = asyncio.Event()
    second_entered = asyncio.Event()
    release = asyncio.Event()

    async def hold(entered: asyncio.Event) -> None:
        entered.set()
        await release.wait()

    first = asyncio.create_task(gate.execute("alpha", hold, first_entered))
    second = asyncio.create_task(gate.execute("beta", hold, second_entered))

    await asyncio.wait_for(first_entered.wait(), timeout=1.0)
    await asyncio.wait_for(second_entered.wait(), timeout=1.0)

    status = await gate.status()
    assert status.partition_count == 2
    assert status.active_partitions == 2
    assert status.leased_operations == 2

    release.set()
    await asyncio.gather(first, second)
    await gate.close_and_wait()


async def test_same_key_uses_one_partition_limit() -> None:
    gate = PartitionedBulkhead(
        label="per-tenant",
        parallelism=1,
        max_partitions=2,
        waiting_room=0,
    )
    release = asyncio.Event()

    async def hold() -> None:
        await release.wait()

    active = asyncio.create_task(gate.execute("alpha", hold))
    await eventually(lambda: has_leased_operations(gate, 1))

    with pytest.raises(BulkheadSaturatedError):
        await gate.execute_now("alpha", asyncio.sleep, 0)

    release.set()
    await active


async def test_concurrent_first_use_creates_one_partition() -> None:
    gate = PartitionedBulkhead(
        label="shared-key",
        parallelism=20,
        max_partitions=5,
    )

    await asyncio.gather(*(gate.execute("same", asyncio.sleep, 0) for _ in range(20)))

    status = await gate.status()
    assert status.partition_count == 1
    assert status.created_total == 1
    assert status.peak_leased_operations >= 1


async def test_limit_rejects_new_key_when_every_partition_is_active() -> None:
    gate = PartitionedBulkhead(
        label="bounded-keys",
        parallelism=1,
        max_partitions=2,
    )
    release = asyncio.Event()

    async def hold() -> None:
        await release.wait()

    active = [asyncio.create_task(gate.execute(key, hold)) for key in ("alpha", "beta")]
    await eventually(lambda: has_leased_operations(gate, 2))

    with pytest.raises(PartitionLimitError) as captured:
        await gate.execute("secret-customer-id", asyncio.sleep, 0)

    error = captured.value
    assert error.label == "bounded-keys"
    assert error.max_partitions == 2
    assert error.active_partitions == 2
    assert "secret-customer-id" not in str(error)
    assert (await gate.status()).limit_rejected_total == 1

    release.set()
    await asyncio.gather(*active)


async def test_capacity_pressure_evicts_least_recent_idle_partition() -> None:
    gate = PartitionedBulkhead(
        label="lru",
        parallelism=1,
        max_partitions=2,
        idle_timeout=60.0,
    )

    await gate.execute("alpha", asyncio.sleep, 0)
    await asyncio.sleep(0)
    await gate.execute("beta", asyncio.sleep, 0)
    await asyncio.sleep(0)
    await gate.execute("beta", asyncio.sleep, 0)
    await gate.execute("gamma", asyncio.sleep, 0)

    status = await gate.status()
    assert status.partition_count == 2
    assert status.created_total == 3
    assert status.evicted_total == 1
    assert not await gate.discard("alpha")
    assert await gate.discard("beta")


async def test_cleanup_removes_only_partitions_past_idle_timeout() -> None:
    gate = PartitionedBulkhead(
        label="cleanup",
        parallelism=1,
        max_partitions=4,
        idle_timeout=0.01,
    )

    await gate.execute("old", asyncio.sleep, 0)
    await asyncio.sleep(0.02)
    await gate.execute("new", asyncio.sleep, 0)

    assert await gate.cleanup_idle() == 1
    status = await gate.status()
    assert status.partition_count == 1
    assert status.evicted_total == 1


async def test_discard_refuses_busy_partition_and_removes_it_after_release() -> None:
    gate = PartitionedBulkhead(
        label="discard",
        parallelism=1,
        max_partitions=2,
    )
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    assert not await gate.discard("alpha")
    release.set()
    await active
    assert await gate.discard("alpha")
    assert not await gate.discard("alpha")
    assert (await gate.status()).discarded_total == 1


async def test_cancelled_waiter_releases_manager_reference() -> None:
    gate = PartitionedBulkhead(
        label="cancelled",
        parallelism=1,
        max_partitions=1,
        waiting_room=1,
    )
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    waiting = asyncio.create_task(gate.execute("alpha", asyncio.sleep, 0))
    await eventually(lambda: has_leased_operations(gate, 2))
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert (await gate.status()).leased_operations == 1
    release.set()
    await active
    assert (await gate.status()).leased_operations == 0


async def test_close_rejects_new_partitions_and_drains_existing_work() -> None:
    gate = PartitionedBulkhead(
        label="shutdown",
        parallelism=1,
        max_partitions=2,
    )
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    await gate.close()
    with pytest.raises(BulkheadClosedError):
        await gate.execute("beta", asyncio.sleep, 0)

    waiter = asyncio.create_task(gate.wait_closed())
    await asyncio.sleep(0)
    assert not waiter.done()
    release.set()
    await asyncio.gather(active, waiter)
    closed = await gate.status()
    assert closed.is_closed
    assert closed.partition_count == 0


async def test_partition_key_must_be_hashable() -> None:
    gate = PartitionedBulkhead(label="keys", parallelism=1, max_partitions=2)

    with pytest.raises(TypeError, match="hashable"):
        gate.slot([])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_partitions": 0}, "max_partitions"),
        ({"max_partitions": True}, "max_partitions"),
        ({"idle_timeout": 0.0}, "idle_timeout"),
        ({"idle_timeout": float("inf")}, "idle_timeout"),
    ],
)
def test_partitioned_configuration_validation(
    kwargs: dict[str, object],
    message: str,
) -> None:
    values: dict[str, object] = {
        "label": "validation",
        "parallelism": 1,
        "max_partitions": 2,
    }
    values.update(kwargs)

    with pytest.raises(ValueError, match=message):
        PartitionedBulkhead(**values)  # type: ignore[arg-type]


async def has_leased_operations(gate: PartitionedBulkhead, expected: int) -> bool:
    return (await gate.status()).leased_operations == expected


# ---------------------------------------------------------------------------
# Eviction race: reserved-slot tests
# ---------------------------------------------------------------------------


def _pause_victim_close(
    gate: PartitionedBulkhead,
    key: Any,
) -> tuple[asyncio.Event, asyncio.Event]:
    """Intercept close_and_wait on the named partition so the test controls timing.

    Returns (evicting_started, allow_close): set allow_close to let the victim
    finish closing; await evicting_started to know T1 is inside the vulnerable window.
    """
    coordinator = gate._coordinator  # type: ignore[attr-defined]
    entry = coordinator._partitions[key]
    evicting_started = asyncio.Event()
    allow_close = asyncio.Event()
    original = entry.bulkhead.close_and_wait

    async def controlled_close() -> None:
        evicting_started.set()
        await allow_close.wait()
        await original()

    entry.bulkhead.close_and_wait = controlled_close  # type: ignore[method-assign]
    return evicting_started, allow_close


async def test_partition_eviction_reserves_capacity_for_replacement() -> None:
    """T1 that initiates eviction must succeed in creating its replacement.

    Without the fix, T2 can steal the freed slot while T1 is closing the victim,
    causing T1 to receive a spurious PartitionLimitError.
    """
    gate = PartitionedBulkhead(label="race", parallelism=1, max_partitions=1)

    # Create idle partition A so it becomes the eviction victim.
    await gate.execute("A", asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, "A")

    # T1 requests key B; it will evict A and pause inside close_and_wait.
    t2_release = asyncio.Event()

    async def hold_c() -> None:
        await t2_release.wait()

    t1 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # T1 has removed A from the map and is mid-close.  T2 races for key C.
    # Buggy code: T2 observes len(partitions)==0 < max==1 and creates C.
    # Fixed code:  T2 observes the logical limit is still at max and fails.
    t2 = asyncio.create_task(gate.execute("C", hold_c))

    # Give T2 time to run and either acquire or fail.
    await asyncio.sleep(0)

    # Resume the eviction so T1 can create its replacement.
    allow_close.set()

    # Wait for T1 to settle.
    await asyncio.sleep(0)

    # Release T2's hold in case it succeeded (buggy path).
    t2_release.set()

    results = await asyncio.wait_for(asyncio.gather(t1, t2, return_exceptions=True), timeout=5.0)

    # T1 must never receive a spurious PartitionLimitError.
    assert not isinstance(results[0], PartitionLimitError), (
        "T1 got spurious PartitionLimitError — eviction race is present"
    )

    status = await gate.status()
    assert status.leased_operations == 0
    assert status.partition_count <= status.max_partitions

    await gate.close_and_wait()


async def test_eviction_reservation_released_on_cancellation() -> None:
    """A task cancelled while closing the victim must release its slot reservation."""
    gate = PartitionedBulkhead(label="cancel-race", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, "A")

    t1 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # Cancel T1 while it is inside complete_cleanup(close_and_wait(victim)).
    t1.cancel()
    allow_close.set()

    with pytest.raises(asyncio.CancelledError):
        await t1

    # Reservation must have been released: another task should now be able
    # to acquire with no PartitionLimitError.
    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._reserved_slots == 0

    # A fresh request should succeed (there is now a free slot).
    await gate.execute("C", asyncio.sleep, 0)

    status = await gate.status()
    assert status.leased_operations == 0
    await gate.close_and_wait()


async def test_eviction_reservation_released_on_manager_close() -> None:
    """If the manager closes while a replacement is pending, the reservation is released."""
    gate = PartitionedBulkhead(label="close-race", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, "A")

    t1 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # Close the manager while T1 is mid-eviction.
    await gate.close()

    # Let the victim finish closing so T1 can re-enter the loop.
    allow_close.set()

    with pytest.raises(BulkheadClosedError):
        await t1

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._reserved_slots == 0

    await gate.wait_closed()


async def test_eviction_reservation_released_on_victim_close_failure() -> None:
    """If the victim's close raises, the reservation must not leak."""
    gate = PartitionedBulkhead(label="fail-close", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    entry_a = coordinator._partitions["A"]

    boom = RuntimeError("simulated close failure")

    async def failing_close() -> None:
        raise boom

    entry_a.bulkhead.close_and_wait = failing_close  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="simulated close failure"):
        await gate.execute("B", asyncio.sleep, 0)

    assert coordinator._reserved_slots == 0

    # Status must still be coherent.
    status = await gate.status()
    assert status.partition_count <= status.max_partitions


async def test_concurrent_requests_for_same_new_key_create_one_partition() -> None:
    """Two tasks racing for the same new key must not create two partitions."""
    gate = PartitionedBulkhead(label="same-key", parallelism=2, max_partitions=1)

    # Fill capacity with an idle partition.
    await gate.execute("existing", asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, "existing")

    # Both tasks want the same new key B.
    t1 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # T2 also wants B; it should either fail or reuse the partition T1 creates.
    t2 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    await asyncio.sleep(0)

    allow_close.set()

    results = await asyncio.wait_for(asyncio.gather(t1, t2, return_exceptions=True), timeout=5.0)

    # Neither task may raise an unexpected exception type.
    for result in results:
        assert result is None or isinstance(result, PartitionLimitError), repr(result)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._reserved_slots == 0

    status = await gate.status()
    assert status.partition_count <= status.max_partitions
    assert status.leased_operations == 0
    await gate.close_and_wait()


async def test_concurrent_different_keys_single_evictable_slot() -> None:
    """Two tasks evicting from the same idle slot cannot both succeed simultaneously."""
    gate = PartitionedBulkhead(label="two-tasks-one-slot", parallelism=1, max_partitions=1)
    await gate.execute("victim", asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, "victim")

    t1 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # T2 wants a different key C but the only evictable slot is taken by T1's reservation.
    t2 = asyncio.create_task(gate.execute("C", asyncio.sleep, 0))
    await asyncio.sleep(0)

    allow_close.set()

    results = await asyncio.wait_for(asyncio.gather(t1, t2, return_exceptions=True), timeout=5.0)

    # T1 must succeed (it initiated the eviction).
    assert results[0] is None, f"T1 failed: {results[0]}"

    # T2 must either succeed (if capacity opened later) or fail cleanly.
    assert results[1] is None or isinstance(results[1], PartitionLimitError)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._reserved_slots == 0

    status = await gate.status()
    assert status.partition_count <= status.max_partitions
    assert status.leased_operations == 0
    await gate.close_and_wait()


async def test_no_reservation_remains_after_successful_eviction() -> None:
    """After a complete eviction cycle, _reserved_slots must return to zero."""
    gate = PartitionedBulkhead(label="no-leak", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._reserved_slots == 0

    await gate.execute("B", asyncio.sleep, 0)

    assert coordinator._reserved_slots == 0
    status = await gate.status()
    assert status.leased_operations == 0
    await gate.close_and_wait()


async def test_partition_count_never_exceeds_max_during_concurrent_eviction() -> None:
    """partition_count must not exceed max_partitions at any observable snapshot."""
    max_p = 2
    gate = PartitionedBulkhead(label="count-invariant", parallelism=1, max_partitions=max_p)

    # Fill to capacity with idle partitions.
    await gate.execute("X", asyncio.sleep, 0)
    await gate.execute("Y", asyncio.sleep, 0)

    snapshots: list[int] = []

    async def snapshot_loop() -> None:
        for _ in range(30):
            s = await gate.status()
            snapshots.append(s.partition_count)
            await asyncio.sleep(0)

    evicting_x, allow_x = _pause_victim_close(gate, "X")
    evicting_y, allow_y = _pause_victim_close(gate, "Y")

    t1 = asyncio.create_task(gate.execute("A", asyncio.sleep, 0))
    t2 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    monitor = asyncio.create_task(snapshot_loop())

    await asyncio.wait_for(evicting_x.wait(), timeout=1.0)
    await asyncio.sleep(0)

    allow_x.set()
    allow_y.set()

    await asyncio.wait_for(asyncio.gather(t1, t2, monitor, return_exceptions=True), timeout=5.0)

    for count in snapshots:
        assert count <= max_p, f"partition_count={count} exceeded max_partitions={max_p}"

    status = await gate.status()
    assert status.leased_operations == 0
    await gate.close_and_wait()


async def test_eviction_counters_remain_coherent() -> None:
    """evicted_total, created_total, and partition_count must agree after evictions."""
    gate = PartitionedBulkhead(label="counters", parallelism=1, max_partitions=1)

    await gate.execute("A", asyncio.sleep, 0)
    await gate.execute("B", asyncio.sleep, 0)  # evicts A
    await gate.execute("C", asyncio.sleep, 0)  # evicts B

    status = await gate.status()
    assert status.created_total == 3
    assert status.evicted_total == 2
    assert status.partition_count == 1
    assert status.leased_operations == 0

    await gate.close_and_wait()
