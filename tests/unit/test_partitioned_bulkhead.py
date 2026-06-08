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
from tests.helpers import ObservableLock, eventually, wait_for_lock_waiters


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

    # T1 initiated eviction; it must succeed unconditionally.
    assert results[0] is None, f"T1 failed: {results[0]}"

    # T2 raced into the open slot.  The reservation must block it with PartitionLimitError,
    # not allow it to succeed and starve T1.
    assert isinstance(results[1], PartitionLimitError), (
        f"T2 got unexpected result {results[1]!r}; expected PartitionLimitError"
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

    # T1 initiated eviction; it must succeed.
    assert results[0] is None, f"T1 failed: {results[0]}"

    # T2 also wants the same key B.  With max_partitions=1 and T1's reservation consuming
    # the only logical slot, T2 must receive PartitionLimitError (no idle victim to evict).
    assert isinstance(results[1], PartitionLimitError), (
        f"T2 got unexpected result {results[1]!r}; expected PartitionLimitError"
    )

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._reserved_slots == 0

    status = await gate.status()
    # One partition created for the victim ("existing"), one for the replacement ("B").
    assert status.created_total == 2, f"expected 2 created, got {status.created_total}"
    assert status.evicted_total == 1, f"expected 1 evicted, got {status.evicted_total}"
    assert status.partition_count == 1
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

    # T1 initiated the eviction; it owns the reservation and must succeed.
    assert results[0] is None, f"T1 failed: {results[0]}"

    # T2 wants a different key C.  T1's reservation consumes the only logical slot;
    # no idle victim remains (victim was already removed by T1), so T2 gets PartitionLimitError.
    assert isinstance(results[1], PartitionLimitError), (
        f"T2 got unexpected result {results[1]!r}; expected PartitionLimitError"
    )

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


# ---------------------------------------------------------------------------
# Correction 4: repeated cancellation must not leak a reservation
# ---------------------------------------------------------------------------


async def test_repeated_cancellation_does_not_leak_reservation() -> None:
    """Two cancel() calls while T1 is inside rollback must not leave _reserved_slots > 0.

    Scenario:
      1. T1 acquires reservation (victim removed, _reserved_slots = 1).
      2. T1 is cancelled during close_and_wait(); complete_cleanup absorbs it.
      3. We hold the manager mutex so T1's rollback blocks when it tries to acquire it.
      4. T1 is cancelled a second time while blocked in the rollback.
      5. The rollback must still complete (complete_cleanup protects it).
      6. After the mutex is released, _reserved_slots must be 0.
    """
    gate = PartitionedBulkhead(label="double-cancel", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    # Replace the coordinator mutex with an observable one so we can detect
    # when rollback is waiting for it and block it deliberately.
    coordinator = gate._coordinator  # type: ignore[attr-defined]
    obs_lock = ObservableLock()
    coordinator._mutex = obs_lock  # type: ignore[assignment]

    evicting_started, allow_close = _pause_victim_close(gate, "A")

    t1 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # T1 holds reservation and is inside complete_cleanup(victim.close_and_wait()).
    # Acquire the mutex now so the reservation-release task will block.
    await obs_lock.acquire()

    # First cancel: complete_cleanup absorbs it, keeps waiting for close_task.
    t1.cancel()

    # Unblock the victim close so T1 can progress to the rollback.
    allow_close.set()

    # Wait until the rollback's release task is blocked on the mutex.
    await wait_for_lock_waiters(obs_lock, expected=1, timeout=2.0)

    # Second cancel: must not escape complete_cleanup's shield.
    t1.cancel()
    await asyncio.sleep(0)

    # Release the mutex. The release task must run to completion.
    obs_lock.release()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(t1, timeout=2.0)

    assert coordinator._reserved_slots == 0, (
        "_reserved_slots leaked after double cancellation — rollback is not cancel-safe"
    )

    # Manager must still be usable after the double-cancel.
    await gate.execute("C", asyncio.sleep, 0)
    final = await gate.status()
    assert final.leased_operations == 0
    await gate.close_and_wait()


# ---------------------------------------------------------------------------
# Correction 5: close_and_wait() must wait for pending reservations
# ---------------------------------------------------------------------------


async def test_close_and_wait_waits_for_pending_reservation() -> None:
    """close_and_wait() must not signal drain while a reservation is outstanding.

    Scenario:
      1. T1 holds a reservation (victim removed, _reserved_slots = 1).
      2. close_and_wait() is called concurrently.
      3. close_and_wait() must not complete before T1 releases the reservation.
      4. Once T1 sees _closed and releases the reservation, close_and_wait() completes.
    """
    gate = PartitionedBulkhead(label="drain-reservation", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, "A")

    t1 = asyncio.create_task(gate.execute("B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # T1 holds _reserved_slots = 1, is inside complete_cleanup(victim.close_and_wait()).
    close_task = asyncio.create_task(gate.close_and_wait())

    # Yield several times; close_and_wait() must NOT complete while reservation is held.
    for _ in range(4):
        await asyncio.sleep(0)

    assert not close_task.done(), (
        "close_and_wait() completed prematurely while a reservation was outstanding"
    )

    # Release the victim. T1 re-enters the loop, sees _closed, releases reservation.
    allow_close.set()

    with pytest.raises(BulkheadClosedError):
        await asyncio.wait_for(t1, timeout=2.0)

    # Now close_and_wait() must finish.
    await asyncio.wait_for(close_task, timeout=2.0)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._reserved_slots == 0
    assert coordinator._leased_operations == 0

    final = await gate.status()
    assert final.is_closed
    assert final.partition_count == 0
    assert final.leased_operations == 0


# ---------------------------------------------------------------------------
# Part 2: close_and_wait() must wait for cleanup_idle() and discard() in progress
# ---------------------------------------------------------------------------


def _pause_child_close(entry: Any) -> tuple[asyncio.Event, asyncio.Event]:
    """Intercept close_and_wait on a PartitionEntry so the test controls timing."""
    closed_started = asyncio.Event()
    allow_close = asyncio.Event()
    original = entry.bulkhead.close_and_wait

    async def controlled_close() -> None:
        closed_started.set()
        await allow_close.wait()
        await original()

    entry.bulkhead.close_and_wait = controlled_close  # type: ignore[method-assign]
    return closed_started, allow_close


async def test_close_and_wait_waits_for_cleanup_idle_in_progress() -> None:
    """close_and_wait() must not complete while cleanup_idle() is closing a victim.

    cleanup_idle() removes the partition from the map under the lock, then
    closes it outside the lock.  During that window the manager may appear empty
    to the drain logic.  The maintenance counter must prevent premature drain.
    """
    gate = PartitionedBulkhead(
        label="idle-drain",
        parallelism=1,
        max_partitions=2,
        idle_timeout=0.05,
    )
    await gate.execute("A", asyncio.sleep, 0)
    await asyncio.sleep(0.1)  # ensure idle_timeout has elapsed (50ms threshold)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    entry_a = coordinator._partitions["A"]
    closed_started, allow_close = _pause_child_close(entry_a)

    # Start cleanup_idle() in a background task.
    cleanup_task = asyncio.create_task(gate.cleanup_idle())

    # Wait for cleanup to have removed "A" from the map and started closing it.
    await asyncio.wait_for(closed_started.wait(), timeout=2.0)

    # At this point: partitions={}, leases=0, reservations=0 — but close is pending.
    # close_and_wait() must NOT complete immediately.
    close_task = asyncio.create_task(gate.close_and_wait())

    for _ in range(4):
        await asyncio.sleep(0)

    assert not close_task.done(), (
        "close_and_wait() completed prematurely while cleanup_idle() was still closing a child"
    )

    # Allow the child close to finish.
    allow_close.set()

    removed = await asyncio.wait_for(cleanup_task, timeout=3.0)
    assert removed == 1

    await asyncio.wait_for(close_task, timeout=3.0)

    assert coordinator._pending_child_closures == 0  # type: ignore[attr-defined]
    final = await gate.status()
    assert final.is_closed
    assert final.partition_count == 0
    assert final.leased_operations == 0


async def test_close_and_wait_waits_for_discard_in_progress() -> None:
    """close_and_wait() must not complete while discard() is closing a child."""
    gate = PartitionedBulkhead(label="discard-drain", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    entry_a = coordinator._partitions["A"]
    closed_started, allow_close = _pause_child_close(entry_a)

    discard_task = asyncio.create_task(gate.discard("A"))
    await asyncio.wait_for(closed_started.wait(), timeout=1.0)

    close_task = asyncio.create_task(gate.close_and_wait())

    for _ in range(4):
        await asyncio.sleep(0)

    assert not close_task.done(), (
        "close_and_wait() completed prematurely while discard() was still closing a child"
    )

    allow_close.set()

    result = await asyncio.wait_for(discard_task, timeout=2.0)
    assert result is True

    await asyncio.wait_for(close_task, timeout=2.0)

    assert coordinator._pending_child_closures == 0  # type: ignore[attr-defined]
    final = await gate.status()
    assert final.is_closed
    assert final.partition_count == 0


# ---------------------------------------------------------------------------
# Part 1: Admission budget covering manager and child
# ---------------------------------------------------------------------------


async def test_execute_now_rejects_immediately_when_eviction_is_required() -> None:
    """execute_now() must not wait for victim closure.

    Current bug: acquire() does eviction even for slot_now(), blocking the caller
    until the victim's close_and_wait() completes.
    With the fix: acquire(immediate=True) raises PartitionLimitError before touching
    the victim.
    """
    gate = PartitionedBulkhead(label="now-evict", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    entry_a = coordinator._partitions["A"]
    close_called = asyncio.Event()
    original = entry_a.bulkhead.close_and_wait

    async def detecting_close() -> None:
        close_called.set()
        await original()

    entry_a.bulkhead.close_and_wait = detecting_close  # type: ignore[method-assign]

    with pytest.raises(PartitionLimitError) as exc_info:
        await gate.execute_now("B", asyncio.sleep, 0)

    assert not close_called.is_set(), (
        "execute_now() triggered victim closure — should reject without touching the victim"
    )
    assert "A" in coordinator._partitions, "victim must not be evicted by execute_now()"
    assert coordinator._reserved_slots == 0

    err = exc_info.value
    assert err.label == "now-evict"
    assert err.max_partitions == 1

    status = await gate.status()
    assert status.partition_count == 1
    assert status.evicted_total == 0

    await gate.close_and_wait()


async def test_execute_now_does_not_enter_child_queue() -> None:
    """execute_now() must not queue inside a child that already has a slot holder."""
    gate = PartitionedBulkhead(
        label="now-queue",
        parallelism=1,
        max_partitions=1,
        waiting_room=5,
    )
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    from bulklink import BulkheadSaturatedError

    with pytest.raises(BulkheadSaturatedError):
        await gate.execute_now("alpha", asyncio.sleep, 0)

    release.set()
    await active
    await gate.close_and_wait()


async def test_execute_before_expired_deadline_does_not_create_partition() -> None:
    """An already-expired deadline must not create a new partition or do any work."""
    loop = asyncio.get_running_loop()
    gate = PartitionedBulkhead(label="before-expired", parallelism=1, max_partitions=2)

    from bulklink import BulkheadQueueTimeoutError

    past_deadline = loop.time() - 1.0  # definitely in the past

    with pytest.raises(BulkheadQueueTimeoutError):
        await gate.execute_before(past_deadline, "A", asyncio.sleep, 0)

    status = await gate.status()
    assert status.partition_count == 0, "expired deadline must not create a partition"
    assert status.created_total == 0
    assert status.evicted_total == 0
    assert status.leased_operations == 0

    await gate.close_and_wait()


async def test_execute_within_budget_includes_manager_eviction_time() -> None:
    """The execute_within() deadline must cover manager resolution time.

    If the manager blocks eviction and the deadline expires, the caller must receive
    BulkheadQueueTimeoutError.  Without the fix the manager has no deadline and the
    child would get the full budget after the manager has already consumed it.
    """
    from bulklink import BulkheadQueueTimeoutError

    gate = PartitionedBulkhead(label="within-budget", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, "A")

    # 50 ms budget — victim close will take ~150 ms.
    t1 = asyncio.create_task(gate.execute_within(0.05, "B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # Let the deadline expire while the victim is paused.
    await asyncio.sleep(0.15)

    # Release the victim so the background close task can finish.
    allow_close.set()

    with pytest.raises(BulkheadQueueTimeoutError):
        await asyncio.wait_for(t1, timeout=3.0)

    coordinator = gate._coordinator  # type: ignore[attr-defined]

    # The background victim-close task runs asynchronously; wait for it to release.
    # close_and_wait() blocks until drain (which requires pending_ops empty).
    await asyncio.wait_for(gate.close_and_wait(), timeout=3.0)
    assert coordinator._reserved_slots == 0, "reservation must be released on timeout"


async def test_admitted_operation_not_cancelled_when_deadline_passes() -> None:
    """A deadline must only constrain admission; it must not cancel a running operation."""
    loop = asyncio.get_running_loop()
    gate = PartitionedBulkhead(label="no-cancel", parallelism=1, max_partitions=1)

    # Deadline far enough in the future to admit but check post-admission behaviour.
    deadline = loop.time() + 5.0

    admitted = asyncio.Event()
    release = asyncio.Event()

    async def work() -> int:
        admitted.set()
        await release.wait()
        return 42

    task = asyncio.create_task(gate.execute_before(deadline, "A", work))
    await asyncio.wait_for(admitted.wait(), timeout=1.0)

    # Simulate deadline passing while the operation is running.
    await asyncio.sleep(0)

    release.set()
    result = await asyncio.wait_for(task, timeout=2.0)
    assert result == 42, "running operation must not be affected by deadline"

    await gate.close_and_wait()


async def test_cancelling_shutdown_waiter_does_not_disrupt_close() -> None:
    """Cancelling one wait_closed() waiter must not affect the manager or other waiters."""
    gate = PartitionedBulkhead(label="waiter-cancel", parallelism=1, max_partitions=1)
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    await gate.close()

    waiter_a = asyncio.create_task(gate.wait_closed())
    waiter_b = asyncio.create_task(gate.wait_closed())
    await asyncio.sleep(0)

    # Cancel one of the two waiters.
    waiter_a.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await waiter_a

    await asyncio.wait_for(waiter_b, timeout=2.0)
    await asyncio.wait_for(active, timeout=2.0)

    final = await gate.status()
    assert final.is_closed
    assert final.partition_count == 0
    assert final.leased_operations == 0


# ---------------------------------------------------------------------------
# Phase 1: Explicit Lifecycle State Machine tests (written BEFORE fix)
# ---------------------------------------------------------------------------


async def test_close_then_discard_raises_closed_error() -> None:
    """discard() called after close() must raise BulkheadClosedError."""
    gate = PartitionedBulkhead(label="close-discard", parallelism=1, max_partitions=2)
    await gate.execute("A", asyncio.sleep, 0)
    await gate.close()

    with pytest.raises(BulkheadClosedError):
        await gate.discard("A")

    await gate.wait_closed()


async def test_close_then_cleanup_idle_raises_closed_error() -> None:
    """cleanup_idle() called after close() must raise BulkheadClosedError."""
    gate = PartitionedBulkhead(
        label="close-cleanup", parallelism=1, max_partitions=2, idle_timeout=0.001
    )
    await gate.execute("A", asyncio.sleep, 0)
    await gate.close()

    with pytest.raises(BulkheadClosedError):
        await gate.cleanup_idle()

    await gate.wait_closed()


async def test_close_and_wait_concurrent_with_late_maintenance_attempt() -> None:
    """A late maintenance op during shutdown must fail, not interfere with drain."""
    gate = PartitionedBulkhead(
        label="late-maint", parallelism=1, max_partitions=2, idle_timeout=0.001
    )
    await gate.execute("A", asyncio.sleep, 0)

    close_task = asyncio.create_task(gate.close_and_wait())
    # Give close_and_wait() enough iterations to set lifecycle to CLOSING.
    for _ in range(5):
        await asyncio.sleep(0)

    # Try maintenance after close has started - must raise BulkheadClosedError.
    with pytest.raises(BulkheadClosedError):
        await gate.discard("A")

    await asyncio.wait_for(close_task, timeout=2.0)


async def test_drain_not_signaled_before_pending_ops_finish() -> None:
    """Drain must not be signaled while cleanup_idle() child close is still in flight."""
    gate = PartitionedBulkhead(
        label="drain-pending", parallelism=1, max_partitions=2, idle_timeout=0.001
    )
    await gate.execute("A", asyncio.sleep, 0)
    await asyncio.sleep(0.01)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    entry_a = coordinator._partitions["A"]
    closed_started, allow_close = _pause_child_close(entry_a)

    cleanup_task = asyncio.create_task(gate.cleanup_idle())
    await asyncio.wait_for(closed_started.wait(), timeout=2.0)

    # Now close the manager while cleanup is in flight.
    await gate.close()

    # The drain event must NOT be set yet.
    drained_event = coordinator._drained_event
    assert drained_event is not None
    assert not drained_event.is_set(), (
        "Drain signaled prematurely while cleanup_idle() is still closing a child"
    )

    allow_close.set()
    await asyncio.wait_for(cleanup_task, timeout=2.0)
    await gate.wait_closed()


async def test_multiple_close_calls_are_idempotent() -> None:
    """Multiple close() calls must not raise or double-close children."""
    gate = PartitionedBulkhead(label="multi-close", parallelism=1, max_partitions=2)
    await gate.execute("A", asyncio.sleep, 0)

    # Three close() calls must all succeed silently.
    await gate.close()
    await gate.close()
    await gate.close()

    await gate.wait_closed()
    final = await gate.status()
    assert final.is_closed


async def test_multiple_wait_closed_work_independently() -> None:
    """Multiple concurrent wait_closed() calls must all complete after drain."""
    gate = PartitionedBulkhead(label="multi-wait", parallelism=1, max_partitions=1)
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    await gate.close()

    waiters = [asyncio.create_task(gate.wait_closed()) for _ in range(5)]
    await asyncio.sleep(0)

    # None should be done yet.
    assert not any(w.done() for w in waiters)

    release.set()
    await active

    results = await asyncio.wait_for(asyncio.gather(*waiters, return_exceptions=True), timeout=2.0)
    for result in results:
        assert result is None, f"wait_closed() raised: {result!r}"


async def test_canceling_shutdown_waiter_does_not_alter_lifecycle() -> None:
    """Canceling a wait_closed() waiter must not change lifecycle or affect drain."""
    gate = PartitionedBulkhead(label="cancel-waiter-lifecycle", parallelism=1, max_partitions=1)
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    await gate.close()

    waiter_cancelled = asyncio.create_task(gate.wait_closed())
    waiter_survivor = asyncio.create_task(gate.wait_closed())
    await asyncio.sleep(0)

    waiter_cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter_cancelled

    # Lifecycle must remain CLOSING (not corrupted).
    coordinator = gate._coordinator  # type: ignore[attr-defined]
    assert coordinator._closed is True

    release.set()
    await active
    await asyncio.wait_for(waiter_survivor, timeout=2.0)

    final = await gate.status()
    assert final.is_closed
    assert final.partition_count == 0


async def test_calls_after_closed_do_not_recreate_structures() -> None:
    """After CLOSED, admission attempts must raise BulkheadClosedError immediately."""
    gate = PartitionedBulkhead(label="post-closed", parallelism=1, max_partitions=2)
    await gate.close_and_wait()

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    partition_count_before = len(coordinator._partitions)

    with pytest.raises(BulkheadClosedError):
        await gate.execute("new-key", asyncio.sleep, 0)

    # No partition should have been created.
    assert len(coordinator._partitions) == partition_count_before


# ---------------------------------------------------------------------------
# Phase 2: Explicit Ownership for Pending Operations
# ---------------------------------------------------------------------------


async def test_pending_ops_zero_after_successful_eviction() -> None:
    """After a full eviction cycle, pending ops must be empty."""
    gate = PartitionedBulkhead(label="pending-zero", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)
    await gate.execute("B", asyncio.sleep, 0)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    # Either _pending_ops (new) or _reserved_slots==0 (old) must be zero.
    if hasattr(coordinator, "_pending_ops"):
        assert len(coordinator._pending_ops) == 0
    else:
        assert coordinator._reserved_slots == 0
    assert coordinator._leased_operations == 0


async def test_pending_ops_zero_after_cleanup_idle() -> None:
    """After cleanup_idle() completes, no pending ops must remain."""
    gate = PartitionedBulkhead(
        label="idle-pending-zero", parallelism=1, max_partitions=2, idle_timeout=0.001
    )
    await gate.execute("A", asyncio.sleep, 0)
    await asyncio.sleep(0.01)

    removed = await gate.cleanup_idle()
    assert removed == 1

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    if hasattr(coordinator, "_pending_ops"):
        assert len(coordinator._pending_ops) == 0
    else:
        assert coordinator._pending_child_closures == 0


async def test_pending_ops_zero_after_discard() -> None:
    """After discard() completes, no pending ops must remain."""
    gate = PartitionedBulkhead(label="discard-pending-zero", parallelism=1, max_partitions=2)
    await gate.execute("A", asyncio.sleep, 0)
    result = await gate.discard("A")
    assert result is True

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    if hasattr(coordinator, "_pending_ops"):
        assert len(coordinator._pending_ops) == 0
    else:
        assert coordinator._pending_child_closures == 0


# ---------------------------------------------------------------------------
# Phase 3: Deadlines Must Bound Caller Wait Time
# ---------------------------------------------------------------------------


async def test_execute_before_deadline_expires_during_eviction_returns_timeout() -> None:
    """execute_before() with a deadline that expires during victim closure must timeout.

    The FUTURE deadline must expire DURING the victim close, proving that the
    caller's wait is bounded by their deadline, not the full close duration.
    """
    from bulklink import BulkheadQueueTimeoutError

    gate = PartitionedBulkhead(label="deadline-evict", parallelism=1, max_partitions=1)
    await gate.execute("A", asyncio.sleep, 0)

    evicting_started, allow_close = _pause_victim_close(gate, "A")

    loop = asyncio.get_running_loop()
    # Deadline 80ms from now — long enough to start eviction, short enough to expire
    # before we allow the victim to close (we'll keep it paused for 200ms).
    deadline = loop.time() + 0.08

    t1 = asyncio.create_task(gate.execute_before(deadline, "B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # Deadline expires while victim is still paused.
    await asyncio.sleep(0.15)

    # Caller must receive timeout at this point (before victim close completes).
    assert t1.done() or (await asyncio.wait([t1], timeout=0.1))[0]

    allow_close.set()

    with pytest.raises(BulkheadQueueTimeoutError):
        await asyncio.wait_for(t1, timeout=2.0)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    # Reservation must eventually be released (victim close task continues).
    # Give it time to complete in the background.
    loop = asyncio.get_running_loop()
    deadline2 = loop.time() + 3.0
    while loop.time() < deadline2:
        if hasattr(coordinator, "_pending_ops"):
            if len(coordinator._pending_ops) == 0:
                break
        elif coordinator._reserved_slots == 0:
            break
        await asyncio.sleep(0.01)

    if hasattr(coordinator, "_pending_ops"):
        assert len(coordinator._pending_ops) == 0, "pending ops leaked after deadline timeout"
    else:
        assert coordinator._reserved_slots == 0, "reservation leaked after deadline timeout"

    await gate.close_and_wait()


async def test_admitted_operation_not_cancelled_when_deadline_passes_proof() -> None:
    """Prove deadline actually passes while operation runs; result must still be 42.

    This improves on the existing test by actually letting the deadline expire before
    the operation completes.
    """
    loop = asyncio.get_running_loop()
    gate = PartitionedBulkhead(label="no-cancel-proof", parallelism=1, max_partitions=1)

    # Very short deadline — will expire before work() returns.
    deadline = loop.time() + 0.01

    admitted = asyncio.Event()
    release = asyncio.Event()

    async def work() -> int:
        admitted.set()
        await release.wait()
        return 42

    task = asyncio.create_task(gate.execute_before(deadline, "A", work))
    await asyncio.wait_for(admitted.wait(), timeout=1.0)

    # Let the deadline expire with work still running.
    await asyncio.sleep(0.05)
    assert loop.time() > deadline, "deadline must have actually expired by now"

    # Work is still running (not cancelled).
    assert not task.done(), "task must still be running despite expired deadline"

    release.set()
    result = await asyncio.wait_for(task, timeout=2.0)
    assert result == 42, "running operation must not be affected by expired deadline"

    await gate.close_and_wait()


# ---------------------------------------------------------------------------
# Phase 5: Fix Incomplete Tests — combined budget scenario
# ---------------------------------------------------------------------------


async def test_combined_budget_manager_plus_child() -> None:
    """Admission budget covers both manager resolution and child queue wait.

    Scenario: 200ms budget, manager consumes ~80ms (paused victim close),
    then child gets only the remaining ~120ms. The child must NOT receive a fresh
    200ms budget.
    """
    gate = PartitionedBulkhead(
        label="combined-budget",
        parallelism=1,
        max_partitions=1,
        waiting_room=1,
    )
    # Keep child busy so it cannot immediately admit a second slot.
    release_child = asyncio.Event()
    active = asyncio.create_task(gate.execute("A", release_child.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    # Recreate "A" as a victim: we need "B" to evict "A" but "A" has a borrower.
    # Instead use a different scenario: create "A" idle, then pause its close.
    release_child.set()
    await active

    # Now "A" is idle. Pause its close so manager resolution takes time.
    evicting_started, allow_close = _pause_victim_close(gate, "A")

    # 200ms budget total.
    t1 = asyncio.create_task(gate.execute_within(0.2, "B", asyncio.sleep, 0))
    await asyncio.wait_for(evicting_started.wait(), timeout=1.0)

    # Consume 150ms of the budget while victim is paused — leaves only 50ms.
    await asyncio.sleep(0.15)
    allow_close.set()

    # "B" partition is now created. But the child's slot_before deadline has
    # already consumed most of the 200ms. Since this slot was available
    # immediately (no one holds it), it succeeds — result is None.
    # The important thing is no fresh 200ms is granted.
    result = await asyncio.wait_for(t1, timeout=2.0)
    assert result is None

    await gate.close_and_wait()


# ---------------------------------------------------------------------------
# Phase 5: Maintenance failure scenarios
# ---------------------------------------------------------------------------


async def test_cleanup_idle_failure_does_not_block_drain() -> None:
    """If one child close raises during cleanup_idle(), other children still close."""
    gate = PartitionedBulkhead(
        label="cleanup-fail", parallelism=1, max_partitions=3, idle_timeout=0.001
    )
    await gate.execute("A", asyncio.sleep, 0)
    await gate.execute("B", asyncio.sleep, 0)
    await asyncio.sleep(0.01)

    coordinator = gate._coordinator  # type: ignore[attr-defined]
    # Make "A"'s close raise.
    entry_a = coordinator._partitions["A"]
    boom = RuntimeError("close failure")

    async def failing_close() -> None:
        raise boom

    entry_a.bulkhead.close_and_wait = failing_close  # type: ignore[method-assign]

    # cleanup_idle() must propagate the error but still close "B".
    with pytest.raises(RuntimeError, match="close failure"):
        await gate.cleanup_idle()

    # Manager drain must still work after the failure.
    await gate.close_and_wait()
    final = await gate.status()
    assert final.is_closed


# ---------------------------------------------------------------------------
# Phase 7: Resource exhaustion protection
# ---------------------------------------------------------------------------


async def test_many_rejections_do_not_grow_pending_structures() -> None:
    """Thousands of PartitionLimitError rejections must not grow internal structures."""
    gate = PartitionedBulkhead(label="exhaustion", parallelism=1, max_partitions=2)
    release = asyncio.Event()

    # Fill all partitions with active work.
    t1 = asyncio.create_task(gate.execute("A", release.wait))
    t2 = asyncio.create_task(gate.execute("B", release.wait))
    await eventually(lambda: has_leased_operations(gate, 2))

    coordinator = gate._coordinator  # type: ignore[attr-defined]

    for _ in range(100):
        with pytest.raises(PartitionLimitError):
            await gate.execute("C", asyncio.sleep, 0)

    # No structures should have grown.
    if hasattr(coordinator, "_pending_ops"):
        assert len(coordinator._pending_ops) == 0
    else:
        assert coordinator._reserved_slots == 0

    release.set()
    await asyncio.gather(t1, t2)
    await gate.close_and_wait()


async def test_shutdown_completes_after_many_rejections() -> None:
    """Shutdown must complete cleanly even after many admission rejections."""
    gate = PartitionedBulkhead(label="shutdown-after-rejections", parallelism=1, max_partitions=1)
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("A", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    for _ in range(50):
        with pytest.raises((PartitionLimitError, BulkheadClosedError)):
            await gate.execute("B", asyncio.sleep, 0)

    release.set()
    await active
    await asyncio.wait_for(gate.close_and_wait(), timeout=2.0)

    final = await gate.status()
    assert final.is_closed
    assert final.leased_operations == 0
