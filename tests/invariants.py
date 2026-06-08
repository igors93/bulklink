from __future__ import annotations

from typing import Any

from bulklink._internal.models import WaitState
from bulklink._internal.partitioned_coordinator import _PartitionLifecycle


async def assert_partitioned_consistent(gate: Any) -> None:
    """Validate all internal invariants of a PartitionedBulkhead under its manager lock."""
    coordinator = gate._coordinator

    async with coordinator._mutex:
        lifecycle = coordinator._lifecycle
        partitions = coordinator._partitions
        leased = coordinator._leased_operations
        pending_ops = coordinator._pending_ops
        counters = coordinator._counters
        drained_event = coordinator._drained_event

        # Lifecycle must be a valid enum value.
        assert isinstance(lifecycle, _PartitionLifecycle), f"invalid lifecycle: {lifecycle!r}"

        # Logical capacity must never exceed max_partitions.
        materialized = len(partitions)
        reserved = sum(op.owns_capacity for op in pending_ops.values())
        logical = materialized + reserved
        assert logical <= coordinator._max_partitions, (
            f"logical capacity {logical} exceeds max_partitions {coordinator._max_partitions}: "
            f"materialized={materialized}, reserved={reserved}"
        )

        # Leased operations must be non-negative.
        assert leased >= 0, f"negative leased_operations: {leased}"

        # leased_operations must equal sum of all borrower counts.
        total_borrowers = sum(e.borrowers for e in partitions.values())
        assert leased == total_borrowers, (
            f"leased_operations={leased} != sum(borrowers)={total_borrowers}"
        )

        # Every borrower count must be non-negative.
        for entry in partitions.values():
            assert entry.borrowers >= 0, f"negative borrowers: {entry.borrowers}"

        # Pending ops dict must be keyed by id(op).
        for op_id, op in pending_ops.items():
            assert op_id == id(op), f"pending op id mismatch: key={op_id}, id(op)={id(op)}"

        # Peak counters must be coherent.
        assert counters.peak_partitions >= materialized, (
            f"peak_partitions={counters.peak_partitions} < current={materialized}"
        )
        assert counters.peak_leased_operations >= leased, (
            f"peak_leased_operations={counters.peak_leased_operations} < current={leased}"
        )

        # Drain signal coherence.
        if drained_event is not None:
            expected_drained = (
                lifecycle is not _PartitionLifecycle.OPEN and leased == 0 and not pending_ops
            )
            if expected_drained:
                assert drained_event.is_set(), (
                    "drain event not set but all drain conditions are met"
                )
            # Note: drain event may be set even if conditions are not met
            # (set once, never cleared) — we only assert the forward direction.

        # CLOSED lifecycle implies no remaining work (may not hold immediately
        # during transition, but we verify once stable).
        if lifecycle is _PartitionLifecycle.CLOSED:
            assert leased == 0, f"CLOSED with active leases: {leased}"

        # All cumulative counters must be non-negative.
        assert counters.created_total >= 0
        assert counters.evicted_total >= 0
        assert counters.discarded_total >= 0
        assert counters.limit_rejected_total >= 0

        # created = still present + evicted + discarded (partitions cleared at end).
        # This only holds after close, but we can check a weaker bound:
        # evicted + discarded <= created (we never un-create).
        assert counters.evicted_total + counters.discarded_total <= counters.created_total, (
            f"evicted={counters.evicted_total} + discarded={counters.discarded_total} "
            f"> created={counters.created_total}"
        )


async def assert_bulkhead_consistent(bulkhead: Any) -> None:
    """Assert capacity, queue state, and counters agree exactly."""
    coordinator = bulkhead._coordinator

    async with coordinator._mutex:
        counters = coordinator._counters
        waiters = tuple(coordinator._waiters)
        in_flight = coordinator._in_flight

        assert len(coordinator._instance_id) == 32
        assert coordinator._snapshot_index >= 0
        assert in_flight >= 0
        assert coordinator.parallelism > 0
        assert 0 <= len(waiters) <= coordinator.waiting_room
        assert len({id(entry) for entry in waiters}) == len(waiters)

        for entry in waiters:
            assert entry.state is WaitState.WAITING
            assert entry.waited_seconds is None
            assert not entry.future.done()

        if waiters:
            assert in_flight >= coordinator.parallelism

        numeric_counters = (
            counters.admitted_total,
            counters.admitted_from_queue_total,
            counters.abandoned_after_admission_total,
            counters.queued_total,
            counters.saturated_total,
            counters.expired_total,
            counters.expired_before_queue_total,
            counters.cancelled_while_waiting_total,
            counters.closed_before_queue_total,
            counters.closed_while_waiting_total,
            counters.finished_total,
            counters.peak_in_flight,
            counters.peak_waiting,
        )
        assert all(value >= 0 for value in numeric_counters)

        settled_waiting = (
            counters.admitted_from_queue_total
            + counters.cancelled_while_waiting_total
            + counters.expired_total
            + counters.closed_while_waiting_total
        )
        assert counters.queued_total == settled_waiting + len(waiters)

        accounted_admissions = (
            counters.finished_total + counters.abandoned_after_admission_total + in_flight
        )
        assert counters.admitted_total == accounted_admissions
        assert counters.admitted_from_queue_total <= counters.admitted_total

        assert in_flight <= counters.peak_in_flight
        assert len(waiters) <= counters.peak_waiting <= coordinator.waiting_room
        assert counters.cumulative_wait_seconds >= 0
        assert counters.longest_wait_seconds >= 0

        drain_event = coordinator._drained_event
        if drain_event is not None:
            should_be_drained = coordinator._closed and in_flight == 0
            assert drain_event.is_set() is should_be_drained
            if should_be_drained:
                assert not waiters

        if counters.admitted_from_queue_total == 0:
            assert counters.cumulative_wait_seconds == 0
            assert counters.longest_wait_seconds == 0
        else:
            assert counters.longest_wait_seconds <= counters.cumulative_wait_seconds
