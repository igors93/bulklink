from __future__ import annotations

from typing import Any

from bulklink._internal.models import WaitState


async def assert_bulkhead_consistent(bulkhead: Any) -> None:
    """Assert capacity, queue state, and counters agree exactly."""
    coordinator = bulkhead._coordinator

    async with coordinator._mutex:
        counters = coordinator._counters
        waiters = tuple(coordinator._waiters)
        in_flight = coordinator._in_flight

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
