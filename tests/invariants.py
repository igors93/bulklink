from __future__ import annotations

from typing import Any

from bulklink._internal.models import WaitState


async def assert_bulkhead_consistent(bulkhead: Any) -> None:
    """Assert public counters and private queue state agree."""
    coordinator = bulkhead._coordinator

    async with coordinator._mutex:
        counters = coordinator._counters
        waiters = tuple(coordinator._waiters)
        in_flight = coordinator._in_flight

        assert 0 <= in_flight <= coordinator.parallelism
        assert 0 <= len(waiters) <= coordinator.waiting_room
        assert len({id(entry) for entry in waiters}) == len(waiters)

        for entry in waiters:
            assert entry.state is WaitState.WAITING
            assert not entry.future.done()

        numeric_counters = (
            counters.admitted_total,
            counters.admitted_from_queue_total,
            counters.queued_total,
            counters.saturated_total,
            counters.expired_total,
            counters.cancelled_total,
            counters.closed_total,
            counters.finished_total,
            counters.peak_in_flight,
            counters.peak_waiting,
        )
        assert all(value >= 0 for value in numeric_counters)

        assert counters.admitted_from_queue_total <= counters.admitted_total
        assert counters.admitted_from_queue_total <= counters.queued_total
        assert counters.finished_total <= counters.admitted_total
        assert in_flight <= counters.admitted_total - counters.finished_total
        assert in_flight <= counters.peak_in_flight <= coordinator.parallelism
        assert len(waiters) <= counters.peak_waiting <= coordinator.waiting_room
        assert counters.cumulative_wait_seconds >= 0
        assert counters.longest_wait_seconds >= 0
        assert counters.longest_wait_seconds <= counters.cumulative_wait_seconds or (
            counters.admitted_from_queue_total == 1
            and counters.longest_wait_seconds == counters.cumulative_wait_seconds
        )
