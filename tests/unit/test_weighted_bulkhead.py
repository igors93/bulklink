from __future__ import annotations

import asyncio

import pytest

from bulklink import (
    BulkheadClosedError,
    BulkheadEventKind,
    BulkheadQueueTimeoutError,
    WeightedBulkhead,
    WeightedBulkheadSaturatedError,
)
from tests.helpers import eventually


async def test_weighted_execute_accounts_capacity_units() -> None:
    gate = WeightedBulkhead(label="weighted-basic", capacity=5)

    result = await gate.execute(3, asyncio.sleep, 0, result="ok")
    current = await gate.status()

    assert result == "ok"
    assert current.used == 0
    assert current.in_flight == 0
    assert current.admitted_total == 1
    assert current.admitted_units_total == 3
    assert current.finished_total == 1
    assert current.finished_units_total == 3
    assert current.average_admitted_cost == 3.0


async def test_weighted_fifo_does_not_allow_a_smaller_request_to_overtake() -> None:
    gate = WeightedBulkhead(label="weighted-fifo", capacity=5, waiting_room=2)
    release = asyncio.Event()
    queued_admissions: list[int] = []

    def observe(event: object) -> None:
        if getattr(event, "kind", None) is BulkheadEventKind.ADMITTED and getattr(
            event, "from_queue", False
        ):
            queued_admissions.append(event.cost)

    gate.add_event_handler(observe)  # type: ignore[arg-type]

    async def hold() -> None:
        async with gate.slot(3):
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=0))

    first = asyncio.create_task(gate.execute(3, asyncio.sleep, 0))
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=1))
    second = asyncio.create_task(gate.execute(1, asyncio.sleep, 0))
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=2))

    await asyncio.sleep(0)
    assert not first.done()
    assert not second.done()
    assert queued_admissions == []

    release.set()
    await asyncio.gather(active, first, second)

    assert queued_admissions == [3, 1]
    final = await gate.status()
    assert final.used == 0
    assert final.waiting == 0
    assert final.waiting_units == 0


async def test_weighted_immediate_admission_reports_requested_cost() -> None:
    gate = WeightedBulkhead(label="weighted-now", capacity=3)

    async with gate.slot(2):
        with pytest.raises(WeightedBulkheadSaturatedError) as captured:
            await gate.execute_now(2, asyncio.sleep, 0)

    assert captured.value.cost == 2
    assert captured.value.capacity == 3
    assert captured.value.used == 2


async def test_weighted_waiter_cancellation_releases_queue_accounting() -> None:
    gate = WeightedBulkhead(label="weighted-cancel", capacity=2, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot(2):
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_weighted_state(gate, used=2, waiting=0))
    waiting = asyncio.create_task(gate.execute(1, asyncio.sleep, 0))
    await eventually(lambda: has_weighted_state(gate, used=2, waiting=1))

    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    current = await gate.status()
    assert current.waiting == 0
    assert current.waiting_units == 0
    assert current.cancelled_while_waiting_total == 1

    release.set()
    await active


async def test_weighted_timeout_and_absolute_deadline_preserve_capacity() -> None:
    gate = WeightedBulkhead(
        label="weighted-deadlines",
        capacity=1,
        waiting_room=1,
        wait_limit=0.02,
    )

    async with gate.slot(1):
        with pytest.raises(BulkheadQueueTimeoutError):
            await gate.execute(1, asyncio.sleep, 0)

    loop = asyncio.get_running_loop()
    with pytest.raises(BulkheadQueueTimeoutError):
        await gate.execute_before(loop.time() - 1.0, 1, asyncio.sleep, 0)

    current = await gate.status()
    assert current.used == 0
    assert current.expired_total == 1
    assert current.expired_before_queue_total == 1


async def test_weighted_resize_up_admits_head_and_shrink_preserves_queued_work() -> None:
    gate = WeightedBulkhead(label="weighted-resize", capacity=3, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot(3):
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=0))
    queued = asyncio.create_task(gate.execute(2, asyncio.sleep, 0))
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=1))

    with pytest.raises(ValueError, match="largest queued operation cost"):
        await gate.resize(1)

    await gate.resize(5)
    await eventually(lambda: has_weighted_state(gate, used=5, waiting=0))

    release.set()
    await asyncio.gather(active, queued)
    assert (await gate.status()).used == 0


async def test_weighted_shrink_below_active_usage_drains_without_cancellation() -> None:
    gate = WeightedBulkhead(label="weighted-shrink", capacity=6)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot(5):
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_weighted_state(gate, used=5, waiting=0))
    await gate.resize(3)

    current = await gate.status()
    assert current.capacity == 3
    assert current.used == 5
    assert current.capacity_excess == 2
    assert current.is_over_capacity

    release.set()
    await active
    assert (await gate.status()).used == 0


async def test_weighted_close_rejects_waiters_and_drains_active_work() -> None:
    gate = WeightedBulkhead(label="weighted-close", capacity=2, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot(2):
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_weighted_state(gate, used=2, waiting=0))
    queued = asyncio.create_task(gate.execute(1, asyncio.sleep, 0))
    await eventually(lambda: has_weighted_state(gate, used=2, waiting=1))

    await gate.close()
    with pytest.raises(BulkheadClosedError):
        await queued
    with pytest.raises(BulkheadClosedError):
        await gate.execute(1, asyncio.sleep, 0)

    release.set()
    await active
    await gate.wait_closed()
    assert (await gate.status()).is_drained


@pytest.mark.parametrize("cost", [0, -1, True, 1.5, "1", None])
async def test_weighted_cost_must_be_a_positive_integer(cost: object) -> None:
    gate = WeightedBulkhead(label="weighted-validation", capacity=3)

    with pytest.raises(ValueError, match="cost must be a positive integer"):
        gate.slot(cost)  # type: ignore[arg-type]


async def test_weighted_cost_cannot_exceed_current_capacity() -> None:
    gate = WeightedBulkhead(label="weighted-too-large", capacity=3)

    with pytest.raises(ValueError, match="cannot exceed"):
        await gate.execute(4, asyncio.sleep, 0)


async def has_weighted_state(
    gate: WeightedBulkhead,
    *,
    used: int,
    waiting: int,
) -> bool:
    current = await gate.status()
    return current.used == used and current.waiting == waiting


async def test_weighted_public_properties_and_all_admission_helpers() -> None:
    gate = WeightedBulkhead(
        label="weighted-helpers",
        capacity=4,
        waiting_room=2,
        wait_limit=1.0,
    )

    assert gate.label == "weighted-helpers"
    assert gate.capacity == 4
    assert gate.waiting_room == 2
    assert gate.wait_limit == 1.0

    assert await gate.execute_within(0.5, 2, asyncio.sleep, 0, result="within") == "within"
    deadline = asyncio.get_running_loop().time() + 1.0
    assert await gate.execute_before(deadline, 2, asyncio.sleep, 0, result="before") == "before"

    async with gate.slot_now(1):
        assert (await gate.status()).used == 1
    async with gate.slot_within(0.5, 1):
        assert (await gate.status()).used == 1
    async with gate.slot_before(asyncio.get_running_loop().time() + 1.0, 1):
        assert (await gate.status()).used == 1

    await gate.resize(4)
    await gate.close()
    await gate.close()
    await gate.wait_closed()
    with pytest.raises(BulkheadClosedError):
        await gate.resize(5)


async def test_weighted_event_handler_removal_and_resize_event_metadata() -> None:
    gate = WeightedBulkhead(label="weighted-events", capacity=2)
    seen: list[object] = []

    def observe(event: object) -> None:
        seen.append(event)

    gate.add_event_handler(observe)  # type: ignore[arg-type]
    gate.remove_event_handler(observe)  # type: ignore[arg-type]
    await gate.execute(1, asyncio.sleep, 0)
    assert seen == []

    gate.add_event_handler(observe)  # type: ignore[arg-type]
    await gate.resize(3)
    resize_event = seen[-1]
    assert resize_event.kind is BulkheadEventKind.RESIZED
    assert resize_event.previous_capacity == 2
    assert resize_event.capacity == 3
    await gate.close_and_wait()


async def test_weighted_wait_closed_may_start_before_close() -> None:
    gate = WeightedBulkhead(label="weighted-wait-close", capacity=1)
    waiter = asyncio.create_task(gate.wait_closed())
    await asyncio.sleep(0)
    assert not waiter.done()

    await gate.close()
    await waiter
    assert (await gate.status()).is_drained


async def test_cancelling_blocking_fifo_head_admits_smaller_follower_without_release() -> None:
    gate = WeightedBulkhead(label="weighted-head-cancel", capacity=5, waiting_room=2)
    active_release = asyncio.Event()
    follower_started = asyncio.Event()
    follower_release = asyncio.Event()

    async def hold_active() -> None:
        async with gate.slot(3):
            await active_release.wait()

    async def follower() -> None:
        async with gate.slot(1):
            follower_started.set()
            await follower_release.wait()

    active = asyncio.create_task(hold_active())
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=0))
    head = asyncio.create_task(gate.execute(3, asyncio.sleep, 0))
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=1))
    tail = asyncio.create_task(follower())
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=2))

    head.cancel()
    with pytest.raises(asyncio.CancelledError):
        await head

    await asyncio.wait_for(follower_started.wait(), timeout=1.0)
    current = await gate.status()
    assert current.used == 4
    assert current.waiting == 0

    follower_release.set()
    active_release.set()
    await asyncio.gather(active, tail)


async def test_expiring_blocking_fifo_head_admits_smaller_follower_without_release() -> None:
    gate = WeightedBulkhead(label="weighted-head-expire", capacity=5, waiting_room=2)
    active_release = asyncio.Event()
    follower_started = asyncio.Event()
    follower_release = asyncio.Event()

    async def hold_active() -> None:
        async with gate.slot(3):
            await active_release.wait()

    async def follower() -> None:
        async with gate.slot(1):
            follower_started.set()
            await follower_release.wait()

    active = asyncio.create_task(hold_active())
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=0))
    head = asyncio.create_task(gate.execute_within(0.02, 3, asyncio.sleep, 0))
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=1))
    tail = asyncio.create_task(follower())
    await eventually(lambda: has_weighted_state(gate, used=3, waiting=2))

    with pytest.raises(BulkheadQueueTimeoutError):
        await head

    await asyncio.wait_for(follower_started.wait(), timeout=1.0)
    current = await gate.status()
    assert current.used == 4
    assert current.waiting == 0

    follower_release.set()
    active_release.set()
    await asyncio.gather(active, tail)
