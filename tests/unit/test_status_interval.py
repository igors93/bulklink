from __future__ import annotations

import asyncio
import dataclasses

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadInterval,
    BulkheadQueueTimeoutError,
    BulkheadSaturatedError,
    BulkheadStatus,
)
from tests.helpers import eventually


async def test_interval_reports_direct_activity_without_mutating_snapshots() -> None:
    gate = AsyncBulkhead(label="interval-direct", parallelism=1)
    start = await gate.status()

    await gate.execute(asyncio.sleep, 0)
    end = await gate.status()
    interval = end.since(start)

    assert isinstance(interval, BulkheadInterval)
    assert interval.start is start
    assert interval.end is end
    assert interval.admitted == 1
    assert interval.direct_admitted == 1
    assert interval.admitted_from_queue == 0
    assert interval.finished == 1
    assert interval.queued == 0
    assert interval.rejected == 0
    assert interval.average_wait_seconds == 0.0
    assert interval.has_activity
    assert start.admitted_total == 0
    assert end.admitted_total == 1


async def test_interval_reports_queue_wait_and_average_for_the_period() -> None:
    gate = AsyncBulkhead(label="interval-queue", parallelism=1, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    start = await gate.status()

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))
    await asyncio.sleep(0.005)
    release.set()
    await asyncio.gather(active, queued)

    interval = (await gate.status()).since(start)
    assert interval.queued == 1
    assert interval.admitted == 1
    assert interval.admitted_from_queue == 1
    assert interval.direct_admitted == 0
    assert interval.settled_waiting == 1
    assert interval.finished == 2
    assert interval.cumulative_wait_seconds > 0
    assert interval.average_wait_seconds > 0


async def test_interval_combines_capacity_and_deadline_rejections() -> None:
    gate = AsyncBulkhead(label="interval-rejections", parallelism=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    start = await gate.status()

    with pytest.raises(BulkheadSaturatedError):
        await gate.execute_now(asyncio.sleep, 0)
    with pytest.raises(BulkheadQueueTimeoutError):
        await gate.execute_before(asyncio.get_running_loop().time() - 1.0, asyncio.sleep, 0)

    interval = (await gate.status()).since(start)
    assert interval.saturated == 1
    assert interval.expired_before_queue == 1
    assert interval.expired == 0
    assert interval.rejected == 2

    release.set()
    await active


async def test_empty_interval_and_resize_are_supported() -> None:
    gate = AsyncBulkhead(label="interval-empty", parallelism=1, waiting_room=2)
    start = await gate.status()
    assert not start.since(start).has_activity

    await gate.resize(3)
    end = await gate.status()
    interval = end.since(start)
    assert interval.start.parallelism == 1
    assert interval.end.parallelism == 3
    assert not interval.has_activity


async def test_interval_rejects_incompatible_or_reversed_snapshots() -> None:
    first_gate = AsyncBulkhead(label="first", parallelism=1, waiting_room=1)
    second_gate = AsyncBulkhead(label="second", parallelism=1, waiting_room=1)
    first = await first_gate.status()
    second = await second_gate.status()

    with pytest.raises(ValueError, match="same bulkhead label"):
        second.since(first)

    later = dataclasses.replace(first, admitted_total=1, finished_total=1)
    with pytest.raises(ValueError, match="decreased cumulative field"):
        first.since(later)

    changed_room = dataclasses.replace(first, waiting_room=2)
    with pytest.raises(ValueError, match="same waiting-room capacity"):
        changed_room.since(first)

    reopened = dataclasses.replace(first, is_closed=True)
    with pytest.raises(ValueError, match="cannot reopen"):
        first.since(reopened)


async def test_interval_is_immutable_and_requires_a_status() -> None:
    gate = AsyncBulkhead(label="immutable-interval", parallelism=1)
    current = await gate.status()
    interval = current.since(current)

    with pytest.raises(dataclasses.FrozenInstanceError):
        interval.admitted = 1  # type: ignore[misc]
    with pytest.raises(TypeError, match="BulkheadStatus"):
        current.since(object())  # type: ignore[arg-type]


def _assert_status(value: BulkheadStatus) -> BulkheadStatus:
    return value


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return _assert_status(await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return _assert_status(await gate.status()).waiting == expected
