from __future__ import annotations

import asyncio

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
    BulkheadEvent,
    BulkheadEventKind,
    BulkheadQueueTimeoutError,
)
from tests.helpers import eventually
from tests.invariants import assert_bulkhead_consistent


async def test_execute_before_admits_before_future_deadline() -> None:
    gate = AsyncBulkhead(label="deadline-success", parallelism=1)
    deadline = asyncio.get_running_loop().time() + 1.0

    result = await gate.execute_before(deadline, asyncio.sleep, 0, result="ok")

    assert result == "ok"
    current = await gate.status()
    assert current.finished_total == 1
    assert current.expired_before_queue_total == 0
    await assert_bulkhead_consistent(gate)


async def test_execute_before_expires_while_waiting_without_running_operation() -> None:
    gate = AsyncBulkhead(label="deadline-queued", parallelism=1, waiting_room=1)
    release = asyncio.Event()
    calls = 0

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    async def should_not_run() -> None:
        nonlocal calls
        calls += 1

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    deadline = asyncio.get_running_loop().time() + 0.02

    with pytest.raises(BulkheadQueueTimeoutError) as captured:
        await gate.execute_before(deadline, should_not_run)

    assert 0 < captured.value.wait_limit <= 0.03
    assert calls == 0
    current = await gate.status()
    assert current.expired_total == 1
    assert current.expired_before_queue_total == 0
    assert current.waiting == 0

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_expired_deadline_rejects_before_queue_and_emits_event() -> None:
    gate = AsyncBulkhead(label="deadline-past", parallelism=1, waiting_room=1)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)
    deadline = asyncio.get_running_loop().time() - 1.0

    with pytest.raises(BulkheadQueueTimeoutError) as captured:
        async with gate.slot_before(deadline):
            raise AssertionError("expired deadline entered the protected body")

    assert captured.value.wait_limit == 0.0
    assert [event.kind for event in events] == [BulkheadEventKind.EXPIRED]
    assert not events[0].from_queue
    assert events[0].waited_seconds == 0.0

    current = await gate.status()
    assert current.queued_total == 0
    assert current.expired_total == 0
    assert current.expired_before_queue_total == 1
    assert current.rejected_total == 1
    await assert_bulkhead_consistent(gate)


async def test_closed_state_has_priority_over_expired_deadline() -> None:
    gate = AsyncBulkhead(label="deadline-closed", parallelism=1)
    await gate.close()
    deadline = asyncio.get_running_loop().time() - 1.0

    with pytest.raises(BulkheadClosedError):
        async with gate.slot_before(deadline):
            pass

    current = await gate.status()
    assert current.closed_before_queue_total == 1
    assert current.expired_before_queue_total == 0
    await assert_bulkhead_consistent(gate)


async def test_deadline_applies_only_to_admission_not_operation_runtime() -> None:
    gate = AsyncBulkhead(label="deadline-body", parallelism=1)
    deadline = asyncio.get_running_loop().time() + 0.001

    async def slow_body() -> str:
        await asyncio.sleep(0.02)
        return "completed"

    assert await gate.execute_before(deadline, slow_body) == "completed"
    current = await gate.status()
    assert current.finished_total == 1
    assert current.expired_total == 0
    assert current.expired_before_queue_total == 0
    await assert_bulkhead_consistent(gate)


async def test_configured_wait_limit_can_expire_before_absolute_deadline() -> None:
    gate = AsyncBulkhead(
        label="deadline-default-first",
        parallelism=1,
        waiting_room=1,
        wait_limit=0.02,
    )
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    deadline = asyncio.get_running_loop().time() + 1.0

    with pytest.raises(BulkheadQueueTimeoutError) as captured:
        await gate.execute_before(deadline, asyncio.sleep, 0)

    assert 0 < captured.value.wait_limit <= 0.03
    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_absolute_deadline_can_expire_before_configured_wait_limit() -> None:
    gate = AsyncBulkhead(
        label="deadline-absolute-first",
        parallelism=1,
        waiting_room=1,
        wait_limit=1.0,
    )
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    deadline = asyncio.get_running_loop().time() + 0.02

    with pytest.raises(BulkheadQueueTimeoutError) as captured:
        await gate.execute_before(deadline, asyncio.sleep, 0)

    assert 0 < captured.value.wait_limit <= 0.03
    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_execute_before_does_not_consume_operation_deadline_keyword() -> None:
    gate = AsyncBulkhead(label="deadline-keyword", parallelism=1)
    absolute = asyncio.get_running_loop().time() + 1.0

    async def operation(*, deadline: float) -> float:
        return deadline

    result = await gate.execute_before(absolute, operation, deadline=7.0)

    assert result == 7.0
    await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected
