from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
    BulkheadEvent,
    BulkheadEventKind,
    BulkheadQueueTimeoutError,
    BulkheadSaturatedError,
)
from tests.helpers import eventually, install_observable_lock, wait_for_lock_waiters
from tests.invariants import assert_bulkhead_consistent


async def test_direct_execution_emits_admission_and_release_events() -> None:
    gate = AsyncBulkhead(label="events-direct", parallelism=1)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)

    assert await gate.execute(asyncio.sleep, 0, result="ok") == "ok"

    assert [event.kind for event in events] == [
        BulkheadEventKind.ADMITTED,
        BulkheadEventKind.RELEASED,
    ]
    assert all(event.label == "events-direct" for event in events)
    assert events[0].in_flight == 1
    assert events[0].from_queue is False
    assert events[1].in_flight == 0
    assert events[1].waited_seconds is None
    await assert_bulkhead_consistent(gate)


async def test_fifo_handoff_emits_events_in_state_transition_order() -> None:
    gate = AsyncBulkhead(label="events-fifo", parallelism=1, waiting_room=1)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    release.set()
    await asyncio.gather(active, queued)

    assert [event.kind for event in events] == [
        BulkheadEventKind.ADMITTED,
        BulkheadEventKind.QUEUED,
        BulkheadEventKind.RELEASED,
        BulkheadEventKind.ADMITTED,
        BulkheadEventKind.RELEASED,
    ]
    queued_admission = events[3]
    assert queued_admission.from_queue
    assert queued_admission.waited_seconds is not None
    assert queued_admission.waited_seconds >= 0
    await assert_bulkhead_consistent(gate)


async def test_saturation_emits_event_without_running_operation() -> None:
    gate = AsyncBulkhead(label="events-saturated", parallelism=1)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)
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

    with pytest.raises(BulkheadSaturatedError):
        await gate.execute_now(should_not_run)

    assert calls == 0
    assert events[-1].kind is BulkheadEventKind.SATURATED
    assert events[-1].in_flight == 1
    assert events[-1].waiting == 0

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_expiration_and_waiting_cancellation_emit_terminal_events() -> None:
    gate = AsyncBulkhead(
        label="events-terminal",
        parallelism=1,
        waiting_room=2,
        wait_limit=0.02,
    )
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))

    with pytest.raises(BulkheadQueueTimeoutError):
        await gate.execute(asyncio.sleep, 0)

    cancelled = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    terminal_kinds = {BulkheadEventKind.EXPIRED, BulkheadEventKind.CANCELLED}
    terminal = [event for event in events if event.kind in terminal_kinds]
    assert [event.kind for event in terminal] == [
        BulkheadEventKind.EXPIRED,
        BulkheadEventKind.CANCELLED,
    ]
    assert all(event.from_queue for event in terminal)
    assert all(event.waited_seconds is not None for event in terminal)

    release.set()
    await active
    await assert_bulkhead_consistent(gate)


async def test_cancellation_after_handoff_emits_abandoned_event() -> None:
    gate = AsyncBulkhead(label="events-abandoned", parallelism=1, waiting_room=1)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)
    lock = install_observable_lock(gate)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await lock.acquire()
    try:
        release.set()
        await wait_for_lock_waiters(lock, 1)
        queued.cancel()
        await wait_for_lock_waiters(lock, 2)
    finally:
        lock.release()

    await active
    with pytest.raises(asyncio.CancelledError):
        await queued

    queued_admissions = [
        event for event in events if event.kind is BulkheadEventKind.ADMITTED and event.from_queue
    ]
    abandoned = [event for event in events if event.kind is BulkheadEventKind.ABANDONED]
    assert len(queued_admissions) == 1
    assert len(abandoned) == 1
    assert abandoned[0].from_queue
    assert abandoned[0].in_flight == 0
    assert abandoned[0].waited_seconds == queued_admissions[0].waited_seconds
    await assert_bulkhead_consistent(gate)


async def test_closing_emits_summary_rejections_and_drained_events() -> None:
    gate = AsyncBulkhead(label="events-close", parallelism=1, waiting_room=1)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: has_in_flight(gate, 1))
    queued = asyncio.create_task(gate.execute(asyncio.sleep, 0))
    await eventually(lambda: has_waiting(gate, 1))

    await gate.close()
    with pytest.raises(BulkheadClosedError):
        await queued

    closed = [event for event in events if event.kind is BulkheadEventKind.CLOSED]
    closed_rejections = [
        event for event in events if event.kind is BulkheadEventKind.CLOSED_REJECTION
    ]
    assert len(closed) == 1
    assert closed[0].affected_waiters == 1
    assert len(closed_rejections) == 1
    assert closed_rejections[0].from_queue

    with pytest.raises(BulkheadClosedError):
        await gate.execute_now(asyncio.sleep, 0)
    assert events[-1].kind is BulkheadEventKind.CLOSED_REJECTION
    assert not events[-1].from_queue

    release.set()
    await active
    await gate.wait_closed()

    assert [event.kind for event in events][-2:] == [
        BulkheadEventKind.RELEASED,
        BulkheadEventKind.DRAINED,
    ]
    assert events[-1].is_closed
    assert events[-1].in_flight == 0
    await assert_bulkhead_consistent(gate)


async def test_handlers_run_outside_coordinator_lock() -> None:
    gate = AsyncBulkhead(label="events-unlocked", parallelism=1)
    lock_states: list[bool] = []

    def handler(event: BulkheadEvent) -> None:
        del event
        lock_states.append(gate._coordinator._mutex.locked())

    gate.add_event_handler(handler)
    await gate.execute(asyncio.sleep, 0)
    await gate.close_and_wait()

    assert lock_states
    assert not any(lock_states)


async def test_handler_failures_are_reported_without_affecting_operations() -> None:
    gate = AsyncBulkhead(label="events-errors", parallelism=1)
    delivered: list[BulkheadEventKind] = []
    reported: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()

    def bad_handler(event: BulkheadEvent) -> None:
        raise RuntimeError(f"failed on {event.kind.value}")

    def good_handler(event: BulkheadEvent) -> None:
        delivered.append(event.kind)

    def exception_handler(
        current_loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        assert current_loop is loop
        reported.append(context)

    loop.set_exception_handler(exception_handler)
    try:
        gate.add_event_handler(bad_handler)
        gate.add_event_handler(good_handler)
        assert await gate.execute(asyncio.sleep, 0, result=42) == 42
    finally:
        loop.set_exception_handler(previous)

    assert delivered == [BulkheadEventKind.ADMITTED, BulkheadEventKind.RELEASED]
    assert len(reported) == 2
    assert all(context["message"] == "Bulklink event handler failed" for context in reported)
    assert all(isinstance(context["exception"], RuntimeError) for context in reported)
    await assert_bulkhead_consistent(gate)


def test_async_handlers_are_rejected_at_registration() -> None:
    gate = AsyncBulkhead(label="events-async", parallelism=1)

    async def handler(event: BulkheadEvent) -> None:
        del event

    with pytest.raises(TypeError, match="synchronous"):
        gate.add_event_handler(handler)


async def test_duplicate_registration_and_removal_are_idempotent() -> None:
    gate = AsyncBulkhead(label="events-registration", parallelism=1)
    events: list[BulkheadEvent] = []
    handler: Callable[[BulkheadEvent], None] = events.append

    gate.add_event_handler(handler)
    gate.add_event_handler(handler)
    await gate.execute(asyncio.sleep, 0)
    assert len(events) == 2

    gate.remove_event_handler(handler)
    gate.remove_event_handler(handler)
    await gate.execute(asyncio.sleep, 0)
    assert len(events) == 2


def test_event_payload_is_immutable_and_contains_no_operation_data() -> None:
    fields = {field.name for field in dataclasses.fields(BulkheadEvent)}
    assert "operation" not in fields
    assert "args" not in fields
    assert "kwargs" not in fields
    assert "result" not in fields
    assert "exception" not in fields

    event = BulkheadEvent(
        kind=BulkheadEventKind.QUEUED,
        label="immutable",
        occurred_at=1.0,
        parallelism=1,
        waiting_room=1,
        in_flight=1,
        waiting=1,
        is_closed=False,
        from_queue=True,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        event.waiting = 0  # type: ignore[misc]


def test_non_callable_handler_is_rejected() -> None:
    gate = AsyncBulkhead(label="events-non-callable", parallelism=1)

    with pytest.raises(TypeError, match="callable"):
        gate.add_event_handler(42)  # type: ignore[arg-type]


@pytest.mark.parametrize("failure", [KeyboardInterrupt(), SystemExit()])
async def test_base_exception_from_handler_cannot_leak_admission(
    failure: BaseException,
) -> None:
    gate = AsyncBulkhead(label="events-base-exception", parallelism=1)
    reported: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()

    def handler(event: BulkheadEvent) -> None:
        del event
        raise failure

    loop.set_exception_handler(lambda current_loop, context: reported.append(context))
    try:
        gate.add_event_handler(handler)
        await gate.execute(asyncio.sleep, 0)
    finally:
        loop.set_exception_handler(previous)

    assert len(reported) == 2
    current = await gate.status()
    assert current.in_flight == 0
    assert current.finished_total == 1
    await assert_bulkhead_consistent(gate)


async def test_invalid_handler_return_is_reported_and_ignored() -> None:
    gate = AsyncBulkhead(label="events-return", parallelism=1)
    reported: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()

    def handler(event: BulkheadEvent) -> bool:
        del event
        return True

    loop.set_exception_handler(lambda current_loop, context: reported.append(context))
    try:
        gate.add_event_handler(handler)  # type: ignore[arg-type]
        await gate.execute(asyncio.sleep, 0)
    finally:
        loop.set_exception_handler(previous)

    assert len(reported) == 2
    assert all(isinstance(item["exception"], TypeError) for item in reported)
    await assert_bulkhead_consistent(gate)


async def test_callable_returning_coroutine_is_reported_without_leaking_it() -> None:
    gate = AsyncBulkhead(label="events-coroutine-return", parallelism=1)
    reported: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()

    class Handler:
        def __call__(self, event: BulkheadEvent) -> object:
            del event

            async def unsupported() -> None:
                return None

            return unsupported()

    loop.set_exception_handler(lambda current_loop, context: reported.append(context))
    try:
        gate.add_event_handler(Handler())  # type: ignore[arg-type]
        await gate.execute(asyncio.sleep, 0)
    finally:
        loop.set_exception_handler(previous)

    assert len(reported) == 2
    assert all(isinstance(item["exception"], TypeError) for item in reported)
    await assert_bulkhead_consistent(gate)


async def test_failing_loop_exception_handler_cannot_break_bulkhead() -> None:
    gate = AsyncBulkhead(label="events-loop-handler", parallelism=1)
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()

    def event_handler(event: BulkheadEvent) -> None:
        del event
        raise RuntimeError("event failure")

    def loop_handler(
        current_loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        del current_loop, context
        raise RuntimeError("loop handler failure")

    loop.set_exception_handler(loop_handler)
    try:
        gate.add_event_handler(event_handler)
        await gate.execute(asyncio.sleep, 0)
    finally:
        loop.set_exception_handler(previous)

    await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
