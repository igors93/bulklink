from __future__ import annotations

import asyncio
from time import monotonic

import pytest

from bulklink import AsyncBulkhead
from bulklink._internal.models import WaitEntry, WaitState


async def test_terminal_transition_requires_a_terminal_target() -> None:
    gate = AsyncBulkhead(label="transition-target", parallelism=1)
    coordinator = gate._coordinator
    entry = WaitEntry(
        future=asyncio.get_running_loop().create_future(),
        enqueued_at=monotonic(),
    )

    async with coordinator._mutex:
        with pytest.raises(ValueError, match="terminal state"):
            coordinator._finish_waiter_locked(
                entry,
                WaitState.WAITING,
                remove_from_queue=False,
            )


async def test_terminal_entry_cannot_be_completed_again() -> None:
    gate = AsyncBulkhead(label="terminal-entry", parallelism=1)
    coordinator = gate._coordinator
    entry = WaitEntry(
        future=asyncio.get_running_loop().create_future(),
        enqueued_at=monotonic(),
    )
    assert entry.transition_to(WaitState.CLOSED)

    async with coordinator._mutex:
        with pytest.raises(RuntimeError, match="only waiting entries"):
            coordinator._finish_waiter_locked(
                entry,
                WaitState.EXPIRED,
                remove_from_queue=False,
            )


async def test_missing_waiting_entry_is_reported_as_corruption() -> None:
    gate = AsyncBulkhead(label="missing-entry", parallelism=1)
    coordinator = gate._coordinator
    entry = WaitEntry(
        future=asyncio.get_running_loop().create_future(),
        enqueued_at=monotonic(),
    )

    async with coordinator._mutex:
        with pytest.raises(RuntimeError, match="missing from the FIFO queue"):
            coordinator._remove_waiter_locked(entry)


async def test_release_without_admission_is_rejected() -> None:
    gate = AsyncBulkhead(label="unmatched-release", parallelism=1)

    with pytest.raises(RuntimeError, match="without a matching admission"):
        await gate._coordinator.release()


async def test_completed_waiter_future_is_reported_before_handoff() -> None:
    gate = AsyncBulkhead(label="completed-waiter", parallelism=1, waiting_room=1)
    coordinator = gate._coordinator
    future = asyncio.get_running_loop().create_future()
    future.set_result(WaitState.CLOSED)
    entry = WaitEntry(future=future, enqueued_at=monotonic())

    async with coordinator._mutex:
        coordinator._in_flight = 1
        coordinator._waiters.append(entry)

        with pytest.raises(RuntimeError, match="future completed before admission"):
            coordinator._finish_admitted_slot_locked()


async def test_expiration_observes_an_existing_terminal_state() -> None:
    gate = AsyncBulkhead(label="existing-state", parallelism=1)
    coordinator = gate._coordinator
    entry = WaitEntry(
        future=asyncio.get_running_loop().create_future(),
        enqueued_at=monotonic(),
    )
    assert entry.transition_to(WaitState.CLOSED)

    assert await coordinator._expire_waiter(entry) is WaitState.CLOSED
