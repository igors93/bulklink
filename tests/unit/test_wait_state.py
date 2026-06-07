from __future__ import annotations

import asyncio

import pytest

from bulklink._internal.models import WaitEntry, WaitState


async def test_wait_entry_reaches_only_one_terminal_state() -> None:
    loop = asyncio.get_running_loop()
    entry = WaitEntry(
        future=loop.create_future(),
        enqueued_at=0.0,
    )

    assert entry.state is WaitState.WAITING
    assert not entry.state.is_terminal

    assert entry.transition_to(WaitState.EXPIRED)
    assert entry.state is WaitState.EXPIRED
    assert entry.state.is_terminal

    assert not entry.transition_to(WaitState.CLOSED)
    assert not entry.transition_to(WaitState.ADMITTED)
    assert entry.state is WaitState.EXPIRED


async def test_wait_entry_cannot_transition_back_to_waiting() -> None:
    loop = asyncio.get_running_loop()
    entry = WaitEntry(
        future=loop.create_future(),
        enqueued_at=0.0,
    )

    with pytest.raises(ValueError, match="cannot transition back"):
        entry.transition_to(WaitState.WAITING)
