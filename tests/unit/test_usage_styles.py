from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead, BulkheadSaturatedError
from bulklink._internal.slot import SlotContext
from tests.helpers import eventually


async def test_execute_returns_original_value() -> None:
    gate = AsyncBulkhead(label="math", parallelism=1)

    async def add(left: int, right: int) -> int:
        return left + right

    assert await gate.execute(add, 2, 3) == 5


async def test_decorator_preserves_metadata_and_result() -> None:
    gate = AsyncBulkhead(label="decorated", parallelism=1)

    @gate
    async def double(value: int) -> int:
        """Double one value."""
        return value * 2

    assert double.__name__ == "double"
    assert double.__doc__ == "Double one value."
    assert await double(4) == 8


async def test_user_exception_propagates_unchanged_and_slot_is_released() -> None:
    gate = AsyncBulkhead(label="errors", parallelism=1)
    original = LookupError("missing")

    async def fail() -> None:
        raise original

    with pytest.raises(LookupError) as caught:
        await gate.execute(fail)

    assert caught.value is original

    async with gate.slot():
        pass


async def test_same_slot_context_cannot_be_entered_twice() -> None:
    gate = AsyncBulkhead(label="guard", parallelism=1)
    slot = gate.slot()

    await slot.__aenter__()
    try:
        with pytest.raises(RuntimeError):
            await slot.__aenter__()
    finally:
        await slot.__aexit__(None, None, None)


async def test_exit_without_enter_is_harmless() -> None:
    gate = AsyncBulkhead(label="unused", parallelism=1)
    slot = gate.slot()

    await slot.__aexit__(None, None, None)

    assert (await gate.status()).in_flight == 0


async def test_same_immediate_slot_context_cannot_be_entered_twice() -> None:
    gate = AsyncBulkhead(label="immediate-guard", parallelism=1)
    slot = gate.slot_now()

    await slot.__aenter__()
    try:
        with pytest.raises(RuntimeError):
            await slot.__aenter__()
    finally:
        await slot.__aexit__(None, None, None)


async def test_concurrent_entry_of_one_slot_context_is_rejected() -> None:
    gate = AsyncBulkhead(label="entering-guard", parallelism=1, waiting_room=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: waiting_for_active(gate))

    slot = gate.slot()
    first_entry = asyncio.create_task(slot.__aenter__())
    await eventually(lambda: waiting_for_queue(gate))

    with pytest.raises(RuntimeError, match="already in use"):
        await slot.__aenter__()

    assert (await gate.status()).waiting == 1

    first_entry.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_entry

    release.set()
    await active

    await slot.__aenter__()
    await slot.__aexit__(None, None, None)
    assert (await gate.status()).in_flight == 0


async def test_slot_context_cannot_reenter_while_release_is_running() -> None:
    release_started = asyncio.Event()
    allow_release = asyncio.Event()

    async def admit() -> None:
        return None

    async def release() -> None:
        release_started.set()
        await allow_release.wait()

    slot = SlotContext(admit=admit, release=release)
    await slot.__aenter__()

    exiting = asyncio.create_task(slot.__aexit__(None, None, None))
    await release_started.wait()

    with pytest.raises(RuntimeError, match="already in use"):
        await slot.__aenter__()

    allow_release.set()
    await exiting

    await slot.__aenter__()
    await slot.__aexit__(None, None, None)


async def test_slot_context_cannot_exit_while_admission_is_running() -> None:
    admission_started = asyncio.Event()
    allow_admission = asyncio.Event()

    async def admit() -> None:
        admission_started.set()
        await allow_admission.wait()

    async def release() -> None:
        return None

    slot = SlotContext(admit=admit, release=release)
    entering = asyncio.create_task(slot.__aenter__())
    await admission_started.wait()

    with pytest.raises(RuntimeError, match="lifecycle transition"):
        await slot.__aexit__(None, None, None)

    entering.cancel()
    with pytest.raises(asyncio.CancelledError):
        await entering

    allow_admission.set()
    await slot.__aenter__()
    await slot.__aexit__(None, None, None)


async def test_failed_immediate_admission_resets_slot_context() -> None:
    gate = AsyncBulkhead(label="failed-admission-reset", parallelism=1)
    release = asyncio.Event()

    async def hold() -> None:
        async with gate.slot():
            await release.wait()

    active = asyncio.create_task(hold())
    await eventually(lambda: waiting_for_active(gate))

    slot = gate.slot_now()
    with pytest.raises(BulkheadSaturatedError):
        await slot.__aenter__()

    release.set()
    await active

    await slot.__aenter__()
    await slot.__aexit__(None, None, None)
    assert (await gate.status()).in_flight == 0


async def waiting_for_active(gate: AsyncBulkhead) -> bool:
    return (await gate.status()).in_flight == 1


async def waiting_for_queue(gate: AsyncBulkhead) -> bool:
    return (await gate.status()).waiting == 1
