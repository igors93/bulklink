from __future__ import annotations

import pytest

from bulklink import AsyncBulkhead


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
