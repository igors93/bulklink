from __future__ import annotations

import pytest

from bulklink import AsyncBulkhead


async def test_bulklink_invokes_failing_operation_exactly_once() -> None:
    gate = AsyncBulkhead(label="external-api", parallelism=1)
    calls = 0

    async def fail() -> None:
        nonlocal calls
        calls += 1
        raise ConnectionError("temporary failure")

    with pytest.raises(ConnectionError):
        await gate.execute(fail)

    assert calls == 1
