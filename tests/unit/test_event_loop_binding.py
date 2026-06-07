from __future__ import annotations

import asyncio

import pytest

from bulklink import AsyncBulkhead


def test_instance_cannot_be_reused_across_event_loops() -> None:
    gate = AsyncBulkhead(label="loop-bound", parallelism=1)

    asyncio.run(gate.status())

    with pytest.raises(RuntimeError, match="different event loops"):
        asyncio.run(gate.status())
