from __future__ import annotations

import asyncio

from bulklink import BulkheadRegistry


async def main() -> None:
    registry = BulkheadRegistry()
    payments = registry.create("payments", parallelism=2, waiting_room=2)
    reports = registry.create("reports", parallelism=1)

    assert await payments.execute(asyncio.sleep, 0, result="paid") == "paid"
    assert await reports.execute(asyncio.sleep, 0, result="ready") == "ready"

    statuses = await registry.statuses()
    assert [status.label for status in statuses] == ["payments", "reports"]

    await registry.close_and_wait()
    assert all(status.is_drained for status in await registry.statuses())


if __name__ == "__main__":
    asyncio.run(main())
