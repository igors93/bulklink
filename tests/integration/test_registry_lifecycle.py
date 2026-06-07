from __future__ import annotations

import asyncio

from bulklink import BulkheadRegistry


async def test_registry_coordinates_independent_bulkheads_under_load() -> None:
    registry = BulkheadRegistry()
    payments = registry.create("payments", parallelism=4, waiting_room=40)
    reports = registry.create("reports", parallelism=2, waiting_room=20)
    completed: list[tuple[str, int]] = []

    async def work(label: str, index: int) -> None:
        await asyncio.sleep(0)
        completed.append((label, index))

    tasks = [
        *(asyncio.create_task(payments.execute(work, "payments", index)) for index in range(30)),
        *(asyncio.create_task(reports.execute(work, "reports", index)) for index in range(20)),
    ]
    await asyncio.gather(*tasks)
    await registry.close_and_wait()

    assert sorted(index for label, index in completed if label == "payments") == list(range(30))
    assert sorted(index for label, index in completed if label == "reports") == list(range(20))
    statuses = await registry.statuses()
    assert [status.label for status in statuses] == ["payments", "reports"]
    assert all(status.is_drained for status in statuses)
    assert [status.finished_total for status in statuses] == [30, 20]
