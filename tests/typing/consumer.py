from __future__ import annotations

import asyncio

from bulklink import (
    AsyncBulkhead,
    BulkheadEvent,
    BulkheadRegistry,
    BulkheadStatus,
    CapacityReport,
)


async def render(value: int) -> str:
    return str(value)


def observe(event: BulkheadEvent) -> None:
    _ = event.kind


async def consume_public_api() -> None:
    gate = AsyncBulkhead(
        label="consumer",
        parallelism=2,
        waiting_room=4,
        wait_limit=1.0,
    )
    gate.add_event_handler(observe)

    direct: str = await gate.execute(render, 1)
    immediate: str = await gate.execute_now(render, 2)
    limited: str = await gate.execute_within(0.5, render, 3)
    loop = asyncio.get_running_loop()
    deadline_limited: str = await gate.execute_before(loop.time() + 1.0, render, 4)
    async with gate.slot_before(loop.time() + 1.0):
        slot_deadline_result: str = await render(5)
    decorated = gate(render)
    decorated_result: str = await decorated(4)

    status: BulkheadStatus = await gate.status()
    report: CapacityReport = await gate.capacity_report()
    await gate.resize(3)
    await gate.close_and_wait()

    registry = BulkheadRegistry()
    registered: AsyncBulkhead = registry.create("registered", parallelism=1)
    statuses: tuple[BulkheadStatus, ...] = await registry.statuses()
    reports: tuple[CapacityReport, ...] = await registry.capacity_reports()
    await registry.close_and_wait()

    _ = (
        direct,
        immediate,
        limited,
        deadline_limited,
        slot_deadline_result,
        decorated_result,
        status,
        report,
        registered,
        statuses,
        reports,
    )
