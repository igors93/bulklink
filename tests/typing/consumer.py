from __future__ import annotations

import asyncio

from bulklink import (
    AsyncBulkhead,
    BulkheadEvent,
    BulkheadInterval,
    BulkheadRegistry,
    BulkheadStatus,
    CapacityReport,
    WeightedBulkhead,
    WeightedBulkheadEvent,
    WeightedBulkheadInterval,
    WeightedBulkheadStatus,
)


async def render(value: int) -> str:
    return str(value)


def observe(event: BulkheadEvent) -> None:
    _ = event.kind


def observe_weighted(event: WeightedBulkheadEvent) -> None:
    _ = (event.kind, event.cost)


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

    previous_status: BulkheadStatus = await gate.status()
    status: BulkheadStatus = await gate.status()
    interval: BulkheadInterval = status.since(previous_status)
    report: CapacityReport = await gate.capacity_report()
    await gate.resize(3)
    await gate.close_and_wait()

    weighted = WeightedBulkhead(
        label="weighted-consumer",
        capacity=5,
        waiting_room=4,
        wait_limit=1.0,
    )
    weighted.add_event_handler(observe_weighted)
    weighted_direct: str = await weighted.execute(2, render, 6)
    weighted_immediate: str = await weighted.execute_now(1, render, 7)
    weighted_limited: str = await weighted.execute_within(0.5, 2, render, 8)
    weighted_deadline: str = await weighted.execute_before(
        loop.time() + 1.0,
        2,
        render,
        9,
    )
    weighted_previous: WeightedBulkheadStatus = await weighted.status()
    weighted_status: WeightedBulkheadStatus = await weighted.status()
    weighted_interval: WeightedBulkheadInterval = weighted_status.since(weighted_previous)
    await weighted.resize(6)
    await weighted.close_and_wait()

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
        interval,
        report,
        weighted_direct,
        weighted_immediate,
        weighted_limited,
        weighted_deadline,
        weighted_status,
        weighted_interval,
        registered,
        statuses,
        reports,
    )
