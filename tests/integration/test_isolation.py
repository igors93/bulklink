from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead
from tests.helpers import eventually


async def test_independent_bulkheads_do_not_share_capacity() -> None:
    payments = AsyncBulkhead(label="payments", parallelism=1, waiting_room=1)
    reports = AsyncBulkhead(label="reports", parallelism=1, waiting_room=1)

    payment_release = asyncio.Event()
    report_started = asyncio.Event()

    async def slow_payment() -> None:
        async with payments.slot():
            await payment_release.wait()

    async def report() -> None:
        async with reports.slot():
            report_started.set()

    payment_task = asyncio.create_task(slow_payment())
    await eventually(lambda: active(payments))

    report_task = asyncio.create_task(report())
    await asyncio.wait_for(report_started.wait(), timeout=0.5)

    payment_release.set()
    await asyncio.gather(payment_task, report_task)


async def active(gate: AsyncBulkhead) -> bool:
    return (await gate.status()).in_flight == 1
