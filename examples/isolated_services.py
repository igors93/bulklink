from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead

payments = AsyncBulkhead(label="payments", parallelism=10, waiting_room=50)
reports = AsyncBulkhead(label="reports", parallelism=2, waiting_room=5)


async def call_service(name: str, delay: float) -> str:
    await asyncio.sleep(delay)
    return name


async def main() -> None:
    payment = payments.execute(call_service, "payment", 0.05)
    report = reports.execute(call_service, "report", 0.2)
    print(await asyncio.gather(payment, report))


if __name__ == "__main__":
    asyncio.run(main())
