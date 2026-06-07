from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead, BulkheadSaturatedError


async def main() -> None:
    gate = AsyncBulkhead(label="tiny", parallelism=1, waiting_room=0)

    async def slow() -> None:
        await asyncio.sleep(0.2)

    first = asyncio.create_task(gate.execute(slow))
    await asyncio.sleep(0)

    try:
        await gate.execute(slow)
    except BulkheadSaturatedError as error:
        print(error)

    await first


if __name__ == "__main__":
    asyncio.run(main())
