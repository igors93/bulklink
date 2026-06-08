from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead


async def main() -> None:
    gate = AsyncBulkhead(label="shutdown-example", parallelism=2, waiting_room=4)

    async def work(item: int) -> int:
        await asyncio.sleep(0.01)
        return item * 2

    tasks = [asyncio.create_task(gate.execute(work, item)) for item in range(6)]
    results = await asyncio.gather(*tasks)
    await gate.close_and_wait()

    status = await gate.status()
    assert results == [0, 2, 4, 6, 8, 10]
    assert status.is_drained


if __name__ == "__main__":
    asyncio.run(main())
