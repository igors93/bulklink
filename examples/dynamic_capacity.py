from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead


async def main() -> None:
    gate = AsyncBulkhead(label="resize-example", parallelism=1, waiting_room=3)
    release = asyncio.Event()
    started: list[int] = []

    async def work(item: int) -> None:
        started.append(item)
        await release.wait()

    tasks = [asyncio.create_task(gate.execute(work, item)) for item in range(4)]
    while (await gate.status()).waiting < 3:
        await asyncio.sleep(0)

    await gate.resize(4)
    while len(started) < 4:
        await asyncio.sleep(0)

    release.set()
    await asyncio.gather(*tasks)
    await gate.close_and_wait()

    assert started == [0, 1, 2, 3]


if __name__ == "__main__":
    asyncio.run(main())
