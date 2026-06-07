from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead


async def main() -> None:
    workers = AsyncBulkhead(
        label="demo-workers",
        parallelism=2,
        waiting_room=4,
        wait_limit=1.0,
    )

    @workers
    async def work(item: int) -> int:
        print(f"start {item}")
        await asyncio.sleep(0.1)
        print(f"finish {item}")
        return item * 2

    values = await asyncio.gather(*(work(item) for item in range(6)))
    print(values)
    print(await workers.status())


if __name__ == "__main__":
    asyncio.run(main())
