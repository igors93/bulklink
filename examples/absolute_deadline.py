from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead, BulkheadQueueTimeoutError


async def main() -> None:
    gate = AsyncBulkhead(label="deadline-example", parallelism=1, waiting_room=1)
    loop = asyncio.get_running_loop()

    result = await gate.execute_before(
        loop.time() + 1.0,
        asyncio.sleep,
        0,
        result="admitted",
    )
    print(result)

    try:
        await gate.execute_before(loop.time() - 1.0, asyncio.sleep, 0)
    except BulkheadQueueTimeoutError:
        print("deadline expired before admission")

    await gate.close_and_wait()


if __name__ == "__main__":
    asyncio.run(main())
