from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead


async def main() -> None:
    gate = AsyncBulkhead(label="interval-example", parallelism=2, waiting_room=2)
    before = await gate.status()

    await asyncio.gather(
        gate.execute(asyncio.sleep, 0),
        gate.execute(asyncio.sleep, 0),
        gate.execute(asyncio.sleep, 0),
    )

    after = await gate.status()
    interval = after.since(before)

    print("admitted", interval.admitted)
    print("queued", interval.queued)
    print("finished", interval.finished)
    print("rejected", interval.rejected)

    await gate.close_and_wait()


if __name__ == "__main__":
    asyncio.run(main())
