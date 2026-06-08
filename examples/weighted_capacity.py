from __future__ import annotations

import asyncio

from bulklink import WeightedBulkhead


async def process(name: str, delay: float = 0.01) -> str:
    await asyncio.sleep(delay)
    return name


async def main() -> None:
    gate = WeightedBulkhead(
        label="reports",
        capacity=5,
        waiting_room=4,
        wait_limit=1.0,
    )
    before = await gate.status()

    results = await asyncio.gather(
        gate.execute(3, process, "large"),
        gate.execute(1, process, "small-a"),
        gate.execute(1, process, "small-b"),
    )

    after = await gate.status()
    interval = after.since(before)
    print(results)
    print("admitted operations", interval.admitted)
    print("admitted units", interval.admitted_units)
    print("peak used", after.peak_used)

    await gate.close_and_wait()


if __name__ == "__main__":
    asyncio.run(main())
