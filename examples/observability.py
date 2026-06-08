from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead, BulkheadEvent, BulkheadEventKind


async def main() -> None:
    gate = AsyncBulkhead(label="events-example", parallelism=1)
    events: list[BulkheadEvent] = []
    gate.add_event_handler(events.append)

    await gate.execute(asyncio.sleep, 0)
    await gate.close_and_wait()

    kinds = [event.kind for event in events]
    assert kinds == [
        BulkheadEventKind.ADMITTED,
        BulkheadEventKind.RELEASED,
        BulkheadEventKind.CLOSED,
        BulkheadEventKind.DRAINED,
    ]


if __name__ == "__main__":
    asyncio.run(main())
