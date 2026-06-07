from __future__ import annotations

import asyncio

from bulklink import AsyncBulkhead
from tests.helpers import eventually
from tests.invariants import assert_bulkhead_consistent


async def test_mixed_success_failure_and_cancellation_stress() -> None:
    gate = AsyncBulkhead(
        label="mixed-stress",
        parallelism=4,
        waiting_room=160,
    )
    release_holders = asyncio.Event()

    async def hold_slot() -> None:
        async with gate.slot():
            await release_holders.wait()

    holders = [asyncio.create_task(hold_slot()) for _ in range(4)]
    await eventually(lambda: has_in_flight(gate, 4))

    async def work(item: int) -> int:
        await asyncio.sleep(0)
        if item % 11 == 0:
            raise LookupError(item)
        return item

    queued = [asyncio.create_task(gate.execute(work, item)) for item in range(120)]
    await eventually(lambda: has_waiting(gate, 120))

    cancelled = queued[::4]
    survivors = [task for index, task in enumerate(queued) if index % 4 != 0]
    for task in cancelled:
        task.cancel()

    cancelled_results = await asyncio.gather(*cancelled, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in cancelled_results)
    await eventually(lambda: has_waiting(gate, len(survivors)))
    await assert_bulkhead_consistent(gate)

    release_holders.set()
    survivor_results = await asyncio.gather(*survivors, return_exceptions=True)
    await asyncio.gather(*holders)

    expected_failures = sum(1 for item in range(120) if item % 4 != 0 and item % 11 == 0)
    assert sum(isinstance(result, LookupError) for result in survivor_results) == expected_failures

    current = await gate.status()
    assert current.in_flight == 0
    assert current.waiting == 0
    assert current.queued_total == 120
    assert current.cancelled_total == len(cancelled)
    assert current.admitted_from_queue_total == len(survivors)
    assert current.admitted_total == len(holders) + len(survivors)
    assert current.finished_total == len(holders) + len(survivors)
    await assert_bulkhead_consistent(gate)


async def test_repeated_handoff_rounds_preserve_all_slots() -> None:
    for round_number in range(50):
        gate = AsyncBulkhead(
            label=f"handoff-round-{round_number}",
            parallelism=3,
            waiting_room=30,
        )

        async def work(value: int) -> int:
            await asyncio.sleep(0)
            return value

        results = await asyncio.gather(*(gate.execute(work, value) for value in range(30)))
        assert results == list(range(30))

        current = await gate.status()
        assert current.in_flight == 0
        assert current.waiting == 0
        assert current.admitted_total == 30
        assert current.finished_total == 30
        await assert_bulkhead_consistent(gate)


async def has_in_flight(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).in_flight == expected


async def has_waiting(gate: AsyncBulkhead, expected: int) -> bool:
    return (await gate.status()).waiting == expected
