from __future__ import annotations

import asyncio
import random
from contextlib import suppress
from dataclasses import dataclass

import pytest

from bulklink import (
    AsyncBulkhead,
    BulkheadClosedError,
    BulkheadSaturatedError,
)
from tests.invariants import assert_bulkhead_consistent


@dataclass(slots=True)
class OperationRecord:
    started: asyncio.Event
    release: asyncio.Event
    task: asyncio.Task[None]


async def _operation(started: asyncio.Event, release: asyncio.Event) -> None:
    started.set()
    await release.wait()


async def _settle_scheduler() -> None:
    for _ in range(3):
        await asyncio.sleep(0)


def _first_pending(records: list[OperationRecord]) -> OperationRecord | None:
    return next(
        (record for record in records if not record.task.done() and not record.started.is_set()),
        None,
    )


def _first_active(records: list[OperationRecord]) -> OperationRecord | None:
    return next(
        (
            record
            for record in records
            if record.started.is_set() and not record.release.is_set() and not record.task.done()
        ),
        None,
    )


async def _run_generated_sequence(seed: int) -> None:
    randomizer = random.Random(seed)
    gate = AsyncBulkhead(
        label=f"model-{seed}",
        parallelism=2,
        waiting_room=4,
    )
    records: list[OperationRecord] = []

    for _ in range(48):
        action = randomizer.randrange(5)

        if action == 0 and len(records) < 18:
            started = asyncio.Event()
            release = asyncio.Event()
            task = asyncio.create_task(gate.execute(_operation, started, release))
            records.append(OperationRecord(started=started, release=release, task=task))
        elif action == 1:
            pending = _first_pending(records)
            if pending is not None:
                pending.task.cancel()
        elif action == 2:
            active = _first_active(records)
            if active is not None:
                active.release.set()
        elif action == 3:
            await gate.resize(randomizer.randint(1, 5))
        else:
            with suppress(BulkheadSaturatedError):
                await gate.execute_now(asyncio.sleep, 0)

        await _settle_scheduler()
        await assert_bulkhead_consistent(gate)

    for record in records:
        record.release.set()

    results = await asyncio.gather(
        *(record.task for record in records),
        return_exceptions=True,
    )
    for result in results:
        assert result is None or isinstance(
            result,
            (asyncio.CancelledError, BulkheadSaturatedError),
        )

    await gate.close_and_wait()
    status = await gate.status()
    assert status.is_drained
    assert status.in_flight == 0
    assert status.waiting == 0
    assert status.admitted_total == (status.finished_total + status.abandoned_after_admission_total)
    assert status.queued_total == status.settled_waiting_total
    await assert_bulkhead_consistent(gate)


@pytest.mark.model
@pytest.mark.parametrize("seed", range(24))
async def test_generated_admission_resize_and_cancellation_sequences(seed: int) -> None:
    await asyncio.wait_for(_run_generated_sequence(seed), timeout=10.0)


async def _run_shutdown_waiter_sequence() -> None:
    gate = AsyncBulkhead(label="model-shutdown", parallelism=2, waiting_room=16)
    records: list[OperationRecord] = []

    for _ in range(12):
        started = asyncio.Event()
        release = asyncio.Event()
        task = asyncio.create_task(gate.execute(_operation, started, release))
        records.append(OperationRecord(started=started, release=release, task=task))

    await _settle_scheduler()
    waiters = [asyncio.create_task(gate.wait_closed()) for _ in range(6)]
    shutdown = asyncio.create_task(gate.close_and_wait())
    await _settle_scheduler()

    waiters[1].cancel()
    waiters[4].cancel()
    for record in records:
        record.release.set()

    operation_results = await asyncio.gather(
        *(record.task for record in records),
        return_exceptions=True,
    )
    for result in operation_results:
        assert result is None or isinstance(result, BulkheadClosedError)

    await shutdown
    waiter_results = await asyncio.gather(*waiters, return_exceptions=True)
    assert sum(isinstance(result, asyncio.CancelledError) for result in waiter_results) == 2
    assert sum(result is None for result in waiter_results) == 4

    status = await gate.status()
    assert status.is_drained
    await assert_bulkhead_consistent(gate)


@pytest.mark.model
async def test_shutdown_waiters_remain_independent_under_cancellation() -> None:
    await asyncio.wait_for(_run_shutdown_waiter_sequence(), timeout=10.0)
