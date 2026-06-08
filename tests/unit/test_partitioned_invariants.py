"""Model-based deterministic tests for PartitionedBulkhead internal invariants.

Each test runs a seed-driven sequence of actions (new key, same key, release,
cancel, timeout, eviction, cleanup, discard, close, wait_closed) and verifies
all invariants after each step via assert_partitioned_consistent().
"""

from __future__ import annotations

import asyncio
import random
from contextlib import suppress
from dataclasses import dataclass, field

import pytest

from bulklink import (
    BulkheadClosedError,
    BulkheadQueueTimeoutError,
    PartitionedBulkhead,
    PartitionLimitError,
)
from tests.invariants import assert_partitioned_consistent


@dataclass(slots=True)
class _PartitionOp:
    key: str
    task: asyncio.Task[None]
    release: asyncio.Event
    started: asyncio.Event
    done: bool = field(default=False)


async def _settle() -> None:
    for _ in range(4):
        await asyncio.sleep(0)


async def _run_model_sequence(seed: int) -> None:
    rng = random.Random(seed)
    gate = PartitionedBulkhead(
        label=f"model-partitioned-{seed}",
        parallelism=2,
        waiting_room=1,
        wait_limit=None,
        max_partitions=3,
        idle_timeout=0.01,
    )
    ops: list[_PartitionOp] = []
    keys = [f"key-{i}" for i in range(6)]
    is_closed = False

    for _ in range(40):
        action = rng.randrange(8)
        await _settle()

        if action == 0 and not is_closed:
            # Admit a new operation on a random key.
            key = rng.choice(keys)
            release = asyncio.Event()
            started = asyncio.Event()

            async def _work(rel: asyncio.Event, started_ev: asyncio.Event) -> None:
                started_ev.set()
                await rel.wait()

            task: asyncio.Task[None] = asyncio.create_task(
                gate.execute(key, _work, release, started)
            )
            op = _PartitionOp(key=key, task=task, release=release, started=started)
            ops.append(op)

        elif action == 1:
            # Release one active operation.
            active = [o for o in ops if o.started.is_set() and not o.release.is_set()]
            if active:
                chosen = rng.choice(active)
                chosen.release.set()

        elif action == 2:
            # Cancel one pending (not yet started) operation.
            pending = [o for o in ops if not o.task.done() and not o.started.is_set()]
            if pending:
                chosen = rng.choice(pending)
                chosen.task.cancel()

        elif action == 3 and not is_closed:
            # Discard an idle partition (if any).
            with suppress(BulkheadClosedError, PartitionLimitError):
                key = rng.choice(keys)
                await gate.discard(key)

        elif action == 4 and not is_closed:
            # Cleanup idle partitions.
            with suppress(BulkheadClosedError):
                await gate.cleanup_idle()

        elif action == 5 and not is_closed:
            # Try an immediate (slot_now) admission.
            key = rng.choice(keys)
            with suppress(BulkheadClosedError, PartitionLimitError):
                from bulklink import BulkheadSaturatedError

                with suppress(BulkheadSaturatedError):
                    await gate.execute_now(key, asyncio.sleep, 0)

        elif action == 6 and not is_closed:
            # Try a timed-out admission (expired deadline).
            loop = asyncio.get_running_loop()
            key = rng.choice(keys)
            past = loop.time() - 1.0
            with suppress(BulkheadClosedError, BulkheadQueueTimeoutError, PartitionLimitError):
                await gate.execute_before(past, key, asyncio.sleep, 0)

        elif action == 7 and not is_closed:
            # Close the gate.
            is_closed = True
            await gate.close()

        await _settle()

        if not is_closed:
            await assert_partitioned_consistent(gate)

    # Release all pending operations and collect results.
    for op in ops:
        op.release.set()

    results = await asyncio.gather(*(o.task for o in ops), return_exceptions=True)
    for result in results:
        assert result is None or isinstance(
            result,
            (
                asyncio.CancelledError,
                BulkheadClosedError,
                PartitionLimitError,
                BulkheadQueueTimeoutError,
            ),
        ), f"unexpected result type: {type(result).__name__}: {result!r}"

    if not is_closed:
        await gate.close_and_wait()
    else:
        await gate.wait_closed()

    final = await gate.status()
    assert final.is_closed
    assert final.leased_operations == 0
    assert final.active_partitions == 0


@pytest.mark.model
@pytest.mark.parametrize("seed", range(24))
async def test_model_partitioned_sequence(seed: int) -> None:
    await asyncio.wait_for(_run_model_sequence(seed), timeout=15.0)


async def test_invariants_hold_during_concurrent_eviction() -> None:
    """Invariants checked at multiple points during concurrent eviction."""
    gate = PartitionedBulkhead(
        label="invariant-eviction",
        parallelism=1,
        max_partitions=2,
        idle_timeout=60.0,
    )

    await gate.execute("A", asyncio.sleep, 0)
    await gate.execute("B", asyncio.sleep, 0)
    await assert_partitioned_consistent(gate)

    # Now force an eviction by adding a third key.
    t1 = asyncio.create_task(gate.execute("C", asyncio.sleep, 0))
    await asyncio.sleep(0)
    await assert_partitioned_consistent(gate)

    await asyncio.wait_for(t1, timeout=2.0)
    await assert_partitioned_consistent(gate)

    await gate.close_and_wait()


async def test_invariants_hold_after_cleanup_idle() -> None:
    """Invariants must hold after cleanup_idle() removes partitions."""
    gate = PartitionedBulkhead(
        label="invariant-cleanup",
        parallelism=1,
        max_partitions=4,
        idle_timeout=0.001,
    )

    for key in ("A", "B", "C"):
        await gate.execute(key, asyncio.sleep, 0)

    await asyncio.sleep(0.01)
    await assert_partitioned_consistent(gate)

    removed = await gate.cleanup_idle()
    assert removed == 3
    await assert_partitioned_consistent(gate)

    await gate.close_and_wait()


async def test_invariants_hold_after_repeated_discard() -> None:
    """Invariants must hold after multiple discard() operations."""
    gate = PartitionedBulkhead(
        label="invariant-discard",
        parallelism=1,
        max_partitions=4,
        idle_timeout=60.0,
    )

    for key in ("A", "B", "C", "D"):
        await gate.execute(key, asyncio.sleep, 0)

    await assert_partitioned_consistent(gate)

    for key in ("A", "B", "C", "D"):
        result = await gate.discard(key)
        assert result is True
        await assert_partitioned_consistent(gate)

    await gate.close_and_wait()


async def test_invariants_hold_through_shutdown_sequence() -> None:
    """Invariants must hold at each stage of the shutdown sequence."""
    gate = PartitionedBulkhead(
        label="invariant-shutdown",
        parallelism=1,
        max_partitions=2,
    )
    release = asyncio.Event()

    t1 = asyncio.create_task(gate.execute("A", release.wait))
    t2 = asyncio.create_task(gate.execute("B", release.wait))
    await asyncio.sleep(0)
    await assert_partitioned_consistent(gate)

    await gate.close()
    await assert_partitioned_consistent(gate)

    release.set()
    await asyncio.gather(t1, t2, return_exceptions=True)
    await gate.wait_closed()

    final = await gate.status()
    assert final.is_closed
    assert final.leased_operations == 0
