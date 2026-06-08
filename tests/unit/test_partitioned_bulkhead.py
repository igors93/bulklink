from __future__ import annotations

import asyncio

import pytest

from bulklink import (
    BulkheadClosedError,
    BulkheadSaturatedError,
    PartitionedBulkhead,
    PartitionLimitError,
)
from tests.helpers import eventually


async def test_different_keys_receive_independent_execution_capacity() -> None:
    gate = PartitionedBulkhead(
        label="tenants",
        parallelism=1,
        max_partitions=2,
    )
    first_entered = asyncio.Event()
    second_entered = asyncio.Event()
    release = asyncio.Event()

    async def hold(entered: asyncio.Event) -> None:
        entered.set()
        await release.wait()

    first = asyncio.create_task(gate.execute("alpha", hold, first_entered))
    second = asyncio.create_task(gate.execute("beta", hold, second_entered))

    await asyncio.wait_for(first_entered.wait(), timeout=1.0)
    await asyncio.wait_for(second_entered.wait(), timeout=1.0)

    status = await gate.status()
    assert status.partition_count == 2
    assert status.active_partitions == 2
    assert status.leased_operations == 2

    release.set()
    await asyncio.gather(first, second)
    await gate.close_and_wait()


async def test_same_key_uses_one_partition_limit() -> None:
    gate = PartitionedBulkhead(
        label="per-tenant",
        parallelism=1,
        max_partitions=2,
        waiting_room=0,
    )
    release = asyncio.Event()

    async def hold() -> None:
        await release.wait()

    active = asyncio.create_task(gate.execute("alpha", hold))
    await eventually(lambda: has_leased_operations(gate, 1))

    with pytest.raises(BulkheadSaturatedError):
        await gate.execute_now("alpha", asyncio.sleep, 0)

    release.set()
    await active


async def test_concurrent_first_use_creates_one_partition() -> None:
    gate = PartitionedBulkhead(
        label="shared-key",
        parallelism=20,
        max_partitions=5,
    )

    await asyncio.gather(*(gate.execute("same", asyncio.sleep, 0) for _ in range(20)))

    status = await gate.status()
    assert status.partition_count == 1
    assert status.created_total == 1
    assert status.peak_leased_operations >= 1


async def test_limit_rejects_new_key_when_every_partition_is_active() -> None:
    gate = PartitionedBulkhead(
        label="bounded-keys",
        parallelism=1,
        max_partitions=2,
    )
    release = asyncio.Event()

    async def hold() -> None:
        await release.wait()

    active = [asyncio.create_task(gate.execute(key, hold)) for key in ("alpha", "beta")]
    await eventually(lambda: has_leased_operations(gate, 2))

    with pytest.raises(PartitionLimitError) as captured:
        await gate.execute("secret-customer-id", asyncio.sleep, 0)

    error = captured.value
    assert error.label == "bounded-keys"
    assert error.max_partitions == 2
    assert error.active_partitions == 2
    assert "secret-customer-id" not in str(error)
    assert (await gate.status()).limit_rejected_total == 1

    release.set()
    await asyncio.gather(*active)


async def test_capacity_pressure_evicts_least_recent_idle_partition() -> None:
    gate = PartitionedBulkhead(
        label="lru",
        parallelism=1,
        max_partitions=2,
        idle_timeout=60.0,
    )

    await gate.execute("alpha", asyncio.sleep, 0)
    await asyncio.sleep(0)
    await gate.execute("beta", asyncio.sleep, 0)
    await asyncio.sleep(0)
    await gate.execute("beta", asyncio.sleep, 0)
    await gate.execute("gamma", asyncio.sleep, 0)

    status = await gate.status()
    assert status.partition_count == 2
    assert status.created_total == 3
    assert status.evicted_total == 1
    assert not await gate.discard("alpha")
    assert await gate.discard("beta")


async def test_cleanup_removes_only_partitions_past_idle_timeout() -> None:
    gate = PartitionedBulkhead(
        label="cleanup",
        parallelism=1,
        max_partitions=4,
        idle_timeout=0.01,
    )

    await gate.execute("old", asyncio.sleep, 0)
    await asyncio.sleep(0.02)
    await gate.execute("new", asyncio.sleep, 0)

    assert await gate.cleanup_idle() == 1
    status = await gate.status()
    assert status.partition_count == 1
    assert status.evicted_total == 1


async def test_discard_refuses_busy_partition_and_removes_it_after_release() -> None:
    gate = PartitionedBulkhead(
        label="discard",
        parallelism=1,
        max_partitions=2,
    )
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    assert not await gate.discard("alpha")
    release.set()
    await active
    assert await gate.discard("alpha")
    assert not await gate.discard("alpha")
    assert (await gate.status()).discarded_total == 1


async def test_cancelled_waiter_releases_manager_reference() -> None:
    gate = PartitionedBulkhead(
        label="cancelled",
        parallelism=1,
        max_partitions=1,
        waiting_room=1,
    )
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    waiting = asyncio.create_task(gate.execute("alpha", asyncio.sleep, 0))
    await eventually(lambda: has_leased_operations(gate, 2))
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert (await gate.status()).leased_operations == 1
    release.set()
    await active
    assert (await gate.status()).leased_operations == 0


async def test_close_rejects_new_partitions_and_drains_existing_work() -> None:
    gate = PartitionedBulkhead(
        label="shutdown",
        parallelism=1,
        max_partitions=2,
    )
    release = asyncio.Event()

    active = asyncio.create_task(gate.execute("alpha", release.wait))
    await eventually(lambda: has_leased_operations(gate, 1))

    await gate.close()
    with pytest.raises(BulkheadClosedError):
        await gate.execute("beta", asyncio.sleep, 0)

    waiter = asyncio.create_task(gate.wait_closed())
    await asyncio.sleep(0)
    assert not waiter.done()
    release.set()
    await asyncio.gather(active, waiter)
    closed = await gate.status()
    assert closed.is_closed
    assert closed.partition_count == 0


async def test_partition_key_must_be_hashable() -> None:
    gate = PartitionedBulkhead(label="keys", parallelism=1, max_partitions=2)

    with pytest.raises(TypeError, match="hashable"):
        gate.slot([])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_partitions": 0}, "max_partitions"),
        ({"max_partitions": True}, "max_partitions"),
        ({"idle_timeout": 0.0}, "idle_timeout"),
        ({"idle_timeout": float("inf")}, "idle_timeout"),
    ],
)
def test_partitioned_configuration_validation(
    kwargs: dict[str, object],
    message: str,
) -> None:
    values: dict[str, object] = {
        "label": "validation",
        "parallelism": 1,
        "max_partitions": 2,
    }
    values.update(kwargs)

    with pytest.raises(ValueError, match=message):
        PartitionedBulkhead(**values)  # type: ignore[arg-type]


async def has_leased_operations(gate: PartitionedBulkhead, expected: int) -> bool:
    return (await gate.status()).leased_operations == expected
