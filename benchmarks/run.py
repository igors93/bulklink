"""Run dependency-free microbenchmarks without enforcing timing thresholds."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import tracemalloc
from collections.abc import Awaitable, Callable
from pathlib import Path
from time import perf_counter_ns
from typing import Any

from bulklink import AsyncBulkhead, PartitionedBulkhead, WeightedBulkhead

Scenario = Callable[[int], Awaitable[None]]


async def _noop() -> None:
    return None


async def _direct(iterations: int) -> None:
    for _ in range(iterations):
        await _noop()


async def _execute(iterations: int) -> None:
    gate = AsyncBulkhead(label="benchmark-execute", parallelism=1)
    for _ in range(iterations):
        await gate.execute(_noop)
    await gate.close_and_wait()


async def _slot(iterations: int) -> None:
    gate = AsyncBulkhead(label="benchmark-slot", parallelism=1)
    for _ in range(iterations):
        async with gate.slot():
            pass
    await gate.close_and_wait()


async def _weighted_execute(iterations: int) -> None:
    gate = WeightedBulkhead(label="benchmark-weighted-execute", capacity=4)
    for index in range(iterations):
        await gate.execute((index % 4) + 1, _noop)
    await gate.close_and_wait()


async def _partitioned_execute(iterations: int) -> None:
    gate = PartitionedBulkhead(
        label="benchmark-partitioned-execute",
        parallelism=4,
        max_partitions=16,
    )
    for index in range(iterations):
        await gate.execute(index % 16, _noop)
    await gate.close_and_wait()


async def _events(iterations: int) -> None:
    gate = AsyncBulkhead(label="benchmark-events", parallelism=1)
    seen = 0

    def observe(_: object) -> None:
        nonlocal seen
        seen += 1

    gate.add_event_handler(observe)
    for _ in range(iterations):
        await gate.execute(_noop)
    if seen != iterations * 2:
        raise RuntimeError("event benchmark observed an unexpected event count")
    await gate.close_and_wait()


async def _status(iterations: int) -> None:
    gate = AsyncBulkhead(label="benchmark-status", parallelism=4, waiting_room=4)
    for _ in range(iterations):
        await gate.status()
    await gate.close_and_wait()


async def _handoff(iterations: int) -> None:
    gate = AsyncBulkhead(
        label="benchmark-handoff",
        parallelism=min(32, max(1, iterations)),
        waiting_room=max(0, iterations),
    )

    async def yield_once() -> None:
        await asyncio.sleep(0)

    await asyncio.gather(*(gate.execute(yield_once) for _ in range(iterations)))
    await gate.close_and_wait()


SCENARIOS: dict[str, Scenario] = {
    "direct": _direct,
    "execute": _execute,
    "slot": _slot,
    "weighted_execute": _weighted_execute,
    "partitioned_execute": _partitioned_execute,
    "events": _events,
    "status": _status,
    "handoff": _handoff,
}


async def _measure(scenario: Scenario, iterations: int, rounds: int) -> dict[str, float]:
    samples: list[float] = []
    for _ in range(rounds):
        started = perf_counter_ns()
        await scenario(iterations)
        elapsed = perf_counter_ns() - started
        samples.append(elapsed / iterations)

    return {
        "median_ns_per_operation": statistics.median(samples),
        "minimum_ns_per_operation": min(samples),
        "maximum_ns_per_operation": max(samples),
    }


async def _measure_waiter_memory(waiters: int) -> dict[str, int]:
    gate = AsyncBulkhead(
        label="benchmark-waiter-memory",
        parallelism=1,
        waiting_room=waiters,
    )
    release = asyncio.Event()

    async def hold() -> None:
        await release.wait()

    active = asyncio.create_task(gate.execute(hold))
    await _wait_for_state(gate, in_flight=1, waiting=0)

    tracemalloc.start()
    before_current, _ = tracemalloc.get_traced_memory()
    queued = [asyncio.create_task(gate.execute(_noop)) for _ in range(waiters)]
    await _wait_for_state(gate, in_flight=1, waiting=waiters)
    after_current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    for task in queued:
        task.cancel()
    await asyncio.gather(*queued, return_exceptions=True)
    release.set()
    await active
    await gate.close_and_wait()

    allocated = max(0, after_current - before_current)
    return {
        "waiters": waiters,
        "allocated_bytes": allocated,
        "peak_bytes": max(0, peak - before_current),
        "approximate_bytes_per_waiter": allocated // max(1, waiters),
    }


async def _wait_for_state(
    gate: AsyncBulkhead,
    *,
    in_flight: int,
    waiting: int,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5.0
    while True:
        status = await gate.status()
        if status.in_flight == in_flight and status.waiting == waiting:
            return
        if loop.time() >= deadline:
            raise RuntimeError("benchmark state was not reached before timeout")
        await asyncio.sleep(0)


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=_positive_integer, default=5_000)
    parser.add_argument("--rounds", type=_positive_integer, default=5)
    parser.add_argument("--waiters", type=_positive_integer, default=1_000)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


async def _run(iterations: int, rounds: int, waiters: int) -> dict[str, Any]:
    results: dict[str, Any] = {
        "iterations": iterations,
        "rounds": rounds,
        "scenarios": {},
    }
    scenarios: dict[str, dict[str, float]] = results["scenarios"]

    for name, scenario in SCENARIOS.items():
        scenarios[name] = await _measure(scenario, iterations, rounds)

    results["waiter_memory"] = await _measure_waiter_memory(waiters)
    return results


def main() -> None:
    arguments = _parse_args()
    results = asyncio.run(
        _run(
            iterations=arguments.iterations,
            rounds=arguments.rounds,
            waiters=arguments.waiters,
        )
    )
    rendered = json.dumps(results, indent=2, sort_keys=True)
    print(rendered)
    if arguments.output is not None:
        arguments.output.write_text(f"{rendered}\n", encoding="utf-8")


if __name__ == "__main__":
    main()
