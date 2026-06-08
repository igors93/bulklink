from __future__ import annotations

import random

import pytest

from bulklink import BulkheadRegistry


@pytest.mark.model
@pytest.mark.parametrize("seed", range(16))
async def test_generated_registry_membership_and_shutdown_sequences(seed: int) -> None:
    randomizer = random.Random(seed)
    registry = BulkheadRegistry()
    expected: list[str] = []
    next_label = 0

    for _ in range(36):
        action = randomizer.randrange(5)

        if action == 0 and not registry.is_closed:
            label = f"member-{next_label}"
            next_label += 1
            registry.create(label, parallelism=randomizer.randint(1, 4))
            expected.append(label)
        elif action == 1 and expected:
            index = randomizer.randrange(len(expected))
            label = expected.pop(index)
            removed = await registry.remove(label)
            assert removed.label == label
        elif action == 2:
            statuses = await registry.statuses()
            assert [status.label for status in statuses] == expected
        elif action == 3:
            reports = await registry.capacity_reports()
            assert [report.status.label for report in reports] == expected
        elif not registry.is_closed:
            await registry.close_all()

        assert registry.labels == tuple(expected)
        assert len(registry) == len(expected)

    await registry.close_and_wait()
    statuses = await registry.statuses()
    assert registry.is_closed
    assert [status.label for status in statuses] == expected
    assert all(status.is_drained for status in statuses)

    with pytest.raises(RuntimeError, match="registry is closed"):
        registry.create("late", parallelism=1)
