"""Named ownership and collective lifecycle management for bulkheads."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from threading import RLock
from typing import Any, TypeVar

from bulklink._internal.cancellation import complete_cleanup
from bulklink._internal.validation import require_label
from bulklink.bulkhead import AsyncBulkhead
from bulklink.capacity import CapacityReport
from bulklink.errors import BulklinkError
from bulklink.status import BulkheadStatus

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class BulkheadRegistryFailure:
    """One bounded failure produced by a collective registry operation."""

    label: str
    error_type: str
    message: str


class BulkheadRegistryOperationError(BulklinkError):
    """Raised after every target was attempted and one or more operations failed."""

    def __init__(
        self,
        *,
        operation: str,
        failures: tuple[BulkheadRegistryFailure, ...],
    ) -> None:
        self.operation = operation
        self.failures = failures
        labels = ", ".join(failure.label for failure in failures)
        super().__init__(f"registry operation {operation!r} failed for: {labels}")


class BulkheadRegistry:
    """Own uniquely named bulkheads and coordinate their lifecycle as one group."""

    def __init__(self) -> None:
        self._mutex = RLock()
        self._bulkheads: dict[str, AsyncBulkhead] = {}
        self._closed = False

    @property
    def is_closed(self) -> bool:
        """Return True after collective shutdown has started."""
        with self._mutex:
            return self._closed

    @property
    def labels(self) -> tuple[str, ...]:
        """Return registered labels in creation order."""
        with self._mutex:
            return tuple(self._bulkheads)

    def __len__(self) -> int:
        with self._mutex:
            return len(self._bulkheads)

    def __contains__(self, label: object) -> bool:
        if not isinstance(label, str) or not label.strip():
            return False
        normalized = label.strip()
        with self._mutex:
            return normalized in self._bulkheads

    def create(
        self,
        label: str,
        *,
        parallelism: int,
        waiting_room: int = 0,
        wait_limit: float | None = None,
    ) -> AsyncBulkhead:
        """Create and register one uniquely named bulkhead."""
        normalized = require_label(label)

        with self._mutex:
            if self._closed:
                raise RuntimeError("bulkhead registry is closed")
            if normalized in self._bulkheads:
                raise ValueError(f"bulkhead {normalized!r} is already registered")

            bulkhead = AsyncBulkhead(
                label=normalized,
                parallelism=parallelism,
                waiting_room=waiting_room,
                wait_limit=wait_limit,
            )
            self._bulkheads[normalized] = bulkhead
            return bulkhead

    def get(self, label: str) -> AsyncBulkhead:
        """Return one registered bulkhead by normalized label."""
        normalized = require_label(label)
        with self._mutex:
            try:
                return self._bulkheads[normalized]
            except KeyError:
                raise KeyError(f"bulkhead {normalized!r} is not registered") from None

    async def remove(self, label: str) -> AsyncBulkhead:
        """Close, drain, and remove one bulkhead from the registry."""
        normalized = require_label(label)
        return await complete_cleanup(self._remove(normalized))

    async def statuses(self) -> tuple[BulkheadStatus, ...]:
        """Return immutable status snapshots in registry creation order."""
        return await _run_collective(
            "statuses",
            self._items_snapshot(),
            lambda bulkhead: bulkhead.status(),
        )

    async def capacity_reports(self) -> tuple[CapacityReport, ...]:
        """Return immutable capacity reports in registry creation order."""
        return await _run_collective(
            "capacity_reports",
            self._items_snapshot(),
            lambda bulkhead: bulkhead.capacity_report(),
        )

    async def close_all(self) -> None:
        """Close every registered bulkhead without waiting for active work."""
        await complete_cleanup(self._close_all())

    async def wait_closed(self) -> None:
        """Wait for every bulkhead after collective shutdown has started."""
        items = self._closed_items_snapshot()
        await _run_collective(
            "wait_closed",
            items,
            lambda bulkhead: bulkhead.wait_closed(),
        )

    async def close_and_wait(self) -> None:
        """Close and drain every registered bulkhead despite caller cancellation."""
        await complete_cleanup(self._close_and_wait())

    async def _remove(self, label: str) -> AsyncBulkhead:
        bulkhead = self.get(label)
        await bulkhead.close_and_wait()

        with self._mutex:
            if self._bulkheads.get(label) is not bulkhead:
                raise KeyError(f"bulkhead {label!r} is not registered")
            del self._bulkheads[label]

        return bulkhead

    async def _close_all(self) -> None:
        await _run_collective(
            "close_all",
            self._begin_shutdown(),
            lambda bulkhead: bulkhead.close(),
        )

    async def _close_and_wait(self) -> None:
        await _run_collective(
            "close_and_wait",
            self._begin_shutdown(),
            lambda bulkhead: bulkhead.close_and_wait(),
        )

    def _items_snapshot(self) -> tuple[tuple[str, AsyncBulkhead], ...]:
        with self._mutex:
            return tuple(self._bulkheads.items())

    def _begin_shutdown(self) -> tuple[tuple[str, AsyncBulkhead], ...]:
        with self._mutex:
            self._closed = True
            return tuple(self._bulkheads.items())

    def _closed_items_snapshot(self) -> tuple[tuple[str, AsyncBulkhead], ...]:
        with self._mutex:
            if not self._closed:
                raise RuntimeError("close_all() must be called before wait_closed()")
            return tuple(self._bulkheads.items())


async def _run_collective(
    operation: str,
    items: tuple[tuple[str, AsyncBulkhead], ...],
    action: Callable[[AsyncBulkhead], Coroutine[Any, Any, T]],
) -> tuple[T, ...]:
    if not items:
        return ()

    raw_results = await asyncio.gather(
        *(action(bulkhead) for _, bulkhead in items),
        return_exceptions=True,
    )
    values: list[T] = []
    failures: list[BulkheadRegistryFailure] = []
    first_error: BaseException | None = None

    for (label, _), result in zip(items, raw_results, strict=True):
        if isinstance(result, BaseException):
            if first_error is None:
                first_error = result
            failures.append(
                BulkheadRegistryFailure(
                    label=label,
                    error_type=type(result).__name__,
                    message=str(result),
                )
            )
        else:
            values.append(result)

    if failures:
        error = BulkheadRegistryOperationError(
            operation=operation,
            failures=tuple(failures),
        )
        raise error from first_error

    return tuple(values)
