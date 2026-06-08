"""Immutable observability events for weighted bulkheads."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from bulklink.events import BulkheadEventKind


@dataclass(frozen=True, slots=True)
class WeightedBulkheadEvent:
    """Read-only weighted event containing capacity and timing metadata only."""

    kind: BulkheadEventKind
    label: str
    occurred_at: float
    capacity: int
    waiting_room: int
    used: int
    in_flight: int
    waiting: int
    is_closed: bool
    cost: int | None = None
    from_queue: bool = False
    waited_seconds: float | None = None
    affected_waiters: int = 0
    previous_capacity: int | None = None


WeightedBulkheadEventHandler = Callable[[WeightedBulkheadEvent], None]
