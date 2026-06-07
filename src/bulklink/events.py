"""Immutable observability events emitted by Bulklink."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class BulkheadEventKind(str, Enum):
    """Stable categories of bulkhead lifecycle events."""

    ADMITTED = "admitted"
    QUEUED = "queued"
    SATURATED = "saturated"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    ABANDONED = "abandoned"
    RELEASED = "released"
    CLOSED = "closed"
    CLOSED_REJECTION = "closed_rejection"
    DRAINED = "drained"


@dataclass(frozen=True, slots=True)
class BulkheadEvent:
    """Read-only event containing only bulkhead state and timing metadata."""

    kind: BulkheadEventKind
    label: str
    occurred_at: float
    parallelism: int
    waiting_room: int
    in_flight: int
    waiting: int
    is_closed: bool
    from_queue: bool = False
    waited_seconds: float | None = None
    affected_waiters: int = 0


BulkheadEventHandler = Callable[[BulkheadEvent], None]
