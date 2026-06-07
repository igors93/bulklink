"""Private queue and counter models."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum, auto


class WakeReason(Enum):
    """Reason a queued operation was awakened."""

    ADMITTED = auto()
    CLOSED = auto()


@dataclass(slots=True, eq=False)
class WaitNode:
    """One FIFO waiting-room entry."""

    future: asyncio.Future[WakeReason]
    enqueued_at: float
    granted: bool = False


@dataclass(slots=True)
class RuntimeCounters:
    """Mutable counters protected by the coordinator lock."""

    admitted_total: int = 0
    admitted_from_queue_total: int = 0
    queued_total: int = 0
    saturated_total: int = 0
    expired_total: int = 0
    cancelled_total: int = 0
    closed_total: int = 0
    finished_total: int = 0
    peak_in_flight: int = 0
    peak_waiting: int = 0
    cumulative_wait_seconds: float = 0.0
    longest_wait_seconds: float = 0.0
