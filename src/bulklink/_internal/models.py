"""Private queue and counter models."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum, auto


class WaitState(Enum):
    """Lifecycle state of one FIFO waiting-room entry."""

    WAITING = auto()
    ADMITTED = auto()
    CANCELLED = auto()
    EXPIRED = auto()
    CLOSED = auto()

    @property
    def is_terminal(self) -> bool:
        """Return True after the entry has left the waiting state."""
        return self is not WaitState.WAITING


@dataclass(slots=True, eq=False)
class WaitEntry:
    """One FIFO waiting-room entry with one irreversible terminal state."""

    future: asyncio.Future[WaitState]
    enqueued_at: float
    state: WaitState = WaitState.WAITING

    def transition_to(self, state: WaitState) -> bool:
        """Move from waiting to one terminal state exactly once."""
        if state is WaitState.WAITING:
            raise ValueError("a wait entry cannot transition back to WAITING")
        if self.state is not WaitState.WAITING:
            return False
        self.state = state
        return True


@dataclass(slots=True)
class RuntimeCounters:
    """Mutable counters protected by the coordinator lock."""

    admitted_total: int = 0
    admitted_from_queue_total: int = 0
    abandoned_after_admission_total: int = 0
    queued_total: int = 0
    saturated_total: int = 0
    expired_total: int = 0
    cancelled_while_waiting_total: int = 0
    closed_before_queue_total: int = 0
    closed_while_waiting_total: int = 0
    finished_total: int = 0
    peak_in_flight: int = 0
    peak_waiting: int = 0
    cumulative_wait_seconds: float = 0.0
    longest_wait_seconds: float = 0.0
