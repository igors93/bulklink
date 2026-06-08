"""Private queue and counter models for weighted admission."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from bulklink._internal.models import WaitState


@dataclass(slots=True, eq=False)
class WeightedWaitEntry:
    """One FIFO weighted waiting-room entry."""

    future: asyncio.Future[WaitState]
    enqueued_at: float
    cost: int
    state: WaitState = WaitState.WAITING
    waited_seconds: float | None = None

    def transition_to(self, state: WaitState) -> bool:
        """Move from waiting to one terminal state exactly once."""
        if state is WaitState.WAITING:
            raise ValueError("a wait entry cannot transition back to WAITING")
        if self.state is not WaitState.WAITING:
            return False
        self.state = state
        return True


@dataclass(slots=True)
class WeightedRuntimeCounters:
    """Mutable weighted counters protected by the coordinator lock."""

    admitted_total: int = 0
    admitted_units_total: int = 0
    admitted_from_queue_total: int = 0
    admitted_from_queue_units_total: int = 0
    abandoned_after_admission_total: int = 0
    abandoned_units_total: int = 0
    queued_total: int = 0
    queued_units_total: int = 0
    saturated_total: int = 0
    expired_total: int = 0
    expired_before_queue_total: int = 0
    cancelled_while_waiting_total: int = 0
    closed_before_queue_total: int = 0
    closed_while_waiting_total: int = 0
    finished_total: int = 0
    finished_units_total: int = 0
    peak_used: int = 0
    peak_in_flight: int = 0
    peak_waiting: int = 0
    peak_waiting_units: int = 0
    cumulative_wait_seconds: float = 0.0
    longest_wait_seconds: float = 0.0
