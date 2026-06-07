"""Cancellation-safe FIFO admission coordination."""

from __future__ import annotations

import asyncio
from collections import deque
from time import monotonic
from typing import Final, Literal

from bulklink._internal.models import RuntimeCounters, WaitNode, WakeReason
from bulklink._internal.validation import (
    require_label,
    require_non_negative_integer,
    require_optional_positive_number,
    require_positive_integer,
)
from bulklink.errors import (
    BulkheadClosedError,
    BulkheadQueueTimeoutError,
    BulkheadSaturatedError,
)
from bulklink.status import BulkheadStatus

WaitOutcome = Literal["cancelled", "expired"]


class AdmissionCoordinator:
    """Own all mutable state and synchronization for one async bulkhead."""

    _CANCELLED: Final[WaitOutcome] = "cancelled"
    _EXPIRED: Final[WaitOutcome] = "expired"

    def __init__(
        self,
        *,
        label: str,
        parallelism: int,
        waiting_room: int,
        wait_limit: float | None,
    ) -> None:
        self._label = require_label(label)
        self._parallelism = require_positive_integer("parallelism", parallelism)
        self._waiting_room = require_non_negative_integer("waiting_room", waiting_room)
        self._wait_limit = require_optional_positive_number("wait_limit", wait_limit)

        self._mutex = asyncio.Lock()
        self._waiters: deque[WaitNode] = deque()
        self._in_flight = 0
        self._closed = False
        self._counters = RuntimeCounters()
        self._owner_loop: asyncio.AbstractEventLoop | None = None

    @property
    def label(self) -> str:
        return self._label

    @property
    def parallelism(self) -> int:
        return self._parallelism

    @property
    def waiting_room(self) -> int:
        return self._waiting_room

    @property
    def wait_limit(self) -> float | None:
        return self._wait_limit

    def _bind_to_running_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = loop
        elif self._owner_loop is not loop:
            raise RuntimeError(
                f"bulkhead {self._label!r} cannot be shared across different event loops"
            )
        return loop

    async def enter(self) -> None:
        """Admit immediately, queue, or reject one operation."""
        loop = self._bind_to_running_loop()

        async with self._mutex:
            if self._closed:
                self._counters.closed_total += 1
                raise BulkheadClosedError(label=self._label)

            if self._in_flight < self._parallelism and not self._waiters:
                self._grant_directly()
                return

            if len(self._waiters) >= self._waiting_room:
                self._counters.saturated_total += 1
                raise BulkheadSaturatedError(
                    label=self._label,
                    in_flight=self._in_flight,
                    waiting=len(self._waiters),
                    parallelism=self._parallelism,
                    waiting_room=self._waiting_room,
                )

            node = WaitNode(future=loop.create_future(), enqueued_at=monotonic())
            self._waiters.append(node)
            self._counters.queued_total += 1
            self._counters.peak_waiting = max(
                self._counters.peak_waiting,
                len(self._waiters),
            )

        try:
            reason = await self._await_wakeup(node)
        except asyncio.TimeoutError as error:
            had_slot = await self._withdraw(node, outcome=self._EXPIRED)
            if had_slot:
                await self.release(mark_finished=False)
            assert self._wait_limit is not None
            raise BulkheadQueueTimeoutError(
                label=self._label,
                wait_limit=self._wait_limit,
            ) from error
        except asyncio.CancelledError:
            had_slot = await self._withdraw(node, outcome=self._CANCELLED)
            if had_slot:
                await self.release(mark_finished=False)
            raise

        if reason is WakeReason.CLOSED:
            raise BulkheadClosedError(label=self._label)

    async def _await_wakeup(self, node: WaitNode) -> WakeReason:
        if self._wait_limit is None:
            return await asyncio.shield(node.future)
        return await asyncio.wait_for(
            asyncio.shield(node.future),
            timeout=self._wait_limit,
        )

    def _grant_directly(self) -> None:
        self._in_flight += 1
        self._counters.admitted_total += 1
        self._counters.peak_in_flight = max(
            self._counters.peak_in_flight,
            self._in_flight,
        )

    async def _withdraw(self, node: WaitNode, *, outcome: WaitOutcome) -> bool:
        """Remove a waiter or report that a slot was already transferred to it."""
        async with self._mutex:
            if outcome == self._EXPIRED:
                self._counters.expired_total += 1
            else:
                self._counters.cancelled_total += 1

            if node.granted:
                return True

            try:
                self._waiters.remove(node)
            except ValueError:
                return node.granted

            if not node.future.done():
                node.future.cancel()
            return False

    async def release(self, *, mark_finished: bool = True) -> None:
        """Release or transfer one previously granted execution slot."""
        self._bind_to_running_loop()

        async with self._mutex:
            if self._in_flight <= 0:
                raise RuntimeError("execution slot released without a matching admission")

            if mark_finished:
                self._counters.finished_total += 1

            while self._waiters:
                node = self._waiters.popleft()
                if node.future.cancelled() or node.future.done():
                    continue

                node.granted = True
                waited = max(0.0, monotonic() - node.enqueued_at)
                self._counters.admitted_total += 1
                self._counters.admitted_from_queue_total += 1
                self._counters.cumulative_wait_seconds += waited
                self._counters.longest_wait_seconds = max(
                    self._counters.longest_wait_seconds,
                    waited,
                )
                node.future.set_result(WakeReason.ADMITTED)
                # Direct transfer keeps _in_flight unchanged.
                return

            self._in_flight -= 1

    async def close(self) -> None:
        """Close admission and wake queued operations with a closed signal."""
        self._bind_to_running_loop()

        async with self._mutex:
            if self._closed:
                return

            self._closed = True
            pending = tuple(self._waiters)
            self._waiters.clear()
            self._counters.closed_total += len(pending)

            for node in pending:
                if not node.future.done():
                    node.future.set_result(WakeReason.CLOSED)

    async def status(self) -> BulkheadStatus:
        """Build an immutable status report under the coordinator lock."""
        self._bind_to_running_loop()

        async with self._mutex:
            counters = self._counters
            return BulkheadStatus(
                label=self._label,
                parallelism=self._parallelism,
                waiting_room=self._waiting_room,
                in_flight=self._in_flight,
                waiting=len(self._waiters),
                admitted_total=counters.admitted_total,
                admitted_from_queue_total=counters.admitted_from_queue_total,
                queued_total=counters.queued_total,
                saturated_total=counters.saturated_total,
                expired_total=counters.expired_total,
                cancelled_total=counters.cancelled_total,
                closed_total=counters.closed_total,
                finished_total=counters.finished_total,
                peak_in_flight=counters.peak_in_flight,
                peak_waiting=counters.peak_waiting,
                cumulative_wait_seconds=counters.cumulative_wait_seconds,
                longest_wait_seconds=counters.longest_wait_seconds,
                is_closed=self._closed,
            )
