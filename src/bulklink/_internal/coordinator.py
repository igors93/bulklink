"""Cancellation-safe FIFO admission coordination."""

from __future__ import annotations

import asyncio
from collections import deque
from time import monotonic
from typing import NoReturn

from bulklink._internal.cancellation import complete_cleanup
from bulklink._internal.models import RuntimeCounters, WaitEntry, WaitState
from bulklink._internal.validation import (
    require_label,
    require_non_negative_integer,
    require_optional_positive_number,
    require_positive_integer,
    resolve_wait_limit,
)
from bulklink.errors import (
    BulkheadClosedError,
    BulkheadQueueTimeoutError,
    BulkheadSaturatedError,
)
from bulklink.status import BulkheadStatus


class AdmissionCoordinator:
    """Own all mutable state and synchronization for one async bulkhead."""

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
        self._waiters: deque[WaitEntry] = deque()
        self._in_flight = 0
        self._closed = False
        self._counters = RuntimeCounters()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._drained_event: asyncio.Event | None = None

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

    def effective_wait_limit(self, requested: float) -> float:
        """Return the shortest limit allowed for one queued admission."""
        return resolve_wait_limit(self._wait_limit, requested)

    def _bind_to_running_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = loop
            self._drained_event = asyncio.Event()
        elif self._owner_loop is not loop:
            raise RuntimeError(
                f"bulkhead {self._label!r} cannot be shared across different event loops"
            )
        return loop

    async def enter(self) -> None:
        """Admit immediately, queue, or reject using the configured wait limit."""
        await self._enter(self._wait_limit)

    async def enter_within(self, wait_limit: float) -> None:
        """Admit using a per-call wait limit no longer than the configured limit."""
        await self._enter(self.effective_wait_limit(wait_limit))

    async def _enter(self, wait_limit: float | None) -> None:
        loop = self._bind_to_running_loop()

        async with self._mutex:
            self._ensure_open_locked()

            if self._can_admit_directly_locked():
                self._grant_directly()
                return

            if len(self._waiters) >= self._waiting_room:
                self._raise_saturated_locked()

            entry = WaitEntry(
                future=loop.create_future(),
                enqueued_at=monotonic(),
            )
            self._waiters.append(entry)
            self._counters.queued_total += 1
            self._counters.peak_waiting = max(
                self._counters.peak_waiting,
                len(self._waiters),
            )

        try:
            state = await self._await_terminal_state(entry, wait_limit)
        except asyncio.TimeoutError as error:
            try:
                state = await complete_cleanup(self._expire_waiter(entry))
            except asyncio.CancelledError:
                await complete_cleanup(self._cancel_waiter(entry))
                raise

            if state is WaitState.ADMITTED:
                return
            if state is WaitState.CLOSED:
                raise BulkheadClosedError(label=self._label) from error
            if state is not WaitState.EXPIRED:
                raise RuntimeError(f"unexpected wait state after timeout: {state.name}") from error

            if wait_limit is None:
                raise RuntimeError("a queued admission expired without a wait limit") from error
            raise BulkheadQueueTimeoutError(
                label=self._label,
                wait_limit=wait_limit,
            ) from error
        except asyncio.CancelledError:
            await complete_cleanup(self._cancel_waiter(entry))
            raise

        if state is WaitState.ADMITTED:
            return
        if state is WaitState.CLOSED:
            raise BulkheadClosedError(label=self._label)
        if state is WaitState.EXPIRED:
            if wait_limit is None:
                raise RuntimeError("a queued admission expired without a wait limit")
            raise BulkheadQueueTimeoutError(
                label=self._label,
                wait_limit=wait_limit,
            )
        if state is WaitState.CANCELLED:
            raise asyncio.CancelledError

        raise RuntimeError(f"unexpected terminal wait state: {state.name}")

    async def enter_now(self) -> None:
        """Admit only when capacity is available without joining the waiting room."""
        self._bind_to_running_loop()

        async with self._mutex:
            self._ensure_open_locked()

            if self._can_admit_directly_locked():
                self._grant_directly()
                return

            self._raise_saturated_locked()

    def _ensure_open_locked(self) -> None:
        if self._closed:
            self._counters.closed_before_queue_total += 1
            raise BulkheadClosedError(label=self._label)

    def _can_admit_directly_locked(self) -> bool:
        return self._in_flight < self._parallelism and not self._waiters

    def _raise_saturated_locked(self) -> NoReturn:
        self._counters.saturated_total += 1
        raise BulkheadSaturatedError(
            label=self._label,
            in_flight=self._in_flight,
            waiting=len(self._waiters),
            parallelism=self._parallelism,
            waiting_room=self._waiting_room,
        )

    async def _await_terminal_state(
        self,
        entry: WaitEntry,
        wait_limit: float | None,
    ) -> WaitState:
        if wait_limit is None:
            return await asyncio.shield(entry.future)
        return await asyncio.wait_for(
            asyncio.shield(entry.future),
            timeout=wait_limit,
        )

    def _grant_directly(self) -> None:
        self._in_flight += 1
        self._counters.admitted_total += 1
        self._counters.peak_in_flight = max(
            self._counters.peak_in_flight,
            self._in_flight,
        )

    async def _expire_waiter(self, entry: WaitEntry) -> WaitState:
        """Expire one entry if it is still waiting."""
        async with self._mutex:
            if entry.state is not WaitState.WAITING:
                return entry.state
            return self._finish_waiter_locked(
                entry,
                WaitState.EXPIRED,
                remove_from_queue=True,
            )

    async def _cancel_waiter(self, entry: WaitEntry) -> WaitState:
        """Cancel waiting or return a slot already transferred to this entry."""
        async with self._mutex:
            if entry.state is WaitState.WAITING:
                return self._finish_waiter_locked(
                    entry,
                    WaitState.CANCELLED,
                    remove_from_queue=True,
                )

            if entry.state is WaitState.ADMITTED:
                self._abandon_admitted_slot_locked()

            return entry.state

    def _finish_waiter_locked(
        self,
        entry: WaitEntry,
        state: WaitState,
        *,
        remove_from_queue: bool,
    ) -> WaitState:
        """Move one waiting entry to a terminal state under the coordinator lock."""
        if state is WaitState.WAITING:
            raise ValueError("a waiting entry requires a terminal state")
        if entry.state is not WaitState.WAITING:
            raise RuntimeError("only waiting entries can be completed")

        if remove_from_queue:
            self._remove_waiter_locked(entry)

        if not entry.transition_to(state):
            raise RuntimeError("waiting entry could not transition to a terminal state")

        if state is WaitState.ADMITTED:
            waited = max(0.0, monotonic() - entry.enqueued_at)
            self._counters.admitted_total += 1
            self._counters.admitted_from_queue_total += 1
            self._counters.cumulative_wait_seconds += waited
            self._counters.longest_wait_seconds = max(
                self._counters.longest_wait_seconds,
                waited,
            )
            entry.future.set_result(WaitState.ADMITTED)
        elif state is WaitState.CANCELLED:
            self._counters.cancelled_while_waiting_total += 1
            entry.future.cancel()
        elif state is WaitState.EXPIRED:
            self._counters.expired_total += 1
            entry.future.set_result(WaitState.EXPIRED)
        elif state is WaitState.CLOSED:
            self._counters.closed_while_waiting_total += 1
            entry.future.set_result(WaitState.CLOSED)
        else:
            raise RuntimeError(f"unsupported terminal wait state: {state.name}")

        return state

    def _remove_waiter_locked(self, entry: WaitEntry) -> None:
        try:
            self._waiters.remove(entry)
        except ValueError as error:
            raise RuntimeError("waiting entry is missing from the FIFO queue") from error

    async def release(self) -> None:
        """Finish one protected operation and release or transfer its slot."""
        self._bind_to_running_loop()

        async with self._mutex:
            self._finish_admitted_slot_locked()

    def _finish_admitted_slot_locked(self) -> None:
        if self._in_flight <= 0:
            raise RuntimeError("execution slot released without a matching admission")
        self._counters.finished_total += 1
        self._release_capacity_locked()

    def _abandon_admitted_slot_locked(self) -> None:
        if self._in_flight <= 0:
            raise RuntimeError("admitted slot abandoned without allocated capacity")
        self._counters.abandoned_after_admission_total += 1
        self._release_capacity_locked()

    def _release_capacity_locked(self) -> None:
        if self._waiters:
            entry = self._waiters.popleft()
            if entry.future.done():
                raise RuntimeError("waiting entry future completed before admission")
            self._finish_waiter_locked(
                entry,
                WaitState.ADMITTED,
                remove_from_queue=False,
            )
            # Direct transfer keeps _in_flight unchanged.
            return

        self._in_flight -= 1
        self._signal_drained_if_ready_locked()

    def _drain_signal(self) -> asyncio.Event:
        event = self._drained_event
        if event is None:
            raise RuntimeError("bulkhead drain signal is unavailable before event-loop binding")
        return event

    def _signal_drained_if_ready_locked(self) -> None:
        if not self._closed or self._in_flight != 0:
            return
        if self._waiters:
            raise RuntimeError("a closed and drained bulkhead cannot retain queued entries")
        self._drain_signal().set()

    async def close(self) -> None:
        """Close admission and wake queued operations with a closed state."""
        self._bind_to_running_loop()

        async with self._mutex:
            if self._closed:
                return

            self._closed = True

            while self._waiters:
                entry = self._waiters.popleft()
                self._finish_waiter_locked(
                    entry,
                    WaitState.CLOSED,
                    remove_from_queue=False,
                )

            self._signal_drained_if_ready_locked()

    async def wait_closed(self) -> None:
        """Wait until admission is closed and every active operation has left."""
        self._bind_to_running_loop()
        await self._drain_signal().wait()

    async def close_and_wait(self) -> None:
        """Close admission safely, then wait until active work has drained."""
        await complete_cleanup(self.close())
        await self.wait_closed()

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
                abandoned_after_admission_total=counters.abandoned_after_admission_total,
                queued_total=counters.queued_total,
                saturated_total=counters.saturated_total,
                expired_total=counters.expired_total,
                cancelled_while_waiting_total=counters.cancelled_while_waiting_total,
                closed_before_queue_total=counters.closed_before_queue_total,
                closed_while_waiting_total=counters.closed_while_waiting_total,
                finished_total=counters.finished_total,
                peak_in_flight=counters.peak_in_flight,
                peak_waiting=counters.peak_waiting,
                cumulative_wait_seconds=counters.cumulative_wait_seconds,
                longest_wait_seconds=counters.longest_wait_seconds,
                is_closed=self._closed,
            )
