"""Cancellation-safe FIFO admission coordination."""

from __future__ import annotations

import asyncio
from collections import deque
from time import monotonic, time

from bulklink._internal.cancellation import complete_cleanup
from bulklink._internal.events import EventDispatcher
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
from bulklink.events import BulkheadEvent, BulkheadEventHandler, BulkheadEventKind
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
        self._event_dispatcher = EventDispatcher()

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

    def add_event_handler(self, handler: BulkheadEventHandler) -> None:
        """Register one synchronous observability handler."""
        self._event_dispatcher.add(handler)

    def remove_event_handler(self, handler: BulkheadEventHandler) -> None:
        """Remove one previously registered observability handler."""
        self._event_dispatcher.remove(handler)

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
        entry: WaitEntry | None = None
        error: Exception | None = None
        events: tuple[BulkheadEvent, ...]

        async with self._mutex:
            if self._closed:
                self._counters.closed_before_queue_total += 1
                error = BulkheadClosedError(label=self._label)
                events = (self._event_locked(BulkheadEventKind.CLOSED_REJECTION),)
            elif self._can_admit_directly_locked():
                events = (self._grant_directly_locked(),)
            elif len(self._waiters) >= self._waiting_room:
                self._counters.saturated_total += 1
                error = BulkheadSaturatedError(
                    label=self._label,
                    in_flight=self._in_flight,
                    waiting=len(self._waiters),
                    parallelism=self._parallelism,
                    waiting_room=self._waiting_room,
                )
                events = (self._event_locked(BulkheadEventKind.SATURATED),)
            else:
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
                events = (
                    self._event_locked(
                        BulkheadEventKind.QUEUED,
                        from_queue=True,
                    ),
                )

        self._event_dispatcher.dispatch(events)
        if error is not None:
            raise error
        if entry is None:
            return

        try:
            state = await self._await_terminal_state(entry, wait_limit)
        except asyncio.TimeoutError as timeout_error:
            try:
                state = await complete_cleanup(self._expire_waiter(entry))
            except asyncio.CancelledError:
                await complete_cleanup(self._cancel_waiter(entry))
                raise

            if state is WaitState.ADMITTED:
                return
            if state is WaitState.CLOSED:
                raise BulkheadClosedError(label=self._label) from timeout_error
            if state is not WaitState.EXPIRED:
                raise RuntimeError(
                    f"unexpected wait state after timeout: {state.name}"
                ) from timeout_error

            if wait_limit is None:
                raise RuntimeError(
                    "a queued admission expired without a wait limit"
                ) from timeout_error
            raise BulkheadQueueTimeoutError(
                label=self._label,
                wait_limit=wait_limit,
            ) from timeout_error
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
        error: Exception | None = None

        async with self._mutex:
            if self._closed:
                self._counters.closed_before_queue_total += 1
                error = BulkheadClosedError(label=self._label)
                event = self._event_locked(BulkheadEventKind.CLOSED_REJECTION)
            elif self._can_admit_directly_locked():
                event = self._grant_directly_locked()
            else:
                self._counters.saturated_total += 1
                error = BulkheadSaturatedError(
                    label=self._label,
                    in_flight=self._in_flight,
                    waiting=len(self._waiters),
                    parallelism=self._parallelism,
                    waiting_room=self._waiting_room,
                )
                event = self._event_locked(BulkheadEventKind.SATURATED)

        self._event_dispatcher.dispatch((event,))
        if error is not None:
            raise error

    def _can_admit_directly_locked(self) -> bool:
        return self._in_flight < self._parallelism and not self._waiters

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

    def _grant_directly_locked(self) -> BulkheadEvent:
        self._in_flight += 1
        self._counters.admitted_total += 1
        self._counters.peak_in_flight = max(
            self._counters.peak_in_flight,
            self._in_flight,
        )
        return self._event_locked(BulkheadEventKind.ADMITTED)

    async def _expire_waiter(self, entry: WaitEntry) -> WaitState:
        """Expire one entry if it is still waiting."""
        events: tuple[BulkheadEvent, ...] = ()
        async with self._mutex:
            if entry.state is WaitState.WAITING:
                state, event = self._finish_waiter_locked(
                    entry,
                    WaitState.EXPIRED,
                    remove_from_queue=True,
                )
                events = (event,)
            else:
                state = entry.state

        self._event_dispatcher.dispatch(events)
        return state

    async def _cancel_waiter(self, entry: WaitEntry) -> WaitState:
        """Cancel waiting or return a slot already transferred to this entry."""
        events: tuple[BulkheadEvent, ...] = ()
        async with self._mutex:
            if entry.state is WaitState.WAITING:
                state, event = self._finish_waiter_locked(
                    entry,
                    WaitState.CANCELLED,
                    remove_from_queue=True,
                )
                events = (event,)
            elif entry.state is WaitState.ADMITTED:
                state = entry.state
                events = self._abandon_admitted_slot_locked(entry)
            else:
                state = entry.state

        self._event_dispatcher.dispatch(events)
        return state

    def _finish_waiter_locked(
        self,
        entry: WaitEntry,
        state: WaitState,
        *,
        remove_from_queue: bool,
    ) -> tuple[WaitState, BulkheadEvent]:
        """Move one waiting entry to a terminal state under the coordinator lock."""
        if state is WaitState.WAITING:
            raise ValueError("a waiting entry requires a terminal state")
        if entry.state is not WaitState.WAITING:
            raise RuntimeError("only waiting entries can be completed")

        if remove_from_queue:
            self._remove_waiter_locked(entry)

        if not entry.transition_to(state):
            raise RuntimeError("waiting entry could not transition to a terminal state")

        waited = max(0.0, monotonic() - entry.enqueued_at)
        if state is WaitState.ADMITTED:
            entry.waited_seconds = waited
            self._counters.admitted_total += 1
            self._counters.admitted_from_queue_total += 1
            self._counters.cumulative_wait_seconds += waited
            self._counters.longest_wait_seconds = max(
                self._counters.longest_wait_seconds,
                waited,
            )
            entry.future.set_result(WaitState.ADMITTED)
            event = self._event_locked(
                BulkheadEventKind.ADMITTED,
                from_queue=True,
                waited_seconds=waited,
            )
        elif state is WaitState.CANCELLED:
            self._counters.cancelled_while_waiting_total += 1
            entry.future.cancel()
            event = self._event_locked(
                BulkheadEventKind.CANCELLED,
                from_queue=True,
                waited_seconds=waited,
            )
        elif state is WaitState.EXPIRED:
            self._counters.expired_total += 1
            entry.future.set_result(WaitState.EXPIRED)
            event = self._event_locked(
                BulkheadEventKind.EXPIRED,
                from_queue=True,
                waited_seconds=waited,
            )
        elif state is WaitState.CLOSED:
            self._counters.closed_while_waiting_total += 1
            entry.future.set_result(WaitState.CLOSED)
            event = self._event_locked(
                BulkheadEventKind.CLOSED_REJECTION,
                from_queue=True,
                waited_seconds=waited,
            )
        else:
            raise RuntimeError(f"unsupported terminal wait state: {state.name}")

        return state, event

    def _remove_waiter_locked(self, entry: WaitEntry) -> None:
        try:
            self._waiters.remove(entry)
        except ValueError as error:
            raise RuntimeError("waiting entry is missing from the FIFO queue") from error

    async def release(self) -> None:
        """Finish one protected operation and release or transfer its slot."""
        self._bind_to_running_loop()

        async with self._mutex:
            events = self._finish_admitted_slot_locked()

        self._event_dispatcher.dispatch(events)

    def _finish_admitted_slot_locked(self) -> tuple[BulkheadEvent, ...]:
        if self._in_flight <= 0:
            raise RuntimeError("execution slot released without a matching admission")
        self._counters.finished_total += 1
        capacity_events = self._release_capacity_locked()
        released = self._event_locked(BulkheadEventKind.RELEASED)
        return (released, *capacity_events)

    def _abandon_admitted_slot_locked(
        self,
        entry: WaitEntry,
    ) -> tuple[BulkheadEvent, ...]:
        if self._in_flight <= 0:
            raise RuntimeError("admitted slot abandoned without allocated capacity")
        self._counters.abandoned_after_admission_total += 1
        capacity_events = self._release_capacity_locked()
        if entry.waited_seconds is None:
            raise RuntimeError("admitted queue entry is missing its wait duration")
        abandoned = self._event_locked(
            BulkheadEventKind.ABANDONED,
            from_queue=True,
            waited_seconds=entry.waited_seconds,
        )
        return (abandoned, *capacity_events)

    def _release_capacity_locked(self) -> tuple[BulkheadEvent, ...]:
        if self._waiters:
            entry = self._waiters.popleft()
            if entry.future.done():
                raise RuntimeError("waiting entry future completed before admission")
            _, event = self._finish_waiter_locked(
                entry,
                WaitState.ADMITTED,
                remove_from_queue=False,
            )
            # Direct transfer keeps _in_flight unchanged.
            return (event,)

        self._in_flight -= 1
        drained = self._signal_drained_if_ready_locked()
        if drained is None:
            return ()
        return (drained,)

    def _drain_signal(self) -> asyncio.Event:
        event = self._drained_event
        if event is None:
            raise RuntimeError("bulkhead drain signal is unavailable before event-loop binding")
        return event

    def _signal_drained_if_ready_locked(self) -> BulkheadEvent | None:
        if not self._closed or self._in_flight != 0:
            return None
        if self._waiters:
            raise RuntimeError("a closed and drained bulkhead cannot retain queued entries")

        signal = self._drain_signal()
        if signal.is_set():
            return None
        signal.set()
        return self._event_locked(BulkheadEventKind.DRAINED)

    async def close(self) -> None:
        """Close admission and wake queued operations with a closed state."""
        self._bind_to_running_loop()

        async with self._mutex:
            if self._closed:
                events: tuple[BulkheadEvent, ...] = ()
            else:
                affected_waiters = len(self._waiters)
                self._closed = True
                pending_events: list[BulkheadEvent] = [
                    self._event_locked(
                        BulkheadEventKind.CLOSED,
                        affected_waiters=affected_waiters,
                    )
                ]

                while self._waiters:
                    entry = self._waiters.popleft()
                    _, event = self._finish_waiter_locked(
                        entry,
                        WaitState.CLOSED,
                        remove_from_queue=False,
                    )
                    pending_events.append(event)

                drained = self._signal_drained_if_ready_locked()
                if drained is not None:
                    pending_events.append(drained)
                events = tuple(pending_events)

        self._event_dispatcher.dispatch(events)

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

    def _event_locked(
        self,
        kind: BulkheadEventKind,
        *,
        from_queue: bool = False,
        waited_seconds: float | None = None,
        affected_waiters: int = 0,
    ) -> BulkheadEvent:
        return BulkheadEvent(
            kind=kind,
            label=self._label,
            occurred_at=time(),
            parallelism=self._parallelism,
            waiting_room=self._waiting_room,
            in_flight=self._in_flight,
            waiting=len(self._waiters),
            is_closed=self._closed,
            from_queue=from_queue,
            waited_seconds=waited_seconds,
            affected_waiters=affected_waiters,
        )
