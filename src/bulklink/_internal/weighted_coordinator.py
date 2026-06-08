"""Cancellation-safe FIFO admission coordination for weighted capacity."""

from __future__ import annotations

import asyncio
from collections import deque
from secrets import token_hex
from time import monotonic, time

from bulklink._internal.cancellation import complete_cleanup
from bulklink._internal.events import EventDispatcher
from bulklink._internal.models import WaitState
from bulklink._internal.validation import (
    require_finite_number,
    require_label,
    require_non_negative_integer,
    require_optional_positive_number,
    require_positive_integer,
    resolve_wait_limit,
)
from bulklink._internal.weighted_models import WeightedRuntimeCounters, WeightedWaitEntry
from bulklink.errors import (
    BulkheadClosedError,
    BulkheadQueueTimeoutError,
    WeightedBulkheadSaturatedError,
)
from bulklink.events import BulkheadEventKind
from bulklink.weighted_events import WeightedBulkheadEvent, WeightedBulkheadEventHandler
from bulklink.weighted_status import WeightedBulkheadStatus


class WeightedAdmissionCoordinator:
    """Own mutable state and synchronization for one weighted bulkhead."""

    def __init__(
        self,
        *,
        label: str,
        capacity: int,
        waiting_room: int,
        wait_limit: float | None,
    ) -> None:
        self._label = require_label(label)
        self._capacity = require_positive_integer("capacity", capacity)
        self._waiting_room = require_non_negative_integer("waiting_room", waiting_room)
        self._wait_limit = require_optional_positive_number("wait_limit", wait_limit)

        self._instance_id = token_hex(16)
        self._snapshot_index = 0
        self._mutex = asyncio.Lock()
        self._waiters: deque[WeightedWaitEntry] = deque()
        self._used = 0
        self._in_flight = 0
        self._waiting_units = 0
        self._closed = False
        self._counters = WeightedRuntimeCounters()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._drained_event: asyncio.Event | None = None
        self._event_dispatcher = EventDispatcher[WeightedBulkheadEvent]()

    @property
    def label(self) -> str:
        return self._label

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def waiting_room(self) -> int:
        return self._waiting_room

    @property
    def wait_limit(self) -> float | None:
        return self._wait_limit

    def add_event_handler(self, handler: WeightedBulkheadEventHandler) -> None:
        """Register one synchronous observability handler."""
        self._event_dispatcher.add(handler)

    def remove_event_handler(self, handler: WeightedBulkheadEventHandler) -> None:
        """Remove one previously registered observability handler."""
        self._event_dispatcher.remove(handler)

    def effective_wait_limit(self, requested: float) -> float:
        """Return the shortest limit allowed for one queued admission."""
        return resolve_wait_limit(self._wait_limit, requested)

    def validated_deadline(self, deadline: float) -> float:
        """Return one finite absolute event-loop deadline."""
        return require_finite_number("deadline", deadline)

    @staticmethod
    def validated_cost(cost: int) -> int:
        """Return one positive integer capacity cost."""
        return require_positive_integer("cost", cost)

    async def resize(self, capacity: int) -> None:
        """Change capacity without cancelling active or queued operations."""
        requested = require_positive_integer("capacity", capacity)
        self._bind_to_running_loop()
        error: Exception | None = None

        async with self._mutex:
            if self._closed:
                error = BulkheadClosedError(label=self._label)
                events: tuple[WeightedBulkheadEvent, ...] = ()
            elif requested == self._capacity:
                events = ()
            else:
                largest_waiting_cost = max((entry.cost for entry in self._waiters), default=0)
                if requested < largest_waiting_cost:
                    error = ValueError(
                        "capacity cannot be reduced below the largest queued operation cost"
                    )
                    events = ()
                else:
                    previous = self._capacity
                    occurred_at = time()
                    self._capacity = requested
                    resized = self._event_locked(
                        BulkheadEventKind.RESIZED,
                        occurred_at=occurred_at,
                        previous_capacity=previous,
                    )
                    admitted = self._admit_available_waiters_locked(occurred_at=occurred_at)
                    events = (resized, *admitted)

        self._event_dispatcher.dispatch(events)
        if error is not None:
            raise error

    def _bind_to_running_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = loop
            self._drained_event = asyncio.Event()
        elif self._owner_loop is not loop:
            raise RuntimeError(
                f"weighted bulkhead {self._label!r} cannot be shared across event loops"
            )
        return loop

    async def enter(self, cost: int) -> None:
        """Admit immediately, queue, or reject using the configured wait limit."""
        await self._enter(self.validated_cost(cost), self._wait_limit)

    async def enter_within(self, cost: int, wait_limit: float) -> None:
        """Admit using a per-call wait limit no longer than the configured limit."""
        validated_cost = self.validated_cost(cost)
        await self._enter(validated_cost, self.effective_wait_limit(wait_limit))

    async def enter_before(self, cost: int, deadline: float) -> None:
        """Admit before one absolute event-loop deadline."""
        validated_cost = self.validated_cost(cost)
        await self._enter(
            validated_cost,
            self._wait_limit,
            deadline=self.validated_deadline(deadline),
        )

    async def _enter(
        self,
        cost: int,
        wait_limit: float | None,
        *,
        deadline: float | None = None,
    ) -> None:
        loop = self._bind_to_running_loop()
        entry: WeightedWaitEntry | None = None
        error: Exception | None = None
        events: tuple[WeightedBulkheadEvent, ...] = ()
        expires_at: float | None = None
        effective_wait_limit = wait_limit

        async with self._mutex:
            if self._closed:
                self._counters.closed_before_queue_total += 1
                error = BulkheadClosedError(label=self._label)
                events = (
                    self._event_locked(
                        BulkheadEventKind.CLOSED_REJECTION,
                        cost=cost,
                    ),
                )
            elif cost > self._capacity:
                error = ValueError("cost cannot exceed the current weighted capacity")
            else:
                now = loop.time()
                if deadline is not None:
                    remaining = deadline - now
                    if remaining <= 0:
                        self._counters.expired_before_queue_total += 1
                        effective_wait_limit = 0.0
                        error = BulkheadQueueTimeoutError(
                            label=self._label,
                            wait_limit=effective_wait_limit,
                        )
                        events = (
                            self._event_locked(
                                BulkheadEventKind.EXPIRED,
                                cost=cost,
                                waited_seconds=0.0,
                            ),
                        )
                    else:
                        expires_at = deadline
                        if wait_limit is not None:
                            expires_at = min(expires_at, now + wait_limit)
                        effective_wait_limit = max(0.0, expires_at - now)

                if error is None and self._can_admit_directly_locked(cost):
                    events = (self._grant_directly_locked(cost),)
                elif error is None and len(self._waiters) >= self._waiting_room:
                    self._counters.saturated_total += 1
                    error = self._saturated_error(cost)
                    events = (
                        self._event_locked(
                            BulkheadEventKind.SATURATED,
                            cost=cost,
                        ),
                    )
                elif error is None:
                    entry = WeightedWaitEntry(
                        future=loop.create_future(),
                        enqueued_at=monotonic(),
                        cost=cost,
                    )
                    self._waiters.append(entry)
                    self._waiting_units += cost
                    counters = self._counters
                    counters.queued_total += 1
                    counters.queued_units_total += cost
                    counters.peak_waiting = max(counters.peak_waiting, len(self._waiters))
                    counters.peak_waiting_units = max(
                        counters.peak_waiting_units,
                        self._waiting_units,
                    )
                    events = (
                        self._event_locked(
                            BulkheadEventKind.QUEUED,
                            cost=cost,
                            from_queue=True,
                        ),
                    )

        self._event_dispatcher.dispatch(events)
        if error is not None:
            raise error
        if entry is None:
            return

        try:
            state = await self._await_terminal_state(
                entry,
                effective_wait_limit,
                expires_at=expires_at,
            )
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
                    f"unexpected weighted wait state after timeout: {state.name}"
                ) from timeout_error
            if effective_wait_limit is None:
                raise RuntimeError(
                    "a queued weighted admission expired without a wait limit"
                ) from timeout_error
            raise BulkheadQueueTimeoutError(
                label=self._label,
                wait_limit=effective_wait_limit,
            ) from timeout_error
        except asyncio.CancelledError:
            await complete_cleanup(self._cancel_waiter(entry))
            raise

        if state is WaitState.ADMITTED:
            return
        if state is WaitState.CLOSED:
            raise BulkheadClosedError(label=self._label)
        if state is WaitState.EXPIRED:
            if effective_wait_limit is None:
                raise RuntimeError("a queued weighted admission expired without a wait limit")
            raise BulkheadQueueTimeoutError(
                label=self._label,
                wait_limit=effective_wait_limit,
            )
        if state is WaitState.CANCELLED:
            raise asyncio.CancelledError
        raise RuntimeError(f"unexpected weighted terminal wait state: {state.name}")

    async def enter_now(self, cost: int) -> None:
        """Admit only when requested units are immediately available."""
        requested = self.validated_cost(cost)
        self._bind_to_running_loop()
        error: Exception | None = None

        async with self._mutex:
            if self._closed:
                self._counters.closed_before_queue_total += 1
                error = BulkheadClosedError(label=self._label)
                event = self._event_locked(
                    BulkheadEventKind.CLOSED_REJECTION,
                    cost=requested,
                )
            elif requested > self._capacity:
                error = ValueError("cost cannot exceed the current weighted capacity")
                event = None
            elif self._can_admit_directly_locked(requested):
                event = self._grant_directly_locked(requested)
            else:
                self._counters.saturated_total += 1
                error = self._saturated_error(requested)
                event = self._event_locked(
                    BulkheadEventKind.SATURATED,
                    cost=requested,
                )

        if event is not None:
            self._event_dispatcher.dispatch((event,))
        if error is not None:
            raise error

    def _saturated_error(self, cost: int) -> WeightedBulkheadSaturatedError:
        return WeightedBulkheadSaturatedError(
            label=self._label,
            cost=cost,
            used=self._used,
            capacity=self._capacity,
            waiting=len(self._waiters),
            waiting_room=self._waiting_room,
        )

    def _can_admit_directly_locked(self, cost: int) -> bool:
        return not self._waiters and self._used + cost <= self._capacity

    async def _await_terminal_state(
        self,
        entry: WeightedWaitEntry,
        wait_limit: float | None,
        *,
        expires_at: float | None = None,
    ) -> WaitState:
        if expires_at is not None:
            wait_limit = max(0.0, expires_at - asyncio.get_running_loop().time())
        if wait_limit is None:
            return await asyncio.shield(entry.future)
        return await asyncio.wait_for(
            asyncio.shield(entry.future),
            timeout=wait_limit,
        )

    def _grant_directly_locked(self, cost: int) -> WeightedBulkheadEvent:
        self._allocate_capacity_locked(cost)
        counters = self._counters
        counters.admitted_total += 1
        counters.admitted_units_total += cost
        return self._event_locked(BulkheadEventKind.ADMITTED, cost=cost)

    def _allocate_capacity_locked(self, cost: int) -> None:
        self._used += cost
        self._in_flight += 1
        counters = self._counters
        counters.peak_used = max(counters.peak_used, self._used)
        counters.peak_in_flight = max(counters.peak_in_flight, self._in_flight)

    def _admit_available_waiters_locked(
        self,
        *,
        occurred_at: float,
    ) -> tuple[WeightedBulkheadEvent, ...]:
        events: list[WeightedBulkheadEvent] = []
        while self._waiters:
            entry = self._waiters[0]
            if self._used + entry.cost > self._capacity:
                break
            self._waiters.popleft()
            if entry.future.done():
                raise RuntimeError("weighted waiting future completed before admission")
            self._allocate_capacity_locked(entry.cost)
            _, event = self._finish_waiter_locked(
                entry,
                WaitState.ADMITTED,
                remove_from_queue=False,
                occurred_at=occurred_at,
            )
            events.append(event)
        return tuple(events)

    async def _expire_waiter(self, entry: WeightedWaitEntry) -> WaitState:
        events: tuple[WeightedBulkheadEvent, ...] = ()
        async with self._mutex:
            if entry.state is WaitState.WAITING:
                occurred_at = time()
                state, event = self._finish_waiter_locked(
                    entry,
                    WaitState.EXPIRED,
                    remove_from_queue=True,
                    occurred_at=occurred_at,
                )
                admitted = self._admit_available_waiters_locked(occurred_at=occurred_at)
                events = (event, *admitted)
            else:
                state = entry.state
        self._event_dispatcher.dispatch(events)
        return state

    async def _cancel_waiter(self, entry: WeightedWaitEntry) -> WaitState:
        events: tuple[WeightedBulkheadEvent, ...] = ()
        async with self._mutex:
            if entry.state is WaitState.WAITING:
                occurred_at = time()
                state, event = self._finish_waiter_locked(
                    entry,
                    WaitState.CANCELLED,
                    remove_from_queue=True,
                    occurred_at=occurred_at,
                )
                admitted = self._admit_available_waiters_locked(occurred_at=occurred_at)
                events = (event, *admitted)
            elif entry.state is WaitState.ADMITTED:
                state = entry.state
                events = self._abandon_admitted_locked(entry)
            else:
                state = entry.state
        self._event_dispatcher.dispatch(events)
        return state

    def _finish_waiter_locked(
        self,
        entry: WeightedWaitEntry,
        state: WaitState,
        *,
        remove_from_queue: bool,
        occurred_at: float | None = None,
    ) -> tuple[WaitState, WeightedBulkheadEvent]:
        if state is WaitState.WAITING:
            raise ValueError("a weighted waiting entry requires a terminal state")
        if entry.state is not WaitState.WAITING:
            raise RuntimeError("only weighted waiting entries can be completed")
        if remove_from_queue:
            self._remove_waiter_locked(entry)

        if not entry.transition_to(state):
            raise RuntimeError("weighted waiting entry could not transition")

        self._waiting_units -= entry.cost
        if self._waiting_units < 0:
            raise RuntimeError("weighted waiting units became negative")

        waited = max(0.0, monotonic() - entry.enqueued_at)
        counters = self._counters
        if state is WaitState.ADMITTED:
            entry.waited_seconds = waited
            counters.admitted_total += 1
            counters.admitted_units_total += entry.cost
            counters.admitted_from_queue_total += 1
            counters.admitted_from_queue_units_total += entry.cost
            counters.cumulative_wait_seconds += waited
            counters.longest_wait_seconds = max(counters.longest_wait_seconds, waited)
            entry.future.set_result(WaitState.ADMITTED)
            event = self._event_locked(
                BulkheadEventKind.ADMITTED,
                occurred_at=occurred_at,
                cost=entry.cost,
                from_queue=True,
                waited_seconds=waited,
            )
        elif state is WaitState.CANCELLED:
            counters.cancelled_while_waiting_total += 1
            entry.future.cancel()
            event = self._event_locked(
                BulkheadEventKind.CANCELLED,
                occurred_at=occurred_at,
                cost=entry.cost,
                from_queue=True,
                waited_seconds=waited,
            )
        elif state is WaitState.EXPIRED:
            counters.expired_total += 1
            entry.future.set_result(WaitState.EXPIRED)
            event = self._event_locked(
                BulkheadEventKind.EXPIRED,
                occurred_at=occurred_at,
                cost=entry.cost,
                from_queue=True,
                waited_seconds=waited,
            )
        elif state is WaitState.CLOSED:
            counters.closed_while_waiting_total += 1
            entry.future.set_result(WaitState.CLOSED)
            event = self._event_locked(
                BulkheadEventKind.CLOSED_REJECTION,
                occurred_at=occurred_at,
                cost=entry.cost,
                from_queue=True,
                waited_seconds=waited,
            )
        else:
            raise RuntimeError(f"unsupported weighted wait state: {state.name}")
        return state, event

    def _remove_waiter_locked(self, entry: WeightedWaitEntry) -> None:
        try:
            self._waiters.remove(entry)
        except ValueError as error:
            raise RuntimeError("weighted entry is missing from the FIFO queue") from error

    async def release(self, cost: int) -> None:
        """Finish one protected operation and release or transfer its capacity."""
        requested = self.validated_cost(cost)
        self._bind_to_running_loop()
        async with self._mutex:
            events = self._finish_admitted_locked(requested)
        self._event_dispatcher.dispatch(events)

    def _finish_admitted_locked(self, cost: int) -> tuple[WeightedBulkheadEvent, ...]:
        if self._in_flight <= 0 or self._used < cost:
            raise RuntimeError("weighted capacity released without a matching admission")

        occurred_at = time()
        counters = self._counters
        counters.finished_total += 1
        counters.finished_units_total += cost
        self._used -= cost
        self._in_flight -= 1
        admitted = self._admit_available_waiters_locked(occurred_at=occurred_at)
        drained = self._signal_drained_if_ready_locked(occurred_at=occurred_at)
        released = self._event_locked(
            BulkheadEventKind.RELEASED,
            occurred_at=occurred_at,
            cost=cost,
        )
        if drained is None:
            return (released, *admitted)
        return (released, *admitted, drained)

    def _abandon_admitted_locked(
        self,
        entry: WeightedWaitEntry,
    ) -> tuple[WeightedBulkheadEvent, ...]:
        if self._in_flight <= 0 or self._used < entry.cost:
            raise RuntimeError("weighted admission abandoned without allocated capacity")
        if entry.waited_seconds is None:
            raise RuntimeError("weighted admission is missing its wait duration")

        occurred_at = time()
        counters = self._counters
        counters.abandoned_after_admission_total += 1
        counters.abandoned_units_total += entry.cost
        self._used -= entry.cost
        self._in_flight -= 1
        admitted = self._admit_available_waiters_locked(occurred_at=occurred_at)
        drained = self._signal_drained_if_ready_locked(occurred_at=occurred_at)
        abandoned = self._event_locked(
            BulkheadEventKind.ABANDONED,
            occurred_at=occurred_at,
            cost=entry.cost,
            from_queue=True,
            waited_seconds=entry.waited_seconds,
        )
        if drained is None:
            return (abandoned, *admitted)
        return (abandoned, *admitted, drained)

    def _drain_signal(self) -> asyncio.Event:
        event = self._drained_event
        if event is None:
            raise RuntimeError("weighted drain signal is unavailable before event-loop binding")
        return event

    def _signal_drained_if_ready_locked(
        self,
        *,
        occurred_at: float | None = None,
    ) -> WeightedBulkheadEvent | None:
        if not self._closed or self._in_flight != 0 or self._used != 0:
            return None
        if self._waiters or self._waiting_units != 0:
            raise RuntimeError("a closed weighted bulkhead cannot retain queued work")

        signal = self._drain_signal()
        if signal.is_set():
            return None
        signal.set()
        return self._event_locked(BulkheadEventKind.DRAINED, occurred_at=occurred_at)

    async def close(self) -> None:
        """Close admission and wake queued operations with a closed state."""
        self._bind_to_running_loop()
        async with self._mutex:
            if self._closed:
                events: tuple[WeightedBulkheadEvent, ...] = ()
            else:
                affected_waiters = len(self._waiters)
                self._closed = True
                pending: list[WeightedBulkheadEvent] = [
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
                    pending.append(event)
                drained = self._signal_drained_if_ready_locked()
                if drained is not None:
                    pending.append(drained)
                events = tuple(pending)
        self._event_dispatcher.dispatch(events)

    async def wait_closed(self) -> None:
        """Wait until admission is closed and every active operation has left."""
        self._bind_to_running_loop()
        await self._drain_signal().wait()

    async def close_and_wait(self) -> None:
        """Close admission safely, then wait until active work has drained."""
        await complete_cleanup(self.close())
        await self.wait_closed()

    async def status(self) -> WeightedBulkheadStatus:
        """Build an immutable weighted status under the coordinator lock."""
        self._bind_to_running_loop()
        async with self._mutex:
            self._snapshot_index += 1
            counters = self._counters
            return WeightedBulkheadStatus(
                instance_id=self._instance_id,
                snapshot_index=self._snapshot_index,
                label=self._label,
                capacity=self._capacity,
                waiting_room=self._waiting_room,
                used=self._used,
                in_flight=self._in_flight,
                waiting=len(self._waiters),
                waiting_units=self._waiting_units,
                admitted_total=counters.admitted_total,
                admitted_units_total=counters.admitted_units_total,
                admitted_from_queue_total=counters.admitted_from_queue_total,
                admitted_from_queue_units_total=counters.admitted_from_queue_units_total,
                abandoned_after_admission_total=counters.abandoned_after_admission_total,
                abandoned_units_total=counters.abandoned_units_total,
                queued_total=counters.queued_total,
                queued_units_total=counters.queued_units_total,
                saturated_total=counters.saturated_total,
                expired_total=counters.expired_total,
                expired_before_queue_total=counters.expired_before_queue_total,
                cancelled_while_waiting_total=counters.cancelled_while_waiting_total,
                closed_before_queue_total=counters.closed_before_queue_total,
                closed_while_waiting_total=counters.closed_while_waiting_total,
                finished_total=counters.finished_total,
                finished_units_total=counters.finished_units_total,
                peak_used=counters.peak_used,
                peak_in_flight=counters.peak_in_flight,
                peak_waiting=counters.peak_waiting,
                peak_waiting_units=counters.peak_waiting_units,
                cumulative_wait_seconds=counters.cumulative_wait_seconds,
                longest_wait_seconds=counters.longest_wait_seconds,
                is_closed=self._closed,
            )

    def _event_locked(
        self,
        kind: BulkheadEventKind,
        *,
        occurred_at: float | None = None,
        cost: int | None = None,
        from_queue: bool = False,
        waited_seconds: float | None = None,
        affected_waiters: int = 0,
        previous_capacity: int | None = None,
    ) -> WeightedBulkheadEvent:
        return WeightedBulkheadEvent(
            kind=kind,
            label=self._label,
            occurred_at=time() if occurred_at is None else occurred_at,
            capacity=self._capacity,
            waiting_room=self._waiting_room,
            used=self._used,
            in_flight=self._in_flight,
            waiting=len(self._waiters),
            is_closed=self._closed,
            cost=cost,
            from_queue=from_queue,
            waited_seconds=waited_seconds,
            affected_waiters=affected_waiters,
            previous_capacity=previous_capacity,
        )
