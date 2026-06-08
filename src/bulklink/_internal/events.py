"""Synchronous event dispatch isolated from bulkhead state transitions."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Iterable
from typing import Generic, TypeVar, cast

EventT = TypeVar("EventT")
EventHandler = Callable[[EventT], None]


class EventDispatcher(Generic[EventT]):
    """Store synchronous handlers and isolate their failures from protected state."""

    def __init__(self) -> None:
        self._handlers: tuple[EventHandler[EventT], ...] = ()

    def add(self, handler: EventHandler[EventT]) -> None:
        """Register one synchronous handler exactly once by identity."""
        if not callable(handler):
            raise TypeError("event handler must be callable")
        if inspect.iscoroutinefunction(handler):
            raise TypeError("event handlers must be synchronous")
        if any(existing is handler for existing in self._handlers):
            return
        self._handlers = (*self._handlers, handler)

    def remove(self, handler: EventHandler[EventT]) -> None:
        """Remove one handler by identity; missing handlers are ignored."""
        self._handlers = tuple(existing for existing in self._handlers if existing is not handler)

    def dispatch(self, events: Iterable[EventT]) -> None:
        """Invoke a stable handler snapshot without propagating handler failures."""
        handlers = self._handlers
        if not handlers:
            return

        loop = asyncio.get_running_loop()
        for event in events:
            for handler in handlers:
                try:
                    callback = cast(Callable[[EventT], object], handler)
                    result = callback(event)
                    if inspect.isawaitable(result):
                        if inspect.iscoroutine(result):
                            result.close()
                        raise TypeError("event handlers must return None and run synchronously")
                    if result is not None:
                        raise TypeError("event handlers must return None")
                except BaseException as error:
                    self._report_failure(loop, event, error)

    @staticmethod
    def _report_failure(
        loop: asyncio.AbstractEventLoop,
        event: EventT,
        error: BaseException,
    ) -> None:
        context = {
            "message": "Bulklink event handler failed",
            "exception": error,
            "bulklink_event": event,
        }
        try:
            loop.call_exception_handler(context)
        except BaseException:
            return
