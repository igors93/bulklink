"""Synchronous event dispatch isolated from bulkhead state transitions."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Iterable
from typing import cast

from bulklink.events import BulkheadEvent, BulkheadEventHandler


class EventDispatcher:
    """Store synchronous handlers and isolate their failures from the bulkhead."""

    def __init__(self) -> None:
        self._handlers: tuple[BulkheadEventHandler, ...] = ()

    def add(self, handler: BulkheadEventHandler) -> None:
        """Register one synchronous handler exactly once by identity."""
        if not callable(handler):
            raise TypeError("event handler must be callable")
        if inspect.iscoroutinefunction(handler):
            raise TypeError("event handlers must be synchronous")
        if any(existing is handler for existing in self._handlers):
            return
        self._handlers = (*self._handlers, handler)

    def remove(self, handler: BulkheadEventHandler) -> None:
        """Remove one handler by identity; missing handlers are ignored."""
        self._handlers = tuple(existing for existing in self._handlers if existing is not handler)

    def dispatch(self, events: Iterable[BulkheadEvent]) -> None:
        """Invoke a stable handler snapshot without propagating handler failures."""
        handlers = self._handlers
        if not handlers:
            return

        loop = asyncio.get_running_loop()
        for event in events:
            for handler in handlers:
                try:
                    callback = cast(Callable[[BulkheadEvent], object], handler)
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
        event: BulkheadEvent,
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
            # A custom loop exception handler must not compromise bulkhead state.
            return
