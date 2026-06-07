"""Async context manager for one granted execution slot."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from types import TracebackType
from typing import Any

from bulklink._internal.cancellation import complete_cleanup

AdmissionAction = Callable[[], Coroutine[Any, Any, None]]


class SlotContext:
    """Own exactly one admission and release lifecycle."""

    def __init__(
        self,
        admit: AdmissionAction,
        release: AdmissionAction,
    ) -> None:
        self._admit = admit
        self._release = release
        self._entered = False

    async def __aenter__(self) -> SlotContext:
        if self._entered:
            raise RuntimeError("the same slot context cannot be entered twice")
        await self._admit()
        self._entered = True
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._entered:
            return
        self._entered = False
        await complete_cleanup(self._release())
