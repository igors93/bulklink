"""Async context manager for one granted execution slot."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from enum import Enum, auto
from types import TracebackType
from typing import Any

from bulklink._internal.cancellation import complete_cleanup

AdmissionAction = Callable[[], Coroutine[Any, Any, None]]


class _SlotState(Enum):
    IDLE = auto()
    ENTERING = auto()
    ENTERED = auto()
    EXITING = auto()


class SlotContext:
    """Own exactly one admission and release lifecycle."""

    def __init__(
        self,
        admit: AdmissionAction,
        release: AdmissionAction,
    ) -> None:
        self._admit = admit
        self._release = release
        self._state = _SlotState.IDLE

    async def __aenter__(self) -> SlotContext:
        if self._state is not _SlotState.IDLE:
            raise RuntimeError("the same slot context is already in use")

        self._state = _SlotState.ENTERING
        try:
            await self._admit()
        except BaseException:
            self._state = _SlotState.IDLE
            raise

        self._state = _SlotState.ENTERED
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._state is _SlotState.IDLE:
            return
        if self._state is not _SlotState.ENTERED:
            raise RuntimeError("the slot context cannot exit during another lifecycle transition")

        self._state = _SlotState.EXITING
        try:
            await complete_cleanup(self._release())
        finally:
            self._state = _SlotState.IDLE
