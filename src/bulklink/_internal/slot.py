"""Async context manager for one granted execution slot."""

from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING

from bulklink._internal.cancellation import complete_cleanup

if TYPE_CHECKING:
    from bulklink._internal.coordinator import AdmissionCoordinator


class SlotContext:
    """Own exactly one admission and release lifecycle."""

    def __init__(self, coordinator: AdmissionCoordinator) -> None:
        self._coordinator = coordinator
        self._entered = False

    async def __aenter__(self) -> SlotContext:
        if self._entered:
            raise RuntimeError("the same slot context cannot be entered twice")
        await self._coordinator.enter()
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
        await complete_cleanup(self._coordinator.release())
