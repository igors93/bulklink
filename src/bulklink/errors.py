"""Public Bulklink exception hierarchy."""

from __future__ import annotations


class BulklinkError(Exception):
    """Base class for errors produced by Bulklink."""


class BulkheadSaturatedError(BulklinkError):
    """Raised when execution capacity and the waiting room are both full."""

    def __init__(
        self,
        *,
        label: str,
        in_flight: int,
        waiting: int,
        parallelism: int,
        waiting_room: int,
    ) -> None:
        self.label = label
        self.in_flight = in_flight
        self.waiting = waiting
        self.parallelism = parallelism
        self.waiting_room = waiting_room
        super().__init__(
            f"bulkhead {label!r} is saturated "
            f"(in_flight={in_flight}/{parallelism}, waiting={waiting}/{waiting_room})"
        )


class BulkheadQueueTimeoutError(BulklinkError):
    """Raised when an operation waits longer than the configured queue deadline.

    This deliberately does not inherit from ``TimeoutError``. A retry library
    configured for network timeouts should not automatically retry local overload.
    """

    def __init__(self, *, label: str, wait_limit: float) -> None:
        self.label = label
        self.wait_limit = wait_limit
        super().__init__(
            f"bulkhead {label!r} waiting deadline expired after {wait_limit:g} seconds"
        )


class BulkheadClosedError(BulklinkError):
    """Raised when an operation tries to use a closed bulkhead."""

    def __init__(self, *, label: str) -> None:
        self.label = label
        super().__init__(f"bulkhead {label!r} is closed")


class WeightedBulkheadSaturatedError(BulklinkError):
    """Raised when requested units cannot start and the waiting room is full."""

    def __init__(
        self,
        *,
        label: str,
        cost: int,
        used: int,
        capacity: int,
        waiting: int,
        waiting_room: int,
    ) -> None:
        self.label = label
        self.cost = cost
        self.used = used
        self.capacity = capacity
        self.waiting = waiting
        self.waiting_room = waiting_room
        super().__init__(
            f"weighted bulkhead {label!r} is saturated "
            f"(cost={cost}, used={used}/{capacity}, waiting={waiting}/{waiting_room})"
        )
