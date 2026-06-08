"""Stable Bulklink public API."""

from bulklink.bulkhead import AsyncBulkhead
from bulklink.capacity import (
    CapacityFinding,
    CapacityFindingCode,
    CapacityReport,
    CapacitySeverity,
)
from bulklink.errors import (
    BulkheadClosedError,
    BulkheadQueueTimeoutError,
    BulkheadSaturatedError,
    BulklinkError,
)
from bulklink.events import BulkheadEvent, BulkheadEventHandler, BulkheadEventKind
from bulklink.registry import (
    BulkheadRegistry,
    BulkheadRegistryFailure,
    BulkheadRegistryOperationError,
)
from bulklink.status import BulkheadStatus

__all__ = [
    "AsyncBulkhead",
    "CapacityFinding",
    "CapacityFindingCode",
    "CapacityReport",
    "CapacitySeverity",
    "BulkheadClosedError",
    "BulkheadEvent",
    "BulkheadEventHandler",
    "BulkheadEventKind",
    "BulkheadQueueTimeoutError",
    "BulkheadRegistry",
    "BulkheadRegistryFailure",
    "BulkheadRegistryOperationError",
    "BulkheadSaturatedError",
    "BulkheadStatus",
    "BulklinkError",
]

__version__ = "0.2.0rc1"
