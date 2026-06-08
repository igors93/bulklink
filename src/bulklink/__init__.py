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
    WeightedBulkheadSaturatedError,
)
from bulklink.events import BulkheadEvent, BulkheadEventHandler, BulkheadEventKind
from bulklink.registry import (
    BulkheadRegistry,
    BulkheadRegistryFailure,
    BulkheadRegistryOperationError,
)
from bulklink.status import BulkheadInterval, BulkheadStatus
from bulklink.weighted import WeightedBulkhead
from bulklink.weighted_events import WeightedBulkheadEvent, WeightedBulkheadEventHandler
from bulklink.weighted_status import WeightedBulkheadInterval, WeightedBulkheadStatus

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
    "BulkheadInterval",
    "BulkheadQueueTimeoutError",
    "BulkheadRegistry",
    "BulkheadRegistryFailure",
    "BulkheadRegistryOperationError",
    "BulkheadSaturatedError",
    "BulkheadStatus",
    "BulklinkError",
    "WeightedBulkhead",
    "WeightedBulkheadEvent",
    "WeightedBulkheadEventHandler",
    "WeightedBulkheadInterval",
    "WeightedBulkheadSaturatedError",
    "WeightedBulkheadStatus",
]

__version__ = "0.5.0"
