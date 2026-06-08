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
    PartitionLimitError,
    WeightedBulkheadSaturatedError,
)
from bulklink.events import BulkheadEvent, BulkheadEventHandler, BulkheadEventKind
from bulklink.partitioned import PartitionedBulkhead
from bulklink.partitioned_status import (
    PartitionedBulkheadInterval,
    PartitionedBulkheadStatus,
)
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
    "PartitionedBulkhead",
    "PartitionedBulkheadInterval",
    "PartitionedBulkheadStatus",
    "PartitionLimitError",
    "WeightedBulkhead",
    "WeightedBulkheadEvent",
    "WeightedBulkheadEventHandler",
    "WeightedBulkheadInterval",
    "WeightedBulkheadSaturatedError",
    "WeightedBulkheadStatus",
]

__version__ = "0.6.0"
