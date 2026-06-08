"""Private state models for partitioned bulkhead ownership."""

from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass

from bulklink.bulkhead import AsyncBulkhead


@dataclass(slots=True, eq=False)
class PartitionEntry:
    """One privately owned partition and its active caller references."""

    key: Hashable
    bulkhead: AsyncBulkhead
    borrowers: int
    last_idle_at: float


@dataclass(slots=True)
class PartitionRuntimeCounters:
    """Mutable partition lifecycle counters protected by the manager lock."""

    created_total: int = 0
    evicted_total: int = 0
    discarded_total: int = 0
    limit_rejected_total: int = 0
    peak_partitions: int = 0
    peak_leased_operations: int = 0
