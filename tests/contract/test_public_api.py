from __future__ import annotations

import bulklink


def test_root_public_api_is_exact_and_small() -> None:
    assert bulklink.__all__ == [
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


def test_version_is_exposed() -> None:
    assert bulklink.__version__ == "0.2.0"
