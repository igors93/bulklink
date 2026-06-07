from __future__ import annotations

import inspect

from bulklink import AsyncBulkhead


def test_public_names_do_not_copy_relinker_vocabulary() -> None:
    public_methods = {
        name
        for name, member in inspect.getmembers(AsyncBulkhead)
        if not name.startswith("_") and callable(member)
    }

    assert {
        "add_event_handler",
        "capacity_report",
        "execute",
        "execute_now",
        "execute_within",
        "slot",
        "slot_now",
        "slot_within",
        "status",
        "close",
        "close_and_wait",
        "wait_closed",
        "remove_event_handler",
        "resize",
    } <= public_methods
    assert "run" not in public_methods
    assert "run_async" not in public_methods
    assert "snapshot" not in public_methods
    assert "retry" not in public_methods
