# Named bulkhead registry

```python
from bulklink import BulkheadRegistry

registry = BulkheadRegistry()

payments = registry.create(
    "payments",
    parallelism=10,
    waiting_room=20,
    wait_limit=1.0,
)
reports = registry.create("reports", parallelism=2)
```

A registry owns a small collection of uniquely named `AsyncBulkhead` instances. It is
optional: direct construction of `AsyncBulkhead` remains fully supported.

## Lookup and ordering

```python
assert registry.get("payments") is payments
assert registry.labels == ("payments", "reports")
```

Labels are normalized in the same way as bulkhead labels. Duplicate names are rejected
instead of silently returning or replacing an existing instance. Snapshot methods keep
creation order and return immutable tuples.

## Read all status values

```python
statuses = await registry.statuses()
reports = await registry.capacity_reports()
```

Each operation takes a stable membership snapshot before awaiting individual
bulkheads. A concurrent removal does not change the result set already in progress.

## Remove one bulkhead safely

```python
removed = await registry.remove("reports")
```

Removal first closes and drains the target. The name is deleted only after active work
has finished, so the registry never silently abandons a live bulkhead.

## Shut down the group

```python
await registry.close_and_wait()
```

Or use separate steps:

```python
await registry.close_all()
await registry.wait_closed()
```

`close_all()` marks the registry closed before collecting its members. New bulkheads
cannot be created after shutdown starts. Cancelling `close_all()` or `close_and_wait()`
does not interrupt the collective cleanup; cancellation is propagated only after all
selected bulkheads have been attempted.

`wait_closed()` requires collective shutdown to have started. This avoids waiting on an
open registry while another caller can still add new members.

## Collective failures

All selected bulkheads are attempted even if one lifecycle operation fails. Afterward,
`BulkheadRegistryOperationError` reports bounded immutable entries containing only:

- the bulkhead label;
- the exception type name;
- the exception message.

No protected operation arguments, results, or exceptions are included in registry
snapshots.
