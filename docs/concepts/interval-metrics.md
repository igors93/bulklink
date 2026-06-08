# Interval metrics

`BulkheadStatus` contains cumulative counters since one bulkhead was created. Compare two
snapshots when an application needs activity for a specific observation period:

```python
before = await payments.status()

# Observe for an application-defined period.

after = await payments.status()
interval = after.since(before)
```

The returned `BulkheadInterval` is immutable and contains:

- `start` and `end`, the original status snapshots;
- `admitted`, `admitted_from_queue`, and `direct_admitted`;
- `queued` and `settled_waiting`;
- `saturated`, `expired`, `expired_before_queue`, and `rejected`;
- cancellation and closed-state queue outcomes;
- `finished` and `abandoned_after_admission`;
- interval queue wait total and `average_wait_seconds`;
- `has_activity`, which is false when no cumulative metric changed.

## No hidden state

Bulklink does not retain windows, reset counters, run a sampler, or create background tasks.
The application chooses when to take snapshots and where to store them.

## Validation

Snapshots must come from the same bulkhead instance in chronological order. Bulklink
checks matching labels, matching waiting-room capacity, nondecreasing counters and peaks,
and the one-way closed lifecycle. Capacity may differ because `resize()` is allowed between
snapshots.

A reversed snapshot order or an obvious comparison between unrelated statuses raises
`ValueError` rather than returning negative metrics.

## What interval metrics do not do

They do not provide automatic time windows, rates per second, metric export, persistence,
or aggregation across processes. Those concerns belong to the application or an optional
observability adapter.
