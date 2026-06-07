# Capacity diagnostics

```python
report = await payments.capacity_report()
```

A capacity report is an immutable interpretation of one `BulkheadStatus` snapshot and
the cumulative counters collected since the bulkhead was created. It never changes
configuration, starts background work, or cancels operations.

## Read a report

```python
print(report.summary)
print(report.severity.value)

for finding in report.findings:
    print(finding.code.value)
    print(finding.message)
    print(finding.recommendation)
```

`requires_attention` becomes true only for warning or critical findings. Advisory
notices are useful configuration observations but do not mean the bulkhead is
currently overloaded.

## Ratios

The report provides deterministic derived values:

- `rejection_ratio` uses saturation and queue-expiration rejections while the
  bulkhead was open;
- `queue_entry_ratio` is the share of open admission attempts that entered the queue;
- `expiration_ratio` is the share of queued operations that expired;
- `average_wait_limit_ratio` and `longest_wait_limit_ratio` compare admitted waits
  with the configured `wait_limit`.

Shutdown rejections and caller cancellation are excluded from capacity-rejection
ratios because they do not represent dependency pressure.

## Conservative thresholds

Historical percentage findings require enough observations:

- rejection findings require at least 20 capacity decisions;
- expiration findings require at least 10 queued operations;
- wait-time findings require at least 5 admissions from the queue.

Current queue pressure uses the live snapshot. A waiting room is considered near
capacity at 80%. Historical queueing is considered frequent when at least half of
open admission attempts entered the queue.

A waiting room is reported as unusually large when it has at least 100 places and is
at least 20 times larger than execution capacity. This is an advisory observation,
not an automatic instruction to reduce the queue.

After a capacity reduction, the report distinguishes temporary active work above the
new limit from ordinary execution saturation. Existing work is allowed to drain rather
than being cancelled.

## Interpretation limits

Reports use cumulative history rather than a rolling time window. An application that
needs minute-by-minute trends should export lifecycle events or periodic status values
to its monitoring system.

A report explains observed pressure but does not decide that increasing concurrency is
safe. The protected database, API, connection pool, or other dependency remains the
source of truth for safe capacity.
