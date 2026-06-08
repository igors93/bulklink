# Weighted capacity

`AsyncBulkhead` treats every admitted operation as one slot. `WeightedBulkhead` is for cases
where operations have different known integer costs.

```python
from bulklink import WeightedBulkhead

reports = WeightedBulkhead(
    label="reports",
    capacity=10,
    waiting_room=20,
    wait_limit=1.0,
)

async with reports.slot(4):
    await generate_large_report()

summary = await reports.execute(2, load_summary)
```

## Capacity and cost

- `capacity` is the total number of available units;
- each operation requests a positive integer `cost`;
- admitted costs may sum up to `capacity`;
- `waiting_room` limits the number of waiting operations, not their total cost;
- a cost greater than the current capacity is rejected with `ValueError` because it can
  never be admitted.

The default cost for context-manager methods is one:

```python
async with reports.slot():
    await small_operation()
```

Execution helpers keep the cost explicit:

```python
await reports.execute(3, operation, argument)
await reports.execute_now(3, operation, argument)
await reports.execute_within(0.5, 3, operation, argument)
await reports.execute_before(deadline, 3, operation, argument)
```

Admission deadlines limit waiting only. Once admitted, Bulklink does not cancel the
protected operation.

## Strict FIFO

Weighted admission remains strict FIFO. Suppose two units are currently free:

```text
first waiter needs 3 units
second waiter needs 1 unit
```

The second waiter does not overtake the first. This may leave capacity temporarily unused,
but it makes admission predictable and prevents large requests from starving.

There are no priorities and no configurable queue policy.

## Resizing

Increasing capacity admits as many consecutive FIFO entries as fit.

Reducing capacity never cancels active work. The status may temporarily report
`used > capacity` while existing work drains.

A reduction below the largest queued operation cost is rejected. Without this rule, a
queued operation could become permanently impossible to admit and block every later FIFO
entry.

## Status and intervals

```python
before = await reports.status()

await reports.execute(4, generate_report)

after = await reports.status()
interval = after.since(before)
```

`WeightedBulkheadStatus` exposes operation counts and unit accounting, including:

- `capacity`, `used`, and `available`;
- `in_flight`, `waiting`, and `waiting_units`;
- admitted, queued, finished, and abandoned unit totals;
- peak active and waiting units;
- queue wait totals and averages.

Snapshots contain an opaque instance identity and sequence number. Cross-instance and
reversed comparisons are rejected.

## Events

`WeightedBulkheadEvent` uses the existing `BulkheadEventKind` lifecycle categories and adds
weighted metadata such as `capacity`, `used`, and `cost`. Handlers are synchronous, run
outside the coordinator lock, and never receive operation arguments, results, or exceptions.

## Boundaries

Weighted capacity does not add:

- priority queues;
- automatic cost estimation;
- fractional costs;
- rate limiting;
- retry or backoff;
- distributed coordination;
- automatic resizing.

The application chooses costs and capacity explicitly.
