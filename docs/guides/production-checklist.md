# Production checklist

Before deployment:

1. Create one bulkhead per failure domain rather than one global bulkhead.
2. Align `parallelism` with the protected resource or dependency capacity.
3. Keep `waiting_room` bounded.
4. Prefer a finite `wait_limit` in request/response systems.
5. Propagate an absolute event-loop deadline when a request already has a total budget.
6. Configure network and database timeouts separately.
7. Decide how saturation and queue expiration are translated at the application edge.
8. Export `status()` values, compare snapshots with `since()`, and review `capacity_report()` findings.
9. Keep event handlers synchronous, fast, and free of blocking I/O.
10. Forward events with non-blocking queue operations when asynchronous export is needed.
11. Keep weighted costs small, documented, and based on stable application knowledge.
12. Never use weighted costs as priorities; FIFO remains strict.
13. Use `close_and_wait()` during application shutdown.
12. Test cancellation of shutdown waiters without cancelling protected work.
13. Avoid automatically retrying overload rejections.
14. Load test with realistic latency and error rates.
15. Treat capacity reductions as gradual drains and monitor `is_over_capacity`.
16. When using a registry, stop creation before shutdown and call `close_and_wait()`.
A large waiting room does not create capacity. It stores more delayed work and uses
more memory.


## Partitioned isolation

- Set `max_partitions` from a realistic cardinality budget.
- Use immutable, stable, hashable partition keys.
- Keep keys free of secrets when possible even though Bulklink does not render them.
- Call `cleanup_idle()` from an application-owned maintenance loop when normal TTL cleanup is desired.
- Treat `PartitionLimitError` as local admission pressure, not as a remote network failure.
- Understand that `parallelism` and `waiting_room` are **per-partition** limits, not global ones.
  The theoretical maximum concurrent operations across all partitions is
  `parallelism × max_partitions`; the theoretical maximum queued operations is
  `waiting_room × max_partitions`. Size both values with this envelope in mind when
  a shared downstream resource has a fixed concurrency limit.
- Do not rely on `max_partitions` alone to protect a downstream resource from overload.
  A small `parallelism` per partition is the right lever for global concurrency control.
- Bulklink is a concurrency limiter, not a rate limiter. It does not enforce
  requests per second; it bounds how many operations may run or wait simultaneously.
- During an LRU eviction, `available_partition_slots` and `is_at_limit` in a status
  snapshot may transiently reflect only materialized partitions (excluding a pending
  replacement reservation). Treat these as informational, not admission guarantees.
