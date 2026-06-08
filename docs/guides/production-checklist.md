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
11. Use `close_and_wait()` during application shutdown.
12. Test cancellation of shutdown waiters without cancelling protected work.
13. Avoid automatically retrying overload rejections.
14. Load test with realistic latency and error rates.
15. Treat capacity reductions as gradual drains and monitor `is_over_capacity`.
16. When using a registry, stop creation before shutdown and call `close_and_wait()`.
A large waiting room does not create capacity. It stores more delayed work and uses
more memory.
