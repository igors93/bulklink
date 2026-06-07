# Production checklist

Before deployment:

1. Create one bulkhead per failure domain rather than one global bulkhead.
2. Align `parallelism` with the protected resource or dependency capacity.
3. Keep `waiting_room` bounded.
4. Prefer a finite `wait_limit` in request/response systems.
5. Configure network and database timeouts separately.
6. Decide how saturation and queue expiration are translated at the application edge.
7. Export `status()` values to metrics and review `capacity_report()` findings.
8. Keep event handlers synchronous, fast, and free of blocking I/O.
9. Forward events with non-blocking queue operations when asynchronous export is needed.
10. Use `close_and_wait()` during application shutdown.
11. Test cancellation of shutdown waiters without cancelling protected work.
12. Avoid automatically retrying overload rejections.
13. Load test with realistic latency and error rates.
14. Treat capacity reductions as gradual drains and monitor `is_over_capacity`.
15. When using a registry, stop creation before shutdown and call `close_and_wait()`.

A large waiting room does not create capacity. It stores more delayed work and uses
more memory.
