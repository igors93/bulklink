# Production checklist

Before deployment:

1. Create one bulkhead per failure domain rather than one global bulkhead.
2. Align `parallelism` with the protected resource or dependency capacity.
3. Keep `waiting_room` bounded.
4. Prefer a finite `wait_limit` in request/response systems.
5. Configure network and database timeouts separately.
6. Decide how saturation and queue expiration are translated at the application edge.
7. Export `status()` values to metrics.
8. Test task cancellation and application shutdown.
9. Avoid automatically retrying overload rejections.
10. Load test with realistic latency and error rates.

A large waiting room does not create capacity. It stores more delayed work and uses
more memory.
