# Examples

Run examples from the repository root after installing the project:

```bash
python -m examples.basic
python -m examples.isolated_services
python -m examples.overload_handling
python -m examples.absolute_deadline
python -m examples.interval_metrics
python -m examples.weighted_capacity
python -m examples.partitioned_isolation
python -m examples.graceful_shutdown
python -m examples.dynamic_capacity
python -m examples.observability
python -m examples.registry
```

Every example is executed by the test suite and must finish without network access,
secrets, or pending background tasks.
