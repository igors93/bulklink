# Status and metrics

```python
current = await payments.status()
```

Useful fields:

- `in_flight`
- `waiting`
- `free_slots`
- `is_saturated`
- `admitted_total`
- `admitted_from_queue_total`
- `queued_total`
- `rejected_total`
- `expired_total`
- `cancelled_total`
- `finished_total`
- `peak_in_flight`
- `peak_waiting`
- `average_wait_seconds`
- `longest_wait_seconds`

The status is immutable and represents one instant. Bulklink does not create a
background metrics task; applications decide how and when to export it.
