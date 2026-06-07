# Bulkhead isolation

A ship uses sealed compartments so one leak does not sink the whole vessel. A
software bulkhead applies the same idea to runtime capacity.

```python
payments = AsyncBulkhead(label="payments", parallelism=10)
emails = AsyncBulkhead(label="emails", parallelism=3)
reports = AsyncBulkhead(label="reports", parallelism=2)
```

If report generation becomes slow, it can occupy at most two report slots. Payment
capacity remains independent.

Bulklink controls simultaneous in-flight operations. It does not control requests
per second and does not retry failures.
