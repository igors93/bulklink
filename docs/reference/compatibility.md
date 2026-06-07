# Compatibility policy

Starting with 1.0, names exported by `bulklink.__all__` are the stable public API.

Before 1.0, minor releases may adjust behavior or naming when necessary. Changes must
be documented in the changelog.

Private modules under `bulklink._internal` may change without deprecation.

Important behavioral contracts:

- bounded in-flight work;
- bounded FIFO waiting;
- no slot leaks after cancellation or exceptions;
- no automatic retries;
- active work is not cancelled by `close()`;
- queued and future work is rejected after `close()`;
- `wait_closed()` completes only after closing and active-work drainage;
- cancelling a shutdown waiter does not cancel protected work;
- event handlers run outside coordinator locks;
- handler failures do not change protected operation outcomes or capacity state;
- event payloads exclude operation arguments, results, and exceptions;
- capacity reports are immutable and never alter admission or configuration.
