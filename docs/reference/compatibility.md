# Compatibility policy

Bulklink supports Python 3.10 through 3.14. The CI matrix runs the full test suite on
each supported version, while release verification installs the built wheel into a clean
virtual environment and checks its public typing contract.

Starting with 1.0, names exported by `bulklink.__all__` are the stable public API.

Before 1.0, minor releases may adjust behavior or naming when necessary. Changes must
be documented in the changelog.

During a release-candidate cycle, new public features are frozen. Candidate updates
should contain only defect fixes, security hardening, documentation corrections, and
release-process changes needed to validate the same intended public contract.

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
- capacity reports are immutable and never alter admission or configuration;
- capacity increases preserve FIFO order;
- capacity reductions never cancel active work;
- closed bulkheads cannot be resized or reopened;
- registry names are unique and never silently replaced;
- registry removal closes and drains before deleting membership;
- collective registry shutdown prevents new creation and attempts every member.
