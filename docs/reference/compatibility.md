# Compatibility policy

Bulklink supports Python 3.10 through 3.14. The CI matrix runs the full test suite on
each supported version, with additional Windows and macOS validation. Release
verification installs the built wheel into a clean virtual environment and checks its
public typing and runtime contracts.

## Stable `0.3.x` contract

Starting with `0.3.0`, patch releases preserve the documented public contract:

- names and order exported by `bulklink.__all__`;
- values of public enums;
- fields of public frozen dataclasses;
- inheritance relationships of public exceptions;
- parameter names and calling conventions of the primary public methods;
- the behavioral guarantees listed below.

A future minor release may intentionally extend or revise the pre-1.0 API, but every
such change must be documented in the changelog. Private modules under
`bulklink._internal` may change without deprecation.

Release candidates freeze new public features. Candidate updates should contain only
defect fixes, security hardening, documentation corrections, compatibility work, and
release-process changes needed to validate the intended contract.

## Behavioral guarantees

- bounded in-flight work;
- bounded FIFO waiting;
- absolute admission deadlines use the owning event loop's monotonic clock;
- an expired absolute deadline never enters the waiting room;
- admission deadlines never cancel work after admission;
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
