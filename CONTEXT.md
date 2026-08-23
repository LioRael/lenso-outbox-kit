# Lenso Outbox Kit context

This repository owns reusable transactional Outbox mechanics for stateful
Lenso Modules. Each owning Module installs the table in its own PostgreSQL
schema and enqueues through the same transaction as its business state.

Hard invariants:

- never introduce a global or cross-Module Outbox table;
- never accept a pool when enqueueing transactionally; require the caller's
  active PostgreSQL connection;
- never claim exactly-once delivery;
- preserve one immutable event identity and payload across retries;
- fence acknowledgements with an opaque lease token;
- require delivery failures to be classified retryable or permanent;
- keep transport, broker topology, business event meaning, payload evolution,
  authorization, and retention with the owning Module or Adapter; and
- keep Kernel, Runtime Driver, and Execution Adapter dependencies out of this
  crate.

The first release is PostgreSQL-specific because transaction atomicity is the
purpose of the Module. A hypothetical storage trait would make important SQL
semantics shallow and untestable.
