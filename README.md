# Lenso Outbox Kit

`lenso-outbox-kit` gives a stateful Lenso Module a transactional Outbox inside
the Module's own PostgreSQL schema. It is a library used by the owning Module,
not a shared Outbox Module and not a Kernel feature.

The owning Module writes its business state and Outbox event through the same
SQL transaction. After commit, a relay claims events with expiring leases and
hands them to an explicit delivery Adapter. Delivery is at least once: a crash
after external delivery but before acknowledgement can deliver the same event
again, so consumers must use `EventId` for idempotency.

## Ownership

The business Module owns:

- the schema and PostgreSQL role;
- event meaning, payload compatibility, authorization, and retention;
- the transaction that changes business state and enqueues the event; and
- the concrete delivery Adapter and its external infrastructure.

The kit owns:

- the standard Outbox table and indexes inside that schema;
- immutable event identity and payload persistence;
- safe concurrent relays through `FOR UPDATE SKIP LOCKED`;
- expiring lease fencing;
- bounded exponential retry and dead-letter transitions; and
- explicit operator requeue with replay evidence.

It does not expose another Module's tables, start a global worker, infer Kernel
Event durability, retry unclassified failures, or promise exactly-once
delivery. It also promises no global ordering across independently claimed
events. Payloads are limited to 1 MiB and metadata to 16 KiB so large content
must remain in Module-owned storage and travel by reference.

## First useful workflow

```rust,no_run
use lenso_outbox_kit::{NewEvent, OutboxStore};

# async fn record_order(
#     pool: &sqlx::PgPool,
# ) -> Result<(), Box<dyn std::error::Error>> {
let mut transaction = pool.begin().await?;

sqlx::query("INSERT INTO orders (id, state) VALUES ($1, 'accepted')")
    .bind("order-42")
    .execute(&mut *transaction)
    .await?;

let event = NewEvent::json(
    "orders.order-accepted.v1",
    &serde_json::json!({ "orderId": "order-42" }),
)?
.with_subject("order-42")?;

OutboxStore::enqueue(&mut transaction, &event).await?;
transaction.commit().await?;
# Ok(())
# }
```

Include `POSTGRES_MIGRATION_SQL` as one immutable migration in the owning
Module's `lenso-postgres-kit` `SchemaPlan`. The Module then calls
`OutboxRelay::drain_once` from its own lifecycle task with a bound delivery
Adapter.

## Verification

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

The PostgreSQL acceptance test runs when `LENSO_OUTBOX_TEST_URL` is set. CI
provides PostgreSQL 18 and exercises atomic commit/rollback, retry with stable
identity, lease recovery, dead-lettering, and explicit requeue.
