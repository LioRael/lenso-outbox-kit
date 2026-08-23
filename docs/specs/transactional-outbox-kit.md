# Transactional Outbox Kit

## Outcome

A stateful Lenso Module can commit one business transition and the event that
describes it atomically, then deliver that event at least once without giving
another Module access to its private tables.

## Ownership assumptions

- The emitting Module is authoritative for business state, event meaning,
  payload compatibility, authorization, and retention.
- The emitting Module owns one PostgreSQL schema and role through
  `lenso-postgres-kit`.
- A concrete delivery Adapter owns broker or remote-system protocol and
  infrastructure policy.
- Consumers own idempotent effects keyed by the immutable Outbox event ID.
- The kit owns conservative envelope resource bounds: 1 MiB payloads and
  16 KiB metadata measured by compact JSON encoding at the public constructor.
  Larger content remains in Module-owned storage and travels by reference so
  one generic relay cannot accumulate unbounded row payloads.

A standalone shared Outbox Module is rejected because a Capability call cannot
join the emitting Module's local SQL transaction. A storage abstraction is
also rejected in the first slice because PostgreSQL transaction, row locking,
and lease behavior are the capability rather than replaceable details.

## Shape and dependency direction

```text
Owning business Module
  -> lenso-postgres-kit (schema lifecycle)
  -> lenso-outbox-kit (transactional persistence and relay)
       -> caller-supplied OutboxSink Adapter
            -> broker, HTTP endpoint, or other delivery target
```

Neither `lenso-kernel` nor a Runtime Driver depends on the kit.

## First useful slice

1. A Module writes business state and a versioned event in one transaction.
2. Rollback removes both facts; commit exposes both.
3. A relay claims pending events with an expiring fenced lease.
4. A retryable failure schedules bounded exponential retry with the same event
   ID and payload.
5. A permanent failure or exhausted retry budget creates a dead letter.
6. An operator explicitly requeues the dead letter into a new delivery cycle.
7. A stale worker cannot acknowledge a lease reclaimed by another worker.

One relay claims immediately before each delivery and processes at most the
configured number per drain. Multiple relay instances may claim concurrently;
the sink itself does not need to be thread-safe, preserving single-owner Lenso
Module implementations.

## Capability ownership

| Capability | Owner | Authoritative data | Public surface | Collaboration | Reason |
|---|---|---|---|---|---|
| Transactional enqueue | Emitting Module through the kit | Event identity, immutable envelope, delivery state in the Module schema | `NewEvent`, `OutboxStore::enqueue` | Caller passes its active `PgConnection` | Preserves one local transaction and private schema |
| Relay lifecycle | Emitting Module through the kit | Lease, attempt, retry, delivery, and dead-letter state | `OutboxRelay::drain_once`, `RelayConfig` | Calls one explicit `OutboxSink` | Keeps scheduling and transport outside Kernel |
| Delivery | Concrete Adapter | External acknowledgement and infrastructure policy | `OutboxSink`, classified `DeliveryOutcome` | Receives `ClaimedEvent` | Transport failures need adapter-owned classification |
| Recovery | Emitting Module operator path | Replay count and current delivery state | `OutboxStore::state`, `requeue_dead_letter` | Explicit operator action | Prevents hidden or automatic replay |

## Verification and operational evidence

- Pure tests cover validation and bounded retry timing.
- PostgreSQL 18 acceptance tests prove atomic commit/rollback, exact duplicate
  enqueue, identity conflict, retry, dead letter, requeue, lease recovery, and
  stale-ack fencing.
- `DrainReport`, `OutboxState`, attempt count, replay count, and classified
  failure evidence are the first operational surface.
- No Console Surface is declared in this slice. A future operator Module may
  project these records through its own authorized Capability; it must not
  query another Module's Outbox table.

## Authoring handoff

Implement as a linked Rust support kit following `lenso-module-authoring` for
the owning Module's vertical integration. Concrete broker delivery belongs in
an Adapter repository when that transport is selected.
