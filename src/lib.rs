//! Transactional Outbox mechanics for Module-owned `PostgreSQL` state.
//!
//! The owning Module installs [`POSTGRES_MIGRATION_SQL`] in its private schema,
//! calls [`OutboxStore::enqueue`] through the same transaction as its business
//! state, and runs [`OutboxRelay`] with an explicit [`OutboxSink`].
//!
//! Delivery is at least once. A sink can observe the same [`EventId`] more than
//! once if a process stops after external delivery but before the lease is
//! acknowledged.

mod error;
mod migration;
mod model;
mod relay;
mod store;

pub use error::{OutboxError, OutboxResult};
pub use migration::POSTGRES_MIGRATION_SQL;
pub use model::{
    ClaimedEvent, DeliveryFailure, DeliveryOutcome, EventId, NewEvent, OutboxEvidence, OutboxState,
};
pub use relay::{DrainReport, OutboxRelay, OutboxSink, RelayConfig};
pub use store::{EnqueueOutcome, OutboxStore};
