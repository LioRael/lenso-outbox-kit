use thiserror::Error;

use crate::EventId;

pub type OutboxResult<T> = Result<T, OutboxError>;

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error("invalid outbox configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid outbox event: {0}")]
    InvalidEvent(String),
    #[error("event identity {0} already exists with different immutable content")]
    EventIdentityConflict(EventId),
    #[error("outbox lease for event {0} is no longer current")]
    StaleLease(EventId),
    #[error("outbox persistence failed: {0}")]
    Persistence(#[from] sqlx::Error),
    #[error("outbox payload serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}
