use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{OutboxError, OutboxResult};

const MAX_EVENT_TYPE_BYTES: usize = 255;
const MAX_SUBJECT_BYTES: usize = 512;
const MAX_CONTENT_TYPE_BYTES: usize = 127;
const MAX_REFERENCE_BYTES: usize = 255;
const MAX_FAILURE_CODE_BYTES: usize = 127;
const MAX_FAILURE_DETAIL_BYTES: usize = 2048;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_METADATA_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(Uuid);

impl EventId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("EventId").field(&self.0).finish()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewEvent {
    id: EventId,
    event_type: String,
    subject: Option<String>,
    content_type: String,
    payload: Vec<u8>,
    metadata: Map<String, Value>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

impl NewEvent {
    pub fn new(
        event_type: impl Into<String>,
        content_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> OutboxResult<Self> {
        let event = Self {
            id: EventId::new(),
            event_type: event_type.into(),
            subject: None,
            content_type: content_type.into(),
            payload: payload.into(),
            metadata: Map::new(),
            correlation_id: None,
            causation_id: None,
        };
        event.validate()?;
        Ok(event)
    }

    pub fn json(event_type: impl Into<String>, payload: &impl Serialize) -> OutboxResult<Self> {
        Self::new(event_type, "application/json", serde_json::to_vec(payload)?)
    }

    #[must_use]
    pub fn with_id(mut self, id: EventId) -> Self {
        self.id = id;
        self
    }

    pub fn with_subject(mut self, subject: impl Into<String>) -> OutboxResult<Self> {
        self.subject = Some(subject.into());
        self.validate()?;
        Ok(self)
    }

    pub fn with_metadata(mut self, metadata: Map<String, Value>) -> OutboxResult<Self> {
        self.metadata = metadata;
        self.validate()?;
        Ok(self)
    }

    pub fn with_correlation_id(mut self, value: impl Into<String>) -> OutboxResult<Self> {
        self.correlation_id = Some(value.into());
        self.validate()?;
        Ok(self)
    }

    pub fn with_causation_id(mut self, value: impl Into<String>) -> OutboxResult<Self> {
        self.causation_id = Some(value.into());
        self.validate()?;
        Ok(self)
    }

    #[must_use]
    pub const fn id(&self) -> EventId {
        self.id
    }

    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    #[must_use]
    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    #[must_use]
    pub const fn metadata(&self) -> &Map<String, Value> {
        &self.metadata
    }

    #[must_use]
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    #[must_use]
    pub fn causation_id(&self) -> Option<&str> {
        self.causation_id.as_deref()
    }

    fn validate(&self) -> OutboxResult<()> {
        validate_text("event type", &self.event_type, MAX_EVENT_TYPE_BYTES)?;
        validate_text("content type", &self.content_type, MAX_CONTENT_TYPE_BYTES)?;
        validate_optional("subject", self.subject.as_deref(), MAX_SUBJECT_BYTES)?;
        validate_optional(
            "correlation id",
            self.correlation_id.as_deref(),
            MAX_REFERENCE_BYTES,
        )?;
        validate_optional(
            "causation id",
            self.causation_id.as_deref(),
            MAX_REFERENCE_BYTES,
        )?;
        if self.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(OutboxError::InvalidEvent(format!(
                "payload cannot exceed {MAX_PAYLOAD_BYTES} bytes"
            )));
        }
        let metadata_bytes = serde_json::to_vec(&self.metadata)?.len();
        if metadata_bytes > MAX_METADATA_BYTES {
            return Err(OutboxError::InvalidEvent(format!(
                "metadata cannot exceed {MAX_METADATA_BYTES} encoded JSON bytes"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxState {
    Pending,
    Leased,
    Delivered,
    Dead,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxEvidence {
    pub id: EventId,
    pub state: OutboxState,
    pub attempt_count: u32,
    pub replay_count: u32,
    pub last_failure: Option<DeliveryFailure>,
    pub available_at: OffsetDateTime,
    pub lease_expires_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub delivered_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedEvent {
    pub id: EventId,
    pub event_type: String,
    pub subject: Option<String>,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub metadata: Map<String, Value>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub attempt: u32,
    pub replay_count: u32,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryFailure {
    code: String,
    detail: String,
}

impl DeliveryFailure {
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> OutboxResult<Self> {
        let failure = Self {
            code: code.into(),
            detail: detail.into(),
        };
        validate_text(
            "delivery failure code",
            &failure.code,
            MAX_FAILURE_CODE_BYTES,
        )?;
        validate_text(
            "delivery failure detail",
            &failure.detail,
            MAX_FAILURE_DETAIL_BYTES,
        )?;
        Ok(failure)
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryOutcome {
    Delivered,
    Retryable(DeliveryFailure),
    Permanent(DeliveryFailure),
}

fn validate_optional(name: &str, value: Option<&str>, max_bytes: usize) -> OutboxResult<()> {
    if let Some(value) = value {
        validate_text(name, value, max_bytes)?;
    }
    Ok(())
}

fn validate_text(name: &str, value: &str, max_bytes: usize) -> OutboxResult<()> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(OutboxError::InvalidEvent(format!(
            "{name} must contain between 1 and {max_bytes} UTF-8 bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::{DeliveryFailure, EventId, MAX_METADATA_BYTES, NewEvent};

    #[test]
    fn event_identity_can_be_supplied_for_idempotent_enqueue() {
        let id = EventId::new();
        let event = NewEvent::new("orders.accepted.v1", "application/json", b"{}".to_vec())
            .expect("valid event")
            .with_id(id);

        assert_eq!(event.id(), id);
    }

    #[test]
    fn empty_event_types_and_failure_details_are_rejected() {
        assert!(NewEvent::new("", "application/json", Vec::new()).is_err());
        assert!(DeliveryFailure::new("network", "").is_err());
    }

    #[test]
    fn metadata_limit_uses_compact_json_encoding() {
        let event = NewEvent::new("orders.accepted.v1", "application/json", Vec::new())
            .expect("valid event");
        let mut exact = Map::new();
        exact.insert(
            "x".to_owned(),
            Value::String("x".repeat(MAX_METADATA_BYTES - 8)),
        );
        assert_eq!(
            serde_json::to_vec(&exact)
                .expect("serialize metadata")
                .len(),
            MAX_METADATA_BYTES
        );
        assert!(event.clone().with_metadata(exact).is_ok());

        let mut oversized = Map::new();
        oversized.insert(
            "x".to_owned(),
            Value::String("x".repeat(MAX_METADATA_BYTES - 7)),
        );
        assert!(event.with_metadata(oversized).is_err());
    }
}
