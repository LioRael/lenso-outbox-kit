use std::time::Duration;

use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    ClaimedEvent, DeliveryFailure, EventId, NewEvent, OutboxError, OutboxEvidence, OutboxResult,
    OutboxState,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueOutcome {
    Inserted,
    AlreadyPresent,
}

#[derive(Clone, Debug)]
pub struct OutboxStore {
    pool: PgPool,
}

impl OutboxStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Persist an event through the caller's active business transaction.
    ///
    /// Supplying the same identity and immutable content is idempotent; reusing
    /// an identity with different content fails closed.
    ///
    /// A raw pooled connection is rejected at compile time:
    ///
    /// ```compile_fail,E0308
    /// use lenso_outbox_kit::{NewEvent, OutboxStore};
    ///
    /// # async fn raw_connection_is_rejected(
    /// #     connection: &mut sqlx::PgConnection,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// let event = NewEvent::new(
    ///     "orders.order-accepted.v1",
    ///     "application/json",
    ///     br#"{\"orderId\":\"order-42\"}"#.to_vec(),
    /// )?;
    /// OutboxStore::enqueue(connection, &event).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue(
        transaction: &mut Transaction<'_, Postgres>,
        event: &NewEvent,
    ) -> OutboxResult<EnqueueOutcome> {
        let metadata = Value::Object(event.metadata().clone());
        let inserted = sqlx::query(
            r"
            INSERT INTO lenso_outbox_events (
                event_id,
                event_type,
                subject,
                content_type,
                payload,
                metadata,
                correlation_id,
                causation_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (event_id) DO NOTHING
            ",
        )
        .bind(event.id().as_uuid())
        .bind(event.event_type())
        .bind(event.subject())
        .bind(event.content_type())
        .bind(event.payload())
        .bind(&metadata)
        .bind(event.correlation_id())
        .bind(event.causation_id())
        .execute(&mut **transaction)
        .await?;

        if inserted.rows_affected() == 1 {
            return Ok(EnqueueOutcome::Inserted);
        }

        let existing = sqlx::query(
            r"
            SELECT event_type, subject, content_type, payload, metadata,
                   correlation_id, causation_id
            FROM lenso_outbox_events
            WHERE event_id = $1
            ",
        )
        .bind(event.id().as_uuid())
        .fetch_one(&mut **transaction)
        .await?;

        let same_content = existing.try_get::<String, _>("event_type")? == event.event_type()
            && existing.try_get::<Option<String>, _>("subject")?.as_deref() == event.subject()
            && existing.try_get::<String, _>("content_type")? == event.content_type()
            && existing.try_get::<Vec<u8>, _>("payload")? == event.payload()
            && existing.try_get::<Value, _>("metadata")? == metadata
            && existing
                .try_get::<Option<String>, _>("correlation_id")?
                .as_deref()
                == event.correlation_id()
            && existing
                .try_get::<Option<String>, _>("causation_id")?
                .as_deref()
                == event.causation_id();

        if same_content {
            Ok(EnqueueOutcome::AlreadyPresent)
        } else {
            Err(OutboxError::EventIdentityConflict(event.id()))
        }
    }

    pub async fn state(&self, id: EventId) -> OutboxResult<Option<OutboxState>> {
        Ok(self.evidence(id).await?.map(|evidence| evidence.state))
    }

    /// Read operator-safe delivery evidence without exposing payload or lease
    /// tokens.
    pub async fn evidence(&self, id: EventId) -> OutboxResult<Option<OutboxEvidence>> {
        let row = sqlx::query(
            r"
            SELECT state,
                   attempt_count,
                   replay_count,
                   last_error_code,
                   last_error_detail,
                   available_at,
                   lease_expires_at,
                   created_at,
                   delivered_at
            FROM lenso_outbox_events
            WHERE event_id = $1
            ",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| row_to_evidence(id, &row)).transpose()
    }

    /// Explicitly schedule one dead letter for a new delivery cycle.
    ///
    /// Returns `false` when the identity is unknown or is not currently dead.
    pub async fn requeue_dead_letter(&self, id: EventId) -> OutboxResult<bool> {
        let result = sqlx::query(
            r"
            UPDATE lenso_outbox_events
            SET state = 'pending',
                attempt_count = 0,
                replay_count = replay_count + 1,
                available_at = clock_timestamp(),
                lease_token = NULL,
                lease_expires_at = NULL,
                dead_letter_reason = NULL,
                delivered_at = NULL
            WHERE event_id = $1 AND state = 'dead'
            ",
        )
        .bind(id.as_uuid())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn claim_one(
        &self,
        excluded: &[EventId],
        lease_duration: Duration,
    ) -> OutboxResult<Option<Lease>> {
        let lease_token = Uuid::now_v7();
        let lease_millis = duration_millis(lease_duration)?;
        let excluded = excluded.iter().map(|id| id.as_uuid()).collect::<Vec<_>>();
        let rows = sqlx::query(
            r"
            WITH candidates AS (
                SELECT event_id
                FROM lenso_outbox_events
                WHERE
                    (
                        (state = 'pending' AND available_at <= clock_timestamp())
                        OR
                        (state = 'leased' AND lease_expires_at <= clock_timestamp())
                    )
                    AND NOT (event_id = ANY($1::uuid[]))
                ORDER BY available_at, created_at, event_id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE lenso_outbox_events AS event
            SET state = 'leased',
                lease_token = $2,
                lease_expires_at = clock_timestamp()
                    + ($3::double precision * interval '1 millisecond'),
                attempt_count = event.attempt_count + 1
            FROM candidates
            WHERE event.event_id = candidates.event_id
            RETURNING event.event_id,
                      event.event_type,
                      event.subject,
                      event.content_type,
                      event.payload,
                      event.metadata,
                      event.correlation_id,
                      event.causation_id,
                      event.attempt_count,
                      event.replay_count,
                      event.created_at,
                      event.lease_token
            ",
        )
        .bind(excluded)
        .bind(lease_token)
        .bind(lease_millis)
        .fetch_all(&self.pool)
        .await?;
        rows.first().map(row_to_lease).transpose()
    }

    pub(crate) async fn acknowledge(&self, lease: &Lease) -> OutboxResult<()> {
        let result = sqlx::query(
            r"
            UPDATE lenso_outbox_events
            SET state = 'delivered',
                lease_token = NULL,
                lease_expires_at = NULL,
                delivered_at = clock_timestamp(),
                dead_letter_reason = NULL
            WHERE event_id = $1 AND state = 'leased' AND lease_token = $2
            ",
        )
        .bind(lease.event.id.as_uuid())
        .bind(lease.token)
        .execute(&self.pool)
        .await?;
        require_current_lease(result.rows_affected(), lease.event.id)
    }

    pub(crate) async fn schedule_retry(
        &self,
        lease: &Lease,
        failure: &DeliveryFailure,
        delay: Duration,
    ) -> OutboxResult<()> {
        let delay_millis = duration_millis(delay)?;
        let result = sqlx::query(
            r"
            UPDATE lenso_outbox_events
            SET state = 'pending',
                available_at = clock_timestamp()
                    + ($3::double precision * interval '1 millisecond'),
                lease_token = NULL,
                lease_expires_at = NULL,
                last_error_code = $4,
                last_error_detail = $5
            WHERE event_id = $1 AND state = 'leased' AND lease_token = $2
            ",
        )
        .bind(lease.event.id.as_uuid())
        .bind(lease.token)
        .bind(delay_millis)
        .bind(failure.code())
        .bind(failure.detail())
        .execute(&self.pool)
        .await?;
        require_current_lease(result.rows_affected(), lease.event.id)
    }

    pub(crate) async fn dead_letter(
        &self,
        lease: &Lease,
        failure: &DeliveryFailure,
    ) -> OutboxResult<()> {
        let result = sqlx::query(
            r"
            UPDATE lenso_outbox_events
            SET state = 'dead',
                lease_token = NULL,
                lease_expires_at = NULL,
                last_error_code = $3,
                last_error_detail = $4,
                dead_letter_reason = $3
            WHERE event_id = $1 AND state = 'leased' AND lease_token = $2
            ",
        )
        .bind(lease.event.id.as_uuid())
        .bind(lease.token)
        .bind(failure.code())
        .bind(failure.detail())
        .execute(&self.pool)
        .await?;
        require_current_lease(result.rows_affected(), lease.event.id)
    }
}

#[derive(Debug)]
pub(crate) struct Lease {
    pub event: ClaimedEvent,
    token: Uuid,
}

fn row_to_lease(row: &sqlx::postgres::PgRow) -> OutboxResult<Lease> {
    let metadata = row.try_get::<Value, _>("metadata")?;
    let Value::Object(metadata) = metadata else {
        return Err(OutboxError::Persistence(sqlx::Error::Protocol(
            "outbox metadata is not a JSON object".to_owned(),
        )));
    };
    let attempt = decode_count(row, "attempt_count", "attempt")?;
    let replay_count = decode_count(row, "replay_count", "replay")?;

    Ok(Lease {
        event: ClaimedEvent {
            id: EventId::from_uuid(row.try_get("event_id")?),
            event_type: row.try_get("event_type")?,
            subject: row.try_get("subject")?,
            content_type: row.try_get("content_type")?,
            payload: row.try_get("payload")?,
            metadata,
            correlation_id: row.try_get("correlation_id")?,
            causation_id: row.try_get("causation_id")?,
            attempt,
            replay_count,
            created_at: row.try_get::<OffsetDateTime, _>("created_at")?,
        },
        token: row.try_get("lease_token")?,
    })
}

fn row_to_evidence(id: EventId, row: &sqlx::postgres::PgRow) -> OutboxResult<OutboxEvidence> {
    let attempt_count = decode_count(row, "attempt_count", "attempt")?;
    let replay_count = decode_count(row, "replay_count", "replay")?;
    let failure_code = row.try_get::<Option<String>, _>("last_error_code")?;
    let failure_detail = row.try_get::<Option<String>, _>("last_error_detail")?;
    let last_failure = match (failure_code, failure_detail) {
        (Some(code), Some(detail)) => Some(DeliveryFailure::new(code, detail)?),
        (None, None) => None,
        _ => {
            return Err(OutboxError::Persistence(sqlx::Error::Protocol(
                "outbox failure evidence is incomplete".to_owned(),
            )));
        }
    };

    Ok(OutboxEvidence {
        id,
        state: parse_state(&row.try_get::<String, _>("state")?)?,
        attempt_count,
        replay_count,
        last_failure,
        available_at: row.try_get("available_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        created_at: row.try_get("created_at")?,
        delivered_at: row.try_get("delivered_at")?,
    })
}

fn decode_count(row: &sqlx::postgres::PgRow, column: &str, name: &str) -> OutboxResult<u32> {
    u32::try_from(row.try_get::<i32, _>(column)?).map_err(|_| {
        OutboxError::Persistence(sqlx::Error::Protocol(format!(
            "outbox {name} count is outside u32"
        )))
    })
}

fn duration_millis(duration: Duration) -> OutboxResult<f64> {
    let millis = duration.as_secs_f64() * 1000.0;
    if !millis.is_finite() || millis <= 0.0 {
        return Err(OutboxError::InvalidConfiguration(
            "lease and retry durations must be finite and greater than zero".to_owned(),
        ));
    }
    Ok(millis)
}

fn parse_state(value: &str) -> OutboxResult<OutboxState> {
    match value {
        "pending" => Ok(OutboxState::Pending),
        "leased" => Ok(OutboxState::Leased),
        "delivered" => Ok(OutboxState::Delivered),
        "dead" => Ok(OutboxState::Dead),
        other => Err(OutboxError::Persistence(sqlx::Error::Protocol(format!(
            "unsupported outbox state {other}"
        )))),
    }
}

fn require_current_lease(rows_affected: u64, id: EventId) -> OutboxResult<()> {
    if rows_affected == 1 {
        Ok(())
    } else {
        Err(OutboxError::StaleLease(id))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::duration_millis;

    #[test]
    fn zero_duration_is_rejected() {
        assert!(duration_millis(Duration::ZERO).is_err());
    }
}
