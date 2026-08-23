use std::time::Duration;

use futures::future::LocalBoxFuture;
use sqlx::PgPool;

use crate::{ClaimedEvent, DeliveryOutcome, OutboxError, OutboxResult, OutboxStore, store::Lease};

/// Concrete transport role used by an owning Module's Outbox relay.
///
/// Implementations must classify every delivery failure. Returned details are
/// persisted as operator evidence and must already be safe to store.
pub trait OutboxSink: std::fmt::Debug {
    fn deliver<'a>(&'a self, event: &'a ClaimedEvent) -> LocalBoxFuture<'a, DeliveryOutcome>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelayConfig {
    pub batch_size: u32,
    pub lease_duration: Duration,
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            lease_duration: Duration::from_secs(30),
            max_attempts: 8,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_mins(5),
        }
    }
}

impl RelayConfig {
    fn validate(&self) -> OutboxResult<()> {
        if self.batch_size == 0 {
            return Err(OutboxError::InvalidConfiguration(
                "batch size must be greater than zero".to_owned(),
            ));
        }
        if self.max_attempts == 0 {
            return Err(OutboxError::InvalidConfiguration(
                "max attempts must be greater than zero".to_owned(),
            ));
        }
        if self.lease_duration.is_zero()
            || self.initial_backoff.is_zero()
            || self.max_backoff.is_zero()
        {
            return Err(OutboxError::InvalidConfiguration(
                "lease and retry durations must be greater than zero".to_owned(),
            ));
        }
        if self.initial_backoff > self.max_backoff {
            return Err(OutboxError::InvalidConfiguration(
                "initial backoff cannot exceed maximum backoff".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DrainReport {
    pub claimed: u32,
    pub delivered: u32,
    pub retry_scheduled: u32,
    pub dead_lettered: u32,
}

#[derive(Clone, Debug)]
pub struct OutboxRelay {
    store: OutboxStore,
    config: RelayConfig,
}

impl OutboxRelay {
    pub fn new(pool: PgPool, config: RelayConfig) -> OutboxResult<Self> {
        config.validate()?;
        Ok(Self {
            store: OutboxStore::new(pool),
            config,
        })
    }

    /// Claim and process at most one configured batch.
    ///
    /// The method returns after every claimed event reaches a persisted outcome.
    /// Callers own scheduling, cancellation, and shutdown between drain calls.
    pub async fn drain_once<S>(&self, sink: &S) -> OutboxResult<DrainReport>
    where
        S: OutboxSink + ?Sized,
    {
        let mut report = DrainReport::default();
        let mut seen = Vec::new();
        for _ in 0..self.config.batch_size {
            let Some(lease) = self
                .store
                .claim_one(&seen, self.config.lease_duration)
                .await?
            else {
                break;
            };
            seen.push(lease.event.id);
            report.claimed += 1;
            match self.finish_delivery(sink, &lease).await? {
                DeliveryDisposition::Delivered => report.delivered += 1,
                DeliveryDisposition::RetryScheduled => report.retry_scheduled += 1,
                DeliveryDisposition::DeadLettered => report.dead_lettered += 1,
            }
        }
        Ok(report)
    }

    async fn finish_delivery<S>(&self, sink: &S, lease: &Lease) -> OutboxResult<DeliveryDisposition>
    where
        S: OutboxSink + ?Sized,
    {
        match sink.deliver(&lease.event).await {
            DeliveryOutcome::Delivered => {
                self.store.acknowledge(lease).await?;
                Ok(DeliveryDisposition::Delivered)
            }
            DeliveryOutcome::Retryable(failure)
                if lease.event.attempt < self.config.max_attempts =>
            {
                let delay = backoff_for_attempt(&self.config, lease.event.attempt);
                self.store.schedule_retry(lease, &failure, delay).await?;
                Ok(DeliveryDisposition::RetryScheduled)
            }
            DeliveryOutcome::Retryable(failure) | DeliveryOutcome::Permanent(failure) => {
                self.store.dead_letter(lease, &failure).await?;
                Ok(DeliveryDisposition::DeadLettered)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryDisposition {
    Delivered,
    RetryScheduled,
    DeadLettered,
}

fn backoff_for_attempt(config: &RelayConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let multiplier = 1_u32 << exponent;
    config
        .initial_backoff
        .saturating_mul(multiplier)
        .min(config.max_backoff)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{RelayConfig, backoff_for_attempt};

    #[test]
    fn backoff_is_bounded_and_deterministic() {
        let config = RelayConfig {
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(10),
            ..RelayConfig::default()
        };

        assert_eq!(backoff_for_attempt(&config, 1), Duration::from_secs(2));
        assert_eq!(backoff_for_attempt(&config, 2), Duration::from_secs(4));
        assert_eq!(backoff_for_attempt(&config, 3), Duration::from_secs(8));
        assert_eq!(backoff_for_attempt(&config, 4), Duration::from_secs(10));
    }

    #[test]
    fn invalid_configurations_fail_closed() {
        let config = RelayConfig {
            batch_size: 0,
            ..RelayConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
