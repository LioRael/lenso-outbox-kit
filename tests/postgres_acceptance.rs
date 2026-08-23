use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::future::LocalBoxFuture;
use lenso_outbox_kit::{
    ClaimedEvent, DeliveryFailure, DeliveryOutcome, EnqueueOutcome, NewEvent, OutboxError,
    OutboxRelay, OutboxSink, OutboxState, OutboxStore, POSTGRES_MIGRATION_SQL, RelayConfig,
};
use sqlx::{AssertSqlSafe, Executor, PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

#[tokio::test]
async fn business_state_and_event_commit_or_rollback_together() {
    let Some(database) = TestDatabase::prepare().await else {
        return;
    };

    sqlx::query("CREATE TABLE orders (id text PRIMARY KEY, state text NOT NULL)")
        .execute(database.pool())
        .await
        .expect("create business table");
    let event = NewEvent::json(
        "orders.order-accepted.v1",
        &serde_json::json!({"orderId": "order-42"}),
    )
    .expect("valid event")
    .with_subject("order-42")
    .expect("valid subject");

    let mut rolled_back = database.pool().begin().await.expect("begin rollback");
    sqlx::query("INSERT INTO orders (id, state) VALUES ('order-42', 'accepted')")
        .execute(rolled_back.as_mut())
        .await
        .expect("insert business state");
    assert_eq!(
        OutboxStore::enqueue(rolled_back.as_mut(), &event)
            .await
            .expect("enqueue event"),
        EnqueueOutcome::Inserted
    );
    rolled_back.rollback().await.expect("rollback transaction");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM orders")
            .fetch_one(database.pool())
            .await
            .expect("count orders"),
        0
    );
    assert_eq!(
        OutboxStore::new(database.pool().clone())
            .state(event.id())
            .await
            .expect("read event state"),
        None
    );

    let mut committed = database.pool().begin().await.expect("begin commit");
    sqlx::query("INSERT INTO orders (id, state) VALUES ('order-42', 'accepted')")
        .execute(committed.as_mut())
        .await
        .expect("insert business state");
    OutboxStore::enqueue(committed.as_mut(), &event)
        .await
        .expect("enqueue committed event");
    committed.commit().await.expect("commit transaction");

    let mut duplicate = database.pool().begin().await.expect("begin duplicate");
    assert_eq!(
        OutboxStore::enqueue(duplicate.as_mut(), &event)
            .await
            .expect("exact duplicate is idempotent"),
        EnqueueOutcome::AlreadyPresent
    );
    duplicate.rollback().await.expect("rollback duplicate");

    let conflicting = NewEvent::new(
        event.event_type(),
        event.content_type(),
        br#"{"orderId":"different"}"#.to_vec(),
    )
    .expect("valid conflicting event")
    .with_id(event.id())
    .with_subject("order-42")
    .expect("valid subject");
    let mut conflict = database.pool().begin().await.expect("begin conflict");
    let conflict_result = OutboxStore::enqueue(conflict.as_mut(), &conflicting).await;
    assert!(matches!(
        conflict_result,
        Err(OutboxError::EventIdentityConflict(id)) if id == event.id()
    ));
    conflict.rollback().await.expect("rollback conflict");

    assert_eq!(
        OutboxStore::new(database.pool().clone())
            .state(event.id())
            .await
            .expect("read committed event state"),
        Some(OutboxState::Pending)
    );

    database.cleanup().await;
}

#[tokio::test]
async fn retry_preserves_identity_and_dead_letters_until_explicit_requeue() {
    let Some(database) = TestDatabase::prepare().await else {
        return;
    };
    let event = enqueue_committed(database.pool(), "billing.invoice-issued.v1").await;
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let retrying = RecordingSink::new(
        attempts.clone(),
        vec![
            DeliveryOutcome::Retryable(failure("upstream-unavailable")),
            DeliveryOutcome::Retryable(failure("upstream-unavailable")),
        ],
    );
    let relay = relay(database.pool(), 2);

    let first = relay.drain_once(&retrying).await.expect("first drain");
    assert_eq!(first.retry_scheduled, 1);
    tokio::time::sleep(Duration::from_millis(8)).await;
    let second = relay.drain_once(&retrying).await.expect("second drain");
    assert_eq!(second.dead_lettered, 1);

    let store = OutboxStore::new(database.pool().clone());
    assert_eq!(
        store.state(event.id()).await.expect("read dead state"),
        Some(OutboxState::Dead)
    );
    let dead_evidence = store
        .evidence(event.id())
        .await
        .expect("read dead evidence")
        .expect("dead evidence exists");
    assert_eq!(dead_evidence.attempt_count, 2);
    assert_eq!(dead_evidence.replay_count, 0);
    assert_eq!(
        dead_evidence
            .last_failure
            .as_ref()
            .expect("failure evidence")
            .code(),
        "upstream-unavailable"
    );
    let seen = attempts.lock().expect("attempt lock").clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].id, event.id());
    assert_eq!(seen[1].id, event.id());
    assert_eq!((seen[0].attempt, seen[1].attempt), (1, 2));

    assert!(
        store
            .requeue_dead_letter(event.id())
            .await
            .expect("requeue dead letter")
    );
    let delivered = RecordingSink::new(attempts.clone(), vec![DeliveryOutcome::Delivered]);
    let replay_report = relay.drain_once(&delivered).await.expect("deliver replay");
    assert_eq!(replay_report.delivered, 1);
    assert_eq!(
        store.state(event.id()).await.expect("read delivered state"),
        Some(OutboxState::Delivered)
    );
    {
        let seen = attempts.lock().expect("attempt lock");
        assert_eq!(seen[2].id, event.id());
        assert_eq!(seen[2].attempt, 1);
        assert_eq!(seen[2].replay_count, 1);
    }

    let delivered_evidence = store
        .evidence(event.id())
        .await
        .expect("read delivered evidence")
        .expect("delivered evidence exists");
    assert_eq!(delivered_evidence.replay_count, 1);
    assert_eq!(delivered_evidence.state, OutboxState::Delivered);

    let permanent_event = enqueue_committed(database.pool(), "billing.invoice-rejected.v1").await;
    let permanent_sink = RecordingSink::new(
        Arc::new(Mutex::new(Vec::new())),
        vec![DeliveryOutcome::Permanent(failure("invalid-target"))],
    );
    let permanent_report = relay
        .drain_once(&permanent_sink)
        .await
        .expect("dead letter permanent failure");
    assert_eq!(permanent_report.dead_lettered, 1);
    assert_eq!(
        store
            .state(permanent_event.id())
            .await
            .expect("read permanent dead letter"),
        Some(OutboxState::Dead)
    );

    database.cleanup().await;
}

#[tokio::test]
async fn expired_lease_can_be_reclaimed_and_stale_ack_is_fenced() {
    let Some(database) = TestDatabase::prepare().await else {
        return;
    };
    let event = enqueue_committed(database.pool(), "shipping.label-requested.v1").await;
    let slow_seen = Arc::new(Mutex::new(Vec::new()));
    let slow_sink = DelayedSink {
        delay: Duration::from_millis(80),
        seen: slow_seen.clone(),
    };
    let short_lease = OutboxRelay::new(
        database.pool().clone(),
        RelayConfig {
            batch_size: 1,
            lease_duration: Duration::from_millis(15),
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        },
    )
    .expect("valid relay");
    let slow_relay = short_lease.clone();
    let fast_seen = Arc::new(Mutex::new(Vec::new()));
    let fast_sink = RecordingSink::new(fast_seen.clone(), vec![DeliveryOutcome::Delivered]);
    let recover_after_expiry = async {
        tokio::time::sleep(Duration::from_millis(35)).await;
        short_lease.drain_once(&fast_sink).await
    };
    let (stale_result, recovered_result) =
        tokio::join!(slow_relay.drain_once(&slow_sink), recover_after_expiry);
    let recovered = recovered_result.expect("reclaim expired lease");
    assert_eq!(recovered.delivered, 1);

    assert!(matches!(
        stale_result,
        Err(OutboxError::StaleLease(id)) if id == event.id()
    ));
    assert_eq!(
        OutboxStore::new(database.pool().clone())
            .state(event.id())
            .await
            .expect("read final state"),
        Some(OutboxState::Delivered)
    );
    assert_eq!(slow_seen.lock().expect("slow lock")[0].id, event.id());
    assert_eq!(fast_seen.lock().expect("fast lock")[0].id, event.id());

    database.cleanup().await;
}

fn relay(pool: &PgPool, max_attempts: u32) -> OutboxRelay {
    OutboxRelay::new(
        pool.clone(),
        RelayConfig {
            batch_size: 8,
            lease_duration: Duration::from_secs(1),
            max_attempts,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        },
    )
    .expect("valid relay")
}

async fn enqueue_committed(pool: &PgPool, event_type: &str) -> NewEvent {
    let event = NewEvent::json(event_type, &serde_json::json!({"value": 42})).expect("valid event");
    let mut transaction = pool.begin().await.expect("begin enqueue");
    OutboxStore::enqueue(transaction.as_mut(), &event)
        .await
        .expect("enqueue event");
    transaction.commit().await.expect("commit event");
    event
}

fn failure(code: &str) -> DeliveryFailure {
    DeliveryFailure::new(code, "operator-safe test detail").expect("valid failure")
}

#[derive(Clone)]
struct RecordingSink {
    seen: Arc<Mutex<Vec<ClaimedEvent>>>,
    outcomes: Arc<Mutex<Vec<DeliveryOutcome>>>,
}

impl RecordingSink {
    fn new(seen: Arc<Mutex<Vec<ClaimedEvent>>>, mut outcomes: Vec<DeliveryOutcome>) -> Self {
        outcomes.reverse();
        Self {
            seen,
            outcomes: Arc::new(Mutex::new(outcomes)),
        }
    }
}

impl fmt::Debug for RecordingSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingSink")
            .finish_non_exhaustive()
    }
}

impl OutboxSink for RecordingSink {
    fn deliver<'a>(&'a self, event: &'a ClaimedEvent) -> LocalBoxFuture<'a, DeliveryOutcome> {
        self.seen.lock().expect("seen lock").push(event.clone());
        let outcome = self.outcomes.lock().expect("outcomes lock").pop();
        Box::pin(async move { outcome.expect("configured delivery outcome") })
    }
}

#[derive(Clone, Debug)]
struct DelayedSink {
    delay: Duration,
    seen: Arc<Mutex<Vec<ClaimedEvent>>>,
}

impl OutboxSink for DelayedSink {
    fn deliver<'a>(&'a self, event: &'a ClaimedEvent) -> LocalBoxFuture<'a, DeliveryOutcome> {
        self.seen.lock().expect("seen lock").push(event.clone());
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            DeliveryOutcome::Delivered
        })
    }
}

struct TestDatabase {
    admin: PgPool,
    pool: PgPool,
    schema: String,
}

impl TestDatabase {
    async fn prepare() -> Option<Self> {
        let Ok(database_url) = std::env::var("LENSO_OUTBOX_TEST_URL") else {
            eprintln!("LENSO_OUTBOX_TEST_URL is not set; skipping PostgreSQL acceptance test");
            return None;
        };
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .expect("connect PostgreSQL test database");
        let schema = format!("outbox_test_{}", Uuid::now_v7().simple());
        let create_schema = format!("CREATE SCHEMA {schema}");
        sqlx::query(AssertSqlSafe(create_schema))
            .execute(&admin)
            .await
            .expect("create isolated test schema");

        let search_path = format!("SET search_path TO {schema}");
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .after_connect(move |connection, _metadata| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    sqlx::query(AssertSqlSafe(search_path))
                        .execute(&mut *connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .expect("connect isolated schema pool");
        pool.execute(POSTGRES_MIGRATION_SQL)
            .await
            .expect("install Outbox migration");

        Some(Self {
            admin,
            pool,
            schema,
        })
    }

    const fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) {
        self.pool.close().await;
        let drop_schema = format!("DROP SCHEMA {} CASCADE", self.schema);
        sqlx::query(AssertSqlSafe(drop_schema))
            .execute(&self.admin)
            .await
            .expect("drop isolated test schema");
        self.admin.close().await;
    }
}
