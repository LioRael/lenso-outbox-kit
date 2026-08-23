/// `PostgreSQL` migration installed inside the owning Module's private schema.
///
/// The SQL is intentionally schema-unqualified. The owning Module must run it
/// through its own `lenso-postgres-kit` `SchemaPlan`, using a `PostgreSQL` role
/// that owns only that schema.
pub const POSTGRES_MIGRATION_SQL: &str = r"
CREATE TABLE lenso_outbox_events (
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL CHECK (length(event_type) BETWEEN 1 AND 255),
    subject text NULL CHECK (subject IS NULL OR length(subject) BETWEEN 1 AND 512),
    content_type text NOT NULL CHECK (length(content_type) BETWEEN 1 AND 127),
    payload bytea NOT NULL CHECK (octet_length(payload) <= 1048576),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    correlation_id text NULL CHECK (correlation_id IS NULL OR length(correlation_id) BETWEEN 1 AND 255),
    causation_id text NULL CHECK (causation_id IS NULL OR length(causation_id) BETWEEN 1 AND 255),
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'delivered', 'dead')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    replay_count integer NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_token uuid NULL,
    lease_expires_at timestamptz NULL,
    last_error_code text NULL,
    last_error_detail text NULL,
    dead_letter_reason text NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    delivered_at timestamptz NULL,
    CHECK (
        (state = 'leased' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (state <> 'leased' AND lease_token IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK ((state = 'delivered') = (delivered_at IS NOT NULL)),
    CHECK ((state = 'dead') = (dead_letter_reason IS NOT NULL))
);

CREATE INDEX lenso_outbox_events_claim_idx
    ON lenso_outbox_events (available_at, created_at, event_id)
    WHERE state = 'pending';

CREATE INDEX lenso_outbox_events_expired_lease_idx
    ON lenso_outbox_events (lease_expires_at, event_id)
    WHERE state = 'leased';
";
