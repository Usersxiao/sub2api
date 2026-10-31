-- Inference has a durable boundary between admission and possible paid dispatch.
-- Expiring a recovery window never removes its uniqueness tombstone or permits
-- a dispatched/unknown request to be sent again.
CREATE TABLE client_invocations (
    invocation_id VARCHAR(200) PRIMARY KEY,
    instance_id VARCHAR(200) NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    client_id VARCHAR(200) NOT NULL,
    consumer_id VARCHAR(128) NOT NULL,
    project_id VARCHAR(200) NOT NULL,
    request_id VARCHAR(200) NOT NULL,
    route_id VARCHAR(200) NOT NULL,
    operation VARCHAR(32) NOT NULL CHECK (operation = 'text'),
    config_version VARCHAR(200) NOT NULL,
    entitlement_version VARCHAR(200) NOT NULL,
    fingerprint CHAR(64) NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    billing_identity_id BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE RESTRICT,
    group_id BIGINT NOT NULL REFERENCES groups(id) ON DELETE RESTRICT,
    owner_token_hash CHAR(64) NOT NULL CHECK (owner_token_hash ~ '^[0-9a-f]{64}$'),
    state VARCHAR(32) NOT NULL CHECK (state IN ('reserved', 'dispatched', 'unknown', 'succeeded', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    dispatch_deadline TIMESTAMPTZ,
    idempotency_expires_at TIMESTAMPTZ NOT NULL,
    -- Plain JSON text preserves all valid UTF-8 model text, including U+0000;
    -- PostgreSQL JSONB's text conversion rejects that otherwise valid JSON.
    -- Only the service's closed Outcome type may read or write this column.
    outcome TEXT,
    UNIQUE (instance_id, user_id, consumer_id, project_id, request_id),
    FOREIGN KEY (consumer_id, instance_id, user_id, client_id)
        REFERENCES client_gateway_consumers(consumer_id, instance_id, user_id, client_id) ON DELETE RESTRICT,
    CHECK (updated_at >= created_at AND lease_expires_at > created_at AND idempotency_expires_at > created_at),
    CHECK (
        (state = 'reserved' AND dispatch_deadline IS NULL AND outcome IS NULL) OR
        (state IN ('dispatched', 'unknown') AND dispatch_deadline IS NOT NULL AND outcome IS NULL) OR
        (state IN ('succeeded', 'failed') AND dispatch_deadline IS NOT NULL AND outcome IS NOT NULL)
    ),
    CHECK (outcome IS NULL OR (json_typeof(outcome::json) = 'object' AND octet_length(outcome) <= 8 * 1024 * 1024))
);
CREATE INDEX client_invocations_owner_created
    ON client_invocations(instance_id, user_id, consumer_id, project_id, created_at);
CREATE INDEX client_invocations_dispatch_deadline
    ON client_invocations(dispatch_deadline) WHERE state = 'dispatched';
