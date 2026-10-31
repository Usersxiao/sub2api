-- Private durable accounting work for an already dispatched client invocation.
-- Frozen payloads and fingerprints never change; leases only authorize billing
-- recovery and never authorize another model dispatch.
CREATE TABLE usage_settlement_events (
    invocation_id VARCHAR(200) PRIMARY KEY REFERENCES client_invocations(invocation_id) ON DELETE RESTRICT,
    request_id VARCHAR(255) NOT NULL,
    api_key_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    group_id BIGINT NOT NULL,
    event_fingerprint CHAR(64) NOT NULL CHECK (event_fingerprint ~ '^[0-9a-f]{64}$'),
    payload TEXT NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('pending', 'settled', 'conflict')),
    lease_token_hash CHAR(64) NOT NULL CHECK (lease_token_hash ~ '^[0-9a-f]{64}$'),
    lease_expires_at TIMESTAMPTZ,
    usage_log_id BIGINT,
    billing_fingerprint CHAR(64),
    attempts BIGINT NOT NULL DEFAULT 1 CHECK (attempts > 0),
    last_error_code VARCHAR(32) CHECK (last_error_code IN ('SETTLEMENT_RETRY', 'SETTLEMENT_CONFLICT')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (request_id, api_key_id),
    CHECK (request_id = 'client:' || invocation_id),
    CHECK (api_key_id > 0 AND user_id > 0 AND group_id > 0 AND updated_at >= created_at),
    CHECK (json_typeof(payload::json) = 'object' AND octet_length(payload) <= 256 * 1024),
    CHECK (
        (state = 'pending' AND lease_expires_at IS NOT NULL AND usage_log_id IS NULL AND billing_fingerprint IS NULL) OR
        (state = 'conflict' AND lease_expires_at IS NULL AND usage_log_id IS NULL AND billing_fingerprint IS NULL) OR
        (state = 'settled' AND lease_expires_at IS NULL AND usage_log_id IS NOT NULL AND usage_log_id > 0
            AND billing_fingerprint IS NOT NULL AND billing_fingerprint ~ '^[0-9a-f]{64}$')
    )
);
CREATE INDEX usage_settlement_events_pending_lease
    ON usage_settlement_events(lease_expires_at, created_at, invocation_id) WHERE state = 'pending';
