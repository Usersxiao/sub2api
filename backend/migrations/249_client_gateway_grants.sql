-- Stable application consumption identities are separate from rotating credentials
-- and from legacy API keys. No inference adapter consumes these grants yet.
CREATE TABLE client_gateway_consumers (
    consumer_id VARCHAR(128) PRIMARY KEY,
    instance_id VARCHAR(200) NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    client_id VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(instance_id, user_id, client_id),
    UNIQUE(consumer_id, instance_id, user_id, client_id)
);

CREATE TABLE client_gateway_grants (
    grant_id VARCHAR(128) PRIMARY KEY,
    token_hash CHAR(64) NOT NULL UNIQUE CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    consumer_id VARCHAR(128) NOT NULL,
    instance_id VARCHAR(200) NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    client_id VARCHAR(200) NOT NULL,
    sid VARCHAR(128) NOT NULL REFERENCES auth_sessions(sid) ON DELETE CASCADE,
    session_epoch BIGINT NOT NULL CHECK (session_epoch > 0),
    config_version VARCHAR(200) NOT NULL,
    entitlement_version VARCHAR(200) NOT NULL,
    route_ids JSONB NOT NULL CHECK (jsonb_typeof(route_ids) = 'array' AND jsonb_array_length(route_ids) > 0),
    operations JSONB NOT NULL CHECK (operations = '["invoke"]'::jsonb),
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    FOREIGN KEY (consumer_id, instance_id, user_id, client_id)
        REFERENCES client_gateway_consumers(consumer_id, instance_id, user_id, client_id) ON DELETE CASCADE,
    CHECK (expires_at > issued_at AND expires_at <= issued_at + INTERVAL '5 minutes')
);
CREATE INDEX client_gateway_grants_session ON client_gateway_grants(user_id, sid, client_id);
CREATE INDEX client_gateway_grants_expiry ON client_gateway_grants(expires_at);
-- Initial issuance and rotation both retire any still-active credential for this
-- scope. Session/epoch liveness is independently checked on every repository use.
CREATE UNIQUE INDEX client_gateway_grants_one_active_scope
    ON client_gateway_grants(instance_id, user_id, sid, client_id) WHERE revoked_at IS NULL;
