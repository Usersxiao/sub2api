-- Immutable platform metering for v2 client settlement events. This journal is
-- deliberately independent of legacy quota counters and Redis snapshots: it
-- does not claim that platform-limit enforcement includes these contributions.
CREATE TABLE usage_settlement_platform_quota_journal (
    invocation_id VARCHAR(200) PRIMARY KEY REFERENCES usage_settlement_events(invocation_id) ON DELETE RESTRICT,
    schema_version SMALLINT NOT NULL CHECK (schema_version = 2),
    event_fingerprint CHAR(64) NOT NULL CHECK (event_fingerprint ~ '^[0-9a-f]{64}$'),
    billing_fingerprint CHAR(64) NOT NULL CHECK (billing_fingerprint ~ '^[0-9a-f]{64}$'),
    request_id VARCHAR(255) NOT NULL,
    api_key_id BIGINT NOT NULL CHECK (api_key_id > 0),
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    platform VARCHAR(32) NOT NULL CHECK (platform ~ '^[a-z][a-z0-9_-]{0,31}$'),
    cost_usd NUMERIC(20,10) NOT NULL CHECK (cost_usd >= 0 AND cost_usd < 10000000000),
    captured_at TIMESTAMPTZ NOT NULL,
    disposition VARCHAR(32) NOT NULL CHECK (disposition IN ('metered', 'unlimited', 'subscription_exempt', 'zero_cost')),
    -- This is an observation of the row at the journal's first insertion,
    -- not a live reference. Admin removal/recreation must preserve its history.
    quota_row_id BIGINT,
    daily_limit_usd NUMERIC(20,10),
    weekly_limit_usd NUMERIC(20,10),
    monthly_limit_usd NUMERIC(20,10),
    daily_usage_usd NUMERIC(20,10),
    weekly_usage_usd NUMERIC(20,10),
    monthly_usage_usd NUMERIC(20,10),
    daily_window_start TIMESTAMPTZ,
    weekly_window_start TIMESTAMPTZ,
    monthly_window_start TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (request_id, api_key_id),
    CHECK (request_id = 'client:' || invocation_id),
    CHECK (recorded_at >= captured_at),
    CHECK (
        (quota_row_id IS NULL AND daily_limit_usd IS NULL AND weekly_limit_usd IS NULL AND monthly_limit_usd IS NULL
            AND daily_usage_usd IS NULL AND weekly_usage_usd IS NULL AND monthly_usage_usd IS NULL
            AND daily_window_start IS NULL AND weekly_window_start IS NULL AND monthly_window_start IS NULL) OR
        (quota_row_id IS NOT NULL AND quota_row_id > 0
            AND daily_usage_usd IS NOT NULL AND weekly_usage_usd IS NOT NULL AND monthly_usage_usd IS NOT NULL)
    ),
    CHECK (
        (disposition = 'metered' AND cost_usd > 0 AND quota_row_id IS NOT NULL
            AND (daily_limit_usd IS NOT NULL OR weekly_limit_usd IS NOT NULL OR monthly_limit_usd IS NOT NULL)) OR
        (disposition = 'unlimited' AND cost_usd > 0
            AND daily_limit_usd IS NULL AND weekly_limit_usd IS NULL AND monthly_limit_usd IS NULL) OR
        (disposition = 'subscription_exempt' AND quota_row_id IS NULL) OR
        (disposition = 'zero_cost' AND cost_usd = 0 AND quota_row_id IS NULL)
    )
);
CREATE INDEX usage_settlement_platform_quota_journal_user_platform_captured
    ON usage_settlement_platform_quota_journal(user_id, platform, captured_at);
