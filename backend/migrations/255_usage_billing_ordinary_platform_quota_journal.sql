-- Ordinary gateway metering is committed with its actual billing effects.
-- These permanent proofs also prevent a replay after hot/archive dedup cleanup.
-- They are an independent source, never a legacy counter or cache snapshot.
CREATE TABLE usage_billing_ordinary_platform_quota_journal (
    request_id VARCHAR(255) NOT NULL,
    api_key_id BIGINT NOT NULL CHECK (api_key_id > 0),
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    schema_version SMALLINT NOT NULL CHECK (schema_version = 1),
    billing_fingerprint CHAR(64) NOT NULL CHECK (billing_fingerprint ~ '^[0-9a-f]{64}$'),
    intent_fingerprint CHAR(64) NOT NULL CHECK (intent_fingerprint ~ '^[0-9a-f]{64}$'),
    platform VARCHAR(32) NOT NULL CHECK (platform ~ '^[a-z][a-z0-9_-]{0,31}$'),
    -- Preserve the raw decimal separately: round(raw,10), then round(...,8)
    -- is not equivalent to the independent billing round(raw,8).
    raw_cost_usd TEXT NOT NULL CHECK (length(raw_cost_usd) BETWEEN 1 AND 512
        AND raw_cost_usd ~ '^[0-9]+([.][0-9]+)?$'),
    cost_usd NUMERIC(20,10) NOT NULL CHECK (cost_usd >= 0 AND cost_usd < 10000000000),
    is_subscription BOOLEAN NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    disposition VARCHAR(32) NOT NULL CHECK (disposition IN ('metered', 'unlimited', 'subscription_exempt', 'zero_cost')),
    -- Immutable observations, intentionally without user/key/quota foreign keys.
    -- Removing and recreating those objects must retain the original proof.
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
    PRIMARY KEY (request_id, api_key_id),
    CHECK (raw_cost_usd::numeric >= 0 AND raw_cost_usd::numeric < 10000000000
        AND round(raw_cost_usd::numeric,10) = cost_usd),
    CHECK (recorded_at >= captured_at),
    CHECK (
        (quota_row_id IS NULL AND daily_limit_usd IS NULL AND weekly_limit_usd IS NULL AND monthly_limit_usd IS NULL
            AND daily_usage_usd IS NULL AND weekly_usage_usd IS NULL AND monthly_usage_usd IS NULL
            AND daily_window_start IS NULL AND weekly_window_start IS NULL AND monthly_window_start IS NULL) OR
        (quota_row_id IS NOT NULL AND quota_row_id > 0
            AND daily_usage_usd IS NOT NULL AND weekly_usage_usd IS NOT NULL AND monthly_usage_usd IS NOT NULL)
    ),
    CHECK (
        (disposition = 'metered' AND NOT is_subscription AND cost_usd > 0 AND quota_row_id IS NOT NULL
            AND (daily_limit_usd IS NOT NULL OR weekly_limit_usd IS NOT NULL OR monthly_limit_usd IS NOT NULL)) OR
        (disposition = 'unlimited' AND NOT is_subscription AND cost_usd > 0
            AND daily_limit_usd IS NULL AND weekly_limit_usd IS NULL AND monthly_limit_usd IS NULL) OR
        (disposition = 'subscription_exempt' AND is_subscription AND quota_row_id IS NULL) OR
        (disposition = 'zero_cost' AND NOT is_subscription AND cost_usd = 0 AND quota_row_id IS NULL)
    )
);
CREATE INDEX usage_billing_ordinary_platform_quota_journal_projection
    ON usage_billing_ordinary_platform_quota_journal(user_id, platform, quota_row_id, captured_at);
