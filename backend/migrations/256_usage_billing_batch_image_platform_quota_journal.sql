-- Batch capture is a distinct source from ordinary request billing. Hold and
-- release never consume platform quota; only capture receives this proof.
CREATE TABLE usage_billing_batch_image_platform_quota_journal (
    request_id VARCHAR(255) NOT NULL,
    api_key_id BIGINT NOT NULL CHECK (api_key_id > 0),
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    batch_id VARCHAR(64) NOT NULL CHECK (batch_id <> ''),
    provider VARCHAR(32) NOT NULL CHECK (provider IN ('gemini_api', 'vertex')),
    schema_version SMALLINT NOT NULL CHECK (schema_version = 1),
    billing_fingerprint CHAR(64) NOT NULL CHECK (billing_fingerprint ~ '^[0-9a-f]{64}$'),
    intent_fingerprint CHAR(64) NOT NULL CHECK (intent_fingerprint ~ '^[0-9a-f]{64}$'),
    platform VARCHAR(32) NOT NULL CHECK (platform = 'gemini'),
    raw_hold_usd TEXT NOT NULL CHECK (length(raw_hold_usd) BETWEEN 1 AND 512 AND raw_hold_usd ~ '^[0-9]+([.][0-9]+)?$'),
    raw_cost_usd TEXT NOT NULL CHECK (length(raw_cost_usd) BETWEEN 1 AND 512 AND raw_cost_usd ~ '^[0-9]+([.][0-9]+)?$'),
    cost_usd NUMERIC(20,10) NOT NULL CHECK (cost_usd >= 0 AND cost_usd < 10000000000),
    -- Kept false to share the quota-observation codec without treating this
    -- wallet operation as subscription billing.
    is_subscription BOOLEAN NOT NULL DEFAULT FALSE CHECK (NOT is_subscription),
    captured_at TIMESTAMPTZ NOT NULL,
    disposition VARCHAR(32) NOT NULL CHECK (disposition IN ('metered', 'unlimited', 'zero_cost')),
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
    CHECK (request_id = 'batch_image_capture:' || batch_id),
    CHECK (raw_hold_usd::numeric >= 0 AND raw_hold_usd::numeric < 10000000000),
    CHECK (raw_cost_usd::numeric >= 0 AND raw_cost_usd::numeric < 10000000000 AND round(raw_cost_usd::numeric,10) = cost_usd),
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
        (disposition = 'zero_cost' AND cost_usd = 0 AND quota_row_id IS NULL)
    )
);
CREATE INDEX usage_billing_batch_image_platform_quota_projection
    ON usage_billing_batch_image_platform_quota_journal(user_id,platform,quota_row_id,captured_at);
