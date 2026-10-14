-- RunningHub integration tables
-- Accounts pool: consumer (Max会员) and enterprise keys, with concurrency tracking
CREATE TABLE IF NOT EXISTS runninghub_accounts (
    id             BIGSERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    tier           TEXT NOT NULL DEFAULT 'consumer' CHECK (tier IN ('consumer', 'enterprise')),
    api_key        TEXT NOT NULL,
    max_concurrency INTEGER NOT NULL DEFAULT 3,
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    in_flight      INTEGER NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_runninghub_accounts_tier_enabled
    ON runninghub_accounts (tier, enabled, in_flight);

-- Per-user RH credit ledger (1 credit = 1 RH coin, set by admin on subscription/top-up)
CREATE TABLE IF NOT EXISTS runninghub_user_credits (
    user_id    BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    rh_credits NUMERIC(20,6) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Task execution log: links users, accounts, and upstream RH tasks
CREATE TABLE IF NOT EXISTS runninghub_tasks (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id       BIGINT NOT NULL REFERENCES runninghub_accounts(id),
    upstream_task_id TEXT NOT NULL,
    app_id           TEXT NOT NULL,
    reserved_rh      NUMERIC(20,6) NOT NULL DEFAULT 0,
    actual_rh        NUMERIC(20,6),
    rate_multiplier  NUMERIC(12,6) NOT NULL DEFAULT 1,
    status           TEXT NOT NULL DEFAULT 'running',
    settled_at       TIMESTAMPTZ,
    response_json    JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_runninghub_tasks_user_created
    ON runninghub_tasks (user_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runninghub_tasks_upstream
    ON runninghub_tasks (upstream_task_id);
