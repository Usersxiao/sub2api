-- RunningHub: pool settings, workflow catalog, staged media, settlement detail

-- Singleton settings row: tier switches + yuan→RH coin conversion for enterprise keys
CREATE TABLE IF NOT EXISTS runninghub_settings (
    id                 SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    consumer_enabled   BOOLEAN NOT NULL DEFAULT TRUE,
    enterprise_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    yuan_to_coin       NUMERIC(12,6) NOT NULL DEFAULT 1,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO runninghub_settings (id) VALUES (1) ON CONFLICT DO NOTHING;

-- Workflow catalog: admin adds by webappId; only enabled rows are visible to clients.
-- node_info_list holds the defaults (admin overrides included); clients may only
-- override fields that exist here.
CREATE TABLE IF NOT EXISTS runninghub_workflows (
    id             BIGSERIAL PRIMARY KEY,
    webapp_id      TEXT NOT NULL UNIQUE,
    name           TEXT NOT NULL,
    description    TEXT NOT NULL DEFAULT '',
    instance_type  TEXT NOT NULL DEFAULT '',
    estimated_rh   NUMERIC(20,6) NOT NULL DEFAULT 60,
    node_info_list JSONB NOT NULL DEFAULT '[]'::jsonb,
    enabled        BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order     INTEGER NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Media staged on sub2api. Uploaded files are account-scoped on RunningHub, so the
-- file is re-uploaded to whichever account runs the task (cached per account).
CREATE TABLE IF NOT EXISTS runninghub_media (
    id           UUID PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    is_public    BOOLEAN NOT NULL DEFAULT FALSE,
    file_name    TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    file_type    TEXT NOT NULL,
    size_bytes   BIGINT NOT NULL DEFAULT 0,
    storage_path TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_runninghub_media_user_created
    ON runninghub_media (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS runninghub_media_uploads (
    media_id           UUID NOT NULL REFERENCES runninghub_media(id) ON DELETE CASCADE,
    account_id         BIGINT NOT NULL REFERENCES runninghub_accounts(id),
    upstream_file_name TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (media_id, account_id)
);

ALTER TABLE runninghub_accounts ADD COLUMN IF NOT EXISTS last_picked_at TIMESTAMPTZ;

ALTER TABLE runninghub_tasks ADD COLUMN IF NOT EXISTS consume_coins NUMERIC(20,6);
ALTER TABLE runninghub_tasks ADD COLUMN IF NOT EXISTS consume_money NUMERIC(20,6);
ALTER TABLE runninghub_tasks ADD COLUMN IF NOT EXISTS outputs_json JSONB;
ALTER TABLE runninghub_tasks ADD COLUMN IF NOT EXISTS error_message TEXT;

CREATE INDEX IF NOT EXISTS idx_runninghub_tasks_unsettled
    ON runninghub_tasks (created_at) WHERE settled_at IS NULL;
