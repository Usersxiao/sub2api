-- Internal rows are accounting identities, never accepted as bearer credentials.
-- One row per consumer/group preserves existing usage and billing foreign keys.
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS kind VARCHAR(32) NOT NULL DEFAULT 'api_key';
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS client_consumer_id VARCHAR(128);
ALTER TABLE api_keys ADD CONSTRAINT api_keys_client_consumer_fk
    FOREIGN KEY (client_consumer_id) REFERENCES client_gateway_consumers(consumer_id) ON DELETE RESTRICT;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_kind_scope_check CHECK (
    (kind = 'api_key' AND client_consumer_id IS NULL AND key NOT LIKE 'internal:%') OR
    (kind = 'client_consumer' AND client_consumer_id IS NOT NULL AND group_id IS NOT NULL
        AND key LIKE 'internal:%')
);
CREATE UNIQUE INDEX api_keys_client_consumer_group
    ON api_keys(client_consumer_id, group_id) WHERE kind = 'client_consumer';
