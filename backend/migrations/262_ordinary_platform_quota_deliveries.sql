-- New Apply writes this eligibility in the same transaction as fresh ordinary
-- money and its source journal. Existing journals are intentionally not backfilled.
-- The application write path establishes that cohort; this is not owner-SQL
-- provenance or an assertion that the legacy quota counter is authoritative.
CREATE TABLE ordinary_platform_quota_deliveries (
    request_id VARCHAR(255) NOT NULL,
    api_key_id BIGINT NOT NULL CHECK (api_key_id > 0),
    source_fingerprint CHAR(64) NOT NULL CHECK (source_fingerprint ~ '^[0-9a-f]{64}$'),
    delivery_mode VARCHAR(32) NOT NULL CHECK (delivery_mode = 'db_increment_v1'),
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    platform VARCHAR(32) NOT NULL CHECK (platform ~ '^[a-z][a-z0-9_-]{0,31}$'),
    quota_row_id BIGINT NOT NULL CHECK (quota_row_id > 0),
    control_generation BIGINT NOT NULL CHECK (control_generation > 0),
    captured_at TIMESTAMPTZ NOT NULL,
    raw_cost_usd TEXT NOT NULL CHECK (length(raw_cost_usd) BETWEEN 1 AND 512
        AND raw_cost_usd ~ '^[0-9]+([.][0-9]+)?$'),
    cost_usd NUMERIC(20,10) NOT NULL CHECK (cost_usd > 0 AND cost_usd < 10000000000),
    state VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','completed')),
    result_status VARCHAR(16),
    applied_windows SMALLINT NOT NULL DEFAULT 0 CHECK (applied_windows BETWEEN 0 AND 7),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (request_id,api_key_id),
    FOREIGN KEY (request_id,api_key_id)
        REFERENCES usage_billing_ordinary_platform_quota_journal(request_id,api_key_id),
    CHECK (raw_cost_usd::numeric > 0 AND raw_cost_usd::numeric < 10000000000
        AND round(raw_cost_usd::numeric,10) = cost_usd),
    CHECK (created_at >= captured_at),
    CHECK (
        (state='pending' AND result_status IS NULL AND applied_windows=0 AND completed_at IS NULL) OR
        (state='completed' AND result_status IS NOT NULL AND completed_at IS NOT NULL AND completed_at>=created_at AND (
            (result_status='applied' AND applied_windows=7) OR
            (result_status='partial' AND applied_windows BETWEEN 1 AND 6) OR
            (result_status IN ('superseded','obsolete') AND applied_windows=0)
        ))
    )
);

-- Birth bindings and completed outcomes are permanent. In particular, deleting
-- a key/quota or pruning financial dedup rows must not reset consumption identity.
CREATE FUNCTION protect_ordinary_platform_quota_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'ordinary platform quota delivery is permanent';
    END IF;
    IF ROW(NEW.request_id,NEW.api_key_id,NEW.source_fingerprint,NEW.delivery_mode,NEW.user_id,NEW.platform,
        NEW.quota_row_id,NEW.control_generation,NEW.captured_at,NEW.raw_cost_usd,NEW.cost_usd,NEW.created_at)
        IS DISTINCT FROM ROW(OLD.request_id,OLD.api_key_id,OLD.source_fingerprint,OLD.delivery_mode,OLD.user_id,OLD.platform,
        OLD.quota_row_id,OLD.control_generation,OLD.captured_at,OLD.raw_cost_usd,OLD.cost_usd,OLD.created_at) THEN
        RAISE EXCEPTION 'ordinary platform quota delivery binding is immutable';
    END IF;
    IF OLD.state<>'pending' OR NEW.state<>'completed' THEN
        RAISE EXCEPTION 'ordinary platform quota delivery terminal state is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ordinary_platform_quota_delivery_immutable
    BEFORE UPDATE OR DELETE ON ordinary_platform_quota_deliveries
    FOR EACH ROW EXECUTE FUNCTION protect_ordinary_platform_quota_delivery();
