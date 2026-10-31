-- This authority admits one new DB writer domain. It preserves existing quota
-- observations without certifying their completeness or a Redis/DB cutover.
-- No singleton is inferred from legacy state; the new process initializes it.
CREATE TABLE user_platform_quota_writer_authority (
    id SMALLINT PRIMARY KEY CHECK (id=1),
    mode VARCHAR(32) NOT NULL CHECK (mode='db_increment_v1'),
    epoch BIGINT NOT NULL CHECK (epoch>0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION protect_user_platform_quota_writer_authority() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'user platform quota writer authority is permanent';
    END IF;
    IF ROW(NEW.id,NEW.mode,NEW.epoch,NEW.created_at)
        IS DISTINCT FROM ROW(OLD.id,OLD.mode,OLD.epoch,OLD.created_at) THEN
        RAISE EXCEPTION 'user platform quota writer authority is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER user_platform_quota_writer_authority_immutable
    BEFORE UPDATE OR DELETE ON user_platform_quota_writer_authority
    FOR EACH ROW EXECUTE FUNCTION protect_user_platform_quota_writer_authority();

-- Only newly qualified births carry an epoch. Historical NULL remains NULL,
-- including pending deliveries; current configuration is not historical proof.
ALTER TABLE ordinary_platform_quota_deliveries ADD COLUMN writer_epoch BIGINT
    CHECK (writer_epoch IS NULL OR writer_epoch>0);
CREATE OR REPLACE FUNCTION protect_ordinary_platform_quota_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'ordinary platform quota delivery is permanent';
    END IF;
    IF ROW(NEW.request_id,NEW.api_key_id,NEW.source_fingerprint,NEW.delivery_mode,NEW.user_id,NEW.platform,
        NEW.quota_row_id,NEW.control_generation,NEW.captured_at,NEW.raw_cost_usd,NEW.cost_usd,NEW.created_at,NEW.writer_epoch)
        IS DISTINCT FROM ROW(OLD.request_id,OLD.api_key_id,OLD.source_fingerprint,OLD.delivery_mode,OLD.user_id,OLD.platform,
        OLD.quota_row_id,OLD.control_generation,OLD.captured_at,OLD.raw_cost_usd,OLD.cost_usd,OLD.created_at,OLD.writer_epoch) THEN
        RAISE EXCEPTION 'ordinary platform quota delivery binding is immutable';
    END IF;
    IF OLD.state<>'pending' OR NEW.state<>'completed' THEN
        RAISE EXCEPTION 'ordinary platform quota delivery terminal state is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION assert_user_platform_quota_writer_scope() RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    writer user_platform_quota_writer_authority%ROWTYPE;
    action TEXT := current_setting('sub2api.quota_writer_action',true);
    mode_value TEXT := current_setting('sub2api.quota_writer_mode',true);
    epoch_value TEXT := current_setting('sub2api.quota_writer_epoch',true);
BEGIN
    -- The statement trigger runs before quota row locks. Hold this lock through
    -- commit, so future controlled authority transitions must wait for writers.
    SELECT * INTO writer FROM user_platform_quota_writer_authority WHERE id=1 FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'user platform quota writer authority is uninitialized' USING ERRCODE='55000';
    END IF;
    IF action IS NULL OR action NOT IN ('ordinary_delivery','admin_reset')
        OR mode_value IS DISTINCT FROM writer.mode
        OR epoch_value IS DISTINCT FROM writer.epoch::text THEN
        RAISE EXCEPTION 'user platform quota writer scope mismatch' USING ERRCODE='23514';
    END IF;
END;
$$;
CREATE FUNCTION fence_user_platform_quota_usage_statement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM assert_user_platform_quota_writer_scope();
    RETURN NULL;
END;
$$;
CREATE TRIGGER user_platform_quota_usage_writer_before_update
    BEFORE UPDATE OF daily_usage_usd,weekly_usage_usd,monthly_usage_usd,
        daily_window_start,weekly_window_start,monthly_window_start
    ON user_platform_quotas FOR EACH STATEMENT
    EXECUTE FUNCTION fence_user_platform_quota_usage_statement();

CREATE FUNCTION fence_user_platform_quota_usage_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.daily_usage_usd IS DISTINCT FROM 0 OR NEW.weekly_usage_usd IS DISTINCT FROM 0
        OR NEW.monthly_usage_usd IS DISTINCT FROM 0 OR NEW.daily_window_start IS NOT NULL
        OR NEW.weekly_window_start IS NOT NULL OR NEW.monthly_window_start IS NOT NULL THEN
        PERFORM assert_user_platform_quota_writer_scope();
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER user_platform_quota_usage_writer_before_insert
    BEFORE INSERT ON user_platform_quotas FOR EACH ROW
    EXECUTE FUNCTION fence_user_platform_quota_usage_insert();
