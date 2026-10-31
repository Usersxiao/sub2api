-- This is a legacy snapshot write fence, not a certified usage baseline or a
-- cache freshness/cutover claim. Existing usage and reset timestamps stay intact.
ALTER TABLE user_platform_quotas
    ADD COLUMN IF NOT EXISTS control_generation BIGINT NOT NULL DEFAULT 1
    CHECK (control_generation > 0);

-- Keep the generation on the row being conditionally UPDATEd. PostgreSQL can
-- then recheck it after waiting for a concurrent administrative row lock.
CREATE OR REPLACE FUNCTION advance_user_platform_quota_control_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.user_id,NEW.platform,NEW.daily_limit_usd,NEW.weekly_limit_usd,
           NEW.monthly_limit_usd,NEW.deleted_at)
       IS DISTINCT FROM
       ROW(OLD.user_id,OLD.platform,OLD.daily_limit_usd,OLD.weekly_limit_usd,
           OLD.monthly_limit_usd,OLD.deleted_at) THEN
        NEW.control_generation := OLD.control_generation + 1;
    ELSIF NEW.control_generation IS DISTINCT FROM OLD.control_generation
          AND NEW.control_generation <> OLD.control_generation + 1 THEN
        RAISE EXCEPTION 'invalid user platform quota control generation transition'
            USING ERRCODE = '23514';
    END IF;
    -- bigint exhaustion raises 22003 and rolls the transaction back; never wrap.
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS user_platform_quota_control_generation_before_update ON user_platform_quotas;
CREATE TRIGGER user_platform_quota_control_generation_before_update
    BEFORE UPDATE ON user_platform_quotas FOR EACH ROW
    EXECUTE FUNCTION advance_user_platform_quota_control_generation();

-- Every actual administrator reset updates one of these protected cutoffs in
-- the same transaction as the legacy usage reset. Natural window rotation and
-- Redis absolute snapshots do not update this table and do not advance control.
-- Even a repeated equal cutoff is a reset command and advances the generation.
CREATE OR REPLACE FUNCTION fence_user_platform_quota_control_reset() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE user_platform_quotas SET control_generation = control_generation + 1
    WHERE id = NEW.quota_row_id;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS user_platform_quota_control_reset_after_update ON user_platform_quota_client_resets;
CREATE TRIGGER user_platform_quota_control_reset_after_update
    AFTER UPDATE OF daily_reset_at,weekly_reset_at,monthly_reset_at
    ON user_platform_quota_client_resets FOR EACH ROW
    EXECUTE FUNCTION fence_user_platform_quota_control_reset();
