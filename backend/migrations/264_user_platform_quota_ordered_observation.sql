-- Order observations of the existing physical quota row. This neither certifies
-- its historical usage baseline nor authorizes cache snapshots or mode changes.
-- Existing values remain untouched; revision 1 starts at this migration boundary.
ALTER TABLE user_platform_quotas
    ADD COLUMN observation_lineage UUID NOT NULL DEFAULT gen_random_uuid()
        CONSTRAINT user_platform_quota_observation_lineage_nonzero
        CHECK (observation_lineage <> '00000000-0000-0000-0000-000000000000'::uuid),
    ADD COLUMN observation_revision BIGINT NOT NULL DEFAULT 1
        CONSTRAINT user_platform_quota_observation_revision_positive
        CHECK (observation_revision > 0),
    ADD CONSTRAINT user_platform_quota_observation_lineage_unique UNIQUE (observation_lineage);

CREATE FUNCTION advance_user_platform_quota_ordered_observation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- Callers supply neither a new stream identity nor their own revision. The
    -- row lock serializes increments with the state that actually commits.
    IF NEW.observation_lineage IS DISTINCT FROM OLD.observation_lineage
        OR NEW.observation_revision IS DISTINCT FROM OLD.observation_revision THEN
        RAISE EXCEPTION 'user platform quota observation metadata is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF ROW(NEW.id,NEW.user_id,NEW.platform,NEW.control_generation,
           NEW.daily_limit_usd,NEW.weekly_limit_usd,NEW.monthly_limit_usd,NEW.deleted_at,
           NEW.daily_usage_usd,NEW.weekly_usage_usd,NEW.monthly_usage_usd,
           NEW.daily_window_start,NEW.weekly_window_start,NEW.monthly_window_start)
       IS DISTINCT FROM
       ROW(OLD.id,OLD.user_id,OLD.platform,OLD.control_generation,
           OLD.daily_limit_usd,OLD.weekly_limit_usd,OLD.monthly_limit_usd,OLD.deleted_at,
           OLD.daily_usage_usd,OLD.weekly_usage_usd,OLD.monthly_usage_usd,
           OLD.daily_window_start,OLD.weekly_window_start,OLD.monthly_window_start) THEN
        -- BIGINT exhaustion aborts the whole state transition; never wrap or
        -- reset this revision on an administrator reset or window rotation.
        NEW.observation_revision := OLD.observation_revision + 1;
    END IF;
    RETURN NEW;
END;
$$;

-- PostgreSQL orders same-kind triggers by name. The existing
-- user_platform_quota_control_generation_before_update runs first, so even an
-- equal-value administrator reset is observed through its advanced generation.
CREATE TRIGGER user_platform_quota_ordered_observation_before_update
    BEFORE UPDATE ON user_platform_quotas FOR EACH ROW
    EXECUTE FUNCTION advance_user_platform_quota_ordered_observation();
