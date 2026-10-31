-- Client-only journal projections need reset history independent of legacy
-- usage/window snapshots. Existing legacy window starts are not reset proof.
CREATE TABLE user_platform_quota_client_resets (
    quota_row_id BIGINT PRIMARY KEY REFERENCES user_platform_quotas(id) ON DELETE CASCADE,
    tracking_started_at TIMESTAMPTZ NOT NULL,
    daily_reset_at TIMESTAMPTZ,
    weekly_reset_at TIMESTAMPTZ,
    monthly_reset_at TIMESTAMPTZ,
    CHECK (daily_reset_at IS NULL OR daily_reset_at >= tracking_started_at),
    CHECK (weekly_reset_at IS NULL OR weekly_reset_at >= tracking_started_at),
    CHECK (monthly_reset_at IS NULL OR monthly_reset_at >= tracking_started_at)
);

-- One shared migration boundary: records captured earlier remain raw journal
-- history, since earlier administrative reset cutoffs have not been tracked.
WITH baseline AS MATERIALIZED (SELECT clock_timestamp() AS started_at)
INSERT INTO user_platform_quota_client_resets(quota_row_id,tracking_started_at)
SELECT q.id,baseline.started_at FROM user_platform_quotas q CROSS JOIN baseline;

CREATE FUNCTION initialize_user_platform_quota_client_resets() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO user_platform_quota_client_resets(quota_row_id,tracking_started_at)
    VALUES(NEW.id,clock_timestamp());
    RETURN NEW;
END;
$$;

CREATE TRIGGER user_platform_quota_client_resets_after_insert
    AFTER INSERT ON user_platform_quotas FOR EACH ROW
    EXECUTE FUNCTION initialize_user_platform_quota_client_resets();
