-- Allocate the future cache domain independently of Redis and process lifetime.
-- This preserves the fixed writer authority and does not certify a usage baseline
-- or authorize publishing, adopting or draining any cache cohort.
ALTER TABLE user_platform_quota_writer_authority
    ADD COLUMN cache_namespace UUID NOT NULL DEFAULT gen_random_uuid()
        CONSTRAINT user_platform_quota_cache_namespace_nonzero
        CHECK (cache_namespace <> '00000000-0000-0000-0000-000000000000'::uuid),
    ADD CONSTRAINT user_platform_quota_cache_namespace_unique UNIQUE (cache_namespace);

CREATE OR REPLACE FUNCTION protect_user_platform_quota_writer_authority() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'user platform quota writer authority is permanent';
    END IF;
    IF ROW(NEW.id,NEW.mode,NEW.epoch,NEW.created_at,NEW.cache_namespace)
        IS DISTINCT FROM ROW(OLD.id,OLD.mode,OLD.epoch,OLD.created_at,OLD.cache_namespace) THEN
        RAISE EXCEPTION 'user platform quota writer authority is immutable';
    END IF;
    RETURN NEW;
END;
$$;
