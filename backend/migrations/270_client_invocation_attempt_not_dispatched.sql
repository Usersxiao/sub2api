-- Record only the pre-dispatch outcome that is serialized against the durable
-- dispatch claim. This state never authorizes a retry by itself.
ALTER TABLE client_invocation_attempts
    ADD COLUMN not_dispatched_at TIMESTAMPTZ,
    DROP CONSTRAINT client_invocation_attempts_state_check,
    ADD CONSTRAINT client_invocation_attempts_state_check CHECK (
        state IN ('prepared', 'not_dispatched', 'dispatched', 'unknown', 'succeeded', 'failed')
    ),
    ADD CONSTRAINT client_invocation_attempts_not_dispatched_time CHECK (
        (state = 'not_dispatched') = (not_dispatched_at IS NOT NULL)
    ),
    ADD CONSTRAINT client_invocation_attempts_not_dispatched_has_no_dispatch CHECK (
        state <> 'not_dispatched' OR dispatch_started_at IS NULL
    ),
    ADD CONSTRAINT client_invocation_attempts_not_dispatched_time_order CHECK (
        not_dispatched_at IS NULL OR updated_at >= not_dispatched_at
    );

CREATE OR REPLACE FUNCTION protect_client_invocation_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'client invocation attempts are immutable records';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF ROW(NEW.invocation_id, NEW.attempt_no, NEW.provider_account_id, NEW.provider_platform,
               NEW.provider_model, NEW.provider_protocol, NEW.provider_payload_hash,
               NEW.provider_counter_proof, NEW.recorded_at)
           IS DISTINCT FROM
           ROW(OLD.invocation_id, OLD.attempt_no, OLD.provider_account_id, OLD.provider_platform,
               OLD.provider_model, OLD.provider_protocol, OLD.provider_payload_hash,
               OLD.provider_counter_proof, OLD.recorded_at) THEN
            RAISE EXCEPTION 'client invocation attempt binding is immutable';
        END IF;
        IF NOT (
            (OLD.state = 'prepared' AND NEW.state = 'dispatched' AND
                OLD.dispatch_started_at IS NULL AND NEW.dispatch_started_at IS NOT NULL AND
                OLD.not_dispatched_at IS NULL AND NEW.not_dispatched_at IS NULL) OR
            (OLD.state = 'prepared' AND NEW.state = 'not_dispatched' AND
                OLD.dispatch_started_at IS NULL AND NEW.dispatch_started_at IS NULL AND
                OLD.not_dispatched_at IS NULL AND NEW.not_dispatched_at IS NOT NULL) OR
            (OLD.state = 'dispatched' AND NEW.state IN ('unknown', 'succeeded', 'failed') AND
                NEW.dispatch_started_at IS NOT DISTINCT FROM OLD.dispatch_started_at AND
                NEW.not_dispatched_at IS NOT DISTINCT FROM OLD.not_dispatched_at) OR
            (OLD.state = 'unknown' AND NEW.state IN ('succeeded', 'failed') AND
                NEW.dispatch_started_at IS NOT DISTINCT FROM OLD.dispatch_started_at AND
                NEW.not_dispatched_at IS NOT DISTINCT FROM OLD.not_dispatched_at)
        ) OR NEW.updated_at < OLD.updated_at THEN
            RAISE EXCEPTION 'invalid client invocation attempt transition';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_client_invocation_attempt_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_invocation_id VARCHAR(200);
    parent_row RECORD;
    attempt_row RECORD;
    attempt_count BIGINT;
BEGIN
    target_invocation_id := CASE WHEN TG_TABLE_NAME = 'client_invocations' THEN NEW.invocation_id
                                 ELSE COALESCE(NEW.invocation_id, OLD.invocation_id) END;
    SELECT i.state, i.provider_account_id, i.provider_platform, i.provider_model,
           i.provider_protocol, i.provider_payload_hash, i.provider_counter_proof
      INTO parent_row
      FROM client_invocations i WHERE i.invocation_id = target_invocation_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'client invocation attempt has no parent invocation';
    END IF;

    SELECT count(*) INTO attempt_count
      FROM client_invocation_attempts a WHERE a.invocation_id = target_invocation_id;
    IF parent_row.provider_payload_hash IS NULL THEN
        IF attempt_count = 0 THEN
            RETURN NULL;
        END IF;
        IF attempt_count <> 1 OR parent_row.state <> 'reserved' THEN
            RAISE EXCEPTION 'unbound client invocation may only retain one prepared or not-dispatched attempt';
        END IF;
        SELECT a.state, a.not_dispatched_at INTO attempt_row
          FROM client_invocation_attempts a WHERE a.invocation_id = target_invocation_id AND a.attempt_no = 1;
        IF attempt_row.state NOT IN ('prepared', 'not_dispatched') OR
           ((attempt_row.state = 'not_dispatched') <> (attempt_row.not_dispatched_at IS NOT NULL)) THEN
            RAISE EXCEPTION 'unbound client invocation attempt is not a valid pre-dispatch state';
        END IF;
        RETURN NULL;
    END IF;
    IF attempt_count <> 1 THEN
        RAISE EXCEPTION 'provider-bound client invocation requires exactly one attempt';
    END IF;

    SELECT a.state, a.provider_account_id, a.provider_platform, a.provider_model,
           a.provider_protocol, a.provider_payload_hash, a.provider_counter_proof
      INTO attempt_row
      FROM client_invocation_attempts a WHERE a.invocation_id = target_invocation_id AND a.attempt_no = 1;
    IF attempt_row.provider_account_id IS DISTINCT FROM parent_row.provider_account_id OR
       attempt_row.provider_platform IS DISTINCT FROM parent_row.provider_platform OR
       attempt_row.provider_model IS DISTINCT FROM parent_row.provider_model OR
       attempt_row.provider_protocol IS DISTINCT FROM parent_row.provider_protocol OR
       attempt_row.provider_payload_hash IS DISTINCT FROM parent_row.provider_payload_hash OR
       attempt_row.provider_counter_proof IS DISTINCT FROM parent_row.provider_counter_proof OR
       attempt_row.state IS DISTINCT FROM parent_row.state THEN
        RAISE EXCEPTION 'client invocation and provider attempt are inconsistent';
    END IF;
    RETURN NULL;
END;
$$;
