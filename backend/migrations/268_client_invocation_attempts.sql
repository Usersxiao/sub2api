-- Keep each provider dispatch as an independently auditable, immutable attempt.
-- This migration introduces attempt 1 only; it does not authorize retries.
CREATE TABLE client_invocation_attempts (
    invocation_id VARCHAR(200) NOT NULL REFERENCES client_invocations(invocation_id) ON DELETE RESTRICT,
    attempt_no SMALLINT NOT NULL CHECK (attempt_no = 1),
    state VARCHAR(32) NOT NULL CHECK (state IN ('dispatched', 'unknown', 'succeeded', 'failed')),
    provider_account_id BIGINT NOT NULL CHECK (provider_account_id > 0),
    provider_platform VARCHAR(32) NOT NULL CHECK (provider_platform ~ '^[a-z][a-z0-9_]{0,31}$'),
    provider_model VARCHAR(500) NOT NULL CHECK (provider_model <> ''),
    provider_protocol VARCHAR(32) NOT NULL CHECK (provider_protocol IN ('messages', 'responses', 'chat-completions')),
    provider_payload_hash CHAR(64) NOT NULL CHECK (provider_payload_hash ~ '^[0-9a-f]{64}$'),
    provider_counter_proof JSONB,
    dispatch_started_at TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (invocation_id, attempt_no),
    CHECK (provider_counter_proof IS NULL OR (
        jsonb_typeof(provider_counter_proof) = 'object' AND octet_length(provider_counter_proof::text) <= 2048
    )),
    CHECK (dispatch_started_at IS NULL OR updated_at >= dispatch_started_at)
);
CREATE INDEX client_invocation_attempts_state_updated
    ON client_invocation_attempts(state, updated_at);

-- Older rows have no persisted dispatch timestamp. Keep it NULL rather than
-- inventing one from invocation creation or completion timestamps.
INSERT INTO client_invocation_attempts (
    invocation_id, attempt_no, state, provider_account_id, provider_platform, provider_model,
    provider_protocol, provider_payload_hash, provider_counter_proof, dispatch_started_at, updated_at
)
SELECT i.invocation_id, 1, i.state, i.provider_account_id, i.provider_platform, i.provider_model,
       i.provider_protocol, i.provider_payload_hash, i.provider_counter_proof, NULL, i.updated_at
FROM client_invocations i
WHERE i.state IN ('dispatched', 'unknown', 'succeeded', 'failed')
  AND i.provider_account_id IS NOT NULL
  AND i.provider_platform IS NOT NULL
  AND i.provider_model IS NOT NULL
  AND i.provider_protocol IS NOT NULL
  AND i.provider_payload_hash IS NOT NULL;

CREATE OR REPLACE FUNCTION protect_client_invocation_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'client invocation attempts are immutable records';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF ROW(NEW.invocation_id, NEW.attempt_no, NEW.provider_account_id, NEW.provider_platform,
               NEW.provider_model, NEW.provider_protocol, NEW.provider_payload_hash,
               NEW.provider_counter_proof, NEW.dispatch_started_at, NEW.recorded_at)
           IS DISTINCT FROM
           ROW(OLD.invocation_id, OLD.attempt_no, OLD.provider_account_id, OLD.provider_platform,
               OLD.provider_model, OLD.provider_protocol, OLD.provider_payload_hash,
               OLD.provider_counter_proof, OLD.dispatch_started_at, OLD.recorded_at) THEN
            RAISE EXCEPTION 'client invocation attempt binding is immutable';
        END IF;
        IF NOT (
            (OLD.state = 'dispatched' AND NEW.state IN ('unknown', 'succeeded', 'failed')) OR
            (OLD.state = 'unknown' AND NEW.state IN ('succeeded', 'failed'))
        ) OR NEW.updated_at < OLD.updated_at THEN
            RAISE EXCEPTION 'invalid client invocation attempt transition';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER client_invocation_attempt_guard
    BEFORE UPDATE OR DELETE ON client_invocation_attempts
    FOR EACH ROW EXECUTE FUNCTION protect_client_invocation_attempt();

-- Deferred checks allow each repository transaction to update both rows in
-- order while ensuring no committed state can split from its provider attempt.
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
        IF attempt_count <> 0 THEN
            RAISE EXCEPTION 'client invocation without a provider binding cannot have an attempt';
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
CREATE CONSTRAINT TRIGGER client_invocations_attempt_consistency
    AFTER INSERT OR UPDATE ON client_invocations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_client_invocation_attempt_consistency();
CREATE CONSTRAINT TRIGGER client_invocation_attempts_consistency
    AFTER INSERT OR UPDATE OR DELETE ON client_invocation_attempts
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_client_invocation_attempt_consistency();
