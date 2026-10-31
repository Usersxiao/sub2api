-- A second provider binding is admissible only after attempt 1 has durable
-- evidence that it never crossed the dispatch claim.
ALTER TABLE client_invocation_attempts
    DROP CONSTRAINT client_invocation_attempts_attempt_no_check,
    ADD CONSTRAINT client_invocation_attempts_attempt_no_check CHECK (attempt_no IN (1, 2));

ALTER TABLE client_invocations
    ADD COLUMN provider_attempt_no SMALLINT;

ALTER TABLE client_invocations
    DROP CONSTRAINT client_invocations_provider_payload_complete,
    ADD CONSTRAINT client_invocations_provider_payload_complete CHECK (
        (num_nonnulls(provider_account_id,provider_platform,provider_model,provider_protocol,
                      provider_payload_hash,provider_attempt_no) = 0) OR
        (num_nonnulls(provider_account_id,provider_platform,provider_model,provider_protocol,
                      provider_payload_hash,provider_attempt_no) = 6 AND
         provider_account_id > 0 AND provider_platform ~ '^[a-z][a-z0-9_]{0,31}$' AND
         provider_model <> '' AND provider_protocol IN ('messages','responses','chat-completions') AND
         provider_payload_hash ~ '^[0-9a-f]{64}$' AND provider_attempt_no IN (1,2))
    ) NOT VALID;

UPDATE client_invocations
   SET provider_attempt_no = 1
 WHERE provider_payload_hash IS NOT NULL;

CREATE OR REPLACE FUNCTION protect_client_invocation_provider_payload() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.provider_account_id,NEW.provider_platform,NEW.provider_model,NEW.provider_protocol,
           NEW.provider_payload_hash,NEW.provider_attempt_no)
        IS DISTINCT FROM ROW(OLD.provider_account_id,OLD.provider_platform,OLD.provider_model,OLD.provider_protocol,
                             OLD.provider_payload_hash,OLD.provider_attempt_no) THEN
        IF OLD.state <> 'reserved' OR NEW.state <> 'dispatched' OR
            OLD.provider_payload_hash IS NOT NULL OR NEW.provider_payload_hash IS NULL OR
            NEW.provider_attempt_no IS NULL THEN
            RAISE EXCEPTION 'client invocation provider payload is immutable';
        END IF;
    END IF;
    IF OLD.state='reserved' AND NEW.state='dispatched' AND
       (NEW.provider_payload_hash IS NULL OR NEW.provider_attempt_no IS NULL) THEN
        RAISE EXCEPTION 'client invocation dispatch requires a provider attempt binding';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_client_invocation_attempt_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_invocation_id VARCHAR(200);
    parent_row RECORD;
    first_attempt RECORD;
    current_attempt RECORD;
    attempt_count BIGINT;
BEGIN
    target_invocation_id := CASE WHEN TG_TABLE_NAME = 'client_invocations' THEN NEW.invocation_id
                                 ELSE COALESCE(NEW.invocation_id, OLD.invocation_id) END;
    SELECT i.state, i.provider_account_id, i.provider_platform, i.provider_model,
           i.provider_protocol, i.provider_payload_hash, i.provider_counter_proof, i.provider_attempt_no
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
        IF parent_row.state <> 'reserved' OR attempt_count NOT IN (1, 2) THEN
            RAISE EXCEPTION 'unbound client invocation may retain at most two reserved attempts';
        END IF;
        SELECT a.state INTO first_attempt
          FROM client_invocation_attempts a
         WHERE a.invocation_id = target_invocation_id AND a.attempt_no = 1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'client invocation attempts must start at attempt 1';
        END IF;
        IF attempt_count = 1 THEN
            IF first_attempt.state NOT IN ('prepared', 'not_dispatched') THEN
                RAISE EXCEPTION 'unbound attempt 1 must remain pre-dispatch';
            END IF;
            RETURN NULL;
        END IF;
        SELECT a.state INTO current_attempt
          FROM client_invocation_attempts a
         WHERE a.invocation_id = target_invocation_id AND a.attempt_no = 2;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'attempt 2 requires a durable not-dispatched attempt 1';
        END IF;
        IF first_attempt.state <> 'not_dispatched' OR current_attempt.state NOT IN ('prepared', 'not_dispatched') THEN
            RAISE EXCEPTION 'attempt 2 requires a durable not-dispatched attempt 1';
        END IF;
        RETURN NULL;
    END IF;

    IF parent_row.provider_attempt_no IS NULL OR parent_row.provider_attempt_no NOT IN (1, 2) OR
       attempt_count <> parent_row.provider_attempt_no THEN
        RAISE EXCEPTION 'provider-bound client invocation must reference its latest attempt';
    END IF;
    IF parent_row.provider_attempt_no = 2 THEN
        SELECT a.state INTO first_attempt
          FROM client_invocation_attempts a
         WHERE a.invocation_id = target_invocation_id AND a.attempt_no = 1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'attempt 2 requires a durable not-dispatched attempt 1';
        END IF;
        IF first_attempt.state <> 'not_dispatched' THEN
            RAISE EXCEPTION 'attempt 2 requires a durable not-dispatched attempt 1';
        END IF;
    END IF;
    SELECT a.state, a.provider_account_id, a.provider_platform, a.provider_model,
           a.provider_protocol, a.provider_payload_hash, a.provider_counter_proof
      INTO current_attempt
      FROM client_invocation_attempts a
     WHERE a.invocation_id = target_invocation_id AND a.attempt_no = parent_row.provider_attempt_no;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'provider-bound client invocation has no current attempt';
    END IF;
    IF current_attempt.provider_account_id IS DISTINCT FROM parent_row.provider_account_id OR
       current_attempt.provider_platform IS DISTINCT FROM parent_row.provider_platform OR
       current_attempt.provider_model IS DISTINCT FROM parent_row.provider_model OR
       current_attempt.provider_protocol IS DISTINCT FROM parent_row.provider_protocol OR
       current_attempt.provider_payload_hash IS DISTINCT FROM parent_row.provider_payload_hash OR
       current_attempt.provider_counter_proof IS DISTINCT FROM parent_row.provider_counter_proof OR
       current_attempt.state IS DISTINCT FROM parent_row.state THEN
        RAISE EXCEPTION 'client invocation and current provider attempt are inconsistent';
    END IF;
    RETURN NULL;
END;
$$;

SET CONSTRAINTS ALL IMMEDIATE;
ALTER TABLE client_invocations VALIDATE CONSTRAINT client_invocations_provider_payload_complete;
SET CONSTRAINTS ALL DEFERRED;
