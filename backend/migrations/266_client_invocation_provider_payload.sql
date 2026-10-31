-- Persist the exact account and provider payload authorized at the final
-- outbound boundary before a client invocation enters dispatched state.
ALTER TABLE client_invocations
    ADD COLUMN provider_account_id BIGINT,
    ADD COLUMN provider_platform VARCHAR(32),
    ADD COLUMN provider_model VARCHAR(500),
    ADD COLUMN provider_protocol VARCHAR(32),
    ADD COLUMN provider_payload_hash CHAR(64),
    ADD CONSTRAINT client_invocations_provider_payload_complete CHECK (
        num_nonnulls(provider_account_id,provider_platform,provider_model,provider_protocol,provider_payload_hash) = 0 OR
        (num_nonnulls(provider_account_id,provider_platform,provider_model,provider_protocol,provider_payload_hash) = 5 AND
         provider_account_id > 0 AND provider_platform ~ '^[a-z][a-z0-9_]{0,31}$' AND
         provider_model <> '' AND provider_protocol IN ('messages','responses','chat-completions') AND
         provider_payload_hash ~ '^[0-9a-f]{64}$')
    );

CREATE OR REPLACE FUNCTION protect_client_invocation_provider_payload() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.provider_account_id,NEW.provider_platform,NEW.provider_model,NEW.provider_protocol,NEW.provider_payload_hash)
        IS DISTINCT FROM ROW(OLD.provider_account_id,OLD.provider_platform,OLD.provider_model,OLD.provider_protocol,OLD.provider_payload_hash) THEN
        IF OLD.state <> 'reserved' OR NEW.state <> 'dispatched' OR
            OLD.provider_payload_hash IS NOT NULL OR NEW.provider_payload_hash IS NULL THEN
            RAISE EXCEPTION 'client invocation provider payload is immutable';
        END IF;
    END IF;
    IF OLD.state='reserved' AND NEW.state='dispatched' AND NEW.provider_payload_hash IS NULL THEN
        RAISE EXCEPTION 'client invocation dispatch requires a provider payload binding';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER client_invocations_provider_payload_guard
    BEFORE UPDATE ON client_invocations
    FOR EACH ROW EXECUTE FUNCTION protect_client_invocation_provider_payload();
