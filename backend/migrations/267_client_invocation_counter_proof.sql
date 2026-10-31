-- Persist the exact pre-dispatch context-count decision beside the immutable
-- provider payload binding. The JSON contains only counts and authority labels.
ALTER TABLE client_invocations
    ADD COLUMN provider_counter_proof JSONB,
    ADD CONSTRAINT client_invocations_provider_counter_proof_valid CHECK (
        provider_counter_proof IS NULL OR (
            jsonb_typeof(provider_counter_proof) = 'object' AND
            octet_length(provider_counter_proof::text) <= 2048 AND
            provider_counter_proof ?& ARRAY['required','output_tokens'] AND
            jsonb_typeof(provider_counter_proof->'required') = 'boolean' AND
            jsonb_typeof(provider_counter_proof->'output_tokens') = 'number' AND
            (provider_counter_proof->>'output_tokens') ~ '^[1-9][0-9]{0,18}$' AND
            (
                (provider_counter_proof->>'required' = 'false' AND
                    provider_counter_proof - ARRAY['required','output_tokens'] = '{}'::jsonb AND
                    NOT (provider_counter_proof ?| ARRAY['input_tokens','context_tokens','authority_kind','authority_id','authority_revision'])) OR
                (provider_counter_proof->>'required' = 'true' AND
                    provider_counter_proof - ARRAY['required','input_tokens','output_tokens','context_tokens','authority_kind','authority_id','authority_revision'] = '{}'::jsonb AND
                    provider_counter_proof ?& ARRAY['input_tokens','context_tokens','authority_kind','authority_id','authority_revision'] AND
                    jsonb_typeof(provider_counter_proof->'input_tokens') = 'number' AND
                    jsonb_typeof(provider_counter_proof->'context_tokens') = 'number' AND
                    jsonb_typeof(provider_counter_proof->'authority_kind') = 'string' AND
                    jsonb_typeof(provider_counter_proof->'authority_id') = 'string' AND
                    jsonb_typeof(provider_counter_proof->'authority_revision') = 'string' AND
                    (provider_counter_proof->>'input_tokens') ~ '^(0|[1-9][0-9]{0,18})$' AND
                    (provider_counter_proof->>'context_tokens') ~ '^[1-9][0-9]{0,18}$' AND
                    (provider_counter_proof->>'input_tokens')::numeric <= 9223372036854775807 AND
                    (provider_counter_proof->>'context_tokens')::numeric <= 9223372036854775807 AND
                    (provider_counter_proof->>'output_tokens')::numeric <= (provider_counter_proof->>'context_tokens')::numeric AND
                    (provider_counter_proof->>'input_tokens')::numeric + (provider_counter_proof->>'output_tokens')::numeric <=
                        (provider_counter_proof->>'context_tokens')::numeric AND
                    provider_counter_proof->>'authority_kind' IN ('tokenizer','native') AND
                    provider_counter_proof->>'authority_id' ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$' AND
                    provider_counter_proof->>'authority_revision' ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$')
            )
        )
    );

CREATE OR REPLACE FUNCTION protect_client_invocation_counter_proof() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.provider_counter_proof IS DISTINCT FROM OLD.provider_counter_proof THEN
        IF OLD.state <> 'reserved' OR NEW.state <> 'dispatched' OR
            OLD.provider_counter_proof IS NOT NULL OR NEW.provider_counter_proof IS NULL THEN
            RAISE EXCEPTION 'client invocation counter proof is immutable';
        END IF;
    END IF;
    IF OLD.state='reserved' AND NEW.state='dispatched' AND NEW.provider_counter_proof IS NULL THEN
        RAISE EXCEPTION 'client invocation dispatch requires a counter proof';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER client_invocations_counter_proof_guard
    BEFORE UPDATE ON client_invocations
    FOR EACH ROW EXECUTE FUNCTION protect_client_invocation_counter_proof();
