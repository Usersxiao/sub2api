-- Search has an independent request counter; retain the original text predicate.
CREATE FUNCTION client_invocation_text_counter_valid(proof jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $fn$ SELECT proof IS NULL OR (
            jsonb_typeof(proof) = 'object' AND
            octet_length(proof::text) <= 2048 AND
            proof ?& ARRAY['required','output_tokens'] AND
            jsonb_typeof(proof->'required') = 'boolean' AND
            jsonb_typeof(proof->'output_tokens') = 'number' AND
            (proof->>'output_tokens') ~ '^[1-9][0-9]{0,18}$' AND
            (
                (proof->>'required' = 'false' AND
                    proof - ARRAY['required','output_tokens'] = '{}'::jsonb AND
                    NOT (proof ?| ARRAY['input_tokens','context_tokens','authority_kind','authority_id','authority_revision'])) OR
                (proof->>'required' = 'true' AND
                    proof - ARRAY['required','input_tokens','output_tokens','context_tokens','authority_kind','authority_id','authority_revision'] = '{}'::jsonb AND
                    proof ?& ARRAY['input_tokens','context_tokens','authority_kind','authority_id','authority_revision'] AND
                    jsonb_typeof(proof->'input_tokens') = 'number' AND
                    jsonb_typeof(proof->'context_tokens') = 'number' AND
                    jsonb_typeof(proof->'authority_kind') = 'string' AND
                    jsonb_typeof(proof->'authority_id') = 'string' AND
                    jsonb_typeof(proof->'authority_revision') = 'string' AND
                    (proof->>'input_tokens') ~ '^(0|[1-9][0-9]{0,18})$' AND
                    (proof->>'context_tokens') ~ '^[1-9][0-9]{0,18}$' AND
                    (proof->>'input_tokens')::numeric <= 9223372036854775807 AND
                    (proof->>'context_tokens')::numeric <= 9223372036854775807 AND
                    (proof->>'output_tokens')::numeric <= (proof->>'context_tokens')::numeric AND
                    (proof->>'input_tokens')::numeric + (proof->>'output_tokens')::numeric <=
                        (proof->>'context_tokens')::numeric AND
                    proof->>'authority_kind' IN ('tokenizer','native') AND
                    proof->>'authority_id' ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$' AND
                    proof->>'authority_revision' ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$')
            )
        ) $fn$;
ALTER TABLE client_invocations
 DROP CONSTRAINT client_invocations_operation_check,
 ADD CONSTRAINT client_invocations_operation_check CHECK (operation IN ('text','search')),
 DROP CONSTRAINT client_invocations_provider_counter_proof_valid,
 ADD CONSTRAINT client_invocations_provider_counter_proof_valid CHECK (
  (operation='text' AND client_invocation_text_counter_valid(provider_counter_proof)) OR
  (operation='search' AND (provider_counter_proof IS NULL OR provider_counter_proof = '{"search_calls":1,"unit":"request","authority":"alpha-pat-v1"}'::jsonb))
 ),
 DROP CONSTRAINT client_invocations_provider_payload_complete,
 ADD CONSTRAINT client_invocations_provider_payload_complete CHECK (
  num_nonnulls(provider_account_id,provider_platform,provider_model,provider_protocol,provider_payload_hash,provider_attempt_no)=0 OR
  (num_nonnulls(provider_account_id,provider_platform,provider_model,provider_protocol,provider_payload_hash,provider_attempt_no)=6 AND
   provider_account_id>0 AND provider_platform ~ '^[a-z][a-z0-9_]{0,31}$' AND provider_model<>'' AND
   provider_payload_hash ~ '^[0-9a-f]{64}$' AND provider_attempt_no IN (1,2) AND
   ((operation='text' AND provider_protocol IN ('messages','responses','chat-completions')) OR
    (operation='search' AND provider_protocol='web-search' AND provider_platform='openai')))
 );
ALTER TABLE client_invocation_attempts DROP CONSTRAINT client_invocation_attempts_provider_protocol_check,
 ADD CONSTRAINT client_invocation_attempts_provider_protocol_check CHECK (provider_protocol IN ('messages','responses','chat-completions','web-search'));

-- Check prepared candidates too, before a parent dispatch binding exists.
CREATE FUNCTION guard_client_invocation_search_proof() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_operation text;
BEGIN
 IF TG_TABLE_NAME='client_invocations' THEN
  IF NEW.operation IS DISTINCT FROM OLD.operation THEN RAISE EXCEPTION 'invocation operation is immutable'; END IF;
 ELSE
  SELECT operation INTO STRICT parent_operation FROM client_invocations WHERE invocation_id=NEW.invocation_id;
  IF parent_operation='search' THEN
   IF NEW.provider_protocol<>'web-search' OR NEW.provider_platform<>'openai' OR
      NEW.provider_counter_proof IS DISTINCT FROM '{"search_calls":1,"unit":"request","authority":"alpha-pat-v1"}'::jsonb THEN
    RAISE EXCEPTION 'search requires its request counter proof';
   END IF;
  -- Preserve legacy prepared text candidates; the parent dispatch still
  -- enforces the original complete text proof predicate.
  ELSIF NEW.provider_protocol='web-search' OR NEW.provider_counter_proof ?| ARRAY['search_calls','unit','authority'] THEN
   RAISE EXCEPTION 'text requires its token counter proof';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER client_invocations_operation_guard BEFORE UPDATE ON client_invocations
 FOR EACH ROW EXECUTE FUNCTION guard_client_invocation_search_proof();
CREATE TRIGGER client_invocation_attempts_search_proof_guard BEFORE INSERT OR UPDATE ON client_invocation_attempts
 FOR EACH ROW EXECUTE FUNCTION guard_client_invocation_search_proof();
