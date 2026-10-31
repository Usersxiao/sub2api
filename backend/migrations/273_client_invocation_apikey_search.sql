-- Versioned official API Key hosted Responses search request-count proof.
-- Existing PAT and text proof semantics and immutable bindings remain in force.
CREATE FUNCTION client_invocation_search_counter_valid(proof jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $fn$ SELECT proof IN (
 '{"search_calls":1,"unit":"request","authority":"alpha-pat-v1"}'::jsonb,
 '{"search_calls":1,"unit":"request","authority":"responses-apikey-v1"}'::jsonb
) $fn$;
ALTER TABLE client_invocations
 DROP CONSTRAINT client_invocations_provider_counter_proof_valid,
 ADD CONSTRAINT client_invocations_provider_counter_proof_valid CHECK (
  (operation='text' AND client_invocation_text_counter_valid(provider_counter_proof)) OR
  (operation='search' AND (provider_counter_proof IS NULL OR client_invocation_search_counter_valid(provider_counter_proof)))
 );
CREATE OR REPLACE FUNCTION guard_client_invocation_search_proof() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_operation text;
BEGIN
 IF TG_TABLE_NAME='client_invocations' THEN
  IF NEW.operation IS DISTINCT FROM OLD.operation THEN RAISE EXCEPTION 'invocation operation is immutable'; END IF;
 ELSE
  SELECT operation INTO STRICT parent_operation FROM client_invocations WHERE invocation_id=NEW.invocation_id;
  IF parent_operation='search' THEN
   IF NEW.provider_protocol<>'web-search' OR NEW.provider_platform<>'openai' OR
      client_invocation_search_counter_valid(NEW.provider_counter_proof) IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'search requires its request counter proof';
   END IF;
  ELSIF NEW.provider_protocol='web-search' OR NEW.provider_counter_proof ?| ARRAY['search_calls','unit','authority'] THEN
   RAISE EXCEPTION 'text requires its token counter proof';
  END IF;
 END IF;
 RETURN NEW;
END $$;
