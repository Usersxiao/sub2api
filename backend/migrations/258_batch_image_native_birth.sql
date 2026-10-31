-- Only the typed atomic job+hold path creates these records. Existing jobs
-- and legacy INSERT writers are deliberately not backfilled or auto-enrolled.
-- Old rows remain unknown. Defaults apply only to future INSERTs, and use the
-- full transaction ID so neither xid wrap nor subtransaction xmin is authority.
ALTER TABLE batch_image_balance_lifecycle_journal ADD COLUMN write_xid xid8;
ALTER TABLE batch_image_balance_lifecycle_journal ALTER COLUMN write_xid SET DEFAULT pg_current_xact_id();
ALTER TABLE usage_billing_batch_image_platform_quota_journal ADD COLUMN write_xid xid8;
ALTER TABLE usage_billing_batch_image_platform_quota_journal ALTER COLUMN write_xid SET DEFAULT pg_current_xact_id();

CREATE TABLE batch_image_native_birth (
    batch_id VARCHAR(64) PRIMARY KEY CHECK (batch_id <> ''),
    job_row_id BIGINT NOT NULL UNIQUE CHECK (job_row_id > 0),
    schema_version SMALLINT NOT NULL CHECK (schema_version = 1),
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    api_key_id BIGINT NOT NULL CHECK (api_key_id > 0),
    provider VARCHAR(32) NOT NULL CHECK (provider IN ('gemini_api','vertex')),
    model VARCHAR(128) NOT NULL CHECK (model <> ''),
    request_hash CHAR(64) NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    raw_hold_usd TEXT NOT NULL CHECK (length(raw_hold_usd) BETWEEN 1 AND 512 AND raw_hold_usd ~ '^[0-9]+([.][0-9]+)?$'),
    reserve_fingerprint CHAR(64) NOT NULL CHECK (reserve_fingerprint ~ '^[0-9a-f]{64}$'),
    proof_fingerprint CHAR(64) NOT NULL CHECK (proof_fingerprint ~ '^[0-9a-f]{64}$'),
    write_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (raw_hold_usd::numeric >= 0 AND raw_hold_usd::numeric < 10000000000)
);

-- The birth is permanent creation authority. No later UPDATE, including one
-- replacing write_xid, may reopen a reserve transaction.
CREATE FUNCTION batch_image_native_birth_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE='23514',
        MESSAGE='native batch birth is immutable';
END;
$$;
CREATE TRIGGER batch_image_native_birth_is_immutable
    BEFORE UPDATE OR DELETE ON batch_image_native_birth
    FOR EACH ROW EXECUTE FUNCTION batch_image_native_birth_reject_mutation();

-- A birth record cannot commit as a pending permission to freeze later. Its
-- initial reserve (including zero) must have been observed in this transaction.
CREATE FUNCTION batch_image_native_birth_require_reserve() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    this_xid xid8 := pg_current_xact_id();
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM batch_image_balance_lifecycle_journal p
        WHERE p.batch_id=NEW.batch_id AND p.operation='reserve'
          AND p.request_id='batch_image_hold:' || NEW.batch_id
          AND p.user_id=NEW.user_id AND p.api_key_id=NEW.api_key_id
          AND p.raw_hold_usd=NEW.raw_hold_usd AND p.raw_actual_usd='0'
          AND p.billing_fingerprint=NEW.reserve_fingerprint
          AND ((NEW.raw_hold_usd::numeric>0 AND p.outcome='reserved'
                AND p.reserve_origin='observed_reserve' AND p.reserve_fingerprint=NEW.reserve_fingerprint)
            OR (NEW.raw_hold_usd::numeric=0 AND p.outcome='zero_hold'
                AND p.reserve_origin='none' AND p.reserve_fingerprint=''))
          AND NEW.write_xid=this_xid AND p.write_xid=this_xid
    ) THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='native batch birth requires a reserve proof from the current transaction';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER batch_image_native_birth_requires_reserve
    AFTER INSERT ON batch_image_native_birth
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION batch_image_native_birth_require_reserve();

-- Independent immutable money witnesses survive receipt/proof loss. They are
-- enrolled only for native batches by the canonical hot-dedup commit fence.
-- No foreign key can remove them with a job, key, user or birth record.
CREATE TABLE batch_image_native_money_operations (
    batch_id VARCHAR(64) NOT NULL CHECK (batch_id <> ''),
    operation VARCHAR(16) NOT NULL CHECK (operation IN ('reserve','capture','release')),
    request_id VARCHAR(255) NOT NULL,
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    api_key_id BIGINT NOT NULL CHECK (api_key_id > 0),
    raw_hold_usd TEXT NOT NULL CHECK (length(raw_hold_usd) BETWEEN 1 AND 512 AND raw_hold_usd ~ '^[0-9]+([.][0-9]+)?$'),
    raw_actual_usd TEXT NOT NULL CHECK (length(raw_actual_usd) BETWEEN 1 AND 512 AND raw_actual_usd ~ '^[0-9]+([.][0-9]+)?$'),
    billing_fingerprint CHAR(64) NOT NULL CHECK (billing_fingerprint ~ '^[0-9a-f]{64}$'),
    proof_fingerprint CHAR(64) NOT NULL CHECK (proof_fingerprint ~ '^[0-9a-f]{64}$'),
    write_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id,operation),
    UNIQUE (request_id,api_key_id),
    CHECK (request_id = (CASE operation WHEN 'reserve' THEN 'batch_image_hold:'
        WHEN 'capture' THEN 'batch_image_capture:' ELSE 'batch_image_release:' END) || batch_id),
    CHECK (raw_hold_usd::numeric >= 0 AND raw_hold_usd::numeric < 10000000000),
    CHECK (raw_actual_usd::numeric >= 0 AND raw_actual_usd::numeric < 10000000000),
    CHECK (operation='capture' OR raw_actual_usd::numeric=0)
);
CREATE UNIQUE INDEX batch_image_native_money_terminal
    ON batch_image_native_money_operations(batch_id) WHERE operation IN ('capture','release');
CREATE FUNCTION batch_image_native_money_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE='23514',
        MESSAGE='native batch money operation is immutable';
END;
$$;
CREATE TRIGGER batch_image_native_money_is_immutable
    BEFORE UPDATE OR DELETE ON batch_image_native_money_operations
    FOR EACH ROW EXECUTE FUNCTION batch_image_native_money_reject_mutation();

-- Native canonical money writes must produce a new matching proof. Merely
-- finding an old permanent proof would allow an old writer to charge again
-- after dedup cleanup. Archive movement never enters this INSERT fence.
CREATE FUNCTION batch_image_native_dedup_require_current_proof() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    operation_name text;
    target_batch text;
    birth batch_image_native_birth%ROWTYPE;
    proof batch_image_balance_lifecycle_journal%ROWTYPE;
    this_xid xid8 := pg_current_xact_id();
BEGIN
    IF left(NEW.request_id,17)='batch_image_hold:' THEN
        operation_name := 'reserve'; target_batch := substring(NEW.request_id FROM 18);
    ELSIF left(NEW.request_id,20)='batch_image_capture:' THEN
        operation_name := 'capture'; target_batch := substring(NEW.request_id FROM 21);
    ELSIF left(NEW.request_id,20)='batch_image_release:' THEN
        operation_name := 'release'; target_batch := substring(NEW.request_id FROM 21);
    ELSE
        RETURN NEW;
    END IF;
    SELECT * INTO birth FROM batch_image_native_birth WHERE batch_id=target_batch;
    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM batch_image_native_money_operations WHERE batch_id=target_batch) THEN
            RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='native batch birth is missing for permanent money history';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.api_key_id<>birth.api_key_id
      OR (operation_name='reserve' AND birth.write_xid<>this_xid) THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='native batch dedup requires a money proof from the current transaction';
    END IF;
    SELECT p.* INTO proof FROM batch_image_balance_lifecycle_journal p
        WHERE p.batch_id=birth.batch_id AND p.operation=operation_name
          AND p.request_id=NEW.request_id AND p.api_key_id=NEW.api_key_id
          AND p.user_id=birth.user_id AND p.raw_hold_usd=birth.raw_hold_usd
          AND p.billing_fingerprint=NEW.request_fingerprint
          AND (operation_name<>'reserve' OR p.billing_fingerprint=birth.reserve_fingerprint)
          AND ((birth.raw_hold_usd::numeric>0 AND p.reserve_origin='observed_reserve'
                AND p.reserve_fingerprint=birth.reserve_fingerprint
                AND ((operation_name='reserve' AND p.outcome='reserved')
                  OR (operation_name='capture' AND p.outcome='captured')
                  OR (operation_name='release' AND p.outcome='refunded')))
            OR (birth.raw_hold_usd::numeric=0 AND p.reserve_origin='none' AND p.reserve_fingerprint=''
                AND ((operation_name IN ('reserve','release') AND p.outcome='zero_hold')
                  OR (operation_name='capture' AND p.outcome='captured' AND p.raw_actual_usd='0'))))
          AND p.write_xid=this_xid;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='native batch dedup requires a money proof from the current transaction';
    END IF;
    IF operation_name='capture' AND NOT EXISTS (
        SELECT 1 FROM usage_billing_batch_image_platform_quota_journal q
        WHERE q.batch_id=birth.batch_id AND q.request_id=proof.request_id
          AND q.user_id=birth.user_id AND q.api_key_id=birth.api_key_id
          AND q.provider=birth.provider AND q.raw_hold_usd=birth.raw_hold_usd
          AND q.raw_cost_usd=proof.raw_actual_usd AND q.billing_fingerprint=proof.billing_fingerprint
          AND q.write_xid=this_xid
    ) THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='native batch capture requires a platform proof from the current transaction';
    END IF;
    INSERT INTO batch_image_native_money_operations
        (batch_id,operation,request_id,user_id,api_key_id,raw_hold_usd,raw_actual_usd,billing_fingerprint,proof_fingerprint)
        VALUES (proof.batch_id,proof.operation,proof.request_id,proof.user_id,proof.api_key_id,
            proof.raw_hold_usd,proof.raw_actual_usd,proof.billing_fingerprint,proof.proof_fingerprint)
        ON CONFLICT DO NOTHING;
    IF NOT EXISTS (
        SELECT 1 FROM batch_image_native_money_operations w
        WHERE w.batch_id=proof.batch_id AND w.operation=proof.operation AND w.request_id=proof.request_id
          AND w.user_id=proof.user_id AND w.api_key_id=proof.api_key_id
          AND w.raw_hold_usd=proof.raw_hold_usd AND w.raw_actual_usd=proof.raw_actual_usd
          AND w.billing_fingerprint=proof.billing_fingerprint AND w.proof_fingerprint=proof.proof_fingerprint
          AND w.write_xid=this_xid
    ) THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='native batch money operation has already committed';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER batch_image_native_dedup_requires_current_proof
    AFTER INSERT ON usage_billing_dedup
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION batch_image_native_dedup_require_current_proof();
