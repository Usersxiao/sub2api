-- Observed money outcomes only. This does not backfill historical operations
-- or certify that an absent receipt identifies a newly created batch.
CREATE TABLE batch_image_balance_lifecycle_journal (
    batch_id VARCHAR(64) NOT NULL CHECK (batch_id <> ''),
    operation VARCHAR(16) NOT NULL CHECK (operation IN ('reserve','capture','release')),
    request_id VARCHAR(255) NOT NULL,
    user_id BIGINT NOT NULL CHECK (user_id > 0),
    api_key_id BIGINT NOT NULL CHECK (api_key_id > 0),
    schema_version SMALLINT NOT NULL CHECK (schema_version = 1),
    billing_fingerprint CHAR(64) NOT NULL CHECK (billing_fingerprint ~ '^[0-9a-f]{64}$'),
    proof_fingerprint CHAR(64) NOT NULL CHECK (proof_fingerprint ~ '^[0-9a-f]{64}$'),
    raw_hold_usd TEXT NOT NULL CHECK (length(raw_hold_usd) BETWEEN 1 AND 512 AND raw_hold_usd ~ '^[0-9]+([.][0-9]+)?$'),
    raw_actual_usd TEXT NOT NULL CHECK (length(raw_actual_usd) BETWEEN 1 AND 512 AND raw_actual_usd ~ '^[0-9]+([.][0-9]+)?$'),
    outcome VARCHAR(32) NOT NULL CHECK (outcome IN ('reserved','captured','refunded','no_hold_terminal','zero_hold')),
    reserve_origin VARCHAR(32) NOT NULL CHECK (reserve_origin IN ('observed_reserve','legacy_dedup','none')),
    reserve_fingerprint VARCHAR(64) NOT NULL CHECK (reserve_fingerprint = '' OR reserve_fingerprint ~ '^[0-9a-f]{64}$'),
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (batch_id,operation),
    UNIQUE (request_id,api_key_id),
    CHECK (request_id = (CASE operation WHEN 'reserve' THEN 'batch_image_hold:'
        WHEN 'capture' THEN 'batch_image_capture:' ELSE 'batch_image_release:' END) || batch_id),
    CHECK (raw_hold_usd::numeric >= 0 AND raw_hold_usd::numeric < 10000000000),
    CHECK (raw_actual_usd::numeric >= 0 AND raw_actual_usd::numeric < 10000000000),
    CHECK (operation = 'capture' OR raw_actual_usd::numeric = 0),
    CHECK (
        (operation='reserve' AND outcome='reserved' AND raw_hold_usd::numeric>0
            AND reserve_origin='observed_reserve' AND reserve_fingerprint=billing_fingerprint) OR
        (operation IN ('reserve','release') AND outcome='zero_hold' AND raw_hold_usd::numeric=0
            AND reserve_origin='none' AND reserve_fingerprint='') OR
        (operation='capture' AND outcome='captured' AND raw_hold_usd::numeric>0
            AND reserve_origin IN ('observed_reserve','legacy_dedup') AND reserve_fingerprint<>'') OR
        (operation='capture' AND outcome='captured' AND raw_hold_usd::numeric=0 AND raw_actual_usd::numeric=0
            AND reserve_origin='none' AND reserve_fingerprint='') OR
        (operation='release' AND outcome='refunded' AND raw_hold_usd::numeric>0
            AND reserve_origin IN ('observed_reserve','legacy_dedup') AND reserve_fingerprint<>'') OR
        (operation='release' AND outcome='no_hold_terminal' AND raw_hold_usd::numeric>0
            AND reserve_origin='none' AND reserve_fingerprint='')
    )
);
-- Every observed terminal result closes the batch, including a no-op release.
CREATE UNIQUE INDEX batch_image_balance_lifecycle_terminal
    ON batch_image_balance_lifecycle_journal(batch_id) WHERE operation IN ('capture','release');
