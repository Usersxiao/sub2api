-- Only new ordinary Apply observations can certify their control generation.
-- Existing journals retain NULL: today's quota row is not historical proof.
ALTER TABLE usage_billing_ordinary_platform_quota_journal
    ADD COLUMN control_generation BIGINT
    CHECK (control_generation IS NULL OR (control_generation > 0 AND quota_row_id IS NOT NULL));
