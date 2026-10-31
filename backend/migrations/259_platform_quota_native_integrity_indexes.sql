-- Native integrity reads find membership by user before loading complete
-- per-batch evidence. These indexes bound that lookup to the selected user;
-- they grant no cohort identity, reset baseline or accounting coverage.
CREATE INDEX batch_image_native_birth_user_batch
    ON batch_image_native_birth(user_id, batch_id);
CREATE INDEX batch_image_native_money_user_batch
    ON batch_image_native_money_operations(user_id, batch_id);
CREATE INDEX batch_image_lifecycle_user_batch
    ON batch_image_balance_lifecycle_journal(user_id, batch_id);
