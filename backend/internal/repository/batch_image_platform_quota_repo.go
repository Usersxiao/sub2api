package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const batchImagePlatformQuotaJournalColumns = platformQuotaJournalSnapshotColumns + `,batch_id,provider,raw_hold_usd`

type batchImagePlatformQuotaJournal struct {
	*ordinaryPlatformQuotaJournal
	batchID, provider, rawHoldUSD string
}

func findBatchImagePlatformQuotaJournal(ctx context.Context, tx *sql.Tx, requestID string, apiKeyID int64) (*batchImagePlatformQuotaJournal, error) {
	j, err := scanBatchImagePlatformQuotaJournal(tx.QueryRowContext(ctx, `SELECT `+batchImagePlatformQuotaJournalColumns+`
		FROM usage_billing_batch_image_platform_quota_journal WHERE request_id=$1 AND api_key_id=$2`, requestID, apiKeyID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

func scanBatchImagePlatformQuotaJournal(scanner sessionScanner) (*batchImagePlatformQuotaJournal, error) {
	j := &batchImagePlatformQuotaJournal{ordinaryPlatformQuotaJournal: &ordinaryPlatformQuotaJournal{}}
	err := scanner.Scan(&j.requestID, &j.apiKeyID, &j.userID, &j.schemaVersion, &j.billingFingerprint,
		&j.intentFingerprint, &j.platform, &j.rawCostUSD, &j.costUSD, &j.isSubscription, &j.capturedAt,
		&j.disposition, &j.quotaRowID, &j.dailyLimit, &j.weeklyLimit, &j.monthlyLimit,
		&j.dailyUsage, &j.weeklyUsage, &j.monthlyUsage, &j.dailyStart, &j.weeklyStart, &j.monthlyStart,
		&j.batchID, &j.provider, &j.rawHoldUSD)
	return j, err
}

func verifyBatchImageBillingProofs(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) error {
	var matches bool
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(bool_and(request_fingerprint=$3),TRUE) FROM (
		SELECT request_fingerprint FROM usage_billing_dedup WHERE request_id=$1 AND api_key_id=$2
		UNION ALL
		SELECT request_fingerprint FROM usage_billing_dedup_archive WHERE request_id=$1 AND api_key_id=$2
	) AS billing_proofs`, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint).Scan(&matches)
	if err != nil {
		return err
	}
	if !matches {
		return service.ErrUsageBillingRequestConflict
	}
	return nil
}

func verifyBatchImagePlatformQuotaJournal(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand, j *batchImagePlatformQuotaJournal) error {
	if j == nil || j.requestID != cmd.RequestID || j.apiKeyID != cmd.APIKeyID || j.userID != cmd.UserID ||
		j.batchID != cmd.BatchID || j.billingFingerprint != cmd.RequestFingerprint {
		return service.ErrUsageBillingRequestConflict
	}
	stored := &service.BatchImagePlatformQuotaIntent{Version: j.schemaVersion, RequestID: j.requestID,
		APIKeyID: j.apiKeyID, UserID: j.userID, BatchID: j.batchID, Provider: j.provider, Platform: j.platform,
		RawHoldUSD: j.rawHoldUSD, RawCostUSD: j.rawCostUSD, CostUSD: j.costUSD, IntentFingerprint: j.intentFingerprint}
	if err := service.ValidateBatchImagePlatformQuotaIntent(cmd, stored); err != nil {
		return service.ErrUsageBillingRequestConflict
	}
	if cmd.CapturePlatformQuota != nil && *cmd.CapturePlatformQuota != *stored {
		return service.ErrUsageBillingRequestConflict
	}
	return verifyBatchImageBillingProofs(ctx, tx, cmd)
}

func observeBatchImagePlatformQuota(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) (*batchImagePlatformQuotaJournal, error) {
	i := cmd.CapturePlatformQuota
	core, err := observePlatformQuotaSnapshot(ctx, tx, &ordinaryPlatformQuotaJournal{requestID: i.RequestID,
		apiKeyID: i.APIKeyID, userID: i.UserID, schemaVersion: i.Version, billingFingerprint: cmd.RequestFingerprint,
		intentFingerprint: i.IntentFingerprint, platform: i.Platform, rawCostUSD: i.RawCostUSD, costUSD: i.CostUSD,
		disposition: "unlimited"})
	if err != nil {
		return nil, err
	}
	return &batchImagePlatformQuotaJournal{core, i.BatchID, i.Provider, i.RawHoldUSD}, nil
}

func insertBatchImagePlatformQuotaJournal(ctx context.Context, tx *sql.Tx, j *batchImagePlatformQuotaJournal) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO usage_billing_batch_image_platform_quota_journal (`+
		batchImagePlatformQuotaJournalColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)`,
		j.requestID, j.apiKeyID, j.userID, j.schemaVersion, j.billingFingerprint, j.intentFingerprint,
		j.platform, j.rawCostUSD, j.costUSD, j.isSubscription, j.capturedAt, j.disposition, j.quotaRowID,
		j.dailyLimit, j.weeklyLimit, j.monthlyLimit, j.dailyUsage, j.weeklyUsage, j.monthlyUsage,
		j.dailyStart, j.weeklyStart, j.monthlyStart, j.batchID, j.provider, j.rawHoldUSD)
	return err
}

func lockBatchImageBalanceOperation(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) error {
	// The batch ID is globally unique. Changed owners and keys must serialize
	// before any wallet effect, rather than opening another lifecycle.
	identity := fmt.Sprintf("sub2api:batch-image-balance:%s", cmd.BatchID)
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, identity)
	return err
}

func batchImageCaptureProofExists(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM usage_billing_batch_image_platform_quota_journal WHERE request_id=$1 AND api_key_id=$2
		UNION ALL
		SELECT 1 FROM usage_billing_dedup WHERE request_id=$1 AND api_key_id=$2
		UNION ALL
		SELECT 1 FROM usage_billing_dedup_archive WHERE request_id=$1 AND api_key_id=$2
	)`, service.BatchImageCaptureRequestID(cmd.BatchID), cmd.APIKeyID).Scan(&exists)
	return exists, err
}

func batchImageReleaseProofExists(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM usage_billing_dedup WHERE request_id=$1 AND api_key_id=$2
		UNION ALL
		SELECT 1 FROM usage_billing_dedup_archive WHERE request_id=$1 AND api_key_id=$2
	)`, service.BatchImageReleaseRequestID(cmd.BatchID), cmd.APIKeyID).Scan(&exists)
	return exists, err
}
