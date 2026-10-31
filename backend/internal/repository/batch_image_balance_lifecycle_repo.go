package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const batchImageBalanceLifecycleColumns = `schema_version,operation,request_id,batch_id,user_id,api_key_id,
	billing_fingerprint,proof_fingerprint,raw_hold_usd,raw_actual_usd,outcome,reserve_origin,reserve_fingerprint`

func batchImageBalanceOperationName(operation batchImageBalanceOperation) string {
	switch operation {
	case batchImageBalanceReserve:
		return "reserve"
	case batchImageBalanceCapture:
		return "capture"
	case batchImageBalanceRelease:
		return "release"
	default:
		return ""
	}
}

// Freeze a valid prospective result to validate every command, including the
// compatibility path with no platform intent. Authority is resolved later in
// the transaction; this candidate is never stored as an observed outcome.
func validateBatchImageBalanceLifecycleCommand(cmd *service.BatchImageBalanceHoldCommand, operation batchImageBalanceOperation) (*service.BatchImageBalanceLifecycleProof, error) {
	op := batchImageBalanceOperationName(operation)
	outcome, origin, reserveFP := "zero_hold", "none", ""
	if cmd.HoldAmount > 0 {
		switch operation {
		case batchImageBalanceReserve:
			outcome, origin, reserveFP = "reserved", "observed_reserve", cmd.RequestFingerprint
		case batchImageBalanceCapture:
			outcome, origin, reserveFP = "captured", "observed_reserve", cmd.RequestFingerprint
			if cmd.ExpectedReserveFingerprint != "" {
				reserveFP = cmd.ExpectedReserveFingerprint
			}
		case batchImageBalanceRelease:
			outcome = "no_hold_terminal"
		}
	} else if operation == batchImageBalanceCapture {
		outcome = "captured"
	}
	return service.FreezeBatchImageBalanceLifecycleProof(cmd, op, outcome, origin, reserveFP)
}

func batchImageBalanceLifecycleRequestExists(ctx context.Context, tx *sql.Tx, requestID string, apiKeyID int64) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM batch_image_balance_lifecycle_journal
		WHERE request_id=$1 AND api_key_id=$2)`, requestID, apiKeyID).Scan(&exists)
	return exists, err
}

func loadBatchImageBalanceLifecycle(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand, rawHold string) (map[string]*service.BatchImageBalanceLifecycleProof, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+batchImageBalanceLifecycleColumns+`
		FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1 ORDER BY operation`, cmd.BatchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	proofs := make(map[string]*service.BatchImageBalanceLifecycleProof)
	for rows.Next() {
		proof := &service.BatchImageBalanceLifecycleProof{}
		if err := rows.Scan(&proof.Version, &proof.Operation, &proof.RequestID, &proof.BatchID,
			&proof.UserID, &proof.APIKeyID, &proof.BillingFingerprint, &proof.ProofFingerprint,
			&proof.RawHoldUSD, &proof.RawActualUSD, &proof.Outcome, &proof.ReserveOrigin, &proof.ReserveFingerprint); err != nil {
			return nil, err
		}
		if service.ValidateBatchImageBalanceLifecycleStoredProof(proof) != nil ||
			proof.UserID != cmd.UserID || proof.APIKeyID != cmd.APIKeyID || proof.RawHoldUSD != rawHold {
			return nil, service.ErrBatchImageBalanceLifecycleConflict
		}
		proofs[proof.Operation] = proof
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if proofs["capture"] != nil && proofs["release"] != nil {
		return nil, service.ErrBatchImageBalanceLifecycleConflict
	}
	return proofs, nil
}

func loadBatchImagePlatformQuotaForLifecycle(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand, rawHold string) (*batchImagePlatformQuotaJournal, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+batchImagePlatformQuotaJournalColumns+`
		FROM usage_billing_batch_image_platform_quota_journal WHERE batch_id=$1`, cmd.BatchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result *batchImagePlatformQuotaJournal
	for rows.Next() {
		j, err := scanBatchImagePlatformQuotaJournal(rows)
		if err != nil {
			return nil, err
		}
		if result != nil || j.userID != cmd.UserID || j.apiKeyID != cmd.APIKeyID || j.rawHoldUSD != rawHold {
			return nil, service.ErrBatchImageBalanceLifecycleConflict
		}
		result = j
	}
	return result, rows.Err()
}

// Check both tiers and every key for the canonical batch operation. A changed
// key must not open a second lifecycle for the globally unique batch ID.
func verifyBatchImageLifecycleBillingProofs(ctx context.Context, tx *sql.Tx, requestID string, apiKeyID int64, fingerprint string) (bool, error) {
	var count int
	var matches bool
	err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(bool_and(api_key_id=$2 AND request_fingerprint=$3),TRUE) FROM (
		SELECT api_key_id,request_fingerprint FROM usage_billing_dedup WHERE request_id=$1
		UNION ALL
		SELECT api_key_id,request_fingerprint FROM usage_billing_dedup_archive WHERE request_id=$1
	) AS proofs`, requestID, apiKeyID, fingerprint).Scan(&count, &matches)
	if err != nil {
		return false, err
	}
	if !matches {
		return false, service.ErrBatchImageBalanceLifecycleConflict
	}
	return count > 0, nil
}

func batchImageLifecycleLegacyRequestExists(ctx context.Context, tx *sql.Tx, requestID string) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM usage_billing_dedup WHERE request_id=$1
		UNION ALL SELECT 1 FROM usage_billing_dedup_archive WHERE request_id=$1
	)`, requestID).Scan(&exists)
	return exists, err
}

func batchImageBalanceLifecycleReserveSource(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand, proofs map[string]*service.BatchImageBalanceLifecycleProof, operation batchImageBalanceOperation) (string, string, error) {
	if cmd.HoldAmount == 0 {
		return "none", "", nil
	}
	if reserve := proofs["reserve"]; reserve != nil {
		if reserve.Outcome != "reserved" || (cmd.ExpectedReserveFingerprint != "" && cmd.ExpectedReserveFingerprint != reserve.BillingFingerprint) {
			return "", "", service.ErrBatchImageBalanceLifecycleConflict
		}
		if _, err := verifyBatchImageLifecycleBillingProofs(ctx, tx, reserve.RequestID, cmd.APIKeyID, reserve.BillingFingerprint); err != nil {
			return "", "", err
		}
		return "observed_reserve", reserve.BillingFingerprint, nil
	}
	holdID := service.BatchImageHoldRequestID(cmd.BatchID)
	exists, err := batchImageLifecycleLegacyRequestExists(ctx, tx, holdID)
	if err != nil {
		return "", "", err
	}
	if !exists {
		if operation == batchImageBalanceRelease {
			return "none", "", nil
		}
		return "", "", service.ErrBatchImageBalanceLifecycleHistory
	}
	expected := cmd.ExpectedReserveFingerprint
	if expected == "" && operation == batchImageBalanceRelease {
		// Request ID is deliberately outside the historical hash. A release's
		// original request payload and zero actual amount reproduce its reserve.
		expected = cmd.RequestFingerprint
	}
	if expected == "" {
		return "", "", service.ErrBatchImageBalanceLifecycleHistory
	}
	present, err := verifyBatchImageLifecycleBillingProofs(ctx, tx, holdID, cmd.APIKeyID, expected)
	if err != nil {
		return "", "", err
	}
	if !present {
		return "", "", service.ErrBatchImageBalanceLifecycleHistory
	}
	return "legacy_dedup", expected, nil
}

func insertBatchImageBalanceLifecycle(ctx context.Context, tx *sql.Tx, proof *service.BatchImageBalanceLifecycleProof) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO batch_image_balance_lifecycle_journal (`+batchImageBalanceLifecycleColumns+`)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, proof.Version, proof.Operation,
		proof.RequestID, proof.BatchID, proof.UserID, proof.APIKeyID, proof.BillingFingerprint, proof.ProofFingerprint,
		proof.RawHoldUSD, proof.RawActualUSD, proof.Outcome, proof.ReserveOrigin, proof.ReserveFingerprint)
	return err
}

func verifyBatchImageLifecycleReserveReference(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand, proof *service.BatchImageBalanceLifecycleProof, proofs map[string]*service.BatchImageBalanceLifecycleProof) error {
	if proof.ReserveOrigin == "none" {
		return nil
	}
	if proof.ReserveOrigin == "observed_reserve" {
		reserve := proofs["reserve"]
		if reserve == nil || reserve.Outcome != "reserved" || reserve.BillingFingerprint != proof.ReserveFingerprint {
			return service.ErrBatchImageBalanceLifecycleConflict
		}
	} else if proofs["reserve"] != nil {
		return service.ErrBatchImageBalanceLifecycleConflict
	}
	_, err := verifyBatchImageLifecycleBillingProofs(ctx, tx, service.BatchImageHoldRequestID(cmd.BatchID), cmd.APIKeyID, proof.ReserveFingerprint)
	return err
}

func batchImageLifecycleConflict(err error) error {
	if errors.Is(err, service.ErrUsageBillingRequestConflict) {
		return service.ErrBatchImageBalanceLifecycleConflict
	}
	return err
}
