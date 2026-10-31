package repository

import (
	"context"
	"database/sql"
	"errors"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const batchImageNativeBirthColumns = `schema_version,job_row_id,batch_id,user_id,api_key_id,provider,model,
	request_hash,raw_hold_usd,reserve_fingerprint,proof_fingerprint`

// This database-only witness is independent from the mutable hot/archive
// receipts and the lifecycle journal. It never creates legacy authority.
type batchImageNativeMoneyOperation struct {
	batchID, operation, requestID                                  string
	userID, apiKeyID                                               int64
	rawHoldUSD, rawActualUSD, billingFingerprint, proofFingerprint string
}

func loadBatchImageNativeMoneyOperations(ctx context.Context, tx *sql.Tx, batchID string) (map[string]*batchImageNativeMoneyOperation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT batch_id,operation,request_id,user_id,api_key_id,
		raw_hold_usd,raw_actual_usd,billing_fingerprint,proof_fingerprint
		FROM batch_image_native_money_operations WHERE batch_id=$1 ORDER BY operation`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	witnesses := make(map[string]*batchImageNativeMoneyOperation)
	for rows.Next() {
		w := &batchImageNativeMoneyOperation{}
		if err := rows.Scan(&w.batchID, &w.operation, &w.requestID, &w.userID, &w.apiKeyID,
			&w.rawHoldUSD, &w.rawActualUSD, &w.billingFingerprint, &w.proofFingerprint); err != nil {
			return nil, err
		}
		witnesses[w.operation] = w
	}
	return witnesses, rows.Err()
}

func verifyBatchImageNativeMoneyOperations(birth *service.BatchImageNativeBirth,
	witnesses map[string]*batchImageNativeMoneyOperation, proofs map[string]*service.BatchImageBalanceLifecycleProof, creatingNative bool) error {
	if birth == nil {
		if len(witnesses) != 0 {
			return service.ErrBatchImageNativeBirthHistory
		}
		return nil
	}
	if creatingNative {
		if len(witnesses) != 0 || len(proofs) != 0 {
			return service.ErrBatchImageNativeBirthHistory
		}
		return nil
	}
	if witnesses["reserve"] == nil {
		return service.ErrBatchImageNativeBirthHistory
	}
	for operation, w := range witnesses {
		if w.batchID != birth.BatchID || w.userID != birth.UserID || w.apiKeyID != birth.APIKeyID || w.rawHoldUSD != birth.RawHoldUSD {
			return service.ErrBatchImageNativeBirthConflict
		}
		proof := proofs[operation]
		if proof == nil {
			return service.ErrBatchImageNativeBirthHistory
		}
		if w.operation != proof.Operation || w.requestID != proof.RequestID || w.rawActualUSD != proof.RawActualUSD ||
			w.billingFingerprint != proof.BillingFingerprint || w.proofFingerprint != proof.ProofFingerprint {
			return service.ErrBatchImageNativeBirthConflict
		}
	}
	for operation := range proofs {
		if witnesses[operation] == nil {
			return service.ErrBatchImageNativeBirthHistory
		}
	}
	return nil
}

func findBatchImageNativeBirth(ctx context.Context, tx *sql.Tx, batchID string) (*service.BatchImageNativeBirth, error) {
	birth := &service.BatchImageNativeBirth{}
	err := tx.QueryRowContext(ctx, `SELECT `+batchImageNativeBirthColumns+` FROM batch_image_native_birth WHERE batch_id=$1`, batchID).
		Scan(&birth.Version, &birth.JobID, &birth.BatchID, &birth.UserID, &birth.APIKeyID, &birth.Provider,
			&birth.Model, &birth.RequestHash, &birth.RawHoldUSD, &birth.ReserveFingerprint, &birth.ProofFingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if service.ValidateBatchImageNativeBirthStoredProof(birth) != nil {
		return nil, service.ErrBatchImageNativeBirthConflict
	}
	return birth, nil
}

func insertBatchImageNativeBirth(ctx context.Context, tx *sql.Tx, birth *service.BatchImageNativeBirth) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO batch_image_native_birth (`+batchImageNativeBirthColumns+`)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, birth.Version, birth.JobID, birth.BatchID, birth.UserID,
		birth.APIKeyID, birth.Provider, birth.Model, birth.RequestHash, birth.RawHoldUSD,
		birth.ReserveFingerprint, birth.ProofFingerprint)
	return err
}

func verifyBatchImageNativeReserve(birth *service.BatchImageNativeBirth, proofs map[string]*service.BatchImageBalanceLifecycleProof) error {
	reserve := proofs["reserve"]
	if reserve == nil {
		return service.ErrBatchImageNativeBirthHistory
	}
	if reserve.BillingFingerprint != birth.ReserveFingerprint || reserve.RawHoldUSD != birth.RawHoldUSD {
		return service.ErrBatchImageNativeBirthConflict
	}
	if birth.RawHoldUSD == "0" {
		if reserve.Outcome != "zero_hold" || reserve.ReserveOrigin != "none" || reserve.ReserveFingerprint != "" {
			return service.ErrBatchImageNativeBirthConflict
		}
	} else if reserve.Outcome != "reserved" || reserve.ReserveOrigin != "observed_reserve" || reserve.ReserveFingerprint != birth.ReserveFingerprint {
		return service.ErrBatchImageNativeBirthConflict
	}
	for operation, proof := range proofs {
		if operation == "reserve" {
			continue
		}
		if birth.RawHoldUSD == "0" {
			if proof.ReserveOrigin != "none" || proof.ReserveFingerprint != "" ||
				(operation == "release" && proof.Outcome != "zero_hold") ||
				(operation == "capture" && proof.Outcome != "captured") {
				return service.ErrBatchImageNativeBirthConflict
			}
		} else if proof.ReserveOrigin != "observed_reserve" || proof.ReserveFingerprint != birth.ReserveFingerprint ||
			(operation == "release" && proof.Outcome != "refunded") || (operation == "capture" && proof.Outcome != "captured") {
			return service.ErrBatchImageNativeBirthConflict
		}
	}
	return nil
}

// CreateBatchImageJobWithBalanceHold is the only fresh-reserve authority. No
// committed job/birth can be observed before its reserve proof and money.
func (r *batchImageRepository) CreateBatchImageJobWithBalanceHold(ctx context.Context, params service.CreateBatchImageJobParams) (*service.BatchImageJob, error) {
	if r == nil {
		return nil, errors.New("batch image repository is nil")
	}
	if !service.IsSupportedBatchImageProvider(params.Provider) {
		return nil, service.ErrBatchImageInvalidProvider
	}
	if params.BatchID == "" {
		batchID, err := service.NewBatchImageID()
		if err != nil {
			return nil, err
		}
		params.BatchID = batchID
	}
	if params.Status == "" {
		params.Status = service.BatchImageJobStatusCreated
	}
	if params.Currency == "" {
		params.Currency = "USD"
	}
	if r.db == nil {
		// Preserve the repository's explicit transaction injection seam. The
		// caller owns commit/rollback and its deferred constraints in this case.
		tx, ok := r.sql.(*sql.Tx)
		if !ok {
			return nil, errors.New("native batch creation requires a database transaction")
		}
		return r.createBatchImageNativeWithTx(ctx, tx, params)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	job, err := r.createBatchImageNativeWithTx(ctx, tx, params)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return job, nil
}

func (r *batchImageRepository) createBatchImageNativeWithTx(ctx context.Context, tx *sql.Tx, params service.CreateBatchImageJobParams) (*service.BatchImageJob, error) {
	if err := lockBatchImageBalanceOperation(ctx, tx, &service.BatchImageBalanceHoldCommand{BatchID: params.BatchID}); err != nil {
		return nil, err
	}
	existing, err := findBatchImageNativeBirth(ctx, tx, params.BatchID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, service.ErrBatchImageNativeBirthConflict
	}
	job, err := createBatchImageJobWithSQL(ctx, tx, params)
	if err != nil {
		return nil, translatePersistenceError(err, nil, service.ErrBatchImageJobExists)
	}
	// The RETURNING job has already crossed DECIMAL(20,10). Bind the exact
	// float representation the existing hold command will subsequently use.
	birth, err := service.FreezeBatchImageNativeBirth(job)
	if err != nil {
		return nil, err
	}
	if err := insertBatchImageNativeBirth(ctx, tx, birth); err != nil {
		return nil, err
	}
	hold, err := strconv.ParseFloat(birth.RawHoldUSD, 64)
	if err != nil {
		return nil, service.ErrBatchImageNativeBirthInput
	}
	cmd := &service.BatchImageBalanceHoldCommand{RequestID: service.BatchImageHoldRequestID(birth.BatchID), BatchID: birth.BatchID,
		UserID: birth.UserID, APIKeyID: birth.APIKeyID, HoldAmount: hold,
		RequestPayloadHash: birth.RequestHash, ExpectedReserveFingerprint: birth.ReserveFingerprint}
	billing := &usageBillingRepository{db: r.db}
	result, err := billing.applyBatchImageBalanceHoldTx(ctx, tx, cmd, batchImageBalanceReserve, reserveUsageBillingBatchImageBalance, true)
	if err != nil {
		return nil, err
	}
	if !result.Applied || result.LifecycleCoverageMissing {
		return nil, service.ErrBatchImageNativeBirthHistory
	}
	return job, nil
}
