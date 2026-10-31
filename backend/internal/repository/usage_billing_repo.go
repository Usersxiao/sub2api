package repository

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type usageBillingRepository struct {
	db              *sql.DB
	writerAuthority service.UserPlatformQuotaWriterAuthority
}

func NewUsageBillingRepository(_ *dbent.Client, sqlDB *sql.DB) service.UsageBillingRepository {
	return &usageBillingRepository{db: sqlDB}
}

func NewUsageBillingRepositoryWithWriterAuthority(_ *dbent.Client, sqlDB *sql.DB, authority service.UserPlatformQuotaWriterAuthority) service.UsageBillingRepository {
	return &usageBillingRepository{db: sqlDB, writerAuthority: authority}
}

func (r *usageBillingRepository) Apply(ctx context.Context, cmd *service.UsageBillingCommand) (_ *service.UsageBillingApplyResult, err error) {
	if cmd == nil {
		return &service.UsageBillingApplyResult{}, nil
	}
	if r == nil || r.db == nil {
		return nil, errors.New("usage billing repository db is nil")
	}

	cmd.Normalize()
	if cmd.RequestID == "" {
		return nil, service.ErrUsageBillingRequestIDRequired
	}
	if cmd.OrdinaryPlatformQuotaDeliveryMode != "" &&
		(cmd.OrdinaryPlatformQuotaDeliveryMode != service.UsageBillingOrdinaryPlatformDeliveryDBV1 || cmd.OrdinaryPlatformQuota == nil) {
		return nil, service.ErrUsageBillingOrdinaryPlatformDelivery
	}
	if cmd.OrdinaryPlatformQuota != nil {
		if err := service.ValidateUsageBillingOrdinaryPlatformIntent(cmd, cmd.OrdinaryPlatformQuota); err != nil {
			return nil, err
		}
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()
	if cmd.OrdinaryPlatformQuotaDeliveryMode == service.UsageBillingOrdinaryPlatformDeliveryDBV1 {
		if err := lockUserPlatformQuotaWriterAuthority(ctx, tx, r.writerAuthority, ""); err != nil {
			return nil, err
		}
	}

	// Permanent ordinary proofs outlive hot/archive retention, including when
	// a later caller omits the new intent. Consult them before touching money.
	lifecycleExists, err := batchImageBalanceLifecycleRequestExists(ctx, tx, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	if lifecycleExists {
		return nil, service.ErrBatchImageBalanceLifecycleConflict
	}
	batchJournal, err := findBatchImagePlatformQuotaJournal(ctx, tx, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	if batchJournal != nil {
		return nil, service.ErrUsageBillingRequestConflict
	}
	journal, err := findOrdinaryPlatformQuotaJournal(ctx, tx, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	if journal != nil {
		if err := verifyOrdinaryPlatformQuotaJournal(ctx, tx, cmd, journal); err != nil {
			return nil, err
		}
		return &service.UsageBillingApplyResult{Applied: false}, nil
	}

	applied, err := r.claimUsageBillingKey(ctx, tx, cmd)
	if err != nil {
		return nil, err
	}
	// A competing transaction can commit its permanent proof while this
	// transaction waits on dedup. Recheck even after a fresh claim, since dedup
	// cleanup may have run between the two observations.
	lifecycleExists, err = batchImageBalanceLifecycleRequestExists(ctx, tx, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	if lifecycleExists {
		return nil, service.ErrBatchImageBalanceLifecycleConflict
	}
	batchJournal, err = findBatchImagePlatformQuotaJournal(ctx, tx, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	if batchJournal != nil {
		return nil, service.ErrUsageBillingRequestConflict
	}
	journal, err = findOrdinaryPlatformQuotaJournal(ctx, tx, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	if journal != nil {
		if err := verifyOrdinaryPlatformQuotaJournal(ctx, tx, cmd, journal); err != nil {
			return nil, err
		}
		return &service.UsageBillingApplyResult{Applied: false}, nil
	}
	if !applied {
		if cmd.OrdinaryPlatformQuota != nil {
			if err := verifyOrdinaryPlatformQuotaBilling(ctx, tx, cmd); err != nil {
				return nil, err
			}
			// An older charge is not proof of its platform or quota incarnation.
			// Never manufacture a historical journal using today's config.
			return nil, service.ErrUsageBillingOrdinaryPlatformCoverageGap
		}
		return &service.UsageBillingApplyResult{Applied: false}, nil
	}
	if cmd.OrdinaryPlatformQuota != nil {
		journal, err = observeOrdinaryPlatformQuota(ctx, tx, cmd)
		if err != nil {
			return nil, err
		}
	}

	result := &service.UsageBillingApplyResult{Applied: true}
	if err := r.applyUsageBillingEffects(ctx, tx, cmd, result); err != nil {
		return nil, err
	}
	if journal != nil {
		if err := insertOrdinaryPlatformQuotaJournal(ctx, tx, journal); err != nil {
			return nil, err
		}
	}
	receipt := ordinaryPlatformQuotaReceipt(journal)
	if journal != nil && journal.disposition == "metered" && cmd.OrdinaryPlatformQuotaDeliveryMode == service.UsageBillingOrdinaryPlatformDeliveryDBV1 {
		receipt.DeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
		receipt.WriterEpoch = r.writerAuthority.Epoch
		if err := insertOrdinaryPlatformQuotaDelivery(ctx, tx, cmd, receipt); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	tx = nil
	// Only a fresh, committed charge exports its original private observation.
	// Duplicate/legacy proofs never acquire a receipt from today's quota state.
	result.OrdinaryPlatformQuotaReceipt = receipt
	return result, nil
}

func (r *usageBillingRepository) claimUsageBillingKey(ctx context.Context, tx *sql.Tx, cmd *service.UsageBillingCommand) (bool, error) {
	return r.claimUsageBillingRequest(ctx, tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
}

func (r *usageBillingRepository) claimUsageBillingRequest(ctx context.Context, tx *sql.Tx, requestID string, apiKeyID int64, requestFingerprint string) (bool, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO usage_billing_dedup (request_id, api_key_id, request_fingerprint)
		VALUES ($1, $2, $3)
		ON CONFLICT (request_id, api_key_id) DO NOTHING
		RETURNING id
	`, requestID, apiKeyID, requestFingerprint).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		var existingFingerprint string
		if err := tx.QueryRowContext(ctx, `
			SELECT request_fingerprint
			FROM usage_billing_dedup
			WHERE request_id = $1 AND api_key_id = $2
		`, requestID, apiKeyID).Scan(&existingFingerprint); err != nil {
			return false, err
		}
		if strings.TrimSpace(existingFingerprint) != strings.TrimSpace(requestFingerprint) {
			return false, service.ErrUsageBillingRequestConflict
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var archivedFingerprint string
	err = tx.QueryRowContext(ctx, `
		SELECT request_fingerprint
		FROM usage_billing_dedup_archive
		WHERE request_id = $1 AND api_key_id = $2
	`, requestID, apiKeyID).Scan(&archivedFingerprint)
	if err == nil {
		if strings.TrimSpace(archivedFingerprint) != strings.TrimSpace(requestFingerprint) {
			return false, service.ErrUsageBillingRequestConflict
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	return true, nil
}

func (r *usageBillingRepository) ReserveBatchImageBalance(ctx context.Context, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return r.applyBatchImageBalanceHold(ctx, cmd, batchImageBalanceReserve, reserveUsageBillingBatchImageBalance)
}

func (r *usageBillingRepository) CaptureBatchImageBalance(ctx context.Context, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return r.applyBatchImageBalanceHold(ctx, cmd, batchImageBalanceCapture, captureUsageBillingBatchImageBalance)
}

func (r *usageBillingRepository) ReleaseBatchImageBalance(ctx context.Context, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return r.applyBatchImageBalanceHold(ctx, cmd, batchImageBalanceRelease, releaseUsageBillingBatchImageBalance)
}

type batchImageBalanceOperation uint8

const (
	batchImageBalanceReserve batchImageBalanceOperation = iota
	batchImageBalanceCapture
	batchImageBalanceRelease
)

func (r *usageBillingRepository) applyBatchImageBalanceHold(
	ctx context.Context,
	cmd *service.BatchImageBalanceHoldCommand,
	operation batchImageBalanceOperation,
	apply func(context.Context, *sql.Tx, *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error),
) (*service.BatchImageBalanceHoldResult, error) {
	if cmd == nil {
		return &service.BatchImageBalanceHoldResult{}, nil
	}
	if r == nil || r.db == nil {
		return nil, errors.New("usage billing repository db is nil")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result, err := r.applyBatchImageBalanceHoldTx(ctx, tx, cmd, operation, apply, false)
	if err != nil {
		return nil, err
	}
	// Duplicate/archive observations have no new money or proof to commit.
	// Rolling back also avoids minting a native hot receipt for an old proof.
	if !result.Applied {
		return result, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// creatingNative is a private transaction-local permission held only by the
// atomic INSERT job+birth+reserve path. It is never accepted from a command.
func (r *usageBillingRepository) applyBatchImageBalanceHoldTx(
	ctx context.Context,
	tx *sql.Tx,
	cmd *service.BatchImageBalanceHoldCommand,
	operation batchImageBalanceOperation,
	apply func(context.Context, *sql.Tx, *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error),
	creatingNative bool,
) (*service.BatchImageBalanceHoldResult, error) {
	cmd.Normalize()
	if cmd.RequestID == "" {
		return nil, service.ErrUsageBillingRequestIDRequired
	}
	if cmd.CapturePlatformQuota != nil {
		if operation != batchImageBalanceCapture {
			return nil, service.ErrBatchImagePlatformQuotaIntent
		}
		if err := service.ValidateBatchImagePlatformQuotaIntent(cmd, cmd.CapturePlatformQuota); err != nil {
			return nil, err
		}
	}
	candidate, err := validateBatchImageBalanceLifecycleCommand(cmd, operation)
	if err != nil {
		return nil, err
	}
	if err := lockBatchImageBalanceOperation(ctx, tx, cmd); err != nil {
		return nil, err
	}
	birth, err := findBatchImageNativeBirth(ctx, tx, cmd.BatchID)
	if err != nil {
		return nil, err
	}
	witnesses, err := loadBatchImageNativeMoneyOperations(ctx, tx, cmd.BatchID)
	if err != nil {
		return nil, err
	}
	if birth == nil && len(witnesses) != 0 {
		return nil, service.ErrBatchImageNativeBirthHistory
	}
	if creatingNative && (birth == nil || operation != batchImageBalanceReserve) {
		return nil, service.ErrBatchImageNativeBirthHistory
	}
	if birth != nil {
		if err := service.ValidateBatchImageNativeBirthCommand(cmd, birth); err != nil {
			return nil, err
		}
		if operation == batchImageBalanceCapture && cmd.CapturePlatformQuota == nil {
			return nil, service.ErrBatchImageNativeBirthInput
		}
	}
	proofs, err := loadBatchImageBalanceLifecycle(ctx, tx, cmd, candidate.RawHoldUSD)
	if err != nil {
		return nil, err
	}
	if birth != nil {
		if creatingNative {
			if len(proofs) != 0 {
				return nil, service.ErrBatchImageNativeBirthHistory
			}
		} else if err := verifyBatchImageNativeReserve(birth, proofs); err != nil {
			return nil, err
		}
	}
	if err := verifyBatchImageNativeMoneyOperations(birth, witnesses, proofs, creatingNative); err != nil {
		return nil, err
	}
	platform, err := loadBatchImagePlatformQuotaForLifecycle(ctx, tx, cmd, candidate.RawHoldUSD)
	if err != nil {
		return nil, err
	}
	if birth != nil {
		if platform != nil && platform.provider != birth.Provider {
			return nil, service.ErrBatchImageNativeBirthConflict
		}
		if (proofs["capture"] != nil && platform == nil) || (platform != nil && proofs["capture"] == nil) {
			return nil, service.ErrBatchImageNativeBirthHistory
		}
	}
	if captured := proofs["capture"]; captured != nil && platform != nil &&
		(captured.BillingFingerprint != platform.billingFingerprint || captured.RawActualUSD != platform.rawCostUSD) {
		return nil, service.ErrBatchImageBalanceLifecycleConflict
	}
	if proofs["release"] != nil && platform != nil {
		return nil, service.ErrBatchImageBalanceLifecycleConflict
	}
	// A permanent replay must still reject conflicting source history. This
	// also closes damaged-data cases left by writers outside this boundary.
	ordinary, err := findOrdinaryPlatformQuotaJournal(ctx, tx, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	if ordinary != nil {
		return nil, service.ErrBatchImageBalanceLifecycleConflict
	}

	// Global batch identity, rather than today's key/owner, controls terminal
	// barriers. Historical receipts only block; they do not invent an outcome.
	captureExists, err := batchImageLifecycleLegacyRequestExists(ctx, tx, service.BatchImageCaptureRequestID(cmd.BatchID))
	if err != nil {
		return nil, err
	}
	releaseExists, err := batchImageLifecycleLegacyRequestExists(ctx, tx, service.BatchImageReleaseRequestID(cmd.BatchID))
	if err != nil {
		return nil, err
	}
	switch operation {
	case batchImageBalanceReserve:
		if proofs["capture"] != nil || proofs["release"] != nil || platform != nil || captureExists || releaseExists {
			return nil, service.ErrBatchImageBalanceLifecycleConflict
		}
	case batchImageBalanceCapture:
		if proofs["release"] != nil || releaseExists {
			return nil, service.ErrBatchImageBalanceLifecycleConflict
		}
	case batchImageBalanceRelease:
		if proofs["capture"] != nil || platform != nil || captureExists {
			return nil, service.ErrBatchImageCaptureAlreadyApplied
		}
	}

	if proof := proofs[candidate.Operation]; proof != nil {
		if birth != nil && operation == batchImageBalanceCapture && platform == nil {
			return nil, service.ErrBatchImageNativeBirthHistory
		}
		if service.ValidateBatchImageBalanceLifecycleProof(cmd, proof) != nil {
			return nil, service.ErrBatchImageBalanceLifecycleConflict
		}
		if _, err := verifyBatchImageLifecycleBillingProofs(ctx, tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint); err != nil {
			return nil, err
		}
		if err := verifyBatchImageLifecycleReserveReference(ctx, tx, cmd, proof, proofs); err != nil {
			return nil, err
		}
		if platform != nil {
			if err := verifyBatchImagePlatformQuotaJournal(ctx, tx, cmd, platform); err != nil {
				return nil, batchImageLifecycleConflict(err)
			}
		}
		return &service.BatchImageBalanceHoldResult{Applied: false,
			PlatformCoverageMissing: operation == batchImageBalanceCapture && cmd.CapturePlatformQuota != nil && platform == nil}, nil
	}
	if platform != nil {
		if birth != nil {
			return nil, service.ErrBatchImageNativeBirthHistory
		}
		if err := verifyBatchImagePlatformQuotaJournal(ctx, tx, cmd, platform); err != nil {
			return nil, batchImageLifecycleConflict(err)
		}
		if _, err := verifyBatchImageLifecycleBillingProofs(ctx, tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint); err != nil {
			return nil, err
		}
		return &service.BatchImageBalanceHoldResult{Applied: false, LifecycleCoverageMissing: true}, nil
	}
	legacy, err := verifyBatchImageLifecycleBillingProofs(ctx, tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
	if err != nil {
		return nil, err
	}
	applied, err := r.claimUsageBillingRequest(ctx, tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
	if err != nil {
		return nil, batchImageLifecycleConflict(err)
	}
	// An ordinary writer can finish while this transaction waits for the shared
	// dedup claim. Recheck its permanent source before observing any money.
	ordinary, err = findOrdinaryPlatformQuotaJournal(ctx, tx, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	if ordinary != nil {
		return nil, service.ErrBatchImageBalanceLifecycleConflict
	}
	if _, err := verifyBatchImageLifecycleBillingProofs(ctx, tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint); err != nil {
		return nil, err
	}
	if !applied || legacy {
		if birth != nil {
			return nil, service.ErrBatchImageNativeBirthHistory
		}
		return &service.BatchImageBalanceHoldResult{Applied: false, LifecycleCoverageMissing: true,
			PlatformCoverageMissing: operation == batchImageBalanceCapture && cmd.CapturePlatformQuota != nil}, nil
	}
	if operation == batchImageBalanceReserve && !creatingNative {
		return nil, service.ErrBatchImageNativeBirthHistory
	}

	proof := candidate
	if operation != batchImageBalanceReserve {
		origin, reserveFP, err := batchImageBalanceLifecycleReserveSource(ctx, tx, cmd, proofs, operation)
		if err != nil {
			return nil, err
		}
		outcome := "captured"
		if operation == batchImageBalanceRelease {
			outcome = "refunded"
			if cmd.HoldAmount == 0 {
				outcome = "zero_hold"
			} else if origin == "none" {
				outcome = "no_hold_terminal"
			}
		}
		proof, err = service.FreezeBatchImageBalanceLifecycleProof(cmd, candidate.Operation, outcome, origin, reserveFP)
		if err != nil {
			return nil, err
		}
	}
	if cmd.CapturePlatformQuota != nil {
		platform, err = observeBatchImagePlatformQuota(ctx, tx, cmd)
		if err != nil {
			return nil, err
		}
	}
	var result *service.BatchImageBalanceHoldResult
	if operation == batchImageBalanceRelease {
		if proof.Outcome == "refunded" {
			// Reservation authority was checked above. The original financial
			// SQL is shared with the old helper without relying on hot dedup.
			result, err = refundUsageBillingBatchImageBalance(ctx, tx, cmd)
		} else {
			result = &service.BatchImageBalanceHoldResult{}
		}
	} else {
		result, err = apply(ctx, tx, cmd)
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		result = &service.BatchImageBalanceHoldResult{}
	}
	result.Applied = true
	if err := insertBatchImageBalanceLifecycle(ctx, tx, proof); err != nil {
		return nil, err
	}
	if platform != nil {
		if err := insertBatchImagePlatformQuotaJournal(ctx, tx, platform); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (r *usageBillingRepository) applyUsageBillingEffects(ctx context.Context, tx *sql.Tx, cmd *service.UsageBillingCommand, result *service.UsageBillingApplyResult) error {
	if cmd.SubscriptionCost > 0 && cmd.SubscriptionID != nil {
		if err := incrementUsageBillingSubscription(ctx, tx, *cmd.SubscriptionID, cmd.SubscriptionCost); err != nil {
			return err
		}
	}

	if cmd.BalanceCost > 0 {
		newBalance, sufficient, err := deductUsageBillingBalance(ctx, tx, cmd.UserID, cmd.BalanceCost)
		if err != nil {
			return err
		}
		result.NewBalance = &newBalance
		result.BalanceOverdrafted = !sufficient
	}

	// Key 已不存在时跳过其自身的额度/限速计数，其余结算项不受影响。
	if cmd.APIKeyQuotaCost > 0 {
		exhausted, err := incrementUsageBillingAPIKeyQuota(ctx, tx, cmd.APIKeyID, cmd.APIKeyQuotaCost)
		if err != nil && !errors.Is(err, service.ErrAPIKeyNotFound) {
			return err
		}
		result.APIKeyQuotaExhausted = exhausted
	}

	if cmd.APIKeyRateLimitCost > 0 {
		if err := incrementUsageBillingAPIKeyRateLimit(ctx, tx, cmd.APIKeyID, cmd.APIKeyRateLimitCost); err != nil && !errors.Is(err, service.ErrAPIKeyNotFound) {
			return err
		}
	}

	// 账号已不存在时跳过其配额计数，其余结算项不受影响。
	if cmd.AccountQuotaCost > 0 && service.AccountTypeSupportsQuota(cmd.AccountType) {
		quotaState, err := incrementUsageBillingAccountQuota(ctx, tx, cmd.AccountID, cmd.AccountQuotaCost, cmd.AccountQuotaHasDailyLimit, cmd.AccountQuotaHasWeeklyLimit)
		if err != nil && !errors.Is(err, service.ErrAccountNotFound) {
			return err
		}
		result.QuotaState = quotaState
	}

	return nil
}

func incrementUsageBillingSubscription(ctx context.Context, tx *sql.Tx, subscriptionID int64, costUSD float64) error {
	const updateSQL = `
		UPDATE user_subscriptions us
		SET
			daily_usage_usd = us.daily_usage_usd + $1,
			weekly_usage_usd = us.weekly_usage_usd + $1,
			monthly_usage_usd = us.monthly_usage_usd + $1,
			updated_at = NOW()
		FROM groups g
		WHERE us.id = $2
			AND us.deleted_at IS NULL
			AND us.group_id = g.id
			AND g.deleted_at IS NULL
	`
	res, err := tx.ExecContext(ctx, updateSQL, costUSD, subscriptionID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	return service.ErrSubscriptionNotFound
}

func deductUsageBillingBalance(ctx context.Context, tx *sql.Tx, userID int64, amount float64) (float64, bool, error) {
	var newBalance float64
	err := tx.QueryRowContext(ctx, `
		UPDATE users
		SET balance = balance - $1,
			updated_at = NOW()
		WHERE id = $2 AND deleted_at IS NULL AND balance >= $1
		RETURNING balance
	`, amount, userID).Scan(&newBalance)
	if err == nil {
		return newBalance, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}

	err = tx.QueryRowContext(ctx, `
		UPDATE users
		SET balance = balance - $1,
			updated_at = NOW()
		WHERE id = $2 AND deleted_at IS NULL
		RETURNING balance
	`, amount, userID).Scan(&newBalance)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, service.ErrUserNotFound
	}
	if err != nil {
		return 0, false, err
	}
	return newBalance, false, nil
}

func reserveUsageBillingBatchImageBalance(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	if cmd.HoldAmount <= 0 {
		return &service.BatchImageBalanceHoldResult{}, nil
	}
	var balance, frozen float64
	err := tx.QueryRowContext(ctx, `
		UPDATE users
		SET balance = balance - $1,
			frozen_balance = COALESCE(frozen_balance, 0) + $1,
			updated_at = NOW()
		WHERE id = $2 AND deleted_at IS NULL AND balance >= $1
		RETURNING balance, frozen_balance
	`, cmd.HoldAmount, cmd.UserID).Scan(&balance, &frozen)
	if err == nil {
		return &service.BatchImageBalanceHoldResult{NewBalance: &balance, FrozenBalance: &frozen}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if exists, existsErr := userExistsForBilling(ctx, tx, cmd.UserID); existsErr != nil {
		return nil, existsErr
	} else if !exists {
		return nil, service.ErrUserNotFound
	}
	return nil, service.ErrBatchImageInsufficientBalance
}

func captureUsageBillingBatchImageBalance(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	if cmd.HoldAmount <= 0 && cmd.ActualAmount <= 0 {
		return &service.BatchImageBalanceHoldResult{}, nil
	}
	if cmd.ActualAmount-cmd.HoldAmount > 0.00000001 {
		return nil, service.ErrBatchImageSettlementCostExceedsHold
	}
	var balance, frozen float64
	err := tx.QueryRowContext(ctx, `
		UPDATE users
		SET balance = balance
				+ CASE WHEN $1 > $2 THEN $1 - $2 ELSE 0 END
				- CASE WHEN $2 > $1 THEN $2 - $1 ELSE 0 END,
			frozen_balance = COALESCE(frozen_balance, 0) - $1,
			updated_at = NOW()
		WHERE id = $3 AND deleted_at IS NULL AND COALESCE(frozen_balance, 0) >= $1
		RETURNING balance, frozen_balance
	`, cmd.HoldAmount, cmd.ActualAmount, cmd.UserID).Scan(&balance, &frozen)
	if err == nil {
		return &service.BatchImageBalanceHoldResult{NewBalance: &balance, FrozenBalance: &frozen}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if exists, existsErr := userExistsForBilling(ctx, tx, cmd.UserID); existsErr != nil {
		return nil, existsErr
	} else if !exists {
		return nil, service.ErrUserNotFound
	}
	return nil, errors.New("batch image frozen balance is insufficient")
}

func releaseUsageBillingBatchImageBalance(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	if cmd.HoldAmount <= 0 {
		return &service.BatchImageBalanceHoldResult{}, nil
	}
	// 释放前校验该 job 确实预留过 hold（hold request id 已被 claim），
	// 防止从未成功冻结的 job 触发"幻影释放"，从其他用户的冻结资金池中凭空生成余额。
	held, heldErr := batchImageHoldClaimExists(ctx, tx, service.BatchImageHoldRequestID(cmd.BatchID), cmd.APIKeyID)
	if heldErr != nil {
		return nil, heldErr
	}
	if !held {
		logger.LegacyPrintf("repository.usage_billing", "[BatchImage] release skipped, hold was never reserved: batch=%s", cmd.BatchID)
		return &service.BatchImageBalanceHoldResult{}, nil
	}
	return refundUsageBillingBatchImageBalance(ctx, tx, cmd)
}

// The caller has already proved reservation authority. Keep the historical
// refund SQL unchanged and share it with the legacy helper above.
func refundUsageBillingBatchImageBalance(ctx context.Context, tx *sql.Tx, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	var balance, frozen float64
	err := tx.QueryRowContext(ctx, `
		UPDATE users
		SET balance = balance + $1,
			frozen_balance = COALESCE(frozen_balance, 0) - $1,
			updated_at = NOW()
		WHERE id = $2 AND deleted_at IS NULL AND COALESCE(frozen_balance, 0) >= $1
		RETURNING balance, frozen_balance
	`, cmd.HoldAmount, cmd.UserID).Scan(&balance, &frozen)
	if err == nil {
		return &service.BatchImageBalanceHoldResult{NewBalance: &balance, FrozenBalance: &frozen}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if exists, existsErr := userExistsForBilling(ctx, tx, cmd.UserID); existsErr != nil {
		return nil, existsErr
	} else if !exists {
		return nil, service.ErrUserNotFound
	}
	return nil, errors.New("batch image frozen balance is insufficient")
}

// batchImageHoldClaimExists 检查 hold request id 是否已在 dedup（或归档）表中被 claim，
// 即该 batch 的冻结操作确实成功提交过。
func batchImageHoldClaimExists(ctx context.Context, tx *sql.Tx, holdRequestID string, apiKeyID int64) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM usage_billing_dedup
		WHERE request_id = $1 AND api_key_id = $2
	`, holdRequestID, apiKeyID).Scan(&exists)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	err = tx.QueryRowContext(ctx, `
		SELECT 1
		FROM usage_billing_dedup_archive
		WHERE request_id = $1 AND api_key_id = $2
	`, holdRequestID, apiKeyID).Scan(&exists)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func userExistsForBilling(ctx context.Context, tx *sql.Tx, userID int64) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM users
		WHERE id = $1 AND deleted_at IS NULL
	`, userID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func incrementUsageBillingAPIKeyQuota(ctx context.Context, tx *sql.Tx, apiKeyID int64, amount float64) (bool, error) {
	var exhausted bool
	err := tx.QueryRowContext(ctx, `
		UPDATE api_keys
		SET quota_used = quota_used + $1,
			status = CASE
				WHEN quota > 0
					AND status = $3
					AND quota_used < quota
					AND quota_used + $1 >= quota
				THEN $4
				ELSE status
			END,
			updated_at = NOW()
		WHERE id = $2 AND deleted_at IS NULL
		RETURNING quota > 0 AND quota_used >= quota AND quota_used - $1 < quota
	`, amount, apiKeyID, service.StatusAPIKeyActive, service.StatusAPIKeyQuotaExhausted).Scan(&exhausted)
	if errors.Is(err, sql.ErrNoRows) {
		return false, service.ErrAPIKeyNotFound
	}
	if err != nil {
		return false, err
	}
	return exhausted, nil
}

func incrementUsageBillingAPIKeyRateLimit(ctx context.Context, tx *sql.Tx, apiKeyID int64, cost float64) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE api_keys SET
			usage_5h = CASE WHEN window_5h_start IS NOT NULL AND window_5h_start + INTERVAL '5 hours' <= NOW() THEN $1 ELSE usage_5h + $1 END,
			usage_1d = CASE WHEN window_1d_start IS NOT NULL AND window_1d_start + INTERVAL '24 hours' <= NOW() THEN $1 ELSE usage_1d + $1 END,
			usage_7d = CASE WHEN window_7d_start IS NOT NULL AND window_7d_start + INTERVAL '7 days' <= NOW() THEN $1 ELSE usage_7d + $1 END,
			window_5h_start = CASE WHEN window_5h_start IS NULL OR window_5h_start + INTERVAL '5 hours' <= NOW() THEN NOW() ELSE window_5h_start END,
			window_1d_start = CASE WHEN window_1d_start IS NULL OR window_1d_start + INTERVAL '24 hours' <= NOW() THEN date_trunc('day', NOW()) ELSE window_1d_start END,
			window_7d_start = CASE WHEN window_7d_start IS NULL OR window_7d_start + INTERVAL '7 days' <= NOW() THEN date_trunc('day', NOW()) ELSE window_7d_start END,
			updated_at = NOW()
		WHERE id = $2 AND deleted_at IS NULL
	`, cost, apiKeyID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return service.ErrAPIKeyNotFound
	}
	return nil
}

// accountQuotaTotalIncrementExpr 累加总额度已用量。
const accountQuotaTotalIncrementExpr = `COALESCE(extra, '{}'::jsonb)
			|| jsonb_build_object('quota_used', COALESCE((extra->>'quota_used')::numeric, 0) + $1)`

// accountQuotaDailyIncrementExpr 在库中设置了日上限时累加日用量，周期过期时从本次金额重新计数。
const accountQuotaDailyIncrementExpr = `
			|| CASE WHEN COALESCE((extra->>'quota_daily_limit')::numeric, 0) > 0 THEN
				jsonb_build_object(
					'quota_daily_used',
					CASE WHEN ` + dailyExpiredExpr + `
					THEN $1
					ELSE COALESCE((extra->>'quota_daily_used')::numeric, 0) + $1 END,
					'quota_daily_start',
					CASE WHEN ` + dailyExpiredExpr + `
					THEN ` + nowUTC + `
					ELSE COALESCE(extra->>'quota_daily_start', ` + nowUTC + `) END
				)
				|| CASE WHEN ` + dailyExpiredExpr + ` AND ` + nextDailyResetAtExpr + ` IS NOT NULL
				   THEN jsonb_build_object('quota_daily_reset_at', ` + nextDailyResetAtExpr + `)
				   ELSE '{}'::jsonb END
			ELSE '{}'::jsonb END`

// accountQuotaWeeklyIncrementExpr 在库中设置了周上限时累加周用量，周期过期时从本次金额重新计数。
const accountQuotaWeeklyIncrementExpr = `
			|| CASE WHEN COALESCE((extra->>'quota_weekly_limit')::numeric, 0) > 0 THEN
				jsonb_build_object(
					'quota_weekly_used',
					CASE WHEN ` + weeklyExpiredExpr + `
					THEN $1
					ELSE COALESCE((extra->>'quota_weekly_used')::numeric, 0) + $1 END,
					'quota_weekly_start',
					CASE WHEN ` + weeklyExpiredExpr + `
					THEN ` + nowUTC + `
					ELSE COALESCE(extra->>'quota_weekly_start', ` + nowUTC + `) END
				)
				|| CASE WHEN ` + weeklyExpiredExpr + ` AND ` + nextWeeklyResetAtExpr + ` IS NOT NULL
				   THEN jsonb_build_object('quota_weekly_reset_at', ` + nextWeeklyResetAtExpr + `)
				   ELSE '{}'::jsonb END
			ELSE '{}'::jsonb END`

// accountQuotaExtraIsObjectCond 要求 extra 为 JSON 对象：省略某个维度的语句只在对象上与完整语句等价。
//
// accountQuotaDailyLimitUnsetCond / accountQuotaWeeklyLimitUnsetCond 在库中未设置日/周上限时为真：
// 键不存在、值为 JSON null 或不大于 0 的数字。其他取值一律为假，交给包含该维度的语句处理。
// 三个条件都只使用 ->、jsonb_typeof 与 jsonb 比较，对任何行求值都不会报错。
const (
	accountQuotaExtraIsObjectCond   = `jsonb_typeof(extra) = 'object'`
	accountQuotaDailyLimitUnsetCond = `(extra->'quota_daily_limit' IS NULL
			OR jsonb_typeof(extra->'quota_daily_limit') = 'null'
			OR (jsonb_typeof(extra->'quota_daily_limit') = 'number' AND extra->'quota_daily_limit' <= '0'::jsonb))`
	accountQuotaWeeklyLimitUnsetCond = `(extra->'quota_weekly_limit' IS NULL
			OR jsonb_typeof(extra->'quota_weekly_limit') = 'null'
			OR (jsonb_typeof(extra->'quota_weekly_limit') = 'number' AND extra->'quota_weekly_limit' <= '0'::jsonb))`
)

const accountQuotaIncrementReturning = `
		RETURNING
			COALESCE((extra->>'quota_used')::numeric, 0),
			COALESCE((extra->>'quota_limit')::numeric, 0),
			COALESCE((extra->>'quota_daily_used')::numeric, 0),
			COALESCE((extra->>'quota_daily_limit')::numeric, 0),
			COALESCE((extra->>'quota_weekly_used')::numeric, 0),
			COALESCE((extra->>'quota_weekly_limit')::numeric, 0)`

// accountQuotaIncrementSQLs 按 accountQuotaIncrementSQLIndex 存放账号配额累加语句。省略日或周维度的语句
// 在 WHERE 中附加 accountQuotaExtraIsObjectCond 与被省略维度的未设置条件，条件不成立时命中 0 行；
// 同时包含日、周维度的语句不附加这些条件，对任何账号配置都给出完整的累加结果。
var accountQuotaIncrementSQLs = buildAccountQuotaIncrementSQLs()

func accountQuotaIncrementSQLIndex(daily, weekly bool) int {
	index := 0
	if daily {
		index |= 1
	}
	if weekly {
		index |= 2
	}
	return index
}

func buildAccountQuotaIncrementSQLs() [4]string {
	var sqls [4]string
	for _, daily := range []bool{false, true} {
		for _, weekly := range []bool{false, true} {
			set := accountQuotaTotalIncrementExpr
			where := "id = $2 AND deleted_at IS NULL"
			if !daily || !weekly {
				where += "\n\t\t\tAND " + accountQuotaExtraIsObjectCond
			}
			if daily {
				set += accountQuotaDailyIncrementExpr
			} else {
				where += "\n\t\t\tAND " + accountQuotaDailyLimitUnsetCond
			}
			if weekly {
				set += accountQuotaWeeklyIncrementExpr
			} else {
				where += "\n\t\t\tAND " + accountQuotaWeeklyLimitUnsetCond
			}
			sqls[accountQuotaIncrementSQLIndex(daily, weekly)] = "UPDATE accounts SET extra = (\n\t\t\t" + set +
				"\n\t\t), updated_at = NOW()\n\t\tWHERE " + where + accountQuotaIncrementReturning
		}
	}
	return sqls
}

// incrementUsageBillingAccountQuota 累加账号配额并返回累加后的状态。hasDaily/hasWeekly 是调用方所见的
// 账号日/周上限设置，只决定先执行哪条语句；该语句的附加条件不成立而命中 0 行时，再执行同时包含日、周维度的语句。
func incrementUsageBillingAccountQuota(ctx context.Context, tx *sql.Tx, accountID int64, amount float64, hasDaily, hasWeekly bool) (*service.AccountQuotaState, error) {
	index := accountQuotaIncrementSQLIndex(hasDaily, hasWeekly)
	fullIndex := accountQuotaIncrementSQLIndex(true, true)
	state, err := execAccountQuotaIncrement(ctx, tx, accountQuotaIncrementSQLs[index], accountID, amount)
	if err != nil {
		return nil, err
	}
	if state == nil && index != fullIndex {
		state, err = execAccountQuotaIncrement(ctx, tx, accountQuotaIncrementSQLs[fullIndex], accountID, amount)
		if err != nil {
			return nil, err
		}
	}
	if state == nil {
		return nil, service.ErrAccountNotFound
	}
	// 任意维度额度在本次递增中从"未超"跨越到"已超"时，必须刷新调度快照，
	// 否则 Redis 中缓存的 Account 仍显示旧的 used 值，后续请求会继续选中本账号，
	// 最终观察到 daily_used / weekly_used 大幅超过配置的 limit。
	// 对于日/周额度，即使本次触发了周期重置（pre=0、post=amount），
	// 判定式 (post-amount) < limit 同样成立，逻辑与总额度保持一致。
	crossedTotal := state.TotalLimit > 0 && state.TotalUsed >= state.TotalLimit && (state.TotalUsed-amount) < state.TotalLimit
	crossedDaily := state.DailyLimit > 0 && state.DailyUsed >= state.DailyLimit && (state.DailyUsed-amount) < state.DailyLimit
	crossedWeekly := state.WeeklyLimit > 0 && state.WeeklyUsed >= state.WeeklyLimit && (state.WeeklyUsed-amount) < state.WeeklyLimit
	if crossedTotal || crossedDaily || crossedWeekly {
		if err := enqueueSchedulerOutbox(ctx, tx, service.SchedulerOutboxEventAccountChanged, &accountID, nil, nil); err != nil {
			logger.LegacyPrintf("repository.usage_billing", "[SchedulerOutbox] enqueue quota exceeded failed: account=%d err=%v", accountID, err)
			return nil, err
		}
	}
	return state, nil
}

// execAccountQuotaIncrement 执行一条账号配额累加语句，未命中任何行时返回 nil 状态。
func execAccountQuotaIncrement(ctx context.Context, tx *sql.Tx, query string, accountID int64, amount float64) (*service.AccountQuotaState, error) {
	rows, err := tx.QueryContext(ctx, query, amount, accountID)
	if err != nil {
		return nil, err
	}

	var state *service.AccountQuotaState
	if rows.Next() {
		state = &service.AccountQuotaState{}
		if err := rows.Scan(
			&state.TotalUsed, &state.TotalLimit,
			&state.DailyUsed, &state.DailyLimit,
			&state.WeeklyUsed, &state.WeeklyLimit,
		); err != nil {
			_ = rows.Close()
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	// 必须在执行下一条 SQL 前显式关闭 rows：pq 驱动在同一连接上
	// 不允许前一条查询的结果集未耗尽时启动新查询，否则会返回
	// "unexpected Parse response" 错误。
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return state, nil
}
