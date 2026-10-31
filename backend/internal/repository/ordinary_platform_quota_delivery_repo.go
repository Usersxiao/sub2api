package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.UserPlatformQuotaOrdinaryDeliveryRepository = (*userPlatformQuotaRepository)(nil)

// Eligibility is minted only here, during a fresh Apply's existing money/source
// transaction. No consumer or lookup inserts it for an older journal. The new
// application writer establishes that cohort; this does not certify direct
// owner SQL, legacy counter authority, cache delivery, or mode transitions.
func insertOrdinaryPlatformQuotaDelivery(ctx context.Context, tx *sql.Tx, cmd *service.UsageBillingCommand, receipt *service.UsageBillingOrdinaryPlatformReceipt) error {
	if err := service.ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt); err != nil {
		return err
	}
	if receipt.WriterEpoch <= 0 {
		return service.ErrUserPlatformQuotaWriterUnavailable
	}
	fingerprint, err := service.FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO ordinary_platform_quota_deliveries
		(request_id,api_key_id,source_fingerprint,delivery_mode,user_id,platform,quota_row_id,control_generation,captured_at,raw_cost_usd,cost_usd,writer_epoch)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::numeric,$12)`, receipt.Intent.RequestID, receipt.Intent.APIKeyID, fingerprint, receipt.DeliveryMode,
		receipt.Intent.UserID, receipt.Intent.Platform, receipt.QuotaRowID, receipt.ControlGeneration, receipt.CapturedAt, receipt.Intent.RawCostUSD, receipt.Intent.CostUSD, receipt.WriterEpoch)
	return err
}

type ordinaryPlatformQuotaDeliveryRecord struct {
	result                         service.UsageBillingOrdinaryPlatformDeliveryResult
	mode, platform, rawCost, cost  string
	userID, quotaRowID, generation int64
	capturedAt                     time.Time
	writerEpoch                    int64
}

func readOrdinaryPlatformQuotaDeliverySource(ctx context.Context, client *dbent.Client, cmd *service.UsageBillingCommand, lock bool) (*ordinaryPlatformQuotaJournal, error) {
	query := `SELECT ` + ordinaryPlatformQuotaJournalColumns + ` FROM usage_billing_ordinary_platform_quota_journal WHERE request_id=$1 AND api_key_id=$2`
	if lock {
		query += ` FOR SHARE`
	}
	rows, err := client.QueryContext(ctx, query, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	journal, err := scanOrdinaryPlatformQuotaJournal(rows)
	if err != nil {
		return nil, err
	}
	return journal, rows.Err()
}

func readOrdinaryPlatformQuotaDelivery(ctx context.Context, client *dbent.Client, cmd *service.UsageBillingCommand, lock bool) (*ordinaryPlatformQuotaDeliveryRecord, error) {
	query := `SELECT source_fingerprint,delivery_mode,user_id,platform,quota_row_id,control_generation,captured_at,raw_cost_usd,cost_usd,
		state,result_status,applied_windows,created_at,completed_at,writer_epoch FROM ordinary_platform_quota_deliveries WHERE request_id=$1 AND api_key_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	rows, err := client.QueryContext(ctx, query, cmd.RequestID, cmd.APIKeyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	record := &ordinaryPlatformQuotaDeliveryRecord{}
	var status sql.NullString
	var completed sql.NullTime
	var mask int64
	var epoch sql.NullInt64
	err = rows.Scan(&record.result.SourceFingerprint, &record.mode, &record.userID, &record.platform, &record.quotaRowID, &record.generation, &record.capturedAt,
		&record.rawCost, &record.cost, &record.result.State, &status, &mask, &record.result.CreatedAt, &completed, &epoch)
	if err != nil {
		return nil, err
	}
	if mask < 0 || mask > 7 {
		return nil, service.ErrUsageBillingOrdinaryPlatformDeliveryResult
	}
	record.writerEpoch = epoch.Int64
	record.result.Outcome = service.UserPlatformQuotaBoundIncrementResult{Status: service.UserPlatformQuotaIncrementStatus(status.String), AppliedWindows: uint8(mask)}
	if completed.Valid {
		at := completed.Time
		record.result.CompletedAt = &at
	}
	return record, rows.Err()
}

func validateOrdinaryPlatformQuotaDeliverySource(cmd *service.UsageBillingCommand, receipt *service.UsageBillingOrdinaryPlatformReceipt, journal *ordinaryPlatformQuotaJournal, writerEpoch int64) error {
	if journal == nil {
		return service.ErrUsageBillingOrdinaryPlatformDeliverySourceMissing
	}
	stored := ordinaryPlatformQuotaReceipt(journal)
	stored.DeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
	stored.WriterEpoch = writerEpoch
	if service.ValidateUsageBillingOrdinaryPlatformDelivery(cmd, stored) != nil {
		return service.ErrUsageBillingOrdinaryPlatformDeliveryConflict
	}
	want, err := service.FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	if err != nil {
		return err
	}
	actual, err := service.FingerprintUsageBillingOrdinaryPlatformDelivery(stored)
	if err != nil || actual != want {
		return service.ErrUsageBillingOrdinaryPlatformDeliveryConflict
	}
	return nil
}

func validateOrdinaryPlatformQuotaDeliveryRecord(cmd *service.UsageBillingCommand, receipt *service.UsageBillingOrdinaryPlatformReceipt, record *ordinaryPlatformQuotaDeliveryRecord) error {
	fingerprint, err := service.FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	if err != nil {
		return err
	}
	if record.mode != receipt.DeliveryMode || record.result.SourceFingerprint != fingerprint || record.userID != receipt.Intent.UserID || record.platform != receipt.Intent.Platform ||
		record.quotaRowID != receipt.QuotaRowID || record.generation != receipt.ControlGeneration || !record.capturedAt.Equal(receipt.CapturedAt) ||
		record.rawCost != receipt.Intent.RawCostUSD || record.cost != receipt.Intent.CostUSD || record.writerEpoch != receipt.WriterEpoch {
		return service.ErrUsageBillingOrdinaryPlatformDeliveryConflict
	}
	return service.ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, record.result, true)
}

// A source has one permanent DB consumption outcome. This transaction neither
// retries money nor touches Redis. The outcome survives an unknown COMMIT reply,
// but does not promise that a later legacy/flusher writer preserves the counter.
func (r *userPlatformQuotaRepository) ConsumeOrdinaryPlatformQuotaUsage(ctx context.Context, cmd *service.UsageBillingCommand, receipt *service.UsageBillingOrdinaryPlatformReceipt) (service.UsageBillingOrdinaryPlatformDeliveryResult, error) {
	var zero service.UsageBillingOrdinaryPlatformDeliveryResult
	if err := service.ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt); err != nil {
		return zero, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if dbent.TxFromContext(ctx) != nil {
		return zero, fmt.Errorf("%w: ambient transaction is unsupported", service.ErrUsageBillingOrdinaryPlatformDelivery)
	}
	if r == nil || r.client == nil {
		return zero, fmt.Errorf("ordinary platform quota delivery repository unavailable")
	}
	// Historical terminal results are independent of today's writer domain.
	// A preliminary read avoids acquiring source/birth locks before authority.
	observed, err := r.GetOrdinaryPlatformQuotaUsageDelivery(ctx, cmd, receipt)
	if err != nil {
		return zero, err
	}
	if observed == nil {
		return zero, service.ErrUsageBillingOrdinaryPlatformDeliveryMissing
	}
	if observed.State == service.OrdinaryPlatformDeliveryCompleted {
		return *observed, nil
	}
	if receipt.WriterEpoch <= 0 {
		return zero, service.ErrUserPlatformQuotaWriterUnavailable
	}
	if r.writerAuthority.Epoch != receipt.WriterEpoch {
		return zero, service.ErrUserPlatformQuotaWriterMismatch
	}
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return zero, err
	}
	defer tx.Rollback()
	txCtx := dbent.NewTxContext(ctx, tx)
	client := tx.Client()
	if err := lockUserPlatformQuotaWriterAuthority(txCtx, client, r.writerAuthority, quotaWriterActionOrdinaryDelivery); err != nil {
		return zero, err
	}
	// Lock order is authority, source, permanent birth, original quota row.
	journal, err := readOrdinaryPlatformQuotaDeliverySource(txCtx, client, cmd, true)
	if err != nil {
		return zero, err
	}
	record, err := readOrdinaryPlatformQuotaDelivery(txCtx, client, cmd, true)
	if err != nil {
		return zero, err
	}
	if record == nil {
		return zero, service.ErrUsageBillingOrdinaryPlatformDeliveryMissing
	}
	if err := validateOrdinaryPlatformQuotaDeliverySource(cmd, receipt, journal, record.writerEpoch); err != nil {
		return zero, err
	}
	if err := validateOrdinaryPlatformQuotaDeliveryRecord(cmd, receipt, record); err != nil {
		return zero, err
	}
	if record.result.State == service.OrdinaryPlatformDeliveryCompleted {
		// Already committed evidence needs no quota read or new effect. Rollback
		// merely releases this call's read locks and cannot undo that outcome.
		record.result.Replayed = true
		return record.result, nil
	}
	binding, err := ordinaryPlatformQuotaReceipt(journal).UsageBinding()
	if err != nil {
		return zero, err
	}
	outcome, err := r.incrementBoundUsage(txCtx, binding)
	if err != nil {
		return zero, err
	}
	rows, err := client.QueryContext(txCtx, `UPDATE ordinary_platform_quota_deliveries
		SET state='completed',result_status=$3,applied_windows=$4,completed_at=GREATEST(clock_timestamp(),created_at)
		WHERE request_id=$1 AND api_key_id=$2 AND state='pending' RETURNING completed_at`, cmd.RequestID, cmd.APIKeyID, string(outcome.Status), int(outcome.AppliedWindows))
	if err != nil {
		return zero, err
	}
	if !rows.Next() {
		readErr := rows.Err()
		_ = rows.Close()
		if readErr != nil {
			return zero, readErr
		}
		return zero, service.ErrUsageBillingOrdinaryPlatformDeliveryConflict
	}
	var completed time.Time
	err = rows.Scan(&completed)
	readErr := rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return zero, err
	}
	if readErr != nil {
		return zero, readErr
	}
	if closeErr != nil {
		return zero, closeErr
	}
	result := record.result
	result.State = service.OrdinaryPlatformDeliveryCompleted
	result.Outcome = outcome
	result.CompletedAt = &completed
	if err := service.ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, result, false); err != nil {
		return zero, err
	}
	if err := tx.Commit(); err != nil {
		return zero, err
	}
	return result, nil
}

// Lookup is a repeatable-read, read-only observation. A missing birth does not
// establish that a legacy charge was never delivered; it grants no new eligibility.
func (r *userPlatformQuotaRepository) GetOrdinaryPlatformQuotaUsageDelivery(ctx context.Context, cmd *service.UsageBillingCommand, receipt *service.UsageBillingOrdinaryPlatformReceipt) (*service.UsageBillingOrdinaryPlatformDeliveryResult, error) {
	if err := service.ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if dbent.TxFromContext(ctx) != nil {
		return nil, fmt.Errorf("%w: ambient transaction is unsupported", service.ErrUsageBillingOrdinaryPlatformDelivery)
	}
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("ordinary platform quota delivery repository unavailable")
	}
	tx, err := r.client.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	journal, err := readOrdinaryPlatformQuotaDeliverySource(ctx, tx.Client(), cmd, false)
	if err != nil {
		return nil, err
	}
	if journal == nil {
		return nil, service.ErrUsageBillingOrdinaryPlatformDeliverySourceMissing
	}
	record, err := readOrdinaryPlatformQuotaDelivery(ctx, tx.Client(), cmd, false)
	if err != nil {
		return nil, err
	}
	if record == nil {
		if err := validateOrdinaryPlatformQuotaDeliverySource(cmd, receipt, journal, 0); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err := validateOrdinaryPlatformQuotaDeliverySource(cmd, receipt, journal, record.writerEpoch); err != nil {
		return nil, err
	}
	if err := validateOrdinaryPlatformQuotaDeliveryRecord(cmd, receipt, record); err != nil {
		return nil, err
	}
	if record.result.State == service.OrdinaryPlatformDeliveryCompleted {
		record.result.Replayed = true
	}
	return &record.result, nil
}
