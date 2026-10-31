//go:build unit

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func nativeBatchParams(f *settlementEventPGFixture, suffix string, hold float64) service.CreateBatchImageJobParams {
	batchID := "native-" + suffix
	key, account := f.event.Command.APIKeyID, f.event.Command.AccountID
	holdID, requestHash := service.BatchImageHoldRequestID(batchID), sessionHash("native submission "+suffix)
	return service.CreateBatchImageJobParams{BatchID: batchID, UserID: f.session.UserID, APIKeyID: &key, AccountID: &account,
		Provider: service.BatchImageProviderGeminiAPI, Model: "native-image-model", Status: service.BatchImageJobStatusCreated,
		ItemCount: 1, EstimatedCost: hold, HoldAmount: &hold, HoldID: &holdID, RequestHash: &requestHash,
		PricingSnapshotVersion: 1, Currency: "USD"}
}

func createNativeBatch(t *testing.T, f *settlementEventPGFixture, suffix string, hold float64) *service.BatchImageJob {
	t.Helper()
	job, err := NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(context.Background(), nativeBatchParams(f, suffix, hold))
	require.NoError(t, err)
	return job
}

func nativeBatchCommand(t *testing.T, job *service.BatchImageJob, operation string, actual float64) *service.BatchImageBalanceHoldCommand {
	t.Helper()
	hold, payload, requestID := job.EstimatedCost, *job.RequestHash, service.BatchImageHoldRequestID(job.BatchID)
	if job.HoldAmount != nil {
		hold = *job.HoldAmount
	}
	reserve := &service.BatchImageBalanceHoldCommand{RequestID: requestID, BatchID: job.BatchID, UserID: job.UserID,
		APIKeyID: *job.APIKeyID, HoldAmount: hold, RequestPayloadHash: payload}
	reserve.Normalize()
	switch operation {
	case "capture":
		requestID, payload = service.BatchImageCaptureRequestID(job.BatchID), sessionHash("native settlement "+job.BatchID)
	case "release":
		requestID = service.BatchImageReleaseRequestID(job.BatchID)
	}
	cmd := &service.BatchImageBalanceHoldCommand{RequestID: requestID, BatchID: job.BatchID, UserID: job.UserID,
		APIKeyID: *job.APIKeyID, HoldAmount: hold, ActualAmount: actual, RequestPayloadHash: payload,
		ExpectedReserveFingerprint: reserve.RequestFingerprint}
	cmd.Normalize()
	if operation == "capture" {
		var err error
		cmd.CapturePlatformQuota, err = service.FreezeBatchImagePlatformQuotaIntent(cmd, job.Provider)
		require.NoError(t, err)
	}
	return cmd
}

func readNativeBirth(t *testing.T, f *settlementEventPGFixture, batchID string) *service.BatchImageNativeBirth {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	birth, err := findBatchImageNativeBirth(context.Background(), tx, batchID)
	require.NoError(t, err)
	require.NotNil(t, birth)
	return birth
}

func nativeBatchCounts(t *testing.T, f *settlementEventPGFixture, batchID string) [4]int {
	t.Helper()
	var result [4]int
	require.NoError(t, f.db.QueryRow(`SELECT
		(SELECT count(*) FROM batch_image_jobs WHERE batch_id=$1),
		(SELECT count(*) FROM batch_image_native_birth WHERE batch_id=$1),
		(SELECT count(*) FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1),
		(SELECT count(*) FROM usage_billing_dedup WHERE request_id IN ('batch_image_hold:'||$1,'batch_image_capture:'||$1,'batch_image_release:'||$1))`, batchID).
		Scan(&result[0], &result[1], &result[2], &result[3]))
	return result
}

func TestBatchImageNativeBirthPostgresAtomicCreationUsesPersistedPrecisionAndZero(t *testing.T) {
	for _, raw := range []float64{2.00000000004, 0.00007812504, 0.00000000004} {
		t.Run(fmt.Sprintf("%.12g", raw), func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			job := createNativeBatch(t, f, "precision", raw)
			birth := readNativeBirth(t, f, job.BatchID)
			persisted := decimal.NewFromFloat(raw).Round(10)
			require.Equal(t, persisted.InexactFloat64(), *job.HoldAmount)
			require.Equal(t, decimal.NewFromFloat(*job.HoldAmount).String(), birth.RawHoldUSD)
			require.NotEqual(t, decimal.NewFromFloat(raw).String(), birth.RawHoldUSD)
			require.Equal(t, job.ID, birth.JobID)
			cmd := nativeBatchCommand(t, job, "reserve", 0)
			proof := readBatchLifecycleProof(t, f, cmd, "reserve")
			require.Equal(t, birth.ReserveFingerprint, proof.BillingFingerprint)
			require.Equal(t, birth.RawHoldUSD, proof.RawHoldUSD)
			if persisted.IsZero() {
				require.Equal(t, "zero_hold", proof.Outcome)
				require.Equal(t, "none", proof.ReserveOrigin)
			} else {
				require.Equal(t, "reserved", proof.Outcome)
				require.Equal(t, "observed_reserve", proof.ReserveOrigin)
			}
			require.Equal(t, [4]int{1, 1, 1, 1}, nativeBatchCounts(t, f, job.BatchID))
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, decimal.NewFromInt(100).Sub(persisted).Round(8).StringFixed(8), balance)
			require.Equal(t, persisted.Round(8).StringFixed(8), frozen)
			if persisted.IsZero() {
				capture := nativeBatchCommand(t, job, "capture", 0)
				result, err := f.billing.CaptureBatchImageBalance(context.Background(), capture)
				require.NoError(t, err)
				require.True(t, result.Applied)
				require.Equal(t, "none", readBatchLifecycleProof(t, f, capture, "capture").ReserveOrigin)
			}
		})
	}
}

func TestBatchImageNativeBirthPostgresAllCreationFailuresRollbackFourFacts(t *testing.T) {
	for _, target := range []string{"job", "birth", "money", "proof", "dedup", "insufficient"} {
		t.Run(target, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			params := nativeBatchParams(f, "rollback", 2)
			table, event := "batch_image_jobs", "INSERT"
			switch target {
			case "birth":
				table = "batch_image_native_birth"
			case "money":
				table, event = "users", "UPDATE"
			case "proof":
				table = "batch_image_balance_lifecycle_journal"
			case "dedup":
				table = "usage_billing_dedup"
			case "insufficient":
				hold := 101.0
				params.HoldAmount, params.EstimatedCost = &hold, hold
			}
			if target != "insufficient" {
				_, err := f.db.Exec(`CREATE FUNCTION native_creation_failure() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN RAISE EXCEPTION 'fixture native creation failure'; END; $$;
					CREATE TRIGGER native_creation_failure BEFORE ` + event + ` ON ` + table + `
					FOR EACH ROW EXECUTE FUNCTION native_creation_failure()`)
				require.NoError(t, err)
			}
			job, err := NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(context.Background(), params)
			require.Error(t, err)
			require.Nil(t, job)
			if target == "insufficient" {
				require.ErrorIs(t, err, service.ErrBatchImageInsufficientBalance)
			} else {
				require.Contains(t, err.Error(), "fixture native creation failure")
			}
			require.Equal(t, [4]int{}, nativeBatchCounts(t, f, params.BatchID))
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "100.00000000", balance)
			require.Equal(t, "0.00000000", frozen)
			if target != "insufficient" {
				_, err = f.db.Exec(`DROP TRIGGER native_creation_failure ON ` + table)
				require.NoError(t, err)
			} else {
				_, err = f.db.Exec(`UPDATE users SET balance=200 WHERE id=$1`, f.session.UserID)
				require.NoError(t, err)
			}
			job, err = NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(context.Background(), params)
			require.NoError(t, err)
			require.NotNil(t, job)
			require.Equal(t, [4]int{1, 1, 1, 1}, nativeBatchCounts(t, f, params.BatchID))
		})
	}
}

func TestBatchImageNativeBirthPostgresCleanupReplaysCaptureAndRefundWithoutNewMoney(t *testing.T) {
	for _, operation := range []string{"capture", "release"} {
		t.Run(operation, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			job := createNativeBatch(t, f, "cleanup", 2)
			batchLifecycleClearDedup(t, f)
			result, err := f.billing.ReserveBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "reserve", 0))
			require.NoError(t, err)
			require.False(t, result.Applied)
			require.False(t, result.LifecycleCoverageMissing)
			actual := 0.0
			apply := f.billing.ReleaseBatchImageBalance
			if operation == "capture" {
				actual, apply = 1.25, f.billing.CaptureBatchImageBalance
			}
			cmd := nativeBatchCommand(t, job, operation, actual)
			result, err = apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, result.Applied)
			before := batchLifecycleJSON(t, f, job.BatchID)
			balance, frozen := batchPlatformQuotaMoney(t, f)
			batchLifecycleClearDedup(t, f)
			result, err = apply(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, result.Applied)
			require.False(t, result.LifecycleCoverageMissing)
			require.False(t, result.PlatformCoverageMissing)
			require.Equal(t, before, batchLifecycleJSON(t, f, job.BatchID))
			afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, balance, afterBalance)
			require.Equal(t, frozen, afterFrozen)
			if operation == "capture" {
				cmd.CapturePlatformQuota = nil
				_, err = apply(context.Background(), cmd)
				require.ErrorIs(t, err, service.ErrBatchImageNativeBirthInput)
			}
		})
	}
}

func TestBatchImageNativeBirthPostgresMissingPermanentFactsNeverReturnCoverageSuccess(t *testing.T) {
	for _, missing := range []string{"reserve", "platform", "capture", "capture-and-platform"} {
		t.Run(missing, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			job := createNativeBatch(t, f, "missing", 2)
			capture := nativeBatchCommand(t, job, "capture", 1.25)
			if missing != "reserve" {
				_, err := f.billing.CaptureBatchImageBalance(context.Background(), capture)
				require.NoError(t, err)
			}
			if missing == "reserve" || missing == "capture" || missing == "capture-and-platform" {
				op := missing
				if missing == "capture-and-platform" {
					op = "capture"
				}
				_, err := f.db.Exec(`DELETE FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1 AND operation=$2`, job.BatchID, op)
				require.NoError(t, err)
			}
			if missing == "platform" || missing == "capture-and-platform" {
				_, err := f.db.Exec(`DELETE FROM usage_billing_batch_image_platform_quota_journal WHERE batch_id=$1`, job.BatchID)
				require.NoError(t, err)
			}
			if missing != "capture-and-platform" {
				batchLifecycleClearDedup(t, f)
			}
			balance, frozen := batchPlatformQuotaMoney(t, f)
			for _, operation := range []string{"reserve", "capture", "release"} {
				actual, apply := 0.0, f.billing.ReserveBatchImageBalance
				if operation == "capture" {
					actual, apply = 1.25, f.billing.CaptureBatchImageBalance
				}
				if operation == "release" {
					apply = f.billing.ReleaseBatchImageBalance
				}
				_, err := apply(context.Background(), nativeBatchCommand(t, job, operation, actual))
				require.ErrorIs(t, err, service.ErrBatchImageNativeBirthHistory)
			}
			afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, balance, afterBalance)
			require.Equal(t, frozen, afterFrozen)
		})
	}
}

func TestBatchImageNativeBirthPostgresIdentitySurvivesKeyAndJobDeletion(t *testing.T) {
	for _, change := range []string{"user", "key", "raw-hold", "hint", "provider", "reserve-payload"} {
		t.Run(change, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			job := createNativeBatch(t, f, "fixed-birth", 2)
			cmd := nativeBatchCommand(t, job, "capture", 1.25)
			cmd.CapturePlatformQuota = nil
			apply := f.billing.CaptureBatchImageBalance
			switch change {
			case "user":
				cmd.UserID++
			case "key":
				cmd.APIKeyID++
			case "raw-hold":
				cmd.HoldAmount = 2.00000000001
			case "hint":
				cmd.ExpectedReserveFingerprint = sessionHash("wrong birth reservation")
			case "reserve-payload":
				cmd = nativeBatchCommand(t, job, "reserve", 0)
				cmd.RequestPayloadHash = sessionHash("other submission")
				apply = f.billing.ReserveBatchImageBalance
			}
			cmd.RequestFingerprint = ""
			cmd.Normalize()
			if change == "provider" {
				var err error
				cmd.CapturePlatformQuota, err = service.FreezeBatchImagePlatformQuotaIntent(cmd, service.BatchImageProviderVertex)
				require.NoError(t, err)
			}
			_, err := apply(context.Background(), cmd)
			require.ErrorIs(t, err, service.ErrBatchImageNativeBirthConflict)
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "98.00000000", balance)
			require.Equal(t, "2.00000000", frozen)
		})
	}
	f := ordinaryPlatformQuotaFixture(t)
	params := nativeBatchParams(f, "deleted-sources", 2)
	job, err := NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(context.Background(), params)
	require.NoError(t, err)
	birth := *readNativeBirth(t, f, job.BatchID)
	require.NoError(t, f.client.APIKey.DeleteOneID(*job.APIKeyID).Exec(context.Background()))
	_, err = f.db.Exec(`DELETE FROM batch_image_jobs WHERE batch_id=$1`, job.BatchID)
	require.NoError(t, err)
	batchLifecycleClearDedup(t, f)
	_, err = f.billing.ReleaseBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "release", 0))
	require.NoError(t, err)
	require.Equal(t, birth, *readNativeBirth(t, f, job.BatchID))
	_, err = NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(context.Background(), params)
	require.ErrorIs(t, err, service.ErrBatchImageNativeBirthConflict)
	counts := nativeBatchCounts(t, f, job.BatchID)
	require.Equal(t, 0, counts[0])
	require.Equal(t, 1, counts[1])
	require.Equal(t, 2, counts[2])
}

func TestBatchImageNativeBirthPostgresLegacyCreationAndStandaloneBirthCannotUpgradeHistory(t *testing.T) {
	for _, kind := range []string{"legacy-create", "direct-insert", "old-reserve-proof"} {
		t.Run(kind, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			params := nativeBatchParams(f, "legacy-birth", 2)
			var job *service.BatchImageJob
			var err error
			if kind == "direct-insert" {
				job, err = createBatchImageJobWithSQL(context.Background(), f.db, params)
			} else {
				job, err = NewBatchImageRepository(f.db).CreateBatchImageJob(context.Background(), params)
			}
			require.NoError(t, err)
			cmd := nativeBatchCommand(t, job, "reserve", 0)
			if kind == "old-reserve-proof" {
				result, err := applyPre258BatchImageReserve(context.Background(), f, cmd)
				require.NoError(t, err)
				require.True(t, result.Applied)
			} else {
				_, err = f.billing.ReserveBatchImageBalance(context.Background(), cmd)
				require.ErrorIs(t, err, service.ErrBatchImageNativeBirthHistory)
			}
			counts := nativeBatchCounts(t, f, job.BatchID)
			require.Equal(t, 1, counts[0])
			require.Zero(t, counts[1])
			_, err = NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(context.Background(), params)
			require.ErrorIs(t, err, service.ErrBatchImageJobExists)
			birth, err := service.FreezeBatchImageNativeBirth(job)
			require.NoError(t, err)
			tx, err := f.db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, insertBatchImageNativeBirth(context.Background(), tx, birth))
			err = tx.Commit()
			require.ErrorContains(t, err, "reserve proof from the current transaction")
			require.Equal(t, counts, nativeBatchCounts(t, f, job.BatchID))
		})
	}
}

func TestBatchImageNativeBirthPostgresOldWriterCommitFenceRequiresCurrentProof(t *testing.T) {
	for _, operation := range []string{"reserve", "capture", "release"} {
		for _, existing := range []bool{false, true} {
			if operation == "reserve" && !existing {
				continue
			}
			t.Run(fmt.Sprintf("%s/existing=%v", operation, existing), func(t *testing.T) {
				f := ordinaryPlatformQuotaFixture(t)
				job := createNativeBatch(t, f, "old-writer", 2)
				actual := 0.0
				if operation == "capture" {
					actual = 1.25
				}
				cmd := nativeBatchCommand(t, job, operation, actual)
				if existing && operation != "reserve" {
					apply := f.billing.CaptureBatchImageBalance
					if operation == "release" {
						apply = f.billing.ReleaseBatchImageBalance
					}
					_, err := apply(context.Background(), cmd)
					require.NoError(t, err)
				}
				createNativeBatch(t, f, "neighbor", 2)
				// Retain the target hold claim, but remove this operation's hot
				// receipt so an old writer will actually run its monetary SQL.
				_, err := f.db.Exec(`DELETE FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID)
				require.NoError(t, err)
				balance, frozen := batchPlatformQuotaMoney(t, f)
				before := batchLifecycleJSON(t, f, job.BatchID)
				tx, err := f.db.BeginTx(context.Background(), nil)
				require.NoError(t, err)
				defer tx.Rollback()
				applied, err := f.billing.(*usageBillingRepository).claimUsageBillingRequest(context.Background(), tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
				require.NoError(t, err)
				require.True(t, applied)
				var money func(context.Context, *sql.Tx, *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error)
				switch operation {
				case "reserve":
					money = reserveUsageBillingBatchImageBalance
				case "capture":
					money = captureUsageBillingBatchImageBalance
				case "release":
					money = releaseUsageBillingBatchImageBalance
				}
				_, err = money(context.Background(), tx, cmd)
				require.NoError(t, err)
				// A real wallet mutation occurs inside the rejected transaction.
				var pendingBalance, pendingFrozen string
				require.NoError(t, tx.QueryRow(`SELECT balance,frozen_balance FROM users WHERE id=$1`, job.UserID).Scan(&pendingBalance, &pendingFrozen))
				require.NotEqual(t, []string{balance, frozen}, []string{pendingBalance, pendingFrozen})
				err = tx.Commit()
				require.ErrorContains(t, err, "money proof from the current transaction")
				afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
				require.Equal(t, balance, afterBalance)
				require.Equal(t, frozen, afterFrozen)
				require.Equal(t, before, batchLifecycleJSON(t, f, job.BatchID))
			})
		}
	}
}

func TestBatchImageNativeBirthPostgresReserveAuthorityIsBirthTransactionAndBirthIsImmutable(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	job := createNativeBatch(t, f, "birth-transaction", 2)
	birth := *readNativeBirth(t, f, job.BatchID)
	for _, mutation := range []string{
		`UPDATE batch_image_native_birth SET created_at=created_at WHERE batch_id=$1`,
		`DELETE FROM batch_image_native_birth WHERE batch_id=$1`,
	} {
		_, err := f.db.Exec(mutation, job.BatchID)
		require.ErrorContains(t, err, "native batch birth is immutable")
		var state interface{ SQLState() string }
		require.ErrorAs(t, err, &state)
		require.Equal(t, "23514", state.SQLState())
		require.Equal(t, birth, *readNativeBirth(t, f, job.BatchID))
	}
	// Even a pre-258 writer that reconstructs a genuinely current 257 proof
	// cannot reserve an already born batch after all reserve receipts vanish.
	batchLifecycleClearDedup(t, f)
	_, err := f.db.Exec(`DELETE FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1 AND operation='reserve'`, job.BatchID)
	require.NoError(t, err)
	cmd := nativeBatchCommand(t, job, "reserve", 0)
	proof, err := service.FreezeBatchImageBalanceLifecycleProof(cmd, "reserve", "reserved", "observed_reserve", cmd.RequestFingerprint)
	require.NoError(t, err)
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockBatchImageBalanceOperation(context.Background(), tx, cmd))
	applied, err := f.billing.(*usageBillingRepository).claimUsageBillingRequest(context.Background(), tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
	require.NoError(t, err)
	require.True(t, applied)
	_, err = reserveUsageBillingBatchImageBalance(context.Background(), tx, cmd)
	require.NoError(t, err)
	require.NoError(t, insertBatchImageBalanceLifecycle(context.Background(), tx, proof))
	var pendingBalance, pendingFrozen string
	require.NoError(t, tx.QueryRow(`SELECT balance,frozen_balance FROM users WHERE id=$1`, job.UserID).Scan(&pendingBalance, &pendingFrozen))
	require.Equal(t, "96.00000000", pendingBalance)
	require.Equal(t, "4.00000000", pendingFrozen)
	err = tx.Commit()
	require.ErrorContains(t, err, "money proof from the current transaction")
	var state interface{ SQLState() string }
	require.ErrorAs(t, err, &state)
	require.Equal(t, "23514", state.SQLState())
	balance, frozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, "98.00000000", balance)
	require.Equal(t, "2.00000000", frozen)
	require.Equal(t, [4]int{1, 1, 0, 0}, nativeBatchCounts(t, f, job.BatchID))
}

func TestBatchImageNativeBirthPostgresArchiveMovementAndOrdinaryMoneyRespectFence(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	job := createNativeBatch(t, f, "archive", 2)
	before := batchLifecycleJSON(t, f, job.BatchID)
	require.NoError(t, NewDashboardAggregationRepository(f.db).CleanupUsageBillingDedup(context.Background(), time.Now().Add(time.Hour)))
	result, err := f.billing.ReserveBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "reserve", 0))
	require.NoError(t, err)
	require.False(t, result.Applied)
	require.Equal(t, before, batchLifecycleJSON(t, f, job.BatchID))
	var hot int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, service.BatchImageHoldRequestID(job.BatchID)).Scan(&hot))
	require.Zero(t, hot)
	ordinary := &service.UsageBillingCommand{RequestID: service.BatchImageCaptureRequestID(job.BatchID), APIKeyID: *job.APIKeyID,
		UserID: job.UserID, BalanceCost: 1.25}
	_, err = f.billing.Apply(context.Background(), ordinary)
	require.ErrorContains(t, err, "money proof from the current transaction")
	balance, frozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, "98.00000000", balance)
	require.Equal(t, "2.00000000", frozen)
	_, err = f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 1.25))
	require.NoError(t, err)
}

func TestBatchImageNativeBirthPostgresCreationSerializesReserveReleaseAndChangedKey(t *testing.T) {
	for _, operation := range []string{"reserve", "release", "changed-key"} {
		t.Run(operation, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			params := nativeBatchParams(f, "locked-create", 2)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			blocker, err := f.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer blocker.Rollback()
			_, err = blocker.Exec(`SELECT id FROM users WHERE id=$1 FOR UPDATE`, params.UserID)
			require.NoError(t, err)
			type creation struct {
				job *service.BatchImageJob
				err error
			}
			created := make(chan creation, 1)
			go func() {
				job, err := NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(ctx, params)
				created <- creation{job, err}
			}()
			require.Eventually(t, func() bool {
				var waiting bool
				err := f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid()
					AND state='active' AND wait_event_type='Lock' AND query LIKE '%UPDATE users%')`).Scan(&waiting)
				return err == nil && waiting
			}, 3*time.Second, 20*time.Millisecond)
			cmd := &service.BatchImageBalanceHoldCommand{RequestID: service.BatchImageHoldRequestID(params.BatchID), BatchID: params.BatchID,
				UserID: params.UserID, APIKeyID: *params.APIKeyID, HoldAmount: *params.HoldAmount, RequestPayloadHash: *params.RequestHash}
			cmd.Normalize()
			cmd.ExpectedReserveFingerprint = cmd.RequestFingerprint
			apply := f.billing.ReserveBatchImageBalance
			if operation == "release" {
				cmd.RequestID = service.BatchImageReleaseRequestID(params.BatchID)
				apply = f.billing.ReleaseBatchImageBalance
			}
			if operation == "changed-key" {
				cmd.APIKeyID++
				cmd.RequestFingerprint = ""
				cmd.Normalize()
			}
			type application struct {
				result *service.BatchImageBalanceHoldResult
				err    error
			}
			done := make(chan application, 1)
			go func() { result, err := apply(ctx, cmd); done <- application{result, err} }()
			require.Eventually(t, func() bool {
				var waiting bool
				err := f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid()
					AND state='active' AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock%')`).Scan(&waiting)
				return err == nil && waiting
			}, 3*time.Second, 20*time.Millisecond)
			require.NoError(t, blocker.Commit())
			birthResult, moneyResult := <-created, <-done
			require.NoError(t, birthResult.err)
			require.NotNil(t, birthResult.job)
			if operation == "changed-key" {
				require.ErrorIs(t, moneyResult.err, service.ErrBatchImageNativeBirthConflict)
			} else {
				require.NoError(t, moneyResult.err)
				require.Equal(t, operation == "release", moneyResult.result.Applied)
			}
			balance, frozen := batchPlatformQuotaMoney(t, f)
			if operation == "release" {
				require.Equal(t, "100.00000000", balance)
				require.Equal(t, "0.00000000", frozen)
			} else {
				require.Equal(t, "98.00000000", balance)
				require.Equal(t, "2.00000000", frozen)
			}
		})
	}
}

func TestBatchImageNativeBirthPostgresRejectsLegacyOriginsAndRecoversNativeStaleHold(t *testing.T) {
	t.Run("legacy-origin-is-not-native-authority", func(t *testing.T) {
		f := ordinaryPlatformQuotaFixture(t)
		job := createNativeBatch(t, f, "bad-origin", 2)
		capture := nativeBatchCommand(t, job, "capture", 1.25)
		_, err := f.billing.CaptureBatchImageBalance(context.Background(), capture)
		require.NoError(t, err)
		changed, err := service.FreezeBatchImageBalanceLifecycleProof(capture, "capture", "captured", "legacy_dedup", capture.ExpectedReserveFingerprint)
		require.NoError(t, err)
		_, err = f.db.Exec(`UPDATE batch_image_balance_lifecycle_journal SET reserve_origin=$1,proof_fingerprint=$2 WHERE batch_id=$3 AND operation='capture'`, changed.ReserveOrigin, changed.ProofFingerprint, job.BatchID)
		require.NoError(t, err)
		for _, operation := range []string{"reserve", "capture", "release"} {
			actual, apply := 0.0, f.billing.ReserveBatchImageBalance
			if operation == "capture" {
				actual, apply = 1.25, f.billing.CaptureBatchImageBalance
			}
			if operation == "release" {
				apply = f.billing.ReleaseBatchImageBalance
			}
			_, err = apply(context.Background(), nativeBatchCommand(t, job, operation, actual))
			require.ErrorIs(t, err, service.ErrBatchImageNativeBirthConflict)
		}
	})
	t.Run("stale-after-atomic-create-refunds-once", func(t *testing.T) {
		f := ordinaryPlatformQuotaFixture(t)
		job := createNativeBatch(t, f, "stale-hold", 2)
		_, err := f.db.Exec(`UPDATE batch_image_jobs SET updated_at=clock_timestamp()-INTERVAL '1 hour' WHERE batch_id=$1`, job.BatchID)
		require.NoError(t, err)
		batchLifecycleClearDedup(t, f)
		recovery := &service.BatchImageBillingRecoveryService{Repo: NewBatchImageRepository(f.db), Billing: f.billing, StaleAfter: time.Minute}
		released, err := recovery.ReleaseStaleUnsubmittedOnce(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, released)
		var status string
		require.NoError(t, f.db.QueryRow(`SELECT status FROM batch_image_jobs WHERE batch_id=$1`, job.BatchID).Scan(&status))
		require.Equal(t, service.BatchImageJobStatusFailed, status)
		proof := readBatchLifecycleProof(t, f, nativeBatchCommand(t, job, "release", 0), "release")
		require.Equal(t, "refunded", proof.Outcome)
		require.Equal(t, "observed_reserve", proof.ReserveOrigin)
		balance, frozen := batchPlatformQuotaMoney(t, f)
		require.Equal(t, "100.00000000", balance)
		require.Equal(t, "0.00000000", frozen)
		released, err = recovery.ReleaseStaleUnsubmittedOnce(context.Background())
		require.NoError(t, err)
		require.Zero(t, released)
	})
}
