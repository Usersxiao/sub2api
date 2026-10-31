//go:build unit

package repository

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func batchPlatformQuotaCommand(t *testing.T, f *settlementEventPGFixture, suffix string, hold, actual float64) *service.BatchImageBalanceHoldCommand {
	t.Helper()
	batchID := "batch-platform-" + suffix
	cmd := &service.BatchImageBalanceHoldCommand{RequestID: service.BatchImageCaptureRequestID(batchID),
		APIKeyID: f.event.Command.APIKeyID, UserID: f.session.UserID, BatchID: batchID,
		HoldAmount: hold, ActualAmount: actual, RequestPayloadHash: sessionHash("batch platform manifest")}
	cmd.Normalize()
	var err error
	cmd.CapturePlatformQuota, err = service.FreezeBatchImagePlatformQuotaIntent(cmd, service.BatchImageProviderGeminiAPI)
	require.NoError(t, err)
	return cmd
}

func batchPlatformQuotaReserve(t *testing.T, f *settlementEventPGFixture, capture *service.BatchImageBalanceHoldCommand) {
	t.Helper()
	reserve := batchPlatformQuotaOperation(capture, service.BatchImageHoldRequestID(capture.BatchID))
	result, err := applyPre258BatchImageReserve(context.Background(), f, reserve)
	require.NoError(t, err)
	require.True(t, result.Applied)
}

// Reproduce the pre-258 reserve transaction using its real money SQL, dedup,
// and 257 proof. These historical fixtures deliberately have no native birth;
// converting them to native jobs would change legacy and nil-intent coverage.
func applyPre258BatchImageReserve(ctx context.Context, f *settlementEventPGFixture, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	cmd.Normalize()
	outcome, origin, reserveFingerprint := "zero_hold", "none", ""
	if cmd.HoldAmount > 0 {
		outcome, origin, reserveFingerprint = "reserved", "observed_reserve", cmd.RequestFingerprint
	}
	proof, err := service.FreezeBatchImageBalanceLifecycleProof(cmd, "reserve", outcome, origin, reserveFingerprint)
	if err != nil {
		return nil, err
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockBatchImageBalanceOperation(ctx, tx, cmd); err != nil {
		return nil, err
	}
	applied, err := f.billing.(*usageBillingRepository).claimUsageBillingRequest(ctx, tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
	if err != nil {
		return nil, err
	}
	if !applied {
		return &service.BatchImageBalanceHoldResult{Applied: false}, nil
	}
	result, err := reserveUsageBillingBatchImageBalance(ctx, tx, cmd)
	if err != nil {
		return nil, err
	}
	if err := insertBatchImageBalanceLifecycle(ctx, tx, proof); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if result == nil {
		result = &service.BatchImageBalanceHoldResult{}
	}
	result.Applied = true
	return result, nil
}

func batchPlatformQuotaOperation(capture *service.BatchImageBalanceHoldCommand, requestID string) *service.BatchImageBalanceHoldCommand {
	cmd := *capture
	cmd.RequestID, cmd.RequestFingerprint, cmd.ActualAmount, cmd.CapturePlatformQuota = requestID, "", 0, nil
	cmd.Normalize()
	return &cmd
}

func batchPlatformQuotaRow(t *testing.T, f *settlementEventPGFixture, dailyLimit *float64) int64 {
	t.Helper()
	id := settlementPlatformQuotaRow(t, f, dailyLimit)
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET platform='gemini' WHERE id=$1`, id)
	require.NoError(t, err)
	return id
}

func readBatchPlatformQuotaJournal(t *testing.T, f *settlementEventPGFixture, cmd *service.BatchImageBalanceHoldCommand) *batchImagePlatformQuotaJournal {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	j, err := findBatchImagePlatformQuotaJournal(context.Background(), tx, cmd.RequestID, cmd.APIKeyID)
	require.NoError(t, err)
	require.NotNil(t, j)
	return j
}

func batchPlatformQuotaJournalJSON(t *testing.T, f *settlementEventPGFixture, cmd *service.BatchImageBalanceHoldCommand) string {
	t.Helper()
	var raw string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(j)::text FROM usage_billing_batch_image_platform_quota_journal j
		WHERE request_id=$1 AND api_key_id=$2`, cmd.RequestID, cmd.APIKeyID).Scan(&raw))
	return raw
}

func batchPlatformQuotaMoney(t *testing.T, f *settlementEventPGFixture) (string, string) {
	t.Helper()
	var balance, frozen string
	require.NoError(t, f.db.QueryRow(`SELECT balance,frozen_balance FROM users WHERE id=$1`, f.session.UserID).Scan(&balance, &frozen))
	return balance, frozen
}

func TestBatchImagePlatformQuotaPostgresIndependentRawMoneyAndMetering(t *testing.T) {
	for _, amounts := range [][2]float64{{1, 0.00007812504}, {1, 0.00000000496}, {0.0000000051, 0.0000000049}, {1, math.SmallestNonzeroFloat64}, {1, 0}} {
		t.Run(fmt.Sprintf("%.12g-%.12g", amounts[0], amounts[1]), func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			limit := 5.0
			quotaID := batchPlatformQuotaRow(t, f, &limit)
			cmd := batchPlatformQuotaCommand(t, f, "rounding", amounts[0], amounts[1])
			originalFingerprint := cmd.RequestFingerprint
			batchPlatformQuotaReserve(t, f, cmd)
			result, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, result.Applied)
			require.Equal(t, originalFingerprint, cmd.RequestFingerprint)
			j := readBatchPlatformQuotaJournal(t, f, cmd)
			require.Equal(t, decimal.NewFromFloat(amounts[0]).String(), j.rawHoldUSD)
			require.Equal(t, decimal.NewFromFloat(amounts[1]).String(), j.rawCostUSD)
			require.Equal(t, decimal.NewFromFloat(amounts[1]).Round(10).StringFixed(10), j.costUSD)
			require.Equal(t, cmd.CapturePlatformQuota.IntentFingerprint, j.intentFingerprint)
			require.Equal(t, service.PlatformGemini, j.platform)
			hold, actual := decimal.NewFromFloat(amounts[0]), decimal.NewFromFloat(amounts[1])
			// Preserve the two independent numeric(20,8) wallet assignments:
			// reserve rounds first, capture later applies raw hold-minus-actual.
			wantBalance := decimal.NewFromInt(100).Sub(hold).Round(8).Add(hold).Sub(actual).Round(8)
			wantFrozen := hold.Round(8).Sub(hold).Round(8)
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, wantBalance.StringFixed(8), balance)
			require.Equal(t, wantFrozen.StringFixed(8), frozen)
			if j.costUSD == "0.0000000000" {
				require.Equal(t, "zero_cost", j.disposition)
				require.False(t, j.quotaRowID.Valid)
			} else {
				require.Equal(t, "metered", j.disposition)
				require.Equal(t, quotaID, j.quotaRowID.Int64)
			}
			if amounts[0] == 0.0000000051 {
				require.Equal(t, "99.99999999", balance, "legacy hold rounding is not round8(actual)")
			}
			before := batchPlatformQuotaJournalJSON(t, f, cmd)
			retry, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, retry.Applied)
			require.Equal(t, before, batchPlatformQuotaJournalJSON(t, f, cmd))
		})
	}
}

func TestBatchImagePlatformQuotaPostgresCaptureMoneyAndJournalRollbackTogether(t *testing.T) {
	for _, target := range []string{"journal", "money"} {
		t.Run(target, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := batchPlatformQuotaCommand(t, f, "rollback", 2, 1.25)
			batchPlatformQuotaReserve(t, f, cmd)
			table, event := "usage_billing_batch_image_platform_quota_journal", "INSERT"
			if target == "money" {
				table, event = "users", "UPDATE"
			}
			_, err := f.db.Exec(`CREATE FUNCTION batch_platform_fixture_failure() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'fixture batch transaction failure'; END; $$;
				CREATE TRIGGER batch_platform_fixture_failure BEFORE ` + event + ` ON ` + table + `
				FOR EACH ROW EXECUTE FUNCTION batch_platform_fixture_failure()`)
			require.NoError(t, err)
			_, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.Error(t, err)
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "98.00000000", balance)
			require.Equal(t, "2.00000000", frozen)
			var dedup, journal int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID).Scan(&dedup))
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_batch_image_platform_quota_journal`).Scan(&journal))
			require.Zero(t, dedup)
			require.Zero(t, journal)
			_, err = f.db.Exec(`DROP TRIGGER batch_platform_fixture_failure ON ` + table)
			require.NoError(t, err)
			result, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, result.Applied)
			balance, frozen = batchPlatformQuotaMoney(t, f)
			require.Equal(t, "98.75000000", balance)
			require.Equal(t, "0.00000000", frozen)
			readBatchPlatformQuotaJournal(t, f, cmd)
		})
	}
}

func TestBatchImagePlatformQuotaPostgresConcurrentCaptureHasOneMoneyAndJournalEffect(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := batchPlatformQuotaCommand(t, f, "concurrent", 2, 1.25)
	batchPlatformQuotaReserve(t, f, cmd)
	const concurrency = 12
	type outcome struct {
		result *service.BatchImageBalanceHoldResult
		err    error
	}
	results := make(chan outcome, concurrency)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidate := *cmd
			<-start
			result, err := f.billing.CaptureBatchImageBalance(context.Background(), &candidate)
			results <- outcome{result, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	applied := 0
	for o := range results {
		require.NoError(t, o.err)
		if o.result.Applied {
			applied++
		}
	}
	require.Equal(t, 1, applied)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_batch_image_platform_quota_journal`).Scan(&count))
	require.Equal(t, 1, count)
	balance, frozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, "98.75000000", balance)
	require.Equal(t, "0.00000000", frozen)
}

func TestBatchImagePlatformQuotaPostgresFreezesAllDispositions(t *testing.T) {
	for _, kind := range []string{"metered", "zero-limit", "missing", "all-null", "soft-deleted", "zero-cost"} {
		t.Run(kind, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			limit, actual := 5.0, 1.25
			var quotaID int64
			if kind != "missing" {
				if kind == "all-null" {
					quotaID = batchPlatformQuotaRow(t, f, nil)
				} else {
					if kind == "zero-limit" {
						limit = 0
					}
					quotaID = batchPlatformQuotaRow(t, f, &limit)
				}
			}
			if kind == "soft-deleted" {
				_, err := f.db.Exec(`UPDATE user_platform_quotas SET deleted_at=clock_timestamp() WHERE id=$1`, quotaID)
				require.NoError(t, err)
			}
			if kind == "zero-cost" {
				actual = 0
			}
			cmd := batchPlatformQuotaCommand(t, f, kind, 2, actual)
			batchPlatformQuotaReserve(t, f, cmd)
			_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.NoError(t, err)
			j := readBatchPlatformQuotaJournal(t, f, cmd)
			require.False(t, j.isSubscription)
			switch kind {
			case "metered", "zero-limit":
				require.Equal(t, "metered", j.disposition)
				require.Equal(t, quotaID, j.quotaRowID.Int64)
				require.Equal(t, fmt.Sprintf("%.10f", limit), j.dailyLimit.String)
			case "zero-cost":
				require.Equal(t, "zero_cost", j.disposition)
				require.False(t, j.quotaRowID.Valid)
			default:
				require.Equal(t, "unlimited", j.disposition)
				require.Equal(t, kind == "all-null", j.quotaRowID.Valid)
			}
		})
	}
}

func TestBatchImagePlatformQuotaPostgresOldCapturesDoNotInventPlatformHistory(t *testing.T) {
	for _, tier := range []string{"hot", "archive", "both", "conflicting-archive"} {
		t.Run(tier, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := batchPlatformQuotaCommand(t, f, tier, 2, 1.25)
			batchPlatformQuotaReserve(t, f, cmd)
			legacy := *cmd
			legacy.CapturePlatformQuota = nil
			result, err := f.billing.CaptureBatchImageBalance(context.Background(), &legacy)
			require.NoError(t, err)
			require.True(t, result.Applied)
			if tier != "hot" {
				_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
					SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID)
				require.NoError(t, err)
			}
			if tier == "archive" {
				_, err = f.db.Exec(`DELETE FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID)
				require.NoError(t, err)
			}
			if tier == "conflicting-archive" {
				_, err = f.db.Exec(`UPDATE usage_billing_dedup_archive SET request_fingerprint=$1 WHERE request_id=$2`, sessionHash("conflicting historical capture"), cmd.RequestID)
				require.NoError(t, err)
			}
			result, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			if tier == "conflicting-archive" {
				require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
			} else {
				require.NoError(t, err)
				require.False(t, result.Applied)
				require.True(t, result.PlatformCoverageMissing)
				result, err = f.billing.CaptureBatchImageBalance(context.Background(), &legacy)
				require.NoError(t, err)
				require.False(t, result.Applied)
				require.False(t, result.PlatformCoverageMissing)
			}
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "98.75000000", balance)
			require.Equal(t, "0.00000000", frozen)
			var count int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_batch_image_platform_quota_journal`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestBatchImagePlatformQuotaPostgresPermanentProofSurvivesCleanupAndChecksBothTiers(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := batchPlatformQuotaCommand(t, f, "proof", 2, 1.25)
	batchPlatformQuotaReserve(t, f, cmd)
	_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
	require.NoError(t, err)
	before := batchPlatformQuotaJournalJSON(t, f, cmd)
	_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
		SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup`)
	require.NoError(t, err)
	for _, tier := range []string{"usage_billing_dedup", "usage_billing_dedup_archive"} {
		_, err = f.db.Exec(`UPDATE `+tier+` SET request_fingerprint=$1 WHERE request_id=$2`, sessionHash("wrong retained proof"), cmd.RequestID)
		require.NoError(t, err)
		_, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
		require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
		_, err = f.db.Exec(`UPDATE `+tier+` SET request_fingerprint=$1 WHERE request_id=$2`, cmd.RequestFingerprint, cmd.RequestID)
		require.NoError(t, err)
	}
	_, err = f.db.Exec(`DELETE FROM usage_billing_dedup; DELETE FROM usage_billing_dedup_archive`)
	require.NoError(t, err)
	for _, withIntent := range []bool{true, false} {
		retry := *cmd
		if !withIntent {
			retry.CapturePlatformQuota = nil
		}
		result, err := f.billing.CaptureBatchImageBalance(context.Background(), &retry)
		require.NoError(t, err)
		require.False(t, result.Applied)
		require.False(t, result.PlatformCoverageMissing)
	}
	require.Equal(t, before, batchPlatformQuotaJournalJSON(t, f, cmd))
	balance, frozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, "98.75000000", balance)
	require.Equal(t, "0.00000000", frozen)
}

func TestBatchImagePlatformQuotaPostgresRejectsChangedIndependentContentAndCrossOperations(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := batchPlatformQuotaCommand(t, f, "identity", 1, 0.00007812504)
	batchPlatformQuotaReserve(t, f, cmd)
	_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
	require.NoError(t, err)
	before := batchPlatformQuotaJournalJSON(t, f, cmd)
	for _, kind := range []string{"provider", "raw-cost", "user", "fingerprint"} {
		changed := *cmd
		changed.CapturePlatformQuota = nil
		switch kind {
		case "provider":
			changed.CapturePlatformQuota, err = service.FreezeBatchImagePlatformQuotaIntent(&changed, service.BatchImageProviderVertex)
			require.NoError(t, err)
		case "raw-cost":
			changed.ActualAmount = 0.00007812503
			changed.CapturePlatformQuota, err = service.FreezeBatchImagePlatformQuotaIntent(&changed, service.BatchImageProviderGeminiAPI)
			require.NoError(t, err)
		case "user":
			changed.UserID++
			changed.RequestFingerprint = ""
			changed.Normalize()
		case "fingerprint":
			changed.RequestFingerprint = sessionHash("other payload")
		}
		_, err = f.billing.CaptureBatchImageBalance(context.Background(), &changed)
		if kind == "fingerprint" {
			require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleInput, kind)
		} else {
			require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict, kind)
		}
	}
	for _, apply := range []func(context.Context, *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error){
		f.billing.ReserveBatchImageBalance, f.billing.ReleaseBatchImageBalance,
	} {
		_, err := apply(context.Background(), cmd)
		require.ErrorIs(t, err, service.ErrBatchImagePlatformQuotaIntent)
	}
	_, err = f.db.Exec(`DELETE FROM usage_billing_dedup; DELETE FROM usage_billing_dedup_archive`)
	require.NoError(t, err)
	ordinary := &service.UsageBillingCommand{RequestID: cmd.RequestID, APIKeyID: cmd.APIKeyID,
		UserID: cmd.UserID, RequestFingerprint: cmd.RequestFingerprint, BalanceCost: 10}
	_, err = f.billing.Apply(context.Background(), ordinary)
	require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
	reserve := batchPlatformQuotaOperation(cmd, service.BatchImageHoldRequestID(cmd.BatchID))
	_, err = f.billing.ReserveBatchImageBalance(context.Background(), reserve)
	require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
	release := batchPlatformQuotaOperation(cmd, service.BatchImageReleaseRequestID(cmd.BatchID))
	_, err = f.billing.ReleaseBatchImageBalance(context.Background(), release)
	require.ErrorIs(t, err, service.ErrBatchImageCaptureAlreadyApplied)
	require.Equal(t, before, batchPlatformQuotaJournalJSON(t, f, cmd))
}

func TestBatchImagePlatformQuotaPostgresCaptureProofBlocksReleaseAcrossAllTiers(t *testing.T) {
	for _, tier := range []string{"permanent", "hot", "archive"} {
		t.Run(tier, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := batchPlatformQuotaCommand(t, f, tier, 2, 1.25)
			batchPlatformQuotaReserve(t, f, cmd)
			if tier != "permanent" {
				cmd.CapturePlatformQuota = nil
			}
			_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.NoError(t, err)
			if tier == "archive" {
				_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
					SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup;
					DELETE FROM usage_billing_dedup`)
				require.NoError(t, err)
			}
			if tier == "permanent" {
				_, err = f.db.Exec(`DELETE FROM usage_billing_dedup; DELETE FROM usage_billing_dedup_archive`)
				require.NoError(t, err)
			}
			neighbor := batchPlatformQuotaCommand(t, f, "neighbor", 2, 1)
			batchPlatformQuotaReserve(t, f, neighbor)
			release := batchPlatformQuotaOperation(cmd, service.BatchImageReleaseRequestID(cmd.BatchID))
			_, err = f.billing.ReleaseBatchImageBalance(context.Background(), release)
			require.ErrorIs(t, err, service.ErrBatchImageCaptureAlreadyApplied)
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "96.75000000", balance)
			require.Equal(t, "2.00000000", frozen, "another job's frozen balance must survive")
		})
	}
}

func TestBatchImagePlatformQuotaPostgresReleaseProofBlocksTypedAndLegacyCapture(t *testing.T) {
	for _, tier := range []string{"hot", "archive"} {
		for _, withIntent := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/intent=%v", tier, withIntent), func(t *testing.T) {
				f := ordinaryPlatformQuotaFixture(t)
				cmd := batchPlatformQuotaCommand(t, f, "released", 2, 1.25)
				batchPlatformQuotaReserve(t, f, cmd)
				release := batchPlatformQuotaOperation(cmd, service.BatchImageReleaseRequestID(cmd.BatchID))
				_, err := f.billing.ReleaseBatchImageBalance(context.Background(), release)
				require.NoError(t, err)
				if tier == "archive" {
					_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
						SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup;
						DELETE FROM usage_billing_dedup`)
					require.NoError(t, err)
				}
				batchPlatformQuotaReserve(t, f, batchPlatformQuotaCommand(t, f, "neighbor", 2, 1))
				if !withIntent {
					cmd.CapturePlatformQuota = nil
				}
				_, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
				require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
				balance, frozen := batchPlatformQuotaMoney(t, f)
				require.Equal(t, "98.00000000", balance)
				require.Equal(t, "2.00000000", frozen)
			})
		}
	}
}

func TestBatchImagePlatformQuotaPostgresCaptureReleaseSerializeBothWinningOrders(t *testing.T) {
	for _, first := range []string{"capture", "release"} {
		t.Run(first, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := batchPlatformQuotaCommand(t, f, "race", 2, 1.25)
			batchPlatformQuotaReserve(t, f, cmd)
			batchPlatformQuotaReserve(t, f, batchPlatformQuotaCommand(t, f, "neighbor", 2, 1))
			release := batchPlatformQuotaOperation(cmd, service.BatchImageReleaseRequestID(cmd.BatchID))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			blocker, err := f.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer blocker.Rollback()
			_, err = blocker.Exec(`SELECT id FROM users WHERE id=$1 FOR UPDATE`, cmd.UserID)
			require.NoError(t, err)
			captureDone, releaseDone := make(chan error, 1), make(chan error, 1)
			capture := func() { _, err := f.billing.CaptureBatchImageBalance(ctx, cmd); captureDone <- err }
			refund := func() { _, err := f.billing.ReleaseBatchImageBalance(ctx, release); releaseDone <- err }
			if first == "capture" {
				go capture()
			} else {
				go refund()
			}
			require.Eventually(t, func() bool {
				var waiting bool
				err := f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid()
					AND state='active' AND wait_event_type='Lock' AND query LIKE '%UPDATE users%')`).Scan(&waiting)
				return err == nil && waiting
			}, 3*time.Second, 20*time.Millisecond)
			if first == "capture" {
				go refund()
			} else {
				go capture()
			}
			require.Eventually(t, func() bool {
				var waiting bool
				err := f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid()
					AND state='active' AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock%')`).Scan(&waiting)
				return err == nil && waiting
			}, 3*time.Second, 20*time.Millisecond)
			require.NoError(t, blocker.Commit())
			captureErr, releaseErr := <-captureDone, <-releaseDone
			balance, frozen := batchPlatformQuotaMoney(t, f)
			if first == "capture" {
				require.NoError(t, captureErr)
				require.ErrorIs(t, releaseErr, service.ErrBatchImageCaptureAlreadyApplied)
				require.Equal(t, "96.75000000", balance)
				readBatchPlatformQuotaJournal(t, f, cmd)
			} else {
				require.NoError(t, releaseErr)
				require.ErrorIs(t, captureErr, service.ErrBatchImageBalanceLifecycleConflict)
				require.Equal(t, "98.00000000", balance)
				var count int
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_batch_image_platform_quota_journal`).Scan(&count))
				require.Zero(t, count)
			}
			require.Equal(t, "2.00000000", frozen)
		})
	}
}

func TestBatchImagePlatformQuotaPostgresSnapshotSurvivesLegacyResetFlushAndReplacement(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	limit := 5.0
	quotaID := batchPlatformQuotaRow(t, f, &limit)
	values := []string{"999999999.1234567891", "888888888.9876543219", "777777777.0123456789",
		"666666666.1357924681", "555555555.2468135792", "444444444.9999999999"}
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET daily_limit_usd=$1,weekly_limit_usd=$2,monthly_limit_usd=$3,
		daily_usage_usd=$4,weekly_usage_usd=$5,monthly_usage_usd=$6 WHERE id=$7`,
		values[0], values[1], values[2], values[3], values[4], values[5], quotaID)
	require.NoError(t, err)
	cmd := batchPlatformQuotaCommand(t, f, "snapshot", 2, 1.25)
	batchPlatformQuotaReserve(t, f, cmd)
	_, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
	require.NoError(t, err)
	j := readBatchPlatformQuotaJournal(t, f, cmd)
	require.Equal(t, values, []string{j.dailyLimit.String, j.weeklyLimit.String, j.monthlyLimit.String,
		j.dailyUsage.String, j.weeklyUsage.String, j.monthlyUsage.String})
	before := batchPlatformQuotaJournalJSON(t, f, cmd)
	quotaRepo := NewUserPlatformQuotaRepository(f.client)
	for _, window := range []string{"daily", "weekly", "monthly"} {
		require.NoError(t, quotaRepo.ResetExpiredWindow(context.Background(), cmd.UserID, service.PlatformGemini, window, time.Now()))
	}
	stale := time.Now().Add(-48 * time.Hour)
	liveQuota, quotaReadErr := quotaRepo.GetByUserPlatform(context.Background(), cmd.UserID, service.PlatformGemini)
	require.NoError(t, quotaReadErr)
	require.NotNil(t, liveQuota)
	require.NoError(t, quotaRepo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{{QuotaRowID: liveQuota.QuotaRowID, ControlGeneration: liveQuota.ControlGeneration, UserID: cmd.UserID,
		Platform: service.PlatformGemini, DailyUsageUSD: 91, WeeklyUsageUSD: 92, MonthlyUsageUSD: 93,
		DailyWindowStart: stale, WeeklyWindowStart: stale, MonthlyWindowStart: stale}}, time.Now()))
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET deleted_at=clock_timestamp() WHERE id=$1`, quotaID)
	require.NoError(t, err)
	newID := batchPlatformQuotaRow(t, f, &limit)
	require.NotEqual(t, quotaID, newID)
	_, err = f.db.Exec(`DELETE FROM user_platform_quotas WHERE id=$1`, quotaID)
	require.NoError(t, err)
	require.Equal(t, before, batchPlatformQuotaJournalJSON(t, f, cmd))
	next := batchPlatformQuotaCommand(t, f, "replacement", 1, 0.5)
	batchPlatformQuotaReserve(t, f, next)
	_, err = f.billing.CaptureBatchImageBalance(context.Background(), next)
	require.NoError(t, err)
	require.Equal(t, newID, readBatchPlatformQuotaJournal(t, f, next).quotaRowID.Int64)
}

func TestBatchImagePlatformQuotaPostgresDeletedCredentialRetainsMoneyAndProof(t *testing.T) {
	for _, deletion := range []string{"before-capture", "after-capture"} {
		t.Run(deletion, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := batchPlatformQuotaCommand(t, f, deletion, 2, 1.25)
			batchPlatformQuotaReserve(t, f, cmd)
			if deletion == "before-capture" {
				require.NoError(t, f.client.APIKey.DeleteOneID(cmd.APIKeyID).Exec(context.Background()))
			}
			_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.NoError(t, err)
			before := batchPlatformQuotaJournalJSON(t, f, cmd)
			if deletion == "after-capture" {
				require.NoError(t, f.client.APIKey.DeleteOneID(cmd.APIKeyID).Exec(context.Background()))
			}
			_, err = f.db.Exec(`DELETE FROM usage_billing_dedup; DELETE FROM usage_billing_dedup_archive`)
			require.NoError(t, err)
			cmd.CapturePlatformQuota = nil
			result, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, result.Applied)
			require.Equal(t, before, batchPlatformQuotaJournalJSON(t, f, cmd))
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "98.75000000", balance)
			require.Equal(t, "0.00000000", frozen)
		})
	}
}
