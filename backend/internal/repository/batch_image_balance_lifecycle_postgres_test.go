//go:build unit

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func readBatchLifecycleProof(t *testing.T, f *settlementEventPGFixture, cmd *service.BatchImageBalanceHoldCommand, operation string) *service.BatchImageBalanceLifecycleProof {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	candidate, err := validateBatchImageBalanceLifecycleCommand(cmd, map[string]batchImageBalanceOperation{
		"reserve": batchImageBalanceReserve, "capture": batchImageBalanceCapture, "release": batchImageBalanceRelease}[operation])
	require.NoError(t, err)
	proofs, err := loadBatchImageBalanceLifecycle(context.Background(), tx, cmd, candidate.RawHoldUSD)
	require.NoError(t, err)
	require.NotNil(t, proofs[operation])
	return proofs[operation]
}

func batchLifecycleCount(t *testing.T, f *settlementEventPGFixture, batchID string) int {
	t.Helper()
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1`, batchID).Scan(&count))
	return count
}

func batchLifecycleJSON(t *testing.T, f *settlementEventPGFixture, batchID string) string {
	t.Helper()
	var raw string
	require.NoError(t, f.db.QueryRow(`SELECT json_agg(j ORDER BY operation)::text FROM batch_image_balance_lifecycle_journal j WHERE batch_id=$1`, batchID).Scan(&raw))
	return raw
}

func batchLifecycleClearDedup(t *testing.T, f *settlementEventPGFixture) {
	t.Helper()
	_, err := f.db.Exec(`DELETE FROM usage_billing_dedup; DELETE FROM usage_billing_dedup_archive`)
	require.NoError(t, err)
}

// Execute the pre-257 transaction boundary with the actual historical money
// helpers. This creates real money/dedup receipts, without any new lifecycle or
// platform proof; no new wrapper is used to simulate an old binary.
func batchLifecycleLegacyApply(t *testing.T, f *settlementEventPGFixture, operation string, cmd *service.BatchImageBalanceHoldCommand) {
	t.Helper()
	cmd.Normalize()
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
	default:
		t.Fatalf("unknown old operation %s", operation)
	}
	_, err = money(context.Background(), tx, cmd)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func batchLifecycleLegacyReserve(t *testing.T, f *settlementEventPGFixture, capture *service.BatchImageBalanceHoldCommand) *service.BatchImageBalanceHoldCommand {
	t.Helper()
	reserve := batchPlatformQuotaOperation(capture, service.BatchImageHoldRequestID(capture.BatchID))
	batchLifecycleLegacyApply(t, f, "reserve", reserve)
	capture.ExpectedReserveFingerprint = reserve.RequestFingerprint
	return reserve
}

func TestBatchImageBalanceLifecyclePostgresReserveAndRefundSurviveDedupCleanup(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := batchPlatformQuotaCommand(t, f, "lifecycle-refund", 2, 1.25)
	reserve := batchPlatformQuotaOperation(cmd, service.BatchImageHoldRequestID(cmd.BatchID))
	result, err := applyPre258BatchImageReserve(context.Background(), f, reserve)
	require.NoError(t, err)
	require.True(t, result.Applied)
	proof := readBatchLifecycleProof(t, f, reserve, "reserve")
	require.Equal(t, "reserved", proof.Outcome)
	require.Equal(t, "observed_reserve", proof.ReserveOrigin)
	require.Equal(t, reserve.RequestFingerprint, proof.ReserveFingerprint)
	before := batchLifecycleJSON(t, f, cmd.BatchID)
	batchLifecycleClearDedup(t, f)
	result, err = f.billing.ReserveBatchImageBalance(context.Background(), reserve)
	require.NoError(t, err)
	require.False(t, result.Applied)
	require.False(t, result.LifecycleCoverageMissing)
	require.Equal(t, before, batchLifecycleJSON(t, f, cmd.BatchID))
	release := batchPlatformQuotaOperation(cmd, service.BatchImageReleaseRequestID(cmd.BatchID))
	result, err = f.billing.ReleaseBatchImageBalance(context.Background(), release)
	require.NoError(t, err)
	require.True(t, result.Applied, "permanent reserve, not hot dedup, authorizes the refund")
	proof = readBatchLifecycleProof(t, f, release, "release")
	require.Equal(t, "refunded", proof.Outcome)
	require.Equal(t, "observed_reserve", proof.ReserveOrigin)
	require.Equal(t, reserve.RequestFingerprint, proof.ReserveFingerprint)
	before = batchLifecycleJSON(t, f, cmd.BatchID)
	batchLifecycleClearDedup(t, f)
	result, err = f.billing.ReleaseBatchImageBalance(context.Background(), release)
	require.NoError(t, err)
	require.False(t, result.Applied)
	require.Equal(t, before, batchLifecycleJSON(t, f, cmd.BatchID))
	_, err = f.billing.ReserveBatchImageBalance(context.Background(), reserve)
	require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
	_, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
	require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
	balance, frozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, "100.00000000", balance)
	require.Equal(t, "0.00000000", frozen)
}

func TestBatchImageBalanceLifecyclePostgresNilIntentCaptureKeepsItsPermanentProof(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := batchPlatformQuotaCommand(t, f, "lifecycle-nil", 2, 1.25)
	batchPlatformQuotaReserve(t, f, cmd)
	intent := cmd.CapturePlatformQuota
	cmd.CapturePlatformQuota = nil
	result, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
	require.NoError(t, err)
	require.True(t, result.Applied)
	proof := readBatchLifecycleProof(t, f, cmd, "capture")
	require.Equal(t, "captured", proof.Outcome)
	require.Equal(t, "observed_reserve", proof.ReserveOrigin)
	before := batchLifecycleJSON(t, f, cmd.BatchID)
	batchLifecycleClearDedup(t, f)
	cmd.CapturePlatformQuota = intent
	result, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
	require.NoError(t, err)
	require.False(t, result.Applied)
	require.False(t, result.LifecycleCoverageMissing)
	require.True(t, result.PlatformCoverageMissing, "financial proof does not manufacture platform history")
	var platforms int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_batch_image_platform_quota_journal`).Scan(&platforms))
	require.Zero(t, platforms)
	require.Equal(t, before, batchLifecycleJSON(t, f, cmd.BatchID))
	balance, frozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, "98.75000000", balance)
	require.Equal(t, "0.00000000", frozen)
}

func TestBatchImageBalanceLifecyclePostgresNoHoldAndZeroOutcomesAreExplicitTerminalFacts(t *testing.T) {
	for _, hold := range []float64{0, 2} {
		t.Run(fmt.Sprintf("unknown-fresh-reserve-%g", hold), func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			capture := batchPlatformQuotaCommand(t, f, "unknown-reserve", hold, 0)
			reserve := batchPlatformQuotaOperation(capture, service.BatchImageHoldRequestID(capture.BatchID))
			_, err := f.billing.ReserveBatchImageBalance(context.Background(), reserve)
			require.ErrorIs(t, err, service.ErrBatchImageNativeBirthHistory)
			require.Zero(t, batchLifecycleCount(t, f, capture.BatchID))
			var receipts int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, reserve.RequestID).Scan(&receipts))
			require.Zero(t, receipts)
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "100.00000000", balance)
			require.Equal(t, "0.00000000", frozen)
		})
	}
	for _, kind := range []string{"no-hold", "zero-release", "zero-capture"} {
		t.Run(kind, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			batchPlatformQuotaReserve(t, f, batchPlatformQuotaCommand(t, f, "neighbor", 2, 1))
			hold := 0.0
			if kind == "no-hold" {
				hold = 2
			}
			cmd := batchPlatformQuotaCommand(t, f, kind, hold, 0)
			var result *service.BatchImageBalanceHoldResult
			var err error
			if kind == "zero-capture" {
				result, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
				proof := readBatchLifecycleProof(t, f, cmd, "capture")
				require.Equal(t, "captured", proof.Outcome)
				require.Equal(t, "none", proof.ReserveOrigin)
			} else {
				if kind == "zero-release" {
					batchPlatformQuotaReserve(t, f, cmd)
				}
				release := batchPlatformQuotaOperation(cmd, service.BatchImageReleaseRequestID(cmd.BatchID))
				result, err = f.billing.ReleaseBatchImageBalance(context.Background(), release)
				proof := readBatchLifecycleProof(t, f, release, "release")
				want := "zero_hold"
				if kind == "no-hold" {
					want = "no_hold_terminal"
				}
				require.Equal(t, want, proof.Outcome)
				require.Equal(t, "none", proof.ReserveOrigin)
				require.Empty(t, proof.ReserveFingerprint)
			}
			require.NoError(t, err)
			require.True(t, result.Applied)
			batchLifecycleClearDedup(t, f)
			reserve := batchPlatformQuotaOperation(cmd, service.BatchImageHoldRequestID(cmd.BatchID))
			_, err = f.billing.ReserveBatchImageBalance(context.Background(), reserve)
			require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
			if kind != "zero-capture" {
				_, err = f.billing.CaptureBatchImageBalance(context.Background(), cmd)
				require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
			}
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "98.00000000", balance)
			require.Equal(t, "2.00000000", frozen)
		})
	}
}

func TestBatchImageBalanceLifecyclePostgresEveryMoneyOutcomeRollsBackWithItsProof(t *testing.T) {
	for _, operation := range []string{"reserve", "capture", "release"} {
		for _, target := range []string{"journal", "money"} {
			t.Run(operation+"/"+target, func(t *testing.T) {
				f := ordinaryPlatformQuotaFixture(t)
				capture := batchPlatformQuotaCommand(t, f, "rollback", 2, 1.25)
				cmd := capture
				apply := f.billing.CaptureBatchImageBalance
				pre258Reserve := func(ctx context.Context, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
					return applyPre258BatchImageReserve(ctx, f, cmd)
				}
				if operation != "reserve" {
					batchPlatformQuotaReserve(t, f, capture)
				}
				switch operation {
				case "reserve":
					// Native creation has its own atomic-creation coverage. Keep this
					// historical transaction test on the real pre-258 boundary.
					cmd, apply = batchPlatformQuotaOperation(capture, service.BatchImageHoldRequestID(capture.BatchID)), pre258Reserve
				case "release":
					cmd, apply = batchPlatformQuotaOperation(capture, service.BatchImageReleaseRequestID(capture.BatchID)), f.billing.ReleaseBatchImageBalance
				}
				table, event := "batch_image_balance_lifecycle_journal", "INSERT"
				if target == "money" {
					table, event = "users", "UPDATE"
				}
				_, err := f.db.Exec(`CREATE FUNCTION batch_lifecycle_failure() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN RAISE EXCEPTION 'fixture lifecycle transaction failure'; END; $$;
					CREATE TRIGGER batch_lifecycle_failure BEFORE ` + event + ` ON ` + table + `
					FOR EACH ROW EXECUTE FUNCTION batch_lifecycle_failure()`)
				require.NoError(t, err)
				_, err = apply(context.Background(), cmd)
				require.Error(t, err)
				var receipts, proofs, platforms int
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID).Scan(&receipts))
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1 AND operation=$2`, cmd.BatchID, operation).Scan(&proofs))
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_batch_image_platform_quota_journal`).Scan(&platforms))
				require.Zero(t, receipts)
				require.Zero(t, proofs)
				require.Zero(t, platforms)
				balance, frozen := batchPlatformQuotaMoney(t, f)
				if operation == "reserve" {
					require.Equal(t, "100.00000000", balance)
					require.Equal(t, "0.00000000", frozen)
				} else {
					require.Equal(t, "98.00000000", balance)
					require.Equal(t, "2.00000000", frozen)
				}
				_, err = f.db.Exec(`DROP TRIGGER batch_lifecycle_failure ON ` + table)
				require.NoError(t, err)
				result, err := apply(context.Background(), cmd)
				require.NoError(t, err)
				require.True(t, result.Applied)
				readBatchLifecycleProof(t, f, cmd, operation)
			})
		}
	}
}

func TestBatchImageBalanceLifecyclePostgresConcurrentReserveAndRefundHaveOneEffect(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	capture := batchPlatformQuotaCommand(t, f, "concurrent-lifecycle", 2, 1.25)
	reserve := batchPlatformQuotaOperation(capture, service.BatchImageHoldRequestID(capture.BatchID))
	release := batchPlatformQuotaOperation(capture, service.BatchImageReleaseRequestID(capture.BatchID))
	pre258Reserve := func(ctx context.Context, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
		return applyPre258BatchImageReserve(ctx, f, cmd)
	}
	for _, operation := range []string{"reserve", "release"} {
		// Establish the historical 257 cohort concurrently, then use today's
		// permanent-proof release path after both retained tiers are removed.
		cmd, apply := reserve, pre258Reserve
		if operation == "release" {
			cmd, apply = release, f.billing.ReleaseBatchImageBalance
			batchLifecycleClearDedup(t, f)
		}
		const concurrency = 12
		type outcome struct {
			result *service.BatchImageBalanceHoldResult
			err    error
		}
		results, start := make(chan outcome, concurrency), make(chan struct{})
		var wg sync.WaitGroup
		for range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				candidate := *cmd
				<-start
				result, err := apply(context.Background(), &candidate)
				results <- outcome{result, err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		applied := 0
		for result := range results {
			require.NoError(t, result.err)
			if result.result.Applied {
				applied++
			}
		}
		require.Equal(t, 1, applied)
	}
	require.Equal(t, 2, batchLifecycleCount(t, f, capture.BatchID))
	balance, frozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, "100.00000000", balance)
	require.Equal(t, "0.00000000", frozen)
}

func TestBatchImageBalanceLifecyclePostgresGlobalIdentityAndRawHoldCannotDrift(t *testing.T) {
	for _, changed := range []string{"user", "key", "raw-hold", "reserve-hint"} {
		t.Run(changed, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := batchPlatformQuotaCommand(t, f, "fixed-identity", 2, 1.25)
			batchPlatformQuotaReserve(t, f, cmd)
			before := batchLifecycleJSON(t, f, cmd.BatchID)
			cmd.CapturePlatformQuota = nil
			switch changed {
			case "user":
				cmd.UserID++
			case "key":
				cmd.APIKeyID++
			case "raw-hold":
				cmd.HoldAmount = 2.00000000001
			case "reserve-hint":
				cmd.ExpectedReserveFingerprint = sessionHash("wrong reserve reference")
			}
			cmd.RequestFingerprint = ""
			cmd.Normalize()
			_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
			require.Equal(t, before, batchLifecycleJSON(t, f, cmd.BatchID))
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, "98.00000000", balance)
			require.Equal(t, "2.00000000", frozen)
		})
	}
	f := ordinaryPlatformQuotaFixture(t)
	cmd := batchPlatformQuotaCommand(t, f, "deleted-key", 2, 1.25)
	batchPlatformQuotaReserve(t, f, cmd)
	require.NoError(t, f.client.APIKey.DeleteOneID(cmd.APIKeyID).Exec(context.Background()))
	batchLifecycleClearDedup(t, f)
	release := batchPlatformQuotaOperation(cmd, service.BatchImageReleaseRequestID(cmd.BatchID))
	_, err := f.billing.ReleaseBatchImageBalance(context.Background(), release)
	require.NoError(t, err)
	readBatchLifecycleProof(t, f, release, "release")
	require.Equal(t, 2, batchLifecycleCount(t, f, cmd.BatchID))
	t.Run("changed-key-waits-on-the-same-batch-lock", func(t *testing.T) {
		f := ordinaryPlatformQuotaFixture(t)
		cmd := batchPlatformQuotaCommand(t, f, "changed-key-lock", 2, 1.25)
		batchPlatformQuotaReserve(t, f, cmd)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		blocker, err := f.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer blocker.Rollback()
		require.NoError(t, lockBatchImageBalanceOperation(ctx, blocker, cmd))
		changed := *cmd
		changed.APIKeyID++
		changed.RequestFingerprint, changed.CapturePlatformQuota = "", nil
		changed.Normalize()
		done := make(chan error, 1)
		go func() { _, err := f.billing.CaptureBatchImageBalance(ctx, &changed); done <- err }()
		require.Eventually(t, func() bool {
			var waiting bool
			err := f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid()
				AND state='active' AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock%')`).Scan(&waiting)
			return err == nil && waiting
		}, 3*time.Second, 20*time.Millisecond, "a changed key must wait on the original batch lock")
		require.NoError(t, blocker.Commit())
		require.ErrorIs(t, <-done, service.ErrBatchImageBalanceLifecycleConflict)
		balance, frozen := batchPlatformQuotaMoney(t, f)
		require.Equal(t, "98.00000000", balance)
		require.Equal(t, "2.00000000", frozen)
	})
}

func TestBatchImageBalanceLifecyclePostgresLegacyReserveReferencesAreStrictAndExplicit(t *testing.T) {
	for _, operation := range []string{"capture", "release"} {
		for _, tier := range []string{"hot", "archive", "both", "conflicting-archive", "missing-hint"} {
			t.Run(operation+"/"+tier, func(t *testing.T) {
				f := ordinaryPlatformQuotaFixture(t)
				capture := batchPlatformQuotaCommand(t, f, "legacy-reference", 2, 1.25)
				reserve := batchLifecycleLegacyReserve(t, f, capture)
				if tier != "hot" && tier != "missing-hint" {
					_, err := f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
						SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup`)
					require.NoError(t, err)
				}
				if tier == "archive" {
					_, err := f.db.Exec(`DELETE FROM usage_billing_dedup`)
					require.NoError(t, err)
				}
				if tier == "conflicting-archive" {
					_, err := f.db.Exec(`UPDATE usage_billing_dedup_archive SET request_fingerprint=$1`, sessionHash("other owner/hold/payload"))
					require.NoError(t, err)
				}
				cmd, apply := capture, f.billing.CaptureBatchImageBalance
				if operation == "release" {
					cmd, apply = batchPlatformQuotaOperation(capture, service.BatchImageReleaseRequestID(capture.BatchID)), f.billing.ReleaseBatchImageBalance
				}
				if tier == "missing-hint" {
					cmd.ExpectedReserveFingerprint, cmd.CapturePlatformQuota = "", nil
				}
				result, err := apply(context.Background(), cmd)
				if tier == "conflicting-archive" {
					require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
					require.Zero(t, batchLifecycleCount(t, f, capture.BatchID))
				} else if tier == "missing-hint" && operation == "capture" {
					require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleHistory)
					require.Zero(t, batchLifecycleCount(t, f, capture.BatchID))
				} else {
					require.NoError(t, err)
					require.True(t, result.Applied)
					proof := readBatchLifecycleProof(t, f, cmd, operation)
					require.Equal(t, "legacy_dedup", proof.ReserveOrigin)
					require.Equal(t, reserve.RequestFingerprint, proof.ReserveFingerprint)
					require.Equal(t, 1, batchLifecycleCount(t, f, capture.BatchID), "do not invent a permanent historical reserve")
					batchLifecycleClearDedup(t, f)
					result, err = apply(context.Background(), cmd)
					require.NoError(t, err)
					require.False(t, result.Applied)
				}
			})
		}
	}
}

func TestBatchImageBalanceLifecyclePostgresHistoricalOwnReceiptsNeverBackfillOutcomes(t *testing.T) {
	for _, operation := range []string{"reserve", "capture", "release", "no-hold-release"} {
		t.Run(operation, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			capture := batchPlatformQuotaCommand(t, f, "historical-own", 2, 1.25)
			reserve := batchPlatformQuotaOperation(capture, service.BatchImageHoldRequestID(capture.BatchID))
			cmd, apply := reserve, f.billing.ReserveBatchImageBalance
			if operation != "no-hold-release" {
				batchLifecycleLegacyApply(t, f, "reserve", reserve)
			}
			switch operation {
			case "capture":
				cmd, apply = capture, f.billing.CaptureBatchImageBalance
				batchLifecycleLegacyApply(t, f, "capture", cmd)
			case "release", "no-hold-release":
				cmd, apply = batchPlatformQuotaOperation(capture, service.BatchImageReleaseRequestID(capture.BatchID)), f.billing.ReleaseBatchImageBalance
				batchLifecycleLegacyApply(t, f, "release", cmd)
			}
			balance, frozen := batchPlatformQuotaMoney(t, f)
			result, err := apply(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, result.Applied)
			require.True(t, result.LifecycleCoverageMissing)
			require.Equal(t, operation == "capture", result.PlatformCoverageMissing)
			require.Zero(t, batchLifecycleCount(t, f, capture.BatchID))
			afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, balance, afterBalance)
			require.Equal(t, frozen, afterFrozen)
		})
	}
	t.Run("pre-257-platform-proof", func(t *testing.T) {
		f := ordinaryPlatformQuotaFixture(t)
		cmd := batchPlatformQuotaCommand(t, f, "pre-257-platform", 2, 1.25)
		batchLifecycleLegacyReserve(t, f, cmd)
		tx, err := f.db.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		defer tx.Rollback()
		applied, err := f.billing.(*usageBillingRepository).claimUsageBillingRequest(context.Background(), tx, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
		require.NoError(t, err)
		require.True(t, applied)
		platform, err := observeBatchImagePlatformQuota(context.Background(), tx, cmd)
		require.NoError(t, err)
		_, err = captureUsageBillingBatchImageBalance(context.Background(), tx, cmd)
		require.NoError(t, err)
		require.NoError(t, insertBatchImagePlatformQuotaJournal(context.Background(), tx, platform))
		require.NoError(t, tx.Commit())
		batchLifecycleClearDedup(t, f)
		result, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
		require.NoError(t, err)
		require.False(t, result.Applied)
		require.True(t, result.LifecycleCoverageMissing)
		require.False(t, result.PlatformCoverageMissing)
		require.Zero(t, batchLifecycleCount(t, f, cmd.BatchID))
		release := batchPlatformQuotaOperation(cmd, service.BatchImageReleaseRequestID(cmd.BatchID))
		_, err = f.billing.ReleaseBatchImageBalance(context.Background(), release)
		require.ErrorIs(t, err, service.ErrBatchImageCaptureAlreadyApplied)
	})
}

func TestBatchImageBalanceLifecyclePostgresRejectsUnprovedCaptureAndChangedLegacyIdentity(t *testing.T) {
	for _, kind := range []string{"missing", "changed-user", "changed-key", "changed-hold", "wrong-hint"} {
		t.Run(kind, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := batchPlatformQuotaCommand(t, f, "unproved", 2, 1.25)
			if kind != "missing" {
				batchLifecycleLegacyReserve(t, f, cmd)
			}
			cmd.CapturePlatformQuota = nil
			switch kind {
			case "changed-user":
				cmd.UserID++
			case "changed-key":
				cmd.APIKeyID++
			case "changed-hold":
				cmd.HoldAmount = 3
			case "wrong-hint":
				cmd.ExpectedReserveFingerprint = sessionHash("incorrect hold evidence")
			}
			// Model a trusted service rebuilding its expected reserve from the
			// current owner/hold/payload, rather than reusing an old user's hint.
			if kind == "changed-user" || kind == "changed-key" || kind == "changed-hold" {
				reserve := batchPlatformQuotaOperation(cmd, service.BatchImageHoldRequestID(cmd.BatchID))
				cmd.ExpectedReserveFingerprint = reserve.RequestFingerprint
			}
			cmd.RequestFingerprint = ""
			cmd.Normalize()
			beforeBalance, beforeFrozen := batchPlatformQuotaMoney(t, f)
			_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			want := service.ErrBatchImageBalanceLifecycleConflict
			if kind == "missing" {
				want = service.ErrBatchImageBalanceLifecycleHistory
			}
			require.ErrorIs(t, err, want)
			require.Zero(t, batchLifecycleCount(t, f, cmd.BatchID))
			balance, frozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, beforeBalance, balance)
			require.Equal(t, beforeFrozen, frozen)
		})
	}
}

func TestBatchImageBalanceLifecyclePostgresOrdinaryApplyCannotReuseAnyOperation(t *testing.T) {
	for _, operation := range []string{"reserve", "capture", "release"} {
		t.Run(operation, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			capture := batchPlatformQuotaCommand(t, f, "ordinary-collision", 2, 1.25)
			reserve := batchPlatformQuotaOperation(capture, service.BatchImageHoldRequestID(capture.BatchID))
			batchPlatformQuotaReserve(t, f, capture)
			cmd := reserve
			if operation == "capture" {
				cmd = capture
				_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
				require.NoError(t, err)
			}
			if operation == "release" {
				cmd = batchPlatformQuotaOperation(capture, service.BatchImageReleaseRequestID(capture.BatchID))
				_, err := f.billing.ReleaseBatchImageBalance(context.Background(), cmd)
				require.NoError(t, err)
			}
			batchLifecycleClearDedup(t, f)
			balance, frozen := batchPlatformQuotaMoney(t, f)
			ordinary := &service.UsageBillingCommand{RequestID: cmd.RequestID, APIKeyID: cmd.APIKeyID, UserID: cmd.UserID, BalanceCost: 10}
			_, err := f.billing.Apply(context.Background(), ordinary)
			require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
			afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, balance, afterBalance)
			require.Equal(t, frozen, afterFrozen)

			// Explicitly damage an independently committed ordinary source to
			// model a conflicting historical writer. Even a permanent batch
			// replay must report this collision before its duplicate return.
			other := ordinaryPlatformQuotaCommand(t, f, "damaged-replay-"+operation, 0.25)
			_, err = f.billing.Apply(context.Background(), other)
			require.NoError(t, err)
			_, err = f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal
				SET request_id=$1 WHERE request_id=$2 AND api_key_id=$3`, cmd.RequestID, other.RequestID, cmd.APIKeyID)
			require.NoError(t, err)
			balance, frozen = batchPlatformQuotaMoney(t, f)
			before := batchLifecycleJSON(t, f, cmd.BatchID)
			apply := f.billing.ReserveBatchImageBalance
			if operation == "capture" {
				apply = f.billing.CaptureBatchImageBalance
			} else if operation == "release" {
				apply = f.billing.ReleaseBatchImageBalance
			}
			_, err = apply(context.Background(), cmd)
			require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict)
			afterBalance, afterFrozen = batchPlatformQuotaMoney(t, f)
			require.Equal(t, balance, afterBalance)
			require.Equal(t, frozen, afterFrozen)
			require.Equal(t, before, batchLifecycleJSON(t, f, cmd.BatchID))
		})
	}
	for _, operation := range []string{"reserve", "capture", "release"} {
		t.Run("ordinary-first/"+operation, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			capture := batchPlatformQuotaCommand(t, f, "reverse-collision", 2, 1.25)
			cmd, apply := capture, f.billing.CaptureBatchImageBalance
			if operation == "reserve" {
				cmd, apply = batchPlatformQuotaOperation(capture, service.BatchImageHoldRequestID(capture.BatchID)), f.billing.ReserveBatchImageBalance
			}
			if operation == "release" {
				cmd, apply = batchPlatformQuotaOperation(capture, service.BatchImageReleaseRequestID(capture.BatchID)), f.billing.ReleaseBatchImageBalance
			}
			ordinary := ordinaryPlatformQuotaCommand(t, f, "reverse", 1.25)
			ordinary.RequestID, ordinary.RequestFingerprint, ordinary.OrdinaryPlatformQuota = cmd.RequestID, "", nil
			ordinary.Normalize()
			var err error
			ordinary.OrdinaryPlatformQuota, err = service.FreezeUsageBillingOrdinaryPlatformIntent(ordinary, service.PlatformOpenAI, 1.25, false)
			require.NoError(t, err)
			_, err = f.billing.Apply(context.Background(), ordinary)
			require.NoError(t, err)
			batchLifecycleClearDedup(t, f)
			_, err = apply(context.Background(), cmd)
			require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleConflict, fmt.Sprintf("%s must not reuse ordinary source", operation))
		})
	}
}
