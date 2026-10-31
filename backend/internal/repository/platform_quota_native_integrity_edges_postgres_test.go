//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func nativeIntegrityEdgesExpectError(t *testing.T, f *settlementEventPGFixture, platform string, asOf time.Time, expected error) {
	t.Helper()
	balance, frozen := batchPlatformQuotaMoney(t, f)
	result, err := f.events.(service.PlatformQuotaJournalProjectionRepository).
		ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, platform, asOf)
	require.ErrorIs(t, err, expected)
	require.Nil(t, result)
	afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, balance, afterBalance)
	require.Equal(t, frozen, afterFrozen)
}

// Explicit damaged-history fixture. These permanent roots reject ordinary
// mutations; bypass the named guard only inside this disposable transaction.
func nativeIntegrityEdgesRemoveRoot(t *testing.T, f *settlementEventPGFixture, batchID, table, trigger string) {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.Exec(`ALTER TABLE ` + pq.QuoteIdentifier(table) + ` DISABLE TRIGGER ` + pq.QuoteIdentifier(trigger))
	require.NoError(t, err)
	_, err = tx.Exec(`DELETE FROM `+pq.QuoteIdentifier(table)+` WHERE batch_id=$1`, batchID)
	require.NoError(t, err)
	_, err = tx.Exec(`ALTER TABLE ` + pq.QuoteIdentifier(table) + ` ENABLE TRIGGER ` + pq.QuoteIdentifier(trigger))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestPlatformQuotaNativeIntegrityEdgesPostgresNonGeminiDriftPrecedesContributionFilters(t *testing.T) {
	for _, hidden := range []string{"no-active-quota", "as-of", "quota-incarnation", "disposition", "birth-only", "witness-only"} {
		t.Run(hidden, func(t *testing.T) {
			f, quotaID, historicalAsOf := batchProjectionFixture(t)
			job := createNativeBatch(t, f, "non-gemini-drift", 2)
			result, err := f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 1.25))
			require.NoError(t, err)
			require.True(t, result.Applied)
			// A valid production 256 row is Gemini-only. Deliberately remove
			// that constraint before simulating a migration/scope corruption.
			var constraint string
			require.NoError(t, f.db.QueryRow(`SELECT conname FROM pg_constraint
				WHERE conrelid='usage_billing_batch_image_platform_quota_journal'::regclass
				AND contype='c' AND pg_get_constraintdef(oid) LIKE '%platform%gemini%'`).Scan(&constraint))
			_, err = f.db.Exec(`ALTER TABLE usage_billing_batch_image_platform_quota_journal DROP CONSTRAINT ` + pq.QuoteIdentifier(constraint))
			require.NoError(t, err)
			_, err = f.db.Exec(`UPDATE usage_billing_batch_image_platform_quota_journal SET platform='openai' WHERE batch_id=$1`, job.BatchID)
			require.NoError(t, err)
			asOf := clientProjectionNow(t, f.db)
			if hidden != "no-active-quota" {
				_, err = f.db.Exec(`UPDATE user_platform_quotas SET platform='openai' WHERE id=$1`, quotaID)
				require.NoError(t, err)
			}
			switch hidden {
			case "as-of":
				asOf = historicalAsOf
			case "quota-incarnation":
				_, err = f.db.Exec(`UPDATE usage_billing_batch_image_platform_quota_journal SET quota_row_id=$1 WHERE batch_id=$2`, quotaID+1000, job.BatchID)
				require.NoError(t, err)
			case "disposition":
				_, err = f.db.Exec(`UPDATE usage_billing_batch_image_platform_quota_journal
					SET disposition='unlimited',quota_row_id=NULL,daily_limit_usd=NULL,weekly_limit_usd=NULL,monthly_limit_usd=NULL,
					daily_usage_usd=NULL,weekly_usage_usd=NULL,monthly_usage_usd=NULL,
					daily_window_start=NULL,weekly_window_start=NULL,monthly_window_start=NULL WHERE batch_id=$1`, job.BatchID)
				require.NoError(t, err)
			case "birth-only":
				nativeIntegrityEdgesRemoveRoot(t, f, job.BatchID, "batch_image_native_money_operations", "batch_image_native_money_is_immutable")
			case "witness-only":
				nativeIntegrityEdgesRemoveRoot(t, f, job.BatchID, "batch_image_native_birth", "batch_image_native_birth_is_immutable")
			}
			nativeIntegrityEdgesExpectError(t, f, service.PlatformOpenAI, asOf, service.ErrPlatformQuotaJournalProjectionConflict)
		})
	}
}

func TestPlatformQuotaNativeIntegrityEdgesPostgresMatchingReceiptsSurviveArchiveAndCompleteCleanup(t *testing.T) {
	for _, operation := range []string{"capture", "release"} {
		t.Run(operation, func(t *testing.T) {
			f, _, _ := batchProjectionFixture(t)
			job := createNativeBatch(t, f, "retained-receipts", 2)
			actual, apply := 0.0, f.billing.ReleaseBatchImageBalance
			if operation == "capture" {
				actual, apply = 1.25, f.billing.CaptureBatchImageBalance
			}
			result, err := apply(context.Background(), nativeBatchCommand(t, job, operation, actual))
			require.NoError(t, err)
			require.True(t, result.Applied)
			before := readBatchProjection(t, f, clientProjectionNow(t, f.db))
			balance, frozen := batchPlatformQuotaMoney(t, f)
			witness := nativeMoneyWitnessJSON(t, f, job.BatchID)
			_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
				SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup`)
			require.NoError(t, err)
			require.Equal(t, before.Monthly.UsageUSD, readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD,
				"matching retained receipts in both layers remain legal")
			_, err = f.db.Exec(`DELETE FROM usage_billing_dedup`)
			require.NoError(t, err)
			require.Equal(t, before.Monthly.UsageUSD, readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD)
			_, err = f.db.Exec(`DELETE FROM usage_billing_dedup_archive`)
			require.NoError(t, err)
			require.Equal(t, before.Monthly.UsageUSD, readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD,
				"permanent native evidence remains sufficient after both receipt layers are gone")
			afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, balance, afterBalance)
			require.Equal(t, frozen, afterFrozen)
			require.Equal(t, witness, nativeMoneyWitnessJSON(t, f, job.BatchID))
		})
	}
}

func TestPlatformQuotaNativeIntegrityEdgesPostgresRetainedReceiptFingerprintConflicts(t *testing.T) {
	for _, table := range []string{"usage_billing_dedup", "usage_billing_dedup_archive"} {
		for _, operation := range []string{"reserve", "capture", "release"} {
			t.Run(table+"/"+operation, func(t *testing.T) {
				f, _, _ := batchProjectionFixture(t)
				job := createNativeBatch(t, f, "retained-conflict", 2)
				terminal, actual, apply := "capture", 1.25, f.billing.CaptureBatchImageBalance
				if operation == "release" {
					terminal, actual, apply = "release", 0, f.billing.ReleaseBatchImageBalance
				}
				_, err := apply(context.Background(), nativeBatchCommand(t, job, terminal, actual))
				require.NoError(t, err)
				if table == "usage_billing_dedup_archive" {
					_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
						SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup`)
					require.NoError(t, err)
				}
				if operation == "reserve" {
					actual = 0
				}
				cmd := nativeBatchCommand(t, job, operation, actual)
				_, err = f.db.Exec(`UPDATE `+pq.QuoteIdentifier(table)+` SET request_fingerprint=$1 WHERE request_id=$2 AND api_key_id=$3`,
					sessionHash("corrupted retained native receipt"), cmd.RequestID, cmd.APIKeyID)
				require.NoError(t, err)
				nativeIntegrityEdgesExpectError(t, f, service.PlatformGemini, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionConflict)
			})
		}
	}
}

func TestPlatformQuotaNativeIntegrityEdgesPostgresUnprovedCanonicalReceiptIsMissingHistory(t *testing.T) {
	for _, table := range []string{"usage_billing_dedup", "usage_billing_dedup_archive"} {
		for _, operation := range []string{"capture", "release"} {
			t.Run(table+"/"+operation, func(t *testing.T) {
				f, _, _ := batchProjectionFixture(t)
				job := createNativeBatch(t, f, "extra-receipt", 2)
				cmd := nativeBatchCommand(t, job, operation, 0)
				// Native writers cannot commit an extra unproved hot claim.
				// Inject it explicitly as damaged legacy/migration history.
				tx, err := f.db.BeginTx(context.Background(), nil)
				require.NoError(t, err)
				defer tx.Rollback()
				if table == "usage_billing_dedup" {
					_, err = tx.Exec(`ALTER TABLE usage_billing_dedup DISABLE TRIGGER batch_image_native_dedup_requires_current_proof`)
					require.NoError(t, err)
				}
				_, err = tx.Exec(`INSERT INTO `+pq.QuoteIdentifier(table)+`(request_id,api_key_id,request_fingerprint,created_at)
					VALUES($1,$2,$3,clock_timestamp())`, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
				require.NoError(t, err)
				if table == "usage_billing_dedup" {
					_, err = tx.Exec(`ALTER TABLE usage_billing_dedup ENABLE TRIGGER batch_image_native_dedup_requires_current_proof`)
					require.NoError(t, err)
				}
				require.NoError(t, tx.Commit())
				nativeIntegrityEdgesExpectError(t, f, service.PlatformGemini, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionHistory)
			})
		}
	}
}

func TestPlatformQuotaNativeIntegrityEdgesPostgresDifferentKeysKeepIndependentOrdinaryIdentityAndReceipts(t *testing.T) {
	f, _, _ := batchProjectionFixture(t)
	job := createNativeBatch(t, f, "different-key-history", 2)
	ordinary := combinedProjectionOrdinaryAt(t, f, "ordinary:different-key-edges",
		clientProjectionNow(t, f.db), 0.25, service.PlatformGemini)
	capture := nativeBatchCommand(t, job, "capture", 1.25)
	require.NotEqual(t, ordinary.APIKeyID, capture.APIKeyID)
	result, err := f.billing.CaptureBatchImageBalance(context.Background(), capture)
	require.NoError(t, err)
	require.True(t, result.Applied)
	// Simulate a cross-domain history/identity remap after both real money
	// transactions. Rebind the ordinary private hash to its remapped identity;
	// the API key remains distinct. This only tests reader discrimination:
	// 258 still rejects new wrong-key canonical INSERTs after native birth.
	oldRequestID := ordinary.RequestID
	ordinary.RequestID, ordinary.RequestFingerprint = capture.RequestID, ""
	ordinary.Normalize()
	ordinary.OrdinaryPlatformQuota, err = service.FreezeUsageBillingOrdinaryPlatformIntent(ordinary, service.PlatformGemini, 0.25, false)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal
		SET request_id=$1,billing_fingerprint=$2,intent_fingerprint=$3 WHERE request_id=$4 AND api_key_id=$5`,
		ordinary.RequestID, ordinary.RequestFingerprint, ordinary.OrdinaryPlatformQuota.IntentFingerprint, oldRequestID, ordinary.APIKeyID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE usage_billing_dedup SET request_id=$1,request_fingerprint=$2 WHERE request_id=$3 AND api_key_id=$4`,
		ordinary.RequestID, ordinary.RequestFingerprint, oldRequestID, ordinary.APIKeyID)
	require.NoError(t, err)
	var receiptCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, ordinary.RequestID).Scan(&receiptCount))
	require.Equal(t, 2, receiptCount)
	require.Equal(t, "1.5000000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD,
		"request ID plus API key identifies each independent source")
	require.NoError(t, NewDashboardAggregationRepository(f.db).CleanupUsageBillingDedup(context.Background(), time.Now().Add(time.Hour)))
	require.Equal(t, "1.5000000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD)
	batchLifecycleClearDedup(t, f)
	require.Equal(t, "1.5000000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD)
}
