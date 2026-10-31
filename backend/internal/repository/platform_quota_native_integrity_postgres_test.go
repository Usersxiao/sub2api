//go:build unit

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Explicit corruption seam: production birth/witness UPDATE and DELETE remain
// forbidden. Only these isolated fixtures temporarily disable their guards.
func nativeProjectionCorrupt(t *testing.T, f *settlementEventPGFixture, table, query string, args ...any) {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	trigger := ""
	if table == "batch_image_native_birth" {
		trigger = "batch_image_native_birth_is_immutable"
	}
	if table == "batch_image_native_money_operations" {
		trigger = "batch_image_native_money_is_immutable"
	}
	if trigger != "" {
		_, err = tx.Exec(`ALTER TABLE ` + table + ` DISABLE TRIGGER ` + trigger)
		require.NoError(t, err)
	}
	_, err = tx.Exec(query, args...)
	require.NoError(t, err)
	if trigger != "" {
		_, err = tx.Exec(`ALTER TABLE ` + table + ` ENABLE TRIGGER ` + trigger)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func nativeProjectionExpectError(t *testing.T, f *settlementEventPGFixture, at time.Time, want error) {
	t.Helper()
	result, err := f.events.(service.PlatformQuotaJournalProjectionRepository).ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformGemini, at)
	require.ErrorIs(t, err, want)
	require.Nil(t, result)
}

func TestNativePlatformQuotaProjectionPostgresValidOpenZeroCaptureAndRefund(t *testing.T) {
	f, _, _ := batchProjectionFixture(t)
	createNativeBatch(t, f, "projection-open", 2)
	createNativeBatch(t, f, "projection-zero-open", 0)
	refund := createNativeBatch(t, f, "projection-refund", 2)
	_, err := f.billing.ReleaseBatchImageBalance(context.Background(), nativeBatchCommand(t, refund, "release", 0))
	require.NoError(t, err)
	capture := createNativeBatch(t, f, "projection-capture", 2)
	_, err = f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, capture, "capture", 0.25))
	require.NoError(t, err)
	zero := createNativeBatch(t, f, "projection-zero-capture", 0)
	_, err = f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, zero, "capture", 0))
	require.NoError(t, err)
	projection := readBatchProjection(t, f, clientProjectionNow(t, f.db))
	require.Equal(t, "0.2500000000", projection.Daily.UsageUSD)
	require.Equal(t, "0.2500000000", projection.Monthly.UsageUSD)
	// An earlier contribution cutoff does not weaken current-snapshot integrity.
	before := projection.AsOf.Add(-time.Hour)
	require.Equal(t, "0.0000000000", readBatchProjection(t, f, before).Monthly.UsageUSD)
}

func TestNativePlatformQuotaProjectionPostgresMissingRootsAndAllOperationProofsClose(t *testing.T) {
	for _, missing := range []string{"birth", "all-witness", "reserve-witness", "capture-witness", "reserve", "capture", "capture-and-platform", "platform", "release", "zero-reserve"} {
		t.Run(missing, func(t *testing.T) {
			f, _, _ := batchProjectionFixture(t)
			hold := 2.0
			if missing == "zero-reserve" {
				hold = 0
			}
			job := createNativeBatch(t, f, "projection-missing", hold)
			if missing == "release" {
				_, err := f.billing.ReleaseBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "release", 0))
				require.NoError(t, err)
			} else if missing != "zero-reserve" {
				_, err := f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 0.25))
				require.NoError(t, err)
			}
			switch missing {
			case "birth":
				nativeProjectionCorrupt(t, f, "batch_image_native_birth", `DELETE FROM batch_image_native_birth WHERE batch_id=$1`, job.BatchID)
			case "all-witness":
				nativeProjectionCorrupt(t, f, "batch_image_native_money_operations", `DELETE FROM batch_image_native_money_operations WHERE batch_id=$1`, job.BatchID)
			case "reserve-witness", "capture-witness":
				op := "reserve"
				if missing == "capture-witness" {
					op = "capture"
				}
				nativeProjectionCorrupt(t, f, "batch_image_native_money_operations", `DELETE FROM batch_image_native_money_operations WHERE batch_id=$1 AND operation=$2`, job.BatchID, op)
			case "platform":
				nativeProjectionCorrupt(t, f, "", `DELETE FROM usage_billing_batch_image_platform_quota_journal WHERE batch_id=$1`, job.BatchID)
			default:
				op := missing
				if missing == "capture-and-platform" {
					op = "capture"
				}
				if missing == "zero-reserve" {
					op = "reserve"
				}
				nativeProjectionCorrupt(t, f, "", `DELETE FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1 AND operation=$2`, job.BatchID, op)
				if missing == "capture-and-platform" {
					nativeProjectionCorrupt(t, f, "", `DELETE FROM usage_billing_batch_image_platform_quota_journal WHERE batch_id=$1`, job.BatchID)
				}
			}
			batchLifecycleClearDedup(t, f)
			nativeProjectionExpectError(t, f, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionHistory)
		})
	}
}

func TestNativePlatformQuotaProjectionPostgresScopeHashOriginAndTransactionConflicts(t *testing.T) {
	for _, kind := range []string{"birth-hash", "witness-hash", "proof-hash", "intent-hash", "proof-owner", "proof-key", "raw-hold", "provider", "platform-owner", "platform-key", "legacy-origin", "reserve-xid", "capture-xid", "platform-xid", "birth-xid", "witness-xid", "invalid-zero-xid", "negative-snapshot"} {
		t.Run(kind, func(t *testing.T) {
			f, _, _ := batchProjectionFixture(t)
			job := createNativeBatch(t, f, "projection-conflict", 2)
			cmd := nativeBatchCommand(t, job, "capture", 0.25)
			_, err := f.billing.CaptureBatchImageBalance(context.Background(), cmd)
			require.NoError(t, err)
			switch kind {
			case "birth-hash":
				nativeProjectionCorrupt(t, f, "batch_image_native_birth", `UPDATE batch_image_native_birth SET proof_fingerprint=repeat('1',64) WHERE batch_id=$1`, job.BatchID)
			case "witness-hash":
				nativeProjectionCorrupt(t, f, "batch_image_native_money_operations", `UPDATE batch_image_native_money_operations SET proof_fingerprint=repeat('1',64) WHERE batch_id=$1 AND operation='capture'`, job.BatchID)
			case "proof-hash":
				nativeProjectionCorrupt(t, f, "", `UPDATE batch_image_balance_lifecycle_journal SET proof_fingerprint=repeat('1',64) WHERE batch_id=$1 AND operation='capture'`, job.BatchID)
			case "intent-hash":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET intent_fingerprint=repeat('1',64) WHERE batch_id=$1`, job.BatchID)
			case "proof-owner":
				nativeProjectionCorrupt(t, f, "", `UPDATE batch_image_balance_lifecycle_journal SET user_id=user_id+1000 WHERE batch_id=$1`, job.BatchID)
			case "proof-key":
				nativeProjectionCorrupt(t, f, "", `UPDATE batch_image_balance_lifecycle_journal SET api_key_id=api_key_id+1000 WHERE batch_id=$1`, job.BatchID)
			case "raw-hold":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET raw_hold_usd='2.00000000001' WHERE batch_id=$1`, job.BatchID)
			case "provider":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET provider='vertex' WHERE batch_id=$1`, job.BatchID)
			case "platform-owner":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET user_id=user_id+1000 WHERE batch_id=$1`, job.BatchID)
			case "platform-key":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET api_key_id=api_key_id+1000 WHERE batch_id=$1`, job.BatchID)
			case "legacy-origin":
				proof, err := service.FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", "legacy_dedup", cmd.ExpectedReserveFingerprint)
				require.NoError(t, err)
				nativeProjectionCorrupt(t, f, "", `UPDATE batch_image_balance_lifecycle_journal SET reserve_origin=$1,proof_fingerprint=$2 WHERE batch_id=$3 AND operation='capture'`, proof.ReserveOrigin, proof.ProofFingerprint, job.BatchID)
			case "reserve-xid", "capture-xid":
				op := "reserve"
				if kind == "capture-xid" {
					op = "capture"
				}
				nativeProjectionCorrupt(t, f, "", `UPDATE batch_image_balance_lifecycle_journal SET write_xid=pg_current_xact_id() WHERE batch_id=$1 AND operation=$2`, job.BatchID, op)
			case "platform-xid":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET write_xid=pg_current_xact_id() WHERE batch_id=$1`, job.BatchID)
			case "birth-xid":
				nativeProjectionCorrupt(t, f, "batch_image_native_birth", `UPDATE batch_image_native_birth SET write_xid=pg_current_xact_id() WHERE batch_id=$1`, job.BatchID)
			case "witness-xid":
				nativeProjectionCorrupt(t, f, "batch_image_native_money_operations", `UPDATE batch_image_native_money_operations SET write_xid=pg_current_xact_id() WHERE batch_id=$1 AND operation='capture'`, job.BatchID)
			case "invalid-zero-xid":
				nativeProjectionCorrupt(t, f, "batch_image_native_birth", `UPDATE batch_image_native_birth SET write_xid='0'::xid8 WHERE batch_id=$1`, job.BatchID)
			case "negative-snapshot":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET daily_usage_usd=-1 WHERE batch_id=$1`, job.BatchID)
			}
			nativeProjectionExpectError(t, f, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionConflict)
		})
	}
	for _, table := range []string{"batch_image_balance_lifecycle_journal", "usage_billing_batch_image_platform_quota_journal"} {
		t.Run("unknown-xid/"+table, func(t *testing.T) {
			f, _, _ := batchProjectionFixture(t)
			job := createNativeBatch(t, f, "projection-unknown-xid", 2)
			_, err := f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 0.25))
			require.NoError(t, err)
			nativeProjectionCorrupt(t, f, "", `UPDATE `+table+` SET write_xid=NULL WHERE batch_id=$1`, job.BatchID)
			nativeProjectionExpectError(t, f, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionHistory)
		})
	}
}

func TestNativePlatformQuotaProjectionPostgresContributionFiltersCannotHideMissingHistory(t *testing.T) {
	for _, hidden := range []string{"user", "quota", "future", "before-quota", "disposition", "no-active-quota"} {
		t.Run(hidden, func(t *testing.T) {
			f, quotaID, _ := batchProjectionFixture(t)
			job := createNativeBatch(t, f, "projection-hidden", 2)
			_, err := f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 0.25))
			require.NoError(t, err)
			asOf := clientProjectionNow(t, f.db)
			nativeProjectionCorrupt(t, f, "", `DELETE FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1 AND operation='capture'`, job.BatchID)
			switch hidden {
			case "user":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET user_id=user_id+1000 WHERE batch_id=$1`, job.BatchID)
			case "quota":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET quota_row_id=$2 WHERE batch_id=$1`, job.BatchID, quotaID+1000)
			case "future":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET captured_at=$2,recorded_at=$2 WHERE batch_id=$1`, job.BatchID, asOf.Add(time.Hour))
			case "before-quota":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET captured_at=(SELECT created_at-INTERVAL '1 day' FROM user_platform_quotas WHERE id=$2) WHERE batch_id=$1`, job.BatchID, quotaID)
			case "disposition":
				nativeProjectionCorrupt(t, f, "", `UPDATE usage_billing_batch_image_platform_quota_journal SET disposition='unlimited',quota_row_id=NULL,
				daily_limit_usd=NULL,weekly_limit_usd=NULL,monthly_limit_usd=NULL,daily_usage_usd=NULL,weekly_usage_usd=NULL,monthly_usage_usd=NULL,
				daily_window_start=NULL,weekly_window_start=NULL,monthly_window_start=NULL WHERE batch_id=$1`, job.BatchID)
			case "no-active-quota":
				require.NoError(t, NewUserPlatformQuotaRepository(f.client).UpsertForUser(context.Background(), f.session.UserID, nil))
			}
			nativeProjectionExpectError(t, f, asOf, service.ErrPlatformQuotaJournalProjectionHistory)
		})
	}
}

func TestNativePlatformQuotaProjectionPostgresResetReplacementAndDeletedSourcesRemainObservations(t *testing.T) {
	f, quotaID, _ := batchProjectionFixture(t)
	job := createNativeBatch(t, f, "projection-preserved", 2)
	_, err := f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 0.25))
	require.NoError(t, err)
	require.NoError(t, NewDashboardAggregationRepository(f.db).CleanupUsageBillingDedup(context.Background(), clientProjectionNow(t, f.db).Add(time.Hour)))
	_, err = f.db.Exec(`DELETE FROM batch_image_jobs WHERE batch_id=$1`, job.BatchID)
	require.NoError(t, err)
	require.NoError(t, f.client.APIKey.DeleteOneID(*job.APIKeyID).Exec(context.Background()))
	require.Equal(t, "0.2500000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD)
	batchLifecycleClearDedup(t, f)
	quota := NewUserPlatformQuotaRepository(f.client)
	require.NoError(t, quota.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformGemini, "monthly", clientProjectionNow(t, f.db)))
	require.Equal(t, "0.0000000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD)
	require.NoError(t, quota.UpsertForUser(context.Background(), f.session.UserID, nil))
	limit := 20.0
	require.NoError(t, quota.UpsertForUser(context.Background(), f.session.UserID, []UserPlatformQuotaRecord{{UserID: f.session.UserID, Platform: service.PlatformGemini, DailyLimitUSD: &limit}}))
	projection := readBatchProjection(t, f, clientProjectionNow(t, f.db))
	require.NotEqual(t, quotaID, projection.QuotaRowID)
	require.Equal(t, "0.0000000000", projection.Monthly.UsageUSD)
}

func TestNativePlatformQuotaProjectionPostgresLegacyAndEmptyRootsKeepOriginalScope(t *testing.T) {
	f, _, _ := batchProjectionFixture(t)
	batchProjectionCaptureAt(t, f, "projection-genuine-legacy", clientProjectionNow(t, f.db), 0.125)
	require.Equal(t, "0.1250000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Daily.UsageUSD)
	job := createNativeBatch(t, f, "projection-both-roots-lost", 2)
	_, err := f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 0.25))
	require.NoError(t, err)
	// Losing both identity roots is outside native discovery. Do not invent
	// birth from a 256 row, job, sequence number, timestamp or migration date.
	nativeProjectionCorrupt(t, f, "batch_image_native_birth", `DELETE FROM batch_image_native_birth WHERE batch_id=$1`, job.BatchID)
	nativeProjectionCorrupt(t, f, "batch_image_native_money_operations", `DELETE FROM batch_image_native_money_operations WHERE batch_id=$1`, job.BatchID)
	require.Equal(t, "0.3750000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Daily.UsageUSD)
}

func TestNativePlatformQuotaProjectionPostgresOneReadonlySnapshotSurvivesConcurrentProofDeletion(t *testing.T) {
	f, _, _ := batchProjectionFixture(t)
	job := createNativeBatch(t, f, "projection-snapshot", 2)
	_, err := f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 0.25))
	require.NoError(t, err)
	before := nativeMoneyWitnessJSON(t, f, job.BatchID)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	blocker, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer blocker.Rollback()
	_, err = blocker.ExecContext(ctx, `SELECT pg_advisory_xact_lock(71825825)`)
	require.NoError(t, err)
	_, err = f.db.Exec(`ALTER TABLE batch_image_native_birth RENAME TO native_projection_snapshot_birth;
		CREATE FUNCTION native_projection_assert_context() RETURNS boolean LANGUAGE plpgsql STABLE AS $$ BEGIN
		 IF current_setting('transaction_isolation')<>'repeatable read' OR current_setting('transaction_read_only')<>'on' THEN
		  RAISE EXCEPTION 'native projection requires readonly repeatable read'; END IF;
		 PERFORM pg_advisory_xact_lock(71825825); RETURN true; END; $$;
		CREATE VIEW batch_image_native_birth AS SELECT * FROM native_projection_snapshot_birth WHERE native_projection_assert_context()`)
	require.NoError(t, err)
	type outcome struct {
		result *service.PlatformQuotaJournalProjection
		err    error
	}
	done := make(chan outcome, 1)
	asOf := clientProjectionNow(t, f.db)
	go func() {
		result, err := f.events.(service.PlatformQuotaJournalProjectionRepository).ReadPlatformQuotaJournalProjection(ctx, f.session.UserID, service.PlatformGemini, asOf)
		done <- outcome{result, err}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := f.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid()
		AND state='active' AND wait_event_type='Lock' AND query LIKE '%SELECT batch_id FROM (%')`).Scan(&waiting)
		return err == nil && waiting
	}, 3*time.Second, 20*time.Millisecond)
	_, err = f.db.Exec(`DELETE FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1 AND operation='capture'`, job.BatchID)
	require.NoError(t, err)
	require.NoError(t, blocker.Commit())
	old := <-done
	require.NoError(t, old.err)
	require.NotNil(t, old.result)
	require.Equal(t, "0.2500000000", old.result.Daily.UsageUSD)
	nativeProjectionExpectError(t, f, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionHistory)
	require.Equal(t, before, nativeMoneyWitnessJSON(t, f, job.BatchID), "the reader never rewrites permanent money evidence")
}

func TestNativePlatformQuotaProjectionPostgresNativeIdentityBoundIncludesOpenZeroBatches(t *testing.T) {
	f, _, _ := batchProjectionFixture(t)
	seed := createNativeBatch(t, f, "projection-bound-seed", 0)
	// Only the scale fixture materializes extra zero/open facts in bulk. Every
	// birth and proof is frozen with the actual codecs, and retains matching
	// full transaction IDs. No real user/provider consumption is manufactured.
	records := make([]map[string]any, service.MaxClientPlatformQuotaProjectionEntries)
	for i := range records {
		job := *seed
		job.ID = seed.ID + int64(i) + 1000
		job.BatchID = fmt.Sprintf("native-projection-bound-%05d", i)
		birth, err := service.FreezeBatchImageNativeBirth(&job)
		require.NoError(t, err)
		cmd := nativeBatchCommand(t, &job, "reserve", 0)
		proof, err := service.FreezeBatchImageBalanceLifecycleProof(cmd, "reserve", "zero_hold", "none", "")
		require.NoError(t, err)
		records[i] = map[string]any{"batch_id": birth.BatchID, "job_row_id": birth.JobID, "schema_version": birth.Version,
			"user_id": birth.UserID, "api_key_id": birth.APIKeyID, "provider": birth.Provider, "model": birth.Model,
			"request_hash": birth.RequestHash, "raw_hold_usd": birth.RawHoldUSD, "reserve_fingerprint": birth.ReserveFingerprint,
			"proof_fingerprint": birth.ProofFingerprint, "lifecycle_proof_fingerprint": proof.ProofFingerprint}
	}
	raw, err := json.Marshal(records)
	require.NoError(t, err)
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	columns := `batch_id text,job_row_id bigint,schema_version smallint,user_id bigint,api_key_id bigint,provider text,model text,
		request_hash text,raw_hold_usd text,reserve_fingerprint text,proof_fingerprint text,lifecycle_proof_fingerprint text`
	_, err = tx.Exec(`INSERT INTO batch_image_balance_lifecycle_journal
		(batch_id,operation,request_id,user_id,api_key_id,schema_version,billing_fingerprint,proof_fingerprint,raw_hold_usd,raw_actual_usd,outcome,reserve_origin,reserve_fingerprint)
		SELECT batch_id,'reserve','batch_image_hold:'||batch_id,user_id,api_key_id,schema_version,reserve_fingerprint,lifecycle_proof_fingerprint,
		raw_hold_usd,'0','zero_hold','none','' FROM jsonb_to_recordset($1::jsonb) AS r(`+columns+`)`, string(raw))
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO batch_image_native_birth (`+batchImageNativeBirthColumns+`)
		SELECT schema_version,job_row_id,batch_id,user_id,api_key_id,provider,model,request_hash,raw_hold_usd,reserve_fingerprint,proof_fingerprint
		FROM jsonb_to_recordset($1::jsonb) AS r(`+columns+`)`, string(raw))
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO batch_image_native_money_operations
		(batch_id,operation,request_id,user_id,api_key_id,raw_hold_usd,raw_actual_usd,billing_fingerprint,proof_fingerprint)
		SELECT batch_id,'reserve','batch_image_hold:'||batch_id,user_id,api_key_id,raw_hold_usd,'0',reserve_fingerprint,lifecycle_proof_fingerprint
		FROM jsonb_to_recordset($1::jsonb) AS r(`+columns+`)`, string(raw))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	nativeProjectionExpectError(t, f, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionUnavailable)
	last := records[len(records)-1]["batch_id"]
	nativeProjectionCorrupt(t, f, "batch_image_native_birth", `DELETE FROM batch_image_native_birth WHERE batch_id=$1`, last)
	nativeProjectionCorrupt(t, f, "batch_image_native_money_operations", `DELETE FROM batch_image_native_money_operations WHERE batch_id=$1`, last)
	_, err = f.db.Exec(`DELETE FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1`, last)
	require.NoError(t, err)
	require.Equal(t, "0.0000000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD, "the complete 10,000 native identity read succeeds")
}

func TestNativePlatformQuotaProjectionPostgresSqlFailureNeverFallsBackToSmallerUsage(t *testing.T) {
	f, _, _ := batchProjectionFixture(t)
	createNativeBatch(t, f, "projection-database-failure", 2)
	_, err := f.db.Exec(`ALTER TABLE batch_image_native_money_operations RENAME TO native_projection_missing_table`)
	require.NoError(t, err)
	nativeProjectionExpectError(t, f, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionUnavailable)
}

func TestNativePlatformQuotaProjectionPostgresOpposingSourcesCannotHideNativeOperationIdentity(t *testing.T) {
	for _, source := range []string{"ordinary", "client"} {
		for _, operation := range []string{"reserve", "capture", "release"} {
			t.Run(source+"/"+operation, func(t *testing.T) {
				f, quotaID, _ := batchProjectionFixture(t)
				job := createNativeBatch(t, f, "projection-source-collision", 2)
				table, requestID := "usage_settlement_platform_quota_journal", "client:native-projection-opposing"
				if source == "ordinary" {
					cmd := combinedProjectionOrdinaryAt(t, f, "ordinary:native-projection-opposing", clientProjectionNow(t, f.db), 0.125, service.PlatformGemini)
					table, requestID = "usage_billing_ordinary_platform_quota_journal", cmd.RequestID
				} else {
					clientProjectionJournalAt(t, f, "native-projection-opposing", clientProjectionNow(t, f.db), 0.125, service.PlatformGemini)
					// Client and batch namespaces are normally disjoint. Only
					// this disposable corruption fixture removes that constraint.
					_, err := f.db.Exec(`ALTER TABLE usage_settlement_platform_quota_journal DROP CONSTRAINT usage_settlement_platform_quota_journal_check`)
					require.NoError(t, err)
				}
				target := nativeBatchCommand(t, job, operation, 0)
				_, err := f.db.Exec(`UPDATE `+table+` SET request_id=$1,api_key_id=$2,user_id=user_id+1000,platform='openai',quota_row_id=$3,
					captured_at=(SELECT created_at-INTERVAL '1 day' FROM user_platform_quotas WHERE id=$4) WHERE request_id=$5`,
					target.RequestID, target.APIKeyID, quotaID+1000, quotaID, requestID)
				require.NoError(t, err)
				nativeProjectionExpectError(t, f, clientProjectionNow(t, f.db), service.ErrPlatformQuotaJournalProjectionConflict)
			})
		}
	}
}
