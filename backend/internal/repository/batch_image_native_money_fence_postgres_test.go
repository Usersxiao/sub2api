//go:build unit

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func nativeMoneyWitnessJSON(t *testing.T, f *settlementEventPGFixture, batchID string) string {
	t.Helper()
	var raw string
	require.NoError(t, f.db.QueryRow(`SELECT COALESCE(json_agg(w ORDER BY operation)::text,'[]')
		FROM batch_image_native_money_operations w WHERE batch_id=$1`, batchID).Scan(&raw))
	return raw
}

func requireNativeMoneyFenceViolation(t *testing.T, err error, message string) {
	t.Helper()
	require.ErrorContains(t, err, message)
	var state interface{ SQLState() string }
	require.ErrorAs(t, err, &state)
	require.Equal(t, "23514", state.SQLState())
}

// Reproduce the pre-258 observed-terminal transaction, including its actual
// wallet mutation and new 257/256 proofs. It deliberately never reads a native
// birth or witness, so only the database commit fence protects this writer.
func nativeMoneyPre258TerminalTx(t *testing.T, f *settlementEventPGFixture,
	cmd *service.BatchImageBalanceHoldCommand, operation string) *sql.Tx {
	t.Helper()
	ctx := context.Background()
	cmd.Normalize()
	outcome := "captured"
	if operation == "release" {
		outcome = "refunded"
	}
	proof, err := service.FreezeBatchImageBalanceLifecycleProof(cmd, operation, outcome,
		"observed_reserve", cmd.ExpectedReserveFingerprint)
	require.NoError(t, err)
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, lockBatchImageBalanceOperation(ctx, tx, cmd))
	applied, err := f.billing.(*usageBillingRepository).claimUsageBillingRequest(ctx, tx,
		cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
	require.NoError(t, err)
	require.True(t, applied)
	var platform *batchImagePlatformQuotaJournal
	if operation == "capture" && cmd.CapturePlatformQuota != nil {
		platform, err = observeBatchImagePlatformQuota(ctx, tx, cmd)
		require.NoError(t, err)
	}
	if operation == "capture" {
		_, err = captureUsageBillingBatchImageBalance(ctx, tx, cmd)
	} else {
		_, err = refundUsageBillingBatchImageBalance(ctx, tx, cmd)
	}
	require.NoError(t, err)
	require.NoError(t, insertBatchImageBalanceLifecycle(ctx, tx, proof))
	if platform != nil {
		require.NoError(t, insertBatchImagePlatformQuotaJournal(ctx, tx, platform))
	}
	var currentProof bool
	require.NoError(t, tx.QueryRow(`SELECT write_xid=pg_current_xact_id()
		FROM batch_image_balance_lifecycle_journal WHERE batch_id=$1 AND operation=$2`,
		cmd.BatchID, operation).Scan(&currentProof))
	require.True(t, currentProof, "the old writer really rebuilt a current-transaction money proof")
	if platform != nil {
		require.NoError(t, tx.QueryRow(`SELECT write_xid=pg_current_xact_id()
			FROM usage_billing_batch_image_platform_quota_journal WHERE batch_id=$1`, cmd.BatchID).Scan(&currentProof))
		require.True(t, currentProof, "the old writer really rebuilt a current-transaction platform proof")
	}
	return tx
}

func TestBatchImageNativeMoneyFencePostgresTerminalWitnessRejectsRebuiltSameAndOppositeOperations(t *testing.T) {
	for _, completed := range []string{"capture", "release"} {
		for _, attempted := range []string{"capture", "release"} {
			t.Run(completed+"-then-"+attempted, func(t *testing.T) {
				f := ordinaryPlatformQuotaFixture(t)
				job := createNativeBatch(t, f, "witness-rebuild", 2)
				actual := 0.0
				apply := f.billing.ReleaseBatchImageBalance
				if completed == "capture" {
					actual, apply = 1.25, f.billing.CaptureBatchImageBalance
				}
				result, err := apply(context.Background(), nativeBatchCommand(t, job, completed, actual))
				require.NoError(t, err)
				require.True(t, result.Applied)
				// This other batch supplies a real pooled hold which an old
				// writer could otherwise consume a second time.
				neighbor := createNativeBatch(t, f, "witness-neighbor", 2)
				witness := nativeMoneyWitnessJSON(t, f, job.BatchID)
				neighborWitness := nativeMoneyWitnessJSON(t, f, neighbor.BatchID)
				_, err = f.db.Exec(`DELETE FROM usage_billing_dedup
					WHERE request_id IN ('batch_image_capture:'||$1,'batch_image_release:'||$1)`, job.BatchID)
				require.NoError(t, err)
				_, err = f.db.Exec(`DELETE FROM usage_billing_dedup_archive
					WHERE request_id IN ('batch_image_capture:'||$1,'batch_image_release:'||$1)`, job.BatchID)
				require.NoError(t, err)
				_, err = f.db.Exec(`DELETE FROM batch_image_balance_lifecycle_journal
					WHERE batch_id=$1 AND operation IN ('capture','release')`, job.BatchID)
				require.NoError(t, err)
				_, err = f.db.Exec(`DELETE FROM usage_billing_batch_image_platform_quota_journal WHERE batch_id=$1`, job.BatchID)
				require.NoError(t, err)
				balance, frozen := batchPlatformQuotaMoney(t, f)
				actual, apply = 0, f.billing.ReleaseBatchImageBalance
				if attempted == "capture" {
					actual, apply = 1.25, f.billing.CaptureBatchImageBalance
				}
				cmd := nativeBatchCommand(t, job, attempted, actual)
				_, err = apply(context.Background(), cmd)
				require.ErrorIs(t, err, service.ErrBatchImageNativeBirthHistory,
					"new readers must preserve the witness rather than falling back to legacy authority")
				tx := nativeMoneyPre258TerminalTx(t, f, cmd, attempted)
				var pendingBalance, pendingFrozen string
				require.NoError(t, tx.QueryRow(`SELECT balance,frozen_balance FROM users WHERE id=$1`, job.UserID).
					Scan(&pendingBalance, &pendingFrozen))
				require.NotEqual(t, []string{balance, frozen}, []string{pendingBalance, pendingFrozen})
				requireNativeMoneyFenceViolation(t, tx.Commit(), "native batch money operation has already committed")
				afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
				require.Equal(t, balance, afterBalance)
				require.Equal(t, frozen, afterFrozen)
				require.Equal(t, witness, nativeMoneyWitnessJSON(t, f, job.BatchID))
				require.Equal(t, neighborWitness, nativeMoneyWitnessJSON(t, f, neighbor.BatchID))
				require.Equal(t, 1, batchLifecycleCount(t, f, job.BatchID), "the rebuilt terminal proof rolled back")
				var platforms, receipts int
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_batch_image_platform_quota_journal
					WHERE batch_id=$1`, job.BatchID).Scan(&platforms))
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup
					WHERE request_id=$1`, cmd.RequestID).Scan(&receipts))
				require.Zero(t, platforms)
				require.Zero(t, receipts)
			})
		}
	}
}

func TestBatchImageNativeMoneyFencePostgresCaptureRequiresCurrentMatchingProviderProof(t *testing.T) {
	for _, missing := range []string{"nil-intent", "provider-drift"} {
		t.Run(missing, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			job := createNativeBatch(t, f, "platform-fence", 2)
			cmd := nativeBatchCommand(t, job, "capture", 1.25)
			if missing == "nil-intent" {
				cmd.CapturePlatformQuota = nil
			} else {
				var err error
				cmd.CapturePlatformQuota, err = service.FreezeBatchImagePlatformQuotaIntent(cmd, service.BatchImageProviderVertex)
				require.NoError(t, err)
			}
			balance, frozen := batchPlatformQuotaMoney(t, f)
			witness := nativeMoneyWitnessJSON(t, f, job.BatchID)
			tx := nativeMoneyPre258TerminalTx(t, f, cmd, "capture")
			var pendingFrozen string
			require.NoError(t, tx.QueryRow(`SELECT frozen_balance FROM users WHERE id=$1`, job.UserID).Scan(&pendingFrozen))
			require.Equal(t, "0.00000000", pendingFrozen, "the legacy capture's real money SQL has already run")
			requireNativeMoneyFenceViolation(t, tx.Commit(), "native batch capture requires a platform proof from the current transaction")
			afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
			require.Equal(t, balance, afterBalance)
			require.Equal(t, frozen, afterFrozen)
			require.Equal(t, witness, nativeMoneyWitnessJSON(t, f, job.BatchID))
			require.Equal(t, 1, batchLifecycleCount(t, f, job.BatchID))
		})
	}
}

func TestBatchImageNativeMoneyFencePostgresWitnessIsImmutable(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	job := createNativeBatch(t, f, "witness-immutable", 2)
	_, err := f.billing.CaptureBatchImageBalance(context.Background(), nativeBatchCommand(t, job, "capture", 1.25))
	require.NoError(t, err)
	before := nativeMoneyWitnessJSON(t, f, job.BatchID)
	for _, operation := range []string{"reserve", "capture"} {
		for _, mutation := range []string{
			`UPDATE batch_image_native_money_operations SET write_xid=pg_current_xact_id() WHERE batch_id=$1 AND operation=$2`,
			`UPDATE batch_image_native_money_operations SET proof_fingerprint=proof_fingerprint WHERE batch_id=$1 AND operation=$2`,
			`DELETE FROM batch_image_native_money_operations WHERE batch_id=$1 AND operation=$2`,
		} {
			_, err := f.db.Exec(mutation, job.BatchID, operation)
			requireNativeMoneyFenceViolation(t, err, "native batch money operation is immutable")
			require.Equal(t, before, nativeMoneyWitnessJSON(t, f, job.BatchID))
		}
	}
}

func TestBatchImageNativeMoneyFencePostgresMigrationLeavesOldMetadataUnknownAndDefaultsNewFullXID(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	ctx := context.Background()
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	oldCmd := batchPlatformQuotaCommand(t, f, "metadata-old", 2, 1.25)
	newCmd := batchPlatformQuotaCommand(t, f, "metadata-new", 2, 1.25)
	oldPlatform, err := observeBatchImagePlatformQuota(ctx, tx, oldCmd)
	require.NoError(t, err)
	newPlatform, err := observeBatchImagePlatformQuota(ctx, tx, newCmd)
	require.NoError(t, err)
	// A transaction-local second schema exercises the actual migration over
	// real pre-258 tables and rows, without rebuilding the shared fixture.
	schema := fmt.Sprintf("native_metadata_%d", time.Now().UnixNano())
	_, err = tx.ExecContext(ctx, `CREATE SCHEMA `+schema)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `SET LOCAL search_path TO `+schema)
	require.NoError(t, err)
	for _, name := range []string{"071_add_usage_billing_dedup.sql", "256_usage_billing_batch_image_platform_quota_journal.sql", "257_batch_image_balance_lifecycle_journal.sql"} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, string(raw))
		require.NoError(t, err, name)
	}
	freeze := func(cmd *service.BatchImageBalanceHoldCommand) *service.BatchImageBalanceLifecycleProof {
		reserve := batchPlatformQuotaOperation(cmd, service.BatchImageHoldRequestID(cmd.BatchID))
		proof, err := service.FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", "observed_reserve", reserve.RequestFingerprint)
		require.NoError(t, err)
		return proof
	}
	require.NoError(t, insertBatchImageBalanceLifecycle(ctx, tx, freeze(oldCmd)))
	require.NoError(t, insertBatchImagePlatformQuotaJournal(ctx, tx, oldPlatform))
	var oldLifecycle, oldQuota string
	require.NoError(t, tx.QueryRow(`SELECT to_jsonb(j)::text FROM batch_image_balance_lifecycle_journal j`).Scan(&oldLifecycle))
	require.NoError(t, tx.QueryRow(`SELECT to_jsonb(j)::text FROM usage_billing_batch_image_platform_quota_journal j`).Scan(&oldQuota))
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "258_batch_image_native_birth.sql"))
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, string(raw))
	require.NoError(t, err)
	var oldUnknown, sameOld bool
	require.NoError(t, tx.QueryRow(`SELECT write_xid IS NULL,(to_jsonb(j)-'write_xid')=$1::jsonb
		FROM batch_image_balance_lifecycle_journal j WHERE batch_id=$2`, oldLifecycle, oldCmd.BatchID).Scan(&oldUnknown, &sameOld))
	require.True(t, oldUnknown)
	require.True(t, sameOld)
	require.NoError(t, tx.QueryRow(`SELECT write_xid IS NULL,(to_jsonb(j)-'write_xid')=$1::jsonb
		FROM usage_billing_batch_image_platform_quota_journal j WHERE batch_id=$2`, oldQuota, oldCmd.BatchID).Scan(&oldUnknown, &sameOld))
	require.True(t, oldUnknown)
	require.True(t, sameOld)
	require.NoError(t, insertBatchImageBalanceLifecycle(ctx, tx, freeze(newCmd)))
	require.NoError(t, insertBatchImagePlatformQuotaJournal(ctx, tx, newPlatform))
	for _, table := range []string{"batch_image_balance_lifecycle_journal", "usage_billing_batch_image_platform_quota_journal"} {
		var current, fullType bool
		require.NoError(t, tx.QueryRow(`SELECT write_xid=pg_current_xact_id(),pg_typeof(write_xid)='xid8'::regtype
			FROM `+table+` WHERE batch_id=$1`, newCmd.BatchID).Scan(&current, &fullType))
		require.True(t, current)
		require.True(t, fullType)
	}
}

func TestBatchImageNativeMoneyFencePostgresSavepointUsesFullTransactionIdentity(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	ctx := context.Background()
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	var topXID string
	require.NoError(t, tx.QueryRow(`SELECT pg_current_xact_id()::text`).Scan(&topXID))
	_, err = tx.ExecContext(ctx, `SAVEPOINT native_child`)
	require.NoError(t, err)
	job, err := (&batchImageRepository{sql: tx}).CreateBatchImageJobWithBalanceHold(ctx, nativeBatchParams(f, "subtransaction", 2))
	require.NoError(t, err)
	var birthXID, proofXID string
	var birthIsSubtransaction, proofIsSubtransaction bool
	require.NoError(t, tx.QueryRow(`SELECT b.write_xid::text,p.write_xid::text,
		b.xmin::text<>((pg_current_xact_id()::text::numeric%4294967296)::text),
		p.xmin::text<>((pg_current_xact_id()::text::numeric%4294967296)::text)
		FROM batch_image_native_birth b JOIN batch_image_balance_lifecycle_journal p USING(batch_id)
		WHERE b.batch_id=$1 AND p.operation='reserve'`, job.BatchID).
		Scan(&birthXID, &proofXID, &birthIsSubtransaction, &proofIsSubtransaction))
	require.Equal(t, topXID, birthXID)
	require.Equal(t, topXID, proofXID)
	require.True(t, birthIsSubtransaction, "this tuple's xmin really belongs to a savepoint subtransaction")
	require.True(t, proofIsSubtransaction)
	_, err = tx.ExecContext(ctx, `RELEASE SAVEPOINT native_child`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(), "full xid8 authority accepts valid proofs written in a savepoint")
	var witnessXID string
	require.NoError(t, f.db.QueryRow(`SELECT write_xid::text FROM batch_image_native_money_operations
		WHERE batch_id=$1 AND operation='reserve'`, job.BatchID).Scan(&witnessXID))
	require.Equal(t, topXID, witnessXID)
	result, err := f.billing.ReserveBatchImageBalance(ctx, nativeBatchCommand(t, job, "reserve", 0))
	require.NoError(t, err)
	require.False(t, result.Applied)
}

func TestBatchImageNativeMoneyFencePostgresMissingBirthRetainsPermanentNativeAuthority(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	ctx := context.Background()
	job := createNativeBatch(t, f, "missing-birth-witness", 2)
	witness := nativeMoneyWitnessJSON(t, f, job.BatchID)
	// Explicit corruption fixture: normal DELETE is separately rejected by
	// the immutable guard. Retain the independent money witness throughout.
	corruption, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer corruption.Rollback()
	_, err = corruption.ExecContext(ctx, `ALTER TABLE batch_image_native_birth DISABLE TRIGGER batch_image_native_birth_is_immutable`)
	require.NoError(t, err)
	_, err = corruption.ExecContext(ctx, `DELETE FROM batch_image_native_birth WHERE batch_id=$1`, job.BatchID)
	require.NoError(t, err)
	_, err = corruption.ExecContext(ctx, `ALTER TABLE batch_image_native_birth ENABLE TRIGGER batch_image_native_birth_is_immutable`)
	require.NoError(t, err)
	err = corruption.Commit()
	require.NoError(t, err)
	balance, frozen := batchPlatformQuotaMoney(t, f)
	for _, operation := range []string{"reserve", "capture", "release"} {
		actual, apply := 0.0, f.billing.ReserveBatchImageBalance
		if operation == "capture" {
			actual, apply = 1.25, f.billing.CaptureBatchImageBalance
		} else if operation == "release" {
			apply = f.billing.ReleaseBatchImageBalance
		}
		_, err := apply(ctx, nativeBatchCommand(t, job, operation, actual))
		require.ErrorIs(t, err, service.ErrBatchImageNativeBirthHistory)
	}
	_, err = f.billing.Apply(ctx, &service.UsageBillingCommand{RequestID: service.BatchImageCaptureRequestID(job.BatchID),
		APIKeyID: *job.APIKeyID, UserID: job.UserID, BalanceCost: 1.25})
	requireNativeMoneyFenceViolation(t, err, "native batch birth is missing for permanent money history")
	tx := nativeMoneyPre258TerminalTx(t, f, nativeBatchCommand(t, job, "capture", 1.25), "capture")
	requireNativeMoneyFenceViolation(t, tx.Commit(), "native batch birth is missing for permanent money history")
	_, err = f.db.Exec(`DELETE FROM batch_image_jobs WHERE batch_id=$1`, job.BatchID)
	require.NoError(t, err)
	_, err = NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(ctx, nativeBatchParams(f, "missing-birth-witness", 2))
	require.ErrorIs(t, err, service.ErrBatchImageNativeBirthHistory)
	afterBalance, afterFrozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, balance, afterBalance)
	require.Equal(t, frozen, afterFrozen)
	require.Equal(t, witness, nativeMoneyWitnessJSON(t, f, job.BatchID))
	var jobs, births int
	require.NoError(t, f.db.QueryRow(`SELECT (SELECT count(*) FROM batch_image_jobs WHERE batch_id=$1),
		(SELECT count(*) FROM batch_image_native_birth WHERE batch_id=$1)`, job.BatchID).Scan(&jobs, &births))
	require.Zero(t, jobs)
	require.Zero(t, births)
}

func TestBatchImageNativeMoneyFencePostgresDeferredWitnessFailureRollsBackAtomicCreation(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	_, err := f.db.Exec(`CREATE FUNCTION native_witness_failure() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'fixture native money witness failure'; END; $$;
		CREATE TRIGGER native_witness_failure BEFORE INSERT ON batch_image_native_money_operations
		FOR EACH ROW EXECUTE FUNCTION native_witness_failure()`)
	require.NoError(t, err)
	params := nativeBatchParams(f, "witness-rollback", 2)
	job, err := NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(context.Background(), params)
	require.ErrorContains(t, err, "fixture native money witness failure")
	require.Nil(t, job)
	require.Equal(t, [4]int{}, nativeBatchCounts(t, f, params.BatchID))
	require.Equal(t, "[]", nativeMoneyWitnessJSON(t, f, params.BatchID))
	balance, frozen := batchPlatformQuotaMoney(t, f)
	require.Equal(t, "100.00000000", balance)
	require.Equal(t, "0.00000000", frozen)
	_, err = f.db.Exec(`DROP TRIGGER native_witness_failure ON batch_image_native_money_operations`)
	require.NoError(t, err)
	job, err = NewBatchImageRepository(f.db).CreateBatchImageJobWithBalanceHold(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, [4]int{1, 1, 1, 1}, nativeBatchCounts(t, f, params.BatchID))
	require.NotEqual(t, "[]", nativeMoneyWitnessJSON(t, f, params.BatchID))
}
