//go:build unit

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func boundQuotaWriter(t *testing.T, repo UserPlatformQuotaRepository) service.UserPlatformQuotaBoundUsageRepository {
	t.Helper()
	writer, ok := NewUserPlatformQuotaServiceAdapter(repo).(service.UserPlatformQuotaBoundUsageRepository)
	require.True(t, ok)
	return writer
}

func boundQuotaBinding(rec *UserPlatformQuotaRecord, at time.Time) service.UserPlatformQuotaUsageBinding {
	return service.UserPlatformQuotaUsageBinding{QuotaRowID: rec.QuotaRowID, ControlGeneration: rec.ControlGeneration,
		UserID: rec.UserID, Platform: rec.Platform, CostUSD: 0.25, CapturedAt: at}
}

func boundQuotaRecordJSON(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var value string
	require.NoError(t, db.QueryRow(`SELECT row_to_json(q)::text FROM user_platform_quotas q WHERE id=$1`, id).Scan(&value))
	return value
}

func TestUserPlatformQuotaBoundPostgresMigrationPreservesHistoricalGenerationAndBatchSchema(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	limit := 100.0
	quotaID := settlementPlatformQuotaRow(t, f, &limit)
	cmd := ordinaryPlatformQuotaCommand(t, f, "before-generation-migration", 0.25)
	result, err := f.billing.Apply(context.Background(), cmd)
	require.NoError(t, err)
	require.NotNil(t, result.OrdinaryPlatformQuotaReceipt)
	// Recreate the actual pre-261 shape containing a committed older proof.
	_, err = f.db.Exec(`ALTER TABLE usage_billing_ordinary_platform_quota_journal DROP COLUMN control_generation`)
	require.NoError(t, err)
	var before string
	require.NoError(t, f.db.QueryRow(`SELECT to_jsonb(j)::text FROM usage_billing_ordinary_platform_quota_journal j`).Scan(&before))
	quotaBefore := boundQuotaRecordJSON(t, f.db, quotaID)
	content, err := os.ReadFile(filepath.Join("..", "..", "migrations", "261_ordinary_platform_quota_control_binding.sql"))
	require.NoError(t, err)
	_, err = f.db.Exec(string(content))
	require.NoError(t, err)
	var after string
	var generation sql.NullInt64
	require.NoError(t, f.db.QueryRow(`SELECT (to_jsonb(j)-'control_generation')::text,control_generation FROM usage_billing_ordinary_platform_quota_journal j`).Scan(&after, &generation))
	require.Equal(t, before, after)
	require.False(t, generation.Valid, "migration does not manufacture historical control provenance")
	require.Equal(t, quotaBefore, boundQuotaRecordJSON(t, f.db, quotaID))
	for _, value := range []int64{0, -1} {
		_, err = f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal SET control_generation=$1`, value)
		require.Error(t, err)
	}
	result, err = f.billing.Apply(context.Background(), cmd)
	require.NoError(t, err)
	require.False(t, result.Applied)
	require.Nil(t, result.OrdinaryPlatformQuotaReceipt)
	var batchHasGeneration bool
	require.NoError(t, f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='usage_billing_batch_image_platform_quota_journal' AND column_name='control_generation')`).Scan(&batchHasGeneration))
	require.False(t, batchHasGeneration)
	fresh := ordinaryPlatformQuotaCommand(t, f, "after-generation-migration", 0.5)
	result, err = f.billing.Apply(context.Background(), fresh)
	require.NoError(t, err)
	require.True(t, result.Applied)
	require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformReceipt(fresh, result.OrdinaryPlatformQuotaReceipt))
	require.Equal(t, quotaID, result.OrdinaryPlatformQuotaReceipt.QuotaRowID)
	require.Positive(t, result.OrdinaryPlatformQuotaReceipt.ControlGeneration)
	journal := readOrdinaryPlatformQuotaJournal(t, f, fresh)
	require.Equal(t, result.OrdinaryPlatformQuotaReceipt.ControlGeneration, journal.controlGeneration.Int64)
	require.True(t, result.OrdinaryPlatformQuotaReceipt.CapturedAt.Equal(journal.capturedAt))
}

func TestUserPlatformQuotaBoundPostgresFreshReceiptIsOriginalAndMoneyAppliedOnce(t *testing.T) {
	for _, name := range []string{"daily-reset", "weekly-reset", "monthly-reset", "limit-edit", "soft-recreate", "physical-recreate"} {
		t.Run(name, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			limit := 100.0
			quotaID := settlementPlatformQuotaRow(t, f, &limit)
			repo := NewUserPlatformQuotaRepository(f.client)
			cmd := ordinaryPlatformQuotaCommand(t, f, "paused-finalize", 0.25)
			result, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, result.Applied)
			receipt := result.OrdinaryPlatformQuotaReceipt
			require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformReceipt(cmd, receipt))
			binding, err := receipt.UsageBinding()
			require.NoError(t, err)
			journalBefore := ordinaryPlatformQuotaJournalJSON(t, f, cmd)
			mutateBoundQuotaControl(t, f, repo, quotaID, name)
			current, err := repo.GetByUserPlatform(context.Background(), cmd.UserID, service.PlatformOpenAI)
			require.NoError(t, err)
			require.NotNil(t, current)
			before := boundQuotaRecordJSON(t, f.db, current.QuotaRowID)
			bound, err := boundQuotaWriter(t, repo).IncrementBoundUsage(context.Background(), binding)
			require.NoError(t, err)
			require.Equal(t, service.QuotaIncrementSuperseded, bound.Status)
			require.Zero(t, bound.AppliedWindows)
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, current.QuotaRowID))
			retry, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, retry.Applied)
			require.Nil(t, retry.OrdinaryPlatformQuotaReceipt)
			require.Equal(t, journalBefore, ordinaryPlatformQuotaJournalJSON(t, f, cmd))
			balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, "99.75000000", balance)
			require.Equal(t, "0.25000000", used)
		})
	}
}

func mutateBoundQuotaControl(t *testing.T, f *settlementEventPGFixture, repo UserPlatformQuotaRepository, quotaID int64, name string) {
	t.Helper()
	ctx := context.Background()
	switch name {
	case "daily-reset", "weekly-reset", "monthly-reset":
		window := map[string]string{"daily-reset": "daily", "weekly-reset": "weekly", "monthly-reset": "monthly"}[name]
		require.NoError(t, repo.ResetExpiredWindow(ctx, f.session.UserID, service.PlatformOpenAI, window, time.Now().UTC()))
	case "limit-edit":
		limit := 200.0
		require.NoError(t, repo.UpsertForUser(ctx, f.session.UserID, []UserPlatformQuotaRecord{{Platform: service.PlatformOpenAI, DailyLimitUSD: &limit}}))
	case "soft-recreate", "physical-recreate":
		if name == "soft-recreate" {
			require.NoError(t, repo.UpsertForUser(ctx, f.session.UserID, nil))
		} else {
			_, err := f.db.Exec(`DELETE FROM user_platform_quotas WHERE id=$1`, quotaID)
			require.NoError(t, err)
		}
		limit := 200.0
		require.NoError(t, repo.UpsertForUser(ctx, f.session.UserID, []UserPlatformQuotaRecord{{Platform: service.PlatformOpenAI, DailyLimitUSD: &limit}}))
	default:
		t.Fatalf("unknown fixture mutation %s", name)
	}
}

func TestUserPlatformQuotaBoundPostgresReceiptIsAbsentWhenMoneyOrCommitFails(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		name := "journal-insert"
		if deferred {
			name = "commit-deferred"
		}
		t.Run(name, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := ordinaryPlatformQuotaCommand(t, f, "failed-observation", 0.25)
			_, err := f.db.Exec(`CREATE FUNCTION bound_fixture_failure() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'fixture transaction failure'; END; $$`)
			require.NoError(t, err)
			trigger := `CREATE TRIGGER bound_fixture_failure BEFORE INSERT ON usage_billing_ordinary_platform_quota_journal
				FOR EACH ROW EXECUTE FUNCTION bound_fixture_failure()`
			if deferred {
				trigger = `CREATE CONSTRAINT TRIGGER bound_fixture_failure AFTER INSERT ON usage_billing_ordinary_platform_quota_journal
				DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION bound_fixture_failure()`
			}
			_, err = f.db.Exec(trigger)
			require.NoError(t, err)
			result, err := f.billing.Apply(context.Background(), cmd)
			require.Error(t, err)
			require.Nil(t, result)
			balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, "100.00000000", balance)
			require.Equal(t, "0.00000000", used)
			var count int
			require.NoError(t, f.db.QueryRow(`SELECT (SELECT count(*) FROM usage_billing_dedup)+(SELECT count(*) FROM usage_billing_ordinary_platform_quota_journal)`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestUserPlatformQuotaBoundPostgresRejectsMissingBindingAndOtherScopes(t *testing.T) {
	f := quotaSnapshotPostgresFixture(t, true)
	rec := f.seed(t, service.PlatformOpenAI)
	writer := boundQuotaWriter(t, f.repo)
	for _, name := range []string{"missing-row", "old-generation", "wrong-user", "wrong-platform", "no-identity", "zero-cost", "nan-cost", "no-clock"} {
		t.Run(name, func(t *testing.T) {
			binding := boundQuotaBinding(rec, quotaIncrementFixtureTime())
			invalid := false
			switch name {
			case "missing-row":
				binding.QuotaRowID++
			case "old-generation":
				binding.ControlGeneration++
			case "wrong-user":
				binding.UserID++
			case "wrong-platform":
				binding.Platform = service.PlatformGemini
			case "no-identity":
				binding.QuotaRowID = 0
				invalid = true
			case "zero-cost":
				binding.CostUSD = 0
				invalid = true
			case "nan-cost":
				binding.CostUSD = math.NaN()
				invalid = true
			case "no-clock":
				binding.CapturedAt = time.Time{}
				invalid = true
			}
			before := boundQuotaRecordJSON(t, f.db, rec.QuotaRowID)
			result, err := writer.IncrementBoundUsage(context.Background(), binding)
			if invalid {
				require.Error(t, err)
				require.Empty(t, result.Status)
			} else {
				require.NoError(t, err)
				require.Equal(t, service.QuotaIncrementSuperseded, result.Status)
			}
			require.Zero(t, result.AppliedWindows)
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
		})
	}
}

func TestUserPlatformQuotaBoundPostgresWindowsAreMonotonicAtNaturalAndMicrosecondBoundaries(t *testing.T) {
	for _, name := range []string{"current", "initial-nil", "daily-rotate", "weekly-rotate", "older-day", "older-week", "all-obsolete", "same-second-future",
		"admin-anchor", "monthly-before-720h", "monthly-at-720h", "monthly-after-720h", "calendar-month-only", "decimal-exact"} {
		t.Run(name, func(t *testing.T) {
			f := quotaSnapshotPostgresFixture(t, true)
			rec := f.seed(t, service.PlatformOpenAI)
			at := time.Date(2026, 10, 26, 12, 0, 0, 123456000, timezone.Location())
			if name == "calendar-month-only" {
				at = time.Date(2026, 11, 1, 12, 0, 0, 123456000, timezone.Location())
			}
			day, week, month := timezone.StartOfDay(at), timezone.StartOfWeek(at), at.Add(-time.Hour)
			starts := [3]*time.Time{&day, &week, &month}
			wantStarts := [3]time.Time{day, week, month}
			usages := [3]string{"1", "2", "3"}
			wantUsage := [3]string{"1.2500000000", "2.2500000000", "3.2500000000"}
			mask := service.QuotaWindowAll
			binding := boundQuotaBinding(rec, at)
			switch name {
			case "initial-nil":
				starts = [3]*time.Time{}
				wantUsage = [3]string{"0.2500000000", "0.2500000000", "0.2500000000"}
				wantStarts[2] = at
			case "daily-rotate":
				old := day.Add(-time.Microsecond)
				starts[0] = &old
				wantUsage[0] = "0.2500000000"
			case "weekly-rotate":
				old := week.Add(-time.Microsecond)
				starts[0], starts[1] = &old, &old
				wantUsage[0], wantUsage[1] = "0.2500000000", "0.2500000000"
			case "older-day":
				later := day.AddDate(0, 0, 1)
				starts[0], wantStarts[0] = &later, later
				mask, wantUsage[0] = service.QuotaWindowWeekly|service.QuotaWindowMonthly, "1.0000000000"
			case "older-week":
				binding.CapturedAt = at.AddDate(0, 0, -7)
				olderMonth := at.AddDate(0, 0, -14)
				starts[2], wantStarts[2] = &olderMonth, olderMonth
				mask, wantUsage[0], wantUsage[1] = service.QuotaWindowMonthly, "1.0000000000", "2.0000000000"
			case "all-obsolete", "same-second-future":
				later := at.Add(24 * time.Hour)
				if name == "same-second-future" {
					later = at.Add(time.Microsecond)
				}
				starts = [3]*time.Time{&later, &later, &later}
				wantStarts = [3]time.Time{later, later, later}
				mask, wantUsage = 0, [3]string{"1.0000000000", "2.0000000000", "3.0000000000"}
			case "admin-anchor":
				anchor := at.Add(-time.Microsecond)
				starts[0], starts[1] = &anchor, &anchor
				wantStarts[0], wantStarts[1] = anchor, anchor
			case "monthly-before-720h":
				start := at.Add(-720*time.Hour + time.Microsecond)
				starts[2], wantStarts[2] = &start, start
			case "monthly-at-720h", "monthly-after-720h":
				start := at.Add(-720 * time.Hour)
				if name == "monthly-after-720h" {
					start = start.Add(-time.Microsecond)
				}
				starts[2] = &start
				wantStarts[2], wantUsage[2] = at, "0.2500000000"
			case "calendar-month-only":
				start := at.AddDate(0, 0, -1)
				starts[2], wantStarts[2] = &start, start
			case "decimal-exact":
				binding.CostUSD = 0.2
				usages = [3]string{"0.1000000001", "0.2000000002", "0.3000000003"}
				wantUsage = [3]string{"0.3000000001", "0.4000000002", "0.5000000003"}
			}
			_, err := f.db.Exec(`UPDATE user_platform_quotas SET daily_usage_usd=$2::numeric,weekly_usage_usd=$3::numeric,monthly_usage_usd=$4::numeric,
				daily_window_start=$5,weekly_window_start=$6,monthly_window_start=$7 WHERE id=$1`, rec.QuotaRowID, usages[0], usages[1], usages[2], starts[0], starts[1], starts[2])
			require.NoError(t, err)
			before := boundQuotaRecordJSON(t, f.db, rec.QuotaRowID)
			result, err := boundQuotaWriter(t, f.repo).IncrementBoundUsage(context.Background(), binding)
			require.NoError(t, err)
			require.Equal(t, mask, result.AppliedWindows)
			wantStatus := service.QuotaIncrementPartial
			if mask == service.QuotaWindowAll {
				wantStatus = service.QuotaIncrementApplied
			}
			if mask == 0 {
				wantStatus = service.QuotaIncrementObsolete
			}
			require.Equal(t, wantStatus, result.Status)
			var actualUsage [3]string
			var actualStart [3]time.Time
			require.NoError(t, f.db.QueryRow(`SELECT daily_usage_usd,weekly_usage_usd,monthly_usage_usd,daily_window_start,weekly_window_start,monthly_window_start
				FROM user_platform_quotas WHERE id=$1`, rec.QuotaRowID).Scan(&actualUsage[0], &actualUsage[1], &actualUsage[2], &actualStart[0], &actualStart[1], &actualStart[2]))
			require.Equal(t, wantUsage, actualUsage)
			for i := range actualStart {
				require.True(t, actualStart[i].Equal(wantStarts[i]), "window %d got %v want %v", i, actualStart[i], wantStarts[i])
			}
			require.Equal(t, rec.ControlGeneration, f.read(t, service.PlatformOpenAI).ControlGeneration)
			if mask == 0 {
				require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
			}
		})
	}
}

func TestUserPlatformQuotaBoundPostgresUpdateFailureAndOuterRollbackPreserveEveryWindow(t *testing.T) {
	for _, name := range []string{"update-error", "outer-rollback"} {
		t.Run(name, func(t *testing.T) {
			f := quotaSnapshotPostgresFixture(t, true)
			rec := f.seed(t, service.PlatformOpenAI)
			binding := boundQuotaBinding(rec, quotaIncrementFixtureTime())
			before := boundQuotaRecordJSON(t, f.db, rec.QuotaRowID)
			if name == "update-error" {
				_, err := f.db.Exec(`CREATE FUNCTION bound_quota_fixture_failure() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN RAISE EXCEPTION 'bound quota fixture failure'; END; $$;
					CREATE TRIGGER bound_quota_fixture_failure BEFORE UPDATE ON user_platform_quotas
					FOR EACH ROW EXECUTE FUNCTION bound_quota_fixture_failure()`)
				require.NoError(t, err)
				result, err := boundQuotaWriter(t, f.repo).IncrementBoundUsage(context.Background(), binding)
				require.Error(t, err)
				require.Empty(t, result.Status)
				require.Zero(t, result.AppliedWindows)
			} else {
				tx, err := f.client.Tx(context.Background())
				require.NoError(t, err)
				ctx := dbent.NewTxContext(context.Background(), tx)
				result, err := boundQuotaWriter(t, f.repo).IncrementBoundUsage(ctx, binding)
				require.NoError(t, err)
				require.Equal(t, service.QuotaIncrementApplied, result.Status)
				require.NoError(t, tx.Rollback())
			}
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
		})
	}
}

func TestUserPlatformQuotaBoundPostgresResetAndBoundIncrementLockOrders(t *testing.T) {
	for _, adminFirst := range []bool{false, true} {
		name := "increment-first"
		if adminFirst {
			name = "admin-first"
		}
		t.Run(name, func(t *testing.T) {
			f := quotaSnapshotPostgresFixture(t, true)
			rec := f.seed(t, service.PlatformOpenAI)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tx, err := f.client.Tx(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			txCtx := dbent.NewTxContext(ctx, tx)
			pid := quotaSnapshotTransactionPID(t, txCtx, tx.Client())
			binding := boundQuotaBinding(rec, time.Now().UTC())
			writer := boundQuotaWriter(t, f.repo)
			if adminFirst {
				require.NoError(t, f.repo.ResetExpiredWindow(txCtx, f.userID, service.PlatformOpenAI, "daily", time.Now().UTC()))
			} else {
				result, err := writer.IncrementBoundUsage(txCtx, binding)
				require.NoError(t, err)
				require.Equal(t, service.QuotaIncrementApplied, result.Status)
			}
			done := make(chan error, 1)
			var delayed service.UserPlatformQuotaBoundIncrementResult
			go func() {
				if adminFirst {
					var err error
					delayed, err = writer.IncrementBoundUsage(ctx, binding)
					done <- err
				} else {
					done <- f.repo.ResetExpiredWindow(ctx, f.userID, service.PlatformOpenAI, "daily", time.Now().UTC())
				}
			}()
			quotaSnapshotWaitForBlock(t, ctx, f.db, pid, done)
			require.NoError(t, tx.Commit())
			quotaSnapshotAwait(t, ctx, done)
			after := f.read(t, service.PlatformOpenAI)
			require.Equal(t, rec.ControlGeneration+1, after.ControlGeneration)
			require.Zero(t, after.DailyUsageUSD)
			if adminFirst {
				require.Equal(t, service.QuotaIncrementSuperseded, delayed.Status)
				require.Zero(t, after.WeeklyUsageUSD)
				require.Zero(t, after.MonthlyUsageUSD)
			} else {
				require.Equal(t, 0.25, after.WeeklyUsageUSD)
				require.Equal(t, 0.25, after.MonthlyUsageUSD)
			}
		})
	}
}

func TestUserPlatformQuotaBoundPostgresApplyObservationAndResetLockOrders(t *testing.T) {
	for _, adminFirst := range []bool{false, true} {
		name := "apply-first"
		if adminFirst {
			name = "admin-first"
		}
		t.Run(name, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			limit := 100.0
			quotaID := settlementPlatformQuotaRow(t, f, &limit)
			repo := NewUserPlatformQuotaRepository(f.client)
			original, err := repo.GetByUserPlatform(context.Background(), f.session.UserID, service.PlatformOpenAI)
			require.NoError(t, err)
			require.NotNil(t, original)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := ordinaryPlatformQuotaCommand(t, f, "lock-order", 0.25)
			applyDone := make(chan error, 1)
			var result *service.UsageBillingApplyResult
			if adminFirst {
				tx, err := f.client.Tx(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				txCtx := dbent.NewTxContext(ctx, tx)
				pid := quotaSnapshotTransactionPID(t, txCtx, tx.Client())
				require.NoError(t, repo.ResetExpiredWindow(txCtx, f.session.UserID, service.PlatformOpenAI, "daily", time.Now().UTC()))
				go func() { var err error; result, err = f.billing.Apply(ctx, cmd); applyDone <- err }()
				quotaSnapshotWaitForBlock(t, ctx, f.db, pid, applyDone)
				require.NoError(t, tx.Commit())
				quotaSnapshotAwait(t, ctx, applyDone)
				require.NotNil(t, result.OrdinaryPlatformQuotaReceipt)
				require.Equal(t, original.ControlGeneration+1, result.OrdinaryPlatformQuotaReceipt.ControlGeneration)
				var cutoff time.Time
				require.NoError(t, f.db.QueryRow(`SELECT daily_reset_at FROM user_platform_quota_client_resets WHERE quota_row_id=$1`, quotaID).Scan(&cutoff))
				require.False(t, result.OrdinaryPlatformQuotaReceipt.CapturedAt.Before(cutoff))
			} else {
				holder, err := f.db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer holder.Rollback()
				var holderPID int
				require.NoError(t, holder.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holderPID))
				lockID := time.Now().UnixNano()
				_, err = holder.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, lockID)
				require.NoError(t, err)
				_, err = f.db.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION bound_apply_fixture_wait() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN PERFORM pg_advisory_xact_lock(%d); RETURN NEW; END; $$;
					CREATE TRIGGER bound_apply_fixture_wait BEFORE INSERT ON usage_billing_ordinary_platform_quota_journal
					FOR EACH ROW EXECUTE FUNCTION bound_apply_fixture_wait()`, lockID))
				require.NoError(t, err)
				go func() { var err error; result, err = f.billing.Apply(ctx, cmd); applyDone <- err }()
				quotaSnapshotWaitForBlock(t, ctx, f.db, holderPID, applyDone)
				var applyPID int
				require.NoError(t, f.db.QueryRowContext(ctx, `SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))`, holderPID).Scan(&applyPID))
				resetDone := make(chan error, 1)
				go func() {
					resetDone <- repo.ResetExpiredWindow(ctx, f.session.UserID, service.PlatformOpenAI, "daily", time.Now().UTC())
				}()
				quotaSnapshotWaitForBlock(t, ctx, f.db, applyPID, resetDone)
				require.NoError(t, holder.Commit())
				quotaSnapshotAwait(t, ctx, applyDone)
				quotaSnapshotAwait(t, ctx, resetDone)
				require.NotNil(t, result.OrdinaryPlatformQuotaReceipt)
				require.Equal(t, original.ControlGeneration, result.OrdinaryPlatformQuotaReceipt.ControlGeneration)
				binding, err := result.OrdinaryPlatformQuotaReceipt.UsageBinding()
				require.NoError(t, err)
				bound, err := boundQuotaWriter(t, repo).IncrementBoundUsage(ctx, binding)
				require.NoError(t, err)
				require.Equal(t, service.QuotaIncrementSuperseded, bound.Status)
			}
			require.True(t, result.Applied)
			require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformReceipt(cmd, result.OrdinaryPlatformQuotaReceipt))
			balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, "99.75000000", balance)
			require.Equal(t, "0.25000000", used)
		})
	}
}

func TestUserPlatformQuotaBoundPostgresRealRedisPausedReceiptCannotChangeNewControls(t *testing.T) {
	for _, mutation := range []string{"daily-reset", "weekly-reset", "monthly-reset", "limit-edit", "soft-recreate", "physical-recreate"} {
		t.Run(mutation, func(t *testing.T) {
			cache, uniqueID := realQuotaCacheCASFixture(t)
			f := ordinaryPlatformQuotaFixture(t)
			limit := 100.0
			quotaID := settlementPlatformQuotaRow(t, f, &limit)
			repo := NewUserPlatformQuotaRepository(f.client)
			cmd := ordinaryPlatformQuotaCommand(t, f, "redis-paused-finalize", 0.25)
			applied, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, applied.Applied)
			require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformReceipt(cmd, applied.OrdinaryPlatformQuotaReceipt))
			binding, err := applied.OrdinaryPlatformQuotaReceipt.UsageBinding()
			require.NoError(t, err)
			mutateBoundQuotaControl(t, f, repo, quotaID, mutation)
			current, err := repo.GetByUserPlatform(context.Background(), binding.UserID, binding.Platform)
			require.NoError(t, err)
			require.NotNil(t, current)
			before := boundQuotaRecordJSON(t, f.db, current.QuotaRowID)
			key := userPlatformQuotaCacheKey(binding.UserID, binding.Platform)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// This server belongs to the isolated lane. Assert ownership before
			// using the fixture's real user ID and delete only this test's keys.
			exists, err := cache.rdb.Exists(ctx, key).Result()
			require.NoError(t, err)
			require.Zero(t, exists)
			t.Cleanup(func() { _ = cache.rdb.Del(context.Background(), key).Err() })
			for _, flusher := range []bool{false, true} {
				for _, oldCache := range []bool{false, true} {
					label := fmt.Sprintf("flusher=%v/old-cache=%v", flusher, oldCache)
					entry := quotaIncrementEntryAt(binding.CapturedAt)
					entry.QuotaRowID, entry.ControlGeneration = current.QuotaRowID, current.ControlGeneration
					if oldCache {
						entry.QuotaRowID, entry.ControlGeneration = binding.QuotaRowID, binding.ControlGeneration
					}
					entry.DailyLimitUSD, entry.WeeklyLimitUSD, entry.MonthlyLimitUSD = &limit, nil, nil
					entry.DailyUsageUSD, entry.WeeklyUsageUSD, entry.MonthlyUsageUSD = 0, 0, 0
					require.NoError(t, cache.SetUserPlatformQuotaCache(ctx, binding.UserID, binding.Platform, entry, time.Minute), label)
					cacheBefore := quotaIncrementRaw(t, cache, binding.UserID)
					dirtyKey := key + ":bound-test-dirty:" + strconv.FormatInt(uniqueID, 10) + ":" + label
					t.Cleanup(func() { _ = cache.rdb.Del(context.Background(), dirtyKey).Err() })
					cached, err := cache.incrementBoundUserPlatformQuotaUsageCache(ctx, binding, 2*time.Minute, flusher, dirtyKey)
					require.NoError(t, err, label)
					if oldCache {
						require.Equal(t, service.QuotaIncrementApplied, cached.Status, label)
						require.Equal(t, service.QuotaWindowAll, cached.AppliedWindows, label)
					} else {
						require.Equal(t, service.QuotaIncrementSuperseded, cached.Status, label)
						require.Equal(t, cacheBefore, quotaIncrementRaw(t, cache, binding.UserID), label)
					}
					if flusher {
						if oldCache {
							require.True(t, cache.rdb.SIsMember(ctx, dirtyKey, userPlatformQuotaDirtyMember(binding.UserID, binding.Platform)).Val(), label)
							cachedEntry, found, err := cache.GetUserPlatformQuotaCache(ctx, binding.UserID, binding.Platform)
							require.NoError(t, err)
							require.True(t, found)
							require.NoError(t, repo.BatchSnapshotUsage(ctx, []UserPlatformQuotaSnapshot{{QuotaRowID: cachedEntry.QuotaRowID, ControlGeneration: cachedEntry.ControlGeneration,
								UserID: binding.UserID, Platform: binding.Platform, DailyUsageUSD: cachedEntry.DailyUsageUSD, WeeklyUsageUSD: cachedEntry.WeeklyUsageUSD,
								MonthlyUsageUSD: cachedEntry.MonthlyUsageUSD, DailyWindowStart: *cachedEntry.DailyWindowStart, WeeklyWindowStart: *cachedEntry.WeeklyWindowStart, MonthlyWindowStart: *cachedEntry.MonthlyWindowStart}}, time.Now().UTC()), label)
						} else {
							require.Zero(t, cache.rdb.Exists(ctx, dirtyKey).Val(), label)
						}
					} else {
						bound, err := boundQuotaWriter(t, repo).IncrementBoundUsage(ctx, binding)
						require.NoError(t, err, label)
						require.Equal(t, service.QuotaIncrementSuperseded, bound.Status, label)
						require.Zero(t, cache.rdb.Exists(ctx, dirtyKey).Val(), label)
					}
					require.Equal(t, before, boundQuotaRecordJSON(t, f.db, current.QuotaRowID), label)
				}
			}
			retry, err := f.billing.Apply(ctx, cmd)
			require.NoError(t, err)
			require.False(t, retry.Applied)
			require.Nil(t, retry.OrdinaryPlatformQuotaReceipt)
			balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, "99.75000000", balance)
			require.Equal(t, "0.25000000", used)
		})
	}
}
