//go:build unit

package repository

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func quotaWriterAuthorityUninitializedFixture(t *testing.T) *settlementEventPGFixture {
	t.Helper()
	f := ordinaryPlatformQuotaFixture(t)
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "263_user_platform_quota_writer_authority.sql"))
	require.NoError(t, err)
	_, err = f.db.Exec(string(raw))
	require.NoError(t, err)
	return f
}

// Include the tuple identity as well as all columns: an equal-value UPDATE
// would leave row_to_json unchanged while still rewriting the authority.
func quotaWriterAuthorityDatabaseJSON(t *testing.T, db *sql.DB) string {
	t.Helper()
	var value string
	require.NoError(t, db.QueryRow(`SELECT COALESCE(jsonb_agg(jsonb_build_object(
		'row',to_jsonb(a),'xmin',a.xmin::text,'ctid',a.ctid::text)),'[]'::jsonb)::text
		FROM user_platform_quota_writer_authority a`).Scan(&value))
	return value
}

func quotaWriterAuthorityZeroQuota(t *testing.T, f *settlementEventPGFixture, authority service.UserPlatformQuotaWriterAuthority) (UserPlatformQuotaRepository, *UserPlatformQuotaRecord) {
	t.Helper()
	repo := NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, authority)
	limit := 100.0
	require.NoError(t, repo.UpsertForUser(context.Background(), f.session.UserID, []UserPlatformQuotaRecord{{
		UserID: f.session.UserID, Platform: service.PlatformOpenAI, DailyLimitUSD: &limit,
		WeeklyLimitUSD: &limit, MonthlyLimitUSD: &limit,
	}}))
	rec, err := repo.GetByUserPlatform(context.Background(), f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Zero(t, rec.DailyUsageUSD)
	require.Nil(t, rec.DailyWindowStart)
	return repo, rec
}

func TestQuotaWriterAuthorityPostgresConcurrentInitializeAndRestartPreservePermanentRow(t *testing.T) {
	f := quotaWriterAuthorityUninitializedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type result struct {
		authority service.UserPlatformQuotaWriterAuthority
		err       error
	}
	const workers = 8
	results := make(chan result, workers)
	var ready sync.WaitGroup
	ready.Add(workers)
	start := make(chan struct{})
	for range workers {
		go func() {
			ready.Done()
			<-start
			authority, err := NewUserPlatformQuotaWriterAuthorityRepository(f.db).InitializeUserPlatformQuotaWriterAuthority(ctx, service.UserPlatformQuotaWriterModeDB)
			results <- result{authority: authority, err: err}
		}()
	}
	ready.Wait()
	close(start)
	want := service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeDB, Epoch: 1}
	for range workers {
		got := <-results
		require.NoError(t, got.err)
		require.Equal(t, want, got.authority)
	}
	before := quotaWriterAuthorityDatabaseJSON(t, f.db)
	// Repeat the concurrent startup against an existing row so the tuple
	// identity proves that conflict handling does not perform a hidden UPDATE.
	for range workers {
		go func() {
			authority, err := NewUserPlatformQuotaWriterAuthorityRepository(f.db).InitializeUserPlatformQuotaWriterAuthority(ctx, service.UserPlatformQuotaWriterModeDB)
			results <- result{authority: authority, err: err}
		}()
	}
	for range workers {
		got := <-results
		require.NoError(t, got.err)
		require.Equal(t, want, got.authority)
	}
	require.Equal(t, before, quotaWriterAuthorityDatabaseJSON(t, f.db))
	for range 3 {
		authority, err := NewUserPlatformQuotaWriterAuthorityRepository(f.db).InitializeUserPlatformQuotaWriterAuthority(ctx, service.UserPlatformQuotaWriterModeDB)
		require.NoError(t, err)
		require.Equal(t, want, authority)
		require.Equal(t, before, quotaWriterAuthorityDatabaseJSON(t, f.db))
	}
	// Exercise the real production factory rather than a test-only selection.
	authority, err := ProvideUserPlatformQuotaWriterAuthority(f.db, &config.Config{})
	require.NoError(t, err)
	require.Equal(t, want, authority)
	require.Equal(t, before, quotaWriterAuthorityDatabaseJSON(t, f.db))
}

func TestQuotaWriterAuthorityPostgresSnapshotModeNeverCreatesOrChangesAuthority(t *testing.T) {
	for _, initialized := range []bool{false, true} {
		name := "uninitialized"
		if initialized {
			name = "initialized"
		}
		t.Run(name, func(t *testing.T) {
			f := quotaWriterAuthorityUninitializedFixture(t)
			repo := NewUserPlatformQuotaWriterAuthorityRepository(f.db)
			if initialized {
				_, err := repo.InitializeUserPlatformQuotaWriterAuthority(context.Background(), service.UserPlatformQuotaWriterModeDB)
				require.NoError(t, err)
			}
			before := quotaWriterAuthorityDatabaseJSON(t, f.db)
			got, err := repo.InitializeUserPlatformQuotaWriterAuthority(context.Background(), service.UserPlatformQuotaWriterModeSnapshot)
			require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterModeUnsupported)
			require.Equal(t, service.UserPlatformQuotaWriterAuthority{}, got)
			require.Equal(t, before, quotaWriterAuthorityDatabaseJSON(t, f.db))
			cfg := &config.Config{}
			cfg.Database.UserPlatformQuotaFlusherEnabled = true
			got, err = ProvideUserPlatformQuotaWriterAuthority(f.db, cfg)
			require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterModeUnsupported)
			require.Equal(t, service.UserPlatformQuotaWriterAuthority{}, got)
			require.Equal(t, before, quotaWriterAuthorityDatabaseJSON(t, f.db))
		})
	}
}

func TestQuotaWriterAuthorityPostgresAuthorityCannotBeDeletedOrRotated(t *testing.T) {
	f, _ := quotaWriterAuthorityFixture(t)
	before := quotaWriterAuthorityDatabaseJSON(t, f.db)
	for _, query := range []string{
		`DELETE FROM user_platform_quota_writer_authority WHERE id=1`,
		`UPDATE user_platform_quota_writer_authority SET epoch=epoch+1 WHERE id=1`,
		`UPDATE user_platform_quota_writer_authority SET mode='cache_snapshot_v1' WHERE id=1`,
		`UPDATE user_platform_quota_writer_authority SET created_at=clock_timestamp() WHERE id=1`,
	} {
		_, err := f.db.Exec(query)
		require.Error(t, err)
		require.Equal(t, before, quotaWriterAuthorityDatabaseJSON(t, f.db))
	}
}

func TestQuotaWriterAuthorityPostgresMeteringRejectsMissingAndIncorrectTransactionScope(t *testing.T) {
	f, authority := quotaWriterAuthorityFixture(t)
	_, rec := quotaWriterAuthorityZeroQuota(t, f, authority)
	before := boundQuotaRecordJSON(t, f.db, rec.QuotaRowID)
	for _, tc := range []struct {
		name, mode, epoch, action string
		install                   bool
	}{
		{name: "legacy raw SQL"},
		{name: "snapshot mode", install: true, mode: service.UserPlatformQuotaWriterModeSnapshot, epoch: "1", action: quotaWriterActionOrdinaryDelivery},
		{name: "missing epoch", install: true, mode: authority.Mode, epoch: "", action: quotaWriterActionOrdinaryDelivery},
		{name: "zero epoch", install: true, mode: authority.Mode, epoch: "0", action: quotaWriterActionOrdinaryDelivery},
		{name: "other epoch", install: true, mode: authority.Mode, epoch: "2", action: quotaWriterActionOrdinaryDelivery},
		{name: "missing action", install: true, mode: authority.Mode, epoch: "1", action: ""},
		{name: "unknown action", install: true, mode: authority.Mode, epoch: "1", action: "snapshot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := f.db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			defer tx.Rollback()
			if tc.install {
				_, err = tx.Exec(`SELECT set_config('sub2api.quota_writer_mode',$1,true),
					set_config('sub2api.quota_writer_epoch',$2,true),set_config('sub2api.quota_writer_action',$3,true)`, tc.mode, tc.epoch, tc.action)
				require.NoError(t, err)
			}
			_, err = tx.Exec(`UPDATE user_platform_quotas SET daily_usage_usd=daily_usage_usd+1 WHERE id=$1`, rec.QuotaRowID)
			require.Error(t, err)
			require.NoError(t, tx.Rollback())
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
		})
	}
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET daily_window_start=clock_timestamp() WHERE id=$1`, rec.QuotaRowID)
	require.Error(t, err, "a window-only overwrite is also metering")
	require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
}

func TestQuotaWriterAuthorityPostgresNonzeroInsertNeedsScopeButEmptyConfigurationDoesNot(t *testing.T) {
	f, authority := quotaWriterAuthorityFixture(t)
	for _, column := range []string{"daily_usage_usd", "weekly_usage_usd", "monthly_usage_usd", "daily_window_start", "weekly_window_start", "monthly_window_start"} {
		t.Run(column, func(t *testing.T) {
			value := "1"
			if strings.HasSuffix(column, "window_start") {
				value = "clock_timestamp()"
			}
			_, err := f.db.Exec(`INSERT INTO user_platform_quotas(user_id,platform,daily_limit_usd,`+column+`)
				VALUES($1,'gemini',100,`+value+`)`, f.session.UserID)
			require.Error(t, err)
			var count int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM user_platform_quotas WHERE user_id=$1`, f.session.UserID).Scan(&count))
			require.Zero(t, count)
		})
	}
	_, rec := quotaWriterAuthorityZeroQuota(t, f, authority)
	require.Zero(t, rec.WeeklyUsageUSD)
	require.Zero(t, rec.MonthlyUsageUSD)
	require.Nil(t, rec.WeeklyWindowStart)
	require.Nil(t, rec.MonthlyWindowStart)
}

func TestQuotaWriterAuthorityPostgresTransactionScopeClearsOnCommitRollbackAndPoolReuse(t *testing.T) {
	for _, commit := range []bool{true, false} {
		name := "rollback"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			f, authority := quotaWriterAuthorityFixture(t)
			_, rec := quotaWriterAuthorityZeroQuota(t, f, authority)
			f.db.SetMaxOpenConns(1)
			ctx := context.Background()
			conn, err := f.db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			var firstPID int
			require.NoError(t, conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&firstPID))
			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, lockUserPlatformQuotaWriterAuthority(ctx, tx, authority, quotaWriterActionOrdinaryDelivery))
			_, err = tx.Exec(`UPDATE user_platform_quotas SET daily_usage_usd=0.25 WHERE id=$1`, rec.QuotaRowID)
			require.NoError(t, err)
			if commit {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}
			require.NoError(t, conn.Close())
			// Force checkout of the same pooled server connection and verify that
			// no subsequent request inherits a transaction's authorization.
			reused, err := f.db.Conn(ctx)
			require.NoError(t, err)
			defer reused.Close()
			var nextPID int
			require.NoError(t, reused.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&nextPID))
			require.Equal(t, firstPID, nextPID)
			for _, setting := range []string{"sub2api.quota_writer_mode", "sub2api.quota_writer_epoch", "sub2api.quota_writer_action"} {
				var value sql.NullString
				require.NoError(t, reused.QueryRowContext(ctx, `SELECT current_setting($1,true)`, setting).Scan(&value))
				require.True(t, !value.Valid || value.String == "", setting)
			}
			_, err = reused.ExecContext(ctx, `UPDATE user_platform_quotas SET daily_usage_usd=daily_usage_usd+1 WHERE id=$1`, rec.QuotaRowID)
			require.Error(t, err)
			require.NoError(t, reused.Close())
			got, err := NewUserPlatformQuotaRepository(f.client).GetByUserPlatform(ctx, rec.UserID, rec.Platform)
			require.NoError(t, err)
			if commit {
				require.Equal(t, 0.25, got.DailyUsageUSD)
			} else {
				require.Zero(t, got.DailyUsageUSD)
			}
		})
	}
}

func TestQuotaWriterAuthorityPostgresLimitGenerationAndSoftDeleteRemainControlOnly(t *testing.T) {
	f, authority := quotaWriterAuthorityFixture(t)
	repo, initial := quotaWriterAuthorityZeroQuota(t, f, authority)
	limit := 25.0
	require.NoError(t, repo.UpsertForUser(context.Background(), initial.UserID, []UserPlatformQuotaRecord{{UserID: initial.UserID, Platform: initial.Platform, DailyLimitUSD: &limit}}))
	current, err := repo.GetByUserPlatform(context.Background(), initial.UserID, initial.Platform)
	require.NoError(t, err)
	require.Equal(t, initial.ControlGeneration+1, current.ControlGeneration)
	require.Equal(t, limit, *current.DailyLimitUSD)
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET control_generation=control_generation+1 WHERE id=$1`, current.QuotaRowID)
	require.NoError(t, err)
	updated, err := repo.GetByUserPlatform(context.Background(), initial.UserID, initial.Platform)
	require.NoError(t, err)
	require.Equal(t, current.ControlGeneration+1, updated.ControlGeneration)
	require.Zero(t, updated.DailyUsageUSD)
	require.Nil(t, updated.DailyWindowStart)
	require.NoError(t, repo.UpsertForUser(context.Background(), initial.UserID, nil))
	missing, err := repo.GetByUserPlatform(context.Background(), initial.UserID, initial.Platform)
	require.NoError(t, err)
	require.Nil(t, missing)
	var generation int64
	var deleted sql.NullTime
	require.NoError(t, f.db.QueryRow(`SELECT control_generation,deleted_at FROM user_platform_quotas WHERE id=$1`, initial.QuotaRowID).Scan(&generation, &deleted))
	require.Equal(t, updated.ControlGeneration+1, generation)
	require.True(t, deleted.Valid)
}

func TestQuotaWriterAuthorityPostgresLegacyAndDetachedWritersNeverAcquireScope(t *testing.T) {
	for _, kind := range []string{"legacy constructor", "fixed constructor detached methods", "fixed constructor zero handle"} {
		t.Run(kind, func(t *testing.T) {
			f, authority := quotaWriterAuthorityFixture(t)
			fixed, rec := quotaWriterAuthorityZeroQuota(t, f, authority)
			repo := NewUserPlatformQuotaRepository(f.client)
			if kind == "fixed constructor detached methods" {
				repo = fixed
			}
			if kind == "fixed constructor zero handle" {
				repo = NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, service.UserPlatformQuotaWriterAuthority{})
			}
			before := boundQuotaRecordJSON(t, f.db, rec.QuotaRowID)
			now := time.Now().UTC().Truncate(time.Microsecond)
			binding := service.UserPlatformQuotaUsageBinding{QuotaRowID: rec.QuotaRowID, ControlGeneration: rec.ControlGeneration,
				UserID: rec.UserID, Platform: rec.Platform, CostUSD: 0.25, CapturedAt: now}
			require.NoError(t, service.ValidateUserPlatformQuotaUsageBinding(binding))
			require.Error(t, repo.IncrementUsageWithReset(context.Background(), rec.UserID, rec.Platform, 0.25, now))
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
			_, err := boundQuotaWriter(t, repo).IncrementBoundUsage(context.Background(), binding)
			require.Error(t, err)
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
			require.Error(t, repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{quotaControlSnapshot(rec, 7)}, now))
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
			if kind != "fixed constructor detached methods" {
				err = repo.ResetExpiredWindow(context.Background(), rec.UserID, rec.Platform, "daily", now)
				if kind == "fixed constructor zero handle" {
					require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
				} else {
					require.Error(t, err)
				}
				require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
			}
		})
	}
}

func TestQuotaWriterAuthorityPostgresDetachedPublicPortsCannotBorrowAdminResetScope(t *testing.T) {
	f, authority := quotaWriterAuthorityFixture(t)
	repo, rec := quotaWriterAuthorityZeroQuota(t, f, authority)
	ctx := context.Background()
	tx, err := f.client.Tx(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	txCtx := dbent.NewTxContext(ctx, tx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, repo.ResetExpiredWindow(txCtx, rec.UserID, rec.Platform, "daily", now))
	reset, err := repo.GetByUserPlatform(txCtx, rec.UserID, rec.Platform)
	require.NoError(t, err)
	require.Equal(t, rec.ControlGeneration+1, reset.ControlGeneration)
	rows, err := tx.Client().QueryContext(txCtx, `SELECT current_setting('sub2api.quota_writer_action',true)`)
	require.NoError(t, err)
	require.True(t, rows.Next())
	var action string
	require.NoError(t, rows.Scan(&action))
	require.NoError(t, rows.Close())
	require.Equal(t, quotaWriterActionAdminReset, action)
	binding := service.UserPlatformQuotaUsageBinding{QuotaRowID: reset.QuotaRowID, ControlGeneration: reset.ControlGeneration,
		UserID: reset.UserID, Platform: reset.Platform, CostUSD: 0.25, CapturedAt: now}
	for _, attempt := range []func() error{
		func() error { return repo.IncrementUsageWithReset(txCtx, rec.UserID, rec.Platform, 0.25, now) },
		func() error { _, err := boundQuotaWriter(t, repo).IncrementBoundUsage(txCtx, binding); return err },
		func() error {
			return repo.BatchSnapshotUsage(txCtx, []UserPlatformQuotaSnapshot{quotaControlSnapshot(reset, 7)}, now)
		},
	} {
		require.ErrorIs(t, attempt(), service.ErrUserPlatformQuotaWriterModeUnsupported)
		unchanged, err := repo.GetByUserPlatform(txCtx, rec.UserID, rec.Platform)
		require.NoError(t, err, "a refused public port must leave the ambient transaction healthy")
		require.Equal(t, reset, unchanged)
	}
	require.NoError(t, tx.Commit(), "the independently authorized reset still commits")
	committed, err := repo.GetByUserPlatform(ctx, rec.UserID, rec.Platform)
	require.NoError(t, err)
	require.Equal(t, reset, committed)
}

func TestQuotaWriterAuthorityPostgresMissingOrMismatchedAuthorityLeavesAdminResetUnchanged(t *testing.T) {
	for _, kind := range []string{"missing authority", "other epoch", "snapshot handle"} {
		t.Run(kind, func(t *testing.T) {
			f := quotaWriterAuthorityUninitializedFixture(t)
			authority := service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeDB, Epoch: 1}
			if kind != "missing authority" {
				var err error
				authority, err = NewUserPlatformQuotaWriterAuthorityRepository(f.db).InitializeUserPlatformQuotaWriterAuthority(context.Background(), authority.Mode)
				require.NoError(t, err)
			}
			_, rec := quotaWriterAuthorityZeroQuota(t, f, authority)
			before := boundQuotaRecordJSON(t, f.db, rec.QuotaRowID)
			wantErr := service.ErrUserPlatformQuotaWriterUnavailable
			if kind == "other epoch" {
				authority.Epoch++
				wantErr = service.ErrUserPlatformQuotaWriterMismatch
			}
			if kind == "snapshot handle" {
				authority.Mode = service.UserPlatformQuotaWriterModeSnapshot
				wantErr = service.ErrUserPlatformQuotaWriterModeUnsupported
			}
			repo := NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, authority)
			err := repo.ResetExpiredWindow(context.Background(), rec.UserID, rec.Platform, "daily", time.Now().UTC())
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, rec.QuotaRowID))
			var reset sql.NullTime
			require.NoError(t, f.db.QueryRow(`SELECT daily_reset_at FROM user_platform_quota_client_resets WHERE quota_row_id=$1`, rec.QuotaRowID).Scan(&reset))
			require.False(t, reset.Valid)
		})
	}
}

func TestQuotaWriterAuthorityPostgresAdminResetLocksAuthorityBeforeQuota(t *testing.T) {
	for _, holdAuthority := range []bool{true, false} {
		name := "authority already locked"
		if !holdAuthority {
			name = "quota already locked"
		}
		t.Run(name, func(t *testing.T) {
			f, authority := quotaWriterAuthorityFixture(t)
			repo, rec := quotaWriterAuthorityZeroQuota(t, f, authority)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			holder, err := f.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer holder.Rollback()
			var pid, lockedID int
			require.NoError(t, holder.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
			if holdAuthority {
				require.NoError(t, holder.QueryRowContext(ctx, `SELECT id FROM user_platform_quota_writer_authority WHERE id=1 FOR UPDATE`).Scan(&lockedID))
			} else {
				require.NoError(t, holder.QueryRowContext(ctx, `SELECT id FROM user_platform_quotas WHERE id=$1 FOR UPDATE`, rec.QuotaRowID).Scan(&lockedID))
			}
			done := make(chan error, 1)
			go func() { done <- repo.ResetExpiredWindow(ctx, rec.UserID, rec.Platform, "daily", time.Now().UTC()) }()
			quotaSnapshotWaitForBlock(t, ctx, f.db, pid, done)
			observer, err := f.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer observer.Rollback()
			if holdAuthority {
				require.NoError(t, observer.QueryRowContext(ctx, `SELECT id FROM user_platform_quotas WHERE id=$1 FOR UPDATE NOWAIT`, rec.QuotaRowID).Scan(&lockedID), "reset waits on authority without holding quota")
			} else {
				err = observer.QueryRowContext(ctx, `SELECT id FROM user_platform_quota_writer_authority WHERE id=1 FOR UPDATE NOWAIT`).Scan(&lockedID)
				require.Error(t, err, "reset keeps authority SHARE while waiting for quota")
			}
			require.NoError(t, observer.Rollback())
			require.NoError(t, holder.Commit())
			quotaSnapshotAwait(t, ctx, done)
			current, err := repo.GetByUserPlatform(context.Background(), rec.UserID, rec.Platform)
			require.NoError(t, err)
			require.Equal(t, rec.ControlGeneration+1, current.ControlGeneration)
			require.Zero(t, current.DailyUsageUSD)
			require.NotNil(t, current.DailyWindowStart)
		})
	}
}

func TestQuotaWriterAuthorityPostgresAdminResetAndScopeRollbackTogether(t *testing.T) {
	f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
	_, err := ordinaryQuotaDeliveryWriter(t, repo).ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.NoError(t, err)
	before := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
	var resetBefore string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(r)::text FROM user_platform_quota_client_resets r WHERE quota_row_id=$1`, receipt.QuotaRowID).Scan(&resetBefore))
	tx, err := f.client.Tx(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	txCtx := dbent.NewTxContext(context.Background(), tx)
	require.NoError(t, repo.ResetExpiredWindow(txCtx, cmd.UserID, receipt.Intent.Platform, "daily", time.Now().UTC()))
	inTx, err := repo.GetByUserPlatform(txCtx, cmd.UserID, receipt.Intent.Platform)
	require.NoError(t, err)
	require.Zero(t, inTx.DailyUsageUSD)
	require.Equal(t, receipt.ControlGeneration+1, inTx.ControlGeneration)
	require.NoError(t, tx.Rollback())
	require.Equal(t, before, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
	var resetAfter string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(r)::text FROM user_platform_quota_client_resets r WHERE quota_row_id=$1`, receipt.QuotaRowID).Scan(&resetAfter))
	require.Equal(t, resetBefore, resetAfter)
}
