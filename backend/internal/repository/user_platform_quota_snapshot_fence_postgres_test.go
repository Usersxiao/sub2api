//go:build unit

package repository

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/stretchr/testify/require"
)

type quotaSnapshotPGFixture struct {
	db     *sql.DB
	client *dbent.Client
	repo   UserPlatformQuotaRepository
	userID int64
}

func quotaSnapshotPostgresFixture(t *testing.T, migrate bool) *quotaSnapshotPGFixture {
	t.Helper()
	db := authSessionIsolatedDB(t)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	_, err := db.Exec(`CREATE TABLE users(id BIGSERIAL PRIMARY KEY)`)
	require.NoError(t, err)
	var userID int64
	require.NoError(t, db.QueryRow(`INSERT INTO users DEFAULT VALUES RETURNING id`).Scan(&userID))
	f := &quotaSnapshotPGFixture{db: db, client: client, repo: NewUserPlatformQuotaRepository(client), userID: userID}
	f.migrate(t, "142_user_platform_quotas.sql")
	f.migrate(t, "254_user_platform_quota_client_resets.sql")
	if migrate {
		f.migrate(t, "260_user_platform_quota_snapshot_fence.sql")
	}
	return f
}

func (f *quotaSnapshotPGFixture) migrate(t *testing.T, name string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
	require.NoError(t, err)
	_, err = f.db.Exec(string(raw))
	require.NoError(t, err, name)
}

func (f *quotaSnapshotPGFixture) seed(t *testing.T, platform string) *UserPlatformQuotaRecord {
	t.Helper()
	limit := 100.0
	require.NoError(t, f.repo.BulkInsertInitial(context.Background(), []UserPlatformQuotaRecord{{UserID: f.userID, Platform: platform, DailyLimitUSD: &limit}}))
	return f.read(t, platform)
}

func (f *quotaSnapshotPGFixture) read(t *testing.T, platform string) *UserPlatformQuotaRecord {
	t.Helper()
	rec, err := f.repo.GetByUserPlatform(context.Background(), f.userID, platform)
	require.NoError(t, err)
	require.NotNil(t, rec)
	return rec
}

func quotaControlSnapshot(rec *UserPlatformQuotaRecord, value float64) UserPlatformQuotaSnapshot {
	start := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	return UserPlatformQuotaSnapshot{QuotaRowID: rec.QuotaRowID, ControlGeneration: rec.ControlGeneration,
		UserID: rec.UserID, Platform: rec.Platform, DailyUsageUSD: value, WeeklyUsageUSD: value + 1, MonthlyUsageUSD: value + 2,
		DailyWindowStart: start, WeeklyWindowStart: start, MonthlyWindowStart: start}
}

func TestUserPlatformQuotaSnapshotFencePostgresMigrationPreservesExistingFactsAndReadsTokens(t *testing.T) {
	f := quotaSnapshotPostgresFixture(t, false)
	var id int64
	start := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, f.db.QueryRow(`INSERT INTO user_platform_quotas(user_id,platform,daily_limit_usd,
		daily_usage_usd,weekly_usage_usd,monthly_usage_usd,daily_window_start,created_at,updated_at)
		VALUES($1,'openai',0,7,8,9,$2,$2,$2) RETURNING id`, f.userID, start).Scan(&id))
	var before string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(s)::text FROM user_platform_quota_client_resets s WHERE quota_row_id=$1`, id).Scan(&before))
	f.migrate(t, "260_user_platform_quota_snapshot_fence.sql")
	f.migrate(t, "260_user_platform_quota_snapshot_fence.sql")
	rec := f.read(t, "openai")
	require.Equal(t, id, rec.QuotaRowID)
	require.Equal(t, int64(1), rec.ControlGeneration)
	require.Equal(t, float64(7), rec.DailyUsageUSD)
	require.Equal(t, float64(8), rec.WeeklyUsageUSD)
	require.Equal(t, float64(9), rec.MonthlyUsageUSD)
	require.Equal(t, float64(0), *rec.DailyLimitUSD)
	require.True(t, rec.DailyWindowStart.Equal(start))
	list, err := f.repo.ListByUser(context.Background(), f.userID)
	require.NoError(t, err)
	require.Equal(t, []UserPlatformQuotaRecord{*rec}, list)
	var after string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(s)::text FROM user_platform_quota_client_resets s WHERE quota_row_id=$1`, id).Scan(&after))
	require.Equal(t, before, after, "generation seeding invents no reset or baseline evidence")
	require.Equal(t, int64(1), f.seed(t, "gemini").ControlGeneration)
}

func TestUserPlatformQuotaSnapshotFencePostgresEveryAdminWindowRejectsPriorGeneration(t *testing.T) {
	for _, window := range []string{"daily", "weekly", "monthly"} {
		t.Run(window, func(t *testing.T) {
			f := quotaSnapshotPostgresFixture(t, true)
			before := f.seed(t, "openai")
			snapshot := quotaControlSnapshot(before, 7)
			require.NoError(t, f.repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{snapshot}, time.Now()))
			require.Equal(t, before.ControlGeneration, f.read(t, "openai").ControlGeneration, "ordinary snapshot is not a control mutation")
			newStart := time.Now().UTC().Truncate(time.Microsecond)
			require.NoError(t, f.repo.ResetExpiredWindow(context.Background(), f.userID, "openai", window, newStart))
			after := f.read(t, "openai")
			require.Equal(t, before.ControlGeneration+1, after.ControlGeneration)
			require.NoError(t, f.repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{snapshot}, time.Now()))
			require.Equal(t, after, f.read(t, "openai"), "a late old-generation snapshot changes no window")
			usage := map[string]float64{"daily": after.DailyUsageUSD, "weekly": after.WeeklyUsageUSD, "monthly": after.MonthlyUsageUSD}
			require.Zero(t, usage[window])
			fresh := quotaControlSnapshot(after, 2)
			require.NoError(t, f.repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{fresh}, time.Now()))
			require.Equal(t, float64(2), f.read(t, "openai").DailyUsageUSD)
			// Reset commands still fence when both requested legacy values and the
			// protected cutoff happen to repeat unchanged.
			_, err := f.db.Exec(`UPDATE user_platform_quota_client_resets SET `+window+`_reset_at=`+window+`_reset_at WHERE quota_row_id=$1`, after.QuotaRowID)
			require.NoError(t, err)
			require.Equal(t, after.ControlGeneration+1, f.read(t, "openai").ControlGeneration)
		})
	}
}

func TestUserPlatformQuotaSnapshotFencePostgresLimitsDefaultsAndScopeAreControlMutations(t *testing.T) {
	f := quotaSnapshotPostgresFixture(t, true)
	ctx := context.Background()
	before := f.seed(t, "openai")
	limit, weekly := 101.0, 500.0
	// A real limits edit advances, an equal-value replacement does not.
	for i := 0; i < 2; i++ {
		require.NoError(t, f.repo.UpsertForUser(ctx, f.userID, []UserPlatformQuotaRecord{{Platform: "openai", DailyLimitUSD: &limit}}))
		require.Equal(t, before.ControlGeneration+1, f.read(t, "openai").ControlGeneration)
	}
	require.NoError(t, f.repo.BulkInsertInitial(ctx, []UserPlatformQuotaRecord{{UserID: f.userID, Platform: "openai", DailyLimitUSD: &weekly, WeeklyLimitUSD: &weekly}}))
	after := f.read(t, "openai")
	require.Equal(t, before.ControlGeneration+2, after.ControlGeneration)
	require.Equal(t, limit, *after.DailyLimitUSD, "default initialization retains existing non-null limit")
	require.NoError(t, f.repo.IncrementUsageWithReset(ctx, f.userID, "openai", 1, time.Now()))
	require.Equal(t, after.ControlGeneration, f.read(t, "openai").ControlGeneration, "natural window rotation is not an admin reset")
	// Moving a surviving row away and back never revives a previously issued token.
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET platform='gemini' WHERE id=$1`, after.QuotaRowID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET platform='openai' WHERE id=$1`, after.QuotaRowID)
	require.NoError(t, err)
	var otherID int64
	require.NoError(t, f.db.QueryRow(`INSERT INTO users DEFAULT VALUES RETURNING id`).Scan(&otherID))
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET user_id=$2 WHERE id=$1`, after.QuotaRowID, otherID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET user_id=$2 WHERE id=$1`, after.QuotaRowID, f.userID)
	require.NoError(t, err)
	require.Equal(t, after.ControlGeneration+4, f.read(t, "openai").ControlGeneration)
	require.NoError(t, f.repo.BatchSnapshotUsage(ctx, []UserPlatformQuotaSnapshot{quotaControlSnapshot(after, 90)}, time.Now()))
	require.Equal(t, float64(1), f.read(t, "openai").DailyUsageUSD)
}

func TestUserPlatformQuotaSnapshotFencePostgresIncarnationMissingTokensAndOtherScopes(t *testing.T) {
	f := quotaSnapshotPostgresFixture(t, true)
	ctx := context.Background()
	old := f.seed(t, "openai")
	gemini := f.seed(t, "gemini")
	limit := 100.0
	require.NoError(t, f.repo.UpsertForUser(ctx, f.userID, []UserPlatformQuotaRecord{{Platform: "gemini", DailyLimitUSD: &limit}}))
	_, err := f.db.Exec(`DELETE FROM user_platform_quotas WHERE id=$1`, old.QuotaRowID)
	require.NoError(t, err)
	current := f.seed(t, "openai")
	require.NotEqual(t, old.QuotaRowID, current.QuotaRowID)
	require.Equal(t, old.ControlGeneration, current.ControlGeneration, "row id disambiguates a fresh generation=1")
	missing := quotaControlSnapshot(current, 91)
	missing.QuotaRowID = 0
	missingGeneration := quotaControlSnapshot(current, 92)
	missingGeneration.ControlGeneration = 0
	wrongScope := quotaControlSnapshot(current, 93)
	wrongScope.Platform = "gemini"
	negative := quotaControlSnapshot(current, 94)
	negative.ControlGeneration = -1
	require.NoError(t, f.repo.BatchSnapshotUsage(ctx, []UserPlatformQuotaSnapshot{quotaControlSnapshot(old, 90), missing, missingGeneration, wrongScope, negative, quotaControlSnapshot(gemini, 3)}, time.Now()))
	require.Zero(t, f.read(t, "openai").DailyUsageUSD)
	require.Equal(t, float64(3), f.read(t, "gemini").DailyUsageUSD, "stale controls do not poison unrelated valid snapshots")
	var otherID int64
	require.NoError(t, f.db.QueryRow(`INSERT INTO users DEFAULT VALUES RETURNING id`).Scan(&otherID))
	foreign := quotaControlSnapshot(current, 95)
	foreign.UserID = otherID
	require.NoError(t, f.repo.BatchSnapshotUsage(ctx, []UserPlatformQuotaSnapshot{foreign, quotaControlSnapshot(current, 4)}, time.Now()))
	require.Equal(t, float64(4), f.read(t, "openai").DailyUsageUSD)
}

func quotaSnapshotTransactionPID(t *testing.T, ctx context.Context, client *dbent.Client) int {
	t.Helper()
	rows, err := client.QueryContext(ctx, `SELECT pg_backend_pid()`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var pid int
	require.NoError(t, rows.Scan(&pid))
	return pid
}

// Observe PostgreSQL's actual blocker edge, never rely on a timing sleep.
func quotaSnapshotWaitForBlock(t *testing.T, ctx context.Context, db *sql.DB, pid int, done <-chan error) {
	t.Helper()
	for {
		select {
		case err := <-done:
			t.Fatalf("writer completed before blocker was released: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
		}
		var waiting bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&waiting))
		if waiting {
			return
		}
		runtime.Gosched()
	}
}

func quotaSnapshotAwait(t *testing.T, ctx context.Context, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestUserPlatformQuotaSnapshotFencePostgresResetAndSnapshotLockOrders(t *testing.T) {
	for _, adminFirst := range []bool{true, false} {
		name := "snapshot-first"
		if adminFirst {
			name = "admin-first"
		}
		t.Run(name, func(t *testing.T) {
			f := quotaSnapshotPostgresFixture(t, true)
			before := f.seed(t, "openai")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tx, err := f.client.Tx(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			txCtx := dbent.NewTxContext(ctx, tx)
			pid := quotaSnapshotTransactionPID(t, txCtx, tx.Client())
			snapshot := quotaControlSnapshot(before, 7)
			if adminFirst {
				require.NoError(t, f.repo.ResetExpiredWindow(txCtx, f.userID, "openai", "daily", time.Now()))
			} else {
				require.NoError(t, f.repo.BatchSnapshotUsage(txCtx, []UserPlatformQuotaSnapshot{snapshot}, time.Now()))
			}
			done := make(chan error, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				if adminFirst {
					done <- f.repo.BatchSnapshotUsage(ctx, []UserPlatformQuotaSnapshot{snapshot}, time.Now())
				} else {
					done <- f.repo.ResetExpiredWindow(ctx, f.userID, "openai", "daily", time.Now())
				}
			}()
			<-started
			quotaSnapshotWaitForBlock(t, ctx, f.db, pid, done)
			require.NoError(t, tx.Commit())
			quotaSnapshotAwait(t, ctx, done)
			after := f.read(t, "openai")
			require.Equal(t, before.ControlGeneration+1, after.ControlGeneration)
			require.Zero(t, after.DailyUsageUSD)
			if adminFirst {
				require.Zero(t, after.WeeklyUsageUSD, "blocked snapshot rechecks q generation after admin commit")
			} else {
				require.Equal(t, float64(8), after.WeeklyUsageUSD, "admin reset changes only its selected window")
			}
		})
	}
}

func TestUserPlatformQuotaSnapshotFencePostgresResetRollbackPreservesAllFacts(t *testing.T) {
	f := quotaSnapshotPostgresFixture(t, true)
	before := f.seed(t, "openai")
	require.NoError(t, f.repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{quotaControlSnapshot(before, 7)}, time.Now()))
	before = f.read(t, "openai")
	var cutoffBefore string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(s)::text FROM user_platform_quota_client_resets s WHERE quota_row_id=$1`, before.QuotaRowID).Scan(&cutoffBefore))
	tx, err := f.client.Tx(context.Background())
	require.NoError(t, err)
	txCtx := dbent.NewTxContext(context.Background(), tx)
	require.NoError(t, f.repo.ResetExpiredWindow(txCtx, f.userID, "openai", "daily", time.Now()))
	inTx, err := f.repo.GetByUserPlatform(txCtx, f.userID, "openai")
	require.NoError(t, err)
	require.Equal(t, before.ControlGeneration+1, inTx.ControlGeneration)
	require.Zero(t, inTx.DailyUsageUSD)
	require.NoError(t, tx.Rollback())
	require.Equal(t, before, f.read(t, "openai"))
	var cutoffAfter string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(s)::text FROM user_platform_quota_client_resets s WHERE quota_row_id=$1`, before.QuotaRowID).Scan(&cutoffAfter))
	require.Equal(t, cutoffBefore, cutoffAfter)
}

func TestUserPlatformQuotaSnapshotFencePostgresDatabaseFailureCanRetry(t *testing.T) {
	f := quotaSnapshotPostgresFixture(t, true)
	before := f.seed(t, "openai")
	_, err := f.db.Exec(`CREATE FUNCTION fail_quota_snapshot_fixture() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'fixture write error'; END; $$;
		CREATE TRIGGER fail_quota_snapshot_fixture BEFORE UPDATE ON user_platform_quotas
		FOR EACH ROW EXECUTE FUNCTION fail_quota_snapshot_fixture()`)
	require.NoError(t, err)
	snapshot := quotaControlSnapshot(before, 7)
	require.Error(t, f.repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{snapshot}, time.Now()))
	require.Equal(t, before, f.read(t, "openai"))
	_, err = f.db.Exec(`DROP TRIGGER fail_quota_snapshot_fixture ON user_platform_quotas; DROP FUNCTION fail_quota_snapshot_fixture()`)
	require.NoError(t, err)
	require.NoError(t, f.repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{snapshot}, time.Now()))
	require.Equal(t, float64(7), f.read(t, "openai").DailyUsageUSD)
}

func TestUserPlatformQuotaSnapshotFencePostgresGenerationExhaustionRollsBack(t *testing.T) {
	f := quotaSnapshotPostgresFixture(t, true)
	before := f.seed(t, "openai")
	// Seed the otherwise unreachable boundary by bypassing only this new guard.
	_, err := f.db.Exec(`ALTER TABLE user_platform_quotas DISABLE TRIGGER user_platform_quota_control_generation_before_update;
		UPDATE user_platform_quotas SET control_generation=9223372036854775807;
		ALTER TABLE user_platform_quotas ENABLE TRIGGER user_platform_quota_control_generation_before_update`)
	require.NoError(t, err)
	before = f.read(t, "openai")
	var resetBefore string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(s)::text FROM user_platform_quota_client_resets s WHERE quota_row_id=$1`, before.QuotaRowID).Scan(&resetBefore))
	require.Error(t, f.repo.ResetExpiredWindow(context.Background(), f.userID, "openai", "daily", time.Now()))
	require.Equal(t, before, f.read(t, "openai"))
	var resetAfter string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(s)::text FROM user_platform_quota_client_resets s WHERE quota_row_id=$1`, before.QuotaRowID).Scan(&resetAfter))
	require.Equal(t, resetBefore, resetAfter)
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET daily_limit_usd=101 WHERE id=$1`, before.QuotaRowID)
	require.Error(t, err)
	require.Equal(t, before, f.read(t, "openai"))
}
