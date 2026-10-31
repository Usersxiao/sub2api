//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func quotaCacheCandidateReader(t *testing.T, repo UserPlatformQuotaRepository) service.UserPlatformQuotaCacheCandidateRepository {
	t.Helper()
	reader, ok := repo.(service.UserPlatformQuotaCacheCandidateRepository)
	require.True(t, ok)
	return reader
}

func quotaCacheCandidateRead(t *testing.T, f *orderedQuotaPGFixture, platform string) *service.UserPlatformQuotaCacheCandidate {
	t.Helper()
	candidate, err := quotaCacheCandidateReader(t, f.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, platform)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	return candidate
}

// Include tuple identity to detect even equal-value hidden updates, together
// with the journals that candidate preparation has no authority to advance.
func quotaCacheCandidateDatabaseState(t *testing.T, f *orderedQuotaPGFixture) string {
	t.Helper()
	var state string
	require.NoError(t, f.db.QueryRow(`SELECT jsonb_build_object(
		'quota',(SELECT COALESCE(jsonb_agg(jsonb_build_object('row',to_jsonb(q),'xmin',q.xmin::text,'ctid',q.ctid::text) ORDER BY q.id),'[]'::jsonb) FROM user_platform_quotas q),
		'authority',(SELECT COALESCE(jsonb_agg(jsonb_build_object('row',to_jsonb(a),'xmin',a.xmin::text,'ctid',a.ctid::text)),'[]'::jsonb) FROM user_platform_quota_writer_authority a),
		'deliveries',(SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY d.request_id),'[]'::jsonb) FROM ordinary_platform_quota_deliveries d),
		'resets',(SELECT COALESCE(jsonb_agg(to_jsonb(r)),'[]'::jsonb) FROM user_platform_quota_client_resets r))::text`).Scan(&state))
	return state
}

func TestUserPlatformQuotaCacheCandidatePostgresExactStateAtomicSnapshotAndRestart(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	initial := f.seed(t, service.PlatformOpenAI)
	before := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	oldState := quotaCacheCandidateDatabaseState(t, f)
	at := time.Date(2020, 3, 5, 7, 11, 13, 123456000, time.FixedZone("fixture east", 8*3600))
	future := time.Date(2099, 7, 9, 13, 17, 19, 654321000, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockUserPlatformQuotaWriterAuthority(ctx, tx, f.authority, quotaWriterActionAdminReset))
	_, err = tx.ExecContext(ctx, `UPDATE user_platform_quotas SET
		daily_limit_usd=1234567890.1234567890,weekly_limit_usd=NULL,monthly_limit_usd=0,
		daily_usage_usd=1234567890.1234567890,weekly_usage_usd=0.0000000001,monthly_usage_usd=9999999999.9999999999,
		daily_window_start=$2,weekly_window_start=$3,monthly_window_start=$4 WHERE id=$1`,
		initial.Token.QuotaRowID, at, at.Add(-2*time.Hour+time.Microsecond), future)
	require.NoError(t, err)
	whilePending := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	require.Equal(t, before.Domain(), whilePending.Domain())
	require.Equal(t, before.Observation(), whilePending.Observation())
	require.Equal(t, before.Fingerprint(), whilePending.Fingerprint(), "one committed snapshot excludes uncommitted amounts, windows and ordering metadata")
	require.Equal(t, oldState, quotaCacheCandidateDatabaseState(t, f))
	require.NoError(t, tx.Commit())
	after := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	observation := after.Observation()
	require.Equal(t, service.UserPlatformQuotaCacheDomain{SchemaVersion: service.UserPlatformQuotaCacheDomainSchemaV2,
		Namespace: quotaDomainPostgresNamespace(t, f.db), WriterAuthority: f.authority}, after.Domain())
	require.Equal(t, "1234567890.1234567890", *observation.DailyLimitUSD)
	require.Nil(t, observation.WeeklyLimitUSD)
	require.Equal(t, "0.0000000000", *observation.MonthlyLimitUSD)
	require.Equal(t, "1234567890.1234567890", observation.DailyUsageUSD)
	require.Equal(t, "0.0000000001", observation.WeeklyUsageUSD)
	require.Equal(t, "9999999999.9999999999", observation.MonthlyUsageUSD)
	require.Equal(t, initial.Token.Revision+1, observation.Token.Revision)
	for i, start := range []*time.Time{observation.DailyWindowStart, observation.WeeklyWindowStart, observation.MonthlyWindowStart} {
		require.NotNil(t, start)
		want := []time.Time{at, at.Add(-2*time.Hour + time.Microsecond), future}[i]
		require.True(t, want.Equal(*start))
		require.Equal(t, time.UTC, start.Location())
		require.Equal(t, want.Nanosecond(), start.Nanosecond())
	}
	committedState := quotaCacheCandidateDatabaseState(t, f)
	for range 3 {
		restarted := quotaCacheCandidateReader(t, NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, f.authority))
		got, err := restarted.GetUserPlatformQuotaCacheCandidate(nil, f.session.UserID, service.PlatformOpenAI)
		require.NoError(t, err)
		require.Equal(t, after.Domain(), got.Domain())
		require.Equal(t, observation, got.Observation())
		require.Equal(t, after.Fingerprint(), got.Fingerprint())
	}
	require.Equal(t, committedState, quotaCacheCandidateDatabaseState(t, f), "repeated/restarted reads never rewrite a row, authority or delivery journal")
}

func TestUserPlatformQuotaCacheCandidatePostgresRevisionOrderAndEqualTokenContentConflict(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	initial := f.seed(t, service.PlatformOpenAI)
	at := time.Date(2026, 10, 25, 9, 11, 13, 123456000, time.UTC)
	f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_window_start=$2 WHERE id=$1`, initial.Token.QuotaRowID, at)
	before := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_usage_usd=0.0000000001 WHERE id=$1`, initial.Token.QuotaRowID)
	after := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	require.Equal(t, before.Observation().Token.ControlGeneration, after.Observation().Token.ControlGeneration)
	require.Equal(t, before.Observation().Token.Lineage, after.Observation().Token.Lineage)
	require.Equal(t, before.Observation().Token.Revision+1, after.Observation().Token.Revision)
	order, err := service.CompareUserPlatformQuotaCacheCandidates(after, before)
	require.NoError(t, err)
	require.Equal(t, service.QuotaObservationNewer, order)
	order, err = service.CompareUserPlatformQuotaCacheCandidates(before, after)
	require.NoError(t, err)
	require.Equal(t, service.QuotaObservationOlder, order)
	copy := after.Observation()
	copy.DailyUsageUSD = "0.0000000002"
	conflicting, err := service.PrepareUserPlatformQuotaCacheCandidate(after.Domain(), copy)
	require.NoError(t, err)
	state := quotaCacheCandidateDatabaseState(t, f)
	order, err = service.CompareUserPlatformQuotaCacheCandidates(conflicting, after)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateConflict)
	require.Empty(t, order)
	stable := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	order, err = service.CompareUserPlatformQuotaCacheCandidates(stable, after)
	require.NoError(t, err)
	require.Equal(t, service.QuotaObservationEqual, order)
	require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f), "content comparison grants no choice of a winning payload and performs no write")
}

func TestUserPlatformQuotaCacheCandidatePostgresAuthorityCheckedForPresentAndMissingRows(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	f.seed(t, service.PlatformOpenAI)
	state := quotaCacheCandidateDatabaseState(t, f)
	for _, platform := range []string{service.PlatformOpenAI, service.PlatformGemini} {
		for _, tc := range []struct {
			name string
			repo UserPlatformQuotaRepository
			want error
		}{
			{"legacy", NewUserPlatformQuotaRepository(f.client), service.ErrUserPlatformQuotaWriterUnavailable},
			{"zero handle", NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, service.UserPlatformQuotaWriterAuthority{}), service.ErrUserPlatformQuotaWriterModeUnsupported},
			{"other epoch", NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, service.UserPlatformQuotaWriterAuthority{Mode: f.authority.Mode, Epoch: f.authority.Epoch + 1}), service.ErrUserPlatformQuotaWriterMismatch},
			{"snapshot", NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeSnapshot, Epoch: f.authority.Epoch}), service.ErrUserPlatformQuotaWriterModeUnsupported},
		} {
			t.Run(platform+"/"+tc.name, func(t *testing.T) {
				candidate, err := quotaCacheCandidateReader(t, tc.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, platform)
				require.ErrorIs(t, err, tc.want)
				require.Nil(t, candidate)
			})
		}
	}
	missing, err := quotaCacheCandidateReader(t, f.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, service.PlatformGemini)
	require.NoError(t, err)
	require.Nil(t, missing)
	require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f))
	_, err = f.db.Exec(`ALTER TABLE user_platform_quota_writer_authority DISABLE TRIGGER user_platform_quota_writer_authority_immutable`)
	require.NoError(t, err)
	_, err = f.db.Exec(`DELETE FROM user_platform_quota_writer_authority`)
	require.NoError(t, err)
	state = quotaCacheCandidateDatabaseState(t, f)
	for _, platform := range []string{service.PlatformOpenAI, service.PlatformGemini} {
		candidate, err := quotaCacheCandidateReader(t, f.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, platform)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
		require.Nil(t, candidate)
	}
	require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f), "missing authority is never initialized by a candidate read")
}

func TestUserPlatformQuotaCacheCandidatePostgresDomainValidatedBeforeMissingQuota(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	f.seed(t, service.PlatformOpenAI)
	_, err := f.db.Exec(`ALTER TABLE user_platform_quota_writer_authority DISABLE TRIGGER user_platform_quota_writer_authority_immutable`)
	require.NoError(t, err)
	_, err = f.db.Exec(`ALTER TABLE user_platform_quota_writer_authority DROP CONSTRAINT user_platform_quota_cache_namespace_nonzero`)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE user_platform_quota_writer_authority SET cache_namespace='00000000-0000-0000-0000-000000000000'::uuid WHERE id=1`)
	require.NoError(t, err)
	state := quotaCacheCandidateDatabaseState(t, f)
	for _, platform := range []string{service.PlatformOpenAI, service.PlatformGemini} {
		candidate, err := quotaCacheCandidateReader(t, f.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, platform)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheDomainInvalid)
		require.Nil(t, candidate)
	}
	require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f), "invalid namespace is neither regenerated nor accepted as an absence")
}

func TestUserPlatformQuotaCacheCandidatePostgresRejectsInvalidContextAndRepository(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	f.seed(t, service.PlatformOpenAI)
	reader := quotaCacheCandidateReader(t, f.repo)
	state := quotaCacheCandidateDatabaseState(t, f)
	for _, tc := range []struct {
		userID   int64
		platform string
	}{
		{0, service.PlatformOpenAI}, {-1, service.PlatformOpenAI}, {f.session.UserID, ""}, {f.session.UserID, "all"},
	} {
		candidate, err := reader.GetUserPlatformQuotaCacheCandidate(context.Background(), tc.userID, tc.platform)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaObservationInvalid)
		require.Nil(t, candidate)
	}
	tx, err := f.client.Tx(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	candidate, err := reader.GetUserPlatformQuotaCacheCandidate(dbent.NewTxContext(context.Background(), tx), f.session.UserID, service.PlatformOpenAI)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaObservationInvalid)
	require.Nil(t, candidate)
	require.NoError(t, tx.Rollback())
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	candidate, err = reader.GetUserPlatformQuotaCacheCandidate(cancelled, f.session.UserID, service.PlatformOpenAI)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, candidate)
	for _, repo := range []UserPlatformQuotaRepository{(*userPlatformQuotaRepository)(nil), NewUserPlatformQuotaRepositoryWithWriterAuthority(nil, f.authority)} {
		candidate, err := quotaCacheCandidateReader(t, repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, service.PlatformOpenAI)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
		require.Nil(t, candidate)
	}
	require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f))
}

func TestUserPlatformQuotaCacheCandidatePostgresRawAuditStillWorksWithoutNamespaceMigration(t *testing.T) {
	f := orderedQuotaPostgresFixture(t) // Intentionally migration 264, not 265.
	before := f.seed(t, service.PlatformOpenAI)
	f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_usage_usd='NaN'::numeric,
		weekly_usage_usd=-0.0000000001,monthly_usage_usd=1,monthly_window_start=NULL WHERE id=$1`, before.Token.QuotaRowID)
	state := quotaCacheCandidateDatabaseState(t, f)
	for range 2 {
		raw := f.read(t, service.PlatformOpenAI)
		require.Equal(t, "NaN", raw.DailyUsageUSD)
		require.Equal(t, "-0.0000000001", raw.WeeklyUsageUSD)
		require.Equal(t, "1.0000000000", raw.MonthlyUsageUSD)
		require.Nil(t, raw.MonthlyWindowStart)
		for _, platform := range []string{service.PlatformOpenAI, service.PlatformGemini} {
			candidate, err := quotaCacheCandidateReader(t, f.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, platform)
			require.Error(t, err, "strict candidates require a persisted namespace even for missing quota")
			require.Nil(t, candidate)
		}
	}
	require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f))
}

func TestUserPlatformQuotaCacheCandidatePostgresRejectsAnomaliesWithoutRepairingAuditState(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	at := time.Date(2026, 10, 25, 9, 11, 13, 123456000, time.UTC)
	for _, column := range []string{"daily_limit_usd", "weekly_limit_usd", "monthly_limit_usd", "daily_usage_usd", "weekly_usage_usd", "monthly_usage_usd"} {
		for _, rawValue := range []string{"NaN", "-0.0000000001"} {
			t.Run(column+"/"+rawValue, func(t *testing.T) {
				f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_limit_usd=100,weekly_limit_usd=NULL,monthly_limit_usd=0,
					daily_usage_usd=0,weekly_usage_usd=0,monthly_usage_usd=0,
					daily_window_start=$2,weekly_window_start=$2,monthly_window_start=$2 WHERE id=$1`, before.Token.QuotaRowID, at)
				f.scopedSQL(t, `UPDATE user_platform_quotas SET `+column+`=$2::numeric WHERE id=$1`, before.Token.QuotaRowID, rawValue)
				state := quotaCacheCandidateDatabaseState(t, f)
				raw := f.read(t, service.PlatformOpenAI)
				fields := map[string]*string{"daily_limit_usd": raw.DailyLimitUSD, "weekly_limit_usd": raw.WeeklyLimitUSD,
					"monthly_limit_usd": raw.MonthlyLimitUSD, "daily_usage_usd": &raw.DailyUsageUSD,
					"weekly_usage_usd": &raw.WeeklyUsageUSD, "monthly_usage_usd": &raw.MonthlyUsageUSD}
				require.NotNil(t, fields[column])
				require.Equal(t, rawValue, *fields[column], "raw audit never normalizes anomalous persisted facts")
				candidate, err := quotaCacheCandidateReader(t, f.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, service.PlatformOpenAI)
				require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
				require.Nil(t, candidate)
				require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f))
			})
		}
	}
	for _, window := range []string{"daily", "weekly", "monthly"} {
		t.Run(window+" missing anchor", func(t *testing.T) {
			f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_limit_usd=100,weekly_limit_usd=NULL,monthly_limit_usd=0,
				daily_usage_usd=0,weekly_usage_usd=0,monthly_usage_usd=0,
				daily_window_start=NULL,weekly_window_start=NULL,monthly_window_start=NULL WHERE id=$1`, before.Token.QuotaRowID)
			f.scopedSQL(t, `UPDATE user_platform_quotas SET `+window+`_usage_usd=0.0000000001 WHERE id=$1`, before.Token.QuotaRowID)
			state := quotaCacheCandidateDatabaseState(t, f)
			require.NotNil(t, f.read(t, service.PlatformOpenAI), "raw audit accepts a positive usage with a NULL anchor")
			candidate, err := quotaCacheCandidateReader(t, f.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, service.PlatformOpenAI)
			require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
			require.Nil(t, candidate)
			require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f))
		})
	}
}

func TestUserPlatformQuotaCacheCandidatePostgresResetDeleteAndRecreateGrantNoCacheAccess(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	f.seed(t, service.PlatformOpenAI)
	initial := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	start := time.Date(2026, 10, 25, 9, 11, 13, 123456000, time.UTC)
	require.NoError(t, f.repo.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "daily", start))
	reset := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	order, err := service.CompareUserPlatformQuotaCacheCandidates(reset, initial)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaObservationIncomparable)
	require.Empty(t, order)
	require.NoError(t, f.repo.UpsertForUser(context.Background(), f.session.UserID, nil))
	missing, err := quotaCacheCandidateReader(t, f.repo).GetUserPlatformQuotaCacheCandidate(context.Background(), f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.Nil(t, missing, "soft-deleted rows produce no absence candidate")
	f.seed(t, service.PlatformOpenAI)
	recreated := quotaCacheCandidateRead(t, f, service.PlatformOpenAI)
	require.NotEqual(t, initial.Observation().Token.Lineage, recreated.Observation().Token.Lineage)
	require.Equal(t, initial.Domain(), recreated.Domain())
	order, err = service.CompareUserPlatformQuotaCacheCandidates(recreated, initial)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaObservationIncomparable)
	require.Empty(t, order)
	state := quotaCacheCandidateDatabaseState(t, f)
	c, _, hook, _ := quotaDomainRedisFixture(t, "miniredis")
	production, err := ProvideBillingCache(c.rdb, f.db, f.authority)
	require.NoError(t, err)
	quotaDomainAssertAllQuotaPortsClosed(t, production.(*billingCache), context.Background(), f.session.UserID)
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
	require.Equal(t, state, quotaCacheCandidateDatabaseState(t, f), "candidate preparation never certifies a baseline or opens a cache writer")
}
