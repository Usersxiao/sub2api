//go:build unit

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func clientProjectionNow(t *testing.T, db *sql.DB) time.Time {
	t.Helper()
	var now time.Time
	require.NoError(t, db.QueryRow(`SELECT clock_timestamp()`).Scan(&now))
	return now
}

func clientProjectionFixture(t *testing.T) (*settlementEventPGFixture, int64, time.Time) {
	t.Helper()
	f := settlementPlatformQuotaFixture(t)
	limit := 50.0
	quotaID := settlementPlatformQuotaRow(t, f, &limit)
	now := clientProjectionNow(t, f.db)
	// A Wednesday in the preceding week leaves distinct day/week boundaries.
	asOf := timezone.StartOfWeek(now).AddDate(0, 0, -7).Add(2*24*time.Hour + 18*time.Hour)
	baseline := asOf.Add(-120 * 24 * time.Hour)
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET created_at=$1 WHERE id=$2`, baseline, quotaID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE user_platform_quota_client_resets SET tracking_started_at=$1 WHERE quota_row_id=$2`, baseline, quotaID)
	require.NoError(t, err)
	return f, quotaID, asOf
}

// Create journal rows through the real invocation, event, billing, and journal
// repositories. Only the clock seam is changed to exercise historical windows.
func clientProjectionJournalAt(t *testing.T, f *settlementEventPGFixture, id string, captured time.Time, cost float64, platforms ...string) {
	t.Helper()
	ctx := context.Background()
	next := *f.in
	next.InvocationID, next.RequestID, next.OwnerToken = id, "request-"+id, invocationTestOwner('B')
	_, err := f.repo.Reserve(ctx, f.grant, &next)
	require.NoError(t, err)
	_, err = startInvocationDispatch(ctx, f.repo, f.grant, next.InvocationID, next.OwnerToken, invocationTestDispatchBinding())
	require.NoError(t, err)
	command, usage := *f.event.Command, *f.event.Usage
	command.RequestID, usage.RequestID = "client:"+id, "client:"+id
	command.RequestFingerprint, command.BalanceCost = "", cost
	usage.InputCost, usage.OutputCost, usage.TotalCost, usage.ActualCost = cost/2, cost/2, cost, cost
	platform := service.PlatformOpenAI
	if len(platforms) > 0 {
		platform = platforms[0]
	}
	event, err := service.FreezeUsageSettlementEventWithPlatform(id, &command, &usage, platform)
	require.NoError(t, err)
	_, err = f.events.Stage(ctx, &service.UsageSettlementBinding{InvocationID: id, OwnerToken: next.OwnerToken}, event, settlementTestLease('B'))
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE usage_settlement_events SET created_at=$1 WHERE invocation_id=$2`, captured, id)
	require.NoError(t, err)
	_, err = f.billing.Apply(ctx, event.Command)
	require.NoError(t, err)
	require.NoError(t, f.events.RecordPlatformQuota(ctx, id, settlementTestLease('B')))
}

func clientProjectionRead(t *testing.T, f *settlementEventPGFixture, asOf time.Time) *service.ClientPlatformQuotaProjection {
	t.Helper()
	reader := NewUsageSettlementEventRepository(f.db).(service.ClientPlatformQuotaProjectionRepository)
	projection, err := reader.ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.NoError(t, err)
	require.NotNil(t, projection)
	require.Equal(t, "client_journal", projection.Source)
	require.False(t, projection.Mergeable)
	return projection
}

func TestClientPlatformQuotaProjectionPostgresReadsRealJournalAtExactDecimalPrecision(t *testing.T) {
	f, quotaID, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "projection-old", asOf.Add(-40*24*time.Hour), 1)
	clientProjectionJournalAt(t, f, "projection-month", asOf.Add(-9*24*time.Hour), 2)
	clientProjectionJournalAt(t, f, "projection-week", timezone.StartOfWeek(asOf).Add(time.Hour), 0.2)
	clientProjectionJournalAt(t, f, "projection-day", timezone.StartOfDay(asOf).Add(time.Hour), 0.1234567891)
	var before string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(q)::text FROM user_platform_quotas q WHERE id=$1`, quotaID).Scan(&before))
	projection := clientProjectionRead(t, f, asOf)
	require.Equal(t, quotaID, projection.QuotaRowID)
	require.Equal(t, f.session.UserID, projection.UserID)
	require.Equal(t, service.PlatformOpenAI, projection.Platform)
	require.Equal(t, "0.1234567891", projection.Daily.UsageUSD)
	require.Equal(t, "0.3234567891", projection.Weekly.UsageUSD)
	require.Equal(t, "2.3234567891", projection.Monthly.UsageUSD)
	require.True(t, timezone.StartOfDay(asOf).Equal(*projection.Daily.Start))
	require.True(t, timezone.StartOfWeek(asOf).Equal(*projection.Weekly.Start))
	require.True(t, asOf.Add(-9*24*time.Hour).Equal(*projection.Monthly.Start))
	require.Equal(t, 720*time.Hour, projection.Monthly.End.Sub(*projection.Monthly.Start))
	var after string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(q)::text FROM user_platform_quotas q WHERE id=$1`, quotaID).Scan(&after))
	require.Equal(t, before, after, "the read never writes legacy quota usage or windows")
	encoded, err := json.Marshal(projection)
	require.NoError(t, err)
	require.Equal(t, "{}", string(encoded))
}

func TestClientPlatformQuotaProjectionPostgresLateJournalRecomputesRollingMonth(t *testing.T) {
	f, _, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "projection-first", asOf.Add(-20*24*time.Hour), 1)
	clientProjectionJournalAt(t, f, "projection-last", asOf.Add(-24*time.Hour), 2)
	before := clientProjectionRead(t, f, asOf)
	require.Equal(t, "3.0000000000", before.Monthly.UsageUSD)
	require.True(t, asOf.Add(-20*24*time.Hour).Equal(*before.Monthly.Start))
	// The late older record changes the first anchor, so the final record now
	// starts a second 30-day period. Recompute from captured order every time.
	clientProjectionJournalAt(t, f, "projection-late", asOf.Add(-40*24*time.Hour), 3)
	after := clientProjectionRead(t, f, asOf)
	require.Equal(t, "2.0000000000", after.Monthly.UsageUSD)
	require.True(t, asOf.Add(-24*time.Hour).Equal(*after.Monthly.Start))
	require.Equal(t, after, clientProjectionRead(t, f, asOf))
}

func TestClientPlatformQuotaProjectionPostgresLegacyFlusherAndLimitEditsStaySeparate(t *testing.T) {
	f, _, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "projection-current", timezone.StartOfDay(asOf).Add(time.Hour), 0.0000000001)
	before := clientProjectionRead(t, f, asOf)
	quota := NewUserPlatformQuotaRepository(f.client)
	limit := 0.0
	require.NoError(t, quota.UpsertForUser(context.Background(), f.session.UserID, []UserPlatformQuotaRecord{{UserID: f.session.UserID, Platform: service.PlatformOpenAI, MonthlyLimitUSD: &limit}}))
	oldWindow := asOf.Add(-80 * 24 * time.Hour)
	liveQuota, quotaReadErr := quota.GetByUserPlatform(context.Background(), f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, quotaReadErr)
	require.NotNil(t, liveQuota)
	require.NoError(t, quota.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{{QuotaRowID: liveQuota.QuotaRowID, ControlGeneration: liveQuota.ControlGeneration, UserID: f.session.UserID, Platform: service.PlatformOpenAI,
		DailyUsageUSD: 999, WeeklyUsageUSD: 888, MonthlyUsageUSD: 777, DailyWindowStart: oldWindow, WeeklyWindowStart: oldWindow, MonthlyWindowStart: oldWindow}}, time.Now()))
	require.Equal(t, before, clientProjectionRead(t, f, asOf))
}

func TestClientPlatformQuotaProjectionPostgresAdminResetIsAtomicAndWindowScoped(t *testing.T) {
	for _, window := range []string{"daily", "weekly", "monthly"} {
		t.Run(window, func(t *testing.T) {
			f, quotaID, _ := clientProjectionFixture(t)
			captured := clientProjectionNow(t, f.db).Add(-time.Second)
			clientProjectionJournalAt(t, f, "projection-before-reset", captured, 0.25)
			quota := NewUserPlatformQuotaRepository(f.client)
			// The legacy requested window start is not the protected cutoff.
			require.NoError(t, quota.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, window, captured.Add(-24*time.Hour)))
			afterReset := clientProjectionNow(t, f.db)
			projection := clientProjectionRead(t, f, afterReset)
			windows := map[string]service.ClientPlatformQuotaProjectionWindow{"daily": projection.Daily, "weekly": projection.Weekly, "monthly": projection.Monthly}
			for name, projected := range windows {
				if name == window {
					require.Equal(t, "0.0000000000", projected.UsageUSD)
					require.NotNil(t, projected.ResetAt)
					require.True(t, projected.ResetAt.After(captured))
				} else {
					require.Equal(t, "0.2500000000", projected.UsageUSD)
					require.Nil(t, projected.ResetAt)
				}
			}
			reader := f.events.(service.ClientPlatformQuotaProjectionRepository)
			_, err := reader.ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, captured)
			require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionHistory)
			var firstCutoff time.Time
			require.NoError(t, f.db.QueryRow(`SELECT `+window+`_reset_at FROM user_platform_quota_client_resets WHERE quota_row_id=$1`, quotaID).Scan(&firstCutoff))
			require.NoError(t, quota.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, window, captured.Add(-48*time.Hour)))
			var secondCutoff time.Time
			require.NoError(t, f.db.QueryRow(`SELECT `+window+`_reset_at FROM user_platform_quota_client_resets WHERE quota_row_id=$1`, quotaID).Scan(&secondCutoff))
			require.False(t, secondCutoff.Before(firstCutoff), "the protected cutoff never follows a backwards legacy start")
			clientProjectionJournalAt(t, f, "projection-after-reset", clientProjectionNow(t, f.db), 0.125)
			projection = clientProjectionRead(t, f, clientProjectionNow(t, f.db))
			windows = map[string]service.ClientPlatformQuotaProjectionWindow{"daily": projection.Daily, "weekly": projection.Weekly, "monthly": projection.Monthly}
			for name, projected := range windows {
				want := "0.3750000000"
				if name == window {
					want = "0.1250000000"
				}
				require.Equal(t, want, projected.UsageUSD)
			}
		})
	}
}

func TestClientPlatformQuotaProjectionPostgresQuotaRecreationRejectsOlderCapturedWork(t *testing.T) {
	f, oldQuotaID, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "projection-old-quota", asOf, 1)
	quota := NewUserPlatformQuotaRepository(f.client)
	require.NoError(t, quota.UpsertForUser(context.Background(), f.session.UserID, nil))
	reader := f.events.(service.ClientPlatformQuotaProjectionRepository)
	projection, err := reader.ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, clientProjectionNow(t, f.db))
	require.NoError(t, err)
	require.Nil(t, projection)
	limit := 50.0
	require.NoError(t, quota.UpsertForUser(context.Background(), f.session.UserID, []UserPlatformQuotaRecord{{UserID: f.session.UserID, Platform: service.PlatformOpenAI, DailyLimitUSD: &limit}}))
	var newQuotaID int64
	require.NoError(t, f.db.QueryRow(`SELECT id FROM user_platform_quotas WHERE user_id=$1 AND platform='openai' AND deleted_at IS NULL`, f.session.UserID).Scan(&newQuotaID))
	require.NotEqual(t, oldQuotaID, newQuotaID)
	// First journal insertion after recreation can observe the new quota row;
	// its earlier captured time must still exclude it from that incarnation.
	clientProjectionJournalAt(t, f, "projection-late-old-call", asOf, 2)
	projection = clientProjectionRead(t, f, clientProjectionNow(t, f.db))
	require.Equal(t, newQuotaID, projection.QuotaRowID)
	require.Equal(t, "0.0000000000", projection.Daily.UsageUSD)
	require.Equal(t, "0.0000000000", projection.Weekly.UsageUSD)
	require.Equal(t, "0.0000000000", projection.Monthly.UsageUSD)
	clientProjectionJournalAt(t, f, "projection-new-quota", clientProjectionNow(t, f.db), 0.5)
	projection = clientProjectionRead(t, f, clientProjectionNow(t, f.db))
	require.Equal(t, "0.5000000000", projection.Monthly.UsageUSD)
	_, err = f.db.Exec(`DELETE FROM user_platform_quotas WHERE id=$1`, oldQuotaID)
	require.NoError(t, err)
	var stateCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM user_platform_quota_client_resets WHERE quota_row_id=$1`, oldQuotaID).Scan(&stateCount))
	require.Zero(t, stateCount, "physical quota cleanup cascades only its tracking state")
}

func TestClientPlatformQuotaProjectionPostgresMigrationSeedsHonestTrackingAndTrigger(t *testing.T) {
	f := settlementPlatformQuotaFixture(t)
	_, err := f.db.Exec(`DROP TRIGGER user_platform_quota_client_resets_after_insert ON user_platform_quotas;
		DROP FUNCTION initialize_user_platform_quota_client_resets(); DROP TABLE user_platform_quota_client_resets`)
	require.NoError(t, err)
	limit := 50.0
	quotaID := settlementPlatformQuotaRow(t, f, &limit)
	old := clientProjectionNow(t, f.db).Add(-90 * 24 * time.Hour)
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET created_at=$1,daily_window_start=$1,weekly_window_start=$1,monthly_window_start=$1 WHERE id=$2`, old, quotaID)
	require.NoError(t, err)
	clientProjectionJournalAt(t, f, "projection-pre-tracking", old.Add(time.Hour), 0.5)
	_, err = f.db.Exec(`INSERT INTO user_platform_quotas(user_id,platform,daily_limit_usd,created_at,updated_at)
		VALUES($1,'gemini',1,$2,$2)`, f.session.UserID, old)
	require.NoError(t, err)
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "254_user_platform_quota_client_resets.sql"))
	require.NoError(t, err)
	_, err = f.db.Exec(string(raw))
	require.NoError(t, err)
	var count, baselineCount, resetCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*),count(DISTINCT tracking_started_at),count(daily_reset_at)+count(weekly_reset_at)+count(monthly_reset_at)
		FROM user_platform_quota_client_resets`).Scan(&count, &baselineCount, &resetCount))
	require.Equal(t, 2, count)
	require.Equal(t, 1, baselineCount)
	require.Zero(t, resetCount, "legacy window starts never manufacture reset provenance")
	reader := f.events.(service.ClientPlatformQuotaProjectionRepository)
	_, err = reader.ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, clientProjectionNow(t, f.db))
	require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionHistory)
	quota := NewUserPlatformQuotaRepository(f.client)
	require.NoError(t, quota.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "monthly", time.Now()))
	projection := clientProjectionRead(t, f, clientProjectionNow(t, f.db))
	require.Equal(t, "0.0000000000", projection.Monthly.UsageUSD)
	before := clientProjectionNow(t, f.db)
	_, err = f.db.Exec(`INSERT INTO user_platform_quotas(user_id,platform,daily_limit_usd,created_at,updated_at)
		VALUES($1,'anthropic',1,clock_timestamp(),clock_timestamp())`, f.session.UserID)
	require.NoError(t, err)
	var started time.Time
	require.NoError(t, f.db.QueryRow(`SELECT s.tracking_started_at FROM user_platform_quota_client_resets s JOIN user_platform_quotas q ON q.id=s.quota_row_id
		WHERE q.user_id=$1 AND q.platform='anthropic'`, f.session.UserID).Scan(&started))
	require.False(t, started.Before(before), "new quota rows receive their own actual tracking start")
}

func TestClientPlatformQuotaProjectionPostgresMissingStateAndDatabaseFailuresFailClosed(t *testing.T) {
	f, quotaID, asOf := clientProjectionFixture(t)
	reader := f.events.(service.ClientPlatformQuotaProjectionRepository)
	for _, kind := range []string{"user", "platform", "time"} {
		user, platform, at := f.session.UserID, service.PlatformOpenAI, asOf
		switch kind {
		case "user":
			user = 0
		case "platform":
			platform = "unknown-platform"
		case "time":
			at = time.Time{}
		}
		_, err := reader.ReadClientPlatformQuotaProjection(context.Background(), user, platform, at)
		require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionInput)
	}
	_, err := f.db.Exec(`DELETE FROM user_platform_quota_client_resets WHERE quota_row_id=$1`, quotaID)
	require.NoError(t, err)
	_, err = reader.ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionUnavailable)
	quota := NewUserPlatformQuotaRepository(f.client)
	err = quota.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "daily", asOf)
	require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionUnavailable)
	var daily float64
	require.NoError(t, f.db.QueryRow(`SELECT daily_usage_usd FROM user_platform_quotas WHERE id=$1`, quotaID).Scan(&daily))
	require.Equal(t, 1.25, daily, "missing reset state rolls back the legacy reset too")
	_, err = f.db.Exec(`DROP TABLE user_platform_quota_client_resets`)
	require.NoError(t, err)
	_, err = reader.ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionUnavailable)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = reader.ReadClientPlatformQuotaProjection(ctx, f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionUnavailable)
	var absent *usageSettlementEventRepository
	_, err = absent.ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionUnavailable)
}

func TestClientPlatformQuotaProjectionPostgresFailedLegacyResetRollsBackProtectedCutoff(t *testing.T) {
	f, quotaID, _ := clientProjectionFixture(t)
	_, err := f.db.Exec(`CREATE FUNCTION projection_fixture_legacy_reset_failure() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'fixture legacy reset failure'; END; $$;
		CREATE TRIGGER projection_fixture_legacy_reset_failure BEFORE UPDATE ON user_platform_quotas
		FOR EACH ROW EXECUTE FUNCTION projection_fixture_legacy_reset_failure()`)
	require.NoError(t, err)
	err = NewUserPlatformQuotaRepository(f.client).ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "daily", time.Now())
	require.Error(t, err)
	var cutoffAbsent bool
	var daily float64
	require.NoError(t, f.db.QueryRow(`SELECT s.daily_reset_at IS NULL,q.daily_usage_usd FROM user_platform_quotas q
		JOIN user_platform_quota_client_resets s ON s.quota_row_id=q.id WHERE q.id=$1`, quotaID).Scan(&cutoffAbsent, &daily))
	require.True(t, cutoffAbsent, "a later legacy write failure rolls back the already updated cutoff")
	require.Equal(t, 1.25, daily)
}

func TestClientPlatformQuotaProjectionPostgresReadUsesReadonlyRepeatableReadAndRejectsTruncation(t *testing.T) {
	f, _, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "projection-read-context", asOf.Add(-time.Hour), 0.0000000001)
	_, err := f.db.Exec(`ALTER TABLE usage_settlement_platform_quota_journal RENAME TO projection_fixture_journal_storage;
		CREATE FUNCTION projection_fixture_assert_read_context() RETURNS boolean LANGUAGE plpgsql STABLE AS $$
		BEGIN
		    IF current_setting('transaction_isolation')<>'repeatable read' OR current_setting('transaction_read_only')<>'on' THEN
		        RAISE EXCEPTION 'fixture requires one readonly repeatable-read snapshot';
		    END IF;
		    RETURN true;
		END; $$;
		CREATE VIEW usage_settlement_platform_quota_journal AS SELECT * FROM projection_fixture_journal_storage
		WHERE projection_fixture_assert_read_context()`)
	require.NoError(t, err)
	projection := clientProjectionRead(t, f, asOf)
	require.Equal(t, "0.0000000001", projection.Daily.UsageUSD)
	// Replicate one actual, verified journal observation behind a fixture view
	// to prove that SQL's safety bound never returns a truncated lower total.
	_, err = f.db.Exec(`CREATE OR REPLACE VIEW usage_settlement_platform_quota_journal AS
		SELECT j.* FROM projection_fixture_journal_storage j CROSS JOIN generate_series(1,10001)
		WHERE projection_fixture_assert_read_context()`)
	require.NoError(t, err)
	projection, err = f.events.(service.ClientPlatformQuotaProjectionRepository).ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrClientPlatformQuotaProjectionUnavailable)
	require.Nil(t, projection)
}

func TestClientPlatformQuotaProjectionPostgresConcurrentResetReadsCompleteSnapshots(t *testing.T) {
	f, _, _ := clientProjectionFixture(t)
	captured := clientProjectionNow(t, f.db)
	clientProjectionJournalAt(t, f, "projection-concurrent", captured, 0.75)
	reader := f.events.(service.ClientPlatformQuotaProjectionRepository)
	asOf := clientProjectionNow(t, f.db).Add(time.Hour)
	const concurrency = 12
	start := make(chan struct{})
	type readResult struct {
		projection *service.ClientPlatformQuotaProjection
		err        error
	}
	results := make(chan readResult, concurrency)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			projection, err := reader.ReadClientPlatformQuotaProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
			results <- readResult{projection, err}
		}()
	}
	close(start)
	require.NoError(t, NewUserPlatformQuotaRepository(f.client).ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "daily", time.Now()))
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		require.NotNil(t, result.projection)
		if result.projection.Daily.ResetAt == nil {
			want := "0.7500000000"
			if captured.Before(timezone.StartOfDay(asOf)) {
				want = "0.0000000000"
			}
			require.Equal(t, want, result.projection.Daily.UsageUSD)
		} else {
			require.Equal(t, "0.0000000000", result.projection.Daily.UsageUSD)
		}
		weeklyWant := "0.7500000000"
		if captured.Before(timezone.StartOfWeek(asOf)) {
			weeklyWant = "0.0000000000"
		}
		require.Equal(t, weeklyWant, result.projection.Weekly.UsageUSD)
		require.Equal(t, "0.7500000000", result.projection.Monthly.UsageUSD)
	}
}
