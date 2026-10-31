//go:build unit

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Ordinary rows are also created through real credential/billing repositories.
// Only captured_at is adjusted as a historical-window clock seam.
func combinedProjectionOrdinaryAt(t *testing.T, f *settlementEventPGFixture, requestID string, captured time.Time, cost float64, platforms ...string) *service.UsageBillingCommand {
	t.Helper()
	key, err := f.client.APIKey.Create().SetUserID(f.session.UserID).SetGroupID(f.in.GroupID).
		SetKey("sk-combined-" + requestID).SetName("combined-projection").Save(context.Background())
	require.NoError(t, err)
	cmd := *f.event.Command
	cmd.APIKeyID, cmd.RequestID, cmd.RequestFingerprint = key.ID, requestID, ""
	cmd.BalanceCost, cmd.SubscriptionCost, cmd.APIKeyQuotaCost = cost, 0, cost
	cmd.SubscriptionID, cmd.BillingType = nil, service.BillingTypeBalance
	cmd.Normalize()
	platform := service.PlatformOpenAI
	if len(platforms) > 0 {
		platform = platforms[0]
	}
	cmd.OrdinaryPlatformQuota, err = service.FreezeUsageBillingOrdinaryPlatformIntent(&cmd, platform, cost, false)
	require.NoError(t, err)
	result, err := f.billing.Apply(context.Background(), &cmd)
	require.NoError(t, err)
	require.True(t, result.Applied)
	_, err = f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal SET captured_at=$1 WHERE request_id=$2 AND api_key_id=$3`,
		captured, cmd.RequestID, cmd.APIKeyID)
	require.NoError(t, err)
	return &cmd
}

func readCombinedProjection(t *testing.T, f *settlementEventPGFixture, asOf time.Time) *service.PlatformQuotaJournalProjection {
	t.Helper()
	reader := f.events.(service.PlatformQuotaJournalProjectionRepository)
	projection, err := reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.NoError(t, err)
	require.NotNil(t, projection)
	require.Equal(t, "combined_journals", projection.Source)
	require.False(t, projection.Mergeable)
	require.False(t, projection.CoverageVerified)
	return projection
}

func TestPlatformQuotaJournalProjectionPostgresCombinesRealSourcesAtExactPrecision(t *testing.T) {
	f, quotaID, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "combined-client-month", asOf.Add(-9*24*time.Hour), 2)
	clientProjectionJournalAt(t, f, "combined-client-day", timezone.StartOfDay(asOf).Add(time.Hour), 0.1234567891)
	combinedProjectionOrdinaryAt(t, f, "ordinary:combined-week", timezone.StartOfWeek(asOf).Add(time.Hour), 0.2)
	combinedProjectionOrdinaryAt(t, f, "ordinary:combined-day", timezone.StartOfDay(asOf).Add(2*time.Hour), 0.0000000001)
	var before string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(q)::text FROM user_platform_quotas q WHERE id=$1`, quotaID).Scan(&before))
	projection := readCombinedProjection(t, f, asOf)
	require.Equal(t, quotaID, projection.QuotaRowID)
	require.Equal(t, f.session.UserID, projection.UserID)
	require.Equal(t, service.PlatformOpenAI, projection.Platform)
	require.Equal(t, "0.1234567892", projection.Daily.UsageUSD)
	require.Equal(t, "0.3234567892", projection.Weekly.UsageUSD)
	require.Equal(t, "2.3234567892", projection.Monthly.UsageUSD)
	require.True(t, asOf.Add(-9*24*time.Hour).Equal(*projection.Monthly.Start))
	require.Equal(t, 720*time.Hour, projection.Monthly.End.Sub(*projection.Monthly.Start))
	var after string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(q)::text FROM user_platform_quotas q WHERE id=$1`, quotaID).Scan(&after))
	require.Equal(t, before, after)
	encoded, err := json.Marshal(projection)
	require.NoError(t, err)
	require.Equal(t, "{}", string(encoded), "source identities and partial totals remain private")
	clientOnly := clientProjectionRead(t, f, asOf)
	require.Equal(t, "0.1234567891", clientOnly.Daily.UsageUSD, "the existing client reader keeps its original scope")
}

func TestPlatformQuotaJournalProjectionPostgresLateCrossSourceEntryReanchorsOneMonth(t *testing.T) {
	f, _, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "combined-first", asOf.Add(-20*24*time.Hour), 1)
	combinedProjectionOrdinaryAt(t, f, "ordinary:combined-last", asOf.Add(-24*time.Hour), 2)
	before := readCombinedProjection(t, f, asOf)
	require.Equal(t, "3.0000000000", before.Monthly.UsageUSD)
	require.True(t, asOf.Add(-20*24*time.Hour).Equal(*before.Monthly.Start))
	// Adding a late ordinary fact changes the shared anchor. Summing two
	// separately projected source windows would incorrectly retain all costs.
	combinedProjectionOrdinaryAt(t, f, "ordinary:combined-late", asOf.Add(-40*24*time.Hour), 3)
	after := readCombinedProjection(t, f, asOf)
	require.Equal(t, "2.0000000000", after.Monthly.UsageUSD)
	require.True(t, asOf.Add(-24*time.Hour).Equal(*after.Monthly.Start))
	require.Equal(t, after, readCombinedProjection(t, f, asOf))
}

func TestPlatformQuotaJournalProjectionPostgresRejectsGlobalOpposingIdentityCollisions(t *testing.T) {
	for _, selected := range []string{"client", "ordinary"} {
		for _, hidden := range []string{"exact", "user", "platform", "quota", "disposition", "after-asof", "before-quota", "fingerprint"} {
			t.Run(selected+"/"+hidden, func(t *testing.T) {
				f, quotaID, asOf := clientProjectionFixture(t)
				captured := timezone.StartOfDay(asOf).Add(time.Hour)
				clientProjectionJournalAt(t, f, "combined-collision", captured, 0.1)
				ordinary := combinedProjectionOrdinaryAt(t, f, "client:combined-collision", captured, 0.1)
				// Deliberately corrupt the independent tables after real writes.
				// Cross-table uniqueness is the behavior this reader must verify.
				table, opposingKey, sourceKey := "usage_billing_ordinary_platform_quota_journal", ordinary.APIKeyID, f.event.APIKeyID
				if selected == "ordinary" {
					table, opposingKey, sourceKey = "usage_settlement_platform_quota_journal", f.event.APIKeyID, ordinary.APIKeyID
				}
				_, err := f.db.Exec(`UPDATE `+table+` SET api_key_id=$1 WHERE request_id=$2 AND api_key_id=$3`, sourceKey, ordinary.RequestID, opposingKey)
				require.NoError(t, err)
				switch hidden {
				case "exact":
					other := "usage_settlement_platform_quota_journal"
					if selected == "ordinary" {
						other = "usage_billing_ordinary_platform_quota_journal"
					}
					_, err = f.db.Exec(`UPDATE `+table+` SET billing_fingerprint=(SELECT billing_fingerprint FROM `+other+` WHERE request_id=$1 AND api_key_id=$2)
						WHERE request_id=$1 AND api_key_id=$2`, ordinary.RequestID, sourceKey)
				case "user":
					_, err = f.db.Exec(`UPDATE `+table+` SET user_id=$1 WHERE request_id=$2 AND api_key_id=$3`, f.session.UserID+1000, ordinary.RequestID, sourceKey)
				case "platform":
					_, err = f.db.Exec(`UPDATE `+table+` SET platform='gemini' WHERE request_id=$1 AND api_key_id=$2`, ordinary.RequestID, sourceKey)
				case "quota":
					_, err = f.db.Exec(`UPDATE `+table+` SET quota_row_id=$1 WHERE request_id=$2 AND api_key_id=$3`, quotaID+1000, ordinary.RequestID, sourceKey)
				case "disposition":
					mode := ""
					if selected == "client" {
						mode = "is_subscription=true,control_generation=NULL,"
					}
					_, err = f.db.Exec(`UPDATE `+table+` SET `+mode+` disposition='subscription_exempt',quota_row_id=NULL,
						daily_limit_usd=NULL,weekly_limit_usd=NULL,monthly_limit_usd=NULL,daily_usage_usd=NULL,weekly_usage_usd=NULL,monthly_usage_usd=NULL,
						daily_window_start=NULL,weekly_window_start=NULL,monthly_window_start=NULL WHERE request_id=$1 AND api_key_id=$2`, ordinary.RequestID, sourceKey)
				case "after-asof":
					_, err = f.db.Exec(`UPDATE `+table+` SET captured_at=$1 WHERE request_id=$2 AND api_key_id=$3`, asOf.Add(time.Hour), ordinary.RequestID, sourceKey)
				case "before-quota":
					_, err = f.db.Exec(`UPDATE `+table+` SET captured_at=(SELECT created_at-INTERVAL '1 second' FROM user_platform_quotas WHERE id=$1)
						WHERE request_id=$2 AND api_key_id=$3`, quotaID, ordinary.RequestID, sourceKey)
				case "fingerprint":
					_, err = f.db.Exec(`UPDATE `+table+` SET billing_fingerprint=$1 WHERE request_id=$2 AND api_key_id=$3`, sessionHash("other source"), ordinary.RequestID, sourceKey)
				}
				require.NoError(t, err)
				reader := f.events.(service.PlatformQuotaJournalProjectionRepository)
				projection, err := reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
				require.ErrorIs(t, err, service.ErrPlatformQuotaJournalProjectionConflict)
				require.Nil(t, projection)
			})
		}
	}
}

func TestPlatformQuotaJournalProjectionPostgresResetAppliesToBothSourcesByWindow(t *testing.T) {
	for _, window := range []string{"daily", "weekly", "monthly"} {
		t.Run(window, func(t *testing.T) {
			f, _, _ := clientProjectionFixture(t)
			captured := clientProjectionNow(t, f.db)
			clientProjectionJournalAt(t, f, "combined-before-reset", captured, 0.25)
			combinedProjectionOrdinaryAt(t, f, "ordinary:before-reset", captured, 0.5)
			quota := NewUserPlatformQuotaRepository(f.client)
			require.NoError(t, quota.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, window, captured.Add(-24*time.Hour)))
			projection := readCombinedProjection(t, f, clientProjectionNow(t, f.db))
			windows := map[string]service.ClientPlatformQuotaProjectionWindow{"daily": projection.Daily, "weekly": projection.Weekly, "monthly": projection.Monthly}
			for name, projected := range windows {
				if name == window {
					require.Equal(t, "0.0000000000", projected.UsageUSD)
					require.NotNil(t, projected.ResetAt)
				} else {
					require.Equal(t, "0.7500000000", projected.UsageUSD)
				}
			}
			clientProjectionJournalAt(t, f, "combined-after-reset", clientProjectionNow(t, f.db), 0.125)
			combinedProjectionOrdinaryAt(t, f, "ordinary:after-reset", clientProjectionNow(t, f.db), 0.0625)
			projection = readCombinedProjection(t, f, clientProjectionNow(t, f.db))
			windows = map[string]service.ClientPlatformQuotaProjectionWindow{"daily": projection.Daily, "weekly": projection.Weekly, "monthly": projection.Monthly}
			for name, projected := range windows {
				want := "0.9375000000"
				if name == window {
					want = "0.1875000000"
				}
				require.Equal(t, want, projected.UsageUSD)
			}
		})
	}
}

func TestPlatformQuotaJournalProjectionPostgresLifecycleAndLegacySnapshotsStaySeparate(t *testing.T) {
	f, oldQuotaID, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "combined-old-row", asOf.Add(-time.Hour), 0.25)
	combinedProjectionOrdinaryAt(t, f, "ordinary:old-row", asOf.Add(-time.Hour), 0.5)
	before := readCombinedProjection(t, f, asOf)
	quota := NewUserPlatformQuotaRepository(f.client)
	limit := 0.0
	require.NoError(t, quota.UpsertForUser(context.Background(), f.session.UserID, []UserPlatformQuotaRecord{{UserID: f.session.UserID, Platform: service.PlatformOpenAI, MonthlyLimitUSD: &limit}}))
	stale := asOf.Add(-80 * 24 * time.Hour)
	liveQuota, quotaReadErr := quota.GetByUserPlatform(context.Background(), f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, quotaReadErr)
	require.NotNil(t, liveQuota)
	require.NoError(t, quota.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{{QuotaRowID: liveQuota.QuotaRowID, ControlGeneration: liveQuota.ControlGeneration, UserID: f.session.UserID, Platform: service.PlatformOpenAI,
		DailyUsageUSD: 999, WeeklyUsageUSD: 888, MonthlyUsageUSD: 777, DailyWindowStart: stale, WeeklyWindowStart: stale, MonthlyWindowStart: stale}}, time.Now()))
	require.Equal(t, before, readCombinedProjection(t, f, asOf))
	require.NoError(t, quota.UpsertForUser(context.Background(), f.session.UserID, nil))
	reader := f.events.(service.PlatformQuotaJournalProjectionRepository)
	projection, err := reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, clientProjectionNow(t, f.db))
	require.NoError(t, err)
	require.Nil(t, projection)
	limit = 50
	require.NoError(t, quota.UpsertForUser(context.Background(), f.session.UserID, []UserPlatformQuotaRecord{{UserID: f.session.UserID, Platform: service.PlatformOpenAI, DailyLimitUSD: &limit}}))
	clientProjectionJournalAt(t, f, "combined-late-old-row", asOf, 1)
	combinedProjectionOrdinaryAt(t, f, "ordinary:late-old-row", asOf, 2)
	projection = readCombinedProjection(t, f, clientProjectionNow(t, f.db))
	require.NotEqual(t, oldQuotaID, projection.QuotaRowID)
	require.Equal(t, "0.0000000000", projection.Monthly.UsageUSD)
	clientProjectionJournalAt(t, f, "combined-new-row", clientProjectionNow(t, f.db), 0.125)
	combinedProjectionOrdinaryAt(t, f, "ordinary:new-row", clientProjectionNow(t, f.db), 0.25)
	projection = readCombinedProjection(t, f, clientProjectionNow(t, f.db))
	require.Equal(t, "0.3750000000", projection.Monthly.UsageUSD)
	_, err = f.db.Exec(`DELETE FROM user_platform_quotas WHERE id=$1`, oldQuotaID)
	require.NoError(t, err)
	require.Equal(t, projection, readCombinedProjection(t, f, projection.AsOf))
}

func TestPlatformQuotaJournalProjectionPostgresHistoryMissingStateAndDatabaseErrors(t *testing.T) {
	f, quotaID, asOf := clientProjectionFixture(t)
	reader := f.events.(service.PlatformQuotaJournalProjectionRepository)
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
		projection, err := reader.ReadPlatformQuotaJournalProjection(context.Background(), user, platform, at)
		require.ErrorIs(t, err, service.ErrPlatformQuotaJournalProjectionInput)
		require.Nil(t, projection)
	}
	_, err := f.db.Exec(`UPDATE user_platform_quota_client_resets SET tracking_started_at=$1 WHERE quota_row_id=$2`, asOf.Add(time.Hour), quotaID)
	require.NoError(t, err)
	projection, err := reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrPlatformQuotaJournalProjectionHistory)
	require.Nil(t, projection)
	_, err = f.db.Exec(`DELETE FROM user_platform_quota_client_resets WHERE quota_row_id=$1`, quotaID)
	require.NoError(t, err)
	projection, err = reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrPlatformQuotaJournalProjectionUnavailable)
	require.Nil(t, projection)
	_, err = f.db.Exec(`DROP TABLE user_platform_quota_client_resets`)
	require.NoError(t, err)
	_, err = reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrPlatformQuotaJournalProjectionUnavailable)
	var absent *usageSettlementEventRepository
	_, err = absent.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrPlatformQuotaJournalProjectionUnavailable)
}

func TestPlatformQuotaJournalProjectionPostgresReadonlySnapshotAndCombinedRawBound(t *testing.T) {
	f, _, asOf := clientProjectionFixture(t)
	clientProjectionJournalAt(t, f, "combined-read-context", asOf.Add(-time.Hour), 0.0000000001)
	combinedProjectionOrdinaryAt(t, f, "ordinary:read-context", asOf.Add(-time.Hour), 0.0000000001)
	_, err := f.db.Exec(`ALTER TABLE usage_settlement_platform_quota_journal RENAME TO combined_fixture_client_storage;
		ALTER TABLE usage_billing_ordinary_platform_quota_journal RENAME TO combined_fixture_ordinary_storage;
		CREATE FUNCTION combined_fixture_assert_read_context() RETURNS boolean LANGUAGE plpgsql STABLE AS $$
		BEGIN
		    IF current_setting('transaction_isolation')<>'repeatable read' OR current_setting('transaction_read_only')<>'on' THEN
		        RAISE EXCEPTION 'fixture requires one readonly repeatable-read snapshot';
		    END IF;
		    RETURN true;
		END; $$;
		CREATE VIEW usage_settlement_platform_quota_journal AS SELECT * FROM combined_fixture_client_storage WHERE combined_fixture_assert_read_context();
		CREATE VIEW usage_billing_ordinary_platform_quota_journal AS SELECT * FROM combined_fixture_ordinary_storage WHERE combined_fixture_assert_read_context()`)
	require.NoError(t, err)
	require.Equal(t, "0.0000000002", readCombinedProjection(t, f, asOf).Daily.UsageUSD)
	// Each source is below 10,000. Their combined 10,001 raw observations
	// must fail as a whole, before returning an apparently lower partial sum.
	for _, source := range []struct {
		table, storage string
		count          int
	}{
		{"usage_settlement_platform_quota_journal", "combined_fixture_client_storage", 5000},
		{"usage_billing_ordinary_platform_quota_journal", "combined_fixture_ordinary_storage", 5001},
	} {
		// Materialize the generated identities and preserve the real tables'
		// indexed opposing lookups; expanding these JSON views per EXISTS would
		// measure the fixture generator instead of the bounded reader.
		_, err = f.db.Exec(fmt.Sprintf(`DROP VIEW %s; CREATE TABLE %s AS
			SELECT (jsonb_populate_record(NULL::%s,to_jsonb(j)||jsonb_build_object('request_id',j.request_id||':copy:'||g.n::text))).*
			FROM %s j CROSS JOIN generate_series(1,%d) g(n);
			CREATE UNIQUE INDEX ON %s(request_id,api_key_id)`, source.table, source.table, source.storage, source.storage, source.count, source.table))
		require.NoError(t, err)
	}
	reader := f.events.(service.PlatformQuotaJournalProjectionRepository)
	projection, err := reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
	require.ErrorIs(t, err, service.ErrPlatformQuotaJournalProjectionUnavailable)
	require.True(t, err == service.ErrPlatformQuotaJournalProjectionUnavailable, "the bound must fail directly, not through a database error in the same category")
	require.Nil(t, projection)
	_, err = f.db.Exec(`DELETE FROM usage_billing_ordinary_platform_quota_journal
		WHERE request_id=(SELECT max(request_id) FROM usage_billing_ordinary_platform_quota_journal)`)
	require.NoError(t, err)
	require.Equal(t, "0.0000010000", readCombinedProjection(t, f, asOf).Daily.UsageUSD, "the complete 10,000-row read must succeed")
}

func TestPlatformQuotaJournalProjectionPostgresConcurrentResetReadsCompleteCombinedSnapshot(t *testing.T) {
	f, _, _ := clientProjectionFixture(t)
	now := clientProjectionNow(t, f.db)
	captured := now
	asOf := timezone.StartOfDay(now).AddDate(0, 0, 1).Add(-time.Microsecond)
	clientProjectionJournalAt(t, f, "combined-concurrent-reset", captured, 0.25)
	combinedProjectionOrdinaryAt(t, f, "ordinary:concurrent-reset", captured, 0.5)
	reader := f.events.(service.PlatformQuotaJournalProjectionRepository)
	const concurrency = 12
	type outcome struct {
		projection *service.PlatformQuotaJournalProjection
		err        error
	}
	results := make(chan outcome, concurrency)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			projection, err := reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformOpenAI, asOf)
			results <- outcome{projection, err}
		}()
	}
	close(start)
	require.NoError(t, NewUserPlatformQuotaRepository(f.client).ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "daily", now))
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		require.NotNil(t, result.projection)
		require.Equal(t, "0.7500000000", result.projection.Weekly.UsageUSD)
		require.Equal(t, "0.7500000000", result.projection.Monthly.UsageUSD)
		if result.projection.Daily.ResetAt == nil {
			require.Equal(t, "0.7500000000", result.projection.Daily.UsageUSD)
		} else {
			require.Equal(t, "0.0000000000", result.projection.Daily.UsageUSD)
		}
	}
}
