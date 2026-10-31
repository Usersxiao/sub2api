//go:build unit

package repository

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func settlementPlatformQuotaFixture(t *testing.T) *settlementEventPGFixture {
	t.Helper()
	f := settlementEventPostgresFixture(t, true)
	event, err := service.FreezeUsageSettlementEventWithPlatform(f.in.InvocationID, f.event.Command, f.event.Usage, service.PlatformOpenAI)
	require.NoError(t, err)
	require.Equal(t, 2, event.SchemaVersion)
	f.event = event
	return f
}

func settlementPlatformQuotaJournalJSON(t *testing.T, f *settlementEventPGFixture) string {
	t.Helper()
	var raw string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(j)::text FROM usage_settlement_platform_quota_journal j WHERE invocation_id=$1`, f.event.InvocationID).Scan(&raw))
	return raw
}

func settlementPlatformQuotaRow(t *testing.T, f *settlementEventPGFixture, dailyLimit *float64) int64 {
	t.Helper()
	start := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	var id int64
	require.NoError(t, f.db.QueryRow(`INSERT INTO user_platform_quotas(user_id,platform,daily_limit_usd,
		daily_usage_usd,weekly_usage_usd,monthly_usage_usd,daily_window_start,weekly_window_start,monthly_window_start,created_at,updated_at)
		VALUES($1,'openai',$2,1.25,2.5,3.75,$3,$3,$3,clock_timestamp(),clock_timestamp()) RETURNING id`,
		f.session.UserID, dailyLimit, start).Scan(&id))
	return id
}

func TestUsageSettlementPlatformQuotaPostgresRequiresRealBillingAndCurrentLease(t *testing.T) {
	f := settlementPlatformQuotaFixture(t)
	invalid := settlementCloneEvent(t, f.event)
	invalid.PlatformQuota.Platform, invalid.EventFingerprint = "unregistered-platform", ""
	_, err := f.events.Stage(context.Background(), f.binding, invalid, settlementTestLease('A'))
	require.ErrorIs(t, err, service.ErrUsageSettlementPayload)
	f.stage(t)
	ctx := context.Background()
	require.ErrorIs(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')), service.ErrUsageSettlementUnconfirmed)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_settlement_platform_quota_journal`).Scan(&count))
	require.Zero(t, count)
	_, err = f.billing.Apply(ctx, f.event.Command)
	require.NoError(t, err)
	require.ErrorIs(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('B')), service.ErrUsageSettlementLease)
	f.expireLease(t)
	require.ErrorIs(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')), service.ErrUsageSettlementLease)
	claimed, err := f.events.ClaimPending(ctx, settlementTestLease('C'), 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.ErrorIs(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')), service.ErrUsageSettlementLease)
	// A committed archived key remains valid proof after logout and a takeover.
	_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
		SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup;
		DELETE FROM usage_billing_dedup`)
	require.NoError(t, err)
	require.NoError(t, NewAuthSessionRepository(f.db).RevokeRefresh(ctx, sessionHash("billing-session-token")))
	require.NoError(t, NewUsageSettlementEventRepository(f.db).RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('C')))
	first := settlementPlatformQuotaJournalJSON(t, f)
	require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('C')))
	require.Equal(t, first, settlementPlatformQuotaJournalJSON(t, f))
	proof := service.UsageSettlementResult{RequestID: f.event.RequestID, APIKeyID: f.event.APIKeyID, UsageLogID: 999,
		BillingFingerprint: f.event.Command.RequestFingerprint}
	require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('C'), proof), service.ErrUsageSettlementUnconfirmed,
		"a real journal never substitutes for a persisted matching usage row")
	// A matching archived row never hides a conflicting hot record.
	_, err = f.db.Exec(`INSERT INTO usage_billing_dedup(request_id,api_key_id,request_fingerprint) VALUES($1,$2,$3)`, f.event.RequestID, f.event.APIKeyID, sessionHash("conflicting hot key"))
	require.NoError(t, err)
	require.ErrorIs(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('C')), service.ErrUsageSettlementEventConflict)
	require.Equal(t, first, settlementPlatformQuotaJournalJSON(t, f))
}

func TestUsageSettlementPlatformQuotaPostgresJournalWriteFailureDoesNotConfirmSuccess(t *testing.T) {
	f := settlementPlatformQuotaFixture(t)
	f.stage(t)
	ctx := context.Background()
	proof := f.persist(t)
	_, err := f.db.Exec(`CREATE FUNCTION settlement_journal_fixture_failure() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'fixture journal write failure'; END; $$;
		CREATE TRIGGER settlement_journal_fixture_failure BEFORE INSERT ON usage_settlement_platform_quota_journal
		FOR EACH ROW EXECUTE FUNCTION settlement_journal_fixture_failure()`)
	require.NoError(t, err)
	require.Error(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
	require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof), service.ErrUsageSettlementUnconfirmed)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_settlement_platform_quota_journal`).Scan(&count))
	require.Zero(t, count)
	_, err = f.db.Exec(`DROP TRIGGER settlement_journal_fixture_failure ON usage_settlement_platform_quota_journal`)
	require.NoError(t, err)
	require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
	require.NoError(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof))
}

func TestUsageSettlementPlatformQuotaPostgresFreezesDispositionAndActualQuotaSnapshot(t *testing.T) {
	for _, kind := range []string{"metered", "zero-limit", "missing", "all-null", "soft-deleted", "zero-cost", "subscription", "subscription-zero"} {
		t.Run(kind, func(t *testing.T) {
			f := settlementPlatformQuotaFixture(t)
			limit := 5.0
			var quotaID int64
			if kind != "missing" {
				if kind == "all-null" {
					quotaID = settlementPlatformQuotaRow(t, f, nil)
				} else {
					if kind == "zero-limit" {
						limit = 0
					}
					quotaID = settlementPlatformQuotaRow(t, f, &limit)
				}
			}
			if kind == "soft-deleted" {
				_, err := f.db.Exec(`UPDATE user_platform_quotas SET deleted_at=clock_timestamp() WHERE id=$1`, quotaID)
				require.NoError(t, err)
			}
			command, usage := *f.event.Command, *f.event.Usage
			if kind == "zero-cost" || kind == "subscription-zero" {
				command.BalanceCost = 0
				usage.InputCost, usage.OutputCost, usage.TotalCost, usage.ActualCost = 0, 0, 0, 0
			}
			if strings.HasPrefix(kind, "subscription") {
				now := time.Now()
				sub, err := f.client.UserSubscription.Create().SetUserID(f.session.UserID).SetGroupID(f.in.GroupID).
					SetStartsAt(now).SetExpiresAt(now.Add(time.Hour)).Save(context.Background())
				require.NoError(t, err)
				command.SubscriptionID, usage.SubscriptionID = &sub.ID, &sub.ID
				command.BillingType, usage.BillingType = service.BillingTypeSubscription, service.BillingTypeSubscription
				command.BalanceCost, command.SubscriptionCost = 0, usage.ActualCost
			}
			command.RequestFingerprint = ""
			event, err := service.FreezeUsageSettlementEventWithPlatform(f.in.InvocationID, &command, &usage, service.PlatformOpenAI)
			require.NoError(t, err)
			f.event = event
			f.stage(t)
			_, err = f.billing.Apply(context.Background(), f.event.Command)
			require.NoError(t, err)
			require.NoError(t, f.events.RecordPlatformQuota(context.Background(), f.event.InvocationID, settlementTestLease('A')))
			journal, err := scanUsageSettlementPlatformQuotaJournal(f.db.QueryRow(`SELECT `+usageSettlementPlatformQuotaJournalColumns+` FROM usage_settlement_platform_quota_journal WHERE invocation_id=$1`, f.event.InvocationID))
			require.NoError(t, err)
			require.True(t, usageSettlementPlatformQuotaJournalMatches(journal, f.event))
			switch kind {
			case "metered", "zero-limit":
				require.Equal(t, "metered", journal.disposition)
				require.Equal(t, quotaID, journal.quotaRowID.Int64)
				require.True(t, journal.dailyLimit.Valid)
				require.Equal(t, fmt.Sprintf("%.10f", limit), journal.dailyLimit.String)
				require.Equal(t, "1.2500000000", journal.dailyUsage.String)
				require.Equal(t, "2.5000000000", journal.weeklyUsage.String)
				require.Equal(t, "3.7500000000", journal.monthlyUsage.String)
				require.True(t, journal.dailyStart.Valid)
			case "zero-cost":
				require.Equal(t, "zero_cost", journal.disposition)
				require.False(t, journal.quotaRowID.Valid)
			case "subscription", "subscription-zero":
				require.Equal(t, "subscription_exempt", journal.disposition)
				require.False(t, journal.quotaRowID.Valid)
			default:
				require.Equal(t, "unlimited", journal.disposition)
				require.Equal(t, kind == "all-null", journal.quotaRowID.Valid)
			}
			if kind == "metered" {
				require.Equal(t, "0.0000781250", journal.costUSD, "journal preserves the independent scale-10 metering cost")
				require.NotEqual(t, f.event.Command.BalanceCost, f.event.PlatformQuota.CostUSD)
			}
			require.True(t, f.event.CreatedAt.Equal(journal.capturedAt))
			require.NotContains(t, settlementPlatformQuotaJournalJSON(t, f), f.binding.OwnerToken)
			require.NotContains(t, settlementPlatformQuotaJournalJSON(t, f), settlementTestLease('A'))
		})
	}
}

func TestUsageSettlementPlatformQuotaPostgresSnapshotPreservesExactDatabaseDecimals(t *testing.T) {
	f := settlementPlatformQuotaFixture(t)
	limit := 5.0
	quotaID := settlementPlatformQuotaRow(t, f, &limit)
	values := []string{"999999999.1234567891", "888888888.9876543219", "777777777.0123456789",
		"666666666.1357924681", "555555555.2468135792", "444444444.9999999999"}
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET daily_limit_usd=$1,weekly_limit_usd=$2,monthly_limit_usd=$3,
		daily_usage_usd=$4,weekly_usage_usd=$5,monthly_usage_usd=$6 WHERE id=$7`,
		values[0], values[1], values[2], values[3], values[4], values[5], quotaID)
	require.NoError(t, err)
	f.stage(t)
	_, err = f.billing.Apply(context.Background(), f.event.Command)
	require.NoError(t, err)
	require.NoError(t, f.events.RecordPlatformQuota(context.Background(), f.event.InvocationID, settlementTestLease('A')))
	actual := make([]string, len(values))
	require.NoError(t, f.db.QueryRow(`SELECT daily_limit_usd,weekly_limit_usd,monthly_limit_usd,daily_usage_usd,weekly_usage_usd,monthly_usage_usd
		FROM usage_settlement_platform_quota_journal WHERE invocation_id=$1`, f.event.InvocationID).
		Scan(&actual[0], &actual[1], &actual[2], &actual[3], &actual[4], &actual[5]))
	require.Equal(t, values, actual, "the first quota snapshot keeps every database decimal digit")
}

func TestUsageSettlementPlatformQuotaPostgresCompleteRequiresMatchingJournal(t *testing.T) {
	f := settlementPlatformQuotaFixture(t)
	f.stage(t)
	ctx := context.Background()
	proof := f.persist(t)
	require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof), service.ErrUsageSettlementUnconfirmed)
	require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
	for _, field := range []string{"event-fingerprint", "billing-fingerprint", "user", "key", "platform", "cost", "date", "disposition"} {
		t.Run(field, func(t *testing.T) {
			var mutation string
			var changed, original any
			switch field {
			case "event-fingerprint":
				mutation, changed, original = "event_fingerprint", sessionHash("other event"), f.event.EventFingerprint
			case "billing-fingerprint":
				mutation, changed, original = "billing_fingerprint", sessionHash("other billing"), f.event.Command.RequestFingerprint
			case "user":
				mutation, changed, original = "user_id", f.event.Command.UserID+100, f.event.Command.UserID
			case "key":
				mutation, changed, original = "api_key_id", f.event.APIKeyID+100, f.event.APIKeyID
			case "platform":
				mutation, changed, original = "platform", service.PlatformGemini, f.event.PlatformQuota.Platform
			case "cost":
				mutation, changed, original = "cost_usd", f.event.PlatformQuota.CostUSD+1, f.event.PlatformQuota.CostUSD
			case "date":
				mutation, changed, original = "captured_at", f.event.CreatedAt.Add(-24*time.Hour), f.event.CreatedAt
			case "disposition":
				mutation, changed, original = "disposition", "subscription_exempt", "unlimited"
			}
			_, err := f.db.Exec(`UPDATE usage_settlement_platform_quota_journal SET `+mutation+`=$1 WHERE invocation_id=$2`, changed, f.event.InvocationID)
			require.NoError(t, err)
			require.ErrorIs(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')), service.ErrUsageSettlementEventConflict)
			require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof), service.ErrUsageSettlementEventConflict)
			_, err = f.db.Exec(`UPDATE usage_settlement_platform_quota_journal SET `+mutation+`=$1 WHERE invocation_id=$2`, original, f.event.InvocationID)
			require.NoError(t, err)
		})
	}
	require.NoError(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof))
	require.NoError(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof))
	require.ErrorIs(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')), service.ErrUsageSettlementLease)
	// A settled event is never reopened to manufacture missing metering evidence.
	_, err := f.db.Exec(`DELETE FROM usage_settlement_platform_quota_journal WHERE invocation_id=$1`, f.event.InvocationID)
	require.NoError(t, err)
	require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof), service.ErrUsageSettlementUnconfirmed)
	var state string
	require.NoError(t, f.db.QueryRow(`SELECT state FROM usage_settlement_events WHERE invocation_id=$1`, f.event.InvocationID).Scan(&state))
	require.Equal(t, string(service.UsageSettlementSettled), state)
}

func TestUsageSettlementPlatformQuotaPostgresConcurrentRecordKeepsFirstAdminSnapshot(t *testing.T) {
	f := settlementPlatformQuotaFixture(t)
	limit := 5.0
	quotaID := settlementPlatformQuotaRow(t, f, &limit)
	f.stage(t)
	ctx := context.Background()
	_, err := f.billing.Apply(ctx, f.event.Command)
	require.NoError(t, err)
	const concurrency = 8
	errors := make(chan error, concurrency)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- NewUsageSettlementEventRepository(f.db).RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A'))
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	before := settlementPlatformQuotaJournalJSON(t, f)
	quota := NewUserPlatformQuotaRepository(f.client)
	require.NoError(t, quota.ResetExpiredWindow(ctx, f.session.UserID, service.PlatformOpenAI, "daily", time.Now()))
	changedLimit := 50.0
	require.NoError(t, quota.UpsertForUser(ctx, f.session.UserID, []UserPlatformQuotaRecord{{UserID: f.session.UserID, Platform: service.PlatformOpenAI, DailyLimitUSD: &changedLimit}}))
	// A current control token still permits legacy absolute snapshots; the private journal remains separate.
	start := time.Now().Add(-48 * time.Hour)
	liveQuota, quotaReadErr := quota.GetByUserPlatform(ctx, f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, quotaReadErr)
	require.NotNil(t, liveQuota)
	require.NoError(t, quota.BatchSnapshotUsage(ctx, []UserPlatformQuotaSnapshot{{QuotaRowID: liveQuota.QuotaRowID, ControlGeneration: liveQuota.ControlGeneration, UserID: f.session.UserID, Platform: service.PlatformOpenAI,
		DailyUsageUSD: 77, WeeklyUsageUSD: 88, MonthlyUsageUSD: 99, DailyWindowStart: start, WeeklyWindowStart: start, MonthlyWindowStart: start}}, time.Now()))
	require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
	require.Equal(t, before, settlementPlatformQuotaJournalJSON(t, f), "admin and legacy snapshot writes never rewrite the private journal")
	var actualDaily float64
	require.NoError(t, f.db.QueryRow(`SELECT daily_usage_usd FROM user_platform_quotas WHERE id=$1`, quotaID).Scan(&actualDaily))
	require.Equal(t, float64(77), actualDaily, "strict journal records never mutate the legacy counters")
	// Soft-delete and physical cleanup have no FK or replay side effects.
	require.NoError(t, quota.UpsertForUser(ctx, f.session.UserID, nil))
	_, err = f.db.Exec(`DELETE FROM user_platform_quotas WHERE id=$1`, quotaID)
	require.NoError(t, err)
	settlementPlatformQuotaRow(t, f, &changedLimit)
	require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
	require.Equal(t, before, settlementPlatformQuotaJournalJSON(t, f))
}

func TestUsageSettlementPlatformQuotaPostgresUnlimitedDecisionDoesNotFollowLaterConfiguration(t *testing.T) {
	f := settlementPlatformQuotaFixture(t)
	f.stage(t)
	ctx := context.Background()
	_, err := f.billing.Apply(ctx, f.event.Command)
	require.NoError(t, err)
	require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
	before := settlementPlatformQuotaJournalJSON(t, f)
	zero := 0.0
	settlementPlatformQuotaRow(t, f, &zero)
	require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
	require.Equal(t, before, settlementPlatformQuotaJournalJSON(t, f))
}

func TestUsageSettlementPlatformQuotaPostgresRecoversBothJournalCrashGaps(t *testing.T) {
	for _, crash := range []string{"after-billing", "after-journal"} {
		t.Run(crash, func(t *testing.T) {
			f := settlementPlatformQuotaFixture(t)
			limit := 5.0
			settlementPlatformQuotaRow(t, f, &limit)
			f.stage(t)
			ctx := context.Background()
			_, err := f.billing.Apply(ctx, f.event.Command)
			require.NoError(t, err)
			var before string
			if crash == "after-journal" {
				require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
				before = settlementPlatformQuotaJournalJSON(t, f)
			}
			var balance float64
			require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&balance))
			f.expireLease(t)
			require.NoError(t, NewAuthSessionRepository(f.db).RevokeRefresh(ctx, sessionHash("billing-session-token")))
			worker := service.NewUsageSettlementReconciler(NewUsageSettlementEventRepository(f.db), NewUsageBillingRepository(f.client, f.db), NewUsageLogRepository(f.client, f.db), &settlementPGInvalidator{})
			result, err := worker.Reconcile(ctx, 10)
			require.NoError(t, err)
			require.Equal(t, 1, result.Settled)
			require.Zero(t, result.Conflicts)
			var afterBalance float64
			require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&afterBalance))
			require.Equal(t, balance, afterBalance)
			var journalCount, usageCount int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_settlement_platform_quota_journal WHERE invocation_id=$1`, f.event.InvocationID).Scan(&journalCount))
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1 AND api_key_id=$2`, f.event.RequestID, f.event.APIKeyID).Scan(&usageCount))
			require.Equal(t, 1, journalCount)
			require.Equal(t, 1, usageCount)
			if before != "" {
				require.Equal(t, before, settlementPlatformQuotaJournalJSON(t, f))
			}
			var capturedAt, usageAt time.Time
			require.NoError(t, f.db.QueryRow(`SELECT j.captured_at,u.created_at FROM usage_settlement_platform_quota_journal j
				JOIN usage_logs u ON u.request_id=j.request_id AND u.api_key_id=j.api_key_id WHERE j.invocation_id=$1`, f.event.InvocationID).Scan(&capturedAt, &usageAt))
			require.True(t, f.event.CreatedAt.Equal(capturedAt))
			require.True(t, capturedAt.Equal(usageAt))
			result, err = worker.Reconcile(ctx, 10)
			require.NoError(t, err)
			require.Zero(t, result.Claimed)
		})
	}
}

func TestUsageSettlementPlatformQuotaPostgresV1RecoveryNeedsNoJournal(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	require.Equal(t, 1, f.event.SchemaVersion)
	f.stage(t)
	ctx := context.Background()
	// The new method is harmless for historical events and invents no platform.
	require.NoError(t, f.events.RecordPlatformQuota(ctx, f.event.InvocationID, settlementTestLease('A')))
	_, err := f.db.Exec(`DROP TABLE usage_settlement_platform_quota_journal`)
	require.NoError(t, err)
	_, err = f.billing.Apply(ctx, f.event.Command)
	require.NoError(t, err)
	f.expireLease(t)
	worker := service.NewUsageSettlementReconciler(f.events, f.billing, f.logs, &settlementPGInvalidator{})
	result, err := worker.Reconcile(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, result.Settled)
}
