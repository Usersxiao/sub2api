//go:build unit

package repository

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func ordinaryPlatformQuotaFixture(t *testing.T) *settlementEventPGFixture {
	t.Helper()
	f := settlementEventPostgresFixture(t, false)
	_, err := f.db.Exec(`UPDATE users SET balance=100 WHERE id=$1`, f.session.UserID)
	require.NoError(t, err)
	// Ordinary credentials have a separate lifecycle from the private ledger's
	// restricted accounting identity, including physical key removal.
	key, err := f.client.APIKey.Create().SetUserID(f.session.UserID).SetKey("sk-ordinary-platform-fixture").
		SetName("ordinary-platform-fixture").SetGroupID(f.in.GroupID).Save(context.Background())
	require.NoError(t, err)
	f.event.Command.APIKeyID = key.ID
	return f
}

func ordinaryPlatformQuotaCommand(t *testing.T, f *settlementEventPGFixture, suffix string, raw float64) *service.UsageBillingCommand {
	t.Helper()
	cmd := *f.event.Command
	cmd.RequestID, cmd.RequestFingerprint = "ordinary:"+suffix, ""
	cmd.BillingType = service.BillingTypeBalance
	cmd.BalanceCost, cmd.SubscriptionCost, cmd.APIKeyQuotaCost = raw, 0, raw
	cmd.SubscriptionID = nil
	cmd.Normalize()
	intent, err := service.FreezeUsageBillingOrdinaryPlatformIntent(&cmd, service.PlatformOpenAI, raw, false)
	require.NoError(t, err)
	cmd.OrdinaryPlatformQuota = intent
	return &cmd
}

func readOrdinaryPlatformQuotaJournal(t *testing.T, f *settlementEventPGFixture, cmd *service.UsageBillingCommand) *ordinaryPlatformQuotaJournal {
	t.Helper()
	j, err := scanOrdinaryPlatformQuotaJournal(f.db.QueryRow(`SELECT `+ordinaryPlatformQuotaJournalColumns+`
		FROM usage_billing_ordinary_platform_quota_journal WHERE request_id=$1 AND api_key_id=$2`, cmd.RequestID, cmd.APIKeyID))
	require.NoError(t, err)
	return j
}

func ordinaryPlatformQuotaJournalJSON(t *testing.T, f *settlementEventPGFixture, cmd *service.UsageBillingCommand) string {
	t.Helper()
	var raw string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(j)::text FROM usage_billing_ordinary_platform_quota_journal j
		WHERE request_id=$1 AND api_key_id=$2`, cmd.RequestID, cmd.APIKeyID).Scan(&raw))
	return raw
}

func ordinaryPlatformQuotaBalanceAndKey(t *testing.T, f *settlementEventPGFixture, cmd *service.UsageBillingCommand) (string, string) {
	t.Helper()
	var balance, used string
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, cmd.UserID).Scan(&balance))
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, cmd.APIKeyID).Scan(&used))
	return balance, used
}

func TestOrdinaryPlatformQuotaPostgresIndependentMoneyAndMeteringRounding(t *testing.T) {
	for _, raw := range []float64{0.00007812504, 0.00000000496, math.SmallestNonzeroFloat64} {
		t.Run(fmt.Sprintf("%.12g", raw), func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			limit := 5.0
			quotaID := settlementPlatformQuotaRow(t, f, &limit)
			cmd := ordinaryPlatformQuotaCommand(t, f, "rounding", raw)
			result, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, result.Applied)
			j := readOrdinaryPlatformQuotaJournal(t, f, cmd)
			require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformReceipt(cmd, result.OrdinaryPlatformQuotaReceipt))
			require.Equal(t, j.quotaRowID.Int64, result.OrdinaryPlatformQuotaReceipt.QuotaRowID)
			require.Equal(t, j.controlGeneration.Int64, result.OrdinaryPlatformQuotaReceipt.ControlGeneration)
			require.Equal(t, decimal.NewFromFloat(raw).String(), j.rawCostUSD)
			require.Equal(t, decimal.NewFromFloat(raw).Round(10).StringFixed(10), j.costUSD)
			require.Equal(t, cmd.OrdinaryPlatformQuota.IntentFingerprint, j.intentFingerprint)
			balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, decimal.NewFromInt(100).Sub(decimal.NewFromFloat(raw).Round(8)).StringFixed(8), balance)
			require.Equal(t, decimal.NewFromFloat(raw).Round(8).StringFixed(8), used)
			if raw == 0.00000000496 {
				require.Equal(t, "0.0000000050", j.costUSD)
				require.Equal(t, "100.00000000", balance, "financial rounding must use raw, not the scale-10 value")
			}
			if raw == math.SmallestNonzeroFloat64 {
				require.Equal(t, "zero_cost", j.disposition)
				require.False(t, j.quotaRowID.Valid)
			} else {
				require.Equal(t, "metered", j.disposition)
				require.Equal(t, quotaID, j.quotaRowID.Int64)
			}
			before := ordinaryPlatformQuotaJournalJSON(t, f, cmd)
			retry, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, retry.Applied)
			require.Equal(t, before, ordinaryPlatformQuotaJournalJSON(t, f, cmd))
		})
	}
}

func TestOrdinaryPlatformQuotaPostgresJournalAndMoneyFailuresRollbackTogether(t *testing.T) {
	for _, target := range []string{"journal", "money"} {
		t.Run(target, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := ordinaryPlatformQuotaCommand(t, f, "rollback", 1.25)
			table, event := "usage_billing_ordinary_platform_quota_journal", "INSERT"
			if target == "money" {
				table, event = "users", "UPDATE"
			}
			_, err := f.db.Exec(`CREATE FUNCTION ordinary_platform_fixture_failure() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'fixture ordinary transaction failure'; END; $$;
				CREATE TRIGGER ordinary_platform_fixture_failure BEFORE ` + event + ` ON ` + table + `
				FOR EACH ROW EXECUTE FUNCTION ordinary_platform_fixture_failure()`)
			require.NoError(t, err)
			_, err = f.billing.Apply(context.Background(), cmd)
			require.Error(t, err)
			balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, "100.00000000", balance)
			require.Equal(t, "0.00000000", used)
			var dedup, journal int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, cmd.RequestID).Scan(&dedup))
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_ordinary_platform_quota_journal`).Scan(&journal))
			require.Zero(t, dedup)
			require.Zero(t, journal)
			_, err = f.db.Exec(`DROP TRIGGER ordinary_platform_fixture_failure ON ` + table)
			require.NoError(t, err)
			result, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, result.Applied)
			balance, used = ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, "98.75000000", balance)
			require.Equal(t, "1.25000000", used)
			readOrdinaryPlatformQuotaJournal(t, f, cmd)
		})
	}
}

func TestOrdinaryPlatformQuotaPostgresConcurrentApplyHasOneMoneyAndJournalEffect(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := ordinaryPlatformQuotaCommand(t, f, "concurrent", 1.25)
	const concurrency = 12
	type outcome struct {
		result *service.UsageBillingApplyResult
		err    error
	}
	results := make(chan outcome, concurrency)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidate := *cmd
			<-start
			result, err := f.billing.Apply(context.Background(), &candidate)
			results <- outcome{result, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	applied := 0
	for o := range results {
		require.NoError(t, o.err)
		if o.result.Applied {
			applied++
		}
	}
	require.Equal(t, 1, applied)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_ordinary_platform_quota_journal`).Scan(&count))
	require.Equal(t, 1, count)
	balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
	require.Equal(t, "98.75000000", balance)
	require.Equal(t, "1.25000000", used)
}

func TestOrdinaryPlatformQuotaPostgresFreezesAllDispositions(t *testing.T) {
	for _, kind := range []string{"metered", "zero-limit", "missing", "all-null", "soft-deleted", "zero-cost", "subscription", "subscription-zero"} {
		t.Run(kind, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
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
			raw := 1.25
			if kind == "zero-cost" || kind == "subscription-zero" {
				raw = 0
			}
			cmd := ordinaryPlatformQuotaCommand(t, f, kind, raw)
			if strings.HasPrefix(kind, "subscription") {
				now := time.Now()
				sub, err := f.client.UserSubscription.Create().SetUserID(f.session.UserID).SetGroupID(f.in.GroupID).
					SetStartsAt(now).SetExpiresAt(now.Add(time.Hour)).Save(context.Background())
				require.NoError(t, err)
				cmd.SubscriptionID = &sub.ID
				cmd.BillingType = service.BillingTypeSubscription
				cmd.BalanceCost, cmd.SubscriptionCost, cmd.RequestFingerprint = 0, raw, ""
				cmd.Normalize()
				cmd.OrdinaryPlatformQuota, err = service.FreezeUsageBillingOrdinaryPlatformIntent(cmd, service.PlatformOpenAI, raw, true)
				require.NoError(t, err)
			}
			result, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, result.Applied)
			j := readOrdinaryPlatformQuotaJournal(t, f, cmd)
			require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformReceipt(cmd, result.OrdinaryPlatformQuotaReceipt))
			require.Equal(t, j.quotaRowID.Int64, result.OrdinaryPlatformQuotaReceipt.QuotaRowID)
			require.Equal(t, j.controlGeneration.Int64, result.OrdinaryPlatformQuotaReceipt.ControlGeneration)
			switch kind {
			case "metered", "zero-limit":
				require.Equal(t, "metered", j.disposition)
				require.Equal(t, quotaID, j.quotaRowID.Int64)
				require.Equal(t, fmt.Sprintf("%.10f", limit), j.dailyLimit.String)
			case "zero-cost":
				require.Equal(t, "zero_cost", j.disposition)
				require.False(t, j.quotaRowID.Valid)
			case "subscription", "subscription-zero":
				require.Equal(t, "subscription_exempt", j.disposition)
				require.False(t, j.quotaRowID.Valid)
			default:
				require.Equal(t, "unlimited", j.disposition)
				require.Equal(t, kind == "all-null", j.quotaRowID.Valid)
			}
			before := ordinaryPlatformQuotaJournalJSON(t, f, cmd)
			_, err = f.db.Exec(`UPDATE user_platform_quotas SET daily_limit_usd=99,deleted_at=clock_timestamp() WHERE user_id=$1`, cmd.UserID)
			require.NoError(t, err)
			retry, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, retry.Applied)
			require.Equal(t, before, ordinaryPlatformQuotaJournalJSON(t, f, cmd))
		})
	}
}

func TestOrdinaryPlatformQuotaPostgresRetainsExactSnapshotAcrossLegacyMutations(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	limit := 5.0
	quotaID := settlementPlatformQuotaRow(t, f, &limit)
	values := []string{"999999999.1234567891", "888888888.9876543219", "777777777.0123456789",
		"666666666.1357924681", "555555555.2468135792", "444444444.9999999999"}
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET daily_limit_usd=$1,weekly_limit_usd=$2,monthly_limit_usd=$3,
		daily_usage_usd=$4,weekly_usage_usd=$5,monthly_usage_usd=$6 WHERE id=$7`,
		values[0], values[1], values[2], values[3], values[4], values[5], quotaID)
	require.NoError(t, err)
	cmd := ordinaryPlatformQuotaCommand(t, f, "snapshot", 1.25)
	_, err = f.billing.Apply(context.Background(), cmd)
	require.NoError(t, err)
	j := readOrdinaryPlatformQuotaJournal(t, f, cmd)
	require.Equal(t, values, []string{j.dailyLimit.String, j.weeklyLimit.String, j.monthlyLimit.String,
		j.dailyUsage.String, j.weeklyUsage.String, j.monthlyUsage.String})
	require.Equal(t, quotaID, j.quotaRowID.Int64)
	before := ordinaryPlatformQuotaJournalJSON(t, f, cmd)
	quotaRepo := NewUserPlatformQuotaRepository(f.client)
	for _, window := range []string{"daily", "weekly", "monthly"} {
		require.NoError(t, quotaRepo.ResetExpiredWindow(context.Background(), cmd.UserID, service.PlatformOpenAI, window, time.Now()))
	}
	stale := time.Now().Add(-48 * time.Hour)
	liveQuota, quotaReadErr := quotaRepo.GetByUserPlatform(context.Background(), cmd.UserID, service.PlatformOpenAI)
	require.NoError(t, quotaReadErr)
	require.NotNil(t, liveQuota)
	require.NoError(t, quotaRepo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{{QuotaRowID: liveQuota.QuotaRowID, ControlGeneration: liveQuota.ControlGeneration, UserID: cmd.UserID,
		Platform: service.PlatformOpenAI, DailyUsageUSD: 91, WeeklyUsageUSD: 92, MonthlyUsageUSD: 93,
		DailyWindowStart: stale, WeeklyWindowStart: stale, MonthlyWindowStart: stale}}, time.Now()))
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET daily_limit_usd=7,deleted_at=clock_timestamp() WHERE id=$1`, quotaID)
	require.NoError(t, err)
	newID := settlementPlatformQuotaRow(t, f, &limit)
	require.NotEqual(t, quotaID, newID)
	_, err = f.db.Exec(`DELETE FROM user_platform_quotas WHERE id=$1`, quotaID)
	require.NoError(t, err)
	require.Equal(t, before, ordinaryPlatformQuotaJournalJSON(t, f, cmd))
	next := ordinaryPlatformQuotaCommand(t, f, "new-incarnation", 0.5)
	_, err = f.billing.Apply(context.Background(), next)
	require.NoError(t, err)
	require.Equal(t, newID, readOrdinaryPlatformQuotaJournal(t, f, next).quotaRowID.Int64)
}

func TestOrdinaryPlatformQuotaPostgresOldDedupCannotInventPlatformHistory(t *testing.T) {
	for _, tier := range []string{"hot", "archive", "both", "conflicting-archive"} {
		t.Run(tier, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := ordinaryPlatformQuotaCommand(t, f, tier, 1.25)
			if tier != "archive" {
				_, err := f.db.Exec(`INSERT INTO usage_billing_dedup(request_id,api_key_id,request_fingerprint) VALUES($1,$2,$3)`, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint)
				require.NoError(t, err)
			}
			if tier != "hot" {
				fp := cmd.RequestFingerprint
				if tier == "conflicting-archive" {
					fp = sessionHash("conflicting old archive")
				}
				_, err := f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
					VALUES($1,$2,$3,clock_timestamp())`, cmd.RequestID, cmd.APIKeyID, fp)
				require.NoError(t, err)
			}
			_, err := f.billing.Apply(context.Background(), cmd)
			if tier == "conflicting-archive" {
				require.ErrorIs(t, err, service.ErrUsageBillingRequestConflict)
			} else {
				require.ErrorIs(t, err, service.ErrUsageBillingOrdinaryPlatformCoverageGap)
				legacy := *cmd
				legacy.OrdinaryPlatformQuota = nil
				result, err := f.billing.Apply(context.Background(), &legacy)
				require.NoError(t, err)
				require.False(t, result.Applied)
			}
			var count int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_ordinary_platform_quota_journal`).Scan(&count))
			require.Zero(t, count)
			if tier == "archive" {
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup`).Scan(&count))
				require.Zero(t, count, "a temporary hot claim must roll back")
			}
			balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, "100.00000000", balance)
			require.Equal(t, "0.00000000", used)
		})
	}
}

func TestOrdinaryPlatformQuotaPostgresPermanentProofSurvivesDedupCleanupAndBlocksBatchReuse(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := ordinaryPlatformQuotaCommand(t, f, "permanent", 1.25)
	_, err := f.billing.Apply(context.Background(), cmd)
	require.NoError(t, err)
	before := ordinaryPlatformQuotaJournalJSON(t, f, cmd)
	_, err = f.db.Exec(`DELETE FROM usage_billing_dedup; DELETE FROM usage_billing_dedup_archive`)
	require.NoError(t, err)
	for _, withIntent := range []bool{true, false} {
		replay := *cmd
		if !withIntent {
			replay.OrdinaryPlatformQuota = nil
		}
		result, err := f.billing.Apply(context.Background(), &replay)
		require.NoError(t, err)
		require.False(t, result.Applied)
	}
	for _, operation := range []string{"reserve", "capture", "release"} {
		batch := &service.BatchImageBalanceHoldCommand{RequestID: cmd.RequestID, APIKeyID: cmd.APIKeyID,
			UserID: cmd.UserID, BatchID: "foreign-operation", HoldAmount: 1, ActualAmount: 0.5,
			RequestFingerprint: cmd.RequestFingerprint}
		switch operation {
		case "reserve":
			_, err = f.billing.ReserveBatchImageBalance(context.Background(), batch)
		case "capture":
			_, err = f.billing.CaptureBatchImageBalance(context.Background(), batch)
		case "release":
			_, err = f.billing.ReleaseBatchImageBalance(context.Background(), batch)
		}
		require.ErrorIs(t, err, service.ErrBatchImageBalanceLifecycleInput, "noncanonical batch commands fail before any money")
	}
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup`).Scan(&count))
	require.Zero(t, count)
	require.Equal(t, before, ordinaryPlatformQuotaJournalJSON(t, f, cmd))
	balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
	require.Equal(t, "98.75000000", balance)
	require.Equal(t, "1.25000000", used)
}

func TestOrdinaryPlatformQuotaPostgresVerifiesEveryExistingDedupTier(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := ordinaryPlatformQuotaCommand(t, f, "all-proofs", 1.25)
	_, err := f.billing.Apply(context.Background(), cmd)
	require.NoError(t, err)
	_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
		SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup`)
	require.NoError(t, err)
	for _, tier := range []string{"usage_billing_dedup", "usage_billing_dedup_archive"} {
		_, err = f.db.Exec(`UPDATE `+tier+` SET request_fingerprint=$1 WHERE request_id=$2`, sessionHash("conflicting proof"), cmd.RequestID)
		require.NoError(t, err)
		_, err = f.billing.Apply(context.Background(), cmd)
		require.ErrorIs(t, err, service.ErrUsageBillingRequestConflict)
		_, err = f.db.Exec(`UPDATE `+tier+` SET request_fingerprint=$1 WHERE request_id=$2`, cmd.RequestFingerprint, cmd.RequestID)
		require.NoError(t, err)
		result, err := f.billing.Apply(context.Background(), cmd)
		require.NoError(t, err)
		require.False(t, result.Applied)
	}
}

func TestOrdinaryPlatformQuotaPostgresRejectsChangedIndependentIntentAndScope(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	cmd := ordinaryPlatformQuotaCommand(t, f, "content", 0.00007812504)
	_, err := f.billing.Apply(context.Background(), cmd)
	require.NoError(t, err)
	before := ordinaryPlatformQuotaJournalJSON(t, f, cmd)
	for _, kind := range []string{"platform", "raw-cost", "user", "billing-fingerprint"} {
		changed := *cmd
		changed.OrdinaryPlatformQuota = nil
		switch kind {
		case "platform":
			changed.OrdinaryPlatformQuota, err = service.FreezeUsageBillingOrdinaryPlatformIntent(&changed, service.PlatformGemini, 0.00007812504, false)
			require.NoError(t, err)
		case "raw-cost":
			// Same scale-8 charge and the same original fingerprint, different
			// raw metering source: the independent intent must detect it.
			changed.OrdinaryPlatformQuota, err = service.FreezeUsageBillingOrdinaryPlatformIntent(&changed, service.PlatformOpenAI, 0.00007812503, false)
			require.NoError(t, err)
		case "user":
			changed.UserID++
		case "billing-fingerprint":
			changed.RequestFingerprint = sessionHash("different billing fingerprint")
		}
		_, err = f.billing.Apply(context.Background(), &changed)
		require.ErrorIs(t, err, service.ErrUsageBillingRequestConflict, kind)
	}
	require.Equal(t, before, ordinaryPlatformQuotaJournalJSON(t, f, cmd))
}

func TestOrdinaryPlatformQuotaPostgresObservesQuotaBeforeTakingConsumptionClock(t *testing.T) {
	f := ordinaryPlatformQuotaFixture(t)
	limit := 5.0
	quotaID := settlementPlatformQuotaRow(t, f, &limit)
	cmd := ordinaryPlatformQuotaCommand(t, f, "admin-race", 1.25)
	blocker, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer blocker.Rollback()
	_, err = blocker.Exec(`UPDATE user_platform_quotas SET daily_usage_usd=0,daily_limit_usd=11 WHERE id=$1`, quotaID)
	require.NoError(t, err)
	var cutoff time.Time
	require.NoError(t, blocker.QueryRow(`UPDATE user_platform_quota_client_resets SET daily_reset_at=clock_timestamp()
		WHERE quota_row_id=$1 RETURNING daily_reset_at`, quotaID).Scan(&cutoff))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := f.billing.Apply(ctx, cmd)
		result <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid()
			AND state='active' AND wait_event_type='Lock' AND query LIKE '%FROM user_platform_quotas%FOR SHARE%')`).Scan(&waiting)
		return err == nil && waiting
	}, 3*time.Second, 20*time.Millisecond, "billing must wait for the actual quota-row observation")
	require.NoError(t, blocker.Commit())
	require.NoError(t, <-result)
	j := readOrdinaryPlatformQuotaJournal(t, f, cmd)
	require.Equal(t, quotaID, j.quotaRowID.Int64)
	require.Equal(t, "11.0000000000", j.dailyLimit.String)
	require.Equal(t, "0.0000000000", j.dailyUsage.String)
	require.True(t, j.capturedAt.After(cutoff), "the clock is read after the serialized reset observation")
}

func TestOrdinaryPlatformQuotaPostgresDeletedCredentialKeepsBillingAndPermanentProof(t *testing.T) {
	for _, deletion := range []string{"before-billing", "after-billing"} {
		t.Run(deletion, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			cmd := ordinaryPlatformQuotaCommand(t, f, deletion, 1.25)
			if deletion == "before-billing" {
				require.NoError(t, f.client.APIKey.DeleteOneID(cmd.APIKeyID).Exec(context.Background()))
			}
			result, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, result.Applied, "key counters may be skipped while existing balance billing remains")
			before := ordinaryPlatformQuotaJournalJSON(t, f, cmd)
			if deletion == "after-billing" {
				require.NoError(t, f.client.APIKey.DeleteOneID(cmd.APIKeyID).Exec(context.Background()))
			}
			_, err = f.db.Exec(`DELETE FROM usage_billing_dedup; DELETE FROM usage_billing_dedup_archive`)
			require.NoError(t, err)
			cmd.OrdinaryPlatformQuota = nil
			result, err = f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, result.Applied)
			require.Equal(t, before, ordinaryPlatformQuotaJournalJSON(t, f, cmd))
			var balance string
			require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, cmd.UserID).Scan(&balance))
			require.Equal(t, "98.75000000", balance)
		})
	}
}
