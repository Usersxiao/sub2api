//go:build unit

package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func ordinaryQuotaDeliveryWriter(t *testing.T, repo UserPlatformQuotaRepository) service.UserPlatformQuotaOrdinaryDeliveryRepository {
	t.Helper()
	writer, ok := NewUserPlatformQuotaServiceAdapter(repo).(service.UserPlatformQuotaOrdinaryDeliveryRepository)
	require.True(t, ok)
	return writer
}

func ordinaryPlatformQuotaDeliveryFixture(t *testing.T) (*settlementEventPGFixture, UserPlatformQuotaRepository, *service.UsageBillingCommand, *service.UsageBillingOrdinaryPlatformReceipt) {
	t.Helper()
	f, authority := quotaWriterAuthorityFixture(t)
	limit := 100.0
	repo := NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, authority)
	require.NoError(t, repo.UpsertForUser(context.Background(), f.session.UserID, []UserPlatformQuotaRecord{{Platform: service.PlatformOpenAI, DailyLimitUSD: &limit}}))
	cmd := ordinaryPlatformQuotaCommand(t, f, "db-delivery", 0.25)
	cmd.OrdinaryPlatformQuotaDeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
	applied, err := f.billing.Apply(context.Background(), cmd)
	require.NoError(t, err)
	require.True(t, applied.Applied)
	receipt := applied.OrdinaryPlatformQuotaReceipt
	require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt))
	return f, repo, cmd, receipt
}

func initializeQuotaWriterAuthorityFixture(t *testing.T, f *settlementEventPGFixture) service.UserPlatformQuotaWriterAuthority {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "263_user_platform_quota_writer_authority.sql"))
	require.NoError(t, err)
	_, err = f.db.Exec(string(raw))
	require.NoError(t, err)
	authority, err := NewUserPlatformQuotaWriterAuthorityRepository(f.db).InitializeUserPlatformQuotaWriterAuthority(context.Background(), service.UserPlatformQuotaWriterModeDB)
	require.NoError(t, err)
	f.billing = NewUsageBillingRepositoryWithWriterAuthority(f.client, f.db, authority)
	return authority
}

func quotaWriterAuthorityFixture(t *testing.T) (*settlementEventPGFixture, service.UserPlatformQuotaWriterAuthority) {
	t.Helper()
	f := ordinaryPlatformQuotaFixture(t)
	return f, initializeQuotaWriterAuthorityFixture(t, f)
}

// Direct owner fixture setup remains explicit; production legacy writers never
// receive this scope or inherit it from a pooled connection.
func ordinaryQuotaDeliveryFixtureSQL(t *testing.T, f *settlementEventPGFixture, receipt *service.UsageBillingOrdinaryPlatformReceipt, query string, args ...any) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockUserPlatformQuotaWriterAuthority(ctx, tx, service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeDB, Epoch: receipt.WriterEpoch}, quotaWriterActionAdminReset))
	_, err = tx.ExecContext(ctx, query, args...)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func ordinaryQuotaDeliveryJSON(t *testing.T, f *settlementEventPGFixture, cmd *service.UsageBillingCommand) string {
	t.Helper()
	var value string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(d)::text FROM ordinary_platform_quota_deliveries d WHERE request_id=$1 AND api_key_id=$2`, cmd.RequestID, cmd.APIKeyID).Scan(&value))
	return value
}

func TestOrdinaryPlatformQuotaDeliveryPostgresBirthPendingLookupAndPermanentConsumption(t *testing.T) {
	f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
	writer := ordinaryQuotaDeliveryWriter(t, repo)
	before := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
	birth := ordinaryQuotaDeliveryJSON(t, f, cmd)
	for range 2 {
		pending, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
		require.NoError(t, err)
		require.NotNil(t, pending)
		require.Equal(t, service.OrdinaryPlatformDeliveryPending, pending.State)
		require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, *pending, true))
		require.False(t, pending.Replayed)
		require.Equal(t, before, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
		require.Equal(t, birth, ordinaryQuotaDeliveryJSON(t, f, cmd))
	}
	first, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.NoError(t, err)
	require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, first, false))
	require.Equal(t, service.QuotaIncrementApplied, first.Outcome.Status)
	require.Equal(t, service.QuotaWindowAll, first.Outcome.AppliedWindows)
	require.False(t, first.Replayed)
	row, err := repo.GetByUserPlatform(context.Background(), cmd.UserID, receipt.Intent.Platform)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, 0.25, row.DailyUsageUSD)
	require.Equal(t, 0.25, row.WeeklyUsageUSD)
	require.Equal(t, 0.25, row.MonthlyUsageUSD)
	after := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
	completed := ordinaryQuotaDeliveryJSON(t, f, cmd)
	for range 2 {
		replay, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
		require.NoError(t, err)
		require.True(t, replay.Replayed)
		want := first
		want.Replayed = true
		require.Equal(t, want, replay)
		read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
		require.NoError(t, err)
		require.NotNil(t, read)
		require.Equal(t, replay, *read)
		require.Equal(t, after, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
		require.Equal(t, completed, ordinaryQuotaDeliveryJSON(t, f, cmd))
	}
	money, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
	require.Equal(t, "99.75000000", money)
	require.Equal(t, "0.25000000", used)
	duplicate, err := f.billing.Apply(context.Background(), cmd)
	require.NoError(t, err)
	require.False(t, duplicate.Applied)
	require.Nil(t, duplicate.OrdinaryPlatformQuotaReceipt)
	require.Equal(t, completed, ordinaryQuotaDeliveryJSON(t, f, cmd))
}

func TestOrdinaryPlatformQuotaDeliveryPostgresOldAndFlusherSourcesNeverAcquireEligibility(t *testing.T) {
	for _, kind := range []string{"old-false-delivered", "true-flusher-delivered", "historical-null-generation"} {
		t.Run(kind, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			limit := 100.0
			settlementPlatformQuotaRow(t, f, &limit)
			repo := NewUserPlatformQuotaRepository(f.client)
			cmd := ordinaryPlatformQuotaCommand(t, f, "no-delivery-birth", 0.25)
			applied, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, applied.Applied)
			receipt := *applied.OrdinaryPlatformQuotaReceipt
			require.Empty(t, receipt.DeliveryMode)
			if kind == "historical-null-generation" {
				_, err = f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal SET control_generation=NULL`)
				require.NoError(t, err)
			} else {
				// Existing writers may already have delivered this cost without a
				// permanent marker. New eligibility must never be inferred from it.
				binding, err := receipt.UsageBinding()
				require.NoError(t, err)
				if kind == "true-flusher-delivered" {
					require.NoError(t, repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{{QuotaRowID: binding.QuotaRowID, ControlGeneration: binding.ControlGeneration,
						UserID: binding.UserID, Platform: binding.Platform, DailyUsageUSD: 0.25, WeeklyUsageUSD: 0.25, MonthlyUsageUSD: 0.25,
						DailyWindowStart: timezone.StartOfDay(binding.CapturedAt), WeeklyWindowStart: timezone.StartOfWeek(binding.CapturedAt), MonthlyWindowStart: binding.CapturedAt}}, time.Now().UTC()))
				} else {
					result, err := boundQuotaWriter(t, repo).IncrementBoundUsage(context.Background(), binding)
					require.NoError(t, err)
					require.Equal(t, service.QuotaIncrementApplied, result.Status)
				}
			}
			before := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
			initializeQuotaWriterAuthorityFixture(t, f)
			cmd.OrdinaryPlatformQuotaDeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
			receipt.DeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, &receipt)
			if kind == "historical-null-generation" {
				require.ErrorIs(t, err, service.ErrUsageBillingOrdinaryPlatformDeliveryConflict)
			} else {
				require.ErrorIs(t, err, service.ErrUsageBillingOrdinaryPlatformDeliveryMissing)
			}
			require.Equal(t, service.UsageBillingOrdinaryPlatformDeliveryResult{}, result)
			found, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, &receipt)
			if kind == "historical-null-generation" {
				require.ErrorIs(t, err, service.ErrUsageBillingOrdinaryPlatformDeliveryConflict)
			} else {
				require.NoError(t, err)
			}
			require.Nil(t, found)
			duplicate, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.False(t, duplicate.Applied)
			require.Nil(t, duplicate.OrdinaryPlatformQuotaReceipt)
			var count int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM ordinary_platform_quota_deliveries`).Scan(&count))
			require.Zero(t, count)
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresEligibilityRequiresFreshMeteredDBMode(t *testing.T) {
	for _, kind := range []string{"unlimited", "all-null", "zero-cost", "tiny-zero-cost", "subscription", "invalid-mode", "mode-without-intent"} {
		t.Run(kind, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			if kind == "all-null" {
				settlementPlatformQuotaRow(t, f, nil)
			} else if kind != "unlimited" {
				limit := 100.0
				settlementPlatformQuotaRow(t, f, &limit)
			}
			initializeQuotaWriterAuthorityFixture(t, f)
			raw := 0.25
			if kind == "zero-cost" {
				raw = 0
			}
			if kind == "tiny-zero-cost" {
				raw = 1e-12
			}
			cmd := ordinaryPlatformQuotaCommand(t, f, "cohort", raw)
			cmd.OrdinaryPlatformQuotaDeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
			if kind == "subscription" {
				now := time.Now()
				sub, err := f.client.UserSubscription.Create().SetUserID(f.session.UserID).SetGroupID(f.in.GroupID).SetStartsAt(now).SetExpiresAt(now.Add(time.Hour)).Save(context.Background())
				require.NoError(t, err)
				cmd.SubscriptionID = &sub.ID
				cmd.BillingType = service.BillingTypeSubscription
				cmd.BalanceCost, cmd.SubscriptionCost, cmd.RequestFingerprint = 0, raw, ""
				cmd.Normalize()
				cmd.OrdinaryPlatformQuota, err = service.FreezeUsageBillingOrdinaryPlatformIntent(cmd, service.PlatformOpenAI, raw, true)
				require.NoError(t, err)
			}
			if kind == "invalid-mode" {
				cmd.OrdinaryPlatformQuotaDeliveryMode = "unknown"
			}
			if kind == "mode-without-intent" {
				cmd.OrdinaryPlatformQuota = nil
			}
			result, err := f.billing.Apply(context.Background(), cmd)
			if kind == "invalid-mode" || kind == "mode-without-intent" {
				require.ErrorIs(t, err, service.ErrUsageBillingOrdinaryPlatformDelivery)
				require.Nil(t, result)
				money, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
				require.Equal(t, "100.00000000", money)
				require.Equal(t, "0.00000000", used)
			} else {
				require.NoError(t, err)
				require.True(t, result.Applied)
				require.NotNil(t, result.OrdinaryPlatformQuotaReceipt)
				require.Empty(t, result.OrdinaryPlatformQuotaReceipt.DeliveryMode)
				require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformReceipt(cmd, result.OrdinaryPlatformQuotaReceipt))
			}
			var count int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM ordinary_platform_quota_deliveries`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresBirthAndMoneyRollbackTogether(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		name := "birth-insert"
		if deferred {
			name = "birth-deferred-commit"
		}
		t.Run(name, func(t *testing.T) {
			f := ordinaryPlatformQuotaFixture(t)
			limit := 100.0
			settlementPlatformQuotaRow(t, f, &limit)
			initializeQuotaWriterAuthorityFixture(t, f)
			cmd := ordinaryPlatformQuotaCommand(t, f, "birth-rollback", 0.25)
			cmd.OrdinaryPlatformQuotaDeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
			_, err := f.db.Exec(`CREATE FUNCTION ordinary_delivery_fixture_birth_failure() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'fixture delivery birth failure'; END; $$`)
			require.NoError(t, err)
			trigger := `CREATE TRIGGER ordinary_delivery_fixture_birth_failure BEFORE INSERT ON ordinary_platform_quota_deliveries
				FOR EACH ROW EXECUTE FUNCTION ordinary_delivery_fixture_birth_failure()`
			if deferred {
				trigger = `CREATE CONSTRAINT TRIGGER ordinary_delivery_fixture_birth_failure AFTER INSERT ON ordinary_platform_quota_deliveries
				DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ordinary_delivery_fixture_birth_failure()`
			}
			_, err = f.db.Exec(trigger)
			require.NoError(t, err)
			result, err := f.billing.Apply(context.Background(), cmd)
			require.Error(t, err)
			require.Nil(t, result)
			money, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, "100.00000000", money)
			require.Equal(t, "0.00000000", used)
			var count int
			require.NoError(t, f.db.QueryRow(`SELECT (SELECT count(*) FROM usage_billing_dedup)+(SELECT count(*) FROM usage_billing_ordinary_platform_quota_journal)+(SELECT count(*) FROM ordinary_platform_quota_deliveries)`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresImmutableBirthAndNonNullTerminalOutcome(t *testing.T) {
	f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
	before := ordinaryQuotaDeliveryJSON(t, f, cmd)
	for _, change := range []string{"source_fingerprint=repeat('b',64)", "quota_row_id=quota_row_id+1", "control_generation=control_generation+1", "captured_at=captured_at+INTERVAL '1 microsecond'",
		"raw_cost_usd='0.5',cost_usd=0.5", "created_at=created_at+INTERVAL '1 microsecond'", "writer_epoch=NULL", "writer_epoch=writer_epoch+1", "state='completed',completed_at=clock_timestamp(),result_status=NULL"} {
		_, err := f.db.Exec(`UPDATE ordinary_platform_quota_deliveries SET ` + change)
		require.Error(t, err, change)
		require.Equal(t, before, ordinaryQuotaDeliveryJSON(t, f, cmd))
	}
	_, err := f.db.Exec(`DELETE FROM ordinary_platform_quota_deliveries`)
	require.Error(t, err)
	_, err = f.db.Exec(`DELETE FROM usage_billing_ordinary_platform_quota_journal`)
	require.Error(t, err, "birth retains its permanent source")
	_, err = ordinaryQuotaDeliveryWriter(t, repo).ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.NoError(t, err)
	completed := ordinaryQuotaDeliveryJSON(t, f, cmd)
	_, err = f.db.Exec(`UPDATE ordinary_platform_quota_deliveries SET state='completed',result_status='superseded',applied_windows=0`)
	require.Error(t, err)
	_, err = f.db.Exec(`DELETE FROM ordinary_platform_quota_deliveries`)
	require.Error(t, err)
	require.Equal(t, completed, ordinaryQuotaDeliveryJSON(t, f, cmd))
}

func TestOrdinaryPlatformQuotaDeliveryPostgresRejectsCallerAndStoredSourceTampering(t *testing.T) {
	for _, change := range []string{"receipt-qid", "receipt-generation", "receipt-clock", "receipt-mode", "receipt-epoch", "command-mode", "command-intent", "missing-source",
		"source-qid", "source-generation", "source-clock", "source-amount"} {
		t.Run(change, func(t *testing.T) {
			f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
			originalCmd, originalReceipt := cmd, receipt
			copiedCmd, copiedReceipt := *cmd, *receipt
			cmd, receipt = &copiedCmd, &copiedReceipt
			switch change {
			case "receipt-qid":
				receipt.QuotaRowID++
			case "receipt-generation":
				receipt.ControlGeneration++
			case "receipt-clock":
				receipt.CapturedAt = receipt.CapturedAt.Add(time.Microsecond)
			case "receipt-mode":
				receipt.DeliveryMode = ""
			case "receipt-epoch":
				receipt.WriterEpoch++
			case "command-mode":
				cmd.OrdinaryPlatformQuotaDeliveryMode = ""
			case "command-intent", "missing-source":
				platform := service.PlatformGemini
				if change == "missing-source" {
					cmd.RequestID = "ordinary:nonexistent-delivery"
					platform = service.PlatformOpenAI
				}
				intent, err := service.FreezeUsageBillingOrdinaryPlatformIntent(cmd, platform, 0.25, false)
				require.NoError(t, err)
				cmd.OrdinaryPlatformQuota = intent
				receipt.Intent = *intent
			case "source-qid":
				_, err := f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal SET quota_row_id=quota_row_id+1`)
				require.NoError(t, err)
			case "source-generation":
				_, err := f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal SET control_generation=control_generation+1`)
				require.NoError(t, err)
			case "source-clock":
				_, err := f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal SET captured_at=captured_at-INTERVAL '1 microsecond'`)
				require.NoError(t, err)
			case "source-amount":
				_, err := f.db.Exec(`UPDATE usage_billing_ordinary_platform_quota_journal SET raw_cost_usd='0.5',cost_usd=0.5`)
				require.NoError(t, err)
			}
			before := boundQuotaRecordJSON(t, f.db, originalReceipt.QuotaRowID)
			birth := ordinaryQuotaDeliveryJSON(t, f, originalCmd)
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
			require.Error(t, err)
			require.Equal(t, service.UsageBillingOrdinaryPlatformDeliveryResult{}, result)
			read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
			require.Error(t, err)
			require.Nil(t, read)
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, originalReceipt.QuotaRowID))
			require.Equal(t, birth, ordinaryQuotaDeliveryJSON(t, f, originalCmd))
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresConcurrentConsumersHaveOneEffectPerBirth(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		name := "same-source"
		if distinct {
			name = "different-sources"
		}
		t.Run(name, func(t *testing.T) {
			f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			const workers = 8
			commands := make([]*service.UsageBillingCommand, workers)
			receipts := make([]*service.UsageBillingOrdinaryPlatformReceipt, workers)
			for i := range workers {
				commands[i], receipts[i] = cmd, receipt
				if distinct && i > 0 {
					commands[i] = ordinaryPlatformQuotaCommand(t, f, fmt.Sprintf("delivery-%d", i), 0.25)
					commands[i].OrdinaryPlatformQuotaDeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
					result, err := f.billing.Apply(context.Background(), commands[i])
					require.NoError(t, err)
					require.True(t, result.Applied)
					receipts[i] = result.OrdinaryPlatformQuotaReceipt
				}
			}
			// A represented month anchor preceding every source avoids confusing
			// this identity test with the separate initial-anchor ordering gap.
			ordinaryQuotaDeliveryFixtureSQL(t, f, receipt, `UPDATE user_platform_quotas SET daily_window_start=$2,weekly_window_start=$3,monthly_window_start=$4 WHERE id=$1`,
				receipt.QuotaRowID, timezone.StartOfDay(receipt.CapturedAt), timezone.StartOfWeek(receipt.CapturedAt), receipt.CapturedAt.Add(-time.Hour))
			type outcome struct {
				result service.UsageBillingOrdinaryPlatformDeliveryResult
				err    error
			}
			results := make(chan outcome, workers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), commands[i], receipts[i])
					results <- outcome{result, err}
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			fresh := 0
			for got := range results {
				require.NoError(t, got.err)
				require.Contains(t, []service.UserPlatformQuotaIncrementStatus{service.QuotaIncrementApplied, service.QuotaIncrementPartial}, got.result.Outcome.Status)
				if !got.result.Replayed {
					fresh++
				}
			}
			wantFresh := 1
			if distinct {
				wantFresh = workers
			}
			require.Equal(t, wantFresh, fresh)
			row, err := repo.GetByUserPlatform(context.Background(), cmd.UserID, receipt.Intent.Platform)
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, float64(wantFresh)*0.25, row.MonthlyUsageUSD)
			require.Greater(t, row.DailyUsageUSD, 0.0)
			require.LessOrEqual(t, row.DailyUsageUSD, row.MonthlyUsageUSD)
			require.Greater(t, row.WeeklyUsageUSD, 0.0)
			require.LessOrEqual(t, row.WeeklyUsageUSD, row.MonthlyUsageUSD)
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresCounterAndTerminalFailuresRollbackToPending(t *testing.T) {
	for _, kind := range []string{"counter-update", "marker-update", "deferred-commit"} {
		t.Run(kind, func(t *testing.T) {
			f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
			before := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
			birth := ordinaryQuotaDeliveryJSON(t, f, cmd)
			_, err := f.db.Exec(`CREATE FUNCTION ordinary_delivery_fixture_failure() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'fixture delivery completion failure'; END; $$`)
			require.NoError(t, err)
			table := "ordinary_platform_quota_deliveries"
			if kind == "counter-update" {
				table = "user_platform_quotas"
			}
			trigger := `CREATE TRIGGER ordinary_delivery_fixture_failure BEFORE UPDATE ON ` + table + ` FOR EACH ROW EXECUTE FUNCTION ordinary_delivery_fixture_failure()`
			if kind == "deferred-commit" {
				trigger = `CREATE CONSTRAINT TRIGGER ordinary_delivery_fixture_failure AFTER UPDATE ON ` + table + `
				DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ordinary_delivery_fixture_failure()`
			}
			_, err = f.db.Exec(trigger)
			require.NoError(t, err)
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
			require.Error(t, err)
			require.Equal(t, service.UsageBillingOrdinaryPlatformDeliveryResult{}, result)
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
			require.Equal(t, birth, ordinaryQuotaDeliveryJSON(t, f, cmd))
			pending, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
			require.NoError(t, err)
			require.NotNil(t, pending)
			require.Equal(t, service.OrdinaryPlatformDeliveryPending, pending.State)
			_, err = f.db.Exec(`DROP TRIGGER ordinary_delivery_fixture_failure ON ` + table)
			require.NoError(t, err)
			result, err = writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
			require.NoError(t, err)
			require.False(t, result.Replayed)
			require.Equal(t, service.QuotaIncrementApplied, result.Outcome.Status)
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresPartialObsoleteAndSupersededRemainTerminal(t *testing.T) {
	for _, kind := range []string{"partial", "obsolete", "superseded"} {
		t.Run(kind, func(t *testing.T) {
			f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
			want := service.UserPlatformQuotaBoundIncrementResult{Status: service.QuotaIncrementSuperseded}
			if kind == "superseded" {
				require.NoError(t, repo.ResetExpiredWindow(context.Background(), cmd.UserID, receipt.Intent.Platform, "daily", time.Now().UTC()))
			} else {
				day, week, month := receipt.CapturedAt.Add(time.Microsecond), timezone.StartOfWeek(receipt.CapturedAt), receipt.CapturedAt.Add(-time.Hour)
				want = service.UserPlatformQuotaBoundIncrementResult{Status: service.QuotaIncrementPartial, AppliedWindows: service.QuotaWindowWeekly | service.QuotaWindowMonthly}
				if kind == "obsolete" {
					week, month = day, day
					want = service.UserPlatformQuotaBoundIncrementResult{Status: service.QuotaIncrementObsolete}
				}
				ordinaryQuotaDeliveryFixtureSQL(t, f, receipt, `UPDATE user_platform_quotas SET daily_usage_usd=1,weekly_usage_usd=2,monthly_usage_usd=3,
					daily_window_start=$2,weekly_window_start=$3,monthly_window_start=$4 WHERE id=$1`, receipt.QuotaRowID, day, week, month)
			}
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
			require.NoError(t, err)
			require.Equal(t, want, result.Outcome)
			require.False(t, result.Replayed)
			terminal := ordinaryQuotaDeliveryJSON(t, f, cmd)
			// Later counter/control changes never reinterpret an already consumed
			// source, including a partial or intentionally obsolete delivery.
			require.NoError(t, repo.ResetExpiredWindow(context.Background(), cmd.UserID, receipt.Intent.Platform, "monthly", time.Now().UTC()))
			ordinaryQuotaDeliveryFixtureSQL(t, f, receipt, `UPDATE user_platform_quotas SET daily_usage_usd=9,weekly_usage_usd=9,monthly_usage_usd=9 WHERE id=$1`, receipt.QuotaRowID)
			before := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
			replay, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
			require.NoError(t, err)
			result.Replayed = true
			require.Equal(t, result, replay)
			read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
			require.NoError(t, err)
			require.NotNil(t, read)
			require.Equal(t, result, *read)
			require.Equal(t, terminal, ordinaryQuotaDeliveryJSON(t, f, cmd))
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresDedupKeyAndQuotaCleanupPreserveOutcome(t *testing.T) {
	f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
	writer := ordinaryQuotaDeliveryWriter(t, repo)
	result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.NoError(t, err)
	terminal := ordinaryQuotaDeliveryJSON(t, f, cmd)
	source := ordinaryPlatformQuotaJournalJSON(t, f, cmd)
	for _, table := range []string{"usage_billing_dedup", "usage_billing_dedup_archive"} {
		_, err = f.db.Exec(`DELETE FROM `+table+` WHERE request_id=$1 AND api_key_id=$2`, cmd.RequestID, cmd.APIKeyID)
		require.NoError(t, err)
	}
	_, err = f.db.Exec(`DELETE FROM api_keys WHERE id=$1`, cmd.APIKeyID)
	require.NoError(t, err)
	_, err = f.db.Exec(`DELETE FROM user_platform_quotas WHERE id=$1`, receipt.QuotaRowID)
	require.NoError(t, err)
	limit := 200.0
	require.NoError(t, repo.UpsertForUser(context.Background(), cmd.UserID, []UserPlatformQuotaRecord{{Platform: receipt.Intent.Platform, DailyLimitUSD: &limit}}))
	current, err := repo.GetByUserPlatform(context.Background(), cmd.UserID, receipt.Intent.Platform)
	require.NoError(t, err)
	require.NotNil(t, current)
	require.NotEqual(t, receipt.QuotaRowID, current.QuotaRowID)
	before := boundQuotaRecordJSON(t, f.db, current.QuotaRowID)
	result.Replayed = true
	for range 2 {
		replay, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
		require.NoError(t, err)
		require.Equal(t, result, replay)
		read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
		require.NoError(t, err)
		require.NotNil(t, read)
		require.Equal(t, result, *read)
		require.Equal(t, before, boundQuotaRecordJSON(t, f.db, current.QuotaRowID))
	}
	require.Equal(t, terminal, ordinaryQuotaDeliveryJSON(t, f, cmd))
	require.Equal(t, source, ordinaryPlatformQuotaJournalJSON(t, f, cmd))
	// A terminal source does not even need the current quota table name to
	// resolve its original outcome; no hidden current-row refresh is allowed.
	_, err = f.db.Exec(`ALTER TABLE user_platform_quotas RENAME TO ordinary_delivery_fixture_unread_quota`)
	require.NoError(t, err)
	replay, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.NoError(t, err)
	require.Equal(t, result, replay)
	read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
	require.NoError(t, err)
	require.NotNil(t, read)
	require.Equal(t, result, *read)
}

func TestOrdinaryPlatformQuotaDeliveryPostgresOwnedTransactionsAndReadonlyLookup(t *testing.T) {
	for _, kind := range []string{"ambient-rejected", "readonly-lookup"} {
		t.Run(kind, func(t *testing.T) {
			f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			before := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
			birth := ordinaryQuotaDeliveryJSON(t, f, cmd)
			if kind == "ambient-rejected" {
				tx, err := f.client.Tx(context.Background())
				require.NoError(t, err)
				defer tx.Rollback()
				ctx := dbent.NewTxContext(context.Background(), tx)
				result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(ctx, cmd, receipt)
				require.ErrorIs(t, err, service.ErrUsageBillingOrdinaryPlatformDelivery)
				require.Equal(t, service.UsageBillingOrdinaryPlatformDeliveryResult{}, result)
				read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(ctx, cmd, receipt)
				require.ErrorIs(t, err, service.ErrUsageBillingOrdinaryPlatformDelivery)
				require.Nil(t, read)
				// Rejection happens before any SQL and leaves the caller's own
				// transaction healthy and under its original owner's control.
				require.NotZero(t, quotaSnapshotTransactionPID(t, ctx, tx.Client()))
				require.NoError(t, tx.Rollback())
				require.Equal(t, birth, ordinaryQuotaDeliveryJSON(t, f, cmd))
			} else {
				_, err := f.db.Exec(`ALTER TABLE ordinary_platform_quota_deliveries RENAME TO ordinary_delivery_fixture_storage;
					CREATE FUNCTION ordinary_delivery_fixture_readonly() RETURNS boolean LANGUAGE plpgsql AS $$
					BEGIN
						IF current_setting('transaction_read_only')<>'on' OR current_setting('transaction_isolation')<>'repeatable read' THEN
							RAISE EXCEPTION 'lookup must use a read-only repeatable-read snapshot';
						END IF;
						RETURN true;
					END; $$;
					CREATE VIEW ordinary_platform_quota_deliveries AS SELECT * FROM ordinary_delivery_fixture_storage WHERE ordinary_delivery_fixture_readonly()`)
				require.NoError(t, err)
				read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
				require.NoError(t, err)
				require.NotNil(t, read)
				require.Equal(t, service.OrdinaryPlatformDeliveryPending, read.State)
				var after string
				require.NoError(t, f.db.QueryRow(`SELECT row_to_json(d)::text FROM ordinary_delivery_fixture_storage d`).Scan(&after))
				require.Equal(t, birth, after)
			}
			require.Equal(t, before, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresResetAndConsumerLockOrders(t *testing.T) {
	for _, adminFirst := range []bool{false, true} {
		name := "consumer-first"
		if adminFirst {
			name = "admin-first"
		}
		t.Run(name, func(t *testing.T) {
			f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			done := make(chan error, 1)
			var result service.UsageBillingOrdinaryPlatformDeliveryResult
			if adminFirst {
				tx, err := f.client.Tx(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				txCtx := dbent.NewTxContext(ctx, tx)
				pid := quotaSnapshotTransactionPID(t, txCtx, tx.Client())
				require.NoError(t, repo.ResetExpiredWindow(txCtx, cmd.UserID, receipt.Intent.Platform, "daily", time.Now().UTC()))
				go func() {
					var err error
					result, err = writer.ConsumeOrdinaryPlatformQuotaUsage(ctx, cmd, receipt)
					done <- err
				}()
				quotaSnapshotWaitForBlock(t, ctx, f.db, pid, done)
				require.NoError(t, tx.Commit())
				quotaSnapshotAwait(t, ctx, done)
				require.Equal(t, service.QuotaIncrementSuperseded, result.Outcome.Status)
				require.Zero(t, result.Outcome.AppliedWindows)
			} else {
				holder, err := f.db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer holder.Rollback()
				var holderPID int
				require.NoError(t, holder.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holderPID))
				lockID := time.Now().UnixNano()
				_, err = holder.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, lockID)
				require.NoError(t, err)
				_, err = f.db.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION ordinary_delivery_fixture_wait() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN PERFORM pg_advisory_xact_lock(%d); RETURN NEW; END; $$;
					CREATE TRIGGER ordinary_delivery_fixture_wait BEFORE UPDATE ON user_platform_quotas
					FOR EACH ROW EXECUTE FUNCTION ordinary_delivery_fixture_wait()`, lockID))
				require.NoError(t, err)
				go func() {
					var err error
					result, err = writer.ConsumeOrdinaryPlatformQuotaUsage(ctx, cmd, receipt)
					done <- err
				}()
				quotaSnapshotWaitForBlock(t, ctx, f.db, holderPID, done)
				var consumerPID int
				require.NoError(t, f.db.QueryRowContext(ctx, `SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))`, holderPID).Scan(&consumerPID))
				resetDone := make(chan error, 1)
				go func() {
					resetDone <- repo.ResetExpiredWindow(ctx, cmd.UserID, receipt.Intent.Platform, "daily", time.Now().UTC())
				}()
				quotaSnapshotWaitForBlock(t, ctx, f.db, consumerPID, resetDone)
				require.NoError(t, holder.Commit())
				quotaSnapshotAwait(t, ctx, done)
				quotaSnapshotAwait(t, ctx, resetDone)
				require.Equal(t, service.QuotaIncrementApplied, result.Outcome.Status)
				require.Equal(t, service.QuotaWindowAll, result.Outcome.AppliedWindows)
			}
			require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, result, false))
			replay, err := writer.ConsumeOrdinaryPlatformQuotaUsage(ctx, cmd, receipt)
			require.NoError(t, err)
			result.Replayed = true
			require.Equal(t, result, replay)
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresWriterEpochBirthAndApplyFailures(t *testing.T) {
	f, _, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
	require.Equal(t, int64(1), receipt.WriterEpoch)
	var stored int64
	require.NoError(t, f.db.QueryRow(`SELECT writer_epoch FROM ordinary_platform_quota_deliveries WHERE request_id=$1 AND api_key_id=$2`, cmd.RequestID, cmd.APIKeyID).Scan(&stored))
	require.Equal(t, receipt.WriterEpoch, stored)
	legacy := *receipt
	legacy.WriterEpoch = 0
	v1, err := service.FingerprintUsageBillingOrdinaryPlatformDelivery(&legacy)
	require.NoError(t, err)
	v2, err := service.FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	require.NoError(t, err)
	require.NotEqual(t, v1, v2)
	money, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
	for _, test := range []struct {
		name      string
		authority service.UserPlatformQuotaWriterAuthority
		want      error
	}{
		{"legacy-constructor", service.UserPlatformQuotaWriterAuthority{}, service.ErrUserPlatformQuotaWriterUnavailable},
		{"wrong-epoch", service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeDB, Epoch: 2}, service.ErrUserPlatformQuotaWriterMismatch},
		{"snapshot-mode", service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeSnapshot, Epoch: 1}, service.ErrUserPlatformQuotaWriterModeUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			attempt := ordinaryPlatformQuotaCommand(t, f, "authority-apply-"+test.name, 0.25)
			attempt.OrdinaryPlatformQuotaDeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
			billing := NewUsageBillingRepositoryWithWriterAuthority(f.client, f.db, test.authority)
			if test.name == "legacy-constructor" {
				billing = NewUsageBillingRepository(f.client, f.db)
			}
			result, err := billing.Apply(context.Background(), attempt)
			require.ErrorIs(t, err, test.want)
			require.Nil(t, result)
			afterMoney, afterUsed := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
			require.Equal(t, money, afterMoney)
			require.Equal(t, used, afterUsed)
			var count int
			require.NoError(t, f.db.QueryRow(`SELECT (SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1)+(SELECT count(*) FROM usage_billing_ordinary_platform_quota_journal WHERE request_id=$1)+(SELECT count(*) FROM ordinary_platform_quota_deliveries WHERE request_id=$1)`, attempt.RequestID).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresHistoricalNullEpochNeverAcquiresAuthority(t *testing.T) {
	for _, completed := range []bool{false, true} {
		name := "pending"
		if completed {
			name = "completed"
		}
		t.Run(name, func(t *testing.T) {
			// Establish the actual pre-263 source, quota effect and v1 birth shape
			// before migration. No current authority is used to infer its origin.
			f := ordinaryPlatformQuotaFixture(t)
			limit := 100.0
			settlementPlatformQuotaRow(t, f, &limit)
			cmd := ordinaryPlatformQuotaCommand(t, f, "historical-epoch", 0.25)
			applied, err := f.billing.Apply(context.Background(), cmd)
			require.NoError(t, err)
			require.True(t, applied.Applied)
			receipt := applied.OrdinaryPlatformQuotaReceipt
			cmd.OrdinaryPlatformQuotaDeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
			receipt.DeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
			fingerprint, err := service.FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
			require.NoError(t, err)
			_, err = f.db.Exec(`INSERT INTO ordinary_platform_quota_deliveries
				(request_id,api_key_id,source_fingerprint,delivery_mode,user_id,platform,quota_row_id,control_generation,captured_at,raw_cost_usd,cost_usd)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::numeric)`, cmd.RequestID, cmd.APIKeyID, fingerprint, receipt.DeliveryMode, cmd.UserID, receipt.Intent.Platform, receipt.QuotaRowID, receipt.ControlGeneration, receipt.CapturedAt, receipt.Intent.RawCostUSD, receipt.Intent.CostUSD)
			require.NoError(t, err)
			if completed {
				binding, err := receipt.UsageBinding()
				require.NoError(t, err)
				outcome, err := boundQuotaWriter(t, NewUserPlatformQuotaRepository(f.client)).IncrementBoundUsage(context.Background(), binding)
				require.NoError(t, err)
				require.Equal(t, service.QuotaIncrementApplied, outcome.Status)
				_, err = f.db.Exec(`UPDATE ordinary_platform_quota_deliveries SET state='completed',result_status='applied',applied_windows=7,completed_at=GREATEST(clock_timestamp(),created_at)`)
				require.NoError(t, err)
			}
			beforeQuota := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
			initializeQuotaWriterAuthorityFixture(t, f)
			before := ordinaryQuotaDeliveryJSON(t, f, cmd)
			var epochIsNull bool
			require.NoError(t, f.db.QueryRow(`SELECT writer_epoch IS NULL FROM ordinary_platform_quota_deliveries`).Scan(&epochIsNull))
			require.True(t, epochIsNull)
			for _, repo := range []UserPlatformQuotaRepository{
				NewUserPlatformQuotaRepository(f.client),
				NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeDB, Epoch: 1}),
			} {
				writer := ordinaryQuotaDeliveryWriter(t, repo)
				read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
				require.NoError(t, err)
				require.NotNil(t, read)
				require.Equal(t, fingerprint, read.SourceFingerprint)
				result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
				if completed {
					require.NoError(t, err)
					require.True(t, result.Replayed)
					require.Equal(t, *read, result)
				} else {
					require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
					require.Equal(t, service.UsageBillingOrdinaryPlatformDeliveryResult{}, result)
				}
				promoted := *receipt
				promoted.WriterEpoch = 1
				read, err = writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, &promoted)
				require.ErrorIs(t, err, service.ErrUsageBillingOrdinaryPlatformDeliveryConflict)
				require.Nil(t, read)
			}
			_, err = f.db.Exec(`UPDATE ordinary_platform_quota_deliveries SET writer_epoch=1`)
			require.Error(t, err)
			require.Equal(t, before, ordinaryQuotaDeliveryJSON(t, f, cmd))
			require.Equal(t, beforeQuota, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
		})
	}
}

func TestOrdinaryPlatformQuotaDeliveryPostgresPendingUsesFixedHandleTerminalDoesNot(t *testing.T) {
	f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
	before := ordinaryQuotaDeliveryJSON(t, f, cmd)
	beforeQuota := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
	for _, authority := range []service.UserPlatformQuotaWriterAuthority{{}, {Mode: service.UserPlatformQuotaWriterModeDB, Epoch: receipt.WriterEpoch + 1}} {
		wrong := NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, authority)
		result, err := ordinaryQuotaDeliveryWriter(t, wrong).ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterMismatch)
		require.Equal(t, service.UsageBillingOrdinaryPlatformDeliveryResult{}, result)
	}
	require.Equal(t, before, ordinaryQuotaDeliveryJSON(t, f, cmd))
	require.Equal(t, beforeQuota, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
	want, err := ordinaryQuotaDeliveryWriter(t, repo).ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.NoError(t, err)
	want.Replayed = true
	// Completed evidence has no dependency on the currently named authority.
	_, err = f.db.Exec(`ALTER TABLE user_platform_quota_writer_authority RENAME TO ordinary_delivery_fixture_unread_authority`)
	require.NoError(t, err)
	legacy := ordinaryQuotaDeliveryWriter(t, NewUserPlatformQuotaRepository(f.client))
	read, err := legacy.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
	require.NoError(t, err)
	require.NotNil(t, read)
	require.Equal(t, want, *read)
	result, err := legacy.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.NoError(t, err)
	require.Equal(t, want, result)
}
