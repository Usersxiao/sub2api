//go:build unit

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func batchProjectionFixture(t *testing.T) (*settlementEventPGFixture, int64, time.Time) {
	t.Helper()
	f, quotaID, asOf := clientProjectionFixture(t)
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET platform='gemini' WHERE id=$1`, quotaID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE users SET balance=100 WHERE id=$1`, f.session.UserID)
	require.NoError(t, err)
	return f, quotaID, asOf
}

// Use a real pre-258 reserve transaction without native birth, followed by the
// current capture. Only journal time changes to exercise historical windows;
// neither projected usage nor quota snapshots are injected.
func batchProjectionCaptureAt(t *testing.T, f *settlementEventPGFixture, batchID string, captured time.Time, cost float64) *service.BatchImageBalanceHoldCommand {
	t.Helper()
	ctx := context.Background()
	key, err := f.client.APIKey.Create().SetUserID(f.session.UserID).SetGroupID(f.in.GroupID).
		SetKey("sk-projection-" + batchID).SetName("batch-projection").Save(ctx)
	require.NoError(t, err)
	cmd := &service.BatchImageBalanceHoldCommand{RequestID: service.BatchImageHoldRequestID(batchID),
		APIKeyID: key.ID, UserID: f.session.UserID, BatchID: batchID, HoldAmount: max(cost, 1), RequestPayloadHash: sessionHash("batch manifest")}
	_, err = applyPre258BatchImageReserve(ctx, f, cmd)
	require.NoError(t, err)
	cmd.RequestID, cmd.RequestFingerprint, cmd.ActualAmount = service.BatchImageCaptureRequestID(batchID), "", cost
	cmd.Normalize()
	cmd.CapturePlatformQuota, err = service.FreezeBatchImagePlatformQuotaIntent(cmd, service.BatchImageProviderVertex)
	require.NoError(t, err)
	result, err := f.billing.CaptureBatchImageBalance(ctx, cmd)
	require.NoError(t, err)
	require.True(t, result.Applied)
	_, err = f.db.Exec(`UPDATE usage_billing_batch_image_platform_quota_journal SET captured_at=$1 WHERE request_id=$2 AND api_key_id=$3`,
		captured, cmd.RequestID, cmd.APIKeyID)
	require.NoError(t, err)
	return cmd
}

func readBatchProjection(t *testing.T, f *settlementEventPGFixture, asOf time.Time) *service.PlatformQuotaJournalProjection {
	t.Helper()
	result, err := f.events.(service.PlatformQuotaJournalProjectionRepository).
		ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformGemini, asOf)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, result.CoverageVerified)
	require.False(t, result.Mergeable)
	return result
}

func TestBatchImagePlatformProjectionPostgresThreeOriginsShareOneRollingWindow(t *testing.T) {
	f, quotaID, asOf := batchProjectionFixture(t)
	clientProjectionJournalAt(t, f, "batch-projection-client", asOf.Add(-20*24*time.Hour), 1, service.PlatformGemini)
	combinedProjectionOrdinaryAt(t, f, "ordinary:batch-projection", asOf.Add(-24*time.Hour), 2, service.PlatformGemini)
	require.Equal(t, "3.0000000000", readBatchProjection(t, f, asOf).Monthly.UsageUSD)
	batchProjectionCaptureAt(t, f, "batch-projection-old", asOf.Add(-40*24*time.Hour), 3)
	result := readBatchProjection(t, f, asOf)
	require.Equal(t, "2.0000000000", result.Monthly.UsageUSD, "late batch capture reanchors the shared month")
	require.True(t, asOf.Add(-24*time.Hour).Equal(*result.Monthly.Start))
	batchProjectionCaptureAt(t, f, "batch-projection-precision", asOf, 0.0000000001)
	result = readBatchProjection(t, f, asOf)
	require.Equal(t, "0.0000000001", result.Daily.UsageUSD)
	require.Equal(t, "2.0000000001", result.Monthly.UsageUSD)
	var daily, weekly, monthly string
	require.NoError(t, f.db.QueryRow(`SELECT daily_usage_usd,weekly_usage_usd,monthly_usage_usd FROM user_platform_quotas WHERE id=$1`, quotaID).
		Scan(&daily, &weekly, &monthly))
	require.Equal(t, []string{"1.2500000000", "2.5000000000", "3.7500000000"}, []string{daily, weekly, monthly})
}

func TestBatchImagePlatformProjectionPostgresGlobalCollisionWithEitherOrigin(t *testing.T) {
	for _, other := range []string{"client", "ordinary"} {
		for _, hidden := range []string{"exact", "user", "platform", "quota", "time", "disposition"} {
			for _, selected := range []string{"batch", "other"} {
				t.Run(other+"/"+hidden+"/"+selected, func(t *testing.T) {
					f, quotaID, asOf := batchProjectionFixture(t)
					batch := batchProjectionCaptureAt(t, f, "batch-projection-collision", asOf, 0.1)
					var table, requestID string
					var keyID int64
					if other == "ordinary" {
						cmd := combinedProjectionOrdinaryAt(t, f, "ordinary:batch-collision", asOf, 0.1, service.PlatformGemini)
						table, requestID, keyID = "usage_billing_ordinary_platform_quota_journal", cmd.RequestID, cmd.APIKeyID
					} else {
						clientProjectionJournalAt(t, f, "batch-collision-client", asOf, 0.1, service.PlatformGemini)
						table, requestID, keyID = "usage_settlement_platform_quota_journal", "client:batch-collision-client", f.event.APIKeyID
						// Valid client/batch namespaces are disjoint. Explicitly
						// inject damaged migration data to exercise the defensive
						// cross-origin check without changing production constraints.
						_, err := f.db.Exec(`ALTER TABLE usage_settlement_platform_quota_journal
							DROP CONSTRAINT usage_settlement_platform_quota_journal_check`)
						require.NoError(t, err)
					}
					// Deliberately corrupt cross-table identity after real writes.
					// The reader must reject even a counterpart hidden by its filters.
					_, err := f.db.Exec(`UPDATE `+table+` SET request_id=$1,api_key_id=$2 WHERE request_id=$3 AND api_key_id=$4`,
						batch.RequestID, batch.APIKeyID, requestID, keyID)
					require.NoError(t, err)
					otherTable, readPlatform := table, service.PlatformGemini
					if selected == "other" {
						table = "usage_billing_batch_image_platform_quota_journal"
					}
					switch hidden {
					case "user":
						_, err = f.db.Exec(`UPDATE `+table+` SET user_id=$1`, f.session.UserID+1000)
					case "platform":
						if selected == "other" {
							// A batch row is constrained to Gemini. Select an OpenAI
							// counterpart instead, leaving the valid batch outside scope.
							_, err = f.db.Exec(`UPDATE user_platform_quotas SET platform='openai' WHERE id=$1`, quotaID)
							require.NoError(t, err)
							_, err = f.db.Exec(`UPDATE ` + otherTable + ` SET platform='openai'`)
							readPlatform = service.PlatformOpenAI
						} else {
							_, err = f.db.Exec(`UPDATE ` + table + ` SET platform='openai'`)
						}
					case "quota":
						_, err = f.db.Exec(`UPDATE `+table+` SET quota_row_id=$1`, quotaID+1000)
					case "time":
						_, err = f.db.Exec(`UPDATE `+table+` SET captured_at=$1`, asOf.Add(time.Hour))
					case "disposition":
						generation := ""
						if table == "usage_billing_ordinary_platform_quota_journal" {
							generation = "control_generation=NULL,"
						}
						_, err = f.db.Exec(`UPDATE ` + table + ` SET ` + generation + `disposition='unlimited',quota_row_id=NULL,
							daily_limit_usd=NULL,weekly_limit_usd=NULL,monthly_limit_usd=NULL,daily_usage_usd=NULL,weekly_usage_usd=NULL,monthly_usage_usd=NULL,
							daily_window_start=NULL,weekly_window_start=NULL,monthly_window_start=NULL`)
					}
					require.NoError(t, err)
					result, err := f.events.(service.PlatformQuotaJournalProjectionRepository).
						ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, readPlatform, asOf)
					require.ErrorIs(t, err, service.ErrPlatformQuotaJournalProjectionConflict)
					require.Nil(t, result)
				})
			}
		}
	}
}

func TestBatchImagePlatformProjectionPostgresResetAndReplacementKeepCapturedIncarnation(t *testing.T) {
	f, quotaID, _ := batchProjectionFixture(t)
	ctx := context.Background()
	batchProjectionCaptureAt(t, f, "batch-before-reset", clientProjectionNow(t, f.db), 0.25)
	quota := NewUserPlatformQuotaRepository(f.client)
	require.NoError(t, quota.ResetExpiredWindow(ctx, f.session.UserID, service.PlatformGemini, "monthly", clientProjectionNow(t, f.db)))
	result := readBatchProjection(t, f, clientProjectionNow(t, f.db))
	require.Equal(t, "0.0000000000", result.Monthly.UsageUSD)
	require.Equal(t, "0.2500000000", result.Daily.UsageUSD)
	batchProjectionCaptureAt(t, f, "batch-after-reset", clientProjectionNow(t, f.db), 0.125)
	require.Equal(t, "0.1250000000", readBatchProjection(t, f, clientProjectionNow(t, f.db)).Monthly.UsageUSD)
	require.NoError(t, quota.UpsertForUser(ctx, f.session.UserID, nil))
	limit := 50.0
	require.NoError(t, quota.UpsertForUser(ctx, f.session.UserID, []UserPlatformQuotaRecord{{UserID: f.session.UserID,
		Platform: service.PlatformGemini, DailyLimitUSD: &limit}}))
	result = readBatchProjection(t, f, clientProjectionNow(t, f.db))
	require.NotEqual(t, quotaID, result.QuotaRowID)
	require.Equal(t, "0.0000000000", result.Monthly.UsageUSD)
}

func TestBatchImagePlatformProjectionPostgresBoundIncludesAllThreeOrigins(t *testing.T) {
	f, _, asOf := batchProjectionFixture(t)
	clientProjectionJournalAt(t, f, "batch-bound-client", asOf, 0.0000000001, service.PlatformGemini)
	combinedProjectionOrdinaryAt(t, f, "ordinary:batch-bound", asOf, 0.0000000001, service.PlatformGemini)
	batchProjectionCaptureAt(t, f, "batch-bound-capture", asOf, 0.0000000001)
	for index, table := range []string{"usage_settlement_platform_quota_journal", "usage_billing_ordinary_platform_quota_journal", "usage_billing_batch_image_platform_quota_journal"} {
		count := 3333
		if index == 2 {
			count = 3335
		}
		_, err := f.db.Exec(`ALTER TABLE ` + table + ` RENAME TO batch_bound_storage_` + fmt.Sprint(index) + `;
			CREATE TABLE ` + table + ` AS SELECT (jsonb_populate_record(NULL::batch_bound_storage_` + fmt.Sprint(index) + `,
				to_jsonb(s)||jsonb_build_object('request_id','bound-` + fmt.Sprint(index) + `-'||g.n::text))).*
			FROM batch_bound_storage_` + fmt.Sprint(index) + ` s CROSS JOIN generate_series(1,` + fmt.Sprint(count) + `) g(n);
			CREATE UNIQUE INDEX ON ` + table + `(request_id,api_key_id)`)
		require.NoError(t, err)
	}
	reader := f.events.(service.PlatformQuotaJournalProjectionRepository)
	result, err := reader.ReadPlatformQuotaJournalProjection(context.Background(), f.session.UserID, service.PlatformGemini, asOf)
	require.Equal(t, service.ErrPlatformQuotaJournalProjectionUnavailable, err)
	require.Nil(t, result)
	_, err = f.db.Exec(`DELETE FROM usage_billing_batch_image_platform_quota_journal WHERE request_id='bound-2-3335'`)
	require.NoError(t, err)
	require.Equal(t, "0.0000010000", readBatchProjection(t, f, asOf).Monthly.UsageUSD)
}
