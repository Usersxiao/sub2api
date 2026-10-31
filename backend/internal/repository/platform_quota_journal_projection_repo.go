package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.PlatformQuotaJournalProjectionRepository = (*usageSettlementEventRepository)(nil)

// ReadPlatformQuotaJournalProjection observes all immutable raw sources on one
// read-only snapshot. It is a partial accounting projection, never a legacy
// counter/cache baseline, total-quota certificate, or admission decision.
func (r *usageSettlementEventRepository) ReadPlatformQuotaJournalProjection(ctx context.Context, userID int64, platform string, asOf time.Time) (*service.PlatformQuotaJournalProjection, error) {
	if userID <= 0 || !domain.IsConcretePlatform(platform) || asOf.IsZero() || asOf.Year() < 1 || asOf.Year() > 9999 {
		return nil, service.ErrPlatformQuotaJournalProjectionInput
	}
	if r == nil || r.db == nil {
		return nil, service.ErrPlatformQuotaJournalProjectionUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
	}
	defer tx.Rollback()
	input := service.PlatformQuotaJournalProjectionInput{UserID: userID, Platform: platform, AsOf: asOf}
	var tracking, dailyReset, weeklyReset, monthlyReset sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT q.id,q.created_at,s.tracking_started_at,s.daily_reset_at,s.weekly_reset_at,s.monthly_reset_at
		FROM user_platform_quotas q LEFT JOIN user_platform_quota_client_resets s ON s.quota_row_id=q.id
		WHERE q.user_id=$1 AND q.platform=$2 AND q.deleted_at IS NULL`, userID, platform).
		Scan(&input.QuotaRowID, &input.QuotaCreatedAt, &tracking, &dailyReset, &weeklyReset, &monthlyReset)
	missingQuota := errors.Is(err, sql.ErrNoRows)
	if err != nil && !missingQuota {
		return nil, errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
	}
	if err := verifyNativePlatformProjection(ctx, tx, userID, platform); err != nil {
		return nil, err
	}
	if missingQuota {
		return nil, nil
	}
	if !tracking.Valid {
		return nil, service.ErrPlatformQuotaJournalProjectionUnavailable
	}
	input.TrackingStartedAt = tracking.Time
	if dailyReset.Valid {
		input.DailyResetAt = &dailyReset.Time
	}
	if weeklyReset.Valid {
		input.WeeklyResetAt = &weeklyReset.Time
	}
	if monthlyReset.Valid {
		input.MonthlyResetAt = &monthlyReset.Time
	}
	for _, cutoff := range []*time.Time{&input.TrackingStartedAt, input.DailyResetAt, input.WeeklyResetAt, input.MonthlyResetAt} {
		if cutoff != nil && asOf.Before(*cutoff) {
			return nil, service.ErrPlatformQuotaJournalProjectionHistory
		}
	}
	// The opposing lookup intentionally has no user/platform/incarnation/time/
	// disposition filter. A conflicting identity outside the selected scope is
	// still a cross-source collision, including two otherwise identical records.
	rows, err := tx.QueryContext(ctx, `SELECT source,request_id,api_key_id,billing_fingerprint,captured_at,cost_usd,cross_source_collision FROM (
		SELECT 'client_journal'::text AS source,j.request_id,j.api_key_id,j.billing_fingerprint,j.captured_at,j.cost_usd,
			EXISTS(SELECT 1 FROM usage_billing_ordinary_platform_quota_journal opposite
				WHERE opposite.request_id=j.request_id AND opposite.api_key_id=j.api_key_id)
			OR EXISTS(SELECT 1 FROM usage_billing_batch_image_platform_quota_journal opposite
				WHERE opposite.request_id=j.request_id AND opposite.api_key_id=j.api_key_id) AS cross_source_collision
		FROM usage_settlement_platform_quota_journal j
		WHERE j.user_id=$1 AND j.platform=$2 AND j.quota_row_id=$3 AND j.disposition='metered'
			AND j.captured_at>=$4 AND j.captured_at<=$5
		UNION ALL
		SELECT 'ordinary_journal'::text AS source,j.request_id,j.api_key_id,j.billing_fingerprint,j.captured_at,j.cost_usd,
			EXISTS(SELECT 1 FROM usage_settlement_platform_quota_journal opposite
				WHERE opposite.request_id=j.request_id AND opposite.api_key_id=j.api_key_id)
			OR EXISTS(SELECT 1 FROM usage_billing_batch_image_platform_quota_journal opposite
				WHERE opposite.request_id=j.request_id AND opposite.api_key_id=j.api_key_id) AS cross_source_collision
		FROM usage_billing_ordinary_platform_quota_journal j
		WHERE j.user_id=$1 AND j.platform=$2 AND j.quota_row_id=$3 AND j.disposition='metered'
			AND j.captured_at>=$4 AND j.captured_at<=$5
		UNION ALL
		SELECT 'batch_image_journal'::text AS source,j.request_id,j.api_key_id,j.billing_fingerprint,j.captured_at,j.cost_usd,
			EXISTS(SELECT 1 FROM usage_settlement_platform_quota_journal opposite
				WHERE opposite.request_id=j.request_id AND opposite.api_key_id=j.api_key_id)
			OR EXISTS(SELECT 1 FROM usage_billing_ordinary_platform_quota_journal opposite
				WHERE opposite.request_id=j.request_id AND opposite.api_key_id=j.api_key_id) AS cross_source_collision
		FROM usage_billing_batch_image_platform_quota_journal j
		WHERE j.user_id=$1 AND j.platform=$2 AND j.quota_row_id=$3 AND j.disposition='metered'
			AND j.captured_at>=$4 AND j.captured_at<=$5
	) AS raw_journals ORDER BY captured_at,source,request_id,api_key_id LIMIT $6`, userID, platform,
		input.QuotaRowID, input.QuotaCreatedAt, asOf, service.MaxClientPlatformQuotaProjectionEntries+1)
	if err != nil {
		return nil, errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry service.PlatformQuotaJournalProjectionEntry
		var collision bool
		if err := rows.Scan(&entry.Source, &entry.RequestID, &entry.APIKeyID, &entry.BillingFingerprint,
			&entry.CapturedAt, &entry.CostUSD, &collision); err != nil {
			return nil, errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
		}
		if collision {
			return nil, service.ErrPlatformQuotaJournalProjectionConflict
		}
		input.Entries = append(input.Entries, entry)
		if len(input.Entries) > service.MaxClientPlatformQuotaProjectionEntries {
			return nil, service.ErrPlatformQuotaJournalProjectionUnavailable
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
	}
	if err := rows.Close(); err != nil {
		return nil, errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
	}
	result, err := service.ProjectPlatformQuotaJournals(input)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
	}
	return result, nil
}
