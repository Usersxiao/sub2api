package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.ClientPlatformQuotaProjectionRepository = (*usageSettlementEventRepository)(nil)

// ReadClientPlatformQuotaProjection observes only the immutable client journal.
// Its result stays separate from legacy enforcement counters and Redis caches.
// Repeatable-read keeps the quota incarnation, reset state, and journal rows on
// one snapshot while an administrator edits or resets the quota concurrently.
func (r *usageSettlementEventRepository) ReadClientPlatformQuotaProjection(ctx context.Context, userID int64, platform string, asOf time.Time) (*service.ClientPlatformQuotaProjection, error) {
	if userID <= 0 || !domain.IsConcretePlatform(platform) || asOf.IsZero() || asOf.Year() < 1 || asOf.Year() > 9999 {
		return nil, service.ErrClientPlatformQuotaProjectionInput
	}
	if r == nil || r.db == nil {
		return nil, service.ErrClientPlatformQuotaProjectionUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, errors.Join(service.ErrClientPlatformQuotaProjectionUnavailable, err)
	}
	defer tx.Rollback()
	input := service.ClientPlatformQuotaProjectionInput{UserID: userID, Platform: platform, AsOf: asOf}
	var tracking, dailyReset, weeklyReset, monthlyReset sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT q.id,q.created_at,s.tracking_started_at,s.daily_reset_at,s.weekly_reset_at,s.monthly_reset_at
		FROM user_platform_quotas q LEFT JOIN user_platform_quota_client_resets s ON s.quota_row_id=q.id
		WHERE q.user_id=$1 AND q.platform=$2 AND q.deleted_at IS NULL`, userID, platform).
		Scan(&input.QuotaRowID, &input.QuotaCreatedAt, &tracking, &dailyReset, &weeklyReset, &monthlyReset)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Join(service.ErrClientPlatformQuotaProjectionUnavailable, err)
	}
	if !tracking.Valid {
		return nil, service.ErrClientPlatformQuotaProjectionUnavailable
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
	// Earlier reset generations have deliberately not been reconstructed from
	// legacy windows. Reject a historical read before any known reset boundary.
	for _, cutoff := range []*time.Time{&input.TrackingStartedAt, input.DailyResetAt, input.WeeklyResetAt, input.MonthlyResetAt} {
		if cutoff != nil && asOf.Before(*cutoff) {
			return nil, service.ErrClientPlatformQuotaProjectionHistory
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT captured_at,cost_usd FROM usage_settlement_platform_quota_journal
		WHERE user_id=$1 AND platform=$2 AND quota_row_id=$3 AND disposition='metered'
		AND captured_at>=$4 AND captured_at<=$5
		ORDER BY captured_at,invocation_id LIMIT $6`, userID, platform, input.QuotaRowID, input.QuotaCreatedAt, asOf,
		service.MaxClientPlatformQuotaProjectionEntries+1)
	if err != nil {
		return nil, errors.Join(service.ErrClientPlatformQuotaProjectionUnavailable, err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry service.ClientPlatformQuotaProjectionEntry
		if err := rows.Scan(&entry.CapturedAt, &entry.CostUSD); err != nil {
			return nil, errors.Join(service.ErrClientPlatformQuotaProjectionUnavailable, err)
		}
		input.Entries = append(input.Entries, entry)
		if len(input.Entries) > service.MaxClientPlatformQuotaProjectionEntries {
			return nil, service.ErrClientPlatformQuotaProjectionUnavailable
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Join(service.ErrClientPlatformQuotaProjectionUnavailable, err)
	}
	if err := rows.Close(); err != nil {
		return nil, errors.Join(service.ErrClientPlatformQuotaProjectionUnavailable, err)
	}
	result, err := service.ProjectClientPlatformQuota(input)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, errors.Join(service.ErrClientPlatformQuotaProjectionUnavailable, err)
	}
	return result, nil
}
