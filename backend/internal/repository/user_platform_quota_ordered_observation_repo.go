package repository

import (
	"context"
	"database/sql"
	"fmt"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.UserPlatformQuotaOrderedObservationRepository = (*userPlatformQuotaRepository)(nil)

// GetOrderedUserPlatformQuotaObservation observes authority, private ordering
// metadata and exact state in one SQL statement's MVCC snapshot. It grants no
// write right and does not turn historical deliveries into current observations.
func (r *userPlatformQuotaRepository) GetOrderedUserPlatformQuotaObservation(ctx context.Context, userID int64, platform string) (*service.UserPlatformQuotaOrderedObservation, error) {
	observation, _, err := r.readUserPlatformQuotaOrderedObservation(ctx, userID, platform, false)
	return observation, err
}

// The raw audit reader remains usable on migration 264. The strict candidate
// reader also selects the persisted cache domain from this same SQL snapshot;
// it never combines an observation with a separately read namespace.
func (r *userPlatformQuotaRepository) readUserPlatformQuotaOrderedObservation(ctx context.Context, userID int64, platform string, includeCacheDomain bool) (*service.UserPlatformQuotaOrderedObservation, *service.UserPlatformQuotaCacheDomain, error) {
	if userID <= 0 || !domain.IsConcretePlatform(platform) {
		return nil, nil, service.ErrUserPlatformQuotaObservationInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if dbent.TxFromContext(ctx) != nil {
		return nil, nil, fmt.Errorf("%w: ambient transaction is unsupported", service.ErrUserPlatformQuotaObservationInvalid)
	}
	if r == nil || r.client == nil || !r.writerAuthorityConfigured {
		return nil, nil, service.ErrUserPlatformQuotaWriterUnavailable
	}
	if err := service.ValidateUserPlatformQuotaWriterAuthority(r.writerAuthority); err != nil {
		return nil, nil, err
	}

	// Anchor on authority, rather than the requested quota: a missing quota must
	// still verify the repository's fixed mode/epoch. The LEFT JOIN produces one
	// nullable observation without a second, potentially inconsistent read.
	authorityColumns := "a.mode,a.epoch,"
	if includeCacheDomain {
		authorityColumns += "a.cache_namespace::text,"
	}
	rows, err := r.client.QueryContext(ctx, `SELECT `+authorityColumns+`
		q.id,q.control_generation,q.observation_lineage::text,q.observation_revision,q.user_id,q.platform,
		q.daily_limit_usd::text,q.weekly_limit_usd::text,q.monthly_limit_usd::text,
		q.daily_usage_usd::text,q.weekly_usage_usd::text,q.monthly_usage_usd::text,
		q.daily_window_start,q.weekly_window_start,q.monthly_window_start
		FROM user_platform_quota_writer_authority a
		LEFT JOIN user_platform_quotas q ON q.user_id=$1 AND q.platform=$2 AND q.deleted_at IS NULL
		WHERE a.id=1`, userID, platform)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, service.ErrUserPlatformQuotaWriterUnavailable
	}
	var authority service.UserPlatformQuotaWriterAuthority
	var namespace string
	var rowID, generation, revision, observedUserID sql.NullInt64
	var lineage, observedPlatform sql.NullString
	var limits, usages [3]sql.NullString
	var starts [3]sql.NullTime
	destinations := []any{&authority.Mode, &authority.Epoch}
	if includeCacheDomain {
		destinations = append(destinations, &namespace)
	}
	destinations = append(destinations,
		&rowID, &generation, &lineage, &revision, &observedUserID, &observedPlatform,
		&limits[0], &limits[1], &limits[2], &usages[0], &usages[1], &usages[2],
		&starts[0], &starts[1], &starts[2])
	if err := rows.Scan(destinations...); err != nil {
		return nil, nil, err
	}
	if rows.Next() {
		return nil, nil, service.ErrUserPlatformQuotaObservationInvalid
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if authority != r.writerAuthority {
		return nil, nil, service.ErrUserPlatformQuotaWriterMismatch
	}
	var cacheDomain *service.UserPlatformQuotaCacheDomain
	if includeCacheDomain {
		cacheDomain = &service.UserPlatformQuotaCacheDomain{
			SchemaVersion:   service.UserPlatformQuotaCacheDomainSchemaV2,
			Namespace:       namespace,
			WriterAuthority: authority,
		}
		if err := service.ValidateUserPlatformQuotaCacheDomain(*cacheDomain); err != nil {
			return nil, nil, err
		}
	}
	if !rowID.Valid {
		return nil, cacheDomain, nil
	}
	if !generation.Valid || !lineage.Valid || !revision.Valid || !observedUserID.Valid || !observedPlatform.Valid ||
		!usages[0].Valid || !usages[1].Valid || !usages[2].Valid || observedUserID.Int64 != userID || observedPlatform.String != platform {
		return nil, nil, service.ErrUserPlatformQuotaObservationInvalid
	}
	observation := &service.UserPlatformQuotaOrderedObservation{
		Token: service.UserPlatformQuotaObservationToken{
			WriterMode: authority.Mode, WriterEpoch: authority.Epoch, QuotaRowID: rowID.Int64,
			ControlGeneration: generation.Int64, Lineage: lineage.String, Revision: revision.Int64,
		},
		UserID: userID, Platform: platform,
		DailyUsageUSD: usages[0].String, WeeklyUsageUSD: usages[1].String, MonthlyUsageUSD: usages[2].String,
	}
	if err := service.ValidateUserPlatformQuotaObservationToken(observation.Token); err != nil {
		return nil, nil, err
	}
	if limits[0].Valid {
		observation.DailyLimitUSD = &limits[0].String
	}
	if limits[1].Valid {
		observation.WeeklyLimitUSD = &limits[1].String
	}
	if limits[2].Valid {
		observation.MonthlyLimitUSD = &limits[2].String
	}
	if starts[0].Valid {
		observation.DailyWindowStart = &starts[0].Time
	}
	if starts[1].Valid {
		observation.WeeklyWindowStart = &starts[1].Time
	}
	if starts[2].Valid {
		observation.MonthlyWindowStart = &starts[2].Time
	}
	return observation, cacheDomain, nil
}
