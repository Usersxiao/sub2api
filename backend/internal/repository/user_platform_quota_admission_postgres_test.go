//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type quotaAdmissionPGFixture struct {
	quota *quotaSnapshotPGFixture
	cache *billingCache
}

func quotaAdmissionPostgresFixture(t *testing.T) *quotaAdmissionPGFixture {
	t.Helper()
	q := quotaSnapshotPostgresFixture(t, true)
	cache, _ := newMiniRedisCache(t)
	t.Cleanup(func() { _ = cache.rdb.Close() })
	require.NoError(t, cache.SetUserBalance(context.Background(), q.userID, 100))
	return &quotaAdmissionPGFixture{quota: q, cache: cache}
}

func (f *quotaAdmissionPGFixture) billing(t *testing.T, flusher bool, cache service.BillingCache) *service.BillingCacheService {
	t.Helper()
	cfg := &config.Config{}
	cfg.Billing.UserPlatformQuotaCacheTTLSeconds = 120
	cfg.Database.UserPlatformQuotaFlusherEnabled = flusher
	svc := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, NewUserPlatformQuotaServiceAdapter(f.quota.repo))
	t.Cleanup(svc.Stop)
	return svc
}

func (f *quotaAdmissionPGFixture) check(svc *service.BillingCacheService) error {
	return svc.CheckBillingEligibility(context.Background(), &service.User{ID: f.quota.userID}, nil, nil, nil, service.PlatformOpenAI)
}

func quotaAdmissionEntry(rec *UserPlatformQuotaRecord, usage float64) *service.UserPlatformQuotaCacheEntry {
	now := time.Now().UTC()
	day, week := timezone.StartOfDay(now), timezone.StartOfWeek(now)
	return &service.UserPlatformQuotaCacheEntry{QuotaRowID: rec.QuotaRowID, ControlGeneration: rec.ControlGeneration,
		SchemaVersion: service.UserPlatformQuotaCacheSchemaV1, DailyLimitUSD: rec.DailyLimitUSD,
		WeeklyLimitUSD: rec.WeeklyLimitUSD, MonthlyLimitUSD: rec.MonthlyLimitUSD,
		DailyUsageUSD: usage, WeeklyUsageUSD: usage, MonthlyUsageUSD: usage,
		DailyWindowStart: &day, WeeklyWindowStart: &week, MonthlyWindowStart: &now}
}

func (f *quotaAdmissionPGFixture) put(t *testing.T, entry *service.UserPlatformQuotaCacheEntry) {
	t.Helper()
	require.NoError(t, f.cache.SetUserPlatformQuotaCache(context.Background(), f.quota.userID, service.PlatformOpenAI, entry, time.Minute))
}

func TestUserPlatformQuotaAdmissionPostgresControlChangesRejectStaleCache(t *testing.T) {
	f := quotaAdmissionPostgresFixture(t)
	rec := f.quota.seed(t, service.PlatformOpenAI)
	f.put(t, quotaAdmissionEntry(rec, 1))
	svc := f.billing(t, false, f.cache)
	require.NoError(t, f.check(svc))
	zero := 0.0
	require.NoError(t, f.quota.repo.UpsertForUser(context.Background(), f.quota.userID,
		[]UserPlatformQuotaRecord{{Platform: service.PlatformOpenAI, DailyLimitUSD: &zero}}))
	// Deliberately retain the old cache, as after failed administrative DEL.
	require.ErrorIs(t, f.check(svc), service.ErrBillingServiceUnavailable)
	require.NoError(t, f.cache.DeleteUserPlatformQuotaCache(context.Background(), f.quota.userID, service.PlatformOpenAI))
	require.ErrorIs(t, f.check(svc), service.ErrUserPlatformDailyQuotaExhausted)
	require.NoError(t, f.quota.repo.ResetExpiredWindow(context.Background(), f.quota.userID, service.PlatformOpenAI, "daily", time.Now()))
	require.ErrorIs(t, f.check(svc), service.ErrBillingServiceUnavailable, "reset changes the current control identity")
}

func TestUserPlatformQuotaAdmissionPostgresLiveNoRowAndNewLimitIgnoreSentinel(t *testing.T) {
	f := quotaAdmissionPostgresFixture(t)
	svc := f.billing(t, false, f.cache)
	// A no-row decision comes from PostgreSQL, even if Redis contains junk.
	require.NoError(t, f.cache.rdb.Set(context.Background(), userPlatformQuotaCacheKey(f.quota.userID, service.PlatformOpenAI), "old-sentinel", time.Minute).Err())
	require.NoError(t, f.check(svc))
	require.False(t, svc.HasUserPlatformQuotaLimit(context.Background(), f.quota.userID, service.PlatformOpenAI))
	f.quota.seed(t, service.PlatformOpenAI)
	require.True(t, svc.HasUserPlatformQuotaLimit(context.Background(), f.quota.userID, service.PlatformOpenAI), "old sentinel must not suppress consumption writes")
	require.ErrorIs(t, f.check(svc), service.ErrBillingServiceUnavailable)
	require.NoError(t, f.cache.DeleteUserPlatformQuotaCache(context.Background(), f.quota.userID, service.PlatformOpenAI))
	require.NoError(t, f.check(svc), "DB-mode cold cache can bootstrap conditionally")
	entry, ok, err := f.cache.GetUserPlatformQuotaCache(context.Background(), f.quota.userID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, entry.CacheInstance, 64)
}

func TestUserPlatformQuotaAdmissionPostgresUsesBothCommittedAndAheadCacheUsage(t *testing.T) {
	for _, cacheAhead := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed-db", true: "ahead-cache"}[cacheAhead], func(t *testing.T) {
			f := quotaAdmissionPostgresFixture(t)
			rec := f.quota.seed(t, service.PlatformOpenAI)
			now := time.Now().UTC()
			dbUsage, cacheUsage := 100.0, 1.0
			if cacheAhead {
				dbUsage, cacheUsage = 1, 100
			}
			require.NoError(t, f.quota.repo.IncrementUsageWithReset(context.Background(), f.quota.userID, service.PlatformOpenAI, dbUsage, now))
			rec = f.quota.read(t, service.PlatformOpenAI)
			f.put(t, quotaAdmissionEntry(rec, cacheUsage))
			svc := f.billing(t, false, f.cache)
			require.ErrorIs(t, f.check(svc), service.ErrUserPlatformDailyQuotaExhausted)
		})
	}
}

type quotaAdmissionCASInterleaver struct {
	service.BillingCache
	before func()
}

func (c *quotaAdmissionCASInterleaver) CompareAndSetUserPlatformQuotaCache(ctx context.Context, userID int64, platform string, expected, next *service.UserPlatformQuotaCacheEntry, ttl time.Duration) (bool, error) {
	if c.before != nil {
		before := c.before
		c.before = nil
		before()
	}
	inner := c.BillingCache.(interface {
		CompareAndSetUserPlatformQuotaCache(context.Context, int64, string, *service.UserPlatformQuotaCacheEntry, *service.UserPlatformQuotaCacheEntry, time.Duration) (bool, error)
	})
	return inner.CompareAndSetUserPlatformQuotaCache(ctx, userID, platform, expected, next, ttl)
}

func TestUserPlatformQuotaAdmissionPostgresConditionalFillKeepsConcurrentConsumption(t *testing.T) {
	f := quotaAdmissionPostgresFixture(t)
	rec := f.quota.seed(t, service.PlatformOpenAI)
	cache := &quotaAdmissionCASInterleaver{BillingCache: f.cache, before: func() {
		f.put(t, quotaAdmissionEntry(rec, 99))
		result, incrementErr := f.cache.IncrUserPlatformQuotaUsageCache(context.Background(), rec.UserID, rec.Platform, 1, time.Minute, false)
		require.NoError(t, incrementErr)
		require.Equal(t, service.QuotaIncrementApplied, result.Status)
	}}
	svc := f.billing(t, false, cache)
	require.ErrorIs(t, f.check(svc), service.ErrUserPlatformDailyQuotaExhausted)
	entry, ok, err := f.cache.GetUserPlatformQuotaCache(context.Background(), rec.UserID, rec.Platform)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 100.0, entry.DailyUsageUSD, "late DB fill must not overwrite a newly installed live cache")
}

func TestUserPlatformQuotaAdmissionPostgresConditionalRotationKeepsOtherWindows(t *testing.T) {
	f := quotaAdmissionPostgresFixture(t)
	rec := f.quota.seed(t, service.PlatformOpenAI)
	entry := quotaAdmissionEntry(rec, 11)
	yesterday := timezone.StartOfDay(time.Now()).AddDate(0, 0, -1)
	entry.DailyWindowStart = &yesterday
	f.put(t, entry)
	cache := &quotaAdmissionCASInterleaver{BillingCache: f.cache, before: func() {
		result, incrementErr := f.cache.IncrUserPlatformQuotaUsageCache(context.Background(), rec.UserID, rec.Platform, 0.5, time.Minute, false)
		require.NoError(t, incrementErr)
		require.Equal(t, service.QuotaIncrementApplied, result.Status)
	}}
	svc := f.billing(t, false, cache)
	require.NoError(t, f.check(svc))
	current, ok, err := f.cache.GetUserPlatformQuotaCache(context.Background(), rec.UserID, rec.Platform)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 0.5, current.DailyUsageUSD, "current-day increment survives atomic rotation")
	require.Equal(t, 11.5, current.WeeklyUsageUSD)
	require.Equal(t, 11.5, current.MonthlyUsageUSD)
}

func TestUserPlatformQuotaAdmissionPostgresFlusherMissAndDatabaseFailureClose(t *testing.T) {
	f := quotaAdmissionPostgresFixture(t)
	f.quota.seed(t, service.PlatformOpenAI)
	svc := f.billing(t, true, f.cache)
	require.ErrorIs(t, f.check(svc), service.ErrBillingServiceUnavailable)
	_, ok, err := f.cache.GetUserPlatformQuotaCache(context.Background(), f.quota.userID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.False(t, ok, "flusher mode never bootstraps its missing authority from a delayed DB mirror")
	_, err = f.quota.db.Exec(`ALTER TABLE user_platform_quotas RENAME TO quota_admission_hidden_fixture`)
	require.NoError(t, err)
	require.ErrorIs(t, f.check(svc), service.ErrBillingServiceUnavailable)
	require.True(t, svc.HasUserPlatformQuotaLimit(context.Background(), f.quota.userID, service.PlatformOpenAI))
}
