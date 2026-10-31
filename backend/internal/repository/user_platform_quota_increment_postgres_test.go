//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUserPlatformQuotaIncrementPostgresRotatedConsumptionSurvivesAdmission(t *testing.T) {
	for _, window := range []string{"daily", "weekly", "monthly"} {
		t.Run(window, func(t *testing.T) {
			f := quotaAdmissionPostgresFixture(t)
			limit := 0.5
			controls := UserPlatformQuotaRecord{Platform: service.PlatformOpenAI}
			var exhausted error
			switch window {
			case "daily":
				controls.DailyLimitUSD, exhausted = &limit, service.ErrUserPlatformDailyQuotaExhausted
			case "weekly":
				controls.WeeklyLimitUSD, exhausted = &limit, service.ErrUserPlatformWeeklyQuotaExhausted
			case "monthly":
				controls.MonthlyLimitUSD, exhausted = &limit, service.ErrUserPlatformMonthlyQuotaExhausted
			}
			require.NoError(t, f.quota.repo.UpsertForUser(context.Background(), f.quota.userID, []UserPlatformQuotaRecord{controls}))
			rec := f.quota.read(t, service.PlatformOpenAI)
			entry := quotaAdmissionEntry(rec, 0)
			now := time.Now()
			switch window {
			case "daily":
				old := timezone.StartOfDay(now).AddDate(0, 0, -1)
				entry.DailyWindowStart, entry.DailyUsageUSD = &old, 99
			case "weekly":
				old := timezone.StartOfWeek(now).AddDate(0, 0, -7)
				entry.WeeklyWindowStart, entry.WeeklyUsageUSD = &old, 99
			case "monthly":
				old := now.Add(-721 * time.Hour)
				entry.MonthlyWindowStart, entry.MonthlyUsageUSD = &old, 99
			}
			f.put(t, entry)
			svc := f.billing(t, true, f.cache)
			require.NoError(t, svc.IncrementUserPlatformQuotaUsage(f.quota.userID, service.PlatformOpenAI, limit))
			before, found, err := f.cache.GetUserPlatformQuotaCache(context.Background(), f.quota.userID, service.PlatformOpenAI)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, limit, before.DailyUsageUSD)
			require.Equal(t, limit, before.WeeklyUsageUSD)
			require.Equal(t, limit, before.MonthlyUsageUSD)
			require.ErrorIs(t, f.check(svc), exhausted)
			require.ErrorIs(t, f.check(svc), exhausted, "a second preflight cannot erase the newly charged window")
			after, _, err := f.cache.GetUserPlatformQuotaCache(context.Background(), f.quota.userID, service.PlatformOpenAI)
			require.NoError(t, err)
			require.Equal(t, before, after, "admission observes the completed rotation without a second rewrite")
			persisted := f.quota.read(t, service.PlatformOpenAI)
			require.Zero(t, persisted.DailyUsageUSD)
			require.Zero(t, persisted.WeeklyUsageUSD)
			require.Zero(t, persisted.MonthlyUsageUSD, "this test verifies an ahead-cache observation, not a durable counter")
		})
	}
}

func TestUserPlatformQuotaIncrementPostgresMissingAndMalformedNeverClaimSuccess(t *testing.T) {
	f := quotaAdmissionPostgresFixture(t)
	rec := f.quota.seed(t, service.PlatformOpenAI)
	svc := f.billing(t, true, f.cache)
	ctx := context.Background()
	require.Error(t, svc.IncrementUserPlatformQuotaUsage(f.quota.userID, service.PlatformOpenAI, 0.5))
	require.Zero(t, f.cache.rdb.Exists(ctx, userPlatformQuotaCacheKey(f.quota.userID, service.PlatformOpenAI)).Val())
	require.ErrorIs(t, f.check(svc), service.ErrBillingServiceUnavailable)
	f.put(t, quotaAdmissionEntry(rec, 1))
	key := userPlatformQuotaCacheKey(f.quota.userID, service.PlatformOpenAI)
	require.NoError(t, f.cache.rdb.HSet(ctx, key, "monthly_usage", "NaN").Err())
	before, err := f.cache.rdb.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	require.Error(t, svc.IncrementUserPlatformQuotaUsage(f.quota.userID, service.PlatformOpenAI, 0.5))
	after, err := f.cache.rdb.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, before, after, "invalid final-window data cannot leave earlier windows partially charged")
	require.ErrorIs(t, f.check(svc), service.ErrBillingServiceUnavailable)
	require.Zero(t, f.quota.read(t, service.PlatformOpenAI).DailyUsageUSD)
}
