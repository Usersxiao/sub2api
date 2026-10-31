//go:build unit

package repository

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func boundQuotaCacheFixture(t *testing.T, backend string) (*billingCache, int64, string) {
	t.Helper()
	var c *billingCache
	var userID int64
	if backend == "redis7" {
		c, userID = realQuotaCacheCASFixture(t)
	} else {
		c, _ = newMiniRedisCache(t)
		userID = 1
	}
	dirty := userPlatformQuotaCacheKey(userID, "openai") + ":bound-test-dirty"
	t.Cleanup(func() { _ = c.rdb.Del(context.Background(), dirty).Err() })
	return c, userID, dirty
}

func boundQuotaCacheBinding(userID int64, entry *service.UserPlatformQuotaCacheEntry, at time.Time) service.UserPlatformQuotaUsageBinding {
	return service.UserPlatformQuotaUsageBinding{QuotaRowID: entry.QuotaRowID, ControlGeneration: entry.ControlGeneration,
		UserID: userID, Platform: "openai", CostUSD: 0.25, CapturedAt: at}
}

func TestUserPlatformQuotaBoundCacheCapturedAtAndMonotoneWindows(t *testing.T) {
	for _, backend := range []string{"miniredis", "redis7"} {
		t.Run(backend, func(t *testing.T) {
			for _, scenario := range []string{"current", "rotate-at-capture", "late-day", "late-week", "late-month", "all-obsolete", "same-second-month", "720h-before", "720h-after"} {
				t.Run(scenario, func(t *testing.T) {
					c, userID, dirty := boundQuotaCacheFixture(t, backend)
					ctx := context.Background()
					captured := quotaIncrementFixtureTime().Add(123456 * time.Microsecond)
					entry := quotaIncrementEntryAt(captured)
					wantMask := service.QuotaWindowAll
					wantStatus := service.QuotaIncrementApplied
					wantUsage := [3]string{"1.25", "2.25", "3.25"}
					switch scenario {
					case "rotate-at-capture":
						day := timezone.StartOfDay(captured).Add(-24 * time.Hour)
						week := timezone.StartOfWeek(captured).Add(-7 * 24 * time.Hour)
						month := captured.Add(-720 * time.Hour).Truncate(time.Second)
						entry.DailyWindowStart, entry.WeeklyWindowStart, entry.MonthlyWindowStart = &day, &week, &month
						wantUsage = [3]string{"0.25", "0.25", "0.25"}
					case "late-day":
						next := timezone.StartOfDay(captured).Add(24 * time.Hour)
						entry.DailyWindowStart = &next
						wantMask, wantStatus, wantUsage[0] = 6, service.QuotaIncrementPartial, "1"
					case "late-week":
						next := timezone.StartOfWeek(captured).Add(7 * 24 * time.Hour)
						entry.DailyWindowStart, entry.WeeklyWindowStart = &next, &next
						wantMask, wantStatus, wantUsage[0], wantUsage[1] = 4, service.QuotaIncrementPartial, "1", "2"
					case "late-month":
						next := captured.Add(time.Second).Truncate(time.Second)
						entry.MonthlyWindowStart = &next
						wantMask, wantStatus, wantUsage[2] = 3, service.QuotaIncrementPartial, "3"
					case "all-obsolete":
						next := captured.Add(time.Second).Truncate(time.Second)
						entry.DailyWindowStart, entry.WeeklyWindowStart, entry.MonthlyWindowStart = &next, &next, &next
						wantMask, wantStatus, wantUsage = 0, service.QuotaIncrementObsolete, [3]string{"1", "2", "3"}
					case "same-second-month":
						// Schema v1 retains Unix-second anchors. A receipt earlier
						// within that same second belongs to the represented window.
						start := captured.Truncate(time.Second)
						entry.MonthlyWindowStart = &start
					case "720h-before":
						captured = captured.Truncate(time.Second).Add(-time.Microsecond)
						start := captured.Truncate(time.Second).Add(time.Second).Add(-720 * time.Hour)
						entry.MonthlyWindowStart = &start
					case "720h-after":
						captured = captured.Truncate(time.Second).Add(time.Microsecond)
						start := captured.Truncate(time.Second).Add(-720 * time.Hour)
						entry.MonthlyWindowStart = &start
						wantUsage[2] = "0.25"
					}
					require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", entry, time.Minute))
					before := quotaIncrementRaw(t, c, userID)
					binding := boundQuotaCacheBinding(userID, entry, captured)
					result, err := c.incrementBoundUserPlatformQuotaUsageCache(ctx, binding, 2*time.Minute, true, dirty)
					require.NoError(t, err)
					require.Equal(t, wantStatus, result.Status)
					require.Equal(t, wantMask, result.AppliedWindows)
					after := quotaIncrementRaw(t, c, userID)
					for i, field := range []string{"daily_usage", "weekly_usage", "monthly_usage"} {
						require.Equal(t, wantUsage[i], after[field], field)
					}
					for _, field := range []string{"cache_instance", "quota_row_id", "control_generation", "schema_version"} {
						require.Equal(t, before[field], after[field], field)
					}
					for i, field := range []string{"daily_window_start", "weekly_window_start", "monthly_window_start"} {
						previous, err := strconv.ParseInt(before[field], 10, 64)
						require.NoError(t, err)
						current, err := strconv.ParseInt(after[field], 10, 64)
						require.NoError(t, err)
						require.GreaterOrEqual(t, current, previous, field)
						if wantMask&(1<<i) == 0 {
							require.Equal(t, before[field], after[field])
						}
					}
					require.Equal(t, wantMask != 0, c.rdb.SIsMember(ctx, dirty, userPlatformQuotaDirtyMember(userID, "openai")).Val())
					if wantMask == 0 {
						require.Equal(t, before, after)
					} else {
						require.Equal(t, "18", after["version"])
					}
				})
			}
		})
	}
}

func TestUserPlatformQuotaBoundCacheSupersededControlsNeverWrite(t *testing.T) {
	for _, backend := range []string{"miniredis", "redis7"} {
		t.Run(backend, func(t *testing.T) {
			for _, field := range []string{"quota_row_id", "control_generation"} {
				t.Run(field, func(t *testing.T) {
					c, userID, dirty := boundQuotaCacheFixture(t, backend)
					ctx, captured := context.Background(), quotaIncrementFixtureTime()
					entry := quotaIncrementEntryAt(captured)
					binding := boundQuotaCacheBinding(userID, entry, captured)
					if field == "quota_row_id" {
						entry.QuotaRowID++
					} else {
						entry.ControlGeneration--
					}
					require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", entry, time.Minute))
					before := quotaIncrementRaw(t, c, userID)
					result, err := c.incrementBoundUserPlatformQuotaUsageCache(ctx, binding, 2*time.Minute, true, dirty)
					require.NoError(t, err)
					require.Equal(t, service.QuotaIncrementSuperseded, result.Status)
					require.Zero(t, result.AppliedWindows)
					require.Equal(t, before, quotaIncrementRaw(t, c, userID))
					require.False(t, c.rdb.Exists(ctx, dirty).Val() > 0)
				})
			}
		})
	}
}

func TestUserPlatformQuotaBoundCacheConflictRechecksIdentityAndCapturedAt(t *testing.T) {
	for _, backend := range []string{"miniredis", "redis7"} {
		t.Run(backend, func(t *testing.T) {
			for _, scenario := range []string{"generation", "row", "late-day"} {
				t.Run(scenario, func(t *testing.T) {
					c, userID, dirty := boundQuotaCacheFixture(t, backend)
					ctx, captured := context.Background(), quotaIncrementFixtureTime()
					entry := quotaIncrementEntryAt(captured)
					require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", entry, time.Minute))
					binding := boundQuotaCacheBinding(userID, entry, captured)
					other := quotaIncrementHookClient(t, c.rdb.Options().Addr)
					key := userPlatformQuotaCacheKey(userID, "openai")
					evals := 0
					c.rdb.AddHook(&quotaIncrementHook{before: func(ctx context.Context, _ redis.Cmder) error {
						evals++
						if evals != 1 {
							return nil
						}
						switch scenario {
						case "generation":
							return other.HSet(ctx, key, "control_generation", entry.ControlGeneration-1).Err()
						case "row":
							return other.HSet(ctx, key, "quota_row_id", entry.QuotaRowID+1).Err()
						default:
							return other.HSet(ctx, key, "daily_window_start", captured.Add(24*time.Hour).Unix(), "daily_usage", "7").Err()
						}
					}})
					result, err := c.incrementBoundUserPlatformQuotaUsageCache(ctx, binding, 2*time.Minute, true, dirty)
					require.NoError(t, err)
					after := quotaIncrementRaw(t, c, userID)
					if scenario == "late-day" {
						require.Equal(t, service.QuotaIncrementPartial, result.Status)
						require.Equal(t, uint8(6), result.AppliedWindows)
						require.Equal(t, 2, evals)
						require.Equal(t, "7", after["daily_usage"])
						require.Equal(t, "2.25", after["weekly_usage"])
						require.Equal(t, "3.25", after["monthly_usage"])
						require.Equal(t, "18", after["version"])
					} else {
						require.Equal(t, service.QuotaIncrementSuperseded, result.Status)
						require.Zero(t, result.AppliedWindows)
						require.Equal(t, 1, evals)
						require.Equal(t, "1", after["daily_usage"])
						require.Equal(t, "2", after["weekly_usage"])
						require.Equal(t, "3", after["monthly_usage"])
						require.Equal(t, "17", after["version"])
						require.Zero(t, c.rdb.Exists(ctx, dirty).Val())
					}
				})
			}
		})
	}
}

func TestUserPlatformQuotaBoundCacheInvalidAndMissingSourceDoNotBootstrap(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx, captured := context.Background(), quotaIncrementFixtureTime()
	entry := quotaIncrementEntryAt(captured)
	binding := boundQuotaCacheBinding(1, entry, captured)
	bad := binding
	bad.ControlGeneration = 0
	result, err := c.IncrBoundUserPlatformQuotaUsageCache(ctx, bad, time.Minute, false)
	require.Error(t, err)
	require.Empty(t, result.Status)
	result, err = c.IncrBoundUserPlatformQuotaUsageCache(ctx, binding, time.Minute, false)
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementMissing, result.Status)
	require.False(t, c.rdb.Exists(ctx, userPlatformQuotaCacheKey(1, "openai")).Val() > 0)
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, time.Minute))
	key := userPlatformQuotaCacheKey(1, "openai")
	require.NoError(t, c.rdb.HDel(ctx, key, "schema_version").Err())
	before := quotaIncrementRaw(t, c, 1)
	result, err = c.IncrBoundUserPlatformQuotaUsageCache(ctx, binding, time.Minute, false)
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementIncompatible, result.Status)
	require.Equal(t, before, quotaIncrementRaw(t, c, 1))
}

func TestUserPlatformQuotaBoundCacheSameSecondOutOfOrderUsesRepresentedAnchor(t *testing.T) {
	for _, backend := range []string{"miniredis", "redis7"} {
		t.Run(backend, func(t *testing.T) {
			c, userID, dirty := boundQuotaCacheFixture(t, backend)
			ctx := context.Background()
			second := quotaIncrementFixtureTime().Truncate(time.Second)
			entry := quotaIncrementEntryAt(second)
			expired := second.Add(-720 * time.Hour)
			entry.MonthlyWindowStart = &expired
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", entry, time.Minute))
			first := boundQuotaCacheBinding(userID, entry, second.Add(900*time.Millisecond))
			result, err := c.incrementBoundUserPlatformQuotaUsageCache(ctx, first, time.Minute, false, dirty)
			require.NoError(t, err)
			require.Equal(t, service.QuotaIncrementApplied, result.Status)
			earlier := first
			earlier.CapturedAt = second.Add(100 * time.Millisecond)
			result, err = c.incrementBoundUserPlatformQuotaUsageCache(ctx, earlier, time.Minute, false, dirty)
			require.NoError(t, err)
			require.Equal(t, service.QuotaIncrementApplied, result.Status)
			require.Equal(t, service.QuotaWindowAll, result.AppliedWindows)
			raw := quotaIncrementRaw(t, c, userID)
			require.Equal(t, strconv.FormatInt(second.Unix(), 10), raw["monthly_window_start"])
			require.Equal(t, "0.5", raw["monthly_usage"], "schema v1 has a second anchor, not microsecond event ordering")
			require.Equal(t, "1.5", raw["daily_usage"])
			require.Equal(t, "2.5", raw["weekly_usage"])
			require.Equal(t, "19", raw["version"])
		})
	}
}
