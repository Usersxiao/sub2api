//go:build unit

package repository

import (
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func quotaIncrementFixtureTime() time.Time {
	return time.Date(2026, 10, 26, 12, 0, 0, 0, timezone.Location())
}

func quotaIncrementEntryAt(now time.Time) *service.UserPlatformQuotaCacheEntry {
	entry := quotaCacheCASEntry()
	day, week, month := timezone.StartOfDay(now), timezone.StartOfWeek(now), now.Add(-time.Hour).UTC().Truncate(time.Second)
	weeklyLimit, monthlyLimit := 200.0, 300.0
	entry.Version = 17
	entry.DailyWindowStart, entry.WeeklyWindowStart, entry.MonthlyWindowStart = &day, &week, &month
	entry.WeeklyLimitUSD, entry.MonthlyLimitUSD = &weeklyLimit, &monthlyLimit
	return entry
}

func quotaIncrementRaw(t *testing.T, c *billingCache, userID int64) map[string]string {
	t.Helper()
	raw, err := c.rdb.HGetAll(context.Background(), userPlatformQuotaCacheKey(userID, "openai")).Result()
	require.NoError(t, err)
	return raw
}

func quotaIncrementAt(c *billingCache, userID int64, cost float64, now time.Time, dirty bool) (service.UserPlatformQuotaIncrementResult, error) {
	return c.incrementUserPlatformQuotaUsageCacheAt(context.Background(), userID, "openai", cost, 2*time.Minute, dirty, func() time.Time { return now }, userPlatformQuotaDirtySetKey())
}

func TestUserPlatformQuotaCacheIncrementExactDecimalAndPreservedControls(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx := context.Background()
	now := quotaIncrementFixtureTime()
	entry := quotaIncrementEntryAt(now)
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, time.Minute))
	key := userPlatformQuotaCacheKey(1, "openai")
	// The raw observation carries more precision than the float64 DTO.
	require.NoError(t, c.rdb.HSet(ctx, key, "daily_usage", "0.100000000000000001", "weekly_usage", "0.1", "monthly_usage", "0").Err())
	before := quotaIncrementRaw(t, c, 1)
	result, err := quotaIncrementAt(c, 1, 0.2, now, true)
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementApplied, result.Status)
	after := quotaIncrementRaw(t, c, 1)
	require.Equal(t, "0.300000000000000001", after["daily_usage"])
	require.Equal(t, "0.3", after["weekly_usage"])
	require.Equal(t, "0.2", after["monthly_usage"])
	require.Equal(t, "18", after["version"])
	for _, field := range []string{"cache_instance", "quota_row_id", "control_generation", "schema_version", "daily_limit", "weekly_limit", "monthly_limit", "daily_window_start", "weekly_window_start", "monthly_window_start"} {
		require.Equal(t, before[field], after[field], field)
	}
	require.Equal(t, 2*time.Minute, mr.TTL(key))
	require.True(t, c.rdb.SIsMember(ctx, userPlatformQuotaDirtySetKey(), userPlatformQuotaDirtyMember(1, "openai")).Val())
	require.Equal(t, time.Duration(userPlatformQuotaDirtyTTLSeconds)*time.Second, mr.TTL(userPlatformQuotaDirtySetKey()))
}

func TestUserPlatformQuotaCacheIncrementRotatesBeforeAddingAtWindowBoundaries(t *testing.T) {
	now := quotaIncrementFixtureTime()
	day, week := timezone.StartOfDay(now), timezone.StartOfWeek(now)
	for _, name := range []string{"current", "daily", "weekly", "monthly-before-720h", "monthly-at-720h", "calendar-month-only", "admin-midday-anchor"} {
		t.Run(name, func(t *testing.T) {
			c, _ := newMiniRedisCache(t)
			entry := quotaIncrementEntryAt(now)
			clock := now
			wantUsage := [3]string{"1.25", "2.25", "3.25"}
			wantStarts := [3]time.Time{*entry.DailyWindowStart, *entry.WeeklyWindowStart, *entry.MonthlyWindowStart}
			switch name {
			case "daily":
				start := day.Add(-time.Second)
				entry.DailyWindowStart = &start
				wantUsage[0], wantStarts[0] = "0.25", day
			case "weekly":
				start := week.Add(-time.Second)
				entry.DailyWindowStart, entry.WeeklyWindowStart = &start, &start
				wantUsage[0], wantUsage[1], wantStarts[0], wantStarts[1] = "0.25", "0.25", day, week
			case "monthly-before-720h":
				start := now.Add(-720*time.Hour + time.Second)
				entry.MonthlyWindowStart, wantStarts[2] = &start, start
			case "monthly-at-720h":
				start := now.Add(-720 * time.Hour)
				entry.MonthlyWindowStart = &start
				wantUsage[2], wantStarts[2] = "0.25", now.UTC()
			case "calendar-month-only":
				clock = time.Date(2026, 11, 1, 12, 0, 0, 0, timezone.Location())
				entry = quotaIncrementEntryAt(clock)
				start := clock.Add(-24 * time.Hour)
				entry.MonthlyWindowStart = &start
				wantStarts = [3]time.Time{*entry.DailyWindowStart, *entry.WeeklyWindowStart, start}
			case "admin-midday-anchor":
				start := now.Add(-time.Minute)
				entry.DailyWindowStart, entry.WeeklyWindowStart = &start, &start
				wantStarts[0], wantStarts[1] = start, start
			}
			require.NoError(t, c.SetUserPlatformQuotaCache(context.Background(), 1, "openai", entry, time.Minute))
			result, err := quotaIncrementAt(c, 1, 0.25, clock, false)
			require.NoError(t, err)
			require.Equal(t, service.QuotaIncrementApplied, result.Status)
			raw := quotaIncrementRaw(t, c, 1)
			for i, field := range []string{"daily_usage", "weekly_usage", "monthly_usage"} {
				require.Equal(t, wantUsage[i], raw[field], field)
			}
			for i, field := range []string{"daily_window_start", "weekly_window_start", "monthly_window_start"} {
				require.Equal(t, strconv.FormatInt(wantStarts[i].Unix(), 10), raw[field], field)
			}
		})
	}
}

func TestUserPlatformQuotaCacheIncrementExplicitMissingAndIncompatible(t *testing.T) {
	for _, name := range []string{"absent", "schema-missing", "schema-zero", "schema-future", "instance-missing", "tokens-missing", "nil-window"} {
		t.Run(name, func(t *testing.T) {
			c, mr := newMiniRedisCache(t)
			ctx := context.Background()
			now := quotaIncrementFixtureTime()
			key := userPlatformQuotaCacheKey(1, "openai")
			want := service.QuotaIncrementIncompatible
			if name == "absent" {
				want = service.QuotaIncrementMissing
			} else {
				require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaIncrementEntryAt(now), time.Minute))
				switch name {
				case "schema-missing":
					require.NoError(t, c.rdb.HDel(ctx, key, "schema_version").Err())
				case "schema-zero":
					require.NoError(t, c.rdb.HSet(ctx, key, "schema_version", "0").Err())
				case "schema-future":
					require.NoError(t, c.rdb.HSet(ctx, key, "schema_version", "2").Err())
				case "instance-missing":
					require.NoError(t, c.rdb.HDel(ctx, key, "cache_instance").Err())
				case "tokens-missing":
					require.NoError(t, c.rdb.HDel(ctx, key, "quota_row_id", "control_generation").Err())
				case "nil-window":
					require.NoError(t, c.rdb.HSet(ctx, key, "monthly_window_start", "").Err())
				}
			}
			before, ttl := quotaIncrementRaw(t, c, 1), mr.TTL(key)
			result, err := quotaIncrementAt(c, 1, 0.25, now, true)
			require.NoError(t, err)
			require.Equal(t, want, result.Status)
			require.Equal(t, before, quotaIncrementRaw(t, c, 1))
			require.Equal(t, ttl, mr.TTL(key))
			require.False(t, mr.Exists(userPlatformQuotaDirtySetKey()))
		})
	}
}

func TestUserPlatformQuotaCacheIncrementRejectsInvalidInputWithoutWrites(t *testing.T) {
	for _, name := range []string{"user", "platform", "cost-zero", "cost-negative", "cost-nan", "cost-inf", "ttl-zero", "ttl-negative", "ttl-fractional", "nil-clock", "bad-clock", "empty-dirty-key"} {
		t.Run(name, func(t *testing.T) {
			c, mr := newMiniRedisCache(t)
			ctx := context.Background()
			now := quotaIncrementFixtureTime()
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaIncrementEntryAt(now), time.Minute))
			before := quotaIncrementRaw(t, c, 1)
			userID, platform, cost, ttl, dirtyKey := int64(1), "openai", 0.25, 2*time.Minute, userPlatformQuotaDirtySetKey()
			clock := func() time.Time { return now }
			switch name {
			case "user":
				userID = 0
			case "platform":
				platform = ""
			case "cost-zero":
				cost = 0
			case "cost-negative":
				cost = -0.25
			case "cost-nan":
				cost = math.NaN()
			case "cost-inf":
				cost = math.Inf(1)
			case "ttl-zero":
				ttl = 0
			case "ttl-negative":
				ttl = -time.Second
			case "ttl-fractional":
				ttl = time.Second + time.Nanosecond
			case "nil-clock":
				clock = nil
			case "bad-clock":
				clock = func() time.Time { return time.Unix(-1, 0) }
			case "empty-dirty-key":
				dirtyKey = ""
			}
			result, err := c.incrementUserPlatformQuotaUsageCacheAt(ctx, userID, platform, cost, ttl, true, clock, dirtyKey)
			require.Error(t, err)
			require.Empty(t, result.Status)
			require.Equal(t, before, quotaIncrementRaw(t, c, 1))
			require.Equal(t, time.Minute, mr.TTL(userPlatformQuotaCacheKey(1, "openai")))
			require.False(t, mr.Exists(userPlatformQuotaDirtySetKey()))
		})
	}
}

func TestUserPlatformQuotaCacheIncrementRejectsCorruptOrUnrepresentableValues(t *testing.T) {
	for _, tc := range []struct {
		name, field, value string
		cost               float64
		remove, expired    bool
	}{
		{name: "nan", field: "weekly_usage", value: "NaN"},
		{name: "infinity", field: "monthly_usage", value: "+Inf"},
		{name: "negative", field: "daily_usage", value: "-1"},
		{name: "missing-usage", field: "daily_usage", remove: true},
		{name: "bad-limit", field: "daily_limit", value: "bad"},
		{name: "negative-version", field: "version", value: "-1"},
		{name: "bad-schema", field: "schema_version", value: "1.0"},
		{name: "bad-instance", field: "cache_instance", value: "x"},
		{name: "partial-token", field: "control_generation", remove: true},
		{name: "bad-token", field: "quota_row_id", value: "bad"},
		{name: "bad-window", field: "monthly_window_start", value: "99999999999999999999"},
		{name: "future-window", field: "monthly_window_start", value: strconv.FormatInt(quotaIncrementFixtureTime().Add(time.Second).Unix(), 10)},
		{name: "does-not-advance", field: "daily_usage", value: "100000000000000000000", cost: 0.01},
		{name: "overflow", field: "daily_usage", value: "1e308", cost: 1e308},
		{name: "raw-underflow", field: "daily_usage", value: "0." + strings.Repeat("0", 399) + "1"},
		{name: "huge-exponent", field: "daily_usage", value: "1e2147483647"},
		{name: "small-exponent-zero", field: "daily_usage", value: "0e-1075"},
		{name: "huge-positive-zero", field: "daily_usage", value: "0e309"},
		{name: "too-long", field: "daily_usage", value: strings.Repeat("0", 2049)},
		{name: "expired-corrupt-still-rejected", field: "daily_usage", value: "0e-1075", expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, mr := newMiniRedisCache(t)
			ctx, now := context.Background(), quotaIncrementFixtureTime()
			entry := quotaIncrementEntryAt(now)
			if tc.expired {
				old := timezone.StartOfDay(now).Add(-time.Second)
				entry.DailyWindowStart = &old
			}
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, time.Minute))
			key := userPlatformQuotaCacheKey(1, "openai")
			if tc.remove {
				require.NoError(t, c.rdb.HDel(ctx, key, tc.field).Err())
			} else {
				require.NoError(t, c.rdb.HSet(ctx, key, tc.field, tc.value).Err())
			}
			before := quotaIncrementRaw(t, c, 1)
			cost := tc.cost
			if cost == 0 {
				cost = 0.25
			}
			result, err := quotaIncrementAt(c, 1, cost, now, true)
			require.Error(t, err)
			require.Empty(t, result.Status)
			require.Equal(t, before, quotaIncrementRaw(t, c, 1))
			require.Equal(t, time.Minute, mr.TTL(key))
			require.False(t, mr.Exists(userPlatformQuotaDirtySetKey()))
		})
	}
}

func TestUserPlatformQuotaCacheIncrementRevisionRemainsExactAndExhausts(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx, now := context.Background(), quotaIncrementFixtureTime()
	entry := quotaIncrementEntryAt(now)
	entry.Version = math.MaxInt64 - 1
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, time.Minute))
	result, err := quotaIncrementAt(c, 1, 0.25, now, false)
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementApplied, result.Status)
	raw := quotaIncrementRaw(t, c, 1)
	require.Equal(t, "9223372036854775807", raw["version"])
	require.Equal(t, "9007199254740993", raw["quota_row_id"])
	require.Equal(t, "9223372036854775807", raw["control_generation"])
	mr.FastForward(10 * time.Second)
	ttl := mr.TTL(userPlatformQuotaCacheKey(1, "openai"))
	result, err = quotaIncrementAt(c, 1, 0.25, now, true)
	require.Error(t, err)
	require.Empty(t, result.Status)
	require.Equal(t, raw, quotaIncrementRaw(t, c, 1))
	require.Equal(t, ttl, mr.TTL(userPlatformQuotaCacheKey(1, "openai")))
	require.False(t, mr.Exists(userPlatformQuotaDirtySetKey()))
}

type quotaIncrementHook struct {
	before func(context.Context, redis.Cmder) error
}

func (h *quotaIncrementHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *quotaIncrementHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *quotaIncrementHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" && len(cmd.Args()) > 1 && cmd.Args()[1] == incrementUserPlatformQuotaUsageScript {
			if err := h.before(ctx, cmd); err != nil {
				return err
			}
		}
		return next(ctx, cmd)
	}
}

func quotaIncrementHookClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestUserPlatformQuotaCacheIncrementDecimalBoundsAcceptZeroAndSmallestObservation(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		cost      float64
	}{
		{"zero-min-exponent", "0e-1074", 0.25},
		{"zero-max-exponent", "0e308", 0.25},
		{"zero-max-length", strings.Repeat("0", 2048), 0.25},
		{"smallest-positive-observation", "5e-324", math.SmallestNonzeroFloat64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newMiniRedisCache(t)
			ctx, now := context.Background(), quotaIncrementFixtureTime()
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaIncrementEntryAt(now), time.Minute))
			key := userPlatformQuotaCacheKey(1, "openai")
			require.NoError(t, c.rdb.HSet(ctx, key, "daily_usage", tc.raw, "weekly_usage", tc.raw, "monthly_usage", tc.raw).Err())
			before, err := strconv.ParseFloat(tc.raw, 64)
			require.NoError(t, err)
			result, err := quotaIncrementAt(c, 1, tc.cost, now, false)
			require.NoError(t, err)
			require.Equal(t, service.QuotaIncrementApplied, result.Status)
			for _, field := range []string{"daily_usage", "weekly_usage", "monthly_usage"} {
				after, parseErr := strconv.ParseFloat(quotaIncrementRaw(t, c, 1)[field], 64)
				require.NoError(t, parseErr)
				require.Greater(t, after, before)
			}
		})
	}
}

func TestUserPlatformQuotaCacheIncrementWrongTypesNeverPartiallyWrite(t *testing.T) {
	for _, name := range []string{"quota-wrongtype", "dirty-wrongtype", "dirty-ignored", "quota-type-race"} {
		t.Run(name, func(t *testing.T) {
			c, mr := newMiniRedisCache(t)
			ctx, now := context.Background(), quotaIncrementFixtureTime()
			key, dirtyKey := userPlatformQuotaCacheKey(1, "openai"), userPlatformQuotaDirtySetKey()
			markDirty := name != "dirty-ignored"
			if name == "quota-wrongtype" {
				require.NoError(t, c.rdb.Set(ctx, key, "quota-fixture", time.Minute).Err())
			} else {
				require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaIncrementEntryAt(now), time.Minute))
			}
			var before map[string]string
			if name != "quota-wrongtype" {
				before = quotaIncrementRaw(t, c, 1)
			}
			if strings.HasPrefix(name, "dirty-") {
				require.NoError(t, c.rdb.Set(ctx, dirtyKey, "dirty-fixture", 3*time.Minute).Err())
			}
			if name == "quota-type-race" {
				other := quotaIncrementHookClient(t, mr.Addr())
				c.rdb.AddHook(&quotaIncrementHook{before: func(ctx context.Context, _ redis.Cmder) error {
					if err := other.Del(ctx, key).Err(); err != nil {
						return err
					}
					return other.Set(ctx, key, "quota-fixture", time.Minute).Err()
				}})
			}
			result, err := quotaIncrementAt(c, 1, 0.25, now, markDirty)
			if name == "dirty-ignored" {
				require.NoError(t, err)
				require.Equal(t, service.QuotaIncrementApplied, result.Status)
				require.Equal(t, "1.25", quotaIncrementRaw(t, c, 1)["daily_usage"])
				require.Equal(t, 2*time.Minute, mr.TTL(key))
			} else {
				require.Error(t, err)
				require.Empty(t, result.Status)
				if name == "dirty-wrongtype" {
					require.Equal(t, before, quotaIncrementRaw(t, c, 1))
				} else {
					require.Equal(t, "quota-fixture", c.rdb.Get(ctx, key).Val())
				}
				require.Equal(t, time.Minute, mr.TTL(key))
			}
			if strings.HasPrefix(name, "dirty-") {
				require.Equal(t, "dirty-fixture", c.rdb.Get(ctx, dirtyKey).Val())
				require.Equal(t, 3*time.Minute, mr.TTL(dirtyKey))
			} else {
				require.False(t, mr.Exists(dirtyKey))
			}
		})
	}
}

func TestUserPlatformQuotaCacheIncrementComparesEveryRawValueAndPresence(t *testing.T) {
	for _, removed := range []bool{false, true} {
		for _, field := range quotaIncrementObservedFields {
			name := field + "/value"
			if removed {
				name = field + "/presence"
			}
			t.Run(name, func(t *testing.T) {
				c, mr := newMiniRedisCache(t)
				ctx, now := context.Background(), quotaIncrementFixtureTime()
				require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaIncrementEntryAt(now), time.Minute))
				key := userPlatformQuotaCacheKey(1, "openai")
				before := quotaIncrementRaw(t, c, 1)
				values := map[string]string{
					"cache_instance": strings.Repeat("b", 64), "version": "18", "quota_row_id": "9007199254740994", "control_generation": "9223372036854775806", "schema_version": "0",
					"daily_usage": "1.0", "weekly_usage": "2.0", "monthly_usage": "3.0", "daily_limit": "100.0", "weekly_limit": "200.0", "monthly_limit": "300.0",
				}
				for _, window := range []string{"daily_window_start", "weekly_window_start", "monthly_window_start"} {
					seconds, err := strconv.ParseInt(before[window], 10, 64)
					require.NoError(t, err)
					values[window] = strconv.FormatInt(seconds+1, 10)
				}
				other := quotaIncrementHookClient(t, mr.Addr())
				calls := 0
				var externallyChanged map[string]string
				c.rdb.AddHook(&quotaIncrementHook{before: func(ctx context.Context, _ redis.Cmder) error {
					calls++
					if calls != 1 {
						return nil
					}
					var err error
					if removed {
						err = other.HDel(ctx, key, field).Err()
					} else {
						err = other.HSet(ctx, key, field, values[field]).Err()
					}
					if err != nil {
						return err
					}
					externallyChanged, err = other.HGetAll(ctx, key).Result()
					return err
				}})
				result, err := quotaIncrementAt(c, 1, 0.25, now, true)
				incompatible := field == "schema_version" || removed && field == "cache_instance"
				if removed && !incompatible {
					require.Error(t, err)
					require.Empty(t, result.Status)
				} else if incompatible {
					require.NoError(t, err)
					require.Equal(t, service.QuotaIncrementIncompatible, result.Status)
				} else {
					require.NoError(t, err)
					require.Equal(t, service.QuotaIncrementApplied, result.Status)
					require.Equal(t, 2, calls, "raw numeric equality does not bypass the observed string fence")
					after := quotaIncrementRaw(t, c, 1)
					require.Equal(t, "1.25", after["daily_usage"])
					require.Equal(t, "2.25", after["weekly_usage"])
					require.Equal(t, "3.25", after["monthly_usage"])
					wantVersion := "18"
					if field == "version" {
						wantVersion = "19"
					}
					require.Equal(t, wantVersion, after["version"])
					if !strings.HasSuffix(field, "usage") && field != "version" {
						require.Equal(t, values[field], after[field])
					}
					return
				}
				require.Equal(t, 1, calls)
				require.Equal(t, externallyChanged, quotaIncrementRaw(t, c, 1), "a stale calculation writes no partial fields")
				require.Equal(t, time.Minute, mr.TTL(key))
				require.False(t, mr.Exists(userPlatformQuotaDirtySetKey()))
			})
		}
	}
}

func TestUserPlatformQuotaCacheIncrementContentionHasThreeAttemptsAndNoOwnWrites(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx, now := context.Background(), quotaIncrementFixtureTime()
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaIncrementEntryAt(now), time.Minute))
	key := userPlatformQuotaCacheKey(1, "openai")
	before := quotaIncrementRaw(t, c, 1)
	other := quotaIncrementHookClient(t, mr.Addr())
	calls, clockCalls := 0, 0
	c.rdb.AddHook(&quotaIncrementHook{before: func(ctx context.Context, _ redis.Cmder) error {
		calls++
		return other.HSet(ctx, key, "daily_usage", "1."+strings.Repeat("0", calls)).Err()
	}})
	result, err := c.incrementUserPlatformQuotaUsageCacheAt(ctx, 1, "openai", 0.25, 2*time.Minute, true, func() time.Time { clockCalls++; return now }, userPlatformQuotaDirtySetKey())
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementContended, result.Status)
	require.Equal(t, 3, calls)
	require.Equal(t, 3, clockCalls)
	before["daily_usage"] = "1.000"
	require.Equal(t, before, quotaIncrementRaw(t, c, 1))
	require.Equal(t, time.Minute, mr.TTL(key))
	require.False(t, mr.Exists(userPlatformQuotaDirtySetKey()))
}

func TestUserPlatformQuotaCacheIncrementRetryRereadsClockAcrossAllWindowBoundaries(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx := context.Background()
	afterBoundary := time.Date(2026, 10, 26, 0, 0, 0, 0, timezone.Location())
	beforeBoundary := afterBoundary.Add(-time.Second)
	entry := quotaIncrementEntryAt(beforeBoundary)
	month := afterBoundary.Add(-720 * time.Hour)
	entry.MonthlyWindowStart = &month
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, time.Minute))
	key := userPlatformQuotaCacheKey(1, "openai")
	other := quotaIncrementHookClient(t, mr.Addr())
	calls, clockCalls := 0, 0
	c.rdb.AddHook(&quotaIncrementHook{before: func(ctx context.Context, _ redis.Cmder) error {
		calls++
		if calls == 1 {
			return other.HSet(ctx, key, "daily_usage", "1.0").Err()
		}
		return nil
	}})
	result, err := c.incrementUserPlatformQuotaUsageCacheAt(ctx, 1, "openai", 0.25, 2*time.Minute, false, func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return beforeBoundary
		}
		return afterBoundary
	}, userPlatformQuotaDirtySetKey())
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementApplied, result.Status)
	require.Equal(t, 2, calls)
	require.Equal(t, 2, clockCalls)
	raw := quotaIncrementRaw(t, c, 1)
	for _, field := range []string{"daily_usage", "weekly_usage", "monthly_usage"} {
		require.Equal(t, "0.25", raw[field], field)
	}
	for _, field := range []string{"daily_window_start", "weekly_window_start", "monthly_window_start"} {
		require.Equal(t, strconv.FormatInt(afterBoundary.Unix(), 10), raw[field], field)
	}
	require.Equal(t, "18", raw["version"])
}

func TestUserPlatformQuotaCacheIncrementDeletionBetweenReadAndCommitIsMissing(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx, now := context.Background(), quotaIncrementFixtureTime()
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaIncrementEntryAt(now), time.Minute))
	other := quotaIncrementHookClient(t, mr.Addr())
	calls := 0
	c.rdb.AddHook(&quotaIncrementHook{before: func(ctx context.Context, _ redis.Cmder) error {
		calls++
		return other.Del(ctx, userPlatformQuotaCacheKey(1, "openai")).Err()
	}})
	result, err := quotaIncrementAt(c, 1, 0.25, now, true)
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementMissing, result.Status)
	require.Equal(t, 1, calls)
	require.False(t, mr.Exists(userPlatformQuotaCacheKey(1, "openai")))
	require.False(t, mr.Exists(userPlatformQuotaDirtySetKey()))
}

func TestUserPlatformQuotaCacheIncrementFollowsAdmissionCASAndFencesStaleCAS(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx, now := context.Background(), quotaIncrementFixtureTime()
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaIncrementEntryAt(now), time.Minute))
	observed := quotaCacheCASRead(t, c)
	next := *observed
	next.DailyUsageUSD, next.WeeklyUsageUSD, next.MonthlyUsageUSD = 0, 0, 0
	other := &billingCache{rdb: quotaIncrementHookClient(t, mr.Addr())}
	calls := 0
	var freshInstance string
	c.rdb.AddHook(&quotaIncrementHook{before: func(ctx context.Context, _ redis.Cmder) error {
		calls++
		if calls != 1 {
			return nil
		}
		applied, err := other.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", observed, &next, time.Minute)
		require.NoError(t, err)
		require.True(t, applied)
		freshInstance = quotaIncrementRaw(t, other, 1)["cache_instance"]
		return nil
	}})
	result, err := quotaIncrementAt(c, 1, 0.25, now, false)
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementApplied, result.Status)
	require.Equal(t, 2, calls)
	after := quotaIncrementRaw(t, c, 1)
	require.NotEqual(t, observed.CacheInstance, freshInstance)
	require.Equal(t, freshInstance, after["cache_instance"])
	require.Equal(t, "19", after["version"])
	for _, field := range []string{"daily_usage", "weekly_usage", "monthly_usage"} {
		require.Equal(t, "0.25", after[field], field)
	}
	applied, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", observed, &next, time.Minute)
	require.NoError(t, err)
	require.False(t, applied)
	require.Equal(t, after, quotaIncrementRaw(t, c, 1))
}

func TestUserPlatformQuotaCacheIncrementRealRedisConcurrentAppliedCostsMatchRevision(t *testing.T) {
	c, userID := realQuotaCacheCASFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	now := quotaIncrementFixtureTime()
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", quotaIncrementEntryAt(now), time.Minute))
	before := quotaIncrementRaw(t, c, userID)
	key := userPlatformQuotaCacheKey(userID, "openai")
	dirtyKey := key + ":increment-test-dirty"
	t.Cleanup(func() { _ = c.rdb.Del(context.Background(), dirtyKey).Err() })
	const workers = 24
	type outcome struct {
		result service.UserPlatformQuotaIncrementResult
		err    error
	}
	results := make(chan outcome, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := c.incrementUserPlatformQuotaUsageCacheAt(ctx, userID, "openai", 0.01, 2*time.Minute, true, func() time.Time { return now }, dirtyKey)
			results <- outcome{result, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	applied := 0
	for got := range results {
		require.NoError(t, got.err)
		switch got.result.Status {
		case service.QuotaIncrementApplied:
			applied++
		case service.QuotaIncrementContended:
		default:
			t.Fatalf("unexpected increment status %q", got.result.Status)
		}
	}
	require.Greater(t, applied, 0)
	after := quotaIncrementRaw(t, c, userID)
	for i, field := range []string{"daily_usage", "weekly_usage", "monthly_usage"} {
		require.Equal(t, strconv.FormatFloat(float64((i+1)*100+applied)/100, 'f', -1, 64), after[field], field)
	}
	require.Equal(t, strconv.Itoa(17+applied), after["version"])
	for _, field := range []string{"cache_instance", "quota_row_id", "control_generation", "schema_version", "daily_limit", "weekly_limit", "monthly_limit", "daily_window_start", "weekly_window_start", "monthly_window_start"} {
		require.Equal(t, before[field], after[field], field)
	}
	members, err := c.rdb.SMembers(ctx, dirtyKey).Result()
	require.NoError(t, err)
	require.Equal(t, []string{userPlatformQuotaDirtyMember(userID, "openai")}, members)
}

func TestUserPlatformQuotaCacheIncrementRealRedisWrongDirtyTypePreservesHashAndExpiries(t *testing.T) {
	c, userID := realQuotaCacheCASFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := quotaIncrementFixtureTime()
	key := userPlatformQuotaCacheKey(userID, "openai")
	dirtyKey := key + ":increment-test-dirty"
	t.Cleanup(func() { _ = c.rdb.Del(context.Background(), dirtyKey).Err() })
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", quotaIncrementEntryAt(now), time.Minute))
	require.NoError(t, c.rdb.Set(ctx, dirtyKey, "fixture", 3*time.Minute).Err())
	before := quotaIncrementRaw(t, c, userID)
	quotaExpiry, err := c.rdb.Do(ctx, "PEXPIRETIME", key).Int64()
	require.NoError(t, err)
	dirtyExpiry, err := c.rdb.Do(ctx, "PEXPIRETIME", dirtyKey).Int64()
	require.NoError(t, err)
	result, err := c.incrementUserPlatformQuotaUsageCacheAt(ctx, userID, "openai", 0.25, 2*time.Minute, true, func() time.Time { return now }, dirtyKey)
	require.Error(t, err)
	require.Empty(t, result.Status)
	require.Equal(t, before, quotaIncrementRaw(t, c, userID))
	afterQuotaExpiry, err := c.rdb.Do(ctx, "PEXPIRETIME", key).Int64()
	require.NoError(t, err)
	require.Equal(t, quotaExpiry, afterQuotaExpiry)
	afterDirtyExpiry, err := c.rdb.Do(ctx, "PEXPIRETIME", dirtyKey).Int64()
	require.NoError(t, err)
	require.Equal(t, dirtyExpiry, afterDirtyExpiry)
	require.Equal(t, "fixture", c.rdb.Get(ctx, dirtyKey).Val())
	// No dirty write is requested here, so an unrelated dirty key is ignored.
	result, err = c.incrementUserPlatformQuotaUsageCacheAt(ctx, userID, "openai", 0.25, 2*time.Minute, false, func() time.Time { return now }, dirtyKey)
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementApplied, result.Status)
	after := quotaIncrementRaw(t, c, userID)
	require.Equal(t, "1.25", after["daily_usage"])
	require.Equal(t, "18", after["version"])
	afterDirtyExpiry, err = c.rdb.Do(ctx, "PEXPIRETIME", dirtyKey).Int64()
	require.NoError(t, err)
	require.Equal(t, dirtyExpiry, afterDirtyExpiry)
	require.Equal(t, "fixture", c.rdb.Get(ctx, dirtyKey).Val())
}
