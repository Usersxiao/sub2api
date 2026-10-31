//go:build unit

package repository

import (
	"context"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func quotaCacheCASEntry() *service.UserPlatformQuotaCacheEntry {
	now := time.Now()
	day, week, start := timezone.StartOfDay(now), timezone.StartOfWeek(now), now.UTC().Add(-time.Hour).Truncate(time.Second)
	limit := 100.0
	return &service.UserPlatformQuotaCacheEntry{QuotaRowID: 9007199254740993, ControlGeneration: math.MaxInt64,
		SchemaVersion: service.UserPlatformQuotaCacheSchemaV1, DailyLimitUSD: &limit, DailyUsageUSD: 1, WeeklyUsageUSD: 2, MonthlyUsageUSD: 3,
		DailyWindowStart: &day, WeeklyWindowStart: &week, MonthlyWindowStart: &start}
}

func quotaCacheCASRead(t *testing.T, c *billingCache) *service.UserPlatformQuotaCacheEntry {
	t.Helper()
	entry, ok, err := c.GetUserPlatformQuotaCache(context.Background(), 1, "openai")
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, entry)
	return entry
}

func TestUserPlatformQuotaCacheCASNXAndRotationUseFreshInstances(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx := context.Background()
	next := quotaCacheCASEntry()
	next.Version = 19 // NX assigns its own initial revision.
	ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", nil, next, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	first := quotaCacheCASRead(t, c)
	require.True(t, validUserPlatformQuotaCacheInstance(first.CacheInstance))
	require.Equal(t, int64(0), first.Version)
	require.Empty(t, next.CacheInstance, "CAS never mutates caller-owned input")
	require.Equal(t, int64(19), next.Version)
	mr.FastForward(10 * time.Second)
	ttl := mr.TTL(userPlatformQuotaCacheKey(1, "openai"))
	ok, err = c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", nil, next, 2*time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, ttl, mr.TTL(userPlatformQuotaCacheKey(1, "openai")), "NX collision never renews TTL")
	require.Equal(t, first, quotaCacheCASRead(t, c))
	replacement := *first
	replacement.DailyUsageUSD = 0
	ok, err = c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", first, &replacement, 2*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	after := quotaCacheCASRead(t, c)
	require.NotEqual(t, first.CacheInstance, after.CacheInstance)
	require.Equal(t, first.Version+1, after.Version)
	require.Zero(t, after.DailyUsageUSD)
	require.Equal(t, first.WeeklyUsageUSD, after.WeeklyUsageUSD)
	require.Equal(t, first.QuotaRowID, after.QuotaRowID)
	require.Equal(t, first.ControlGeneration, after.ControlGeneration)
}

func TestUserPlatformQuotaCacheCASConcurrentNXAndObservedRevisionHaveOneWinner(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx := context.Background()
	const workers = 12
	for _, fill := range []bool{true, false} {
		var expected *service.UserPlatformQuotaCacheEntry
		if !fill {
			expected = quotaCacheCASRead(t, c)
		}
		start := make(chan struct{})
		type result struct {
			applied bool
			err     error
		}
		results := make(chan result, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				applied, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", expected, quotaCacheCASEntry(), time.Minute)
				results <- result{applied, err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		winners := 0
		for got := range results {
			require.NoError(t, got.err)
			if got.applied {
				winners++
			}
		}
		require.Equal(t, 1, winners)
	}
	require.Equal(t, int64(1), quotaCacheCASRead(t, c).Version)
}

func TestUserPlatformQuotaCacheCASRejectsConcurrentIncrementWithoutTTLRefresh(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx := context.Background()
	ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", nil, quotaCacheCASEntry(), time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	observed := quotaCacheCASRead(t, c)
	result, incrementErr := c.IncrUserPlatformQuotaUsageCache(ctx, 1, "openai", 0.25, time.Minute, true)
	require.NoError(t, incrementErr)
	require.Equal(t, service.QuotaIncrementApplied, result.Status)
	afterIncr := quotaCacheCASRead(t, c)
	require.Equal(t, observed.CacheInstance, afterIncr.CacheInstance)
	require.Equal(t, observed.Version+1, afterIncr.Version)
	mr.FastForward(10 * time.Second)
	ttl := mr.TTL(userPlatformQuotaCacheKey(1, "openai"))
	ok, err = c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", observed, observed, 2*time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, ttl, mr.TTL(userPlatformQuotaCacheKey(1, "openai")))
	require.Equal(t, afterIncr, quotaCacheCASRead(t, c), "a stale window refresh preserves the concurrent cost")
}

func TestUserPlatformQuotaCacheCASInstanceClosesRawSetAndDeleteRecreateABA(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		name := "raw-set"
		if deleted {
			name = "delete-recreate"
		}
		t.Run(name, func(t *testing.T) {
			c, mr := newMiniRedisCache(t)
			ctx := context.Background()
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaCacheCASEntry(), time.Minute))
			first := quotaCacheCASRead(t, c)
			if deleted {
				require.NoError(t, c.DeleteUserPlatformQuotaCache(ctx, 1, "openai"))
			}
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", first, time.Minute))
			current := quotaCacheCASRead(t, c)
			require.NotEqual(t, first.CacheInstance, current.CacheInstance)
			require.Equal(t, first.Version, current.Version)
			require.Equal(t, first.QuotaRowID, current.QuotaRowID)
			require.Equal(t, first.ControlGeneration, current.ControlGeneration)
			mr.FastForward(10 * time.Second)
			ttl := mr.TTL(userPlatformQuotaCacheKey(1, "openai"))
			ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", first, first, 2*time.Minute)
			require.NoError(t, err)
			require.False(t, ok)
			require.Equal(t, ttl, mr.TTL(userPlatformQuotaCacheKey(1, "openai")))
			require.Equal(t, current, quotaCacheCASRead(t, c))
		})
	}
}

func TestUserPlatformQuotaCacheCASComparesEveryIdentityFieldExactly(t *testing.T) {
	for _, field := range []string{"cache_instance", "version", "quota_row_id", "control_generation", "schema_version"} {
		t.Run(field, func(t *testing.T) {
			c, mr := newMiniRedisCache(t)
			ctx := context.Background()
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaCacheCASEntry(), time.Minute))
			expected := quotaCacheCASRead(t, c)
			value := "1"
			if field == "cache_instance" {
				value = strings.Repeat("a", 64)
			}
			if field == "schema_version" {
				value = "0"
			}
			require.NoError(t, c.rdb.HSet(ctx, userPlatformQuotaCacheKey(1, "openai"), field, value).Err())
			mr.FastForward(10 * time.Second)
			ttl := mr.TTL(userPlatformQuotaCacheKey(1, "openai"))
			before, err := c.rdb.HGetAll(ctx, userPlatformQuotaCacheKey(1, "openai")).Result()
			require.NoError(t, err)
			ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", expected, expected, 2*time.Minute)
			require.NoError(t, err)
			require.False(t, ok)
			after, err := c.rdb.HGetAll(ctx, userPlatformQuotaCacheKey(1, "openai")).Result()
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, ttl, mr.TTL(userPlatformQuotaCacheKey(1, "openai")))
		})
	}
}

func TestUserPlatformQuotaCacheCASExactInt64RevisionAndExhaustion(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx := context.Background()
	entry := quotaCacheCASEntry()
	entry.Version = math.MaxInt64 - 1
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, time.Minute))
	observed := quotaCacheCASRead(t, c)
	ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", observed, observed, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	after := quotaCacheCASRead(t, c)
	require.Equal(t, int64(math.MaxInt64), after.Version)
	mr.FastForward(10 * time.Second)
	ttl := mr.TTL(userPlatformQuotaCacheKey(1, "openai"))
	ok, err = c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", after, after, 2*time.Minute)
	require.Error(t, err)
	require.False(t, ok)
	require.Equal(t, after, quotaCacheCASRead(t, c))
	require.Equal(t, ttl, mr.TTL(userPlatformQuotaCacheKey(1, "openai")))
}

func TestUserPlatformQuotaCacheCASDoesNotRoundAdjacentLargeIdentityIntegers(t *testing.T) {
	for _, field := range []string{"row", "generation", "version"} {
		t.Run(field, func(t *testing.T) {
			c, _ := newMiniRedisCache(t)
			ctx := context.Background()
			entry := quotaCacheCASEntry()
			entry.Version = math.MaxInt64 - 2
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, time.Minute))
			before := quotaCacheCASRead(t, c)
			expected := *before
			switch field {
			case "row":
				expected.QuotaRowID--
			case "generation":
				expected.ControlGeneration--
			case "version":
				expected.Version--
			}
			ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", &expected, &expected, time.Minute)
			require.NoError(t, err)
			require.False(t, ok, "adjacent decimal int64 values must not collapse through Lua numbers")
			require.Equal(t, before, quotaCacheCASRead(t, c))
		})
	}
}

func TestUserPlatformQuotaCacheCASRejectsInvalidShapeAndTTLBeforeWriting(t *testing.T) {
	for _, name := range []string{"nil-next", "nil-window", "missing-row", "wrong-schema", "negative-version", "nan-usage", "infinite-limit", "invalid-instance", "missing-expected-instance", "identity-transition", "ttl-zero", "ttl-negative", "ttl-fractional"} {
		t.Run(name, func(t *testing.T) {
			c, mr := newMiniRedisCache(t)
			ctx := context.Background()
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaCacheCASEntry(), time.Minute))
			expected := quotaCacheCASRead(t, c)
			next := *expected
			nextPtr := &next
			ttl := 2 * time.Minute
			switch name {
			case "nil-next":
				nextPtr = nil
			case "nil-window":
				next.MonthlyWindowStart = nil
			case "missing-row":
				next.QuotaRowID = 0
			case "wrong-schema":
				next.SchemaVersion = 0
			case "negative-version":
				next.Version = -1
			case "nan-usage":
				next.WeeklyUsageUSD = math.NaN()
			case "infinite-limit":
				value := math.Inf(1)
				next.WeeklyLimitUSD = &value
			case "invalid-instance":
				next.CacheInstance = "bad"
			case "missing-expected-instance":
				expected.CacheInstance = ""
			case "identity-transition":
				next.ControlGeneration = 2
			case "ttl-zero":
				ttl = 0
			case "ttl-negative":
				ttl = -time.Second
			case "ttl-fractional":
				ttl = time.Second + time.Nanosecond
			}
			before, err := c.rdb.HGetAll(ctx, userPlatformQuotaCacheKey(1, "openai")).Result()
			require.NoError(t, err)
			beforeTTL := mr.TTL(userPlatformQuotaCacheKey(1, "openai"))
			ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", expected, nextPtr, ttl)
			require.Error(t, err)
			require.False(t, ok)
			after, err := c.rdb.HGetAll(ctx, userPlatformQuotaCacheKey(1, "openai")).Result()
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, beforeTTL, mr.TTL(userPlatformQuotaCacheKey(1, "openai")))
		})
	}
}

func TestUserPlatformQuotaCacheStrictReadRejectsMalformedValuesInGetAndBatch(t *testing.T) {
	for _, tc := range []struct {
		field, value string
		remove       bool
	}{
		{"daily_usage", "bad", false}, {"weekly_usage", "NaN", false}, {"monthly_usage", "+Inf", false}, {"daily_usage", "-1", false}, {"daily_usage", "", true},
		{"daily_limit", "bad", false}, {"weekly_limit", "NaN", false}, {"monthly_limit", "-1", false}, {"monthly_limit", "", true},
		{"version", "-1", false}, {"version", "bad", false}, {"version", "9223372036854775808", false}, {"version", "", true},
		{"schema_version", "-1", false}, {"schema_version", "2", false}, {"schema_version", "bad", false},
		{"daily_window_start", "bad", false}, {"weekly_window_start", "-1", false}, {"monthly_window_start", "253402300800", false}, {"daily_window_start", "", true},
		{"cache_instance", "bad", false}, {"cache_instance", strings.Repeat("A", 64), false},
	} {
		t.Run(tc.field+"-"+tc.value, func(t *testing.T) {
			c, _ := newMiniRedisCache(t)
			ctx := context.Background()
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaCacheCASEntry(), time.Minute))
			if tc.remove {
				require.NoError(t, c.rdb.HDel(ctx, userPlatformQuotaCacheKey(1, "openai"), tc.field).Err())
			} else {
				require.NoError(t, c.rdb.HSet(ctx, userPlatformQuotaCacheKey(1, "openai"), tc.field, tc.value).Err())
			}
			entry, ok, err := c.GetUserPlatformQuotaCache(ctx, 1, "openai")
			require.Error(t, err)
			require.False(t, ok)
			require.Nil(t, entry)
			batch, err := c.BatchGetUserPlatformQuotaCache(ctx, []service.UserPlatformQuotaKey{{UserID: 2, Platform: "gemini"}, {UserID: 1, Platform: "openai"}})
			require.Error(t, err)
			require.Nil(t, batch, "never return partially trusted usage on a malformed batch member")
		})
	}
}

func TestUserPlatformQuotaCacheLegacyShapeReadsButCannotAuthorizeCAS(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx := context.Background()
	require.NoError(t, c.rdb.HSet(ctx, userPlatformQuotaCacheKey(1, "openai"), "daily_usage", "1", "weekly_usage", "2", "monthly_usage", "3").Err())
	legacy := quotaCacheCASRead(t, c)
	require.Zero(t, legacy.SchemaVersion)
	require.Empty(t, legacy.CacheInstance)
	updated := quotaCacheCASEntry()
	ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", legacy, updated, time.Minute)
	require.Error(t, err)
	require.False(t, ok)
	ok, err = c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", nil, updated, time.Minute)
	require.NoError(t, err)
	require.False(t, ok, "NX never overwrites an existing legacy hash")
	require.Equal(t, legacy, quotaCacheCASRead(t, c))
}

func TestUserPlatformQuotaCacheRawSetInvalidInputLeavesStoredValueAndTTL(t *testing.T) {
	for _, name := range []string{"nil", "negative-usage", "nan-limit", "bad-version", "bad-schema", "partial-token", "negative-token", "bad-time", "time-zone-overflow", "ttl-zero", "ttl-fractional"} {
		t.Run(name, func(t *testing.T) {
			c, mr := newMiniRedisCache(t)
			ctx := context.Background()
			require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaCacheCASEntry(), time.Minute))
			before := quotaCacheCASRead(t, c)
			candidate := *before
			entry := &candidate
			ttl := 2 * time.Minute
			switch name {
			case "nil":
				entry = nil
			case "negative-usage":
				candidate.MonthlyUsageUSD = -1
			case "nan-limit":
				value := math.NaN()
				candidate.MonthlyLimitUSD = &value
			case "bad-version":
				candidate.Version = -1
			case "bad-schema":
				candidate.SchemaVersion = 2
			case "partial-token":
				candidate.QuotaRowID = 0
			case "negative-token":
				candidate.ControlGeneration = -1
			case "bad-time":
				value := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
				candidate.DailyWindowStart = &value
			case "time-zone-overflow":
				value := time.Date(9999, 12, 31, 23, 59, 59, 0, time.FixedZone("fixture-west", -3600))
				candidate.DailyWindowStart = &value
			case "ttl-zero":
				ttl = 0
			case "ttl-fractional":
				ttl = time.Second + time.Nanosecond
			}
			beforeTTL := mr.TTL(userPlatformQuotaCacheKey(1, "openai"))
			require.Error(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, ttl))
			require.Equal(t, before, quotaCacheCASRead(t, c))
			require.Equal(t, beforeTTL, mr.TTL(userPlatformQuotaCacheKey(1, "openai")))
		})
	}
}

func TestUserPlatformQuotaCacheCASDeletedObservationAndCancelledWriteStayAbsent(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx := context.Background()
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaCacheCASEntry(), time.Minute))
	before := quotaCacheCASRead(t, c)
	require.NoError(t, c.DeleteUserPlatformQuotaCache(ctx, 1, "openai"))
	ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", before, before, time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	ok, err = c.CompareAndSetUserPlatformQuotaCache(cancelled, 1, "openai", nil, quotaCacheCASEntry(), time.Minute)
	require.Error(t, err)
	require.False(t, ok)
	_, found, err := c.GetUserPlatformQuotaCache(ctx, 1, "openai")
	require.NoError(t, err)
	require.False(t, found)
}

func TestUserPlatformQuotaCacheCASAndRawSetWrongTypePreserveValueAndTTL(t *testing.T) {
	c, mr := newMiniRedisCache(t)
	ctx := context.Background()
	key := userPlatformQuotaCacheKey(1, "openai")
	require.NoError(t, c.rdb.Set(ctx, key, "fixture", time.Minute).Err())
	ttl := mr.TTL(key)
	ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 1, "openai", nil, quotaCacheCASEntry(), 2*time.Minute)
	require.Error(t, err)
	require.False(t, ok)
	require.Error(t, c.SetUserPlatformQuotaCache(ctx, 1, "openai", quotaCacheCASEntry(), 2*time.Minute))
	value, err := c.rdb.Get(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "fixture", value)
	require.Equal(t, ttl, mr.TTL(key))
}

func realQuotaCacheCASFixture(t *testing.T) (*billingCache, int64) {
	t.Helper()
	addr := os.Getenv("QUOTA_CACHE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set QUOTA_CACHE_TEST_REDIS_ADDR to a disposable Redis server")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, rdb.Ping(ctx).Err())
	userID := time.Now().UnixNano()
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(), userPlatformQuotaCacheKey(userID, "openai")).Err()
		_ = rdb.Close()
	})
	return &billingCache{rdb: rdb}, userID
}

func TestUserPlatformQuotaCacheCASRealRedisConcurrentNXAndCAS(t *testing.T) {
	c, userID := realQuotaCacheCASFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const workers = 12
	for _, fill := range []bool{true, false} {
		var expected *service.UserPlatformQuotaCacheEntry
		if !fill {
			var found bool
			var err error
			expected, found, err = c.GetUserPlatformQuotaCache(ctx, userID, "openai")
			require.NoError(t, err)
			require.True(t, found)
		}
		start := make(chan struct{})
		type result struct {
			applied bool
			err     error
		}
		results := make(chan result, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				applied, err := c.CompareAndSetUserPlatformQuotaCache(ctx, userID, "openai", expected, quotaCacheCASEntry(), time.Minute)
				results <- result{applied, err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		winners := 0
		for got := range results {
			require.NoError(t, got.err)
			if got.applied {
				winners++
			}
		}
		require.Equal(t, 1, winners)
	}
	entry, found, err := c.GetUserPlatformQuotaCache(ctx, userID, "openai")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), entry.Version)
	require.Equal(t, int64(9007199254740993), entry.QuotaRowID)
	require.Equal(t, int64(math.MaxInt64), entry.ControlGeneration)
}

func TestUserPlatformQuotaCacheCASRealRedisIncrementABAAndTTL(t *testing.T) {
	c, userID := realQuotaCacheCASFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", quotaCacheCASEntry(), time.Minute))
	first, _, err := c.GetUserPlatformQuotaCache(ctx, userID, "openai")
	require.NoError(t, err)
	result, incrementErr := c.IncrUserPlatformQuotaUsageCache(ctx, userID, "openai", 0.25, time.Minute, false)
	require.NoError(t, incrementErr)
	require.Equal(t, service.QuotaIncrementApplied, result.Status)
	key := userPlatformQuotaCacheKey(userID, "openai")
	expires, err := c.rdb.Do(ctx, "PEXPIRETIME", key).Int64()
	require.NoError(t, err)
	ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, userID, "openai", first, first, 2*time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	afterExpiry, err := c.rdb.Do(ctx, "PEXPIRETIME", key).Int64()
	require.NoError(t, err)
	require.Equal(t, expires, afterExpiry)
	current, _, err := c.GetUserPlatformQuotaCache(ctx, userID, "openai")
	require.NoError(t, err)
	require.Equal(t, first.DailyUsageUSD+0.25, current.DailyUsageUSD)
	// Recreate the same identity and usage revision with a new instance.
	require.NoError(t, c.DeleteUserPlatformQuotaCache(ctx, userID, "openai"))
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", current, time.Minute))
	ok, err = c.CompareAndSetUserPlatformQuotaCache(ctx, userID, "openai", current, current, time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	recreated, _, err := c.GetUserPlatformQuotaCache(ctx, userID, "openai")
	require.NoError(t, err)
	require.NotEqual(t, current.CacheInstance, recreated.CacheInstance)
	require.Equal(t, current.Version, recreated.Version)
}

func TestUserPlatformQuotaCacheCASRealRedisWrongTypeAndMalformedReadClose(t *testing.T) {
	c, userID := realQuotaCacheCASFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := userPlatformQuotaCacheKey(userID, "openai")
	require.NoError(t, c.rdb.Set(ctx, key, "fixture", time.Minute).Err())
	expires, err := c.rdb.Do(ctx, "PEXPIRETIME", key).Int64()
	require.NoError(t, err)
	ok, err := c.CompareAndSetUserPlatformQuotaCache(ctx, userID, "openai", nil, quotaCacheCASEntry(), 2*time.Minute)
	require.Error(t, err)
	require.False(t, ok)
	require.Error(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", quotaCacheCASEntry(), 2*time.Minute))
	afterExpiry, err := c.rdb.Do(ctx, "PEXPIRETIME", key).Int64()
	require.NoError(t, err)
	require.Equal(t, expires, afterExpiry)
	value, err := c.rdb.Get(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "fixture", value)
	require.NoError(t, c.rdb.Del(ctx, key).Err())
	require.NoError(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", quotaCacheCASEntry(), time.Minute))
	require.NoError(t, c.rdb.HSet(ctx, key, "weekly_usage", "NaN").Err())
	entry, found, err := c.GetUserPlatformQuotaCache(ctx, userID, "openai")
	require.Error(t, err)
	require.False(t, found)
	require.Nil(t, entry)
	batch, err := c.BatchGetUserPlatformQuotaCache(ctx, []service.UserPlatformQuotaKey{{UserID: userID, Platform: "openai"}})
	require.Error(t, err)
	require.Nil(t, batch)
}
