//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/stretchr/testify/require"
)

func (f *fakeFullCache) CompareAndSetUserPlatformQuotaCache(ctx context.Context, _ int64, _ string, expected, next *UserPlatformQuotaCacheEntry, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	f.casCalls++
	hook, casErr := f.casHook, f.casErr
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx, expected, next)
	}
	if casErr != nil {
		return false, casErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if expected == nil {
		if f.entry != nil {
			return false, nil
		}
	} else if f.entry == nil || f.entry.CacheInstance != expected.CacheInstance || f.entry.Version != expected.Version ||
		f.entry.QuotaRowID != expected.QuotaRowID || f.entry.ControlGeneration != expected.ControlGeneration {
		return false, nil
	}
	copy := *next
	copy.Version = 0
	if expected != nil {
		copy.Version = expected.Version + 1
	}
	copy.CacheInstance = fmt.Sprintf("%064x", f.casCalls+100)
	f.entry = &copy
	f.lastSetTTL = ttl
	return true, nil
}

type quotaCacheWithoutCAS struct{ BillingCache }

func quotaObservationFixture(t *testing.T) (*BillingCacheService, *fakeQuotaRepo, *fakeFullCache) {
	t.Helper()
	now := time.Now()
	day, week := timezone.StartOfDay(now), timezone.StartOfWeek(now)
	monthlyStart := now.Add(-time.Hour)
	limit := 10.0
	rec := &UserPlatformQuotaRecord{
		QuotaRowID: 101, ControlGeneration: 7, UserID: 1, Platform: "openai", DailyLimitUSD: &limit,
		DailyWindowStart: &day, WeeklyWindowStart: &week, MonthlyWindowStart: &monthlyStart,
	}
	entry := &UserPlatformQuotaCacheEntry{
		QuotaRowID: rec.QuotaRowID, ControlGeneration: rec.ControlGeneration,
		SchemaVersion: UserPlatformQuotaCacheSchemaV1, CacheInstance: strings.Repeat("a", 64), Version: 5,
		DailyLimitUSD: rec.DailyLimitUSD, DailyWindowStart: &day, WeeklyWindowStart: &week, MonthlyWindowStart: &monthlyStart,
	}
	repo, cache := &fakeQuotaRepo{rec: rec}, &fakeFullCache{entry: entry}
	return newServiceForPreflight(t, repo, cache), repo, cache
}

func TestCheckUserPlatformQuotaEligibility_FreshControlsAndConservativeUsage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*UserPlatformQuotaRecord, *UserPlatformQuotaCacheEntry)
		want   error
	}{
		{name: "under limit", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			r.DailyUsageUSD, e.DailyUsageUSD = 3, 4
		}},
		{name: "DB daily ahead", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			r.DailyUsageUSD, e.DailyUsageUSD = 10, 1
		}, want: ErrUserPlatformDailyQuotaExhausted},
		{name: "Redis daily ahead", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			r.DailyUsageUSD, e.DailyUsageUSD = 1, 10
		}, want: ErrUserPlatformDailyQuotaExhausted},
		{name: "DB weekly ahead", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			r.WeeklyLimitUSD = r.DailyLimitUSD
			r.WeeklyUsageUSD, e.WeeklyUsageUSD = 10, 1
		}, want: ErrUserPlatformWeeklyQuotaExhausted},
		{name: "Redis weekly ahead", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			r.WeeklyLimitUSD = r.DailyLimitUSD
			r.WeeklyUsageUSD, e.WeeklyUsageUSD = 1, 10
		}, want: ErrUserPlatformWeeklyQuotaExhausted},
		{name: "DB monthly ahead", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			r.MonthlyLimitUSD = r.DailyLimitUSD
			r.MonthlyUsageUSD, e.MonthlyUsageUSD = 10, 1
		}, want: ErrUserPlatformMonthlyQuotaExhausted},
		{name: "Redis monthly ahead", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			r.MonthlyLimitUSD = r.DailyLimitUSD
			r.MonthlyUsageUSD, e.MonthlyUsageUSD = 1, 10
		}, want: ErrUserPlatformMonthlyQuotaExhausted},
		{name: "cached zero limit ignored", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			zero := 0.0
			e.DailyLimitUSD = &zero
			e.DailyUsageUSD = 1
		}},
		{name: "DB zero limit blocks", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) { zero := 0.0; r.DailyLimitUSD = &zero }, want: ErrUserPlatformDailyQuotaExhausted},
		{name: "DB nil limits unlimited", change: func(r *UserPlatformQuotaRecord, e *UserPlatformQuotaCacheEntry) {
			r.DailyLimitUSD = nil
			e.DailyUsageUSD = 1000
		}},
	} {
		for _, flusher := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/flusher=%t", tc.name, flusher), func(t *testing.T) {
				svc, repo, cache := quotaObservationFixture(t)
				svc.cfg.Database.UserPlatformQuotaFlusherEnabled = flusher
				tc.change(repo.rec, cache.entry)
				err := svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai")
				if tc.want == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.want)
				}
				require.Equal(t, 1, repo.getCalls)
				if !repo.rec.HasAnyLimit() {
					require.Zero(t, cache.getCalls)
				}
				require.Zero(t, cache.setCalls)
				require.Zero(t, cache.casCalls)
			})
		}
	}
}

func TestCheckUserPlatformQuotaEligibility_NoRowIgnoresStaleCacheAndDoesNotWriteSentinel(t *testing.T) {
	for _, flusher := range []bool{false, true} {
		t.Run(fmt.Sprintf("flusher=%t", flusher), func(t *testing.T) {
			svc, repo, cache := quotaObservationFixture(t)
			svc.cfg.Database.UserPlatformQuotaFlusherEnabled = flusher
			repo.rec = nil
			cache.getErr = errors.New("stale cache is irrelevant")
			require.NoError(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"))
			require.Zero(t, cache.getCalls)
			require.Zero(t, cache.setCalls)
			require.Zero(t, cache.casCalls)
			// A new configuration must be observed on the following request.
			limit := 10.0
			repo.rec = &UserPlatformQuotaRecord{QuotaRowID: 202, ControlGeneration: 1, UserID: 1, Platform: "openai", DailyLimitUSD: &limit}
			require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrBillingServiceUnavailable)
			require.Equal(t, 2, repo.getCalls)
		})
	}
}

func TestCheckUserPlatformQuotaEligibility_BadDatabaseObservationFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*BillingCacheService, *fakeQuotaRepo)
	}{
		{name: "missing repository", change: func(s *BillingCacheService, r *fakeQuotaRepo) { s.userPlatformQuotaRepo = nil }},
		{name: "database error", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.getErr = errors.New("DB unavailable") }},
		{name: "missing row token", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.rec.QuotaRowID = 0 }},
		{name: "missing generation", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.rec.ControlGeneration = 0 }},
		{name: "wrong user", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.rec.UserID = 2 }},
		{name: "wrong platform", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.rec.Platform = "gemini" }},
		{name: "NaN usage", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.rec.DailyUsageUSD = math.NaN() }},
		{name: "infinite usage", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.rec.WeeklyUsageUSD = math.Inf(1) }},
		{name: "negative usage", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.rec.MonthlyUsageUSD = -1 }},
		{name: "bad limit", change: func(s *BillingCacheService, r *fakeQuotaRepo) { bad := math.NaN(); r.rec.DailyLimitUSD = &bad }},
		{name: "negative limit", change: func(s *BillingCacheService, r *fakeQuotaRepo) { bad := -1.0; r.rec.WeeklyLimitUSD = &bad }},
		{name: "unanchored positive usage", change: func(s *BillingCacheService, r *fakeQuotaRepo) { r.rec.DailyWindowStart = nil; r.rec.DailyUsageUSD = 1 }},
		{name: "future window", change: func(s *BillingCacheService, r *fakeQuotaRepo) {
			future := time.Now().Add(time.Hour)
			r.rec.MonthlyWindowStart = &future
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, repo, cache := quotaObservationFixture(t)
			tc.change(svc, repo)
			require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrBillingServiceUnavailable)
			require.Zero(t, cache.getCalls)
		})
	}
}

func TestCheckUserPlatformQuotaEligibility_CallerCancellationNeverAllows(t *testing.T) {
	t.Run("already canceled", func(t *testing.T) {
		svc, repo, _ := quotaObservationFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(ctx, 1, "openai"), ErrBillingServiceUnavailable)
		require.Zero(t, repo.getCalls)
	})
	t.Run("caller context reaches DB and cancellation closes no row", func(t *testing.T) {
		svc, repo, cache := quotaObservationFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		repo.get = func(got context.Context, _ int64, _ string) (*UserPlatformQuotaRecord, error) {
			deadline, present := got.Deadline()
			require.True(t, present)
			require.LessOrEqual(t, time.Until(deadline), balanceLoadTimeout)
			require.NoError(t, got.Err())
			cancel()
			require.ErrorIs(t, got.Err(), context.Canceled)
			return nil, nil
		}
		require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(ctx, 1, "openai"), ErrBillingServiceUnavailable)
		require.Zero(t, cache.getCalls)
	})
}

func TestCheckUserPlatformQuotaEligibility_BadCacheObservationFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*BillingCacheService, *fakeFullCache)
	}{
		{name: "cache unavailable", change: func(s *BillingCacheService, c *fakeFullCache) { s.cache = nil }},
		{name: "cache read error", change: func(s *BillingCacheService, c *fakeFullCache) { c.getErr = errors.New("corrupt hash") }},
		{name: "old schema", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.SchemaVersion = 0 }},
		{name: "unknown schema", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.SchemaVersion = 2 }},
		{name: "legacy no instance", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.CacheInstance = "" }},
		{name: "malformed instance", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.CacheInstance = strings.Repeat("G", 64) }},
		{name: "old row", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.QuotaRowID++ }},
		{name: "old generation", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.ControlGeneration-- }},
		{name: "negative version", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.Version = -1 }},
		{name: "exhausted version", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.Version = math.MaxInt64 }},
		{name: "bad usage", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.DailyUsageUSD = math.NaN() }},
		{name: "negative usage", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.WeeklyUsageUSD = -1 }},
		{name: "bad cached limit", change: func(s *BillingCacheService, c *fakeFullCache) { bad := math.Inf(1); c.entry.MonthlyLimitUSD = &bad }},
		{name: "missing window", change: func(s *BillingCacheService, c *fakeFullCache) { c.entry.DailyWindowStart = nil }},
		{name: "future window", change: func(s *BillingCacheService, c *fakeFullCache) {
			future := time.Now().Add(time.Hour)
			c.entry.MonthlyWindowStart = &future
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, cache := quotaObservationFixture(t)
			tc.change(svc, cache)
			require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrBillingServiceUnavailable)
			require.Zero(t, cache.casCalls)
			require.Zero(t, cache.setCalls)
		})
	}
}

func TestCheckUserPlatformQuotaEligibility_ConditionalFillByMode(t *testing.T) {
	for _, flusher := range []bool{false, true} {
		t.Run(fmt.Sprintf("flusher=%t", flusher), func(t *testing.T) {
			svc, repo, cache := quotaObservationFixture(t)
			svc.cfg.Database.UserPlatformQuotaFlusherEnabled = flusher
			repo.rec.DailyWindowStart, repo.rec.WeeklyWindowStart, repo.rec.MonthlyWindowStart = nil, nil, nil
			cache.entry = nil
			err := svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai")
			if flusher {
				require.ErrorIs(t, err, ErrBillingServiceUnavailable)
				require.Zero(t, cache.casCalls)
				require.Equal(t, 1, repo.getCalls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, cache.casCalls)
				require.Equal(t, 2, repo.getCalls)
				require.True(t, validPlatformQuotaInstance(cache.entry.CacheInstance))
				require.Equal(t, int64(0), cache.entry.Version)
				require.NotNil(t, cache.entry.DailyWindowStart)
				require.NotNil(t, cache.entry.WeeklyWindowStart)
				require.NotNil(t, cache.entry.MonthlyWindowStart)
				require.Equal(t, 60*time.Second, cache.lastSetTTL)
			}
			require.Zero(t, cache.setCalls)
		})
	}
}

func TestCheckUserPlatformQuotaEligibility_MissingCASNeverUsesBlindSet(t *testing.T) {
	for _, cold := range []bool{true, false} {
		t.Run(fmt.Sprintf("cold=%t", cold), func(t *testing.T) {
			svc, _, cache := quotaObservationFixture(t)
			if cold {
				cache.entry = nil
			} else {
				past := time.Now().AddDate(0, 0, -2)
				cache.entry.DailyWindowStart = &past
			}
			svc.cache = &quotaCacheWithoutCAS{BillingCache: cache}
			require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrBillingServiceUnavailable)
			require.Zero(t, cache.setCalls)
		})
	}
}

func TestCheckUserPlatformQuotaEligibility_ExpiredWindowsRotateConditionallyAndReread(t *testing.T) {
	svc, repo, cache := quotaObservationFixture(t)
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	repo.rec.DailyWindowStart, repo.rec.WeeklyWindowStart, repo.rec.MonthlyWindowStart = &past, &past, &past
	repo.rec.DailyUsageUSD, repo.rec.WeeklyUsageUSD, repo.rec.MonthlyUsageUSD = 100, 200, 300
	cache.entry.DailyWindowStart, cache.entry.WeeklyWindowStart, cache.entry.MonthlyWindowStart = &past, &past, &past
	cache.entry.DailyUsageUSD, cache.entry.WeeklyUsageUSD, cache.entry.MonthlyUsageUSD = 100, 200, 300
	instance := cache.entry.CacheInstance
	require.NoError(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"))
	require.Equal(t, 1, cache.casCalls)
	require.Equal(t, 2, repo.getCalls)
	require.Equal(t, 2, cache.getCalls)
	require.Equal(t, int64(6), cache.entry.Version)
	require.NotEqual(t, instance, cache.entry.CacheInstance)
	require.Zero(t, cache.entry.DailyUsageUSD)
	require.Zero(t, cache.entry.WeeklyUsageUSD)
	require.Zero(t, cache.entry.MonthlyUsageUSD)
	require.Equal(t, int64(101), cache.entry.QuotaRowID)
	require.Equal(t, int64(7), cache.entry.ControlGeneration)
	require.Zero(t, cache.setCalls)
}

func TestCheckUserPlatformQuotaEligibility_ConditionalFillCollisionPreservesAheadUsage(t *testing.T) {
	svc, _, cache := quotaObservationFixture(t)
	ahead := *cache.entry
	ahead.DailyUsageUSD = 12
	cache.entry = nil
	cache.casHook = func(_ context.Context, expected, _ *UserPlatformQuotaCacheEntry) (bool, error) {
		require.Nil(t, expected)
		cache.entry = &ahead
		return false, nil
	}
	require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrUserPlatformDailyQuotaExhausted)
	require.Equal(t, float64(12), cache.entry.DailyUsageUSD)
	require.Equal(t, 1, cache.casCalls)
	require.Zero(t, cache.setCalls)
}

func TestCheckUserPlatformQuotaEligibility_RotationCollisionRereadsIncrementAndControls(t *testing.T) {
	svc, repo, cache := quotaObservationFixture(t)
	past := time.Now().AddDate(0, 0, -2)
	cache.entry.DailyWindowStart = &past
	cache.entry.WeeklyUsageUSD = 2
	cache.casHook = func(_ context.Context, expected, _ *UserPlatformQuotaCacheEntry) (bool, error) {
		cache.entry.Version++
		cache.entry.WeeklyUsageUSD = 11
		require.NotEqual(t, expected.Version, cache.entry.Version)
		// A control change observed on the retry cannot be hidden by the old CAS.
		repo.rec.ControlGeneration++
		return false, nil
	}
	require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrBillingServiceUnavailable)
	require.Equal(t, 2, repo.getCalls)
	require.Equal(t, 1, cache.casCalls)
	require.Equal(t, float64(11), cache.entry.WeeklyUsageUSD)
	require.Zero(t, cache.setCalls)
}

func TestCheckUserPlatformQuotaEligibility_RotationRetryRetainsConcurrentIncrement(t *testing.T) {
	svc, repo, cache := quotaObservationFixture(t)
	past := time.Now().AddDate(0, 0, -2)
	cache.entry.DailyWindowStart = &past
	cache.entry.DailyUsageUSD = 100
	cache.entry.WeeklyUsageUSD = 2
	weeklyLimit := 10.0
	repo.rec.WeeklyLimitUSD = &weeklyLimit
	cache.casHook = func(_ context.Context, expected, _ *UserPlatformQuotaCacheEntry) (bool, error) {
		cache.entry.Version++
		cache.entry.WeeklyUsageUSD = 11
		require.NotEqual(t, expected.Version, cache.entry.Version)
		cache.casHook = nil
		return false, nil
	}
	require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrUserPlatformWeeklyQuotaExhausted)
	require.Equal(t, 3, repo.getCalls)
	require.Equal(t, 2, cache.casCalls)
	require.Equal(t, float64(11), cache.entry.WeeklyUsageUSD)
	require.Zero(t, cache.entry.DailyUsageUSD)
	require.Zero(t, cache.setCalls)
}

func TestCheckUserPlatformQuotaEligibility_CASFailureAndBoundedCollisionsClose(t *testing.T) {
	t.Run("conditional write error", func(t *testing.T) {
		svc, _, cache := quotaObservationFixture(t)
		cache.entry = nil
		cache.casErr = errors.New("Redis write unavailable")
		require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrBillingServiceUnavailable)
		require.Equal(t, 1, cache.casCalls)
		require.Zero(t, cache.setCalls)
	})
	t.Run("three collisions", func(t *testing.T) {
		svc, repo, cache := quotaObservationFixture(t)
		cache.entry = nil
		cache.casHook = func(context.Context, *UserPlatformQuotaCacheEntry, *UserPlatformQuotaCacheEntry) (bool, error) {
			return false, nil
		}
		require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"), ErrBillingServiceUnavailable)
		require.Equal(t, 3, cache.casCalls)
		require.Equal(t, 4, repo.getCalls)
		require.Equal(t, 4, cache.getCalls)
		require.Zero(t, cache.setCalls)
	})
	t.Run("cancellation during collision", func(t *testing.T) {
		svc, repo, cache := quotaObservationFixture(t)
		cache.entry = nil
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cache.casHook = func(got context.Context, _, _ *UserPlatformQuotaCacheEntry) (bool, error) {
			require.Same(t, ctx, got)
			cancel()
			return false, nil
		}
		require.ErrorIs(t, svc.checkUserPlatformQuotaEligibility(ctx, 1, "openai"), ErrBillingServiceUnavailable)
		require.Equal(t, 1, repo.getCalls)
		require.Equal(t, 1, cache.casCalls)
	})
}

func TestHasUserPlatformQuotaLimit_UsesFreshDatabaseConservatively(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*BillingCacheService, *fakeQuotaRepo, *fakeFullCache)
		want   bool
	}{
		{name: "active limit ignores cached sentinel", change: func(s *BillingCacheService, r *fakeQuotaRepo, c *fakeFullCache) {
			c.entry = &UserPlatformQuotaCacheEntry{}
		}, want: true},
		{name: "no row ignores cached limit", change: func(s *BillingCacheService, r *fakeQuotaRepo, c *fakeFullCache) { r.rec = nil }},
		{name: "no limits", change: func(s *BillingCacheService, r *fakeQuotaRepo, c *fakeFullCache) { r.rec.DailyLimitUSD = nil }},
		{name: "missing repository", change: func(s *BillingCacheService, r *fakeQuotaRepo, c *fakeFullCache) { s.userPlatformQuotaRepo = nil }, want: true},
		{name: "database error", change: func(s *BillingCacheService, r *fakeQuotaRepo, c *fakeFullCache) { r.getErr = errors.New("DB down") }, want: true},
		{name: "malformed record", change: func(s *BillingCacheService, r *fakeQuotaRepo, c *fakeFullCache) { r.rec.DailyUsageUSD = math.NaN() }, want: true},
		{name: "simple mode", change: func(s *BillingCacheService, r *fakeQuotaRepo, c *fakeFullCache) { s.cfg.RunMode = "simple" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, repo, cache := quotaObservationFixture(t)
			tc.change(svc, repo, cache)
			require.Equal(t, tc.want, svc.HasUserPlatformQuotaLimit(context.Background(), 1, "openai"))
			require.Zero(t, cache.getCalls)
		})
	}
	t.Run("canceled context", func(t *testing.T) {
		svc, repo, _ := quotaObservationFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.True(t, svc.HasUserPlatformQuotaLimit(ctx, 1, "openai"))
		require.Zero(t, repo.getCalls)
	})
	t.Run("cancellation during DB cannot prove no row", func(t *testing.T) {
		svc, repo, _ := quotaObservationFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		repo.get = func(readCtx context.Context, _ int64, _ string) (*UserPlatformQuotaRecord, error) {
			deadline, present := readCtx.Deadline()
			require.True(t, present)
			require.LessOrEqual(t, time.Until(deadline), balanceLoadTimeout)
			cancel()
			return nil, nil
		}
		require.True(t, svc.HasUserPlatformQuotaLimit(ctx, 1, "openai"))
	})
}
