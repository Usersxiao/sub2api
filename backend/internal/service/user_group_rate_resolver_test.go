package service

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	gocache "github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/require"
)

type userGroupRateResolverRepoStub struct {
	UserGroupRateRepository

	rate  *float64
	err   error
	calls int
	wait  <-chan struct{}
}

func (s *userGroupRateResolverRepoStub) GetByUserAndGroup(ctx context.Context, userID, groupID int64) (*float64, error) {
	s.calls++
	if s.wait != nil {
		<-s.wait
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.rate, nil
}

func TestNewUserGroupRateResolver_Defaults(t *testing.T) {
	resolver := newUserGroupRateResolver(nil, nil, 0, nil, "")

	require.NotNil(t, resolver)
	require.NotNil(t, resolver.cache)
	require.Equal(t, defaultUserGroupRateCacheTTL, resolver.cacheTTL)
	require.NotNil(t, resolver.sf)
	require.Equal(t, "service.gateway", resolver.logComponent)
}

func TestUserGroupRateResolverResolve_FallbackForNilResolverAndInvalidIDs(t *testing.T) {
	var nilResolver *userGroupRateResolver
	require.Equal(t, 1.4, nilResolver.Resolve(context.Background(), 101, 202, 1.4))

	resolver := newUserGroupRateResolver(nil, nil, time.Second, nil, "service.test")
	require.Equal(t, 1.4, resolver.Resolve(context.Background(), 0, 202, 1.4))
	require.Equal(t, 1.4, resolver.Resolve(context.Background(), 101, 0, 1.4))
}

func TestUserGroupRateResolverResolve_InvalidCacheEntryLoadsRepoAndCaches(t *testing.T) {
	resetGatewayHotpathStatsForTest()

	rate := 1.7
	repo := &userGroupRateResolverRepoStub{rate: &rate}
	cache := gocache.New(time.Minute, time.Minute)
	cache.Set("101:202", "bad-cache", time.Minute)
	resolver := newUserGroupRateResolver(repo, cache, time.Minute, nil, "service.test")

	got := resolver.Resolve(context.Background(), 101, 202, 1.2)
	require.Equal(t, rate, got)
	require.Equal(t, 1, repo.calls)

	cached, ok := cache.Get("101:202")
	require.True(t, ok)
	require.Equal(t, userGroupRateCacheEntry{override: rate, hasOverride: true}, cached)

	hit, miss, load, _, fallback := GatewayUserGroupRateCacheStats()
	require.Equal(t, int64(0), hit)
	require.Equal(t, int64(1), miss)
	require.Equal(t, int64(1), load)
	require.Equal(t, int64(0), fallback)
}

func TestGatewayServiceGetUserGroupRateMultiplier_FallbacksAndUsesExistingResolver(t *testing.T) {
	var nilSvc *GatewayService
	require.Equal(t, 1.3, nilSvc.getUserGroupRateMultiplier(context.Background(), 101, 202, 1.3))

	rate := 1.9
	repo := &userGroupRateResolverRepoStub{rate: &rate}
	resolver := newUserGroupRateResolver(repo, nil, time.Minute, nil, "service.gateway")
	svc := &GatewayService{userGroupRateResolver: resolver}

	got := svc.getUserGroupRateMultiplier(context.Background(), 101, 202, 1.2)
	require.Equal(t, rate, got)
	require.Equal(t, 1, repo.calls)
}

func TestUserGroupRateResolverResolve_DefaultChangesDoNotWaitForCacheTTL(t *testing.T) {
	for _, override := range []bool{false, true} {
		name := "group_default"
		if override {
			name = "user_override"
		}
		t.Run(name, func(t *testing.T) {
			rate := 0.8
			repo := &userGroupRateResolverRepoStub{}
			if override {
				repo.rate = &rate
			}
			resolver := newUserGroupRateResolver(repo, nil, time.Minute, nil, "service.test")
			for _, currentDefault := range []float64{1, 0.3, 1.4} {
				want := currentDefault
				if override {
					want = rate
				}
				require.Equal(t, want, resolver.Resolve(context.Background(), 101, 202, currentDefault))
			}
			require.Equal(t, 1, repo.calls, "cache the override lookup, including its absence")
		})
	}
}

func TestUserGroupRateResolverResolve_SingleflightPreservesEachCallersDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unblock := make(chan struct{})
		repo := &userGroupRateResolverRepoStub{wait: unblock}
		// Disable the cache janitor so the bubble owns only the blocked lookup.
		resolver := newUserGroupRateResolver(repo, gocache.New(time.Minute, -1), time.Minute, nil, "service.test")
		firstDone, secondDone := make(chan struct{}), make(chan struct{})
		var first, second float64
		go func() {
			first = resolver.Resolve(context.Background(), 101, 202, 1)
			close(firstDone)
		}()
		synctest.Wait()
		go func() {
			second = resolver.Resolve(context.Background(), 101, 202, 0.3)
			close(secondDone)
		}()
		synctest.Wait()
		close(unblock)
		<-firstDone
		<-secondDone
		require.Equal(t, 1.0, first)
		require.Equal(t, 0.3, second)
		require.Equal(t, 1, repo.calls)
		require.Equal(t, 0.2, resolver.Resolve(context.Background(), 101, 202, 0.2))
		require.Equal(t, 1, repo.calls)
	})
}
