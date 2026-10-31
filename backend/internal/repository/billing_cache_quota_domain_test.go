//go:build unit

package repository

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type quotaDomainCommandHook struct {
	commands  atomic.Int64
	pipelines atomic.Int64
}

func (h *quotaDomainCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *quotaDomainCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		return next(ctx, cmd)
	}
}
func (h *quotaDomainCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.pipelines.Add(1)
		h.commands.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

func quotaDomainTestDomain() service.UserPlatformQuotaCacheDomain {
	return service.UserPlatformQuotaCacheDomain{
		SchemaVersion:   service.UserPlatformQuotaCacheDomainSchemaV2,
		Namespace:       "657f2ed0-7c7c-4c13-8bec-dcd9b5e69a40",
		WriterAuthority: service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeDB, Epoch: 1},
	}
}

// Observe and seed through a separate client: assertions never inflate the
// production client's command count. Real Redis is opt-in and disposable.
func quotaDomainRedisFixture(t *testing.T, backend string) (*billingCache, *redis.Client, *quotaDomainCommandHook, int64) {
	t.Helper()
	var legacy *billingCache
	var userID int64
	if backend == "redis7" {
		legacy, userID = realQuotaCacheCASFixture(t)
	} else {
		fixture, mr := newMiniRedisCache(t)
		legacy = fixture
		mr.SetTime(time.Now())
		userID = time.Now().UnixNano()
	}
	options := *legacy.rdb.Options()
	guarded := redis.NewClient(&options)
	t.Cleanup(func() { _ = guarded.Close() })
	hook := &quotaDomainCommandHook{}
	guarded.AddHook(hook)
	domain := quotaDomainTestDomain()
	return &billingCache{rdb: guarded, quotaDomain: &domain}, legacy.rdb, hook, userID
}

func quotaDomainAssertAllQuotaPortsClosed(t *testing.T, c *billingCache, ctx context.Context, userID int64) {
	t.Helper()
	entry := quotaCacheCASEntry()
	keys := []service.UserPlatformQuotaKey{{UserID: userID, Platform: "openai"}}
	got, found, err := c.GetUserPlatformQuotaCache(ctx, userID, "openai")
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
	require.Nil(t, got)
	require.False(t, found)
	require.ErrorIs(t, c.SetUserPlatformQuotaCache(ctx, userID, "openai", entry, time.Minute), service.ErrUserPlatformQuotaCacheHandoffRequired)
	for _, expected := range []*service.UserPlatformQuotaCacheEntry{nil, entry} {
		applied, err := c.CompareAndSetUserPlatformQuotaCache(ctx, userID, "openai", expected, entry, time.Minute)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
		require.False(t, applied)
	}
	for _, markDirty := range []bool{false, true} {
		result, err := c.IncrUserPlatformQuotaUsageCache(ctx, userID, "openai", 0.25, time.Minute, markDirty)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
		require.Equal(t, service.UserPlatformQuotaIncrementResult{}, result)
		bound, err := c.IncrBoundUserPlatformQuotaUsageCache(ctx, boundQuotaCacheBinding(userID, entry, time.Now()), time.Minute, markDirty)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
		require.Equal(t, service.UserPlatformQuotaBoundIncrementResult{}, bound)
	}
	require.ErrorIs(t, c.DeleteUserPlatformQuotaCache(ctx, userID, "openai"), service.ErrUserPlatformQuotaCacheHandoffRequired)
	for _, count := range []int{0, 1} {
		dirty, err := c.PopDirtyUserPlatformQuotaKeys(ctx, count)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
		require.Nil(t, dirty)
	}
	for _, batch := range [][]service.UserPlatformQuotaKey{nil, {}, keys} {
		require.ErrorIs(t, c.ReaddDirtyUserPlatformQuotaKeys(ctx, batch), service.ErrUserPlatformQuotaCacheHandoffRequired)
		entries, err := c.BatchGetUserPlatformQuotaCache(ctx, batch)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
		require.Nil(t, entries)
	}
	// Invalid input, nil entries and private helper calls cannot bypass the
	// handoff requirement through an earlier validation/empty-input return.
	require.ErrorIs(t, c.SetUserPlatformQuotaCache(ctx, 0, "", nil, 0), service.ErrUserPlatformQuotaCacheHandoffRequired)
	applied, err := c.CompareAndSetUserPlatformQuotaCache(ctx, 0, "", nil, nil, 0)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
	require.False(t, applied)
	bound, err := c.IncrBoundUserPlatformQuotaUsageCache(ctx, service.UserPlatformQuotaUsageBinding{}, 0, true)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
	require.Equal(t, service.UserPlatformQuotaBoundIncrementResult{}, bound)
	result, err := c.incrementUserPlatformQuotaUsageCacheAt(ctx, 0, "", 0, 0, true, nil, "")
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
	require.Equal(t, service.UserPlatformQuotaIncrementResult{}, result)
	bound, err = c.incrementBoundUserPlatformQuotaUsageCache(ctx, service.UserPlatformQuotaUsageBinding{}, 0, true, "")
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
	require.Equal(t, service.UserPlatformQuotaBoundIncrementResult{}, bound)
}

type quotaDomainRedisSnapshot struct {
	Type      string
	Value     any
	ExpiresAt int64
}

func quotaDomainSnapshot(t *testing.T, observer *redis.Client, keys []string) map[string]quotaDomainRedisSnapshot {
	t.Helper()
	ctx := context.Background()
	states := make(map[string]quotaDomainRedisSnapshot, len(keys))
	for _, key := range keys {
		kind, err := observer.Type(ctx, key).Result()
		require.NoError(t, err)
		expires, err := observer.Do(ctx, "PEXPIRETIME", key).Int64()
		require.NoError(t, err)
		state := quotaDomainRedisSnapshot{Type: kind, ExpiresAt: expires}
		switch kind {
		case "hash":
			state.Value, err = observer.HGetAll(ctx, key).Result()
		case "set":
			var members []string
			members, err = observer.SMembers(ctx, key).Result()
			sort.Strings(members)
			state.Value = members
		case "string":
			state.Value, err = observer.Get(ctx, key).Result()
		case "none":
		default:
			t.Fatalf("unsupported fixture type %s", kind)
		}
		require.NoError(t, err)
		states[key] = state
	}
	return states
}

func TestBillingCacheQuotaDomainHandoffGateNeverReadsOrMutatesRedis(t *testing.T) {
	for _, backend := range []string{"miniredis", "redis7"} {
		t.Run(backend, func(t *testing.T) {
			for _, state := range []string{"miss", "legacy", "deleted", "wrong-type", "incomplete-hash", "other-schema", "other-epoch", "other-lineage"} {
				t.Run(state, func(t *testing.T) {
					c, observer, hook, userID := quotaDomainRedisFixture(t, backend)
					ctx := context.Background()
					legacyKey := userPlatformQuotaCacheKey(userID, "openai")
					legacy := NewBillingCache(observer).(*billingCache)
					if state != "miss" {
						require.NoError(t, legacy.SetUserPlatformQuotaCache(ctx, userID, "openai", quotaCacheCASEntry(), time.Hour))
					}
					switch state {
					case "deleted":
						require.NoError(t, observer.Del(ctx, legacyKey).Err())
					case "wrong-type":
						require.NoError(t, observer.Set(ctx, legacyKey, "opaque fixture", time.Hour).Err())
					case "incomplete-hash":
						require.NoError(t, observer.HDel(ctx, legacyKey, "control_generation").Err())
					case "other-schema":
						require.NoError(t, observer.HSet(ctx, legacyKey, "schema_version", 2).Err())
					case "other-epoch":
						require.NoError(t, observer.HSet(ctx, legacyKey, "writer_epoch", "99").Err())
					case "other-lineage":
						require.NoError(t, observer.HSet(ctx, legacyKey, "observation_lineage", "1f6d9fb1-f97b-426a-b38e-4cf02215a966").Err())
					}
					// These opaque fixture keys are deliberately not a proposed v2
					// key format or evidence of a published/certified new domain.
					candidateHash := fmt.Sprintf("quota-domain-test:%d:opaque-cohort", userID)
					candidateDirty := candidateHash + ":dirty"
					require.NoError(t, observer.HSet(ctx, candidateHash, "namespace", c.quotaDomain.Namespace, "schema_version", "2", "writer_epoch", "1").Err())
					require.NoError(t, observer.Expire(ctx, candidateHash, time.Hour).Err())
					require.NoError(t, observer.SAdd(ctx, candidateDirty, "opaque member").Err())
					require.NoError(t, observer.Expire(ctx, candidateDirty, time.Hour).Err())
					dirtyKey := userPlatformQuotaDirtySetKey()
					member := userPlatformQuotaDirtyMember(userID, "openai")
					existed, err := observer.Exists(ctx, dirtyKey).Result()
					require.NoError(t, err)
					require.NoError(t, observer.SAdd(ctx, dirtyKey, member).Err())
					if existed == 0 {
						require.NoError(t, observer.Expire(ctx, dirtyKey, time.Hour).Err())
					}
					t.Cleanup(func() {
						_ = observer.Del(ctx, legacyKey, candidateHash, candidateDirty).Err()
						_ = observer.SRem(ctx, dirtyKey, member).Err()
					})
					keys := []string{legacyKey, dirtyKey, candidateHash, candidateDirty}
					before := quotaDomainSnapshot(t, observer, keys)
					quotaDomainAssertAllQuotaPortsClosed(t, c, ctx, userID)
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					quotaDomainAssertAllQuotaPortsClosed(t, c, cancelled, userID)
					require.Zero(t, hook.commands.Load(), "the gate must precede every Redis command")
					require.Zero(t, hook.pipelines.Load())
					require.Equal(t, before, quotaDomainSnapshot(t, observer, keys), "values, dirty members, key types and absolute expiries remain unchanged")
				})
			}
		})
	}
}

func TestBillingCacheQuotaDomainHandoffGateNeedsNoRedisClient(t *testing.T) {
	domain := quotaDomainTestDomain()
	quotaDomainAssertAllQuotaPortsClosed(t, &billingCache{quotaDomain: &domain}, context.Background(), 1)
}

func TestBillingCacheQuotaDomainLeavesOtherBillingCachesAvailable(t *testing.T) {
	for _, backend := range []string{"miniredis", "redis7"} {
		t.Run(backend, func(t *testing.T) {
			c, observer, hook, userID := quotaDomainRedisFixture(t, backend)
			ctx := context.Background()
			t.Cleanup(func() {
				_ = observer.Del(ctx, billingBalanceKey(userID), billingSubKey(userID, userID), billingRateLimitKey(userID)).Err()
			})
			require.NoError(t, c.SetUserBalance(ctx, userID, 10))
			require.NoError(t, c.DeductUserBalance(ctx, userID, 0.25))
			balance, err := c.GetUserBalance(ctx, userID)
			require.NoError(t, err)
			require.Equal(t, 9.75, balance)
			require.NoError(t, c.InvalidateUserBalance(ctx, userID))
			_, err = c.GetUserBalance(ctx, userID)
			require.ErrorIs(t, err, redis.Nil)
			subscription := &service.SubscriptionCacheData{Status: "active", ExpiresAt: time.Now().Add(time.Hour).Truncate(time.Second), DailyUsage: 1, WeeklyUsage: 2, MonthlyUsage: 3, Version: 4}
			require.NoError(t, c.SetSubscriptionCache(ctx, userID, userID, subscription))
			require.NoError(t, c.UpdateSubscriptionUsage(ctx, userID, userID, 0.25))
			gotSub, err := c.GetSubscriptionCache(ctx, userID, userID)
			require.NoError(t, err)
			require.Equal(t, 1.25, gotSub.DailyUsage)
			require.Equal(t, 2.25, gotSub.WeeklyUsage)
			require.Equal(t, 3.25, gotSub.MonthlyUsage)
			require.Equal(t, subscription.Status, gotSub.Status)
			require.True(t, subscription.ExpiresAt.Equal(gotSub.ExpiresAt))
			require.NoError(t, c.InvalidateSubscriptionCache(ctx, userID, userID))
			_, err = c.GetSubscriptionCache(ctx, userID, userID)
			require.ErrorIs(t, err, redis.Nil)
			now := time.Now().Unix()
			rate := &service.APIKeyRateLimitCacheData{Usage5h: 1, Usage1d: 2, Usage7d: 3, Window5h: now, Window1d: now, Window7d: now}
			require.NoError(t, c.SetAPIKeyRateLimit(ctx, userID, rate))
			require.NoError(t, c.UpdateAPIKeyRateLimitUsage(ctx, userID, 0.25))
			gotRate, err := c.GetAPIKeyRateLimit(ctx, userID)
			require.NoError(t, err)
			require.Equal(t, 1.25, gotRate.Usage5h)
			require.Equal(t, 2.25, gotRate.Usage1d)
			require.Equal(t, 3.25, gotRate.Usage7d)
			require.NoError(t, c.InvalidateAPIKeyRateLimit(ctx, userID))
			_, err = c.GetAPIKeyRateLimit(ctx, userID)
			require.ErrorIs(t, err, redis.Nil)
			require.Positive(t, hook.commands.Load(), "non-quota caches still use Redis")
			commands, pipelines := hook.commands.Load(), hook.pipelines.Load()
			quotaDomainAssertAllQuotaPortsClosed(t, c, ctx, userID)
			require.Equal(t, commands, hook.commands.Load())
			require.Equal(t, pipelines, hook.pipelines.Load())
		})
	}
}

func TestBillingCacheQuotaDomainLegacyFixtureRemainsConfinedToLegacyKeys(t *testing.T) {
	c, observer, hook, userID := quotaDomainRedisFixture(t, "miniredis")
	legacy := NewBillingCache(observer).(*billingCache)
	require.Nil(t, legacy.quotaDomain)
	ctx := context.Background()
	require.NoError(t, legacy.SetUserPlatformQuotaCache(ctx, userID, "openai", quotaCacheCASEntry(), time.Minute))
	result, err := legacy.IncrUserPlatformQuotaUsageCache(ctx, userID, "openai", 0.25, time.Minute, true)
	require.NoError(t, err)
	require.Equal(t, service.QuotaIncrementApplied, result.Status)
	entry, found, err := legacy.GetUserPlatformQuotaCache(ctx, userID, "openai")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1.25, entry.DailyUsageUSD)
	require.True(t, observer.SIsMember(ctx, userPlatformQuotaDirtySetKey(), userPlatformQuotaDirtyMember(userID, "openai")).Val())
	before := quotaDomainSnapshot(t, observer, []string{userPlatformQuotaCacheKey(userID, "openai"), userPlatformQuotaDirtySetKey()})
	quotaDomainAssertAllQuotaPortsClosed(t, c, ctx, userID)
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
	require.Equal(t, before, quotaDomainSnapshot(t, observer, []string{userPlatformQuotaCacheKey(userID, "openai"), userPlatformQuotaDirtySetKey()}))
}
