//go:build unit

package repository

import (
	"context"
	"io"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type quotaCandidateCacheCommandHook struct {
	gets       atomic.Int64
	evals      atomic.Int64
	beforeEval func(context.Context, redis.Cmder) error
}

func (h *quotaCandidateCacheCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *quotaCandidateCacheCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *quotaCandidateCacheCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "get" {
			h.gets.Add(1)
		}
		if cmd.Name() == "eval" && len(cmd.Args()) > 1 && cmd.Args()[1] == replaceUserPlatformQuotaCacheCandidateScript {
			h.evals.Add(1)
			if h.beforeEval != nil {
				if err := h.beforeEval(ctx, cmd); err != nil {
					return err
				}
			}
		}
		return next(ctx, cmd)
	}
}

func quotaCandidateCacheRealFixture(t *testing.T) (*redis.Client, *redis.Client, *quotaCandidateCacheCommandHook, int64) {
	t.Helper()
	legacy, userID := realQuotaCacheCASFixture(t)
	options := *legacy.rdb.Options()
	guarded := redis.NewClient(&options)
	t.Cleanup(func() { _ = guarded.Close() })
	hook := &quotaCandidateCacheCommandHook{}
	guarded.AddHook(hook)
	return guarded, legacy.rdb, hook, userID
}

// Test-only seeding supplies an existing cohort. The production primitive has
// no initialization port, and none of these fixtures certifies a usage baseline.
func quotaCandidateCacheSeed(t *testing.T, observer *redis.Client, candidate *service.UserPlatformQuotaCacheCandidate, incarnation string, ttl time.Duration) string {
	t.Helper()
	key := quotaCandidateCacheKey(t, candidate)
	require.NoError(t, observer.Set(context.Background(), key, quotaCandidateCacheEncode(t, candidate, incarnation), ttl).Err())
	t.Cleanup(func() { _ = observer.Del(context.Background(), key).Err() })
	return key
}

func TestUserPlatformQuotaCacheCandidateCacheRedisExistingOrderPreservesExactBytesAndExpiry(t *testing.T) {
	rdb, observer, hook, userID := quotaCandidateCacheRealFixture(t)
	ctx := context.Background()
	current := quotaCandidateCacheFixture(t, userID, 9007199254740993)
	key := quotaCandidateCacheSeed(t, observer, current, quotaCandidateCacheIncarnation, time.Minute)
	before := quotaDomainSnapshot(t, observer, []string{key})
	for _, tc := range []struct {
		revision int64
		want     service.UserPlatformQuotaObservationOrder
	}{
		{9007199254740992, service.QuotaObservationOlder}, {9007199254740993, service.QuotaObservationEqual},
	} {
		order, err := replaceUserPlatformQuotaCacheCandidate(nil, rdb, quotaCandidateCacheFixture(t, userID, tc.revision), time.Hour)
		require.NoError(t, err)
		require.Equal(t, tc.want, order)
		require.Equal(t, before, quotaDomainSnapshot(t, observer, []string{key}), "older/equal never rewrites or extends the original absolute expiry")
	}
	require.Zero(t, hook.evals.Load())
	next := quotaCandidateCacheFixture(t, userID, 9007199254740994)
	hook.beforeEval = func(_ context.Context, cmd redis.Cmder) error {
		require.Len(t, cmd.Args(), 7)
		require.Equal(t, 1, cmd.Args()[2], "the script declares one key and works in a single Redis Cluster slot")
		require.Equal(t, key, cmd.Args()[3])
		retry, ok := cmd.(interface{ NoRetry() bool })
		require.True(t, ok)
		require.True(t, retry.NoRetry())
		clone, ok := cmd.Clone().(interface{ NoRetry() bool })
		require.True(t, ok)
		require.True(t, clone.NoRetry())
		return nil
	}
	order, err := replaceUserPlatformQuotaCacheCandidate(ctx, rdb, next, time.Hour)
	require.NoError(t, err)
	require.Equal(t, service.QuotaObservationNewer, order)
	require.Equal(t, int64(1), hook.evals.Load())
	actual, err := observer.Get(ctx, key).Bytes()
	require.NoError(t, err)
	require.Equal(t, quotaCandidateCacheEncode(t, next, quotaCandidateCacheIncarnation), actual)
	decoded, incarnation, err := decodeUserPlatformQuotaCacheCandidateEnvelope(actual)
	require.NoError(t, err)
	require.Equal(t, quotaCandidateCacheIncarnation, incarnation, "replacement preserves the creation incarnation")
	require.Equal(t, next.Observation(), decoded.Observation())
	expiry, err := observer.Do(ctx, "PEXPIRETIME", key).Int64()
	require.NoError(t, err)
	require.Greater(t, expiry, before[key].ExpiresAt+int64((30*time.Minute)/time.Millisecond))
	require.Equal(t, int64(3), hook.gets.Load())
}

func TestUserPlatformQuotaCacheCandidateCacheRedisInt64AdjacentAndMaximumRevisionsNeverRound(t *testing.T) {
	for _, pair := range [][2]int64{{9007199254740992, 9007199254740993}, {math.MaxInt64 - 1, math.MaxInt64}} {
		rdb, observer, hook, userID := quotaCandidateCacheRealFixture(t)
		current := quotaCandidateCacheFixture(t, userID, pair[0])
		next := quotaCandidateCacheFixture(t, userID, pair[1])
		key := quotaCandidateCacheSeed(t, observer, current, quotaCandidateCacheIncarnation, time.Minute)
		order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), rdb, next, time.Minute)
		require.NoError(t, err)
		require.Equal(t, service.QuotaObservationNewer, order)
		raw, err := observer.Get(context.Background(), key).Bytes()
		require.NoError(t, err)
		require.Equal(t, quotaCandidateCacheEncode(t, next, quotaCandidateCacheIncarnation), raw)
		before := quotaDomainSnapshot(t, observer, []string{key})
		order, err = replaceUserPlatformQuotaCacheCandidate(context.Background(), rdb, next, time.Hour)
		require.NoError(t, err)
		require.Equal(t, service.QuotaObservationEqual, order)
		order, err = replaceUserPlatformQuotaCacheCandidate(context.Background(), rdb, current, time.Hour)
		require.NoError(t, err)
		require.Equal(t, service.QuotaObservationOlder, order)
		require.Equal(t, before, quotaDomainSnapshot(t, observer, []string{key}))
		require.Equal(t, int64(1), hook.evals.Load())
	}
}

func TestUserPlatformQuotaCacheCandidateCacheRedisRefusesCrossScopeAndSameRevisionConflict(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*service.UserPlatformQuotaCacheDomain, *service.UserPlatformQuotaOrderedObservation)
		want   error
	}{
		{"namespace", func(d *service.UserPlatformQuotaCacheDomain, _ *service.UserPlatformQuotaOrderedObservation) {
			d.Namespace = "c968f547-cc78-499c-aefc-197533a7a0d8"
		}, service.ErrUserPlatformQuotaObservationIncomparable},
		{"writer", func(d *service.UserPlatformQuotaCacheDomain, v *service.UserPlatformQuotaOrderedObservation) {
			d.WriterAuthority.Epoch++
			v.Token.WriterEpoch++
		}, service.ErrUserPlatformQuotaObservationIncomparable},
		{"user", func(_ *service.UserPlatformQuotaCacheDomain, v *service.UserPlatformQuotaOrderedObservation) {
			v.UserID++
		}, service.ErrUserPlatformQuotaObservationIncomparable},
		{"platform", func(_ *service.UserPlatformQuotaCacheDomain, v *service.UserPlatformQuotaOrderedObservation) {
			v.Platform = service.PlatformGemini
		}, service.ErrUserPlatformQuotaObservationIncomparable},
		{"row", func(_ *service.UserPlatformQuotaCacheDomain, v *service.UserPlatformQuotaOrderedObservation) {
			v.Token.QuotaRowID++
		}, service.ErrUserPlatformQuotaObservationIncomparable},
		{"control", func(_ *service.UserPlatformQuotaCacheDomain, v *service.UserPlatformQuotaOrderedObservation) {
			v.Token.ControlGeneration--
		}, service.ErrUserPlatformQuotaObservationIncomparable},
		{"lineage", func(_ *service.UserPlatformQuotaCacheDomain, v *service.UserPlatformQuotaOrderedObservation) {
			v.Token.Lineage = "bf7acc39-e306-4838-9ee0-87ab7f4ba42f"
		}, service.ErrUserPlatformQuotaObservationIncomparable},
		{"same revision payload", func(_ *service.UserPlatformQuotaCacheDomain, v *service.UserPlatformQuotaOrderedObservation) {
			v.DailyUsageUSD = "0.0000000002"
		}, service.ErrUserPlatformQuotaCacheCandidateConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rdb, observer, hook, userID := quotaCandidateCacheRealFixture(t)
			candidate := quotaCandidateCacheFixture(t, userID, 9007199254740993)
			domain, raw := candidate.Domain(), candidate.Observation()
			tc.mutate(&domain, raw)
			foreign, err := service.PrepareUserPlatformQuotaCacheCandidate(domain, raw)
			require.NoError(t, err)
			key := quotaCandidateCacheKey(t, candidate)
			require.NoError(t, observer.Set(context.Background(), key, quotaCandidateCacheEncode(t, foreign, quotaCandidateCacheIncarnation), time.Minute).Err())
			t.Cleanup(func() { _ = observer.Del(context.Background(), key).Err() })
			before := quotaDomainSnapshot(t, observer, []string{key})
			order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), rdb, candidate, time.Hour)
			require.ErrorIs(t, err, tc.want)
			require.Empty(t, order)
			require.Zero(t, hook.evals.Load())
			require.Equal(t, before, quotaDomainSnapshot(t, observer, []string{key}))
		})
	}
}

func TestUserPlatformQuotaCacheCandidateCacheRedisMissingMalformedAndLegacyNeverInitialize(t *testing.T) {
	for _, state := range []string{"miss", "legacy only", "wrong hash", "wrong set", "invalid JSON", "wrong digest", "oversized", "noncanonical"} {
		t.Run(state, func(t *testing.T) {
			rdb, observer, hook, userID := quotaCandidateCacheRealFixture(t)
			candidate := quotaCandidateCacheFixture(t, userID, 2)
			key := quotaCandidateCacheKey(t, candidate)
			legacyKey := userPlatformQuotaCacheKey(userID, "openai")
			t.Cleanup(func() { _ = observer.Del(context.Background(), key, legacyKey).Err() })
			ctx := context.Background()
			switch state {
			case "legacy only":
				require.NoError(t, NewBillingCache(observer).SetUserPlatformQuotaCache(ctx, userID, "openai", quotaCacheCASEntry(), time.Minute))
			case "wrong hash":
				require.NoError(t, observer.HSet(ctx, key, "schema_version", "1", "daily_usage", "1").Err())
				require.NoError(t, observer.Expire(ctx, key, time.Minute).Err())
			case "wrong set":
				require.NoError(t, observer.SAdd(ctx, key, "opaque fixture").Err())
				require.NoError(t, observer.Expire(ctx, key, time.Minute).Err())
			case "invalid JSON":
				require.NoError(t, observer.Set(ctx, key, `{"schema_version":1}`, time.Minute).Err())
			case "wrong digest":
				raw := quotaCandidateCacheEncode(t, candidate, quotaCandidateCacheIncarnation)
				require.NoError(t, observer.Set(ctx, key, strings.Replace(string(raw), candidate.Fingerprint(), strings.Repeat("0", 64), 1), time.Minute).Err())
			case "oversized":
				require.NoError(t, observer.Set(ctx, key, strings.Repeat("x", userPlatformQuotaCacheCandidateMaxBytes+1), time.Minute).Err())
			case "noncanonical":
				require.NoError(t, observer.Set(ctx, key, " "+string(quotaCandidateCacheEncode(t, candidate, quotaCandidateCacheIncarnation)), time.Minute).Err())
			}
			keys := []string{key, legacyKey}
			before := quotaDomainSnapshot(t, observer, keys)
			order, err := replaceUserPlatformQuotaCacheCandidate(ctx, rdb, candidate, time.Hour)
			if state == "miss" || state == "legacy only" {
				require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
			} else {
				require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
			}
			require.Empty(t, order)
			require.Zero(t, hook.evals.Load())
			require.Equal(t, before, quotaDomainSnapshot(t, observer, keys))
		})
	}
}

func TestUserPlatformQuotaCacheCandidateCacheRedisEvictionOrExpiryBetweenReadAndCASNeverResurrects(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(map[bool]string{false: "evicted", true: "expired"}[expire], func(t *testing.T) {
			rdb, observer, hook, userID := quotaCandidateCacheRealFixture(t)
			current := quotaCandidateCacheFixture(t, userID, 1)
			next := quotaCandidateCacheFixture(t, userID, 2)
			key := quotaCandidateCacheSeed(t, observer, current, quotaCandidateCacheIncarnation, time.Hour)
			hook.beforeEval = func(ctx context.Context, _ redis.Cmder) error {
				if expire {
					return observer.PExpireAt(ctx, key, time.Unix(1, 0)).Err()
				}
				return observer.Del(ctx, key).Err()
			}
			order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), rdb, next, time.Hour)
			require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheHandoffRequired)
			require.Empty(t, order)
			exists, existsErr := observer.Exists(context.Background(), key).Result()
			require.NoError(t, existsErr)
			require.Zero(t, exists)
			require.Equal(t, int64(1), hook.gets.Load())
			require.Equal(t, int64(1), hook.evals.Load())
		})
	}
}

func TestUserPlatformQuotaCacheCandidateCacheRedisConcurrentReplacementNeverRetriesStaleBytes(t *testing.T) {
	for _, state := range []string{"same content new incarnation", "newer revision", "same revision other payload", "wrong type"} {
		t.Run(state, func(t *testing.T) {
			rdb, observer, hook, userID := quotaCandidateCacheRealFixture(t)
			current := quotaCandidateCacheFixture(t, userID, 1)
			next := quotaCandidateCacheFixture(t, userID, 2)
			key := quotaCandidateCacheSeed(t, observer, current, quotaCandidateCacheIncarnation, time.Minute)
			var concurrentState map[string]quotaDomainRedisSnapshot
			hook.beforeEval = func(ctx context.Context, _ redis.Cmder) error {
				if state == "wrong type" {
					if err := observer.Del(ctx, key).Err(); err != nil {
						return err
					}
					if err := observer.HSet(ctx, key, "opaque", "concurrent").Err(); err != nil {
						return err
					}
					if err := observer.Expire(ctx, key, 2*time.Minute).Err(); err != nil {
						return err
					}
				} else {
					replacement := current
					incarnation := quotaCandidateCacheIncarnation
					switch state {
					case "same content new incarnation":
						incarnation = "abcdef1234567890abcdef1234567890"
					case "newer revision":
						replacement = quotaCandidateCacheFixture(t, userID, 3)
					case "same revision other payload":
						raw := current.Observation()
						raw.DailyUsageUSD = "0.0000000002"
						var err error
						replacement, err = service.PrepareUserPlatformQuotaCacheCandidate(current.Domain(), raw)
						if err != nil {
							return err
						}
					}
					if err := observer.Set(ctx, key, quotaCandidateCacheEncode(t, replacement, incarnation), 2*time.Minute).Err(); err != nil {
						return err
					}
				}
				concurrentState = quotaDomainSnapshot(t, observer, []string{key})
				return nil
			}
			order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), rdb, next, time.Hour)
			if state == "wrong type" {
				require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
			} else {
				require.ErrorIs(t, err, errUserPlatformQuotaCacheCandidateChanged)
			}
			require.Empty(t, order)
			require.Equal(t, int64(1), hook.gets.Load(), "a changed entry requires an explicit future read, never an internal retry")
			require.Equal(t, int64(1), hook.evals.Load())
			require.Equal(t, concurrentState, quotaDomainSnapshot(t, observer, []string{key}))
		})
	}
}

func TestUserPlatformQuotaCacheCandidateCacheRedisLostReplyNeverReplaysCommittedReplacement(t *testing.T) {
	_, observer, _, userID := quotaCandidateCacheRealFixture(t)
	current := quotaCandidateCacheFixture(t, userID, 1)
	next := quotaCandidateCacheFixture(t, userID, 2)
	key := quotaCandidateCacheSeed(t, observer, current, quotaCandidateCacheIncarnation, time.Minute)
	control := &quotaLostReplyControl{}
	options := *observer.Options()
	dial := options.Dialer
	options.MaxRetries = 3
	options.Protocol = 2
	options.DisableIdentity = true
	options.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &quotaLostReplyConn{Conn: conn, control: control}, nil
	}
	faultClient := redis.NewClient(&options)
	t.Cleanup(func() { _ = faultClient.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	order, err := replaceUserPlatformQuotaCacheCandidate(ctx, faultClient, next, time.Hour)
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, order, "an uncertain response claims neither application nor non-application")
	require.True(t, control.dropped.Load())
	require.Equal(t, int64(1), control.evalWrites.Load(), "the client and the outer primitive both avoid mutation replay")
	actual, err := observer.Get(ctx, key).Bytes()
	require.NoError(t, err)
	require.Equal(t, quotaCandidateCacheEncode(t, next, quotaCandidateCacheIncarnation), actual, "Redis committed once before the response was lost")
}

func TestUserPlatformQuotaCacheCandidateCacheRedisConcurrentStaleReadersHaveOneWinner(t *testing.T) {
	rdb, observer, hook, userID := quotaCandidateCacheRealFixture(t)
	current := quotaCandidateCacheFixture(t, userID, 1)
	next := quotaCandidateCacheFixture(t, userID, 2)
	key := quotaCandidateCacheSeed(t, observer, current, quotaCandidateCacheIncarnation, time.Minute)
	const workers = 4
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready := make(chan struct{}, workers)
	release := make(chan struct{})
	hook.beforeEval = func(ctx context.Context, _ redis.Cmder) error {
		ready <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	type result struct {
		order service.UserPlatformQuotaObservationOrder
		err   error
	}
	results := make(chan result, workers)
	for range workers {
		go func() {
			order, err := replaceUserPlatformQuotaCacheCandidate(ctx, rdb, next, time.Hour)
			results <- result{order, err}
		}()
	}
	for range workers {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("all readers must reach the CAS barrier before release")
		}
	}
	close(release)
	winners, changed := 0, 0
	for range workers {
		got := <-results
		if got.err == nil {
			require.Equal(t, service.QuotaObservationNewer, got.order)
			winners++
		} else {
			require.ErrorIs(t, got.err, errUserPlatformQuotaCacheCandidateChanged)
			require.Empty(t, got.order)
			changed++
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, workers-1, changed)
	require.Equal(t, int64(workers), hook.gets.Load())
	require.Equal(t, int64(workers), hook.evals.Load())
	raw, err := observer.Get(context.Background(), key).Bytes()
	require.NoError(t, err)
	require.Equal(t, quotaCandidateCacheEncode(t, next, quotaCandidateCacheIncarnation), raw)
}

func TestUserPlatformQuotaCacheCandidateCacheRedisInvalidPreconditionsNeverTouchServer(t *testing.T) {
	rdb, _, hook, userID := quotaCandidateCacheRealFixture(t)
	allCommands := &quotaDomainCommandHook{}
	rdb.AddHook(allCommands)
	candidate := quotaCandidateCacheFixture(t, userID, 1)
	for _, ttl := range []time.Duration{-time.Second, 0, time.Nanosecond, 999 * time.Millisecond, 1500 * time.Millisecond} {
		order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), rdb, candidate, ttl)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
		require.Empty(t, order)
	}
	for _, invalid := range []*service.UserPlatformQuotaCacheCandidate{nil, {}} {
		order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), rdb, invalid, time.Minute)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
		require.Empty(t, order)
	}
	order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), nil, candidate, time.Minute)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
	require.Empty(t, order)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	order, err = replaceUserPlatformQuotaCacheCandidate(cancelled, rdb, candidate, time.Minute)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, order)
	require.Zero(t, allCommands.commands.Load())
	require.Zero(t, allCommands.pipelines.Load())
	require.Zero(t, hook.gets.Load())
	require.Zero(t, hook.evals.Load())
}
