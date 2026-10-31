//go:build unit

package repository

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type quotaLostReplyControl struct {
	evalWrites atomic.Int64
	claimed    atomic.Bool
	dropped    atomic.Bool
}

// Discard the response only after the server has produced it, so the test
// models a committed mutation with a lost response rather than a failed send.
type quotaLostReplyConn struct {
	net.Conn
	control *quotaLostReplyControl
	armed   atomic.Bool
}

func (c *quotaLostReplyConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("\r\neval\r\n")) {
		c.control.evalWrites.Add(1)
		if c.control.claimed.CompareAndSwap(false, true) {
			c.armed.Store(true)
		}
	}
	return c.Conn.Write(p)
}

func (c *quotaLostReplyConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.armed.CompareAndSwap(true, false) {
		c.control.dropped.Store(true)
		return 0, io.EOF
	}
	return n, err
}

func TestUserPlatformQuotaCacheIncrementLostReplyDoesNotReplay(t *testing.T) {
	for _, backend := range []string{"miniredis-tcp", "redis7-tcp"} {
		t.Run(backend, func(t *testing.T) {
			var cache *billingCache
			var userID int64
			if backend == "redis7-tcp" {
				cache, userID = realQuotaCacheCASFixture(t)
			} else {
				cache, _ = newMiniRedisCache(t)
				userID = 1
				t.Cleanup(func() { _ = cache.rdb.Close() })
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entry := quotaCacheCASEntry()
			now := time.Now().UTC().Truncate(time.Second)
			entry.DailyWindowStart, entry.WeeklyWindowStart, entry.MonthlyWindowStart = &now, &now, &now
			require.NoError(t, cache.SetUserPlatformQuotaCache(ctx, userID, service.PlatformOpenAI, entry, time.Minute))
			before, found, err := cache.GetUserPlatformQuotaCache(ctx, userID, service.PlatformOpenAI)
			require.NoError(t, err)
			require.True(t, found)
			control := &quotaLostReplyControl{}
			options := *cache.rdb.Options()
			dial := options.Dialer
			options.MaxRetries = 3 // Exercise the normal retrying client, not a special no-retry client.
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
			faultCache := &billingCache{rdb: faultClient}
			result, err := faultCache.incrementUserPlatformQuotaUsageCacheAt(ctx, userID, service.PlatformOpenAI, 0.25, time.Minute, false, func() time.Time { return now }, userPlatformQuotaDirtySetKey())
			require.ErrorIs(t, err, io.EOF)
			require.Empty(t, result.Status, "an uncertain response must never claim application or non-application")
			require.True(t, control.dropped.Load())
			require.Equal(t, int64(1), control.evalWrites.Load(), "neither go-redis nor the outer conflict loop may replay the mutation")
			after, found, err := cache.GetUserPlatformQuotaCache(ctx, userID, service.PlatformOpenAI)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, before.DailyUsageUSD+0.25, after.DailyUsageUSD)
			require.Equal(t, before.WeeklyUsageUSD+0.25, after.WeeklyUsageUSD)
			require.Equal(t, before.MonthlyUsageUSD+0.25, after.MonthlyUsageUSD)
			require.Equal(t, before.Version+1, after.Version)
			require.Equal(t, before.CacheInstance, after.CacheInstance)
		})
	}
	cmd := &quotaIncrementCommand{Cmd: redis.NewCmd(context.Background(), "eval", incrementUserPlatformQuotaUsageScript, 2, "quota", "dirty")}
	cmd.SetFirstKeyPos(3)
	require.True(t, cmd.NoRetry())
	require.True(t, cmd.Clone().NoRetry(), "a future command clone must retain the transport guarantee")
}

func TestUserPlatformQuotaBoundCacheLostReplyDoesNotReplay(t *testing.T) {
	for _, backend := range []string{"miniredis-tcp", "redis7-tcp"} {
		t.Run(backend, func(t *testing.T) {
			var cache *billingCache
			var userID int64
			if backend == "redis7-tcp" {
				cache, userID = realQuotaCacheCASFixture(t)
			} else {
				cache, _ = newMiniRedisCache(t)
				userID = 1
				t.Cleanup(func() { _ = cache.rdb.Close() })
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entry := quotaCacheCASEntry()
			now := time.Now().UTC().Truncate(time.Second)
			entry.DailyWindowStart, entry.WeeklyWindowStart, entry.MonthlyWindowStart = &now, &now, &now
			require.NoError(t, cache.SetUserPlatformQuotaCache(ctx, userID, service.PlatformOpenAI, entry, time.Minute))
			before, found, err := cache.GetUserPlatformQuotaCache(ctx, userID, service.PlatformOpenAI)
			require.NoError(t, err)
			require.True(t, found)
			control := &quotaLostReplyControl{}
			options := *cache.rdb.Options()
			dial := options.Dialer
			options.MaxRetries = 3 // Exercise the normal retrying client, not a special no-retry client.
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
			faultCache := &billingCache{rdb: faultClient}
			binding := boundQuotaCacheBinding(userID, entry, now)
			result, err := faultCache.IncrBoundUserPlatformQuotaUsageCache(ctx, binding, time.Minute, false)
			require.ErrorIs(t, err, io.EOF)
			require.Empty(t, result.Status, "an uncertain response must never claim application or non-application")
			require.True(t, control.dropped.Load())
			require.Equal(t, int64(1), control.evalWrites.Load(), "neither go-redis nor the outer conflict loop may replay the mutation")
			after, found, err := cache.GetUserPlatformQuotaCache(ctx, userID, service.PlatformOpenAI)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, before.DailyUsageUSD+0.25, after.DailyUsageUSD)
			require.Equal(t, before.WeeklyUsageUSD+0.25, after.WeeklyUsageUSD)
			require.Equal(t, before.MonthlyUsageUSD+0.25, after.MonthlyUsageUSD)
			require.Equal(t, before.Version+1, after.Version)
			require.Equal(t, before.CacheInstance, after.CacheInstance)
		})
	}
	cmd := &quotaIncrementCommand{Cmd: redis.NewCmd(context.Background(), "eval", incrementUserPlatformQuotaUsageScript, 2, "quota", "dirty")}
	cmd.SetFirstKeyPos(3)
	require.True(t, cmd.NoRetry())
	require.True(t, cmd.Clone().NoRetry(), "a future command clone must retain the transport guarantee")
}
