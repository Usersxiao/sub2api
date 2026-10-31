//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type boundQuotaCacheSpy struct {
	fakeIncrCache
	bindings    []UserPlatformQuotaUsageBinding
	ttl         time.Duration
	markDirty   bool
	boundResult UserPlatformQuotaBoundIncrementResult
	boundErr    error
	before      func(context.Context)
}

func (s *boundQuotaCacheSpy) IncrBoundUserPlatformQuotaUsageCache(ctx context.Context, binding UserPlatformQuotaUsageBinding, ttl time.Duration, markDirty bool) (UserPlatformQuotaBoundIncrementResult, error) {
	s.bindings = append(s.bindings, binding)
	s.ttl, s.markDirty = ttl, markDirty
	if s.before != nil {
		s.before(ctx)
	}
	return s.boundResult, s.boundErr
}

func TestIncrementBoundUserPlatformQuotaUsageOnlyAcknowledgesAllWindows(t *testing.T) {
	binding, err := validOrdinaryPlatformReceiptForTest(ordinaryPlatformReceiptCommandForTest(t, 1.25, false)).UsageBinding()
	require.NoError(t, err)
	commitErr := errors.New("reply lost after commit")
	for _, tc := range []struct {
		name    string
		status  UserPlatformQuotaIncrementStatus
		windows uint8
		err     error
		wantErr bool
	}{
		{name: "applied", status: QuotaIncrementApplied, windows: QuotaWindowAll},
		{name: "applied without windows", status: QuotaIncrementApplied, wantErr: true},
		{name: "applied incomplete", status: QuotaIncrementApplied, windows: QuotaWindowDaily, wantErr: true},
		{name: "applied invalid mask", status: QuotaIncrementApplied, windows: 15, wantErr: true},
		{name: "partial", status: QuotaIncrementPartial, windows: QuotaWindowWeekly | QuotaWindowMonthly, wantErr: true},
		{name: "superseded", status: QuotaIncrementSuperseded, wantErr: true},
		{name: "obsolete", status: QuotaIncrementObsolete, wantErr: true},
		{name: "missing", status: QuotaIncrementMissing, wantErr: true},
		{name: "incompatible", status: QuotaIncrementIncompatible, wantErr: true},
		{name: "contended", status: QuotaIncrementContended, wantErr: true},
		{name: "unknown", status: "future", windows: QuotaWindowAll, wantErr: true},
		{name: "empty", wantErr: true},
		{name: "error wins applied", status: QuotaIncrementApplied, windows: QuotaWindowAll, err: commitErr, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := &boundQuotaCacheSpy{boundResult: UserPlatformQuotaBoundIncrementResult{Status: tc.status, AppliedWindows: tc.windows}, boundErr: tc.err}
			err := (&BillingCacheService{cache: cache}).IncrementBoundUserPlatformQuotaUsage(binding)
			require.Equal(t, tc.wantErr, err != nil)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
			require.Equal(t, []UserPlatformQuotaUsageBinding{binding}, cache.bindings)
			require.Empty(t, cache.calls, "the unbound writer is never used")
		})
	}
}

func TestIncrementBoundUserPlatformQuotaUsageConfigAndUnsupportedWriters(t *testing.T) {
	binding, err := validOrdinaryPlatformReceiptForTest(ordinaryPlatformReceiptCommandForTest(t, 1.25, false)).UsageBinding()
	require.NoError(t, err)
	for _, enabled := range []bool{false, true} {
		cfg := &config.Config{}
		cfg.Database.UserPlatformQuotaFlusherEnabled = enabled
		cfg.Billing.UserPlatformQuotaCacheTTLSeconds = 120
		cache := &boundQuotaCacheSpy{boundResult: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll}}
		require.NoError(t, (&BillingCacheService{cache: cache, cfg: cfg}).IncrementBoundUserPlatformQuotaUsage(binding))
		require.Equal(t, enabled, cache.markDirty)
		require.Equal(t, 120*time.Second, cache.ttl)
	}
	cache := &boundQuotaCacheSpy{boundResult: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll}}
	require.NoError(t, (&BillingCacheService{cache: cache}).IncrementBoundUserPlatformQuotaUsage(binding))
	require.Equal(t, 24*time.Hour, cache.ttl)
	require.False(t, cache.markDirty)
	unbound := &fakeIncrCache{}
	for _, service := range []*BillingCacheService{nil, {}, {cache: unbound}} {
		require.Error(t, service.IncrementBoundUserPlatformQuotaUsage(binding))
	}
	require.Empty(t, unbound.calls)
	invalid := binding
	invalid.ControlGeneration = 0
	require.ErrorIs(t, (&BillingCacheService{cache: cache}).IncrementBoundUserPlatformQuotaUsage(invalid), ErrUserPlatformQuotaUsageBinding)
	require.Len(t, cache.bindings, 1)
}

func TestIncrementBoundUserPlatformQuotaUsageWaitsForBoundedCommit(t *testing.T) {
	binding, err := validOrdinaryPlatformReceiptForTest(ordinaryPlatformReceiptCommandForTest(t, 1.25, false)).UsageBinding()
	require.NoError(t, err)
	entered := make(chan context.Context)
	release := make(chan struct{})
	done := make(chan error, 1)
	cache := &boundQuotaCacheSpy{boundResult: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll},
		before: func(ctx context.Context) { entered <- ctx; <-release }}
	go func() { done <- (&BillingCacheService{cache: cache}).IncrementBoundUserPlatformQuotaUsage(binding) }()
	ctx := <-entered
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > cacheWriteTimeout {
		close(release)
		<-done
		t.Fatalf("unbounded bookkeeping context: %v", deadline)
	}
	select {
	case err := <-done:
		close(release)
		t.Fatalf("returned before committed result: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-done)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, binding.CapturedAt, cache.bindings[0].CapturedAt)
}
