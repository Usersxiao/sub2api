//go:build unit

package service

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type quotaIncrementGatewayCache struct {
	boundQuotaCacheSpy
	balanceInvalidations int
}

type unboundQuotaGatewayCache struct {
	fakeIncrCache
}

func (*unboundQuotaGatewayCache) InvalidateUserBalance(context.Context, int64) error { return nil }

func (s *quotaIncrementGatewayCache) InvalidateUserBalance(context.Context, int64) error {
	s.balanceInvalidations++
	return nil
}

type quotaIncrementDBCall struct {
	binding     UserPlatformQuotaUsageBinding
	ctxErr      error
	hasDeadline bool
	command     UsageBillingCommand
	receipt     UsageBillingOrdinaryPlatformReceipt
}

type quotaIncrementDBSpy struct {
	fakeQuotaRepo
	increments     chan quotaIncrementDBCall
	legacyCalls    int
	boundResult    UserPlatformQuotaBoundIncrementResult
	boundErr       error
	beforeBound    func(context.Context)
	boundCalls     int
	lookupCalls    int
	mutateDelivery func(*UsageBillingOrdinaryPlatformDeliveryResult)
}

type quotaBindingOnlyDBSpy struct {
	fakeQuotaRepo
	boundCalls int
}

func (s *quotaBindingOnlyDBSpy) IncrementBoundUsage(context.Context, UserPlatformQuotaUsageBinding) (UserPlatformQuotaBoundIncrementResult, error) {
	s.boundCalls++
	return UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll}, nil
}

func (s *quotaIncrementDBSpy) IncrementUsageWithReset(context.Context, int64, string, float64, time.Time) error {
	s.legacyCalls++
	return errors.New("unexpected legacy quota writer")
}

func (s *quotaIncrementDBSpy) IncrementBoundUsage(context.Context, UserPlatformQuotaUsageBinding) (UserPlatformQuotaBoundIncrementResult, error) {
	s.boundCalls++
	return UserPlatformQuotaBoundIncrementResult{}, errors.New("unexpected binding-only quota writer")
}

func (s *quotaIncrementDBSpy) ConsumeOrdinaryPlatformQuotaUsage(ctx context.Context, cmd *UsageBillingCommand, receipt *UsageBillingOrdinaryPlatformReceipt) (UsageBillingOrdinaryPlatformDeliveryResult, error) {
	if s.beforeBound != nil {
		s.beforeBound(ctx)
	}
	if err := ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt); err != nil {
		return UsageBillingOrdinaryPlatformDeliveryResult{}, err
	}
	binding, err := receipt.UsageBinding()
	if err != nil {
		return UsageBillingOrdinaryPlatformDeliveryResult{}, err
	}
	_, hasDeadline := ctx.Deadline()
	s.increments <- quotaIncrementDBCall{binding: binding, ctxErr: ctx.Err(), hasDeadline: hasDeadline, command: *cmd, receipt: *receipt}
	fingerprint, err := FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	if err != nil {
		return UsageBillingOrdinaryPlatformDeliveryResult{}, err
	}
	created := receipt.CapturedAt.Add(time.Microsecond)
	completed := created.Add(time.Microsecond)
	result := UsageBillingOrdinaryPlatformDeliveryResult{State: OrdinaryPlatformDeliveryCompleted, SourceFingerprint: fingerprint,
		Outcome: s.boundResult, CreatedAt: created, CompletedAt: &completed}
	if s.mutateDelivery != nil {
		s.mutateDelivery(&result)
	}
	return result, s.boundErr
}

func (s *quotaIncrementDBSpy) GetOrdinaryPlatformQuotaUsageDelivery(context.Context, *UsageBillingCommand, *UsageBillingOrdinaryPlatformReceipt) (*UsageBillingOrdinaryPlatformDeliveryResult, error) {
	s.lookupCalls++
	return nil, nil
}

type quotaIncrementFinancialRepo struct {
	UsageBillingRepository
	applyCalls       int
	balanceDeducted  float64
	committedRequest string
	mutateReceipt    func(*UsageBillingCommand, *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt
}

func (s *quotaIncrementFinancialRepo) Apply(_ context.Context, cmd *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	s.applyCalls++
	if s.committedRequest == cmd.RequestID {
		return &UsageBillingApplyResult{Applied: false}, nil
	}
	s.committedRequest = cmd.RequestID
	s.balanceDeducted += cmd.BalanceCost
	zeroBalance := 0.0
	receipt := validOrdinaryPlatformReceiptForTest(cmd)
	if s.mutateReceipt != nil {
		receipt = s.mutateReceipt(cmd, receipt)
	}
	return &UsageBillingApplyResult{Applied: true, NewBalance: &zeroBalance, OrdinaryPlatformQuotaReceipt: receipt}, nil
}

type quotaIncrementLegacyUserRepo struct {
	UserRepository
	deductCalls int
	amount      float64
}

func (s *quotaIncrementLegacyUserRepo) DeductBalance(_ context.Context, _ int64, amount float64) error {
	s.deductCalls++
	s.amount += amount
	return nil
}

func quotaIncrementGatewayFixture(flusherEnabled bool, status UserPlatformQuotaIncrementStatus, cacheErr error) (*postUsageBillingParams, *billingDeps, *quotaIncrementGatewayCache, *quotaIncrementDBSpy) {
	cfg := &config.Config{}
	cfg.Database.UserPlatformQuotaFlusherEnabled = flusherEnabled
	cfg.Billing.UserPlatformQuotaCacheTTLSeconds = 120
	windows := uint8(0)
	if status == QuotaIncrementApplied {
		windows = QuotaWindowAll
	}
	cache := &quotaIncrementGatewayCache{boundQuotaCacheSpy: boundQuotaCacheSpy{boundResult: UserPlatformQuotaBoundIncrementResult{Status: status, AppliedWindows: windows}, boundErr: cacheErr}}
	limit := 10.0
	quota := &quotaIncrementDBSpy{
		fakeQuotaRepo: fakeQuotaRepo{rec: &UserPlatformQuotaRecord{QuotaRowID: 101, ControlGeneration: 1, UserID: 1, Platform: PlatformOpenAI, DailyLimitUSD: &limit}},
		increments:    make(chan quotaIncrementDBCall, 2),
		boundResult:   UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll},
	}
	params := &postUsageBillingParams{
		Cost: &CostBreakdown{ActualCost: 1.25, TotalCost: 1.25}, User: &User{ID: 1},
		APIKey: &APIKey{ID: 2}, Account: &Account{ID: 3}, Platform: PlatformOpenAI,
	}
	deps := &billingDeps{
		cfg: cfg, deferredService: &DeferredService{}, userPlatformQuotaRepo: quota,
		billingCacheService: &BillingCacheService{cfg: cfg, cache: cache, userPlatformQuotaRepo: quota},
	}
	return params, deps, cache, quota
}

func assertQuotaIncrementDBFallback(t *testing.T, quota *quotaIncrementDBSpy, flusherEnabled bool) {
	t.Helper()
	if flusherEnabled {
		select {
		case call := <-quota.increments:
			t.Fatalf("flusher mode unexpectedly used legacy DB fallback: %+v", call)
		default:
		}
		return
	}
	select {
	case call := <-quota.increments:
		require.Equal(t, int64(1), call.binding.UserID)
		require.Equal(t, PlatformOpenAI, call.binding.Platform)
		require.Equal(t, 1.25, call.binding.CostUSD)
		require.NoError(t, call.ctxErr, "consumed usage retains detached DB bookkeeping")
		require.True(t, call.hasDeadline, "bound DB bookkeeping remains bounded")
		require.Equal(t, int64(101), call.binding.QuotaRowID)
		require.Equal(t, int64(1), call.binding.ControlGeneration)
		require.Equal(t, time.Date(2026, 10, 25, 3, 4, 5, 678000000, time.UTC), call.binding.CapturedAt)
		require.Equal(t, UsageBillingOrdinaryPlatformDeliveryDBV1, call.command.OrdinaryPlatformQuotaDeliveryMode)
		require.Equal(t, UsageBillingOrdinaryPlatformDeliveryDBV1, call.receipt.DeliveryMode)
		require.NoError(t, ValidateUsageBillingOrdinaryPlatformDelivery(&call.command, &call.receipt))
	case <-time.After(2 * time.Second):
		t.Fatal("cache failure suppressed existing DB fallback")
	}
}

func TestGatewayUserPlatformQuotaIncrementFailureKeepsFinancialApplyOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status UserPlatformQuotaIncrementStatus
		err    error
	}{
		{name: "missing", status: QuotaIncrementMissing},
		{name: "incompatible", status: QuotaIncrementIncompatible},
		{name: "contended", status: QuotaIncrementContended},
		{name: "unknown", status: "future-status"},
		{name: "superseded", status: QuotaIncrementSuperseded},
		{name: "partial", status: QuotaIncrementPartial},
		{name: "obsolete", status: QuotaIncrementObsolete},
		{name: "uncertain commit", err: errors.New("commit response lost")},
	} {
		for _, flusherEnabled := range []bool{false, true} {
			mode := "/db-fallback"
			if flusherEnabled {
				mode = "/flusher"
			}
			t.Run(tc.name+mode, func(t *testing.T) {
				params, deps, cache, quota := quotaIncrementGatewayFixture(flusherEnabled, tc.status, tc.err)
				if tc.status == QuotaIncrementPartial {
					cache.boundResult.AppliedWindows = QuotaWindowWeekly | QuotaWindowMonthly
				}
				financial := &quotaIncrementFinancialRepo{}
				before := userPlatformQuotaCacheIncrErrorTotal.Load()
				legacyBefore := userPlatformQuotaCacheIncrLegacyErrorTotal.Load()
				applied, err := applyUsageBilling(context.Background(), "cache-failure-request", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
				require.NoError(t, err, "post-commit bookkeeping never becomes a retryable financial error")
				require.True(t, applied)
				require.Equal(t, 1, financial.applyCalls)
				require.Equal(t, 1.25, financial.balanceDeducted)
				require.Equal(t, before+1, userPlatformQuotaCacheIncrErrorTotal.Load())
				require.Equal(t, legacyBefore, userPlatformQuotaCacheIncrLegacyErrorTotal.Load())
				require.Len(t, cache.bindings, 1, "uncertain non-idempotent cache writes are never retried")
				require.Equal(t, flusherEnabled, cache.markDirty)
				require.Equal(t, 1, cache.balanceInvalidations, "other committed cache handling is retained")
				assertQuotaIncrementDBFallback(t, quota, flusherEnabled)

				applied, err = applyUsageBilling(context.Background(), "cache-failure-request", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
				require.NoError(t, err)
				require.False(t, applied)
				require.Equal(t, 2, financial.applyCalls, "only the explicit duplicate request reaches Apply again")
				require.Equal(t, 1.25, financial.balanceDeducted)
				require.Len(t, cache.bindings, 1, "duplicate financial result skips quota finalization")
				require.Equal(t, before+1, userPlatformQuotaCacheIncrErrorTotal.Load())
				select {
				case call := <-quota.increments:
					t.Fatalf("duplicate request replayed DB quota bookkeeping: %+v", call)
				default:
				}
			})
		}
	}
}

func TestGatewayUserPlatformQuotaIncrementLegacyKeepsMoneyAndReportsSourceGap(t *testing.T) {
	for _, flusherEnabled := range []bool{false, true} {
		name := "db-fallback"
		if flusherEnabled {
			name = "flusher"
		}
		t.Run(name, func(t *testing.T) {
			params, deps, cache, quota := quotaIncrementGatewayFixture(flusherEnabled, QuotaIncrementApplied, nil)
			userRepo := &quotaIncrementLegacyUserRepo{}
			deps.userRepo = userRepo
			before := userPlatformQuotaLegacyUnboundErrorTotal.Load()
			cacheBefore := userPlatformQuotaCacheIncrLegacyErrorTotal.Load()
			mainBefore := userPlatformQuotaCacheIncrErrorTotal.Load()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			applied, err := applyUsageBilling(ctx, "legacy-no-source", &UsageLog{BillingType: BillingTypeBalance}, params, deps, nil)
			require.NoError(t, err)
			require.True(t, applied)
			require.Equal(t, 1, userRepo.deductCalls)
			require.Equal(t, 1.25, userRepo.amount)
			require.Empty(t, cache.bindings, "a money-only fallback has no persistent platform source")
			require.Empty(t, cache.calls)
			require.Zero(t, quota.getCalls, "today's quota cannot supply historical proof")
			require.Zero(t, quota.legacyCalls)
			require.Empty(t, quota.increments)
			require.Equal(t, 1, cache.balanceInvalidations)
			require.Equal(t, before+1, userPlatformQuotaLegacyUnboundErrorTotal.Load())
			require.Equal(t, cacheBefore, userPlatformQuotaCacheIncrLegacyErrorTotal.Load())
			require.Equal(t, mainBefore, userPlatformQuotaCacheIncrErrorTotal.Load())
		})
	}
}

func TestGatewayUserPlatformQuotaIncrementAppliedDoesNotCountFailure(t *testing.T) {
	params, deps, cache, _ := quotaIncrementGatewayFixture(true, QuotaIncrementApplied, nil)
	before := userPlatformQuotaCacheIncrErrorTotal.Load()
	legacyBefore := userPlatformQuotaCacheIncrLegacyErrorTotal.Load()
	applied, err := applyUsageBilling(context.Background(), "cache-applied-request", &UsageLog{BillingType: BillingTypeBalance}, params, deps, &quotaIncrementFinancialRepo{})
	require.NoError(t, err)
	require.True(t, applied)
	require.Len(t, cache.bindings, 1)
	require.Equal(t, before, userPlatformQuotaCacheIncrErrorTotal.Load())
	require.Equal(t, legacyBefore, userPlatformQuotaCacheIncrLegacyErrorTotal.Load())
}

func TestGatewayUserPlatformQuotaReceiptMissingOrMismatchedNeverReconstructsSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*UsageBillingCommand, *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt
	}{
		{name: "missing", mutate: func(_ *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
			return nil
		}},
		{name: "missing generation", mutate: func(_ *UsageBillingCommand, r *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
			r.ControlGeneration = 0
			return r
		}},
		{name: "different origin", mutate: func(_ *UsageBillingCommand, r *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
			r.Intent.APIKeyID++
			return r
		}},
		{name: "different fingerprint", mutate: func(_ *UsageBillingCommand, r *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
			r.BillingFingerprint = "different"
			return r
		}},
		{name: "unknown disposition", mutate: func(_ *UsageBillingCommand, r *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
			r.Disposition = "future"
			return r
		}},
		{name: "no capture time", mutate: func(_ *UsageBillingCommand, r *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
			r.CapturedAt = time.Time{}
			return r
		}},
	} {
		for _, enabled := range []bool{false, true} {
			mode := "/db-fallback"
			if enabled {
				mode = "/flusher"
			}
			t.Run(tc.name+mode, func(t *testing.T) {
				params, deps, cache, quota := quotaIncrementGatewayFixture(enabled, QuotaIncrementApplied, nil)
				financial := &quotaIncrementFinancialRepo{mutateReceipt: tc.mutate}
				before := userPlatformQuotaReceiptErrorTotal.Load()
				applied, err := applyUsageBilling(context.Background(), "invalid-receipt", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
				require.NoError(t, err)
				require.True(t, applied)
				require.Equal(t, 1, financial.applyCalls)
				require.Equal(t, 1.25, financial.balanceDeducted)
				require.Equal(t, before+1, userPlatformQuotaReceiptErrorTotal.Load())
				require.Empty(t, cache.bindings)
				require.Empty(t, cache.calls)
				require.Empty(t, quota.increments)
				require.Zero(t, quota.getCalls, "current quota never supplies missing Apply identity")
				require.Zero(t, quota.legacyCalls)
				require.Equal(t, 1, cache.balanceInvalidations)
				applied, err = applyUsageBilling(context.Background(), "invalid-receipt", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
				require.NoError(t, err)
				require.False(t, applied)
				require.Equal(t, 1.25, financial.balanceDeducted)
				require.Equal(t, before+1, userPlatformQuotaReceiptErrorTotal.Load())
			})
		}
	}
}

func TestGatewayUserPlatformQuotaReceiptDispositionsIgnoreLaterLimits(t *testing.T) {
	for _, tc := range []struct {
		name                string
		raw                 float64
		subscription        bool
		unlimited           bool
		retainQuotaIdentity bool
	}{
		{name: "unlimited then limit added", raw: 1.25, unlimited: true},
		{name: "all null quota then limit added", raw: 1.25, unlimited: true, retainQuotaIdentity: true},
		{name: "zero cost"},
		{name: "tiny positive rounded zero", raw: 1e-11},
		{name: "smallest positive rounded zero", raw: math.SmallestNonzeroFloat64},
		{name: "subscription", raw: 1.25, subscription: true},
	} {
		for _, enabled := range []bool{false, true} {
			mode := "/db-fallback"
			if enabled {
				mode = "/flusher"
			}
			t.Run(tc.name+mode, func(t *testing.T) {
				params, deps, cache, quota := quotaIncrementGatewayFixture(enabled, QuotaIncrementApplied, nil)
				params.Cost = &CostBreakdown{ActualCost: tc.raw, TotalCost: tc.raw}
				params.IsSubscriptionBill = tc.subscription
				log := &UsageLog{BillingType: BillingTypeBalance}
				if tc.subscription {
					params.Subscription = &UserSubscription{ID: 4}
					log.BillingType = BillingTypeSubscription
				}
				financial := &quotaIncrementFinancialRepo{mutateReceipt: func(cmd *UsageBillingCommand, r *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
					if tc.unlimited {
						r.Disposition = "unlimited"
						r.DeliveryMode = ""
						if !tc.retainQuotaIdentity {
							r.QuotaRowID, r.ControlGeneration = 0, 0
						}
					}
					require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, r))
					return r
				}}
				before := userPlatformQuotaReceiptErrorTotal.Load()
				applied, err := applyUsageBilling(context.Background(), "exempt-receipt", log, params, deps, financial)
				require.NoError(t, err)
				require.True(t, applied)
				require.Equal(t, 1, financial.applyCalls)
				require.Empty(t, cache.bindings)
				require.Empty(t, cache.calls)
				require.Empty(t, quota.increments)
				require.Zero(t, quota.getCalls, "a current configured limit does not change the historical disposition")
				require.Equal(t, before, userPlatformQuotaReceiptErrorTotal.Load())
				applied, err = applyUsageBilling(context.Background(), "exempt-receipt", log, params, deps, financial)
				require.NoError(t, err)
				require.False(t, applied)
				require.Equal(t, 2, financial.applyCalls)
				if tc.raw < 1e-10 {
					require.Zero(t, financial.balanceDeducted, "the original scale-8 money amount stays zero")
				}
			})
		}
	}
}

func TestGatewayUserPlatformQuotaBoundDBOutcomesStayBookkeepingOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result UserPlatformQuotaBoundIncrementResult
		err    error
	}{
		{name: "superseded", result: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementSuperseded}},
		{name: "obsolete", result: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementObsolete}},
		{name: "partial", result: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementPartial, AppliedWindows: QuotaWindowWeekly | QuotaWindowMonthly}},
		{name: "applied incomplete", result: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowDaily}},
		{name: "unknown", result: UserPlatformQuotaBoundIncrementResult{Status: "future"}},
		{name: "execution error", result: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll}, err: errors.New("uncertain commit")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, deps, cache, quota := quotaIncrementGatewayFixture(false, QuotaIncrementApplied, nil)
			quota.boundResult, quota.boundErr = tc.result, tc.err
			before := userPlatformQuotaDBIncrErrorTotal.Load()
			financial := &quotaIncrementFinancialRepo{}
			applied, err := applyUsageBilling(context.Background(), "db-unconfirmed", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
			require.NoError(t, err)
			require.True(t, applied)
			assertQuotaIncrementDBFallback(t, quota, false)
			require.Eventually(t, func() bool { return userPlatformQuotaDBIncrErrorTotal.Load() == before+1 }, time.Second, time.Millisecond)
			require.Equal(t, 1, financial.applyCalls)
			require.Equal(t, 1.25, financial.balanceDeducted)
			require.Len(t, cache.bindings, 1)
			require.Empty(t, cache.calls)
			require.Empty(t, quota.increments, "uncertain DB outcome is never automatically retried")
			require.Zero(t, quota.legacyCalls)
			require.Zero(t, quota.boundCalls)
			require.Zero(t, quota.lookupCalls, "production does not automatically look up or replay an unknown delivery")
		})
	}
}

func TestGatewayUserPlatformQuotaBoundWritersNeverFallBack(t *testing.T) {
	params, deps, cache, quota := quotaIncrementGatewayFixture(false, QuotaIncrementApplied, nil)
	unboundCache := &unboundQuotaGatewayCache{}
	deps.billingCacheService.cache = unboundCache
	before := userPlatformQuotaCacheIncrErrorTotal.Load()
	applied, err := applyUsageBilling(context.Background(), "unsupported-cache", &UsageLog{BillingType: BillingTypeBalance}, params, deps, &quotaIncrementFinancialRepo{})
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, before+1, userPlatformQuotaCacheIncrErrorTotal.Load())
	require.Empty(t, unboundCache.calls)
	assertQuotaIncrementDBFallback(t, quota, false)

	params, deps, cache, _ = quotaIncrementGatewayFixture(false, QuotaIncrementApplied, nil)
	unboundRepo := &quotaBindingOnlyDBSpy{}
	deps.userPlatformQuotaRepo = unboundRepo
	dbBefore := userPlatformQuotaDBIncrErrorTotal.Load()
	applied, err = applyUsageBilling(context.Background(), "unsupported-db", &UsageLog{BillingType: BillingTypeBalance}, params, deps, &quotaIncrementFinancialRepo{})
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, dbBefore+1, userPlatformQuotaDBIncrErrorTotal.Load())
	require.Len(t, cache.bindings, 1)
	require.Empty(t, cache.calls)
	require.Zero(t, unboundRepo.getCalls)
	require.Zero(t, unboundRepo.boundCalls, "source consumer support never falls back to a binding-only writer")
}

func TestGatewayUserPlatformQuotaBoundNilConfigUsesBoundFallback(t *testing.T) {
	params, deps, cache, quota := quotaIncrementGatewayFixture(false, QuotaIncrementApplied, nil)
	deps.cfg, deps.billingCacheService.cfg = nil, nil
	applied, err := applyUsageBilling(context.Background(), "nil-config-bound", &UsageLog{BillingType: BillingTypeBalance}, params, deps, &quotaIncrementFinancialRepo{})
	require.NoError(t, err)
	require.True(t, applied)
	require.Len(t, cache.bindings, 1)
	require.Equal(t, 24*time.Hour, cache.ttl)
	require.False(t, cache.markDirty)
	require.Empty(t, cache.calls)
	assertQuotaIncrementDBFallback(t, quota, false)
}

func TestGatewayUserPlatformQuotaReceiptRejectsNonCredentialOrigin(t *testing.T) {
	params, deps, cache, quota := quotaIncrementGatewayFixture(true, QuotaIncrementApplied, nil)
	params.APIKey.Kind = APIKeyKindClientConsumer
	before := userPlatformQuotaReceiptErrorTotal.Load()
	financial := &quotaIncrementFinancialRepo{}
	applied, err := applyUsageBilling(context.Background(), "non-credential-receipt", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, 1, financial.applyCalls)
	require.Equal(t, 1.25, financial.balanceDeducted)
	require.Equal(t, before+1, userPlatformQuotaReceiptErrorTotal.Load())
	require.Empty(t, cache.bindings)
	require.Empty(t, cache.calls)
	require.Empty(t, quota.increments)
	require.Zero(t, quota.getCalls)
}

func TestGatewayUserPlatformQuotaBoundRequiredParentCancellationKeepsBookkeeping(t *testing.T) {
	params, deps, cache, quota := quotaIncrementGatewayFixture(false, QuotaIncrementApplied, nil)
	cmd := ordinaryPlatformReceiptCommandForTest(t, 1.25, false)
	cmd.OrdinaryPlatformQuotaDeliveryMode = UsageBillingOrdinaryPlatformDeliveryDBV1
	receipt := validOrdinaryPlatformReceiptForTest(cmd)
	entered := make(chan struct{})
	release := make(chan struct{})
	quota.beforeBound = func(context.Context) { close(entered); <-release }
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ := WithRequiredUsageSettlement(parent, cmd.RequestID, cmd.APIKeyID)
	require.False(t, durableUsageSettlement(ctx))
	newBalance := 0.0
	finalizePostUsageBilling(ctx, cmd, params, deps, &UsageBillingApplyResult{Applied: true, NewBalance: &newBalance, OrdinaryPlatformQuotaReceipt: receipt})
	<-entered
	cancel()
	close(release)
	assertQuotaIncrementDBFallback(t, quota, false)
	require.Len(t, cache.bindings, 1)
	require.Empty(t, cache.calls)
}

func TestGatewayOrdinaryPlatformDeliveryFreezesOnlyFalseMode(t *testing.T) {
	for _, mode := range []string{"false", "nil", "true"} {
		t.Run(mode, func(t *testing.T) {
			params, deps, cache, quota := quotaIncrementGatewayFixture(mode == "true", QuotaIncrementApplied, nil)
			if mode == "nil" {
				deps.cfg, deps.billingCacheService.cfg = nil, nil
			}
			var capturedCmd *UsageBillingCommand
			var capturedReceipt *UsageBillingOrdinaryPlatformReceipt
			financial := &quotaIncrementFinancialRepo{mutateReceipt: func(cmd *UsageBillingCommand, receipt *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
				copyCmd, copyReceipt := *cmd, *receipt
				capturedCmd, capturedReceipt = &copyCmd, &copyReceipt
				return receipt
			}}
			applied, err := applyUsageBilling(context.Background(), "freeze-delivery-mode", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
			require.NoError(t, err)
			require.True(t, applied)
			require.Len(t, cache.bindings, 1)
			if mode == "true" {
				require.Empty(t, capturedCmd.OrdinaryPlatformQuotaDeliveryMode)
				require.Empty(t, capturedReceipt.DeliveryMode)
				require.Empty(t, quota.increments)
			} else {
				require.NoError(t, ValidateUsageBillingOrdinaryPlatformDelivery(capturedCmd, capturedReceipt))
				assertQuotaIncrementDBFallback(t, quota, false)
			}
			require.Zero(t, quota.lookupCalls)
			require.Zero(t, quota.boundCalls)
			require.Zero(t, quota.legacyCalls)
		})
	}
}

func TestGatewayOrdinaryPlatformDeliveryMissingQualificationIsSourceGap(t *testing.T) {
	params, deps, cache, quota := quotaIncrementGatewayFixture(false, QuotaIncrementApplied, nil)
	financial := &quotaIncrementFinancialRepo{mutateReceipt: func(cmd *UsageBillingCommand, receipt *UsageBillingOrdinaryPlatformReceipt) *UsageBillingOrdinaryPlatformReceipt {
		receipt.DeliveryMode = ""
		require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, receipt), "the older receipt shape remains valid independently")
		return receipt
	}}
	before := userPlatformQuotaReceiptErrorTotal.Load()
	applied, err := applyUsageBilling(context.Background(), "missing-delivery-mode", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, 1, financial.applyCalls)
	require.Equal(t, 1.25, financial.balanceDeducted)
	require.Equal(t, 1, cache.balanceInvalidations)
	require.Equal(t, before+1, userPlatformQuotaReceiptErrorTotal.Load())
	require.Empty(t, cache.bindings)
	require.Empty(t, quota.increments)
	require.Zero(t, quota.getCalls)
	require.Zero(t, quota.boundCalls)
	require.Zero(t, quota.lookupCalls)
}

func TestGatewayOrdinaryPlatformDeliveryValidatesCompletionWithoutRetries(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mutate      func(*UsageBillingOrdinaryPlatformDeliveryResult)
		wantFailure bool
	}{
		{name: "replayed original result", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.Replayed = true }},
		{name: "pending result", wantFailure: true, mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) {
			r.State, r.Outcome, r.CompletedAt = OrdinaryPlatformDeliveryPending, UserPlatformQuotaBoundIncrementResult{}, nil
		}},
		{name: "wrong source", wantFailure: true, mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.SourceFingerprint = "different" }},
		{name: "no commit time", wantFailure: true, mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.CompletedAt = nil }},
		{name: "unknown state", wantFailure: true, mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.State = "future" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, deps, cache, quota := quotaIncrementGatewayFixture(false, QuotaIncrementApplied, nil)
			quota.mutateDelivery = tc.mutate
			financial := &quotaIncrementFinancialRepo{}
			before := userPlatformQuotaDBIncrErrorTotal.Load()
			applied, err := applyUsageBilling(context.Background(), "delivery-completion", &UsageLog{BillingType: BillingTypeBalance}, params, deps, financial)
			if tc.wantFailure {
				require.Eventually(t, func() bool { return userPlatformQuotaDBIncrErrorTotal.Load() == before+1 }, time.Second, time.Millisecond)
			}
			require.NoError(t, err)
			require.True(t, applied)
			assertQuotaIncrementDBFallback(t, quota, false)
			require.Equal(t, 1, financial.applyCalls)
			require.Equal(t, 1.25, financial.balanceDeducted)
			require.Len(t, cache.bindings, 1)
			require.Zero(t, quota.lookupCalls)
			require.Zero(t, quota.boundCalls)
			require.Empty(t, quota.increments)
		})
	}
}

func TestGatewayOrdinaryPlatformDeliveryCopiesImmutableSourceBeforeGoroutine(t *testing.T) {
	params, deps, cache, quota := quotaIncrementGatewayFixture(false, QuotaIncrementApplied, nil)
	cmd, receipt, _ := ordinaryPlatformDeliveryFixture(t)
	originalCmd, originalIntent, originalReceipt := *cmd, *cmd.OrdinaryPlatformQuota, *receipt
	entered, release := make(chan struct{}), make(chan struct{})
	quota.beforeBound = func(context.Context) { close(entered); <-release }
	zeroBalance := 0.0
	finalizePostUsageBilling(context.Background(), cmd, params, deps, &UsageBillingApplyResult{Applied: true, NewBalance: &zeroBalance, OrdinaryPlatformQuotaReceipt: receipt})
	<-entered
	cmd.UserID, cmd.OrdinaryPlatformQuota.UserID = 999, 999
	cmd.OrdinaryPlatformQuotaDeliveryMode = "future"
	receipt.Intent.RawCostUSD, receipt.ControlGeneration, receipt.DeliveryMode = "999", 999, "future"
	close(release)
	select {
	case call := <-quota.increments:
		require.Equal(t, originalCmd.UserID, call.command.UserID)
		require.Equal(t, originalIntent, *call.command.OrdinaryPlatformQuota)
		require.Equal(t, originalReceipt, call.receipt)
		require.NoError(t, ValidateUsageBillingOrdinaryPlatformDelivery(&call.command, &call.receipt))
	case <-time.After(time.Second):
		t.Fatal("mutable caller state changed the scheduled source")
	}
	require.Len(t, cache.bindings, 1)
	require.Zero(t, quota.boundCalls)
	require.Zero(t, quota.lookupCalls)
}
