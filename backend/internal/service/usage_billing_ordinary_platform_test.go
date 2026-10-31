//go:build unit

package service

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func ordinaryPlatformBillingCommand(raw float64, subscription bool) *UsageBillingCommand {
	cmd := &UsageBillingCommand{
		RequestID: "ordinary-request", UserID: 1, APIKeyID: 2, AccountID: 3,
		AccountType: AccountTypeAPIKey, Model: "ordinary-model", InputTokens: 10, OutputTokens: 5,
		BalanceCost: raw,
	}
	if subscription {
		subscriptionID := int64(4)
		cmd.BillingType, cmd.SubscriptionID = BillingTypeSubscription, &subscriptionID
		cmd.BalanceCost, cmd.SubscriptionCost = 0, raw
	}
	cmd.Normalize()
	return cmd
}

type ordinaryPlatformLegacyCacheSpy struct {
	durablePlatformLegacyCacheSpy
	rateLimitInvalidations int
}

func (s *ordinaryPlatformLegacyCacheSpy) InvalidateAPIKeyRateLimit(context.Context, int64) error {
	s.rateLimitInvalidations++
	return nil
}

func TestUsageBillingOrdinaryPlatformIntentFreezesRawAndIndependentScales(t *testing.T) {
	for _, tc := range []struct {
		name                string
		raw                 float64
		platform, financial string
	}{
		{name: "zero", raw: 0, platform: "0.0000000000", financial: "0"},
		{name: "double-rounding-boundary", raw: 0.00000000495, platform: "0.0000000050", financial: "0"},
		{name: "financial-half", raw: 0.000000005, platform: "0.0000000050", financial: "0.00000001"},
		{name: "separate-platform-precision", raw: 0.000078125, platform: "0.0000781250", financial: "0.00007813"},
		{name: "smallest-finite", raw: math.SmallestNonzeroFloat64, platform: "0.0000000000", financial: "0"},
		{name: "large-finite", raw: 9999999999.999998, platform: "9999999999.9999980000", financial: "9999999999.999998"},
	} {
		for _, subscription := range []bool{false, true} {
			name := tc.name + "/balance"
			if subscription {
				name = tc.name + "/subscription"
			}
			t.Run(name, func(t *testing.T) {
				cmd := ordinaryPlatformBillingCommand(tc.raw, subscription)
				before := *cmd
				intent, err := FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformOpenAI, tc.raw, subscription)
				require.NoError(t, err)
				require.Equal(t, before, *cmd, "freezing does not normalize or mutate the financial command")
				require.Equal(t, tc.platform, intent.CostUSD)
				require.Equal(t, decimal.NewFromFloat(tc.raw).String(), intent.RawCostUSD)
				require.Equal(t, subscription, intent.IsSubscription)
				financial := cmd.BalanceCost
				if subscription {
					financial = cmd.SubscriptionCost
				}
				require.Equal(t, tc.financial, decimal.NewFromFloat(financial).String())
				require.NoError(t, ValidateUsageBillingOrdinaryPlatformIntent(cmd, intent))
				second, err := FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformOpenAI, tc.raw, subscription)
				require.NoError(t, err)
				require.Equal(t, intent, second)
			})
		}
	}
}

func TestUsageBillingOrdinaryPlatformIntentKeepsHistoricalFingerprintAndPrivateJSON(t *testing.T) {
	cmd := ordinaryPlatformBillingCommand(0.000078125, false)
	const historicalFingerprint = "c810e6da655e73da68e31ee5d7620b8e10bbe8708a4c1f59ea09d903edeec5ef"
	require.Equal(t, historicalFingerprint, cmd.RequestFingerprint)
	before := buildUsageBillingFingerprint(cmd)
	intent, err := FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformOpenAI, 0.000078125, false)
	require.NoError(t, err)
	cmd.OrdinaryPlatformQuota = intent
	require.Equal(t, before, buildUsageBillingFingerprint(cmd))
	cmd.Normalize()
	require.Equal(t, historicalFingerprint, cmd.RequestFingerprint)
	encoded, err := json.Marshal(intent)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(encoded))
	encoded, err = json.Marshal(cmd)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "OrdinaryPlatformQuota")
	require.NotContains(t, string(encoded), intent.IntentFingerprint)
	other, err := FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformAnthropic, 0.000078125, false)
	require.NoError(t, err)
	require.NotEqual(t, intent.IntentFingerprint, other.IntentFingerprint)
	require.Equal(t, historicalFingerprint, cmd.RequestFingerprint)
}

func TestUsageBillingOrdinaryPlatformIntentNeverEntersV1OrV2SettlementWire(t *testing.T) {
	legacy, err := DecodeUsageSettlementEvent([]byte(usageSettlementV1Golden))
	require.NoError(t, err)
	intent, err := FreezeUsageBillingOrdinaryPlatformIntent(legacy.Command, PlatformOpenAI, legacy.Usage.ActualCost, false)
	require.NoError(t, err)
	legacy.Command.OrdinaryPlatformQuota = intent
	encoded, err := EncodeUsageSettlementEvent(legacy)
	require.NoError(t, err)
	require.Equal(t, usageSettlementV1Golden, string(encoded))
	decoded, err := DecodeUsageSettlementEvent(encoded)
	require.NoError(t, err)
	require.Nil(t, decoded.Command.OrdinaryPlatformQuota)

	base := settlementEventFixture(t)
	v2, err := FreezeUsageSettlementEventWithPlatform(base.InvocationID, base.Command, base.Usage, PlatformOpenAI)
	require.NoError(t, err)
	before, err := EncodeUsageSettlementEvent(v2)
	require.NoError(t, err)
	v2.Command.OrdinaryPlatformQuota, err = FreezeUsageBillingOrdinaryPlatformIntent(v2.Command, PlatformOpenAI, v2.Usage.ActualCost, false)
	require.NoError(t, err)
	after, err := EncodeUsageSettlementEvent(v2)
	require.NoError(t, err)
	require.Equal(t, before, after)
	decoded, err = DecodeUsageSettlementEvent(after)
	require.NoError(t, err)
	require.Nil(t, decoded.Command.OrdinaryPlatformQuota)
}

func TestUsageBillingOrdinaryPlatformIntentRejectsInvalidServerInputs(t *testing.T) {
	for _, raw := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), 1e10, math.MaxFloat64} {
		_, err := FreezeUsageBillingOrdinaryPlatformIntent(ordinaryPlatformBillingCommand(1, false), PlatformOpenAI, raw, false)
		require.ErrorIs(t, err, ErrUsageBillingOrdinaryPlatformIntent)
	}
	for _, platform := range []string{"", PlatformComposite, "unknown", " openai", "OPENAI"} {
		_, err := FreezeUsageBillingOrdinaryPlatformIntent(ordinaryPlatformBillingCommand(1, false), platform, 1, false)
		require.ErrorIs(t, err, ErrUsageBillingOrdinaryPlatformIntent)
	}
	_, err := FreezeUsageBillingOrdinaryPlatformIntent(nil, PlatformOpenAI, 1, false)
	require.ErrorIs(t, err, ErrUsageBillingOrdinaryPlatformIntent)
	for _, tc := range []struct {
		name   string
		mutate func(*UsageBillingCommand, *UsageBillingOrdinaryPlatformIntent)
	}{
		{name: "version", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.Version++ }},
		{name: "request", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.RequestID += "-other" }},
		{name: "key", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.APIKeyID++ }},
		{name: "user", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.UserID++ }},
		{name: "hash", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) {
			i.IntentFingerprint = strings.Repeat("a", 64)
		}},
		{name: "raw-text", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.RawCostUSD = "NaN" }},
		{name: "raw-exponent", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.RawCostUSD = "1e0" }},
		{name: "raw-negative", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.RawCostUSD = "-1" }},
		{name: "raw-noncanonical", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.RawCostUSD = "1.0" }},
		{name: "raw-too-long", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) {
			i.RawCostUSD = strings.Repeat("0", 513)
		}},
		{name: "raw-overflow", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.RawCostUSD = "10000000000" }},
		{name: "fixed-scale", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.CostUSD = "1" }},
		{name: "fixed-overprecision", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.CostUSD = "1.00000000001" }},
		{name: "different-platform-cost", mutate: func(c *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) {
			i.CostUSD = "2.0000000000"
			i.IntentFingerprint = usageBillingOrdinaryPlatformHash(c.RequestFingerprint, i)
		}},
		{name: "different-financial-cost", mutate: func(c *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) {
			i.RawCostUSD, i.CostUSD = "2", "2.0000000000"
			i.IntentFingerprint = usageBillingOrdinaryPlatformHash(c.RequestFingerprint, i)
		}},
		{name: "subscription-flag", mutate: func(_ *UsageBillingCommand, i *UsageBillingOrdinaryPlatformIntent) { i.IsSubscription = true }},
		{name: "invalid-billing-type", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.BillingType = 2 }},
		{name: "zero-key", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.APIKeyID = 0 }},
		{name: "zero-user", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.UserID = 0 }},
		{name: "request-empty", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.RequestID = "" }},
		{name: "request-padding", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.RequestID = " ordinary-request" }},
		{name: "request-overflow", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) {
			c.RequestID = strings.Repeat("a", 256)
		}},
		{name: "billing-fingerprint", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.RequestFingerprint = "unbound" }},
		{name: "nan-financial-cost", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.BalanceCost = math.NaN() }},
		{name: "infinite-other-cost", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.SubscriptionCost = math.Inf(1) }},
		{name: "both-financial-paths", mutate: func(c *UsageBillingCommand, _ *UsageBillingOrdinaryPlatformIntent) { c.SubscriptionCost = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := ordinaryPlatformBillingCommand(1, false)
			intent, err := FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformOpenAI, 1, false)
			require.NoError(t, err)
			tc.mutate(cmd, intent)
			require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformIntent(cmd, intent), ErrUsageBillingOrdinaryPlatformIntent)
		})
	}
	cmd := ordinaryPlatformBillingCommand(0.00000000495, false)
	intent, err := FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformOpenAI, 0.00000000495, false)
	require.NoError(t, err)
	intent.RawCostUSD = "0.000000005"
	intent.IntentFingerprint = usageBillingOrdinaryPlatformHash(cmd.RequestFingerprint, intent)
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformIntent(cmd, intent), ErrUsageBillingOrdinaryPlatformIntent, "raw10 must not replace raw when proving independent financial rounding")
	cmd = ordinaryPlatformBillingCommand(1, true)
	intent, err = FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformOpenAI, 1, true)
	require.NoError(t, err)
	cmd.SubscriptionID = nil
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformIntent(cmd, intent), ErrUsageBillingOrdinaryPlatformIntent)
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformIntent(nil, intent), ErrUsageBillingOrdinaryPlatformIntent)
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformIntent(cmd, nil), ErrUsageBillingOrdinaryPlatformIntent)
}

func TestUsageBillingOrdinaryPlatformIntentReachesBothRealGatewayRecorders(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		t.Run(kind, func(t *testing.T) {
			logs := &requiredUsageLogRepoStub{}
			billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: false}}
			fixture := newRequiredUsageFixture(kind, logs, billing)
			require.NoError(t, fixture.call(context.Background()))
			require.Equal(t, 1, billing.calls)
			require.NotNil(t, billing.cmd.OrdinaryPlatformQuota)
			require.NoError(t, ValidateUsageBillingOrdinaryPlatformIntent(billing.cmd, billing.cmd.OrdinaryPlatformQuota))
			require.Equal(t, *fixture.platform, billing.cmd.OrdinaryPlatformQuota.Platform)
			require.Equal(t, decimal.NewFromFloat(logs.queued.ActualCost).String(), billing.cmd.OrdinaryPlatformQuota.RawCostUSD)
		})
	}
}

func TestUsageBillingOrdinaryPlatformIntentSkipsClientAndMalformedCredentialOrigins(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, origin := range []string{"client", "unknown-kind", "consumer-pointer", "internal-prefix", "durable"} {
			t.Run(kind+"/"+origin, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{inserted: true}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: false}}
				fixture := newRequiredUsageFixture(kind, logs, billing)
				ctx := context.Background()
				switch origin {
				case "client":
					fixture.key.Kind = APIKeyKindClientConsumer
				case "unknown-kind":
					fixture.key.Kind = "unknown-kind"
				case "consumer-pointer":
					consumerID := "consumer-fixture"
					fixture.key.ClientConsumerID = &consumerID
				case "internal-prefix":
					fixture.key.Key = InternalAPIKeyPrefix + "private"
				case "durable":
					ctx, _ = WithDurableUsageSettlement(ctx, durableUsageBinding(), fixture.key.ID, &durableUsageEventRepoStub{})
				}
				require.NoError(t, fixture.call(ctx))
				require.Equal(t, 1, billing.calls)
				require.Nil(t, billing.cmd.OrdinaryPlatformQuota)
			})
		}
	}
}

func TestUsageBillingOrdinaryPlatformIntentCoversSubscriptionZeroAndUsesBoundFinalize(t *testing.T) {
	for _, mode := range []string{"ordinary", "subscription", "zero", "simple", "missing-platform"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Database.UserPlatformQuotaFlusherEnabled = true
			cache := &ordinaryPlatformLegacyCacheSpy{}
			limit := 10.0
			quotaRepo := &fakeQuotaRepo{rec: &UserPlatformQuotaRecord{QuotaRowID: 101, ControlGeneration: 1, UserID: 1, Platform: PlatformOpenAI, DailyLimitUSD: &limit}}
			deps := &billingDeps{cfg: cfg, deferredService: &DeferredService{}, userPlatformQuotaRepo: quotaRepo, billingCacheService: &BillingCacheService{cfg: cfg, cache: cache, userPlatformQuotaRepo: quotaRepo}}
			params := &postUsageBillingParams{Cost: &CostBreakdown{ActualCost: 1.25, TotalCost: 1.25}, User: &User{ID: 1}, APIKey: &APIKey{ID: 2}, Account: &Account{ID: 3}, Platform: PlatformOpenAI}
			log := &UsageLog{BillingType: BillingTypeBalance}
			switch mode {
			case "subscription":
				params.IsSubscriptionBill = true
				params.Subscription = &UserSubscription{ID: 4}
				log.BillingType = BillingTypeSubscription
			case "zero":
				params.Cost = &CostBreakdown{}
			case "simple":
				params.SimpleModeKeyRateLimitOnly = true
			case "missing-platform":
				params.Platform = ""
			}
			zeroBalance := 0.0
			repo := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true, NewBalance: &zeroBalance}}
			repo.before = func(context.Context) {
				repo.result.OrdinaryPlatformQuotaReceipt = validOrdinaryPlatformReceiptForTest(repo.cmd)
				if repo.cmd.OrdinaryPlatformQuota != nil {
					require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(repo.cmd, repo.result.OrdinaryPlatformQuotaReceipt))
				}
			}
			_, err := applyUsageBilling(context.Background(), "ordinary-request", log, params, deps, repo)
			require.NoError(t, err)
			require.Equal(t, 1, repo.calls)
			if mode == "simple" || mode == "missing-platform" {
				require.Nil(t, repo.cmd.OrdinaryPlatformQuota)
			} else {
				require.NotNil(t, repo.cmd.OrdinaryPlatformQuota)
				require.NoError(t, ValidateUsageBillingOrdinaryPlatformIntent(repo.cmd, repo.cmd.OrdinaryPlatformQuota))
				require.Equal(t, mode == "subscription", repo.cmd.OrdinaryPlatformQuota.IsSubscription)
			}
			if mode == "ordinary" {
				require.Equal(t, 1, cache.quotaIncrements, "the valid committed receipt reaches the bound quota writer")
			} else {
				require.Zero(t, cache.quotaIncrements)
			}
			if mode == "simple" {
				require.Equal(t, 1, cache.rateLimitInvalidations)
			} else {
				require.Zero(t, cache.rateLimitInvalidations)
			}
		})
	}
}

func TestUsageBillingOrdinaryPlatformIntentRejectsInvalidConcretePlatformBeforeApply(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, platform := range []string{PlatformComposite, "unknown-provider", " openai"} {
			t.Run(kind+"/"+platform, func(t *testing.T) {
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
				fixture := newRequiredUsageFixture(kind, &requiredUsageLogRepoStub{}, billing)
				*fixture.platform = platform
				require.ErrorIs(t, fixture.call(context.Background()), ErrUsageBillingOrdinaryPlatformIntent)
				require.Zero(t, billing.calls)
			})
		}
	}
}

func TestUsageBillingOrdinaryPlatformIntentSurfacesSourceCoverageFailure(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		t.Run(kind, func(t *testing.T) {
			logs := &requiredUsageLogRepoStub{}
			billing := &requiredUsageBillingRepoStub{err: ErrUsageBillingOrdinaryPlatformCoverageGap}
			fixture := newRequiredUsageFixture(kind, logs, billing)
			require.ErrorIs(t, fixture.call(context.Background()), ErrUsageBillingOrdinaryPlatformCoverageGap)
			require.NotNil(t, billing.cmd.OrdinaryPlatformQuota)
			require.Equal(t, 1, logs.bestEffort)
			require.Zero(t, logs.queued.ActualCost, "a source-coverage failure must not be presented as successful ordinary settlement")
		})
	}
}

func TestUsageBillingOrdinaryPlatformIntentKeepsLegacyMoneyAndReportsSourceGap(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		t.Run(kind, func(t *testing.T) {
			logs := &requiredUsageLogRepoStub{}
			fixture := newRequiredUsageFixture(kind, logs, nil)
			before := userPlatformQuotaLegacyUnboundErrorTotal.Load()
			require.NoError(t, fixture.call(context.Background()))
			require.Equal(t, 1, fixture.legacy.deductCalls)
			require.Equal(t, 1, logs.bestEffort)
			require.Equal(t, before+1, userPlatformQuotaLegacyUnboundErrorTotal.Load())
		})
	}
}
