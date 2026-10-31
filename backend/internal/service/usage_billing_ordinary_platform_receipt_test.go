//go:build unit

package service

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validOrdinaryPlatformReceiptForTest(cmd *UsageBillingCommand) *UsageBillingOrdinaryPlatformReceipt {
	if cmd.OrdinaryPlatformQuota == nil {
		return nil
	}
	r := &UsageBillingOrdinaryPlatformReceipt{Intent: *cmd.OrdinaryPlatformQuota, BillingFingerprint: cmd.RequestFingerprint,
		CapturedAt: time.Date(2026, 10, 25, 3, 4, 5, 678000000, time.UTC)}
	switch {
	case r.Intent.IsSubscription:
		r.Disposition = "subscription_exempt"
	case r.Intent.CostUSD == "0.0000000000":
		r.Disposition = "zero_cost"
	default:
		r.Disposition, r.QuotaRowID, r.ControlGeneration = "metered", 101, 1
		r.DeliveryMode = cmd.OrdinaryPlatformQuotaDeliveryMode
	}
	return r
}

func ordinaryPlatformReceiptCommandForTest(t *testing.T, raw float64, subscription bool) *UsageBillingCommand {
	t.Helper()
	cmd := ordinaryPlatformBillingCommand(raw, subscription)
	var err error
	cmd.OrdinaryPlatformQuota, err = FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformOpenAI, raw, subscription)
	require.NoError(t, err)
	return cmd
}

func TestOrdinaryPlatformReceiptBindsEntireCommittedIntent(t *testing.T) {
	cmd := ordinaryPlatformReceiptCommandForTest(t, 1.25, false)
	valid := validOrdinaryPlatformReceiptForTest(cmd)
	require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, valid))
	for _, tc := range []struct {
		name   string
		mutate func(*UsageBillingOrdinaryPlatformReceipt)
	}{
		{name: "intent version", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.Version++ }},
		{name: "request", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.RequestID += "-other" }},
		{name: "key", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.APIKeyID++ }},
		{name: "user", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.UserID++ }},
		{name: "platform", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.Platform = PlatformAnthropic }},
		{name: "raw amount", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.RawCostUSD = "1.25000000001" }},
		{name: "rounded amount", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.CostUSD = "1.2600000000" }},
		{name: "subscription", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.IsSubscription = true }},
		{name: "intent fingerprint", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.IntentFingerprint = "wrong" }},
		{name: "billing fingerprint", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.BillingFingerprint = "wrong" }},
		{name: "quota identity", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.QuotaRowID = 0 }},
		{name: "generation", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.ControlGeneration = 0 }},
		{name: "negative generation", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.ControlGeneration = -1 }},
		{name: "missing clock", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.CapturedAt = time.Time{} }},
		{name: "negative clock", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.CapturedAt = time.Unix(-1, 0) }},
		{name: "out of range clock", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) {
			r.CapturedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
		{name: "unknown disposition", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Disposition = "later" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := *valid
			tc.mutate(&r)
			require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, &r), ErrUsageBillingOrdinaryPlatformReceipt)
		})
	}
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformReceipt(nil, valid), ErrUsageBillingOrdinaryPlatformReceipt)
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, nil), ErrUsageBillingOrdinaryPlatformReceipt)
	withoutIntent := *cmd
	withoutIntent.OrdinaryPlatformQuota = nil
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformReceipt(&withoutIntent, valid), ErrUsageBillingOrdinaryPlatformReceipt)
}

func TestOrdinaryPlatformReceiptDispositionAndRawBinding(t *testing.T) {
	for _, tc := range []struct {
		name         string
		raw          float64
		subscription bool
		disposition  string
	}{
		{name: "metered raw precision", raw: 0.00007812504, disposition: "metered"},
		{name: "unlimited", raw: 1.25, disposition: "unlimited"},
		{name: "all null quota", raw: 1.25, disposition: "unlimited"},
		{name: "subscription", raw: 1.25, subscription: true, disposition: "subscription_exempt"},
		{name: "subscription zero", subscription: true, disposition: "subscription_exempt"},
		{name: "zero", disposition: "zero_cost"},
		{name: "tiny rounds to zero", raw: math.SmallestNonzeroFloat64, disposition: "zero_cost"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := ordinaryPlatformReceiptCommandForTest(t, tc.raw, tc.subscription)
			r := validOrdinaryPlatformReceiptForTest(cmd)
			r.Disposition = tc.disposition
			if tc.name == "unlimited" {
				r.QuotaRowID, r.ControlGeneration = 0, 0
			}
			require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, r))
			binding, err := r.UsageBinding()
			if tc.disposition == "metered" {
				require.NoError(t, err)
				require.Equal(t, tc.raw, binding.CostUSD, "raw scale is retained independently of journal scale 10")
				require.Equal(t, r.CapturedAt, binding.CapturedAt)
				require.Equal(t, int64(101), binding.QuotaRowID)
				require.Equal(t, int64(1), binding.ControlGeneration)
				require.NoError(t, ValidateUserPlatformQuotaUsageBinding(binding))
			} else {
				require.Error(t, err)
				require.Equal(t, UserPlatformQuotaUsageBinding{}, binding)
			}
			wrong := *r
			if tc.disposition == "metered" || tc.disposition == "unlimited" {
				wrong.Disposition = "zero_cost"
			} else {
				wrong.QuotaRowID, wrong.ControlGeneration = 101, 1
			}
			require.Error(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, &wrong))
		})
	}
}

func TestUserPlatformQuotaUsageBindingRejectsInvalidObservation(t *testing.T) {
	r := validOrdinaryPlatformReceiptForTest(ordinaryPlatformReceiptCommandForTest(t, 1.25, false))
	valid, err := r.UsageBinding()
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		mutate func(*UserPlatformQuotaUsageBinding)
	}{
		{name: "quota", mutate: func(b *UserPlatformQuotaUsageBinding) { b.QuotaRowID = 0 }},
		{name: "generation", mutate: func(b *UserPlatformQuotaUsageBinding) { b.ControlGeneration = 0 }},
		{name: "user", mutate: func(b *UserPlatformQuotaUsageBinding) { b.UserID = 0 }},
		{name: "platform", mutate: func(b *UserPlatformQuotaUsageBinding) { b.Platform = PlatformComposite }},
		{name: "empty platform", mutate: func(b *UserPlatformQuotaUsageBinding) { b.Platform = "" }},
		{name: "zero", mutate: func(b *UserPlatformQuotaUsageBinding) { b.CostUSD = 0 }},
		{name: "negative", mutate: func(b *UserPlatformQuotaUsageBinding) { b.CostUSD = -1 }},
		{name: "nan", mutate: func(b *UserPlatformQuotaUsageBinding) { b.CostUSD = math.NaN() }},
		{name: "positive inf", mutate: func(b *UserPlatformQuotaUsageBinding) { b.CostUSD = math.Inf(1) }},
		{name: "negative inf", mutate: func(b *UserPlatformQuotaUsageBinding) { b.CostUSD = math.Inf(-1) }},
		{name: "clock", mutate: func(b *UserPlatformQuotaUsageBinding) { b.CapturedAt = time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := valid
			tc.mutate(&b)
			require.ErrorIs(t, ValidateUserPlatformQuotaUsageBinding(b), ErrUserPlatformQuotaUsageBinding)
		})
	}
}

func TestOrdinaryPlatformReceiptAndBoundObservationStayPrivate(t *testing.T) {
	cmd := ordinaryPlatformReceiptCommandForTest(t, 1.25, false)
	r := validOrdinaryPlatformReceiptForTest(cmd)
	b, err := r.UsageBinding()
	require.NoError(t, err)
	for _, value := range []any{r, b, UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementPartial, AppliedWindows: QuotaWindowWeekly | QuotaWindowMonthly}} {
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(encoded))
	}
	encoded, err := json.Marshal(UsageBillingApplyResult{Applied: true, OrdinaryPlatformQuotaReceipt: r})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "Receipt")
	require.NotContains(t, string(encoded), r.BillingFingerprint)
	require.NotContains(t, string(encoded), r.CapturedAt.Format(time.RFC3339))
}

func TestOrdinaryPlatformReceiptUsageBindingRejectsInvalidRawDirectCall(t *testing.T) {
	valid := validOrdinaryPlatformReceiptForTest(ordinaryPlatformReceiptCommandForTest(t, 1.25, false))
	for _, raw := range []string{"", "-1", "NaN", "+Inf", "1e999", strings.Repeat("1", 513)} {
		r := *valid
		r.Intent.RawCostUSD = raw
		_, err := r.UsageBinding()
		require.Error(t, err)
	}
}
