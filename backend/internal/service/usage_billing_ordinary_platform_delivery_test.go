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

func ordinaryPlatformDeliveryFixture(t *testing.T) (*UsageBillingCommand, *UsageBillingOrdinaryPlatformReceipt, UsageBillingOrdinaryPlatformDeliveryResult) {
	t.Helper()
	cmd := ordinaryPlatformReceiptCommandForTest(t, 1.25, false)
	cmd.OrdinaryPlatformQuotaDeliveryMode = UsageBillingOrdinaryPlatformDeliveryDBV1
	receipt := validOrdinaryPlatformReceiptForTest(cmd)
	require.NoError(t, ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt))
	fingerprint, err := FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	require.NoError(t, err)
	created := receipt.CapturedAt.Add(time.Microsecond)
	completed := created.Add(time.Microsecond)
	return cmd, receipt, UsageBillingOrdinaryPlatformDeliveryResult{State: OrdinaryPlatformDeliveryCompleted, SourceFingerprint: fingerprint,
		Outcome: UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll}, CreatedAt: created, CompletedAt: &completed}
}

func TestOrdinaryPlatformDeliveryRequiresExplicitMeteredQualification(t *testing.T) {
	cmd, receipt, _ := ordinaryPlatformDeliveryFixture(t)
	legacy := *receipt
	legacy.DeliveryMode = ""
	require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, &legacy), "old receipt shape stays readable")
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformDelivery(cmd, &legacy), ErrUsageBillingOrdinaryPlatformDelivery)
	plainCommand := *cmd
	plainCommand.OrdinaryPlatformQuotaDeliveryMode = ""
	require.Error(t, ValidateUsageBillingOrdinaryPlatformReceipt(&plainCommand, receipt))
	require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(&plainCommand, &legacy))
	unknownCommand := *cmd
	unknownCommand.OrdinaryPlatformQuotaDeliveryMode = "future"
	require.Error(t, ValidateUsageBillingOrdinaryPlatformReceipt(&unknownCommand, &legacy))
	unknownReceipt := *receipt
	unknownReceipt.DeliveryMode = "future"
	require.Error(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, &unknownReceipt))
	for _, disposition := range []string{"unlimited", "subscription_exempt", "zero_cost"} {
		raw, subscription := 1.25, false
		if disposition == "subscription_exempt" {
			subscription = true
		}
		if disposition == "zero_cost" {
			raw = 1e-11
		}
		exemptCmd := ordinaryPlatformReceiptCommandForTest(t, raw, subscription)
		exemptCmd.OrdinaryPlatformQuotaDeliveryMode = UsageBillingOrdinaryPlatformDeliveryDBV1
		exempt := validOrdinaryPlatformReceiptForTest(exemptCmd)
		exempt.Disposition, exempt.DeliveryMode = disposition, ""
		if disposition == "unlimited" {
			exempt.QuotaRowID, exempt.ControlGeneration = 0, 0
		}
		require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(exemptCmd, exempt))
		require.Error(t, ValidateUsageBillingOrdinaryPlatformDelivery(exemptCmd, exempt))
		exempt.DeliveryMode = UsageBillingOrdinaryPlatformDeliveryDBV1
		require.Error(t, ValidateUsageBillingOrdinaryPlatformReceipt(exemptCmd, exempt))
	}
	require.Error(t, ValidateUsageBillingOrdinaryPlatformDelivery(nil, receipt))
	require.Error(t, ValidateUsageBillingOrdinaryPlatformDelivery(cmd, nil))
}

func TestOrdinaryPlatformDeliveryFingerprintBindsClosedSourceAndCanonicalTime(t *testing.T) {
	_, receipt, result := ordinaryPlatformDeliveryFixture(t)
	zone := *receipt
	zone.CapturedAt = zone.CapturedAt.In(time.FixedZone("fixture", 8*60*60))
	fingerprint, err := FingerprintUsageBillingOrdinaryPlatformDelivery(&zone)
	require.NoError(t, err)
	require.Equal(t, result.SourceFingerprint, fingerprint, "the same instant has one UTC source identity")
	for _, tc := range []struct {
		name   string
		mutate func(*UsageBillingOrdinaryPlatformReceipt)
	}{
		{name: "request", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.RequestID += "-new" }},
		{name: "key", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.APIKeyID++ }},
		{name: "user", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.UserID++ }},
		{name: "platform", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.Platform = PlatformAnthropic }},
		{name: "raw precision", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.RawCostUSD = "1.25000000001" }},
		{name: "amount", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) {
			r.Intent.RawCostUSD, r.Intent.CostUSD = "1.5", "1.5000000000"
		}},
		{name: "billing proof", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.BillingFingerprint = strings.Repeat("b", 64) }},
		{name: "quota", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.QuotaRowID++ }},
		{name: "generation", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.ControlGeneration++ }},
		{name: "nanosecond", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.CapturedAt = r.CapturedAt.Add(time.Nanosecond) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := *receipt
			tc.mutate(&r)
			r.Intent.IntentFingerprint = usageBillingOrdinaryPlatformHash(r.BillingFingerprint, &r.Intent)
			fp, err := FingerprintUsageBillingOrdinaryPlatformDelivery(&r)
			require.NoError(t, err)
			require.NotEqual(t, result.SourceFingerprint, fp)
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*UsageBillingOrdinaryPlatformReceipt)
	}{
		{name: "missing mode", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.DeliveryMode = "" }},
		{name: "unknown mode", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.DeliveryMode = "future" }},
		{name: "wrong disposition", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Disposition = "unlimited" }},
		{name: "no identity", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.QuotaRowID = 0 }},
		{name: "old generation", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.ControlGeneration = 0 }},
		{name: "invalid intent", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.IntentFingerprint = strings.Repeat("0", 64) }},
		{name: "oversize raw", mutate: func(r *UsageBillingOrdinaryPlatformReceipt) { r.Intent.RawCostUSD = strings.Repeat("1", 513) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := *receipt
			tc.mutate(&r)
			_, err := FingerprintUsageBillingOrdinaryPlatformDelivery(&r)
			require.Error(t, err)
		})
	}
	_, err = FingerprintUsageBillingOrdinaryPlatformDelivery(nil)
	require.Error(t, err)
}

func TestOrdinaryPlatformDeliveryResultRecognizesStableTerminalOutcomes(t *testing.T) {
	cmd, receipt, full := ordinaryPlatformDeliveryFixture(t)
	for _, outcome := range []UserPlatformQuotaBoundIncrementResult{
		{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll},
		{Status: QuotaIncrementPartial, AppliedWindows: QuotaWindowWeekly | QuotaWindowMonthly},
		{Status: QuotaIncrementSuperseded},
		{Status: QuotaIncrementObsolete},
	} {
		for _, replayed := range []bool{false, true} {
			r := full
			r.Outcome, r.Replayed = outcome, replayed
			require.NoError(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, r, false))
		}
	}
	pending := full
	pending.State, pending.Outcome, pending.CompletedAt = OrdinaryPlatformDeliveryPending, UserPlatformQuotaBoundIncrementResult{}, nil
	require.NoError(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, pending, true))
	require.Error(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, pending, false))
	pending.Replayed = true
	require.Error(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, pending, true))
}

func TestOrdinaryPlatformDeliveryResultRejectsUnboundOrImpossibleResults(t *testing.T) {
	cmd, receipt, full := ordinaryPlatformDeliveryFixture(t)
	for _, tc := range []struct {
		name   string
		mutate func(*UsageBillingOrdinaryPlatformDeliveryResult)
	}{
		{name: "source mismatch", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.SourceFingerprint = strings.Repeat("a", 64) }},
		{name: "zero birth", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.CreatedAt = time.Time{} }},
		{name: "birth before source", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) {
			r.CreatedAt = receipt.CapturedAt.Add(-time.Microsecond)
		}},
		{name: "unknown state", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.State = "future" }},
		{name: "missing completion", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.CompletedAt = nil }},
		{name: "old completion", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) {
			at := r.CreatedAt.Add(-time.Microsecond)
			r.CompletedAt = &at
		}},
		{name: "incomplete applied", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.Outcome.AppliedWindows = 3 }},
		{name: "invalid mask", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.Outcome.AppliedWindows = 15 }},
		{name: "empty partial", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) {
			r.Outcome = UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementPartial}
		}},
		{name: "full partial", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.Outcome.Status = QuotaIncrementPartial }},
		{name: "superseded with mutation", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.Outcome.Status = QuotaIncrementSuperseded }},
		{name: "uncommitted missing", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) {
			r.Outcome = UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementMissing}
		}},
		{name: "unknown outcome", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) { r.Outcome.Status = "future" }},
		{name: "pending with effects", mutate: func(r *UsageBillingOrdinaryPlatformDeliveryResult) {
			r.State, r.CompletedAt = OrdinaryPlatformDeliveryPending, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := full
			tc.mutate(&r)
			require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, r, true), ErrUsageBillingOrdinaryPlatformDeliveryResult)
		})
	}
}

func TestOrdinaryPlatformDeliveryPrivateModePreservesMoneyAndSettlementWire(t *testing.T) {
	cmd, receipt, result := ordinaryPlatformDeliveryFixture(t)
	receipt.WriterEpoch = 7
	before := cmd.RequestFingerprint
	cmd.Normalize()
	require.Equal(t, before, cmd.RequestFingerprint)
	require.Equal(t, before, buildUsageBillingFingerprint(cmd))
	for _, value := range []any{receipt, result} {
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(encoded))
	}
	encoded, err := json.Marshal(cmd)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "DeliveryMode")
	require.NotContains(t, string(encoded), UsageBillingOrdinaryPlatformDeliveryDBV1)
	legacy, err := DecodeUsageSettlementEvent([]byte(usageSettlementV1Golden))
	require.NoError(t, err)
	legacy.Command.OrdinaryPlatformQuotaDeliveryMode = UsageBillingOrdinaryPlatformDeliveryDBV1
	encoded, err = EncodeUsageSettlementEvent(legacy)
	require.NoError(t, err)
	require.Equal(t, usageSettlementV1Golden, string(encoded))
	base := settlementEventFixture(t)
	v2, err := FreezeUsageSettlementEventWithPlatform(base.InvocationID, base.Command, base.Usage, PlatformOpenAI)
	require.NoError(t, err)
	beforeWire, err := EncodeUsageSettlementEvent(v2)
	require.NoError(t, err)
	v2.Command.OrdinaryPlatformQuotaDeliveryMode = UsageBillingOrdinaryPlatformDeliveryDBV1
	afterWire, err := EncodeUsageSettlementEvent(v2)
	require.NoError(t, err)
	require.Equal(t, beforeWire, afterWire)
	decoded, err := DecodeUsageSettlementEvent(afterWire)
	require.NoError(t, err)
	require.Empty(t, decoded.Command.OrdinaryPlatformQuotaDeliveryMode)
}

func TestOrdinaryPlatformDeliveryWriterEpochPreservesV1AndBindsV2(t *testing.T) {
	cmd, receipt, terminal := ordinaryPlatformDeliveryFixture(t)
	// Captured from the explicit v1 representation before adding writer epoch.
	const v1Fingerprint = "c100d5c0184280bcfc492286ff6a9f3fcd88ab697563b1cff5bfc38bf6d523d2"
	const v2EpochOneFingerprint = "6a2ca688d93526798aa404d9f058b49550c6a5524e90b02e2a7bf151777da96b"
	require.Zero(t, receipt.WriterEpoch)
	v1, err := FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	require.NoError(t, err)
	require.Equal(t, v1Fingerprint, v1, "an epoch-zero source retains its exact historic identity")
	require.NoError(t, ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt), "historic source shape remains readable without granting pending consumption")
	require.NoError(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, terminal, false))
	seen := map[string]bool{v1: true}
	for _, epoch := range []int64{1, 2, math.MaxInt64} {
		r := *receipt
		r.WriterEpoch = epoch
		require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, &r))
		require.NoError(t, ValidateUsageBillingOrdinaryPlatformDelivery(cmd, &r))
		fingerprint, err := FingerprintUsageBillingOrdinaryPlatformDelivery(&r)
		require.NoError(t, err)
		require.False(t, seen[fingerprint], "each epoch is an independent source identity")
		seen[fingerprint] = true
		if epoch == 1 {
			require.Equal(t, v2EpochOneFingerprint, fingerprint, "v2 has its own explicit canonical version and epoch field")
		}
		require.Error(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, &r, terminal, false), "a v1 terminal proof cannot acknowledge a v2 source")
		currentTerminal := terminal
		currentTerminal.SourceFingerprint = fingerprint
		require.NoError(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, &r, currentTerminal, false))
		r.CapturedAt = r.CapturedAt.In(time.FixedZone("fixture", 8*60*60))
		zoned, err := FingerprintUsageBillingOrdinaryPlatformDelivery(&r)
		require.NoError(t, err)
		require.Equal(t, fingerprint, zoned)
	}
	unchanged, err := FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	require.NoError(t, err)
	require.Equal(t, v1Fingerprint, unchanged)
	require.Equal(t, cmd.RequestFingerprint, buildUsageBillingFingerprint(cmd), "writer epoch does not change financial identity")
}

func TestOrdinaryPlatformDeliveryWriterEpochRejectsNegativeValues(t *testing.T) {
	cmd, receipt, result := ordinaryPlatformDeliveryFixture(t)
	receipt.WriterEpoch = -1
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, receipt), ErrUsageBillingOrdinaryPlatformReceipt)
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt), ErrUsageBillingOrdinaryPlatformDelivery)
	_, err := FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	require.ErrorIs(t, err, ErrUsageBillingOrdinaryPlatformDelivery)
	require.ErrorIs(t, ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, result, false), ErrUsageBillingOrdinaryPlatformDeliveryResult)
}
