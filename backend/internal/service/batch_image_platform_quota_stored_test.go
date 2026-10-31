//go:build unit

package service

import (
	"math"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestBatchImagePlatformQuotaStoredIntentAcceptsExactRawAndIndependentScaleTen(t *testing.T) {
	for _, tc := range []struct {
		name, fixedCost string
		hold, actual    float64
	}{
		{name: "zero", fixedCost: "0.0000000000"},
		{name: "normal", hold: 2, actual: 1.25, fixedCost: "1.2500000000"},
		{name: "raw-before-wallet-rounding", hold: 0.000078125, actual: 0.00000000496, fixedCost: "0.0000000050"},
		{name: "scale-ten-half", hold: 1, actual: 0.00000000005, fixedCost: "0.0000000001"},
		{name: "smallest-finite", hold: math.SmallestNonzeroFloat64, actual: math.SmallestNonzeroFloat64, fixedCost: "0.0000000000"},
		{name: "largest-finite-in-domain", hold: 9999999999.999998, actual: 9999999999.999998, fixedCost: "9999999999.9999980000"},
	} {
		for _, provider := range []string{BatchImageProviderGeminiAPI, BatchImageProviderVertex} {
			t.Run(tc.name+"/"+provider, func(t *testing.T) {
				cmd := batchImagePlatformCommand(tc.hold, tc.actual)
				intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, provider)
				require.NoError(t, err)
				beforeIntent, beforeCommand := *intent, *cmd
				require.NoError(t, ValidateBatchImagePlatformQuotaStoredIntent(intent, cmd.RequestFingerprint))
				require.Equal(t, decimal.NewFromFloat(tc.hold).String(), intent.RawHoldUSD)
				require.Equal(t, decimal.NewFromFloat(tc.actual).String(), intent.RawCostUSD)
				require.Equal(t, tc.fixedCost, intent.CostUSD)
				require.Equal(t, beforeIntent, *intent)
				require.Equal(t, beforeCommand, *cmd)
			})
		}
	}
}

func TestBatchImagePlatformQuotaStoredIntentRejectsHashAndScopeTampering(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*BatchImagePlatformQuotaIntent)
	}{
		{name: "version", mutate: func(i *BatchImagePlatformQuotaIntent) { i.Version++ }},
		{name: "request", mutate: func(i *BatchImagePlatformQuotaIntent) { i.RequestID += "x" }},
		{name: "batch", mutate: func(i *BatchImagePlatformQuotaIntent) { i.BatchID += "x" }},
		{name: "owner", mutate: func(i *BatchImagePlatformQuotaIntent) { i.UserID++ }},
		{name: "key", mutate: func(i *BatchImagePlatformQuotaIntent) { i.APIKeyID++ }},
		{name: "provider", mutate: func(i *BatchImagePlatformQuotaIntent) { i.Provider = BatchImageProviderVertex }},
		{name: "hold", mutate: func(i *BatchImagePlatformQuotaIntent) { i.RawHoldUSD = "3" }},
		{name: "raw-cost-same-legacy-and-fixed-ten", mutate: func(i *BatchImagePlatformQuotaIntent) { i.RawCostUSD = "0.00000000497" }},
		{name: "cost", mutate: func(i *BatchImagePlatformQuotaIntent) { i.CostUSD = "0.0000000049" }},
		{name: "hash", mutate: func(i *BatchImagePlatformQuotaIntent) { i.IntentFingerprint = strings.Repeat("a", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := batchImagePlatformCommand(2, 0.00000000496)
			intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
			require.NoError(t, err)
			tc.mutate(intent)
			require.ErrorIs(t, ValidateBatchImagePlatformQuotaStoredIntent(intent, cmd.RequestFingerprint), ErrBatchImagePlatformQuotaIntent)
		})
	}
	cmd := batchImagePlatformCommand(2, 1.25)
	intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
	require.NoError(t, err)
	for _, billingFingerprint := range []string{"", strings.Repeat("a", 63), strings.Repeat("A", 64), strings.Repeat("g", 64), strings.Repeat("b", 64)} {
		require.ErrorIs(t, ValidateBatchImagePlatformQuotaStoredIntent(intent, billingFingerprint), ErrBatchImagePlatformQuotaIntent)
	}
	require.ErrorIs(t, ValidateBatchImagePlatformQuotaStoredIntent(nil, cmd.RequestFingerprint), ErrBatchImagePlatformQuotaIntent)
}

func TestBatchImagePlatformQuotaStoredIntentRejectsRehashedInvalidClosedFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*BatchImagePlatformQuotaIntent)
	}{
		{name: "version", mutate: func(i *BatchImagePlatformQuotaIntent) { i.Version = 2 }},
		{name: "zero-owner", mutate: func(i *BatchImagePlatformQuotaIntent) { i.UserID = 0 }},
		{name: "negative-owner", mutate: func(i *BatchImagePlatformQuotaIntent) { i.UserID = -1 }},
		{name: "zero-key", mutate: func(i *BatchImagePlatformQuotaIntent) { i.APIKeyID = 0 }},
		{name: "negative-key", mutate: func(i *BatchImagePlatformQuotaIntent) { i.APIKeyID = -1 }},
		{name: "hold-operation", mutate: func(i *BatchImagePlatformQuotaIntent) { i.RequestID = BatchImageHoldRequestID(i.BatchID) }},
		{name: "release-operation", mutate: func(i *BatchImagePlatformQuotaIntent) { i.RequestID = BatchImageReleaseRequestID(i.BatchID) }},
		{name: "different-batch-request", mutate: func(i *BatchImagePlatformQuotaIntent) { i.BatchID += "x" }},
		{name: "request-padding", mutate: func(i *BatchImagePlatformQuotaIntent) { i.RequestID += " " }},
		{name: "provider", mutate: func(i *BatchImagePlatformQuotaIntent) { i.Provider = "unknown" }},
		{name: "provider-padding", mutate: func(i *BatchImagePlatformQuotaIntent) { i.Provider += " " }},
		{name: "provider-platform", mutate: func(i *BatchImagePlatformQuotaIntent) { i.Platform = PlatformOpenAI }},
		{name: "missing-platform", mutate: func(i *BatchImagePlatformQuotaIntent) { i.Platform = "" }},
		{name: "cost-scale", mutate: func(i *BatchImagePlatformQuotaIntent) { i.CostUSD = "0.5" }},
		{name: "cost-overprecision", mutate: func(i *BatchImagePlatformQuotaIntent) { i.CostUSD = "0.50000000001" }},
		{name: "wrong-rounded-cost", mutate: func(i *BatchImagePlatformQuotaIntent) { i.CostUSD = "0.6000000000" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := batchImagePlatformCommand(2, 0.5)
			intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
			require.NoError(t, err)
			tc.mutate(intent)
			intent.IntentFingerprint = batchImagePlatformQuotaHash(cmd.RequestFingerprint, intent)
			require.ErrorIs(t, ValidateBatchImagePlatformQuotaStoredIntent(intent, cmd.RequestFingerprint), ErrBatchImagePlatformQuotaIntent)
		})
	}
	for _, batchID := range []string{"", " padded", "padded ", strings.Repeat("b", 65), string([]byte{0xff})} {
		cmd := batchImagePlatformCommand(2, 0.5)
		intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
		require.NoError(t, err)
		intent.BatchID, intent.RequestID = batchID, BatchImageCaptureRequestID(batchID)
		intent.IntentFingerprint = batchImagePlatformQuotaHash(cmd.RequestFingerprint, intent)
		require.ErrorIs(t, ValidateBatchImagePlatformQuotaStoredIntent(intent, cmd.RequestFingerprint), ErrBatchImagePlatformQuotaIntent)
	}
	for _, raw := range []string{"", "NaN", "Inf", "1e0", "-1", "01", "1.0", "0.10000000000000001", "10000000000", strings.Repeat("0", 513)} {
		for _, hold := range []bool{false, true} {
			cmd := batchImagePlatformCommand(2, 0.5)
			intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
			require.NoError(t, err)
			if hold {
				intent.RawHoldUSD = raw
			} else {
				intent.RawCostUSD = raw
				if value, err := decimal.NewFromString(raw); err == nil && len(raw) <= 512 {
					intent.CostUSD = value.Round(10).StringFixed(10)
				}
			}
			intent.IntentFingerprint = batchImagePlatformQuotaHash(cmd.RequestFingerprint, intent)
			require.ErrorIs(t, ValidateBatchImagePlatformQuotaStoredIntent(intent, cmd.RequestFingerprint), ErrBatchImagePlatformQuotaIntent)
		}
	}
}

func TestBatchImagePlatformQuotaStoredIntentDoesNotInventCaptureManifestOrCommandBinding(t *testing.T) {
	cmd := batchImagePlatformCommand(0.000078125, 0.00000000496)
	intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
	require.NoError(t, err)
	storedFingerprint := cmd.RequestFingerprint
	missingManifest := *cmd
	missingManifest.RequestPayloadHash, missingManifest.RequestFingerprint = "", ""
	missingManifest.Normalize()
	require.NotEqual(t, storedFingerprint, missingManifest.RequestFingerprint)
	require.NoError(t, ValidateBatchImagePlatformQuotaStoredIntent(intent, storedFingerprint),
		"a stored read validates the original private hash without inventing the removed manifest")
	require.ErrorIs(t, ValidateBatchImagePlatformQuotaIntent(&missingManifest, intent), ErrBatchImagePlatformQuotaIntent,
		"live commands must retain their actual manifest binding")
	// The old fixed-ten billing fingerprint collides for these two raw costs.
	// A valid new stored hash still does not replace the exact live raw binding.
	changedRaw := *intent
	changedRaw.RawCostUSD = "0.00000000497"
	changedRaw.IntentFingerprint = batchImagePlatformQuotaHash(storedFingerprint, &changedRaw)
	require.NoError(t, ValidateBatchImagePlatformQuotaStoredIntent(&changedRaw, storedFingerprint))
	require.ErrorIs(t, ValidateBatchImagePlatformQuotaIntent(cmd, &changedRaw), ErrBatchImagePlatformQuotaIntent)
	// Stored validity alone does not certify another record's owner/provider.
	// Repository readers must also bind the independently stored money proof.
	changedScope := *intent
	changedScope.UserID++
	changedScope.Provider = BatchImageProviderVertex
	changedScope.IntentFingerprint = batchImagePlatformQuotaHash(storedFingerprint, &changedScope)
	require.NoError(t, ValidateBatchImagePlatformQuotaStoredIntent(&changedScope, storedFingerprint))
	require.ErrorIs(t, ValidateBatchImagePlatformQuotaIntent(cmd, &changedScope), ErrBatchImagePlatformQuotaIntent)
}
