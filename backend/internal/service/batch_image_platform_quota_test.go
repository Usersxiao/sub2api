//go:build unit

package service

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func batchImagePlatformCommand(hold, cost float64) *BatchImageBalanceHoldCommand {
	cmd := &BatchImageBalanceHoldCommand{
		RequestID: BatchImageCaptureRequestID("imgbatch_intent"), BatchID: "imgbatch_intent",
		UserID: 7, APIKeyID: 8, HoldAmount: hold, ActualAmount: cost, RequestPayloadHash: "manifest-fixture",
	}
	cmd.Normalize()
	return cmd
}

func TestBatchImagePlatformQuotaIntentPreservesRawCaptureAmounts(t *testing.T) {
	for _, tc := range []struct {
		name         string
		hold, cost   float64
		platformCost string
	}{
		{name: "zero", platformCost: "0.0000000000"},
		{name: "normal", hold: 1.25, cost: 0.75, platformCost: "0.7500000000"},
		{name: "independent-raw-precision", hold: 0.000078125, cost: 0.00000000495, platformCost: "0.0000000050"},
		{name: "platform-half", hold: 1, cost: 0.00000000005, platformCost: "0.0000000001"},
		{name: "smallest-finite", hold: math.SmallestNonzeroFloat64, cost: math.SmallestNonzeroFloat64, platformCost: "0.0000000000"},
		{name: "largest-allowed", hold: 9999999999.999998, cost: 9999999999.999998, platformCost: "9999999999.9999980000"},
	} {
		for _, provider := range []string{BatchImageProviderGeminiAPI, BatchImageProviderVertex} {
			t.Run(tc.name+"/"+provider, func(t *testing.T) {
				cmd := batchImagePlatformCommand(tc.hold, tc.cost)
				before := *cmd
				intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, provider)
				require.NoError(t, err)
				require.Equal(t, before, *cmd, "capture money and its historical fingerprint remain unchanged")
				require.Equal(t, decimal.NewFromFloat(tc.hold).String(), intent.RawHoldUSD)
				require.Equal(t, decimal.NewFromFloat(tc.cost).String(), intent.RawCostUSD)
				require.Equal(t, tc.platformCost, intent.CostUSD)
				require.Equal(t, provider, intent.Provider)
				require.Equal(t, PlatformGemini, intent.Platform)
				require.NoError(t, ValidateBatchImagePlatformQuotaIntent(cmd, intent))
				second, err := FreezeBatchImagePlatformQuotaIntent(cmd, provider)
				require.NoError(t, err)
				require.Equal(t, intent, second)
			})
		}
	}
}

func TestBatchImagePlatformQuotaIntentKeepsHistoricalFingerprintAndPrivateJSON(t *testing.T) {
	cmd := batchImagePlatformCommand(0.000078125, 0.00000000496)
	const historicalFingerprint = "a803727536b78eb8f4fb8fa80d97c6d58a8b72a4ac97032f1464ea34df355e75"
	require.Equal(t, historicalFingerprint, cmd.RequestFingerprint)
	intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
	require.NoError(t, err)
	cmd.CapturePlatformQuota = intent
	cmd.Normalize()
	require.Equal(t, historicalFingerprint, cmd.RequestFingerprint)
	require.Equal(t, historicalFingerprint, buildBatchImageBalanceHoldFingerprint(cmd))
	encoded, err := json.Marshal(intent)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(encoded))
	encoded, err = json.Marshal(cmd)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "CapturePlatformQuota")
	require.NotContains(t, string(encoded), intent.IntentFingerprint)
	encoded, err = json.Marshal(&BatchImageBalanceHoldResult{PlatformCoverageMissing: true})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "PlatformCoverageMissing")

	// Raw amounts may collide in the old %.10f fingerprint and fixed10 cost;
	// the independent private proof still binds each original shortest decimal.
	other := batchImagePlatformCommand(0.000078125, 0.00000000497)
	require.Equal(t, cmd.RequestFingerprint, other.RequestFingerprint)
	otherIntent, err := FreezeBatchImagePlatformQuotaIntent(other, BatchImageProviderGeminiAPI)
	require.NoError(t, err)
	require.Equal(t, intent.CostUSD, otherIntent.CostUSD)
	require.NotEqual(t, intent.IntentFingerprint, otherIntent.IntentFingerprint)
	vertexIntent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderVertex)
	require.NoError(t, err)
	require.NotEqual(t, intent.IntentFingerprint, vertexIntent.IntentFingerprint)
}

func TestBatchImagePlatformQuotaIntentRejectsInvalidInputs(t *testing.T) {
	for _, amount := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), 1e10, math.MaxFloat64} {
		for _, hold := range []bool{false, true} {
			cmd := batchImagePlatformCommand(1, 0.5)
			if hold {
				cmd.HoldAmount = amount
			} else {
				cmd.ActualAmount = amount
			}
			_, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
			require.ErrorIs(t, err, ErrBatchImagePlatformQuotaIntent)
		}
	}
	for _, provider := range []string{"", "unknown", " gemini_api", "GEMINI_API", "gemini"} {
		_, err := FreezeBatchImagePlatformQuotaIntent(batchImagePlatformCommand(1, 0.5), provider)
		require.ErrorIs(t, err, ErrBatchImagePlatformQuotaIntent)
	}
	for _, batchID := range []string{"", " padded", "padded ", strings.Repeat("b", 65), string([]byte{0xff})} {
		cmd := batchImagePlatformCommand(1, 0.5)
		cmd.BatchID = batchID
		cmd.RequestID = BatchImageCaptureRequestID(batchID)
		_, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
		require.ErrorIs(t, err, ErrBatchImagePlatformQuotaIntent)
	}
	for _, mutate := range []func(*BatchImageBalanceHoldCommand){
		func(c *BatchImageBalanceHoldCommand) { c.APIKeyID = 0 },
		func(c *BatchImageBalanceHoldCommand) { c.UserID = 0 },
		func(c *BatchImageBalanceHoldCommand) { c.RequestID = BatchImageHoldRequestID(c.BatchID) },
		func(c *BatchImageBalanceHoldCommand) { c.RequestID = BatchImageReleaseRequestID(c.BatchID) },
		func(c *BatchImageBalanceHoldCommand) { c.RequestID += " " },
		func(c *BatchImageBalanceHoldCommand) { c.RequestFingerprint = "" },
		func(c *BatchImageBalanceHoldCommand) { c.RequestFingerprint = strings.Repeat("a", 64) },
		func(c *BatchImageBalanceHoldCommand) { c.ActualAmount = 0.6 },
	} {
		cmd := batchImagePlatformCommand(1, 0.5)
		mutate(cmd)
		_, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
		require.ErrorIs(t, err, ErrBatchImagePlatformQuotaIntent)
	}
	_, err := FreezeBatchImagePlatformQuotaIntent(nil, BatchImageProviderGeminiAPI)
	require.ErrorIs(t, err, ErrBatchImagePlatformQuotaIntent)
}

func TestBatchImagePlatformQuotaIntentRejectsTampering(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*BatchImageBalanceHoldCommand, *BatchImagePlatformQuotaIntent)
	}{
		{name: "version", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) { i.Version++ }},
		{name: "request", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) { i.RequestID += "x" }},
		{name: "batch", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) { i.BatchID += "x" }},
		{name: "key", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) { i.APIKeyID++ }},
		{name: "user", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) { i.UserID++ }},
		{name: "provider", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) {
			i.Provider = BatchImageProviderVertex
		}},
		{name: "unknown-provider", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) { i.Provider = "future" }},
		{name: "platform", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) { i.Platform = PlatformOpenAI }},
		{name: "hash", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) {
			i.IntentFingerprint = strings.Repeat("a", 64)
		}},
		{name: "hold", mutate: func(c *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) {
			i.RawHoldUSD = "2"
			i.IntentFingerprint = batchImagePlatformQuotaHash(c.RequestFingerprint, i)
		}},
		{name: "cost", mutate: func(c *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) {
			i.RawCostUSD = "0.6"
			i.CostUSD = "0.6000000000"
			i.IntentFingerprint = batchImagePlatformQuotaHash(c.RequestFingerprint, i)
		}},
		{name: "fixed-scale", mutate: func(c *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) {
			i.CostUSD = "0.5"
			i.IntentFingerprint = batchImagePlatformQuotaHash(c.RequestFingerprint, i)
		}},
		{name: "overprecision", mutate: func(_ *BatchImageBalanceHoldCommand, i *BatchImagePlatformQuotaIntent) { i.CostUSD = "0.50000000001" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := batchImagePlatformCommand(1, 0.5)
			intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
			require.NoError(t, err)
			tc.mutate(cmd, intent)
			require.ErrorIs(t, ValidateBatchImagePlatformQuotaIntent(cmd, intent), ErrBatchImagePlatformQuotaIntent)
		})
	}
	for _, raw := range []string{"", "NaN", "1e0", "-1", "01", "1.0", "10000000000", strings.Repeat("0", 513)} {
		for _, hold := range []bool{false, true} {
			cmd := batchImagePlatformCommand(1, 0.5)
			intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
			require.NoError(t, err)
			if hold {
				intent.RawHoldUSD = raw
			} else {
				intent.RawCostUSD = raw
			}
			intent.IntentFingerprint = batchImagePlatformQuotaHash(cmd.RequestFingerprint, intent)
			require.ErrorIs(t, ValidateBatchImagePlatformQuotaIntent(cmd, intent), ErrBatchImagePlatformQuotaIntent)
		}
	}
	cmd := batchImagePlatformCommand(1, 0.00000000496)
	intent, err := FreezeBatchImagePlatformQuotaIntent(cmd, BatchImageProviderGeminiAPI)
	require.NoError(t, err)
	intent.RawCostUSD = "0.00000000497"
	intent.IntentFingerprint = batchImagePlatformQuotaHash(cmd.RequestFingerprint, intent)
	require.ErrorIs(t, ValidateBatchImagePlatformQuotaIntent(cmd, intent), ErrBatchImagePlatformQuotaIntent, "same legacy fingerprint and fixed10 amount still require exact raw binding")
	require.ErrorIs(t, ValidateBatchImagePlatformQuotaIntent(nil, intent), ErrBatchImagePlatformQuotaIntent)
	require.ErrorIs(t, ValidateBatchImagePlatformQuotaIntent(cmd, nil), ErrBatchImagePlatformQuotaIntent)
}

func TestBatchImagePlatformQuotaIntentIsAttachedOnlyAtCapture(t *testing.T) {
	for _, provider := range []string{BatchImageProviderGeminiAPI, BatchImageProviderVertex} {
		job := testSettlingBatchImageJob("imgbatch_helpers_" + provider)
		job.Provider = provider
		billing := &fakeBatchImageBillingRepo{}
		require.NoError(t, reserveBatchImageBalanceHold(context.Background(), billing, job, "request-hash"))
		require.NoError(t, captureBatchImageBalanceHold(context.Background(), billing, job, 0, "manifest-hash"))
		require.NoError(t, releaseBatchImageBalanceHold(context.Background(), billing, job, "request-hash"))
		require.Len(t, billing.reserves, 1)
		require.Nil(t, billing.reserves[0].CapturePlatformQuota)
		require.Len(t, billing.releases, 1)
		require.Nil(t, billing.releases[0].CapturePlatformQuota)
		require.Len(t, billing.captures, 1)
		cmd := billing.captures[0]
		require.NotNil(t, cmd.CapturePlatformQuota)
		require.Equal(t, "0.0000000000", cmd.CapturePlatformQuota.CostUSD)
		require.Equal(t, provider, cmd.CapturePlatformQuota.Provider)
		require.Equal(t, "manifest-hash", cmd.RequestPayloadHash)
		require.NoError(t, ValidateBatchImagePlatformQuotaIntent(cmd, cmd.CapturePlatformQuota))
	}
	for _, tc := range []struct {
		name   string
		mutate func(*BatchImageJob)
		cost   float64
	}{
		{name: "unknown-provider", cost: 0.5, mutate: func(j *BatchImageJob) { j.Provider = "future" }},
		{name: "invalid-hold", cost: 0.5, mutate: func(j *BatchImageJob) { v := math.Inf(1); j.HoldAmount = &v }},
		{name: "invalid-cost", cost: math.NaN()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := testSettlingBatchImageJob("imgbatch_invalid_helper")
			if tc.mutate != nil {
				tc.mutate(job)
			}
			billing := &fakeBatchImageBillingRepo{}
			err := captureBatchImageBalanceHold(context.Background(), billing, job, tc.cost, "manifest-hash")
			require.ErrorIs(t, err, ErrBatchImageSettlementBillingFailed)
			require.ErrorIs(t, err, ErrBatchImagePlatformQuotaIntent)
			require.Empty(t, billing.captures)
		})
	}
}

func TestBatchImagePlatformQuotaIntentUsesRealSettlementPriceAndManifest(t *testing.T) {
	for _, snapshotVersion := range []int{0, 1} {
		job := testSettlingBatchImageJob("imgbatch_settlement_intent")
		job.Provider = BatchImageProviderVertex
		job.PricingSnapshotVersion = snapshotVersion
		job.BillableUnitPrice = 0.000078125
		job.SuccessCount, job.FailCount, job.ItemCount = 1, 0, 1
		repo := newFakeBatchImageRepository()
		repo.jobs[job.BatchID] = job
		billing := &fakeBatchImageBillingRepo{}
		svc := &BatchImageSettlementService{Repo: repo, BillingRepo: billing, Pricing: &fakeBatchImagePricingResolver{unitPrice: 0.00000000495}}
		result, err := svc.Settle(context.Background(), job.BatchID)
		require.NoError(t, err)
		require.Len(t, billing.captures, 1)
		cmd := billing.captures[0]
		require.NoError(t, ValidateBatchImagePlatformQuotaIntent(cmd, cmd.CapturePlatformQuota))
		require.Equal(t, result.ManifestHash, cmd.RequestPayloadHash)
		require.Equal(t, BuildBatchImageSettlementManifestHash(job), cmd.RequestPayloadHash)
		require.Equal(t, BatchImageProviderVertex, cmd.CapturePlatformQuota.Provider)
		if snapshotVersion == 1 {
			require.Equal(t, "0.000078125", cmd.CapturePlatformQuota.RawCostUSD)
			require.Equal(t, "0.0000781250", cmd.CapturePlatformQuota.CostUSD)
		} else {
			require.Equal(t, "0.00000000495", cmd.CapturePlatformQuota.RawCostUSD, "historical v0 still uses the live settlement resolver")
			require.Equal(t, "0.0000000050", cmd.CapturePlatformQuota.CostUSD)
		}
	}
}

type batchImagePlatformCoverageMissingRepo struct {
	fakeBatchImageBillingRepo
}

func (r *batchImagePlatformCoverageMissingRepo) CaptureBatchImageBalance(_ context.Context, cmd *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	r.captures = append(r.captures, cmd)
	return &BatchImageBalanceHoldResult{Applied: false, PlatformCoverageMissing: true}, nil
}

func TestBatchImagePlatformQuotaMissingHistoricalProofDoesNotFailSettlement(t *testing.T) {
	job := testSettlingBatchImageJob("imgbatch_historical_capture")
	job.RetryCount = batchImageSettlementMaxRetries - 1
	repo := newFakeBatchImageRepository()
	repo.jobs[job.BatchID] = job
	billing := &batchImagePlatformCoverageMissingRepo{}
	svc := &BatchImageSettlementService{Repo: repo, BillingRepo: billing, Pricing: &fakeBatchImagePricingResolver{unitPrice: 0.25}}
	_, err := svc.Settle(context.Background(), job.BatchID)
	require.NoError(t, err)
	require.Equal(t, BatchImageJobStatusCompleted, repo.jobs[job.BatchID].Status)
	require.Len(t, billing.captures, 1)
	require.Empty(t, billing.releases)
	// Completed legacy jobs retain the existing early return and never create
	// a new proof from live prices or a second capture attempt.
	_, err = svc.Settle(context.Background(), job.BatchID)
	require.NoError(t, err)
	require.Len(t, billing.captures, 1)
}

func TestBatchImagePlatformQuotaCaptureConflictDoesNotExhaustOrRelease(t *testing.T) {
	job := testSettlingBatchImageJob("imgbatch_capture_conflict")
	job.RetryCount = batchImageSettlementMaxRetries - 1
	repo := newFakeBatchImageRepository()
	repo.jobs[job.BatchID] = job
	billing := &fakeBatchImageBillingRepo{captureErr: ErrUsageBillingRequestConflict}
	svc := &BatchImageSettlementService{Repo: repo, BillingRepo: billing, Pricing: &fakeBatchImagePricingResolver{unitPrice: 0.25}}
	for attempt := 0; attempt < 2; attempt++ {
		_, err := svc.Settle(context.Background(), job.BatchID)
		require.ErrorIs(t, err, ErrUsageBillingRequestConflict)
		require.Equal(t, BatchImageJobStatusSettling, repo.jobs[job.BatchID].Status)
		require.Equal(t, batchImageSettlementMaxRetries-1, repo.jobs[job.BatchID].RetryCount)
		require.Nil(t, repo.jobs[job.BatchID].LastErrorCode)
		require.Empty(t, billing.releases)
	}
	job = testSettlingBatchImageJob("imgbatch_release_after_capture")
	billing = &fakeBatchImageBillingRepo{releaseErr: ErrBatchImageCaptureAlreadyApplied}
	err := releaseBatchImageBalanceHold(context.Background(), billing, job, "request-hash")
	require.ErrorIs(t, err, ErrBatchImageCaptureAlreadyApplied)
	require.NotErrorIs(t, err, ErrUsageBillingRequestConflict, "the historical release compatibility branch must not consume this proof")
}
