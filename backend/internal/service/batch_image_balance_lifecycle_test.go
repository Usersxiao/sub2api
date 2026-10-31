//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func batchLifecycleCommand(operation string, hold, actual float64) *BatchImageBalanceHoldCommand {
	cmd := &BatchImageBalanceHoldCommand{UserID: 7, APIKeyID: 8, BatchID: "imgbatch_lifecycle",
		HoldAmount: hold, ActualAmount: actual, RequestPayloadHash: "operation-payload"}
	switch operation {
	case "reserve":
		cmd.RequestID = BatchImageHoldRequestID(cmd.BatchID)
	case "capture":
		cmd.RequestID = BatchImageCaptureRequestID(cmd.BatchID)
	case "release":
		cmd.RequestID = BatchImageReleaseRequestID(cmd.BatchID)
	}
	cmd.Normalize()
	return cmd
}

func TestBatchImageBalanceLifecycleProofClosedOutcomeMatrix(t *testing.T) {
	for _, tc := range []struct {
		operation, outcome, origin string
		hold, actual               float64
	}{
		{"reserve", "reserved", "observed_reserve", 1, 0},
		{"reserve", "zero_hold", "none", 0, 0},
		{"capture", "captured", "observed_reserve", 1, 0.75},
		{"capture", "captured", "legacy_dedup", 1, 0},
		{"capture", "captured", "none", 0, 0},
		{"release", "refunded", "observed_reserve", 1, 0},
		{"release", "refunded", "legacy_dedup", 1, 0},
		{"release", "no_hold_terminal", "none", 1, 0},
		{"release", "zero_hold", "none", 0, 0},
	} {
		t.Run(tc.operation+"/"+tc.outcome+"/"+tc.origin, func(t *testing.T) {
			cmd := batchLifecycleCommand(tc.operation, tc.hold, tc.actual)
			reserveFP := ""
			if tc.origin != "none" {
				reserveFP = strings.Repeat("a", 64)
				if tc.operation == "reserve" {
					reserveFP = cmd.RequestFingerprint
				}
				cmd.ExpectedReserveFingerprint = reserveFP
			}
			before := *cmd
			proof, err := FreezeBatchImageBalanceLifecycleProof(cmd, tc.operation, tc.outcome, tc.origin, reserveFP)
			require.NoError(t, err)
			require.Equal(t, before, *cmd)
			require.Equal(t, reserveFP, proof.ReserveFingerprint)
			require.NoError(t, ValidateBatchImageBalanceLifecycleProof(cmd, proof))
			require.NoError(t, ValidateBatchImageBalanceLifecycleStoredProof(proof))
			second, err := FreezeBatchImageBalanceLifecycleProof(cmd, tc.operation, tc.outcome, tc.origin, reserveFP)
			require.NoError(t, err)
			require.Equal(t, proof, second)
		})
	}
	// Every other matrix combination is rejected, including zero-hold capture
	// with positive cost and no reservation evidence.
	for _, operation := range []string{"reserve", "capture", "release", "unknown", " capture"} {
		for _, outcome := range []string{"reserved", "captured", "refunded", "no_hold_terminal", "zero_hold", "unknown"} {
			for _, origin := range []string{"observed_reserve", "legacy_dedup", "none", "unknown"} {
				cmd := batchLifecycleCommand(operation, 1, 0)
				reserveFP := ""
				if origin != "none" {
					reserveFP = strings.Repeat("a", 64)
				}
				if operation == "reserve" && origin == "observed_reserve" {
					reserveFP = cmd.RequestFingerprint
				}
				allowed := (operation == "reserve" && outcome == "reserved" && origin == "observed_reserve") ||
					(operation == "capture" && outcome == "captured" && (origin == "observed_reserve" || origin == "legacy_dedup")) ||
					(operation == "release" && outcome == "refunded" && (origin == "observed_reserve" || origin == "legacy_dedup")) ||
					(operation == "release" && outcome == "no_hold_terminal" && origin == "none")
				_, err := FreezeBatchImageBalanceLifecycleProof(cmd, operation, outcome, origin, reserveFP)
				if allowed {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, ErrBatchImageBalanceLifecycleInput)
				}
			}
		}
	}
	_, err := FreezeBatchImageBalanceLifecycleProof(batchLifecycleCommand("capture", 0, 0.000000001), "capture", "captured", "none", "")
	require.ErrorIs(t, err, ErrBatchImageBalanceLifecycleInput)
	for _, operation := range []string{"reserve", "release"} {
		_, err := FreezeBatchImageBalanceLifecycleProof(batchLifecycleCommand(operation, 1, 0.1), operation, "zero_hold", "none", "")
		require.ErrorIs(t, err, ErrBatchImageBalanceLifecycleInput)
	}
}

func TestBatchImageBalanceLifecycleProofPreservesRawAndHistoricalFingerprint(t *testing.T) {
	for _, amount := range []float64{0, math.SmallestNonzeroFloat64, 0.00000000496, 0.000078125, 9999999999.999998} {
		cmd := batchLifecycleCommand("capture", amount, amount)
		origin, reserveFP := "observed_reserve", strings.Repeat("a", 64)
		if amount == 0 {
			origin, reserveFP = "none", ""
		}
		before := *cmd
		proof, err := FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", origin, reserveFP)
		require.NoError(t, err)
		require.Equal(t, before, *cmd)
		require.Equal(t, decimal.NewFromFloat(amount).String(), proof.RawHoldUSD)
		require.Equal(t, decimal.NewFromFloat(amount).String(), proof.RawActualUSD)
		require.Equal(t, cmd.RequestFingerprint, proof.BillingFingerprint)
		require.Equal(t, before.RequestFingerprint, buildBatchImageBalanceHoldFingerprint(cmd))
	}
	a := batchLifecycleCommand("capture", 1, 0.00000000496)
	b := batchLifecycleCommand("capture", 1, 0.00000000497)
	require.Equal(t, a.RequestFingerprint, b.RequestFingerprint)
	pa, err := FreezeBatchImageBalanceLifecycleProof(a, "capture", "captured", "observed_reserve", strings.Repeat("a", 64))
	require.NoError(t, err)
	pb, err := FreezeBatchImageBalanceLifecycleProof(b, "capture", "captured", "observed_reserve", strings.Repeat("a", 64))
	require.NoError(t, err)
	require.NotEqual(t, pa.ProofFingerprint, pb.ProofFingerprint, "proof binds raw values hidden by the historical fmt10 fingerprint")
}

func TestBatchImageBalanceLifecycleProofRejectsInvalidCurrentInputs(t *testing.T) {
	for _, amount := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), 1e10, math.MaxFloat64} {
		for _, hold := range []bool{false, true} {
			cmd := batchLifecycleCommand("capture", 1, 0.5)
			if hold {
				cmd.HoldAmount = amount
			} else {
				cmd.ActualAmount = amount
			}
			_, err := FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", "observed_reserve", strings.Repeat("a", 64))
			require.ErrorIs(t, err, ErrBatchImageBalanceLifecycleInput)
		}
	}
	for _, mutate := range []func(*BatchImageBalanceHoldCommand){
		func(c *BatchImageBalanceHoldCommand) { c.UserID = 0 },
		func(c *BatchImageBalanceHoldCommand) { c.APIKeyID = 0 },
		func(c *BatchImageBalanceHoldCommand) { c.BatchID = "" },
		func(c *BatchImageBalanceHoldCommand) { c.BatchID = " padded" },
		func(c *BatchImageBalanceHoldCommand) { c.BatchID = strings.Repeat("b", 65) },
		func(c *BatchImageBalanceHoldCommand) { c.BatchID = string([]byte{0xff}) },
		func(c *BatchImageBalanceHoldCommand) { c.RequestID += " " },
		func(c *BatchImageBalanceHoldCommand) { c.RequestID = BatchImageReleaseRequestID(c.BatchID) },
		func(c *BatchImageBalanceHoldCommand) { c.RequestFingerprint = "" },
		func(c *BatchImageBalanceHoldCommand) { c.RequestFingerprint = strings.Repeat("a", 64) },
		func(c *BatchImageBalanceHoldCommand) { c.RequestPayloadHash += "changed" },
		func(c *BatchImageBalanceHoldCommand) { c.ExpectedReserveFingerprint = "invalid" },
	} {
		cmd := batchLifecycleCommand("capture", 1, 0.5)
		mutate(cmd)
		_, err := FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", "observed_reserve", strings.Repeat("a", 64))
		require.ErrorIs(t, err, ErrBatchImageBalanceLifecycleInput)
	}
	_, err := FreezeBatchImageBalanceLifecycleProof(nil, "reserve", "reserved", "observed_reserve", "")
	require.ErrorIs(t, err, ErrBatchImageBalanceLifecycleInput)
}

func TestBatchImageBalanceLifecycleProofRejectsTamperingAndChecksStoredEvidence(t *testing.T) {
	for _, mutate := range []func(*BatchImageBalanceLifecycleProof){
		func(p *BatchImageBalanceLifecycleProof) { p.Version++ },
		func(p *BatchImageBalanceLifecycleProof) { p.Operation = "release" },
		func(p *BatchImageBalanceLifecycleProof) { p.RequestID += "other" },
		func(p *BatchImageBalanceLifecycleProof) { p.BatchID += "other" },
		func(p *BatchImageBalanceLifecycleProof) { p.APIKeyID++ },
		func(p *BatchImageBalanceLifecycleProof) { p.UserID++ },
		func(p *BatchImageBalanceLifecycleProof) { p.RawHoldUSD = "2" },
		func(p *BatchImageBalanceLifecycleProof) { p.RawActualUSD = "0.6" },
		func(p *BatchImageBalanceLifecycleProof) { p.Outcome = "refunded" },
		func(p *BatchImageBalanceLifecycleProof) { p.ReserveOrigin = "legacy_dedup" },
		func(p *BatchImageBalanceLifecycleProof) { p.ReserveFingerprint = strings.Repeat("b", 64) },
		func(p *BatchImageBalanceLifecycleProof) { p.BillingFingerprint = strings.Repeat("b", 64) },
		func(p *BatchImageBalanceLifecycleProof) { p.ProofFingerprint = strings.Repeat("b", 64) },
	} {
		cmd := batchLifecycleCommand("capture", 1, 0.5)
		proof, err := FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", "observed_reserve", strings.Repeat("a", 64))
		require.NoError(t, err)
		mutate(proof)
		require.ErrorIs(t, ValidateBatchImageBalanceLifecycleProof(cmd, proof), ErrBatchImageBalanceLifecycleInput)
		require.ErrorIs(t, ValidateBatchImageBalanceLifecycleStoredProof(proof), ErrBatchImageBalanceLifecycleInput)
	}
	for _, raw := range []string{"", "1.0", "01", "-1", "NaN", "1e-3", "10000000000", strings.Repeat("0", 513), "0.123456789123456789"} {
		for _, hold := range []bool{false, true} {
			cmd := batchLifecycleCommand("capture", 1, 0.5)
			proof, err := FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", "observed_reserve", strings.Repeat("a", 64))
			require.NoError(t, err)
			if hold {
				proof.RawHoldUSD = raw
			} else {
				proof.RawActualUSD = raw
			}
			proof.ProofFingerprint = batchImageBalanceLifecycleHash(proof)
			require.ErrorIs(t, ValidateBatchImageBalanceLifecycleProof(cmd, proof), ErrBatchImageBalanceLifecycleInput)
			require.ErrorIs(t, ValidateBatchImageBalanceLifecycleStoredProof(proof), ErrBatchImageBalanceLifecycleInput)
		}
	}
	cmd := batchLifecycleCommand("capture", 1, 0.5)
	proof, err := FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", "observed_reserve", strings.Repeat("a", 64))
	require.NoError(t, err)
	cmd.RequestPayloadHash = "a different current payload"
	require.NoError(t, ValidateBatchImageBalanceLifecycleStoredProof(proof), "stored hash validates without reconstructing an unknown original payload")
	require.ErrorIs(t, ValidateBatchImageBalanceLifecycleProof(cmd, proof), ErrBatchImageBalanceLifecycleInput)
	require.ErrorIs(t, ValidateBatchImageBalanceLifecycleStoredProof(nil), ErrBatchImageBalanceLifecycleInput)
	require.ErrorIs(t, ValidateBatchImageBalanceLifecycleProof(nil, proof), ErrBatchImageBalanceLifecycleInput)
	require.ErrorIs(t, ValidateBatchImageBalanceLifecycleProof(cmd, nil), ErrBatchImageBalanceLifecycleInput)
}

func TestBatchImageBalanceLifecycleReservationReferenceIsBound(t *testing.T) {
	for _, origin := range []string{"observed_reserve", "legacy_dedup"} {
		cmd := batchLifecycleCommand("capture", 1, 0.5)
		cmd.ExpectedReserveFingerprint = strings.Repeat("a", 64)
		proof, err := FreezeBatchImageBalanceLifecycleProof(cmd, "capture", "captured", origin, cmd.ExpectedReserveFingerprint)
		require.NoError(t, err)
		proof.ReserveFingerprint = strings.Repeat("b", 64)
		proof.ProofFingerprint = batchImageBalanceLifecycleHash(proof)
		require.NoError(t, ValidateBatchImageBalanceLifecycleStoredProof(proof), "the codec checks evidence binding; the repository validates the referenced row")
		require.ErrorIs(t, ValidateBatchImageBalanceLifecycleProof(cmd, proof), ErrBatchImageBalanceLifecycleInput)
	}
	cmd := batchLifecycleCommand("reserve", 1, 0)
	_, err := FreezeBatchImageBalanceLifecycleProof(cmd, "reserve", "reserved", "observed_reserve", strings.Repeat("a", 64))
	require.ErrorIs(t, err, ErrBatchImageBalanceLifecycleInput, "an observed reserve references its own historical billing fingerprint")
	for _, operation := range []string{"capture", "release"} {
		cmd := batchLifecycleCommand(operation, 0, 0)
		outcome := "captured"
		if operation == "release" {
			outcome = "zero_hold"
		}
		_, err := FreezeBatchImageBalanceLifecycleProof(cmd, operation, outcome, "none", strings.Repeat("a", 64))
		require.ErrorIs(t, err, ErrBatchImageBalanceLifecycleInput)
	}
}

func TestBatchImageBalanceLifecycleHintUsesOriginalSubmissionAndPrivateJSON(t *testing.T) {
	job := testSettlingBatchImageJob("imgbatch_original_reserve")
	job.RequestHash = batchImageStringPtr(" original-submission-hash ")
	for _, requestID := range []string{BatchImageHoldRequestID(job.BatchID), BatchImageCaptureRequestID(job.BatchID), BatchImageReleaseRequestID(job.BatchID)} {
		cmd, err := buildBatchImageHoldCommand(job, requestID, 0.5, " settlement-manifest ")
		require.NoError(t, err)
		reserve := &BatchImageBalanceHoldCommand{RequestID: BatchImageHoldRequestID(job.BatchID), APIKeyID: *job.APIKeyID,
			UserID: job.UserID, BatchID: job.BatchID, HoldAmount: *job.HoldAmount, RequestPayloadHash: "original-submission-hash"}
		reserve.Normalize()
		require.Equal(t, reserve.RequestFingerprint, cmd.ExpectedReserveFingerprint)
		require.Equal(t, "settlement-manifest", cmd.RequestPayloadHash)
		cmd.Normalize()
		before := cmd.RequestFingerprint
		cmd.ExpectedReserveFingerprint = strings.Repeat("b", 64)
		require.Equal(t, before, buildBatchImageBalanceHoldFingerprint(cmd))
		encoded, err := json.Marshal(cmd)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "ExpectedReserveFingerprint")
	}
	cmd := batchLifecycleCommand("reserve", 1, 0)
	proof, err := FreezeBatchImageBalanceLifecycleProof(cmd, "reserve", "reserved", "observed_reserve", cmd.RequestFingerprint)
	require.NoError(t, err)
	encoded, err := json.Marshal(proof)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(encoded))
	encoded, err = json.Marshal(&BatchImageBalanceHoldResult{Applied: false, LifecycleCoverageMissing: true})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "LifecycleCoverageMissing")
	require.Contains(t, string(encoded), `"Applied":false`)
}

type batchLifecycleMissingBillingRepo struct{ fakeBatchImageBillingRepo }

func (r *batchLifecycleMissingBillingRepo) CaptureBatchImageBalance(_ context.Context, cmd *BatchImageBalanceHoldCommand) (*BatchImageBalanceHoldResult, error) {
	r.captures = append(r.captures, cmd)
	return &BatchImageBalanceHoldResult{Applied: false, LifecycleCoverageMissing: true, PlatformCoverageMissing: true}, nil
}

func TestBatchImageBalanceLifecycleHistoricalFalseResultStillCompletes(t *testing.T) {
	job := testSettlingBatchImageJob("imgbatch_missing_lifecycle")
	job.RetryCount = batchImageSettlementMaxRetries - 1
	repo := newFakeBatchImageRepository()
	repo.jobs[job.BatchID] = job
	billing := &batchLifecycleMissingBillingRepo{}
	svc := &BatchImageSettlementService{Repo: repo, BillingRepo: billing, Pricing: &fakeBatchImagePricingResolver{unitPrice: 0.25}}
	_, err := svc.Settle(context.Background(), job.BatchID)
	require.NoError(t, err)
	require.Equal(t, BatchImageJobStatusCompleted, repo.jobs[job.BatchID].Status)
	require.Len(t, billing.captures, 1)
	require.Empty(t, billing.releases)
}

func TestBatchImageBalanceLifecycleErrorsNeverExhaustOrPassReleaseCompatibility(t *testing.T) {
	for _, sentinel := range []error{ErrBatchImageBalanceLifecycleConflict, ErrBatchImageBalanceLifecycleHistory, ErrBatchImageBalanceLifecycleInput} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			job := testSettlingBatchImageJob("imgbatch_lifecycle_conflict")
			job.RetryCount = batchImageSettlementMaxRetries - 1
			repo := newFakeBatchImageRepository()
			repo.jobs[job.BatchID] = job
			billing := &fakeBatchImageBillingRepo{captureErr: fmt.Errorf("wrapped: %w", sentinel)}
			svc := &BatchImageSettlementService{Repo: repo, BillingRepo: billing, Pricing: &fakeBatchImagePricingResolver{unitPrice: 0.25}}
			for attempt := 0; attempt < 2; attempt++ {
				_, err := svc.Settle(context.Background(), job.BatchID)
				require.ErrorIs(t, err, sentinel)
				require.NotErrorIs(t, err, ErrUsageBillingRequestConflict)
				require.Equal(t, batchImageSettlementMaxRetries-1, repo.jobs[job.BatchID].RetryCount)
				require.Nil(t, repo.jobs[job.BatchID].LastErrorCode)
				require.Equal(t, BatchImageJobStatusSettling, repo.jobs[job.BatchID].Status)
				require.Empty(t, billing.releases)
			}
			billing = &fakeBatchImageBillingRepo{releaseErr: fmt.Errorf("wrapped: %w", sentinel)}
			err := releaseBatchImageBalanceHold(context.Background(), billing, job, "submission-hash")
			require.ErrorIs(t, err, sentinel)
			require.NotErrorIs(t, err, ErrUsageBillingRequestConflict)
		})
	}
}
