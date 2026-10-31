//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func batchImageNativeBirthJob() *BatchImageJob {
	keyID := int64(8)
	hold := 1.25
	return &BatchImageJob{ID: 51, BatchID: "imgbatch_native_birth", UserID: 7, APIKeyID: &keyID,
		Provider: BatchImageProviderGeminiAPI, Model: "gemini-image", Status: BatchImageJobStatusCreated,
		Currency: "USD", EstimatedCost: 9, HoldAmount: &hold, RequestHash: batchImageStringPtr(strings.Repeat("a", 64))}
}

func TestBatchImageNativeBirthFreezesReturnedJobAndPreservesHold(t *testing.T) {
	for _, provider := range []string{BatchImageProviderGeminiAPI, BatchImageProviderVertex} {
		for _, amount := range []float64{0, 0.0000000001, 0.000078125, 1.25, 9999999999.999998} {
			job := batchImageNativeBirthJob()
			job.Provider, job.HoldAmount = provider, &amount
			before := *job
			birth, err := FreezeBatchImageNativeBirth(job)
			require.NoError(t, err)
			require.Equal(t, before, *job)
			require.Equal(t, decimal.NewFromFloat(amount).String(), birth.RawHoldUSD)
			require.Equal(t, job.ID, birth.JobID)
			require.Equal(t, job.UserID, birth.UserID)
			require.Equal(t, *job.APIKeyID, birth.APIKeyID)
			require.Equal(t, provider, birth.Provider)
			require.Equal(t, job.Model, birth.Model)
			reserve, err := buildBatchImageHoldCommand(job, BatchImageHoldRequestID(job.BatchID), 0, *job.RequestHash)
			require.NoError(t, err)
			reserve.Normalize()
			require.Equal(t, reserve.RequestFingerprint, birth.ReserveFingerprint)
			require.NoError(t, ValidateBatchImageNativeBirthStoredProof(birth))
			require.NoError(t, ValidateBatchImageNativeBirthCommand(reserve, birth))
			second, err := FreezeBatchImageNativeBirth(job)
			require.NoError(t, err)
			require.Equal(t, birth, second)
		}
	}
	job := batchImageNativeBirthJob()
	job.HoldAmount, job.EstimatedCost = nil, 0.75
	birth, err := FreezeBatchImageNativeBirth(job)
	require.NoError(t, err)
	require.Equal(t, "0.75", birth.RawHoldUSD)
	encoded, err := json.Marshal(birth)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(encoded))
}

func TestBatchImageNativeBirthRejectsMalformedOrPreviouslyExecutedJobs(t *testing.T) {
	for _, mutate := range []func(*BatchImageJob){
		func(j *BatchImageJob) { j.ID = 0 },
		func(j *BatchImageJob) { j.UserID = 0 },
		func(j *BatchImageJob) { j.APIKeyID = nil },
		func(j *BatchImageJob) { v := int64(0); j.APIKeyID = &v },
		func(j *BatchImageJob) { j.BatchID = "" },
		func(j *BatchImageJob) { j.BatchID = " padded" },
		func(j *BatchImageJob) { j.BatchID = strings.Repeat("b", 65) },
		func(j *BatchImageJob) { j.BatchID = string([]byte{0xff}) },
		func(j *BatchImageJob) { j.Status = BatchImageJobStatusSubmitted },
		func(j *BatchImageJob) { j.Currency = "" },
		func(j *BatchImageJob) { j.Currency = "usd" },
		func(j *BatchImageJob) { j.Provider = "unknown" },
		func(j *BatchImageJob) { j.Provider = " gemini_api" },
		func(j *BatchImageJob) { j.Model = "" },
		func(j *BatchImageJob) { j.Model = " model" },
		func(j *BatchImageJob) { j.Model = strings.Repeat("m", 129) },
		func(j *BatchImageJob) { j.Model = string([]byte{0xff}) },
		func(j *BatchImageJob) { j.RequestHash = nil },
		func(j *BatchImageJob) { j.RequestHash = batchImageStringPtr("unbound") },
		func(j *BatchImageJob) { j.RequestHash = batchImageStringPtr(strings.Repeat("A", 64)) },
		func(j *BatchImageJob) { j.ProviderJobName = batchImageStringPtr("providers/already-started") },
		func(j *BatchImageJob) { j.ProviderInputRef = batchImageStringPtr("files/input") },
		func(j *BatchImageJob) { j.ProviderOutputRef = batchImageStringPtr("files/output") },
		func(j *BatchImageJob) { j.GCSInputURI = batchImageStringPtr("gs://input") },
		func(j *BatchImageJob) { j.GCSOutputURI = batchImageStringPtr("gs://output") },
		func(j *BatchImageJob) { v := 0.0; j.ActualCost = &v },
		func(j *BatchImageJob) { v := time.Now(); j.SubmittedAt = &v },
		func(j *BatchImageJob) { v := time.Now(); j.StartedAt = &v },
		func(j *BatchImageJob) { v := time.Now(); j.FinishedAt = &v },
		func(j *BatchImageJob) { v := time.Now(); j.SettledAt = &v },
	} {
		job := batchImageNativeBirthJob()
		mutate(job)
		_, err := FreezeBatchImageNativeBirth(job)
		require.ErrorIs(t, err, ErrBatchImageNativeBirthInput)
	}
	for _, amount := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), 1e10, math.MaxFloat64} {
		for _, fallback := range []bool{false, true} {
			job := batchImageNativeBirthJob()
			job.HoldAmount = &amount
			if fallback {
				job.HoldAmount, job.EstimatedCost = nil, amount
			}
			_, err := FreezeBatchImageNativeBirth(job)
			require.ErrorIs(t, err, ErrBatchImageNativeBirthInput, "malformed money never becomes a zero hold")
		}
	}
	_, err := FreezeBatchImageNativeBirth(nil)
	require.ErrorIs(t, err, ErrBatchImageNativeBirthInput)
}

func TestBatchImageNativeBirthStoredProofBindsEveryIdentityAndReserveInput(t *testing.T) {
	for _, mutate := range []func(*BatchImageNativeBirth){
		func(b *BatchImageNativeBirth) { b.Version++ },
		func(b *BatchImageNativeBirth) { b.JobID++ },
		func(b *BatchImageNativeBirth) { b.BatchID += "other" },
		func(b *BatchImageNativeBirth) { b.UserID++ },
		func(b *BatchImageNativeBirth) { b.APIKeyID++ },
		func(b *BatchImageNativeBirth) { b.Provider = BatchImageProviderVertex },
		func(b *BatchImageNativeBirth) { b.Model += "other" },
		func(b *BatchImageNativeBirth) { b.RequestHash = strings.Repeat("b", 64) },
		func(b *BatchImageNativeBirth) { b.RawHoldUSD = "2" },
		func(b *BatchImageNativeBirth) { b.ReserveFingerprint = strings.Repeat("b", 64) },
		func(b *BatchImageNativeBirth) { b.ProofFingerprint = strings.Repeat("b", 64) },
	} {
		birth, err := FreezeBatchImageNativeBirth(batchImageNativeBirthJob())
		require.NoError(t, err)
		mutate(birth)
		require.ErrorIs(t, ValidateBatchImageNativeBirthStoredProof(birth), ErrBatchImageNativeBirthInput)
	}
	for _, raw := range []string{"", "01", "1.250", "NaN", "-1", "1e0", "10000000000", strings.Repeat("0", 513), "0.123456789123456789"} {
		birth, err := FreezeBatchImageNativeBirth(batchImageNativeBirthJob())
		require.NoError(t, err)
		birth.RawHoldUSD = raw
		birth.ProofFingerprint = batchImageNativeBirthHash(birth)
		require.ErrorIs(t, ValidateBatchImageNativeBirthStoredProof(birth), ErrBatchImageNativeBirthInput)
	}
	birth, err := FreezeBatchImageNativeBirth(batchImageNativeBirthJob())
	require.NoError(t, err)
	birth.ReserveFingerprint = strings.Repeat("b", 64)
	birth.ProofFingerprint = batchImageNativeBirthHash(birth)
	require.ErrorIs(t, ValidateBatchImageNativeBirthStoredProof(birth), ErrBatchImageNativeBirthInput, "private hash alone cannot replace the original reconstructed reserve fingerprint")
	require.ErrorIs(t, ValidateBatchImageNativeBirthStoredProof(nil), ErrBatchImageNativeBirthInput)
}

func TestBatchImageNativeBirthAllowsCanonicalCommandsAndDistinctCaptureManifest(t *testing.T) {
	job := batchImageNativeBirthJob()
	birth, err := FreezeBatchImageNativeBirth(job)
	require.NoError(t, err)
	for _, requestID := range []string{BatchImageHoldRequestID(job.BatchID), BatchImageCaptureRequestID(job.BatchID), BatchImageReleaseRequestID(job.BatchID)} {
		actual, payload := 0.0, *job.RequestHash
		if requestID == BatchImageCaptureRequestID(job.BatchID) {
			actual, payload = 0.75, "settlement-manifest"
		}
		cmd, err := buildBatchImageHoldCommand(job, requestID, actual, payload)
		require.NoError(t, err)
		cmd.Normalize()
		if actual > 0 {
			cmd.CapturePlatformQuota, err = FreezeBatchImagePlatformQuotaIntent(cmd, job.Provider)
			require.NoError(t, err)
			require.NotEqual(t, birth.ReserveFingerprint, cmd.RequestFingerprint)
		}
		before := *cmd
		require.NoError(t, ValidateBatchImageNativeBirthCommand(cmd, birth))
		require.Equal(t, before, *cmd)
		cmd.ExpectedReserveFingerprint = ""
		require.NoError(t, ValidateBatchImageNativeBirthCommand(cmd, birth), "absence of a hint does not replace durable evidence resolution in the repository")
	}
}

func TestBatchImageNativeBirthCommandRejectsScopeHoldHintAndProviderDrift(t *testing.T) {
	for _, mutate := range []func(*BatchImageBalanceHoldCommand){
		func(c *BatchImageBalanceHoldCommand) { c.UserID++ },
		func(c *BatchImageBalanceHoldCommand) { c.APIKeyID++ },
		func(c *BatchImageBalanceHoldCommand) {
			c.BatchID += "other"
			c.RequestID = BatchImageCaptureRequestID(c.BatchID)
		},
		func(c *BatchImageBalanceHoldCommand) { c.HoldAmount += 0.1 },
		func(c *BatchImageBalanceHoldCommand) { c.ExpectedReserveFingerprint = strings.Repeat("b", 64) },
		func(c *BatchImageBalanceHoldCommand) { c.CapturePlatformQuota.Provider = BatchImageProviderVertex },
	} {
		job := batchImageNativeBirthJob()
		birth, err := FreezeBatchImageNativeBirth(job)
		require.NoError(t, err)
		cmd, err := buildBatchImageHoldCommand(job, BatchImageCaptureRequestID(job.BatchID), 0.75, "manifest")
		require.NoError(t, err)
		cmd.Normalize()
		cmd.CapturePlatformQuota, err = FreezeBatchImagePlatformQuotaIntent(cmd, job.Provider)
		require.NoError(t, err)
		mutate(cmd)
		cmd.RequestFingerprint = ""
		cmd.Normalize()
		require.ErrorIs(t, ValidateBatchImageNativeBirthCommand(cmd, birth), ErrBatchImageNativeBirthConflict)
	}
	job := batchImageNativeBirthJob()
	hold := 0.00000000496
	job.HoldAmount = &hold
	birth, err := FreezeBatchImageNativeBirth(job)
	require.NoError(t, err)
	cmd, err := buildBatchImageHoldCommand(job, BatchImageHoldRequestID(job.BatchID), 0, *job.RequestHash)
	require.NoError(t, err)
	cmd.HoldAmount = 0.00000000497
	cmd.Normalize()
	require.Equal(t, birth.ReserveFingerprint, cmd.RequestFingerprint)
	require.ErrorIs(t, ValidateBatchImageNativeBirthCommand(cmd, birth), ErrBatchImageNativeBirthConflict, "raw hold remains exact when old fmt10 fingerprints collide")
	cmd, err = buildBatchImageHoldCommand(job, BatchImageHoldRequestID(job.BatchID), 0, "other-submission")
	require.NoError(t, err)
	cmd.Normalize()
	require.ErrorIs(t, ValidateBatchImageNativeBirthCommand(cmd, birth), ErrBatchImageNativeBirthConflict)
	for _, mutate := range []func(*BatchImageBalanceHoldCommand){
		func(c *BatchImageBalanceHoldCommand) { c.RequestID = "other-operation" },
		func(c *BatchImageBalanceHoldCommand) { c.RequestFingerprint = "stale" },
		func(c *BatchImageBalanceHoldCommand) { c.ExpectedReserveFingerprint = "invalid" },
		func(c *BatchImageBalanceHoldCommand) { c.HoldAmount = math.NaN() },
		func(c *BatchImageBalanceHoldCommand) { c.ActualAmount = math.Inf(1) },
	} {
		cmd, err := buildBatchImageHoldCommand(job, BatchImageCaptureRequestID(job.BatchID), 0, "manifest")
		require.NoError(t, err)
		cmd.Normalize()
		mutate(cmd)
		require.ErrorIs(t, ValidateBatchImageNativeBirthCommand(cmd, birth), ErrBatchImageNativeBirthInput)
	}
	require.ErrorIs(t, ValidateBatchImageNativeBirthCommand(nil, birth), ErrBatchImageNativeBirthInput)
}

func TestBatchImageNativeBirthSubmitUsesAtomicCreationOnce(t *testing.T) {
	svc, repo, queue, gemini, _ := newTestBatchImagePublicService(true)
	billing := svc.BillingRepo.(*fakeBatchImageBillingRepo)
	first, err := svc.Submit(context.Background(), testBatchImageOwner(), validBatchImageSubmitRequest(), "native-idempotency")
	require.NoError(t, err)
	second, err := svc.Submit(context.Background(), testBatchImageOwner(), validBatchImageSubmitRequest(), "native-idempotency")
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, 1, repo.nativeCreateCalls)
	require.Len(t, repo.jobs, 1)
	require.Len(t, billing.reserves, 1, "the fake creation transaction owns the only reserve call")
	require.Empty(t, billing.releases)
	require.Len(t, gemini.submits, 1)
	require.Equal(t, []string{first.ID}, queue.enqueued)
}

func TestBatchImageNativeBirthCreationErrorsNeverReachProviderOrLeaveJob(t *testing.T) {
	for _, cause := range []error{ErrBatchImageInsufficientBalance, ErrBatchImageNativeBirthInput, ErrBatchImageNativeBirthConflict, ErrBatchImageNativeBirthHistory, errors.New("birth transaction SQL failed")} {
		t.Run(cause.Error(), func(t *testing.T) {
			svc, repo, queue, gemini, vertex := newTestBatchImagePublicService(true)
			repo.nativeCreateErr = cause
			_, err := svc.Submit(context.Background(), testBatchImageOwner(), validBatchImageSubmitRequest(), "")
			require.ErrorIs(t, err, cause)
			require.Equal(t, 1, repo.nativeCreateCalls)
			require.Empty(t, repo.jobs)
			require.Empty(t, gemini.submits)
			require.Empty(t, vertex.submits)
			require.Empty(t, queue.enqueued)
			billing := svc.BillingRepo.(*fakeBatchImageBillingRepo)
			require.Empty(t, billing.reserves)
			require.Empty(t, billing.releases)
			require.Empty(t, svc.AuthCache.(*fakeBatchImageAuthCacheInvalidator).userIDs)
		})
	}
}

func TestBatchImageNativeBirthSubmitDoesNotReadReserveAgainAfterCommit(t *testing.T) {
	svc, repo, _, gemini, _ := newTestBatchImagePublicService(true)
	// This hook stands for an already committed native creation transaction.
	// A second reserve confirmation would fail after the money commit.
	repo.nativeCreateHook = func(context.Context, CreateBatchImageJobParams) error { return nil }
	billing := &fakeBatchImageBillingRepo{reserveErr: errors.New("post-commit confirmation read failed")}
	svc.BillingRepo = billing
	_, err := svc.Submit(context.Background(), testBatchImageOwner(), validBatchImageSubmitRequest(), "")
	require.NoError(t, err)
	require.Len(t, repo.jobs, 1)
	require.Len(t, gemini.submits, 1)
	require.Empty(t, billing.reserves)
	require.Empty(t, billing.releases)
}

func TestBatchImageNativeBirthErrorsNeverExhaustOrPassReleaseCompatibility(t *testing.T) {
	for _, sentinel := range []error{ErrBatchImageNativeBirthInput, ErrBatchImageNativeBirthConflict, ErrBatchImageNativeBirthHistory} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			job := testSettlingBatchImageJob("imgbatch_native_conflict")
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
