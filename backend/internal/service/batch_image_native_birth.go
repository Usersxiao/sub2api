package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/shopspring/decimal"
)

var (
	ErrBatchImageNativeBirthInput    = errors.New("invalid batch image native birth input")
	ErrBatchImageNativeBirthConflict = errors.New("batch image native birth conflict")
	ErrBatchImageNativeBirthHistory  = errors.New("batch image native birth history is missing")
)

// BatchImageNativeBirth records a job born with an atomic reservation. It is
// private evidence for this creation path, not historical backfill, proof of
// complete provider consumption, or certification of every writer's coverage.
type BatchImageNativeBirth struct {
	Version            int    `json:"-"`
	JobID              int64  `json:"-"`
	BatchID            string `json:"-"`
	UserID             int64  `json:"-"`
	APIKeyID           int64  `json:"-"`
	Provider           string `json:"-"`
	Model              string `json:"-"`
	RequestHash        string `json:"-"`
	RawHoldUSD         string `json:"-"`
	ReserveFingerprint string `json:"-"`
	ProofFingerprint   string `json:"-"`
}

// FreezeBatchImageNativeBirth accepts the job returned by INSERT RETURNING.
// Its hold has already crossed the job column's decimal(20,10) boundary. It
// neither rounds a pre-insert price nor clamps malformed amounts into zero.
func FreezeBatchImageNativeBirth(job *BatchImageJob) (*BatchImageNativeBirth, error) {
	if job == nil || job.ID <= 0 || job.APIKeyID == nil || *job.APIKeyID <= 0 || job.UserID <= 0 ||
		job.Status != BatchImageJobStatusCreated || strings.TrimSpace(batchImageDerefString(job.ProviderJobName)) != "" || job.Currency != "USD" ||
		job.ActualCost != nil || strings.TrimSpace(batchImageDerefString(job.ProviderInputRef)) != "" || strings.TrimSpace(batchImageDerefString(job.ProviderOutputRef)) != "" ||
		strings.TrimSpace(batchImageDerefString(job.GCSInputURI)) != "" || strings.TrimSpace(batchImageDerefString(job.GCSOutputURI)) != "" ||
		job.SubmittedAt != nil || job.StartedAt != nil || job.FinishedAt != nil || job.SettledAt != nil {
		return nil, ErrBatchImageNativeBirthInput
	}
	hold := job.EstimatedCost
	if job.HoldAmount != nil {
		hold = *job.HoldAmount
	}
	if !batchImagePlatformAmountValid(hold) {
		return nil, ErrBatchImageNativeBirthInput
	}
	requestHash := batchImageDerefString(job.RequestHash)
	if !batchImagePlatformFingerprint.MatchString(requestHash) {
		return nil, ErrBatchImageNativeBirthInput
	}
	reserve, err := buildBatchImageHoldCommand(job, BatchImageHoldRequestID(job.BatchID), 0, requestHash)
	if err != nil {
		return nil, ErrBatchImageNativeBirthInput
	}
	reserve.Normalize()
	birth := &BatchImageNativeBirth{
		Version: 1, JobID: job.ID, BatchID: job.BatchID, UserID: job.UserID, APIKeyID: *job.APIKeyID,
		Provider: job.Provider, Model: job.Model, RequestHash: requestHash,
		RawHoldUSD: decimal.NewFromFloat(hold).String(), ReserveFingerprint: reserve.RequestFingerprint,
	}
	birth.ProofFingerprint = batchImageNativeBirthHash(birth)
	if err := ValidateBatchImageNativeBirthStoredProof(birth); err != nil {
		return nil, err
	}
	return birth, nil
}

// ValidateBatchImageNativeBirthStoredProof verifies the closed native birth
// record and reproduces the original reserve fingerprint from its bound scope.
func ValidateBatchImageNativeBirthStoredProof(birth *BatchImageNativeBirth) error {
	if birth == nil || birth.Version != 1 || birth.JobID <= 0 || birth.UserID <= 0 || birth.APIKeyID <= 0 ||
		birth.BatchID == "" || len(birth.BatchID) > 64 || !utf8.ValidString(birth.BatchID) || birth.BatchID != strings.TrimSpace(birth.BatchID) ||
		!IsSupportedBatchImageProvider(birth.Provider) || birth.Model == "" || len(birth.Model) > 128 ||
		!utf8.ValidString(birth.Model) || birth.Model != strings.TrimSpace(birth.Model) ||
		!batchImagePlatformFingerprint.MatchString(birth.RequestHash) ||
		!batchImagePlatformFingerprint.MatchString(birth.ReserveFingerprint) ||
		!batchImagePlatformFingerprint.MatchString(birth.ProofFingerprint) {
		return ErrBatchImageNativeBirthInput
	}
	if _, err := batchImagePlatformRawAmount(birth.RawHoldUSD); err != nil || !batchImageBalanceLifecycleShortestRaw(birth.RawHoldUSD) {
		return ErrBatchImageNativeBirthInput
	}
	hold, err := strconv.ParseFloat(birth.RawHoldUSD, 64)
	if err != nil {
		return ErrBatchImageNativeBirthInput
	}
	reserve := &BatchImageBalanceHoldCommand{RequestID: BatchImageHoldRequestID(birth.BatchID), APIKeyID: birth.APIKeyID,
		UserID: birth.UserID, BatchID: birth.BatchID, HoldAmount: hold, RequestPayloadHash: birth.RequestHash}
	reserve.Normalize()
	if reserve.RequestFingerprint != birth.ReserveFingerprint || batchImageNativeBirthHash(birth) != birth.ProofFingerprint {
		return ErrBatchImageNativeBirthInput
	}
	return nil
}

// ValidateBatchImageNativeBirthCommand binds any canonical balance operation
// to the immutable owner, key, batch and exact persisted hold. Capture's
// manifest fingerprint remains distinct from the original reserve fingerprint.
func ValidateBatchImageNativeBirthCommand(cmd *BatchImageBalanceHoldCommand, birth *BatchImageNativeBirth) error {
	if cmd == nil || cmd.APIKeyID <= 0 || cmd.UserID <= 0 ||
		!batchImagePlatformAmountValid(cmd.HoldAmount) || !batchImagePlatformAmountValid(cmd.ActualAmount) ||
		!batchImagePlatformFingerprint.MatchString(cmd.RequestFingerprint) || cmd.RequestFingerprint != buildBatchImageBalanceHoldFingerprint(cmd) ||
		(cmd.ExpectedReserveFingerprint != "" && !batchImagePlatformFingerprint.MatchString(cmd.ExpectedReserveFingerprint)) {
		return ErrBatchImageNativeBirthInput
	}
	if err := ValidateBatchImageNativeBirthStoredProof(birth); err != nil {
		return err
	}
	if cmd.RequestID != BatchImageHoldRequestID(cmd.BatchID) && cmd.RequestID != BatchImageCaptureRequestID(cmd.BatchID) && cmd.RequestID != BatchImageReleaseRequestID(cmd.BatchID) {
		return ErrBatchImageNativeBirthInput
	}
	if cmd.UserID != birth.UserID || cmd.APIKeyID != birth.APIKeyID || cmd.BatchID != birth.BatchID ||
		decimal.NewFromFloat(cmd.HoldAmount).String() != birth.RawHoldUSD ||
		(cmd.ExpectedReserveFingerprint != "" && cmd.ExpectedReserveFingerprint != birth.ReserveFingerprint) ||
		(cmd.RequestID == BatchImageHoldRequestID(cmd.BatchID) && cmd.RequestFingerprint != birth.ReserveFingerprint) ||
		(cmd.CapturePlatformQuota != nil && cmd.CapturePlatformQuota.Provider != birth.Provider) {
		return ErrBatchImageNativeBirthConflict
	}
	return nil
}

func batchImageNativeBirthHash(birth *BatchImageNativeBirth) string {
	canonical := struct {
		Version            int    `json:"version"`
		JobID              int64  `json:"job_id"`
		BatchID            string `json:"batch_id"`
		UserID             int64  `json:"user_id"`
		APIKeyID           int64  `json:"api_key_id"`
		Provider           string `json:"provider"`
		Model              string `json:"model"`
		RequestHash        string `json:"request_hash"`
		RawHoldUSD         string `json:"raw_hold_usd"`
		ReserveFingerprint string `json:"reserve_fingerprint"`
	}{birth.Version, birth.JobID, birth.BatchID, birth.UserID, birth.APIKeyID, birth.Provider, birth.Model,
		birth.RequestHash, birth.RawHoldUSD, birth.ReserveFingerprint}
	encoded, _ := json.Marshal(canonical) // Only finite integers and strings.
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
