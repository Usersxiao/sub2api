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
	ErrBatchImageBalanceLifecycleConflict = errors.New("batch image balance lifecycle conflict")
	ErrBatchImageBalanceLifecycleHistory  = errors.New("batch image balance lifecycle history is missing")
	ErrBatchImageBalanceLifecycleInput    = errors.New("invalid batch image balance lifecycle input")
)

// BatchImageBalanceLifecycleProof describes an observed operation and its
// reservation reference. A legacy_dedup reference does not certify the original
// freeze; a no_hold_terminal proof does not certify a refund. Repository code
// must validate the referenced evidence before committing this private proof
// atomically with the operation. This codec does not establish native cohort,
// complete historical coverage, or exact net-wallet accounting.
type BatchImageBalanceLifecycleProof struct {
	Version            int    `json:"-"`
	Operation          string `json:"-"`
	RequestID          string `json:"-"`
	BatchID            string `json:"-"`
	RawHoldUSD         string `json:"-"`
	RawActualUSD       string `json:"-"`
	Outcome            string `json:"-"`
	ReserveOrigin      string `json:"-"`
	ReserveFingerprint string `json:"-"`
	ProofFingerprint   string `json:"-"`
	APIKeyID           int64  `json:"-"`
	UserID             int64  `json:"-"`
	BillingFingerprint string `json:"-"`
}

// FreezeBatchImageBalanceLifecycleProof requires a normalized command and
// leaves its historical fingerprint and raw financial amounts untouched.
func FreezeBatchImageBalanceLifecycleProof(cmd *BatchImageBalanceHoldCommand, operation, outcome, reserveOrigin, reserveFingerprint string) (*BatchImageBalanceLifecycleProof, error) {
	if cmd == nil || !batchImagePlatformAmountValid(cmd.HoldAmount) || !batchImagePlatformAmountValid(cmd.ActualAmount) {
		return nil, ErrBatchImageBalanceLifecycleInput
	}
	proof := &BatchImageBalanceLifecycleProof{
		Version: 1, Operation: operation, RequestID: cmd.RequestID, BatchID: cmd.BatchID,
		RawHoldUSD: decimal.NewFromFloat(cmd.HoldAmount).String(), RawActualUSD: decimal.NewFromFloat(cmd.ActualAmount).String(),
		Outcome: outcome, ReserveOrigin: reserveOrigin, ReserveFingerprint: reserveFingerprint,
		APIKeyID: cmd.APIKeyID, UserID: cmd.UserID, BillingFingerprint: cmd.RequestFingerprint,
	}
	proof.ProofFingerprint = batchImageBalanceLifecycleHash(proof)
	if err := ValidateBatchImageBalanceLifecycleProof(cmd, proof); err != nil {
		return nil, err
	}
	return proof, nil
}

// ValidateBatchImageBalanceLifecycleProof validates the closed operation and
// outcome matrix, exact raw command binding, and independent private hash. The
// optional expected fingerprint is a hint to check, not reservation evidence.
func ValidateBatchImageBalanceLifecycleProof(cmd *BatchImageBalanceHoldCommand, proof *BatchImageBalanceLifecycleProof) error {
	if cmd == nil || proof == nil || cmd.APIKeyID <= 0 || cmd.UserID <= 0 ||
		cmd.BatchID == "" || len(cmd.BatchID) > 64 || !utf8.ValidString(cmd.BatchID) || cmd.BatchID != strings.TrimSpace(cmd.BatchID) ||
		!batchImagePlatformAmountValid(cmd.HoldAmount) || !batchImagePlatformAmountValid(cmd.ActualAmount) ||
		!batchImagePlatformFingerprint.MatchString(cmd.RequestFingerprint) || cmd.RequestFingerprint != buildBatchImageBalanceHoldFingerprint(cmd) ||
		proof.RequestID != cmd.RequestID || proof.BatchID != cmd.BatchID || proof.APIKeyID != cmd.APIKeyID || proof.UserID != cmd.UserID ||
		proof.BillingFingerprint != cmd.RequestFingerprint || !batchImagePlatformFingerprint.MatchString(proof.ProofFingerprint) ||
		(cmd.ExpectedReserveFingerprint != "" && !batchImagePlatformFingerprint.MatchString(cmd.ExpectedReserveFingerprint)) {
		return ErrBatchImageBalanceLifecycleInput
	}
	if err := ValidateBatchImageBalanceLifecycleStoredProof(proof); err != nil {
		return err
	}
	rawHold, err := batchImagePlatformRawAmount(proof.RawHoldUSD)
	if err != nil || !rawHold.Equal(decimal.NewFromFloat(cmd.HoldAmount)) {
		return ErrBatchImageBalanceLifecycleInput
	}
	rawActual, err := batchImagePlatformRawAmount(proof.RawActualUSD)
	if err != nil || !rawActual.Equal(decimal.NewFromFloat(cmd.ActualAmount)) {
		return ErrBatchImageBalanceLifecycleInput
	}
	if (proof.Outcome == "captured" || proof.Outcome == "refunded") && cmd.HoldAmount > 0 &&
		cmd.ExpectedReserveFingerprint != "" && proof.ReserveFingerprint != cmd.ExpectedReserveFingerprint {
		return ErrBatchImageBalanceLifecycleInput
	}
	return nil
}

// ValidateBatchImageBalanceLifecycleStoredProof validates durable evidence
// without inventing the original payload hash. The closed private hash binds
// the stored billing fingerprint; current-command validation separately
// recomputes that fingerprint from its real payload.
func ValidateBatchImageBalanceLifecycleStoredProof(proof *BatchImageBalanceLifecycleProof) error {
	if proof == nil || proof.Version != 1 || proof.APIKeyID <= 0 || proof.UserID <= 0 ||
		proof.BatchID == "" || len(proof.BatchID) > 64 || !utf8.ValidString(proof.BatchID) || proof.BatchID != strings.TrimSpace(proof.BatchID) ||
		!batchImagePlatformFingerprint.MatchString(proof.BillingFingerprint) || !batchImagePlatformFingerprint.MatchString(proof.ProofFingerprint) {
		return ErrBatchImageBalanceLifecycleInput
	}
	rawHold, err := batchImagePlatformRawAmount(proof.RawHoldUSD)
	if err != nil || !batchImageBalanceLifecycleShortestRaw(proof.RawHoldUSD) {
		return ErrBatchImageBalanceLifecycleInput
	}
	rawActual, err := batchImagePlatformRawAmount(proof.RawActualUSD)
	if err != nil || !batchImageBalanceLifecycleShortestRaw(proof.RawActualUSD) {
		return ErrBatchImageBalanceLifecycleInput
	}
	var requestID string
	valid := false
	switch proof.Operation {
	case "reserve":
		requestID = BatchImageHoldRequestID(proof.BatchID)
		valid = rawActual.IsZero() && ((proof.Outcome == "reserved" && rawHold.IsPositive() && proof.ReserveOrigin == "observed_reserve" && proof.ReserveFingerprint == proof.BillingFingerprint) ||
			(proof.Outcome == "zero_hold" && rawHold.IsZero() && proof.ReserveOrigin == "none" && proof.ReserveFingerprint == ""))
	case "capture":
		requestID = BatchImageCaptureRequestID(proof.BatchID)
		valid = proof.Outcome == "captured" && ((rawHold.IsPositive() && batchImageBalanceLifecycleHasReserve(proof)) ||
			(rawHold.IsZero() && rawActual.IsZero() && proof.ReserveOrigin == "none" && proof.ReserveFingerprint == ""))
	case "release":
		requestID = BatchImageReleaseRequestID(proof.BatchID)
		valid = rawActual.IsZero() && ((proof.Outcome == "refunded" && rawHold.IsPositive() && batchImageBalanceLifecycleHasReserve(proof)) ||
			(proof.Outcome == "no_hold_terminal" && rawHold.IsPositive() && proof.ReserveOrigin == "none" && proof.ReserveFingerprint == "") ||
			(proof.Outcome == "zero_hold" && rawHold.IsZero() && proof.ReserveOrigin == "none" && proof.ReserveFingerprint == ""))
	}
	if !valid || proof.RequestID != requestID || batchImageBalanceLifecycleHash(proof) != proof.ProofFingerprint {
		return ErrBatchImageBalanceLifecycleInput
	}
	return nil
}

func batchImageBalanceLifecycleHasReserve(proof *BatchImageBalanceLifecycleProof) bool {
	return (proof.ReserveOrigin == "observed_reserve" || proof.ReserveOrigin == "legacy_dedup") &&
		batchImagePlatformFingerprint.MatchString(proof.ReserveFingerprint)
}

func batchImageBalanceLifecycleShortestRaw(raw string) bool {
	amount, err := strconv.ParseFloat(raw, 64)
	return err == nil && batchImagePlatformAmountValid(amount) && decimal.NewFromFloat(amount).String() == raw
}

func batchImageBalanceLifecycleHash(proof *BatchImageBalanceLifecycleProof) string {
	// This closed encoding is separate from public JSON, the platform intent,
	// and the unchanged historical billing fingerprint.
	canonical := struct {
		Version            int    `json:"version"`
		Operation          string `json:"operation"`
		RequestID          string `json:"request_id"`
		BatchID            string `json:"batch_id"`
		APIKeyID           int64  `json:"api_key_id"`
		UserID             int64  `json:"user_id"`
		BillingFingerprint string `json:"billing_fingerprint"`
		RawHoldUSD         string `json:"raw_hold_usd"`
		RawActualUSD       string `json:"raw_actual_usd"`
		Outcome            string `json:"outcome"`
		ReserveOrigin      string `json:"reserve_origin"`
		ReserveFingerprint string `json:"reserve_fingerprint"`
	}{proof.Version, proof.Operation, proof.RequestID, proof.BatchID, proof.APIKeyID, proof.UserID, proof.BillingFingerprint,
		proof.RawHoldUSD, proof.RawActualUSD, proof.Outcome, proof.ReserveOrigin, proof.ReserveFingerprint}
	encoded, _ := json.Marshal(canonical) // Only finite integers and strings.
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
