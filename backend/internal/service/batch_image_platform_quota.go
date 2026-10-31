package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/shopspring/decimal"
)

var (
	ErrBatchImagePlatformQuotaIntent = errors.New("invalid batch image platform quota intent")
	// This is intentionally independent of ErrUsageBillingRequestConflict:
	// release's historical fingerprint compatibility must not swallow it.
	ErrBatchImageCaptureAlreadyApplied = errors.New("batch image capture already applied")
	batchImagePlatformRawDecimal       = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+)?$`)
	batchImagePlatformFingerprint      = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// BatchImagePlatformQuotaIntent is private evidence for a batch capture. Its
// platform cost independently rounds the raw actual amount to scale 10. It is
// not the net wallet delta: historical capture SQL retains its own raw
// hold-minus-actual arithmetic and numeric(20,8) rounding. For pricing snapshot
// v0 it binds the current settlement price, not the original execution price.
type BatchImagePlatformQuotaIntent struct {
	Version           int    `json:"-"`
	RequestID         string `json:"-"`
	BatchID           string `json:"-"`
	Provider          string `json:"-"`
	Platform          string `json:"-"`
	RawHoldUSD        string `json:"-"`
	RawCostUSD        string `json:"-"`
	CostUSD           string `json:"-"`
	IntentFingerprint string `json:"-"`
	APIKeyID          int64  `json:"-"`
	UserID            int64  `json:"-"`
}

// FreezeBatchImagePlatformQuotaIntent requires a normalized capture command and
// preserves both raw floats' shortest decimal representations. It does not
// mutate or quantize the command, its historical fingerprint, or wallet money.
func FreezeBatchImagePlatformQuotaIntent(cmd *BatchImageBalanceHoldCommand, provider string) (*BatchImagePlatformQuotaIntent, error) {
	if cmd == nil || !batchImagePlatformAmountValid(cmd.HoldAmount) || !batchImagePlatformAmountValid(cmd.ActualAmount) {
		return nil, ErrBatchImagePlatformQuotaIntent
	}
	platform, ok := batchImageCapturePlatform(provider)
	if !ok {
		return nil, ErrBatchImagePlatformQuotaIntent
	}
	rawCost := decimal.NewFromFloat(cmd.ActualAmount)
	intent := &BatchImagePlatformQuotaIntent{
		Version: 1, RequestID: cmd.RequestID, BatchID: cmd.BatchID, Provider: provider, Platform: platform,
		RawHoldUSD: decimal.NewFromFloat(cmd.HoldAmount).String(), RawCostUSD: rawCost.String(),
		CostUSD: rawCost.Round(10).StringFixed(10), APIKeyID: cmd.APIKeyID, UserID: cmd.UserID,
	}
	intent.IntentFingerprint = batchImagePlatformQuotaHash(cmd.RequestFingerprint, intent)
	if err := ValidateBatchImagePlatformQuotaIntent(cmd, intent); err != nil {
		return nil, err
	}
	return intent, nil
}

// ValidateBatchImagePlatformQuotaIntent verifies the exact raw command binding
// and the independent platform amount without changing historical money logic.
func ValidateBatchImagePlatformQuotaIntent(cmd *BatchImageBalanceHoldCommand, intent *BatchImagePlatformQuotaIntent) error {
	if cmd == nil || intent == nil || cmd.APIKeyID <= 0 || cmd.UserID <= 0 ||
		cmd.BatchID == "" || len(cmd.BatchID) > 64 || !utf8.ValidString(cmd.BatchID) || cmd.BatchID != strings.TrimSpace(cmd.BatchID) ||
		cmd.RequestID != BatchImageCaptureRequestID(cmd.BatchID) ||
		!batchImagePlatformFingerprint.MatchString(cmd.RequestFingerprint) ||
		cmd.RequestFingerprint != buildBatchImageBalanceHoldFingerprint(cmd) ||
		!batchImagePlatformAmountValid(cmd.HoldAmount) || !batchImagePlatformAmountValid(cmd.ActualAmount) ||
		intent.RequestID != cmd.RequestID || intent.BatchID != cmd.BatchID || intent.APIKeyID != cmd.APIKeyID || intent.UserID != cmd.UserID {
		return ErrBatchImagePlatformQuotaIntent
	}
	if err := ValidateBatchImagePlatformQuotaStoredIntent(intent, cmd.RequestFingerprint); err != nil {
		return err
	}
	rawHold, err := batchImagePlatformRawAmount(intent.RawHoldUSD)
	if err != nil || !rawHold.Equal(decimal.NewFromFloat(cmd.HoldAmount)) {
		return ErrBatchImagePlatformQuotaIntent
	}
	rawCost, err := batchImagePlatformRawAmount(intent.RawCostUSD)
	if err != nil || !rawCost.Equal(decimal.NewFromFloat(cmd.ActualAmount)) {
		return ErrBatchImagePlatformQuotaIntent
	}
	return nil
}

// ValidateBatchImagePlatformQuotaStoredIntent checks the closed persisted
// intent against its stored billing fingerprint, without reconstructing a
// capture command or inventing its original manifest. The caller must bind
// that fingerprint and scope to the independent lifecycle/money evidence.
func ValidateBatchImagePlatformQuotaStoredIntent(intent *BatchImagePlatformQuotaIntent, billingFingerprint string) error {
	if intent == nil || intent.Version != 1 || intent.APIKeyID <= 0 || intent.UserID <= 0 ||
		intent.BatchID == "" || len(intent.BatchID) > 64 || !utf8.ValidString(intent.BatchID) || intent.BatchID != strings.TrimSpace(intent.BatchID) ||
		intent.RequestID != BatchImageCaptureRequestID(intent.BatchID) ||
		!batchImagePlatformFingerprint.MatchString(billingFingerprint) ||
		!batchImagePlatformFingerprint.MatchString(intent.IntentFingerprint) {
		return ErrBatchImagePlatformQuotaIntent
	}
	platform, ok := batchImageCapturePlatform(intent.Provider)
	if !ok || intent.Platform != platform {
		return ErrBatchImagePlatformQuotaIntent
	}
	if _, err := batchImagePlatformRawAmount(intent.RawHoldUSD); err != nil || !batchImageBalanceLifecycleShortestRaw(intent.RawHoldUSD) {
		return ErrBatchImagePlatformQuotaIntent
	}
	rawCost, err := batchImagePlatformRawAmount(intent.RawCostUSD)
	if err != nil || !batchImageBalanceLifecycleShortestRaw(intent.RawCostUSD) {
		return ErrBatchImagePlatformQuotaIntent
	}
	platformCost := rawCost.Round(10)
	if platformCost.GreaterThanOrEqual(decimal.New(1, 10)) || platformCost.StringFixed(10) != intent.CostUSD ||
		batchImagePlatformQuotaHash(billingFingerprint, intent) != intent.IntentFingerprint {
		return ErrBatchImagePlatformQuotaIntent
	}
	return nil
}

func batchImageCapturePlatform(provider string) (string, bool) {
	switch provider {
	case BatchImageProviderGeminiAPI, BatchImageProviderVertex:
		return PlatformGemini, true
	default:
		return "", false
	}
}

func batchImagePlatformAmountValid(amount float64) bool {
	return !math.IsNaN(amount) && !math.IsInf(amount, 0) && amount >= 0 && amount < 1e10
}

func batchImagePlatformRawAmount(raw string) (decimal.Decimal, error) {
	if len(raw) == 0 || len(raw) > 512 || !batchImagePlatformRawDecimal.MatchString(raw) {
		return decimal.Zero, ErrBatchImagePlatformQuotaIntent
	}
	amount, err := decimal.NewFromString(raw)
	if err != nil || amount.IsNegative() || amount.GreaterThanOrEqual(decimal.New(1, 10)) || amount.String() != raw {
		return decimal.Zero, ErrBatchImagePlatformQuotaIntent
	}
	return amount, nil
}

func batchImagePlatformQuotaHash(billingFingerprint string, intent *BatchImagePlatformQuotaIntent) string {
	// Closed private representation; independent from public JSON and from the
	// historical batch capture fingerprint (which intentionally stays intact).
	canonical := struct {
		Version            int    `json:"version"`
		RequestID          string `json:"request_id"`
		BatchID            string `json:"batch_id"`
		APIKeyID           int64  `json:"api_key_id"`
		UserID             int64  `json:"user_id"`
		BillingFingerprint string `json:"billing_fingerprint"`
		Provider           string `json:"provider"`
		Platform           string `json:"platform"`
		RawHoldUSD         string `json:"raw_hold_usd"`
		RawCostUSD         string `json:"raw_cost_usd"`
		CostUSD            string `json:"cost_usd"`
	}{intent.Version, intent.RequestID, intent.BatchID, intent.APIKeyID, intent.UserID, billingFingerprint,
		intent.Provider, intent.Platform, intent.RawHoldUSD, intent.RawCostUSD, intent.CostUSD}
	encoded, _ := json.Marshal(canonical) // Only finite integers and strings.
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
