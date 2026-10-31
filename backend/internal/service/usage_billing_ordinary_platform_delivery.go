package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/shopspring/decimal"
)

const UsageBillingOrdinaryPlatformDeliveryDBV1 = "db_increment_v1"

type UsageBillingOrdinaryPlatformDeliveryState string

const (
	OrdinaryPlatformDeliveryPending   UsageBillingOrdinaryPlatformDeliveryState = "pending"
	OrdinaryPlatformDeliveryCompleted UsageBillingOrdinaryPlatformDeliveryState = "completed"
)

var (
	ErrUsageBillingOrdinaryPlatformDelivery              = errors.New("invalid ordinary platform quota delivery source")
	ErrUsageBillingOrdinaryPlatformDeliveryResult        = errors.New("invalid ordinary platform quota delivery result")
	ErrUsageBillingOrdinaryPlatformDeliverySourceMissing = errors.New("ordinary platform quota delivery source is missing")
	ErrUsageBillingOrdinaryPlatformDeliveryMissing       = errors.New("ordinary platform quota delivery qualification is missing")
	ErrUsageBillingOrdinaryPlatformDeliveryConflict      = errors.New("ordinary platform quota delivery source conflicts")
)

type UsageBillingOrdinaryPlatformDeliveryResult struct {
	State             UsageBillingOrdinaryPlatformDeliveryState `json:"-"`
	SourceFingerprint string                                    `json:"-"`
	Outcome           UserPlatformQuotaBoundIncrementResult     `json:"-"`
	Replayed          bool                                      `json:"-"`
	CreatedAt         time.Time                                 `json:"-"`
	CompletedAt       *time.Time                                `json:"-"`
}

// This optional source-level port never accepts a detached binding as proof of
// delivery. It does not replay caches, financial Apply or provider work.
type UserPlatformQuotaOrdinaryDeliveryRepository interface {
	ConsumeOrdinaryPlatformQuotaUsage(context.Context, *UsageBillingCommand, *UsageBillingOrdinaryPlatformReceipt) (UsageBillingOrdinaryPlatformDeliveryResult, error)
	GetOrdinaryPlatformQuotaUsageDelivery(context.Context, *UsageBillingCommand, *UsageBillingOrdinaryPlatformReceipt) (*UsageBillingOrdinaryPlatformDeliveryResult, error)
}

// Source validation retains epoch-zero identities for historical lookups and
// terminal results. Only the repository can authorize a pending DB consumption
// against its persisted writer authority; this check never grants that right.
func ValidateUsageBillingOrdinaryPlatformDelivery(cmd *UsageBillingCommand, receipt *UsageBillingOrdinaryPlatformReceipt) error {
	if ValidateUsageBillingOrdinaryPlatformReceipt(cmd, receipt) != nil || receipt.Disposition != "metered" ||
		receipt.DeliveryMode != UsageBillingOrdinaryPlatformDeliveryDBV1 || cmd.OrdinaryPlatformQuotaDeliveryMode != UsageBillingOrdinaryPlatformDeliveryDBV1 {
		return ErrUsageBillingOrdinaryPlatformDelivery
	}
	return nil
}

// Fingerprint uses an explicit closed representation, independent of the public
// JSON surface, the historical money fingerprint and v1/v2 settlement payloads.
func FingerprintUsageBillingOrdinaryPlatformDelivery(receipt *UsageBillingOrdinaryPlatformReceipt) (string, error) {
	if receipt == nil || receipt.WriterEpoch < 0 || receipt.Disposition != "metered" || receipt.DeliveryMode != UsageBillingOrdinaryPlatformDeliveryDBV1 ||
		receipt.QuotaRowID <= 0 || receipt.ControlGeneration <= 0 || !validUserPlatformQuotaCapturedAt(receipt.CapturedAt) ||
		!usageBillingOrdinaryFingerprint.MatchString(receipt.BillingFingerprint) {
		return "", ErrUsageBillingOrdinaryPlatformDelivery
	}
	i := receipt.Intent
	if i.Version != 1 || i.RequestID == "" || len(i.RequestID) > 255 || strings.TrimSpace(i.RequestID) != i.RequestID ||
		i.APIKeyID <= 0 || i.UserID <= 0 || !domain.IsConcretePlatform(i.Platform) || i.IsSubscription ||
		len(i.RawCostUSD) == 0 || len(i.RawCostUSD) > 512 || !usageBillingOrdinaryRawDecimal.MatchString(i.RawCostUSD) ||
		len(i.CostUSD) == 0 || len(i.CostUSD) > 21 || !usageBillingOrdinaryFingerprint.MatchString(i.IntentFingerprint) {
		return "", ErrUsageBillingOrdinaryPlatformDelivery
	}
	raw, err := decimal.NewFromString(i.RawCostUSD)
	if err != nil || raw.IsNegative() || raw.GreaterThanOrEqual(decimal.New(1, 10)) || raw.String() != i.RawCostUSD ||
		raw.Round(10).IsZero() || raw.Round(10).StringFixed(10) != i.CostUSD ||
		usageBillingOrdinaryPlatformHash(receipt.BillingFingerprint, &i) != i.IntentFingerprint {
		return "", ErrUsageBillingOrdinaryPlatformDelivery
	}
	// The epoch-zero representation is the exact historical v1 field order and
	// encoding. Historical lookups must retain their original source identity.
	canonicalV1 := struct {
		Version            int    `json:"version"`
		IntentVersion      int    `json:"intent_version"`
		RequestID          string `json:"request_id"`
		APIKeyID           int64  `json:"api_key_id"`
		UserID             int64  `json:"user_id"`
		Platform           string `json:"platform"`
		RawCostUSD         string `json:"raw_cost_usd"`
		CostUSD            string `json:"cost_usd"`
		IsSubscription     bool   `json:"is_subscription"`
		IntentFingerprint  string `json:"intent_fingerprint"`
		BillingFingerprint string `json:"billing_fingerprint"`
		Disposition        string `json:"disposition"`
		QuotaRowID         int64  `json:"quota_row_id"`
		ControlGeneration  int64  `json:"control_generation"`
		CapturedAt         string `json:"captured_at"`
		DeliveryMode       string `json:"delivery_mode"`
	}{1, i.Version, i.RequestID, i.APIKeyID, i.UserID, i.Platform, i.RawCostUSD, i.CostUSD, i.IsSubscription,
		i.IntentFingerprint, receipt.BillingFingerprint, receipt.Disposition, receipt.QuotaRowID, receipt.ControlGeneration,
		receipt.CapturedAt.UTC().Format(time.RFC3339Nano), receipt.DeliveryMode}
	var canonical any = canonicalV1
	if receipt.WriterEpoch > 0 {
		// Keep v2 explicit and closed rather than changing v1 with an optional
		// field. The epoch binds persisted writer authority, not financial Apply.
		canonical = struct {
			Version            int    `json:"version"`
			IntentVersion      int    `json:"intent_version"`
			RequestID          string `json:"request_id"`
			APIKeyID           int64  `json:"api_key_id"`
			UserID             int64  `json:"user_id"`
			Platform           string `json:"platform"`
			RawCostUSD         string `json:"raw_cost_usd"`
			CostUSD            string `json:"cost_usd"`
			IsSubscription     bool   `json:"is_subscription"`
			IntentFingerprint  string `json:"intent_fingerprint"`
			BillingFingerprint string `json:"billing_fingerprint"`
			Disposition        string `json:"disposition"`
			QuotaRowID         int64  `json:"quota_row_id"`
			ControlGeneration  int64  `json:"control_generation"`
			CapturedAt         string `json:"captured_at"`
			DeliveryMode       string `json:"delivery_mode"`
			WriterEpoch        int64  `json:"writer_epoch"`
		}{2, i.Version, i.RequestID, i.APIKeyID, i.UserID, i.Platform, i.RawCostUSD, i.CostUSD, i.IsSubscription,
			i.IntentFingerprint, receipt.BillingFingerprint, receipt.Disposition, receipt.QuotaRowID, receipt.ControlGeneration,
			receipt.CapturedAt.UTC().Format(time.RFC3339Nano), receipt.DeliveryMode, receipt.WriterEpoch}
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", ErrUsageBillingOrdinaryPlatformDelivery
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd *UsageBillingCommand, receipt *UsageBillingOrdinaryPlatformReceipt, result UsageBillingOrdinaryPlatformDeliveryResult, allowPending bool) error {
	if ValidateUsageBillingOrdinaryPlatformDelivery(cmd, receipt) != nil {
		return ErrUsageBillingOrdinaryPlatformDeliveryResult
	}
	fingerprint, err := FingerprintUsageBillingOrdinaryPlatformDelivery(receipt)
	if err != nil || result.SourceFingerprint != fingerprint || !validUserPlatformQuotaCapturedAt(result.CreatedAt) || result.CreatedAt.Before(receipt.CapturedAt) {
		return ErrUsageBillingOrdinaryPlatformDeliveryResult
	}
	switch result.State {
	case OrdinaryPlatformDeliveryPending:
		if !allowPending || result.CompletedAt != nil || result.Outcome.Status != "" || result.Outcome.AppliedWindows != 0 || result.Replayed {
			return ErrUsageBillingOrdinaryPlatformDeliveryResult
		}
	case OrdinaryPlatformDeliveryCompleted:
		if result.CompletedAt == nil || !validUserPlatformQuotaCapturedAt(*result.CompletedAt) || result.CompletedAt.Before(result.CreatedAt) {
			return ErrUsageBillingOrdinaryPlatformDeliveryResult
		}
		mask := result.Outcome.AppliedWindows
		switch result.Outcome.Status {
		case QuotaIncrementApplied:
			if mask != QuotaWindowAll {
				return ErrUsageBillingOrdinaryPlatformDeliveryResult
			}
		case QuotaIncrementPartial:
			if mask == 0 || mask >= QuotaWindowAll {
				return ErrUsageBillingOrdinaryPlatformDeliveryResult
			}
		case QuotaIncrementSuperseded, QuotaIncrementObsolete:
			if mask != 0 {
				return ErrUsageBillingOrdinaryPlatformDeliveryResult
			}
		default:
			return ErrUsageBillingOrdinaryPlatformDeliveryResult
		}
	default:
		return ErrUsageBillingOrdinaryPlatformDeliveryResult
	}
	return nil
}
