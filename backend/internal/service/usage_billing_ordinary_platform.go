package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/shopspring/decimal"
)

var (
	ErrUsageBillingOrdinaryPlatformIntent      = errors.New("invalid ordinary billing platform intent")
	ErrUsageBillingOrdinaryPlatformCoverageGap = errors.New("ordinary billing platform source coverage is missing")
	usageBillingOrdinaryRawDecimal             = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+)?$`)
	usageBillingOrdinaryFingerprint            = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// UsageBillingOrdinaryPlatformIntent is frozen only from internal gateway
// accounting inputs. It is neither a public request DTO nor a client settlement
// payload, and does not change legacy counters or establish merged admission.
type UsageBillingOrdinaryPlatformIntent struct {
	Version           int    `json:"-"`
	RequestID         string `json:"-"`
	APIKeyID          int64  `json:"-"`
	UserID            int64  `json:"-"`
	Platform          string `json:"-"`
	RawCostUSD        string `json:"-"`
	CostUSD           string `json:"-"`
	IsSubscription    bool   `json:"-"`
	IntentFingerprint string `json:"-"`
}

// FreezeUsageBillingOrdinaryPlatformIntent preserves the raw float's shortest
// decimal representation, then independently rounds it to platform scale 10.
// Validation separately compares raw scale 8 with the normalized financial
// command; rounding the platform amount a second time would double-round.
func FreezeUsageBillingOrdinaryPlatformIntent(cmd *UsageBillingCommand, platform string, rawActualCost float64, isSubscription bool) (*UsageBillingOrdinaryPlatformIntent, error) {
	if cmd == nil || math.IsNaN(rawActualCost) || math.IsInf(rawActualCost, 0) || rawActualCost < 0 || rawActualCost >= 1e10 {
		return nil, ErrUsageBillingOrdinaryPlatformIntent
	}
	raw := decimal.NewFromFloat(rawActualCost)
	intent := &UsageBillingOrdinaryPlatformIntent{
		Version: 1, RequestID: cmd.RequestID, APIKeyID: cmd.APIKeyID, UserID: cmd.UserID,
		Platform: platform, RawCostUSD: raw.String(), CostUSD: raw.Round(10).StringFixed(10), IsSubscription: isSubscription,
	}
	intent.IntentFingerprint = usageBillingOrdinaryPlatformHash(cmd.RequestFingerprint, intent)
	if err := ValidateUsageBillingOrdinaryPlatformIntent(cmd, intent); err != nil {
		return nil, err
	}
	return intent, nil
}

// ValidateUsageBillingOrdinaryPlatformIntent checks both the original financial
// binding and the independent platform proof without mutating either input.
func ValidateUsageBillingOrdinaryPlatformIntent(cmd *UsageBillingCommand, intent *UsageBillingOrdinaryPlatformIntent) error {
	if cmd == nil || intent == nil || intent.Version != 1 || cmd.RequestID == "" || len(cmd.RequestID) > 255 ||
		cmd.RequestID != strings.TrimSpace(cmd.RequestID) || !usageBillingOrdinaryFingerprint.MatchString(cmd.RequestFingerprint) ||
		cmd.APIKeyID <= 0 || cmd.UserID <= 0 || intent.RequestID != cmd.RequestID || intent.APIKeyID != cmd.APIKeyID || intent.UserID != cmd.UserID ||
		!domain.IsConcretePlatform(intent.Platform) || len(intent.RawCostUSD) == 0 || len(intent.RawCostUSD) > 512 ||
		!usageBillingOrdinaryRawDecimal.MatchString(intent.RawCostUSD) || len(intent.CostUSD) == 0 || len(intent.CostUSD) > 21 ||
		!usageBillingOrdinaryFingerprint.MatchString(intent.IntentFingerprint) ||
		(cmd.BillingType != BillingTypeBalance && cmd.BillingType != BillingTypeSubscription) ||
		intent.IsSubscription != (cmd.BillingType == BillingTypeSubscription) ||
		math.IsNaN(cmd.BalanceCost) || math.IsInf(cmd.BalanceCost, 0) || math.IsNaN(cmd.SubscriptionCost) || math.IsInf(cmd.SubscriptionCost, 0) {
		return ErrUsageBillingOrdinaryPlatformIntent
	}
	raw, err := decimal.NewFromString(intent.RawCostUSD)
	if err != nil || raw.IsNegative() || raw.GreaterThanOrEqual(decimal.New(1, 10)) || raw.String() != intent.RawCostUSD {
		return ErrUsageBillingOrdinaryPlatformIntent
	}
	platformCost := raw.Round(10)
	if platformCost.GreaterThanOrEqual(decimal.New(1, 10)) || platformCost.StringFixed(10) != intent.CostUSD {
		return ErrUsageBillingOrdinaryPlatformIntent
	}
	financialCost := raw.Round(UsageBillingMonetaryScale)
	if intent.IsSubscription {
		if cmd.SubscriptionID == nil || *cmd.SubscriptionID <= 0 || cmd.BalanceCost != 0 ||
			!financialCost.Equal(decimal.NewFromFloat(cmd.SubscriptionCost)) {
			return ErrUsageBillingOrdinaryPlatformIntent
		}
	} else if cmd.SubscriptionCost != 0 || !financialCost.Equal(decimal.NewFromFloat(cmd.BalanceCost)) {
		return ErrUsageBillingOrdinaryPlatformIntent
	}
	if usageBillingOrdinaryPlatformHash(cmd.RequestFingerprint, intent) != intent.IntentFingerprint {
		return ErrUsageBillingOrdinaryPlatformIntent
	}
	return nil
}

func usageBillingOrdinaryPlatformHash(billingFingerprint string, intent *UsageBillingOrdinaryPlatformIntent) string {
	// This closed representation is independent of public JSON serialization
	// and of the historical UsageBillingCommand fingerprint layout.
	canonical := struct {
		Version            int    `json:"version"`
		RequestID          string `json:"request_id"`
		APIKeyID           int64  `json:"api_key_id"`
		UserID             int64  `json:"user_id"`
		BillingFingerprint string `json:"billing_fingerprint"`
		Platform           string `json:"platform"`
		RawCostUSD         string `json:"raw_cost_usd"`
		CostUSD            string `json:"cost_usd"`
		IsSubscription     bool   `json:"is_subscription"`
	}{intent.Version, intent.RequestID, intent.APIKeyID, intent.UserID, billingFingerprint,
		intent.Platform, intent.RawCostUSD, intent.CostUSD, intent.IsSubscription}
	encoded, _ := json.Marshal(canonical) // Only finite integers, booleans and strings.
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
