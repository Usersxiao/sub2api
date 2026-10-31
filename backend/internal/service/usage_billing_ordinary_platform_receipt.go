package service

import (
	"errors"
	"strconv"
	"time"
)

var ErrUsageBillingOrdinaryPlatformReceipt = errors.New("invalid ordinary billing platform receipt")

// UsageBillingOrdinaryPlatformReceipt is produced by a fresh committed Apply.
// It is not reconstructed from a later configuration read or an old journal.
type UsageBillingOrdinaryPlatformReceipt struct {
	Intent             UsageBillingOrdinaryPlatformIntent `json:"-"`
	BillingFingerprint string                             `json:"-"`
	Disposition        string                             `json:"-"`
	QuotaRowID         int64                              `json:"-"`
	ControlGeneration  int64                              `json:"-"`
	CapturedAt         time.Time                          `json:"-"`
	DeliveryMode       string                             `json:"-"`
	// Zero retains the historical v1 identity without authorizing a new DB
	// consumption. A positive epoch participates in the v2 delivery fingerprint.
	WriterEpoch int64 `json:"-"`
}

func ValidateUsageBillingOrdinaryPlatformReceipt(cmd *UsageBillingCommand, receipt *UsageBillingOrdinaryPlatformReceipt) error {
	if cmd == nil || receipt == nil || cmd.OrdinaryPlatformQuota == nil ||
		cmd.OrdinaryPlatformQuotaDeliveryMode != "" && cmd.OrdinaryPlatformQuotaDeliveryMode != UsageBillingOrdinaryPlatformDeliveryDBV1 ||
		receipt.BillingFingerprint != cmd.RequestFingerprint || receipt.Intent != *cmd.OrdinaryPlatformQuota ||
		ValidateUsageBillingOrdinaryPlatformIntent(cmd, &receipt.Intent) != nil ||
		!validUserPlatformQuotaCapturedAt(receipt.CapturedAt) || receipt.WriterEpoch < 0 {
		return ErrUsageBillingOrdinaryPlatformReceipt
	}
	if receipt.DeliveryMode != "" && (receipt.DeliveryMode != UsageBillingOrdinaryPlatformDeliveryDBV1 ||
		receipt.Disposition != "metered" || receipt.DeliveryMode != cmd.OrdinaryPlatformQuotaDeliveryMode) {
		return ErrUsageBillingOrdinaryPlatformReceipt
	}
	validIdentity := receipt.QuotaRowID > 0 && receipt.ControlGeneration > 0
	emptyIdentity := receipt.QuotaRowID == 0 && receipt.ControlGeneration == 0
	switch receipt.Disposition {
	case "metered":
		if receipt.Intent.IsSubscription || receipt.Intent.CostUSD == "0.0000000000" || !validIdentity {
			return ErrUsageBillingOrdinaryPlatformReceipt
		}
	case "unlimited":
		if receipt.Intent.IsSubscription || receipt.Intent.CostUSD == "0.0000000000" || !validIdentity && !emptyIdentity {
			return ErrUsageBillingOrdinaryPlatformReceipt
		}
	case "subscription_exempt":
		if !receipt.Intent.IsSubscription || !emptyIdentity {
			return ErrUsageBillingOrdinaryPlatformReceipt
		}
	case "zero_cost":
		if receipt.Intent.IsSubscription || receipt.Intent.CostUSD != "0.0000000000" || !emptyIdentity {
			return ErrUsageBillingOrdinaryPlatformReceipt
		}
	default:
		return ErrUsageBillingOrdinaryPlatformReceipt
	}
	return nil
}

// UsageBinding preserves the raw float used by legacy metering, independently
// of the journal's scale-10 amount and the money command's scale-8 amount.
// The caller first validates this receipt against the original Apply command.
func (receipt *UsageBillingOrdinaryPlatformReceipt) UsageBinding() (UserPlatformQuotaUsageBinding, error) {
	var zero UserPlatformQuotaUsageBinding
	if receipt == nil || receipt.Disposition != "metered" || receipt.Intent.IsSubscription ||
		receipt.Intent.CostUSD == "0.0000000000" || len(receipt.Intent.RawCostUSD) == 0 || len(receipt.Intent.RawCostUSD) > 512 ||
		!usageBillingOrdinaryRawDecimal.MatchString(receipt.Intent.RawCostUSD) {
		return zero, ErrUsageBillingOrdinaryPlatformReceipt
	}
	cost, err := strconv.ParseFloat(receipt.Intent.RawCostUSD, 64)
	if err != nil {
		return zero, ErrUsageBillingOrdinaryPlatformReceipt
	}
	binding := UserPlatformQuotaUsageBinding{QuotaRowID: receipt.QuotaRowID, ControlGeneration: receipt.ControlGeneration,
		UserID: receipt.Intent.UserID, Platform: receipt.Intent.Platform, CostUSD: cost, CapturedAt: receipt.CapturedAt}
	if err := ValidateUserPlatformQuotaUsageBinding(binding); err != nil {
		return zero, err
	}
	return binding, nil
}
