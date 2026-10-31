package service

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

var ErrUserPlatformQuotaUsageBinding = errors.New("invalid user platform quota usage binding")

// UserPlatformQuotaUsageBinding retains the actual Apply observation. Current
// configuration and the bookkeeping clock must never replace these fields.
type UserPlatformQuotaUsageBinding struct {
	QuotaRowID        int64     `json:"-"`
	ControlGeneration int64     `json:"-"`
	UserID            int64     `json:"-"`
	Platform          string    `json:"-"`
	CostUSD           float64   `json:"-"`
	CapturedAt        time.Time `json:"-"`
}

func ValidateUserPlatformQuotaUsageBinding(binding UserPlatformQuotaUsageBinding) error {
	if binding.QuotaRowID <= 0 || binding.ControlGeneration <= 0 || binding.UserID <= 0 ||
		!domain.IsConcretePlatform(binding.Platform) || binding.CostUSD <= 0 || math.IsNaN(binding.CostUSD) || math.IsInf(binding.CostUSD, 0) ||
		!validUserPlatformQuotaCapturedAt(binding.CapturedAt) {
		return ErrUserPlatformQuotaUsageBinding
	}
	return nil
}

func validUserPlatformQuotaCapturedAt(at time.Time) bool {
	return !at.IsZero() && at.Unix() >= 0 && at.Unix() <= 253402300799
}

const (
	QuotaWindowDaily   uint8 = 1
	QuotaWindowWeekly  uint8 = 2
	QuotaWindowMonthly uint8 = 4
	QuotaWindowAll     uint8 = 7
)

const (
	QuotaIncrementSuperseded UserPlatformQuotaIncrementStatus = "superseded"
	QuotaIncrementPartial    UserPlatformQuotaIncrementStatus = "partial"
	QuotaIncrementObsolete   UserPlatformQuotaIncrementStatus = "obsolete"
)

type UserPlatformQuotaBoundIncrementResult struct {
	Status         UserPlatformQuotaIncrementStatus `json:"-"`
	AppliedWindows uint8                            `json:"-"`
}

// Optional interfaces keep unrelated repository/cache implementations outside
// this bound path. Unsupported writers are explicit failures, never a fallback.
type UserPlatformQuotaBoundUsageRepository interface {
	IncrementBoundUsage(ctx context.Context, binding UserPlatformQuotaUsageBinding) (UserPlatformQuotaBoundIncrementResult, error)
}

type UserPlatformQuotaBoundUsageCache interface {
	IncrBoundUserPlatformQuotaUsageCache(ctx context.Context, binding UserPlatformQuotaUsageBinding, ttl time.Duration, markDirty bool) (UserPlatformQuotaBoundIncrementResult, error)
}
