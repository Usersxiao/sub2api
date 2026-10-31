package service

import (
	"context"
	"errors"
	"time"
)

var (
	ErrUserPlatformQuotaObservationInvalid      = errors.New("invalid user platform quota ordered observation")
	ErrUserPlatformQuotaObservationIncomparable = errors.New("incomparable user platform quota observations")
)

// UserPlatformQuotaObservationToken identifies a committed observation of one
// physical quota row. Revision orders changes only inside this exact domain;
// neither lineage nor control generation is a sortable timestamp. The token
// does not certify usage coverage, a recovery baseline, or cache write access.
type UserPlatformQuotaObservationToken struct {
	WriterMode        string `json:"-"`
	WriterEpoch       int64  `json:"-"`
	QuotaRowID        int64  `json:"-"`
	ControlGeneration int64  `json:"-"`
	Lineage           string `json:"-"`
	Revision          int64  `json:"-"`
}

func ValidateUserPlatformQuotaObservationToken(token UserPlatformQuotaObservationToken) error {
	if ValidateUserPlatformQuotaWriterAuthority(UserPlatformQuotaWriterAuthority{
		Mode: token.WriterMode, Epoch: token.WriterEpoch,
	}) != nil || token.QuotaRowID <= 0 || token.ControlGeneration <= 0 || token.Revision <= 0 ||
		!validUserPlatformQuotaObservationLineage(token.Lineage) {
		return ErrUserPlatformQuotaObservationInvalid
	}
	return nil
}

func validUserPlatformQuotaObservationLineage(lineage string) bool {
	if len(lineage) != 36 {
		return false
	}
	nonzero := false
	for i, ch := range lineage {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if ch != '-' {
				return false
			}
			continue
		}
		if !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f') {
			return false
		}
		if ch != '0' {
			nonzero = true
		}
	}
	return nonzero
}

type UserPlatformQuotaObservationOrder string

const (
	QuotaObservationOlder UserPlatformQuotaObservationOrder = "older"
	QuotaObservationEqual UserPlatformQuotaObservationOrder = "equal"
	QuotaObservationNewer UserPlatformQuotaObservationOrder = "newer"
)

// CompareUserPlatformQuotaObservationTokens compares candidate to current.
// Equal means equal ordering metadata, not proof that caller-supplied payloads
// match. Cross-row, lineage, writer-domain and control changes need a separately
// verified handoff; they never become newer by lexical or numerical comparison.
func CompareUserPlatformQuotaObservationTokens(candidate, current UserPlatformQuotaObservationToken) (UserPlatformQuotaObservationOrder, error) {
	if ValidateUserPlatformQuotaObservationToken(candidate) != nil || ValidateUserPlatformQuotaObservationToken(current) != nil {
		return "", ErrUserPlatformQuotaObservationInvalid
	}
	if candidate.WriterMode != current.WriterMode || candidate.WriterEpoch != current.WriterEpoch ||
		candidate.QuotaRowID != current.QuotaRowID || candidate.ControlGeneration != current.ControlGeneration ||
		candidate.Lineage != current.Lineage {
		return "", ErrUserPlatformQuotaObservationIncomparable
	}
	if candidate.Revision < current.Revision {
		return QuotaObservationOlder, nil
	}
	if candidate.Revision > current.Revision {
		return QuotaObservationNewer, nil
	}
	return QuotaObservationEqual, nil
}

// All amounts retain PostgreSQL NUMERIC text and all window anchors retain
// database precision. This private single-snapshot observation is deliberately
// separate from public quota DTOs, legacy float adapters and Redis schema v1.
type UserPlatformQuotaOrderedObservation struct {
	Token              UserPlatformQuotaObservationToken `json:"-"`
	UserID             int64                             `json:"-"`
	Platform           string                            `json:"-"`
	DailyLimitUSD      *string                           `json:"-"`
	WeeklyLimitUSD     *string                           `json:"-"`
	MonthlyLimitUSD    *string                           `json:"-"`
	DailyUsageUSD      string                            `json:"-"`
	WeeklyUsageUSD     string                            `json:"-"`
	MonthlyUsageUSD    string                            `json:"-"`
	DailyWindowStart   *time.Time                        `json:"-"`
	WeeklyWindowStart  *time.Time                        `json:"-"`
	MonthlyWindowStart *time.Time                        `json:"-"`
}

// A missing active row returns nil only after the fixed writer authority has
// been checked. Implementations must not initialize or adopt an authority here.
type UserPlatformQuotaOrderedObservationRepository interface {
	GetOrderedUserPlatformQuotaObservation(context.Context, int64, string) (*UserPlatformQuotaOrderedObservation, error)
}
