package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

var (
	ErrUserPlatformQuotaCacheCandidateInvalid  = errors.New("invalid user platform quota cache candidate")
	ErrUserPlatformQuotaCacheCandidateConflict = errors.New("user platform quota cache candidates conflict at the same revision")
	userPlatformQuotaCacheCandidateAmount      = regexp.MustCompile(`^(?:0|[1-9][0-9]{0,9})(?:\.[0-9]{1,10})?$`)
)

// A candidate freezes a validated, exact observation and its durable cache
// domain. It establishes content identity, not a usage baseline or write right.
// Private fields and copied accessors keep its canonical contents immutable.
type UserPlatformQuotaCacheCandidate struct {
	domain      UserPlatformQuotaCacheDomain
	observation UserPlatformQuotaOrderedObservation
	fingerprint string
}

// This optional private read port does not publish Redis entries or change
// quota admission, delivery state, writer modes, or baseline qualification.
type UserPlatformQuotaCacheCandidateRepository interface {
	GetUserPlatformQuotaCacheCandidate(context.Context, int64, string) (*UserPlatformQuotaCacheCandidate, error)
}

func PrepareUserPlatformQuotaCacheCandidate(cacheDomain UserPlatformQuotaCacheDomain, observation *UserPlatformQuotaOrderedObservation) (*UserPlatformQuotaCacheCandidate, error) {
	if ValidateUserPlatformQuotaCacheDomain(cacheDomain) != nil || observation == nil ||
		ValidateUserPlatformQuotaObservationToken(observation.Token) != nil ||
		observation.Token.WriterMode != cacheDomain.WriterAuthority.Mode ||
		observation.Token.WriterEpoch != cacheDomain.WriterAuthority.Epoch ||
		observation.UserID <= 0 || !domain.IsConcretePlatform(observation.Platform) {
		return nil, ErrUserPlatformQuotaCacheCandidateInvalid
	}

	frozen := copyUserPlatformQuotaCacheCandidateObservation(observation)
	limits := []**string{&frozen.DailyLimitUSD, &frozen.WeeklyLimitUSD, &frozen.MonthlyLimitUSD}
	usages := []*string{&frozen.DailyUsageUSD, &frozen.WeeklyUsageUSD, &frozen.MonthlyUsageUSD}
	starts := []**time.Time{&frozen.DailyWindowStart, &frozen.WeeklyWindowStart, &frozen.MonthlyWindowStart}
	for i := range usages {
		if *limits[i] != nil {
			amount, err := canonicalUserPlatformQuotaCacheCandidateAmount(**limits[i])
			if err != nil {
				return nil, err
			}
			*limits[i] = &amount
		}
		usage, err := canonicalUserPlatformQuotaCacheCandidateAmount(*usages[i])
		if err != nil {
			return nil, err
		}
		*usages[i] = usage
		if *starts[i] == nil {
			if usage != "0.0000000000" {
				return nil, ErrUserPlatformQuotaCacheCandidateInvalid
			}
			continue
		}
		start := (**starts[i]).UTC()
		if start.IsZero() || start.Year() < 1 || start.Year() > 9999 || start.Nanosecond()%1000 != 0 {
			return nil, ErrUserPlatformQuotaCacheCandidateInvalid
		}
		*starts[i] = &start
	}
	fingerprint, err := fingerprintUserPlatformQuotaCacheCandidate(cacheDomain, frozen)
	if err != nil {
		return nil, err
	}
	return &UserPlatformQuotaCacheCandidate{domain: cacheDomain, observation: *frozen, fingerprint: fingerprint}, nil
}

func (candidate *UserPlatformQuotaCacheCandidate) Domain() UserPlatformQuotaCacheDomain {
	if candidate == nil {
		return UserPlatformQuotaCacheDomain{}
	}
	return candidate.domain
}

func (candidate *UserPlatformQuotaCacheCandidate) Observation() *UserPlatformQuotaOrderedObservation {
	if candidate == nil {
		return nil
	}
	return copyUserPlatformQuotaCacheCandidateObservation(&candidate.observation)
}

func (candidate *UserPlatformQuotaCacheCandidate) Fingerprint() string {
	if candidate == nil {
		return ""
	}
	return candidate.fingerprint
}

// Compare checks exact domain and scope before the row's durable revision.
// Equal metadata with unequal canonical contents is a conflict; neither payload
// is chosen merely because its digest or UUID sorts later.
func CompareUserPlatformQuotaCacheCandidates(candidate, current *UserPlatformQuotaCacheCandidate) (UserPlatformQuotaObservationOrder, error) {
	if !validUserPlatformQuotaCacheCandidate(candidate) || !validUserPlatformQuotaCacheCandidate(current) {
		return "", ErrUserPlatformQuotaCacheCandidateInvalid
	}
	if candidate.domain != current.domain || candidate.observation.UserID != current.observation.UserID ||
		candidate.observation.Platform != current.observation.Platform {
		return "", ErrUserPlatformQuotaObservationIncomparable
	}
	order, err := CompareUserPlatformQuotaObservationTokens(candidate.observation.Token, current.observation.Token)
	if err != nil {
		return "", err
	}
	if order == QuotaObservationEqual && candidate.fingerprint != current.fingerprint {
		return "", ErrUserPlatformQuotaCacheCandidateConflict
	}
	return order, nil
}

func canonicalUserPlatformQuotaCacheCandidateAmount(value string) (string, error) {
	if len(value) == 0 || len(value) > 21 || !userPlatformQuotaCacheCandidateAmount.MatchString(value) {
		return "", ErrUserPlatformQuotaCacheCandidateInvalid
	}
	integer, fraction, _ := strings.Cut(value, ".")
	return integer + "." + fraction + strings.Repeat("0", 10-len(fraction)), nil
}

func copyUserPlatformQuotaCacheCandidatePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyUserPlatformQuotaCacheCandidateObservation(observation *UserPlatformQuotaOrderedObservation) *UserPlatformQuotaOrderedObservation {
	if observation == nil {
		return nil
	}
	copy := *observation
	copy.DailyLimitUSD = copyUserPlatformQuotaCacheCandidatePointer(observation.DailyLimitUSD)
	copy.WeeklyLimitUSD = copyUserPlatformQuotaCacheCandidatePointer(observation.WeeklyLimitUSD)
	copy.MonthlyLimitUSD = copyUserPlatformQuotaCacheCandidatePointer(observation.MonthlyLimitUSD)
	copy.DailyWindowStart = copyUserPlatformQuotaCacheCandidatePointer(observation.DailyWindowStart)
	copy.WeeklyWindowStart = copyUserPlatformQuotaCacheCandidatePointer(observation.WeeklyWindowStart)
	copy.MonthlyWindowStart = copyUserPlatformQuotaCacheCandidatePointer(observation.MonthlyWindowStart)
	return &copy
}

func equalUserPlatformQuotaCacheCandidatePointer[T comparable](left, right *T) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validUserPlatformQuotaCacheCandidate(candidate *UserPlatformQuotaCacheCandidate) bool {
	if candidate == nil {
		return false
	}
	prepared, err := PrepareUserPlatformQuotaCacheCandidate(candidate.domain, &candidate.observation)
	if err != nil || candidate.fingerprint != prepared.fingerprint {
		return false
	}
	actual, canonical := &candidate.observation, &prepared.observation
	return actual.Token == canonical.Token && actual.UserID == canonical.UserID && actual.Platform == canonical.Platform &&
		actual.DailyUsageUSD == canonical.DailyUsageUSD && actual.WeeklyUsageUSD == canonical.WeeklyUsageUSD && actual.MonthlyUsageUSD == canonical.MonthlyUsageUSD &&
		equalUserPlatformQuotaCacheCandidatePointer(actual.DailyLimitUSD, canonical.DailyLimitUSD) &&
		equalUserPlatformQuotaCacheCandidatePointer(actual.WeeklyLimitUSD, canonical.WeeklyLimitUSD) &&
		equalUserPlatformQuotaCacheCandidatePointer(actual.MonthlyLimitUSD, canonical.MonthlyLimitUSD) &&
		equalUserPlatformQuotaCacheCandidatePointer(actual.DailyWindowStart, canonical.DailyWindowStart) &&
		equalUserPlatformQuotaCacheCandidatePointer(actual.WeeklyWindowStart, canonical.WeeklyWindowStart) &&
		equalUserPlatformQuotaCacheCandidatePointer(actual.MonthlyWindowStart, canonical.MonthlyWindowStart)
}

func userPlatformQuotaCacheCandidateTimestamp(value *time.Time) *string {
	if value == nil {
		return nil
	}
	text := value.Format("2006-01-02T15:04:05.000000Z")
	return &text
}

func fingerprintUserPlatformQuotaCacheCandidate(cacheDomain UserPlatformQuotaCacheDomain, observation *UserPlatformQuotaOrderedObservation) (string, error) {
	// Keep every field explicit so future DTO or serializer changes cannot
	// silently change content identity. No read clock participates in this hash.
	canonical := struct {
		SchemaVersion      int64   `json:"schema_version"`
		Namespace          string  `json:"namespace"`
		DomainWriterMode   string  `json:"domain_writer_mode"`
		DomainWriterEpoch  int64   `json:"domain_writer_epoch"`
		TokenWriterMode    string  `json:"token_writer_mode"`
		TokenWriterEpoch   int64   `json:"token_writer_epoch"`
		QuotaRowID         int64   `json:"quota_row_id"`
		ControlGeneration  int64   `json:"control_generation"`
		Lineage            string  `json:"lineage"`
		Revision           int64   `json:"revision"`
		UserID             int64   `json:"user_id"`
		Platform           string  `json:"platform"`
		DailyLimitUSD      *string `json:"daily_limit_usd"`
		WeeklyLimitUSD     *string `json:"weekly_limit_usd"`
		MonthlyLimitUSD    *string `json:"monthly_limit_usd"`
		DailyUsageUSD      string  `json:"daily_usage_usd"`
		WeeklyUsageUSD     string  `json:"weekly_usage_usd"`
		MonthlyUsageUSD    string  `json:"monthly_usage_usd"`
		DailyWindowStart   *string `json:"daily_window_start"`
		WeeklyWindowStart  *string `json:"weekly_window_start"`
		MonthlyWindowStart *string `json:"monthly_window_start"`
	}{
		SchemaVersion: cacheDomain.SchemaVersion, Namespace: cacheDomain.Namespace,
		DomainWriterMode: cacheDomain.WriterAuthority.Mode, DomainWriterEpoch: cacheDomain.WriterAuthority.Epoch,
		TokenWriterMode: observation.Token.WriterMode, TokenWriterEpoch: observation.Token.WriterEpoch,
		QuotaRowID: observation.Token.QuotaRowID, ControlGeneration: observation.Token.ControlGeneration,
		Lineage: observation.Token.Lineage, Revision: observation.Token.Revision,
		UserID: observation.UserID, Platform: observation.Platform,
		DailyLimitUSD: observation.DailyLimitUSD, WeeklyLimitUSD: observation.WeeklyLimitUSD, MonthlyLimitUSD: observation.MonthlyLimitUSD,
		DailyUsageUSD: observation.DailyUsageUSD, WeeklyUsageUSD: observation.WeeklyUsageUSD, MonthlyUsageUSD: observation.MonthlyUsageUSD,
		DailyWindowStart:   userPlatformQuotaCacheCandidateTimestamp(observation.DailyWindowStart),
		WeeklyWindowStart:  userPlatformQuotaCacheCandidateTimestamp(observation.WeeklyWindowStart),
		MonthlyWindowStart: userPlatformQuotaCacheCandidateTimestamp(observation.MonthlyWindowStart),
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", ErrUserPlatformQuotaCacheCandidateInvalid
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
