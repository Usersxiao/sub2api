package service

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/shopspring/decimal"
)

// MaxClientPlatformQuotaProjectionEntries bounds a complete read. Truncation is
// an error rather than an apparently complete, smaller usage amount.
const MaxClientPlatformQuotaProjectionEntries = 10000

var (
	ErrClientPlatformQuotaProjectionUnavailable = errors.New("client platform quota projection is temporarily unavailable")
	ErrClientPlatformQuotaProjectionHistory     = errors.New("client platform quota projection requires historical reset provenance")
	ErrClientPlatformQuotaProjectionInput       = errors.New("invalid client platform quota projection input")
	clientPlatformQuotaAmount                   = regexp.MustCompile(`^[0-9]+(?:\.[0-9]{1,10})?$`)
)

// ClientPlatformQuotaProjectionRepository is an internal, read-only accounting
// port. Its result is client journal usage, not a merged quota or admission.
type ClientPlatformQuotaProjectionRepository interface {
	ReadClientPlatformQuotaProjection(context.Context, int64, string, time.Time) (*ClientPlatformQuotaProjection, error)
}

type ClientPlatformQuotaProjectionEntry struct {
	CapturedAt time.Time `json:"-"`
	CostUSD    string    `json:"-"`
}

type ClientPlatformQuotaProjectionInput struct {
	QuotaRowID        int64                                `json:"-"`
	UserID            int64                                `json:"-"`
	Platform          string                               `json:"-"`
	QuotaCreatedAt    time.Time                            `json:"-"`
	TrackingStartedAt time.Time                            `json:"-"`
	DailyResetAt      *time.Time                           `json:"-"`
	WeeklyResetAt     *time.Time                           `json:"-"`
	MonthlyResetAt    *time.Time                           `json:"-"`
	AsOf              time.Time                            `json:"-"`
	Entries           []ClientPlatformQuotaProjectionEntry `json:"-"`
}

type ClientPlatformQuotaProjectionWindow struct {
	// Daily and weekly Start/End describe the natural calendar bucket. ResetAt
	// separately identifies a stricter, exclusive lower consumption boundary.
	// A monthly window has no Start/End until an eligible journal entry anchors it.
	Start    *time.Time `json:"-"`
	End      *time.Time `json:"-"`
	ResetAt  *time.Time `json:"-"`
	UsageUSD string     `json:"-"`
}

type ClientPlatformQuotaProjection struct {
	Source     string                              `json:"-"`
	Mergeable  bool                                `json:"-"`
	QuotaRowID int64                               `json:"-"`
	UserID     int64                               `json:"-"`
	Platform   string                              `json:"-"`
	AsOf       time.Time                           `json:"-"`
	Daily      ClientPlatformQuotaProjectionWindow `json:"-"`
	Weekly     ClientPlatformQuotaProjectionWindow `json:"-"`
	Monthly    ClientPlatformQuotaProjectionWindow `json:"-"`
}

type clientPlatformQuotaParsedEntry struct {
	capturedAt time.Time
	cost       decimal.Decimal
}

// ProjectClientPlatformQuota recomputes client-only usage from a complete frozen
// journal read. Sorting a private copy makes late insertion order irrelevant,
// including rolling-month anchors. No legacy counter, cache, or source row is
// changed, and missing pre-tracking reset provenance returns no partial result.
func ProjectClientPlatformQuota(input ClientPlatformQuotaProjectionInput) (*ClientPlatformQuotaProjection, error) {
	if len(input.Entries) > MaxClientPlatformQuotaProjectionEntries {
		return nil, ErrClientPlatformQuotaProjectionUnavailable
	}
	if input.QuotaRowID <= 0 || input.UserID <= 0 || !domain.IsConcretePlatform(input.Platform) ||
		!clientPlatformQuotaTimeValid(input.QuotaCreatedAt) || !clientPlatformQuotaTimeValid(input.TrackingStartedAt) ||
		!clientPlatformQuotaTimeValid(input.AsOf) || input.QuotaCreatedAt.After(input.AsOf) || input.TrackingStartedAt.After(input.AsOf) {
		return nil, ErrClientPlatformQuotaProjectionInput
	}
	for _, reset := range []*time.Time{input.DailyResetAt, input.WeeklyResetAt, input.MonthlyResetAt} {
		if reset != nil && (!clientPlatformQuotaTimeValid(*reset) || reset.After(input.AsOf) ||
			reset.Before(input.QuotaCreatedAt) || reset.Before(input.TrackingStartedAt)) {
			return nil, ErrClientPlatformQuotaProjectionInput
		}
	}
	entries := make([]clientPlatformQuotaParsedEntry, 0, len(input.Entries))
	maxAmount := decimal.New(1, 10)
	for _, entry := range input.Entries {
		if !clientPlatformQuotaTimeValid(entry.CapturedAt) || len(entry.CostUSD) > 21 || !clientPlatformQuotaAmount.MatchString(entry.CostUSD) {
			return nil, ErrClientPlatformQuotaProjectionInput
		}
		cost, err := decimal.NewFromString(entry.CostUSD)
		if err != nil || cost.IsNegative() || cost.GreaterThanOrEqual(maxAmount) {
			return nil, ErrClientPlatformQuotaProjectionInput
		}
		// Real metered journal rows are positive. Accepting an exact zero is
		// harmless, but it must not invent a rolling-month anchor or require
		// provenance for a contribution that consumed no quota.
		if cost.IsZero() || entry.CapturedAt.Before(input.QuotaCreatedAt) || entry.CapturedAt.After(input.AsOf) {
			continue
		}
		entries = append(entries, clientPlatformQuotaParsedEntry{capturedAt: entry.CapturedAt, cost: cost})
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].capturedAt.Before(entries[j].capturedAt) })

	dailyStart := timezone.StartOfDay(input.AsOf)
	weeklyStart := timezone.StartOfWeek(input.AsOf)
	dailyEnd := dailyStart.AddDate(0, 0, 1)
	weeklyEnd := weeklyStart.AddDate(0, 0, 7)
	if !clientPlatformQuotaTimeValid(dailyStart) || !clientPlatformQuotaTimeValid(weeklyStart) ||
		!clientPlatformQuotaTimeValid(dailyEnd) || !clientPlatformQuotaTimeValid(weeklyEnd) {
		return nil, ErrClientPlatformQuotaProjectionInput
	}
	result := &ClientPlatformQuotaProjection{
		Source: "client_journal", Mergeable: false, QuotaRowID: input.QuotaRowID,
		UserID: input.UserID, Platform: input.Platform, AsOf: input.AsOf,
		Daily:   ClientPlatformQuotaProjectionWindow{Start: clientPlatformQuotaTimeCopy(&dailyStart), End: clientPlatformQuotaTimeCopy(&dailyEnd), ResetAt: clientPlatformQuotaTimeCopy(input.DailyResetAt)},
		Weekly:  ClientPlatformQuotaProjectionWindow{Start: clientPlatformQuotaTimeCopy(&weeklyStart), End: clientPlatformQuotaTimeCopy(&weeklyEnd), ResetAt: clientPlatformQuotaTimeCopy(input.WeeklyResetAt)},
		Monthly: ClientPlatformQuotaProjectionWindow{ResetAt: clientPlatformQuotaTimeCopy(input.MonthlyResetAt)},
	}
	daily, weekly, monthly := decimal.Zero, decimal.Zero, decimal.Zero
	const monthDuration = 30 * 24 * time.Hour
	for _, entry := range entries {
		if !entry.capturedAt.Before(dailyStart) && clientPlatformQuotaAfterReset(entry.capturedAt, input.DailyResetAt) {
			if entry.capturedAt.Before(input.TrackingStartedAt) {
				return nil, ErrClientPlatformQuotaProjectionHistory
			}
			daily = daily.Add(entry.cost)
		}
		if !entry.capturedAt.Before(weeklyStart) && clientPlatformQuotaAfterReset(entry.capturedAt, input.WeeklyResetAt) {
			if entry.capturedAt.Before(input.TrackingStartedAt) {
				return nil, ErrClientPlatformQuotaProjectionHistory
			}
			weekly = weekly.Add(entry.cost)
		}
		if !clientPlatformQuotaAfterReset(entry.capturedAt, input.MonthlyResetAt) {
			continue
		}
		// A pre-tracking contribution can change every later rolling anchor,
		// even when that contribution itself appears expired at AsOf.
		if entry.capturedAt.Before(input.TrackingStartedAt) {
			return nil, ErrClientPlatformQuotaProjectionHistory
		}
		if result.Monthly.End == nil || !entry.capturedAt.Before(*result.Monthly.End) {
			start, end := entry.capturedAt, entry.capturedAt.Add(monthDuration)
			if !clientPlatformQuotaTimeValid(end) {
				return nil, ErrClientPlatformQuotaProjectionInput
			}
			result.Monthly.Start = clientPlatformQuotaTimeCopy(&start)
			result.Monthly.End = clientPlatformQuotaTimeCopy(&end)
			monthly = entry.cost
		} else {
			monthly = monthly.Add(entry.cost)
		}
	}
	if result.Monthly.End != nil && !input.AsOf.Before(*result.Monthly.End) {
		result.Monthly.Start, result.Monthly.End = nil, nil
		monthly = decimal.Zero
	}
	result.Daily.UsageUSD = daily.StringFixed(10)
	result.Weekly.UsageUSD = weekly.StringFixed(10)
	result.Monthly.UsageUSD = monthly.StringFixed(10)
	return result, nil
}

func clientPlatformQuotaAfterReset(capturedAt time.Time, reset *time.Time) bool {
	return reset == nil || capturedAt.After(*reset)
}

func clientPlatformQuotaTimeValid(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999
}

func clientPlatformQuotaTimeCopy(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
