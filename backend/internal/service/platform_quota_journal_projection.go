package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	PlatformQuotaJournalSourceClient     = "client_journal"
	PlatformQuotaJournalSourceOrdinary   = "ordinary_journal"
	PlatformQuotaJournalSourceBatchImage = "batch_image_journal"
	PlatformQuotaJournalSourceCombined   = "combined_journals"
)

var (
	ErrPlatformQuotaJournalProjectionInput       = fmt.Errorf("platform quota journal projection: %w", ErrClientPlatformQuotaProjectionInput)
	ErrPlatformQuotaJournalProjectionUnavailable = fmt.Errorf("platform quota journal projection: %w", ErrClientPlatformQuotaProjectionUnavailable)
	ErrPlatformQuotaJournalProjectionHistory     = fmt.Errorf("platform quota journal projection: %w", ErrClientPlatformQuotaProjectionHistory)
	ErrPlatformQuotaJournalProjectionConflict    = errors.New("platform quota journals contain a repeated billing identity")
	platformQuotaJournalFingerprint              = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// PlatformQuotaJournalProjectionRepository is a private read of observed
// sources. It does not establish completeness of any accounting origin.
type PlatformQuotaJournalProjectionRepository interface {
	ReadPlatformQuotaJournalProjection(context.Context, int64, string, time.Time) (*PlatformQuotaJournalProjection, error)
}

type PlatformQuotaJournalProjectionEntry struct {
	Source             string    `json:"-"`
	RequestID          string    `json:"-"`
	APIKeyID           int64     `json:"-"`
	BillingFingerprint string    `json:"-"`
	CapturedAt         time.Time `json:"-"`
	CostUSD            string    `json:"-"`
}

type PlatformQuotaJournalProjectionInput struct {
	QuotaRowID        int64                                 `json:"-"`
	UserID            int64                                 `json:"-"`
	Platform          string                                `json:"-"`
	QuotaCreatedAt    time.Time                             `json:"-"`
	TrackingStartedAt time.Time                             `json:"-"`
	DailyResetAt      *time.Time                            `json:"-"`
	WeeklyResetAt     *time.Time                            `json:"-"`
	MonthlyResetAt    *time.Time                            `json:"-"`
	AsOf              time.Time                             `json:"-"`
	Entries           []PlatformQuotaJournalProjectionEntry `json:"-"`
}

type PlatformQuotaJournalProjection struct {
	Source           string                              `json:"-"`
	Mergeable        bool                                `json:"-"`
	CoverageVerified bool                                `json:"-"`
	QuotaRowID       int64                               `json:"-"`
	UserID           int64                               `json:"-"`
	Platform         string                              `json:"-"`
	AsOf             time.Time                           `json:"-"`
	Daily            ClientPlatformQuotaProjectionWindow `json:"-"`
	Weekly           ClientPlatformQuotaProjectionWindow `json:"-"`
	Monthly          ClientPlatformQuotaProjectionWindow `json:"-"`
}

// ProjectPlatformQuotaJournals first validates mutually exclusive source
// identities, then recomputes one timeline from all raw journals. In particular,
// separately calculated rolling-month totals cannot be added: each source can
// change the other's 720-hour anchors. Repeated billing identities are a conflict
// even when all values agree; this function never silently chooses an origin.
func ProjectPlatformQuotaJournals(input PlatformQuotaJournalProjectionInput) (*PlatformQuotaJournalProjection, error) {
	if len(input.Entries) > MaxClientPlatformQuotaProjectionEntries {
		return nil, ErrPlatformQuotaJournalProjectionUnavailable
	}
	type billingIdentity struct {
		requestID string
		apiKeyID  int64
	}
	seen := make(map[billingIdentity]struct{}, len(input.Entries))
	entries := make([]ClientPlatformQuotaProjectionEntry, 0, len(input.Entries))
	for _, entry := range input.Entries {
		if (entry.Source != PlatformQuotaJournalSourceClient && entry.Source != PlatformQuotaJournalSourceOrdinary &&
			entry.Source != PlatformQuotaJournalSourceBatchImage) ||
			entry.RequestID == "" || len(entry.RequestID) > 255 || !utf8.ValidString(entry.RequestID) ||
			entry.RequestID != strings.TrimSpace(entry.RequestID) || entry.APIKeyID <= 0 ||
			!platformQuotaJournalFingerprint.MatchString(entry.BillingFingerprint) {
			return nil, ErrPlatformQuotaJournalProjectionInput
		}
		key := billingIdentity{requestID: entry.RequestID, apiKeyID: entry.APIKeyID}
		if _, exists := seen[key]; exists {
			return nil, ErrPlatformQuotaJournalProjectionConflict
		}
		seen[key] = struct{}{}
		entries = append(entries, ClientPlatformQuotaProjectionEntry{CapturedAt: entry.CapturedAt, CostUSD: entry.CostUSD})
	}
	projection, err := ProjectClientPlatformQuota(ClientPlatformQuotaProjectionInput{
		QuotaRowID: input.QuotaRowID, UserID: input.UserID, Platform: input.Platform,
		QuotaCreatedAt: input.QuotaCreatedAt, TrackingStartedAt: input.TrackingStartedAt,
		DailyResetAt: input.DailyResetAt, WeeklyResetAt: input.WeeklyResetAt, MonthlyResetAt: input.MonthlyResetAt,
		AsOf: input.AsOf, Entries: entries,
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrClientPlatformQuotaProjectionInput):
			return nil, ErrPlatformQuotaJournalProjectionInput
		case errors.Is(err, ErrClientPlatformQuotaProjectionHistory):
			return nil, ErrPlatformQuotaJournalProjectionHistory
		default:
			return nil, errors.Join(ErrPlatformQuotaJournalProjectionUnavailable, err)
		}
	}
	return &PlatformQuotaJournalProjection{
		Source: PlatformQuotaJournalSourceCombined, Mergeable: false, CoverageVerified: false,
		QuotaRowID: projection.QuotaRowID, UserID: projection.UserID, Platform: projection.Platform, AsOf: projection.AsOf,
		Daily: projection.Daily, Weekly: projection.Weekly, Monthly: projection.Monthly,
	}, nil
}
