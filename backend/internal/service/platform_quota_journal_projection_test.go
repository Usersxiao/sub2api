//go:build unit

package service

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func platformQuotaProjectionInput() PlatformQuotaJournalProjectionInput {
	base := clientPlatformProjectionInput()
	return PlatformQuotaJournalProjectionInput{
		QuotaRowID: base.QuotaRowID, UserID: base.UserID, Platform: base.Platform,
		QuotaCreatedAt: base.QuotaCreatedAt, TrackingStartedAt: base.TrackingStartedAt, AsOf: base.AsOf,
	}
}

func platformQuotaProjectionEntry(source, requestID string, keyID int64, captured time.Time, cost string) PlatformQuotaJournalProjectionEntry {
	return PlatformQuotaJournalProjectionEntry{Source: source, RequestID: requestID, APIKeyID: keyID,
		BillingFingerprint: strings.Repeat("a", 64), CapturedAt: captured, CostUSD: cost}
}

func TestPlatformQuotaJournalProjectionEmptyIsPrivateObservedAccounting(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := platformQuotaProjectionInput()
	result, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "combined_journals", result.Source)
	require.False(t, result.Mergeable)
	require.False(t, result.CoverageVerified)
	require.Equal(t, input.QuotaRowID, result.QuotaRowID)
	require.Equal(t, input.UserID, result.UserID)
	require.Equal(t, input.Platform, result.Platform)
	require.Equal(t, input.AsOf, result.AsOf)
	for _, window := range []ClientPlatformQuotaProjectionWindow{result.Daily, result.Weekly, result.Monthly} {
		require.Equal(t, "0.0000000000", window.UsageUSD)
	}
	require.Nil(t, result.Monthly.Start)
	for _, private := range []any{input, result, result.Daily, platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "private-request", 12, input.AsOf, "1")} {
		encoded, marshalErr := json.Marshal(private)
		require.NoError(t, marshalErr)
		require.JSONEq(t, `{}`, string(encoded))
	}
}

func TestPlatformQuotaJournalProjectionCombinesExactRawCalendarUsage(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := platformQuotaProjectionInput()
	input.Entries = []PlatformQuotaJournalProjectionEntry{
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-month", 1, clientPlatformProjectionTime("2026-10-01T00:00:00Z"), "8"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-week", 2, clientPlatformProjectionTime("2026-10-19T00:00:00Z"), "2"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-day", 1, clientPlatformProjectionTime("2026-10-24T00:00:00Z"), "0.1"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-asof", 2, input.AsOf, "0.0000000001"),
	}
	result, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "0.1000000001", result.Daily.UsageUSD)
	require.Equal(t, "2.1000000001", result.Weekly.UsageUSD)
	require.Equal(t, "10.1000000001", result.Monthly.UsageUSD)
	require.Equal(t, clientPlatformProjectionTime("2026-10-01T00:00:00Z"), *result.Monthly.Start)
	require.Equal(t, 720*time.Hour, result.Monthly.End.Sub(*result.Monthly.Start))
	require.False(t, result.Mergeable)
	require.False(t, result.CoverageVerified)
}

func TestPlatformQuotaJournalProjectionLateCrossSourceEntryRebuildsMonthlyTimeline(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := platformQuotaProjectionInput()
	anchor := input.QuotaCreatedAt
	input.AsOf = anchor.Add(45 * 24 * time.Hour)
	input.Entries = []PlatformQuotaJournalProjectionEntry{
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-day20", 2, anchor.Add(20*24*time.Hour), "2"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-day40", 2, anchor.Add(40*24*time.Hour), "4"),
	}
	before, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "6.0000000000", before.Monthly.UsageUSD)
	// The ordinary event is expired on its own at day45, yet changes the
	// combined anchor and moves client day20 into the earlier 30-day bucket.
	input.Entries = append(input.Entries, platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-day0", 1, anchor, "1"))
	source := append([]PlatformQuotaJournalProjectionEntry(nil), input.Entries...)
	result, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, source, input.Entries)
	require.Equal(t, "4.0000000000", result.Monthly.UsageUSD)
	require.Equal(t, anchor.Add(40*24*time.Hour), *result.Monthly.Start)
	require.Equal(t, anchor.Add(70*24*time.Hour), *result.Monthly.End)
	input.Entries = []PlatformQuotaJournalProjectionEntry{source[2], source[1], source[0]}
	reordered, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, result, reordered)
	repeated, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, result, repeated)
}

func TestPlatformQuotaJournalProjectionRejectsEveryRepeatedBillingIdentity(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	for _, kind := range []string{"exact", "cross-source", "different-hash", "different-cost", "outside-asof", "zero"} {
		t.Run(kind, func(t *testing.T) {
			input := platformQuotaProjectionInput()
			first := platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "same-request", 2, input.AsOf, "1")
			second := first
			switch kind {
			case "cross-source":
				second.Source = PlatformQuotaJournalSourceOrdinary
			case "different-hash":
				second.BillingFingerprint = strings.Repeat("b", 64)
			case "different-cost":
				second.CostUSD = "2"
			case "outside-asof":
				second.CapturedAt = input.AsOf.Add(time.Hour)
			case "zero":
				first.CostUSD, second.CostUSD = "0", "0"
			}
			input.Entries = []PlatformQuotaJournalProjectionEntry{first, second}
			result, err := ProjectPlatformQuotaJournals(input)
			require.ErrorIs(t, err, ErrPlatformQuotaJournalProjectionConflict)
			require.Nil(t, result)
		})
	}
	input := platformQuotaProjectionInput()
	input.Entries = []PlatformQuotaJournalProjectionEntry{
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "same-request", 1, input.AsOf, "1"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "same-request", 2, input.AsOf, "2"),
	}
	result, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err, "request IDs are scoped by API key, independently of the source label")
	require.Equal(t, "3.0000000000", result.Monthly.UsageUSD)
}

func TestPlatformQuotaJournalProjectionSharesWindowScopedResetAcrossSources(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	reset := clientPlatformProjectionTime("2026-10-24T12:00:00Z")
	for _, kind := range []string{"daily", "weekly", "monthly"} {
		t.Run(kind, func(t *testing.T) {
			input := platformQuotaProjectionInput()
			input.Entries = []PlatformQuotaJournalProjectionEntry{
				platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-before", 1, reset.Add(-time.Hour), "1"),
				platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-at-reset", 2, reset, "4"),
				platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-after", 2, reset.Add(time.Hour), "8"),
			}
			switch kind {
			case "daily":
				input.DailyResetAt = &reset
			case "weekly":
				input.WeeklyResetAt = &reset
			case "monthly":
				input.MonthlyResetAt = &reset
			}
			result, err := ProjectPlatformQuotaJournals(input)
			require.NoError(t, err)
			for name, window := range map[string]ClientPlatformQuotaProjectionWindow{"daily": result.Daily, "weekly": result.Weekly, "monthly": result.Monthly} {
				if name == kind {
					require.Equal(t, "8.0000000000", window.UsageUSD)
					require.Equal(t, reset, *window.ResetAt)
					require.NotSame(t, &reset, window.ResetAt)
				} else {
					require.Equal(t, "13.0000000000", window.UsageUSD)
					require.Nil(t, window.ResetAt)
				}
			}
		})
	}
}

func TestPlatformQuotaJournalProjectionMixedSourcesRespectDSTCalendarBoundaries(t *testing.T) {
	loc := clientPlatformProjectionTimezone(t, "America/New_York")
	for _, tc := range []struct {
		month                        time.Month
		day, dailyHours, weeklyHours int
	}{
		{time.March, 8, 23, 167}, {time.November, 1, 25, 169},
	} {
		t.Run(tc.month.String(), func(t *testing.T) {
			input := platformQuotaProjectionInput()
			start := time.Date(2026, tc.month, tc.day, 0, 0, 0, 0, loc)
			input.AsOf = start.Add(18 * time.Hour)
			input.Entries = []PlatformQuotaJournalProjectionEntry{
				platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-before-day", 1, start.Add(-time.Nanosecond), "1"),
				platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-day-start", 2, start, "2"),
			}
			result, err := ProjectPlatformQuotaJournals(input)
			require.NoError(t, err)
			require.Equal(t, "2.0000000000", result.Daily.UsageUSD)
			require.Equal(t, "3.0000000000", result.Weekly.UsageUSD)
			require.Equal(t, time.Duration(tc.dailyHours)*time.Hour, result.Daily.End.Sub(*result.Daily.Start))
			require.Equal(t, time.Duration(tc.weeklyHours)*time.Hour, result.Weekly.End.Sub(*result.Weekly.Start))
		})
	}
}

func TestPlatformQuotaJournalProjectionMonthlyBoundaryIsExact720Hours(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := platformQuotaProjectionInput()
	anchor := input.QuotaCreatedAt
	end := anchor.Add(720 * time.Hour)
	input.Entries = []PlatformQuotaJournalProjectionEntry{
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-anchor", 2, anchor, "1"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-before-end", 1, end.Add(-time.Nanosecond), "2"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-at-end", 1, end, "4"),
	}
	input.AsOf = end.Add(-time.Nanosecond)
	before, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "3.0000000000", before.Monthly.UsageUSD)
	input.AsOf = end
	result, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "4.0000000000", result.Monthly.UsageUSD)
	require.Equal(t, end, *result.Monthly.Start)
	input.Entries = input.Entries[:2]
	expired, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Nil(t, expired.Monthly.Start)
	require.Nil(t, expired.Monthly.End)
	require.Equal(t, "0.0000000000", expired.Monthly.UsageUSD)
}

func TestPlatformQuotaJournalProjectionHistoryReturnsNoPartialSourceNumbers(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := platformQuotaProjectionInput()
	input.TrackingStartedAt = clientPlatformProjectionTime("2026-10-24T10:00:00Z")
	input.Entries = []PlatformQuotaJournalProjectionEntry{
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-unknown-history", 1, input.TrackingStartedAt.Add(-time.Hour), "1"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-known", 2, input.TrackingStartedAt.Add(time.Hour), "2"),
	}
	result, err := ProjectPlatformQuotaJournals(input)
	require.ErrorIs(t, err, ErrPlatformQuotaJournalProjectionHistory)
	require.ErrorIs(t, err, ErrClientPlatformQuotaProjectionHistory)
	require.Nil(t, result)
	input.DailyResetAt, input.WeeklyResetAt = &input.TrackingStartedAt, &input.TrackingStartedAt
	result, err = ProjectPlatformQuotaJournals(input)
	require.ErrorIs(t, err, ErrPlatformQuotaJournalProjectionHistory, "monthly history needs its own reset")
	require.Nil(t, result)
	input.MonthlyResetAt = &input.TrackingStartedAt
	result, err = ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "2.0000000000", result.Monthly.UsageUSD)
	require.False(t, result.CoverageVerified, "reset provenance is not a financial-source coverage certificate")
}

func TestPlatformQuotaJournalProjectionRejectsInvalidIdentityScopeAndAmounts(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	for _, tc := range []struct {
		name   string
		mutate func(*PlatformQuotaJournalProjectionInput)
	}{
		{name: "source", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].Source = "combined_journals" }},
		{name: "missing-request", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].RequestID = "" }},
		{name: "padded-request", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].RequestID = " request" }},
		{name: "oversized-request", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].RequestID = strings.Repeat("a", 256) }},
		{name: "invalid-utf8", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].RequestID = string([]byte{255}) }},
		{name: "key", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].APIKeyID = 0 }},
		{name: "missing-fingerprint", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].BillingFingerprint = "" }},
		{name: "noncanonical-fingerprint", mutate: func(i *PlatformQuotaJournalProjectionInput) {
			i.Entries[0].BillingFingerprint = strings.Repeat("A", 64)
		}},
		{name: "user-scope", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.UserID = 0 }},
		{name: "row-scope", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.QuotaRowID = 0 }},
		{name: "platform-scope", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Platform = PlatformComposite }},
		{name: "tracking-in-future", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.TrackingStartedAt = i.AsOf.Add(time.Second) }},
		{name: "missing-time", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].CapturedAt = time.Time{} }},
		{name: "negative-cost", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].CostUSD = "-1" }},
		{name: "nan-cost", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].CostUSD = "NaN" }},
		{name: "overprecision", mutate: func(i *PlatformQuotaJournalProjectionInput) { i.Entries[0].CostUSD = "0.00000000001" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := platformQuotaProjectionInput()
			input.Entries = []PlatformQuotaJournalProjectionEntry{platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "request", 1, input.AsOf, "1")}
			tc.mutate(&input)
			result, err := ProjectPlatformQuotaJournals(input)
			require.ErrorIs(t, err, ErrPlatformQuotaJournalProjectionInput)
			require.ErrorIs(t, err, ErrClientPlatformQuotaProjectionInput)
			require.Nil(t, result)
		})
	}
}

func TestPlatformQuotaJournalProjectionBoundsBothSourcesTogether(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := platformQuotaProjectionInput()
	for index := 0; index < MaxClientPlatformQuotaProjectionEntries; index++ {
		source := []string{PlatformQuotaJournalSourceClient, PlatformQuotaJournalSourceOrdinary, PlatformQuotaJournalSourceBatchImage}[index%3]
		input.Entries = append(input.Entries, platformQuotaProjectionEntry(source, "request-"+strconv.Itoa(index), 1, input.AsOf, "0.0000000001"))
	}
	result, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "0.0000010000", result.Monthly.UsageUSD)
	input.Entries = append(input.Entries, platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "request-over-limit", 2, input.AsOf, "1"))
	result, err = ProjectPlatformQuotaJournals(input)
	require.ErrorIs(t, err, ErrPlatformQuotaJournalProjectionUnavailable)
	require.ErrorIs(t, err, ErrClientPlatformQuotaProjectionUnavailable)
	require.Nil(t, result)
}

func TestPlatformQuotaJournalProjectionBatchCaptureSharesRawTimeline(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := platformQuotaProjectionInput()
	anchor := input.QuotaCreatedAt
	input.AsOf = anchor.Add(45 * 24 * time.Hour)
	input.Entries = []PlatformQuotaJournalProjectionEntry{
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceClient, "client-day20", 2, anchor.Add(20*24*time.Hour), "2"),
		platformQuotaProjectionEntry(PlatformQuotaJournalSourceOrdinary, "ordinary-day40", 1, anchor.Add(40*24*time.Hour), "4"),
	}
	before, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "6.0000000000", before.Monthly.UsageUSD)
	input.Entries = append(input.Entries, platformQuotaProjectionEntry(PlatformQuotaJournalSourceBatchImage,
		"batch_image_capture:old", 3, anchor, "0.0000000001"))
	result, err := ProjectPlatformQuotaJournals(input)
	require.NoError(t, err)
	require.Equal(t, "4.0000000000", result.Monthly.UsageUSD)
	require.Equal(t, anchor.Add(40*24*time.Hour), *result.Monthly.Start)
	require.False(t, result.CoverageVerified)
	require.False(t, result.Mergeable)
}

func TestPlatformQuotaJournalProjectionBatchIdentityConflictsWithEitherOrigin(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	for _, other := range []string{PlatformQuotaJournalSourceClient, PlatformQuotaJournalSourceOrdinary} {
		t.Run(other, func(t *testing.T) {
			input := platformQuotaProjectionInput()
			input.Entries = []PlatformQuotaJournalProjectionEntry{
				platformQuotaProjectionEntry(other, "batch_image_capture:same", 3, input.AsOf, "0.1"),
				platformQuotaProjectionEntry(PlatformQuotaJournalSourceBatchImage, "batch_image_capture:same", 3, input.AsOf, "0.1"),
			}
			result, err := ProjectPlatformQuotaJournals(input)
			require.ErrorIs(t, err, ErrPlatformQuotaJournalProjectionConflict)
			require.Nil(t, result)
		})
	}
}
