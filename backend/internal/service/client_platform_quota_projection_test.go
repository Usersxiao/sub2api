//go:build unit

package service

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/stretchr/testify/require"
)

func clientPlatformProjectionTimezone(t *testing.T, name string) *time.Location {
	t.Helper()
	previousName, previousLocal := timezone.Name(), time.Local
	t.Cleanup(func() {
		// LoadLocation("Local") resolves time.Local, so restore that pointer
		// before restoring an initially unconfigured timezone package.
		time.Local = previousLocal
		require.NoError(t, timezone.Init(previousName))
		time.Local = previousLocal
	})
	require.NoError(t, timezone.Init(name))
	return timezone.Location()
}

func clientPlatformProjectionTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func clientPlatformProjectionInput() ClientPlatformQuotaProjectionInput {
	return ClientPlatformQuotaProjectionInput{
		QuotaRowID: 7, UserID: 8, Platform: PlatformOpenAI,
		QuotaCreatedAt:    clientPlatformProjectionTime("2026-01-01T00:00:00Z"),
		TrackingStartedAt: clientPlatformProjectionTime("2026-01-01T00:00:00Z"),
		AsOf:              clientPlatformProjectionTime("2026-10-24T14:00:00Z"),
	}
}

func TestClientPlatformQuotaProjectionEmptyIsPrivateClientOnlyAccounting(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := clientPlatformProjectionInput()
	result, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, "client_journal", result.Source)
	require.False(t, result.Mergeable)
	require.Equal(t, input.QuotaRowID, result.QuotaRowID)
	require.Equal(t, input.UserID, result.UserID)
	require.Equal(t, input.Platform, result.Platform)
	require.Equal(t, input.AsOf, result.AsOf)
	require.Equal(t, clientPlatformProjectionTime("2026-10-24T00:00:00Z"), *result.Daily.Start)
	require.Equal(t, clientPlatformProjectionTime("2026-10-25T00:00:00Z"), *result.Daily.End)
	require.Equal(t, clientPlatformProjectionTime("2026-10-19T00:00:00Z"), *result.Weekly.Start)
	require.Equal(t, clientPlatformProjectionTime("2026-10-26T00:00:00Z"), *result.Weekly.End)
	for _, window := range []ClientPlatformQuotaProjectionWindow{result.Daily, result.Weekly, result.Monthly} {
		require.Equal(t, "0.0000000000", window.UsageUSD)
	}
	require.Nil(t, result.Monthly.Start)
	require.Nil(t, result.Monthly.End)
	for _, private := range []any{input, result, result.Daily, ClientPlatformQuotaProjectionEntry{CapturedAt: input.AsOf, CostUSD: "1"}} {
		encoded, marshalErr := json.Marshal(private)
		require.NoError(t, marshalErr)
		require.JSONEq(t, `{}`, string(encoded))
	}
}

func TestClientPlatformQuotaProjectionCalendarWindowsRespectDST(t *testing.T) {
	loc := clientPlatformProjectionTimezone(t, "America/New_York")
	for _, tc := range []struct {
		name                     string
		month                    time.Month
		day, dayHours, weekHours int
	}{
		{name: "spring-forward", month: time.March, day: 8, dayHours: 23, weekHours: 167},
		{name: "fall-back", month: time.November, day: 1, dayHours: 25, weekHours: 169},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := clientPlatformProjectionInput()
			input.AsOf = time.Date(2026, tc.month, tc.day, 20, 0, 0, 0, loc)
			start := time.Date(2026, tc.month, tc.day, 0, 0, 0, 0, loc)
			input.Entries = []ClientPlatformQuotaProjectionEntry{
				{CapturedAt: start.Add(-time.Nanosecond), CostUSD: "2"},
				{CapturedAt: start, CostUSD: "1"},
				{CapturedAt: start.AddDate(0, 0, 1), CostUSD: "4"},
			}
			result, err := ProjectClientPlatformQuota(input)
			require.NoError(t, err)
			require.Equal(t, start, *result.Daily.Start)
			require.Equal(t, time.Duration(tc.dayHours)*time.Hour, result.Daily.End.Sub(*result.Daily.Start))
			require.Equal(t, time.Duration(tc.weekHours)*time.Hour, result.Weekly.End.Sub(*result.Weekly.Start))
			require.Equal(t, time.Monday, result.Weekly.Start.Weekday())
			require.Equal(t, "1.0000000000", result.Daily.UsageUSD)
			require.Equal(t, "3.0000000000", result.Weekly.UsageUSD)
		})
	}
}

func TestClientPlatformQuotaProjectionMonthlyUsesExact720Hours(t *testing.T) {
	loc := clientPlatformProjectionTimezone(t, "America/New_York")
	anchor := time.Date(2026, time.March, 1, 12, 0, 0, 0, loc)
	end := anchor.Add(720 * time.Hour)
	require.Equal(t, 13, end.In(loc).Hour(), "elapsed 720h crosses the DST transition")
	input := clientPlatformProjectionInput()
	input.Entries = []ClientPlatformQuotaProjectionEntry{
		{CapturedAt: anchor, CostUSD: "1"},
		{CapturedAt: end.Add(-time.Nanosecond), CostUSD: "2"},
		{CapturedAt: end, CostUSD: "4"},
	}
	input.AsOf = end.Add(-time.Nanosecond)
	before, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, anchor, *before.Monthly.Start)
	require.Equal(t, end, *before.Monthly.End)
	require.Equal(t, "3.0000000000", before.Monthly.UsageUSD)
	input.AsOf = end
	atBoundary, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, end, *atBoundary.Monthly.Start)
	require.Equal(t, "4.0000000000", atBoundary.Monthly.UsageUSD)
	input.Entries = input.Entries[:2]
	expired, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Nil(t, expired.Monthly.Start)
	require.Nil(t, expired.Monthly.End)
	require.Equal(t, "0.0000000000", expired.Monthly.UsageUSD)
	input.AsOf = end.Add(400 * 24 * time.Hour)
	gap, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Nil(t, gap.Monthly.Start)
	require.Equal(t, "0.0000000000", gap.Monthly.UsageUSD)
}

func TestClientPlatformQuotaProjectionResetsOnlyTheRequestedWindow(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	reset := clientPlatformProjectionTime("2026-10-24T12:00:00Z")
	for _, tc := range []struct {
		name                   string
		set                    func(*ClientPlatformQuotaProjectionInput)
		daily, weekly, monthly string
	}{
		{name: "daily", set: func(i *ClientPlatformQuotaProjectionInput) { i.DailyResetAt = &reset }, daily: "4.0000000000", weekly: "7.0000000000", monthly: "7.0000000000"},
		{name: "weekly", set: func(i *ClientPlatformQuotaProjectionInput) { i.WeeklyResetAt = &reset }, daily: "7.0000000000", weekly: "4.0000000000", monthly: "7.0000000000"},
		{name: "monthly", set: func(i *ClientPlatformQuotaProjectionInput) { i.MonthlyResetAt = &reset }, daily: "7.0000000000", weekly: "7.0000000000", monthly: "4.0000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := clientPlatformProjectionInput()
			input.Entries = []ClientPlatformQuotaProjectionEntry{
				{CapturedAt: reset.Add(-time.Hour), CostUSD: "1"},
				{CapturedAt: reset, CostUSD: "2"},
				{CapturedAt: reset.Add(time.Hour), CostUSD: "4"},
			}
			tc.set(&input)
			result, err := ProjectClientPlatformQuota(input)
			require.NoError(t, err)
			require.Equal(t, tc.daily, result.Daily.UsageUSD)
			require.Equal(t, tc.weekly, result.Weekly.UsageUSD)
			require.Equal(t, tc.monthly, result.Monthly.UsageUSD)
			require.Equal(t, clientPlatformProjectionTime("2026-10-24T00:00:00Z"), *result.Daily.Start, "calendar start and reset provenance remain separate")
			for _, window := range []ClientPlatformQuotaProjectionWindow{result.Daily, result.Weekly, result.Monthly} {
				if window.ResetAt != nil {
					require.Equal(t, reset, *window.ResetAt)
					require.NotSame(t, &reset, window.ResetAt)
				}
			}
		})
	}
}

func TestClientPlatformQuotaProjectionLateEntriesRebuildMonthlyAnchorsWithoutMutation(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	anchor := clientPlatformProjectionTime("2026-01-01T00:00:00Z")
	input := clientPlatformProjectionInput()
	input.AsOf = anchor.Add(45 * 24 * time.Hour)
	input.Entries = []ClientPlatformQuotaProjectionEntry{
		{CapturedAt: anchor.Add(20 * 24 * time.Hour), CostUSD: "2"},
		{CapturedAt: anchor.Add(40 * 24 * time.Hour), CostUSD: "4"},
	}
	initial, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, "6.0000000000", initial.Monthly.UsageUSD)
	input.Entries = append(input.Entries, ClientPlatformQuotaProjectionEntry{CapturedAt: anchor, CostUSD: "1"})
	source := append([]ClientPlatformQuotaProjectionEntry(nil), input.Entries...)
	late, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, source, input.Entries)
	require.Equal(t, anchor.Add(40*24*time.Hour), *late.Monthly.Start)
	require.Equal(t, "4.0000000000", late.Monthly.UsageUSD, "the late first event changes which complete rolling window is current")
	input.Entries = []ClientPlatformQuotaProjectionEntry{source[2], source[1], source[0]}
	reordered, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, late, reordered)
}

func TestClientPlatformQuotaProjectionFiltersLifecycleAndFutureConsumption(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := clientPlatformProjectionInput()
	input.QuotaCreatedAt = clientPlatformProjectionTime("2026-10-23T00:00:00Z")
	input.Entries = []ClientPlatformQuotaProjectionEntry{
		{CapturedAt: input.QuotaCreatedAt.Add(-time.Nanosecond), CostUSD: "16"},
		{CapturedAt: input.QuotaCreatedAt, CostUSD: "1"},
		{CapturedAt: input.AsOf, CostUSD: "2"},
		{CapturedAt: input.AsOf.Add(time.Nanosecond), CostUSD: "8"},
	}
	result, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, "2.0000000000", result.Daily.UsageUSD)
	require.Equal(t, "3.0000000000", result.Weekly.UsageUSD)
	require.Equal(t, "3.0000000000", result.Monthly.UsageUSD)
}

func TestClientPlatformQuotaProjectionPreservesExactDecimalAndSkipsZeroAnchors(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := clientPlatformProjectionInput()
	input.TrackingStartedAt = input.AsOf.Add(-time.Hour)
	input.Entries = []ClientPlatformQuotaProjectionEntry{{CapturedAt: input.QuotaCreatedAt, CostUSD: "0.0000000000"}}
	zero, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Nil(t, zero.Monthly.Start)
	for _, amount := range []string{"0.1", "0.2", "0.0000000001", "9999999999.9999999999"} {
		input.Entries = append(input.Entries, ClientPlatformQuotaProjectionEntry{CapturedAt: input.AsOf, CostUSD: amount})
	}
	result, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, "10000000000.3000000000", result.Daily.UsageUSD)
	require.Equal(t, result.Daily.UsageUSD, result.Weekly.UsageUSD)
	require.Equal(t, result.Daily.UsageUSD, result.Monthly.UsageUSD)
	require.Equal(t, input.AsOf, *result.Monthly.Start, "zero history does not choose an anchor")
}

func TestClientPlatformQuotaProjectionHistoryRequiresRelevantWindowProvenance(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	old := clientPlatformProjectionTime("2026-10-24T09:00:00Z")
	for _, tc := range []struct {
		name                   string
		daily, weekly, monthly bool
		history                bool
	}{
		{name: "all-unknown", history: true},
		{name: "daily-reset-only", daily: true, history: true},
		{name: "monthly-still-unknown", daily: true, weekly: true, history: true},
		{name: "all-reset", daily: true, weekly: true, monthly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := clientPlatformProjectionInput()
			input.TrackingStartedAt = old.Add(time.Hour)
			input.Entries = []ClientPlatformQuotaProjectionEntry{{CapturedAt: old, CostUSD: "1"}, {CapturedAt: input.TrackingStartedAt.Add(time.Hour), CostUSD: "2"}}
			if tc.daily {
				input.DailyResetAt = &input.TrackingStartedAt
			}
			if tc.weekly {
				input.WeeklyResetAt = &input.TrackingStartedAt
			}
			if tc.monthly {
				input.MonthlyResetAt = &input.TrackingStartedAt
			}
			result, err := ProjectClientPlatformQuota(input)
			if tc.history {
				require.ErrorIs(t, err, ErrClientPlatformQuotaProjectionHistory)
				require.Nil(t, result, "do not return partial known-window amounts")
				return
			}
			require.NoError(t, err)
			require.Equal(t, "2.0000000000", result.Monthly.UsageUSD)
		})
	}
	input := clientPlatformProjectionInput()
	input.TrackingStartedAt = clientPlatformProjectionTime("2026-10-24T10:00:00Z")
	previousWeek := clientPlatformProjectionTime("2026-10-18T09:00:00Z")
	input.Entries = []ClientPlatformQuotaProjectionEntry{{CapturedAt: previousWeek, CostUSD: "1"}}
	input.MonthlyResetAt = &input.TrackingStartedAt
	result, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err, "natural daily/weekly boundaries and an explicit monthly reset exclude unknown history")
	require.Equal(t, "0.0000000000", result.Weekly.UsageUSD)
	input.MonthlyResetAt = nil
	input.Entries[0].CapturedAt = input.QuotaCreatedAt
	result, err = ProjectClientPlatformQuota(input)
	require.ErrorIs(t, err, ErrClientPlatformQuotaProjectionHistory, "apparently expired old history still affects rolling anchors")
	require.Nil(t, result)
}

func TestClientPlatformQuotaProjectionBoundsCompleteInput(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	input := clientPlatformProjectionInput()
	input.Entries = make([]ClientPlatformQuotaProjectionEntry, MaxClientPlatformQuotaProjectionEntries)
	for i := range input.Entries {
		input.Entries[i] = ClientPlatformQuotaProjectionEntry{CapturedAt: input.AsOf, CostUSD: "0.0000000001"}
	}
	result, err := ProjectClientPlatformQuota(input)
	require.NoError(t, err)
	require.Equal(t, "0.0000010000", result.Monthly.UsageUSD)
	input.Entries = append(input.Entries, input.Entries[0])
	result, err = ProjectClientPlatformQuota(input)
	require.ErrorIs(t, err, ErrClientPlatformQuotaProjectionUnavailable)
	require.Nil(t, result)
}

func TestClientPlatformQuotaProjectionRejectsInvalidInput(t *testing.T) {
	clientPlatformProjectionTimezone(t, "UTC")
	for _, amount := range []string{"", "-1", "-0", "+1", "NaN", "Infinity", "1e-10", " 1", "1 ", ".1", "1.", "0.00000000001", "10000000000", strings.Repeat("0", 22), "1/2"} {
		t.Run("amount-"+amount, func(t *testing.T) {
			input := clientPlatformProjectionInput()
			input.Entries = []ClientPlatformQuotaProjectionEntry{{CapturedAt: input.AsOf, CostUSD: amount}}
			result, err := ProjectClientPlatformQuota(input)
			require.ErrorIs(t, err, ErrClientPlatformQuotaProjectionInput)
			require.Nil(t, result)
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ClientPlatformQuotaProjectionInput)
	}{
		{name: "row", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.QuotaRowID = 0 }},
		{name: "user", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.UserID = -1 }},
		{name: "platform", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.Platform = PlatformComposite }},
		{name: "unknown-platform", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.Platform = "unknown" }},
		{name: "zero-as-of", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.AsOf = time.Time{} }},
		{name: "zero-created", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.QuotaCreatedAt = time.Time{} }},
		{name: "zero-tracking", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.TrackingStartedAt = time.Time{} }},
		{name: "future-created", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.QuotaCreatedAt = i.AsOf.Add(time.Second) }},
		{name: "future-tracking", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.TrackingStartedAt = i.AsOf.Add(time.Second) }},
		{name: "future-reset", mutate: func(i *ClientPlatformQuotaProjectionInput) { reset := i.AsOf.Add(time.Second); i.DailyResetAt = &reset }},
		{name: "zero-reset", mutate: func(i *ClientPlatformQuotaProjectionInput) { reset := time.Time{}; i.WeeklyResetAt = &reset }},
		{name: "reset-before-created", mutate: func(i *ClientPlatformQuotaProjectionInput) {
			reset := i.QuotaCreatedAt.Add(-time.Second)
			i.MonthlyResetAt = &reset
		}},
		{name: "reset-before-tracking", mutate: func(i *ClientPlatformQuotaProjectionInput) {
			i.TrackingStartedAt = i.QuotaCreatedAt.Add(2 * time.Hour)
			reset := i.QuotaCreatedAt.Add(time.Hour)
			i.MonthlyResetAt = &reset
		}},
		{name: "zero-entry-time", mutate: func(i *ClientPlatformQuotaProjectionInput) {
			i.Entries = []ClientPlatformQuotaProjectionEntry{{CostUSD: "1"}}
		}},
		{name: "out-of-range-time", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.AsOf = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{name: "derived-window-out-of-range", mutate: func(i *ClientPlatformQuotaProjectionInput) { i.AsOf = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := clientPlatformProjectionInput()
			tc.mutate(&input)
			result, err := ProjectClientPlatformQuota(input)
			require.ErrorIs(t, err, ErrClientPlatformQuotaProjectionInput)
			require.Nil(t, result)
		})
	}
}
