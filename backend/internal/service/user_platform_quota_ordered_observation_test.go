//go:build unit

package service

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func orderedQuotaTokenFixture() UserPlatformQuotaObservationToken {
	return UserPlatformQuotaObservationToken{
		WriterMode: UserPlatformQuotaWriterModeDB, WriterEpoch: 1,
		QuotaRowID: 7, ControlGeneration: 3, Lineage: "6cdd745f-8bb7-4762-af1c-8005684753df", Revision: 1,
	}
}

func TestUserPlatformQuotaOrderedObservationTokenValidation(t *testing.T) {
	valid := orderedQuotaTokenFixture()
	require.NoError(t, ValidateUserPlatformQuotaObservationToken(valid))
	for _, tc := range []struct {
		name   string
		mutate func(*UserPlatformQuotaObservationToken)
	}{
		{"missing mode", func(v *UserPlatformQuotaObservationToken) { v.WriterMode = "" }},
		{"snapshot mode", func(v *UserPlatformQuotaObservationToken) { v.WriterMode = UserPlatformQuotaWriterModeSnapshot }},
		{"missing epoch", func(v *UserPlatformQuotaObservationToken) { v.WriterEpoch = 0 }},
		{"negative epoch", func(v *UserPlatformQuotaObservationToken) { v.WriterEpoch = -1 }},
		{"missing row", func(v *UserPlatformQuotaObservationToken) { v.QuotaRowID = 0 }},
		{"negative row", func(v *UserPlatformQuotaObservationToken) { v.QuotaRowID = -1 }},
		{"missing generation", func(v *UserPlatformQuotaObservationToken) { v.ControlGeneration = 0 }},
		{"negative generation", func(v *UserPlatformQuotaObservationToken) { v.ControlGeneration = -1 }},
		{"missing revision", func(v *UserPlatformQuotaObservationToken) { v.Revision = 0 }},
		{"negative revision", func(v *UserPlatformQuotaObservationToken) { v.Revision = -1 }},
		{"missing lineage", func(v *UserPlatformQuotaObservationToken) { v.Lineage = "" }},
		{"zero lineage", func(v *UserPlatformQuotaObservationToken) { v.Lineage = "00000000-0000-0000-0000-000000000000" }},
		{"uppercase lineage", func(v *UserPlatformQuotaObservationToken) { v.Lineage = strings.ToUpper(v.Lineage) }},
		{"bad separators", func(v *UserPlatformQuotaObservationToken) { v.Lineage = "6cdd745fa8bb7-4762-af1c-8005684753df" }},
		{"bad hex", func(v *UserPlatformQuotaObservationToken) { v.Lineage = "gcdd745f-8bb7-4762-af1c-8005684753df" }},
		{"cache nonce", func(v *UserPlatformQuotaObservationToken) { v.Lineage = strings.Repeat("a", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := valid
			tc.mutate(&value)
			require.ErrorIs(t, ValidateUserPlatformQuotaObservationToken(value), ErrUserPlatformQuotaObservationInvalid)
		})
	}
	valid.Revision = math.MaxInt64
	require.NoError(t, ValidateUserPlatformQuotaObservationToken(valid), "an exhausted committed revision remains readable")
}

func TestUserPlatformQuotaOrderedObservationSameDomainRevisionOrder(t *testing.T) {
	current := orderedQuotaTokenFixture()
	current.Revision = 9007199254740993 // Must not round through JavaScript/Lua doubles.
	for _, tc := range []struct {
		revision int64
		want     UserPlatformQuotaObservationOrder
	}{
		{1, QuotaObservationOlder},
		{current.Revision - 1, QuotaObservationOlder},
		{current.Revision, QuotaObservationEqual},
		{current.Revision + 1, QuotaObservationNewer},
		{math.MaxInt64, QuotaObservationNewer},
	} {
		candidate := current
		candidate.Revision = tc.revision
		got, err := CompareUserPlatformQuotaObservationTokens(candidate, current)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestUserPlatformQuotaOrderedObservationCrossDomainNeverSorted(t *testing.T) {
	current := orderedQuotaTokenFixture()
	for _, tc := range []struct {
		name   string
		mutate func(*UserPlatformQuotaObservationToken)
	}{
		{"writer epoch", func(v *UserPlatformQuotaObservationToken) { v.WriterEpoch++ }},
		{"physical row", func(v *UserPlatformQuotaObservationToken) { v.QuotaRowID++ }},
		{"control generation", func(v *UserPlatformQuotaObservationToken) { v.ControlGeneration++ }},
		{"stable lineage", func(v *UserPlatformQuotaObservationToken) { v.Lineage = "6cdd745f-8bb7-4762-af1c-8005684753de" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := current
			tc.mutate(&candidate)
			for _, revision := range []int64{1, 2, math.MaxInt64} {
				candidate.Revision = revision
				got, err := CompareUserPlatformQuotaObservationTokens(candidate, current)
				require.ErrorIs(t, err, ErrUserPlatformQuotaObservationIncomparable)
				require.Empty(t, got)
				got, err = CompareUserPlatformQuotaObservationTokens(current, candidate)
				require.ErrorIs(t, err, ErrUserPlatformQuotaObservationIncomparable)
				require.Empty(t, got)
			}
		})
	}
}

func TestUserPlatformQuotaOrderedObservationInvalidTokenNeverCompares(t *testing.T) {
	valid := orderedQuotaTokenFixture()
	invalid := valid
	invalid.Revision = 0
	for _, pair := range [][2]UserPlatformQuotaObservationToken{{invalid, valid}, {valid, invalid}, {invalid, invalid}} {
		got, err := CompareUserPlatformQuotaObservationTokens(pair[0], pair[1])
		require.ErrorIs(t, err, ErrUserPlatformQuotaObservationInvalid)
		require.Empty(t, got)
	}
}

func TestUserPlatformQuotaOrderedObservationMetadataStaysPrivate(t *testing.T) {
	now := time.Now()
	limit := "9007199254740993.1234567890"
	token := orderedQuotaTokenFixture()
	observation := UserPlatformQuotaOrderedObservation{
		Token: token, UserID: 42, Platform: PlatformOpenAI,
		DailyLimitUSD: &limit, WeeklyLimitUSD: &limit, MonthlyLimitUSD: &limit,
		DailyUsageUSD: "0.0000000001", WeeklyUsageUSD: "0.0000000001", MonthlyUsageUSD: "0.0000000001",
		DailyWindowStart: &now, WeeklyWindowStart: &now, MonthlyWindowStart: &now,
	}
	for _, value := range []any{token, observation} {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(raw))
	}
	var supplied UserPlatformQuotaObservationToken
	require.NoError(t, json.Unmarshal([]byte(`{"WriterMode":"db_increment_v1","WriterEpoch":1,"QuotaRowID":7,"ControlGeneration":3,"Lineage":"6cdd745f-8bb7-4762-af1c-8005684753df","Revision":9,"revision":9}`), &supplied))
	require.Equal(t, UserPlatformQuotaObservationToken{}, supplied, "public JSON cannot supply an ordered observation token")
}
