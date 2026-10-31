//go:build unit

package service

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func quotaCandidateFixture() (UserPlatformQuotaCacheDomain, *UserPlatformQuotaOrderedObservation) {
	domain := UserPlatformQuotaCacheDomain{SchemaVersion: UserPlatformQuotaCacheDomainSchemaV2,
		Namespace:       "5e878391-629f-4abb-9439-785ad2cc45ce",
		WriterAuthority: UserPlatformQuotaWriterAuthority{Mode: UserPlatformQuotaWriterModeDB, Epoch: 7}}
	day := time.Date(2020, 3, 5, 7, 11, 13, 123456000, time.FixedZone("fixture east", 8*3600))
	week := day.Add(-2*time.Hour + time.Microsecond)
	month := time.Date(2099, 7, 9, 13, 17, 19, 654321000, time.UTC)
	daily, monthly := "10", "0"
	return domain, &UserPlatformQuotaOrderedObservation{
		Token: UserPlatformQuotaObservationToken{WriterMode: domain.WriterAuthority.Mode, WriterEpoch: domain.WriterAuthority.Epoch,
			QuotaRowID: 9007199254740993, ControlGeneration: math.MaxInt64, Lineage: "afc81bea-9158-4b1b-b8a1-7bc17f8e3ef6", Revision: 9007199254740993},
		UserID: 42, Platform: PlatformOpenAI, DailyLimitUSD: &daily, MonthlyLimitUSD: &monthly,
		DailyUsageUSD: "1", WeeklyUsageUSD: "2", MonthlyUsageUSD: "3",
		DailyWindowStart: &day, WeeklyWindowStart: &week, MonthlyWindowStart: &month,
	}
}

func quotaCandidatePrepare(t *testing.T, domain UserPlatformQuotaCacheDomain, observation *UserPlatformQuotaOrderedObservation) *UserPlatformQuotaCacheCandidate {
	t.Helper()
	candidate, err := PrepareUserPlatformQuotaCacheCandidate(domain, observation)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	return candidate
}

var quotaCandidateMoneyFields = []struct {
	name string
	set  func(*UserPlatformQuotaOrderedObservation, string)
	get  func(*UserPlatformQuotaOrderedObservation) string
}{
	{"daily limit", func(v *UserPlatformQuotaOrderedObservation, s string) { v.DailyLimitUSD = &s }, func(v *UserPlatformQuotaOrderedObservation) string { return *v.DailyLimitUSD }},
	{"weekly limit", func(v *UserPlatformQuotaOrderedObservation, s string) { v.WeeklyLimitUSD = &s }, func(v *UserPlatformQuotaOrderedObservation) string { return *v.WeeklyLimitUSD }},
	{"monthly limit", func(v *UserPlatformQuotaOrderedObservation, s string) { v.MonthlyLimitUSD = &s }, func(v *UserPlatformQuotaOrderedObservation) string { return *v.MonthlyLimitUSD }},
	{"daily usage", func(v *UserPlatformQuotaOrderedObservation, s string) { v.DailyUsageUSD = s }, func(v *UserPlatformQuotaOrderedObservation) string { return v.DailyUsageUSD }},
	{"weekly usage", func(v *UserPlatformQuotaOrderedObservation, s string) { v.WeeklyUsageUSD = s }, func(v *UserPlatformQuotaOrderedObservation) string { return v.WeeklyUsageUSD }},
	{"monthly usage", func(v *UserPlatformQuotaOrderedObservation, s string) { v.MonthlyUsageUSD = s }, func(v *UserPlatformQuotaOrderedObservation) string { return v.MonthlyUsageUSD }},
}

func TestUserPlatformQuotaCacheCandidateCanonicalMoneyRetainsExactNumericFacts(t *testing.T) {
	for _, field := range quotaCandidateMoneyFields {
		t.Run(field.name, func(t *testing.T) {
			for _, tc := range []struct{ input, want string }{
				{"0", "0.0000000000"}, {"0.0000000000", "0.0000000000"}, {"1", "1.0000000000"},
				{"0.0000000001", "0.0000000001"}, {"1.20", "1.2000000000"},
				{"1234567890.1234567890", "1234567890.1234567890"}, {"9999999999.9999999999", "9999999999.9999999999"},
			} {
				t.Run(tc.input, func(t *testing.T) {
					domain, observation := quotaCandidateFixture()
					field.set(observation, tc.input)
					candidate := quotaCandidatePrepare(t, domain, observation)
					require.Equal(t, tc.want, field.get(candidate.Observation()))
					require.Equal(t, tc.input, field.get(observation), "construction never canonicalizes caller-owned input in place")
				})
			}
		})
	}
	domain, observation := quotaCandidateFixture()
	candidate := quotaCandidatePrepare(t, domain, observation)
	got := candidate.Observation()
	require.Nil(t, got.WeeklyLimitUSD, "NULL remains unlimited")
	require.Equal(t, "0.0000000000", *got.MonthlyLimitUSD, "zero remains a disabled limit")
	require.Equal(t, "3.0000000000", got.MonthlyUsageUSD, "usage above a limit is an observed fact")
	require.Equal(t, observation.Token, got.Token, "int64 ordering metadata stays exact above 2^53")
}

func TestUserPlatformQuotaCacheCandidateRejectsMalformedOrRoundedMoney(t *testing.T) {
	invalid := []string{"", " ", " 1", "1 ", "\t1", "1\n", "+1", "-1", "-0", "-0.0000000000",
		"NaN", "nan", "Infinity", "-Infinity", "Inf", "1e0", "0e-10", "1E3", ".1", "1.",
		"00", "01", "00.1", "1.00000000001", "0.00000000001", "10000000000", "99999999999.0", "1,000", "１", "1\x00"}
	for _, field := range quotaCandidateMoneyFields {
		t.Run(field.name, func(t *testing.T) {
			for _, raw := range invalid {
				t.Run(raw, func(t *testing.T) {
					domain, observation := quotaCandidateFixture()
					field.set(observation, raw)
					candidate, err := PrepareUserPlatformQuotaCacheCandidate(domain, observation)
					require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateInvalid)
					require.Nil(t, candidate)
					require.Equal(t, raw, field.get(observation))
				})
			}
		})
	}
}

func TestUserPlatformQuotaCacheCandidateRequiresCoherentPrivateIdentityAndDomain(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*UserPlatformQuotaCacheDomain, *UserPlatformQuotaOrderedObservation)
	}{
		{"domain schema", func(d *UserPlatformQuotaCacheDomain, _ *UserPlatformQuotaOrderedObservation) { d.SchemaVersion = 1 }},
		{"domain namespace", func(d *UserPlatformQuotaCacheDomain, _ *UserPlatformQuotaOrderedObservation) {
			d.Namespace = "00000000-0000-0000-0000-000000000000"
		}},
		{"domain authority", func(d *UserPlatformQuotaCacheDomain, _ *UserPlatformQuotaOrderedObservation) {
			d.WriterAuthority.Epoch = 0
		}},
		{"writer mismatch", func(d *UserPlatformQuotaCacheDomain, _ *UserPlatformQuotaOrderedObservation) {
			d.WriterAuthority.Epoch++
		}},
		{"token mode", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Token.WriterMode = UserPlatformQuotaWriterModeSnapshot
		}},
		{"token epoch", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.Token.WriterEpoch = 0 }},
		{"token row", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.Token.QuotaRowID = 0 }},
		{"token generation", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Token.ControlGeneration = 0
		}},
		{"token revision", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.Token.Revision = 0 }},
		{"token lineage", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Token.Lineage = strings.Repeat("a", 64)
		}},
		{"user zero", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.UserID = 0 }},
		{"user negative", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.UserID = -1 }},
		{"platform empty", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.Platform = "" }},
		{"platform all", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.Platform = "all" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			domain, observation := quotaCandidateFixture()
			tc.mutate(&domain, observation)
			candidate, err := PrepareUserPlatformQuotaCacheCandidate(domain, observation)
			require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateInvalid)
			require.Nil(t, candidate)
		})
	}
	domain, observation := quotaCandidateFixture()
	candidate, err := PrepareUserPlatformQuotaCacheCandidate(domain, nil)
	require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateInvalid)
	require.Nil(t, candidate)
	valid := quotaCandidatePrepare(t, domain, observation)
	for _, invalid := range []*UserPlatformQuotaCacheCandidate{nil, {}} {
		for _, pair := range [][2]*UserPlatformQuotaCacheCandidate{{invalid, valid}, {valid, invalid}, {invalid, invalid}} {
			order, err := CompareUserPlatformQuotaCacheCandidates(pair[0], pair[1])
			require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateInvalid)
			require.Empty(t, order)
		}
	}
}

func TestUserPlatformQuotaCacheCandidatePreservesArbitraryWindowAnchors(t *testing.T) {
	for _, start := range []time.Time{
		time.Date(1, 1, 1, 0, 0, 0, 1000, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
		time.Date(2020, 3, 5, 7, 11, 13, 123456000, time.FixedZone("past", 8*3600)),
		time.Date(2099, 7, 9, 13, 17, 19, 654321000, time.FixedZone("future", -5*3600)),
		time.Date(0, 12, 31, 23, 30, 0, 0, time.FixedZone("local year zero", -3600)),
		time.Date(10000, 1, 1, 0, 30, 0, 0, time.FixedZone("local year ten thousand", 3600)),
	} {
		domain, observation := quotaCandidateFixture()
		observation.DailyWindowStart, observation.WeeklyWindowStart, observation.MonthlyWindowStart = &start, &start, &start
		candidate := quotaCandidatePrepare(t, domain, observation)
		got := candidate.Observation()
		for _, actual := range []*time.Time{got.DailyWindowStart, got.WeeklyWindowStart, got.MonthlyWindowStart} {
			require.NotNil(t, actual)
			require.True(t, start.Equal(*actual), "construction does not align, expire or rotate anchors")
			require.Equal(t, time.UTC, actual.Location())
			require.Equal(t, start.Nanosecond(), actual.Nanosecond())
		}
	}
	for _, field := range []string{"daily", "weekly", "monthly"} {
		t.Run(field, func(t *testing.T) {
			for _, start := range []*time.Time{nil, {},
				func() *time.Time { v := time.Date(0, 12, 31, 23, 59, 59, 0, time.UTC); return &v }(),
				func() *time.Time { v := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC); return &v }(),
				func() *time.Time { v := time.Date(2026, 1, 1, 0, 0, 0, 1, time.UTC); return &v }(),
			} {
				domain, observation := quotaCandidateFixture()
				switch field {
				case "daily":
					observation.DailyWindowStart = start
				case "weekly":
					observation.WeeklyWindowStart = start
				case "monthly":
					observation.MonthlyWindowStart = start
				}
				candidate, err := PrepareUserPlatformQuotaCacheCandidate(domain, observation)
				require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateInvalid)
				require.Nil(t, candidate)
			}
		})
	}
	domain, observation := quotaCandidateFixture()
	observation.DailyUsageUSD, observation.WeeklyUsageUSD, observation.MonthlyUsageUSD = "0", "0", "0"
	observation.DailyWindowStart, observation.WeeklyWindowStart, observation.MonthlyWindowStart = nil, nil, nil
	got := quotaCandidatePrepare(t, domain, observation).Observation()
	require.Nil(t, got.DailyWindowStart)
	require.Nil(t, got.WeeklyWindowStart)
	require.Nil(t, got.MonthlyWindowStart)
}

func TestUserPlatformQuotaCacheCandidateOwnsDeepCopiesOfInputAndAccessors(t *testing.T) {
	domain, observation := quotaCandidateFixture()
	weekly := "20"
	observation.WeeklyLimitUSD = &weekly
	candidate := quotaCandidatePrepare(t, domain, observation)
	expectedDomain, expectedObservation, fingerprint := candidate.Domain(), candidate.Observation(), candidate.Fingerprint()
	domain.Namespace = "01b1739d-0e3c-4845-8550-1866a739049d"
	observation.Token.Revision++
	observation.UserID++
	observation.Platform = PlatformGemini
	observation.DailyUsageUSD, observation.WeeklyUsageUSD, observation.MonthlyUsageUSD = "99", "98", "97"
	for _, limit := range []*string{observation.DailyLimitUSD, observation.WeeklyLimitUSD, observation.MonthlyLimitUSD} {
		*limit = "999"
	}
	for _, start := range []*time.Time{observation.DailyWindowStart, observation.WeeklyWindowStart, observation.MonthlyWindowStart} {
		*start = start.Add(time.Microsecond)
	}
	require.Equal(t, expectedDomain, candidate.Domain())
	require.Equal(t, expectedObservation, candidate.Observation())
	returned := candidate.Observation()
	returned.Token.Revision++
	returned.UserID++
	returned.DailyUsageUSD = "100"
	for _, limit := range []*string{returned.DailyLimitUSD, returned.WeeklyLimitUSD, returned.MonthlyLimitUSD} {
		*limit = "888"
	}
	for _, start := range []*time.Time{returned.DailyWindowStart, returned.WeeklyWindowStart, returned.MonthlyWindowStart} {
		*start = start.Add(-time.Hour)
	}
	returnedDomain := candidate.Domain()
	returnedDomain.WriterAuthority.Epoch++
	returnedDomain.Namespace = ""
	require.Equal(t, expectedDomain, candidate.Domain())
	require.Equal(t, expectedObservation, candidate.Observation())
	require.Equal(t, fingerprint, candidate.Fingerprint())
}

func TestUserPlatformQuotaCacheCandidateFingerprintBindsCanonicalFacts(t *testing.T) {
	domain, observation := quotaCandidateFixture()
	current := quotaCandidatePrepare(t, domain, observation)
	require.NotEmpty(t, current.Fingerprint())
	observation.DailyUsageUSD = "1.0000000000"
	*observation.DailyLimitUSD = "10.0000000000"
	start := observation.DailyWindowStart.UTC()
	observation.DailyWindowStart = &start
	canonical := quotaCandidatePrepare(t, domain, observation)
	require.Equal(t, current.Fingerprint(), canonical.Fingerprint(), "equivalent decimals and timezone representations have one fingerprint")
	for _, tc := range []struct {
		name   string
		mutate func(*UserPlatformQuotaCacheDomain, *UserPlatformQuotaOrderedObservation)
	}{
		{"namespace", func(d *UserPlatformQuotaCacheDomain, _ *UserPlatformQuotaOrderedObservation) {
			d.Namespace = "01b1739d-0e3c-4845-8550-1866a739049d"
		}},
		{"authority", func(d *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			d.WriterAuthority.Epoch++
			v.Token.WriterEpoch++
		}},
		{"row", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.Token.QuotaRowID++ }},
		{"generation", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Token.ControlGeneration--
		}},
		{"revision", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.Token.Revision++ }},
		{"lineage", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Token.Lineage = "bf7acc39-e306-4838-9ee0-87ab7f4ba42f"
		}},
		{"user", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.UserID++ }},
		{"platform", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Platform = PlatformGemini
		}},
		{"null limit versus zero", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			s := "0"
			v.WeeklyLimitUSD = &s
		}},
		{"zero limit versus null", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.MonthlyLimitUSD = nil }},
		{"microsecond", func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			at := v.DailyWindowStart.Add(time.Microsecond)
			v.DailyWindowStart = &at
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			domain, observation := quotaCandidateFixture()
			tc.mutate(&domain, observation)
			candidate := quotaCandidatePrepare(t, domain, observation)
			require.NotEqual(t, current.Fingerprint(), candidate.Fingerprint())
		})
	}
	for _, field := range quotaCandidateMoneyFields {
		domain, observation := quotaCandidateFixture()
		field.set(observation, "9.0000000001")
		require.NotEqual(t, current.Fingerprint(), quotaCandidatePrepare(t, domain, observation).Fingerprint(), field.name)
	}
}

func TestUserPlatformQuotaCacheCandidateComparisonUsesRevisionOnlyInsideExactScope(t *testing.T) {
	domain, raw := quotaCandidateFixture()
	current := quotaCandidatePrepare(t, domain, raw)
	for _, tc := range []struct {
		revision int64
		want     UserPlatformQuotaObservationOrder
	}{
		{1, QuotaObservationOlder}, {raw.Token.Revision - 1, QuotaObservationOlder}, {raw.Token.Revision, QuotaObservationEqual},
		{raw.Token.Revision + 1, QuotaObservationNewer}, {math.MaxInt64, QuotaObservationNewer},
	} {
		domain, observation := quotaCandidateFixture()
		observation.Token.Revision = tc.revision
		if tc.revision != raw.Token.Revision {
			observation.DailyUsageUSD = "9876543210.0000000001"
		}
		order, err := CompareUserPlatformQuotaCacheCandidates(quotaCandidatePrepare(t, domain, observation), current)
		require.NoError(t, err)
		require.Equal(t, tc.want, order)
	}
	for _, mutate := range []func(*UserPlatformQuotaCacheDomain, *UserPlatformQuotaOrderedObservation){
		func(d *UserPlatformQuotaCacheDomain, _ *UserPlatformQuotaOrderedObservation) {
			d.Namespace = "01b1739d-0e3c-4845-8550-1866a739049d"
		},
		func(d *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			d.WriterAuthority.Epoch++
			v.Token.WriterEpoch++
		},
		func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.UserID++ },
		func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Platform = PlatformGemini
		},
		func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) { v.Token.QuotaRowID++ },
		func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Token.ControlGeneration--
		},
		func(_ *UserPlatformQuotaCacheDomain, v *UserPlatformQuotaOrderedObservation) {
			v.Token.Lineage = "bf7acc39-e306-4838-9ee0-87ab7f4ba42f"
		},
	} {
		for _, revision := range []int64{1, raw.Token.Revision, math.MaxInt64} {
			domain, observation := quotaCandidateFixture()
			mutate(&domain, observation)
			observation.Token.Revision = revision
			candidate := quotaCandidatePrepare(t, domain, observation)
			for _, pair := range [][2]*UserPlatformQuotaCacheCandidate{{candidate, current}, {current, candidate}} {
				order, err := CompareUserPlatformQuotaCacheCandidates(pair[0], pair[1])
				require.ErrorIs(t, err, ErrUserPlatformQuotaObservationIncomparable)
				require.Empty(t, order)
			}
		}
	}
}

func TestUserPlatformQuotaCacheCandidateEqualTokenRejectsPayloadConflicts(t *testing.T) {
	domain, observation := quotaCandidateFixture()
	current := quotaCandidatePrepare(t, domain, observation)
	for _, field := range quotaCandidateMoneyFields {
		t.Run(field.name, func(t *testing.T) {
			domain, observation := quotaCandidateFixture()
			field.set(observation, "9.0000000001")
			candidate := quotaCandidatePrepare(t, domain, observation)
			for _, pair := range [][2]*UserPlatformQuotaCacheCandidate{{candidate, current}, {current, candidate}} {
				order, err := CompareUserPlatformQuotaCacheCandidates(pair[0], pair[1])
				require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateConflict)
				require.Empty(t, order)
			}
		})
	}
	for _, mutate := range []func(*UserPlatformQuotaOrderedObservation){
		func(v *UserPlatformQuotaOrderedObservation) {
			at := v.DailyWindowStart.Add(time.Microsecond)
			v.DailyWindowStart = &at
		},
		func(v *UserPlatformQuotaOrderedObservation) {
			at := v.WeeklyWindowStart.Add(time.Microsecond)
			v.WeeklyWindowStart = &at
		},
		func(v *UserPlatformQuotaOrderedObservation) {
			at := v.MonthlyWindowStart.Add(time.Microsecond)
			v.MonthlyWindowStart = &at
		},
		func(v *UserPlatformQuotaOrderedObservation) { s := "0"; v.WeeklyLimitUSD = &s },
		func(v *UserPlatformQuotaOrderedObservation) { v.MonthlyLimitUSD = nil },
	} {
		domain, observation := quotaCandidateFixture()
		mutate(observation)
		order, err := CompareUserPlatformQuotaCacheCandidates(quotaCandidatePrepare(t, domain, observation), current)
		require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateConflict)
		require.Empty(t, order)
	}
	// NULL and a concrete start remain distinct even when usage is zero.
	domain, observation = quotaCandidateFixture()
	observation.DailyUsageUSD = "0"
	withAnchor := quotaCandidatePrepare(t, domain, observation)
	observation.DailyWindowStart = nil
	withoutAnchor := quotaCandidatePrepare(t, domain, observation)
	order, err := CompareUserPlatformQuotaCacheCandidates(withAnchor, withoutAnchor)
	require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateConflict)
	require.Empty(t, order)
}

func TestUserPlatformQuotaCacheCandidateStaysPrivateAndGrantsNoPublicationAuthority(t *testing.T) {
	domain, observation := quotaCandidateFixture()
	candidate := quotaCandidatePrepare(t, domain, observation)
	raw, err := json.Marshal(candidate)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(raw))
	var supplied UserPlatformQuotaCacheCandidate
	require.NoError(t, json.Unmarshal([]byte(`{"domain":{"Namespace":"5e878391-629f-4abb-9439-785ad2cc45ce"},"observation":{"DailyUsageUSD":"1"},"Fingerprint":"caller-supplied","Certified":true,"Ready":true}`), &supplied))
	order, err := CompareUserPlatformQuotaCacheCandidates(&supplied, candidate)
	require.ErrorIs(t, err, ErrUserPlatformQuotaCacheCandidateInvalid)
	require.Empty(t, order)
	typ := reflect.TypeOf(*candidate)
	for i := 0; i < typ.NumField(); i++ {
		require.NotEmpty(t, typ.Field(i).PkgPath, "candidate fields remain private")
	}
	for _, name := range []string{"Certified", "Ready", "Publish", "Commit", "Restore"} {
		_, exists := reflect.TypeOf(candidate).MethodByName(name)
		require.False(t, exists, "preparation exposes no %s authorization", name)
	}
	var absent *UserPlatformQuotaCacheCandidate
	require.Equal(t, UserPlatformQuotaCacheDomain{}, absent.Domain())
	require.Nil(t, absent.Observation())
	require.Empty(t, absent.Fingerprint())
}
