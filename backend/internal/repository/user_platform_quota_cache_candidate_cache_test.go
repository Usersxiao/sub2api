//go:build unit

package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

const quotaCandidateCacheIncarnation = "1234567890abcdef1234567890abcdef"

func quotaCandidateCacheFixture(t *testing.T, userID, revision int64) *service.UserPlatformQuotaCacheCandidate {
	t.Helper()
	domain := quotaDomainTestDomain()
	start := time.Date(2020, 3, 5, 7, 11, 13, 123456000, time.FixedZone("fixture east", 8*3600))
	future := time.Date(2099, 7, 9, 13, 17, 19, 654321000, time.UTC)
	disabled, maximum := "0", "9999999999.9999999999"
	observation := &service.UserPlatformQuotaOrderedObservation{
		Token: service.UserPlatformQuotaObservationToken{WriterMode: domain.WriterAuthority.Mode, WriterEpoch: domain.WriterAuthority.Epoch,
			QuotaRowID: 9007199254740993, ControlGeneration: math.MaxInt64, Lineage: "afc81bea-9158-4b1b-b8a1-7bc17f8e3ef6", Revision: revision},
		UserID: userID, Platform: service.PlatformOpenAI, WeeklyLimitUSD: &disabled, MonthlyLimitUSD: &maximum,
		DailyUsageUSD: "0.0000000001", WeeklyUsageUSD: "1234567890.1234567890", MonthlyUsageUSD: "9999999999.9999999999",
		DailyWindowStart: &start, WeeklyWindowStart: &future, MonthlyWindowStart: &start,
	}
	candidate, err := service.PrepareUserPlatformQuotaCacheCandidate(domain, observation)
	require.NoError(t, err)
	return candidate
}

func quotaCandidateCacheEncode(t *testing.T, candidate *service.UserPlatformQuotaCacheCandidate, incarnation string) []byte {
	t.Helper()
	raw, err := encodeUserPlatformQuotaCacheCandidateEnvelope(candidate, incarnation)
	require.NoError(t, err)
	return raw
}

func quotaCandidateCacheKey(t *testing.T, candidate *service.UserPlatformQuotaCacheCandidate) string {
	t.Helper()
	observation := candidate.Observation()
	key, err := userPlatformQuotaCacheCandidateKey(candidate.Domain(), observation.UserID, observation.Platform)
	require.NoError(t, err)
	return key
}

func TestUserPlatformQuotaCacheCandidateCacheEnvelopeRoundTripRetainsEveryExactFact(t *testing.T) {
	candidate := quotaCandidateCacheFixture(t, math.MaxInt64, 9007199254740993)
	raw := quotaCandidateCacheEncode(t, candidate, quotaCandidateCacheIncarnation)
	require.LessOrEqual(t, len(raw), userPlatformQuotaCacheCandidateMaxBytes)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for field, want := range map[string]string{
		"schema_version": "2", "user_id": "9223372036854775807", "quota_row_id": "9007199254740993",
		"control_generation": "9223372036854775807", "revision": "9007199254740993",
		"daily_limit_usd": "null", "weekly_limit_usd": `"0.0000000000"`,
		"monthly_limit_usd": `"9999999999.9999999999"`, "daily_usage_usd": `"0.0000000001"`,
		"weekly_usage_usd": `"1234567890.1234567890"`, "monthly_usage_usd": `"9999999999.9999999999"`,
		"daily_window_start": `"2020-03-04T23:11:13.123456Z"`, "weekly_window_start": `"2099-07-09T13:17:19.654321Z"`,
		"incarnation": `"` + quotaCandidateCacheIncarnation + `"`, "fingerprint": `"` + candidate.Fingerprint() + `"`,
	} {
		require.Equal(t, want, string(fields[field]), field)
	}
	decoded, incarnation, err := decodeUserPlatformQuotaCacheCandidateEnvelope(raw)
	require.NoError(t, err)
	require.Equal(t, quotaCandidateCacheIncarnation, incarnation)
	require.Equal(t, candidate.Domain(), decoded.Domain())
	require.Equal(t, candidate.Observation(), decoded.Observation())
	require.Equal(t, candidate.Fingerprint(), decoded.Fingerprint())
	require.Equal(t, raw, quotaCandidateCacheEncode(t, decoded, incarnation))
	public, err := json.Marshal(decoded)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(public))
	zero := decoded.Observation()
	zero.DailyUsageUSD, zero.WeeklyUsageUSD, zero.MonthlyUsageUSD = "0", "0", "0"
	zero.DailyWindowStart, zero.WeeklyWindowStart, zero.MonthlyWindowStart = nil, nil, nil
	zeroCandidate, err := service.PrepareUserPlatformQuotaCacheCandidate(decoded.Domain(), zero)
	require.NoError(t, err)
	zeroRaw := quotaCandidateCacheEncode(t, zeroCandidate, incarnation)
	require.Contains(t, string(zeroRaw), `"daily_window_start":null`)
	got, _, err := decodeUserPlatformQuotaCacheCandidateEnvelope(zeroRaw)
	require.NoError(t, err)
	require.Nil(t, got.Observation().DailyWindowStart)
}

func TestUserPlatformQuotaCacheCandidateCacheEnvelopeRejectsAmbiguousOrTamperedBytes(t *testing.T) {
	candidate := quotaCandidateCacheFixture(t, 9007199254740993, 9007199254740993)
	raw := string(quotaCandidateCacheEncode(t, candidate, quotaCandidateCacheIncarnation))
	replace := func(from, to string) []byte {
		require.Contains(t, raw, from)
		return []byte(strings.Replace(raw, from, to, 1))
	}
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"empty", nil}, {"null", []byte("null")}, {"array", []byte("[]")}, {"truncated", []byte(raw[:len(raw)-1])},
		{"leading whitespace", []byte(" " + raw)}, {"trailing whitespace", []byte(raw + "\n")},
		{"internal whitespace", replace(`"schema_version":2`, `"schema_version": 2`)},
		{"duplicate field", replace(`"schema_version":2,`, `"schema_version":2,"schema_version":2,`)},
		{"unknown field", []byte(strings.TrimSuffix(raw, "}") + `,"ready":true}`)},
		{"field order", replace(`"schema_version":2,"incarnation":"`+quotaCandidateCacheIncarnation+`"`, `"incarnation":"`+quotaCandidateCacheIncarnation+`","schema_version":2`)},
		{"second object", []byte(raw + raw)},
		{"legacy schema", replace(`"schema_version":2`, `"schema_version":1`)},
		{"zero incarnation", replace(quotaCandidateCacheIncarnation, strings.Repeat("0", 32))},
		{"uppercase incarnation", replace(quotaCandidateCacheIncarnation, strings.ToUpper(quotaCandidateCacheIncarnation))},
		{"wrong digest", replace(candidate.Fingerprint(), strings.Repeat("0", 64))},
		{"noncanonical decimal", replace(`"daily_usage_usd":"0.0000000001"`, `"daily_usage_usd":"1e-10"`)},
		{"negative zero", replace(`"weekly_limit_usd":"0.0000000000"`, `"weekly_limit_usd":"-0.0000000000"`)},
		{"unquoted amount", replace(`"weekly_usage_usd":"1234567890.1234567890"`, `"weekly_usage_usd":1234567890.1234567890`)},
		{"amount content", replace(`"daily_usage_usd":"0.0000000001"`, `"daily_usage_usd":"0.0000000002"`)},
		{"null versus disabled", replace(`"daily_limit_usd":null`, `"daily_limit_usd":"0.0000000000"`)},
		{"missing positive usage anchor", replace(`"daily_window_start":"2020-03-04T23:11:13.123456Z"`, `"daily_window_start":null`)},
		{"UTC alias", replace("2020-03-04T23:11:13.123456Z", "2020-03-04T23:11:13.123456+00:00")},
		{"extra fractional digit", replace("2020-03-04T23:11:13.123456Z", "2020-03-04T23:11:13.1234560Z")},
		{"microsecond content", replace("2020-03-04T23:11:13.123456Z", "2020-03-04T23:11:13.123457Z")},
		{"int overflow", replace(`"revision":9007199254740993`, `"revision":9223372036854775808`)},
		{"int exponent", replace(`"revision":9007199254740993`, `"revision":9.007199254740993e15`)},
		{"int decimal", replace(`"revision":9007199254740993`, `"revision":9007199254740993.0`)},
		{"token writer mismatch", replace(`"token_writer_epoch":1`, `"token_writer_epoch":2`)},
		{"zero row", replace(`"quota_row_id":9007199254740993`, `"quota_row_id":0`)},
		{"oversized", bytes.Repeat([]byte("x"), userPlatformQuotaCacheCandidateMaxBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoded, incarnation, err := decodeUserPlatformQuotaCacheCandidateEnvelope(tc.payload)
			require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
			require.Nil(t, decoded)
			require.Empty(t, incarnation)
		})
	}
}

func TestUserPlatformQuotaCacheCandidateCacheEnvelopeRequiresValidatedCandidateAndIncarnation(t *testing.T) {
	candidate := quotaCandidateCacheFixture(t, 42, math.MaxInt64)
	for _, invalid := range []string{"", "0", strings.Repeat("0", 32), strings.Repeat("a", 31), strings.Repeat("a", 33),
		strings.Repeat("A", 32), strings.Repeat("g", 32), " " + strings.Repeat("a", 31)} {
		encoded, err := encodeUserPlatformQuotaCacheCandidateEnvelope(candidate, invalid)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
		require.Nil(t, encoded)
	}
	for _, invalid := range []*service.UserPlatformQuotaCacheCandidate{nil, {}} {
		encoded, err := encodeUserPlatformQuotaCacheCandidateEnvelope(invalid, quotaCandidateCacheIncarnation)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
		require.Nil(t, encoded)
	}
	decoded, incarnation, err := decodeUserPlatformQuotaCacheCandidateEnvelope(quotaCandidateCacheEncode(t, candidate, quotaCandidateCacheIncarnation))
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), decoded.Observation().Token.Revision)
	require.Equal(t, quotaCandidateCacheIncarnation, incarnation)
}

func TestUserPlatformQuotaCacheCandidateCacheKeySeparatesNamespaceAndLegacyRoutes(t *testing.T) {
	domain := quotaDomainTestDomain()
	userID := int64(9007199254740993)
	key, err := userPlatformQuotaCacheCandidateKey(domain, userID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.Equal(t, "billing:upq:v2:"+domain.Namespace+":"+strconv.FormatInt(userID, 10)+":openai", key)
	require.NotEqual(t, userPlatformQuotaCacheKey(userID, "openai"), key)
	require.NotEqual(t, userPlatformQuotaDirtySetKey(), key)
	require.NotContains(t, key, "{")
	require.NotContains(t, key, "}", "a single-key script needs no user-injected Redis Cluster hash tag")
	otherNamespace := domain
	otherNamespace.Namespace = "c968f547-cc78-499c-aefc-197533a7a0d8"
	for _, tc := range []struct {
		domain   service.UserPlatformQuotaCacheDomain
		userID   int64
		platform string
	}{
		{otherNamespace, userID, "openai"}, {domain, userID + 1, "openai"}, {domain, userID, "gemini"},
	} {
		other, err := userPlatformQuotaCacheCandidateKey(tc.domain, tc.userID, tc.platform)
		require.NoError(t, err)
		require.NotEqual(t, key, other)
	}
	for _, tc := range []struct {
		userID   int64
		platform string
	}{{0, "openai"}, {-1, "openai"}, {userID, ""}, {userID, "all"}, {userID, "{openai}"}} {
		key, err := userPlatformQuotaCacheCandidateKey(domain, tc.userID, tc.platform)
		require.Error(t, err)
		require.Empty(t, key)
	}
	domain.Namespace = "{c968f547-cc78-499c-aefc-197533a7a0d8}"
	key, err = userPlatformQuotaCacheCandidateKey(domain, userID, "openai")
	require.Error(t, err)
	require.Empty(t, key)
}

func TestUserPlatformQuotaCacheCandidateCacheProductionLegacyGateStillIssuesZeroCommands(t *testing.T) {
	c, observer, hook, userID := quotaDomainRedisFixture(t, "miniredis")
	candidate := quotaCandidateCacheFixture(t, userID, 9007199254740993)
	key := quotaCandidateCacheKey(t, candidate)
	ctx := context.Background()
	require.NoError(t, observer.Set(ctx, key, quotaCandidateCacheEncode(t, candidate, quotaCandidateCacheIncarnation), time.Hour).Err())
	legacy := NewBillingCache(observer).(*billingCache)
	require.NoError(t, legacy.SetUserPlatformQuotaCache(ctx, userID, "openai", quotaCacheCASEntry(), time.Hour))
	require.NoError(t, legacy.ReaddDirtyUserPlatformQuotaKeys(ctx, []service.UserPlatformQuotaKey{{UserID: userID, Platform: "openai"}}))
	keys := []string{key, userPlatformQuotaCacheKey(userID, "openai"), userPlatformQuotaDirtySetKey()}
	before := quotaDomainSnapshot(t, observer, keys)
	quotaDomainAssertAllQuotaPortsClosed(t, c, ctx, userID)
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
	require.Equal(t, before, quotaDomainSnapshot(t, observer, keys))
}

func TestUserPlatformQuotaCacheCandidateCacheInvalidPreconditionsIssueZeroCommands(t *testing.T) {
	c, _, hook, userID := quotaDomainRedisFixture(t, "miniredis")
	candidate := quotaCandidateCacheFixture(t, userID, 1)
	for _, ttl := range []time.Duration{-time.Second, 0, time.Nanosecond, 999 * time.Millisecond, 1500 * time.Millisecond} {
		order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), c.rdb, candidate, ttl)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
		require.Empty(t, order)
	}
	for _, invalid := range []*service.UserPlatformQuotaCacheCandidate{nil, {}} {
		order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), c.rdb, invalid, time.Minute)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheCandidateInvalid)
		require.Empty(t, order)
	}
	order, err := replaceUserPlatformQuotaCacheCandidate(context.Background(), nil, candidate, time.Minute)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
	require.Empty(t, order)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	order, err = replaceUserPlatformQuotaCacheCandidate(cancelled, c.rdb, candidate, time.Minute)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, order)
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
}
