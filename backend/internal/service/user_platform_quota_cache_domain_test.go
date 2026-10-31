//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func quotaCacheDomainForTest() UserPlatformQuotaCacheDomain {
	return UserPlatformQuotaCacheDomain{
		SchemaVersion:   UserPlatformQuotaCacheDomainSchemaV2,
		Namespace:       "ddde90ac-5d13-4c6f-8580-19d01b2a021a",
		WriterAuthority: UserPlatformQuotaWriterAuthority{Mode: UserPlatformQuotaWriterModeDB, Epoch: 1},
	}
}

func TestUserPlatformQuotaCacheDomainValidation(t *testing.T) {
	valid := quotaCacheDomainForTest()
	require.NoError(t, ValidateUserPlatformQuotaCacheDomain(valid))
	for _, tc := range []struct {
		name   string
		change func(*UserPlatformQuotaCacheDomain)
	}{
		{"missing schema", func(v *UserPlatformQuotaCacheDomain) { v.SchemaVersion = 0 }},
		{"legacy schema", func(v *UserPlatformQuotaCacheDomain) { v.SchemaVersion = 1 }},
		{"future schema", func(v *UserPlatformQuotaCacheDomain) { v.SchemaVersion = 3 }},
		{"missing namespace", func(v *UserPlatformQuotaCacheDomain) { v.Namespace = "" }},
		{"zero namespace", func(v *UserPlatformQuotaCacheDomain) { v.Namespace = "00000000-0000-0000-0000-000000000000" }},
		{"noncanonical namespace", func(v *UserPlatformQuotaCacheDomain) { v.Namespace = strings.ToUpper(v.Namespace) }},
		{"cache ABA nonce", func(v *UserPlatformQuotaCacheDomain) { v.Namespace = strings.Repeat("a", 64) }},
		{"missing writer", func(v *UserPlatformQuotaCacheDomain) { v.WriterAuthority = UserPlatformQuotaWriterAuthority{} }},
		{"missing epoch", func(v *UserPlatformQuotaCacheDomain) { v.WriterAuthority.Epoch = 0 }},
		{"negative epoch", func(v *UserPlatformQuotaCacheDomain) { v.WriterAuthority.Epoch = -1 }},
		{"snapshot mode", func(v *UserPlatformQuotaCacheDomain) { v.WriterAuthority.Mode = UserPlatformQuotaWriterModeSnapshot }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := valid
			tc.change(&value)
			require.ErrorIs(t, ValidateUserPlatformQuotaCacheDomain(value), ErrUserPlatformQuotaCacheDomainInvalid)
		})
	}
}

func TestUserPlatformQuotaCacheDomainStaysPrivate(t *testing.T) {
	domain := quotaCacheDomainForTest()
	raw, err := json.Marshal(domain)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(raw))
	var supplied UserPlatformQuotaCacheDomain
	require.NoError(t, json.Unmarshal([]byte(`{"SchemaVersion":2,"Namespace":"ddde90ac-5d13-4c6f-8580-19d01b2a021a","WriterAuthority":{"Mode":"db_increment_v1","Epoch":1},"schema_version":2,"namespace":"ddde90ac-5d13-4c6f-8580-19d01b2a021a"}`), &supplied))
	require.Equal(t, UserPlatformQuotaCacheDomain{}, supplied)
}

func TestCheckUserPlatformQuotaEligibility_CacheHandoffNeverSeedsRawDatabaseObservation(t *testing.T) {
	for _, flusher := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("flusher=%t/legacy-cached=%t", flusher, cached), func(t *testing.T) {
				svc, repo, cache := quotaObservationFixture(t)
				svc.cfg.Database.UserPlatformQuotaFlusherEnabled = flusher
				if !cached {
					cache.entry = nil
				}
				cache.getErr = ErrUserPlatformQuotaCacheHandoffRequired
				// Even an apparently empty DB baseline is not a handoff proof.
				repo.rec.DailyUsageUSD, repo.rec.WeeklyUsageUSD, repo.rec.MonthlyUsageUSD = 0, 0, 0
				repo.rec.DailyWindowStart, repo.rec.WeeklyWindowStart, repo.rec.MonthlyWindowStart = nil, nil, nil
				err := svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai")
				require.ErrorIs(t, err, ErrBillingServiceUnavailable)
				require.ErrorIs(t, err, ErrUserPlatformQuotaCacheHandoffRequired)
				require.Equal(t, 1, repo.getCalls)
				require.Equal(t, 1, cache.getCalls)
				require.Zero(t, cache.casCalls)
				require.Zero(t, cache.setCalls)
			})
		}
	}
}

func TestCheckUserPlatformQuotaEligibility_CacheHandoffDoesNotInventQuotaConfiguration(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			svc, repo, cache := quotaObservationFixture(t)
			cache.getErr = ErrUserPlatformQuotaCacheHandoffRequired
			if missing {
				repo.rec = nil
			} else {
				repo.rec.DailyLimitUSD = nil
			}
			require.NoError(t, svc.checkUserPlatformQuotaEligibility(context.Background(), 1, "openai"))
			require.Equal(t, 1, repo.getCalls)
			require.Zero(t, cache.getCalls)
			require.Zero(t, cache.casCalls)
			require.Zero(t, cache.setCalls)
		})
	}
}
