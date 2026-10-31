//go:build unit

package service

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUserPlatformQuotaWriterAuthorityRequiresSupportedDBModeAndPositiveEpoch(t *testing.T) {
	for _, epoch := range []int64{1, 2, math.MaxInt64} {
		require.NoError(t, ValidateUserPlatformQuotaWriterAuthority(UserPlatformQuotaWriterAuthority{Mode: UserPlatformQuotaWriterModeDB, Epoch: epoch}))
	}
	for _, tc := range []struct {
		name      string
		authority UserPlatformQuotaWriterAuthority
		want      error
	}{
		{name: "missing mode", authority: UserPlatformQuotaWriterAuthority{Epoch: 1}, want: ErrUserPlatformQuotaWriterModeUnsupported},
		{name: "unknown mode", authority: UserPlatformQuotaWriterAuthority{Mode: "future", Epoch: 1}, want: ErrUserPlatformQuotaWriterModeUnsupported},
		{name: "snapshot mode is not implemented", authority: UserPlatformQuotaWriterAuthority{Mode: UserPlatformQuotaWriterModeSnapshot, Epoch: 1}, want: ErrUserPlatformQuotaWriterModeUnsupported},
		{name: "missing epoch", authority: UserPlatformQuotaWriterAuthority{Mode: UserPlatformQuotaWriterModeDB}, want: ErrUserPlatformQuotaWriterUnavailable},
		{name: "negative epoch", authority: UserPlatformQuotaWriterAuthority{Mode: UserPlatformQuotaWriterModeDB, Epoch: -1}, want: ErrUserPlatformQuotaWriterUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, ValidateUserPlatformQuotaWriterAuthority(tc.authority), tc.want)
		})
	}
}

func TestUserPlatformQuotaWriterAuthorityAndReceiptStayPrivate(t *testing.T) {
	_, receipt, _ := ordinaryPlatformDeliveryFixture(t)
	receipt.WriterEpoch = 9
	authority := UserPlatformQuotaWriterAuthority{Mode: UserPlatformQuotaWriterModeDB, Epoch: receipt.WriterEpoch}
	for _, value := range []any{authority, receipt} {
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(encoded))
	}
	var supplied UserPlatformQuotaWriterAuthority
	require.NoError(t, json.Unmarshal([]byte(`{"Mode":"db_increment_v1","Epoch":9,"mode":"db_increment_v1","epoch":9}`), &supplied))
	require.Equal(t, UserPlatformQuotaWriterAuthority{}, supplied, "public JSON cannot grant persisted writer authority")
	var suppliedReceipt UsageBillingOrdinaryPlatformReceipt
	require.NoError(t, json.Unmarshal([]byte(`{"WriterEpoch":9,"writer_epoch":9}`), &suppliedReceipt))
	require.Zero(t, suppliedReceipt.WriterEpoch)
}
