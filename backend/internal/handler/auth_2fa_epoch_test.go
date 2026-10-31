//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	"github.com/gin-gonic/gin"
	"github.com/pquerna/otp/totp"
	"github.com/stretchr/testify/require"
)

func twoFactorEpochFixture(t *testing.T) (*AuthHandler, *userHandlerRepoStub, *oauthPendingFlowTotpCacheStub, *testutil.MemoryAuthSessions, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	secret := "JBSWY3DPEHPK3PXP"
	users := &userHandlerRepoStub{user: &service.User{
		// This existing synthetic account avoids unrelated Ent email-backfill
		// side effects in the lightweight handler fixture.
		ID: 113, Email: "two-factor@oidc-connect.invalid", Username: "Two-factor user",
		PasswordHash: "verified-password-hash", Role: service.RoleUser, Status: service.StatusActive,
		SessionEpoch: 1, TotpEnabled: true, TotpSecretEncrypted: &secret,
	}}
	cfg := &config.Config{JWT: config.JWTConfig{Secret: "two-factor-epoch-test", ExpireHour: 1, RefreshTokenExpireDays: 7}}
	settings := service.NewSettingService(&oauthPendingFlowSettingRepoStub{values: map[string]string{
		service.SettingKeyTotpEnabled: "true",
	}}, cfg)
	cache := &oauthPendingFlowTotpCacheStub{}
	ledger := testutil.NewAuthSessionRepository()
	auth := service.NewAuthService(nil, users, nil, nil, cfg, settings, nil, nil, nil, nil, nil, nil, nil)
	auth.SetAuthSessionRepository(ledger)
	h := &AuthHandler{
		authService: auth, userService: service.NewUserService(users, nil, nil, nil), settingSvc: settings,
		totpService: service.NewTotpService(users, oauthPendingFlowTotpEncryptorStub{}, cache, settings, nil, nil),
	}
	return h, users, cache, ledger, secret
}

func completeTwoFactorEpochLogin(t *testing.T, h *AuthHandler, tempToken, secret string) *httptest.ResponseRecorder {
	t.Helper()
	code, err := totp.GenerateCode(secret, time.Now().UTC())
	require.NoError(t, err)
	// The challenge deliberately carries a correct OTP. These cases must fail
	// because the earlier password proof was invalidated, not because of TOTP.
	require.NoError(t, h.totpService.VerifyCode(context.Background(), 113, code))
	body, err := json.Marshal(map[string]string{"temp_token": tempToken, "totp_code": code})
	require.NoError(t, err)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/auth/login/2fa", strings.NewReader(string(body)))
	c.Request.Header.Set("Content-Type", "application/json")
	h.Login2FA(c)
	return w
}

func TestAuth2FAPendingPasswordProofRejectsChangedEpoch(t *testing.T) {
	for _, event := range []string{"password_change", "revoke_all"} {
		t.Run(event, func(t *testing.T) {
			h, users, cache, ledger, secret := twoFactorEpochFixture(t)
			ctx := context.Background()
			tempToken, err := h.totpService.CreateLoginSession(ctx, users.user.ID, users.user.Email, users.user.SessionEpoch)
			require.NoError(t, err)
			pending, err := cache.GetLoginSession(ctx, tempToken)
			require.NoError(t, err)
			require.Equal(t, int64(1), pending.SessionEpoch)

			// The PostgreSQL trigger/transaction is covered by repository tests.
			// Mirror its resulting account epoch in both unit fixture authorities.
			var epoch int64
			if event == "password_change" {
				users.user.PasswordHash = "new-password-hash"
				epoch, err = ledger.RevokeAll(ctx, users.user.ID)
			} else {
				epoch, err = h.authService.RevokeAllSessionsWithEpoch(ctx, users.user.ID)
			}
			require.NoError(t, err)
			users.user.SessionEpoch = epoch
			require.Greater(t, epoch, pending.SessionEpoch)

			w := completeTwoFactorEpochLogin(t, h, tempToken, secret)
			require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
			var result struct {
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
			require.Equal(t, "TOKEN_REVOKED", result.Reason)
			require.NotContains(t, w.Body.String(), "access_token")
			require.NotContains(t, w.Body.String(), "refresh_token")
			pending, err = cache.GetLoginSession(ctx, tempToken)
			require.NoError(t, err)
			require.Nil(t, pending)
			sessions, err := ledger.List(ctx, users.user.ID)
			require.NoError(t, err)
			require.Empty(t, sessions)
		})
	}
}

func TestAuth2FALegacyPendingProofRequiresFreshPasswordLogin(t *testing.T) {
	h, users, cache, ledger, secret := twoFactorEpochFixture(t)
	ctx := context.Background()
	// A pre-upgrade Redis challenge has no durable epoch and receives no bypass.
	tempToken := "legacy-two-factor-challenge"
	require.NoError(t, cache.SetLoginSession(ctx, tempToken, &service.TotpLoginSession{
		UserID: users.user.ID, Email: users.user.Email, TokenExpiry: time.Now().Add(time.Minute),
	}, time.Minute))
	w := completeTwoFactorEpochLogin(t, h, tempToken, secret)
	require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "TOKEN_REVOKED")
	require.NotContains(t, w.Body.String(), "access_token")
	pending, err := cache.GetLoginSession(ctx, tempToken)
	require.NoError(t, err)
	require.Nil(t, pending)
	sessions, err := ledger.List(ctx, users.user.ID)
	require.NoError(t, err)
	require.Empty(t, sessions)
}

func TestAuth2FACurrentEpochStillCompletesLogin(t *testing.T) {
	h, users, cache, ledger, secret := twoFactorEpochFixture(t)
	ctx := context.Background()
	tempToken, err := h.totpService.CreateLoginSession(ctx, users.user.ID, users.user.Email, users.user.SessionEpoch)
	require.NoError(t, err)
	w := completeTwoFactorEpochLogin(t, h, tempToken, secret)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var result struct {
		Data struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.NotEmpty(t, result.Data.AccessToken)
	require.NotEmpty(t, result.Data.RefreshToken)
	claims, err := h.authService.ValidateToken(result.Data.AccessToken)
	require.NoError(t, err)
	require.Equal(t, users.user.SessionEpoch, claims.SessionEpoch)
	require.NoError(t, h.authService.ValidateActiveSession(ctx, claims))
	pending, err := cache.GetLoginSession(ctx, tempToken)
	require.NoError(t, err)
	require.Nil(t, pending)
	sessions, err := ledger.List(ctx, users.user.ID)
	require.NoError(t, err)
	require.Len(t, sessions, 1)
}
