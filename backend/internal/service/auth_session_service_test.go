//go:build unit

package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

type sessionServiceUserRepo struct {
	service.UserRepository
	user *service.User
}

func (r *sessionServiceUserRepo) GetByID(_ context.Context, id int64) (*service.User, error) {
	if id != r.user.ID {
		return nil, service.ErrUserNotFound
	}
	copy := *r.user
	return &copy, nil
}

func (r *sessionServiceUserRepo) GetByEmail(ctx context.Context, email string) (*service.User, error) {
	if email != r.user.Email {
		return nil, service.ErrUserNotFound
	}
	return r.GetByID(ctx, r.user.ID)
}

func authSessionServiceFixture() (*service.AuthService, *testutil.MemoryAuthSessions, *service.User) {
	user := &service.User{ID: 41, Email: "session@example.com", PasswordHash: "fixture-hash", Role: service.RoleUser, Status: service.StatusActive, SessionEpoch: 1}
	cfg := &config.Config{JWT: config.JWTConfig{Secret: "session-unit-secret", AccessTokenExpireMinutes: 15, RefreshTokenExpireDays: 7}}
	svc := service.NewAuthService(nil, &sessionServiceUserRepo{user: user}, nil, nil, cfg, nil, nil, nil, nil, nil, nil, nil, nil)
	repo := testutil.NewAuthSessionRepository()
	svc.SetAuthSessionRepository(repo)
	return svc, repo, user
}

func TestAuthSessionServiceRequiresDurableAuthority(t *testing.T) {
	svc, _, user := authSessionServiceFixture()
	svc.SetAuthSessionRepository(nil)
	ctx := context.Background()
	_, err := svc.GenerateToken(ctx, user)
	require.ErrorIs(t, err, service.ErrServiceUnavailable)
	_, err = svc.GenerateTokenPair(ctx, user, "")
	require.ErrorIs(t, err, service.ErrServiceUnavailable)
	require.ErrorIs(t, svc.ValidateActiveSession(ctx, &service.JWTClaims{}), service.ErrServiceUnavailable)
	require.ErrorIs(t, svc.RevokeAllUserTokens(ctx, user.ID), service.ErrServiceUnavailable)
}

func TestAuthSessionServiceRotationKeepsIdentityAndAbsoluteExpiry(t *testing.T) {
	svc, _, user := authSessionServiceFixture()
	ctx := context.Background()
	first, err := svc.GenerateTokenPair(ctx, user, "")
	require.NoError(t, err)
	firstClaims, err := svc.ValidateToken(first.AccessToken)
	require.NoError(t, err)
	require.Equal(t, first.Session.SessionID, firstClaims.SessionID)
	require.Positive(t, firstClaims.SessionEpoch)
	require.NoError(t, svc.ValidateActiveSession(ctx, firstClaims))

	rotated, err := svc.RefreshTokenPair(ctx, first.RefreshToken)
	require.NoError(t, err)
	require.NotEqual(t, first.RefreshToken, rotated.RefreshToken)
	require.Equal(t, first.Session.SessionID, rotated.Session.SessionID)
	require.Equal(t, first.Session.ExpiresAt, rotated.Session.ExpiresAt)
	require.Equal(t, first.Session.Epoch, rotated.Session.Epoch)
	encoded, err := json.Marshal(rotated.TokenPair)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "SessionID")
	require.NotContains(t, string(encoded), "fixture-hash")
}

func TestAuthSessionServiceRefreshReplayRevokesAccessAndReplacement(t *testing.T) {
	svc, _, user := authSessionServiceFixture()
	ctx := context.Background()
	first, err := svc.GenerateTokenPair(ctx, user, "")
	require.NoError(t, err)
	rotated, err := svc.RefreshTokenPair(ctx, first.RefreshToken)
	require.NoError(t, err)
	_, err = svc.RefreshTokenPair(ctx, first.RefreshToken)
	require.ErrorIs(t, err, service.ErrRefreshTokenReused)
	claims, err := svc.ValidateToken(rotated.AccessToken)
	require.NoError(t, err)
	require.ErrorIs(t, svc.ValidateActiveSession(ctx, claims), service.ErrTokenRevoked)
	_, err = svc.RefreshTokenPair(ctx, rotated.RefreshToken)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
}

func TestAuthSessionServiceCurrentAndAllRevocation(t *testing.T) {
	svc, _, user := authSessionServiceFixture()
	ctx := context.Background()
	one, err := svc.GenerateTokenPair(ctx, user, "")
	require.NoError(t, err)
	two, err := svc.GenerateTokenPair(ctx, user, "")
	require.NoError(t, err)
	oneClaims, _ := svc.ValidateToken(one.AccessToken)
	twoClaims, _ := svc.ValidateToken(two.AccessToken)
	require.ErrorIs(t, svc.RevokeUserSession(ctx, 999, two.Session.SessionID), service.ErrTokenRevoked)
	require.NoError(t, svc.RevokeRefreshToken(ctx, one.RefreshToken))
	require.ErrorIs(t, svc.ValidateActiveSession(ctx, oneClaims), service.ErrTokenRevoked)
	require.NoError(t, svc.ValidateActiveSession(ctx, twoClaims))
	epoch, err := svc.RevokeAllSessionsWithEpoch(ctx, user.ID)
	require.NoError(t, err)
	require.Greater(t, epoch, twoClaims.SessionEpoch)
	require.ErrorIs(t, svc.ValidateActiveSession(ctx, twoClaims), service.ErrTokenRevoked)
	user.SessionEpoch = epoch
	fresh, err := svc.GenerateTokenPair(ctx, user, "")
	require.NoError(t, err)
	require.Equal(t, epoch, fresh.Session.Epoch)
}

func TestAuthSessionServiceRejectsLegacyOrAlteredSessionClaims(t *testing.T) {
	svc, _, user := authSessionServiceFixture()
	ctx := context.Background()
	pair, err := svc.GenerateTokenPair(ctx, user, "")
	require.NoError(t, err)
	claims, _ := svc.ValidateToken(pair.AccessToken)
	legacy := *claims
	legacy.SessionEpoch = 0
	require.ErrorIs(t, svc.ValidateActiveSession(ctx, &legacy), service.ErrTokenRevoked)
	altered := *claims
	altered.BindingHash = "different-binding"
	require.ErrorIs(t, svc.ValidateActiveSession(ctx, &altered), service.ErrTokenRevoked)
}

func TestAuthSessionServiceOutagePreservesServiceError(t *testing.T) {
	svc, repo, user := authSessionServiceFixture()
	ctx := context.Background()
	token, err := svc.GenerateToken(ctx, user)
	require.NoError(t, err)
	claims, _ := svc.ValidateToken(token)
	repo.ReadErr = errors.New("database connection closed")
	require.ErrorIs(t, svc.ValidateActiveSession(ctx, claims), service.ErrServiceUnavailable)
	repo.ReadErr = nil
	require.NoError(t, svc.ValidateActiveSession(ctx, claims))
}

func TestAuthSessionServiceAccessOnlyRefreshDoesNotExtendSession(t *testing.T) {
	svc, _, user := authSessionServiceFixture()
	ctx := context.Background()
	token, err := svc.GenerateToken(ctx, user)
	require.NoError(t, err)
	before, _ := svc.ValidateToken(token)
	refreshed, err := svc.RefreshToken(ctx, token)
	require.NoError(t, err)
	after, _ := svc.ValidateToken(refreshed)
	require.Equal(t, before.SessionID, after.SessionID)
	require.Equal(t, before.ExpiresAt, after.ExpiresAt)
	require.WithinDuration(t, time.Now().Add(15*time.Minute), after.ExpiresAt.Time, time.Second)
}

func TestAuthSessionServicePasswordVerificationCreatesNoHiddenSession(t *testing.T) {
	svc, _, user := authSessionServiceFixture()
	require.NoError(t, user.SetPassword("unit-login-password"))
	ctx := context.Background()
	verified, err := svc.AuthenticatePassword(ctx, user.Email, "unit-login-password")
	require.NoError(t, err)
	require.Equal(t, user.ID, verified.ID)
	sessions, err := svc.ListSessions(ctx, user.ID)
	require.NoError(t, err)
	require.Empty(t, sessions)
	_, err = svc.GenerateTokenPair(ctx, verified, "")
	require.NoError(t, err)
	sessions, err = svc.ListSessions(ctx, user.ID)
	require.NoError(t, err)
	require.Len(t, sessions, 1)
}

func TestAuthSessionServiceExpiredClaimsRequireAuthenticSignature(t *testing.T) {
	svc, _, user := authSessionServiceFixture()
	pair, err := svc.GenerateTokenPair(context.Background(), user, "")
	require.NoError(t, err)
	claims, err := svc.ValidateToken(pair.AccessToken)
	require.NoError(t, err)
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Hour))
	for _, tc := range []struct {
		name       string
		secret     string
		futureNbf  bool
		wantError  error
		wantClaims bool
	}{
		{"signed expiration", "session-unit-secret", false, service.ErrTokenExpired, true},
		{"forged expired identity", "wrong-signing-secret", false, service.ErrInvalidToken, false},
		{"expired and future nbf", "session-unit-secret", true, service.ErrInvalidToken, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := *claims
			if tc.futureNbf {
				copy.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour))
			}
			token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, &copy).SignedString([]byte(tc.secret))
			require.NoError(t, err)
			verified, err := svc.ValidateToken(token)
			require.ErrorIs(t, err, tc.wantError)
			if tc.wantClaims {
				require.NotNil(t, verified)
			} else {
				require.Nil(t, verified)
			}
		})
	}
}
