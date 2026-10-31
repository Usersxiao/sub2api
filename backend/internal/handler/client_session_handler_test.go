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
	"github.com/stretchr/testify/require"
)

func sessionHandlerFixture(t *testing.T) (*ClientConfigurationHandler, *service.AuthService, string, *service.JWTClaims) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	auth := service.NewAuthService(nil, nil, nil, nil, &config.Config{JWT: config.JWTConfig{Secret: "session-test", ExpireHour: 1}}, nil, nil, nil, nil, nil, nil, nil, nil)
	auth.SetAuthSessionRepository(testutil.NewAuthSessionRepository())
	token, err := auth.GenerateToken(context.Background(), &service.User{SessionEpoch: 1, ID: 29, Email: "user@example.com", Role: service.RoleUser, Status: service.StatusActive})
	require.NoError(t, err)
	claims, err := auth.ValidateToken(token)
	require.NoError(t, err)
	h := &ClientConfigurationHandler{auth: auth, service: &clientConfigurationHandlerStub{config: service.DefaultClientConfiguration()}}
	return h, auth, token, claims
}

func sessionHandlerRequest(h *ClientConfigurationHandler, token, method, path, body string) *httptest.ResponseRecorder {
	r := gin.New()
	r.GET("/client/session", h.GetSession)
	r.GET("/client/sessions", h.ListSessions)
	r.POST("/client/sessions/revoke", h.RevokeSessions)
	r.DELETE("/client/sessions/:sid", h.RevokeDeviceSession)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestClientSessionProofUsesDurableScope(t *testing.T) {
	h, auth, token, claims := sessionHandlerFixture(t)
	w := sessionHandlerRequest(h, token, "GET", "/client/session?sid=forged&sessionEpoch=999", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	var envelope struct {
		Data clientSessionProof `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
	p := envelope.Data
	require.Equal(t, claims.SessionID, p.SID)
	require.Equal(t, claims.SessionEpoch, p.SessionEpoch)
	require.Equal(t, "29", p.UpstreamUserID)
	require.Equal(t, service.DefaultClientInstanceID, p.InstanceID)
	require.Equal(t, "active", p.State)
	require.Positive(t, p.ValidUntil.Sub(p.CheckedAt))
	require.LessOrEqual(t, p.ValidUntil.Sub(p.CheckedAt), 60*time.Second)
	require.False(t, p.ValidUntil.After(p.SessionExpiresAt))
	require.NoError(t, auth.RevokeUserSession(context.Background(), 29, claims.SessionID))
	require.Equal(t, 401, sessionHandlerRequest(h, token, "GET", "/client/session", "").Code)
	require.Equal(t, 401, sessionHandlerRequest(h, "", "GET", "/client/session", "").Code)
}

func TestClientSessionRevocationRequiresExactScope(t *testing.T) {
	for _, target := range []string{"current", "all"} {
		t.Run(target, func(t *testing.T) {
			h, auth, token, claims := sessionHandlerFixture(t)
			body := map[string]any{"schemaVersion": 1, "instanceId": service.DefaultClientInstanceID, "upstreamUserId": "29", "sid": claims.SessionID, "sessionEpoch": claims.SessionEpoch, "target": target}
			body["upstreamUserId"] = "30"
			raw, _ := json.Marshal(body)
			require.Equal(t, 400, sessionHandlerRequest(h, token, "POST", "/client/sessions/revoke", string(raw)).Code)
			require.NoError(t, auth.ValidateActiveSession(context.Background(), claims))
			body["upstreamUserId"] = "29"
			raw, _ = json.Marshal(body)
			w := sessionHandlerRequest(h, token, "POST", "/client/sessions/revoke", string(raw))
			require.Equal(t, 200, w.Code, w.Body.String())
			require.ErrorIs(t, auth.ValidateActiveSession(context.Background(), claims), service.ErrTokenRevoked)
			var envelope struct {
				Data struct {
					ResultingEpoch int64 `json:"resultingEpoch"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
			if target == "all" {
				require.Greater(t, envelope.Data.ResultingEpoch, claims.SessionEpoch)
			}
		})
	}
}

func TestClientDeviceSessionsCannotRevokeOtherAccount(t *testing.T) {
	h, auth, token, _ := sessionHandlerFixture(t)
	otherToken, err := auth.GenerateToken(context.Background(), &service.User{SessionEpoch: 1, ID: 30, Email: "other@example.com", Role: service.RoleUser, Status: service.StatusActive})
	require.NoError(t, err)
	otherClaims, err := auth.ValidateToken(otherToken)
	require.NoError(t, err)
	w := sessionHandlerRequest(h, token, "GET", "/client/sessions", "")
	require.Equal(t, 200, w.Code)
	for _, private := range []string{otherClaims.SessionID, "token_version", "binding_hash", "password", "refresh", "@"} {
		require.NotContains(t, w.Body.String(), private)
	}
	require.Equal(t, 401, sessionHandlerRequest(h, token, "DELETE", "/client/sessions/"+otherClaims.SessionID, "").Code)
	require.NoError(t, auth.ValidateActiveSession(context.Background(), otherClaims))
}
