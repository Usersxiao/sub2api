//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type clientConfigurationHandlerStub struct {
	userID int64
	saves  int
	err    error
	config service.ClientConfiguration
}

func (s *clientConfigurationHandlerStub) Get(context.Context) (*service.ClientConfiguration, error) {
	return &s.config, s.err
}
func (s *clientConfigurationHandlerStub) Save(_ context.Context, cfg *service.ClientConfiguration) error {
	s.saves++
	s.config = *cfg
	return s.err
}
func (s *clientConfigurationHandlerStub) Snapshot(_ context.Context, id int64) (*service.ClientConfigurationSnapshot, error) {
	s.userID = id
	return &service.ClientConfigurationSnapshot{SchemaVersion: 1, InstanceID: service.DefaultClientInstanceID, UpstreamUserID: "1"}, s.err
}

func clientConfigHandlerRequest(t *testing.T, stub *clientConfigurationHandlerStub, userID int64, role, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	h := &ClientConfigurationHandler{service: stub}
	r := gin.New()
	r.Use(func(c *gin.Context) {
		if userID > 0 {
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: userID})
		}
		if role != "" {
			c.Set(string(middleware.ContextKeyUserRole), role)
		}
		c.Next()
	})
	r.GET("/api/v1/client/configuration", h.GetConfiguration)
	r.GET("/api/v1/admin/client-config", h.GetAdminConfiguration)
	r.PUT("/api/v1/admin/client-config", h.PutAdminConfiguration)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestClientConfigurationHandlerAuthentication(t *testing.T) {
	stub := &clientConfigurationHandlerStub{config: service.DefaultClientConfiguration()}
	w := clientConfigHandlerRequest(t, stub, 0, "", http.MethodGet, "/api/v1/client/configuration", "")
	require.Equal(t, 401, w.Code)
	require.Equal(t, int64(0), stub.userID)
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		w = clientConfigHandlerRequest(t, stub, 1, service.RoleUser, method, "/api/v1/admin/client-config", `{}`)
		require.Equal(t, 403, w.Code)
	}
	require.Zero(t, stub.saves)
}

func TestClientConfigurationHandlerUsesVerifiedUser(t *testing.T) {
	stub := &clientConfigurationHandlerStub{config: service.DefaultClientConfiguration()}
	w := clientConfigHandlerRequest(t, stub, 1, service.RoleAdmin, http.MethodGet, "/api/v1/client/configuration?upstreamUserId=999&instanceId=PRIVATE", "")
	require.Equal(t, 200, w.Code)
	require.Equal(t, int64(1), stub.userID)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	require.Contains(t, w.Body.String(), `"code":0`)
	require.NotContains(t, w.Body.String(), "PRIVATE")
	require.NotContains(t, w.Body.String(), "999")
}

func TestClientConfigurationHandlerErrorDoesNotEchoBackend(t *testing.T) {
	stub := &clientConfigurationHandlerStub{err: errors.New("sub2api PRIVATE_TOKEN at http://private.invalid"), config: service.DefaultClientConfiguration()}
	for _, path := range []string{"/api/v1/client/configuration", "/api/v1/admin/client-config"} {
		w := clientConfigHandlerRequest(t, stub, 1, service.RoleAdmin, http.MethodGet, path, "")
		require.Equal(t, 503, w.Code)
		require.NotContains(t, w.Body.String(), "sub2api")
		require.NotContains(t, w.Body.String(), "PRIVATE")
		require.NotContains(t, w.Body.String(), "private.invalid")
	}
	stub.err = service.ErrUserNotFound
	w := clientConfigHandlerRequest(t, stub, 1, service.RoleUser, http.MethodGet, "/api/v1/client/configuration", "")
	require.Equal(t, 401, w.Code)
}

func TestClientConfigurationHandlerAdminPublish(t *testing.T) {
	stub := &clientConfigurationHandlerStub{config: service.DefaultClientConfiguration()}
	raw, err := json.Marshal(service.DefaultClientConfiguration())
	require.NoError(t, err)
	w := clientConfigHandlerRequest(t, stub, 1, service.RoleAdmin, http.MethodPut, "/api/v1/admin/client-config", string(raw))
	require.Equal(t, 200, w.Code)
	require.Equal(t, 1, stub.saves)
	w = clientConfigHandlerRequest(t, stub, 1, service.RoleAdmin, http.MethodGet, "/api/v1/admin/client-config", "")
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Body.String(), service.DefaultClientInstanceID)
	for _, body := range []string{`{"schemaVersion":1,"apiKey":"PRIVATE"}`, string(raw) + `{}`, strings.Repeat(" ", 1024*1024+1)} {
		w = clientConfigHandlerRequest(t, stub, 1, service.RoleAdmin, http.MethodPut, "/api/v1/admin/client-config", body)
		require.Equal(t, 400, w.Code)
		require.Equal(t, 1, stub.saves)
		require.NotContains(t, w.Body.String(), "PRIVATE")
	}
	stub.err = errors.New("PRIVATE storage failure")
	w = clientConfigHandlerRequest(t, stub, 1, service.RoleAdmin, http.MethodPut, "/api/v1/admin/client-config", string(raw))
	require.Equal(t, 400, w.Code)
	require.NotContains(t, w.Body.String(), "PRIVATE")
}

func TestClientConfigurationHandlerPublishesChatOnlyWhenExecutionIsAssembled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, enabled := range []bool{false, true} {
		stub := &clientConfigurationHandlerStub{config: service.DefaultClientConfiguration()}
		h := &ClientConfigurationHandler{service: stub}
		if enabled {
			h.EnableChatExecution()
		}
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1})
			c.Next()
		})
		r.GET("/api/v1/client/configuration", h.GetConfiguration)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/client/configuration", nil))
		require.Equal(t, 200, w.Code)
		var envelope struct {
			Data struct {
				Bootstrap struct {
					Features struct {
						Chat, Editing, Media, Runninghub, BackgroundTasks bool
					} `json:"features"`
				} `json:"bootstrap"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
		features := envelope.Data.Bootstrap.Features
		require.Equal(t, enabled, features.Chat)
		require.False(t, features.Editing || features.Media || features.Runninghub || features.BackgroundTasks)
	}
}
