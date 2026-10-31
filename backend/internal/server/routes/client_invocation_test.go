//go:build unit

package routes

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestClientInvocationRouteIsRegisteredAndFailsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterClientInvocationRoutes(router.Group("/api/v1"), &handler.Handlers{
		ClientInvocation: handler.NewClientInvocationHandler(nil),
	})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/client/invoke/deepseek-flash", strings.NewReader(`{"input":[]}`))
	request.Header.Set("Authorization", "Bearer adg_fixture")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Contains(t, response.Body.String(), "MODEL_ADMISSION_UNAVAILABLE")
	require.NotContains(t, response.Body.String(), "deepseek-flash")
	require.NotContains(t, response.Body.String(), "input")
}

func TestClientInvocationRouteRegistrationRequiresHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterClientInvocationRoutes(router.Group("/api/v1"), &handler.Handlers{})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/client/invoke/route", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusNotFound, response.Code)
}

func TestClientInvocationRecoveryRouteIsRegisteredAndFailsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterClientInvocationRoutes(router.Group("/api/v1"), &handler.Handlers{
		ClientInvocation: handler.NewClientInvocationHandler(nil),
	})

	request := httptest.NewRequest(http.MethodGet, "/api/v1/client/invocations/project-a/request-a", nil)
	request.Header.Set("Authorization", "Bearer adg_fixture")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Contains(t, response.Body.String(), "MODEL_ADMISSION_UNAVAILABLE")
	require.NotContains(t, response.Body.String(), "request-a")
}
