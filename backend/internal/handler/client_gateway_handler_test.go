//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type gatewayHandlerConfiguration struct {
	snapshot *service.ClientConfigurationSnapshot
}

func (s *gatewayHandlerConfiguration) Snapshot(context.Context, int64) (*service.ClientConfigurationSnapshot, error) {
	return s.snapshot, nil
}

type gatewayHandlerRepository struct {
	grant  *service.ClientGatewayGrant
	hash   string
	writes int
}

func (r *gatewayHandlerRepository) Issue(_ context.Context, g *service.ClientGatewayGrant, hash, previous, consumer string) (*service.ClientGatewayGrant, error) {
	r.writes++
	copy := *g
	r.grant = &copy
	r.hash = hash
	return &copy, nil
}
func (r *gatewayHandlerRepository) GetByTokenHash(_ context.Context, hash string) (*service.ClientGatewayGrant, error) {
	if hash != r.hash {
		return nil, service.ErrClientGatewayRevoked
	}
	return r.grant, nil
}
func (r *gatewayHandlerRepository) Revoke(context.Context, *service.AuthSession, string, string) (time.Time, error) {
	return time.Now().UTC(), nil
}

func gatewayHandlerRequest(h *ClientConfigurationHandler, token, path, body, contentType string) *httptest.ResponseRecorder {
	r := gin.New()
	r.POST("/client/gateway-session", h.CreateGatewaySession)
	r.POST("/client/gateway-session/revoke", h.RevokeGatewaySession)
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestClientGatewayHandlerBindsToVerifiedSessionAndRejectsMalformedRequests(t *testing.T) {
	h, auth, token, claims := sessionHandlerFixture(t)
	var route service.ClientModelRoute
	route.RouteID = "writing"
	route.Availability.Status = "available"
	configuration := &gatewayHandlerConfiguration{snapshot: &service.ClientConfigurationSnapshot{InstanceID: service.DefaultClientInstanceID, Catalog: service.ClientConfigurationCatalog{ClientConfigurationScope: service.ClientConfigurationScope{ConfigVersion: "cfg-1", EntitlementVersion: "ent-1"}, Routes: []service.ClientModelRoute{route}}}}
	repo := &gatewayHandlerRepository{}
	h.gateway = service.NewClientGatewayService(configuration, repo)
	body := map[string]any{"schemaVersion": 1, "instanceId": service.DefaultClientInstanceID, "upstreamUserId": "29", "sid": claims.SessionID, "sessionEpoch": claims.SessionEpoch, "clientId": "autodesign", "configVersion": "cfg-1", "entitlementVersion": "ent-1"}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	for _, bad := range []string{`{}`, `{"accessToken":"private"}`, string(raw) + `{}`, strings.Repeat("x", 17*1024)} {
		w := gatewayHandlerRequest(h, token, "/client/gateway-session", bad, "application/json")
		require.Equal(t, 400, w.Code)
		require.NotContains(t, w.Body.String(), "private")
	}
	require.Equal(t, 400, gatewayHandlerRequest(h, token, "/client/gateway-session", string(raw), "text/plain").Code)
	require.Equal(t, 401, gatewayHandlerRequest(h, "", "/client/gateway-session", string(raw), "application/json").Code)
	body["upstreamUserId"] = "30"
	forged, _ := json.Marshal(body)
	require.Equal(t, 400, gatewayHandlerRequest(h, token, "/client/gateway-session", string(forged), "application/json").Code)
	require.Zero(t, repo.writes)
	w := gatewayHandlerRequest(h, token, "/client/gateway-session", string(raw), "application/json")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	var envelope struct {
		Data service.ClientGatewayCredential `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope))
	require.Equal(t, claims.SessionID, envelope.Data.SID)
	require.Equal(t, claims.SessionEpoch, envelope.Data.SessionEpoch)
	require.NotEmpty(t, envelope.Data.AccessToken)
	for _, private := range []string{"token_hash", "password", "groupIds", "api_key", "refresh_token"} {
		require.NotContains(t, w.Body.String(), private)
	}
	delete(body, "clientId")
	delete(body, "configVersion")
	delete(body, "entitlementVersion")
	body["upstreamUserId"] = "29"
	body["grantId"] = envelope.Data.GrantID
	revocation, _ := json.Marshal(body)
	w = gatewayHandlerRequest(h, token, "/client/gateway-session/revoke", string(revocation), "application/json")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "accessToken")
	require.NoError(t, auth.RevokeUserSession(context.Background(), 29, claims.SessionID))
	require.Equal(t, 401, gatewayHandlerRequest(h, token, "/client/gateway-session", string(raw), "application/json").Code)
}

func TestClientGatewayHandlerEmptyCatalogRetainsAccountSession(t *testing.T) {
	h, auth, token, claims := sessionHandlerFixture(t)
	configuration := &gatewayHandlerConfiguration{snapshot: &service.ClientConfigurationSnapshot{InstanceID: service.DefaultClientInstanceID, Catalog: service.ClientConfigurationCatalog{ClientConfigurationScope: service.ClientConfigurationScope{ConfigVersion: "cfg-1", EntitlementVersion: "ent-1"}}}}
	repo := &gatewayHandlerRepository{}
	h.gateway = service.NewClientGatewayService(configuration, repo)
	body, _ := json.Marshal(service.ClientGatewaySessionRequest{ClientGatewayScope: service.ClientGatewayScope{SchemaVersion: 1, InstanceID: service.DefaultClientInstanceID, UpstreamUserID: "29", SID: claims.SessionID, SessionEpoch: claims.SessionEpoch}, ClientID: "autodesign", ConfigVersion: "cfg-1", EntitlementVersion: "ent-1"})
	w := gatewayHandlerRequest(h, token, "/client/gateway-session", string(body), "application/json")
	require.Equal(t, 403, w.Code)
	require.Contains(t, w.Body.String(), "MODEL_ROUTE_REVOKED")
	require.Zero(t, repo.writes)
	require.NoError(t, auth.ValidateActiveSession(context.Background(), claims))
}
