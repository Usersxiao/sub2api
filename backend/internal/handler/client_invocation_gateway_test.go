package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestClientInvocationSelectionLeaseIsBoundAndConsumedOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(5)
	selected := &service.AccountSelectionResult{Account: &service.Account{ID: 91}}
	lease := newClientInvocationSelectionLease("responses", service.PlatformAnthropic, groupID, "upstream-model", selected)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), clientInvocationSelectionLeaseKey{}, lease))

	selection, used, err := clientInvocationSelectionFromContext(c, "responses", service.PlatformAnthropic, &groupID, "upstream-model", nil)
	require.NoError(t, err)
	require.True(t, used)
	require.Same(t, selected, selection)

	selection, used, err = clientInvocationSelectionFromContext(c, "responses", service.PlatformAnthropic, &groupID, "upstream-model", nil)
	require.True(t, used, "a consumed invocation lease must remain authoritative during failover")
	require.ErrorIs(t, err, service.ErrClientInvocationAdmissionUnavailable)
	require.Nil(t, selection, "a single-dispatch invocation must stop instead of falling through to the regular scheduler")
}

func TestClientInvocationSelectionLeaseRejectsMismatchedRouteAndReleasesOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(5)
	releaseCalls := 0
	selected := &service.AccountSelectionResult{
		Account:     &service.Account{ID: 92},
		ReleaseFunc: func() { releaseCalls++ },
	}
	lease := newClientInvocationSelectionLease("responses", service.PlatformAnthropic, groupID, "upstream-model", selected)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), clientInvocationSelectionLeaseKey{}, lease))

	_, used, err := clientInvocationSelectionFromContext(c, "chat-completions", service.PlatformAnthropic, &groupID, "upstream-model", nil)
	require.True(t, used)
	require.ErrorIs(t, err, service.ErrClientInvocationAdmissionUnavailable)

	lease.releaseSelection()
	selected.ReleaseFunc()
	lease.releaseSelection()
	require.Equal(t, 1, releaseCalls)
}

func TestClientInvocationGatewayPathIsClosedToTextProtocols(t *testing.T) {
	for protocol, want := range map[string]string{
		"messages":         "/v1/messages",
		"responses":        "/v1/responses",
		"chat-completions": "/v1/chat/completions",
	} {
		path, ok := clientInvocationGatewayPath(protocol)
		require.True(t, ok)
		require.Equal(t, want, path)
	}
	for _, protocol := range []string{"", "gemini", "stream", "tools", "../../v1/responses"} {
		_, ok := clientInvocationGatewayPath(protocol)
		require.False(t, ok, protocol)
	}
}

func TestClientInvocationBillingIdentityMatchesMiddlewareProjection(t *testing.T) {
	groupID := int64(5)
	consumerID := "consumer-fixture"
	identity := &service.APIKey{
		ID: 73, Kind: service.APIKeyKindClientConsumer, ClientConsumerID: &consumerID,
		UserID: 9, Status: service.StatusActive, GroupID: &groupID,
		User:  &service.User{ID: 9, SessionEpoch: 4, Status: service.StatusActive},
		Group: &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformOpenAI, Status: service.StatusActive},
	}
	projection := *identity
	require.True(t, clientInvocationBillingIdentityMatches(identity, &projection), "middleware adds compiled rules on a value copy")

	for _, mutate := range []func(*service.APIKey){
		func(key *service.APIKey) { key.ID++ },
		func(key *service.APIKey) { key.UserID++ },
		func(key *service.APIKey) { user := *key.User; user.SessionEpoch++; key.User = &user },
		func(key *service.APIKey) { other := "other-consumer"; key.ClientConsumerID = &other },
		func(key *service.APIKey) { other := groupID + 1; key.GroupID = &other },
		func(key *service.APIKey) { key.Key = "credential" },
	} {
		mutated := projection
		mutate(&mutated)
		require.False(t, clientInvocationBillingIdentityMatches(identity, &mutated))
	}
}

func TestClientInvocationAdmissionDefersCompositeUntilMappingIsBudgetBound(t *testing.T) {
	consumerID := "consumer-fixture"
	groupID := int64(5)
	identity := &service.APIKey{
		ID: 73, Kind: service.APIKeyKindClientConsumer, ClientConsumerID: &consumerID,
		UserID: 9, User: &service.User{ID: 9}, GroupID: &groupID,
		Group: &service.Group{ID: groupID, Platform: service.PlatformComposite}, Status: service.StatusAPIKeyActive,
	}
	adapter := &ClientInvocationGatewayAdapter{}
	_, _, err := adapter.AdmitClientInvocation(context.Background(), &service.PreparedClientInvocation{
		Grant:           &service.ClientGatewayGrant{ConsumerID: consumerID},
		Resolution:      &service.ClientInvocationResolution{GroupID: groupID},
		BillingIdentity: identity,
	})
	require.ErrorIs(t, err, service.ErrClientInvocationAdmissionUnavailable)
}

func TestClientInvocationUserSlotMarkerIsBoundToInternalIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	clientIdentity := &service.APIKey{ID: 73, Kind: service.APIKeyKindClientConsumer}
	c.Set(clientInvocationUserSlotContextKey, int64(73))

	calls := 0
	acquire := func() (func(), error) {
		calls++
		return func() {}, nil
	}
	release, err := acquireClientInvocationAwareUserSlot(c, clientIdentity, acquire)
	require.NoError(t, err)
	require.NotNil(t, release)
	require.Zero(t, calls, "the existing handler must reuse the adapter's acquired user slot")

	ordinaryIdentity := &service.APIKey{ID: 73, Kind: service.APIKeyKindCredential}
	release, err = acquireClientInvocationAwareUserSlot(c, ordinaryIdentity, acquire)
	require.NoError(t, err)
	require.NotNil(t, release)
	require.Equal(t, 1, calls, "ordinary gateway identities must keep the standard concurrency path")

	clientIdentity.ID = 74
	_, err = acquireClientInvocationAwareUserSlot(c, clientIdentity, acquire)
	require.NoError(t, err)
	require.Equal(t, 2, calls, "a different internal identity must not reuse the slot")
}

func TestClientInvocationIngressBindingKeepsRequestMetadataContextOnly(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/models/invoke", nil)
	request.RemoteAddr = "203.0.113.8:4100"
	request.Header.Set("User-Agent", "AutoDesign test")
	request.Header.Set("Authorization", "Bearer secret")
	ctx := WithClientInvocationIngressRequest(context.Background(), request, "203.0.113.9")
	ingress, ok := ctx.Value(clientInvocationIngressRequestKey{}).(clientInvocationIngressMetadata)
	require.True(t, ok)
	require.Equal(t, "203.0.113.8:4100", ingress.remoteAddr)
	require.Equal(t, "203.0.113.9", ingress.clientIP)
	require.Equal(t, "AutoDesign test", ingress.headers.Get("User-Agent"))
	require.Empty(t, ingress.headers.Get("Authorization"))
	require.Empty(t, ingress.headers.Get("Cookie"))
}

type clientInvocationProtocolHandlerFixture struct {
	protocol      string
	requestBody   []byte
	sawSettlement bool
	calls         int
}

func (h *clientInvocationProtocolHandlerFixture) Messages(c *gin.Context) {
	h.respond(c, "messages")
}

func (h *clientInvocationProtocolHandlerFixture) Responses(c *gin.Context) {
	h.respond(c, "responses")
}

func (h *clientInvocationProtocolHandlerFixture) ChatCompletions(c *gin.Context) {
	h.respond(c, "chat-completions")
}

func (h *clientInvocationProtocolHandlerFixture) respond(c *gin.Context, protocol string) {
	h.calls++
	h.protocol = protocol
	h.sawSettlement = service.RequiredUsageSettlement(c.Request.Context())
	h.requestBody, _ = io.ReadAll(c.Request.Body)
	c.Data(http.StatusOK, "application/json", []byte(`{"complete":true}`))
}

func TestClientInvocationGatewayDispatchReusesProtocolHandlersAndSettlementContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		protocol string
		platform string
		wantPath string
		wantSide string
	}{
		{protocol: "messages", platform: service.PlatformAnthropic, wantPath: "/v1/messages", wantSide: "gateway"},
		{protocol: "responses", platform: service.PlatformAnthropic, wantPath: "/v1/responses", wantSide: "gateway"},
		{protocol: "chat-completions", platform: service.PlatformAnthropic, wantPath: "/v1/chat/completions", wantSide: "gateway"},
		{protocol: "messages", platform: service.PlatformOpenAI, wantPath: "/v1/messages", wantSide: "openai"},
		{protocol: "responses", platform: service.PlatformOpenAI, wantPath: "/v1/responses", wantSide: "openai"},
		{protocol: "chat-completions", platform: service.PlatformOpenAI, wantPath: "/v1/chat/completions", wantSide: "openai"},
	} {
		t.Run(test.protocol+"/"+test.platform, func(t *testing.T) {
			groupID := int64(5)
			gateway := &clientInvocationProtocolHandlerFixture{}
			openAI := &clientInvocationProtocolHandlerFixture{}
			adapter := &ClientInvocationGatewayAdapter{gateway: gateway, openAI: openAI}
			prepared := &service.PreparedClientInvocation{
				Text:            &service.ValidatedClientTextInvocation{Protocol: test.protocol, Payload: []byte(`{"model":"fixture-model","stream":false}`)},
				Resolution:      &service.ClientInvocationResolution{GroupID: groupID, ModelID: "fixture-model", Platform: test.platform},
				BillingIdentity: &service.APIKey{ID: 23, Kind: service.APIKeyKindClientConsumer, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: test.platform}},
			}
			ctx, _ := service.WithRequiredUsageSettlement(context.Background(), "client:invocation-abc", prepared.BillingIdentity.ID)
			recorder := newClientInvocationResponseWriter()
			c, _ := gin.CreateTestContext(recorder)
			path, ok := clientInvocationGatewayPath(test.protocol)
			require.True(t, ok)
			request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(prepared.Text.Payload))
			request = request.WithContext(ctx)
			c.Request = request
			c.Set(clientInvocationUserSlotContextKey, prepared.BillingIdentity.ID)
			selection := &service.AccountSelectionResult{Account: &service.Account{ID: 42}}
			lease := newClientInvocationSelectionLease(test.protocol, test.platform, groupID, "fixture-model", selection)
			state := &clientInvocationGatewayState{prepared: prepared, context: c, response: recorder, selection: lease}
			dispatchContext := context.WithValue(ctx, clientInvocationGatewayStateKey{}, state)
			c.Request = c.Request.WithContext(dispatchContext)

			response, err := adapter.DispatchClientInvocation(dispatchContext, prepared)
			require.NoError(t, err)
			require.Equal(t, []byte(`{"complete":true}`), response)
			selected, other := gateway, openAI
			if test.wantSide == "openai" {
				selected, other = openAI, gateway
			}
			require.Equal(t, 1, selected.calls)
			require.Equal(t, test.protocol, selected.protocol)
			require.True(t, selected.sawSettlement)
			require.True(t, bytes.Equal(prepared.Text.Payload, selected.requestBody))
			require.Zero(t, other.calls)
		})
	}
}

func TestClientInvocationGatewayDispatchFailsClosedWithoutOwnedContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	gateway := &clientInvocationProtocolHandlerFixture{}
	adapter := &ClientInvocationGatewayAdapter{gateway: gateway}
	prepared := &service.PreparedClientInvocation{
		Text:            &service.ValidatedClientTextInvocation{Protocol: "responses", Payload: []byte(`{"model":"fixture-model"}`)},
		Resolution:      &service.ClientInvocationResolution{Platform: service.PlatformAnthropic},
		BillingIdentity: &service.APIKey{ID: 23, Kind: service.APIKeyKindClientConsumer, Group: &service.Group{Platform: service.PlatformAnthropic}},
	}
	_, err := adapter.DispatchClientInvocation(context.Background(), prepared)
	require.ErrorIs(t, err, errClientInvocationGatewayAdapterUnavailable)
	require.Zero(t, gateway.calls)
}
