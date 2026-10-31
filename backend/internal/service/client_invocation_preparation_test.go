//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type invocationGrantFixture struct {
	grant  *ClientGatewayGrant
	err    error
	reads  int
	onRead func(int)
}

func (s *invocationGrantFixture) Validate(_ context.Context, token, route, operation string) (*ClientGatewayGrant, error) {
	s.reads++
	if s.onRead != nil {
		s.onRead(s.reads)
	}
	if token != "adg_fixture" || route != "chat" || operation != "invoke" {
		return nil, ErrClientGatewayRevoked
	}
	if s.err != nil {
		return nil, s.err
	}
	copy := *s.grant
	return &copy, nil
}

type invocationResolutionFixture struct {
	resolution *ClientInvocationResolution
	err        error
	userID     int64
	reads      int
}

func (s *invocationResolutionFixture) ResolveInvocation(_ context.Context, userID int64, route, config, entitlement string) (*ClientInvocationResolution, error) {
	s.userID = userID
	s.reads++
	if s.err != nil {
		return nil, s.err
	}
	return s.resolution, nil
}

type invocationBillingFixture struct {
	identity  *APIKey
	err       error
	reads     int
	consumer  string
	groupID   int64
	onResolve func()
}

func (s *invocationBillingFixture) Resolve(_ context.Context, grant *ClientGatewayGrant, groupID int64) (*APIKey, error) {
	s.reads++
	s.consumer, s.groupID = grant.ConsumerID, groupID
	if s.onResolve != nil {
		s.onResolve()
	}
	return s.identity, s.err
}

func invocationPreparationFixture(t *testing.T) (*ClientInvocationPreparationService, []byte, *invocationGrantFixture, *invocationResolutionFixture, *invocationBillingFixture) {
	t.Helper()
	var route ClientModelRoute
	require.NoError(t, json.Unmarshal([]byte(`{"routeId":"chat","target":{"kind":"model","modelId":"shared-model"},"displayName":"Shared model","providerDisplay":"Provider","purposes":["chat"],"operation":"text","availability":{"status":"available"},"protocol":"responses","transport":"json","inputKinds":["text"],"outputKinds":["text"],"parameters":[],"prices":[],"capabilities":{}}`), &route))
	route.Parameters = []ClientModelParameter{invocationBudgetParameter(route.Protocol)}
	grants := &invocationGrantFixture{grant: &ClientGatewayGrant{ClientGatewayScope: ClientGatewayScope{1, "instance", "7", "sid", 2},
		GrantID: "grant", ClientID: "autodesign", ConsumerID: "consumer", ConfigVersion: "cfg-1", EntitlementVersion: "ent-1"}}
	routes := &invocationResolutionFixture{resolution: &ClientInvocationResolution{Scope: ClientConfigurationScope{InstanceID: "instance", ConfigVersion: "cfg-1", EntitlementVersion: "ent-1"},
		GroupID: 42, ModelID: "shared-model", Platform: PlatformOpenAI, Route: route}}
	groupID := int64(42)
	consumer := "consumer"
	billing := &invocationBillingFixture{identity: &APIKey{ID: 81, Kind: APIKeyKindClientConsumer, ClientConsumerID: &consumer, UserID: 7, GroupID: &groupID, Status: StatusActive,
		User: &User{ID: 7, Status: StatusActive}, Group: &Group{ID: 42, Status: StatusActive, Platform: PlatformOpenAI}}}
	raw := []byte(`{"projectId":"project-a","routeId":"chat","operation":"text","configVersion":"cfg-1","entitlementVersion":"ent-1","requestId":"request-a","input":[{"kind":"text","text":"Draft a title"}],"parameters":{}}`)
	return NewClientInvocationPreparationService(grants, routes, billing), raw, grants, routes, billing
}

func TestClientInvocationPreparationBindsGrantRouteAndRealBillingIdentity(t *testing.T) {
	svc, raw, grants, routes, billing := invocationPreparationFixture(t)
	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	require.Equal(t, int64(7), routes.userID)
	require.Equal(t, "consumer", billing.consumer)
	require.Equal(t, int64(42), billing.groupID)
	require.Equal(t, int64(81), prepared.BillingIdentity.ID)
	require.Equal(t, 2, grants.reads)
	require.Contains(t, string(prepared.Text.Payload), `"model":"shared-model"`)
	// Accidental JSON response serialization yields no internal identity or input.
	serialized, err := json.Marshal(prepared)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(serialized))
}

func TestClientInvocationPreparationBuildsCompositePayloadFromResolvedUpstream(t *testing.T) {
	svc, raw, _, routes, billing := invocationPreparationFixture(t)
	routes.resolution.Platform = PlatformAnthropic
	routes.resolution.CompositeDecision = &CompositeRouteDecision{
		Matched: true, Source: CompositeRouteSourceExplicit, GroupID: 42, PublicModel: "shared-model",
		TargetPlatform: PlatformAnthropic, UpstreamModel: "claude-private", Endpoint: CompositeRouteEndpointResponses,
	}
	billing.identity.Group.Platform = PlatformComposite

	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	require.Equal(t, "shared-model", prepared.Resolution.ModelID)
	require.Equal(t, "claude-private", prepared.Text.ModelID)
	require.Contains(t, string(prepared.Text.Payload), `"model":"claude-private"`)
	require.Equal(t, CompositeRouteEndpointResponses, prepared.Resolution.CompositeDecision.Endpoint)
}

func TestClientInvocationPreparationRejectsMismatchedCompositeDecisionBeforeAccounting(t *testing.T) {
	for _, change := range []string{"group", "public-model", "platform", "endpoint", "unmatched", "unsupported-protocol"} {
		t.Run(change, func(t *testing.T) {
			svc, raw, _, routes, billing := invocationPreparationFixture(t)
			routes.resolution.Platform = PlatformAnthropic
			routes.resolution.CompositeDecision = &CompositeRouteDecision{
				Matched: true, Source: CompositeRouteSourceExplicit, GroupID: 42, PublicModel: "shared-model",
				TargetPlatform: PlatformAnthropic, UpstreamModel: "claude-private", Endpoint: CompositeRouteEndpointResponses,
			}
			billing.identity.Group.Platform = PlatformComposite
			switch change {
			case "group":
				routes.resolution.CompositeDecision.GroupID++
			case "public-model":
				routes.resolution.CompositeDecision.PublicModel = "other-model"
			case "platform":
				routes.resolution.CompositeDecision.TargetPlatform = PlatformOpenAI
			case "endpoint":
				routes.resolution.CompositeDecision.Endpoint = CompositeRouteEndpointMessages
			case "unmatched":
				routes.resolution.CompositeDecision.Matched = false
			case "unsupported-protocol":
				routes.resolution.Route.Protocol = "gemini"
			}
			_, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
			require.Error(t, err)
			require.Zero(t, billing.reads)
		})
	}
}

func TestClientInvocationPreparationRejectsBeforeAccounting(t *testing.T) {
	for _, name := range []string{"malformed", "path", "grant", "version", "route", "private-scope", "parameters"} {
		t.Run(name, func(t *testing.T) {
			svc, raw, grants, routes, billing := invocationPreparationFixture(t)
			path := "chat"
			switch name {
			case "malformed":
				raw = []byte(`{"apiKey":"SECRET"}`)
			case "path":
				path = "another-route"
			case "grant":
				grants.err = ErrTokenRevoked
			case "version":
				grants.grant.ConfigVersion = "cfg-new"
			case "route":
				routes.err = ErrClientGatewayRevoked
			case "private-scope":
				routes.resolution.Scope.InstanceID = "other-instance"
			case "parameters":
				routes.resolution.Route.Protocol = "unimplemented"
			}
			_, err := svc.Prepare(context.Background(), "adg_fixture", path, raw)
			require.Error(t, err)
			require.Zero(t, billing.reads)
		})
	}
}

func TestClientInvocationPreparationRejectsAccountingIdentityMismatch(t *testing.T) {
	for _, name := range []string{"id", "kind", "consumer", "credential", "user", "group", "platform", "empty-platform", "inactive", "dependency"} {
		t.Run(name, func(t *testing.T) {
			svc, raw, _, routes, billing := invocationPreparationFixture(t)
			switch name {
			case "id":
				billing.identity.ID = 0
			case "kind":
				billing.identity.Kind = APIKeyKindCredential
			case "consumer":
				consumer := "another-consumer"
				billing.identity.ClientConsumerID = &consumer
			case "credential":
				billing.identity.Key = "credential-sentinel"
			case "user":
				billing.identity.User.ID = 8
			case "group":
				billing.identity.Group.ID = 43
			case "platform":
				billing.identity.Group.Platform = PlatformAnthropic
			case "empty-platform":
				routes.resolution.Platform = ""
			case "inactive":
				billing.identity.Status = StatusDisabled
			case "dependency":
				billing.err = errors.New("database unavailable")
			}
			_, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
			require.Error(t, err)
		})
	}
}

func TestClientInvocationPreparationRejectsRevocationDuringAccountingIO(t *testing.T) {
	svc, raw, grants, _, billing := invocationPreparationFixture(t)
	billing.onResolve = func() { grants.err = ErrTokenRevoked }
	_, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
	require.ErrorIs(t, err, ErrTokenRevoked)
	require.Equal(t, 2, grants.reads)
}

func TestClientInvocationPreparationRejectsChangedGrantAfterAccountingIO(t *testing.T) {
	for _, field := range []string{"consumer", "config", "entitlement"} {
		t.Run(field, func(t *testing.T) {
			svc, raw, grants, _, _ := invocationPreparationFixture(t)
			grants.onRead = func(read int) {
				if read != 2 {
					return
				}
				switch field {
				case "consumer":
					grants.grant.ConsumerID = "another-consumer"
				case "config":
					grants.grant.ConfigVersion = "cfg-new"
				case "entitlement":
					grants.grant.EntitlementVersion = "ent-new"
				}
			}
			_, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
			require.ErrorIs(t, err, ErrClientGatewayRevoked)
		})
	}
}

func TestClientInvocationPreparationDeepSeekPreservesThinking(t *testing.T) {
	for _, tc := range []struct {
		platform, protocol string
		enabled            bool
	}{
		{PlatformDeepseek, "chat-completions", true},
		{PlatformOpenAI, "chat-completions", false},
		{PlatformDeepseek, "responses", false},
	} {
		t.Run(tc.platform+"/"+tc.protocol, func(t *testing.T) {
			svc, raw, _, routes, billing := invocationPreparationFixture(t)
			routes.resolution.Platform = tc.platform
			billing.identity.Group.Platform = tc.platform
			routes.resolution.Route.Protocol = tc.protocol
			routes.resolution.Route.Parameters = []ClientModelParameter{invocationBudgetParameter(tc.protocol)}
			prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
			require.NoError(t, err)
			var payload map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(prepared.Text.Payload, &payload))
			if tc.enabled {
				require.JSONEq(t, `{"type":"enabled"}`, string(payload["thinking"]))
			} else {
				require.NotContains(t, payload, "thinking")
			}
			// The same adapter payload must pass the original counter/proof boundary.
			_, err = CheckClientInvocationTokenBudget(context.Background(), prepared, nil)
			require.NoError(t, err)
			require.Equal(t, int64(128), prepared.Text.OutputTokens)
			if tc.enabled {
				payload["thinking"] = json.RawMessage(`{"type":"disabled"}`)
				prepared.Text.Payload, err = json.Marshal(payload)
				require.NoError(t, err)
				_, err = CheckClientInvocationTokenBudget(context.Background(), prepared, nil)
				require.ErrorIs(t, err, ErrClientInvocationParameters)
			}
		})
	}
}
