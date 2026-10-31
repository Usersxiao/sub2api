//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func publishClientConfig(t *testing.T, settings *clientConfigSettingsStub, cfg ClientConfiguration) {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	settings.raw = string(raw)
}

func requireClientResolution(t *testing.T, svc *ClientConfigurationService, routeID string) *ClientInvocationResolution {
	t.Helper()
	snapshot, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	resolved, err := svc.ResolveInvocation(context.Background(), 1, routeID, snapshot.Catalog.ConfigVersion, snapshot.Catalog.EntitlementVersion)
	require.NoError(t, err)
	require.Equal(t, snapshot.Catalog.ClientConfigurationScope, resolved.Scope)
	return resolved
}

func TestClientInvocationResolutionKeepsIdenticalModelsInPublishedGroups(t *testing.T) {
	svc, settings, groups, channels, _ := clientConfigServiceFixture(t)
	cfg := clientConfigFixture(t)
	second := cfg.Routes[0]
	second.GroupIDs = []int64{8}
	second.Route.RouteID = "writing-other"
	cfg.Routes = append(cfg.Routes, second)
	publishClientConfig(t, settings, cfg)
	groups.groups = append(groups.groups, Group{ID: 8, Platform: PlatformOpenAI, Status: StatusActive})
	channels.channels[0].GroupIDs = []int64{7, 8}

	first := requireClientResolution(t, svc, "writing")
	other := requireClientResolution(t, svc, "writing-other")
	require.Equal(t, int64(7), first.GroupID)
	require.Equal(t, int64(8), other.GroupID)
	require.Equal(t, "test-model", first.ModelID)
	require.Equal(t, first.ModelID, other.ModelID)
	require.Equal(t, PlatformOpenAI, first.Platform)
	require.Equal(t, "responses", first.Route.Protocol)
	raw, err := json.Marshal(first)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(raw), "routing evidence must never serialize into a public DTO")
}

func TestClientInvocationResolutionUsesPublishedPriorityIndependentOfRows(t *testing.T) {
	svc, settings, groups, channels, _ := clientConfigServiceFixture(t)
	cfg := clientConfigFixture(t)
	cfg.Routes[0].GroupIDs = []int64{8, 7}
	publishClientConfig(t, settings, cfg)
	groups.groups = []Group{{ID: 7, Platform: PlatformOpenAI, Status: StatusActive}, {ID: 8, Platform: PlatformOpenAI, Status: StatusActive}}
	second := channels.channels[0]
	second.ID = 2
	second.GroupIDs = []int64{8}
	channels.channels = append(channels.channels, second)
	require.Equal(t, int64(8), requireClientResolution(t, svc, "writing").GroupID)
	before, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	channels.channels[0], channels.channels[1] = channels.channels[1], channels.channels[0]
	groups.groups[0], groups.groups[1] = groups.groups[1], groups.groups[0]
	require.Equal(t, int64(8), requireClientResolution(t, svc, "writing").GroupID)
	after, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, before.Catalog.EntitlementVersion, after.Catalog.EntitlementVersion)

	// An inaccessible priority group grants no access despite administrator role.
	groups.groups = []Group{{ID: 7, Platform: PlatformOpenAI, Status: StatusActive}}
	require.Equal(t, int64(7), requireClientResolution(t, svc, "writing").GroupID)
}

func TestClientInvocationResolutionBindsCandidateChangesWithVisibleRouteUnchanged(t *testing.T) {
	svc, settings, groups, channels, _ := clientConfigServiceFixture(t)
	cfg := clientConfigFixture(t)
	cfg.Routes[0].GroupIDs = []int64{7, 8}
	publishClientConfig(t, settings, cfg)
	groups.groups = append(groups.groups, Group{ID: 8, Platform: PlatformOpenAI, Status: StatusActive})
	second := channels.channels[0]
	second.ID = 2
	second.GroupIDs = []int64{8}
	channels.channels = append(channels.channels, second)

	before, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	channels.channels[0].Status = "disabled"
	after, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, before.Catalog.Routes, after.Catalog.Routes)
	require.Equal(t, before.Catalog.ConfigVersion, after.Catalog.ConfigVersion)
	require.NotEqual(t, before.Catalog.EntitlementVersion, after.Catalog.EntitlementVersion)
	_, err = svc.ResolveInvocation(context.Background(), 1, "writing", before.Catalog.ConfigVersion, before.Catalog.EntitlementVersion)
	require.ErrorIs(t, err, ErrClientGatewayChanged)
	require.Equal(t, int64(8), requireClientResolution(t, svc, "writing").GroupID)
}

func TestClientInvocationResolutionBindsChannelModelMapping(t *testing.T) {
	svc, _, _, channels, accounts := clientConfigServiceFixture(t)
	accounts.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"channel-model": "provider-model"}}
	channels.channels[0].ModelMapping[PlatformOpenAI]["test-model"] = "channel-model"

	before, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	resolved := requireClientResolution(t, svc, "writing")
	require.Equal(t, &clientChannelModelBinding{ChannelID: 1, Platform: PlatformOpenAI,
		SourceModel: "test-model", MappedModel: "channel-model"}, resolved.ChannelMapping)

	channels.channels[0].ModelMapping[PlatformOpenAI]["test-model"] = "channel-model-v2"
	accounts.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"channel-model-v2": "provider-model-v2"}}
	after, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, before.Catalog.ConfigVersion, after.Catalog.ConfigVersion)
	require.NotEqual(t, before.Catalog.EntitlementVersion, after.Catalog.EntitlementVersion,
		"a channel model remap invalidates the previously published invocation scope")
	_, err = svc.ResolveInvocation(context.Background(), 1, "writing", before.Catalog.ConfigVersion, before.Catalog.EntitlementVersion)
	require.ErrorIs(t, err, ErrClientGatewayChanged)
}

func TestClientInvocationResolutionRejectsConflictingChannelModelMappings(t *testing.T) {
	svc, _, _, channels, accounts := clientConfigServiceFixture(t)
	second := channels.channels[0]
	second.ID = 2
	second.ModelMapping = map[string]map[string]string{PlatformOpenAI: {"test-model": "other-channel-model"}}
	channels.channels = append(channels.channels, second)
	accounts.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{
		"test-model": "test-model", "other-channel-model": "provider-model",
	}}

	snapshot, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Empty(t, snapshot.Catalog.Routes,
		"the route must not depend on which supported channel happens to be listed first")
}

func TestClientInvocationResolutionBindsAccountModelMappings(t *testing.T) {
	svc, _, _, _, accounts := clientConfigServiceFixture(t)
	accounts.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"test-model": "provider-model-v1"}}

	before, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	resolved := requireClientResolution(t, svc, "writing")
	require.Equal(t, []clientAccountModelBinding{{AccountID: 1, AccountPlatform: PlatformOpenAI,
		InputModel: "test-model", UpstreamModel: "provider-model-v1"}}, resolved.AccountMappings)

	accounts.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"test-model": "provider-model-v2"}}
	after, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, before.Catalog.ConfigVersion, after.Catalog.ConfigVersion)
	require.NotEqual(t, before.Catalog.EntitlementVersion, after.Catalog.EntitlementVersion,
		"an account model remap invalidates the earlier invocation scope")
	_, err = svc.ResolveInvocation(context.Background(), 1, "writing", before.Catalog.ConfigVersion, before.Catalog.EntitlementVersion)
	require.ErrorIs(t, err, ErrClientGatewayChanged)
}

func TestClientInvocationResolutionRejectsDifferentUpstreamModelsAcrossAccounts(t *testing.T) {
	svc, _, _, _, accounts := clientConfigServiceFixture(t)
	accounts.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"test-model": "provider-model-a"}}
	accounts.accounts = append(accounts.accounts, Account{ID: 2, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"model_mapping": map[string]any{"test-model": "provider-model-b"}}})

	snapshot, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Empty(t, snapshot.Catalog.Routes,
		"a route with account-dependent upstream models would make token counting scheduler-dependent")
}

func TestClientInvocationResolutionValidatesVersionsStatusAndAvailability(t *testing.T) {
	svc, settings, groups, _, _ := clientConfigServiceFixture(t)
	before, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	for _, values := range [][3]string{
		{"writing", "cfg-old", before.Catalog.EntitlementVersion},
		{"writing", before.Catalog.ConfigVersion, "ent-old"},
	} {
		_, err = svc.ResolveInvocation(context.Background(), 1, values[0], values[1], values[2])
		require.ErrorIs(t, err, ErrClientGatewayChanged)
	}
	_, err = svc.ResolveInvocation(context.Background(), 1, "missing", before.Catalog.ConfigVersion, before.Catalog.EntitlementVersion)
	require.ErrorIs(t, err, ErrClientGatewayRevoked)
	_, err = svc.ResolveInvocation(context.Background(), 1, "../writing", before.Catalog.ConfigVersion, before.Catalog.EntitlementVersion)
	require.ErrorIs(t, err, ErrClientGatewayScope)

	cfg := clientConfigFixture(t)
	cfg.Defaults = map[string]string{}
	cfg.Routes[0].Route.Availability.Status = "disabled"
	cfg.Routes[0].Route.Availability.Reason = "Paused"
	publishClientConfig(t, settings, cfg)
	disabled, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, disabled.Catalog.Routes, 1)
	_, err = svc.ResolveInvocation(context.Background(), 1, "writing", disabled.Catalog.ConfigVersion, disabled.Catalog.EntitlementVersion)
	require.ErrorIs(t, err, ErrClientGatewayRevoked)

	groups.err = errors.New("directory dependency failed")
	_, err = svc.ResolveInvocation(context.Background(), 1, "writing", disabled.Catalog.ConfigVersion, disabled.Catalog.EntitlementVersion)
	require.EqualError(t, err, "directory dependency failed")
}

func TestClientInvocationResolutionUsesChannelAliasWithoutReplacingPublicModel(t *testing.T) {
	svc, _, _, channels, accounts := clientConfigServiceFixture(t)
	channels.channels[0].ModelMapping[PlatformOpenAI]["test-model"] = "upstream-alias"
	accounts.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"upstream-alias": "provider-private"}}
	resolved := requireClientResolution(t, svc, "writing")
	require.Equal(t, "test-model", resolved.ModelID)
	require.Equal(t, "test-model", resolved.Route.Target.ModelID)
}

func TestClientInvocationResolutionRejectsCompositePlatformAmbiguity(t *testing.T) {
	svc, _, groups, channels, accounts := clientConfigServiceFixture(t)
	groups.groups[0].Platform = PlatformComposite
	channels.channels[0].ModelMapping[PlatformAnthropic] = map[string]string{"test-model": "test-model"}
	accounts.accounts = append(accounts.accounts, Account{ID: 2, Platform: PlatformAnthropic, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"model_mapping": map[string]any{"test-model": "test-model"}}})
	before, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Empty(t, before.Catalog.Routes, "composite routes stay unavailable without a resolver")
	_, err = svc.ResolveInvocation(context.Background(), 1, "writing", before.Catalog.ConfigVersion, before.Catalog.EntitlementVersion)
	require.ErrorIs(t, err, ErrClientGatewayRevoked)

	svc.composite = NewCompositeRouteResolver(compositeRouteRepoStub{routes: []CompositeModelRoute{{
		ID: 91, GroupID: 7, PublicModel: "test-model", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformOpenAI, UpstreamModel: "test-model", Endpoint: CompositeRouteEndpointResponses, Enabled: true,
	}}})
	after, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, after.Catalog.Routes, 1)
	resolved := requireClientResolution(t, svc, "writing")
	require.Equal(t, PlatformOpenAI, resolved.Platform)
	require.NotNil(t, resolved.CompositeDecision)
	require.Equal(t, "test-model", resolved.CompositeDecision.UpstreamModel)
}

func TestClientInvocationResolutionBindsCompositeDecisionIntoEntitlement(t *testing.T) {
	svc, _, groups, channels, accounts := clientConfigServiceFixture(t)
	groups.groups[0].Platform = PlatformComposite
	channels.channels[0].ModelPricing = []ChannelModelPricing{{Platform: PlatformAnthropic, Models: []string{"test-model", "claude-private", "claude-next"}}}
	channels.channels[0].ModelMapping[PlatformAnthropic] = map[string]string{"test-model": "test-model", "claude-private": "claude-private", "claude-next": "claude-next"}
	accounts.accounts = append(accounts.accounts, Account{ID: 2, Platform: PlatformAnthropic, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"model_mapping": map[string]any{"claude-private": "claude-private", "claude-next": "claude-next"}}})
	svc.composite = NewCompositeRouteResolver(compositeRouteRepoStub{routes: []CompositeModelRoute{{
		ID: 92, GroupID: 7, PublicModel: "test-model", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformAnthropic, UpstreamModel: "claude-private", Endpoint: CompositeRouteEndpointResponses, Enabled: true,
	}}})

	before, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, before.Catalog.Routes, 1)
	resolved, err := svc.ResolveInvocation(context.Background(), 1, "writing", before.Catalog.ConfigVersion, before.Catalog.EntitlementVersion)
	require.NoError(t, err)
	require.Equal(t, "test-model", resolved.ModelID, "public model remains the authorization identity")
	require.Equal(t, PlatformAnthropic, resolved.Platform)
	require.NotNil(t, resolved.CompositeDecision)
	require.Equal(t, "claude-private", resolved.CompositeDecision.UpstreamModel)
	require.Equal(t, &clientChannelModelBinding{ChannelID: channels.channels[0].ID, Platform: PlatformAnthropic,
		SourceModel: "claude-private", MappedModel: "claude-private"}, resolved.ChannelMapping,
		"composite channel mapping starts from the resolved upstream model")

	svc.composite = NewCompositeRouteResolver(compositeRouteRepoStub{routes: []CompositeModelRoute{{
		ID: 92, GroupID: 7, PublicModel: "test-model", MatchType: CompositeRouteMatchExact,
		TargetPlatform: PlatformAnthropic, UpstreamModel: "claude-next", Endpoint: CompositeRouteEndpointResponses, Enabled: true,
	}}})
	after, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, after.Catalog.Routes, 1, "the replacement upstream remains available on the same target platform")
	require.NotEqual(t, before.Catalog.EntitlementVersion, after.Catalog.EntitlementVersion,
		"an upstream model change invalidates grants even when the provider platform is unchanged")
	_, err = svc.ResolveInvocation(context.Background(), 1, "writing", before.Catalog.ConfigVersion, before.Catalog.EntitlementVersion)
	require.ErrorIs(t, err, ErrClientGatewayChanged)
}
