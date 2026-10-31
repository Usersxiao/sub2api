//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type clientConfigSettingsStub struct {
	raw    string
	err    error
	writes int
}

func (s *clientConfigSettingsStub) GetValue(context.Context, string) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	if s.raw == "" {
		return "", ErrSettingNotFound
	}
	return s.raw, nil
}
func (s *clientConfigSettingsStub) Set(_ context.Context, _ string, value string) error {
	s.writes++
	s.raw = value
	return s.err
}

type clientConfigUsersStub struct {
	user *User
	err  error
}

func (s *clientConfigUsersStub) GetByID(context.Context, int64) (*User, error) { return s.user, s.err }

type clientConfigGroupsStub struct {
	groups []Group
	err    error
}

func (s *clientConfigGroupsStub) GetAvailableGroups(context.Context, int64) ([]Group, error) {
	return s.groups, s.err
}

type clientConfigGroupReaderStub struct{ err error }

func (s *clientConfigGroupReaderStub) GetByID(_ context.Context, id int64) (*Group, error) {
	return &Group{ID: id}, s.err
}

type clientConfigChannelsStub struct {
	channels []Channel
	err      error
}

func (s *clientConfigChannelsStub) ListAll(context.Context) ([]Channel, error) {
	return s.channels, s.err
}

type clientConfigAccountsStub struct {
	accounts []Account
	err      error
}

func (s *clientConfigAccountsStub) ListModelAvailabilityCandidates(context.Context, *int64, []string, bool) ([]Account, error) {
	return s.accounts, s.err
}

const clientConfigRouteFixture = `{"routeId":"writing","target":{"kind":"model","modelId":"test-model"},"displayName":"Writing","providerDisplay":"AI","purposes":["chat","design"],"operation":"text","availability":{"status":"available"},"protocol":"responses","transport":"json","capabilities":{"streaming":true,"tools":true,"structuredOutput":false,"vision":false,"uploads":false,"asynchronous":false,"cancellation":false},"inputKinds":["text"],"outputKinds":["text"],"parameters":[],"limits":{},"prices":[]}`

func clientConfigFixture(t *testing.T) ClientConfiguration {
	t.Helper()
	cfg := DefaultClientConfiguration()
	var route ClientModelRoute
	require.NoError(t, json.Unmarshal([]byte(clientConfigRouteFixture), &route))
	cfg.Routes = []ClientConfiguredRoute{{GroupIDs: []int64{7}, Route: route}}
	cfg.Defaults["chat"] = "writing"
	return cfg
}

func clientConfigServiceFixture(t *testing.T) (*ClientConfigurationService, *clientConfigSettingsStub, *clientConfigGroupsStub, *clientConfigChannelsStub, *clientConfigAccountsStub) {
	t.Helper()
	cfg := clientConfigFixture(t)
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	settings := &clientConfigSettingsStub{raw: string(raw)}
	groups := &clientConfigGroupsStub{groups: []Group{{ID: 7, Platform: PlatformOpenAI, Status: StatusActive}}}
	channels := &clientConfigChannelsStub{channels: []Channel{{ID: 1, Name: "PRIVATE_CHANNEL", Status: StatusActive, GroupIDs: []int64{7}, ModelMapping: map[string]map[string]string{PlatformOpenAI: {"test-model": "test-model"}}}}}
	accounts := &clientConfigAccountsStub{accounts: []Account{{ID: 1, Name: "PRIVATE_ACCOUNT", Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"model_mapping": map[string]any{"test-model": "test-model"}, "api_key": "PRIVATE_SECRET"}}}}
	svc := NewClientConfigurationService(settings, &clientConfigUsersStub{user: &User{ID: 1, Role: RoleAdmin, Status: StatusActive, Balance: -1.25}}, groups, &clientConfigGroupReaderStub{}, channels, accounts, &GatewayService{}, nil)
	svc.now = func() time.Time { return time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC) }
	return svc, settings, groups, channels, accounts
}

func TestClientConfigurationDefaultSnapshot(t *testing.T) {
	svc, settings, _, _, _ := clientConfigServiceFixture(t)
	settings.raw = ""
	snapshot, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, DefaultClientInstanceID, snapshot.InstanceID)
	require.Equal(t, "1", snapshot.UpstreamUserID)
	require.Empty(t, snapshot.Catalog.Routes)
	require.NotNil(t, snapshot.Catalog.Routes)
	require.Equal(t, snapshot.Bootstrap.ClientConfigurationScope, snapshot.Catalog.ClientConfigurationScope)
	require.Equal(t, snapshot.Bootstrap.CatalogRefresh.ExpiresAt, snapshot.Catalog.ExpiresAt)
	require.Equal(t, 30, snapshot.Bootstrap.CatalogRefresh.PollAfterSeconds)
	expires, err := time.Parse(time.RFC3339Nano, snapshot.Catalog.ExpiresAt)
	require.NoError(t, err)
	require.Equal(t, time.Minute, expires.Sub(svc.now()))
	require.Equal(t, "-1.25", snapshot.Bootstrap.Quotas[0].Remaining)
	raw, err := json.Marshal(snapshot)
	require.NoError(t, err)
	for _, name := range []string{"groupIds", "principalId", "sid", "epoch", "PRIVATE_", "token", "api_key"} {
		require.NotContains(t, string(raw), name)
	}
	require.False(t, snapshot.Bootstrap.Features.Chat)
	require.False(t, snapshot.Bootstrap.Features.Editing)
	require.False(t, snapshot.Bootstrap.Features.Media)
	require.False(t, snapshot.Bootstrap.Features.Runninghub)
	require.False(t, snapshot.Bootstrap.Features.BackgroundTasks)
}

func TestClientConfigurationPermissions(t *testing.T) {
	cases := []struct {
		name   string
		change func(*clientConfigGroupsStub, *clientConfigChannelsStub, *clientConfigAccountsStub)
		count  int
	}{
		{"configured", func(_ *clientConfigGroupsStub, _ *clientConfigChannelsStub, _ *clientConfigAccountsStub) {}, 1},
		{"channel alias resolves before account check", func(g *clientConfigGroupsStub, c *clientConfigChannelsStub, a *clientConfigAccountsStub) {
			g.groups[0].ModelAllowlist = GroupModelAllowlist{Enabled: true, Models: []string{"test-model"}}
			c.channels[0].ModelMapping[PlatformOpenAI]["test-model"] = "upstream-model"
			a.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"upstream-model": "provider-model"}}
		}, 1},
		{"channel alias target unsupported by account", func(_ *clientConfigGroupsStub, c *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			c.channels[0].ModelMapping[PlatformOpenAI]["test-model"] = "upstream-model"
		}, 0},
		{"channel wildcard resolves before account check", func(_ *clientConfigGroupsStub, c *clientConfigChannelsStub, a *clientConfigAccountsStub) {
			c.channels[0].ModelMapping = map[string]map[string]string{PlatformOpenAI: {"test-*": "upstream-model"}}
			c.channels[0].ModelPricing = []ChannelModelPricing{{Platform: PlatformOpenAI, Models: []string{"test-model"}}}
			a.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"upstream-model": "provider-model"}}
		}, 1},
		{"admin has no group shortcut", func(g *clientConfigGroupsStub, _ *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			g.groups = nil
		}, 0},
		{"disabled group", func(g *clientConfigGroupsStub, _ *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			g.groups[0].Status = "disabled"
		}, 0},
		{"group model revoked", func(g *clientConfigGroupsStub, _ *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			g.groups[0].ModelAllowlist = GroupModelAllowlist{Enabled: true, Models: []string{"other-model"}}
		}, 0},
		{"no channels", func(_ *clientConfigGroupsStub, c *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			c.channels = nil
		}, 0},
		{"disabled channel", func(_ *clientConfigGroupsStub, c *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			c.channels[0].Status = "disabled"
		}, 0},
		{"unbound channel", func(_ *clientConfigGroupsStub, c *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			c.channels[0].GroupIDs = []int64{8}
		}, 0},
		{"channel model removed", func(_ *clientConfigGroupsStub, c *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			c.channels[0].ModelMapping = nil
		}, 0},
		{"wrong channel platform", func(_ *clientConfigGroupsStub, c *clientConfigChannelsStub, _ *clientConfigAccountsStub) {
			c.channels[0].ModelMapping = map[string]map[string]string{PlatformAnthropic: {"test-model": "test-model"}}
		}, 0},
		{"no accounts", func(_ *clientConfigGroupsStub, _ *clientConfigChannelsStub, a *clientConfigAccountsStub) {
			a.accounts = nil
		}, 0},
		{"disabled account", func(_ *clientConfigGroupsStub, _ *clientConfigChannelsStub, a *clientConfigAccountsStub) {
			a.accounts[0].Status = "disabled"
		}, 0},
		{"unschedulable account", func(_ *clientConfigGroupsStub, _ *clientConfigChannelsStub, a *clientConfigAccountsStub) {
			a.accounts[0].Schedulable = false
		}, 0},
		{"account model removed", func(_ *clientConfigGroupsStub, _ *clientConfigChannelsStub, a *clientConfigAccountsStub) {
			a.accounts[0].Credentials = map[string]any{"model_mapping": map[string]any{"other-model": "other-model"}}
		}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, g, c, a := clientConfigServiceFixture(t)
			tc.change(g, c, a)
			snapshot, err := svc.Snapshot(context.Background(), 1)
			require.NoError(t, err)
			require.Len(t, snapshot.Catalog.Routes, tc.count)
			if tc.count == 0 {
				require.Empty(t, snapshot.Catalog.Defaults)
			} else {
				require.Equal(t, "writing", snapshot.Catalog.Defaults["chat"])
				raw, err := json.Marshal(snapshot)
				require.NoError(t, err)
				for _, private := range []string{"PRIVATE_", "groupIds", "GroupIDs", "Credentials", "api_key"} {
					require.NotContains(t, string(raw), private)
				}
			}
		})
	}
}

func TestClientConfigurationUsesExistingSubscriptionAndGroupPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		exclusive, restricted, subscribed, allowed bool
		count                                      int
	}{
		{"public", false, false, false, false, 1},
		{"restricted public", false, true, false, false, 0},
		{"explicit public", false, true, false, true, 1},
		{"exclusive", true, false, false, false, 0},
		{"explicit exclusive", true, false, false, true, 1},
		{"subscription required", false, false, false, false, 0},
		{"active subscription", false, false, true, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, _, _ := clientConfigServiceFixture(t)
			user := &User{ID: 1, Role: RoleAdmin, RestrictPublicGroups: tc.restricted}
			if tc.allowed {
				user.AllowedGroups = []int64{7}
			}
			group := Group{ID: 7, Platform: PlatformOpenAI, Status: StatusActive, IsExclusive: tc.exclusive}
			if strings.Contains(tc.name, "subscription") {
				group.SubscriptionType = SubscriptionTypeSubscription
			}
			subs := &visibilitySubRepo{}
			if tc.subscribed {
				subs.subscriptions = []UserSubscription{{UserID: 1, GroupID: 7, Status: SubscriptionStatusActive, ExpiresAt: time.Now().Add(time.Hour)}}
			}
			svc.groups = &APIKeyService{userRepo: &visibilityUserRepo{user: user}, groupRepo: &visibilityGroupRepo{groups: []Group{group}}, userSubRepo: subs}
			snapshot, err := svc.Snapshot(context.Background(), 1)
			require.NoError(t, err)
			require.Len(t, snapshot.Catalog.Routes, tc.count)
		})
	}
}

func TestClientConfigurationVersionsAndAtomicScope(t *testing.T) {
	svc, settings, groups, _, _ := clientConfigServiceFixture(t)
	first, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	svc.now = func() time.Time { return time.Date(2026, 10, 24, 12, 0, 10, 0, time.UTC) }
	second, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, first.Catalog.ConfigVersion, second.Catalog.ConfigVersion)
	require.Equal(t, first.Catalog.EntitlementVersion, second.Catalog.EntitlementVersion)
	require.Equal(t, first.Catalog.ETag, second.Catalog.ETag)
	require.NotEqual(t, first.Catalog.ExpiresAt, second.Catalog.ExpiresAt)
	groups.groups = nil
	third, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, first.Catalog.ConfigVersion, third.Catalog.ConfigVersion)
	require.NotEqual(t, first.Catalog.EntitlementVersion, third.Catalog.EntitlementVersion)
	require.Equal(t, third.Bootstrap.EntitlementVersion, third.Catalog.EntitlementVersion)
	cfg := clientConfigFixture(t)
	cfg.Routes[0].Route.DisplayName = "New published name"
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	settings.raw = string(raw)
	fourth, err := svc.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.NotEqual(t, first.Catalog.ConfigVersion, fourth.Catalog.ConfigVersion)
}

func TestClientConfigurationValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ClientConfiguration)
	}{
		{"duplicate route", func(c *ClientConfiguration) { c.Routes = append(c.Routes, c.Routes[0]) }},
		{"missing group binding", func(c *ClientConfiguration) { c.Routes[0].GroupIDs = nil }},
		{"duplicate group binding", func(c *ClientConfiguration) { c.Routes[0].GroupIDs = []int64{7, 7} }},
		{"missing default", func(c *ClientConfiguration) { c.Defaults["chat"] = "missing" }},
		{"default purpose mismatch", func(c *ClientConfiguration) { c.Defaults["image"] = "writing" }},
		{"disabled default", func(c *ClientConfiguration) {
			c.Routes[0].Route.Availability.Status = "disabled"
			c.Routes[0].Route.Availability.Reason = "Disabled"
		}},
		{"runninghub excluded", func(c *ClientConfiguration) { c.Routes[0].Route.Protocol = "runninghub" }},
		{"workflow excluded", func(c *ClientConfiguration) { c.Routes[0].Route.Target.Kind = "workflow" }},
		{"invented price excluded", func(c *ClientConfiguration) { c.Routes[0].Route.Prices = []json.RawMessage{json.RawMessage(`{}`)} }},
		{"negative limit", func(c *ClientConfiguration) { v := int64(-1); c.Routes[0].Route.Limits.FileBytes = &v }},
		{"empty input", func(c *ClientConfiguration) { c.Routes[0].Route.InputKinds = nil }},
		{"duplicate purpose", func(c *ClientConfiguration) { c.Routes[0].Route.Purposes = []string{"chat", "chat"} }},
		{"unsafe client version", func(c *ClientConfiguration) { c.MinClientVersion = "1.2.3-beta" }},
		{"route limit", func(c *ClientConfiguration) { c.Routes = make([]ClientConfiguredRoute, 201) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clientConfigFixture(t)
			tc.change(&cfg)
			require.Error(t, validateClientConfiguration(&cfg))
		})
	}
	cfg := clientConfigFixture(t)
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	_, err = ParseClientConfiguration(raw)
	require.NoError(t, err)
	_, err = ParseClientConfiguration([]byte(strings.Replace(string(raw), `"routeId":"writing"`, `"routeId":"writing","apiKey":"PRIVATE"`, 1)))
	require.Error(t, err)
	_, err = ParseClientConfiguration(append(raw, []byte(`{}`)...))
	require.Error(t, err)
	_, err = ParseClientConfiguration([]byte(strings.Repeat(" ", 1024*1024+1)))
	require.Error(t, err)
}

func TestClientConfigurationParameterParity(t *testing.T) {
	for _, tc := range []struct {
		name, declaration string
		valid             bool
	}{
		{"emoji UTF16 length two", `{"name":"prompt","label":"Prompt","editable":true,"type":"string","default":"😀","minLength":2,"maxLength":2}`, true},
		{"emoji exceeds one UTF16 unit", `{"name":"prompt","label":"Prompt","editable":true,"type":"string","default":"😀","minLength":1,"maxLength":1}`, false},
		{"anchored numeric step", `{"name":"amount","label":"Amount","editable":true,"type":"number","default":0.3,"minimum":0.1,"maximum":1,"step":0.2}`, true},
		{"misaligned numeric step", `{"name":"amount","label":"Amount","editable":true,"type":"number","default":0.2,"minimum":0.1,"maximum":1,"step":0.2}`, false},
		{"unsafe integer", `{"name":"amount","label":"Amount","editable":true,"type":"integer","default":9007199254740992,"minimum":0,"maximum":9007199254740992}`, false},
		{"reserved parameter", `{"name":"api_key","label":"Key","editable":true,"type":"string","default":"x","minLength":0,"maxLength":9}`, false},
		{"enum default", `{"name":"mode","label":"Mode","editable":true,"type":"enum","default":"a","options":[{"value":"a","label":"A"}]}`, true},
		{"enum missing default", `{"name":"mode","label":"Mode","editable":true,"type":"enum","default":"b","options":[{"value":"a","label":"A"}]}`, false},
		{"boolean with extra bound", `{"name":"toggle","label":"Toggle","editable":true,"type":"boolean","default":true,"minimum":0}`, false},
		{"boolean null default", `{"name":"toggle","label":"Toggle","editable":true,"type":"boolean","default":null}`, false},
		{"boolean missing default", `{"name":"toggle","label":"Toggle","editable":true,"type":"boolean"}`, false},
		{"string null default", `{"name":"prompt","label":"Prompt","editable":true,"type":"string","default":null,"minLength":0,"maxLength":5}`, false},
		{"number null default", `{"name":"amount","label":"Amount","editable":true,"type":"number","default":null,"minimum":0,"maximum":1}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p ClientModelParameter
			require.NoError(t, json.Unmarshal([]byte(tc.declaration), &p))
			err := validateClientParameter(p)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestClientConfigurationSaveAndFailures(t *testing.T) {
	svc, settings, groups, channels, accounts := clientConfigServiceFixture(t)
	cfg := clientConfigFixture(t)
	require.NoError(t, svc.Save(context.Background(), &cfg))
	require.Equal(t, 1, settings.writes)
	loaded, err := svc.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, cfg, *loaded)
	svc.groupReader = &clientConfigGroupReaderStub{err: errors.New("missing group")}
	require.Error(t, svc.Save(context.Background(), &cfg))
	require.Equal(t, 1, settings.writes)
	for _, tc := range []struct {
		name string
		set  func(error)
	}{
		{"settings", func(err error) { settings.err = err }},
		{"groups", func(err error) { groups.err = err }},
		{"channels", func(err error) { channels.err = err }},
		{"accounts", func(err error) { accounts.err = err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.set(errors.New("private backend error"))
			snapshot, err := svc.Snapshot(context.Background(), 1)
			require.Error(t, err)
			require.Nil(t, snapshot)
			tc.set(nil)
		})
	}
	svc.users = &clientConfigUsersStub{user: &User{ID: 1, Status: "disabled"}}
	_, err = svc.Snapshot(context.Background(), 1)
	require.ErrorIs(t, err, ErrUserNotFound)
}
