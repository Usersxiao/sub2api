//go:build unit

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type gatewayConfigurationStub struct {
	snapshot   *ClientConfigurationSnapshot
	err        error
	onSnapshot func()
}

func (s *gatewayConfigurationStub) Snapshot(context.Context, int64) (*ClientConfigurationSnapshot, error) {
	if s.onSnapshot != nil {
		s.onSnapshot()
	}
	return s.snapshot, s.err
}

type gatewayRepositoryStub struct {
	grant                  *ClientGatewayGrant
	hash                   string
	err                    error
	issues, reads, revokes int
}

func (r *gatewayRepositoryStub) Issue(_ context.Context, g *ClientGatewayGrant, hash, previous, consumer string) (*ClientGatewayGrant, error) {
	r.issues++
	if r.err != nil {
		return nil, r.err
	}
	copy := *g
	r.grant = &copy
	r.hash = hash
	return &copy, nil
}
func (r *gatewayRepositoryStub) GetByTokenHash(_ context.Context, hash string) (*ClientGatewayGrant, error) {
	r.reads++
	if r.err != nil {
		return nil, r.err
	}
	if hash != r.hash {
		return nil, ErrClientGatewayRevoked
	}
	copy := *r.grant
	return &copy, nil
}
func (r *gatewayRepositoryStub) Revoke(context.Context, *AuthSession, string, string) (time.Time, error) {
	r.revokes++
	return time.Now(), r.err
}

func gatewayServiceFixture(t *testing.T) (*ClientGatewayService, *AuthSession, ClientGatewaySessionRequest, *gatewayConfigurationStub, *gatewayRepositoryStub) {
	t.Helper()
	session := &AuthSession{UserID: 41, SessionID: "active-sid", Epoch: 3, ExpiresAt: time.Now().Add(time.Hour)}
	var route ClientModelRoute
	route.RouteID = "writing"
	route.Availability.Status = "available"
	snapshot := &ClientConfigurationSnapshot{SchemaVersion: 1, InstanceID: DefaultClientInstanceID, UpstreamUserID: "41", Catalog: ClientConfigurationCatalog{ClientConfigurationScope: ClientConfigurationScope{ConfigVersion: "cfg-1", EntitlementVersion: "ent-1"}, Routes: []ClientModelRoute{route}}}
	configuration := &gatewayConfigurationStub{snapshot: snapshot}
	repo := &gatewayRepositoryStub{}
	svc := NewClientGatewayService(configuration, repo)
	req := ClientGatewaySessionRequest{ClientGatewayScope: GatewaySessionScope(DefaultClientInstanceID, session), ClientID: ClientGatewayClientID, ConfigVersion: "cfg-1", EntitlementVersion: "ent-1"}
	return svc, session, req, configuration, repo
}

func TestClientGatewayServiceIssuesSeparateBoundCredential(t *testing.T) {
	svc, session, req, _, repo := gatewayServiceFixture(t)
	session.ExpiresAt = time.Now().Add(35 * time.Second)
	credential, err := svc.Issue(context.Background(), session, req)
	require.NoError(t, err)
	require.Equal(t, req.ClientGatewayScope, credential.ClientGatewayScope)
	require.Equal(t, ClientGatewayClientID, credential.ClientID)
	require.Equal(t, "cfg-1", credential.ConfigVersion)
	require.Equal(t, []string{"writing"}, credential.RouteIDs)
	require.Equal(t, []string{"invoke"}, credential.Operations)
	require.Equal(t, "Bearer", credential.TokenType)
	require.Len(t, credential.AccessToken, 47)
	require.Contains(t, credential.AccessToken, "adg_")
	require.NotEqual(t, credential.AccessToken, repo.hash)
	require.Equal(t, GatewayTokenHash(credential.AccessToken), repo.hash)
	require.False(t, credential.ExpiresAt.After(session.ExpiresAt))
	require.LessOrEqual(t, credential.ExpiresAt.Sub(credential.IssuedAt), ClientGatewayMaxTTL)
	require.Equal(t, 2, repo.reads)
	validated, err := svc.Validate(context.Background(), credential.AccessToken, "writing", "invoke")
	require.NoError(t, err)
	require.Equal(t, credential.ConsumerID, validated.ConsumerID)
	_, err = svc.Validate(context.Background(), credential.AccessToken, "other", "invoke")
	require.ErrorIs(t, err, ErrClientGatewayRevoked)
	_, err = svc.Validate(context.Background(), credential.AccessToken, "writing", "task:read")
	require.ErrorIs(t, err, ErrClientGatewayRevoked)
}

func TestClientGatewayServiceRejectsScopeAndDirectoryBeforeMint(t *testing.T) {
	cases := []struct {
		name   string
		change func(*AuthSession, *ClientGatewaySessionRequest, *gatewayConfigurationStub)
		want   error
	}{
		{"user", func(_ *AuthSession, r *ClientGatewaySessionRequest, _ *gatewayConfigurationStub) {
			r.UpstreamUserID = "42"
		}, ErrClientGatewayScope},
		{"sid", func(_ *AuthSession, r *ClientGatewaySessionRequest, _ *gatewayConfigurationStub) { r.SID = "forged" }, ErrClientGatewayScope},
		{"epoch", func(_ *AuthSession, r *ClientGatewaySessionRequest, _ *gatewayConfigurationStub) { r.SessionEpoch++ }, ErrClientGatewayScope},
		{"instance", func(_ *AuthSession, r *ClientGatewaySessionRequest, _ *gatewayConfigurationStub) {
			r.InstanceID = "other"
		}, ErrClientGatewayScope},
		{"client", func(_ *AuthSession, r *ClientGatewaySessionRequest, _ *gatewayConfigurationStub) {
			r.ClientID = "arbitrary-client"
		}, ErrClientGatewayScope},
		{"config", func(_ *AuthSession, r *ClientGatewaySessionRequest, _ *gatewayConfigurationStub) {
			r.ConfigVersion = "old"
		}, ErrClientGatewayChanged},
		{"permission", func(_ *AuthSession, r *ClientGatewaySessionRequest, _ *gatewayConfigurationStub) {
			r.EntitlementVersion = "old"
		}, ErrClientGatewayChanged},
		{"empty", func(_ *AuthSession, _ *ClientGatewaySessionRequest, c *gatewayConfigurationStub) {
			c.snapshot.Catalog.Routes = nil
		}, ErrClientGatewayRevoked},
		{"disabled", func(_ *AuthSession, _ *ClientGatewaySessionRequest, c *gatewayConfigurationStub) {
			c.snapshot.Catalog.Routes[0].Availability.Status = "disabled"
		}, ErrClientGatewayRevoked},
		{"expired", func(s *AuthSession, _ *ClientGatewaySessionRequest, _ *gatewayConfigurationStub) {
			s.ExpiresAt = time.Now().Add(-time.Second)
		}, ErrTokenExpired},
		{"unavailable", func(_ *AuthSession, _ *ClientGatewaySessionRequest, c *gatewayConfigurationStub) {
			c.err = errors.New("sensitive-backend-detail")
		}, ErrClientGatewayUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, session, req, c, repo := gatewayServiceFixture(t)
			tc.change(session, &req, c)
			credential, err := svc.Issue(context.Background(), session, req)
			require.Nil(t, credential)
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, repo.issues)
		})
	}
}

func TestClientGatewayServiceRechecksPublicationAndSessionAfterIO(t *testing.T) {
	svc, session, req, c, repo := gatewayServiceFixture(t)
	reads := 0
	c.onSnapshot = func() {
		reads++
		if reads == 2 {
			c.snapshot.Catalog.ConfigVersion = "cfg-2"
		}
	}
	credential, err := svc.Issue(context.Background(), session, req)
	require.Nil(t, credential)
	require.ErrorIs(t, err, ErrClientGatewayChanged)
	require.Equal(t, 1, repo.revokes)
	c.onSnapshot = nil
	c.snapshot.Catalog.ConfigVersion = "cfg-1"
	credential, err = svc.Issue(context.Background(), session, req)
	require.NoError(t, err)
	c.onSnapshot = func() { repo.err = ErrTokenRevoked }
	_, err = svc.Validate(context.Background(), credential.AccessToken, "writing", "invoke")
	require.ErrorIs(t, err, ErrTokenRevoked)
}

func TestClientGatewayServiceRejectsJWTAndAPIKeysWithoutRepositoryRead(t *testing.T) {
	svc, _, _, _, repo := gatewayServiceFixture(t)
	for _, token := range []string{"eyJ.mock.jwt", "sk-ordinary-key", "adg_short", ""} {
		_, err := svc.Validate(context.Background(), token, "writing", "invoke")
		require.ErrorIs(t, err, ErrClientGatewayRevoked)
	}
	require.Zero(t, repo.reads)
	var nilSvc *ClientGatewayService
	_, err := nilSvc.Validate(context.Background(), "adg_"+string(make([]byte, 43)), "writing", "invoke")
	require.ErrorIs(t, err, ErrClientGatewayUnavailable)
}

func TestClientGatewayServiceRevocationIndependentOfDirectoryAvailability(t *testing.T) {
	svc, session, req, configuration, repo := gatewayServiceFixture(t)
	configuration.err = errors.New("directory outage")
	result, err := svc.Revoke(context.Background(), session, ClientGatewayRevokeRequest{ClientGatewayScope: req.ClientGatewayScope, GrantID: "own-grant"})
	require.NoError(t, err)
	require.Equal(t, "own-grant", result.GrantID)
	require.Equal(t, 1, repo.revokes)
	wrong := req.ClientGatewayScope
	wrong.UpstreamUserID = "42"
	_, err = svc.Revoke(context.Background(), session, ClientGatewayRevokeRequest{ClientGatewayScope: wrong, GrantID: "own-grant"})
	require.ErrorIs(t, err, ErrClientGatewayScope)
	require.Equal(t, 1, repo.revokes)
}
