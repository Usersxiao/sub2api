package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const ClientGatewayClientID = "autodesign"
const ClientGatewayMaxTTL = 5 * time.Minute

var (
	ErrClientGatewayScope       = infraerrors.BadRequest("MODEL_GATEWAY_SCOPE_INVALID", "Gateway session scope does not match")
	ErrClientGatewayRevoked     = infraerrors.Forbidden("MODEL_ROUTE_REVOKED", "No current model authorization is available")
	ErrClientGatewayChanged     = infraerrors.Conflict("MODEL_CONFIG_CHANGED", "Model configuration changed; synchronize and try again")
	ErrClientGatewayUnavailable = infraerrors.New(503, "UPSTREAM_UNAVAILABLE", "Model authorization is temporarily unavailable")
)

type ClientGatewayScope struct {
	SchemaVersion  int    `json:"schemaVersion"`
	InstanceID     string `json:"instanceId"`
	UpstreamUserID string `json:"upstreamUserId"`
	SID            string `json:"sid"`
	SessionEpoch   int64  `json:"sessionEpoch"`
}

type ClientGatewaySessionRequest struct {
	ClientGatewayScope
	ClientID           string `json:"clientId"`
	ConsumerID         string `json:"consumerId,omitempty"`
	ConfigVersion      string `json:"configVersion"`
	EntitlementVersion string `json:"entitlementVersion"`
	PreviousGrantID    string `json:"previousGrantId,omitempty"`
}

type ClientGatewayRevokeRequest struct {
	ClientGatewayScope
	GrantID string `json:"grantId"`
}

type ClientGatewayRevokeResult struct {
	ClientGatewayScope
	GrantID   string    `json:"grantId"`
	RevokedAt time.Time `json:"revokedAt"`
}

type ClientGatewayGrant struct {
	ClientGatewayScope
	GrantID            string     `json:"grantId"`
	ClientID           string     `json:"clientId"`
	ConsumerID         string     `json:"consumerId"`
	ConfigVersion      string     `json:"configVersion"`
	EntitlementVersion string     `json:"entitlementVersion"`
	RouteIDs           []string   `json:"routeIds"`
	Operations         []string   `json:"operations"`
	IssuedAt           time.Time  `json:"issuedAt"`
	ExpiresAt          time.Time  `json:"expiresAt"`
	RevokedAt          *time.Time `json:"-"`
}

// This is the only response carrying a credential, and only over the trusted
// daemon/backend boundary. Repositories persist TokenHash, never AccessToken.
type ClientGatewayCredential struct {
	ClientGatewayGrant
	AccessToken string `json:"accessToken"`
	TokenType   string `json:"tokenType"`
}

type ClientGatewayRepository interface {
	Issue(ctx context.Context, grant *ClientGatewayGrant, tokenHash, previousGrantID, expectedConsumerID string) (*ClientGatewayGrant, error)
	GetByTokenHash(ctx context.Context, tokenHash string) (*ClientGatewayGrant, error)
	Revoke(ctx context.Context, session *AuthSession, instanceID, grantID string) (time.Time, error)
}

type ClientGatewayConfiguration interface {
	Snapshot(context.Context, int64) (*ClientConfigurationSnapshot, error)
}

type ClientGatewayService struct {
	configuration ClientGatewayConfiguration
	repository    ClientGatewayRepository
	now           func() time.Time
}

func NewClientGatewayService(configuration ClientGatewayConfiguration, repository ClientGatewayRepository) *ClientGatewayService {
	return &ClientGatewayService{configuration: configuration, repository: repository, now: time.Now}
}

func GatewaySessionScope(instanceID string, s *AuthSession) ClientGatewayScope {
	return ClientGatewayScope{1, instanceID, strconv.FormatInt(s.UserID, 10), s.SessionID, s.Epoch}
}

func (s *ClientGatewayService) Issue(ctx context.Context, session *AuthSession, req ClientGatewaySessionRequest) (*ClientGatewayCredential, error) {
	if s == nil || s.repository == nil || s.configuration == nil {
		return nil, ErrClientGatewayUnavailable
	}
	if session == nil || req.ClientID != ClientGatewayClientID || !clientIdentifier.MatchString(req.ConfigVersion) || !clientIdentifier.MatchString(req.EntitlementVersion) ||
		(req.ConsumerID != "" && !clientIdentifier.MatchString(req.ConsumerID)) || (req.PreviousGrantID != "" && !clientIdentifier.MatchString(req.PreviousGrantID)) {
		return nil, ErrClientGatewayScope
	}
	snapshot, err := s.configuration.Snapshot(ctx, session.UserID)
	if err != nil {
		return nil, ErrClientGatewayUnavailable
	}
	if snapshot == nil || req.ClientGatewayScope != GatewaySessionScope(snapshot.InstanceID, session) {
		return nil, ErrClientGatewayScope
	}
	if req.ConfigVersion != snapshot.Catalog.ConfigVersion || req.EntitlementVersion != snapshot.Catalog.EntitlementVersion {
		return nil, ErrClientGatewayChanged
	}
	routes := make([]string, 0, len(snapshot.Catalog.Routes))
	for _, route := range snapshot.Catalog.Routes {
		if route.Availability.Status == "available" {
			routes = append(routes, route.RouteID)
		}
	}
	if len(routes) == 0 {
		return nil, ErrClientGatewayRevoked
	}
	now := s.now().UTC()
	expires := now.Add(ClientGatewayMaxTTL)
	if session.ExpiresAt.Before(expires) {
		expires = session.ExpiresAt
	}
	if !expires.After(now) {
		return nil, ErrTokenExpired
	}
	secret, err := gatewayRandom("adg_")
	if err != nil {
		return nil, ErrClientGatewayUnavailable
	}
	id, err := gatewayRandom("grant-")
	if err != nil {
		return nil, ErrClientGatewayUnavailable
	}
	consumerID, err := gatewayRandom("consumer-")
	if err != nil {
		return nil, ErrClientGatewayUnavailable
	}
	grant := &ClientGatewayGrant{ClientGatewayScope: req.ClientGatewayScope, GrantID: id, ClientID: req.ClientID, ConsumerID: consumerID,
		ConfigVersion: req.ConfigVersion, EntitlementVersion: req.EntitlementVersion, RouteIDs: routes, Operations: []string{"invoke"}, IssuedAt: now, ExpiresAt: expires}
	stored, err := s.repository.Issue(ctx, grant, GatewayTokenHash(secret), req.PreviousGrantID, req.ConsumerID)
	if err != nil {
		return nil, err
	}
	// Publication changes during issuance must not release an obsolete credential.
	if _, err = s.Validate(ctx, secret, "", ""); err != nil {
		_, _ = s.repository.Revoke(ctx, session, stored.InstanceID, stored.GrantID)
		return nil, err
	}
	return &ClientGatewayCredential{ClientGatewayGrant: *stored, AccessToken: secret, TokenType: "Bearer"}, nil
}

// Validate is a closed server-side foundation for the future invocation adapter.
// It does not dispatch, bill, grant project/tool access, or accept an account JWT.
func (s *ClientGatewayService) Validate(ctx context.Context, token, routeID, operation string) (*ClientGatewayGrant, error) {
	if s == nil || s.repository == nil || s.configuration == nil {
		return nil, ErrClientGatewayUnavailable
	}
	if !strings.HasPrefix(token, "adg_") || len(token) != 47 {
		return nil, ErrClientGatewayRevoked
	}
	grant, err := s.repository.GetByTokenHash(ctx, GatewayTokenHash(token))
	if err != nil {
		return nil, err
	}
	userID, err := strconv.ParseInt(grant.UpstreamUserID, 10, 64)
	if err != nil || userID <= 0 {
		return nil, ErrClientGatewayRevoked
	}
	snapshot, err := s.configuration.Snapshot(ctx, userID)
	if err != nil || snapshot == nil {
		return nil, ErrClientGatewayUnavailable
	}
	if snapshot.InstanceID != grant.InstanceID || snapshot.Catalog.ConfigVersion != grant.ConfigVersion || snapshot.Catalog.EntitlementVersion != grant.EntitlementVersion {
		return nil, ErrClientGatewayChanged
	}
	available := make(map[string]bool)
	for _, route := range snapshot.Catalog.Routes {
		if route.Availability.Status == "available" {
			available[route.RouteID] = true
		}
	}
	for _, id := range grant.RouteIDs {
		if !available[id] {
			return nil, ErrClientGatewayRevoked
		}
	}
	if routeID != "" && (!clientContains(grant.RouteIDs, routeID) || !available[routeID]) {
		return nil, ErrClientGatewayRevoked
	}
	if operation != "" && !clientContains(grant.Operations, operation) {
		return nil, ErrClientGatewayRevoked
	}
	// A concurrent logout while snapshot I/O was pending wins before admission.
	return s.repository.GetByTokenHash(ctx, GatewayTokenHash(token))
}

func (s *ClientGatewayService) Revoke(ctx context.Context, session *AuthSession, req ClientGatewayRevokeRequest) (*ClientGatewayRevokeResult, error) {
	if s == nil || s.repository == nil {
		return nil, ErrClientGatewayUnavailable
	}
	if session == nil || !clientIdentifier.MatchString(req.GrantID) || !clientIdentifier.MatchString(req.InstanceID) {
		return nil, ErrClientGatewayScope
	}
	if req.ClientGatewayScope != GatewaySessionScope(req.InstanceID, session) {
		return nil, ErrClientGatewayScope
	}
	// The ledger checks the persisted instance and owner. Revocation must remain
	// available when directory/channel reads are temporarily failing.
	when, err := s.repository.Revoke(ctx, session, req.InstanceID, req.GrantID)
	if err != nil {
		return nil, err
	}
	return &ClientGatewayRevokeResult{req.ClientGatewayScope, req.GrantID, when}, nil
}

func GatewayTokenHash(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}
func gatewayRandom(prefix string) (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
