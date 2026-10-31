package service

import (
	"context"
	"strconv"
)

// These interfaces keep credential validation and the directory's single
// permission evaluator authoritative. Request fields never establish identity.
type ClientInvocationGrantValidator interface {
	Validate(context.Context, string, string, string) (*ClientGatewayGrant, error)
}

type ClientInvocationRouteResolver interface {
	ResolveInvocation(context.Context, int64, string, string, string) (*ClientInvocationResolution, error)
}

// PreparedClientInvocation is internal admission data, not a serializable DTO or
// a dispatch receipt. Billing, idempotent dispatch, project/tool authorization and
// protocol execution are still separate requirements of the invocation adapter.
type PreparedClientInvocation struct {
	Search          *ValidatedClientSearchInvocation `json:"-"`
	Request         *ClientInvocationRequest         `json:"-"`
	Grant           *ClientGatewayGrant              `json:"-"`
	Resolution      *ClientInvocationResolution      `json:"-"`
	Text            *ValidatedClientTextInvocation   `json:"-"`
	BillingIdentity *APIKey                          `json:"-"`
}

type ClientInvocationPreparationService struct {
	grants  ClientInvocationGrantValidator
	routes  ClientInvocationRouteResolver
	billing ClientBillingIdentityRepository
}

func NewClientInvocationPreparationService(grants ClientInvocationGrantValidator, routes ClientInvocationRouteResolver, billing ClientBillingIdentityRepository) *ClientInvocationPreparationService {
	return &ClientInvocationPreparationService{grants: grants, routes: routes, billing: billing}
}

// Prepare performs no model request and never reserves or deducts money. The
// future adapter must recheck admission immediately before dispatch and use the
// existing quota/subscription, concurrency, routing and settlement handlers.
func (s *ClientInvocationPreparationService) Prepare(ctx context.Context, token, pathRouteID string, raw []byte) (*PreparedClientInvocation, error) {
	if s == nil || s.grants == nil || s.routes == nil || s.billing == nil {
		return nil, ErrClientGatewayUnavailable
	}
	req, err := ParseClientInvocation(raw)
	if err != nil {
		return nil, err
	}
	if req.RouteID != pathRouteID {
		return nil, ErrClientGatewayRevoked
	}
	grant, err := s.grants.Validate(ctx, token, req.RouteID, "invoke")
	if err != nil {
		return nil, err
	}
	if grant == nil || grant.ConfigVersion != req.ConfigVersion || grant.EntitlementVersion != req.EntitlementVersion {
		return nil, ErrClientGatewayChanged
	}
	userID, err := strconv.ParseInt(grant.UpstreamUserID, 10, 64)
	if err != nil || userID <= 0 {
		return nil, ErrClientGatewayRevoked
	}
	resolved, err := s.routes.ResolveInvocation(ctx, userID, req.RouteID, req.ConfigVersion, req.EntitlementVersion)
	if err != nil {
		return nil, err
	}
	if resolved == nil || resolved.GroupID <= 0 || resolved.Scope.InstanceID != grant.InstanceID ||
		resolved.Scope.ConfigVersion != grant.ConfigVersion || resolved.Scope.EntitlementVersion != grant.EntitlementVersion ||
		resolved.Route.RouteID != req.RouteID || resolved.ModelID != resolved.Route.Target.ModelID {
		return nil, ErrClientGatewayChanged
	}
	effectiveModel, err := clientInvocationEffectiveModel(resolved)
	if err != nil {
		return nil, err
	}
	var text *ValidatedClientTextInvocation
	var search *ValidatedClientSearchInvocation
	if req.Operation == "search" {
		if resolved.Platform != PlatformOpenAI || resolved.CompositeDecision != nil {
			return nil, ErrClientInvocationParameters
		}
		if _, streaming := ClientInvocationTextStream(ctx); streaming {
			return nil, ErrClientInvocationParameters
		}
		search, err = ValidateClientSearchInvocation(resolved.Route, req)
	} else {
		text, err = validateClientTextInvocationForModel(resolved.Route, req, effectiveModel, resolved.Platform)
		if err == nil {
			err = prepareClientInvocationTextStream(ctx, resolved.Route, text)
		}
	}
	if err != nil {
		return nil, err
	}
	identity, err := s.billing.Resolve(ctx, grant, resolved.GroupID)
	if err != nil {
		return nil, err
	}
	if identity == nil || identity.ID <= 0 || !identity.IsClientConsumer() || identity.ClientConsumerID == nil ||
		*identity.ClientConsumerID != grant.ConsumerID || identity.Key != "" || identity.UserID != userID ||
		identity.User == nil || identity.User.ID != userID || !identity.User.IsActive() || identity.User.DeletedAt != nil ||
		identity.GroupID == nil || *identity.GroupID != resolved.GroupID || identity.Group == nil ||
		identity.Group.ID != resolved.GroupID || identity.Group.Status != StatusActive || !identity.IsActive() ||
		resolved.Platform == "" || !clientInvocationRouteMatchesBillingIdentity(&PreparedClientInvocation{Resolution: resolved, BillingIdentity: identity}) {
		return nil, ErrClientGatewayRevoked
	}
	// Database work can wait behind logout or grant rotation. Liveness must win
	// again before admitting the internal accounting identity to an adapter.
	current, err := s.grants.Validate(ctx, token, req.RouteID, "invoke")
	if err != nil {
		return nil, err
	}
	if !sameClientInvocationGrant(grant, current) {
		return nil, ErrClientGatewayRevoked
	}
	return &PreparedClientInvocation{Request: req, Grant: current, Resolution: resolved, Text: text, Search: search, BillingIdentity: identity}, nil
}

func sameClientInvocationGrant(first, second *ClientGatewayGrant) bool {
	return first != nil && second != nil && first.ClientGatewayScope == second.ClientGatewayScope &&
		first.GrantID == second.GrantID && first.ClientID == second.ClientID && first.ConsumerID == second.ConsumerID &&
		first.ConfigVersion == second.ConfigVersion && first.EntitlementVersion == second.EntitlementVersion
}
