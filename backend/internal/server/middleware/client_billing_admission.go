package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// ClientBillingAdmission checks an already verified internal accounting identity.
// It does not authenticate bearer input, verify a persistent grant, or advance
// the Gin chain. The caller must validate the current grant and resolve its route
// before admission, then retain the gateway's model/composite/dispatch checks.
type ClientBillingAdmission func(c *gin.Context, identity *service.APIKey, grant *service.ClientGatewayGrant, groupID int64) bool

// NewClientBillingAdmission shares the ordinary gateway's post-authentication
// checks without allowing a normal API key to enter through this internal path.
// No HTTP route is registered by this constructor.
func NewClientBillingAdmission(apiKeyService *service.APIKeyService, subscriptionService *service.SubscriptionService, cfg *config.Config) ClientBillingAdmission {
	return func(c *gin.Context, identity *service.APIKey, grant *service.ClientGatewayGrant, groupID int64) bool {
		if c == nil {
			return false
		}
		if apiKeyService == nil || cfg == nil {
			AbortWithError(c, http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "Model admission is temporarily unavailable")
			return false
		}
		if !clientBillingIdentityBound(identity, grant, groupID) || !clientBillingInvocationPath(c) {
			AbortWithError(c, http.StatusForbidden, "MODEL_ROUTE_REVOKED", "No current model authorization is available")
			return false
		}
		if cfg.RunMode != config.RunModeSimple && identity.Group.IsSubscriptionType() && subscriptionService == nil {
			AbortWithError(c, http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "Model admission is temporarily unavailable")
			return false
		}
		// Normal credential lookup compiles these rules. Accounting lookup does
		// not pass through that cache, so prepare the same rules on a local copy.
		bound := *identity
		bound.CompiledIPWhitelist = ip.CompileIPRules(bound.IPWhitelist)
		bound.CompiledIPBlacklist = ip.CompileIPRules(bound.IPBlacklist)
		return admitAPIKeyWithSubscription(c, &bound, apiKeyService, subscriptionService, cfg)
	}
}

func clientBillingIdentityBound(identity *service.APIKey, grant *service.ClientGatewayGrant, groupID int64) bool {
	if grant == nil || grant.SchemaVersion != 1 || grant.ClientID != service.ClientGatewayClientID ||
		grant.InstanceID == "" || grant.SID == "" || grant.GrantID == "" || grant.ConsumerID == "" ||
		grant.RevokedAt != nil || !time.Now().Before(grant.ExpiresAt) ||
		len(grant.Operations) != 1 || grant.Operations[0] != "invoke" ||
		identity == nil || identity.ID <= 0 || !identity.IsClientConsumer() || identity.Key != "" ||
		identity.ClientConsumerID == nil || *identity.ClientConsumerID != grant.ConsumerID ||
		identity.User == nil || identity.User.DeletedAt != nil || grant.SessionEpoch <= 0 || identity.User.SessionEpoch != grant.SessionEpoch ||
		groupID <= 0 || identity.GroupID == nil || *identity.GroupID != groupID ||
		identity.Group == nil || identity.Group.ID != groupID || !service.IsGroupContextValid(identity.Group) {
		return false
	}
	userID, err := strconv.ParseInt(grant.UpstreamUserID, 10, 64)
	return err == nil && userID > 0 && identity.UserID == userID && identity.User.ID == userID
}

// This accounting entry is only for generation. Keeping the path canonical also
// prevents accidental use of the ordinary gateway's read-only skipBilling paths.
func clientBillingInvocationPath(c *gin.Context) bool {
	if c.Request == nil || c.Request.URL == nil || c.Request.Method != http.MethodPost {
		return false
	}
	switch c.Request.URL.Path {
	case "/v1/messages", "/v1/responses", "/v1/chat/completions", "/v1/alpha/search":
		return true
	default:
		return false
	}
}
