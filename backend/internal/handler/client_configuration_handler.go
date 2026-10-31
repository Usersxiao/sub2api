package handler

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type clientConfigurationService interface {
	Get(context.Context) (*service.ClientConfiguration, error)
	Save(context.Context, *service.ClientConfiguration) error
	Snapshot(context.Context, int64) (*service.ClientConfigurationSnapshot, error)
}

type ClientConfigurationHandler struct {
	service clientConfigurationService
	routes  service.ClientInvocationRouteResolver
	auth    *service.AuthService
	gateway *service.ClientGatewayService
	// chatExecution is set only after the invoke composition was assembled, so
	// bootstrap.features.chat reports what this instance can actually execute.
	chatExecution bool
}

// EnableChatExecution is called by the invoke provider after a successful
// composition. It is not an administrator setting and changes no versions.
func (h *ClientConfigurationHandler) EnableChatExecution() {
	if h != nil {
		h.chatExecution = true
	}
}

func NewClientConfigurationHandler(settings service.SettingRepository, users *service.UserService, groups *service.APIKeyService, groupReader service.GroupRepository, channels service.ChannelRepository, accounts service.AccountRepository, gateway *service.GatewayService, auth *service.AuthService, grants service.ClientGatewayRepository, composite *service.CompositeRouteResolver) *ClientConfigurationHandler {
	configuration := service.NewClientConfigurationService(settings, users, groups, groupReader, channels, accounts, gateway, composite)
	return &ClientConfigurationHandler{service: configuration, routes: configuration, auth: auth, gateway: service.NewClientGatewayService(configuration, grants)}
}

// ResolveInvocation uses the same published snapshot and permission evaluator
// as the directory and grant issuer, so invoke cannot resolve a different route.
func (h *ClientConfigurationHandler) ResolveInvocation(ctx context.Context, userID int64, routeID, configVersion, entitlementVersion string) (*service.ClientInvocationResolution, error) {
	if h == nil || h.routes == nil {
		return nil, service.ErrClientGatewayUnavailable
	}
	return h.routes.ResolveInvocation(ctx, userID, routeID, configVersion, entitlementVersion)
}

func (h *ClientConfigurationHandler) Validate(ctx context.Context, token, routeID, operation string) (*service.ClientGatewayGrant, error) {
	if h == nil || h.gateway == nil {
		return nil, service.ErrClientGatewayUnavailable
	}
	return h.gateway.Validate(ctx, token, routeID, operation)
}

func (h *ClientConfigurationHandler) GetConfiguration(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Authentication required")
		return
	}
	snapshot, err := h.service.Snapshot(c.Request.Context(), subject.UserID)
	if err != nil {
		if errors.Is(err, service.ErrUserNotFound) {
			response.Unauthorized(c, "Account sign-in has ended")
			return
		}
		response.Error(c, http.StatusServiceUnavailable, "Client configuration is temporarily unavailable")
		return
	}
	if h.chatExecution && snapshot != nil {
		published := *snapshot
		published.Bootstrap.Features.Chat = true
		snapshot = &published
	}
	response.Success(c, snapshot)
}

func (h *ClientConfigurationHandler) GetAdminConfiguration(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if role, ok := middleware.GetUserRoleFromContext(c); !ok || role != service.RoleAdmin {
		response.Forbidden(c, "Admin access required")
		return
	}
	cfg, err := h.service.Get(c.Request.Context())
	if err != nil {
		response.Error(c, http.StatusServiceUnavailable, "Client configuration is temporarily unavailable")
		return
	}
	response.Success(c, cfg)
}

func (h *ClientConfigurationHandler) PutAdminConfiguration(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if role, ok := middleware.GetUserRoleFromContext(c); !ok || role != service.RoleAdmin {
		response.Forbidden(c, "Admin access required")
		return
	}
	contentType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || contentType != "application/json" {
		response.BadRequest(c, "Expected application/json")
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1024*1024)
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		response.BadRequest(c, "Configuration exceeds 1 MiB")
		return
	}
	cfg, err := service.ParseClientConfiguration(raw)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if err := h.service.Save(c.Request.Context(), cfg); err != nil {
		response.BadRequest(c, "Configuration was not saved. Verify referenced groups and try again.")
		return
	}
	response.Success(c, cfg)
}
