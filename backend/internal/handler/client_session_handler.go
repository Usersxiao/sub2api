package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type clientSessionScope struct {
	SchemaVersion  int    `json:"schemaVersion"`
	InstanceID     string `json:"instanceId"`
	UpstreamUserID string `json:"upstreamUserId"`
	SID            string `json:"sid"`
	SessionEpoch   int64  `json:"sessionEpoch"`
}

type clientSessionProof struct {
	clientSessionScope
	State            string    `json:"state"`
	CheckedAt        time.Time `json:"checkedAt"`
	ValidUntil       time.Time `json:"validUntil"`
	SessionExpiresAt time.Time `json:"sessionExpiresAt"`
}

// Session identity is always obtained from a validated token and the live ledger.
// Query parameters and JWT payload parsing by the client never establish scope.
func (h *ClientConfigurationHandler) verifiedSession(c *gin.Context) (*service.AuthSession, bool) {
	c.Header("Cache-Control", "no-store")
	parts := strings.Fields(c.GetHeader("Authorization"))
	if h.auth == nil || len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		response.Unauthorized(c, "Authentication required")
		return nil, false
	}
	claims, err := h.auth.ValidateToken(parts[1])
	if err != nil {
		response.ErrorFrom(c, err)
		return nil, false
	}
	session, err := h.auth.GetSession(c.Request.Context(), claims)
	if err != nil {
		response.ErrorFrom(c, err)
		return nil, false
	}
	return session, true
}

func (h *ClientConfigurationHandler) sessionScope(c *gin.Context, s *service.AuthSession) (clientSessionScope, bool) {
	cfg, err := h.service.Get(c.Request.Context())
	if err != nil || cfg == nil {
		response.Error(c, http.StatusServiceUnavailable, "Session verification temporarily unavailable")
		return clientSessionScope{}, false
	}
	return clientSessionScope{1, cfg.InstanceID, strconv.FormatInt(s.UserID, 10), s.SessionID, s.Epoch}, true
}

func (h *ClientConfigurationHandler) GetSession(c *gin.Context) {
	checkedAt := time.Now().UTC()
	s, ok := h.verifiedSession(c)
	if !ok {
		return
	}
	scope, ok := h.sessionScope(c, s)
	if !ok {
		return
	}
	until := checkedAt.Add(60 * time.Second)
	if s.ExpiresAt.Before(until) {
		until = s.ExpiresAt
	}
	if !until.After(time.Now()) {
		response.ErrorFrom(c, service.ErrTokenExpired)
		return
	}
	response.Success(c, clientSessionProof{scope, "active", checkedAt, until, s.ExpiresAt})
}

func (h *ClientConfigurationHandler) RevokeSessions(c *gin.Context) {
	s, ok := h.verifiedSession(c)
	if !ok {
		return
	}
	scope, ok := h.sessionScope(c, s)
	if !ok {
		return
	}
	var req struct {
		clientSessionScope
		Target string `json:"target"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16*1024)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		response.BadRequest(c, "Invalid session revocation request")
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF || req.clientSessionScope != scope || (req.Target != "current" && req.Target != "all") {
		response.BadRequest(c, "Session scope does not match the authenticated session")
		return
	}
	epoch := s.Epoch
	var err error
	if req.Target == "all" {
		epoch, err = h.auth.RevokeAllSessionsWithEpoch(c.Request.Context(), s.UserID)
	} else {
		err = h.auth.RevokeUserSession(c.Request.Context(), s.UserID, s.SessionID)
	}
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, struct {
		clientSessionScope
		Target         string    `json:"target"`
		RevokedAt      time.Time `json:"revokedAt"`
		ResultingEpoch int64     `json:"resultingEpoch"`
	}{scope, req.Target, time.Now().UTC(), epoch})
}

func (h *ClientConfigurationHandler) ListSessions(c *gin.Context) {
	s, ok := h.verifiedSession(c)
	if !ok {
		return
	}
	sessions, err := h.auth.ListSessions(c.Request.Context(), s.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	type device struct {
		SID       string    `json:"sid"`
		Current   bool      `json:"current"`
		CreatedAt time.Time `json:"createdAt"`
		ExpiresAt time.Time `json:"expiresAt"`
	}
	items := make([]device, 0, len(sessions))
	for _, item := range sessions {
		items = append(items, device{item.SessionID, item.SessionID == s.SessionID, item.CreatedAt, item.ExpiresAt})
	}
	response.Success(c, gin.H{"sessions": items})
}

func (h *ClientConfigurationHandler) RevokeDeviceSession(c *gin.Context) {
	s, ok := h.verifiedSession(c)
	if !ok {
		return
	}
	if err := h.auth.RevokeUserSession(c.Request.Context(), s.UserID, c.Param("sid")); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"revoked": true})
}
