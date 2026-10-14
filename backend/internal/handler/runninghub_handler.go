package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// RunningHubHandler bridges the RunningHub service to the Gin router.
// User endpoints sit behind API-key auth; admin endpoints behind admin auth.
type RunningHubHandler struct {
	svc *service.RunningHubService
}

func NewRunningHubHandler(svc *service.RunningHubService) *RunningHubHandler {
	return &RunningHubHandler{svc: svc}
}

func rhError(c *gin.Context, err error) {
	var ue *service.RunningHubUpstreamError
	switch {
	case errors.Is(err, service.ErrRunningHubNotFound):
		response.NotFound(c, err.Error())
	case errors.Is(err, service.ErrRunningHubInsufficient):
		response.Error(c, http.StatusPaymentRequired, err.Error())
	case errors.Is(err, service.ErrRunningHubPoolBusy):
		response.Error(c, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, service.ErrRunningHubNoDB):
		response.Error(c, http.StatusServiceUnavailable, err.Error())
	case errors.As(err, &ue):
		response.Error(c, http.StatusBadGateway, err.Error())
	default:
		response.BadRequest(c, err.Error())
	}
}

func (h *RunningHubHandler) userID(c *gin.Context) (int64, bool) {
	sub, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok || sub.UserID <= 0 {
		response.Unauthorized(c, "authentication required")
		return 0, false
	}
	return sub.UserID, true
}

func pathID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "invalid id")
		return 0, false
	}
	return id, true
}

// ── User endpoints ────────────────────────────────────────────────────────────

func (h *RunningHubHandler) Credits(c *gin.Context) {
	uid, ok := h.userID(c)
	if !ok {
		return
	}
	v, err := h.svc.Credits(c.Request.Context(), uid)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) ListWorkflows(c *gin.Context) {
	v, err := h.svc.ListWorkflows(c.Request.Context(), true)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) Submit(c *gin.Context) {
	uid, ok := h.userID(c)
	if !ok {
		return
	}
	var req service.RunningHubSubmitRequest
	if c.Request.ContentLength != 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			response.BadRequest(c, err.Error())
			return
		}
	}
	// Rate multiplier comes from the key's group — never from the client.
	if key, exists := middleware.GetAPIKeyFromContext(c); exists && key != nil && key.User != nil && key.GroupID != nil {
		if rate, ok := key.User.GroupRates[*key.GroupID]; ok && rate > 0 {
			req.RateMultiplier = rate
		}
	}
	t, err := h.svc.Submit(c.Request.Context(), uid, c.Param("app_id"), req)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Accepted(c, t)
}

func (h *RunningHubHandler) ListTasks(c *gin.Context) {
	uid, ok := h.userID(c)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	v, err := h.svc.ListTasks(c.Request.Context(), uid, limit)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) Query(c *gin.Context) {
	uid, ok := h.userID(c)
	if !ok {
		return
	}
	t, err := h.svc.Query(c.Request.Context(), uid, c.Param("id"))
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, t)
}

func (h *RunningHubHandler) Cancel(c *gin.Context) {
	uid, ok := h.userID(c)
	if !ok {
		return
	}
	t, err := h.svc.Cancel(c.Request.Context(), uid, c.Param("id"))
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, t)
}

func (h *RunningHubHandler) Upload(c *gin.Context) {
	uid, ok := h.userID(c)
	if !ok {
		return
	}
	h.stage(c, uid, false)
}

func (h *RunningHubHandler) stage(c *gin.Context, uid int64, public bool) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		response.BadRequest(c, "multipart field 'file' is required")
		return
	}
	defer file.Close()
	m, err := h.svc.StageMedia(c.Request.Context(), uid, public, file, header.Filename, header.Header.Get("Content-Type"))
	if err != nil {
		rhError(c, err)
		return
	}
	response.Created(c, m)
}
