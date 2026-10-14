package handler

import (
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// ── Accounts ──────────────────────────────────────────────────────────────────

func (h *RunningHubHandler) AdminListAccounts(c *gin.Context) {
	v, err := h.svc.ListAccounts(c.Request.Context())
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

type rhAddAccountReq struct {
	Name           string `json:"name"`
	Tier           string `json:"tier"`
	APIKey         string `json:"api_key"`
	MaxConcurrency int    `json:"max_concurrency"`
}

func (h *RunningHubHandler) AdminAddAccount(c *gin.Context) {
	var req rhAddAccountReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	id, err := h.svc.AddAccount(c.Request.Context(), req.Name, req.Tier, req.APIKey, req.MaxConcurrency)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Created(c, gin.H{"id": id})
}

func (h *RunningHubHandler) AdminUpdateAccount(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	var req service.RunningHubAccountUpdate
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if err := h.svc.UpdateAccount(c.Request.Context(), id, req); err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, gin.H{"id": id})
}

func (h *RunningHubHandler) AdminDeleteAccount(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	if err := h.svc.DeleteAccount(c.Request.Context(), id); err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, gin.H{"id": id})
}

// ── Settings ──────────────────────────────────────────────────────────────────

func (h *RunningHubHandler) AdminGetSettings(c *gin.Context) {
	v, err := h.svc.GetSettings(c.Request.Context())
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) AdminUpdateSettings(c *gin.Context) {
	var req service.RunningHubSettings
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	v, err := h.svc.UpdateSettings(c.Request.Context(), req)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

// ── Workflows ─────────────────────────────────────────────────────────────────

func (h *RunningHubHandler) AdminListWorkflows(c *gin.Context) {
	v, err := h.svc.ListWorkflows(c.Request.Context(), false)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) AdminGetWorkflow(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	v, err := h.svc.GetWorkflow(c.Request.Context(), id)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) AdminCreateWorkflow(c *gin.Context) {
	var req service.RunningHubWorkflowCreate
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	v, err := h.svc.CreateWorkflow(c.Request.Context(), req)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Created(c, v)
}

func (h *RunningHubHandler) AdminUpdateWorkflow(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	var req service.RunningHubWorkflowUpdate
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	v, err := h.svc.UpdateWorkflow(c.Request.Context(), id, req)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) AdminRefreshWorkflow(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	v, err := h.svc.RefreshWorkflowNodes(c.Request.Context(), id)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) AdminDeleteWorkflow(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	if err := h.svc.DeleteWorkflow(c.Request.Context(), id); err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, gin.H{"id": id})
}

// AdminUploadMedia stages a public file used as a workflow default (e.g. a
// replacement image or audio); its ref goes into a node's fieldValue.
func (h *RunningHubHandler) AdminUploadMedia(c *gin.Context) {
	uid, ok := h.userID(c)
	if !ok {
		return
	}
	h.stage(c, uid, true)
}

// ── Credits ───────────────────────────────────────────────────────────────────

func (h *RunningHubHandler) AdminGetUserCredits(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	v, err := h.svc.Credits(c.Request.Context(), id)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, v)
}

func (h *RunningHubHandler) AdminGrantRH(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	var body struct {
		Amount float64 `json:"amount"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	bal, err := h.svc.GrantRH(c.Request.Context(), id, body.Amount)
	if err != nil {
		rhError(c, err)
		return
	}
	response.Success(c, gin.H{"user_id": id, "rh_credits": bal})
}

// AdminGetMediaFile serves a staged public media file for browser preview.
// Uses http.ServeContent so Range requests work for video/audio seeking.
// Path: GET /api/v1/admin/runninghub/media/:id/file
func (h *RunningHubHandler) AdminGetMediaFile(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		response.BadRequest(c, "media id required")
		return
	}
	f, err := h.svc.GetMediaFile(c.Request.Context(), id)
	if err != nil {
		rhError(c, err)
		return
	}
	file, err := os.Open(f.StoragePath)
	if err != nil {
		rhError(c, err)
		return
	}
	defer file.Close()
	ct := f.ContentType
	if ct == "" {
		if detected := mime.TypeByExtension(filepath.Ext(f.FileName)); detected != "" {
			ct = detected
		} else {
			ct = "application/octet-stream"
		}
	}
	c.Header("Content-Type", ct)
	c.Header("Content-Disposition", `inline; filename="`+f.FileName+`"`)
	http.ServeContent(c.Writer, c.Request, f.FileName, time.Time{}, file)
}

// AdminProxyRHFile proxies a RunningHub file download so the admin UI can
// render image/video/audio previews without exposing API keys to the browser.
// Downloads to a temp file first so http.ServeContent can handle Range requests.
// Path: GET /api/v1/admin/runninghub/rh-file?fileName=<rh-filename>
func (h *RunningHubHandler) AdminProxyRHFile(c *gin.Context) {
	fileName := c.Query("fileName")
	if fileName == "" {
		response.BadRequest(c, "fileName required")
		return
	}
	resp, err := h.svc.ProxyRHFile(c.Request.Context(), fileName)
	if err != nil {
		rhError(c, err)
		return
	}
	defer resp.Body.Close()

	// Buffer to a temp file so http.ServeContent can do Range requests.
	tmp, err := os.CreateTemp("", "rh-preview-*"+filepath.Ext(fileName))
	if err != nil {
		rhError(c, err)
		return
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		rhError(c, err)
		return
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		rhError(c, err)
		return
	}

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	c.Header("Content-Type", ct)
	c.Header("Content-Disposition", "inline")
	c.Header("Cache-Control", "private, max-age=3600")
	http.ServeContent(c.Writer, c.Request, fileName, time.Time{}, tmp)
}
