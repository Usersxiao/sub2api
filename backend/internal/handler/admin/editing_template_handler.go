package admin

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type EditingTemplateHandler struct {
	service *service.EditingTemplateService
}

func NewEditingTemplateHandler(svc *service.EditingTemplateService) *EditingTemplateHandler {
	return &EditingTemplateHandler{service: svc}
}

func (h *EditingTemplateHandler) List(c *gin.Context) {
	page, pageSize := response.ParsePagination(c)
	items, result, err := h.service.ListAdmin(c.Request.Context(), c.Query("status"), c.Query("search"), pagination.PaginationParams{Page: page, PageSize: pageSize})
	if response.ErrorFrom(c, err) {
		return
	}
	response.Paginated(c, items, result.Total, result.Page, result.PageSize)
}

func (h *EditingTemplateHandler) Get(c *gin.Context) {
	template, err := h.service.GetAdmin(c.Request.Context(), c.Param("id"))
	if response.ErrorFrom(c, err) {
		return
	}
	response.Success(c, template)
}

func (h *EditingTemplateHandler) Approve(c *gin.Context) {
	reviewerID, ok := editingTemplateReviewerID(c)
	if !ok {
		return
	}
	template, err := h.service.Approve(c.Request.Context(), reviewerID, c.Param("id"))
	if response.ErrorFrom(c, err) {
		return
	}
	response.Success(c, template)
}

type editingTemplateReviewRequest struct {
	Reason string `json:"reason"`
}

func (h *EditingTemplateHandler) Reject(c *gin.Context) {
	reviewerID, ok := editingTemplateReviewerID(c)
	if !ok {
		return
	}
	var input editingTemplateReviewRequest
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "驳回参数无效")
		return
	}
	template, err := h.service.Reject(c.Request.Context(), reviewerID, c.Param("id"), input.Reason)
	if response.ErrorFrom(c, err) {
		return
	}
	response.Success(c, template)
}

func (h *EditingTemplateHandler) Unlist(c *gin.Context) {
	reviewerID, ok := editingTemplateReviewerID(c)
	if !ok {
		return
	}
	var input editingTemplateReviewRequest
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&input); err != nil {
			response.BadRequest(c, "下架参数无效")
			return
		}
	}
	template, err := h.service.Unlist(c.Request.Context(), reviewerID, c.Param("id"), strings.TrimSpace(input.Reason))
	if response.ErrorFrom(c, err) {
		return
	}
	response.Success(c, template)
}

func (h *EditingTemplateHandler) DownloadArtifact(c *gin.Context) {
	artifact, reader, err := h.service.OpenAdminArtifact(c.Request.Context(), c.Param("id"), c.Param("kind"))
	serveAdminEditingTemplateArtifact(c, artifact, reader, err)
}

func serveAdminEditingTemplateArtifact(c *gin.Context, artifact *service.EditingTemplateArtifact, reader io.ReadCloser, err error) {
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	defer func() { _ = reader.Close() }()
	disposition := "inline"
	if artifact.Kind == service.EditingTemplateArtifactProject {
		disposition = "attachment"
	}
	if value := mime.FormatMediaType(disposition, map[string]string{"filename": artifact.OriginalFilename}); value != "" {
		c.Header("Content-Disposition", value)
	}
	c.Header("Content-Type", artifact.ContentType)
	c.Header("Content-Length", fmt.Sprintf("%d", artifact.ByteSize))
	c.Header("ETag", `"`+artifact.SHA256+`"`)
	c.Header("X-Content-Type-Options", "nosniff")
	c.Status(http.StatusOK)
	_, _ = io.Copy(c.Writer, reader)
}

func editingTemplateReviewerID(c *gin.Context) (int64, bool) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "管理员身份无效")
		return 0, false
	}
	return subject.UserID, true
}
