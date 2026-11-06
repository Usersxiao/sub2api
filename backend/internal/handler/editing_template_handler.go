package handler

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
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

func (h *EditingTemplateHandler) Create(c *gin.Context) {
	ownerID, ok := editingTemplateOwnerID(c)
	if !ok {
		return
	}
	var input service.EditingTemplateMetadataInput
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "模板信息格式无效")
		return
	}
	template, err := h.service.Create(c.Request.Context(), ownerID, input)
	if response.ErrorFrom(c, err) {
		return
	}
	response.Created(c, template)
}

func (h *EditingTemplateHandler) Update(c *gin.Context) {
	ownerID, ok := editingTemplateOwnerID(c)
	if !ok {
		return
	}
	var input service.EditingTemplateMetadataInput
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "模板信息格式无效")
		return
	}
	template, err := h.service.Update(c.Request.Context(), ownerID, c.Param("id"), input)
	if response.ErrorFrom(c, err) {
		return
	}
	response.Success(c, template)
}

func (h *EditingTemplateHandler) ListMine(c *gin.Context) {
	ownerID, ok := editingTemplateOwnerID(c)
	if !ok {
		return
	}
	page, pageSize := response.ParsePagination(c)
	items, result, err := h.service.ListMine(c.Request.Context(), ownerID, c.Query("status"), pagination.PaginationParams{Page: page, PageSize: pageSize})
	if response.ErrorFrom(c, err) {
		return
	}
	response.Paginated(c, items, result.Total, result.Page, result.PageSize)
}

func (h *EditingTemplateHandler) GetMine(c *gin.Context) {
	ownerID, ok := editingTemplateOwnerID(c)
	if !ok {
		return
	}
	template, err := h.service.GetMine(c.Request.Context(), ownerID, c.Param("id"))
	if response.ErrorFrom(c, err) {
		return
	}
	response.Success(c, template)
}

func (h *EditingTemplateHandler) UploadArtifact(c *gin.Context) {
	ownerID, ok := editingTemplateOwnerID(c)
	if !ok {
		return
	}
	kind := strings.TrimSpace(c.Param("kind"))
	maxBytes := h.service.MaxArtifactBytes(kind)
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes+(1<<20))
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		response.BadRequest(c, "请选择要上传的模板文件")
		return
	}
	defer func() { _ = file.Close() }()
	contentType := header.Header.Get("Content-Type")
	template, err := h.service.UploadArtifact(c.Request.Context(), ownerID, c.Param("id"), kind, header.Filename, contentType, file)
	if response.ErrorFrom(c, err) {
		return
	}
	response.Success(c, template)
}

func (h *EditingTemplateHandler) Submit(c *gin.Context) {
	ownerID, ok := editingTemplateOwnerID(c)
	if !ok {
		return
	}
	template, err := h.service.Submit(c.Request.Context(), ownerID, c.Param("id"))
	if response.ErrorFrom(c, err) {
		return
	}
	response.Success(c, template)
}

func (h *EditingTemplateHandler) DownloadMineArtifact(c *gin.Context) {
	ownerID, ok := editingTemplateOwnerID(c)
	if !ok {
		return
	}
	artifact, reader, err := h.service.OpenMineArtifact(c.Request.Context(), ownerID, c.Param("id"), c.Param("kind"))
	serveEditingTemplateArtifact(c, artifact, reader, err)
}

func (h *EditingTemplateHandler) ListPublic(c *gin.Context) {
	page, pageSize := response.ParsePagination(c)
	items, result, err := h.service.ListPublic(c.Request.Context(), c.Query("search"), pagination.PaginationParams{Page: page, PageSize: pageSize})
	if response.ErrorFrom(c, err) {
		return
	}
	for index := range items {
		redactEditingTemplatePublicFields(&items[index])
	}
	response.Paginated(c, items, result.Total, result.Page, result.PageSize)
}

func (h *EditingTemplateHandler) GetPublic(c *gin.Context) {
	template, err := h.service.GetPublic(c.Request.Context(), c.Param("id"))
	if response.ErrorFrom(c, err) {
		return
	}
	redactEditingTemplatePublicFields(template)
	response.Success(c, template)
}

func (h *EditingTemplateHandler) DownloadPublicArtifact(c *gin.Context) {
	artifact, reader, err := h.service.OpenPublicArtifact(c.Request.Context(), c.Param("id"), c.Param("kind"))
	serveEditingTemplateArtifact(c, artifact, reader, err)
}

func editingTemplateOwnerID(c *gin.Context) (int64, bool) {
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "请先登录")
		return 0, false
	}
	return subject.UserID, true
}

func serveEditingTemplateArtifact(c *gin.Context, artifact *service.EditingTemplateArtifact, reader io.ReadCloser, err error) {
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
	c.Header("ETag", `"`+artifact.SHA256+`"`)
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Accept-Ranges", "bytes")
	start, end, partial, valid := editingTemplateByteRange(c.GetHeader("Range"), artifact.ByteSize)
	if !valid {
		c.Header("Content-Range", fmt.Sprintf("bytes */%d", artifact.ByteSize))
		c.Status(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if partial {
		if start > 0 {
			if _, err := io.CopyN(io.Discard, reader, start); err != nil {
				return
			}
		}
		length := end - start + 1
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, artifact.ByteSize))
		c.Header("Content-Length", fmt.Sprintf("%d", length))
		c.Status(http.StatusPartialContent)
		_, _ = io.CopyN(c.Writer, reader, length)
		return
	}
	c.Header("Content-Length", fmt.Sprintf("%d", artifact.ByteSize))
	c.Status(http.StatusOK)
	_, _ = io.Copy(c.Writer, reader)
}

func editingTemplateByteRange(value string, size int64) (start, end int64, partial, valid bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, size - 1, false, size >= 0
	}
	if size <= 0 || !strings.HasPrefix(value, "bytes=") || strings.Contains(value, ",") {
		return 0, 0, false, false
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false, false
	}
	if parts[0] == "" {
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false, false
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true, true
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false, false
	}
	end = size - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < start {
			return 0, 0, false, false
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end, true, true
}

func redactEditingTemplatePublicFields(template *service.EditingTemplate) {
	template.OwnerID = 0
	template.RejectionReason = ""
	template.ModerationAction = ""
	template.Reviews = nil
	template.ReviewedAt = nil
	template.RejectedAt = nil
	template.UnlistedAt = nil
}
