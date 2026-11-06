package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/google/uuid"
)

const (
	EditingTemplateStatusDraft         = "draft"
	EditingTemplateStatusPendingReview = "pending_review"
	EditingTemplateStatusApproved      = "approved"
	EditingTemplateStatusRejected      = "rejected"
	EditingTemplateStatusUnlisted      = "unlisted"

	EditingTemplateArtifactProject = "project"
	EditingTemplateArtifactCover   = "cover"
	EditingTemplateArtifactPreview = "preview"

	EditingTemplateReviewSubmit  = "submit"
	EditingTemplateReviewApprove = "approve"
	EditingTemplateReviewReject  = "reject"
	EditingTemplateReviewUnlist  = "unlist"
)

var (
	ErrEditingTemplateNotFound = infraerrors.NotFound("EDITING_TEMPLATE_NOT_FOUND", "剪辑模板不存在")
	ErrEditingTemplateState    = infraerrors.Conflict("EDITING_TEMPLATE_STATE_CHANGED", "剪辑模板状态已变化，请刷新后重试")
)

type EditingTemplate struct {
	ID               string                              `json:"id"`
	OwnerID          int64                               `json:"owner_id,omitempty"`
	Title            string                              `json:"title"`
	Description      string                              `json:"description"`
	EditingBrief     string                              `json:"editing_brief"`
	AspectRatio      string                              `json:"aspect_ratio"`
	DurationMS       int64                               `json:"duration_ms"`
	Status           string                              `json:"status"`
	RejectionReason  string                              `json:"rejection_reason,omitempty"`
	ModerationAction string                              `json:"moderation_action,omitempty"`
	Artifacts        map[string]*EditingTemplateArtifact `json:"artifacts"`
	Reviews          []EditingTemplateReview             `json:"reviews,omitempty"`
	SubmittedAt      *time.Time                          `json:"submitted_at,omitempty"`
	ReviewedAt       *time.Time                          `json:"reviewed_at,omitempty"`
	PublishedAt      *time.Time                          `json:"published_at,omitempty"`
	RejectedAt       *time.Time                          `json:"rejected_at,omitempty"`
	UnlistedAt       *time.Time                          `json:"unlisted_at,omitempty"`
	CreatedAt        time.Time                           `json:"created_at"`
	UpdatedAt        time.Time                           `json:"updated_at"`
}

type EditingTemplateArtifact struct {
	ID               int64     `json:"id"`
	TemplateID       string    `json:"template_id"`
	Kind             string    `json:"kind"`
	StorageKey       string    `json:"-"`
	OriginalFilename string    `json:"original_filename"`
	ContentType      string    `json:"content_type"`
	ByteSize         int64     `json:"byte_size"`
	SHA256           string    `json:"sha256"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type EditingTemplateReview struct {
	ID         int64     `json:"id"`
	TemplateID string    `json:"template_id"`
	ActorID    *int64    `json:"actor_id,omitempty"`
	Action     string    `json:"action"`
	FromStatus string    `json:"from_status"`
	ToStatus   string    `json:"to_status"`
	Reason     string    `json:"reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type EditingTemplateMetadataInput struct {
	Title        string `json:"title"`
	Description  string `json:"description"`
	EditingBrief string `json:"editing_brief"`
	AspectRatio  string `json:"aspect_ratio"`
	DurationMS   int64  `json:"duration_ms"`
}

type EditingTemplateListFilter struct {
	OwnerID *int64
	Status  string
	Search  string
	Public  bool
}

type EditingTemplateRepository interface {
	Create(ctx context.Context, template *EditingTemplate) error
	GetByID(ctx context.Context, id string) (*EditingTemplate, error)
	GetByOwner(ctx context.Context, id string, ownerID int64) (*EditingTemplate, error)
	List(ctx context.Context, filter EditingTemplateListFilter, page pagination.PaginationParams) ([]EditingTemplate, *pagination.PaginationResult, error)
	UpdateMetadata(ctx context.Context, template *EditingTemplate, expectedStatuses []string) error
	UpsertArtifact(ctx context.Context, artifact *EditingTemplateArtifact, ownerID int64, expectedStatuses []string) (*EditingTemplateArtifact, error)
	Transition(ctx context.Context, id string, ownerID *int64, expectedStatuses []string, toStatus, action, reason string, actorID *int64) error
}

type EditingTemplateStorage interface {
	Put(ctx context.Context, templateID, kind, filename, contentType string, reader io.Reader, maxBytes int64) (*EditingTemplateArtifact, error)
	Open(ctx context.Context, storageKey string) (io.ReadCloser, error)
	Delete(ctx context.Context, storageKey string) error
}

type EditingTemplateService struct {
	repo       EditingTemplateRepository
	storage    EditingTemplateStorage
	moderation *ContentModerationService
	cfg        *config.Config
}

func NewEditingTemplateService(repo EditingTemplateRepository, storage EditingTemplateStorage, moderation *ContentModerationService, cfg *config.Config) *EditingTemplateService {
	return &EditingTemplateService{repo: repo, storage: storage, moderation: moderation, cfg: cfg}
}

func (s *EditingTemplateService) Create(ctx context.Context, ownerID int64, input EditingTemplateMetadataInput) (*EditingTemplate, error) {
	if ownerID <= 0 {
		return nil, infraerrors.Unauthorized("EDITING_TEMPLATE_LOGIN_REQUIRED", "请先登录")
	}
	input = normalizeEditingTemplateMetadata(input)
	if err := validateEditingTemplateMetadata(input); err != nil {
		return nil, err
	}
	action, err := s.moderateMetadata(ctx, ownerID, input)
	if err != nil {
		return nil, err
	}
	template := &EditingTemplate{
		ID: uuid.NewString(), OwnerID: ownerID, Title: input.Title, Description: input.Description,
		EditingBrief: input.EditingBrief, AspectRatio: input.AspectRatio, DurationMS: input.DurationMS,
		Status: EditingTemplateStatusDraft, ModerationAction: action, Artifacts: map[string]*EditingTemplateArtifact{},
	}
	if err := s.repo.Create(ctx, template); err != nil {
		return nil, fmt.Errorf("create editing template: %w", err)
	}
	return s.repo.GetByOwner(ctx, template.ID, ownerID)
}

func (s *EditingTemplateService) Update(ctx context.Context, ownerID int64, id string, input EditingTemplateMetadataInput) (*EditingTemplate, error) {
	template, err := s.ownerTemplate(ctx, ownerID, id)
	if err != nil {
		return nil, err
	}
	if !oneOf(template.Status, EditingTemplateStatusDraft, EditingTemplateStatusRejected) {
		return nil, ErrEditingTemplateState
	}
	input = normalizeEditingTemplateMetadata(input)
	if err := validateEditingTemplateMetadata(input); err != nil {
		return nil, err
	}
	action, err := s.moderateMetadata(ctx, ownerID, input)
	if err != nil {
		return nil, err
	}
	template.Title = input.Title
	template.Description = input.Description
	template.EditingBrief = input.EditingBrief
	template.AspectRatio = input.AspectRatio
	template.DurationMS = input.DurationMS
	template.ModerationAction = action
	template.Status = EditingTemplateStatusDraft
	template.RejectionReason = ""
	if err := s.repo.UpdateMetadata(ctx, template, []string{EditingTemplateStatusDraft, EditingTemplateStatusRejected}); err != nil {
		return nil, err
	}
	return s.repo.GetByOwner(ctx, id, ownerID)
}

func (s *EditingTemplateService) UploadArtifact(ctx context.Context, ownerID int64, id, kind, filename, contentType string, reader io.Reader) (*EditingTemplate, error) {
	template, err := s.ownerTemplate(ctx, ownerID, id)
	if err != nil {
		return nil, err
	}
	if !oneOf(template.Status, EditingTemplateStatusDraft, EditingTemplateStatusRejected) {
		return nil, ErrEditingTemplateState
	}
	if err := validateEditingTemplateArtifact(kind, filename, contentType); err != nil {
		return nil, err
	}
	artifact, err := s.storage.Put(ctx, id, kind, filename, contentType, reader, s.MaxArtifactBytes(kind))
	if err != nil {
		return nil, err
	}
	previous, err := s.repo.UpsertArtifact(ctx, artifact, ownerID, []string{EditingTemplateStatusDraft, EditingTemplateStatusRejected})
	if err != nil {
		_ = s.storage.Delete(context.Background(), artifact.StorageKey)
		return nil, err
	}
	if previous != nil && previous.StorageKey != artifact.StorageKey {
		_ = s.storage.Delete(context.Background(), previous.StorageKey)
	}
	return s.repo.GetByOwner(ctx, id, ownerID)
}

func (s *EditingTemplateService) Submit(ctx context.Context, ownerID int64, id string) (*EditingTemplate, error) {
	template, err := s.ownerTemplate(ctx, ownerID, id)
	if err != nil {
		return nil, err
	}
	if !oneOf(template.Status, EditingTemplateStatusDraft, EditingTemplateStatusRejected) {
		return nil, ErrEditingTemplateState
	}
	for _, kind := range []string{EditingTemplateArtifactProject, EditingTemplateArtifactCover, EditingTemplateArtifactPreview} {
		if template.Artifacts[kind] == nil {
			return nil, infraerrors.BadRequest("EDITING_TEMPLATE_ARTIFACT_REQUIRED", "请先上传工程包、封面和预览成片")
		}
	}
	owner := ownerID
	if err := s.repo.Transition(ctx, id, &owner, []string{EditingTemplateStatusDraft, EditingTemplateStatusRejected}, EditingTemplateStatusPendingReview, EditingTemplateReviewSubmit, "", &owner); err != nil {
		return nil, err
	}
	return s.repo.GetByOwner(ctx, id, ownerID)
}

func (s *EditingTemplateService) Approve(ctx context.Context, reviewerID int64, id string) (*EditingTemplate, error) {
	if uuid.Validate(id) != nil {
		return nil, ErrEditingTemplateNotFound
	}
	template, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	for _, kind := range []string{EditingTemplateArtifactProject, EditingTemplateArtifactCover, EditingTemplateArtifactPreview} {
		if template.Artifacts[kind] == nil {
			return nil, infraerrors.BadRequest("EDITING_TEMPLATE_ARTIFACT_REQUIRED", "模板缺少工程包、封面或预览成片")
		}
	}
	if err := s.repo.Transition(ctx, id, nil, []string{EditingTemplateStatusPendingReview}, EditingTemplateStatusApproved, EditingTemplateReviewApprove, "", &reviewerID); err != nil {
		return nil, err
	}
	return s.repo.GetByID(ctx, id)
}

func (s *EditingTemplateService) Reject(ctx context.Context, reviewerID int64, id, reason string) (*EditingTemplate, error) {
	if uuid.Validate(id) != nil {
		return nil, ErrEditingTemplateNotFound
	}
	reason = strings.TrimSpace(reason)
	if reason == "" || utf8.RuneCountInString(reason) > 1000 {
		return nil, infraerrors.BadRequest("EDITING_TEMPLATE_REJECTION_REASON_INVALID", "驳回原因必填且不能超过 1000 字")
	}
	if _, err := s.repo.GetByID(ctx, id); err != nil {
		return nil, err
	}
	if err := s.repo.Transition(ctx, id, nil, []string{EditingTemplateStatusPendingReview}, EditingTemplateStatusRejected, EditingTemplateReviewReject, reason, &reviewerID); err != nil {
		return nil, err
	}
	return s.repo.GetByID(ctx, id)
}

func (s *EditingTemplateService) Unlist(ctx context.Context, reviewerID int64, id, reason string) (*EditingTemplate, error) {
	if uuid.Validate(id) != nil {
		return nil, ErrEditingTemplateNotFound
	}
	reason = strings.TrimSpace(reason)
	if utf8.RuneCountInString(reason) > 1000 {
		return nil, infraerrors.BadRequest("EDITING_TEMPLATE_UNLIST_REASON_INVALID", "下架原因不能超过 1000 字")
	}
	if _, err := s.repo.GetByID(ctx, id); err != nil {
		return nil, err
	}
	if err := s.repo.Transition(ctx, id, nil, []string{EditingTemplateStatusApproved}, EditingTemplateStatusUnlisted, EditingTemplateReviewUnlist, reason, &reviewerID); err != nil {
		return nil, err
	}
	return s.repo.GetByID(ctx, id)
}

func (s *EditingTemplateService) GetMine(ctx context.Context, ownerID int64, id string) (*EditingTemplate, error) {
	return s.ownerTemplate(ctx, ownerID, id)
}

func (s *EditingTemplateService) GetAdmin(ctx context.Context, id string) (*EditingTemplate, error) {
	if uuid.Validate(id) != nil {
		return nil, ErrEditingTemplateNotFound
	}
	return s.repo.GetByID(ctx, id)
}

func (s *EditingTemplateService) GetPublic(ctx context.Context, id string) (*EditingTemplate, error) {
	if uuid.Validate(id) != nil {
		return nil, ErrEditingTemplateNotFound
	}
	template, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if template.Status != EditingTemplateStatusApproved {
		return nil, ErrEditingTemplateNotFound
	}
	return template, nil
}

func (s *EditingTemplateService) ListMine(ctx context.Context, ownerID int64, status string, page pagination.PaginationParams) ([]EditingTemplate, *pagination.PaginationResult, error) {
	return s.repo.List(ctx, EditingTemplateListFilter{OwnerID: &ownerID, Status: status}, page)
}

func (s *EditingTemplateService) ListAdmin(ctx context.Context, status, search string, page pagination.PaginationParams) ([]EditingTemplate, *pagination.PaginationResult, error) {
	return s.repo.List(ctx, EditingTemplateListFilter{Status: status, Search: search}, page)
}

func (s *EditingTemplateService) ListPublic(ctx context.Context, search string, page pagination.PaginationParams) ([]EditingTemplate, *pagination.PaginationResult, error) {
	return s.repo.List(ctx, EditingTemplateListFilter{Status: EditingTemplateStatusApproved, Search: search, Public: true}, page)
}

func (s *EditingTemplateService) OpenMineArtifact(ctx context.Context, ownerID int64, id, kind string) (*EditingTemplateArtifact, io.ReadCloser, error) {
	template, err := s.ownerTemplate(ctx, ownerID, id)
	return s.openArtifact(ctx, template, kind, err)
}

func (s *EditingTemplateService) OpenAdminArtifact(ctx context.Context, id, kind string) (*EditingTemplateArtifact, io.ReadCloser, error) {
	template, err := s.GetAdmin(ctx, id)
	return s.openArtifact(ctx, template, kind, err)
}

func (s *EditingTemplateService) OpenPublicArtifact(ctx context.Context, id, kind string) (*EditingTemplateArtifact, io.ReadCloser, error) {
	template, err := s.GetPublic(ctx, id)
	return s.openArtifact(ctx, template, kind, err)
}

func (s *EditingTemplateService) openArtifact(ctx context.Context, template *EditingTemplate, kind string, sourceErr error) (*EditingTemplateArtifact, io.ReadCloser, error) {
	if sourceErr != nil {
		return nil, nil, sourceErr
	}
	if !validEditingTemplateArtifactKind(kind) || template.Artifacts[kind] == nil {
		return nil, nil, infraerrors.NotFound("EDITING_TEMPLATE_ARTIFACT_NOT_FOUND", "模板文件不存在")
	}
	artifact := template.Artifacts[kind]
	reader, err := s.storage.Open(ctx, artifact.StorageKey)
	if err != nil {
		return nil, nil, fmt.Errorf("open editing template artifact: %w", err)
	}
	return artifact, reader, nil
}

func (s *EditingTemplateService) MaxArtifactBytes(kind string) int64 {
	const (
		defaultProject = int64(256 * 1024 * 1024)
		defaultCover   = int64(16 * 1024 * 1024)
		defaultPreview = int64(256 * 1024 * 1024)
	)
	if s == nil || s.cfg == nil {
		switch kind {
		case EditingTemplateArtifactCover:
			return defaultCover
		case EditingTemplateArtifactPreview:
			return defaultPreview
		default:
			return defaultProject
		}
	}
	configured := s.cfg.EditingTemplates.MaxProjectBytes
	defaultValue := defaultProject
	switch kind {
	case EditingTemplateArtifactCover:
		configured, defaultValue = s.cfg.EditingTemplates.MaxCoverBytes, defaultCover
	case EditingTemplateArtifactPreview:
		configured, defaultValue = s.cfg.EditingTemplates.MaxPreviewBytes, defaultPreview
	}
	if configured <= 0 {
		return defaultValue
	}
	return configured
}

func (s *EditingTemplateService) ownerTemplate(ctx context.Context, ownerID int64, id string) (*EditingTemplate, error) {
	if ownerID <= 0 || uuid.Validate(id) != nil {
		return nil, ErrEditingTemplateNotFound
	}
	return s.repo.GetByOwner(ctx, id, ownerID)
}

func (s *EditingTemplateService) moderateMetadata(ctx context.Context, ownerID int64, input EditingTemplateMetadataInput) (string, error) {
	if s.moderation == nil {
		return ContentModerationActionAllow, nil
	}
	body, _ := json.Marshal(map[string]any{"messages": []map[string]string{{"role": "user", "content": input.Title + "\n" + input.Description}}})
	decision, err := s.moderation.Check(ctx, ContentModerationCheckInput{
		UserID: ownerID, Endpoint: "/editing-templates", Provider: "autodesign",
		Model: "editing-template-metadata", Protocol: ContentModerationProtocolOpenAIChat, Body: body,
	})
	if err != nil {
		return "", fmt.Errorf("moderate editing template metadata: %w", err)
	}
	if decision != nil && decision.Blocked {
		message := strings.TrimSpace(decision.Message)
		if message == "" {
			message = "标题或简介未通过内容审核"
		}
		return "", infraerrors.BadRequest("EDITING_TEMPLATE_CONTENT_BLOCKED", message)
	}
	if decision != nil && decision.Action != "" {
		return decision.Action, nil
	}
	return ContentModerationActionAllow, nil
}

func normalizeEditingTemplateMetadata(input EditingTemplateMetadataInput) EditingTemplateMetadataInput {
	input.Title = strings.TrimSpace(input.Title)
	input.Description = strings.TrimSpace(input.Description)
	input.EditingBrief = strings.TrimSpace(input.EditingBrief)
	input.AspectRatio = strings.TrimSpace(input.AspectRatio)
	return input
}

func validateEditingTemplateMetadata(input EditingTemplateMetadataInput) error {
	if input.Title == "" || utf8.RuneCountInString(input.Title) > 80 {
		return infraerrors.BadRequest("EDITING_TEMPLATE_TITLE_INVALID", "标题必填且不能超过 80 字")
	}
	if utf8.RuneCountInString(input.Description) > 1000 {
		return infraerrors.BadRequest("EDITING_TEMPLATE_DESCRIPTION_INVALID", "简介不能超过 1000 字")
	}
	if utf8.RuneCountInString(input.EditingBrief) > 6000 {
		return infraerrors.BadRequest("EDITING_TEMPLATE_BRIEF_INVALID", "剪辑要领不能超过 6000 字")
	}
	if utf8.RuneCountInString(input.AspectRatio) > 32 {
		return infraerrors.BadRequest("EDITING_TEMPLATE_ASPECT_RATIO_INVALID", "画幅格式无效")
	}
	if input.DurationMS < 0 || input.DurationMS > int64(24*time.Hour/time.Millisecond) {
		return infraerrors.BadRequest("EDITING_TEMPLATE_DURATION_INVALID", "模板时长无效")
	}
	return nil
}

func validateEditingTemplateArtifact(kind, filename, contentType string) error {
	if !validEditingTemplateArtifactKind(kind) {
		return infraerrors.BadRequest("EDITING_TEMPLATE_ARTIFACT_KIND_INVALID", "模板文件类型无效")
	}
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(filename)))
	mediaType, _, _ := mime.ParseMediaType(contentType)
	mediaType = strings.ToLower(mediaType)
	switch kind {
	case EditingTemplateArtifactProject:
		if ext != ".ccproj" {
			return infraerrors.BadRequest("EDITING_TEMPLATE_PROJECT_INVALID", "工程包必须是 .ccproj 文件")
		}
	case EditingTemplateArtifactCover:
		if !oneOf(ext, ".jpg", ".jpeg", ".png", ".webp") || (mediaType != "" && !strings.HasPrefix(mediaType, "image/")) {
			return infraerrors.BadRequest("EDITING_TEMPLATE_COVER_INVALID", "封面必须是 JPG、PNG 或 WebP 图片")
		}
	case EditingTemplateArtifactPreview:
		if !oneOf(ext, ".mp4", ".webm", ".mov") || (mediaType != "" && !strings.HasPrefix(mediaType, "video/")) {
			return infraerrors.BadRequest("EDITING_TEMPLATE_PREVIEW_INVALID", "预览成片必须是 MP4、WebM 或 MOV 视频")
		}
	}
	return nil
}

func validEditingTemplateArtifactKind(kind string) bool {
	return oneOf(kind, EditingTemplateArtifactProject, EditingTemplateArtifactCover, EditingTemplateArtifactPreview)
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
