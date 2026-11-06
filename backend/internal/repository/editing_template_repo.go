package repository

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

type editingTemplateRepository struct {
	db *sql.DB
}

func NewEditingTemplateRepository(db *sql.DB) service.EditingTemplateRepository {
	return &editingTemplateRepository{db: db}
}

const editingTemplateColumns = `
	id, owner_id, title, description, editing_brief, aspect_ratio, duration_ms,
	status, rejection_reason, moderation_action, submitted_at, reviewed_at,
	published_at, rejected_at, unlisted_at, created_at, updated_at`

func (r *editingTemplateRepository) Create(ctx context.Context, template *service.EditingTemplate) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO editing_templates (
			id, owner_id, title, description, editing_brief, aspect_ratio,
			duration_ms, status, moderation_action
		) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9)
	`, template.ID, template.OwnerID, template.Title, template.Description, template.EditingBrief,
		template.AspectRatio, template.DurationMS, template.Status, template.ModerationAction)
	return err
}

func (r *editingTemplateRepository) GetByID(ctx context.Context, id string) (*service.EditingTemplate, error) {
	template, err := scanEditingTemplate(r.db.QueryRowContext(ctx, `SELECT `+editingTemplateColumns+` FROM editing_templates WHERE id = $1::uuid`, id))
	if err != nil {
		return nil, editingTemplateScanError(err)
	}
	if err := r.loadRelations(ctx, template); err != nil {
		return nil, err
	}
	return template, nil
}

func (r *editingTemplateRepository) GetByOwner(ctx context.Context, id string, ownerID int64) (*service.EditingTemplate, error) {
	template, err := scanEditingTemplate(r.db.QueryRowContext(ctx, `SELECT `+editingTemplateColumns+` FROM editing_templates WHERE id = $1::uuid AND owner_id = $2`, id, ownerID))
	if err != nil {
		return nil, editingTemplateScanError(err)
	}
	if err := r.loadRelations(ctx, template); err != nil {
		return nil, err
	}
	return template, nil
}

func (r *editingTemplateRepository) List(ctx context.Context, filter service.EditingTemplateListFilter, page pagination.PaginationParams) ([]service.EditingTemplate, *pagination.PaginationResult, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 5)
	if filter.OwnerID != nil {
		args = append(args, *filter.OwnerID)
		where = append(where, fmt.Sprintf("owner_id = $%d", len(args)))
	}
	if strings.TrimSpace(filter.Status) != "" {
		args = append(args, strings.TrimSpace(filter.Status))
		where = append(where, fmt.Sprintf("status = $%d", len(args)))
	}
	if search := strings.TrimSpace(filter.Search); search != "" {
		args = append(args, "%"+search+"%")
		where = append(where, fmt.Sprintf("(title ILIKE $%d OR description ILIKE $%d)", len(args), len(args)))
	}
	clause := strings.Join(where, " AND ")
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM editing_templates WHERE `+clause, args...).Scan(&total); err != nil {
		return nil, nil, err
	}
	page.Page = max(page.Page, 1)
	page.PageSize = page.Limit()
	order := "updated_at DESC, id DESC"
	if filter.Public {
		order = "published_at DESC NULLS LAST, id DESC"
	} else if filter.Status == service.EditingTemplateStatusPendingReview {
		order = "submitted_at ASC NULLS LAST, id ASC"
	}
	listArgs := append(append([]any{}, args...), page.PageSize, page.Offset())
	query := fmt.Sprintf(`SELECT %s FROM editing_templates WHERE %s ORDER BY %s LIMIT $%d OFFSET $%d`, editingTemplateColumns, clause, order, len(args)+1, len(args)+2)
	rows, err := r.db.QueryContext(ctx, query, listArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	items := make([]service.EditingTemplate, 0)
	for rows.Next() {
		template, scanErr := scanEditingTemplate(rows)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		if err := r.loadArtifacts(ctx, template); err != nil {
			return nil, nil, err
		}
		items = append(items, *template)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	pages := int(math.Ceil(float64(total) / float64(page.PageSize)))
	if pages < 1 {
		pages = 1
	}
	return items, &pagination.PaginationResult{Total: total, Page: page.Page, PageSize: page.PageSize, Pages: pages}, nil
}

func (r *editingTemplateRepository) UpdateMetadata(ctx context.Context, template *service.EditingTemplate, expectedStatuses []string) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE editing_templates SET
			title = $3, description = $4, editing_brief = $5, aspect_ratio = $6,
			duration_ms = $7, moderation_action = $8, status = 'draft',
			rejection_reason = '', reviewed_at = NULL, rejected_at = NULL,
			unlisted_at = NULL, updated_at = NOW()
		WHERE id = $1::uuid AND owner_id = $2 AND status = ANY($9)
	`, template.ID, template.OwnerID, template.Title, template.Description, template.EditingBrief,
		template.AspectRatio, template.DurationMS, template.ModerationAction, pq.Array(expectedStatuses))
	if err != nil {
		return err
	}
	return editingTemplateRowsChanged(result)
}

func (r *editingTemplateRepository) UpsertArtifact(ctx context.Context, artifact *service.EditingTemplateArtifact, ownerID int64, expectedStatuses []string) (*service.EditingTemplateArtifact, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM editing_templates WHERE id = $1::uuid AND owner_id = $2 FOR UPDATE`, artifact.TemplateID, ownerID).Scan(&status); err != nil {
		return nil, editingTemplateScanError(err)
	}
	if !editingTemplateStatusAllowed(status, expectedStatuses) {
		return nil, service.ErrEditingTemplateState
	}
	previous, err := scanEditingTemplateArtifact(tx.QueryRowContext(ctx, `
		SELECT id, template_id::text, kind, storage_key, original_filename, content_type, byte_size, sha256, created_at, updated_at
		FROM editing_template_artifacts WHERE template_id = $1::uuid AND kind = $2
	`, artifact.TemplateID, artifact.Kind))
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == sql.ErrNoRows {
		previous = nil
	}
	err = tx.QueryRowContext(ctx, `
		INSERT INTO editing_template_artifacts (
			template_id, kind, storage_key, original_filename, content_type, byte_size, sha256
		) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (template_id, kind) DO UPDATE SET
			storage_key = EXCLUDED.storage_key,
			original_filename = EXCLUDED.original_filename,
			content_type = EXCLUDED.content_type,
			byte_size = EXCLUDED.byte_size,
			sha256 = EXCLUDED.sha256,
			updated_at = NOW()
		RETURNING id, created_at, updated_at
	`, artifact.TemplateID, artifact.Kind, artifact.StorageKey, artifact.OriginalFilename,
		artifact.ContentType, artifact.ByteSize, artifact.SHA256).Scan(&artifact.ID, &artifact.CreatedAt, &artifact.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE editing_templates SET status = 'draft', rejection_reason = '',
			reviewed_at = NULL, rejected_at = NULL, unlisted_at = NULL, updated_at = NOW()
		WHERE id = $1::uuid
	`, artifact.TemplateID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return previous, nil
}

func (r *editingTemplateRepository) Transition(ctx context.Context, id string, ownerID *int64, expectedStatuses []string, toStatus, action, reason string, actorID *int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	query := `SELECT status FROM editing_templates WHERE id = $1::uuid`
	args := []any{id}
	if ownerID != nil {
		query += ` AND owner_id = $2`
		args = append(args, *ownerID)
	}
	query += ` FOR UPDATE`
	var fromStatus string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&fromStatus); err != nil {
		return editingTemplateScanError(err)
	}
	if !editingTemplateStatusAllowed(fromStatus, expectedStatuses) {
		return service.ErrEditingTemplateState
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE editing_templates SET
			status = $2::text,
			rejection_reason = CASE WHEN $2 = 'rejected' THEN $3 WHEN $2 IN ('pending_review', 'approved') THEN '' ELSE rejection_reason END,
			submitted_at = CASE WHEN $2 = 'pending_review' THEN NOW() ELSE submitted_at END,
			reviewed_at = CASE WHEN $2 IN ('approved', 'rejected') THEN NOW() ELSE reviewed_at END,
			published_at = CASE WHEN $2 = 'approved' THEN NOW() ELSE published_at END,
			rejected_at = CASE WHEN $2 = 'rejected' THEN NOW() WHEN $2 = 'pending_review' THEN NULL ELSE rejected_at END,
			unlisted_at = CASE WHEN $2 = 'unlisted' THEN NOW() WHEN $2 = 'pending_review' THEN NULL ELSE unlisted_at END,
			updated_at = NOW()
		WHERE id = $1::uuid AND status = $4
	`, id, toStatus, reason, fromStatus)
	if err != nil {
		return err
	}
	if err := editingTemplateRowsChanged(result); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO editing_template_reviews (template_id, actor_id, action, from_status, to_status, reason)
		VALUES ($1::uuid, $2, $3, $4, $5, $6)
	`, id, actorID, action, fromStatus, toStatus, reason); err != nil {
		return err
	}
	return tx.Commit()
}

type editingTemplateScanner interface {
	Scan(dest ...any) error
}

func scanEditingTemplate(scanner editingTemplateScanner) (*service.EditingTemplate, error) {
	template := &service.EditingTemplate{Artifacts: map[string]*service.EditingTemplateArtifact{}}
	var submittedAt, reviewedAt, publishedAt, rejectedAt, unlistedAt sql.NullTime
	err := scanner.Scan(
		&template.ID, &template.OwnerID, &template.Title, &template.Description,
		&template.EditingBrief, &template.AspectRatio, &template.DurationMS,
		&template.Status, &template.RejectionReason, &template.ModerationAction,
		&submittedAt, &reviewedAt, &publishedAt, &rejectedAt, &unlistedAt,
		&template.CreatedAt, &template.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	template.SubmittedAt = nullableTimePointer(submittedAt)
	template.ReviewedAt = nullableTimePointer(reviewedAt)
	template.PublishedAt = nullableTimePointer(publishedAt)
	template.RejectedAt = nullableTimePointer(rejectedAt)
	template.UnlistedAt = nullableTimePointer(unlistedAt)
	return template, nil
}

func scanEditingTemplateArtifact(scanner editingTemplateScanner) (*service.EditingTemplateArtifact, error) {
	artifact := &service.EditingTemplateArtifact{}
	err := scanner.Scan(&artifact.ID, &artifact.TemplateID, &artifact.Kind, &artifact.StorageKey,
		&artifact.OriginalFilename, &artifact.ContentType, &artifact.ByteSize, &artifact.SHA256,
		&artifact.CreatedAt, &artifact.UpdatedAt)
	return artifact, err
}

func (r *editingTemplateRepository) loadRelations(ctx context.Context, template *service.EditingTemplate) error {
	if err := r.loadArtifacts(ctx, template); err != nil {
		return err
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, template_id::text, actor_id, action, from_status, to_status, reason, created_at
		FROM editing_template_reviews WHERE template_id = $1::uuid ORDER BY created_at DESC, id DESC
	`, template.ID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	template.Reviews = make([]service.EditingTemplateReview, 0)
	for rows.Next() {
		var review service.EditingTemplateReview
		var actorID sql.NullInt64
		if err := rows.Scan(&review.ID, &review.TemplateID, &actorID, &review.Action,
			&review.FromStatus, &review.ToStatus, &review.Reason, &review.CreatedAt); err != nil {
			return err
		}
		if actorID.Valid {
			review.ActorID = &actorID.Int64
		}
		template.Reviews = append(template.Reviews, review)
	}
	return rows.Err()
}

func (r *editingTemplateRepository) loadArtifacts(ctx context.Context, template *service.EditingTemplate) error {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, template_id::text, kind, storage_key, original_filename, content_type, byte_size, sha256, created_at, updated_at
		FROM editing_template_artifacts WHERE template_id = $1::uuid ORDER BY kind
	`, template.ID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	template.Artifacts = make(map[string]*service.EditingTemplateArtifact)
	for rows.Next() {
		artifact, scanErr := scanEditingTemplateArtifact(rows)
		if scanErr != nil {
			return scanErr
		}
		template.Artifacts[artifact.Kind] = artifact
	}
	return rows.Err()
}

func editingTemplateScanError(err error) error {
	if err == sql.ErrNoRows {
		return service.ErrEditingTemplateNotFound
	}
	return err
}

func editingTemplateRowsChanged(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return service.ErrEditingTemplateState
	}
	return nil
}

func editingTemplateStatusAllowed(status string, expected []string) bool {
	for _, candidate := range expected {
		if status == candidate {
			return true
		}
	}
	return false
}

func nullableTimePointer(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	copy := value.Time
	return &copy
}
