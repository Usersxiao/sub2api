//go:build unit

package repository

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Exercise the SQL against PostgreSQL: fake repositories miss parameter type
// inference between the varchar status column and CASE text comparisons.
func TestEditingTemplateRepositoryReviewTransitions(t *testing.T) {
	ctx := context.Background()
	db := authSessionIsolatedDB(t)
	_, err := db.Exec(`CREATE TABLE users(id BIGINT PRIMARY KEY); INSERT INTO users VALUES (1)`)
	require.NoError(t, err)
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "274_editing_templates.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(migration))
	require.NoError(t, err)
	ownerID := int64(1)
	repo := NewEditingTemplateRepository(db)
	template := &service.EditingTemplate{ID: uuid.NewString(), OwnerID: ownerID, Title: "Complex video", Status: "draft", ModerationAction: "allow"}
	require.NoError(t, repo.Create(ctx, template))
	for _, step := range []struct{ from, to, action, reason string }{
		{"draft", "pending_review", "submit", ""},
		{"pending_review", "rejected", "reject", "Fix the preview"},
		{"rejected", "pending_review", "submit", ""},
		{"pending_review", "approved", "approve", ""},
		{"approved", "unlisted", "unlist", "Updated version available"},
	} {
		require.NoError(t, repo.Transition(ctx, template.ID, &ownerID, []string{step.from}, step.to, step.action, step.reason, &ownerID))
		got, err := repo.GetByID(ctx, template.ID)
		require.NoError(t, err)
		require.Equal(t, step.to, got.Status)
		require.Equal(t, step.action, got.Reviews[0].Action)
		require.NotNil(t, got.SubmittedAt)
		if step.to == "approved" {
			require.NotNil(t, got.PublishedAt)
			require.NotNil(t, got.ReviewedAt)
			require.Empty(t, got.RejectionReason)
			require.Nil(t, got.RejectedAt)
		}
	}
	require.ErrorIs(t, repo.Transition(ctx, template.ID, &ownerID, []string{"pending_review"}, "approved", "approve", "", &ownerID), service.ErrEditingTemplateState)
	got, err := repo.GetByID(ctx, template.ID)
	require.NoError(t, err)
	require.Len(t, got.Reviews, 5)
	require.NotNil(t, got.UnlistedAt)
}
