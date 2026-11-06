package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEditingTemplatesMigrationDefinesReviewAndArtifactBoundaries(t *testing.T) {
	content, err := FS.ReadFile("274_editing_templates.sql")
	require.NoError(t, err)

	sql := strings.Join(strings.Fields(string(content)), " ")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS editing_templates")
	require.Contains(t, sql, "REFERENCES users(id) ON DELETE CASCADE")
	require.Contains(t, sql, "'draft', 'pending_review', 'approved', 'rejected', 'unlisted'")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS editing_template_artifacts")
	require.Contains(t, sql, "UNIQUE (template_id, kind)")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS editing_template_reviews")
	require.Contains(t, sql, "WHERE status = 'approved'")
	require.NotContains(t, sql, " BYTEA")
}
