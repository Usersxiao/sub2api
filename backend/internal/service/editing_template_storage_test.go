package service

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLocalEditingTemplateStorageWritesAtomicallyAndIsolatesPaths(t *testing.T) {
	root := t.TempDir()
	storage := NewLocalEditingTemplateStorage(&config.Config{EditingTemplates: config.EditingTemplateConfig{DataDir: root}})
	id := uuid.NewString()

	artifact, err := storage.Put(context.Background(), id, EditingTemplateArtifactProject, "demo.ccproj", "application/octet-stream", bytes.NewBufferString("project-data"), 64)
	require.NoError(t, err)
	require.Equal(t, int64(12), artifact.ByteSize)
	require.Len(t, artifact.SHA256, 64)
	require.NotContains(t, artifact.StorageKey, "..")

	reader, err := storage.Open(context.Background(), artifact.StorageKey)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, "project-data", string(data))

	entries, err := os.ReadDir(filepath.Join(root, id, EditingTemplateArtifactProject))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.False(t, entries[0].IsDir())

	_, err = storage.Open(context.Background(), "../../outside")
	require.Error(t, err)
}

func TestLocalEditingTemplateStorageRejectsOversizedUploadWithoutResidue(t *testing.T) {
	root := t.TempDir()
	storage := NewLocalEditingTemplateStorage(&config.Config{EditingTemplates: config.EditingTemplateConfig{DataDir: root}})
	id := uuid.NewString()

	_, err := storage.Put(context.Background(), id, EditingTemplateArtifactCover, "cover.png", "image/png", bytes.NewBufferString("12345"), 4)
	require.Error(t, err)

	directory := filepath.Join(root, id, EditingTemplateArtifactCover)
	entries, readErr := os.ReadDir(directory)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}
