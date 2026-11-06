//go:build unit

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
)

func TestEditingTemplateS3Configuration(t *testing.T) {
	if _, err := ProvideEditingTemplateStorage(&config.Config{EditingTemplates: config.EditingTemplateConfig{Storage: "unknown"}}); err == nil {
		t.Fatal("invalid storage silently fell back")
	}
	if _, err := NewS3EditingTemplateStorage(config.EditingTemplateS3Config{}); err == nil {
		t.Fatal("missing S3 configuration accepted")
	}
}

// Run against a real disposable MinIO bucket, never a mocked S3 handler.
func TestEditingTemplateMinIORoundTrip(t *testing.T) {
	endpoint := os.Getenv("EDITING_TEMPLATE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set EDITING_TEMPLATE_TEST_S3_ENDPOINT for MinIO integration")
	}
	store, err := NewS3EditingTemplateStorage(config.EditingTemplateS3Config{Endpoint: endpoint, Bucket: "editing-template-acceptance", Region: "us-east-1", Prefix: "storage-test", AccessKeyID: "template-local", SecretAccessKey: "template-local-acceptance-2026", ForcePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := bytes.Repeat([]byte("template-project-media-roundtrip\n"), 4096)
	id := uuid.NewString()
	artifact, err := store.Put(ctx, id, "project", "project.ccproj", "application/octet-stream", bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Delete(ctx, artifact.StorageKey) })
	hash := sha256.Sum256(data)
	if artifact.SHA256 != hex.EncodeToString(hash[:]) || artifact.ByteSize != int64(len(data)) {
		t.Fatal("wrong artifact integrity metadata")
	}
	reader, err := store.Open(ctx, artifact.StorageKey)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("MinIO object bytes differ")
	}
	if _, err = store.Put(ctx, id, "preview", "preview.mp4", "video/mp4", bytes.NewReader(data), 10); err == nil {
		t.Fatal("oversized upload accepted")
	}
	if _, err = store.Open(ctx, "../outside"); err == nil {
		t.Fatal("out-of-prefix key accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = store.Put(cancelled, id, "cover", "cover.jpg", "image/jpeg", bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("cancelled upload accepted")
	}
	if err = store.Delete(ctx, artifact.StorageKey); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Open(ctx, artifact.StorageKey); err == nil {
		t.Fatal("deleted object still available")
	}
}
