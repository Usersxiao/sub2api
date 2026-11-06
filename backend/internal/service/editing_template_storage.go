package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/google/uuid"
)

type LocalEditingTemplateStorage struct {
	rootDir string
}

func NewLocalEditingTemplateStorage(cfg *config.Config) *LocalEditingTemplateStorage {
	root := ""
	if cfg != nil {
		root = strings.TrimSpace(cfg.EditingTemplates.DataDir)
	}
	if root == "" {
		base := strings.TrimSpace(os.Getenv("DATA_DIR"))
		if base == "" {
			base = "./data"
		}
		root = filepath.Join(base, "editing-templates")
	}
	return &LocalEditingTemplateStorage{rootDir: filepath.Clean(root)}
}

func ProvideEditingTemplateStorage(cfg *config.Config) (EditingTemplateStorage, error) {
	if cfg == nil || cfg.EditingTemplates.Storage == "" || cfg.EditingTemplates.Storage == "local" {
		return NewLocalEditingTemplateStorage(cfg), nil
	}
	if cfg.EditingTemplates.Storage == "s3" {
		return NewS3EditingTemplateStorage(cfg.EditingTemplates.S3)
	}
	return nil, fmt.Errorf("unknown editing template storage: %q", cfg.EditingTemplates.Storage)
}

func (s *LocalEditingTemplateStorage) RootDir() string {
	if s == nil {
		return ""
	}
	return s.rootDir
}

func (s *LocalEditingTemplateStorage) Put(ctx context.Context, templateID, kind, filename, contentType string, reader io.Reader, maxBytes int64) (*EditingTemplateArtifact, error) {
	if s == nil || s.rootDir == "" || reader == nil {
		return nil, infraerrors.InternalServer("EDITING_TEMPLATE_STORAGE_UNAVAILABLE", "模板存储未配置")
	}
	if uuid.Validate(templateID) != nil || !validEditingTemplateArtifactKind(kind) {
		return nil, infraerrors.BadRequest("EDITING_TEMPLATE_STORAGE_KEY_INVALID", "模板存储路径无效")
	}
	if maxBytes <= 0 {
		return nil, infraerrors.InternalServer("EDITING_TEMPLATE_STORAGE_LIMIT_INVALID", "模板上传限制未配置")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ext := strings.ToLower(filepath.Ext(filepath.Base(filename)))
	if len(ext) > 12 {
		ext = ""
	}
	directory := filepath.Join(s.rootDir, templateID, kind)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create editing template storage directory: %w", err)
	}
	tempFile, err := os.CreateTemp(directory, ".upload-*")
	if err != nil {
		return nil, fmt.Errorf("create editing template upload: %w", err)
	}
	tempPath := tempFile.Name()
	committed := false
	defer func() {
		_ = tempFile.Close()
		if !committed {
			_ = os.Remove(tempPath)
		}
	}()

	hasher := sha256.New()
	written, err := copyWithContext(ctx, io.MultiWriter(tempFile, hasher), io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("store editing template artifact: %w", err)
	}
	if written == 0 {
		return nil, infraerrors.BadRequest("EDITING_TEMPLATE_ARTIFACT_EMPTY", "上传文件为空")
	}
	if written > maxBytes {
		return nil, infraerrors.BadRequest("EDITING_TEMPLATE_ARTIFACT_TOO_LARGE", fmt.Sprintf("上传文件超过 %d 字节限制", maxBytes))
	}
	if err := tempFile.Sync(); err != nil {
		return nil, fmt.Errorf("sync editing template artifact: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return nil, fmt.Errorf("close editing template artifact: %w", err)
	}
	objectName := uuid.NewString() + ext
	finalPath := filepath.Join(directory, objectName)
	if err := os.Rename(tempPath, finalPath); err != nil {
		return nil, fmt.Errorf("commit editing template artifact: %w", err)
	}
	committed = true
	if dir, openErr := os.Open(directory); openErr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	if strings.TrimSpace(contentType) == "" {
		contentType = mime.TypeByExtension(ext)
	}
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/octet-stream"
	}
	return &EditingTemplateArtifact{
		TemplateID:       templateID,
		Kind:             kind,
		StorageKey:       filepath.ToSlash(filepath.Join(templateID, kind, objectName)),
		OriginalFilename: filepath.Base(filename),
		ContentType:      contentType,
		ByteSize:         written,
		SHA256:           hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func (s *LocalEditingTemplateStorage) Open(ctx context.Context, storageKey string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.resolve(storageKey)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func (s *LocalEditingTemplateStorage) Delete(ctx context.Context, storageKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.resolve(storageKey)
	if err != nil {
		return err
	}
	err = os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *LocalEditingTemplateStorage) resolve(storageKey string) (string, error) {
	if s == nil || s.rootDir == "" {
		return "", infraerrors.InternalServer("EDITING_TEMPLATE_STORAGE_UNAVAILABLE", "模板存储未配置")
	}
	storageKey = filepath.FromSlash(strings.TrimSpace(storageKey))
	if storageKey == "" || filepath.IsAbs(storageKey) {
		return "", infraerrors.BadRequest("EDITING_TEMPLATE_STORAGE_KEY_INVALID", "模板存储路径无效")
	}
	clean := filepath.Clean(storageKey)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", infraerrors.BadRequest("EDITING_TEMPLATE_STORAGE_KEY_INVALID", "模板存储路径无效")
	}
	path := filepath.Join(s.rootDir, clean)
	relative, err := filepath.Rel(s.rootDir, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", infraerrors.BadRequest("EDITING_TEMPLATE_STORAGE_KEY_INVALID", "模板存储路径无效")
	}
	return path, nil
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buffer := make([]byte, 64*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		read, readErr := src.Read(buffer)
		if read > 0 {
			count, writeErr := dst.Write(buffer[:read])
			written += int64(count)
			if writeErr != nil {
				return written, writeErr
			}
			if count != read {
				return written, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}
