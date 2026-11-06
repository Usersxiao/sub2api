package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// S3EditingTemplateStorage uses the same private artifact contract for S3 and MinIO.
// A bounded spool computes the checksum before upload and never buffers video in RAM.
type S3EditingTemplateStorage struct {
	client *s3.Client
	bucket string
	prefix string
}

func NewS3EditingTemplateStorage(cfg config.EditingTemplateS3Config) (*S3EditingTemplateStorage, error) {
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil {
		return nil, fmt.Errorf("editing template S3 endpoint must be an HTTP(S) URL")
	}
	if strings.TrimSpace(cfg.Bucket) == "" || cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("editing template S3 bucket and credentials are required")
	}
	prefix := strings.Trim(cfg.Prefix, "/")
	if strings.Contains(prefix, "..") || strings.Contains(prefix, "\\") {
		return nil, fmt.Errorf("invalid editing template S3 prefix")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	client := s3.NewFromConfig(aws.Config{
		Region:                     region,
		Credentials:                credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(cfg.Endpoint)
		options.UsePathStyle = cfg.ForcePathStyle
	})
	return &S3EditingTemplateStorage{client: client, bucket: cfg.Bucket, prefix: prefix}, nil
}

func (s *S3EditingTemplateStorage) Put(ctx context.Context, templateID, kind, filename, contentType string, reader io.Reader, maxBytes int64) (*EditingTemplateArtifact, error) {
	if uuid.Validate(templateID) != nil || !validEditingTemplateArtifactKind(kind) {
		return nil, infraerrors.BadRequest("EDITING_TEMPLATE_STORAGE_KEY_INVALID", "模板存储路径无效")
	}
	if reader == nil || maxBytes <= 0 || maxBytes == int64(^uint64(0)>>1) {
		return nil, fmt.Errorf("invalid editing template upload limit or reader")
	}
	file, err := os.CreateTemp("", "editing-template-s3-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hash := sha256.New()
	size, err := copyWithContext(ctx, io.MultiWriter(file, hash), io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, infraerrors.BadRequest("EDITING_TEMPLATE_ARTIFACT_EMPTY", "上传文件为空")
	}
	if size > maxBytes {
		return nil, infraerrors.BadRequest("EDITING_TEMPLATE_ARTIFACT_TOO_LARGE", "上传文件超过大小限制")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	ext := strings.ToLower(path.Ext(path.Base(filename)))
	if len(ext) > 12 {
		ext = ""
	}
	if contentType == "" {
		contentType = mime.TypeByExtension(ext)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	key := path.Join(s.prefix, templateID, kind, uuid.NewString()+ext)
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: file,
		ContentLength: aws.Int64(size), ContentType: aws.String(contentType)})
	if err != nil {
		return nil, fmt.Errorf("upload editing template object: %w", err)
	}
	return &EditingTemplateArtifact{TemplateID: templateID, Kind: kind, StorageKey: key,
		OriginalFilename: path.Base(filename), ContentType: contentType, ByteSize: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func (s *S3EditingTemplateStorage) validKey(key string) bool {
	if key == "" || path.Clean(key) != key || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") {
		return false
	}
	relative := key
	if s.prefix != "" {
		if !strings.HasPrefix(key, s.prefix+"/") {
			return false
		}
		relative = strings.TrimPrefix(key, s.prefix+"/")
	}
	parts := strings.Split(relative, "/")
	return len(parts) == 3 && uuid.Validate(parts[0]) == nil && validEditingTemplateArtifactKind(parts[1]) && !strings.HasPrefix(parts[2], ".")
}

func (s *S3EditingTemplateStorage) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if !s.validKey(key) {
		return nil, fmt.Errorf("invalid editing template object key")
	}
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	return result.Body, nil
}

func (s *S3EditingTemplateStorage) Delete(ctx context.Context, key string) error {
	if !s.validKey(key) {
		return fmt.Errorf("invalid editing template object key")
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	return err
}
