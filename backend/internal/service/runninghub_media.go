package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// Node values of the form "media:<uuid>" refer to files staged on sub2api.
// They are uploaded lazily to whichever account runs the task, because
// RunningHub file names are only valid on the account that uploaded them.
const rhMediaPrefix = "media:"

const rhMaxMediaBytes = 512 << 20

type RunningHubMedia struct {
	ID          string `json:"id"`
	Ref         string `json:"ref"` // value to put into a node's fieldValue
	FileName    string `json:"file_name"`
	ContentType string `json:"content_type"`
	FileType    string `json:"file_type"`
	SizeBytes   int64  `json:"size_bytes"`
}

func rhFileType(filename, contentType string) string {
	ct := strings.ToLower(contentType)
	ext := strings.ToLower(path.Ext(filename))
	switch {
	case strings.HasPrefix(ct, "image/"), ext == ".jpg", ext == ".jpeg", ext == ".png", ext == ".webp", ext == ".gif":
		return "image"
	case strings.HasPrefix(ct, "audio/"), ext == ".mp3", ext == ".wav", ext == ".m4a", ext == ".flac", ext == ".ogg":
		return "audio"
	case strings.HasPrefix(ct, "video/"), ext == ".mp4", ext == ".mov", ext == ".webm", ext == ".mkv":
		return "video"
	}
	return "input"
}

// StageMedia stores an upload on local disk. Public media (admin defaults for a
// workflow) can be referenced by any user's task.
func (s *RunningHubService) StageMedia(ctx context.Context, userID int64, public bool, r io.Reader, filename, contentType string) (RunningHubMedia, error) {
	if s.db == nil {
		return RunningHubMedia{}, ErrRunningHubNoDB
	}
	if err := os.MkdirAll(s.mediaDir, 0o750); err != nil {
		return RunningHubMedia{}, err
	}
	id := uuid.New().String()
	base := filepath.Base(filename)
	if base == "." || base == string(filepath.Separator) || base == "" {
		base = "file"
	}
	dst := filepath.Join(s.mediaDir, id+strings.ToLower(filepath.Ext(base)))
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return RunningHubMedia{}, err
	}
	n, err := io.Copy(f, io.LimitReader(r, rhMaxMediaBytes+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil && n > rhMaxMediaBytes {
		err = fmt.Errorf("file exceeds %d MB", rhMaxMediaBytes>>20)
	}
	if err != nil {
		_ = os.Remove(dst)
		return RunningHubMedia{}, err
	}
	m := RunningHubMedia{
		ID: id, Ref: rhMediaPrefix + id, FileName: base,
		ContentType: contentType, FileType: rhFileType(base, contentType), SizeBytes: n,
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO runninghub_media(id,user_id,is_public,file_name,content_type,file_type,size_bytes,storage_path)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
		m.ID, userID, public, m.FileName, m.ContentType, m.FileType, m.SizeBytes, dst)
	if err != nil {
		_ = os.Remove(dst)
		return RunningHubMedia{}, err
	}
	return m, nil
}

// resolveMediaRefs replaces every "media:<uuid>" fieldValue with an upstream
// fileName valid on account a, uploading (and caching) as needed.
func (s *RunningHubService) resolveMediaRefs(ctx context.Context, userID int64, a rhAccountRow, nodes []rhNode) error {
	for _, n := range nodes {
		v, _ := n["fieldValue"].(string)
		if !strings.HasPrefix(v, rhMediaPrefix) {
			continue
		}
		name, err := s.mediaOnAccount(ctx, userID, strings.TrimPrefix(v, rhMediaPrefix), a)
		if err != nil {
			return err
		}
		n["fieldValue"] = name
	}
	return nil
}

func (s *RunningHubService) mediaOnAccount(ctx context.Context, userID int64, mediaID string, a rhAccountRow) (string, error) {
	if _, err := uuid.Parse(mediaID); err != nil {
		return "", fmt.Errorf("invalid media reference %q", mediaID)
	}
	var cached string
	err := s.db.QueryRowContext(ctx,
		`SELECT upstream_file_name FROM runninghub_media_uploads WHERE media_id=$1 AND account_id=$2`,
		mediaID, a.ID).Scan(&cached)
	if err == nil {
		// Still enforce ownership on cache hits.
		if err := s.checkMediaAccess(ctx, userID, mediaID); err != nil {
			return "", err
		}
		return cached, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}

	var fileName, fileType, storagePath string
	err = s.db.QueryRowContext(ctx,
		`SELECT file_name, file_type, storage_path FROM runninghub_media
		 WHERE id=$1 AND (user_id=$2 OR is_public)`,
		mediaID, userID).Scan(&fileName, &fileType, &storagePath)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("media %s not found", mediaID)
	}
	if err != nil {
		return "", err
	}
	f, err := os.Open(storagePath)
	if err != nil {
		return "", fmt.Errorf("media %s unavailable: %w", mediaID, err)
	}
	defer f.Close()
	name, err := s.uploadFile(ctx, a.APIKey, fileType, fileName, f)
	if err != nil {
		return "", fmt.Errorf("upload media to RunningHub: %w", err)
	}
	_, _ = s.db.ExecContext(ctx,
		`INSERT INTO runninghub_media_uploads(media_id, account_id, upstream_file_name)
		 VALUES($1,$2,$3) ON CONFLICT (media_id, account_id) DO UPDATE SET upstream_file_name=EXCLUDED.upstream_file_name`,
		mediaID, a.ID, name)
	return name, nil
}

func (s *RunningHubService) checkMediaAccess(ctx context.Context, userID int64, mediaID string) error {
	var ok bool
	err := s.db.QueryRowContext(ctx,
		`SELECT true FROM runninghub_media WHERE id=$1 AND (user_id=$2 OR is_public)`,
		mediaID, userID).Scan(&ok)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("media %s not found", mediaID)
	}
	return err
}

// RunningHubMediaFile is a local media file ready to be served to a browser.
type RunningHubMediaFile struct {
	StoragePath string
	ContentType string
	FileName    string
}

// GetMediaFile looks up a staged media file by its UUID (without the "media:" prefix).
// Only public files are accessible here (admin workflow defaults).
func (s *RunningHubService) GetMediaFile(ctx context.Context, id string) (RunningHubMediaFile, error) {
	if s.db == nil {
		return RunningHubMediaFile{}, ErrRunningHubNoDB
	}
	var f RunningHubMediaFile
	err := s.db.QueryRowContext(ctx,
		`SELECT storage_path, content_type, file_name FROM runninghub_media WHERE id=$1 AND is_public`, id).
		Scan(&f.StoragePath, &f.ContentType, &f.FileName)
	if errors.Is(err, sql.ErrNoRows) {
		return f, ErrRunningHubNotFound
	}
	return f, err
}

// ProxyRHFile fetches a file from RunningHub using any enabled account key
// and returns the raw response body for the caller to stream to the browser.
// This allows admin UI to preview workflow node default files (image/video/audio)
// without exposing API keys to the frontend.
func (s *RunningHubService) ProxyRHFile(ctx context.Context, fileName string) (*http.Response, error) {
	a, err := s.anyAccount(ctx)
	if err != nil {
		return nil, err
	}
	q := url.Values{"apiKey": {a.APIKey}, "fileName": {fileName}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		s.baseURL+"/task/openapi/download?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("RunningHub download HTTP %d", resp.StatusCode)
	}
	return resp, nil
}
