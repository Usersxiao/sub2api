package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// RunningHub business code returned (with HTTP 200) when an account's queue is full.
// The task is rejected immediately, so the scheduler must move on to another account.
const rhCodeQueueMaxed = 421

var errRHQueueMaxed = errors.New("RunningHub account queue is full")

// rhEnvelope is the common {code,msg,data} wrapper of RunningHub OpenAPI responses.
type rhEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// RunningHubUpstreamError carries a non-zero business code from RunningHub.
type RunningHubUpstreamError struct {
	Code int
	Msg  string
}

func (e *RunningHubUpstreamError) Error() string {
	return fmt.Sprintf("RunningHub error %d: %s", e.Code, e.Msg)
}

func (s *RunningHubService) decodeEnvelope(res *http.Response) (rhEnvelope, error) {
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	var env rhEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return env, fmt.Errorf("RunningHub HTTP %d", res.StatusCode)
		}
		return env, fmt.Errorf("RunningHub: invalid response: %w", err)
	}
	if env.Code == rhCodeQueueMaxed {
		return env, errRHQueueMaxed
	}
	if env.Code != 0 {
		return env, &RunningHubUpstreamError{Code: env.Code, Msg: env.Msg}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return env, fmt.Errorf("RunningHub HTTP %d", res.StatusCode)
	}
	return env, nil
}

func (s *RunningHubService) upstreamPost(ctx context.Context, path, key string, body any) (rhEnvelope, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return rhEnvelope{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	res, err := s.httpClient.Do(req)
	if err != nil {
		return rhEnvelope{}, err
	}
	return s.decodeEnvelope(res)
}

// rhNode is one entry of a webapp's nodeInfoList.
type rhNode = map[string]any

// rhAppDetail holds the fields we care about from /api/webapp/detail.
type rhAppDetail struct {
	Name               string    `json:"name"`
	Description        string    `json:"description"`
	Covers             []rhCover `json:"covers"`
	InstanceType       string    `json:"instanceType"`
	AvgRunningSeconds  int       `json:"avgRunningSeconds"`
	RunningSuccessRate int       `json:"runningSuccessRate"`
}

type rhCover struct {
	URL          string `json:"url"`
	ThumbnailURI string `json:"thumbnailUri"`
}

// coverURL returns the thumbnail URI of the first cover image.
func (d rhAppDetail) coverURL() string {
	for _, c := range d.Covers {
		if c.ThumbnailURI != "" {
			return c.ThumbnailURI
		}
		if c.URL != "" {
			return c.URL
		}
	}
	return ""
}

// fetchAppDetail calls /api/webapp/detail and returns the subset of fields we use.
func (s *RunningHubService) fetchAppDetail(ctx context.Context, key, webappID string) (rhAppDetail, error) {
	env, err := s.upstreamPost(ctx, "/api/webapp/detail", key, map[string]any{
		"apiKey":   key,
		"webappId": webappID,
		"id":       webappID,
	})
	if err != nil {
		return rhAppDetail{}, fmt.Errorf("fetch webapp detail: %w", err)
	}
	var d rhAppDetail
	if err := json.Unmarshal(env.Data, &d); err != nil {
		return d, fmt.Errorf("parse webapp detail: %w", err)
	}
	return d, nil
}

// fetchAppNodes reads the default nodeInfoList of a webapp via apiCallDemo.
func (s *RunningHubService) fetchAppNodes(ctx context.Context, key, webappID string) ([]rhNode, error) {
	q := url.Values{"apiKey": {key}, "webappId": {webappID}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/api/webapp/apiCallDemo?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	res, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	env, err := s.decodeEnvelope(res)
	if err != nil {
		return nil, err
	}
	var data struct {
		NodeInfoList []rhNode `json:"nodeInfoList"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return nil, fmt.Errorf("RunningHub apiCallDemo: %w", err)
	}
	if data.NodeInfoList == nil {
		data.NodeInfoList = []rhNode{}
	}
	return data.NodeInfoList, nil
}

// uploadFile pushes a file to the given account and returns the upstream fileName.
func (s *RunningHubService) uploadFile(ctx context.Context, key, fileType, filename string, r io.Reader) (string, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("apiKey", key)
	_ = mw.WriteField("fileType", fileType)
	part, err := mw.CreateFormFile("file", filename)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(part, r); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/task/openapi/upload", &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	res, err := s.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	env, err := s.decodeEnvelope(res)
	if err != nil {
		return "", err
	}
	var data struct {
		FileName string `json:"fileName"`
	}
	_ = json.Unmarshal(env.Data, &data)
	if data.FileName == "" {
		return "", errors.New("RunningHub upload returned no fileName")
	}
	return data.FileName, nil
}

// rhOutputs is the parsed /task/openapi/outputs payload.
type rhOutputs struct {
	Files []RunningHubOutputFile
	// Coins is consumeCoins (consumer keys); Money is consumeMoney in yuan (enterprise keys).
	Coins, Money *float64
	Raw          json.RawMessage
}

type RunningHubOutputFile struct {
	URL      string `json:"url"`
	FileType string `json:"file_type"`
	NodeID   string `json:"node_id,omitempty"`
}

func parseRHOutputs(data json.RawMessage) rhOutputs {
	out := rhOutputs{Raw: data}
	var items []map[string]any
	if err := json.Unmarshal(data, &items); err != nil {
		return out
	}
	for _, it := range items {
		if u, _ := it["fileUrl"].(string); u != "" {
			ft, _ := it["fileType"].(string)
			nid, _ := it["nodeId"].(string)
			out.Files = append(out.Files, RunningHubOutputFile{URL: u, FileType: ft, NodeID: nid})
		}
		// Each output item repeats the task-level cost, so take the max rather than summing.
		out.Coins = maxPtr(out.Coins, rhNumber(it["consumeCoins"]))
		out.Money = maxPtr(out.Money, rhNumber(it["consumeMoney"]))
	}
	return out
}

func rhNumber(v any) *float64 {
	switch x := v.(type) {
	case float64:
		return &x
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
			return &f
		}
	}
	return nil
}

func maxPtr(a, b *float64) *float64 {
	if a == nil {
		return b
	}
	if b == nil || *a >= *b {
		return a
	}
	return b
}
