package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRHOutputs_EnterpriseMoney(t *testing.T) {
	// Shape captured from a real enterprise-key run.
	raw := json.RawMessage(`[{"fileUrl":"https://x/a.mp4","fileType":"mp4","taskCostTime":"1452","nodeId":"131",
		"thirdPartyConsumeMoney":null,"consumeMoney":"2.421","consumeCoins":null}]`)
	out := parseRHOutputs(raw)
	require.Len(t, out.Files, 1)
	require.Equal(t, "https://x/a.mp4", out.Files[0].URL)
	require.Equal(t, "131", out.Files[0].NodeID)
	require.Nil(t, out.Coins)
	require.NotNil(t, out.Money)
	require.InDelta(t, 2.421, *out.Money, 1e-9)
}

func TestParseRHOutputs_MultiFileTakesMaxNotSum(t *testing.T) {
	raw := json.RawMessage(`[{"fileUrl":"a","consumeCoins":"120"},{"fileUrl":"b","consumeCoins":120}]`)
	out := parseRHOutputs(raw)
	require.Len(t, out.Files, 2)
	require.InDelta(t, 120, *out.Coins, 1e-9)
}

func TestMergeRHNodes(t *testing.T) {
	base := []rhNode{
		{"nodeId": "12", "fieldName": "image", "fieldValue": "default.jpg", "fieldType": "IMAGE"},
		{"nodeId": "20", "fieldName": "value", "fieldValue": "1280"},
	}
	out, err := mergeRHNodes(base, []RunningHubNodeOverride{{NodeID: "20", FieldName: "value", FieldValue: "832"}})
	require.NoError(t, err)
	require.Equal(t, "832", out[1]["fieldValue"])
	require.Equal(t, "IMAGE", out[0]["fieldType"])
	require.Equal(t, "1280", base[1]["fieldValue"], "base must not be mutated")

	_, err = mergeRHNodes(base, []RunningHubNodeOverride{{NodeID: "99", FieldName: "x", FieldValue: 1}})
	require.Error(t, err)
}

func TestUpstreamPost_QueueMaxedIsSentinel(t *testing.T) {
	srv := stubRH(func(r *http.Request) string {
		return `{"code":421,"msg":"TASK_QUEUE_MAXED","data":null}`
	})
	s := &RunningHubService{baseURL: "http://rh.test", httpClient: srv}
	_, err := s.upstreamPost(context.Background(), "/task/openapi/ai-app/run", "k", map[string]any{})
	require.ErrorIs(t, err, errRHQueueMaxed)
}

func TestUpstreamPost_BusinessError(t *testing.T) {
	srv := stubRH(func(r *http.Request) string {
		return `{"code":803,"msg":"APIKEY_INVALID","data":null}`
	})
	s := &RunningHubService{baseURL: "http://rh.test", httpClient: srv}
	_, err := s.upstreamPost(context.Background(), "/x", "k", nil)
	var ue *RunningHubUpstreamError
	require.True(t, errors.As(err, &ue))
	require.Equal(t, 803, ue.Code)
}

func TestFetchAppNodes(t *testing.T) {
	srv := stubRH(func(r *http.Request) string {
		require.Equal(t, "/api/webapp/apiCallDemo", r.URL.Path)
		require.Equal(t, "app1", r.URL.Query().Get("webappId"))
		return `{"code":0,"msg":"success","data":{"nodeInfoList":[{"nodeId":"1","fieldName":"image","fieldValue":"a.png"}]}}`
	})
	s := &RunningHubService{baseURL: "http://rh.test", httpClient: srv}
	nodes, err := s.fetchAppNodes(context.Background(), "k", "app1")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "a.png", nodes[0]["fieldValue"])
}

func TestRHFileType(t *testing.T) {
	require.Equal(t, "image", rhFileType("a.JPG", ""))
	require.Equal(t, "audio", rhFileType("x", "audio/mpeg"))
	require.Equal(t, "video", rhFileType("clip.mp4", ""))
}

type rhStubTransport func(*http.Request) string

func (f rhStubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(f(r))), Header: http.Header{}}, nil
}

// stubRH returns a client answering every request with body(r), so tests
// run without binding a local listener.
func stubRH(body func(*http.Request) string) *http.Client {
	return &http.Client{Transport: rhStubTransport(body)}
}

func TestFetchAppDetail(t *testing.T) {
	srv := stubRH(func(r *http.Request) string {
		require.Equal(t, "/api/webapp/detail", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		return `{"code":0,"msg":"success","data":{"name":"InfiniteTalk","instanceType":"plus","avgRunningSeconds":1403,"runningSuccessRate":94}}`
	})
	s := &RunningHubService{baseURL: "http://rh.test", httpClient: srv}
	d, err := s.fetchAppDetail(context.Background(), "k", "app1")
	require.NoError(t, err)
	require.Equal(t, "InfiniteTalk", d.Name)
	require.Equal(t, 1403, d.AvgRunningSeconds)
	require.Equal(t, 94, d.RunningSuccessRate)
}

func TestRHEstimateCoins(t *testing.T) {
	// Matches the official 计费说明 rate card.
	require.InDelta(t, 1.5, rhEstimateCoins(75, "lite"), 1e-9)
	require.InDelta(t, 15, rhEstimateCoins(75, "standard"), 1e-9)
	require.InDelta(t, 30, rhEstimateCoins(75, "plus"), 1e-9)
	require.InDelta(t, 45, rhEstimateCoins(75, "ultra"), 1e-9)
	require.InDelta(t, 561.2, rhEstimateCoins(1403, "Plus"), 1e-9, "tier match is case-insensitive")
	// Unknown or empty tier falls back to standard, matching RunningHub's default.
	require.InDelta(t, 15, rhEstimateCoins(75, ""), 1e-9)
	require.InDelta(t, 15, rhEstimateCoins(75, "mystery"), 1e-9)
	// Unknown runtime yields no estimate so the caller can fall back.
	require.Zero(t, rhEstimateCoins(0, "plus"))
}
