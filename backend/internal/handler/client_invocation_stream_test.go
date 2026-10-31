//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type invocationStreamRunnerFixture struct {
	invocationRunnerFixture
	reasoning    []string
	chunks       []string
	beforeReturn func()
}

func (f *invocationStreamRunnerFixture) SubmitStream(ctx context.Context, token, routeID string, raw []byte, sink service.ClientInvocationTextSink) (*service.ClientInvocationExecution, error) {
	f.submits++
	f.token, f.routeID, f.raw = token, routeID, string(raw)
	_, f.ingress = ctx.Value(clientInvocationIngressRequestKey{}).(clientInvocationIngressMetadata)
	for _, chunk := range f.reasoning {
		if err := service.ClientInvocationReasoningStream(ctx)(chunk); err != nil {
			return nil, err
		}
	}
	for _, chunk := range f.chunks {
		if err := sink(chunk); err != nil {
			return nil, err
		}
	}
	if f.beforeReturn != nil {
		f.beforeReturn()
	}
	return f.execution, f.err
}

const invocationStreamRequest = `{"projectId":"project-a","routeId":"deepseek-flash","operation":"text","configVersion":"cfg-1","entitlementVersion":"ent-1","requestId":"request-a","input":[{"kind":"text","text":"ping"}],"parameters":{}}`

func invokeSSEFixture(runner *invocationStreamRunnerFixture, rec *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/client/invoke/:route_id", newClientInvocationHandlerWithRunner(&invocationGrantValidatorFixture{}, runner).Invoke)
	request := httptest.NewRequest(http.MethodPost, "/client/invoke/deepseek-flash", strings.NewReader(invocationStreamRequest))
	request.Header.Set("Authorization", "Bearer adg_fixture")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	router.ServeHTTP(rec, request)
}

func readInvocationSSEFixture(t *testing.T, raw string) []clientInvocationStreamEventDTO {
	t.Helper()
	var events []clientInvocationStreamEventDTO
	for _, line := range strings.Split(raw, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event clientInvocationStreamEventDTO
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event))
		events = append(events, event)
	}
	return events
}

func TestClientInvocationStreamHandlerFlushesTextBeforeSettledTerminal(t *testing.T) {
	rec := httptest.NewRecorder()
	runner := &invocationStreamRunnerFixture{
		invocationRunnerFixture: invocationRunnerFixture{execution: &service.ClientInvocationExecution{
			Receipt: invocationReceiptFixture(service.ClientInvocationSucceeded),
			Result:  &service.ClientInvocationResponse{Text: "你好", Usage: service.ClientInvocationUsage{InputTokens: 9, OutputTokens: 2, TotalTokens: 11}},
		}}, chunks: []string{"你", "好"},
	}
	runner.beforeReturn = func() {
		require.True(t, rec.Flushed, "text must flush while execution is still in progress")
		require.Len(t, readInvocationSSEFixture(t, rec.Body.String()), 2)
		require.NotContains(t, rec.Body.String(), `"type":"terminal"`)
	}
	invokeSSEFixture(runner, rec)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.Equal(t, "no", rec.Header().Get("X-Accel-Buffering"))
	events := readInvocationSSEFixture(t, rec.Body.String())
	require.Len(t, events, 3)
	for i, event := range events {
		require.Equal(t, 1, event.SchemaVersion)
		require.Equal(t, i+1, event.Sequence)
		require.Equal(t, "project-a", event.ProjectID)
		require.Equal(t, "request-a", event.RequestID)
		require.Equal(t, "deepseek-flash", event.RouteID)
	}
	require.Equal(t, "text_delta", events[0].Type)
	require.Equal(t, "你", events[0].Delta)
	require.Equal(t, "好", events[1].Delta)
	require.Equal(t, "terminal", events[2].Type)
	require.Equal(t, "succeeded", events[2].Receipt.State)
	require.Equal(t, "你好", events[2].Output.Text)
	require.Equal(t, int64(11), events[2].Output.Usage.TotalTokens)
	require.Nil(t, events[2].Error)
	require.True(t, runner.ingress)
	require.NotContains(t, rec.Body.String(), "adg_fixture")
}

func TestClientInvocationStreamHandlerDoesNotPromotePartialOrReplayOutput(t *testing.T) {
	for _, test := range []struct {
		name      string
		chunks    []string
		err       error
		execution *service.ClientInvocationExecution
		wantCode  int
		wantType  string
	}{
		{name: "admission JSON", err: service.ErrClientInvocationAdmissionUnavailable, wantCode: 503},
		{name: "partial failure", chunks: []string{"partial"}, err: errors.New("PRIVATE_UPSTREAM_SECRET"), wantCode: 200, wantType: "terminal"},
		{name: "replay", execution: &service.ClientInvocationExecution{Receipt: invocationReceiptFixture(service.ClientInvocationUnknown)}, wantCode: 200, wantType: "terminal"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			runner := &invocationStreamRunnerFixture{invocationRunnerFixture: invocationRunnerFixture{execution: test.execution, err: test.err}, chunks: test.chunks}
			invokeSSEFixture(runner, rec)
			require.Equal(t, test.wantCode, rec.Code)
			require.NotContains(t, rec.Body.String(), "PRIVATE_")
			events := readInvocationSSEFixture(t, rec.Body.String())
			if test.wantType == "" {
				require.Empty(t, events)
				require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
				return
			}
			terminal := events[len(events)-1]
			require.Equal(t, "terminal", terminal.Type)
			require.Nil(t, terminal.Output)
			if test.err != nil {
				require.Nil(t, terminal.Receipt)
				require.Equal(t, "MODEL_REQUEST_OUTCOME_UNKNOWN", terminal.Error.Code)
			} else {
				require.Equal(t, "unknown", terminal.Receipt.State)
				require.Nil(t, terminal.Error)
				require.Len(t, events, 1)
			}
		})
	}
}

func TestClientInvocationStreamNegotiationKeepsJSONAndZeroQuality(t *testing.T) {
	require.True(t, clientInvocationWantsStream("application/json, text/event-stream"))
	require.False(t, clientInvocationWantsStream("text/event-stream;q=0"))
	require.False(t, clientInvocationWantsStream("application/json"))
	require.False(t, clientInvocationWantsStream(""))
}

type clientInvocationStreamingProtocolFixture struct {
	clientInvocationProtocolHandlerFixture
	inspect func(*gin.Context)
}

func (h *clientInvocationStreamingProtocolFixture) ChatCompletions(c *gin.Context) {
	h.calls++
	if h.inspect != nil {
		h.inspect(c)
	}
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	for _, frame := range []string{
		`data: {"choices":[{"index":0,"delta":{"content":"live"},"finish_reason":null}]}` + "\n\n",
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}` + "\n\n",
		"data: [DONE]\n\n",
	} {
		_, _ = c.Writer.WriteString(frame)
		c.Writer.Flush()
	}
}

func TestClientInvocationStreamGatewayUsesExistingHandlerAndBoundSettlementContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(5)
	protocol := &clientInvocationStreamingProtocolFixture{}
	adapter := &ClientInvocationGatewayAdapter{openAI: protocol}
	prepared := &service.PreparedClientInvocation{
		Text:            &service.ValidatedClientTextInvocation{Protocol: "chat-completions", Payload: []byte(`{"model":"fixture-model","stream":true,"stream_options":{"include_usage":true}}`)},
		Resolution:      &service.ClientInvocationResolution{GroupID: groupID, ModelID: "fixture-model", Platform: service.PlatformDeepseek},
		BillingIdentity: &service.APIKey{ID: 23, Kind: service.APIKeyKindClientConsumer, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformDeepseek}},
	}
	var delivered string
	ctx, _ := service.WithRequiredUsageSettlement(context.Background(), "client:invocation-abc", 23)
	ctx = service.WithClientInvocationTextStream(ctx, func(delta string) error { delivered += delta; return nil })
	recorder := newClientInvocationResponseWriter()
	sink, _ := service.ClientInvocationTextStream(ctx)
	recorder.stream = service.NewClientInvocationChatStream(sink)
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(prepared.Text.Payload))).WithContext(ctx)
	c.Set(clientInvocationUserSlotContextKey, prepared.BillingIdentity.ID)
	lease := newClientInvocationSelectionLease("chat-completions", service.PlatformDeepseek, groupID, "fixture-model", &service.AccountSelectionResult{Account: &service.Account{ID: 42}})
	state := &clientInvocationGatewayState{prepared: prepared, context: c, response: recorder, selection: lease}
	dispatchContext := context.WithValue(ctx, clientInvocationGatewayStateKey{}, state)
	protocol.inspect = func(c *gin.Context) {
		require.True(t, service.RequiredUsageSettlement(c.Request.Context()))
		require.Empty(t, delivered)
	}
	raw, err := adapter.DispatchClientInvocation(dispatchContext, prepared)
	require.NoError(t, err)
	require.Equal(t, 1, protocol.calls)
	require.Equal(t, "live", delivered)
	result, err := service.ParseClientInvocationResponse("chat-completions", raw)
	require.NoError(t, err)
	require.Equal(t, "live", result.Text)
	require.Equal(t, int64(4), result.Usage.TotalTokens)
	require.Zero(t, recorder.body.Len(), "the gateway does not buffer the raw upstream SSE")
}

func TestClientInvocationStreamRejectsSearchBeforeSubmission(t *testing.T) {
	runner := &invocationStreamRunnerFixture{}
	router := gin.New()
	router.POST("/client/invoke/:route_id", newClientInvocationHandlerWithRunner(&invocationGrantValidatorFixture{}, runner).Invoke)
	req := httptest.NewRequest(http.MethodPost, "/client/invoke/writing", strings.NewReader(`{"projectId":"project-a","requestId":"request-a","routeId":"writing","operation":"search","configVersion":"cfg-1","entitlementVersion":"ent-1","query":"search","source":"web","maxResults":2}`))
	req.Header.Set("Authorization", "Bearer adg_fixture")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)
	require.Equal(t, http.StatusBadRequest, res.Code)
	require.Zero(t, runner.submits)
	require.NotContains(t, res.Header().Get("Content-Type"), "event-stream")
}

func TestClientInvocationStreamHandlerPreservesThinkingAndLength(t *testing.T) {
	rec := httptest.NewRecorder()
	runner := &invocationStreamRunnerFixture{
		invocationRunnerFixture: invocationRunnerFixture{execution: &service.ClientInvocationExecution{
			Receipt: invocationReceiptFixture(service.ClientInvocationSucceeded),
			Result:  &service.ClientInvocationResponse{Text: "partial", Reasoning: "先规划", FinishReason: "length", Usage: service.ClientInvocationUsage{InputTokens: 9, OutputTokens: 2, TotalTokens: 11}},
		}}, reasoning: []string{"先", "规划"}, chunks: []string{"partial"},
	}
	invokeSSEFixture(runner, rec)
	events := readInvocationSSEFixture(t, rec.Body.String())
	require.Len(t, events, 4)
	require.Equal(t, "reasoning_delta", events[0].Type)
	require.Equal(t, "先", events[0].Delta)
	require.Equal(t, "reasoning_delta", events[1].Type)
	require.Equal(t, "text_delta", events[2].Type)
	require.Equal(t, "length", events[3].Output.FinishReason)
	require.Equal(t, "先规划", events[3].Output.Reasoning)
	require.Equal(t, "succeeded", events[3].Receipt.State)
	for i, event := range events {
		require.Equal(t, i+1, event.Sequence)
		require.Equal(t, "request-a", event.RequestID)
	}
}

func withClientInvocationKeepAlive(t *testing.T, interval time.Duration) {
	t.Helper()
	previous := clientInvocationStreamKeepAlive
	clientInvocationStreamKeepAlive = interval
	t.Cleanup(func() { clientInvocationStreamKeepAlive = previous })
}

// A turn that writes a file buffers its tool arguments until the terminal, so
// the stream must keep proving it is alive while the provider still generates.
func TestClientInvocationStreamHandlerKeepsStartedStreamAliveWhileToolsBuffer(t *testing.T) {
	withClientInvocationKeepAlive(t, 5*time.Millisecond)
	rec := httptest.NewRecorder()
	runner := &invocationStreamRunnerFixture{
		invocationRunnerFixture: invocationRunnerFixture{execution: &service.ClientInvocationExecution{
			Receipt: invocationReceiptFixture(service.ClientInvocationSucceeded),
			Result:  &service.ClientInvocationResponse{Reasoning: "plan", Usage: service.ClientInvocationUsage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}},
		}}, reasoning: []string{"plan"},
	}
	runner.beforeReturn = func() { time.Sleep(60 * time.Millisecond) }
	invokeSSEFixture(runner, rec)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, ": keepalive\n\n")
	require.Less(t, strings.Index(body, `"type":"reasoning_delta"`), strings.Index(body, ": keepalive"))
	require.Less(t, strings.LastIndex(body, ": keepalive"), strings.Index(body, `"type":"terminal"`))
	events := readInvocationSSEFixture(t, body)
	require.Len(t, events, 2)
	require.Equal(t, 2, events[1].Sequence)
}

// Before the first event the response may still be a JSON error, so silence
// there must not commit the stream.
func TestClientInvocationStreamHandlerKeepAliveNeverStartsTheStream(t *testing.T) {
	withClientInvocationKeepAlive(t, 5*time.Millisecond)
	rec := httptest.NewRecorder()
	runner := &invocationStreamRunnerFixture{invocationRunnerFixture: invocationRunnerFixture{err: service.ErrClientInvocationAdmissionUnavailable}}
	runner.beforeReturn = func() { time.Sleep(40 * time.Millisecond) }
	invokeSSEFixture(runner, rec)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.NotContains(t, rec.Body.String(), "keepalive")
}
