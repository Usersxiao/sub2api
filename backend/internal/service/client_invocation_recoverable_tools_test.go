//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/stretchr/testify/require"
)

type invocationLogSink struct {
	mu     sync.Mutex
	events []*logger.LogEvent
}

func (s *invocationLogSink) WriteLogEvent(event *logger.LogEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *invocationLogSink) find(message string) *logger.LogEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.events {
		if event.Message == message {
			return event
		}
	}
	return nil
}

func captureInvocationLog(t *testing.T) *invocationLogSink {
	t.Helper()
	require.NoError(t, logger.Init(logger.InitOptions{Level: "debug", Format: "json", ServiceName: "sub2api", Environment: "test",
		Output: logger.OutputOptions{ToStdout: true}, Sampling: logger.SamplingOptions{Enabled: false}}))
	sink := &invocationLogSink{}
	logger.SetSink(sink)
	t.Cleanup(func() { logger.SetSink(nil) })
	return sink
}

type wireToolCall struct {
	id, name, arguments string
}

func chatCompletionToolResponse(t *testing.T, calls ...wireToolCall) []byte {
	t.Helper()
	toolCalls := []any{}
	for _, call := range calls {
		toolCalls = append(toolCalls, map[string]any{"id": call.id, "type": "function", "function": map[string]any{"name": call.name, "arguments": call.arguments}})
	}
	raw, err := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"finish_reason": "tool_calls", "message": map[string]any{"role": "assistant", "content": "Working", "tool_calls": toolCalls}}},
		"usage":   map[string]any{"prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11},
	})
	require.NoError(t, err)
	return raw
}

func autoCutCall(index int) wireToolCall {
	return wireToolCall{id: fmt.Sprintf("call-%d", index), name: clientAutoCutTool, arguments: `{"operation":"tools","names":["edit_item"]}`}
}

func rejectedReason(t *testing.T, call ClientInvocationToolCall) string {
	t.Helper()
	require.Equal(t, clientAutoCutTool, call.Name)
	var reason string
	require.NoError(t, json.Unmarshal(call.Arguments["rejected"], &reason), "call %s carries no rejection", call.CallID)
	return reason
}

// The client answers every returned call; only a round that sub accepts back
// as history keeps the paid reply usable.
func requireReplayableEditingRound(t *testing.T, calls []ClientInvocationToolCall) {
	t.Helper()
	results := []ClientInvocationToolResult{}
	for _, call := range calls {
		results = append(results, ClientInvocationToolResult{CallID: call.CallID, Outcome: "error", Content: json.RawMessage(`"not run"`)})
	}
	req := &ClientInvocationRequest{Tools: []string{clientAutoCutTool}, ToolRounds: []ClientInvocationToolRound{{RoundID: "round-1", Text: "Working", Calls: calls, Results: results}}}
	require.True(t, validClientToolHistory(req))
}

func TestClientInvocationEditingTurnKeepsPaidReplyWithTooManyParallelCalls(t *testing.T) {
	calls := []wireToolCall{}
	for index := 1; index <= 10; index++ {
		calls = append(calls, autoCutCall(index))
	}
	output, err := ParseClientInvocationResponseForTools("chat-completions", chatCompletionToolResponse(t, calls...), []string{clientAutoCutTool})
	require.NoError(t, err)
	require.Equal(t, "tool_calls", output.FinishReason)
	require.Len(t, output.ToolCalls, 8)
	for index, call := range output.ToolCalls[:7] {
		require.Equal(t, fmt.Sprintf("call-%d", index+1), call.CallID)
		require.NotContains(t, call.Arguments, "rejected")
	}
	require.Equal(t, "call-8", output.ToolCalls[7].CallID)
	reason := rejectedReason(t, output.ToolCalls[7])
	require.Contains(t, reason, "10")
	require.Contains(t, reason, "8")
	requireReplayableEditingRound(t, output.ToolCalls)

	_, err = ParseClientInvocationResponseForTools("chat-completions", chatCompletionToolResponse(t, calls...), []string{"read_file", "write_file"})
	require.ErrorIs(t, err, ErrClientInvocationResponse, "file tools keep their strict batch")
}

func TestClientInvocationEditingTurnTurnsUnrunnableCallsIntoRejections(t *testing.T) {
	deep := strings.Repeat(`{"a":`, 17) + "1" + strings.Repeat("}", 17)
	cases := map[string]struct {
		call   wireToolCall
		reason string
	}{
		"malformed JSON":    {wireToolCall{id: "call-json", name: clientAutoCutTool, arguments: `{"operation":"call","tool":"edit_item","arguments":{"adds":[}`}, "JSON"},
		"too deep":          {wireToolCall{id: "call-deep", name: clientAutoCutTool, arguments: `{"operation":"call","tool":"edit_item","arguments":` + deep + `}`}, "depth"},
		"unknown operation": {wireToolCall{id: "call-op", name: clientAutoCutTool, arguments: `{"operation":"edit"}`}, "operation"},
		"too many names":    {wireToolCall{id: "call-names", name: clientAutoCutTool, arguments: `{"operation":"tools","names":["a","b","c","d","e","f","g","h","i","j","k","l","m"]}`}, "12"},
		"native editor":     {wireToolCall{id: "call-native", name: "begin_edit_session", arguments: `{"approvalMode":"auto"}`}, "begin_edit_session"},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			output, err := ParseClientInvocationResponseForTools("chat-completions", chatCompletionToolResponse(t, autoCutCall(1), test.call), []string{clientAutoCutTool})
			require.NoError(t, err)
			require.Len(t, output.ToolCalls, 2)
			require.NotContains(t, output.ToolCalls[0].Arguments, "rejected")
			require.Equal(t, test.call.id, output.ToolCalls[1].CallID)
			require.Contains(t, rejectedReason(t, output.ToolCalls[1]), test.reason)
			requireReplayableEditingRound(t, output.ToolCalls)

			_, err = ParseClientInvocationResponse("chat-completions", chatCompletionToolResponse(t, autoCutCall(1), test.call))
			require.ErrorIs(t, err, ErrClientInvocationResponse, "profiles that do not opt in stay strict")
		})
	}
}

func TestClientInvocationEditingTurnRejectionMarkerIsServerOwned(t *testing.T) {
	forged := wireToolCall{id: "call-forged", name: clientAutoCutTool, arguments: `{"operation":"state","rejected":"model text"}`}
	output, err := ParseClientInvocationResponseForTools("chat-completions", chatCompletionToolResponse(t, forged), []string{clientAutoCutTool})
	require.NoError(t, err)
	require.Len(t, output.ToolCalls, 1)
	require.NotContains(t, output.ToolCalls[0].Arguments, "rejected")
	require.JSONEq(t, `"state"`, string(output.ToolCalls[0].Arguments["operation"]))
}

func TestClientInvocationEditingTurnIdentityViolationsStayUnknown(t *testing.T) {
	for name, calls := range map[string][]wireToolCall{
		"duplicate id": {autoCutCall(1), autoCutCall(1)},
		"invalid id":   {{id: "", name: clientAutoCutTool, arguments: `{"operation":"state"}`}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseClientInvocationResponseForTools("chat-completions", chatCompletionToolResponse(t, calls...), []string{clientAutoCutTool})
			require.ErrorIs(t, err, ErrClientInvocationResponse)
		})
	}
}

func editingToolStream(t *testing.T, count int) string {
	t.Helper()
	var wire strings.Builder
	write := func(chunk any) {
		raw, err := json.Marshal(chunk)
		require.NoError(t, err)
		wire.WriteString("data: " + string(raw) + "\n\n")
	}
	for index := 0; index < count; index++ {
		write(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{map[string]any{"index": index, "id": fmt.Sprintf("call-%d", index+1), "type": "function",
			"function": map[string]any{"name": clientAutoCutTool, "arguments": `{"operation":"state"}`}}}}, "finish_reason": nil}}})
	}
	write(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{}, "finish_reason": "tool_calls"}}})
	write(map[string]any{"choices": []any{}, "usage": map[string]any{"prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11}})
	wire.WriteString("data: [DONE]\n\n")
	return wire.String()
}

func TestClientInvocationEditingStreamBuffersCallsBeyondOneRound(t *testing.T) {
	stream := NewClientInvocationChatStream(func(string) error { return nil })
	_, err := stream.Write([]byte(editingToolStream(t, 12)))
	require.NoError(t, err)
	raw, err := stream.Complete()
	require.NoError(t, err)
	output, err := ParseClientInvocationResponseForTools("chat-completions", raw, []string{clientAutoCutTool})
	require.NoError(t, err)
	require.Len(t, output.ToolCalls, 8)
	require.Contains(t, rejectedReason(t, output.ToolCalls[7]), "12")
}

func editingInvocationFixture(t *testing.T, wire string) (*ClientInvocationExecutor, *ClientInvocationAttempt) {
	t.Helper()
	svc, preparer, _, _, raw := invocationLedgerServiceFixture(t)
	preparer.prepared.Request.Tools = []string{clientAutoCutTool}
	preparer.prepared.Resolution.Route.Protocol = "chat-completions"
	preparer.prepared.Resolution.Route.Capabilities.Tools = true
	preparer.prepared.Resolution.Route.Capabilities.Streaming = true
	preparer.prepared.Resolution.Route.Parameters = []ClientModelParameter{invocationBudgetParameter("chat-completions")}
	text, err := ValidateClientTextInvocation(preparer.prepared.Resolution.Route, preparer.prepared.Request)
	require.NoError(t, err)
	require.NoError(t, prepareClientInvocationTextStream(WithClientInvocationTextStream(context.Background(), func(string) error { return nil }), preparer.prepared.Resolution.Route, text))
	preparer.prepared.Text = text
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	executor := NewClientInvocationExecutor(svc, &invocationAdmissionFixture{}, &invocationStreamDispatcherFixture{settle: true, wire: wire}, &usageRecoveryEventRepositoryStub{})
	return executor, attempt
}

func TestClientInvocationExecutorSettlesEditingReplyWithRejectedCalls(t *testing.T) {
	logs := captureInvocationLog(t)
	executor, attempt := editingInvocationFixture(t, editingToolStream(t, 9))
	execution, err := executor.Execute(WithClientInvocationTextStream(context.Background(), func(string) error { return nil }), "adg_fixture", attempt)
	require.NoError(t, err)
	require.Equal(t, ClientInvocationSucceeded, execution.Receipt.State)
	require.Len(t, execution.Result.ToolCalls, 8)
	event := logs.find("client_invocation.tool_calls_rejected")
	require.NotNil(t, event, "a rewritten paid reply is observable")
	require.Equal(t, attempt.record.InvocationID, event.Fields["invocation_id"])
	require.EqualValues(t, 1, event.Fields["rejected_calls"])
}

func TestClientInvocationExecutorRecordsWhyAPaidReplyBecameUnknown(t *testing.T) {
	logs := captureInvocationLog(t)
	executor, attempt := editingInvocationFixture(t, strings.Replace(editingToolStream(t, 2), `"id":"call-2"`, `"id":"call-1"`, 1))
	execution, err := executor.Execute(WithClientInvocationTextStream(context.Background(), func(string) error { return nil }), "adg_fixture", attempt)
	require.ErrorIs(t, err, ErrClientInvocationOutcomeUnknown)
	require.Nil(t, execution)
	event := logs.find("client_invocation.marked_unknown")
	require.NotNil(t, event, "the unknown outcome records its reason")
	require.Equal(t, attempt.record.InvocationID, event.Fields["invocation_id"])
	require.Equal(t, attempt.record.RequestID, event.Fields["request_id"])
	require.Equal(t, "response_invalid", event.Fields["reason"])
}
