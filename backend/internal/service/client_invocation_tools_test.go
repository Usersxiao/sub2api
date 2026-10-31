//go:build unit

package service

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"strings"
	"testing"
)

func clientFileToolRequest(t *testing.T) (ClientModelRoute, *ClientInvocationRequest) {
	t.Helper()
	route := invocationRouteFixture(t)
	route.Protocol = "chat-completions"
	route.Capabilities.Tools = true
	route.Parameters = []ClientModelParameter{invocationBudgetParameter("chat-completions")}
	req := invocationRequestFixture()
	req.Tools = []string{"read_file", "write_file"}
	req.ToolRounds = []ClientInvocationToolRound{{RoundID: "request-first", Text: "Writing", Calls: []ClientInvocationToolCall{{CallID: "call-1", Name: "write_file", Arguments: map[string]json.RawMessage{"path": json.RawMessage(`"index.html"`), "content": json.RawMessage(`"<h1>Hello</h1>"`)}}}, Results: []ClientInvocationToolResult{{CallID: "call-1", Outcome: "success", Content: json.RawMessage(`{"path":"index.html"}`)}}}}
	return route, req
}
func TestClientInvocationFileToolHistoryAndPublication(t *testing.T) {
	route, req := clientFileToolRequest(t)
	raw, err := json.Marshal(req)
	require.NoError(t, err)
	parsed, err := ParseClientInvocation(raw)
	require.NoError(t, err)
	validated, err := ValidateClientTextInvocation(route, parsed)
	require.NoError(t, err)
	require.Equal(t, "write_file", gjson.GetBytes(validated.Payload, "tools.1.function.name").String())
	require.Equal(t, "assistant", gjson.GetBytes(validated.Payload, `messages.#(role=="assistant").role`).String())
	require.Equal(t, "call-1", gjson.GetBytes(validated.Payload, `messages.#(role=="assistant").tool_calls.0.id`).String())
	require.Equal(t, "tool", gjson.GetBytes(validated.Payload, `messages.#(role=="tool").role`).String())
	require.Equal(t, "call-1", gjson.GetBytes(validated.Payload, `messages.#(role=="tool").tool_call_id`).String())
	require.Equal(t, `{"path":"index.html"}`, gjson.GetBytes(validated.Payload, `messages.#(role=="tool").content`).String())
	route.Capabilities.Tools = false
	_, err = ValidateClientTextInvocation(route, req)
	require.ErrorIs(t, err, ErrClientInvocationParameters)
	route.Capabilities.Tools = true
	route.Protocol = "responses"
	_, err = ValidateClientTextInvocation(route, req)
	require.ErrorIs(t, err, ErrClientInvocationParameters)
	for name, candidate := range map[string]string{
		"unmatched result": strings.Replace(string(raw), `"results":[{"callId":"call-1"`, `"results":[{"callId":"other"`, 1),
		"duplicate call":   strings.Replace(string(raw), `"tools":["read_file","write_file"]`, `"tools":["write_file","write_file"]`, 1),
		"host tool":        strings.Replace(string(raw), `"name":"write_file"`, `"name":"terminal"`, 1),
		"null text":        strings.Replace(string(raw), `"text":"Writing"`, `"text":null`, 1),
		"extra field":      strings.Replace(string(raw), `"roundId":`, `"token":"PRIVATE","roundId":`, 1),
		"null tools":       strings.Replace(string(raw), `"tools":["read_file","write_file"]`, `"tools":null`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseClientInvocation([]byte(candidate))
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
}
func clientToolStream(t *testing.T, arguments string) string {
	t.Helper()
	chunks := []any{
		map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": "call-1", "type": "function", "function": map[string]any{"name": "write_file", "arguments": arguments[:len(arguments)/2]}}}}, "finish_reason": nil}}},
		map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{map[string]any{"index": 0, "function": map[string]any{"arguments": arguments[len(arguments)/2:]}}}}, "finish_reason": nil}}},
		map[string]any{"choices": []any{map[string]any{"delta": map[string]any{}, "finish_reason": "tool_calls"}}},
		map[string]any{"choices": []any{}, "usage": map[string]any{"prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11}},
	}
	var wire strings.Builder
	for _, chunk := range chunks {
		raw, err := json.Marshal(chunk)
		require.NoError(t, err)
		wire.WriteString("data: " + string(raw) + "\n\n")
	}
	wire.WriteString("data: [DONE]\n\n")
	return wire.String()
}
func TestClientInvocationToolStreamBuffersArgumentsUntilComplete(t *testing.T) {
	stream := NewClientInvocationChatStream(func(string) error { t.Fatal("tool arguments exposed as text"); return nil })
	for _, b := range []byte(clientToolStream(t, `{"path":"index.html","content":"<h1>你好</h1>"}`)) {
		_, err := stream.Write([]byte{b})
		require.NoError(t, err)
	}
	raw, err := stream.Complete()
	require.NoError(t, err)
	output, err := ParseClientInvocationResponse("chat-completions", raw)
	require.NoError(t, err)
	require.Len(t, output.ToolCalls, 1)
	require.Equal(t, "call-1", output.ToolCalls[0].CallID)
	require.JSONEq(t, `"<h1>你好</h1>"`, string(output.ToolCalls[0].Arguments["content"]))
	for _, arguments := range []string{`{"path":"index.html"`, `{"path":"index.html","content":"x","content":"y"}`, `{"path":"index.html","content":"x","project":"other"}`} {
		broken := NewClientInvocationChatStream(func(string) error { return nil })
		_, err := broken.Write([]byte(clientToolStream(t, arguments)))
		require.NoError(t, err)
		raw, err := broken.Complete()
		require.NoError(t, err)
		_, err = ParseClientInvocationResponse("chat-completions", raw)
		require.ErrorIs(t, err, ErrClientInvocationResponse)
	}
}
func TestClientInvocationToolsStillRequireSameRequestSettlement(t *testing.T) {
	for _, settle := range []bool{false, true} {
		svc, preparer, _, _, raw := invocationLedgerServiceFixture(t)
		preparer.prepared.Request.Tools = []string{"write_file"}
		preparer.prepared.Resolution.Route.Protocol = "chat-completions"
		preparer.prepared.Resolution.Route.Capabilities.Tools = true
		preparer.prepared.Resolution.Route.Capabilities.Streaming = true
		preparer.prepared.Resolution.Route.Parameters = []ClientModelParameter{invocationBudgetParameter("chat-completions")}
		text, err := ValidateClientTextInvocation(preparer.prepared.Resolution.Route, preparer.prepared.Request)
		require.NoError(t, err)
		require.NoError(t, prepareClientInvocationTextStream(WithClientInvocationTextStream(context.Background(), func(string) error { return nil }), preparer.prepared.Resolution.Route, text))
		preparer.prepared.Text = text
		dispatcher := &invocationStreamDispatcherFixture{settle: settle, wire: clientToolStream(t, `{"path":"index.html","content":"Hello"}`)}
		attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
		require.NoError(t, err)
		executor := NewClientInvocationExecutor(svc, &invocationAdmissionFixture{}, dispatcher, &usageRecoveryEventRepositoryStub{})
		execution, err := executor.Execute(WithClientInvocationTextStream(context.Background(), func(string) error { return nil }), "adg_fixture", attempt)
		if !settle {
			require.ErrorIs(t, err, ErrClientInvocationOutcomeUnknown)
			require.Nil(t, execution)
		} else {
			require.NoError(t, err)
			require.Len(t, execution.Result.ToolCalls, 1)
		}
	}
}

func TestClientInvocationDeepSeekToolReasoningRoundTrip(t *testing.T) {
	route, req := clientFileToolRequest(t)
	req.ToolRounds[0].Reasoning = "先规划\n完整写入 <页面>"
	for _, platform := range []string{PlatformDeepseek, PlatformOpenAI} {
		payload := map[string]any{"messages": []map[string]string{{"role": "user", "content": "Create the page"}}}
		require.NoError(t, applyClientFileTools(route, req, payload, platform))
		raw, err := json.Marshal(payload)
		require.NoError(t, err)
		require.Equal(t, "system", gjson.GetBytes(raw, "messages.0.role").String())
		require.Contains(t, gjson.GetBytes(raw, "messages.0.content").String(), "complete, usable files")
		require.Equal(t, "Create the page", gjson.GetBytes(raw, `messages.#(role=="user").content`).String())
		reasoning := gjson.GetBytes(raw, `messages.#(role=="assistant").reasoning_content`)
		if platform == PlatformDeepseek {
			require.Equal(t, req.ToolRounds[0].Reasoning, reasoning.String())
		} else {
			require.False(t, reasoning.Exists())
		}
	}
	raw, err := json.Marshal(req)
	require.NoError(t, err)
	parsed, err := ParseClientInvocation(raw)
	require.NoError(t, err)
	require.Equal(t, req.ToolRounds[0].Reasoning, parsed.ToolRounds[0].Reasoning)
	for _, value := range []string{`null`, `[]`, `42`} {
		var fields map[string]any
		require.NoError(t, json.Unmarshal(raw, &fields))
		var invalid any
		require.NoError(t, json.Unmarshal([]byte(value), &invalid))
		fields["toolRounds"].([]any)[0].(map[string]any)["reasoning"] = invalid
		bad, err := json.Marshal(fields)
		require.NoError(t, err)
		_, err = ParseClientInvocation(bad)
		require.ErrorIs(t, err, ErrClientInvocationParameters)
	}
}

func TestClientInvocationToolStreamTruncationDoesNotExecuteFragments(t *testing.T) {
	for _, finish := range []string{"length", "content_filter"} {
		wire := strings.Replace(clientToolStream(t, `{"path":"index.html","content":"unfinished`), `"finish_reason":"tool_calls"`, `"finish_reason":"`+finish+`"`, 1)
		stream := NewClientInvocationChatStream(func(string) error { t.Fatal("arguments exposed"); return nil })
		_, err := stream.Write([]byte(wire))
		require.NoError(t, err)
		raw, err := stream.Complete()
		require.NoError(t, err)
		output, err := ParseClientInvocationResponse("chat-completions", raw)
		require.NoError(t, err)
		require.Equal(t, finish, output.FinishReason)
		require.Empty(t, output.ToolCalls)
	}
}

func TestClientInvocationDeepSeekAbsentReasoningIsCanonicalBeforeProof(t *testing.T) {
	route, req := clientFileToolRequest(t)
	payload := map[string]any{"model": "deepseek-v4-flash", "thinking": map[string]string{"type": "enabled"}, "messages": []map[string]string{{"role": "user", "content": "Create page"}}}
	require.NoError(t, applyClientFileTools(route, req, payload, PlatformDeepseek))
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	account := &Account{Platform: PlatformDeepseek, Type: AccountTypeAPIKey}
	require.Equal(t, string(raw), string(ensureDeepSeekChatReasoningPlaceholders(account, raw)), "the original gateway must not mutate the payload after it was counted and authorized")
}
