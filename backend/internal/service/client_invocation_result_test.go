//go:build unit

package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestParseClientInvocationResponseProtocols(t *testing.T) {
	for _, test := range []struct {
		protocol string
		response string
	}{
		{"responses", `{"status":"completed","output":[{"type":"reasoning","summary":[]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Hello "},{"type":"output_text","text":"world"}]}],"usage":{"input_tokens":8,"output_tokens":3}}`},
		{"messages", `{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"Hello "},{"type":"text","text":"world"}],"usage":{"input_tokens":8,"output_tokens":3}}`},
		{"chat-completions", `{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"Hello world"}}],"usage":{"prompt_tokens":8,"completion_tokens":3,"total_tokens":11}}`},
	} {
		t.Run(test.protocol, func(t *testing.T) {
			result, err := ParseClientInvocationResponse(test.protocol, []byte(test.response))
			require.NoError(t, err)
			require.Equal(t, "Hello world", result.Text)
			require.Equal(t, ClientInvocationUsage{InputTokens: 8, OutputTokens: 3, TotalTokens: 11}, result.Usage)
			encoded, marshalErr := json.Marshal(result)
			require.NoError(t, marshalErr)
			require.NotContains(t, string(encoded), "usage")
		})
	}
}

func TestParseClientInvocationResponseRefusalAndComputedUsage(t *testing.T) {
	result, err := ParseClientInvocationResponse("responses", []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"Not available"}]}],"usage":{"input_tokens":2,"output_tokens":4}}`))
	require.NoError(t, err)
	require.Equal(t, "Not available", result.Text)
	require.Equal(t, int64(6), result.Usage.TotalTokens)

	result, err = ParseClientInvocationResponse("chat-completions", []byte(`{"choices":[{"finish_reason":"length","message":{"role":"assistant","content":"partial"}}],"usage":{"prompt_tokens":1,"completion_tokens":2}}`))
	require.NoError(t, err)
	require.Equal(t, int64(3), result.Usage.TotalTokens)
}

func TestParseClientInvocationResponseRejectsIncompleteOrUnmeteredResults(t *testing.T) {
	for _, test := range []struct{ name, protocol, response string }{
		{"unsupported protocol", "vendor", `{}`},
		{"malformed JSON", "messages", `{"content":`},
		{"duplicate keys", "messages", `{"stop_reason":"end_turn","stop_reason":"end_turn"}`},
		{"missing usage", "messages", `{"type":"message","stop_reason":"end_turn","content":[]}`},
		{"wrong usage field", "chat-completions", `{"choices":[],"usage":{"input_tokens":1,"output_tokens":1}}`},
		{"responses incomplete", "responses", `{"status":"incomplete","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`},
		{"messages tool use", "messages", `{"type":"message","stop_reason":"tool_use","content":[],"usage":{"input_tokens":1,"output_tokens":1}}`},
		{"chat tool call", "chat-completions", `{"choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","content":""}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`},
		{"multiple choices", "chat-completions", `{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"a"}},{"finish_reason":"stop","message":{"role":"assistant","content":"b"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`},
		{"mismatched total", "chat-completions", `{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"a"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":3}}`},
		{"quoted count", "messages", `{"type":"message","stop_reason":"end_turn","content":[],"usage":{"input_tokens":"1","output_tokens":1}}`},
		{"fractional count", "messages", `{"type":"message","stop_reason":"end_turn","content":[],"usage":{"input_tokens":1.5,"output_tokens":1}}`},
		{"negative count", "messages", `{"type":"message","stop_reason":"end_turn","content":[],"usage":{"input_tokens":1,"output_tokens":-1}}`},
		{"overflow count", "messages", `{"type":"message","stop_reason":"end_turn","content":[],"usage":{"input_tokens":9223372036854775808,"output_tokens":1}}`},
		{"null text", "messages", `{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":null}],"usage":{"input_tokens":1,"output_tokens":1}}`},
		{"non-assistant response", "responses", `{"status":"completed","output":[{"type":"message","role":"user","content":[{"type":"output_text","text":"x"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`},
		{"unsupported response block", "responses", `{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"image","url":"private"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ParseClientInvocationResponse(test.protocol, []byte(test.response))
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrClientInvocationResponse)
			require.Equal(t, "MODEL_RESPONSE_INVALID", infraerrors.Reason(err))
			require.NotContains(t, err.Error(), "private")
		})
	}
}

func TestParseClientInvocationResponseEnforcesByteLimits(t *testing.T) {
	base := `{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"%s"}],"usage":{"input_tokens":1,"output_tokens":1}}`
	for _, raw := range []string{
		fmt.Sprintf(base, strings.Repeat("x", ClientInvocationMaxBytes)),
		`{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"safe"}],"usage":{"input_tokens":1,"output_tokens":1}}` + strings.Repeat(" ", ClientInvocationMaxBytes),
	} {
		_, err := ParseClientInvocationResponse("messages", []byte(raw))
		require.ErrorIs(t, err, ErrClientInvocationResponse)
	}
}

func TestParseClientInvocationReasoningAndTermination(t *testing.T) {
	for _, finish := range []string{"stop", "length", "content_filter"} {
		for _, content := range []string{`"partial page"`, `null`} {
			raw := fmt.Sprintf(`{"choices":[{"finish_reason":%q,"message":{"role":"assistant","content":%s,"reasoning_content":"先规划页面"}}],"usage":{"prompt_tokens":8,"completion_tokens":3}}`, finish, content)
			output, err := ParseClientInvocationResponse("chat-completions", []byte(raw))
			require.NoError(t, err)
			require.Equal(t, "先规划页面", output.Reasoning)
			require.Equal(t, finish, output.FinishReason)
			require.Empty(t, output.ToolCalls)
		}
	}
	for _, finish := range []string{"length", "content_filter"} {
		raw := fmt.Sprintf(`{"choices":[{"finish_reason":%q,"message":{"role":"assistant","content":null,"reasoning_content":"规划","tool_calls":[{"id":"call-1","type":"function","function":{"name":"write_file","arguments":"{\"path\":"}}]}}],"usage":{"prompt_tokens":8,"completion_tokens":3}}`, finish)
		output, err := ParseClientInvocationResponse("chat-completions", []byte(raw))
		require.NoError(t, err)
		require.Equal(t, finish, output.FinishReason)
		require.Empty(t, output.ToolCalls, "truncated arguments must never become executable")
	}
	for _, value := range []string{`{}`, `42`, `[]`, `"` + strings.Repeat("x", ClientInvocationMaxBytes) + `"`} {
		raw := fmt.Sprintf(`{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"text","reasoning_content":%s}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`, value)
		_, err := ParseClientInvocationResponse("chat-completions", []byte(raw))
		require.ErrorIs(t, err, ErrClientInvocationResponse)
	}
	output, err := ParseClientInvocationResponse("messages", []byte(`{"type":"message","stop_reason": "max_tokens", "content":[{"type":"text","text":"partial"}],"usage":{"input_tokens":8,"output_tokens":3}}`))
	require.NoError(t, err)
	require.Equal(t, "length", output.FinishReason)
}
