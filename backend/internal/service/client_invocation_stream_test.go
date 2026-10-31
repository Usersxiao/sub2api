//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"strings"
	"testing"
)

func clientInvocationStreamFixture() string {
	return ": heartbeat\r\n\r\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"你\"},\"finish_reason\":null}]}\r\n\r\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"好\",\"reasoning_content\":\"合成思考\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":2,\"total_tokens\":11}}\n\n" +
		"data: [DONE]\n\n"
}

func TestClientInvocationChatStreamDeliversBeforeDoneAndHandlesSplitUTF8(t *testing.T) {
	var deltas, reasoning []string
	stream := NewClientInvocationChatStream(func(delta string) error { deltas = append(deltas, delta); return nil }, func(delta string) error { reasoning = append(reasoning, delta); return nil })
	for _, b := range []byte(clientInvocationStreamFixture()) {
		_, err := stream.Write([]byte{b})
		require.NoError(t, err)
		if len(deltas) > 0 {
			require.Equal(t, "你", deltas[0])
		}
	}
	require.Equal(t, []string{"你", "好"}, deltas)
	raw, err := stream.Complete()
	require.NoError(t, err)
	result, err := ParseClientInvocationResponse("chat-completions", raw)
	require.NoError(t, err)
	require.Equal(t, "你好", result.Text)
	require.Equal(t, ClientInvocationUsage{InputTokens: 9, OutputTokens: 2, TotalTokens: 11}, result.Usage)
	require.Equal(t, []string{"合成思考"}, reasoning)
	require.Equal(t, "合成思考", result.Reasoning)
	require.Equal(t, "stop", result.FinishReason)
}

func TestClientInvocationChatStreamRequiresCompleteProtocolEvidence(t *testing.T) {
	valid := clientInvocationStreamFixture()
	for name, candidate := range map[string]string{
		"missing done":     strings.Replace(valid, "data: [DONE]\n\n", "", 1),
		"missing usage":    strings.Replace(valid, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":2,\"total_tokens\":11}}\n\n", "", 1),
		"missing finish":   strings.Replace(valid, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", "", 1),
		"partial frame":    strings.TrimSuffix(valid, "\n"),
		"bad usage":        strings.Replace(valid, `"total_tokens":11`, `"total_tokens":12`, 1),
		"negative usage":   strings.Replace(valid, `"prompt_tokens":9`, `"prompt_tokens":-1`, 1),
		"multiple choices": "data: {\"choices\":[{},{}]}\n\n",
		"tool call":        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"function\":{\"arguments\":\"PRIVATE_SECRET\"}}]},\"finish_reason\":null}]}\n\n",
		"bad finish":       strings.Replace(valid, `"finish_reason":"stop"`, `"finish_reason":"tool_calls"`, 1),
		"provider error":   "data: {\"error\":{\"message\":\"PRIVATE_SECRET\"}}\n\n",
		"after done":       valid + "data: {\"choices\":[]}\n\n",
		"duplicate key":    "data: {\"choices\":[],\"choices\":[]}\n\n",
		"oversized frame":  "data: " + strings.Repeat("x", ClientInvocationMaxBytes) + "\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			stream := NewClientInvocationChatStream(func(string) error { return nil })
			_, _ = stream.Write([]byte(candidate))
			raw, err := stream.Complete()
			require.ErrorIs(t, err, ErrClientInvocationResponse)
			require.Nil(t, raw)
			require.NotContains(t, err.Error(), "PRIVATE_")
		})
	}
}

func TestClientInvocationChatStreamSinkFailureNeverBecomesACompletedReply(t *testing.T) {
	stream := NewClientInvocationChatStream(func(string) error { return errors.New("browser disconnected") })
	_, err := stream.Write([]byte(clientInvocationStreamFixture()))
	require.ErrorIs(t, err, ErrClientInvocationOutcomeUnknown)
	raw, err := stream.Complete()
	require.ErrorIs(t, err, ErrClientInvocationResponse)
	require.Nil(t, raw)
}

func TestClientInvocationPreparationSelectsStreamInternallyAndBindsItsPayload(t *testing.T) {
	preparation, raw, _, routes, _ := invocationPreparationFixture(t)
	routes.resolution.Route.Protocol = "chat-completions"
	routes.resolution.Route.Parameters = []ClientModelParameter{invocationBudgetParameter("chat-completions")}
	routes.resolution.Route.Capabilities.Streaming = true
	ctx := WithClientInvocationTextStream(context.Background(), func(string) error { return nil })
	prepared, err := preparation.Prepare(ctx, "adg_fixture", "chat", raw)
	require.NoError(t, err)
	require.True(t, gjson.GetBytes(prepared.Text.Payload, "stream").Bool())
	require.True(t, gjson.GetBytes(prepared.Text.Payload, "stream_options.include_usage").Bool())
	streamFP, err := ClientInvocationFingerprint(prepared)
	require.NoError(t, err)
	plain, err := preparation.Prepare(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	plainFP, err := ClientInvocationFingerprint(plain)
	require.NoError(t, err)
	require.NotEqual(t, streamFP, plainFP, "the durable identity cannot silently change its final provider mode")
	routes.resolution.Route.Capabilities.Streaming = false
	_, err = preparation.Prepare(ctx, "adg_fixture", "chat", raw)
	require.ErrorIs(t, err, ErrClientInvocationParameters)
}

type invocationStreamDispatcherFixture struct {
	settle bool
	wire   string
	calls  int
}

func (d *invocationStreamDispatcherFixture) DispatchClientInvocation(ctx context.Context, prepared *PreparedClientInvocation) ([]byte, error) {
	d.calls++
	if err := authorizeInvocationTestProviderRequest(ctx, prepared, prepared.Text.Payload); err != nil {
		return nil, err
	}
	sink, ok := ClientInvocationTextStream(ctx)
	if !ok {
		return nil, ErrClientInvocationResponse
	}
	stream := NewClientInvocationChatStream(sink, ClientInvocationReasoningStream(ctx))
	if _, err := stream.Write([]byte(d.wire)); err != nil {
		return nil, err
	}
	if d.settle {
		ack := requiredUsageSettlement(ctx)
		ack.mu.Lock()
		ack.billed, ack.persisted, ack.completed = true, true, true
		ack.result.UsageLogID, ack.result.BillingFingerprint = 42, "billing-fingerprint"
		ack.durable.alreadySettled = true
		ack.mu.Unlock()
	}
	return stream.Complete()
}

func TestClientInvocationStreamCompositionRequiresSettlementAndNeverRedispatches(t *testing.T) {
	for _, settle := range []bool{true, false} {
		t.Run(map[bool]string{true: "settled", false: "unconfirmed"}[settle], func(t *testing.T) {
			deps, preparer, ledger, _ := clientInvocationCompositionDependenciesFixture(t)
			prepared := preparer.prepared
			prepared.Text.Protocol = "chat-completions"
			prepared.Resolution.Route.Protocol = "chat-completions"
			prepared.Text.Payload = json.RawMessage(`{"model":"shared-model","messages":[{"role":"user","content":"Draft a title"}],"max_completion_tokens":128,"stream":true,"stream_options":{"include_usage":true}}`)
			prepared.Text.OutputTokens = 128
			dispatcher := &invocationStreamDispatcherFixture{settle: settle, wire: clientInvocationStreamFixture()}
			deps.Dispatcher = dispatcher
			composition, err := NewClientInvocationComposition(deps)
			require.NoError(t, err)
			raw := []byte(`{"projectId":"project-a","routeId":"chat","operation":"text","configVersion":"cfg-1","entitlementVersion":"ent-1","requestId":"request-a","input":[{"kind":"text","text":"Draft a title"}],"parameters":{}}`)
			var text strings.Builder
			result, err := composition.SubmitStream(context.Background(), "adg_fixture", "chat", raw, func(delta string) error {
				require.Equal(t, 0, ledger.finishes, "text arrives before settlement finishes")
				text.WriteString(delta)
				return nil
			})
			require.Equal(t, "你好", text.String())
			if settle {
				require.NoError(t, err)
				require.Equal(t, ClientInvocationSucceeded, result.Receipt.State)
				require.Equal(t, "你好", result.Result.Text)
			} else {
				require.ErrorIs(t, err, ErrClientInvocationOutcomeUnknown)
				require.Nil(t, result)
				require.Equal(t, ClientInvocationUnknown, ledger.record.State)
				require.Zero(t, ledger.finishes)
			}
			ledger.owned = false
			replay, err := composition.SubmitStream(context.Background(), "adg_fixture", "chat", raw, func(string) error { t.Fatal("replay must not emit output"); return nil })
			require.NoError(t, err)
			require.Nil(t, replay.Result)
			require.Equal(t, 1, dispatcher.calls)
			_, err = composition.Get(context.Background(), "adg_fixture", "project-a", "request-a")
			require.NoError(t, err)
			require.Equal(t, 1, dispatcher.calls)
		})
	}
}

func TestClientInvocationReasoningStreamFailureAndBounds(t *testing.T) {
	stream := NewClientInvocationChatStream(func(string) error { return nil }, func(string) error { return errors.New("observer closed") })
	_, err := stream.Write([]byte(clientInvocationStreamFixture()))
	require.ErrorIs(t, err, ErrClientInvocationOutcomeUnknown)
	raw, err := stream.Complete()
	require.ErrorIs(t, err, ErrClientInvocationResponse)
	require.Nil(t, raw)
	for _, candidate := range []string{
		`data: {"choices":[{"delta":{"reasoning_content":42},"finish_reason":null}]}` + "\n\n",
		strings.Repeat(`data: {"choices":[{"delta":{"reasoning_content":"`+strings.Repeat("x", 524288)+`"},"finish_reason":null}]}`+"\n\n", 3),
	} {
		stream := NewClientInvocationChatStream(func(string) error { return nil })
		_, err := stream.Write([]byte(candidate))
		require.ErrorIs(t, err, ErrClientInvocationResponse)
	}
}
