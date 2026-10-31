//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// The editing profile admits one server-defined tool, auto_cut, through which
// the AutoDesign client forwards calls to the project's editor. Its schema and
// system prompt are owned here, never supplied by the client. An editing turn
// has no fixed round cap: Stop, the balance gate and the request size bound it.
func clientAutoCutRound(index int) ClientInvocationToolRound {
	id := fmt.Sprintf("call-%d", index)
	return ClientInvocationToolRound{RoundID: fmt.Sprintf("request-%d", index), Text: "",
		Calls:   []ClientInvocationToolCall{{CallID: id, Name: "auto_cut", Arguments: map[string]json.RawMessage{"operation": json.RawMessage(`"call"`), "tool": json.RawMessage(`"get_timeline"`), "arguments": json.RawMessage(`{}`)}}},
		Results: []ClientInvocationToolResult{{CallID: id, Outcome: "success", Content: json.RawMessage(`"ok"`)}}}
}
func clientAutoCutRequest(t *testing.T, rounds int) (ClientModelRoute, *ClientInvocationRequest) {
	t.Helper()
	route, req := clientFileToolRequest(t)
	req.Tools = []string{"auto_cut"}
	req.ToolRounds = nil
	for index := 0; index < rounds; index++ {
		req.ToolRounds = append(req.ToolRounds, clientAutoCutRound(index))
	}
	return route, req
}

func TestClientInvocationAutoCutToolPublicationAndUncappedRounds(t *testing.T) {
	route, req := clientAutoCutRequest(t, 12)
	raw, err := json.Marshal(req)
	require.NoError(t, err)
	parsed, err := ParseClientInvocation(raw)
	require.NoError(t, err)
	validated, err := ValidateClientTextInvocation(route, parsed)
	require.NoError(t, err)
	require.Equal(t, int64(1), gjson.GetBytes(validated.Payload, "tools.#").Int())
	require.Equal(t, "auto_cut", gjson.GetBytes(validated.Payload, "tools.0.function.name").String())
	require.Equal(t, `["operation"]`, gjson.GetBytes(validated.Payload, "tools.0.function.parameters.required").Raw)
	require.Contains(t, gjson.GetBytes(validated.Payload, "messages.0.content").String(), "video editing")
	require.NotContains(t, gjson.GetBytes(validated.Payload, "messages.0.content").String(), "complete, usable files")
	require.False(t, gjson.GetBytes(validated.Payload, "tool_choice").Exists(), "an editing turn is not forced to stop after three rounds")
	require.Len(t, gjson.GetBytes(validated.Payload, `messages.#(role=="tool")#`).Array(), 12)
}

func TestClientInvocationAutoCutProfileRejectsMixingAndBadArguments(t *testing.T) {
	route, req := clientAutoCutRequest(t, 1)
	raw, err := json.Marshal(req)
	require.NoError(t, err)
	// Each mutation below must actually change the request, or it proves nothing.
	for _, needle := range []string{`"tools":["auto_cut"]`, `"operation":"call"`, `,"tool":"get_timeline"`, `"arguments":{}`, `"name":"auto_cut"`} {
		require.Contains(t, string(raw), needle)
	}
	for name, candidate := range map[string]string{
		"mixed with file tools": strings.Replace(string(raw), `"tools":["auto_cut"]`, `"tools":["auto_cut","read_file"]`, 1),
		"duplicate tool":        strings.Replace(string(raw), `"tools":["auto_cut"]`, `"tools":["auto_cut","auto_cut"]`, 1),
		"unknown operation":     strings.Replace(string(raw), `"operation":"call"`, `"operation":"shell"`, 1),
		"call without tool":     strings.Replace(string(raw), `,"tool":"get_timeline"`, ``, 1),
		"non-object arguments":  strings.Replace(string(raw), `"arguments":{}`, `"arguments":"rm -rf"`, 1),
		"file call in editing":  strings.Replace(string(raw), `"name":"auto_cut"`, `"name":"write_file"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			parsed, err := ParseClientInvocation([]byte(candidate))
			if err == nil {
				_, err = ValidateClientTextInvocation(route, parsed)
			}
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
}

func TestClientInvocationFileToolsKeepThreeRounds(t *testing.T) {
	route, req := clientFileToolRequest(t)
	base := req.ToolRounds[0]
	for index := 1; index < 4; index++ {
		round := base
		round.RoundID = fmt.Sprintf("request-file-%d", index)
		round.Calls = []ClientInvocationToolCall{{CallID: fmt.Sprintf("file-%d", index), Name: "read_file", Arguments: map[string]json.RawMessage{"path": json.RawMessage(`"index.html"`)}}}
		round.Results = []ClientInvocationToolResult{{CallID: fmt.Sprintf("file-%d", index), Outcome: "success", Content: json.RawMessage(`"x"`)}}
		req.ToolRounds = append(req.ToolRounds, round)
	}
	raw, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = ParseClientInvocation(raw)
	require.ErrorIs(t, err, ErrClientInvocationParameters)
	req.ToolRounds = req.ToolRounds[:3]
	validated, err := ValidateClientTextInvocation(route, req)
	require.NoError(t, err)
	require.Equal(t, "none", gjson.GetBytes(validated.Payload, "tool_choice").String())
}

func TestClientInvocationAutoCutCallsSettleAfterManyRounds(t *testing.T) {
	svc, preparer, _, _, raw := invocationLedgerServiceFixture(t)
	_, req := clientAutoCutRequest(t, 5)
	preparer.prepared.Request.Tools = req.Tools
	preparer.prepared.Request.ToolRounds = req.ToolRounds
	preparer.prepared.Resolution.Route.Protocol = "chat-completions"
	preparer.prepared.Resolution.Route.Capabilities.Tools = true
	preparer.prepared.Resolution.Route.Capabilities.Streaming = true
	preparer.prepared.Resolution.Route.Parameters = []ClientModelParameter{invocationBudgetParameter("chat-completions")}
	text, err := ValidateClientTextInvocation(preparer.prepared.Resolution.Route, preparer.prepared.Request)
	require.NoError(t, err)
	require.NoError(t, prepareClientInvocationTextStream(WithClientInvocationTextStream(context.Background(), func(string) error { return nil }), preparer.prepared.Resolution.Route, text))
	preparer.prepared.Text = text
	// A fresh call identity: the history above already used call-0..call-4.
	wire := strings.Replace(strings.Replace(clientToolStream(t, `{"operation":"call","tool":"get_timeline","arguments":{}}`),
		`"name":"write_file"`, `"name":"auto_cut"`, 1), `"id":"call-1"`, `"id":"call-fresh"`, 1)
	dispatcher := &invocationStreamDispatcherFixture{settle: true, wire: wire}
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	executor := NewClientInvocationExecutor(svc, &invocationAdmissionFixture{}, dispatcher, &usageRecoveryEventRepositoryStub{})
	execution, err := executor.Execute(WithClientInvocationTextStream(context.Background(), func(string) error { return nil }), "adg_fixture", attempt)
	require.NoError(t, err)
	require.Len(t, execution.Result.ToolCalls, 1)
	require.Equal(t, "auto_cut", execution.Result.ToolCalls[0].Name)
}
