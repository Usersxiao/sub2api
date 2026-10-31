//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func clientInvocationCompositionDependenciesFixture(t *testing.T) (ClientInvocationCompositionDependencies, *invocationPreparerStub, *invocationLedgerStub, *httpUpstreamRecorder) {
	t.Helper()
	_, preparer, grants, ledger, _ := invocationLedgerServiceFixture(t)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"object":"response.input_tokens","input_tokens":12}`)),
	}}
	account := clientInvocationNativeCounterAccount()
	account.ID = 81
	dependencies := ClientInvocationCompositionDependencies{
		Preparer: preparer, GrantValidator: grants, Ledger: ledger,
		Accounts:      &clientInvocationNativeCounterAccountRepo{account: account},
		OpenAIGateway: &OpenAIGatewayService{httpUpstream: upstream},
		Admission:     &invocationAdmissionFixture{}, Dispatcher: &invocationDispatcherFixture{},
		SettlementEvents: &usageRecoveryEventRepositoryStub{},
	}
	return dependencies, preparer, ledger, upstream
}

func TestNewClientInvocationCompositionRequiresEveryServerDependency(t *testing.T) {
	tests := []struct {
		name   string
		remove func(*ClientInvocationCompositionDependencies)
	}{
		{name: "preparer", remove: func(d *ClientInvocationCompositionDependencies) { d.Preparer = nil }},
		{name: "grant validator", remove: func(d *ClientInvocationCompositionDependencies) { d.GrantValidator = nil }},
		{name: "durable ledger", remove: func(d *ClientInvocationCompositionDependencies) { d.Ledger = nil }},
		{name: "account repository", remove: func(d *ClientInvocationCompositionDependencies) { d.Accounts = nil }},
		{name: "OpenAI gateway", remove: func(d *ClientInvocationCompositionDependencies) { d.OpenAIGateway = nil }},
		{name: "OpenAI upstream", remove: func(d *ClientInvocationCompositionDependencies) { d.OpenAIGateway.httpUpstream = nil }},
		{name: "admission", remove: func(d *ClientInvocationCompositionDependencies) { d.Admission = nil }},
		{name: "dispatcher", remove: func(d *ClientInvocationCompositionDependencies) { d.Dispatcher = nil }},
		{name: "settlement events", remove: func(d *ClientInvocationCompositionDependencies) { d.SettlementEvents = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dependencies, _, _, _ := clientInvocationCompositionDependenciesFixture(t)
			test.remove(&dependencies)
			composition, err := NewClientInvocationComposition(dependencies)
			require.Nil(t, composition)
			require.ErrorIs(t, err, ErrClientInvocationCompositionUnavailable)
		})
	}

	dependencies, _, _, _ := clientInvocationCompositionDependenciesFixture(t)
	dependencies.Accounts = (*clientInvocationNativeCounterAccountRepo)(nil)
	composition, err := NewClientInvocationComposition(dependencies)
	require.Nil(t, composition)
	require.ErrorIs(t, err, ErrClientInvocationCompositionUnavailable, "typed nil interfaces must fail closed")
}

func TestClientInvocationCompositionUsesNativeCounterAndDoesNotInventSettlement(t *testing.T) {
	dependencies, preparer, ledger, upstream := clientInvocationCompositionDependenciesFixture(t)
	limit := int64(256)
	preparer.prepared.Resolution.Route.Limits.ContextTokens = &limit
	dispatcher := dependencies.Dispatcher.(*invocationDispatcherFixture)
	dispatcher.response = []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":12,"output_tokens":2}}`)
	composition, err := NewClientInvocationComposition(dependencies)
	require.NoError(t, err)
	require.NotNil(t, composition.Ledger)
	require.NotNil(t, composition.Executor)

	attempt, err := composition.Ledger.Reserve(context.Background(), "adg_fixture", "chat", []byte(`{"projectId":"project-a","routeId":"chat","operation":"text","configVersion":"cfg-1","entitlementVersion":"ent-1","requestId":"request-a","input":[{"kind":"text","text":"Draft a title"}],"parameters":{}}`))
	require.NoError(t, err)
	result, err := composition.Executor.Execute(context.Background(), "adg_fixture", attempt)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrClientInvocationOutcomeUnknown, "the test dispatcher does not claim a durable usage settlement")
	require.Len(t, upstream.requests, 1, "the composition injects the native counter into the final-payload gate")
	require.Equal(t, "https://api.openai.com/v1/responses/input_tokens", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer fixture-key", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, 1, dispatcher.calls)
	require.Equal(t, 1, ledger.starts)
	require.Zero(t, ledger.finishes)
	require.Equal(t, ClientInvocationUnknown, ledger.record.State)
}

func TestClientInvocationCompositionSubmitExecutesOwnedAttemptOnceAndReplaysReceipt(t *testing.T) {
	dependencies, _, ledger, upstream := clientInvocationCompositionDependenciesFixture(t)
	dispatcher := dependencies.Dispatcher.(*invocationDispatcherFixture)
	dispatcher.settle = true
	dispatcher.response = []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":9,"output_tokens":2}}`)
	composition, err := NewClientInvocationComposition(dependencies)
	require.NoError(t, err)
	raw := []byte(`{"projectId":"project-a","routeId":"chat","operation":"text","configVersion":"cfg-1","entitlementVersion":"ent-1","requestId":"request-a","input":[{"kind":"text","text":"Draft a title"}],"parameters":{}}`)

	first, err := composition.Submit(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	require.Equal(t, ClientInvocationSucceeded, first.Receipt.State)
	require.Equal(t, "done", first.Result.Text)
	require.Equal(t, int64(11), first.Result.Usage.TotalTokens)
	require.Equal(t, 1, dispatcher.calls)
	require.Empty(t, upstream.requests, "a route without contextTokens does not call the OpenAI native counter")

	// The same project/request identity is now owned by the durable record.
	ledger.owned = false
	replay, err := composition.Submit(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	require.Nil(t, replay.Result, "a replay must not return or recreate model output")
	require.Equal(t, first.Receipt.InvocationID, replay.Receipt.InvocationID)
	require.Equal(t, ClientInvocationSucceeded, replay.Receipt.State)
	require.Equal(t, 1, dispatcher.calls, "a replay must never dispatch again")
	require.Equal(t, 1, ledger.starts)

	receipt, err := composition.Get(context.Background(), "adg_fixture", "project-a", "request-a")
	require.NoError(t, err)
	require.Equal(t, first.Receipt.InvocationID, receipt.InvocationID)
	require.Equal(t, 1, dispatcher.calls)
}

func TestClientInvocationCompositionSubmitFailsClosedWithoutAssembly(t *testing.T) {
	var composition *ClientInvocationComposition
	_, err := composition.Submit(context.Background(), "adg_fixture", "chat", []byte(`{}`))
	require.ErrorIs(t, err, ErrClientInvocationAdmissionUnavailable)
	_, err = composition.Get(context.Background(), "adg_fixture", "project-a", "request-a")
	require.ErrorIs(t, err, ErrClientInvocationAdmissionUnavailable)
}
