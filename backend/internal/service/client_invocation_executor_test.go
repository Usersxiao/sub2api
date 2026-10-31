//go:build unit

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type invocationDispatcherFixture struct {
	response     []byte
	err          error
	calls        int
	settle       bool
	contextValue any
}

type invocationAdmissionFixture struct {
	deny         error
	calls        int
	releases     int
	contextValue any
}

type invocationAdmissionContextKey struct{}

func (a *invocationAdmissionFixture) AdmitClientInvocation(ctx context.Context, prepared *PreparedClientInvocation) (context.Context, func(), error) {
	a.calls++
	if a.deny != nil {
		return nil, nil, a.deny
	}
	if prepared == nil || prepared.Text == nil {
		return nil, nil, ErrClientInvocationParameters
	}
	ctx, _, selectionErr := WithClientInvocationTokenSelection(ctx, prepared,
		&Account{ID: 81, Platform: prepared.Resolution.Platform}, prepared.Resolution.ModelID)
	if selectionErr != nil {
		return nil, nil, selectionErr
	}
	if a.contextValue != nil {
		ctx = context.WithValue(ctx, invocationAdmissionContextKey{}, a.contextValue)
	}
	return ctx, func() { a.releases++ }, nil
}

func (d *invocationDispatcherFixture) DispatchClientInvocation(ctx context.Context, prepared *PreparedClientInvocation) ([]byte, error) {
	d.calls++
	d.contextValue, _ = ctx.Value(invocationAdmissionContextKey{}).(string)
	if _, gated := ctx.Value(clientInvocationProviderPayloadAuthorizerKey{}).(ClientInvocationProviderPayloadAuthorizer); gated {
		if err := authorizeInvocationTestProviderRequest(ctx, prepared, prepared.Text.Payload); err != nil {
			return nil, err
		}
	}
	if d.settle {
		ack := requiredUsageSettlement(ctx)
		ack.mu.Lock()
		ack.billed, ack.persisted, ack.completed = true, true, true
		ack.result.UsageLogID = 42
		ack.result.BillingFingerprint = "billing-fingerprint"
		ack.durable.alreadySettled = true
		ack.mu.Unlock()
	}
	return d.response, d.err
}

func TestClientInvocationExecutorPassesAdmittedContextToDispatcher(t *testing.T) {
	svc, _, _, _, raw := invocationLedgerServiceFixture(t)
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	const admitted = "gateway-context"
	admission := &invocationAdmissionFixture{contextValue: admitted}
	dispatcher := &invocationDispatcherFixture{
		response: []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":9,"output_tokens":2}}`),
		settle:   true,
	}
	executor := NewClientInvocationExecutor(svc, admission, dispatcher, &usageRecoveryEventRepositoryStub{})

	_, err = executor.Execute(context.Background(), "adg_fixture", attempt)
	require.NoError(t, err)
	require.Equal(t, admitted, dispatcher.contextValue)
}

func TestClientInvocationExecutorRequiresDurableSettlementBeforeSuccess(t *testing.T) {
	svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	dispatcher := &invocationDispatcherFixture{response: []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":9,"output_tokens":2}}`), settle: true}
	admission := &invocationAdmissionFixture{}
	executor := NewClientInvocationExecutor(svc, admission, dispatcher, &usageRecoveryEventRepositoryStub{})

	result, err := executor.Execute(context.Background(), "adg_fixture", attempt)
	require.NoError(t, err)
	require.Equal(t, 1, dispatcher.calls)
	require.Equal(t, 1, admission.calls)
	require.Equal(t, 1, admission.releases)
	require.Equal(t, ClientInvocationSucceeded, result.Receipt.State)
	require.Equal(t, "done", result.Result.Text)
	require.Equal(t, int64(11), result.Result.Usage.TotalTokens)
	require.Equal(t, ClientInvocationSucceeded, ledger.record.State)
}

func TestClientInvocationExecutorReleasesAdmissionWhenTrustedCountingFails(t *testing.T) {
	svc, preparer, _, ledger, raw := invocationLedgerServiceFixture(t)
	limit := int64(256)
	preparer.prepared.Resolution.Route.Limits.ContextTokens = &limit
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)

	admission := &invocationAdmissionFixture{}
	dispatcher := &invocationDispatcherFixture{}
	executor := NewClientInvocationExecutor(svc, admission, dispatcher, &usageRecoveryEventRepositoryStub{})
	_, err = executor.Execute(context.Background(), "adg_fixture", attempt)

	require.ErrorIs(t, err, ErrClientInvocationTokenCountUnavailable)
	require.Equal(t, 1, admission.calls, "admission selects the account before trusted counting")
	require.Equal(t, 1, admission.releases, "a rejected count releases the admitted user/session resources")
	require.Equal(t, 1, dispatcher.calls, "the dispatcher reaches the final payload gate, which rejects before sending")
	require.Zero(t, ledger.starts, "count failure stays before the durable dispatch boundary")
	require.Equal(t, ClientInvocationReserved, ledger.record.State)
}

func TestClientInvocationExecutorKeepsUncertainDispatchNonRetryable(t *testing.T) {
	for _, test := range []struct {
		name        string
		response    []byte
		dispatchErr error
		settle      bool
	}{
		{name: "transport error", dispatchErr: errors.New("upstream detail"), settle: false},
		{name: "invalid response", response: []byte(`{"status":"incomplete"}`), settle: true},
		{name: "unconfirmed settlement", response: []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":9,"output_tokens":2}}`), settle: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
			attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
			require.NoError(t, err)
			dispatcher := &invocationDispatcherFixture{response: test.response, err: test.dispatchErr, settle: test.settle}
			admission := &invocationAdmissionFixture{}
			executor := NewClientInvocationExecutor(svc, admission, dispatcher, &usageRecoveryEventRepositoryStub{})

			result, err := executor.Execute(context.Background(), "adg_fixture", attempt)
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrClientInvocationOutcomeUnknown)
			require.Equal(t, 1, dispatcher.calls)
			require.Equal(t, 1, admission.calls)
			require.Equal(t, 1, admission.releases)
			require.Equal(t, 0, ledger.finishes)
			require.Equal(t, ClientInvocationUnknown, ledger.record.State)
			require.Nil(t, ledger.record.Outcome)
		})
	}
}

func TestClientInvocationExecutorDoesNotDispatchUnownedAttempt(t *testing.T) {
	svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
	_, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	ledger.owned = false
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	dispatcher := &invocationDispatcherFixture{}
	admission := &invocationAdmissionFixture{}
	executor := NewClientInvocationExecutor(svc, admission, dispatcher, &usageRecoveryEventRepositoryStub{})
	_, err = executor.Execute(context.Background(), "adg_fixture", attempt)
	require.ErrorIs(t, err, ErrClientInvocationOwner)
	require.Zero(t, dispatcher.calls)
}

func TestClientInvocationDispatchBoundaryRequiresAdmission(t *testing.T) {
	svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	_, _, err = svc.startDispatchWithAdmission(context.Background(), "adg_fixture", attempt, nil)
	require.ErrorIs(t, err, ErrClientInvocationAdmissionUnavailable)
	require.Zero(t, ledger.starts)
	require.Equal(t, ClientInvocationReserved, ledger.record.State)
}

func TestClientInvocationExecutorRequiresAndReleasesAdmission(t *testing.T) {
	svc, _, grants, ledger, raw := invocationLedgerServiceFixture(t)
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	dispatcher := &invocationDispatcherFixture{response: []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":9,"output_tokens":2}}`), settle: true}
	denied := &invocationAdmissionFixture{deny: ErrClientGatewayRevoked}
	executor := NewClientInvocationExecutor(svc, denied, dispatcher, &usageRecoveryEventRepositoryStub{})
	_, err = executor.Execute(context.Background(), "adg_fixture", attempt)
	require.ErrorIs(t, err, ErrClientGatewayRevoked)
	require.Equal(t, 1, denied.calls)
	require.Zero(t, denied.releases)
	require.Zero(t, dispatcher.calls)
	require.Equal(t, ClientInvocationReserved, ledger.record.State)

	grants.err = nil
	attempt, err = svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	admission := &invocationAdmissionFixture{}
	ledger.onStart = func() { grants.err = ErrClientGatewayRevoked }
	executor = NewClientInvocationExecutor(svc, admission, dispatcher, &usageRecoveryEventRepositoryStub{})
	_, err = executor.Execute(context.Background(), "adg_fixture", attempt)
	require.ErrorIs(t, err, ErrClientInvocationOutcomeUnknown)
	require.Equal(t, 1, admission.releases)
	require.Equal(t, 1, dispatcher.calls, "the final provider gate rechecks revocation after the durable boundary")
	require.Equal(t, ClientInvocationUnknown, ledger.record.State)
}
