//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
)

func allowInvocationDispatch(ctx context.Context, prepared *PreparedClientInvocation) (context.Context, error) {
	selected, _, err := WithClientInvocationTokenSelection(ctx, prepared,
		&Account{ID: 81, Platform: prepared.Resolution.Platform}, prepared.Resolution.ModelID)
	return selected, err
}

func authorizeInvocationTestProviderRequest(ctx context.Context, prepared *PreparedClientInvocation, body []byte) error {
	selection, ok := ClientInvocationTokenSelectionFromContext(ctx)
	if !ok {
		return ErrClientInvocationProviderPayload
	}
	return authorizeInvocationTestProviderRequestAs(ctx, prepared, selection.AccountID, body)
}

func authorizeInvocationTestProviderRequestAs(ctx context.Context, prepared *PreparedClientInvocation, accountID int64, body []byte) error {
	endpoint := "/v1/responses"
	switch prepared.Text.Protocol {
	case "messages":
		endpoint = "/v1/messages"
	case "chat-completions":
		endpoint = "/v1/chat/completions"
	}
	request := httptest.NewRequest(http.MethodPost, "https://provider.test"+endpoint, bytes.NewReader(body))
	request = request.WithContext(ctx)
	return AuthorizeClientInvocationProviderRequest(request, accountID)
}

type invocationPreparerStub struct {
	prepared *PreparedClientInvocation
	err      error
	reads    int
}

func (p *invocationPreparerStub) Prepare(context.Context, string, string, []byte) (*PreparedClientInvocation, error) {
	p.reads++
	return p.prepared, p.err
}

type invocationLedgerGrantStub struct {
	grant *ClientGatewayGrant
	err   error
}

func (g *invocationLedgerGrantStub) Validate(_ context.Context, token, route, op string) (*ClientGatewayGrant, error) {
	if g.err != nil {
		return nil, g.err
	}
	if token != "adg_fixture" || (route != "" && route != "chat") || op != "invoke" {
		return nil, ErrClientGatewayRevoked
	}
	copy := *g.grant
	return &copy, nil
}

type invocationLedgerStub struct {
	record             *ClientInvocationRecord
	owned              bool
	reserves           int
	prepares           int
	starts             int
	finishes           int
	prepared           ClientInvocationDispatchBinding
	attemptState       string
	prepareErr         error
	startErr           error
	notDispatchedErr   error
	notDispatchedMarks int
	onReserve          func()
	onGet              func()
	onStart            func()
}

func (l *invocationLedgerStub) Reserve(_ context.Context, grant *ClientGatewayGrant, in *ClientInvocationReservation) (*ClientInvocationClaim, error) {
	l.reserves++
	now := time.Now().UTC()
	if l.record == nil {
		l.record = &ClientInvocationRecord{InvocationID: in.InvocationID, InstanceID: grant.InstanceID, UserID: 7, ConsumerID: grant.ConsumerID,
			ProjectID: in.ProjectID, RequestID: in.RequestID, RouteID: in.RouteID, Operation: in.Operation,
			ConfigVersion: in.ConfigVersion, EntitlementVersion: in.EntitlementVersion, Fingerprint: in.Fingerprint,
			BillingIdentityID: in.BillingIdentityID, GroupID: in.GroupID, State: ClientInvocationReserved,
			CreatedAt: now, UpdatedAt: now, LeaseExpiresAt: now.Add(ClientInvocationLease), IdempotencyExpiresAt: now.Add(ClientInvocationRetention)}
	}
	if l.onReserve != nil {
		l.onReserve()
	}
	return &ClientInvocationClaim{Record: l.record, Owned: l.owned}, nil
}

func (l *invocationLedgerStub) Get(context.Context, *ClientGatewayGrant, string, string) (*ClientInvocationRecord, error) {
	if l.onGet != nil {
		l.onGet()
	}
	return l.record, nil
}

func (l *invocationLedgerStub) PrepareDispatch(_ context.Context, _ *ClientGatewayGrant, _ string, _ string, binding ClientInvocationDispatchBinding) (*ClientInvocationRecord, error) {
	if l.record.State != ClientInvocationReserved || l.record.ProviderPayloadHash != "" {
		return nil, ErrClientInvocationOwner
	}
	l.prepares++
	l.prepared = binding
	l.attemptState = "prepared"
	if l.prepareErr != nil {
		return nil, l.prepareErr
	}
	return l.record, nil
}

func (l *invocationLedgerStub) StartDispatch(_ context.Context, _ *ClientGatewayGrant, _ string, _ string, binding ClientInvocationDispatchBinding) (*ClientInvocationRecord, error) {
	l.starts++
	if l.record.State != ClientInvocationReserved || l.attemptState != "prepared" || l.prepares == 0 || l.prepared.AccountID != binding.AccountID ||
		l.prepared.Platform != binding.Platform || l.prepared.ModelID != binding.ModelID || l.prepared.Protocol != binding.Protocol ||
		l.prepared.PayloadHash != binding.PayloadHash || !sameClientInvocationCounterProof(l.prepared.CounterProof, binding.CounterProof) {
		return nil, ErrClientInvocationOwner
	}
	if l.startErr != nil {
		return nil, l.startErr
	}
	copy := *l.record
	copy.State, copy.UpdatedAt = ClientInvocationDispatched, time.Now().UTC()
	copy.ProviderAccountID, copy.ProviderPlatform, copy.ProviderModel = binding.AccountID, binding.Platform, binding.ModelID
	copy.ProviderProtocol, copy.ProviderPayloadHash = binding.Protocol, binding.PayloadHash
	copy.ProviderCounterProof = binding.CounterProof
	l.record = &copy
	l.attemptState = ClientInvocationDispatched
	if l.onStart != nil {
		l.onStart()
	}
	return l.record, nil
}

func (l *invocationLedgerStub) MarkPreparedNotDispatched(_ context.Context, _, _ string) (*ClientInvocationRecord, error) {
	l.notDispatchedMarks++
	if l.notDispatchedErr != nil {
		return nil, l.notDispatchedErr
	}
	if l.record.State != ClientInvocationReserved {
		return nil, ErrClientInvocationOwner
	}
	if l.attemptState == ClientInvocationAttemptNotDispatched {
		return l.record, nil
	}
	if l.attemptState != "prepared" {
		return nil, ErrClientInvocationOwner
	}
	l.attemptState = ClientInvocationAttemptNotDispatched
	return l.record, nil
}

func (l *invocationLedgerStub) MarkUnknown(context.Context, string, string) (*ClientInvocationRecord, error) {
	if l.record.State != ClientInvocationDispatched && l.record.State != ClientInvocationUnknown {
		return nil, ErrClientInvocationOwner
	}
	copy := *l.record
	copy.State, copy.Outcome, copy.UpdatedAt = ClientInvocationUnknown, nil, time.Now().UTC()
	l.record = &copy
	return l.record, nil
}

func (l *invocationLedgerStub) Finish(_ context.Context, _, _ string, out ClientInvocationOutcome) (*ClientInvocationRecord, error) {
	l.finishes++
	copy := *l.record
	copy.State, copy.Outcome, copy.UpdatedAt = out.State, &out, time.Now().UTC()
	l.record = &copy
	return l.record, nil
}

func invocationLedgerServiceFixture(t *testing.T) (*ClientInvocationLedgerService, *invocationPreparerStub, *invocationLedgerGrantStub, *invocationLedgerStub, []byte) {
	t.Helper()
	preparation, raw, _, _, _ := invocationPreparationFixture(t)
	prepared, err := preparation.Prepare(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	prepared.Grant.RouteIDs = []string{"chat"}
	prepared.Grant.Operations = []string{"invoke"}
	preparer := &invocationPreparerStub{prepared: prepared}
	grants := &invocationLedgerGrantStub{grant: prepared.Grant}
	ledger := &invocationLedgerStub{owned: true}
	return NewClientInvocationLedgerService(preparer, grants, ledger, nil), preparer, grants, ledger, raw
}

func TestClientInvocationLedgerServiceReservesWithoutDispatchAndRedactsReceipt(t *testing.T) {
	svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	require.True(t, attempt.Owned())
	require.Equal(t, ClientInvocationReserved, attempt.Receipt.State)
	require.Zero(t, ledger.starts)
	require.Zero(t, ledger.finishes)
	encoded, err := json.Marshal(attempt)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(encoded))
	encoded, err = json.Marshal(attempt.Receipt)
	require.NoError(t, err)
	for _, secret := range []string{attempt.ownerToken, ledger.record.Fingerprint, "Draft a title", "consumer", "BillingIdentity", "groupId", "upstreamUserId", "settled", "adg_"} {
		require.NotContains(t, string(encoded), secret)
	}
	// Inputs retained for an owned attempt are a copy of the caller's buffer.
	raw[0] = '!'
	require.Equal(t, byte('{'), attempt.raw[0])
}

func TestClientInvocationLedgerServiceMarksOnlyDispatchedAttemptsUnknown(t *testing.T) {
	svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	_, err = svc.MarkUnknown(context.Background(), attempt)
	require.ErrorIs(t, err, ErrClientInvocationOwner)
	require.Equal(t, ClientInvocationReserved, ledger.record.State)

	prepared, dispatchContext, err := svc.startDispatchWithAdmission(context.Background(), "adg_fixture", attempt, allowInvocationDispatch)
	require.NoError(t, err)
	require.Zero(t, ledger.starts, "admission alone must not cross the provider dispatch boundary")
	require.NoError(t, authorizeInvocationTestProviderRequest(dispatchContext, prepared, prepared.Text.Payload))
	require.Equal(t, &ClientInvocationCounterProof{OutputTokens: prepared.Text.OutputTokens}, ledger.record.ProviderCounterProof)
	receipt, err := svc.MarkUnknown(context.Background(), attempt)
	require.NoError(t, err)
	require.Equal(t, ClientInvocationUnknown, receipt.State)
	require.Equal(t, ClientInvocationUnknown, ledger.record.State)
	encoded, err := json.Marshal(receipt)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "outcome")
}

func TestClientInvocationLedgerServiceDuplicateDoesNotOwnOrStart(t *testing.T) {
	svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
	_, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	ledger.owned = false
	duplicate, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	require.False(t, duplicate.Owned())
	require.Nil(t, duplicate.Prepared)
	_, _, err = svc.startDispatchWithAdmission(context.Background(), "adg_fixture", duplicate, allowInvocationDispatch)
	require.ErrorIs(t, err, ErrClientInvocationOwner)
	require.Zero(t, ledger.starts)
}

func TestClientInvocationLedgerServiceRechecksBeforeAndAfterDispatchBoundary(t *testing.T) {
	for _, moment := range []string{"prepare", "fingerprint", "after_start"} {
		t.Run(moment, func(t *testing.T) {
			svc, preparer, grants, ledger, raw := invocationLedgerServiceFixture(t)
			attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
			require.NoError(t, err)
			switch moment {
			case "prepare":
				preparer.err = ErrClientGatewayRevoked
			case "fingerprint":
				preparer.prepared.Text.Payload = json.RawMessage(`{"model":"different-model"}`)
			case "after_start":
				ledger.onStart = func() { grants.err = ErrClientGatewayRevoked }
			}
			prepared, dispatchContext, admissionErr := svc.startDispatchWithAdmission(context.Background(), "adg_fixture", attempt, allowInvocationDispatch)
			if moment != "after_start" {
				require.Error(t, admissionErr)
				require.Zero(t, ledger.starts)
				return
			}
			require.NoError(t, admissionErr)
			err = authorizeInvocationTestProviderRequest(dispatchContext, prepared, prepared.Text.Payload)
			require.ErrorIs(t, err, ErrClientGatewayRevoked)
			if moment == "after_start" {
				require.Equal(t, 1, ledger.starts)
				require.Equal(t, ClientInvocationDispatched, ledger.record.State, "do not restore retryable state after crossing the dispatch boundary")
			}
		})
	}
}

func TestClientInvocationLedgerServiceRevocationWinsDuringReserveAndRecovery(t *testing.T) {
	for _, moment := range []string{"reserve", "get"} {
		t.Run(moment, func(t *testing.T) {
			svc, _, grants, ledger, raw := invocationLedgerServiceFixture(t)
			if moment == "reserve" {
				ledger.onReserve = func() { grants.err = ErrClientGatewayRevoked }
				_, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
				require.ErrorIs(t, err, ErrClientGatewayRevoked)
			} else {
				_, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
				require.NoError(t, err)
				ledger.onGet = func() { grants.err = ErrTokenRevoked }
				_, err = svc.Get(context.Background(), "adg_fixture", "project-a", "request-a")
				require.ErrorIs(t, err, ErrTokenRevoked)
			}
		})
	}
}

func TestClientInvocationLedgerServiceLateFinishSurvivesLogoutButRecoveryReauthenticates(t *testing.T) {
	svc, preparer, grants, ledger, raw := invocationLedgerServiceFixture(t)
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	prepared, dispatchContext, err := svc.startDispatchWithAdmission(context.Background(), "adg_fixture", attempt, allowInvocationDispatch)
	require.NoError(t, err)
	require.NoError(t, authorizeInvocationTestProviderRequest(dispatchContext, prepared, prepared.Text.Payload))
	require.Equal(t, 2, preparer.reads)
	grants.err = ErrTokenRevoked
	text := "Finished owned text"
	receipt, err := svc.Finish(context.Background(), attempt, ClientInvocationOutcome{State: ClientInvocationSucceeded, Text: &text})
	require.NoError(t, err)
	require.Equal(t, ClientInvocationSucceeded, receipt.State)
	require.Equal(t, 1, ledger.finishes)
	_, err = svc.Get(context.Background(), "adg_fixture", "project-a", "request-a")
	require.ErrorIs(t, err, ErrTokenRevoked)
	grants.err = nil
	grants.grant.GrantID, grants.grant.SID, grants.grant.ConfigVersion = "relogged-grant", "relogged-session", "cfg-new"
	recovered, err := svc.Get(context.Background(), "adg_fixture", "project-a", "request-a")
	require.NoError(t, err)
	require.Equal(t, receipt.InvocationID, recovered.InvocationID)
	require.Equal(t, "cfg-1", recovered.ConfigVersion, "the receipt retains its submission snapshot")
	require.Equal(t, 1, ledger.starts)
}

func TestClientInvocationLedgerServiceTokenGatePrecedesDispatch(t *testing.T) {
	for _, mode := range []string{"unavailable", "estimated", "mutated-payload", "over-limit", "revoked-while-counting", "exact"} {
		t.Run(mode, func(t *testing.T) {
			svc, prep, grants, ledger, raw := invocationLedgerServiceFixture(t)
			limit := int64(256)
			prep.prepared.Resolution.Route.Limits.ContextTokens = &limit
			attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
			require.NoError(t, err)
			if mode != "unavailable" {
				svc.counter = clientInvocationTokenCounterFunc(func(_ context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
					count := clientInvocationExactCount(in, 128)
					switch mode {
					case "estimated":
						count.Accuracy = ClientInvocationTokenCountEstimated
					case "mutated-payload":
						in.Payload[0] = '!'
					case "over-limit":
						count.InputTokens = 129
					case "revoked-while-counting":
						grants.err = ErrClientGatewayRevoked
					}
					return count, nil
				})
			}
			prepared, dispatchContext, err := svc.startDispatchWithAdmission(context.Background(), "adg_fixture", attempt, allowInvocationDispatch)
			require.NoError(t, err)
			err = authorizeInvocationTestProviderRequest(dispatchContext, prepared, prepared.Text.Payload)
			if mode == "exact" {
				require.NoError(t, err)
				require.Equal(t, 1, ledger.starts)
				require.Equal(t, ClientInvocationDispatched, ledger.record.State)
				require.Equal(t, int64(81), ledger.record.ProviderAccountID)
				require.Equal(t, prepared.Resolution.Platform, ledger.record.ProviderPlatform)
				require.Equal(t, "shared-model", ledger.record.ProviderModel)
				require.Equal(t, "responses", ledger.record.ProviderProtocol)
				require.Equal(t, HashUsageRequestPayload(prepared.Text.Payload), ledger.record.ProviderPayloadHash)
				require.Equal(t, &ClientInvocationCounterProof{
					Required: true, InputTokens: ptrInt64(128), OutputTokens: 128,
					ContextTokens: ptrInt64(256), AuthorityKind: "tokenizer", AuthorityID: "explicit-model-tokenizer", AuthorityRevision: "registry-v1",
				}, ledger.record.ProviderCounterProof)
			} else {
				require.Error(t, err)
				switch mode {
				case "estimated", "mutated-payload", "unavailable":
					require.ErrorIs(t, err, ErrClientInvocationTokenCountUnavailable)
				case "over-limit":
					require.ErrorIs(t, err, ErrClientInvocationContextLimit)
				case "revoked-while-counting":
					require.ErrorIs(t, err, ErrClientGatewayRevoked)
				}
				require.Zero(t, ledger.starts)
				require.Equal(t, ClientInvocationReserved, ledger.record.State)
			}
		})
	}
}

func TestClientInvocationLedgerServiceMarksPreparedAttemptWhenDispatchClaimErrors(t *testing.T) {
	for _, stage := range []string{"prepare", "start", "marker-failure"} {
		t.Run(stage, func(t *testing.T) {
			svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
			attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
			require.NoError(t, err)
			claimErr := ErrClientGatewayUnavailable
			if stage == "prepare" {
				ledger.prepareErr = claimErr
			} else {
				ledger.startErr = claimErr
			}
			if stage == "marker-failure" {
				ledger.notDispatchedErr = ErrClientInvocationOwner
			}

			prepared, dispatchContext, err := svc.startDispatchWithAdmission(context.Background(), "adg_fixture", attempt, allowInvocationDispatch)
			require.NoError(t, err)
			err = authorizeInvocationTestProviderRequest(dispatchContext, prepared, prepared.Text.Payload)
			require.ErrorIs(t, err, claimErr)
			require.Equal(t, 1, ledger.notDispatchedMarks)
			require.Equal(t, ClientInvocationReserved, ledger.record.State)
			wantAttemptState := ClientInvocationAttemptNotDispatched
			if stage == "marker-failure" {
				wantAttemptState = "prepared"
			}
			require.Equal(t, wantAttemptState, ledger.attemptState,
				"a marker persistence error must preserve the original dispatch error")
			require.False(t, ClientInvocationProviderPayloadAuthorized(dispatchContext))
		})
	}
}

func TestClientInvocationProviderPayloadRejectsMismatchedFinalRequestBeforeDispatch(t *testing.T) {
	for _, mismatch := range []string{"account", "model", "endpoint", "invalid-json", "duplicate-model"} {
		t.Run(mismatch, func(t *testing.T) {
			svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
			attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
			require.NoError(t, err)
			prepared, dispatchContext, err := svc.startDispatchWithAdmission(context.Background(), "adg_fixture", attempt, allowInvocationDispatch)
			require.NoError(t, err)

			body := bytes.Clone(prepared.Text.Payload)
			accountID := int64(81)
			switch mismatch {
			case "account":
				accountID++
			case "model":
				body, err = sjson.SetBytes(body, "model", "other-provider-model")
				require.NoError(t, err)
			case "endpoint":
				request := httptest.NewRequest(http.MethodPost, "https://provider.test/v1/unregistered", bytes.NewReader(body))
				request = request.WithContext(dispatchContext)
				err = AuthorizeClientInvocationProviderRequest(request, accountID)
			case "invalid-json":
				body = []byte(`{"model":`)
			case "duplicate-model":
				body = []byte(`{"model":"shared-model","model":"other-model","max_output_tokens":128}`)
			}
			if mismatch != "endpoint" {
				err = authorizeInvocationTestProviderRequestAs(dispatchContext, prepared, accountID, body)
			}
			require.ErrorIs(t, err, ErrClientInvocationProviderPayload)
			require.Zero(t, ledger.starts)
			require.Equal(t, ClientInvocationReserved, ledger.record.State)
			require.False(t, ClientInvocationProviderPayloadAuthorized(dispatchContext))
		})
	}
}

type invocationAdmissionObservedKey struct{}

func TestClientInvocationLedgerServiceAdmitsSelectedAccountBeforeCounting(t *testing.T) {
	svc, preparer, _, ledger, raw := invocationLedgerServiceFixture(t)
	prepared := preparer.prepared
	limit := int64(256)
	prepared.Resolution.Route.Limits.ContextTokens = &limit
	prepared.Resolution.ChannelMapping = &clientChannelModelBinding{
		ChannelID: 12, Platform: PlatformOpenAI, SourceModel: "shared-model", MappedModel: "channel-model",
	}
	prepared.Resolution.AccountMappings = []clientAccountModelBinding{{
		AccountID: 91, AccountPlatform: PlatformOpenAI, InputModel: "channel-model", UpstreamModel: "provider-model",
	}}
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)

	var order []string
	ledger.onStart = func() { order = append(order, "dispatch") }
	svc.counter = clientInvocationTokenCounterFunc(func(ctx context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
		order = append(order, "count")
		require.Equal(t, true, ctx.Value(invocationAdmissionObservedKey{}))
		selection, ok := ClientInvocationTokenSelectionFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, int64(91), selection.AccountID)
		require.Equal(t, int64(42), selection.GroupID)
		require.Equal(t, PlatformOpenAI, selection.Platform)
		require.Equal(t, "provider-model", in.ModelID)
		require.Contains(t, string(in.Payload), `"model":"provider-model"`)
		return clientInvocationExactCount(in, 64), nil
	})

	prepared, dispatchContext, err := svc.startDispatchWithAdmission(context.Background(), "adg_fixture", attempt, func(ctx context.Context, fresh *PreparedClientInvocation) (context.Context, error) {
		order = append(order, "admission")
		selected, _, selectErr := WithClientInvocationTokenSelection(ctx, fresh,
			&Account{ID: 91, Platform: PlatformOpenAI}, "channel-model")
		if selectErr != nil {
			return nil, selectErr
		}
		return context.WithValue(selected, invocationAdmissionObservedKey{}, true), nil
	})
	require.NoError(t, err)
	providerPayload, err := sjson.SetBytes(prepared.Text.Payload, "model", "provider-model")
	require.NoError(t, err)
	require.NoError(t, authorizeInvocationTestProviderRequest(dispatchContext, prepared, providerPayload))
	require.Equal(t, []string{"admission", "count", "dispatch"}, order)
	require.Equal(t, 1, ledger.starts)
}

func TestClientInvocationLedgerServiceRecoveryRejectsForeignOrExpiredRows(t *testing.T) {
	for _, field := range []string{"user", "consumer", "instance", "project", "request", "route", "expired", "unknown-state"} {
		t.Run(field, func(t *testing.T) {
			svc, _, _, ledger, raw := invocationLedgerServiceFixture(t)
			_, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
			require.NoError(t, err)
			switch field {
			case "user":
				ledger.record.UserID++
			case "consumer":
				ledger.record.ConsumerID = "foreign"
			case "instance":
				ledger.record.InstanceID = "foreign"
			case "project":
				ledger.record.ProjectID = "foreign"
			case "request":
				ledger.record.RequestID = "foreign"
			case "route":
				ledger.record.RouteID = "foreign"
			case "expired":
				ledger.record.IdempotencyExpiresAt = time.Now().Add(-time.Second)
			case "unknown-state":
				ledger.record.State = "unrecognized"
			}
			_, err = svc.Get(context.Background(), "adg_fixture", "project-a", "request-a")
			require.ErrorIs(t, err, ErrClientInvocationNotFound)
		})
	}
}

func TestClientInvocationFingerprintCanonicalAndBoundToExecution(t *testing.T) {
	_, preparer, _, _, _ := invocationLedgerServiceFixture(t)
	p := preparer.prepared
	p.Text.Payload = json.RawMessage(`{"temperature":1.0,"model":"shared-model","stream":false,"input":"Draft a title"}`)
	first, err := ClientInvocationFingerprint(p)
	require.NoError(t, err)
	p.Text.Payload = json.RawMessage(`{"input":"Draft a title","stream":false,"model":"shared-model","temperature":1e0}`)
	second, err := ClientInvocationFingerprint(p)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Len(t, first, 64)
	for _, change := range []string{"text", "route", "config", "entitlement", "group", "billing", "payload", "channel mapping", "account mapping"} {
		t.Run(change, func(t *testing.T) {
			_, prep, _, _, _ := invocationLedgerServiceFixture(t)
			p := prep.prepared
			before, err := ClientInvocationFingerprint(p)
			require.NoError(t, err)
			switch change {
			case "text":
				p.Request.Input[0].Text = "A different title"
			case "route":
				p.Request.RouteID = "another-route"
			case "config":
				p.Request.ConfigVersion = "cfg-new"
			case "entitlement":
				p.Request.EntitlementVersion = "ent-new"
			case "group":
				p.Resolution.GroupID++
				*p.BillingIdentity.GroupID++
				p.BillingIdentity.Group.ID++
			case "billing":
				p.BillingIdentity.ID++
			case "payload":
				p.Text.Payload = json.RawMessage(`{"model":"shared-model","temperature":0.5}`)
			case "channel mapping":
				p.Resolution.ChannelMapping = &clientChannelModelBinding{ChannelID: 99, Platform: PlatformOpenAI,
					SourceModel: "shared-model", MappedModel: "private-model"}
			case "account mapping":
				p.Resolution.AccountMappings = []clientAccountModelBinding{{AccountID: 99, AccountPlatform: PlatformOpenAI,
					InputModel: "shared-model", UpstreamModel: "private-model"}}
			}
			after, err := ClientInvocationFingerprint(p)
			require.NoError(t, err)
			require.NotEqual(t, before, after)
		})
	}
}

func TestClientInvocationFingerprintBindsCompositeRouteDecision(t *testing.T) {
	svc, raw, _, routes, billing := invocationPreparationFixture(t)
	routes.resolution.Platform = PlatformAnthropic
	routes.resolution.CompositeDecision = &CompositeRouteDecision{
		Matched: true, Source: CompositeRouteSourceExplicit, GroupID: 42, PublicModel: "shared-model",
		TargetPlatform: PlatformAnthropic, UpstreamModel: "claude-private", Endpoint: CompositeRouteEndpointResponses,
	}
	billing.identity.Group.Platform = PlatformComposite
	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	first, err := ClientInvocationFingerprint(prepared)
	require.NoError(t, err)
	prepared.Resolution.CompositeDecision.UpstreamModel = "claude-next"
	_, err = ClientInvocationFingerprint(prepared)
	require.ErrorIs(t, err, ErrClientInvocationParameters, "a stale decision whose model no longer matches the payload is rejected")
	prepared.Resolution.CompositeDecision.UpstreamModel = "claude-private"
	prepared.Resolution.CompositeDecision.Source = CompositeRouteSourceAccount
	second, err := ClientInvocationFingerprint(prepared)
	require.NoError(t, err)
	require.NotEqual(t, first, second, "decision provenance participates in the durable fingerprint")
}

func TestClientInvocationOutcomeAllowsOnlyNormalizedBoundedResult(t *testing.T) {
	text := "A result\x00with an embedded zero"
	require.True(t, ValidateClientInvocationOutcome(ClientInvocationOutcome{State: ClientInvocationSucceeded, Text: &text}))
	require.True(t, ValidateClientInvocationOutcome(ClientInvocationOutcome{State: ClientInvocationFailed, ErrorCode: "UPSTREAM_UNAVAILABLE"}))
	huge := strings.Repeat("x", ClientInvocationMaxBytes+1)
	invalid := string([]byte{0xff})
	for _, outcome := range []ClientInvocationOutcome{
		{State: ClientInvocationSucceeded}, {State: ClientInvocationSucceeded, Text: &huge}, {State: ClientInvocationSucceeded, Text: &invalid},
		{State: ClientInvocationSucceeded, Text: &text, ErrorCode: "INTERNAL_ERROR"},
		{State: ClientInvocationFailed, Text: &text, ErrorCode: "INTERNAL_ERROR"},
		{State: ClientInvocationFailed, ErrorCode: "https://provider/secret"}, {State: ClientInvocationFailed, ErrorCode: "ARBITRARY_UPSTREAM_ERROR"},
		{State: ClientInvocationUnknown},
	} {
		require.False(t, ValidateClientInvocationOutcome(outcome))
	}
}
