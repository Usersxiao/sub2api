//go:build unit

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type clientInvocationTokenCounterFunc func(context.Context, ClientInvocationTokenInput) (*ClientInvocationTokenCount, error)

func (f clientInvocationTokenCounterFunc) CountClientInvocationTokens(ctx context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
	return f(ctx, in)
}

type clientInvocationProviderPayloadAuthorizerFunc func(context.Context, int64, string, []byte) error

func (f clientInvocationProviderPayloadAuthorizerFunc) AuthorizeClientInvocationProviderPayload(ctx context.Context, accountID int64, endpoint string, payload []byte) error {
	return f(ctx, accountID, endpoint, payload)
}

func TestAuthorizeClientInvocationProviderRequestRestoresAndPinsFinalBody(t *testing.T) {
	payload := []byte(`{"model":"provider-model","max_completion_tokens":128}`)
	calls := 0
	ctx := WithClientInvocationProviderPayloadAuthorizer(context.Background(), clientInvocationProviderPayloadAuthorizerFunc(func(_ context.Context, accountID int64, endpoint string, body []byte) error {
		calls++
		require.Equal(t, int64(91), accountID)
		require.Equal(t, "/v1/chat/completions", endpoint)
		require.Equal(t, payload, body)
		return nil
	}))
	request := httptest.NewRequest(http.MethodPost, "https://provider.test/v1/chat/completions", bytes.NewReader(payload)).WithContext(ctx)
	require.NoError(t, AuthorizeClientInvocationProviderRequest(request, 91))
	require.NoError(t, AuthorizeClientInvocationProviderRequest(request, 91))
	require.Equal(t, 1, calls)
	require.Equal(t, int64(len(payload)), request.ContentLength)
	require.Nil(t, request.GetBody, "automatic net/http replay must stay disabled for a single-dispatch invocation")
	require.True(t, HTTPUpstreamRedirectsDisabled(request.Context()))
	body, err := io.ReadAll(request.Body)
	require.NoError(t, err)
	require.Equal(t, payload, body)
}

func TestAuthorizeClientInvocationProviderRequestRestoresBodyOnDenial(t *testing.T) {
	want := errors.New("provider binding denied")
	payload := []byte(`{"model":"provider-model"}`)
	ctx := WithClientInvocationProviderPayloadAuthorizer(context.Background(), clientInvocationProviderPayloadAuthorizerFunc(func(context.Context, int64, string, []byte) error {
		return want
	}))
	request := httptest.NewRequest(http.MethodPost, "https://provider.test/v1/responses", bytes.NewReader(payload)).WithContext(ctx)
	require.ErrorIs(t, AuthorizeClientInvocationProviderRequest(request, 91), want)
	body, err := io.ReadAll(request.Body)
	require.NoError(t, err)
	require.Equal(t, payload, body)
}

func TestClientInvocationProviderPayloadRejectionHidesInternalErrors(t *testing.T) {
	private := errors.New("PRIVATE_DATABASE_DETAIL")
	gate := &clientInvocationProviderPayloadGate{rejection: private}
	ctx := context.WithValue(context.Background(), clientInvocationProviderPayloadAuthorizerKey{}, gate)
	require.Nil(t, clientInvocationProviderPayloadRejection(ctx))

	gate.rejection = ErrClientGatewayRevoked
	require.ErrorIs(t, clientInvocationProviderPayloadRejection(ctx), ErrClientGatewayRevoked)
}

func TestClientInvocationProviderJSONRejectsDuplicateKeysAndNonIntegerBudgets(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"model":"one","model":"two"}`),
		[]byte(`{"model":"one","options":{"count":1,"count":2}}`),
	} {
		require.False(t, validClientInvocationProviderJSON(payload), string(payload))
	}
	for _, payload := range [][]byte{
		[]byte(`{"model":"one","max_tokens":128}`),
		[]byte(`{"model":"one","max_tokens":128.5}`),
		[]byte(`{"model":"one","max_tokens":1e2}`),
		[]byte(` {"model":"one","options":[1,true,null,{"value":"ok"}]} `),
	} {
		require.True(t, validClientInvocationProviderJSON(payload), string(payload))
	}
	for _, payload := range [][]byte{[]byte(`{"max_tokens":128.5}`), []byte(`{"max_tokens":1e2}`)} {
		_, ok := clientInvocationProviderOutputTokens("messages", payload)
		require.False(t, ok, string(payload))
	}
}

func clientInvocationTokenFixture(t *testing.T) *PreparedClientInvocation {
	t.Helper()
	svc, raw, _, routes, _ := invocationPreparationFixture(t)
	limit := int64(256)
	routes.resolution.Route.Limits.ContextTokens = &limit
	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	return prepared
}

func clientInvocationExactCount(in ClientInvocationTokenInput, tokens int64) *ClientInvocationTokenCount {
	return &ClientInvocationTokenCount{AccountID: in.AccountID, GroupID: in.GroupID, Platform: in.Platform,
		ModelID: in.ModelID, Protocol: in.Protocol, PayloadHash: in.PayloadHash,
		Accuracy: ClientInvocationTokenCountExact, InputTokens: tokens,
		Authority: ClientInvocationTokenAuthority{Kind: "tokenizer", ID: "explicit-model-tokenizer", Revision: "registry-v1"}}
}

func TestClientInvocationCounterProofValidatesBoundBudget(t *testing.T) {
	inputTokens, contextTokens := int64(32), int64(256)
	valid := &ClientInvocationCounterProof{
		Required: true, InputTokens: &inputTokens, OutputTokens: 64, ContextTokens: &contextTokens,
		AuthorityKind: "native", AuthorityID: "responses-counter", AuthorityRevision: "rev-1",
	}
	require.True(t, ValidateClientInvocationCounterProof(valid))
	require.True(t, ValidateClientInvocationCounterProof(&ClientInvocationCounterProof{OutputTokens: 64}))

	invalid := []ClientInvocationCounterProof{
		{},
		{Required: true, OutputTokens: 64},
		{InputTokens: &inputTokens, OutputTokens: 64},
		{Required: true, InputTokens: &inputTokens, OutputTokens: 64, ContextTokens: &contextTokens,
			AuthorityKind: "provider", AuthorityID: "responses-counter", AuthorityRevision: "rev-1"},
		{Required: true, InputTokens: &inputTokens, OutputTokens: 64, ContextTokens: &contextTokens,
			AuthorityKind: "native", AuthorityID: "", AuthorityRevision: "rev-1"},
		{Required: true, InputTokens: &inputTokens, OutputTokens: 225, ContextTokens: &contextTokens,
			AuthorityKind: "native", AuthorityID: "responses-counter", AuthorityRevision: "rev-1"},
		{Required: true, InputTokens: &inputTokens, OutputTokens: 64, ContextTokens: &inputTokens,
			AuthorityKind: "native", AuthorityID: "responses-counter", AuthorityRevision: "rev-1"},
	}
	for _, proof := range invalid {
		require.False(t, ValidateClientInvocationCounterProof(&proof), "%+v", proof)
	}
}

func TestClientInvocationTokenBudgetBindsExactCountToEffectivePayload(t *testing.T) {
	for _, authority := range []string{"tokenizer", "native"} {
		t.Run(authority, func(t *testing.T) {
			prepared := clientInvocationTokenFixture(t)
			ctx := context.WithValue(context.Background(), struct{}{}, "counting-context")
			calls := 0
			counter := clientInvocationTokenCounterFunc(func(got context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
				calls++
				require.Same(t, ctx, got)
				require.Equal(t, "shared-model", in.ModelID)
				require.Equal(t, "responses", in.Protocol)
				require.Equal(t, prepared.Text.Payload, in.Payload)
				hash := sha256.Sum256(in.Payload)
				require.Equal(t, hex.EncodeToString(hash[:]), in.PayloadHash)
				result := clientInvocationExactCount(in, 128)
				result.Authority.Kind = authority
				return result, nil
			})
			budget, err := CheckClientInvocationTokenBudget(ctx, prepared, counter)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Equal(t, int64(128), *budget.InputTokens)
			require.Equal(t, int64(128), budget.OutputTokens)
			require.Equal(t, int64(256), *budget.ContextTokens)
			require.Equal(t, authority, budget.Authority.Kind)
			raw, err := json.Marshal(budget)
			require.NoError(t, err)
			require.JSONEq(t, `{}`, string(raw))
		})
	}
}

func TestClientInvocationTokenBudgetCountsCompositeUpstreamModelAndPayload(t *testing.T) {
	svc, raw, _, routes, billing := invocationPreparationFixture(t)
	limit := int64(256)
	routes.resolution.Route.Limits.ContextTokens = &limit
	routes.resolution.Platform = PlatformAnthropic
	routes.resolution.CompositeDecision = &CompositeRouteDecision{
		Matched: true, Source: CompositeRouteSourceExplicit, GroupID: 42, PublicModel: "shared-model",
		TargetPlatform: PlatformAnthropic, UpstreamModel: "claude-private", Endpoint: CompositeRouteEndpointResponses,
	}
	billing.identity.Group.Platform = PlatformComposite
	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	budget, err := CheckClientInvocationTokenBudget(context.Background(), prepared, clientInvocationTokenCounterFunc(func(_ context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
		require.Equal(t, "claude-private", in.ModelID)
		require.Contains(t, string(in.Payload), `"model":"claude-private"`)
		return clientInvocationExactCount(in, 64), nil
	}))
	require.NoError(t, err)
	require.Equal(t, "claude-private", budget.ModelID)
	require.Equal(t, int64(64), *budget.InputTokens)
}

func TestClientInvocationTokenBudgetBindsSelectedAccountMapping(t *testing.T) {
	prepared := clientInvocationTokenFixture(t)
	prepared.Resolution.ChannelMapping = &clientChannelModelBinding{
		ChannelID: 12, Platform: PlatformOpenAI, SourceModel: "shared-model", MappedModel: "channel-model",
	}
	prepared.Resolution.AccountMappings = []clientAccountModelBinding{{
		AccountID: 91, AccountPlatform: PlatformOpenAI, InputModel: "channel-model", UpstreamModel: "provider-model",
	}}
	ctx, model, err := WithClientInvocationTokenSelection(context.Background(), prepared,
		&Account{ID: 91, Platform: PlatformOpenAI}, "channel-model")
	require.NoError(t, err)
	require.Equal(t, "provider-model", model)

	budget, err := CheckClientInvocationTokenBudget(ctx, prepared, clientInvocationTokenCounterFunc(func(_ context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
		require.Equal(t, int64(91), in.AccountID)
		require.Equal(t, int64(42), in.GroupID)
		require.Equal(t, PlatformOpenAI, in.Platform)
		require.Equal(t, "provider-model", in.ModelID)
		require.Contains(t, string(in.Payload), `"model":"provider-model"`)
		return clientInvocationExactCount(in, 64), nil
	}))
	require.NoError(t, err)
	require.Equal(t, int64(91), budget.AccountID)
	require.Equal(t, int64(42), budget.GroupID)
	require.Equal(t, PlatformOpenAI, budget.Platform)
	require.Equal(t, "provider-model", budget.ModelID)
}

func TestClientInvocationTokenBudgetRejectsCounterBoundToAnotherSelection(t *testing.T) {
	for _, field := range []string{"account", "group", "platform"} {
		t.Run(field, func(t *testing.T) {
			prepared := clientInvocationTokenFixture(t)
			ctx, _, err := WithClientInvocationTokenSelection(context.Background(), prepared,
				&Account{ID: 91, Platform: PlatformOpenAI}, "shared-model")
			require.NoError(t, err)
			_, err = CheckClientInvocationTokenBudget(ctx, prepared, clientInvocationTokenCounterFunc(func(_ context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
				count := clientInvocationExactCount(in, 64)
				switch field {
				case "account":
					count.AccountID++
				case "group":
					count.GroupID++
				case "platform":
					count.Platform = PlatformAnthropic
				}
				return count, nil
			}))
			require.ErrorIs(t, err, ErrClientInvocationTokenCountUnavailable)
		})
	}
}

func TestClientInvocationDefersSelectedAccountSlotAndKeepsWaitPlan(t *testing.T) {
	releases := 0
	selection := &AccountSelectionResult{
		Account: &Account{ID: 91, Concurrency: 3}, Acquired: true,
		ReleaseFunc: func() { releases++ },
	}
	scheduling := config.GatewaySchedulingConfig{FallbackWaitTimeout: 12 * time.Second, FallbackMaxWaiting: 7}

	require.NoError(t, deferAccountSelectionSlot(selection, scheduling))
	require.Equal(t, 1, releases)
	require.False(t, selection.Acquired)
	require.Nil(t, selection.ReleaseFunc)
	require.Equal(t, &AccountWaitPlan{
		AccountID: 91, MaxConcurrency: 3, Timeout: 12 * time.Second, MaxWaiting: 7,
	}, selection.WaitPlan)

	require.NoError(t, deferAccountSelectionSlot(selection, scheduling))
	require.Equal(t, 1, releases, "rechecking a deferred selection must not release the slot twice")
}

func TestClientInvocationTokenBudgetChecksInputPlusOutputWithoutOverflow(t *testing.T) {
	for _, input := range []int64{129, math.MaxInt64} {
		prepared := clientInvocationTokenFixture(t)
		_, err := CheckClientInvocationTokenBudget(context.Background(), prepared, clientInvocationTokenCounterFunc(func(_ context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
			return clientInvocationExactCount(in, input), nil
		}))
		require.ErrorIs(t, err, ErrClientInvocationContextLimit)
	}
	prepared := clientInvocationTokenFixture(t)
	prepared.Resolution.Route.Parameters[0].Default = json.RawMessage(`257`)
	var err error
	prepared.Text, err = ValidateClientTextInvocation(prepared.Resolution.Route, prepared.Request)
	require.NoError(t, err)
	_, err = CheckClientInvocationTokenBudget(context.Background(), prepared, clientInvocationTokenCounterFunc(func(context.Context, ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
		t.Fatal("an output budget already above context is rejected before counting")
		return nil, nil
	}))
	require.ErrorIs(t, err, ErrClientInvocationContextLimit)
}

func TestClientInvocationTokenBudgetRejectsUnavailableOrUnboundCounting(t *testing.T) {
	for _, name := range []string{"dependency", "nil", "error", "estimated", "unknown", "model", "protocol", "payload", "negative", "authority-kind", "authority-id", "authority-revision", "mutated-input"} {
		t.Run(name, func(t *testing.T) {
			prepared := clientInvocationTokenFixture(t)
			original := bytes.Clone(prepared.Text.Payload)
			var counter ClientInvocationTokenCounter = clientInvocationTokenCounterFunc(func(_ context.Context, in ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
				count := clientInvocationExactCount(in, 1)
				switch name {
				case "nil":
					return nil, nil
				case "error":
					return nil, errors.New("PRIVATE_NATIVE_TOKENIZER_SECRET")
				case "estimated":
					count.Accuracy = ClientInvocationTokenCountEstimated
				case "unknown":
					count.Accuracy = ClientInvocationTokenCountUnknown
				case "model":
					count.ModelID = "fallback-model"
				case "protocol":
					count.Protocol = "chat-completions"
				case "payload":
					count.PayloadHash = "different-request"
				case "negative":
					count.InputTokens = -1
				case "authority-kind":
					count.Authority.Kind = "default-fallback"
				case "authority-id":
					count.Authority.ID = ""
				case "authority-revision":
					count.Authority.Revision = ""
				case "mutated-input":
					in.Payload[0] = '!'
				}
				return count, nil
			})
			if name == "dependency" {
				counter = nil
			}
			budget, err := CheckClientInvocationTokenBudget(context.Background(), prepared, counter)
			require.Nil(t, budget)
			require.ErrorIs(t, err, ErrClientInvocationTokenCountUnavailable)
			require.NotContains(t, err.Error(), "PRIVATE_")
			require.Equal(t, original, []byte(prepared.Text.Payload), "a counter receives an owned payload copy")
		})
	}
}

func TestClientInvocationTokenBudgetRejectsStalePreparedPayloadBeforeCounting(t *testing.T) {
	for _, name := range []string{"model", "resolved-model", "protocol", "prompt", "payload", "budget", "input", "parameters", "route"} {
		t.Run(name, func(t *testing.T) {
			prepared := clientInvocationTokenFixture(t)
			switch name {
			case "model":
				prepared.Text.ModelID = "other-model"
			case "resolved-model":
				prepared.Resolution.ModelID = "other-model"
			case "protocol":
				prepared.Text.Protocol = "messages"
			case "prompt":
				prepared.Text.Prompt = "changed"
			case "payload":
				prepared.Text.Payload = json.RawMessage(`{"model":"other-model"}`)
			case "budget":
				prepared.Text.OutputTokens++
			case "input":
				prepared.Request.Input[0].Text = "changed"
			case "parameters":
				prepared.Request.Parameters["max_output_tokens"] = json.RawMessage(`256`)
			case "route":
				prepared.Resolution.Route.Target.ModelID = "other-model"
			}
			budget, err := CheckClientInvocationTokenBudget(context.Background(), prepared, clientInvocationTokenCounterFunc(func(context.Context, ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
				t.Fatal("stale preparation must be rejected before invoking a counter")
				return nil, nil
			}))
			require.Nil(t, budget)
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
}

func TestClientInvocationTokenBudgetOmitsCountWhenContextIsNotDeclared(t *testing.T) {
	prepared := clientInvocationTokenFixture(t)
	prepared.Resolution.Route.Limits.ContextTokens = nil
	budget, err := CheckClientInvocationTokenBudget(context.Background(), prepared, clientInvocationTokenCounterFunc(func(context.Context, ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
		t.Fatal("no undeclared context limit or supposedly exact zero is invented")
		return nil, nil
	}))
	require.NoError(t, err)
	require.Nil(t, budget.InputTokens)
	require.Nil(t, budget.ContextTokens)
	require.Nil(t, budget.Authority)
	require.Equal(t, int64(128), budget.OutputTokens)
}
