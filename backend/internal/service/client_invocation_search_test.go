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

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func searchRequestRaw() []byte {
	return []byte(`{"projectId":"project-a","routeId":"chat","operation":"search","configVersion":"cfg-1","entitlementVersion":"ent-1","requestId":"request-search","query":"  最新设计标准 🌏  ","source":"web","maxResults":2}`)
}
func searchRoute(t *testing.T) ClientModelRoute {
	t.Helper()
	var route ClientModelRoute
	require.NoError(t, json.Unmarshal([]byte(`{"routeId":"chat","target":{"kind":"model","modelId":"shared-model"},"displayName":"Search","providerDisplay":"Provider","purposes":["research"],"operation":"search","availability":{"status":"available"},"protocol":"web-search","transport":"json","inputKinds":["text"],"outputKinds":["text"],"parameters":[],"prices":[],"capabilities":{}}`), &route))
	return route
}
func TestClientSearchStrictAdmission(t *testing.T) {
	req, err := ParseClientInvocation(searchRequestRaw())
	require.NoError(t, err)
	require.Equal(t, "  最新设计标准 🌏  ", req.Query)
	_, err = ValidateClientSearchInvocation(searchRoute(t), req)
	require.NoError(t, err)
	for _, field := range []string{`"input":[]`, `"parameters":{}`, `"tools":[]`, `"toolRounds":[]`, `"provider":"x"`, `"apiKey":"secret"`, `"endpoint":"https://evil.test"`, `"stream":false`} {
		t.Run(field, func(t *testing.T) {
			raw := strings.TrimSuffix(string(searchRequestRaw()), "}") + "," + field + "}"
			_, err := ParseClientInvocation([]byte(raw))
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
	for _, edit := range []func(map[string]any){
		func(m map[string]any) { m["query"] = " " }, func(m map[string]any) { m["source"] = "x" }, func(m map[string]any) { m["maxResults"] = 0 }, func(m map[string]any) { m["maxResults"] = 21 }, func(m map[string]any) { m["maxResults"] = 1.5 }, func(m map[string]any) { m["query"] = strings.Repeat("🌏", 2049) },
	} {
		var body map[string]any
		require.NoError(t, json.Unmarshal(searchRequestRaw(), &body))
		edit(body)
		raw, _ := json.Marshal(body)
		_, err := ParseClientInvocation(raw)
		require.ErrorIs(t, err, ErrClientInvocationParameters)
	}
	duplicate := strings.Replace(string(searchRequestRaw()), `"source":"web"`, `"source":"x","source":"web"`, 1)
	_, err = ParseClientInvocation([]byte(duplicate))
	require.ErrorIs(t, err, ErrClientInvocationParameters)
}
func TestClientSearchPreparationKeepsTextProofSeparate(t *testing.T) {
	svc, _, _, routes, _ := invocationPreparationFixture(t)
	routes.resolution.Route = searchRoute(t)
	p, err := svc.Prepare(context.Background(), "adg_fixture", "chat", searchRequestRaw())
	require.NoError(t, err)
	require.Nil(t, p.Text)
	require.NotNil(t, p.Search)
	_, err = ClientInvocationFingerprint(p)
	require.NoError(t, err)
	_, err = CheckClientInvocationTokenBudget(context.Background(), p, nil)
	require.ErrorIs(t, err, ErrClientInvocationParameters)
	require.False(t, ValidateClientInvocationCounterProof(&ClientInvocationCounterProof{}))
	require.True(t, ValidateClientInvocationSearchProof(NewClientInvocationSearchProof()))
	_, err = svc.Prepare(WithClientInvocationTextStream(context.Background(), func(string) error { return nil }), "adg_fixture", "chat", searchRequestRaw())
	require.ErrorIs(t, err, ErrClientInvocationParameters)
	routes.resolution.Platform = PlatformGrok
	_, err = svc.Prepare(context.Background(), "adg_fixture", "chat", searchRequestRaw())
	require.ErrorIs(t, err, ErrClientInvocationParameters)
}
func searchSSE(t *testing.T, output []any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{"status": "completed", "output": output}})
	require.NoError(t, err)
	return []byte("data: " + string(raw) + "\n\n")
}
func TestClientSearchNormalizesOnlyCompleteSources(t *testing.T) {
	req, err := ParseClientInvocation(searchRequestRaw())
	require.NoError(t, err)
	call := map[string]any{"type": "web_search_call", "status": "completed", "action": map[string]any{"type": "search", "sources": []any{
		map[string]any{"type": "url", "url": "https://example.test/a", "title": "Title", "snippet": "Evidence"},
		map[string]any{"type": "url", "url": "https://example.test/a", "title": "duplicate"},
		map[string]any{"type": "url", "url": "https://example.test/b"},
		map[string]any{"type": "url", "url": "https://example.test/c"},
	}}}
	msg := map[string]any{"type": "message", "content": []any{map[string]any{"type": "output_text", "text": "unexposed report", "annotations": []any{map[string]any{"type": "url_citation", "url": "https://example.test/b"}, map[string]any{"type": "url_citation", "url": "https://example.test/b"}}}}}
	out, err := normalizeClientSearchSSE(searchSSE(t, []any{call, msg}), req)
	require.NoError(t, err)
	require.Len(t, out.Sources, 2)
	require.Equal(t, "Evidence", out.Sources[0].Snippet)
	require.Equal(t, "https://example.test/b", out.Sources[1].Title)
	require.Equal(t, []ClientSearchCitation{{"source-2"}}, out.Citations)
	require.True(t, ValidateClientSearchOutput(out))
	for _, output := range [][]any{{msg}, {call, call}} {
		_, err := normalizeClientSearchSSE(searchSSE(t, output), req)
		require.ErrorIs(t, err, ErrClientInvocationResponse)
	}
	action := call["action"].(map[string]any)
	for _, sources := range []any{nil, []any{map[string]any{"type": "url", "url": "https://user:pass@example.test"}}, []any{map[string]any{"type": "url", "url": "javascript:alert(1)"}}} {
		action["sources"] = sources
		_, err := normalizeClientSearchSSE(searchSSE(t, []any{call}), req)
		require.ErrorIs(t, err, ErrClientInvocationResponse)
	}
	action["sources"] = []any{}
	out, err = normalizeClientSearchSSE(searchSSE(t, []any{call}), req)
	require.NoError(t, err)
	require.Empty(t, out.Sources)
	_, err = normalizeClientSearchSSE(searchSSE(t, []any{call, msg}), req)
	require.ErrorIs(t, err, ErrClientInvocationResponse)
	for _, raw := range []string{"data: [DONE]\n\n", `data: {"type":"response.incomplete"}` + "\n\n", "data: invalid\n\n"} {
		_, err = normalizeClientSearchSSE([]byte(raw), req)
		require.ErrorIs(t, err, ErrClientInvocationResponse)
	}
}
func TestClientSearchInflightUsesExactGroupCallPrice(t *testing.T) {
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 9
	price := 0.023
	groupID := int64(1)
	key := &APIKey{User: &User{ID: 1}, GroupID: &groupID, Group: &Group{RateMultiplier: 2, WebSearchPricePerCall: &price}}
	deps := inflightEstimateDeps{cfg: cfg, billing: NewBillingService(cfg, nil), userGroupRate: func(context.Context, int64, int64, float64) float64 { return 3 }}
	amount, priced := deps.estimate(context.Background(), key, InflightEstimateRequest{Model: "unpriced-token-alias", Kind: InflightEstimateWebSearch, Units: 1, MaxTokens: 9999999, BodyBytes: 9999999, SearchCalls: 200})
	require.True(t, priced)
	require.InDelta(t, 0.069, amount, 1e-12)
}

func TestClientSearchProviderBodyPreservesFullMultibyteQuery(t *testing.T) {
	req, err := ParseClientInvocation(searchRequestRaw())
	require.NoError(t, err)
	req.Query = strings.Repeat("汉", 4096)
	search, err := ValidateClientSearchInvocation(searchRoute(t), req)
	require.NoError(t, err)
	body, err := buildClientSearchProviderBody(search.Payload, search.ModelID)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload))
	prompt := payload["input"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	require.Contains(t, prompt, req.Query)
	require.NotContains(t, prompt, "<truncated>")
}
func TestClientSearchFinalPayloadRejectsChangedProviderControls(t *testing.T) {
	svc, _, _, routes, _ := invocationPreparationFixture(t)
	routes.resolution.Route = searchRoute(t)
	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", searchRequestRaw())
	require.NoError(t, err)
	account := &Account{ID: 19, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"access_token": "at-synthetic", "auth_mode": OpenAIAuthModePersonalAccessToken}}
	ctx, _, err := WithClientInvocationTokenSelection(context.Background(), prepared, account, prepared.ModelID())
	require.NoError(t, err)
	body, err := buildClientSearchProviderBody(prepared.Payload(), prepared.ModelID())
	require.NoError(t, err)
	for _, edit := range []string{"account", "endpoint", "stream", "tool-choice", "extra-token-budget"} {
		t.Run(edit, func(t *testing.T) {
			id := account.ID
			endpoint := "/backend-api/codex/responses"
			candidate := body
			var payload map[string]any
			require.NoError(t, json.Unmarshal(body, &payload))
			switch edit {
			case "account":
				id++
			case "endpoint":
				endpoint = "/v1/alpha/search"
			case "stream":
				payload["stream"] = false
			case "tool-choice":
				payload["tool_choice"] = "auto"
			case "extra-token-budget":
				payload["max_output_tokens"] = 10
			}
			if edit != "account" && edit != "endpoint" {
				candidate, _ = json.Marshal(payload)
			}
			gate := &clientInvocationProviderPayloadGate{ledger: &ClientInvocationLedgerService{}, ctx: ctx, prepared: prepared, attempt: &ClientInvocationAttempt{record: &ClientInvocationRecord{}}}
			require.ErrorIs(t, gate.AuthorizeClientInvocationProviderPayload(ctx, id, endpoint, candidate), ErrClientInvocationProviderPayload)
			require.False(t, gate.Authorized())
		})
	}
}

func unauditedClientSearchAccounts() map[string]*Account {
	return map[string]*Account{

		"relay-api-key": {ID: 19, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "synthetic-key", "base_url": "https://relay.example/v1"}},
		"api-key-pat-metadata": {ID: 19, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Credentials: map[string]any{
			"api_key": "synthetic-key", "access_token": "at-synthetic", "auth_mode": OpenAIAuthModePersonalAccessToken,
		}},
		"subscription-oauth": {ID: 19, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"access_token": "synthetic-oauth", "plan_type": "plus"}},
		"setup-token":        {ID: 19, Platform: PlatformOpenAI, Type: AccountTypeSetupToken, Credentials: map[string]any{"access_token": "at-synthetic", "auth_mode": OpenAIAuthModePersonalAccessToken}},
	}
}

func TestClientSearchSelectionRequiresItsAuditedAccountType(t *testing.T) {
	svc, _, _, routes, _ := invocationPreparationFixture(t)
	routes.resolution.Route = searchRoute(t)
	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", searchRequestRaw())
	require.NoError(t, err)
	for name, account := range unauditedClientSearchAccounts() {
		t.Run(name, func(t *testing.T) {
			ctx, model, err := WithClientInvocationTokenSelection(context.Background(), prepared, account, prepared.ModelID())
			require.ErrorIs(t, err, ErrClientInvocationParameters)
			require.Nil(t, ctx)
			require.Empty(t, model)
		})
	}
}

func TestClientSearchForwardRejectsUnauditedAccountsBeforeTransport(t *testing.T) {
	for name, account := range unauditedClientSearchAccounts() {
		t.Run(name, func(t *testing.T) {
			upstream := &httpUpstreamRecorder{}
			gateway := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("POST", "/v1/alpha/search", nil)
			authorizerCalled := false
			ctx := WithClientInvocationProviderPayloadAuthorizer(context.Background(), clientInvocationProviderPayloadAuthorizerFunc(func(context.Context, int64, string, []byte) error {
				authorizerCalled = true
				return nil
			}))
			result, err := gateway.ForwardAlphaSearch(ctx, c, account, []byte(`{"model":"gpt-5.4","commands":[{"search_query":[{"q":"query"}]}]}`))
			require.ErrorIs(t, err, ErrClientInvocationProviderPayload)
			require.Nil(t, result)
			require.Nil(t, upstream.lastReq)
			require.False(t, authorizerCalled)
			require.Empty(t, recorder.Body.String())
		})
	}
}

func TestClientSearchProofDoesNotBorrowTextOrUnauditedAuthorities(t *testing.T) {
	for _, raw := range []string{
		`null`,
		`{"search_calls":1,"unit":"request","authority":"responses-apikey-v2"}`,
		`{"search_calls":1,"unit":"request","authority":"openai-responses-input-tokens"}`,
		`{"search_calls":1,"unit":"request","authority":"alpha-pat-v2"}`,
		`{"search_calls":0,"unit":"request","authority":"alpha-pat-v1"}`,
		`{"search_calls":2,"unit":"request","authority":"alpha-pat-v1"}`,
		`{"search_calls":1,"unit":"token","authority":"alpha-pat-v1"}`,
		`{"required":true,"input_tokens":1,"output_tokens":1,"context_tokens":2,"authority_kind":"native","authority_id":"openai-responses-input-tokens","authority_revision":"responses-input-tokens-v1"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var proof *ClientInvocationSearchProof
			require.NoError(t, json.Unmarshal([]byte(raw), &proof))
			require.False(t, ValidateClientInvocationSearchProof(proof))
		})
	}
}

func TestClientSearchOfficialAPIKeySelectionAndRequest(t *testing.T) {
	svc, _, _, routes, _ := invocationPreparationFixture(t)
	routes.resolution.Route = searchRoute(t)
	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", searchRequestRaw())
	require.NoError(t, err)
	account := &Account{ID: 19, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Credentials: map[string]any{"api_key": "synthetic-key", "base_url": "https://api.openai.com/v1"}}
	ctx, model, err := WithClientInvocationTokenSelection(context.Background(), prepared, account, prepared.ModelID())
	require.NoError(t, err)
	require.Equal(t, prepared.ModelID(), model)
	body, err := buildClientSearchProviderBody(prepared.Payload(), model)
	require.NoError(t, err)
	gateway := &OpenAIGatewayService{cfg: &config.Config{}}
	ctx = WithClientInvocationProviderPayloadAuthorizer(ctx, clientInvocationProviderPayloadAuthorizerFunc(func(context.Context, int64, string, []byte) error { return nil }))
	request, err := gateway.buildOpenAIAlphaSearchResponsesWebSearchRequest(ctx, nil, account, prepared.Payload(), body, "synthetic-key")
	require.NoError(t, err)
	require.Equal(t, "https://api.openai.com/v1/responses", request.URL.String())
	require.Equal(t, "Bearer synthetic-key", request.Header.Get("Authorization"))
	for _, header := range []string{"OpenAI-Beta", "Session_ID", "Conversation_ID", "ChatGPT-Account-ID", "Originator", "Version"} {
		require.Empty(t, request.Header.Get(header), header)
	}
	require.True(t, ValidateClientInvocationSearchProof(&ClientInvocationSearchProof{1, "request", "responses-apikey-v1"}))
	require.NotEqual(t, *NewClientInvocationSearchProof(), ClientInvocationSearchProof{1, "request", "responses-apikey-v1"})
}

func TestClientSearchAPIKeyRejectsUntrustedAccountAndEndpoint(t *testing.T) {
	svc, _, _, routes, _ := invocationPreparationFixture(t)
	routes.resolution.Route = searchRoute(t)
	prepared, err := svc.Prepare(context.Background(), "adg_fixture", "chat", searchRequestRaw())
	require.NoError(t, err)
	account := &Account{ID: 19, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Credentials: map[string]any{"api_key": "synthetic-key"}}
	for _, base := range []string{"http://api.openai.com/v1", "https://api.openai.com.evil/v1", "https://user@api.openai.com/v1", "https://api.openai.com:444/v1", "https://api.openai.com/v1?x=1", "https://relay.example/v1", "https://api.openai.com/v1/responses"} {
		account.Credentials["base_url"] = base
		_, _, err := WithClientInvocationTokenSelection(context.Background(), prepared, account, prepared.ModelID())
		require.ErrorIs(t, err, ErrClientInvocationParameters, base)
	}
	for _, mutate := range []func(*Account){
		func(a *Account) { a.Status = "disabled" },
		func(a *Account) { a.Credentials["api_key"] = "" },
		func(a *Account) { a.Platform = PlatformAnthropic },
		func(a *Account) { parent := int64(1); a.ParentAccountID = &parent },
		func(a *Account) { a.Credentials["auth_mode"] = OpenAIAuthModePersonalAccessToken },
		func(a *Account) { a.Credentials["access_token"] = "at-synthetic" },
	} {
		candidate := *account
		candidate.Credentials = map[string]any{"api_key": "synthetic-key"}
		mutate(&candidate)
		_, _, err := WithClientInvocationTokenSelection(context.Background(), prepared, &candidate, prepared.ModelID())
		require.ErrorIs(t, err, ErrClientInvocationParameters)
	}
	delete(account.Credentials, "base_url")
	ctx, _, err := WithClientInvocationTokenSelection(context.Background(), prepared, account, prepared.ModelID())
	require.NoError(t, err)
	body, err := buildClientSearchProviderBody(prepared.Payload(), prepared.ModelID())
	require.NoError(t, err)
	gate := &clientInvocationProviderPayloadGate{ledger: &ClientInvocationLedgerService{}, ctx: ctx, prepared: prepared, attempt: &ClientInvocationAttempt{record: &ClientInvocationRecord{}}}
	ctx = WithClientInvocationProviderPayloadAuthorizer(ctx, gate)
	for _, endpoint := range []string{"https://relay.example/v1/responses", "http://api.openai.com/v1/responses", "https://api.openai.com:444/v1/responses", "https://api.openai.com/v1/responses?x=1", "https://api.openai.com/v1/responses?", "https://api.openai.com/v1/responses#fragment", "https://user@api.openai.com/v1/responses", "https://api.openai.com/v1/%72esponses", "https://chatgpt.com/backend-api/codex/responses"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		require.NoError(t, err)
		require.ErrorIs(t, AuthorizeClientInvocationProviderRequest(req, account.ID), ErrClientInvocationProviderPayload, endpoint)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", bytes.NewReader(body))
	require.NoError(t, err)
	req.Host = "relay.example"
	require.ErrorIs(t, AuthorizeClientInvocationProviderRequest(req, account.ID), ErrClientInvocationProviderPayload)
	// Both authorities require the same canonical controls, and the selected
	// authority cannot authorize the other adapter's endpoint.
	for _, proof := range []*ClientInvocationSearchProof{NewClientInvocationSearchProof(), {1, "request", "responses-apikey-v1"}} {
		selection, _ := ClientInvocationTokenSelectionFromContext(ctx)
		selection.SearchAuthority = proof.Authority
		selected := context.WithValue(ctx, clientInvocationTokenSelectionKey{}, selection)
		for _, edit := range []string{"endpoint", "max_tool_calls", "tool_choice", "include", "tools", "store", "stream"} {
			var payload map[string]any
			require.NoError(t, json.Unmarshal(body, &payload))
			endpoint := clientInvocationSearchEndpoint(proof.Authority)
			switch edit {
			case "endpoint":
				if proof.Authority == "alpha-pat-v1" {
					endpoint = "/v1/responses"
				} else {
					endpoint = "/backend-api/codex/responses"
				}
			case "max_tool_calls":
				payload[edit] = 2
			case "tool_choice":
				payload[edit] = "auto"
			case "include":
				payload[edit] = []any{}
			case "tools":
				payload[edit] = []any{map[string]any{"type": "web_search"}, map[string]any{"type": "web_search"}}
			case "store":
				payload[edit] = true
			case "stream":
				payload[edit] = false
			}
			raw, _ := json.Marshal(payload)
			g := &clientInvocationProviderPayloadGate{ledger: &ClientInvocationLedgerService{}, ctx: selected, prepared: prepared, attempt: &ClientInvocationAttempt{record: &ClientInvocationRecord{}}}
			require.ErrorIs(t, g.AuthorizeClientInvocationProviderPayload(selected, account.ID, endpoint, raw), ErrClientInvocationProviderPayload)
		}
	}
}
