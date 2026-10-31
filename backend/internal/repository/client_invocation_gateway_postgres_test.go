//go:build unit

package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type invocationGatewayPGAccountRepository struct {
	service.AccountRepository
	account service.Account
}

func (r invocationGatewayPGAccountRepository) GetByID(_ context.Context, id int64) (*service.Account, error) {
	if r.account.ID != id {
		return nil, service.ErrNoAvailableAccounts
	}
	account := r.account
	return &account, nil
}

func (r invocationGatewayPGAccountRepository) ListSchedulableByGroupIDAndPlatform(_ context.Context, groupID int64, platform string) ([]service.Account, error) {
	if r.account.Platform != platform || !invocationGatewayPGAccountInGroup(r.account, groupID) {
		return nil, nil
	}
	return []service.Account{r.account}, nil
}

func (r invocationGatewayPGAccountRepository) ListSchedulableByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	if r.account.Platform != platform {
		return nil, nil
	}
	return []service.Account{r.account}, nil
}

func (r invocationGatewayPGAccountRepository) ListSchedulableUngroupedByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	if r.account.Platform != platform || len(r.account.GroupIDs) != 0 || len(r.account.AccountGroups) != 0 {
		return nil, nil
	}
	return []service.Account{r.account}, nil
}

func invocationGatewayPGAccountInGroup(account service.Account, groupID int64) bool {
	for _, id := range account.GroupIDs {
		if id == groupID {
			return true
		}
	}
	for _, group := range account.AccountGroups {
		if group.GroupID == groupID {
			return true
		}
	}
	return false
}

type invocationGatewayPGConcurrencyCache struct{ service.ConcurrencyCache }

func (invocationGatewayPGConcurrencyCache) AcquireAccountSlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}

func (invocationGatewayPGConcurrencyCache) ReleaseAccountSlot(context.Context, int64, string) error {
	return nil
}

func (invocationGatewayPGConcurrencyCache) GetAccountConcurrency(context.Context, int64) (int, error) {
	return 0, nil
}

func (invocationGatewayPGConcurrencyCache) GetAccountConcurrencyBatch(_ context.Context, accountIDs []int64) (map[int64]int, error) {
	loads := make(map[int64]int, len(accountIDs))
	for _, accountID := range accountIDs {
		loads[accountID] = 0
	}
	return loads, nil
}

func (invocationGatewayPGConcurrencyCache) GetAccountsLoadBatch(_ context.Context, accounts []service.AccountWithConcurrency) (map[int64]*service.AccountLoadInfo, error) {
	loads := make(map[int64]*service.AccountLoadInfo, len(accounts))
	for _, account := range accounts {
		loads[account.ID] = &service.AccountLoadInfo{AccountID: account.ID}
	}
	return loads, nil
}

type invocationGatewayPGHTTPUpstream struct {
	mu       sync.Mutex
	calls    int
	account  int64
	body     []byte
	response []byte
}

func (u *invocationGatewayPGHTTPUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	u.account = accountID
	u.body, _ = io.ReadAll(req.Body)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
			"X-Request-Id": []string{"resp-postgres-fixture"},
		},
		Body:    io.NopCloser(bytes.NewReader(u.response)),
		Request: req,
	}, nil
}

func (u *invocationGatewayPGHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, attempt int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, attempt)
}

func (u *invocationGatewayPGHTTPUpstream) snapshot() (int, int64, []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls, u.account, bytes.Clone(u.body)
}

func TestClientInvocationResponsesHandlerPostgresPersistsSettlementBeforeSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	f := settlementEventPostgresFixture(t, false)
	ctx := context.Background()
	identity, err := NewClientBillingIdentityRepository(f.db, f.client).Resolve(ctx, f.grant, f.in.GroupID)
	require.NoError(t, err)
	require.Equal(t, f.session.UserID, identity.User.ID)
	require.Equal(t, f.grant.SessionEpoch, identity.User.SessionEpoch)
	require.True(t, service.IsGroupContextValid(identity.Group))
	require.Equal(t, service.PlatformOpenAI, identity.Group.Platform)
	require.Equal(t, service.StatusActive, identity.Group.Status)
	require.True(t, identity.Group.ModelAllowlist.Allows("gpt-5.4"))
	identity.User.Concurrency = 0 // This test exercises settlement, not the Redis-backed slot limit.
	identity.Group.RateMultiplier = 1

	account := service.Account{
		ID: f.event.Usage.AccountID, Name: "responses-fixture", Platform: service.PlatformOpenAI,
		Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 2,
		GroupIDs: []int64{f.in.GroupID}, Credentials: map[string]any{
			"api_key": "fixture-provider-key", "base_url": "https://provider.fixture",
		},
	}
	accountRepo := invocationGatewayPGAccountRepository{account: account}
	usageLogs := NewUsageLogRepository(f.client, f.db)
	usageBilling := NewUsageBillingRepository(f.client, f.db)
	userRepo := NewUserRepository(f.client, f.db)
	quotaRepo := NewUserPlatformQuotaServiceAdapter(NewUserPlatformQuotaRepository(f.client))
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	concurrency := service.NewConcurrencyService(invocationGatewayPGConcurrencyCache{})
	billingCache := service.NewBillingCacheService(nil, userRepo, nil, nil, nil, nil, cfg, quotaRepo)
	t.Cleanup(billingCache.Stop)
	billing := service.NewBillingService(cfg, nil)
	upstream := &invocationGatewayPGHTTPUpstream{response: []byte(`{"id":"resp-postgres-fixture","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"handler settled response"}]}],"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15}}`)}
	gatewayService := service.NewOpenAIGatewayService(accountRepo, usageLogs, usageBilling, userRepo, nil, nil, nil,
		cfg, nil, concurrency, billing, nil, billingCache, upstream, &service.DeferredService{}, nil, nil,
		service.NewModelPricingResolver(nil, billing), nil, nil, nil, quotaRepo)
	apiKeyService := service.NewAPIKeyService(NewAPIKeyRepository(f.client, f.db), nil, nil, nil, nil, nil, cfg)
	openAIHandler := handler.NewOpenAIGatewayHandler(gatewayService, concurrency, billingCache, apiKeyService,
		nil, nil, nil, nil, cfg)
	clientGateway := handler.NewGatewayHandler(nil, nil, nil, nil, nil, concurrency, billingCache,
		nil, apiKeyService, nil, nil, nil, nil, cfg, nil)
	admit := middleware.NewClientBillingAdmission(apiKeyService, nil, cfg)
	adapter := handler.NewClientInvocationGatewayAdapter(clientGateway, openAIHandler,
		func(c *gin.Context, key *service.APIKey, grant *service.ClientGatewayGrant, groupID int64) bool {
			require.Equal(t, http.MethodPost, c.Request.Method)
			require.Equal(t, "/v1/responses", c.Request.URL.Path)
			return admit(c, key, grant, groupID)
		})

	var route service.ClientModelRoute
	require.NoError(t, json.Unmarshal([]byte(`{"routeId":"writing","target":{"kind":"model","modelId":"gpt-5.4"},"displayName":"Writing","providerDisplay":"OpenAI","purposes":["editing"],"operation":"text","availability":{"status":"available"},"protocol":"responses","transport":"json","inputKinds":["text"],"outputKinds":["text"],"parameters":[{"name":"max_output_tokens","label":"Max output tokens","editable":true,"type":"integer","default":64,"minimum":1,"maximum":512}],"prices":[],"capabilities":{}}`), &route))
	req := &service.ClientInvocationRequest{ProjectID: "project-responses", RouteID: "writing", Operation: "text",
		ConfigVersion: f.grant.ConfigVersion, EntitlementVersion: f.grant.EntitlementVersion,
		RequestID: "request-responses-postgres", Input: []service.ClientTextInputPart{{Kind: "text", Text: "Write one sentence."}},
		Parameters: map[string]json.RawMessage{}}
	textInput, err := service.ValidateClientTextInvocation(route, req)
	require.NoError(t, err)
	prepared := &service.PreparedClientInvocation{Request: req, Grant: f.grant, Text: textInput, BillingIdentity: identity,
		Resolution: &service.ClientInvocationResolution{
			Scope: service.ClientConfigurationScope{InstanceID: f.grant.InstanceID, ConfigVersion: f.grant.ConfigVersion,
				EntitlementVersion: f.grant.EntitlementVersion},
			GroupID: f.in.GroupID, ModelID: textInput.ModelID, Platform: service.PlatformOpenAI, Route: route,
		}}
	composition, err := service.NewClientInvocationComposition(service.ClientInvocationCompositionDependencies{
		Preparer: invocationServicePGPreparer{prepared: prepared}, GrantValidator: invocationServicePGGrantValidator{grant: f.grant},
		Ledger: NewClientInvocationLedgerRepository(f.db), Accounts: accountRepo, OpenAIGateway: gatewayService,
		Admission: adapter, Dispatcher: adapter, SettlementEvents: NewUsageSettlementEventRepository(f.db),
	})
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE users SET balance=20 WHERE id=$1`, f.session.UserID)
	require.NoError(t, err)
	var balanceBefore float64
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&balanceBefore))

	ingress := httptest.NewRequest(http.MethodPost, "/api/client/invoke", nil)
	ingress.RemoteAddr = "203.0.113.8:4100"
	ingress.Header.Set("User-Agent", "AutoDesign PostgreSQL integration")
	invocationCtx := handler.WithClientInvocationIngressRequest(ctx, ingress, "203.0.113.8")
	attempt, err := composition.Ledger.Reserve(invocationCtx, "adg_postgres_responses_fixture", req.RouteID, []byte(`{"fixture":"real-responses-handler"}`))
	require.NoError(t, err)
	result, err := composition.Executor.Execute(invocationCtx, "adg_postgres_responses_fixture", attempt)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationSucceeded, result.Receipt.State)
	require.Equal(t, "handler settled response", result.Result.Text)

	calls, accountID, payload := upstream.snapshot()
	require.Equal(t, 1, calls)
	require.Equal(t, account.ID, accountID)
	require.Equal(t, "gpt-5.4", jsonStringField(payload, "model"))

	var state, providerProtocol, providerModel, providerHash string
	var counterProofRaw []byte
	var usageLogID int64
	require.NoError(t, f.db.QueryRow(`SELECT state,provider_protocol,provider_model,provider_payload_hash,provider_counter_proof FROM client_invocations WHERE invocation_id=$1`, result.Receipt.InvocationID).
		Scan(&state, &providerProtocol, &providerModel, &providerHash, &counterProofRaw))
	require.Equal(t, string(service.ClientInvocationSucceeded), state)
	require.Equal(t, "responses", providerProtocol)
	require.Equal(t, "gpt-5.4", providerModel)
	require.NotEmpty(t, providerHash)
	var counterProof service.ClientInvocationCounterProof
	require.NoError(t, json.Unmarshal(counterProofRaw, &counterProof))
	require.False(t, counterProof.Required, "the fixture route has no context-count requirement")
	require.Positive(t, counterProof.OutputTokens)
	require.NoError(t, f.db.QueryRow(`SELECT usage_log_id FROM usage_settlement_events WHERE invocation_id=$1 AND state='settled'`, result.Receipt.InvocationID).Scan(&usageLogID))
	require.Positive(t, usageLogID)
	var rowCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE id=$1 AND request_id=$2 AND api_key_id=$3 AND input_tokens=12 AND output_tokens=3 AND actual_cost>0`,
		usageLogID, "client:"+result.Receipt.InvocationID, identity.ID).Scan(&rowCount))
	require.Equal(t, 1, rowCount)
	var balanceAfter float64
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&balanceAfter))
	require.Less(t, balanceAfter, balanceBefore, "the executor must observe committed handler billing and settlement before succeeding")

	_, err = f.db.Exec(`CREATE FUNCTION reject_client_invocation_settlement() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.state = 'settled' THEN RAISE EXCEPTION 'fixture settlement completion fault'; END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER reject_client_invocation_settlement BEFORE UPDATE OF state ON usage_settlement_events
		FOR EACH ROW EXECUTE FUNCTION reject_client_invocation_settlement()`)
	require.NoError(t, err)
	request2 := *req
	request2.RequestID = "request-responses-postgres-unconfirmed"
	textInput2, err := service.ValidateClientTextInvocation(route, &request2)
	require.NoError(t, err)
	prepared2 := *prepared
	prepared2.Request = &request2
	prepared2.Text = textInput2
	composition2, err := service.NewClientInvocationComposition(service.ClientInvocationCompositionDependencies{
		Preparer: invocationServicePGPreparer{prepared: &prepared2}, GrantValidator: invocationServicePGGrantValidator{grant: f.grant},
		Ledger: NewClientInvocationLedgerRepository(f.db), Accounts: accountRepo, OpenAIGateway: gatewayService,
		Admission: adapter, Dispatcher: adapter, SettlementEvents: NewUsageSettlementEventRepository(f.db),
	})
	require.NoError(t, err)
	attempt2, err := composition2.Ledger.Reserve(invocationCtx, "adg_postgres_responses_fixture", req.RouteID, []byte(`{"fixture":"settlement-ack-fault"}`))
	require.NoError(t, err)
	_, err = composition2.Executor.Execute(invocationCtx, "adg_postgres_responses_fixture", attempt2)
	require.ErrorIs(t, err, service.ErrClientInvocationOutcomeUnknown)
	var unknownCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocations WHERE state='unknown'`).Scan(&unknownCount))
	require.Equal(t, 1, unknownCount, "the handler's successful HTTP response must not override a failed durable settlement acknowledgment")
	var unsettledCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_settlement_events WHERE state <> 'settled'`).Scan(&unsettledCount))
	require.Equal(t, 1, unsettledCount)
}

func jsonStringField(body []byte, field string) string {
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		return ""
	}
	result, _ := value[field].(string)
	return result
}

// DeepSeek Flash is published as a chat-completions route. The durable client
// settlement must accept the same final payload proof that /v1/responses uses.
func TestClientInvocationChatCompletionsHandlerPostgresPersistsDeepSeekSettlement(t *testing.T) {
	gin.SetMode(gin.TestMode)
	f := settlementEventPostgresFixture(t, false)
	ctx := context.Background()
	_, err := f.db.Exec(`UPDATE groups SET platform=$1, model_allowlist='{"enabled":true,"models":["deepseek-flash"]}' WHERE id=$2`, service.PlatformDeepseek, f.in.GroupID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE client_gateway_grants SET route_ids='["writing","deepseek-flash"]' WHERE grant_id=$1`, f.grant.GrantID)
	require.NoError(t, err)
	f.grant.RouteIDs = []string{"writing", "deepseek-flash"}
	identity, err := NewClientBillingIdentityRepository(f.db, f.client).Resolve(ctx, f.grant, f.in.GroupID)
	require.NoError(t, err)
	require.Equal(t, service.PlatformDeepseek, identity.Group.Platform)
	identity.User.Concurrency = 0
	identity.Group.RateMultiplier = 1
	require.True(t, identity.Group.ModelAllowlist.Allows("deepseek-flash"))

	account := service.Account{
		ID: f.event.Usage.AccountID, Name: "deepseek-fixture", Platform: service.PlatformDeepseek,
		Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Concurrency: 2,
		GroupIDs: []int64{f.in.GroupID}, Credentials: map[string]any{
			"api_key": "fixture-provider-key", "base_url": "https://provider.fixture", "api_protocol": "chat_completions",
		},
	}
	accountRepo := invocationGatewayPGAccountRepository{account: account}
	usageLogs := NewUsageLogRepository(f.client, f.db)
	usageBilling := NewUsageBillingRepository(f.client, f.db)
	userRepo := NewUserRepository(f.client, f.db)
	quotaRepo := NewUserPlatformQuotaServiceAdapter(NewUserPlatformQuotaRepository(f.client))
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	concurrency := service.NewConcurrencyService(invocationGatewayPGConcurrencyCache{})
	billingCache := service.NewBillingCacheService(nil, userRepo, nil, nil, nil, nil, cfg, quotaRepo)
	t.Cleanup(billingCache.Stop)
	billing := service.NewBillingService(cfg, nil)
	upstream := &invocationGatewayPGHTTPUpstream{response: []byte(`{"id":"chatcmpl-fixture","object":"chat.completion","model":"deepseek-flash","choices":[{"index":0,"message":{"role":"assistant","content":"AUTODESIGN_OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":13,"completion_tokens":6,"total_tokens":19}}`)}
	gatewayService := service.NewOpenAIGatewayService(accountRepo, usageLogs, usageBilling, userRepo, nil, nil, nil,
		cfg, nil, concurrency, billing, nil, billingCache, upstream, &service.DeferredService{}, nil, nil,
		service.NewModelPricingResolver(nil, billing), nil, nil, nil, quotaRepo)
	apiKeyService := service.NewAPIKeyService(NewAPIKeyRepository(f.client, f.db), nil, nil, nil, nil, nil, cfg)
	openAIHandler := handler.NewOpenAIGatewayHandler(gatewayService, concurrency, billingCache, apiKeyService,
		nil, nil, nil, nil, cfg)
	clientGateway := handler.NewGatewayHandler(nil, nil, nil, nil, nil, concurrency, billingCache,
		nil, apiKeyService, nil, nil, nil, nil, cfg, nil)
	adapter := handler.NewClientInvocationGatewayAdapter(clientGateway, openAIHandler, middleware.NewClientBillingAdmission(apiKeyService, nil, cfg))

	var route service.ClientModelRoute
	require.NoError(t, json.Unmarshal([]byte(`{"routeId":"deepseek-flash","target":{"kind":"model","modelId":"deepseek-flash"},"displayName":"DeepSeek Flash","providerDisplay":"DeepSeek","purposes":["chat"],"operation":"text","availability":{"status":"available"},"protocol":"chat-completions","transport":"json","inputKinds":["text"],"outputKinds":["text"],"parameters":[{"name":"max_tokens","label":"Max tokens","editable":true,"type":"integer","default":16,"minimum":1,"maximum":4096,"step":1}],"limits":{"outputTokens":4096},"prices":[],"capabilities":{}}`), &route))
	req := &service.ClientInvocationRequest{ProjectID: "project-deepseek", RouteID: "deepseek-flash", Operation: "text",
		ConfigVersion: f.grant.ConfigVersion, EntitlementVersion: f.grant.EntitlementVersion,
		RequestID: "request-deepseek-postgres", Input: []service.ClientTextInputPart{{Kind: "text", Text: "Reply with exactly: AUTODESIGN_OK"}},
		Parameters: map[string]json.RawMessage{}}
	textInput, err := service.ValidateClientTextInvocation(route, req)
	require.NoError(t, err)
	prepared := &service.PreparedClientInvocation{Request: req, Grant: f.grant, Text: textInput, BillingIdentity: identity,
		Resolution: &service.ClientInvocationResolution{
			Scope: service.ClientConfigurationScope{InstanceID: f.grant.InstanceID, ConfigVersion: f.grant.ConfigVersion,
				EntitlementVersion: f.grant.EntitlementVersion},
			GroupID: f.in.GroupID, ModelID: textInput.ModelID, Platform: service.PlatformDeepseek, Route: route,
		}}
	composition, err := service.NewClientInvocationComposition(service.ClientInvocationCompositionDependencies{
		Preparer: invocationServicePGPreparer{prepared: prepared}, GrantValidator: invocationServicePGGrantValidator{grant: f.grant},
		Ledger: NewClientInvocationLedgerRepository(f.db), Accounts: accountRepo, OpenAIGateway: gatewayService,
		Admission: adapter, Dispatcher: adapter, SettlementEvents: NewUsageSettlementEventRepository(f.db),
	})
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE users SET balance=20 WHERE id=$1`, f.session.UserID)
	require.NoError(t, err)

	ingress := httptest.NewRequest(http.MethodPost, "/api/v1/client/invoke/deepseek-flash", nil)
	ingress.RemoteAddr = "203.0.113.9:4100"
	ingress.Header.Set("User-Agent", "AutoDesign PostgreSQL integration")
	invocationCtx := handler.WithClientInvocationIngressRequest(ctx, ingress, "203.0.113.9")
	raw := []byte(`{"fixture":"real-chat-completions-handler"}`)
	result, err := composition.Submit(invocationCtx, "adg_postgres_deepseek_fixture", req.RouteID, raw)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationSucceeded, result.Receipt.State)
	require.Equal(t, "AUTODESIGN_OK", result.Result.Text)
	require.Equal(t, int64(19), result.Result.Usage.TotalTokens)

	calls, accountID, payload := upstream.snapshot()
	require.Equal(t, 1, calls)
	require.Equal(t, account.ID, accountID)
	require.Equal(t, "deepseek-flash", jsonStringField(payload, "model"))

	var usageLogID int64
	require.NoError(t, f.db.QueryRow(`SELECT usage_log_id FROM usage_settlement_events WHERE invocation_id=$1 AND state='settled'`, result.Receipt.InvocationID).Scan(&usageLogID))
	var rowCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE id=$1 AND request_id=$2 AND api_key_id=$3 AND model='deepseek-flash' AND input_tokens=13 AND output_tokens=6 AND actual_cost>0`,
		usageLogID, "client:"+result.Receipt.InvocationID, identity.ID).Scan(&rowCount))
	require.Equal(t, 1, rowCount)

	replay, err := composition.Submit(invocationCtx, "adg_postgres_deepseek_fixture", req.RouteID, raw)
	require.NoError(t, err)
	require.Nil(t, replay.Result)
	require.Equal(t, result.Receipt.InvocationID, replay.Receipt.InvocationID)
	calls, _, _ = upstream.snapshot()
	require.Equal(t, 1, calls, "the same submission must not reach the provider twice")
}
