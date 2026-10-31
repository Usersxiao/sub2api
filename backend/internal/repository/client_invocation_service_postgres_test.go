//go:build unit

package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type invocationServicePGPreparer struct {
	prepared *service.PreparedClientInvocation
}

func (p invocationServicePGPreparer) Prepare(context.Context, string, string, []byte) (*service.PreparedClientInvocation, error) {
	return p.prepared, nil
}

type invocationServicePGGrantValidator struct{ grant *service.ClientGatewayGrant }

func (v invocationServicePGGrantValidator) Validate(context.Context, string, string, string) (*service.ClientGatewayGrant, error) {
	grant := *v.grant
	return &grant, nil
}

type invocationServicePGAccountRepository struct {
	service.AccountRepository
	account *service.Account
}

func (r invocationServicePGAccountRepository) GetByID(context.Context, int64) (*service.Account, error) {
	return r.account, nil
}

type invocationServicePGAdmission struct{ account *service.Account }

func (a invocationServicePGAdmission) AdmitClientInvocation(ctx context.Context, prepared *service.PreparedClientInvocation) (context.Context, func(), error) {
	selected, _, err := service.WithClientInvocationTokenSelection(ctx, prepared, a.account, prepared.Resolution.ModelID)
	return selected, func() {}, err
}

type invocationServicePGUpstream struct{ service.HTTPUpstream }

func (invocationServicePGUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	return nil, errors.New("unexpected native counter request")
}

func (invocationServicePGUpstream) DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile) (*http.Response, error) {
	return nil, errors.New("unexpected native counter request")
}

type invocationServicePGDispatcher struct {
	gateway     *service.OpenAIGatewayService
	identity    *service.APIKey
	account     *service.Account
	calls       int
	dispatchErr error
}

func (d *invocationServicePGDispatcher) DispatchClientInvocation(ctx context.Context, prepared *service.PreparedClientInvocation) ([]byte, error) {
	d.calls++
	if d.calls != 1 || prepared.Text.Protocol != "responses" {
		d.dispatchErr = service.ErrClientInvocationProviderPayload
		return nil, d.dispatchErr
	}
	request := httptest.NewRequest(http.MethodPost, "https://provider.test/v1/responses", bytes.NewReader(prepared.Text.Payload)).WithContext(ctx)
	if err := service.AuthorizeClientInvocationProviderRequest(request, d.account.ID); err != nil {
		d.dispatchErr = err
		return nil, d.dispatchErr
	}
	fingerprint, err := service.ClientInvocationFingerprint(prepared)
	if err != nil {
		d.dispatchErr = err
		return nil, d.dispatchErr
	}
	if err := d.gateway.RecordUsage(ctx, &service.OpenAIRecordUsageInput{
		Result: &service.OpenAIForwardResult{
			RequestID: "simulated-provider-response", Model: prepared.Text.ModelID, UpstreamModel: prepared.Text.ModelID,
			Duration: 12 * time.Millisecond, Usage: service.OpenAIUsage{InputTokens: 12, OutputTokens: 3},
		},
		APIKey: d.identity, User: d.identity.User, Account: d.account,
		InboundEndpoint: "/client/invoke", UpstreamEndpoint: "/v1/responses",
		RequestPayloadHash: fingerprint, QuotaPlatform: service.PlatformOpenAI,
	}); err != nil {
		d.dispatchErr = err
		return nil, d.dispatchErr
	}
	return []byte(`{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"settled response"}]}],"usage":{"input_tokens":12,"output_tokens":3}}`), nil
}

func TestClientInvocationServicePostgresRequiresPersistedGatewaySettlement(t *testing.T) {
	f := settlementEventPostgresFixture(t, false)
	ctx := context.Background()
	identity, err := NewClientBillingIdentityRepository(f.db, f.client).Resolve(ctx, f.grant, f.in.GroupID)
	require.NoError(t, err)
	require.NotNil(t, identity.User)
	require.NotNil(t, identity.Group)
	rateMultiplier := 1.0
	identity.Group.RateMultiplier = rateMultiplier

	account := &service.Account{ID: f.event.Usage.AccountID, Platform: service.PlatformOpenAI,
		Type: service.AccountTypeAPIKey, Status: service.StatusActive}
	accountRepo := invocationServicePGAccountRepository{account: account}
	usageLogs := NewUsageLogRepository(f.client, f.db)
	usageBilling := NewUsageBillingRepository(f.client, f.db)
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	billing := service.NewBillingService(cfg, nil)
	gateway := service.NewOpenAIGatewayService(accountRepo, usageLogs, usageBilling, nil, nil, nil, nil,
		cfg, nil, nil, billing, nil, &service.BillingCacheService{}, invocationServicePGUpstream{},
		&service.DeferredService{}, nil, nil, service.NewModelPricingResolver(nil, billing), nil, nil, nil, nil)

	var route service.ClientModelRoute
	require.NoError(t, json.Unmarshal([]byte(`{"routeId":"writing","target":{"kind":"model","modelId":"gpt-5.4"},"displayName":"Writing","providerDisplay":"OpenAI","purposes":["editing"],"operation":"text","availability":{"status":"available"},"protocol":"responses","transport":"json","inputKinds":["text"],"outputKinds":["text"],"parameters":[{"name":"max_output_tokens","label":"Max output tokens","editable":true,"type":"integer","default":64,"minimum":1,"maximum":512}],"prices":[],"capabilities":{}}`), &route))
	req := &service.ClientInvocationRequest{ProjectID: "project-a", RouteID: "writing", Operation: "text",
		ConfigVersion: f.grant.ConfigVersion, EntitlementVersion: f.grant.EntitlementVersion,
		RequestID: "request-service-postgres", Input: []service.ClientTextInputPart{{Kind: "text", Text: "Write one sentence."}},
		Parameters: map[string]json.RawMessage{}}
	textInput, err := service.ValidateClientTextInvocation(route, req)
	require.NoError(t, err)
	prepared := &service.PreparedClientInvocation{
		Request: req, Grant: f.grant, Text: textInput, BillingIdentity: identity,
		Resolution: &service.ClientInvocationResolution{
			Scope: service.ClientConfigurationScope{InstanceID: f.grant.InstanceID, ConfigVersion: f.grant.ConfigVersion,
				EntitlementVersion: f.grant.EntitlementVersion},
			GroupID: f.in.GroupID, ModelID: textInput.ModelID, Platform: service.PlatformOpenAI, Route: route,
		},
	}
	preparer := invocationServicePGPreparer{prepared: prepared}
	grantValidator := invocationServicePGGrantValidator{grant: f.grant}
	dispatcher := &invocationServicePGDispatcher{gateway: gateway, identity: identity, account: account}
	composition, err := service.NewClientInvocationComposition(service.ClientInvocationCompositionDependencies{
		Preparer: preparer, GrantValidator: grantValidator, Ledger: NewClientInvocationLedgerRepository(f.db),
		Accounts: accountRepo, OpenAIGateway: gateway, Admission: invocationServicePGAdmission{account: account},
		Dispatcher: dispatcher, SettlementEvents: NewUsageSettlementEventRepository(f.db),
	})
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE users SET balance=20 WHERE id=$1`, f.session.UserID)
	require.NoError(t, err)
	var balanceBefore float64
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&balanceBefore))

	attempt, err := composition.Ledger.Reserve(ctx, "adg_postgres_fixture", req.RouteID, []byte(`{"fixture":"real-database-service-path"}`))
	require.NoError(t, err)
	result, err := composition.Executor.Execute(ctx, "adg_postgres_fixture", attempt)
	require.NoError(t, dispatcher.dispatchErr)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationSucceeded, result.Receipt.State)
	require.Equal(t, "settled response", result.Result.Text)
	require.Equal(t, 1, dispatcher.calls)

	var state, providerProtocol, providerModel, providerHash string
	var usageLogID int64
	require.NoError(t, f.db.QueryRow(`SELECT state,provider_protocol,provider_model,provider_payload_hash FROM client_invocations WHERE invocation_id=$1`, result.Receipt.InvocationID).
		Scan(&state, &providerProtocol, &providerModel, &providerHash))
	require.Equal(t, string(service.ClientInvocationSucceeded), state)
	require.Equal(t, "responses", providerProtocol)
	require.Equal(t, "gpt-5.4", providerModel)
	require.NotEmpty(t, providerHash)
	require.NoError(t, f.db.QueryRow(`SELECT usage_log_id FROM usage_settlement_events WHERE invocation_id=$1 AND state='settled'`, result.Receipt.InvocationID).Scan(&usageLogID))
	require.Positive(t, usageLogID)
	var rowCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE id=$1 AND request_id=$2 AND api_key_id=$3 AND input_tokens=12 AND output_tokens=3 AND actual_cost>0`,
		usageLogID, "client:"+result.Receipt.InvocationID, identity.ID).Scan(&rowCount))
	require.Equal(t, 1, rowCount)
	var balanceAfter float64
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&balanceAfter))
	require.Less(t, balanceAfter, balanceBefore, "the real billing repository committed a balance deduction before the executor succeeded")
}
