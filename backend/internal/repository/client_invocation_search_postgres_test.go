//go:build unit

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type searchPGRouteResolver struct {
	resolved *service.ClientInvocationResolution
}

func (r searchPGRouteResolver) ResolveInvocation(context.Context, int64, string, string, string) (*service.ClientInvocationResolution, error) {
	return r.resolved, nil
}

func TestClientSearchHandlerPostgres(t *testing.T) {
	cases := []struct{ accountType, scenario string }{}
	for _, accountType := range []string{"pat", "apikey"} {
		for _, scenario := range []string{"success", "missing-sources", "stage-fault", "billing-fault", "usage-fault", "settlement-fault", "finish-fault", "unsupported-account", "unsupported-key-pat-metadata", "unsupported-oauth", "unsupported-setup-token"} {
			if accountType == "apikey" && strings.HasPrefix(scenario, "unsupported-") {
				continue
			}
			cases = append(cases, struct{ accountType, scenario string }{accountType, scenario})
		}
	}
	for _, tc := range cases {
		t.Run(tc.accountType+"/"+tc.scenario, func(t *testing.T) {
			accountType, scenario := tc.accountType, tc.scenario
			gin.SetMode(gin.TestMode)
			f := settlementEventPostgresFixture(t, false)
			ctx := context.Background()
			identity, err := NewClientBillingIdentityRepository(f.db, f.client).Resolve(ctx, f.grant, f.in.GroupID)
			require.NoError(t, err)
			_, err = f.db.Exec(`UPDATE users SET concurrency=0 WHERE id=$1`, f.session.UserID)
			require.NoError(t, err)
			identity.User.Concurrency = 0
			identity.Group.RateMultiplier = 1
			price := 0.023
			identity.Group.WebSearchPricePerCall = &price
			_, err = f.db.Exec(`UPDATE groups SET web_search_price_per_call=$1,rate_multiplier=1 WHERE id=$2`, price, f.in.GroupID)
			require.NoError(t, err)
			account := service.Account{ID: f.event.Usage.AccountID, Name: "search-pat-fixture", Platform: service.PlatformOpenAI,
				Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true, Concurrency: 2, GroupIDs: []int64{f.in.GroupID}, Credentials: map[string]any{
					"access_token": "at-synthetic-search-token", "auth_mode": service.OpenAIAuthModePersonalAccessToken,
					"chatgpt_account_id": "synthetic-search-account", "chatgpt_account_is_fedramp": true,
				}}
			if accountType == "apikey" {
				account.Type = service.AccountTypeAPIKey
				account.Credentials = map[string]any{"api_key": "synthetic-key", "base_url": "https://api.openai.com/v1"}
			}
			if strings.HasPrefix(scenario, "unsupported-") {
				account.Type = service.AccountTypeAPIKey
				account.Credentials = map[string]any{"api_key": "synthetic-key", "base_url": "https://provider.fixture"}
				switch scenario {
				case "unsupported-key-pat-metadata":
					account.Credentials["base_url"] = "https://api.openai.com/v1"
					account.Credentials["auth_mode"] = service.OpenAIAuthModePersonalAccessToken
					account.Credentials["access_token"] = "at-synthetic"
				case "unsupported-oauth", "unsupported-setup-token":
					account.Type = service.AccountTypeOAuth
					account.Credentials = map[string]any{"access_token": "synthetic-oauth", "chatgpt_account_id": "synthetic-account"}
					if scenario == "unsupported-setup-token" {
						account.Type = service.AccountTypeSetupToken
					}
				}
			}
			accountRepo := invocationGatewayPGAccountRepository{account: account}
			userRepo := NewUserRepository(f.client, f.db)
			quotaRepo := NewUserPlatformQuotaServiceAdapter(NewUserPlatformQuotaRepository(f.client))
			cfg := &config.Config{}
			cfg.Default.RateMultiplier = 1
			concurrency := service.NewConcurrencyService(invocationGatewayPGConcurrencyCache{})
			billingCache := service.NewBillingCacheService(nil, userRepo, nil, nil, nil, nil, cfg, quotaRepo)
			t.Cleanup(billingCache.Stop)
			billing := service.NewBillingService(cfg, nil)
			response := `data: {"type":"response.completed","response":{"status":"completed","output":[{"type":"web_search_call","status":"completed","action":{"type":"search","sources":[{"type":"url","url":"https://example.test/evidence","title":"Evidence","snippet":"A complete source"}]}},{"type":"message","content":[{"type":"output_text","text":"unexposed report","annotations":[{"type":"url_citation","url":"https://example.test/evidence"}]}]}]}}` + "\n\n"
			if scenario == "missing-sources" {
				response = `data: {"type":"response.completed","response":{"status":"completed","output":[{"type":"web_search_call","status":"completed","action":{"type":"search"}}]}}` + "\n\n"
			}
			upstream := &invocationGatewayPGHTTPUpstream{response: []byte(response)}
			gateway := service.NewOpenAIGatewayService(accountRepo, NewUsageLogRepository(f.client, f.db), NewUsageBillingRepository(f.client, f.db), userRepo, nil, nil, nil,
				cfg, nil, concurrency, billing, nil, billingCache, upstream, &service.DeferredService{}, nil, nil, service.NewModelPricingResolver(nil, billing), nil, nil, nil, quotaRepo)
			keyService := service.NewAPIKeyService(NewAPIKeyRepository(f.client, f.db), nil, nil, nil, nil, nil, cfg)
			openAI := handler.NewOpenAIGatewayHandler(gateway, concurrency, billingCache, keyService, nil, nil, nil, nil, cfg)
			clientGateway := handler.NewGatewayHandler(nil, nil, nil, nil, nil, concurrency, billingCache, nil, keyService, nil, nil, nil, nil, cfg, nil)
			adapter := handler.NewClientInvocationGatewayAdapter(clientGateway, openAI, middleware.NewClientBillingAdmission(keyService, nil, cfg))
			var route service.ClientModelRoute
			require.NoError(t, json.Unmarshal([]byte(`{"routeId":"writing","target":{"kind":"model","modelId":"gpt-5.4"},"displayName":"Search","providerDisplay":"Provider","purposes":["research"],"operation":"search","availability":{"status":"available"},"protocol":"web-search","transport":"json","inputKinds":["text"],"outputKinds":["text"],"parameters":[],"prices":[],"capabilities":{}}`), &route))
			grants := invocationServicePGGrantValidator{grant: f.grant}
			preparer := service.NewClientInvocationPreparationService(grants, searchPGRouteResolver{&service.ClientInvocationResolution{
				Scope: service.ClientConfigurationScope{InstanceID: f.grant.InstanceID, ConfigVersion: f.grant.ConfigVersion, EntitlementVersion: f.grant.EntitlementVersion}, GroupID: f.in.GroupID, ModelID: "gpt-5.4", Platform: service.PlatformOpenAI, Route: route,
			}}, NewClientBillingIdentityRepository(f.db, f.client))
			makeComposition := func() *service.ClientInvocationComposition {
				c, err := service.NewClientInvocationComposition(service.ClientInvocationCompositionDependencies{Preparer: preparer, GrantValidator: grants, Ledger: NewClientInvocationLedgerRepository(f.db), Accounts: accountRepo, OpenAIGateway: gateway, Admission: adapter, Dispatcher: adapter, SettlementEvents: NewUsageSettlementEventRepository(f.db)})
				require.NoError(t, err)
				return c
			}
			composition := makeComposition()
			if scenario == "settlement-fault" {
				_, err = f.db.Exec(`CREATE FUNCTION reject_search_settlement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='settled' THEN RAISE EXCEPTION 'synthetic ack failure'; END IF;RETURN NEW;END $$;
     CREATE TRIGGER reject_search_settlement BEFORE UPDATE OF state ON usage_settlement_events FOR EACH ROW EXECUTE FUNCTION reject_search_settlement()`)
				require.NoError(t, err)
			}
			if scenario == "billing-fault" || scenario == "usage-fault" {
				table := "usage_billing_dedup"
				if scenario == "usage-fault" {
					table = "usage_logs"
				}
				_, err = f.db.Exec(`CREATE FUNCTION reject_search_persistence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic search persistence failure'; END $$;
     CREATE TRIGGER reject_search_persistence BEFORE INSERT ON ` + table + ` FOR EACH ROW EXECUTE FUNCTION reject_search_persistence()`)
				require.NoError(t, err)
			}
			if scenario == "stage-fault" {
				_, err = f.db.Exec(`CREATE FUNCTION reject_search_stage() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic stage failure'; END $$;
     CREATE TRIGGER reject_search_stage BEFORE INSERT ON usage_settlement_events FOR EACH ROW EXECUTE FUNCTION reject_search_stage()`)
				require.NoError(t, err)
			}
			if scenario == "finish-fault" {
				_, err = f.db.Exec(`CREATE FUNCTION reject_search_finish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='succeeded' THEN RAISE EXCEPTION 'synthetic finish failure'; END IF;RETURN NEW;END $$;
     CREATE TRIGGER reject_search_finish BEFORE UPDATE OF state ON client_invocations FOR EACH ROW EXECUTE FUNCTION reject_search_finish()`)
				require.NoError(t, err)
			}
			raw, err := json.Marshal(map[string]any{"projectId": "search-project", "requestId": "search-request", "routeId": "writing", "operation": "search", "configVersion": f.grant.ConfigVersion, "entitlementVersion": f.grant.EntitlementVersion, "query": "  verify sources  ", "source": "web", "maxResults": 2})
			require.NoError(t, err)
			ingress := httptest.NewRequest(http.MethodPost, "/client/invoke/writing", nil)
			ingress.RemoteAddr = "203.0.113.8:4100"
			invocationCtx := handler.WithClientInvocationIngressRequest(ctx, ingress, "203.0.113.8")
			result, err := composition.Submit(invocationCtx, "synthetic-grant", "writing", raw)
			if strings.HasPrefix(scenario, "unsupported-") {
				require.Error(t, err)
				require.Nil(t, result)
				calls, _, _ := upstream.snapshot()
				require.Zero(t, calls)
				// Rebuilding the composition and reading the original identity do not
				// turn a rejected admission into a provider attempt or charge.
				recovered := makeComposition()
				receipt, err := recovered.Get(ctx, "synthetic-grant", "search-project", "search-request")
				require.NoError(t, err)
				require.Equal(t, service.ClientInvocationReserved, receipt.State)
				require.Equal(t, "search-request", receipt.RequestID)
				replay, err := recovered.Submit(invocationCtx, "synthetic-grant", "writing", raw)
				require.NoError(t, err)
				require.Equal(t, receipt.InvocationID, replay.Receipt.InvocationID)
				require.Nil(t, replay.Search)
				require.Nil(t, replay.Result)
				var attempts, events, usage, billingCount, journal int
				require.NoError(t, f.db.QueryRow(`SELECT
 (SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1),
 (SELECT count(*) FROM usage_settlement_events WHERE invocation_id=$1),
 (SELECT count(*) FROM usage_logs WHERE request_id='client:'||$1),
 (SELECT count(*) FROM usage_billing_dedup WHERE request_id='client:'||$1),
 (SELECT count(*) FROM usage_settlement_platform_quota_journal WHERE invocation_id=$1)`, receipt.InvocationID).Scan(&attempts, &events, &usage, &billingCount, &journal))
				require.Equal(t, []int{0, 0, 0, 0, 0}, []int{attempts, events, usage, billingCount, journal})
				var money float64
				require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&money))
				require.Equal(t, float64(20), money)
				calls, _, _ = upstream.snapshot()
				require.Zero(t, calls)
				t.Logf("scenario=%s originalID=%s state=%s provider=0 attempts=0 events=0 billing=0 usage=0 journal=0 balance=20", scenario, receipt.RequestID, receipt.State)
				return
			}
			if scenario != "success" {
				require.ErrorIs(t, err, service.ErrClientInvocationOutcomeUnknown)
			} else {
				require.NoError(t, err)
				require.Nil(t, result.Result)
				require.Equal(t, "search", result.Receipt.Operation)
				require.Equal(t, service.ClientInvocationSucceeded, result.Receipt.State)
				require.Equal(t, "  verify sources  ", result.Search.Query)
				require.Equal(t, []service.ClientSearchSource{{SourceID: "source-1", URL: "https://example.test/evidence", Title: "Evidence", Snippet: "A complete source"}}, result.Search.Sources)
			}
			calls, selected, payload := upstream.snapshot()
			require.Equal(t, 1, calls)
			require.Equal(t, account.ID, selected)
			var wire map[string]any
			require.NoError(t, json.Unmarshal(payload, &wire))
			require.Equal(t, float64(1), wire["max_tool_calls"])
			require.Equal(t, map[string]any{"type": "web_search"}, wire["tool_choice"])
			require.NotContains(t, wire, "max_output_tokens")
			var state, protocol, hash string
			var proof []byte
			require.NoError(t, f.db.QueryRow(`SELECT state,provider_protocol,provider_payload_hash,provider_counter_proof FROM client_invocations WHERE project_id='search-project'`).Scan(&state, &protocol, &hash, &proof))
			require.Equal(t, "web-search", protocol)
			require.Equal(t, service.HashUsageRequestPayload(payload), hash)
			authority := "alpha-pat-v1"
			if accountType == "apikey" {
				authority = "responses-apikey-v1"
			}
			require.JSONEq(t, `{"search_calls":1,"unit":"request","authority":"`+authority+`"}`, string(proof))
			if scenario == "success" {
				require.Equal(t, "succeeded", state)
			} else {
				require.Equal(t, "unknown", state)
			}
			var money float64
			require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&money))
			var usageCount, settled int
			require.NoError(t, f.db.QueryRow(`SELECT count(*),count(*) FILTER (WHERE state='settled') FROM usage_settlement_events WHERE invocation_id IN (SELECT invocation_id FROM client_invocations WHERE project_id='search-project')`).Scan(&usageCount, &settled))
			switch scenario {
			case "success", "finish-fault":
				require.Equal(t, 1, settled)
				require.InDelta(t, 20-price, money, 1e-8)
			case "billing-fault", "usage-fault", "settlement-fault":
				require.Equal(t, 1, usageCount)
				require.Zero(t, settled)
				expected := 20 - price
				if scenario == "billing-fault" {
					expected = 20
				}
				require.InDelta(t, expected, money, 1e-8)
			case "missing-sources", "stage-fault":
				require.Zero(t, usageCount)
				require.InDelta(t, 20, money, 1e-8)
			}
			// Reconstruct the composition to prove receipt recovery is database-backed.
			recovered := makeComposition()
			receipt, err := recovered.Get(ctx, "synthetic-grant", "search-project", "search-request")
			require.NoError(t, err)
			require.Equal(t, state, receipt.State)
			if scenario == "billing-fault" || scenario == "usage-fault" || scenario == "settlement-fault" {
				var frozenPayload, frozenHash string
				require.NoError(t, f.db.QueryRow(`SELECT payload,event_fingerprint FROM usage_settlement_events WHERE invocation_id=$1`, receipt.InvocationID).Scan(&frozenPayload, &frozenHash))
				var persisted, billed int
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, "client:"+receipt.InvocationID).Scan(&persisted))
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, "client:"+receipt.InvocationID).Scan(&billed))
				if scenario == "settlement-fault" {
					require.Equal(t, 1, persisted)
				} else {
					require.Zero(t, persisted)
				}
				if scenario == "billing-fault" {
					require.Zero(t, billed)
				} else {
					require.Equal(t, 1, billed)
				}
				// Recovery consumes the frozen price and request identity, even after
				// administrator pricing changes. It does not recover model output.
				_, err = f.db.Exec(`UPDATE groups SET web_search_price_per_call=7 WHERE id=$1`, f.in.GroupID)
				require.NoError(t, err)
				trigger, table := "reject_search_settlement", "usage_settlement_events"
				if scenario != "settlement-fault" {
					trigger, table = "reject_search_persistence", "usage_billing_dedup"
					if scenario == "usage-fault" {
						table = "usage_logs"
					}
				}
				_, err = f.db.Exec(`DROP TRIGGER ` + trigger + ` ON ` + table)
				require.NoError(t, err)
				_, err = f.db.Exec(`UPDATE usage_settlement_events SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE invocation_id=$1`, receipt.InvocationID)
				require.NoError(t, err)
				invalidator := &settlementPGInvalidator{}
				worker := service.NewUsageSettlementReconciler(NewUsageSettlementEventRepository(f.db), NewUsageBillingRepository(f.client, f.db), NewUsageLogRepository(f.client, f.db), invalidator)
				recovery, recoveryErr := worker.Reconcile(ctx, 10)
				require.NoError(t, recoveryErr)
				require.Equal(t, service.UsageSettlementRecoveryResult{Claimed: 1, Settled: 1}, recovery)
				require.Equal(t, 1, invalidator.calls)
				var finalPayload, finalHash, eventState string
				var finalMoney float64
				require.NoError(t, f.db.QueryRow(`SELECT payload,event_fingerprint,state FROM usage_settlement_events WHERE invocation_id=$1`, receipt.InvocationID).Scan(&finalPayload, &finalHash, &eventState))
				require.Equal(t, frozenPayload, finalPayload)
				require.Equal(t, frozenHash, finalHash)
				require.Equal(t, "settled", eventState)
				require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&finalMoney))
				require.InDelta(t, 20-price, finalMoney, 1e-8)
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, "client:"+receipt.InvocationID).Scan(&persisted))
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`, "client:"+receipt.InvocationID).Scan(&billed))
				require.Equal(t, 1, persisted)
				require.Equal(t, 1, billed)
				receipt, err = makeComposition().Get(ctx, "synthetic-grant", "search-project", "search-request")
				require.NoError(t, err)
				require.Equal(t, service.ClientInvocationUnknown, receipt.State)
				body, marshalErr := json.Marshal(receipt)
				require.NoError(t, marshalErr)
				require.NotContains(t, string(body), "sources")
				recovery, recoveryErr = worker.Reconcile(ctx, 10)
				require.NoError(t, recoveryErr)
				require.Equal(t, service.UsageSettlementRecoveryResult{}, recovery)
				require.Equal(t, 1, invalidator.calls)
			}
			if scenario == "stage-fault" || scenario == "finish-fault" {
				require.Nil(t, result)
				receiptJSON, marshalErr := json.Marshal(receipt)
				require.NoError(t, marshalErr)
				require.NotContains(t, string(receiptJSON), "sources")
				// Removing a write fault does not create a frozen event or restore
				// output. A settled event likewise grants no right to repeat inference.
				accountingSnapshot := func() string {
					var snapshot string
					require.NoError(t, f.db.QueryRow(`SELECT json_build_object(
     'balance',(SELECT balance::text FROM users WHERE id=$1),
     'billing',(SELECT count(*) FROM usage_billing_dedup WHERE request_id=$2),
     'usage',(SELECT count(*) FROM usage_logs WHERE request_id=$2),
     'journal',(SELECT count(*) FROM usage_settlement_platform_quota_journal WHERE invocation_id=$3),
     'event',(SELECT row_to_json(e) FROM usage_settlement_events e WHERE invocation_id=$3))::text`, f.session.UserID, "client:"+receipt.InvocationID, receipt.InvocationID).Scan(&snapshot))
					return snapshot
				}
				expectedCount := 0
				trigger, table := "reject_search_stage", "usage_settlement_events"
				if scenario == "finish-fault" {
					expectedCount = 1
					trigger, table = "reject_search_finish", "client_invocations"
					var frozenCost string
					require.NoError(t, f.db.QueryRow(`SELECT payload::json->'command'->>'balance_cost' FROM usage_settlement_events WHERE invocation_id=$1`, receipt.InvocationID).Scan(&frozenCost))
					require.Equal(t, "0.023", frozenCost)
				}
				for _, query := range []string{
					`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1`,
					`SELECT count(*) FROM usage_logs WHERE request_id=$1`,
				} {
					var count int
					require.NoError(t, f.db.QueryRow(query, "client:"+receipt.InvocationID).Scan(&count))
					require.Equal(t, expectedCount, count)
				}
				var journalCount int
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_settlement_platform_quota_journal WHERE invocation_id=$1`, receipt.InvocationID).Scan(&journalCount))
				require.Equal(t, expectedCount, journalCount)
				before := accountingSnapshot()
				_, err = f.db.Exec(`DROP TRIGGER ` + trigger + ` ON ` + table)
				require.NoError(t, err)
				invalidator := &settlementPGInvalidator{}
				worker := service.NewUsageSettlementReconciler(NewUsageSettlementEventRepository(f.db), NewUsageBillingRepository(f.client, f.db), NewUsageLogRepository(f.client, f.db), invalidator)
				for i := 0; i < 2; i++ {
					recovery, recoveryErr := worker.Reconcile(ctx, 10)
					require.NoError(t, recoveryErr)
					require.Equal(t, service.UsageSettlementRecoveryResult{}, recovery)
					require.JSONEq(t, before, accountingSnapshot())
				}
				require.Zero(t, invalidator.calls)
				receipt, err = makeComposition().Get(ctx, "synthetic-grant", "search-project", "search-request")
				require.NoError(t, err)
				afterJSON, marshalErr := json.Marshal(receipt)
				require.NoError(t, marshalErr)
				require.JSONEq(t, string(receiptJSON), string(afterJSON))
			}
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					replay, replayErr := recovered.Submit(invocationCtx, "synthetic-grant", "writing", raw)
					require.NoError(t, replayErr)
					if replayErr == nil {
						require.Nil(t, replay.Search)
						require.Nil(t, replay.Result)
						require.Equal(t, receipt.InvocationID, replay.Receipt.InvocationID)
					}
				}()
			}
			wg.Wait()
			calls, _, _ = upstream.snapshot()
			require.Equal(t, 1, calls)
			var attempts int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1`, receipt.InvocationID).Scan(&attempts))
			require.Equal(t, 1, attempts)
			t.Logf("scenario=%s providerRequests=%d attempts=%d receipt=%s settled=%d usageEvents=%d", scenario, calls, attempts, state, settled, usageCount)
		})
	}
}

func TestClientSearchLedgerProofPostgres(t *testing.T) {
	for _, operation := range []string{"text", "search"} {
		t.Run(operation, func(t *testing.T) {
			f := invocationLedgerFixture(t)
			f.in.Operation = operation
			f.reserve(t)
			var valid bool
			require.NoError(t, f.db.QueryRow(`SELECT client_invocation_text_counter_valid('{"required":false,"output_tokens":1}'::jsonb)`).Scan(&valid))
			require.True(t, valid)
			for _, proof := range []string{`{"required":false,"output_tokens":0}`, `{"search_calls":1,"unit":"request","authority":"alpha-pat-v1"}`, `{"required":false,"output_tokens":1,"search_calls":1}`} {
				require.NoError(t, f.db.QueryRow(`SELECT COALESCE(client_invocation_text_counter_valid($1::jsonb),false)`, proof).Scan(&valid))
				require.False(t, valid)
			}
			textBinding := invocationTestDispatchBinding()
			searchBinding := textBinding
			searchBinding.Protocol = service.ClientInvocationSearchProtocol
			searchBinding.CounterProof = nil
			searchBinding.SearchProof = service.NewClientInvocationSearchProof()
			good, bad := textBinding, searchBinding
			if operation == "search" {
				good, bad = searchBinding, textBinding
			}
			_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, bad)
			require.Error(t, err)
			// Direct SQL cannot mix the operation/protocol/proof before dispatch either.
			proof := `{"search_calls":1,"unit":"request","authority":"alpha-pat-v1"}`
			if operation == "search" {
				proof = `{"required":false,"output_tokens":1}`
			}
			_, err = f.db.Exec(`INSERT INTO client_invocation_attempts(invocation_id,attempt_no,state,provider_account_id,provider_platform,provider_model,provider_protocol,provider_payload_hash,provider_counter_proof)
    VALUES($1,1,'prepared',19,'openai','provider-model',$2,$3,$4::jsonb)`, f.in.InvocationID, good.Protocol, good.PayloadHash, proof)
			require.Error(t, err)
			_, err = f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, good)
			require.NoError(t, err)
			_, err = f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, good)
			require.NoError(t, err)
			_, err = f.db.Exec(`UPDATE client_invocations SET operation=$1 WHERE invocation_id=$2`, map[string]string{"text": "search", "search": "text"}[operation], f.in.InvocationID)
			require.Error(t, err)
		})
	}
}

func TestClientSearchUnauditedProofPostgres(t *testing.T) {
	for _, proof := range []string{
		`{"search_calls":1,"unit":"request","authority":"responses-apikey-v2"}`,
		`{"search_calls":1,"unit":"request","authority":"openai-responses-input-tokens"}`,
		`{"search_calls":2,"unit":"request","authority":"alpha-pat-v1"}`,
	} {
		t.Run(proof, func(t *testing.T) {
			f := invocationLedgerFixture(t)
			f.in.Operation = "search"
			f.reserve(t)
			binding := invocationTestDispatchBinding()
			binding.Protocol = service.ClientInvocationSearchProtocol
			binding.CounterProof = nil
			require.NoError(t, json.Unmarshal([]byte(proof), &binding.SearchProof))
			_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
			require.Error(t, err)
			_, err = f.db.Exec(`UPDATE client_invocations SET provider_counter_proof=$1::jsonb WHERE invocation_id=$2`, proof, f.in.InvocationID)
			require.Error(t, err)
			_, err = f.db.Exec(`INSERT INTO client_invocation_attempts(invocation_id,attempt_no,state,provider_account_id,provider_platform,provider_model,provider_protocol,provider_payload_hash,provider_counter_proof)
 VALUES($1,1,'prepared',19,'openai','provider-model','web-search',$2,$3::jsonb)`, f.in.InvocationID, binding.PayloadHash, proof)
			require.Error(t, err)
			var attempts int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1`, f.in.InvocationID).Scan(&attempts))
			require.Zero(t, attempts)
		})
	}
}

func TestClientSearchAPIKeyLedgerProofPostgres(t *testing.T) {
	for _, authority := range []string{"alpha-pat-v1", "responses-apikey-v1"} {
		t.Run(authority, func(t *testing.T) {
			f := invocationLedgerFixture(t)
			f.in.Operation = "search"
			f.reserve(t)
			binding := invocationTestDispatchBinding()
			binding.Protocol = service.ClientInvocationSearchProtocol
			binding.CounterProof = nil
			binding.SearchProof = &service.ClientInvocationSearchProof{SearchCalls: 1, Unit: "request", Authority: authority}
			_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
			require.NoError(t, err)
			other := "responses-apikey-v1"
			if authority == other {
				other = "alpha-pat-v1"
			}
			_, err = f.db.Exec(`UPDATE client_invocation_attempts SET provider_counter_proof=jsonb_set(provider_counter_proof,'{authority}',to_jsonb($1::text)) WHERE invocation_id=$2`, other, f.in.InvocationID)
			require.Error(t, err)
			changed := binding
			changed.SearchProof = &service.ClientInvocationSearchProof{SearchCalls: 1, Unit: "request", Authority: other}
			_, err = f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, changed)
			require.Error(t, err)
			_, err = f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
			require.NoError(t, err)
			_, err = f.db.Exec(`UPDATE client_invocations SET provider_counter_proof=jsonb_set(provider_counter_proof,'{authority}',to_jsonb($1::text)) WHERE invocation_id=$2`, other, f.in.InvocationID)
			require.Error(t, err)
			var proof []byte
			require.NoError(t, f.db.QueryRow(`SELECT provider_counter_proof FROM client_invocations WHERE invocation_id=$1`, f.in.InvocationID).Scan(&proof))
			require.Contains(t, string(proof), authority)
		})
	}
}

func TestClientSearchAPIKeyForwardMigrationPreservesPATPostgres(t *testing.T) {
	f := invocationLedgerFixtureAt(t, true, true, false)
	f.in.Operation = "search"
	f.reserve(t)
	binding := invocationTestDispatchBinding()
	binding.Protocol = service.ClientInvocationSearchProtocol
	binding.CounterProof = nil
	binding.SearchProof = service.NewClientInvocationSearchProof()
	_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)
	before, err := f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)
	var valid bool
	// 272 rejects the new authority before its forward migration exists.
	_, err = f.db.Exec(`UPDATE client_invocations SET provider_counter_proof='{"search_calls":1,"unit":"request","authority":"responses-apikey-v1"}'::jsonb WHERE invocation_id=$1`, f.in.InvocationID)
	require.Error(t, err)
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "273_client_invocation_apikey_search.sql"))
	require.NoError(t, err)
	_, err = f.db.Exec(string(migration))
	require.NoError(t, err)
	require.NoError(t, f.db.QueryRow(`SELECT client_invocation_search_counter_valid('{"search_calls":1,"unit":"request","authority":"responses-apikey-v1"}'::jsonb)`).Scan(&valid))
	require.True(t, valid)
	// A new repository reconstructs the original old-authority row unchanged.
	after, err := NewClientInvocationLedgerRepository(f.db).Get(context.Background(), f.grant, f.in.ProjectID, f.in.RequestID)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

// Full production migrations inspect public schema metadata, so this test uses
// its own database rather than the minimal Ent/per-schema repository fixture.
// The explicit disposable DSN is the same opt-in as the other PostgreSQL tests.
func clientSearchUpgradeDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("AUTH_SESSION_TEST_DSN")
	if dsn == "" {
		t.Skip("set AUTH_SESSION_TEST_DSN to a disposable PostgreSQL database")
	}
	base, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	name := fmt.Sprintf("client_search_upgrade_%d", time.Now().UnixNano())
	_, err = base.Exec(`CREATE DATABASE ` + name)
	require.NoError(t, err)
	db, err := sql.Open("postgres", dsn+" dbname="+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		_, err := base.Exec(`DROP DATABASE ` + name)
		require.NoError(t, err)
	})
	return db
}

func applyClientSearchMigrationsThrough(t *testing.T, db *sql.DB, version string) {
	t.Helper()
	files, err := fs.Glob(migrations.FS, "*.sql")
	require.NoError(t, err)
	selected := fstest.MapFS{}
	for _, name := range files {
		if name[:3] <= version {
			raw, err := migrations.FS.ReadFile(name)
			require.NoError(t, err)
			selected[name] = &fstest.MapFile{Data: raw}
		}
	}
	// Keep checksum, transaction, non-transactional index and advisory-lock
	// behavior in the production runner; the test only selects the release set.
	require.NoError(t, applyMigrationsFS(context.Background(), db, selected))
}

func TestClientSearchForwardMigrationsPreserveSettledTextPostgres(t *testing.T) {
	db := clientSearchUpgradeDB(t)
	ctx := context.Background()
	applyClientSearchMigrationsThrough(t, db, "266")
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	u, err := client.User.Create().SetEmail("session@example.test").SetPasswordHash("fixture-password-hash").SetBalance(20).Save(ctx)
	require.NoError(t, err)
	session := createSessionFixture(t, db, NewAuthSessionRepository(db), u.ID, "upgrade-session")
	group, err := client.Group.Create().SetName("upgrade-group").SetPlatform(service.PlatformOpenAI).Save(ctx)
	require.NoError(t, err)
	account, err := client.Account.Create().SetName("upgrade-account").SetPlatform(service.PlatformOpenAI).SetType(service.AccountTypeAPIKey).Save(ctx)
	require.NoError(t, err)
	grant := issueBillingGrant(t, db, session, "upgrade-grant")
	identity, err := NewClientBillingIdentityRepository(db, client).Resolve(ctx, grant, group.ID)
	require.NoError(t, err)
	var route service.ClientModelRoute
	require.NoError(t, json.Unmarshal([]byte(`{"routeId":"writing","target":{"kind":"model","modelId":"gpt-5.4"},"displayName":"Writing","providerDisplay":"Provider","purposes":["editing"],"operation":"text","availability":{"status":"available"},"protocol":"responses","transport":"json","inputKinds":["text"],"outputKinds":["text"],"parameters":[{"name":"max_output_tokens","label":"Max output tokens","editable":true,"type":"integer","default":5,"minimum":1,"maximum":128}],"prices":[],"capabilities":{}}`), &route))
	prepared := func(id string) *service.PreparedClientInvocation {
		req := &service.ClientInvocationRequest{ProjectID: "upgrade-project", RequestID: id, RouteID: "writing", Operation: "text",
			ConfigVersion: grant.ConfigVersion, EntitlementVersion: grant.EntitlementVersion,
			Input: []service.ClientTextInputPart{{Kind: "text", Text: "private upgrade input"}}, Parameters: map[string]json.RawMessage{}}
		text, err := service.ValidateClientTextInvocation(route, req)
		require.NoError(t, err)
		return &service.PreparedClientInvocation{Request: req, Grant: grant, BillingIdentity: identity, Text: text,
			Resolution: &service.ClientInvocationResolution{Scope: service.ClientConfigurationScope{InstanceID: grant.InstanceID,
				ConfigVersion: grant.ConfigVersion, EntitlementVersion: grant.EntitlementVersion}, GroupID: group.ID, ModelID: "gpt-5.4", Platform: service.PlatformOpenAI, Route: route}}
	}
	inputs := []*service.PreparedClientInvocation{prepared("legacy-unknown"), prepared("settled-text")}
	legacyFingerprint, err := service.ClientInvocationFingerprint(inputs[0])
	require.NoError(t, err)
	// A pre-267 unknown has a known payload binding but no token proof or
	// recorded dispatch timestamp. Later migrations must retain that absence.
	_, err = db.Exec(`INSERT INTO client_invocations
 (invocation_id,instance_id,user_id,client_id,consumer_id,project_id,request_id,route_id,operation,
 config_version,entitlement_version,fingerprint,billing_identity_id,group_id,owner_token_hash,state,
 created_at,updated_at,lease_expires_at,idempotency_expires_at)
 VALUES('legacy-invocation',$1,$2,$3,$4,'upgrade-project','legacy-unknown','writing','text',$5,$6,$7,$8,$9,$10,
 'reserved',clock_timestamp(),clock_timestamp(),clock_timestamp()+INTERVAL '30 seconds',clock_timestamp()+INTERVAL '7 days')`,
		grant.InstanceID, u.ID, grant.ClientID, grant.ConsumerID, grant.ConfigVersion, grant.EntitlementVersion,
		legacyFingerprint, identity.ID, group.ID, service.GatewayTokenHash(invocationTestOwner('L')))
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE client_invocations SET state='dispatched',provider_account_id=$1,provider_platform='openai',
 provider_model='gpt-5.4',provider_protocol='responses',provider_payload_hash=$2,dispatch_deadline=clock_timestamp()+INTERVAL '15 minutes'
 WHERE invocation_id='legacy-invocation'`, account.ID, sessionHash("legacy payload"))
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE client_invocations SET state='unknown' WHERE invocation_id='legacy-invocation'`)
	require.NoError(t, err)
	applyClientSearchMigrationsThrough(t, db, "271")
	var proofAbsent, dispatchTimeAbsent bool
	require.NoError(t, db.QueryRow(`SELECT provider_counter_proof IS NULL,dispatch_started_at IS NULL FROM client_invocation_attempts
 WHERE invocation_id='legacy-invocation' AND attempt_no=1`).Scan(&proofAbsent, &dispatchTimeAbsent))
	require.True(t, proofAbsent)
	require.True(t, dispatchTimeAbsent)

	repo := NewClientInvocationLedgerRepository(db)
	fingerprint, err := service.ClientInvocationFingerprint(inputs[1])
	require.NoError(t, err)
	owner := invocationTestOwner('S')
	claim, err := repo.Reserve(ctx, grant, &service.ClientInvocationReservation{InvocationID: "settled-invocation", ProjectID: "upgrade-project",
		RequestID: "settled-text", RouteID: "writing", Operation: "text", ConfigVersion: grant.ConfigVersion, EntitlementVersion: grant.EntitlementVersion,
		Fingerprint: fingerprint, BillingIdentityID: identity.ID, GroupID: group.ID, OwnerToken: owner})
	require.NoError(t, err)
	require.True(t, claim.Owned)
	inputTokens, contextTokens := int64(10), int64(128)
	binding := service.ClientInvocationDispatchBinding{AccountID: account.ID, Platform: service.PlatformOpenAI, ModelID: "gpt-5.4",
		Protocol: "responses", PayloadHash: service.HashUsageRequestPayload(inputs[1].Text.Payload), CounterProof: &service.ClientInvocationCounterProof{
			Required: true, InputTokens: &inputTokens, OutputTokens: 5, ContextTokens: &contextTokens,
			AuthorityKind: "tokenizer", AuthorityID: "synthetic-tokenizer", AuthorityRevision: "v1"}}
	_, err = startInvocationDispatch(ctx, repo, grant, claim.Record.InvocationID, owner, binding)
	require.NoError(t, err)
	cost := 0.01234567
	usage := &service.UsageLog{UserID: u.ID, APIKeyID: identity.ID, AccountID: account.ID, RequestID: "client:" + claim.Record.InvocationID,
		Model: "gpt-5.4", GroupID: &group.ID, InputTokens: 10, OutputTokens: 5, InputCost: cost / 2, OutputCost: cost / 2,
		TotalCost: cost, ActualCost: cost, RateMultiplier: 1, BillingType: service.BillingTypeBalance, RequestType: service.RequestTypeSync}
	command := &service.UsageBillingCommand{RequestID: usage.RequestID, APIKeyID: identity.ID, UserID: u.ID, AccountID: account.ID,
		AccountType: service.AccountTypeAPIKey, Model: usage.Model, InputTokens: 10, OutputTokens: 5, BalanceCost: cost, RequestPayloadHash: binding.PayloadHash}
	event, err := service.FreezeUsageSettlementEventWithPlatform(claim.Record.InvocationID, command, usage, service.PlatformOpenAI)
	require.NoError(t, err)
	f := &settlementEventPGFixture{invocationPostgresFixture: &invocationPostgresFixture{db: db},
		events: NewUsageSettlementEventRepository(db), billing: NewUsageBillingRepository(client, db), logs: NewUsageLogRepository(client, db),
		binding: &service.UsageSettlementBinding{InvocationID: event.InvocationID, OwnerToken: owner}, event: event}
	f.stage(t)
	settlement := f.persist(t)
	require.NoError(t, f.events.RecordPlatformQuota(ctx, event.InvocationID, f.event.LeaseToken))
	require.NoError(t, f.events.Complete(ctx, event.InvocationID, f.event.LeaseToken, settlement))
	text := "private settled text"
	_, err = repo.Finish(ctx, event.InvocationID, owner, service.ClientInvocationOutcome{State: service.ClientInvocationSucceeded, Text: &text})
	require.NoError(t, err)

	snapshot := func() map[string]string {
		out := map[string]string{}
		for _, table := range []string{"client_invocations", "client_invocation_attempts", "usage_settlement_events", "usage_billing_dedup",
			"usage_logs", "usage_settlement_platform_quota_journal", "users", "api_keys", "accounts", "groups",
			"auth_sessions", "client_gateway_consumers", "client_gateway_grants"} {
			var rows string
			filter := ""
			if table == "client_invocations" || table == "client_invocation_attempts" {
				filter = ` WHERE invocation_id IN ('legacy-invocation','settled-invocation')`
			}
			require.NoError(t, db.QueryRow(`SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb)::text FROM `+table+` r`+filter).Scan(&rows))
			out[table] = rows
		}
		return out
	}
	before := snapshot()
	var events, dedup, logs, journal int
	require.NoError(t, db.QueryRow(`SELECT (SELECT count(*) FROM usage_settlement_events WHERE state='settled'),
 (SELECT count(*) FROM usage_billing_dedup),(SELECT count(*) FROM usage_logs),
 (SELECT count(*) FROM usage_settlement_platform_quota_journal)`).Scan(&events, &dedup, &logs, &journal))
	require.Equal(t, []int{1, 1, 1, 1}, []int{events, dedup, logs, journal})
	var balance string
	require.NoError(t, db.QueryRow(`SELECT balance::text FROM users WHERE id=$1`, u.ID).Scan(&balance))
	require.Equal(t, "19.98765433", balance)
	var history271 string
	require.NoError(t, db.QueryRow(`SELECT jsonb_agg(to_jsonb(m) ORDER BY filename)::text FROM schema_migrations m WHERE filename<'272'`).Scan(&history271))
	searchReservation := service.ClientInvocationReservation{InvocationID: "upgrade-search-pat", ProjectID: "search-project", RequestID: "upgrade-search-pat",
		RouteID: "writing", Operation: "search", ConfigVersion: grant.ConfigVersion, EntitlementVersion: grant.EntitlementVersion,
		Fingerprint: sessionHash("search input"), BillingIdentityID: identity.ID, GroupID: group.ID, OwnerToken: invocationTestOwner('P')}
	_, err = repo.Reserve(ctx, grant, &searchReservation)
	require.ErrorContains(t, err, "client_invocations_operation_check", "271 has no search operation")
	apiKeyReservation := searchReservation
	apiKeyReservation.InvocationID, apiKeyReservation.RequestID, apiKeyReservation.OwnerToken = "upgrade-search-apikey", "upgrade-search-apikey", invocationTestOwner('K')
	searchBinding := binding
	searchBinding.Protocol, searchBinding.CounterProof, searchBinding.SearchProof = service.ClientInvocationSearchProtocol, nil, service.NewClientInvocationSearchProof()
	apiKeyBinding := searchBinding
	apiKeyBinding.SearchProof = &service.ClientInvocationSearchProof{SearchCalls: 1, Unit: "request", Authority: "responses-apikey-v1"}
	var patBefore *service.ClientInvocationRecord
	for _, version := range []string{"272", "273"} {
		applyClientSearchMigrationsThrough(t, db, version)
		require.Equal(t, before, snapshot(), "migration %s must preserve every original business row", version)
		var originalHistory string
		require.NoError(t, db.QueryRow(`SELECT jsonb_agg(to_jsonb(m) ORDER BY filename)::text FROM schema_migrations m WHERE filename<'272'`).Scan(&originalHistory))
		require.Equal(t, history271, originalHistory, "original checksums and applied_at must remain unchanged")
		for _, table := range []string{"client_invocations", "client_invocation_attempts"} {
			for _, change := range []string{`provider_account_id=provider_account_id+1`, `provider_model='changed-model'`,
				`provider_payload_hash='` + sessionHash("changed payload") + `'`, `provider_counter_proof='{"required":false,"output_tokens":6}'::jsonb`} {
				_, err := db.Exec(`UPDATE ` + table + ` SET ` + change + ` WHERE invocation_id='settled-invocation'`)
				require.ErrorContains(t, err, "immutable", "version=%s table=%s change=%s", version, table, change)
			}
		}
		for _, input := range inputs {
			fresh := service.NewClientInvocationLedgerService(invocationServicePGPreparer{input}, invocationServicePGGrantValidator{grant}, NewClientInvocationLedgerRepository(db), nil)
			composition := &service.ClientInvocationComposition{Ledger: fresh, Executor: service.NewClientInvocationExecutor(fresh, nil, nil, f.events)}
			receipt, err := composition.Get(ctx, "synthetic-grant", input.Request.ProjectID, input.Request.RequestID)
			require.NoError(t, err)
			wantState := service.ClientInvocationSucceeded
			if input.Request.RequestID == "legacy-unknown" {
				wantState = service.ClientInvocationUnknown
			}
			require.Equal(t, wantState, receipt.State)
			replay, err := composition.Submit(ctx, "synthetic-grant", "writing", []byte(`{}`))
			require.NoError(t, err)
			require.Equal(t, receipt, replay.Receipt)
			require.Nil(t, replay.Result)
			require.Nil(t, replay.Search)
			raw, err := json.Marshal(replay)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			require.Len(t, fields, 1)
			require.Contains(t, fields, "receipt")
			require.NotContains(t, string(raw), "private")
		}
		if version == "272" {
			for _, reservation := range []service.ClientInvocationReservation{searchReservation, apiKeyReservation} {
				claim, err := repo.Reserve(ctx, grant, &reservation)
				require.NoError(t, err)
				require.True(t, claim.Owned)
			}
			_, err = repo.PrepareDispatch(ctx, grant, apiKeyReservation.InvocationID, apiKeyReservation.OwnerToken, apiKeyBinding)
			require.ErrorContains(t, err, "search requires its request counter proof")
			patBefore, err = startInvocationDispatch(ctx, repo, grant, searchReservation.InvocationID, searchReservation.OwnerToken, searchBinding)
			require.NoError(t, err)
		} else {
			patAfter, err := NewClientInvocationLedgerRepository(db).Get(ctx, grant, searchReservation.ProjectID, searchReservation.RequestID)
			require.NoError(t, err)
			require.Equal(t, patBefore, patAfter)
			apiKey, err := startInvocationDispatch(ctx, repo, grant, apiKeyReservation.InvocationID, apiKeyReservation.OwnerToken, apiKeyBinding)
			require.NoError(t, err)
			require.Equal(t, service.ClientInvocationDispatched, apiKey.State)
			var parentProof, attemptProof string
			require.NoError(t, db.QueryRow(`SELECT i.provider_counter_proof::text,a.provider_counter_proof::text FROM client_invocations i
 JOIN client_invocation_attempts a USING(invocation_id) WHERE i.invocation_id=$1 AND a.attempt_no=1`, apiKeyReservation.InvocationID).Scan(&parentProof, &attemptProof))
			require.JSONEq(t, `{"search_calls":1,"unit":"request","authority":"responses-apikey-v1"}`, parentProof)
			require.Equal(t, parentProof, attemptProof)
		}
		require.Equal(t, before, snapshot(), "receipt recovery/replay must not write or charge")
		// The full migration history, including checksums/applied_at, is also
		// stable when the production runner is invoked again at this release.
		var historyBefore, historyAfter string
		require.NoError(t, db.QueryRow(`SELECT jsonb_agg(to_jsonb(m) ORDER BY filename)::text FROM schema_migrations m`).Scan(&historyBefore))
		applyClientSearchMigrationsThrough(t, db, version)
		require.NoError(t, db.QueryRow(`SELECT jsonb_agg(to_jsonb(m) ORDER BY filename)::text FROM schema_migrations m`).Scan(&historyAfter))
		require.Equal(t, historyBefore, historyAfter)
		hashes := map[string]string{}
		for table, rows := range before {
			hashes[table] = sessionHash(rows)
		}
		audit, err := json.Marshal(map[string]any{"version": version, "settledInvocation": event.InvocationID, "legacyState": "unknown",
			"balance": balance, "businessTableSha256": hashes, "history271Sha256": sessionHash(history271),
			"settledEvents": events, "billingDedup": dedup, "usageLogs": logs, "quotaJournal": journal, "receiptOnly": true, "providerRequests": 0})
		require.NoError(t, err)
		t.Logf("upgrade-audit=%s", audit)
	}
}
