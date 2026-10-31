//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type invocationGrantValidatorFixture struct {
	grant     *service.ClientGatewayGrant
	err       error
	calls     int
	token     string
	routeID   string
	operation string
}

func (f *invocationGrantValidatorFixture) Validate(_ context.Context, token, routeID, operation string) (*service.ClientGatewayGrant, error) {
	f.calls++
	f.token, f.routeID, f.operation = token, routeID, operation
	return f.grant, f.err
}

func TestClientInvocationHandlerRequiresAndValidatesRestrictedGrant(t *testing.T) {
	gin.SetMode(gin.TestMode)
	validator := &invocationGrantValidatorFixture{grant: &service.ClientGatewayGrant{GrantID: "grant-fixture"}}
	router := gin.New()
	router.POST("/client/invoke/:route_id", NewClientInvocationHandler(validator).Invoke)

	unauthenticated := httptest.NewRecorder()
	router.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodPost, "/client/invoke/deepseek-flash", strings.NewReader(`{"secret":"body"}`)))
	require.Equal(t, http.StatusUnauthorized, unauthenticated.Code)
	require.Equal(t, "no-store", unauthenticated.Header().Get("Cache-Control"))
	require.Zero(t, validator.calls)
	require.NotContains(t, unauthenticated.Body.String(), "body")

	request := httptest.NewRequest(http.MethodPost, "/client/invoke/deepseek-flash", strings.NewReader(`{"secret":"body"}`))
	request.Header.Set("Authorization", "Bearer adg_fixture")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, 1, validator.calls)
	require.Equal(t, "adg_fixture", validator.token)
	require.Equal(t, "deepseek-flash", validator.routeID)
	require.Equal(t, "invoke", validator.operation)
	require.NotContains(t, response.Body.String(), "body")
	require.NotContains(t, response.Body.String(), "adg_fixture")
}

func TestClientInvocationHandlerMapsGrantRejectionAndFailsClosedWithoutValidator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name       string
		handler    *ClientInvocationHandler
		wantStatus int
		wantCode   string
	}{
		{name: "revoked grant", handler: NewClientInvocationHandler(&invocationGrantValidatorFixture{err: service.ErrClientGatewayRevoked}), wantStatus: http.StatusForbidden, wantCode: "MODEL_ROUTE_REVOKED"},
		{name: "missing validator", handler: NewClientInvocationHandler(nil), wantStatus: http.StatusServiceUnavailable, wantCode: "MODEL_ADMISSION_UNAVAILABLE"},
		{name: "empty validation result", handler: NewClientInvocationHandler(&invocationGrantValidatorFixture{}), wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := gin.New()
			router.POST("/client/invoke/:route_id", test.handler.Invoke)
			request := httptest.NewRequest(http.MethodPost, "/client/invoke/route", nil)
			request.Header.Set("Authorization", "Bearer adg_fixture")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			require.Equal(t, test.wantStatus, response.Code)
			require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
			if test.wantCode != "" {
				require.Contains(t, response.Body.String(), test.wantCode)
			}
			require.NotContains(t, response.Body.String(), "adg_fixture")
		})
	}

	validator := &invocationGrantValidatorFixture{err: errors.New("internal grant lookup detail")}
	router := gin.New()
	router.POST("/client/invoke/:route_id", NewClientInvocationHandler(validator).Invoke)
	request := httptest.NewRequest(http.MethodPost, "/client/invoke/route", nil)
	request.Header.Set("Authorization", "Bearer adg_fixture")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.NotContains(t, response.Body.String(), "internal grant lookup detail")
}

type invocationRunnerFixture struct {
	execution *service.ClientInvocationExecution
	receipt   *service.ClientInvocationReceipt
	err       error
	submits   int
	gets      int
	token     string
	routeID   string
	raw       string
	ingress   bool
	project   string
	request   string
}

func (f *invocationRunnerFixture) Submit(ctx context.Context, token, routeID string, raw []byte) (*service.ClientInvocationExecution, error) {
	f.submits++
	f.token, f.routeID, f.raw = token, routeID, string(raw)
	_, f.ingress = ctx.Value(clientInvocationIngressRequestKey{}).(clientInvocationIngressMetadata)
	return f.execution, f.err
}

func (f *invocationRunnerFixture) Get(_ context.Context, token, projectID, requestID string) (*service.ClientInvocationReceipt, error) {
	f.gets++
	f.token, f.project, f.request = token, projectID, requestID
	return f.receipt, f.err
}

func invocationRunnerRouter(runner *invocationRunnerFixture, grants *invocationGrantValidatorFixture) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := newClientInvocationHandlerWithRunner(grants, runner)
	router.POST("/client/invoke/:route_id", h.Invoke)
	router.GET("/client/invocations/:project_id/:request_id", h.GetInvocation)
	return router
}

func invocationReceiptFixture(state string) *service.ClientInvocationReceipt {
	now := time.Now().UTC()
	return &service.ClientInvocationReceipt{InvocationID: "invocation-fixture", ProjectID: "project-a", RequestID: "request-a",
		RouteID: "deepseek-flash", Operation: "text", ConfigVersion: "cfg-1", EntitlementVersion: "ent-1", State: state,
		CreatedAt: now, UpdatedAt: now, IdempotencyExpiresAt: now.Add(time.Hour)}
}

func TestClientInvocationHandlerReturnsCompletedOutputAndUsageForOwnedSubmission(t *testing.T) {
	runner := &invocationRunnerFixture{execution: &service.ClientInvocationExecution{
		Receipt: invocationReceiptFixture(service.ClientInvocationSucceeded),
		Result:  &service.ClientInvocationResponse{Text: "AUTODESIGN_OK", Usage: service.ClientInvocationUsage{InputTokens: 13, OutputTokens: 6, TotalTokens: 19}},
	}}
	grants := &invocationGrantValidatorFixture{}
	body := `{"projectId":"project-a","routeId":"deepseek-flash","operation":"text","configVersion":"cfg-1","entitlementVersion":"ent-1","requestId":"request-a","input":[{"kind":"text","text":"ping"}],"parameters":{}}`
	request := httptest.NewRequest(http.MethodPost, "/client/invoke/deepseek-flash", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer adg_fixture")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	invocationRunnerRouter(runner, grants).ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, 1, runner.submits)
	require.Zero(t, grants.calls, "the composition owns grant validation for an enabled route")
	require.Equal(t, "adg_fixture", runner.token)
	require.Equal(t, "deepseek-flash", runner.routeID)
	require.JSONEq(t, body, runner.raw)
	require.True(t, runner.ingress, "trusted ingress metadata must reach the private gateway adapter")
	var envelope struct {
		Code int `json:"code"`
		Data struct {
			Receipt map[string]any `json:"receipt"`
			Output  struct {
				Text  string `json:"text"`
				Usage struct {
					InputTokens  int64 `json:"inputTokens"`
					OutputTokens int64 `json:"outputTokens"`
					TotalTokens  int64 `json:"totalTokens"`
				} `json:"usage"`
			} `json:"output"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
	require.Zero(t, envelope.Code)
	require.Equal(t, "succeeded", envelope.Data.Receipt["state"])
	require.Equal(t, "invocation-fixture", envelope.Data.Receipt["invocationId"])
	require.Equal(t, "AUTODESIGN_OK", envelope.Data.Output.Text)
	require.Equal(t, int64(13), envelope.Data.Output.Usage.InputTokens)
	require.Equal(t, int64(6), envelope.Data.Output.Usage.OutputTokens)
	require.Equal(t, int64(19), envelope.Data.Output.Usage.TotalTokens)
	require.NotContains(t, response.Body.String(), "adg_fixture")
	require.NotContains(t, response.Body.String(), "cost")
}

func TestClientInvocationHandlerReplayReturnsReceiptWithoutOutput(t *testing.T) {
	runner := &invocationRunnerFixture{execution: &service.ClientInvocationExecution{Receipt: invocationReceiptFixture(service.ClientInvocationDispatched)}}
	request := httptest.NewRequest(http.MethodPost, "/client/invoke/deepseek-flash", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer adg_fixture")
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response := httptest.NewRecorder()
	invocationRunnerRouter(runner, &invocationGrantValidatorFixture{}).ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), `"state":"dispatched"`)
	require.NotContains(t, response.Body.String(), `"output"`)
}

func TestClientInvocationHandlerMapsRunnerOutcomesWithoutEchoingInput(t *testing.T) {
	for _, test := range []struct {
		name        string
		contentType string
		err         error
		wantStatus  int
		wantCode    string
		wantSubmits int
	}{
		{name: "outcome unknown", contentType: "application/json", err: service.ErrClientInvocationOutcomeUnknown, wantStatus: http.StatusBadGateway, wantCode: "MODEL_REQUEST_OUTCOME_UNKNOWN", wantSubmits: 1},
		{name: "idempotency conflict", contentType: "application/json", err: service.ErrClientInvocationConflict, wantStatus: http.StatusConflict, wantCode: "MODEL_IDEMPOTENCY_CONFLICT", wantSubmits: 1},
		{name: "revoked grant", contentType: "application/json", err: service.ErrClientGatewayRevoked, wantStatus: http.StatusForbidden, wantCode: "MODEL_ROUTE_REVOKED", wantSubmits: 1},
		{name: "non-json body", contentType: "text/plain", wantStatus: http.StatusBadRequest, wantCode: "MODEL_PARAMETERS_INVALID"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := &invocationRunnerFixture{err: test.err}
			request := httptest.NewRequest(http.MethodPost, "/client/invoke/deepseek-flash", strings.NewReader(`{"input":[{"kind":"text","text":"private prompt"}]}`))
			request.Header.Set("Authorization", "Bearer adg_fixture")
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()
			invocationRunnerRouter(runner, &invocationGrantValidatorFixture{}).ServeHTTP(response, request)
			require.Equal(t, test.wantStatus, response.Code)
			require.Contains(t, response.Body.String(), test.wantCode)
			require.Equal(t, test.wantSubmits, runner.submits)
			require.NotContains(t, response.Body.String(), "private prompt")
			require.NotContains(t, response.Body.String(), "adg_fixture")
		})
	}
}

func TestClientInvocationHandlerRecoveryQueryUsesOriginalIdentity(t *testing.T) {
	runner := &invocationRunnerFixture{receipt: invocationReceiptFixture(service.ClientInvocationUnknown)}
	router := invocationRunnerRouter(runner, &invocationGrantValidatorFixture{})
	request := httptest.NewRequest(http.MethodGet, "/client/invocations/project-a/request-a", nil)
	request.Header.Set("Authorization", "Bearer adg_fixture")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, 1, runner.gets)
	require.Zero(t, runner.submits, "recovery must never dispatch")
	require.Equal(t, "project-a", runner.project)
	require.Equal(t, "request-a", runner.request)
	require.Contains(t, response.Body.String(), `"state":"unknown"`)

	unauthenticated := httptest.NewRecorder()
	router.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, "/client/invocations/project-a/request-a", nil))
	require.Equal(t, http.StatusUnauthorized, unauthenticated.Code)
	require.Equal(t, 1, runner.gets)

	closed := gin.New()
	closed.GET("/client/invocations/:project_id/:request_id", NewClientInvocationHandler(&invocationGrantValidatorFixture{}).GetInvocation)
	closedResponse := httptest.NewRecorder()
	closed.ServeHTTP(closedResponse, request.Clone(request.Context()))
	require.Equal(t, http.StatusServiceUnavailable, closedResponse.Code)
	require.Contains(t, closedResponse.Body.String(), "MODEL_ADMISSION_UNAVAILABLE")
}

func TestClientInvocationHandlerSearchJSONAndReceiptOnlyRecovery(t *testing.T) {
	receipt := invocationReceiptFixture(service.ClientInvocationSucceeded)
	receipt.Operation = "search"
	output := &service.ClientSearchOutput{Query: "query", Sources: []service.ClientSearchSource{}, Citations: []service.ClientSearchCitation{}, Usage: service.ClientSearchUsage{SearchCalls: 1, Unit: "request"}}
	runner := &invocationRunnerFixture{execution: &service.ClientInvocationExecution{Receipt: receipt, Search: output}, receipt: receipt}
	router := invocationRunnerRouter(runner, &invocationGrantValidatorFixture{})
	req := httptest.NewRequest(http.MethodPost, "/client/invoke/deepseek-flash", strings.NewReader(`{"query":"query"}`))
	req.Header.Set("Authorization", "Bearer adg_fixture")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)
	require.Equal(t, http.StatusOK, res.Code)
	var body struct {
		Data struct {
			Receipt *service.ClientInvocationReceipt `json:"receipt"`
			Output  json.RawMessage                  `json:"output"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &body))
	expected, err := json.Marshal(output)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(body.Data.Output))
	require.Equal(t, "search", body.Data.Receipt.Operation)
	get := httptest.NewRequest(http.MethodGet, "/client/invocations/project-a/request-a", nil)
	get.Header.Set("Authorization", "Bearer adg_fixture")
	recovered := httptest.NewRecorder()
	router.ServeHTTP(recovered, get)
	require.Equal(t, http.StatusOK, recovered.Code)
	require.NotContains(t, recovered.Body.String(), `"output"`)
	require.NotContains(t, recovered.Body.String(), `"query"`)
	require.Equal(t, 1, runner.submits)
}
