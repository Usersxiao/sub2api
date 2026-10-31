package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

var errClientInvocationGatewayAdapterUnavailable = errors.New("client invocation gateway adapter unavailable")

const clientInvocationUserSlotContextKey = "_private_client_invocation_user_slot"

type clientInvocationProtocolHandler interface {
	Messages(*gin.Context)
	Responses(*gin.Context)
	ChatCompletions(*gin.Context)
}

type clientInvocationGatewayState struct {
	prepared  *service.PreparedClientInvocation
	context   *gin.Context
	response  *clientInvocationResponseWriter
	selection *clientInvocationSelectionLease
}

type clientInvocationSelectionLease struct {
	protocol string
	platform string
	groupID  int64
	model    string
	result   *service.AccountSelectionResult

	mu             sync.Mutex
	consumed       bool
	release        func()
	releaseSession func()
}

type clientInvocationSelectionPlan struct {
	selection      *service.AccountSelectionResult
	routingModel   string
	sessionHash    string
	releaseSession func()
}

func newClientInvocationSelectionLease(protocol, platform string, groupID int64, model string, selection *service.AccountSelectionResult) *clientInvocationSelectionLease {
	lease := &clientInvocationSelectionLease{protocol: protocol, platform: platform, groupID: groupID, model: model, result: selection}
	if selection != nil && selection.ReleaseFunc != nil {
		originalRelease := selection.ReleaseFunc
		var releaseOnce sync.Once
		lease.release = func() { releaseOnce.Do(originalRelease) }
		selection.ReleaseFunc = lease.release
	}
	return lease
}

func (l *clientInvocationSelectionLease) releaseSelection() {
	if l != nil && l.release != nil {
		l.release()
	}
}

func (l *clientInvocationSelectionLease) releaseSessionOnFailure(status int) {
	if l != nil && l.releaseSession != nil && (status < http.StatusOK || status >= http.StatusMultipleChoices) {
		l.releaseSession()
	}
}

type clientInvocationResponseWriter struct {
	headers http.Header
	body    bytes.Buffer
	status  int
	stream  *service.ClientInvocationChatStream
}

func newClientInvocationResponseWriter() *clientInvocationResponseWriter {
	return &clientInvocationResponseWriter{headers: make(http.Header)}
}

func (w *clientInvocationResponseWriter) Header() http.Header { return w.headers }

func (w *clientInvocationResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *clientInvocationResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.stream != nil && w.status >= http.StatusOK && w.status < http.StatusMultipleChoices {
		return w.stream.Write(data)
	}
	if w.body.Len()+len(data) > service.ClientInvocationMaxBytes {
		return 0, service.ErrClientInvocationResponse
	}
	return w.body.Write(data)
}

func (w *clientInvocationResponseWriter) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
}

type clientInvocationGatewayStateKey struct{}
type clientInvocationIngressRequestKey struct{}
type clientInvocationSelectionLeaseKey struct{}

type clientInvocationIngressMetadata struct {
	remoteAddr string
	clientIP   string
	headers    http.Header
}

// WithClientInvocationIngressRequest carries the authenticated HTTP request's
// network metadata into the private gateway adapter. clientIP must be resolved
// by the trusted ingress middleware; raw forwarded-IP headers are never copied.
func WithClientInvocationIngressRequest(ctx context.Context, request *http.Request, clientIP string) context.Context {
	if ctx == nil || request == nil {
		return ctx
	}
	headers := make(http.Header)
	for _, value := range request.Header.Values("User-Agent") {
		headers.Add("User-Agent", value)
	}
	return context.WithValue(ctx, clientInvocationIngressRequestKey{}, clientInvocationIngressMetadata{
		remoteAddr: request.RemoteAddr,
		clientIP:   clientIP,
		headers:    headers,
	})
}

// ClientInvocationGatewayAdapter calls the same protocol handlers as the
// public gateway. The private context, user slot, and first account selection
// are created only after the client grant and accounting identity are checked.
type ClientInvocationGatewayAdapter struct {
	gateway        clientInvocationProtocolHandler
	openAI         clientInvocationProtocolHandler
	gatewayHandler *GatewayHandler
	openAIHandler  *OpenAIGatewayHandler
	admit          middleware.ClientBillingAdmission
	concurrency    *ConcurrencyHelper
	billing        *service.BillingCacheService
}

func NewClientInvocationGatewayAdapter(gateway *GatewayHandler, openAI *OpenAIGatewayHandler, admit middleware.ClientBillingAdmission) *ClientInvocationGatewayAdapter {
	adapter := &ClientInvocationGatewayAdapter{
		gateway: gateway, openAI: openAI, gatewayHandler: gateway, openAIHandler: openAI, admit: admit,
	}
	if gateway != nil {
		adapter.concurrency = gateway.concurrencyHelper
		adapter.billing = gateway.billingCacheService
	}
	return adapter
}

func (a *ClientInvocationGatewayAdapter) AdmitClientInvocation(ctx context.Context, prepared *service.PreparedClientInvocation) (context.Context, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if a == nil || prepared == nil || prepared.BillingIdentity == nil || prepared.BillingIdentity.Group == nil || prepared.BillingIdentity.User == nil ||
		prepared.Grant == nil || prepared.Resolution == nil || prepared.Protocol() == "" || prepared.Request == nil {
		return nil, nil, service.ErrClientInvocationAdmissionUnavailable
	}
	identity := prepared.BillingIdentity
	if identity.Group.ID != prepared.Resolution.GroupID || identity.GroupID == nil || *identity.GroupID != prepared.Resolution.GroupID ||
		identity.User.ID != identity.UserID || identity.ClientConsumerID == nil || *identity.ClientConsumerID != prepared.Grant.ConsumerID ||
		identity.Key != "" || !identity.IsClientConsumer() || !identity.IsActive() {
		return nil, nil, service.ErrClientGatewayRevoked
	}
	if a.admit == nil || a.concurrency == nil || a.billing == nil {
		return nil, nil, service.ErrClientInvocationAdmissionUnavailable
	}
	if identity.Group.Platform == service.PlatformComposite {
		// The route decision is now bound to the prepared payload and budget, but
		// channel/account model mappings still need a final dispatch-bound check.
		return nil, nil, service.ErrClientInvocationAdmissionUnavailable
	}
	path, ok := clientInvocationGatewayPath(prepared.Protocol())
	if !ok || len(prepared.Payload()) == 0 || len(prepared.Payload()) > service.ClientInvocationMaxBytes {
		return nil, nil, service.ErrClientInvocationParameters
	}
	if identity.Group.ModelAllowlistEnabled() && !identity.Group.ModelAllowlist.Allows(prepared.ModelID()) {
		return nil, nil, service.ErrClientGatewayRevoked
	}
	ingress, ok := ctx.Value(clientInvocationIngressRequestKey{}).(clientInvocationIngressMetadata)
	if !ok {
		return nil, nil, service.ErrClientInvocationAdmissionUnavailable
	}

	recorder := newClientInvocationResponseWriter()
	if sink, streaming := service.ClientInvocationTextStream(ctx); streaming {
		if prepared.Protocol() != "chat-completions" || !gjson.GetBytes(prepared.Payload(), "stream").Bool() {
			return nil, nil, service.ErrClientInvocationParameters
		}
		recorder.stream = service.NewClientInvocationChatStream(sink, service.ClientInvocationReasoningStream(ctx))
	}
	c, _ := gin.CreateTestContext(recorder)
	req := &http.Request{
		Method: http.MethodPost, URL: &url.URL{Path: path},
		Body:          io.NopCloser(bytes.NewReader(prepared.Payload())),
		ContentLength: int64(len(prepared.Payload())),
	}
	req = req.WithContext(ctx)
	req.RemoteAddr = ingress.remoteAddr
	if parsedIP := net.ParseIP(ingress.clientIP); parsedIP != nil {
		req.RemoteAddr = net.JoinHostPort(parsedIP.String(), "0")
	}
	req.Header = ingress.headers.Clone()
	req.Header.Set("Content-Type", "application/json")
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "AutoDesign")
	}
	c.Request = req
	if !a.admit(c, identity, prepared.Grant, prepared.Resolution.GroupID) {
		return nil, nil, service.ErrClientInvocationAdmissionUnavailable
	}
	boundIdentity, ok := middleware.GetAPIKeyFromContext(c)
	if !ok || !clientInvocationBillingIdentityMatches(identity, boundIdentity) {
		return nil, nil, service.ErrClientGatewayRevoked
	}
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID != identity.UserID {
		return nil, nil, service.ErrClientGatewayRevoked
	}
	subscription, _ := middleware.GetSubscriptionFromContext(c)
	release, err := a.concurrency.AcquireUserSlotWithWait(c, subject.UserID, subject.Concurrency, false, new(bool))
	if err != nil {
		return nil, nil, err
	}
	if err = a.billing.CheckBillingEligibility(c.Request.Context(), identity.User, identity, identity.Group, subscription, service.QuotaPlatform(c.Request.Context(), identity)); err != nil {
		release()
		return nil, nil, err
	}
	plan, err := a.selectClientInvocationAccount(c.Request.Context(), c, prepared)
	if err != nil {
		if plan != nil && plan.selection != nil && plan.selection.ReleaseFunc != nil {
			plan.selection.ReleaseFunc()
		}
		if plan != nil && plan.releaseSession != nil {
			plan.releaseSession()
		}
		release()
		return nil, nil, err
	}
	selection := plan.selection
	if selection == nil || selection.Account == nil {
		if selection != nil && selection.ReleaseFunc != nil {
			selection.ReleaseFunc()
		}
		release()
		return nil, nil, service.ErrClientInvocationAdmissionUnavailable
	}
	var deferSelectionErr error
	if domain.UsesOpenAIGateway(prepared.Resolution.Platform) {
		deferSelectionErr = a.openAIHandler.gatewayService.DeferAccountSelectionSlot(selection)
	} else {
		deferSelectionErr = a.gatewayHandler.gatewayService.DeferAccountSelectionSlot(selection)
	}
	if deferSelectionErr != nil {
		if selection.Acquired && selection.ReleaseFunc != nil {
			selection.ReleaseFunc()
			selection.ReleaseFunc = nil
			selection.Acquired = false
		}
		if plan.releaseSession != nil {
			plan.releaseSession()
		}
		release()
		return nil, nil, deferSelectionErr
	}
	tokenContext, _, err := service.WithClientInvocationTokenSelection(c.Request.Context(), prepared, selection.Account, plan.routingModel)
	if err != nil {
		if plan.releaseSession != nil {
			plan.releaseSession()
		}
		release()
		return nil, nil, err
	}
	c.Request = c.Request.WithContext(tokenContext)
	selectionPlatform := prepared.Resolution.Platform
	if domain.UsesOpenAIGateway(selectionPlatform) {
		selectionPlatform = openAICompatibleRequestPlatform(c.Request.Context(), identity)
	}
	lease := newClientInvocationSelectionLease(prepared.Protocol(), selectionPlatform, prepared.Resolution.GroupID, plan.routingModel, selection)
	lease.releaseSession = plan.releaseSession
	var userReleaseOnce sync.Once
	userRelease := func() { userReleaseOnce.Do(release) }
	c.Set(clientInvocationUserSlotContextKey, identity.ID)
	state := &clientInvocationGatewayState{prepared: prepared, context: c, response: recorder, selection: lease}
	dispatchContext := context.WithValue(c.Request.Context(), clientInvocationGatewayStateKey{}, state)
	dispatchContext = context.WithValue(dispatchContext, clientInvocationSelectionLeaseKey{}, lease)
	c.Request = c.Request.WithContext(dispatchContext)
	return dispatchContext, func() {
		lease.releaseSelection()
		lease.releaseSessionOnFailure(recorder.status)
		userRelease()
	}, nil
}

func (a *ClientInvocationGatewayAdapter) selectClientInvocationAccount(ctx context.Context, c *gin.Context, prepared *service.PreparedClientInvocation) (*clientInvocationSelectionPlan, error) {
	if a == nil || c == nil || c.Request == nil || prepared == nil || prepared.Protocol() == "" || prepared.Resolution == nil || prepared.BillingIdentity == nil {
		return nil, service.ErrClientInvocationAdmissionUnavailable
	}
	identity := prepared.BillingIdentity
	groupID := identity.GroupID
	model := prepared.Resolution.ModelID
	body := prepared.Payload()
	if domain.UsesOpenAIGateway(prepared.Resolution.Platform) {
		h := a.openAIHandler
		if h == nil || h.gatewayService == nil {
			return nil, service.ErrClientInvocationAdmissionUnavailable
		}
		ctx, _ = h.gatewayService.WithOpenAIRequestPricingContext(ctx, groupID)
		c.Request = c.Request.WithContext(ctx)
		requestPlatform := openAICompatibleRequestPlatform(ctx, identity)
		sessionHash := h.gatewayService.GenerateSessionHash(c, body)
		requestedModel := gjson.GetBytes(body, "model").String()
		model = service.NormalizeOpenAICompatRequestedModel(requestedModel)
		switch prepared.Protocol() {
		case "messages":
			model = resolveOpenAIMessagesDispatchMappedModel(c, identity, requestedModel)
			if model == "" {
				model = service.NormalizeOpenAICompatRequestedModel(requestedModel)
			}
			sessionHash, _ = resolveOpenAIMessagesMetadataSession(c, sessionHash, h.gatewayService.ExtractSessionID(c, body), requestedModel, body)
			selection, _, err := h.gatewayService.SelectAccountWithSchedulerForCapability(ctx, groupID, "", sessionHash, model, nil,
				service.OpenAIUpstreamTransportAny, service.OpenAIEndpointCapabilityChatCompletions, false, false, true, requestPlatform)
			return &clientInvocationSelectionPlan{selection: selection, routingModel: model, sessionHash: sessionHash}, err
		case "chat-completions":
			mapping, _ := h.gatewayService.ResolveChannelMappingAndRestrict(ctx, groupID, requestedModel)
			model = openAIChannelForwardModel(mapping, requestedModel)
			selection, _, err := h.gatewayService.SelectAccountWithSchedulerForCapability(ctx, groupID, "", sessionHash, model, nil,
				service.OpenAIUpstreamTransportAny, service.OpenAIEndpointCapabilityChatCompletions, false, false, true, requestPlatform)
			return &clientInvocationSelectionPlan{selection: selection, routingModel: model, sessionHash: sessionHash}, err
		case service.ClientInvocationSearchProtocol:
			if prepared.Search == nil {
				return nil, service.ErrClientInvocationParameters
			}
			selection, _, err := h.gatewayService.SelectAccountWithSchedulerForCapability(ctx, groupID, "", sessionHash, model, nil,
				service.OpenAIUpstreamTransportHTTPSSE, service.OpenAIEndpointCapabilityAlphaSearch, false, false, false, service.PlatformOpenAI)
			if err == nil && (selection == nil || selection.Account == nil || !service.ClientInvocationSearchAccountSupported(selection.Account)) {
				if selection != nil && selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
				return nil, service.ErrClientInvocationAdmissionUnavailable
			}
			return &clientInvocationSelectionPlan{selection: selection, routingModel: model, sessionHash: sessionHash}, err
		case "responses":
			if strings.TrimSpace(gjson.GetBytes(body, "previous_response_id").String()) != "" {
				return nil, service.ErrClientInvocationParameters
			}
			mapping, _ := h.gatewayService.ResolveChannelMappingAndRestrict(ctx, groupID, requestedModel)
			model = openAIChannelForwardModel(mapping, requestedModel)
			imageIntent := service.IsExplicitImageGenerationIntent("/v1/responses", requestedModel, body)
			requiredCapability := openAIResponsesRequiredCapabilityForRequest(imageIntent, false, requestPlatform)
			selection, _, err := h.gatewayService.SelectAccountWithSchedulerForCapability(ctx, groupID, strings.TrimSpace(gjson.GetBytes(body, "previous_response_id").String()), sessionHash, model, nil,
				service.OpenAIUpstreamTransportAny, requiredCapability, false, false, !imageIntent, requestPlatform)
			return &clientInvocationSelectionPlan{selection: selection, routingModel: model, sessionHash: sessionHash}, err
		default:
			return nil, service.ErrClientInvocationParameters
		}
	}

	h := a.gatewayHandler
	if h == nil || h.gatewayService == nil {
		return nil, service.ErrClientInvocationAdmissionUnavailable
	}
	protocol := ""
	switch prepared.Protocol() {
	case "messages":
		protocol = domain.PlatformAnthropic
	case "responses":
		protocol = "responses"
	case "chat-completions":
		protocol = "chat_completions"
	default:
		return nil, service.ErrClientInvocationParameters
	}
	parsed, err := service.ParseGatewayRequest(service.NewRequestBodyRef(body), protocol)
	if err != nil || parsed == nil {
		return nil, service.ErrClientInvocationParameters
	}
	parsed.SessionContext = &service.SessionContext{ClientIP: ip.GetClientIP(c), UserAgent: c.GetHeader("User-Agent"), APIKeyID: identity.ID}
	ctx, _ = service.WithGatewayTokenRequestPricing(ctx)
	c.Request = c.Request.WithContext(ctx)
	sessionHash := h.gatewayService.GenerateSessionHash(parsed)
	platform := prepared.Resolution.Platform
	if prepared.Protocol() == "chat-completions" && platform == service.PlatformGemini && sessionHash != "" {
		sessionHash = "gemini:" + sessionHash
	}
	if prepared.Protocol() == "messages" && platform == service.PlatformGemini && sessionHash != "" {
		sessionHash = "gemini:" + sessionHash
	}
	if prepared.Protocol() == "messages" && sessionHash != "" {
		boundID, _ := h.gatewayService.GetCachedSessionAccountID(ctx, groupID, sessionHash)
		if boundID > 0 {
			id := int64(0)
			if groupID != nil {
				id = *groupID
			}
			ctx = service.WithPrefetchedStickySession(ctx, boundID, id, h.metadataBridgeEnabled())
		}
	}
	metadataUserID := ""
	sub2apiUserID := int64(0)
	if prepared.Protocol() == "messages" {
		metadataUserID = parsed.MetadataUserID
		sub2apiUserID = identity.UserID
	}
	selection, err := h.gatewayService.SelectAccountWithLoadAwareness(ctx, groupID, sessionHash, parsed.Model, nil, metadataUserID, sub2apiUserID)
	plan := &clientInvocationSelectionPlan{selection: selection, routingModel: parsed.Model, sessionHash: sessionHash}
	if selection != nil && selection.Account != nil && sessionHash != "" {
		plan.releaseSession = func() { h.gatewayService.ReleaseAccountSession(context.Background(), selection.Account, sessionHash) }
	}
	return plan, err
}

func clientInvocationSelectionFromContext(c *gin.Context, protocol, platform string, groupID *int64, model string, excluded map[int64]struct{}) (*service.AccountSelectionResult, bool, error) {
	if c == nil || c.Request == nil {
		return nil, false, nil
	}
	lease, _ := c.Request.Context().Value(clientInvocationSelectionLeaseKey{}).(*clientInvocationSelectionLease)
	if lease == nil {
		return nil, false, nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.consumed {
		// The durable invocation ledger currently binds one provider dispatch.
		// Keep the lease authoritative after its first use so a shared gateway
		// failover loop cannot fall through to the regular account scheduler.
		return nil, true, service.ErrClientInvocationAdmissionUnavailable
	}
	requestedGroupID := int64(0)
	if groupID != nil {
		requestedGroupID = *groupID
	}
	if len(excluded) != 0 || lease.protocol != protocol || lease.platform != platform || lease.groupID != requestedGroupID || lease.model != model {
		return nil, true, service.ErrClientInvocationAdmissionUnavailable
	}
	lease.consumed = true
	return lease.result, true, nil
}

func (a *ClientInvocationGatewayAdapter) DispatchClientInvocation(ctx context.Context, prepared *service.PreparedClientInvocation) ([]byte, error) {
	if a == nil || ctx == nil || prepared == nil {
		return nil, errClientInvocationGatewayAdapterUnavailable
	}
	state, _ := ctx.Value(clientInvocationGatewayStateKey{}).(*clientInvocationGatewayState)
	if state == nil || state.prepared != prepared || state.context == nil || state.response == nil || prepared.Protocol() == "" || prepared.Resolution == nil ||
		state.selection == nil || state.selection.result == nil || state.selection.result.Account == nil ||
		state.selection.protocol != prepared.Protocol() || state.selection.groupID != prepared.Resolution.GroupID ||
		!clientInvocationUserSlotPreacquired(state.context, prepared.BillingIdentity) || !service.RequiredUsageSettlement(ctx) {
		return nil, errClientInvocationGatewayAdapterUnavailable
	}
	protocolHandler := a.gateway
	if prepared.BillingIdentity == nil || prepared.BillingIdentity.Group == nil {
		return nil, errClientInvocationGatewayAdapterUnavailable
	}
	platform := prepared.Resolution.Platform
	if platform == "" {
		return nil, errClientInvocationGatewayAdapterUnavailable
	}
	if domain.UsesOpenAIGateway(platform) {
		platform = openAICompatibleRequestPlatform(ctx, prepared.BillingIdentity)
		protocolHandler = a.openAI
	}
	if state.selection.platform != platform || state.selection.model == "" {
		return nil, errClientInvocationGatewayAdapterUnavailable
	}
	if protocolHandler == nil {
		return nil, errClientInvocationGatewayAdapterUnavailable
	}
	state.context.Request = state.context.Request.WithContext(ctx)
	switch prepared.Protocol() {
	case service.ClientInvocationSearchProtocol:
		searchHandler, ok := protocolHandler.(interface{ AlphaSearch(*gin.Context) })
		if !ok {
			return nil, errClientInvocationGatewayAdapterUnavailable
		}
		searchHandler.AlphaSearch(state.context)
	case "messages":
		protocolHandler.Messages(state.context)
	case "responses":
		protocolHandler.Responses(state.context)
	case "chat-completions":
		protocolHandler.ChatCompletions(state.context)
	default:
		return nil, service.ErrClientInvocationParameters
	}
	status := state.response.status
	if status == 0 {
		status = http.StatusOK
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return nil, service.ErrClientInvocationOutcomeUnknown
	}
	if state.response.stream != nil {
		return state.response.stream.Complete()
	}
	if state.response.body.Len() == 0 {
		return nil, service.ErrClientInvocationOutcomeUnknown
	}
	return bytes.Clone(state.response.body.Bytes()), nil
}

func clientInvocationGatewayPath(protocol string) (string, bool) {
	switch protocol {
	case service.ClientInvocationSearchProtocol:
		return "/v1/alpha/search", true
	case "messages":
		return "/v1/messages", true
	case "responses":
		return "/v1/responses", true
	case "chat-completions":
		return "/v1/chat/completions", true
	default:
		return "", false
	}
}

func clientInvocationUserSlotPreacquired(c *gin.Context, apiKey *service.APIKey) bool {
	if c == nil || apiKey == nil || !apiKey.IsClientConsumer() {
		return false
	}
	value, ok := c.Get(clientInvocationUserSlotContextKey)
	identityID, valid := value.(int64)
	return ok && valid && identityID == apiKey.ID
}

func clientInvocationBillingIdentityMatches(expected, admitted *service.APIKey) bool {
	if expected == nil || admitted == nil || expected.ID <= 0 || admitted.ID != expected.ID ||
		!expected.IsClientConsumer() || !admitted.IsClientConsumer() || expected.Key != "" || admitted.Key != "" ||
		expected.Status != service.StatusActive || admitted.Status != expected.Status ||
		expected.ClientConsumerID == nil || admitted.ClientConsumerID == nil || *admitted.ClientConsumerID != *expected.ClientConsumerID ||
		expected.UserID <= 0 || admitted.UserID != expected.UserID || expected.User == nil || admitted.User == nil ||
		expected.User.ID != expected.UserID || admitted.User.ID != expected.UserID ||
		expected.User.SessionEpoch <= 0 || admitted.User.SessionEpoch != expected.User.SessionEpoch ||
		expected.User.Status != service.StatusActive || admitted.User.Status != expected.User.Status ||
		expected.User.DeletedAt != nil || admitted.User.DeletedAt != nil ||
		expected.GroupID == nil || admitted.GroupID == nil || *admitted.GroupID != *expected.GroupID ||
		expected.Group == nil || admitted.Group == nil || expected.Group.ID != *expected.GroupID || admitted.Group.ID != *expected.GroupID ||
		expected.Group.Platform == "" || admitted.Group.Platform != expected.Group.Platform ||
		expected.Group.Status != service.StatusActive || admitted.Group.Status != expected.Group.Status ||
		!service.IsGroupContextValid(expected.Group) || !service.IsGroupContextValid(admitted.Group) {
		return false
	}
	return true
}

func acquireClientInvocationAwareUserSlot(c *gin.Context, apiKey *service.APIKey, acquire func() (func(), error)) (func(), error) {
	if clientInvocationUserSlotPreacquired(c, apiKey) {
		return func() {}, nil
	}
	return acquire()
}
