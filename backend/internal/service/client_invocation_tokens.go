package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/tidwall/gjson"
)

const (
	ClientInvocationTokenCountExact     = "exact"
	ClientInvocationTokenCountEstimated = "estimated"
	ClientInvocationTokenCountUnknown   = "unknown"
)

var (
	ErrClientInvocationContextLimit          = infraerrors.BadRequest("MODEL_CONTEXT_LIMIT_EXCEEDED", "The input and output budget exceed this model's context limit")
	ErrClientInvocationTokenCountUnavailable = infraerrors.New(503, "MODEL_TOKEN_COUNT_UNAVAILABLE", "Trusted model token counting is temporarily unavailable")
	ErrClientInvocationProviderPayload       = infraerrors.New(503, "MODEL_PROVIDER_PAYLOAD_UNVERIFIED", "The final provider request could not be verified")
)

type ClientInvocationProviderPayloadAuthorizer interface {
	AuthorizeClientInvocationProviderPayload(context.Context, int64, string, []byte) error
}

type clientInvocationProviderPayloadAuthorizerKey struct{}
type clientInvocationProviderRequestAuthorizationKey struct{}

type clientInvocationProviderRequestAuthorization struct {
	accountID   int64
	endpoint    string
	payloadHash string
}

func WithClientInvocationProviderPayloadAuthorizer(ctx context.Context, authorizer ClientInvocationProviderPayloadAuthorizer) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, clientInvocationProviderPayloadAuthorizerKey{}, authorizer)
}

// AuthorizeClientInvocationProviderRequest checks the final request body at the
// last shared transport boundary. It restores the body for the real HTTP client.
func AuthorizeClientInvocationProviderRequest(request *http.Request, accountID int64) error {
	if request == nil || request.Context() == nil {
		return ErrClientInvocationProviderPayload
	}
	authorizer, ok := request.Context().Value(clientInvocationProviderPayloadAuthorizerKey{}).(ClientInvocationProviderPayloadAuthorizer)
	if !ok || authorizer == nil {
		return nil
	}
	if request.Method != http.MethodPost || request.URL == nil || request.Body == nil || accountID <= 0 {
		return ErrClientInvocationProviderPayload
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, ClientInvocationMaxBytes+1))
	_ = request.Body.Close()
	if err != nil || len(body) == 0 || len(body) > ClientInvocationMaxBytes || !validClientInvocationProviderJSON(body) {
		return ErrClientInvocationProviderPayload
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	request.GetBody = nil

	hash := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(hash[:])
	endpoint := request.URL.Path
	// Verify the final search origin too, before a durable attempt or a cached
	// authorization can be used. A matching path on a relay is not authority.
	if gate, ok := authorizer.(*clientInvocationProviderPayloadGate); ok && gate.prepared != nil && gate.prepared.Search != nil {
		selection, selected := ClientInvocationTokenSelectionFromContext(gate.ctx)
		host := "chatgpt.com"
		if selection.SearchAuthority == "responses-apikey-v1" {
			host = "api.openai.com"
		}
		if !selected || clientInvocationSearchEndpoint(selection.SearchAuthority) == "" ||
			request.URL.Scheme != "https" || request.URL.Host != host ||
			endpoint != clientInvocationSearchEndpoint(selection.SearchAuthority) || request.URL.RawPath != "" ||
			request.URL.User != nil || request.URL.RawQuery != "" || request.URL.ForceQuery || request.URL.Fragment != "" ||
			(request.Host != "" && request.Host != host) {
			return ErrClientInvocationProviderPayload
		}
	}
	if prior, exists := request.Context().Value(clientInvocationProviderRequestAuthorizationKey{}).(clientInvocationProviderRequestAuthorization); exists {
		if prior.accountID != accountID || prior.endpoint != endpoint || prior.payloadHash != payloadHash {
			return ErrClientInvocationProviderPayload
		}
		return nil
	}
	if err = authorizer.AuthorizeClientInvocationProviderPayload(request.Context(), accountID, endpoint, body); err != nil {
		return err
	}
	requestContext := WithHTTPUpstreamRedirectsDisabled(request.Context())
	authorizedRequest := request.WithContext(context.WithValue(requestContext, clientInvocationProviderRequestAuthorizationKey{}, clientInvocationProviderRequestAuthorization{
		accountID: accountID, endpoint: endpoint, payloadHash: payloadHash,
	}))
	*request = *authorizedRequest
	return nil
}

func ClientInvocationProviderPayloadAuthorized(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	authorizer, ok := ctx.Value(clientInvocationProviderPayloadAuthorizerKey{}).(interface{ Authorized() bool })
	return ok && authorizer.Authorized()
}

func ClientInvocationProviderPayloadAuthorizationRequired(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	authorizer, ok := ctx.Value(clientInvocationProviderPayloadAuthorizerKey{}).(ClientInvocationProviderPayloadAuthorizer)
	return ok && authorizer != nil
}

// TokenCounter is a trusted server dependency, never supplied by an HTTP caller.
// An implementation may claim exact only with an explicitly registered model
// tokenizer and its complete protocol framing, or an authoritative native count
// for the same selected model/account and effective payload used for dispatch.
// Generic tokenizer fallback, token/character estimates, and provider estimates
// must report estimated/unknown. No production counter is registered here.
type ClientInvocationTokenCounter interface {
	CountClientInvocationTokens(context.Context, ClientInvocationTokenInput) (*ClientInvocationTokenCount, error)
}

type ClientInvocationTokenInput struct {
	AccountID   int64           `json:"-"`
	GroupID     int64           `json:"-"`
	Platform    string          `json:"-"`
	ModelID     string          `json:"-"`
	Protocol    string          `json:"-"`
	PayloadHash string          `json:"-"`
	Payload     json.RawMessage `json:"-"`
}

// Authority identifies the audited tokenizer/model registry revision, or native
// counting authority revision. These are assertions of the injected trusted
// implementation; echoing a client-provided string never establishes trust.
type ClientInvocationTokenAuthority struct {
	Kind     string `json:"-"` // tokenizer or native
	ID       string `json:"-"`
	Revision string `json:"-"`
}

type ClientInvocationTokenCount struct {
	AccountID   int64                          `json:"-"`
	GroupID     int64                          `json:"-"`
	Platform    string                         `json:"-"`
	ModelID     string                         `json:"-"`
	Protocol    string                         `json:"-"`
	PayloadHash string                         `json:"-"`
	Accuracy    string                         `json:"-"`
	InputTokens int64                          `json:"-"`
	Authority   ClientInvocationTokenAuthority `json:"-"`
}

// ClientInvocationCounterProof is persisted with the one-time dispatch binding.
// It contains only bounded counts and trusted counter identity, never prompt
// content or the upstream response.
type ClientInvocationCounterProof struct {
	Required          bool   `json:"required"`
	InputTokens       *int64 `json:"input_tokens,omitempty"`
	OutputTokens      int64  `json:"output_tokens"`
	ContextTokens     *int64 `json:"context_tokens,omitempty"`
	AuthorityKind     string `json:"authority_kind,omitempty"`
	AuthorityID       string `json:"authority_id,omitempty"`
	AuthorityRevision string `json:"authority_revision,omitempty"`
}

// A nil InputTokens explicitly means no context limit required token counting;
// it is never represented as a supposedly exact zero. This private result is
// admission evidence, not a public receipt or a settlement acknowledgment.
type ClientInvocationTokenBudget struct {
	AccountID     int64                           `json:"-"`
	GroupID       int64                           `json:"-"`
	Platform      string                          `json:"-"`
	ModelID       string                          `json:"-"`
	Protocol      string                          `json:"-"`
	PayloadHash   string                          `json:"-"`
	InputTokens   *int64                          `json:"-"`
	OutputTokens  int64                           `json:"-"`
	ContextTokens *int64                          `json:"-"`
	Authority     *ClientInvocationTokenAuthority `json:"-"`
}

type ClientInvocationTokenSelection struct {
	AccountID       int64
	GroupID         int64
	Platform        string
	Protocol        string
	DispatchModel   string
	ModelID         string
	SearchAuthority string
}

type clientInvocationTokenSelectionKey struct{}

type clientInvocationProviderPayloadGate struct {
	mu         sync.Mutex
	ledger     *ClientInvocationLedgerService
	counter    ClientInvocationTokenCounter
	ctx        context.Context
	token      string
	attempt    *ClientInvocationAttempt
	prepared   *PreparedClientInvocation
	started    bool
	authorized bool
	rejection  error
}

func (g *clientInvocationProviderPayloadGate) Authorized() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.authorized
}

func (g *clientInvocationProviderPayloadGate) markPreparedNotDispatched() {
	if g == nil || g.ledger == nil || g.attempt == nil {
		return
	}
	base := g.ctx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(base), 2*time.Second)
	defer cancel()
	_ = g.ledger.MarkPreparedNotDispatched(ctx, g.attempt)
}

func (g *clientInvocationProviderPayloadGate) AuthorizeClientInvocationProviderPayload(_ context.Context, accountID int64, endpoint string, payload []byte) (err error) {
	if g == nil || g.ledger == nil || g.prepared == nil || g.prepared.Protocol() == "" || g.prepared.Resolution == nil ||
		g.prepared.Request == nil || g.prepared.Grant == nil || g.attempt == nil || g.attempt.record == nil {
		return ErrClientInvocationProviderPayload
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	defer func() {
		if err != nil {
			g.rejection = err
		}
	}()
	if g.started {
		return ErrClientInvocationProviderPayload
	}
	if len(payload) == 0 || len(payload) > ClientInvocationMaxBytes || !validClientInvocationProviderJSON(payload) {
		return ErrClientInvocationProviderPayload
	}
	selection, ok := ClientInvocationTokenSelectionFromContext(g.ctx)
	if !ok || accountID != selection.AccountID || accountID <= 0 || selection.GroupID != g.prepared.Resolution.GroupID || selection.Platform != g.prepared.Resolution.Platform || selection.Protocol != g.prepared.Protocol() || !clientLabel(selection.ModelID) || !clientLabel(selection.DispatchModel) {
		return ErrClientInvocationProviderPayload
	}
	var protocol, modelID, binding string
	var proof *ClientInvocationCounterProof
	var searchProof *ClientInvocationSearchProof
	if g.prepared.Search != nil {
		expected, buildErr := buildClientSearchProviderBody(g.prepared.Search.Payload, selection.ModelID)
		if buildErr != nil || (clientInvocationSearchEndpoint(selection.SearchAuthority) == "" || endpoint != clientInvocationSearchEndpoint(selection.SearchAuthority)) || !bytes.Equal(expected, payload) || selection.Platform != PlatformOpenAI {
			return ErrClientInvocationProviderPayload
		}
		protocol, modelID = ClientInvocationSearchProtocol, selection.ModelID
		hash := sha256.Sum256(payload)
		binding = hex.EncodeToString(hash[:])
		searchProof = &ClientInvocationSearchProof{1, "request", selection.SearchAuthority}
	} else {
		protocol, ok = clientInvocationProviderProtocol(endpoint)
		if !ok {
			return ErrClientInvocationProviderPayload
		}
		modelID = strings.TrimSpace(gjson.GetBytes(payload, "model").String())
		if modelID == "" || modelID != selection.ModelID {
			return ErrClientInvocationProviderPayload
		}
		outputTokens, ok := clientInvocationProviderOutputTokens(protocol, payload)
		if !ok || outputTokens != g.prepared.Text.OutputTokens {
			return ErrClientInvocationProviderPayload
		}
		hash := sha256.Sum256(payload)
		binding = hex.EncodeToString(hash[:])
		limit := g.prepared.Resolution.Route.Limits.ContextTokens
		proof = &ClientInvocationCounterProof{Required: limit != nil, OutputTokens: outputTokens}
		if limit != nil {
			if outputTokens > *limit || g.counter == nil {
				if outputTokens > *limit {
					return ErrClientInvocationContextLimit
				}
				return ErrClientInvocationTokenCountUnavailable
			}
			input := ClientInvocationTokenInput{
				AccountID: accountID, GroupID: selection.GroupID, Platform: selection.Platform,
				ModelID: modelID, Protocol: protocol, PayloadHash: binding, Payload: bytes.Clone(payload),
			}
			count, err := g.counter.CountClientInvocationTokens(g.ctx, input)
			if err != nil || !bytes.Equal(input.Payload, payload) || !validClientInvocationTokenCount(input, count) {
				return ErrClientInvocationTokenCountUnavailable
			}
			if count.InputTokens > *limit-outputTokens {
				return ErrClientInvocationContextLimit
			}
			inputTokens, contextTokens := count.InputTokens, *limit
			proof.InputTokens, proof.ContextTokens = &inputTokens, &contextTokens
			proof.AuthorityKind, proof.AuthorityID, proof.AuthorityRevision = count.Authority.Kind, count.Authority.ID, count.Authority.Revision
		}

	}
	if err := g.ledger.recheckGrant(g.ctx, g.token, g.prepared.Grant, g.prepared.Request.RouteID); err != nil {
		return err
	}
	dispatchBinding := ClientInvocationDispatchBinding{
		AccountID: accountID, Platform: selection.Platform, ModelID: modelID, Protocol: protocol, PayloadHash: binding,
		CounterProof: proof, SearchProof: searchProof,
	}
	preparedRecord, err := g.ledger.ledger.PrepareDispatch(g.ctx, g.prepared.Grant, g.attempt.record.InvocationID, g.attempt.ownerToken, dispatchBinding)
	if err != nil {
		g.markPreparedNotDispatched()
		return err
	}
	if !sameInvocationRecordIdentity(g.attempt.record, preparedRecord) || preparedRecord.State != ClientInvocationReserved ||
		preparedRecord.ProviderPayloadHash != "" ||
		!g.ledger.recordMatchesGrant(preparedRecord, g.prepared.Grant, g.prepared.Request.ProjectID, g.prepared.Request.RequestID) {
		return ErrClientInvocationConflict
	}
	record, err := g.ledger.ledger.StartDispatch(g.ctx, g.prepared.Grant, g.attempt.record.InvocationID, g.attempt.ownerToken, dispatchBinding)
	if err != nil {
		g.markPreparedNotDispatched()
		return err
	}
	if !sameInvocationRecordIdentity(g.attempt.record, record) || record.State != ClientInvocationDispatched ||
		record.ProviderAccountID != dispatchBinding.AccountID || record.ProviderPlatform != dispatchBinding.Platform ||
		record.ProviderModel != dispatchBinding.ModelID || record.ProviderProtocol != dispatchBinding.Protocol ||
		record.ProviderPayloadHash != dispatchBinding.PayloadHash || !sameClientInvocationCounterProof(record.ProviderCounterProof, dispatchBinding.CounterProof) || !sameClientInvocationSearchProof(record.ProviderSearchProof, dispatchBinding.SearchProof) ||
		!g.ledger.recordMatchesGrant(record, g.prepared.Grant, g.prepared.Request.ProjectID, g.prepared.Request.RequestID) {
		return ErrClientInvocationConflict
	}
	g.attempt.record = record
	if err = g.ledger.recheckGrant(g.ctx, g.token, g.prepared.Grant, g.prepared.Request.RouteID); err != nil {
		g.started = true
		return err
	}
	g.started = true
	g.authorized = true
	return nil
}

func clientInvocationProviderPayloadRejection(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	gate, ok := ctx.Value(clientInvocationProviderPayloadAuthorizerKey{}).(*clientInvocationProviderPayloadGate)
	if !ok || gate == nil {
		return nil
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return safeClientInvocationProviderPayloadRejection(gate.rejection)
}

func safeClientInvocationProviderPayloadRejection(err error) error {
	for _, safe := range []error{
		ErrClientInvocationProviderPayload, ErrClientInvocationContextLimit, ErrClientInvocationTokenCountUnavailable,
		ErrClientGatewayScope, ErrClientGatewayRevoked, ErrClientGatewayChanged, ErrClientGatewayUnavailable,
		ErrClientInvocationConflict, ErrClientInvocationExpired, ErrClientInvocationOwner, ErrTokenExpired, ErrTokenRevoked,
	} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	return nil
}

func clientInvocationProviderProtocol(endpoint string) (string, bool) {
	path := strings.TrimRight(endpoint, "/")
	switch {
	case path == "/v1/messages" || strings.HasSuffix(path, "/v1/messages"):
		return "messages", true
	case path == "/v1/responses" || strings.HasSuffix(path, "/v1/responses"):
		return "responses", true
	case path == "/v1/chat/completions" || strings.HasSuffix(path, "/v1/chat/completions"):
		return "chat-completions", true
	default:
		return "", false
	}
}

func validClientInvocationProviderJSON(payload []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if !consumeUniqueClientInvocationJSONValue(decoder) {
		return false
	}
	_, err := decoder.Token()
	return errors.Is(err, io.EOF)
}

func consumeUniqueClientInvocationJSONValue(decoder *json.Decoder) bool {
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return true
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return false
			}
			key, ok := keyToken.(string)
			if !ok {
				return false
			}
			if _, duplicate := keys[key]; duplicate {
				return false
			}
			keys[key] = struct{}{}
			if !consumeUniqueClientInvocationJSONValue(decoder) {
				return false
			}
		}
		closing, err := decoder.Token()
		return err == nil && closing == json.Delim('}')
	case '[':
		for decoder.More() {
			if !consumeUniqueClientInvocationJSONValue(decoder) {
				return false
			}
		}
		closing, err := decoder.Token()
		return err == nil && closing == json.Delim(']')
	default:
		return false
	}
}

func clientInvocationProviderOutputTokens(protocol string, payload []byte) (int64, bool) {
	var names []string
	switch protocol {
	case "responses":
		names = []string{"max_output_tokens"}
	case "messages":
		names = []string{"max_tokens"}
	case "chat-completions":
		names = []string{"max_completion_tokens", "max_tokens"}
	default:
		return 0, false
	}
	var value int64
	found := false
	for _, name := range names {
		result := gjson.GetBytes(payload, name)
		if !result.Exists() {
			continue
		}
		if found || result.Type != gjson.Number {
			return 0, false
		}
		parsed, err := strconv.ParseInt(result.Raw, 10, 64)
		if err != nil || parsed <= 0 {
			return 0, false
		}
		value, found = parsed, true
	}
	return value, found
}

func validClientInvocationTokenCount(input ClientInvocationTokenInput, count *ClientInvocationTokenCount) bool {
	return count != nil && count.AccountID == input.AccountID && count.GroupID == input.GroupID && count.Platform == input.Platform &&
		count.ModelID == input.ModelID && count.Protocol == input.Protocol && count.PayloadHash == input.PayloadHash &&
		count.Accuracy == ClientInvocationTokenCountExact && count.InputTokens >= 0 &&
		clientContains([]string{"tokenizer", "native"}, count.Authority.Kind) &&
		clientIdentifier.MatchString(count.Authority.ID) && clientIdentifier.MatchString(count.Authority.Revision)
}

func ValidateClientInvocationCounterProof(proof *ClientInvocationCounterProof) bool {
	if proof == nil || proof.OutputTokens <= 0 {
		return false
	}
	if !proof.Required {
		return proof.InputTokens == nil && proof.ContextTokens == nil && proof.AuthorityKind == "" && proof.AuthorityID == "" && proof.AuthorityRevision == ""
	}
	return proof.InputTokens != nil && *proof.InputTokens >= 0 && proof.ContextTokens != nil && *proof.ContextTokens >= proof.OutputTokens &&
		*proof.InputTokens <= *proof.ContextTokens-proof.OutputTokens &&
		clientContains([]string{"tokenizer", "native"}, proof.AuthorityKind) &&
		clientIdentifier.MatchString(proof.AuthorityID) && clientIdentifier.MatchString(proof.AuthorityRevision)
}

func sameClientInvocationCounterProof(first, second *ClientInvocationCounterProof) bool {
	if first == nil || second == nil {
		return first == second
	}
	return first.Required == second.Required && first.OutputTokens == second.OutputTokens &&
		sameOptionalInt64(first.InputTokens, second.InputTokens) && sameOptionalInt64(first.ContextTokens, second.ContextTokens) &&
		first.AuthorityKind == second.AuthorityKind && first.AuthorityID == second.AuthorityID && first.AuthorityRevision == second.AuthorityRevision
}

func sameOptionalInt64(first, second *int64) bool {
	return (first == nil && second == nil) || (first != nil && second != nil && *first == *second)
}

func WithClientInvocationTokenSelection(ctx context.Context, prepared *PreparedClientInvocation, account *Account, dispatchModel string) (context.Context, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if prepared == nil || prepared.Resolution == nil || prepared.Protocol() == "" || account == nil || account.ID <= 0 ||
		account.Platform != prepared.Resolution.Platform || !clientLabel(dispatchModel) {
		return nil, "", ErrClientInvocationParameters
	}
	if prepared.Search != nil && !ClientInvocationSearchAccountSupported(account) {
		return nil, "", ErrClientInvocationParameters
	}
	modelID, err := clientInvocationEffectiveModelForAccount(prepared.Resolution, account)
	if err != nil || !clientLabel(modelID) {
		return nil, "", ErrClientInvocationParameters
	}
	selection := ClientInvocationTokenSelection{
		AccountID: account.ID, GroupID: prepared.Resolution.GroupID, Platform: account.Platform,
		Protocol: prepared.Protocol(), DispatchModel: dispatchModel, ModelID: modelID,
	}
	if prepared.Search != nil {
		selection.SearchAuthority = clientInvocationSearchProofForAccount(account).Authority
	}
	return context.WithValue(ctx, clientInvocationTokenSelectionKey{}, selection), modelID, nil
}

func ClientInvocationTokenSelectionFromContext(ctx context.Context) (ClientInvocationTokenSelection, bool) {
	if ctx == nil {
		return ClientInvocationTokenSelection{}, false
	}
	selection, ok := ctx.Value(clientInvocationTokenSelectionKey{}).(ClientInvocationTokenSelection)
	return selection, ok
}

func clientInvocationEffectiveModelForAccount(resolution *ClientInvocationResolution, account *Account) (string, error) {
	modelID, err := clientInvocationEffectiveModel(resolution)
	if err != nil || account == nil {
		return "", ErrClientInvocationParameters
	}
	if len(resolution.AccountMappings) == 0 {
		return modelID, nil
	}
	inputModel := modelID
	if resolution.ChannelMapping != nil {
		inputModel = resolution.ChannelMapping.MappedModel
	}
	for _, mapping := range resolution.AccountMappings {
		if mapping.AccountID == account.ID && mapping.AccountPlatform == account.Platform && mapping.InputModel == inputModel {
			return mapping.UpstreamModel, nil
		}
	}
	return "", ErrClientInvocationParameters
}

// CheckClientInvocationTokenBudget sends no model request. The future execution
// adapter must call it against the freshly prepared, final effective payload
// before crossing StartDispatch, and must dispatch exactly that counted payload.
func CheckClientInvocationTokenBudget(ctx context.Context, prepared *PreparedClientInvocation, counter ClientInvocationTokenCounter) (*ClientInvocationTokenBudget, error) {
	if prepared == nil || prepared.Request == nil || prepared.Resolution == nil || prepared.Text == nil {
		return nil, ErrClientInvocationParameters
	}
	if !clientInvocationRouteMatchesBillingIdentity(prepared) {
		return nil, ErrClientInvocationParameters
	}
	route := prepared.Resolution.Route
	effectiveModel, err := clientInvocationEffectiveModel(prepared.Resolution)
	if err != nil {
		return nil, err
	}
	canonical, err := validateClientTextInvocationForModel(route, prepared.Request, effectiveModel, prepared.Resolution.Platform)
	if err != nil {
		return nil, err
	}
	text := prepared.Text
	if text.ModelID != canonical.ModelID || text.Protocol != canonical.Protocol ||
		text.Prompt != canonical.Prompt || text.OutputTokens != canonical.OutputTokens || !bytes.Equal(text.Payload, canonical.Payload) {
		return nil, ErrClientInvocationParameters
	}
	selection, hasSelection := ClientInvocationTokenSelectionFromContext(ctx)
	if hasSelection {
		selectedModel, selectErr := clientInvocationEffectiveModelForAccount(prepared.Resolution, &Account{ID: selection.AccountID, Platform: selection.Platform})
		if selectErr != nil || selection.AccountID <= 0 || selection.GroupID != prepared.Resolution.GroupID ||
			selection.Protocol != text.Protocol || selection.Platform != prepared.Resolution.Platform ||
			!clientLabel(selection.DispatchModel) || selection.ModelID != selectedModel {
			return nil, ErrClientInvocationParameters
		}
		canonical, err = validateClientTextInvocationForModel(route, prepared.Request, selection.ModelID, selection.Platform)
		if err != nil || canonical.Protocol != text.Protocol || canonical.Prompt != text.Prompt || canonical.OutputTokens != text.OutputTokens {
			return nil, ErrClientInvocationParameters
		}
	}
	hash := sha256.Sum256(canonical.Payload)
	binding := hex.EncodeToString(hash[:])
	budget := &ClientInvocationTokenBudget{ModelID: canonical.ModelID, Protocol: canonical.Protocol, PayloadHash: binding, OutputTokens: canonical.OutputTokens}
	if hasSelection {
		budget.AccountID, budget.GroupID, budget.Platform = selection.AccountID, selection.GroupID, selection.Platform
	}
	if route.Limits.ContextTokens == nil {
		return budget, nil
	}
	limit := *route.Limits.ContextTokens
	budget.ContextTokens = &limit
	if canonical.OutputTokens > limit {
		return nil, ErrClientInvocationContextLimit
	}
	if counter == nil {
		return nil, ErrClientInvocationTokenCountUnavailable
	}
	input := ClientInvocationTokenInput{ModelID: canonical.ModelID, Protocol: canonical.Protocol, PayloadHash: binding, Payload: bytes.Clone(canonical.Payload)}
	if hasSelection {
		input.AccountID, input.GroupID, input.Platform = selection.AccountID, selection.GroupID, selection.Platform
	}
	count, err := counter.CountClientInvocationTokens(ctx, input)
	if err != nil || count == nil || count.ModelID != canonical.ModelID || count.Protocol != canonical.Protocol ||
		count.AccountID != input.AccountID || count.GroupID != input.GroupID || count.Platform != input.Platform ||
		count.PayloadHash != binding || count.Accuracy != ClientInvocationTokenCountExact || count.InputTokens < 0 ||
		!clientContains([]string{"tokenizer", "native"}, count.Authority.Kind) ||
		!clientIdentifier.MatchString(count.Authority.ID) || !clientIdentifier.MatchString(count.Authority.Revision) ||
		!bytes.Equal(input.Payload, canonical.Payload) {
		return nil, ErrClientInvocationTokenCountUnavailable
	}
	// Subtraction avoids overflow even if an authority returns MaxInt64 tokens.
	if count.InputTokens > limit-canonical.OutputTokens {
		return nil, ErrClientInvocationContextLimit
	}
	tokens, authority := count.InputTokens, count.Authority
	budget.InputTokens, budget.Authority = &tokens, &authority
	return budget, nil
}
