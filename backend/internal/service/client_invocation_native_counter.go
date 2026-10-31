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
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	clientInvocationOpenAIResponsesTokenAuthorityID       = "openai-responses-input-tokens"
	clientInvocationOpenAIResponsesTokenAuthorityRevision = "responses-input-tokens-v1"
	clientInvocationNativeTokenCountTimeout               = 20 * time.Second
)

// ClientInvocationTokenAccountRepository provides the account lookup required
// to verify the selected provider account before native token counting.
type ClientInvocationTokenAccountRepository interface {
	GetByID(context.Context, int64) (*Account, error)
}

type clientInvocationTokenAccountLookup = ClientInvocationTokenAccountRepository

// ClientInvocationNativeTokenCounter trusts only OpenAI's native Responses
// input-token endpoint for an official OpenAI API Key account. Other account
// types and payload shapes stay unavailable until their counting authority is
// independently verified.
type ClientInvocationNativeTokenCounter struct {
	accounts clientInvocationTokenAccountLookup
	gateway  *OpenAIGatewayService
}

func NewClientInvocationNativeTokenCounter(accounts ClientInvocationTokenAccountRepository, gateway *OpenAIGatewayService) *ClientInvocationNativeTokenCounter {
	return &ClientInvocationNativeTokenCounter{accounts: accounts, gateway: gateway}
}

func (s *ClientInvocationNativeTokenCounter) CountClientInvocationTokens(ctx context.Context, input ClientInvocationTokenInput) (*ClientInvocationTokenCount, error) {
	if s == nil || s.accounts == nil || s.gateway == nil || s.gateway.httpUpstream == nil || input.AccountID <= 0 ||
		input.GroupID <= 0 || input.Platform != PlatformOpenAI || input.Protocol != "responses" ||
		!clientLabel(input.ModelID) || len(input.Payload) == 0 || len(input.Payload) > ClientInvocationMaxBytes ||
		!validClientInvocationProviderJSON(input.Payload) {
		return nil, ErrClientInvocationTokenCountUnavailable
	}

	hash := sha256.Sum256(input.Payload)
	if input.PayloadHash != hex.EncodeToString(hash[:]) {
		return nil, ErrClientInvocationTokenCountUnavailable
	}

	countBody, err := clientInvocationOpenAIResponsesCountBody(input)
	if err != nil {
		return nil, ErrClientInvocationTokenCountUnavailable
	}
	account, err := s.accounts.GetByID(ctx, input.AccountID)
	if err != nil || account == nil || account.ID != input.AccountID || account.Platform != PlatformOpenAI ||
		account.Type != AccountTypeAPIKey || account.IsShadow() || account.Status != StatusActive ||
		!isOfficialOpenAIResponsesCountBaseURL(account.GetOpenAIBaseURL()) {
		return nil, ErrClientInvocationTokenCountUnavailable
	}

	requestCtx, cancel := context.WithTimeout(WithHTTPUpstreamRedirectsDisabled(ctx), clientInvocationNativeTokenCountTimeout)
	defer cancel()
	token, authMode, err := s.gateway.GetAccessToken(requestCtx, account)
	if err != nil || authMode != "apikey" || strings.TrimSpace(token) == "" {
		return nil, ErrClientInvocationTokenCountUnavailable
	}
	req, err := s.gateway.buildInputTokensUpstreamRequest(requestCtx, nil, account, countBody, token)
	if err != nil {
		return nil, ErrClientInvocationTokenCountUnavailable
	}
	if !isExactOpenAIResponsesCountEndpoint(req.URL) {
		return nil, ErrClientInvocationTokenCountUnavailable
	}

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.gateway.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil || resp == nil || resp.Body == nil {
		return nil, ErrClientInvocationTokenCountUnavailable
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, ErrClientInvocationTokenCountUnavailable
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, ClientInvocationMaxBytes+1))
	if err != nil || len(responseBody) == 0 || len(responseBody) > ClientInvocationMaxBytes || !validClientInvocationProviderJSON(responseBody) {
		return nil, ErrClientInvocationTokenCountUnavailable
	}
	inputTokens, err := parseClientInvocationNativeInputTokens(responseBody)
	if err != nil {
		return nil, ErrClientInvocationTokenCountUnavailable
	}

	return &ClientInvocationTokenCount{
		AccountID: input.AccountID, GroupID: input.GroupID, Platform: input.Platform,
		ModelID: input.ModelID, Protocol: input.Protocol, PayloadHash: input.PayloadHash,
		Accuracy: ClientInvocationTokenCountExact, InputTokens: inputTokens,
		Authority: ClientInvocationTokenAuthority{
			Kind: "native", ID: clientInvocationOpenAIResponsesTokenAuthorityID,
			Revision: clientInvocationOpenAIResponsesTokenAuthorityRevision,
		},
	}, nil
}

type clientInvocationOpenAIResponsesCountPayload struct {
	Model           string           `json:"model"`
	Input           json.RawMessage  `json:"input"`
	Instructions    *json.RawMessage `json:"instructions,omitempty"`
	Stream          *json.RawMessage `json:"stream,omitempty"`
	MaxOutputTokens json.RawMessage  `json:"max_output_tokens,omitempty"`
}

func clientInvocationOpenAIResponsesCountBody(input ClientInvocationTokenInput) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(input.Payload, &fields); err != nil {
		return nil, errors.New("invalid responses token-count payload")
	}
	for _, name := range []string{"instructions", "stream"} {
		if value, exists := fields[name]; exists && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errors.New("null responses token-count field")
		}
	}
	var payload clientInvocationOpenAIResponsesCountPayload
	decoder := json.NewDecoder(bytes.NewReader(input.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil || payload.Model != input.ModelID || len(payload.Input) == 0 {
		return nil, errors.New("unsupported responses token-count payload")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("trailing responses token-count payload")
	}
	var textInput *string
	if json.Unmarshal(payload.Input, &textInput) != nil || textInput == nil {
		return nil, errors.New("responses token counter supports text input only")
	}
	if payload.Instructions != nil {
		var instructions string
		if bytes.Equal(bytes.TrimSpace(*payload.Instructions), []byte("null")) || json.Unmarshal(*payload.Instructions, &instructions) != nil {
			return nil, errors.New("invalid responses instructions")
		}
	}
	if payload.Stream != nil {
		var stream bool
		if bytes.Equal(bytes.TrimSpace(*payload.Stream), []byte("null")) || json.Unmarshal(*payload.Stream, &stream) != nil || stream {
			return nil, errors.New("streaming input is unsupported")
		}
	}
	if len(payload.MaxOutputTokens) > 0 {
		value, err := strconv.ParseInt(string(payload.MaxOutputTokens), 10, 64)
		if err != nil || value <= 0 {
			return nil, errors.New("invalid output token budget")
		}
	}
	countRequest := openAIInputTokensCountRequest{Model: payload.Model, Input: bytes.Clone(payload.Input)}
	if payload.Instructions != nil {
		_ = json.Unmarshal(*payload.Instructions, &countRequest.Instructions)
	}
	return json.Marshal(countRequest)
}

func parseClientInvocationNativeInputTokens(body []byte) (int64, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || len(fields) != 2 {
		return 0, errors.New("invalid native token-count response")
	}
	var object string
	if json.Unmarshal(fields["object"], &object) != nil || object != "response.input_tokens" {
		return 0, errors.New("unexpected native token-count response object")
	}
	value, err := strconv.ParseInt(string(fields["input_tokens"]), 10, 64)
	if err != nil || value < 0 {
		return 0, errors.New("native token count is outside the accepted range")
	}
	return value, nil
}

func isOfficialOpenAIResponsesCountBaseURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.ForceQuery || parsed.Fragment != "" || !strings.EqualFold(strings.TrimSuffix(parsed.Hostname(), "."), "api.openai.com") {
		return false
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return false
	}
	switch strings.TrimRight(parsed.EscapedPath(), "/") {
	case "", "/v1":
		return true
	default:
		return false
	}
}

func isExactOpenAIResponsesCountEndpoint(target *url.URL) bool {
	return target != nil && strings.EqualFold(target.Scheme, "https") && strings.EqualFold(strings.TrimSuffix(target.Hostname(), "."), "api.openai.com") &&
		(target.Port() == "" || target.Port() == "443") && target.Path == "/v1/responses/input_tokens" &&
		target.RawQuery == "" && target.Fragment == "" && target.User == nil
}
