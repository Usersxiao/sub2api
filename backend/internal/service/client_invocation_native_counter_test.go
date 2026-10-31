//go:build unit

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type clientInvocationNativeCounterAccountRepo struct {
	account *Account
	err     error
	calls   int
}

func (r *clientInvocationNativeCounterAccountRepo) GetByID(_ context.Context, _ int64) (*Account, error) {
	r.calls++
	return r.account, r.err
}

func clientInvocationNativeCounterInput(payload string) ClientInvocationTokenInput {
	bytes := []byte(payload)
	hash := sha256.Sum256(bytes)
	return ClientInvocationTokenInput{
		AccountID: 71, GroupID: 91, Platform: PlatformOpenAI, ModelID: "gpt-5.4",
		Protocol: "responses", PayloadHash: hex.EncodeToString(hash[:]), Payload: bytes,
	}
}

func clientInvocationNativeCounterAccount() *Account {
	return &Account{
		ID: 71, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive,
		Concurrency: 2,
		Credentials: map[string]any{"api_key": "fixture-key", "base_url": "https://api.openai.com/v1"},
	}
}

func TestClientInvocationNativeTokenCounterUsesOfficialResponsesInputTokens(t *testing.T) {
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"object":"response.input_tokens","input_tokens":12}`)),
	}}
	repo := &clientInvocationNativeCounterAccountRepo{account: clientInvocationNativeCounterAccount()}
	counter := NewClientInvocationNativeTokenCounter(repo, &OpenAIGatewayService{httpUpstream: upstream})
	input := clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello","stream":false,"max_output_tokens":64}`)

	count, err := counter.CountClientInvocationTokens(context.Background(), input)

	require.NoError(t, err)
	require.Equal(t, int64(12), count.InputTokens)
	require.Equal(t, ClientInvocationTokenCountExact, count.Accuracy)
	require.Equal(t, ClientInvocationTokenAuthority{Kind: "native", ID: clientInvocationOpenAIResponsesTokenAuthorityID, Revision: clientInvocationOpenAIResponsesTokenAuthorityRevision}, count.Authority)
	require.Equal(t, input.AccountID, count.AccountID)
	require.Equal(t, input.GroupID, count.GroupID)
	require.Equal(t, input.Platform, count.Platform)
	require.Equal(t, input.ModelID, count.ModelID)
	require.Equal(t, input.Protocol, count.Protocol)
	require.Equal(t, input.PayloadHash, count.PayloadHash)
	require.Equal(t, 1, repo.calls)
	require.Equal(t, "https://api.openai.com/v1/responses/input_tokens", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer fixture-key", upstream.lastReq.Header.Get("Authorization"))
	require.True(t, HTTPUpstreamRedirectsDisabled(upstream.lastReq.Context()))
	require.JSONEq(t, `{"model":"gpt-5.4","input":"hello"}`, string(upstream.lastBody))
}

func TestClientInvocationNativeTokenCounterRejectsUnsupportedAuthorityAndPayload(t *testing.T) {
	tests := []struct {
		name    string
		input   ClientInvocationTokenInput
		account *Account
	}{
		{name: "non responses protocol", input: func() ClientInvocationTokenInput {
			in := clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello"}`)
			in.Protocol = "chat-completions"
			return in
		}(), account: clientInvocationNativeCounterAccount()},
		{name: "wrong account id", input: func() ClientInvocationTokenInput {
			in := clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello"}`)
			in.AccountID++
			return in
		}(), account: clientInvocationNativeCounterAccount()},
		{name: "oauth", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello"}`), account: &Account{ID: 71, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive}},
		{name: "custom relay", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello"}`), account: &Account{ID: 71, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Credentials: map[string]any{"api_key": "fixture-key", "base_url": "https://relay.example/v1"}}},
		{name: "inactive account", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello"}`), account: &Account{ID: 71, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusDisabled, Credentials: map[string]any{"api_key": "fixture-key"}}},
		{name: "payload hash mismatch", input: func() ClientInvocationTokenInput {
			in := clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello"}`)
			in.PayloadHash = strings.Repeat("0", 64)
			return in
		}(), account: clientInvocationNativeCounterAccount()},
		{name: "unknown provider field", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello","temperature":0.2}`), account: clientInvocationNativeCounterAccount()},
		{name: "quoted output budget", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello","max_output_tokens":"64"}`), account: clientInvocationNativeCounterAccount()},
		{name: "wrong platform", input: func() ClientInvocationTokenInput {
			in := clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello"}`)
			in.Platform = PlatformDeepseek
			return in
		}(), account: clientInvocationNativeCounterAccount()},
		{name: "model mismatch", input: clientInvocationNativeCounterInput(`{"model":"other-model","input":"hello"}`), account: clientInvocationNativeCounterAccount()},
		{name: "null input", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":null}`), account: clientInvocationNativeCounterAccount()},
		{name: "null instructions", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello","instructions":null}`), account: clientInvocationNativeCounterAccount()},
		{name: "null stream", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello","stream":null}`), account: clientInvocationNativeCounterAccount()},
		{name: "non text input", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":[{"type":"input_text","text":"hello"}]}`), account: clientInvocationNativeCounterAccount()},
		{name: "streaming input", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello","stream":true}`), account: clientInvocationNativeCounterAccount()},
		{name: "duplicate model key", input: clientInvocationNativeCounterInput(`{"model":"gpt-5.4","model":"gpt-5.4","input":"hello"}`), account: clientInvocationNativeCounterAccount()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &httpUpstreamRecorder{}
			repo := &clientInvocationNativeCounterAccountRepo{account: tt.account}
			counter := NewClientInvocationNativeTokenCounter(repo, &OpenAIGatewayService{httpUpstream: upstream})
			count, err := counter.CountClientInvocationTokens(context.Background(), tt.input)
			require.Nil(t, count)
			require.ErrorIs(t, err, ErrClientInvocationTokenCountUnavailable)
			require.Nil(t, upstream.lastReq)
		})
	}
}

func TestClientInvocationNativeTokenCounterRejectsUnverifiedResponses(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "not found", status: http.StatusNotFound, body: `{"error":"unsupported"}`},
		{name: "quoted integer", status: http.StatusOK, body: `{"object":"response.input_tokens","input_tokens":"12"}`},
		{name: "fractional integer", status: http.StatusOK, body: `{"object":"response.input_tokens","input_tokens":12.5}`},
		{name: "negative integer", status: http.StatusOK, body: `{"object":"response.input_tokens","input_tokens":-1}`},
		{name: "wrong response object", status: http.StatusOK, body: `{"object":"response","input_tokens":12}`},
		{name: "extra response field", status: http.StatusOK, body: `{"object":"response.input_tokens","input_tokens":12,"estimated":true}`},
		{name: "duplicate response field", status: http.StatusOK, body: `{"object":"response.input_tokens","input_tokens":12,"input_tokens":12}`},
		{name: "malformed response", status: http.StatusOK, body: `not-json`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: tt.status,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(tt.body)),
			}}
			counter := NewClientInvocationNativeTokenCounter(&clientInvocationNativeCounterAccountRepo{account: clientInvocationNativeCounterAccount()}, &OpenAIGatewayService{httpUpstream: upstream})
			count, err := counter.CountClientInvocationTokens(context.Background(), clientInvocationNativeCounterInput(`{"model":"gpt-5.4","input":"hello"}`))
			require.Nil(t, count)
			require.ErrorIs(t, err, ErrClientInvocationTokenCountUnavailable)
		})
	}
}

func TestIsOfficialOpenAIResponsesCountBaseURL(t *testing.T) {
	for _, tt := range []struct {
		url  string
		want bool
	}{
		{url: "https://api.openai.com", want: true},
		{url: "https://API.OPENAI.COM./v1/", want: true},
		{url: "https://api.openai.com:443/v1", want: true},
		{url: "http://api.openai.com/v1"},
		{url: "https://api.openai.com.evil.test/v1"},
		{url: "https://user@api.openai.com/v1"},
		{url: "https://api.openai.com:8443/v1"},
		{url: "https://api.openai.com/v1?tenant=other"},
		{url: "https://api.openai.com/v2"},
	} {
		t.Run(tt.url, func(t *testing.T) {
			require.Equal(t, tt.want, isOfficialOpenAIResponsesCountBaseURL(tt.url))
		})
	}
}
