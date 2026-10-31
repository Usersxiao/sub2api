package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

const ClientInvocationSearchProtocol = "web-search"

// Search counts requests, independently of the text tokenizer's budget proof.
// Each authority identifies a separate hosted-search adapter revision.
type ClientInvocationSearchProof struct {
	SearchCalls int    `json:"search_calls"`
	Unit        string `json:"unit"`
	Authority   string `json:"authority"`
}

func NewClientInvocationSearchProof() *ClientInvocationSearchProof {
	return &ClientInvocationSearchProof{1, "request", "alpha-pat-v1"}
}
func ValidateClientInvocationSearchProof(p *ClientInvocationSearchProof) bool {
	return p != nil && p.SearchCalls == 1 && p.Unit == "request" &&
		(p.Authority == "alpha-pat-v1" || p.Authority == "responses-apikey-v1")
}

// ClientInvocationSearchAccountSupported is shared by admission, selection and
// forwarding. API Key search uses only the official Responses origin and its own
// request-count authority; text input-token counting is unrelated.
func ClientInvocationSearchAccountSupported(account *Account) bool {
	return clientInvocationSearchProofForAccount(account) != nil
}

func clientInvocationSearchProofForAccount(account *Account) *ClientInvocationSearchProof {
	if account == nil {
		return nil
	}
	if account.IsOpenAIPersonalAccessToken() {
		return NewClientInvocationSearchProof()
	}
	if account.Platform != PlatformOpenAI || account.Type != AccountTypeAPIKey ||
		account.Status != StatusActive || account.IsShadow() || strings.TrimSpace(account.GetCredential("api_key")) == "" ||
		strings.TrimSpace(account.GetCredential("access_token")) != "" ||
		isOpenAIPersonalAccessTokenAuthMode(account.GetCredential(openAIAuthModeCredentialKey)) ||
		isOpenAIPersonalAccessTokenAuthMode(account.GetCredential(openAIAuthModeLegacyCredentialKey)) ||
		!isOfficialOpenAIResponsesCountBaseURL(account.GetOpenAIBaseURL()) {
		return nil
	}
	return &ClientInvocationSearchProof{1, "request", "responses-apikey-v1"}
}

func clientInvocationSearchEndpoint(authority string) string {
	switch authority {
	case "alpha-pat-v1":
		return "/backend-api/codex/responses"
	case "responses-apikey-v1":
		return "/v1/responses"
	default:
		return ""
	}
}

type ValidatedClientSearchInvocation struct {
	ModelID string
	Payload json.RawMessage
}
type ClientSearchSource struct {
	SourceID string `json:"sourceId"`
	URL      string `json:"url"`
	Title    string `json:"title"`
	Snippet  string `json:"snippet"`
}
type ClientSearchCitation struct {
	SourceID string `json:"sourceId"`
}
type ClientSearchUsage struct {
	SearchCalls int    `json:"searchCalls"`
	Unit        string `json:"unit"`
}
type ClientSearchOutput struct {
	Query     string                 `json:"query"`
	Sources   []ClientSearchSource   `json:"sources"`
	Citations []ClientSearchCitation `json:"citations"`
	Usage     ClientSearchUsage      `json:"usage"`
}

func validClientSearchRequest(req *ClientInvocationRequest) bool {
	return req != nil && req.Operation == "search" && clientIdentifier.MatchString(req.ProjectID) &&
		clientIdentifier.MatchString(req.RequestID) && clientIdentifier.MatchString(req.RouteID) &&
		clientIdentifier.MatchString(req.ConfigVersion) && clientIdentifier.MatchString(req.EntitlementVersion) &&
		utf8.ValidString(req.Query) && strings.TrimSpace(req.Query) != "" && len(utf16.Encode([]rune(req.Query))) <= 4096 &&
		req.Source == "web" && req.MaxResults >= 1 && req.MaxResults <= 20 &&
		req.Input == nil && req.Parameters == nil && req.Tools == nil && req.ToolRounds == nil
}

func ValidateClientSearchInvocation(route ClientModelRoute, req *ClientInvocationRequest) (*ValidatedClientSearchInvocation, error) {
	if !validClientSearchRequest(req) {
		return nil, ErrClientInvocationParameters
	}
	if route.RouteID != req.RouteID || route.Operation != "search" || route.Availability.Status != "available" || route.Target.Kind != "model" {
		return nil, ErrClientGatewayRevoked
	}
	declaration := DefaultClientConfiguration()
	declaration.Routes = []ClientConfiguredRoute{{GroupIDs: []int64{1}, Route: route}}
	if validateClientConfiguration(&declaration) != nil || route.Protocol != ClientInvocationSearchProtocol || route.Transport != "json" ||
		route.Capabilities.Streaming || route.Capabilities.Tools || route.Capabilities.Asynchronous || len(route.Parameters) != 0 ||
		route.Limits.ContextTokens != nil || route.Limits.OutputTokens != nil || !clientContains(route.InputKinds, "text") || !clientContains(route.OutputKinds, "text") {
		return nil, ErrClientInvocationParameters
	}
	// The standalone adapter owns every provider control. Query stays byte-for-byte
	// original, and maxResults bounds normalization rather than provider credentials.
	payload, err := json.Marshal(map[string]any{"model": route.Target.ModelID,
		"commands": []any{map[string]any{"search_query": []any{map[string]any{"q": req.Query}}}}})
	if err != nil {
		return nil, ErrClientInvocationParameters
	}
	return &ValidatedClientSearchInvocation{ModelID: route.Target.ModelID, Payload: payload}, nil
}

func (p *PreparedClientInvocation) Protocol() string {
	if p != nil && p.Search != nil {
		return ClientInvocationSearchProtocol
	}
	if p != nil && p.Text != nil {
		return p.Text.Protocol
	}
	return ""
}
func (p *PreparedClientInvocation) Payload() json.RawMessage {
	if p != nil && p.Search != nil {
		return p.Search.Payload
	}
	if p != nil && p.Text != nil {
		return p.Text.Payload
	}
	return nil
}
func (p *PreparedClientInvocation) ModelID() string {
	if p != nil && p.Search != nil {
		return p.Search.ModelID
	}
	if p != nil && p.Text != nil {
		return p.Text.ModelID
	}
	return ""
}

func clientSearchFingerprint(p *PreparedClientInvocation) (string, error) {
	if p == nil || p.Request == nil || p.Grant == nil || p.Resolution == nil || p.Search == nil || p.Text != nil ||
		p.BillingIdentity == nil || p.BillingIdentity.ID <= 0 || p.Resolution.GroupID <= 0 || !clientInvocationRouteMatchesBillingIdentity(p) ||
		p.Resolution.Platform != PlatformOpenAI || p.Resolution.CompositeDecision != nil {
		return "", ErrClientInvocationParameters
	}
	canonical, err := ValidateClientSearchInvocation(p.Resolution.Route, p.Request)
	if err != nil || p.Search.ModelID != p.Resolution.ModelID || p.Search.ModelID != canonical.ModelID || !bytes.Equal(p.Search.Payload, canonical.Payload) {
		return "", ErrClientInvocationParameters
	}
	raw, err := json.Marshal(struct {
		Version            int
		Request            *ClientInvocationRequest
		GroupID, BillingID int64
		Platform           string
		Payload            json.RawMessage
		Channel            *clientChannelModelBinding
		Accounts           []clientAccountModelBinding
	}{1, p.Request, p.Resolution.GroupID, p.BillingIdentity.ID, p.Resolution.Platform, p.Search.Payload, p.Resolution.ChannelMapping, p.Resolution.AccountMappings})
	if err != nil {
		return "", ErrClientInvocationParameters
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func buildClientSearchProviderBody(alpha []byte, model string) ([]byte, error) {
	raw, err := buildOpenAIAlphaSearchResponsesWebSearchBody(alpha, model)
	if err != nil {
		return nil, err
	}
	var body map[string]any
	if json.Unmarshal(raw, &body) != nil {
		return nil, ErrClientInvocationParameters
	}
	// The closed client envelope already bounds query length; preserve all of it.
	body["input"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"] = openAIAlphaSearchResponsesWebSearchPromptWithLimit(alpha, ClientInvocationMaxBytes)
	body["tool_choice"] = map[string]string{"type": "web_search"}
	body["max_tool_calls"] = 1
	body["include"] = []string{"web_search_call.action.sources"}
	return json.Marshal(body)
}

// Only a completed hosted search with its explicit full source list is admitted.
// Deltas and opaque/encrypted output are never a substitute for that list.
func normalizeClientSearchSSE(raw []byte, req *ClientInvocationRequest) (*ClientSearchOutput, error) {
	if !validClientSearchRequest(req) || len(raw) > ClientInvocationMaxBytes {
		return nil, ErrClientInvocationResponse
	}
	var completed json.RawMessage
	for _, block := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n\n") {
		data := openAIAlphaSearchSSEData(block)
		if data == "" || data == "[DONE]" {
			continue
		}
		if !validClientInvocationProviderJSON([]byte(data)) {
			return nil, ErrClientInvocationResponse
		}
		var event struct {
			Type     string          `json:"type"`
			Response json.RawMessage `json:"response"`
		}
		if json.Unmarshal([]byte(data), &event) != nil {
			return nil, ErrClientInvocationResponse
		}
		switch event.Type {
		case "error", "response.failed", "response.incomplete":
			return nil, ErrClientInvocationResponse
		case "response.completed":
			if completed != nil {
				return nil, ErrClientInvocationResponse
			}
			completed = event.Response
		}
	}
	var response struct {
		Status string            `json:"status"`
		Output []json.RawMessage `json:"output"`
	}
	if len(completed) == 0 || json.Unmarshal(completed, &response) != nil || response.Status != "completed" {
		return nil, ErrClientInvocationResponse
	}
	out := &ClientSearchOutput{Query: req.Query, Sources: []ClientSearchSource{}, Citations: []ClientSearchCitation{}, Usage: ClientSearchUsage{1, "request"}}
	all := map[string]ClientSearchSource{}
	ordered := []string{}
	cited := []string{}
	calls := 0
	for _, item := range response.Output {
		var kind struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(item, &kind) != nil {
			return nil, ErrClientInvocationResponse
		}
		switch kind.Type {
		case "web_search_call":
			calls++
			var call struct {
				Status string `json:"status"`
				Action struct {
					Type    string `json:"type"`
					Sources []struct {
						Type    string `json:"type"`
						URL     string `json:"url"`
						Title   string `json:"title"`
						Snippet string `json:"snippet"`
					} `json:"sources"`
				} `json:"action"`
			}
			if json.Unmarshal(item, &call) != nil || call.Status != "completed" || call.Action.Type != "search" || call.Action.Sources == nil || len(call.Action.Sources) > 200 {
				return nil, ErrClientInvocationResponse
			}
			for _, source := range call.Action.Sources {
				if source.Type != "url" || !validClientSearchURL(source.URL) || !utf8.ValidString(source.Title) || !utf8.ValidString(source.Snippet) || len(utf16.Encode([]rune(source.Title))) > 1000 || len(utf16.Encode([]rune(source.Snippet))) > 8192 {
					return nil, ErrClientInvocationResponse
				}
				title := source.Title
				if strings.TrimSpace(title) == "" {
					title = source.URL
				}
				if len(utf16.Encode([]rune(title))) > 1000 {
					return nil, ErrClientInvocationResponse
				}
				if _, seen := all[source.URL]; !seen {
					ordered = append(ordered, source.URL)
					all[source.URL] = ClientSearchSource{URL: source.URL, Title: title, Snippet: source.Snippet}
				}
			}
		case "message":
			var msg struct {
				Content []struct {
					Annotations []struct {
						Type string `json:"type"`
						URL  string `json:"url"`
					} `json:"annotations"`
				} `json:"content"`
			}
			if json.Unmarshal(item, &msg) != nil {
				return nil, ErrClientInvocationResponse
			}
			for _, content := range msg.Content {
				for _, a := range content.Annotations {
					if a.Type == "url_citation" {
						cited = append(cited, a.URL)
					}
				}
			}
		case "reasoning": // No reasoning or generated report is exposed.
		default:
			return nil, ErrClientInvocationResponse
		}
	}
	if calls != 1 {
		return nil, ErrClientInvocationResponse
	}
	for _, u := range cited {
		if _, ok := all[u]; !ok {
			return nil, ErrClientInvocationResponse
		}
	}
	ids := map[string]string{}
	for _, u := range ordered {
		if len(out.Sources) == req.MaxResults {
			break
		}
		source := all[u]
		source.SourceID = fmt.Sprintf("source-%d", len(out.Sources)+1)
		ids[u] = source.SourceID
		out.Sources = append(out.Sources, source)
	}
	seen := map[string]bool{}
	for _, u := range cited {
		if id := ids[u]; id != "" && !seen[id] {
			out.Citations = append(out.Citations, ClientSearchCitation{id})
			seen[id] = true
		}
	}
	return out, nil
}
func validClientSearchURL(raw string) bool {
	if !utf8.ValidString(raw) {
		return false
	}
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil &&
		!strings.Contains(u.Host, "@") && len(utf16.Encode([]rune(raw))) <= 4096 && strings.IndexFunc(raw, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}
func ValidateClientSearchOutput(out *ClientSearchOutput) bool {
	if out == nil || !utf8.ValidString(out.Query) || strings.TrimSpace(out.Query) == "" || len(utf16.Encode([]rune(out.Query))) > 4096 || out.Sources == nil || out.Citations == nil || len(out.Sources) > 20 || len(out.Citations) > 20 || out.Usage != (ClientSearchUsage{1, "request"}) {
		return false
	}
	ids := map[string]bool{}
	urls := map[string]bool{}
	for _, s := range out.Sources {
		if !clientIdentifier.MatchString(s.SourceID) || ids[s.SourceID] || urls[s.URL] || !validClientSearchURL(s.URL) || s.Title == "" || !utf8.ValidString(s.Title) || !utf8.ValidString(s.Snippet) || len(utf16.Encode([]rune(s.Title))) > 1000 || len(utf16.Encode([]rune(s.Snippet))) > 8192 {
			return false
		}
		ids[s.SourceID] = true
		urls[s.URL] = true
	}
	seen := map[string]bool{}
	for _, c := range out.Citations {
		if !ids[c.SourceID] || seen[c.SourceID] {
			return false
		}
		seen[c.SourceID] = true
	}
	return true
}

func sameClientInvocationSearchProof(a, b *ClientInvocationSearchProof) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
