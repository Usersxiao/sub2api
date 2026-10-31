package service

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const ClientInvocationMaxBytes = 1024 * 1024
const clientInvocationMaxParts = 2000

var ErrClientInvocationParameters = infraerrors.BadRequest("MODEL_PARAMETERS_INVALID", "Check the model input and allowed parameters")

// ClientInvocationRequest matches the shared app invocation DTO. Identity and
// authorization are established separately from the restricted grant, never here.
// Optional project-file tools remain data; local authorization is enforced by the host.
type ClientInvocationRequest struct {
	Query              string                      `json:"query,omitempty"`
	Source             string                      `json:"source,omitempty"`
	MaxResults         int                         `json:"maxResults,omitempty"`
	ProjectID          string                      `json:"projectId"`
	RouteID            string                      `json:"routeId"`
	Operation          string                      `json:"operation"`
	ConfigVersion      string                      `json:"configVersion"`
	EntitlementVersion string                      `json:"entitlementVersion"`
	RequestID          string                      `json:"requestId"`
	Input              []ClientTextInputPart       `json:"input"`
	Parameters         map[string]json.RawMessage  `json:"parameters"`
	Tools              []string                    `json:"tools,omitempty"`
	ToolRounds         []ClientInvocationToolRound `json:"toolRounds,omitempty"`
}

type ClientTextInputPart struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// ValidatedClientTextInvocation contains server-selected routing information and
// a closed, defaulted parameter set. It grants neither dispatch nor project access.
// Payload is assembled only after all parameters pass validation; caller input
// can never select a model, URL, credential, arbitrary tool, or streaming mode.
type ValidatedClientTextInvocation struct {
	ModelID      string
	Protocol     string
	Prompt       string
	Parameters   map[string]json.RawMessage
	Payload      json.RawMessage
	OutputTokens int64
}

func ParseClientInvocation(raw []byte) (*ClientInvocationRequest, error) {
	if len(raw) == 0 || len(raw) > ClientInvocationMaxBytes || !utf8.Valid(raw) || !validClientInvocationJSON(raw) {
		return nil, ErrClientInvocationParameters
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil, ErrClientInvocationParameters
	}
	var operation string
	_ = json.Unmarshal(fields["operation"], &operation)
	if operation == "search" {
		if !clientInvocationFields(fields, "projectId", "requestId", "routeId", "operation", "configVersion", "entitlementVersion", "query", "source", "maxResults") {
			return nil, ErrClientInvocationParameters
		}
		var req ClientInvocationRequest
		if json.Unmarshal(raw, &req) != nil || !validClientSearchRequest(&req) {
			return nil, ErrClientInvocationParameters
		}
		return &req, nil
	}
	if !validClientToolWire(fields) {
		return nil, ErrClientInvocationParameters
	}
	delete(fields, "tools")
	delete(fields, "toolRounds")
	if !clientInvocationFields(fields, "projectId", "routeId", "operation", "configVersion", "entitlementVersion", "requestId", "input", "parameters") {
		return nil, ErrClientInvocationParameters
	}
	var req ClientInvocationRequest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return nil, ErrClientInvocationParameters
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, ErrClientInvocationParameters
	}
	if !validClientInvocationRequest(&req) {
		return nil, ErrClientInvocationParameters
	}
	return &req, nil
}

func (part *ClientTextInputPart) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || !clientInvocationFields(fields, "kind", "text") {
		return ErrClientInvocationParameters
	}
	var wire struct {
		Kind *string `json:"kind"`
		Text *string `json:"text"`
	}
	if json.Unmarshal(raw, &wire) != nil || wire.Kind == nil || wire.Text == nil || *wire.Kind != "text" {
		return ErrClientInvocationParameters
	}
	part.Kind, part.Text = *wire.Kind, *wire.Text
	return nil
}

func clientInvocationFields(fields map[string]json.RawMessage, allowed ...string) bool {
	if len(fields) != len(allowed) {
		return false
	}
	for _, key := range allowed {
		if _, exists := fields[key]; !exists {
			return false
		}
	}
	return true
}

func validClientInvocationRequest(req *ClientInvocationRequest) bool {
	if req == nil || !clientIdentifier.MatchString(req.ProjectID) || !clientIdentifier.MatchString(req.RouteID) ||
		!clientIdentifier.MatchString(req.ConfigVersion) || !clientIdentifier.MatchString(req.EntitlementVersion) ||
		!clientIdentifier.MatchString(req.RequestID) || req.Operation != "text" || req.Query != "" || req.Source != "" || req.MaxResults != 0 || len(req.Input) == 0 ||
		len(req.Input) > clientInvocationMaxParts || req.Parameters == nil || len(req.Parameters) > 200 {
		return false
	}
	if !validClientToolHistory(req) {
		return false
	}
	total := 0
	for _, part := range req.Input {
		if part.Kind != "text" || !utf8.ValidString(part.Text) {
			return false
		}
		total += len(part.Text)
		if total > ClientInvocationMaxBytes {
			return false
		}
	}
	return true
}

func ValidateClientTextInvocation(route ClientModelRoute, req *ClientInvocationRequest) (*ValidatedClientTextInvocation, error) {
	return validateClientTextInvocationForModel(route, req, route.Target.ModelID, "")
}

func validateClientTextInvocationForModel(route ClientModelRoute, req *ClientInvocationRequest, modelID, platform string) (*ValidatedClientTextInvocation, error) {
	if !validClientInvocationRequest(req) {
		return nil, ErrClientInvocationParameters
	}
	if route.RouteID != req.RouteID || route.Operation != req.Operation || route.Availability.Status != "available" || route.Target.Kind != "model" {
		return nil, ErrClientGatewayRevoked
	}
	// Reuse the publication's complete declaration validator. The local envelope
	// supplies only structural group metadata; it performs no group lookup and
	// establishes no authorization. ResolveInvocation owns the real group decision.
	declaration := DefaultClientConfiguration()
	declaration.Routes = []ClientConfiguredRoute{{GroupIDs: []int64{1}, Route: route}}
	if validateClientConfiguration(&declaration) != nil {
		return nil, ErrClientInvocationParameters
	}
	if route.Transport != "json" || !clientContains([]string{"responses", "messages", "chat-completions"}, route.Protocol) ||
		!clientContains(route.InputKinds, "text") || !clientContains(route.OutputKinds, "text") || !clientLabel(route.Target.ModelID) || !clientLabel(modelID) || route.Capabilities.Asynchronous {
		return nil, ErrClientInvocationParameters
	}
	parameters := make(map[string]json.RawMessage, len(route.Parameters))
	declarations := make(map[string]ClientModelParameter, len(route.Parameters))
	for _, declaration := range route.Parameters {
		if _, duplicate := declarations[declaration.Name]; duplicate || clientInvocationControlName(declaration.Name) || !clientInvocationScalarType(declaration.Default, declaration.Type) || validateClientParameter(declaration) != nil || !validClientInvocationProtocolParameter(route.Protocol, declaration, declaration.Default) {
			return nil, ErrClientInvocationParameters
		}
		declarations[declaration.Name] = declaration
		parameters[declaration.Name] = bytes.Clone(declaration.Default)
	}
	for name, value := range req.Parameters {
		declaration, ok := declarations[name]
		if !ok || !declaration.Editable || clientInvocationControlName(name) {
			return nil, ErrClientInvocationParameters
		}
		// Reuse declaration validation for exact JSON type, UTF-16 string lengths,
		// enum values, finite/safe numbers, bounds and step anchored at minimum.
		declaration.Default = value
		if !clientInvocationScalarType(value, declaration.Type) || validateClientParameter(declaration) != nil {
			return nil, ErrClientInvocationParameters
		}
		parameters[name] = bytes.Clone(value)
	}
	parameterBytes := 0
	for name, value := range parameters {
		parameterBytes += len(name) + len(value)
		if parameterBytes > ClientInvocationMaxBytes {
			return nil, ErrClientInvocationParameters
		}
		if !validClientInvocationProtocolParameter(route.Protocol, declarations[name], value) {
			return nil, ErrClientInvocationParameters
		}
		if declarations[name].Type == "integer" {
			// Native decoders commonly require an integer JSON spelling. Values
			// such as 128.0/128e0 remain semantically identical to 128, but must
			// not reach a provider's integer field in their original spelling.
			var number float64
			if json.Unmarshal(value, &number) != nil {
				return nil, ErrClientInvocationParameters
			}
			parameters[name], _ = json.Marshal(int64(number))
		}
	}
	outputTokens, err := clientInvocationOutputBudget(route, parameters)
	if err != nil {
		return nil, err
	}
	parts := make([]string, len(req.Input))
	for i, part := range req.Input {
		parts[i] = part.Text
	}
	prompt := strings.Join(parts, "\n")
	if len(prompt) > ClientInvocationMaxBytes {
		return nil, ErrClientInvocationParameters
	}
	payload := make(map[string]any, len(parameters)+3)
	for name, value := range parameters {
		payload[name] = value
	}
	// These controls are always selected by the adapter, after parameter merge.
	payload["model"] = modelID
	payload["stream"] = false
	// Keep native DeepSeek thinking enabled. Its bounded reasoning and finish
	// reason survive the response adapter and subsequent tool-call rounds.
	if platform == PlatformDeepseek && route.Protocol == "chat-completions" {
		payload["thinking"] = map[string]string{"type": "enabled"}
	}
	if route.Protocol == "responses" {
		payload["input"] = prompt
	} else {
		payload["messages"] = []map[string]string{{"role": "user", "content": prompt}}
	}
	if err := applyClientFileTools(route, req, payload, platform); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > ClientInvocationMaxBytes {
		return nil, ErrClientInvocationParameters
	}
	return &ValidatedClientTextInvocation{ModelID: modelID, Protocol: route.Protocol, Prompt: prompt,
		Parameters: parameters, Payload: raw, OutputTokens: outputTokens}, nil
}

func clientInvocationEffectiveModel(resolution *ClientInvocationResolution) (string, error) {
	if resolution == nil || resolution.ModelID == "" || resolution.Route.Target.ModelID != resolution.ModelID {
		return "", ErrClientInvocationParameters
	}
	decision := resolution.CompositeDecision
	modelID := resolution.ModelID
	if decision != nil {
		endpoint, supported := clientCompositeEndpointForProtocol(resolution.Route.Protocol)
		if !supported || !decision.Matched || decision.GroupID != resolution.GroupID || decision.PublicModel != resolution.ModelID ||
			decision.TargetPlatform != resolution.Platform || decision.Endpoint != endpoint || !isConcreteRequestPlatform(decision.TargetPlatform) || !clientLabel(decision.UpstreamModel) {
			return "", ErrClientInvocationParameters
		}
		modelID = decision.UpstreamModel
	}
	if binding := resolution.ChannelMapping; binding != nil {
		if binding.ChannelID <= 0 || binding.Platform != resolution.Platform || binding.SourceModel != modelID || !clientLabel(binding.MappedModel) {
			return "", ErrClientInvocationParameters
		}
	}
	if len(resolution.AccountMappings) > 0 {
		inputModel := modelID
		if binding := resolution.ChannelMapping; binding != nil {
			inputModel = binding.MappedModel
		}
		seen := make(map[int64]struct{}, len(resolution.AccountMappings))
		upstreamModel := ""
		for _, binding := range resolution.AccountMappings {
			if binding.AccountID <= 0 || !clientLabel(binding.AccountPlatform) || binding.InputModel != inputModel || !clientLabel(binding.UpstreamModel) {
				return "", ErrClientInvocationParameters
			}
			if _, duplicate := seen[binding.AccountID]; duplicate {
				return "", ErrClientInvocationParameters
			}
			seen[binding.AccountID] = struct{}{}
			if upstreamModel != "" && upstreamModel != binding.UpstreamModel {
				return "", ErrClientInvocationParameters
			}
			upstreamModel = binding.UpstreamModel
		}
	}
	return modelID, nil
}

func clientInvocationRouteMatchesBillingIdentity(prepared *PreparedClientInvocation) bool {
	if prepared == nil || prepared.Resolution == nil || prepared.BillingIdentity == nil || prepared.BillingIdentity.Group == nil ||
		prepared.BillingIdentity.GroupID == nil || *prepared.BillingIdentity.GroupID != prepared.Resolution.GroupID ||
		prepared.BillingIdentity.Group.ID != prepared.Resolution.GroupID {
		return false
	}
	if prepared.BillingIdentity.Group.Platform == PlatformComposite {
		return prepared.Resolution.CompositeDecision != nil && prepared.Resolution.CompositeDecision.TargetPlatform == prepared.Resolution.Platform
	}
	return prepared.Resolution.CompositeDecision == nil && prepared.BillingIdentity.Group.Platform == prepared.Resolution.Platform
}

// Primitive decoding is explicit: JSON null is not a boolean, number, or
// string even though encoding/json accepts it into those zero-valued Go types.
func clientInvocationScalarType(raw json.RawMessage, kind string) bool {
	if len(raw) == 0 || len(raw) > ClientInvocationMaxBytes || !utf8.Valid(raw) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF {
		return false
	}
	switch kind {
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "number", "integer":
		_, ok := value.(json.Number)
		return ok
	case "string", "enum":
		_, ok := value.(string)
		return ok
	default:
		return false
	}
}

func clientInvocationControlName(name string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", ".", "", ":", "").Replace(strings.ToLower(name))
	return clientContains(strings.Fields("model stream messages input user tools toolchoice requestid servicetier apikey accesstoken refreshtoken authorization baseurl endpoint secret password provider"), normalized)
}

// Reject duplicate keys before decoding so canonical request fingerprints never
// depend on a JSON parser's first/last-key preference. Bound nesting and breadth
// even when an invalid request carries an otherwise ignored nested value.
func validClientInvocationJSON(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	nodes := 0
	var visit func(int) bool
	visit = func(depth int) bool {
		nodes++
		if depth > 16 || nodes > 10000 {
			return false
		}
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		delimiter, composite := token.(json.Delim)
		if !composite {
			return true
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[name] || len(seen) >= 200 {
					return false
				}
				seen[name] = true
				if !visit(depth + 1) {
					return false
				}
			}
			end, err := decoder.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			count := 0
			for decoder.More() {
				count++
				if count > clientInvocationMaxParts || !visit(depth+1) {
					return false
				}
			}
			end, err := decoder.Token()
			return err == nil && end == json.Delim(']')
		default:
			return false
		}
	}
	if !visit(0) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}
