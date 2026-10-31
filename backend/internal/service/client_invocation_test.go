//go:build unit

package service

import (
	"encoding/json"
	"strings"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

func invocationRequestFixture() *ClientInvocationRequest {
	return &ClientInvocationRequest{ProjectID: "project-1", RouteID: "writing", Operation: "text", ConfigVersion: "cfg-1",
		EntitlementVersion: "ent-1", RequestID: "request-1", Input: []ClientTextInputPart{{Kind: "text", Text: "First"}, {Kind: "text", Text: "Second"}}, Parameters: map[string]json.RawMessage{}}
}
func invocationRouteFixture(t *testing.T) ClientModelRoute {
	t.Helper()
	route := clientConfigFixture(t).Routes[0].Route
	route.Parameters = []ClientModelParameter{invocationBudgetParameter("responses")}
	return route
}
func invocationNumber(value float64) *float64 { return &value }
func invocationLength(value int) *int         { return &value }

func invocationBudgetParameter(protocol string) ClientModelParameter {
	name := "max_output_tokens"
	if protocol == "messages" {
		name = "max_tokens"
	} else if protocol == "chat-completions" {
		name = "max_completion_tokens"
	}
	return ClientModelParameter{Name: name, Label: "Output budget", Editable: true, Type: "integer", Default: json.RawMessage(`128`), Minimum: invocationNumber(1), Maximum: invocationNumber(1024)}
}

func TestClientInvocationStrictRequestContract(t *testing.T) {
	raw, err := json.Marshal(invocationRequestFixture())
	require.NoError(t, err)
	parsed, err := ParseClientInvocation(raw)
	require.NoError(t, err)
	require.Equal(t, invocationRequestFixture(), parsed)
	text := string(raw)
	cases := map[string]string{
		"extra credentials":     strings.Replace(text, `"projectId":`, `"accessToken":"PRIVATE_SECRET","projectId":`, 1),
		"extra model":           strings.Replace(text, `"projectId":`, `"model":"PRIVATE_MODEL","projectId":`, 1),
		"extra version":         strings.Replace(text, `"projectId":`, `"schemaVersion":1,"projectId":`, 1),
		"extra streaming":       strings.Replace(text, `"projectId":`, `"stream":true,"projectId":`, 1),
		"case mismatch":         strings.Replace(text, `"projectId":`, `"ProjectId":`, 1),
		"duplicate top key":     strings.Replace(text, `"projectId":`, `"projectId":"other","projectId":`, 1),
		"duplicate parameter":   strings.Replace(text, `"parameters":{}`, `"parameters":{"temperature":0.2,"temperature":0.3}`, 1),
		"missing text":          strings.Replace(text, `"kind":"text","text":"First"`, `"kind":"text"`, 1),
		"null text":             strings.Replace(text, `"text":"First"`, `"text":null`, 1),
		"numeric text":          strings.Replace(text, `"text":"First"`, `"text":5`, 1),
		"case part field":       strings.Replace(text, `"text":"First"`, `"Text":"First"`, 1),
		"extra part field":      strings.Replace(text, `"text":"First"`, `"text":"First","url":"http://PRIVATE_HOST"`, 1),
		"asset reference":       strings.Replace(text, `"kind":"text","text":"First"`, `"kind":"image","assetId":"asset-1"`, 1),
		"empty input":           strings.Replace(text, `[{"kind":"text","text":"First"},{"kind":"text","text":"Second"}]`, `[]`, 1),
		"null parameters":       strings.Replace(text, `"parameters":{}`, `"parameters":null`, 1),
		"missing parameters":    strings.Replace(text, `,"parameters":{}`, ``, 1),
		"bad identifier":        strings.Replace(text, `"project-1"`, `"../project"`, 1),
		"unsupported operation": strings.Replace(text, `"operation":"text"`, `"operation":"image"`, 1),
		"trailing JSON":         text + `{}`, "array root": `[]`, "null root": `null`, "empty": ``,
		"oversized": text + strings.Repeat(" ", ClientInvocationMaxBytes),
	}
	for name, candidate := range cases {
		t.Run(name, func(t *testing.T) {
			value, err := ParseClientInvocation([]byte(candidate))
			require.ErrorIs(t, err, ErrClientInvocationParameters)
			require.Nil(t, value)
			require.Equal(t, "MODEL_PARAMETERS_INVALID", infraerrors.Reason(err))
			require.NotContains(t, err.Error(), "PRIVATE_")
		})
	}
	_, err = ParseClientInvocation(append(raw[:len(raw)-1], 255, '}'))
	require.ErrorIs(t, err, ErrClientInvocationParameters)
}

func TestClientInvocationCanonicalTextPayload(t *testing.T) {
	for _, protocol := range []string{"responses", "messages", "chat-completions"} {
		t.Run(protocol, func(t *testing.T) {
			route := invocationRouteFixture(t)
			route.Protocol = protocol
			route.Parameters = []ClientModelParameter{
				{Name: "temperature", Label: "Temperature", Editable: true, Type: "number", Default: json.RawMessage(`0.5`), Minimum: invocationNumber(0), Maximum: invocationNumber(1), Step: invocationNumber(0.1)},
				invocationBudgetParameter(protocol),
			}
			if protocol != "messages" {
				route.Parameters = append(route.Parameters, ClientModelParameter{Name: "store", Label: "Store", Editable: false, Type: "boolean", Default: json.RawMessage(`false`)})
			}
			req := invocationRequestFixture()
			req.Parameters["temperature"] = json.RawMessage(`0.7`)
			result, err := ValidateClientTextInvocation(route, req)
			require.NoError(t, err)
			require.Equal(t, "First\nSecond", result.Prompt)
			require.Equal(t, "test-model", result.ModelID)
			require.Equal(t, protocol, result.Protocol)
			require.Equal(t, int64(128), result.OutputTokens)
			var payload map[string]any
			require.NoError(t, json.Unmarshal(result.Payload, &payload))
			require.Equal(t, "test-model", payload["model"])
			require.Equal(t, false, payload["stream"])
			require.Equal(t, 0.7, payload["temperature"])
			if protocol != "messages" {
				require.Equal(t, false, payload["store"])
			} else {
				require.NotContains(t, payload, "store")
			}
			require.NotContains(t, payload, "requestId")
			require.NotContains(t, payload, "projectId")
			if protocol == "responses" {
				require.Equal(t, "First\nSecond", payload["input"])
				require.NotContains(t, payload, "messages")
			} else {
				require.Equal(t, []any{map[string]any{"role": "user", "content": "First\nSecond"}}, payload["messages"])
				require.NotContains(t, payload, "input")
			}
			req.Parameters["temperature"][0] = '9'
			route.Parameters[1].Default[0] = '9'
			require.Equal(t, `0.7`, string(result.Parameters["temperature"]))
			require.Equal(t, `128`, string(result.Parameters[invocationBudgetParameter(protocol).Name]))
		})
	}
}

func TestClientInvocationDeclaredParameterValues(t *testing.T) {
	parameters := []ClientModelParameter{
		invocationBudgetParameter("chat-completions"),
		{Name: "temperature", Label: "Temperature", Editable: true, Type: "number", Default: json.RawMessage(`0.5`), Minimum: invocationNumber(0), Maximum: invocationNumber(1), Step: invocationNumber(0.1)},
		{Name: "seed", Label: "Seed", Editable: true, Type: "integer", Default: json.RawMessage(`2`), Minimum: invocationNumber(2), Maximum: invocationNumber(8), Step: invocationNumber(2)},
		{Name: "stop", Label: "Stop", Editable: true, Type: "string", Default: json.RawMessage(`"😀"`), MinLength: invocationLength(2), MaxLength: invocationLength(3)},
		{Name: "store", Label: "Store", Editable: true, Type: "boolean", Default: json.RawMessage(`false`)},
	}
	for _, tc := range []struct{ name, good, bad string }{
		{"temperature", `0.8`, `1.1`}, {"temperature", `0.7`, `0.75`}, {"temperature", `0`, `"0.5"`},
		{"seed", `4`, `3`}, {"seed", `8`, `3.5`}, {"seed", `6`, `9007199254740992`},
		{"stop", `"😀a"`, `"a"`}, {"stop", `"ab"`, `"😀😀"`},
		{"store", `true`, `"true"`}, {"store", `false`, `null`},
	} {
		t.Run(tc.name+"/"+tc.bad, func(t *testing.T) {
			route := invocationRouteFixture(t)
			route.Protocol, route.Parameters = "chat-completions", parameters
			req := invocationRequestFixture()
			req.Parameters[tc.name] = json.RawMessage(tc.good)
			_, err := ValidateClientTextInvocation(route, req)
			require.NoError(t, err)
			req.Parameters[tc.name] = json.RawMessage(tc.bad)
			_, err = ValidateClientTextInvocation(route, req)
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
	for _, value := range []string{`{}`, `[]`, `null`, `1e999`, `NaN`} {
		route := invocationRouteFixture(t)
		route.Protocol, route.Parameters = "chat-completions", parameters
		req := invocationRequestFixture()
		req.Parameters["temperature"] = json.RawMessage(value)
		_, err := ValidateClientTextInvocation(route, req)
		require.ErrorIs(t, err, ErrClientInvocationParameters)
	}
}

func TestClientInvocationRejectsUnapprovedParametersAndControlDeclarations(t *testing.T) {
	for _, name := range []string{"model", "stream", "messages", "input", "user", "tools", "tool_choice", "request_id", "service_tier", "base_url", "api_key", "access-token", "Provider", "authorization"} {
		t.Run(name, func(t *testing.T) {
			route := invocationRouteFixture(t)
			req := invocationRequestFixture()
			req.Parameters[name] = json.RawMessage(`"PRIVATE_SECRET"`)
			_, err := ValidateClientTextInvocation(route, req)
			require.ErrorIs(t, err, ErrClientInvocationParameters)
			require.NotContains(t, err.Error(), "PRIVATE_SECRET")
			route.Parameters = []ClientModelParameter{{Name: name, Label: "Control", Type: "string", Editable: true, Default: json.RawMessage(`"provider-value"`), MinLength: invocationLength(0), MaxLength: invocationLength(100)}}
			_, err = ValidateClientTextInvocation(route, invocationRequestFixture())
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
	route := invocationRouteFixture(t)
	route.Parameters = []ClientModelParameter{invocationBudgetParameter(route.Protocol), {Name: "store", Label: "Store", Editable: false, Type: "boolean", Default: json.RawMessage(`false`)}}
	req := invocationRequestFixture()
	req.Parameters["store"] = json.RawMessage(`false`)
	_, err := ValidateClientTextInvocation(route, req)
	require.ErrorIs(t, err, ErrClientInvocationParameters)
	route.Parameters = append(route.Parameters, route.Parameters[1])
	_, err = ValidateClientTextInvocation(route, invocationRequestFixture())
	require.ErrorIs(t, err, ErrClientInvocationParameters)
}

func TestClientInvocationRejectsRouteMismatchAndUnsupportedCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ClientModelRoute)
	}{
		{"another route", func(r *ClientModelRoute) { r.RouteID = "another" }}, {"another operation", func(r *ClientModelRoute) { r.Operation = "image" }},
		{"withdrawn", func(r *ClientModelRoute) { r.Availability.Status = "withdrawn" }}, {"workflow", func(r *ClientModelRoute) { r.Target.Kind = "workflow" }},
		{"multipart", func(r *ClientModelRoute) { r.Transport = "multipart" }}, {"protocol", func(r *ClientModelRoute) { r.Protocol = "gemini" }},
		{"no text input", func(r *ClientModelRoute) { r.InputKinds = []string{"image"} }}, {"no text output", func(r *ClientModelRoute) { r.OutputKinds = []string{"image"} }},
		{"missing model", func(r *ClientModelRoute) { r.Target.ModelID = "" }}, {"async", func(r *ClientModelRoute) { r.Capabilities.Asynchronous = true }},
		{"invalid public metadata", func(r *ClientModelRoute) { r.DisplayName = "" }},
		{"invalid input vocabulary", func(r *ClientModelRoute) { r.InputKinds = []string{"text", "url"} }},
		{"missing parameter declaration", func(r *ClientModelRoute) { r.Parameters = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := invocationRouteFixture(t)
			tc.change(&route)
			_, err := ValidateClientTextInvocation(route, invocationRequestFixture())
			require.Error(t, err)
			require.Contains(t, []string{"MODEL_ROUTE_REVOKED", "MODEL_PARAMETERS_INVALID"}, infraerrors.Reason(err))
		})
	}
}

func TestClientInvocationRouteOutputBudget(t *testing.T) {
	for _, tc := range []struct{ protocol, name string }{{"messages", "max_tokens"}, {"responses", "max_output_tokens"}, {"chat-completions", "max_completion_tokens"}, {"chat-completions", "max_tokens"}} {
		t.Run(tc.protocol+"/"+tc.name, func(t *testing.T) {
			route := invocationRouteFixture(t)
			route.Protocol = tc.protocol
			limit := int64(256)
			route.Limits.OutputTokens = &limit
			route.Parameters = []ClientModelParameter{{Name: tc.name, Label: "Output budget", Editable: true, Type: "integer", Default: json.RawMessage(`128`), Minimum: invocationNumber(1), Maximum: invocationNumber(1024)}}
			req := invocationRequestFixture()
			req.Parameters[tc.name] = json.RawMessage(`256`)
			_, err := ValidateClientTextInvocation(route, req)
			require.NoError(t, err)
			req.Parameters[tc.name] = json.RawMessage(`257`)
			_, err = ValidateClientTextInvocation(route, req)
			require.ErrorIs(t, err, ErrClientInvocationParameters)
			delete(req.Parameters, tc.name)
			route.Parameters[0].Default = json.RawMessage(`512`)
			_, err = ValidateClientTextInvocation(route, req)
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
}

func TestClientInvocationRejectsNullDefaultsAndOverrides(t *testing.T) {
	for _, declaration := range []ClientModelParameter{
		{Name: "store", Label: "Boolean", Type: "boolean", Editable: true, Default: json.RawMessage(`false`)},
		{Name: "temperature", Label: "Number", Type: "number", Editable: true, Default: json.RawMessage(`0`), Minimum: invocationNumber(0), Maximum: invocationNumber(1)},
		{Name: "seed", Label: "Integer", Type: "integer", Editable: true, Default: json.RawMessage(`0`), Minimum: invocationNumber(0), Maximum: invocationNumber(1)},
		{Name: "stop", Label: "String", Type: "string", Editable: true, Default: json.RawMessage(`""`), MinLength: invocationLength(0), MaxLength: invocationLength(1)},
	} {
		t.Run(declaration.Type, func(t *testing.T) {
			route := invocationRouteFixture(t)
			route.Protocol = "chat-completions"
			route.Parameters = []ClientModelParameter{invocationBudgetParameter(route.Protocol), declaration}
			req := invocationRequestFixture()
			_, err := ValidateClientTextInvocation(route, req)
			require.NoError(t, err)
			for _, value := range []string{`null`, ` {"nested":"value"} `, `[]`} {
				req.Parameters[declaration.Name] = json.RawMessage(value)
				_, err = ValidateClientTextInvocation(route, req)
				require.ErrorIs(t, err, ErrClientInvocationParameters)
			}
			route.Parameters[1].Default = json.RawMessage(`null`)
			_, err = ValidateClientTextInvocation(route, invocationRequestFixture())
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
}

func TestClientInvocationBoundsProgrammaticPayloads(t *testing.T) {
	route := invocationRouteFixture(t)
	route.Protocol = "chat-completions"
	route.Parameters = []ClientModelParameter{invocationBudgetParameter(route.Protocol), {Name: "stop", Label: "Stop", Type: "string", Editable: true,
		Default: json.RawMessage(`""`), MinLength: invocationLength(0), MaxLength: invocationLength(ClientInvocationMaxBytes + 1)}}
	req := invocationRequestFixture()
	req.Parameters["stop"] = json.RawMessage(`"` + strings.Repeat("x", ClientInvocationMaxBytes) + `"`)
	_, err := ValidateClientTextInvocation(route, req)
	require.ErrorIs(t, err, ErrClientInvocationParameters)
	delete(req.Parameters, "stop")
	// Raw fields fit independently, but the serialized payload still has one byte limit.
	req.Input = []ClientTextInputPart{{Kind: "text", Text: strings.Repeat("x", ClientInvocationMaxBytes)}}
	_, err = ValidateClientTextInvocation(route, req)
	require.ErrorIs(t, err, ErrClientInvocationParameters)
}
