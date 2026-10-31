//go:build unit

package service

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientInvocationProtocolRequiresExplicitNativeOutputBudget(t *testing.T) {
	for _, protocol := range []string{"responses", "messages", "chat-completions"} {
		t.Run(protocol, func(t *testing.T) {
			route := invocationRouteFixture(t)
			route.Protocol, route.Parameters = protocol, []ClientModelParameter{}
			limit := int64(1024)
			route.Limits.OutputTokens = &limit
			_, err := ValidateClientTextInvocation(route, invocationRequestFixture())
			require.ErrorIs(t, err, ErrClientInvocationParameters, "a route limit does not invent an output budget")
			budget := invocationBudgetParameter(protocol)
			route.Parameters = []ClientModelParameter{budget}
			for _, spelling := range []string{`128`, `128.0`, `128e0`} {
				req := invocationRequestFixture()
				req.Parameters[budget.Name] = json.RawMessage(spelling)
				text, err := ValidateClientTextInvocation(route, req)
				require.NoError(t, err)
				require.Equal(t, int64(128), text.OutputTokens)
				require.Equal(t, `128`, string(text.Parameters[budget.Name]), "wire output budgets always have an integer spelling")
			}
			for _, invalid := range []string{`0`, `-1`, `1.5`, `null`, `"128"`, `9007199254740992`} {
				route.Parameters = []ClientModelParameter{budget}
				req := invocationRequestFixture()
				req.Parameters[budget.Name] = json.RawMessage(invalid)
				_, err = ValidateClientTextInvocation(route, req)
				require.ErrorIs(t, err, ErrClientInvocationParameters, invalid)
			}
			budget.Type = "number"
			route.Parameters = []ClientModelParameter{budget}
			_, err = ValidateClientTextInvocation(route, invocationRequestFixture())
			require.ErrorIs(t, err, ErrClientInvocationParameters, "budgets require integer declarations")
		})
	}
}

func TestClientInvocationProtocolRejectsForeignAndUnreviewedParameters(t *testing.T) {
	for _, protocol := range []string{"responses", "messages", "chat-completions"} {
		for _, name := range []string{"vendor_control", "n", "best_of", "logprobs", "function_call", "truncation", "reasoning_effort", "max_output_tokens", "max_tokens", "max_completion_tokens"} {
			if name == invocationBudgetParameter(protocol).Name || (protocol == "chat-completions" && name == "max_tokens") {
				continue
			}
			t.Run(protocol+"/"+name, func(t *testing.T) {
				route := invocationRouteFixture(t)
				route.Protocol = protocol
				route.Parameters = []ClientModelParameter{invocationBudgetParameter(protocol), {Name: name, Label: "Unreviewed", Type: "integer", Editable: true,
					Default: json.RawMessage(`1`), Minimum: invocationNumber(0), Maximum: invocationNumber(1024)}}
				_, err := ValidateClientTextInvocation(route, invocationRequestFixture())
				require.ErrorIs(t, err, ErrClientInvocationParameters)
			})
		}
	}
	for _, protocol := range []string{"responses", "messages"} {
		route := invocationRouteFixture(t)
		route.Protocol = protocol
		route.Parameters = []ClientModelParameter{invocationBudgetParameter(protocol), {Name: "stop", Label: "Stop", Type: "string", Default: json.RawMessage(`"end"`), MinLength: invocationLength(0), MaxLength: invocationLength(100)}}
		_, err := ValidateClientTextInvocation(route, invocationRequestFixture())
		require.ErrorIs(t, err, ErrClientInvocationParameters, protocol)
	}
}

func TestClientInvocationProtocolAppliesNativeRangesBeyondAdministratorBounds(t *testing.T) {
	for _, tc := range []struct {
		protocol, name, kind, good, bad string
		minimum, maximum                float64
	}{
		{"responses", "temperature", "number", `2`, `2.1`, -10, 10},
		{"messages", "temperature", "number", `1`, `1.1`, -10, 10},
		{"messages", "top_p", "number", `1`, `-0.1`, -10, 10},
		{"messages", "top_k", "integer", `0`, `-1`, -10, 10},
		{"chat-completions", "temperature", "number", `0`, `-0.1`, -10, 10},
		{"chat-completions", "top_p", "number", `1`, `1.1`, -10, 10},
		{"chat-completions", "frequency_penalty", "number", `-2`, `-2.1`, -10, 10},
		{"chat-completions", "presence_penalty", "number", `2`, `2.1`, -10, 10},
	} {
		t.Run(tc.protocol+"/"+tc.name, func(t *testing.T) {
			route := invocationRouteFixture(t)
			route.Protocol = tc.protocol
			parameter := ClientModelParameter{Name: tc.name, Label: "Native parameter", Type: tc.kind, Editable: true,
				Default: json.RawMessage(tc.good), Minimum: invocationNumber(tc.minimum), Maximum: invocationNumber(tc.maximum)}
			route.Parameters = []ClientModelParameter{invocationBudgetParameter(tc.protocol), parameter}
			req := invocationRequestFixture()
			_, err := ValidateClientTextInvocation(route, req)
			require.NoError(t, err)
			req.Parameters[tc.name] = json.RawMessage(tc.bad)
			_, err = ValidateClientTextInvocation(route, req)
			require.ErrorIs(t, err, ErrClientInvocationParameters)
			// Bad native defaults remain invalid even with a legal override.
			route.Parameters[1].Default = json.RawMessage(tc.bad)
			req.Parameters[tc.name] = json.RawMessage(tc.good)
			_, err = ValidateClientTextInvocation(route, req)
			require.ErrorIs(t, err, ErrClientInvocationParameters)
		})
	}
}

func TestClientInvocationProtocolRejectsConflictingEffectiveParameters(t *testing.T) {
	route := invocationRouteFixture(t)
	route.Protocol = "chat-completions"
	legacy := invocationBudgetParameter(route.Protocol)
	legacy.Name = "max_tokens"
	route.Parameters = []ClientModelParameter{invocationBudgetParameter(route.Protocol), legacy}
	_, err := ValidateClientTextInvocation(route, invocationRequestFixture())
	require.ErrorIs(t, err, ErrClientInvocationParameters, "two output budget names are ambiguous even when their values agree")
	route.Protocol = "messages"
	route.Parameters = []ClientModelParameter{invocationBudgetParameter(route.Protocol),
		{Name: "temperature", Label: "Temperature", Type: "number", Default: json.RawMessage(`0.5`), Minimum: invocationNumber(0), Maximum: invocationNumber(1)},
		{Name: "top_p", Label: "Top p", Type: "number", Default: json.RawMessage(`0.9`), Minimum: invocationNumber(0), Maximum: invocationNumber(1)}}
	_, err = ValidateClientTextInvocation(route, invocationRequestFixture())
	require.ErrorIs(t, err, ErrClientInvocationParameters)
	// A single Messages sampler is permitted; no parameter is silently dropped.
	route.Parameters = route.Parameters[:2]
	_, err = ValidateClientTextInvocation(route, invocationRequestFixture())
	require.NoError(t, err)
}
