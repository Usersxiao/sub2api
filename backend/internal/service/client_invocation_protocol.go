package service

import (
	"encoding/json"
	"math"
)

// This is deliberately narrower than the providers' full APIs. A declaration in
// administrator configuration cannot enable an arbitrary provider control. New
// parameters require an explicit protocol rule and response/accounting review.
func validClientInvocationProtocolParameter(protocol string, declaration ClientModelParameter, value json.RawMessage) bool {
	if !clientInvocationScalarType(value, declaration.Type) {
		return false
	}
	if declaration.Name == "store" {
		return (protocol == "responses" || protocol == "chat-completions") && declaration.Type == "boolean"
	}
	if declaration.Name == "stop" {
		return protocol == "chat-completions" && declaration.Type == "string"
	}
	var kind string
	var minimum, maximum float64
	switch protocol {
	case "responses":
		switch declaration.Name {
		case "max_output_tokens":
			kind, minimum, maximum = "integer", 1, 9007199254740991
		case "temperature":
			kind, minimum, maximum = "number", 0, 2
		case "top_p":
			kind, minimum, maximum = "number", 0, 1
		default:
			return false
		}
	case "messages":
		switch declaration.Name {
		case "max_tokens":
			kind, minimum, maximum = "integer", 1, 9007199254740991
		case "temperature", "top_p":
			kind, minimum, maximum = "number", 0, 1
		case "top_k":
			kind, minimum, maximum = "integer", 0, 9007199254740991
		default:
			return false
		}
	case "chat-completions":
		switch declaration.Name {
		case "max_tokens", "max_completion_tokens":
			kind, minimum, maximum = "integer", 1, 9007199254740991
		case "temperature":
			kind, minimum, maximum = "number", 0, 2
		case "top_p":
			kind, minimum, maximum = "number", 0, 1
		case "frequency_penalty", "presence_penalty":
			kind, minimum, maximum = "number", -2, 2
		case "seed":
			kind, minimum, maximum = "integer", -9007199254740991, 9007199254740991
		default:
			return false
		}
	default:
		return false
	}
	if declaration.Type != kind {
		return false
	}
	var number float64
	return json.Unmarshal(value, &number) == nil && !math.IsNaN(number) && !math.IsInf(number, 0) &&
		number >= minimum && number <= maximum && (kind != "integer" || math.Trunc(number) == number)
}

func clientInvocationOutputBudget(route ClientModelRoute, parameters map[string]json.RawMessage) (int64, error) {
	var names []string
	switch route.Protocol {
	case "responses":
		names = []string{"max_output_tokens"}
	case "messages":
		names = []string{"max_tokens"}
		// Current native Messages models do not share a universal contract for
		// specifying both samplers. Keep this initial adapter to one sampler.
		if parameters["temperature"] != nil && parameters["top_p"] != nil {
			return 0, ErrClientInvocationParameters
		}
	case "chat-completions":
		names = []string{"max_completion_tokens", "max_tokens"}
	default:
		return 0, ErrClientInvocationParameters
	}
	var budget int64
	found := false
	for _, name := range names {
		if value, exists := parameters[name]; exists {
			if found {
				return 0, ErrClientInvocationParameters
			}
			var number float64
			if json.Unmarshal(value, &number) != nil || number < 1 || number > 9007199254740991 || math.Trunc(number) != number {
				return 0, ErrClientInvocationParameters
			}
			budget, found = int64(number), true
		}
	}
	if !found || (route.Limits.OutputTokens != nil && budget > *route.Limits.OutputTokens) {
		return 0, ErrClientInvocationParameters
	}
	return budget, nil
}
