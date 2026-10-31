package service

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

var ErrClientInvocationResponse = infraerrors.New(502, "MODEL_RESPONSE_INVALID", "The model returned an unsupported response")

type ClientInvocationUsage struct {
	InputTokens  int64 `json:"-"`
	OutputTokens int64 `json:"-"`
	TotalTokens  int64 `json:"-"`
}

// ClientInvocationResponse is the bounded text and upstream usage required by
// the private invocation adapter. It contains no upstream headers or payload.
type ClientInvocationResponse struct {
	Text         string                     `json:"-"`
	Reasoning    string                     `json:"-"`
	FinishReason string                     `json:"-"`
	ToolCalls    []ClientInvocationToolCall `json:"-"`
	Usage        ClientInvocationUsage      `json:"-"`
}

// ParseClientInvocationResponse accepts only completed, single-turn text
// results from protocols already admitted by ValidateClientTextInvocation.
// Usage is mandatory because the caller must settle the actual upstream charge.
func ParseClientInvocationResponse(protocol string, raw []byte) (*ClientInvocationResponse, error) {
	if len(raw) == 0 || len(raw) > ClientInvocationMaxBytes || !utf8.Valid(raw) || !validClientInvocationJSON(raw) {
		return nil, ErrClientInvocationResponse
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, ErrClientInvocationResponse
	}
	var text string
	var usage ClientInvocationUsage
	var calls []ClientInvocationToolCall
	var reasoning string
	finishReason := "stop"
	var err error
	switch protocol {
	case "responses":
		text, err = parseClientResponsesText(fields)
		if err == nil {
			usage, err = parseClientResponsesUsage(fields["usage"])
		}
	case "messages":
		text, err = parseClientMessagesText(fields)
		if bytes.Equal(bytes.TrimSpace(fields["stop_reason"]), []byte(`"max_tokens"`)) {
			finishReason = "length"
		}
		if err == nil {
			usage, err = parseClientMessagesUsage(fields["usage"])
		}
	case "chat-completions":
		var output *ClientInvocationResponse
		output, err = parseClientChatCompletionsOutput(fields)
		if err == nil {
			text, reasoning, finishReason, calls = output.Text, output.Reasoning, output.FinishReason, output.ToolCalls
			usage, err = parseClientChatCompletionsUsage(fields["usage"])
		}
	default:
		return nil, ErrClientInvocationResponse
	}
	if err != nil || len(text) > ClientInvocationMaxBytes || len(reasoning) > ClientInvocationMaxBytes || !utf8.ValidString(text) || !utf8.ValidString(reasoning) {
		return nil, ErrClientInvocationResponse
	}
	total := usage.InputTokens + usage.OutputTokens
	if total < 0 || (usage.TotalTokens != 0 && usage.TotalTokens != total) {
		return nil, ErrClientInvocationResponse
	}
	usage.TotalTokens = total
	return &ClientInvocationResponse{Text: text, Reasoning: reasoning, FinishReason: finishReason, ToolCalls: calls, Usage: usage}, nil
}

func parseClientResponsesText(fields map[string]json.RawMessage) (string, error) {
	var status string
	if readClientString(fields["status"], &status) != nil || status != "completed" {
		return "", ErrClientInvocationResponse
	}
	var output []json.RawMessage
	if readClientArray(fields["output"], &output) != nil || len(output) > clientInvocationMaxParts {
		return "", ErrClientInvocationResponse
	}
	var text bytes.Buffer
	for _, rawItem := range output {
		item, err := readClientObject(rawItem)
		if err != nil {
			return "", err
		}
		var kind string
		if readClientString(item["type"], &kind) != nil {
			return "", ErrClientInvocationResponse
		}
		switch kind {
		case "message":
			var role string
			if readClientString(item["role"], &role) != nil || role != "assistant" {
				return "", ErrClientInvocationResponse
			}
			var content []json.RawMessage
			if readClientArray(item["content"], &content) != nil || len(content) > clientInvocationMaxParts {
				return "", ErrClientInvocationResponse
			}
			for _, rawPart := range content {
				part, err := readClientObject(rawPart)
				if err != nil {
					return "", err
				}
				var partType string
				if readClientString(part["type"], &partType) != nil {
					return "", ErrClientInvocationResponse
				}
				switch partType {
				case "output_text":
					if appendClientText(&text, part["text"]) != nil {
						return "", ErrClientInvocationResponse
					}
				case "refusal":
					if appendClientText(&text, part["refusal"]) != nil {
						return "", ErrClientInvocationResponse
					}
				default:
					return "", ErrClientInvocationResponse
				}
			}
		case "reasoning":
			continue
		default:
			return "", ErrClientInvocationResponse
		}
		if text.Len() > ClientInvocationMaxBytes {
			return "", ErrClientInvocationResponse
		}
	}
	return text.String(), nil
}

func parseClientMessagesText(fields map[string]json.RawMessage) (string, error) {
	var messageType string
	if readClientString(fields["type"], &messageType) != nil || messageType != "message" {
		return "", ErrClientInvocationResponse
	}
	var stopReason string
	if readClientString(fields["stop_reason"], &stopReason) != nil || !clientContains([]string{"end_turn", "stop_sequence", "max_tokens"}, stopReason) {
		return "", ErrClientInvocationResponse
	}
	var content []json.RawMessage
	if readClientArray(fields["content"], &content) != nil || len(content) > clientInvocationMaxParts {
		return "", ErrClientInvocationResponse
	}
	var text bytes.Buffer
	for _, rawPart := range content {
		part, err := readClientObject(rawPart)
		if err != nil {
			return "", err
		}
		var kind string
		if readClientString(part["type"], &kind) != nil || kind != "text" || appendClientText(&text, part["text"]) != nil {
			return "", ErrClientInvocationResponse
		}
		if text.Len() > ClientInvocationMaxBytes {
			return "", ErrClientInvocationResponse
		}
	}
	return text.String(), nil
}

func parseClientChatCompletionsOutput(fields map[string]json.RawMessage) (*ClientInvocationResponse, error) {
	var choices []json.RawMessage
	if readClientArray(fields["choices"], &choices) != nil || len(choices) != 1 {
		return nil, ErrClientInvocationResponse
	}
	choice, err := readClientObject(choices[0])
	if err != nil {
		return nil, err
	}
	var finish string
	if readClientString(choice["finish_reason"], &finish) != nil || !clientContains([]string{"stop", "length", "content_filter", "tool_calls"}, finish) {
		return nil, ErrClientInvocationResponse
	}
	message, err := readClientObject(choice["message"])
	if err != nil {
		return nil, err
	}
	var role, text string
	if readClientString(message["role"], &role) != nil || role != "assistant" || message["function_call"] != nil {
		return nil, ErrClientInvocationResponse
	}
	content := message["content"]
	if len(content) == 0 && finish != "tool_calls" {
		return nil, ErrClientInvocationResponse
	}
	if len(content) > 0 && !bytes.Equal(bytes.TrimSpace(content), []byte("null")) {
		if readClientString(content, &text) != nil {
			return nil, ErrClientInvocationResponse
		}
	}
	result := &ClientInvocationResponse{Text: text, FinishReason: finish}
	if raw := message["reasoning_content"]; len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if readClientString(raw, &result.Reasoning) != nil {
			return nil, ErrClientInvocationResponse
		}
	}
	// A truncated tool argument is not an executable call. Preserve the owned
	// text/reasoning and termination so the client can explain the incomplete turn.
	if finish == "length" || finish == "content_filter" {
		return result, nil
	}
	if finish == "tool_calls" {
		calls, err := parseClientToolCalls(message["tool_calls"])
		if err != nil {
			return nil, err
		}
		result.ToolCalls = calls
		return result, nil
	}
	if calls := message["tool_calls"]; len(calls) > 0 && !bytes.Equal(bytes.TrimSpace(calls), []byte("null")) && !bytes.Equal(bytes.TrimSpace(calls), []byte("[]")) {
		return nil, ErrClientInvocationResponse
	}
	return result, nil
}

func parseClientResponsesUsage(raw json.RawMessage) (ClientInvocationUsage, error) {
	fields, err := readClientObject(raw)
	if err != nil {
		return ClientInvocationUsage{}, err
	}
	input, err := readClientInteger(fields["input_tokens"])
	if err != nil {
		return ClientInvocationUsage{}, err
	}
	output, err := readClientInteger(fields["output_tokens"])
	return ClientInvocationUsage{InputTokens: input, OutputTokens: output}, err
}

func parseClientMessagesUsage(raw json.RawMessage) (ClientInvocationUsage, error) {
	fields, err := readClientObject(raw)
	if err != nil {
		return ClientInvocationUsage{}, err
	}
	input, err := readClientInteger(fields["input_tokens"])
	if err != nil {
		return ClientInvocationUsage{}, err
	}
	output, err := readClientInteger(fields["output_tokens"])
	return ClientInvocationUsage{InputTokens: input, OutputTokens: output}, err
}

func parseClientChatCompletionsUsage(raw json.RawMessage) (ClientInvocationUsage, error) {
	fields, err := readClientObject(raw)
	if err != nil {
		return ClientInvocationUsage{}, err
	}
	input, err := readClientInteger(fields["prompt_tokens"])
	if err != nil {
		return ClientInvocationUsage{}, err
	}
	output, err := readClientInteger(fields["completion_tokens"])
	if err != nil {
		return ClientInvocationUsage{}, err
	}
	var total int64
	if value, exists := fields["total_tokens"]; exists {
		total, err = readClientInteger(value)
		if err != nil {
			return ClientInvocationUsage{}, err
		}
	}
	return ClientInvocationUsage{InputTokens: input, OutputTokens: output, TotalTokens: total}, nil
}

func readClientObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, ErrClientInvocationResponse
	}
	return fields, nil
}

func readClientArray(raw json.RawMessage, out *[]json.RawMessage) error {
	if len(raw) == 0 || json.Unmarshal(raw, out) != nil || *out == nil {
		return ErrClientInvocationResponse
	}
	return nil
}

func readClientString(raw json.RawMessage, out *string) error {
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, out) != nil || !utf8.ValidString(*out) {
		return ErrClientInvocationResponse
	}
	return nil
}

func readClientInteger(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, ErrClientInvocationResponse
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF {
		return 0, ErrClientInvocationResponse
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, ErrClientInvocationResponse
	}
	integer, err := number.Int64()
	if err != nil || integer < 0 {
		return 0, ErrClientInvocationResponse
	}
	return integer, nil
}

func appendClientText(target *bytes.Buffer, raw json.RawMessage) error {
	var value string
	if readClientString(raw, &value) != nil || target.Len()+len(value) > ClientInvocationMaxBytes {
		return ErrClientInvocationResponse
	}
	target.WriteString(value)
	return nil
}
