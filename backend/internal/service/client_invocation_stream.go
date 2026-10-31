package service

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

type clientInvocationTextStreamKey struct{}
type clientInvocationReasoningStreamKey struct{}

type ClientInvocationTextSink func(string) error

// WithClientInvocationTextStream selects the bounded text SSE adapter. It does
// not grant execution: the regular preparation, final-payload gate and required
// usage settlement still govern the same durable invocation.
func WithClientInvocationTextStream(ctx context.Context, sink ClientInvocationTextSink) context.Context {
	return context.WithValue(ctx, clientInvocationTextStreamKey{}, sink)
}

func ClientInvocationTextStream(ctx context.Context) (ClientInvocationTextSink, bool) {
	sink, ok := ctx.Value(clientInvocationTextStreamKey{}).(ClientInvocationTextSink)
	return sink, ok && sink != nil
}

// Reasoning uses the same request, generation and bounded event stream as text.
func WithClientInvocationReasoningStream(ctx context.Context, sink ClientInvocationTextSink) context.Context {
	return context.WithValue(ctx, clientInvocationReasoningStreamKey{}, sink)
}

func ClientInvocationReasoningStream(ctx context.Context) ClientInvocationTextSink {
	sink, _ := ctx.Value(clientInvocationReasoningStreamKey{}).(ClientInvocationTextSink)
	return sink
}

func prepareClientInvocationTextStream(ctx context.Context, route ClientModelRoute, text *ValidatedClientTextInvocation) error {
	if _, streaming := ClientInvocationTextStream(ctx); !streaming {
		return nil
	}
	if text == nil || text.Protocol != "chat-completions" || !route.Capabilities.Streaming {
		return ErrClientInvocationParameters
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(text.Payload, &payload) != nil || payload == nil {
		return ErrClientInvocationParameters
	}
	payload["stream"] = json.RawMessage(`true`)
	payload["stream_options"] = json.RawMessage(`{"include_usage":true}`)
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > ClientInvocationMaxBytes {
		return ErrClientInvocationParameters
	}
	text.Payload = raw
	return nil
}

// ClientInvocationChatStream consumes the existing gateway's chat-completions
// SSE output, forwarding only validated text. It never exposes provider error
// bodies, headers or bookkeeping. Reasoning uses a separate bounded sink;
// tool arguments stay buffered until the settled terminal.
// Complete reconstructs the existing bounded response only after a finish,
// usage and [DONE]; the executor separately requires durable settlement.
type ClientInvocationChatStream struct {
	sink          ClientInvocationTextSink
	reasoningSink ClientInvocationTextSink
	pending       []byte
	data          bytes.Buffer
	text          bytes.Buffer
	reasoning     bytes.Buffer
	usage         json.RawMessage
	finish        string
	done          bool
	err           error
	totalBytes    int
	events        int
	tools         []clientInvocationStreamTool
}

func NewClientInvocationChatStream(sink ClientInvocationTextSink, reasoningSink ...ClientInvocationTextSink) *ClientInvocationChatStream {
	stream := &ClientInvocationChatStream{sink: sink}
	if len(reasoningSink) > 0 {
		stream.reasoningSink = reasoningSink[0]
	}
	return stream
}

func (s *ClientInvocationChatStream) Write(raw []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	s.totalBytes += len(raw)
	if s.totalBytes > 64*ClientInvocationMaxBytes {
		s.err = ErrClientInvocationResponse
		return 0, s.err
	}
	n := len(raw)
	for len(raw) > 0 {
		end := bytes.IndexByte(raw, '\n')
		if end < 0 {
			if len(s.pending)+len(raw) > ClientInvocationMaxBytes {
				s.err = ErrClientInvocationResponse
			} else {
				s.pending = append(s.pending, raw...)
			}
			break
		}
		if len(s.pending)+end > ClientInvocationMaxBytes {
			s.err = ErrClientInvocationResponse
			break
		}
		s.pending = append(s.pending, raw[:end]...)
		s.err = s.line(bytes.TrimSuffix(s.pending, []byte{'\r'}))
		s.pending = s.pending[:0]
		raw = raw[end+1:]
		if s.err != nil {
			break
		}
	}
	if s.err != nil {
		return 0, s.err
	}
	return n, nil
}

func (s *ClientInvocationChatStream) line(line []byte) error {
	if len(line) == 0 {
		if s.data.Len() == 0 {
			return nil
		}
		raw := bytes.Clone(s.data.Bytes())
		s.data.Reset()
		return s.event(bytes.TrimSuffix(raw, []byte{'\n'}))
	}
	if line[0] == ':' {
		return nil
	}
	field, value, found := bytes.Cut(line, []byte{':'})
	if !found {
		return ErrClientInvocationResponse
	}
	if !bytes.Equal(field, []byte("data")) {
		// SSE event/id/retry controls do not become application events.
		if bytes.Equal(field, []byte("event")) || bytes.Equal(field, []byte("id")) || bytes.Equal(field, []byte("retry")) {
			return nil
		}
		return ErrClientInvocationResponse
	}
	value = bytes.TrimPrefix(value, []byte{' '})
	if s.data.Len()+len(value)+1 > ClientInvocationMaxBytes {
		return ErrClientInvocationResponse
	}
	s.data.Write(value)
	s.data.WriteByte('\n')
	return nil
}

func (s *ClientInvocationChatStream) event(raw []byte) error {
	s.events++
	if s.done || s.events > 65536 || !utf8.Valid(raw) {
		return ErrClientInvocationResponse
	}
	if bytes.Equal(raw, []byte("[DONE]")) {
		if s.finish == "" || len(s.usage) == 0 {
			return ErrClientInvocationResponse
		}
		s.done = true
		return nil
	}
	if !validClientInvocationJSON(raw) {
		return ErrClientInvocationResponse
	}
	fields, err := readClientObject(raw)
	if err != nil || fields["error"] != nil {
		return ErrClientInvocationResponse
	}
	if usage := fields["usage"]; len(usage) > 0 && !bytes.Equal(bytes.TrimSpace(usage), []byte("null")) {
		if len(s.usage) != 0 {
			return ErrClientInvocationResponse
		}
		parsed, err := parseClientChatCompletionsUsage(usage)
		if err != nil || parsed.InputTokens > 9007199254740991 || parsed.OutputTokens > 9007199254740991 ||
			(parsed.TotalTokens != 0 && parsed.TotalTokens != parsed.InputTokens+parsed.OutputTokens) {
			return ErrClientInvocationResponse
		}
		s.usage = bytes.Clone(usage)
	}
	var choices []json.RawMessage
	if readClientArray(fields["choices"], &choices) != nil || len(choices) > 1 {
		return ErrClientInvocationResponse
	}
	if len(choices) == 0 {
		return nil
	}
	if s.finish != "" {
		return ErrClientInvocationResponse
	}
	choice, err := readClientObject(choices[0])
	if err != nil {
		return err
	}
	if index := choice["index"]; index != nil {
		n, err := readClientInteger(index)
		if err != nil || n != 0 {
			return ErrClientInvocationResponse
		}
	}
	if finish := choice["finish_reason"]; len(finish) > 0 && !bytes.Equal(bytes.TrimSpace(finish), []byte("null")) {
		var reason string
		if s.finish != "" || readClientString(finish, &reason) != nil || !clientContains([]string{"stop", "length", "content_filter", "tool_calls"}, reason) {
			return ErrClientInvocationResponse
		}
		s.finish = reason
	}
	delta, err := readClientObject(choice["delta"])
	if err != nil {
		return err
	}
	if value := delta["function_call"]; len(value) > 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return ErrClientInvocationResponse
	}
	if value := delta["tool_calls"]; len(value) > 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		if err := s.appendTools(value); err != nil {
			return err
		}
	}
	if role := delta["role"]; role != nil {
		var value string
		if readClientString(role, &value) != nil || value != "assistant" {
			return ErrClientInvocationResponse
		}
	}
	if content := delta["content"]; len(content) > 0 && !bytes.Equal(bytes.TrimSpace(content), []byte("null")) {
		var value string
		if readClientString(content, &value) != nil || s.text.Len()+len(value) > ClientInvocationMaxBytes {
			return ErrClientInvocationResponse
		}
		if value != "" {
			s.text.WriteString(value)
			if s.sink == nil || s.sink(value) != nil {
				return ErrClientInvocationOutcomeUnknown
			}
		}
	}
	if reasoning := delta["reasoning_content"]; len(reasoning) > 0 && !bytes.Equal(bytes.TrimSpace(reasoning), []byte("null")) {
		var value string
		if readClientString(reasoning, &value) != nil || s.reasoning.Len()+len(value) > ClientInvocationMaxBytes {
			return ErrClientInvocationResponse
		}
		if value != "" {
			s.reasoning.WriteString(value)
			if s.reasoningSink != nil && s.reasoningSink(value) != nil {
				return ErrClientInvocationOutcomeUnknown
			}
		}
	}
	return nil
}

func (s *ClientInvocationChatStream) Complete() ([]byte, error) {
	if s == nil || s.err != nil || !s.done || s.finish == "" || len(s.usage) == 0 ||
		len(bytes.TrimSpace(s.pending)) != 0 || s.data.Len() != 0 {
		return nil, ErrClientInvocationResponse
	}
	message := map[string]any{"role": "assistant", "content": s.text.String()}
	if s.reasoning.Len() > 0 {
		message["reasoning_content"] = s.reasoning.String()
	}
	if len(s.tools) > 0 {
		if s.finish != "tool_calls" && s.finish != "length" && s.finish != "content_filter" {
			return nil, ErrClientInvocationResponse
		}
		if s.finish == "tool_calls" {
			message["tool_calls"] = s.tools
		}
	} else if s.finish == "tool_calls" {
		return nil, ErrClientInvocationResponse
	}
	return json.Marshal(map[string]any{
		"choices": []map[string]any{{"finish_reason": s.finish, "message": message}},
		"usage":   json.RawMessage(strings.TrimSpace(string(s.usage))),
	})
}

type clientInvocationStreamTool struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func (s *ClientInvocationChatStream) appendTools(raw json.RawMessage) error {
	var deltas []map[string]json.RawMessage
	if json.Unmarshal(raw, &deltas) != nil || len(deltas) > 8 {
		return ErrClientInvocationResponse
	}
	for _, delta := range deltas {
		index, err := readClientInteger(delta["index"])
		if err != nil || index < 0 || index > 7 || index > int64(len(s.tools)) {
			return ErrClientInvocationResponse
		}
		if index == int64(len(s.tools)) {
			s.tools = append(s.tools, clientInvocationStreamTool{})
		}
		tool := &s.tools[index]
		for key, raw := range delta {
			switch key {
			case "index":
			case "id":
				if tool.ID != "" || readClientString(raw, &tool.ID) != nil || !clientIdentifier.MatchString(tool.ID) {
					return ErrClientInvocationResponse
				}
			case "type":
				if tool.Type != "" || readClientString(raw, &tool.Type) != nil || tool.Type != "function" {
					return ErrClientInvocationResponse
				}
			case "function":
				fields, err := readClientObject(raw)
				if err != nil {
					return err
				}
				for key, value := range fields {
					var fragment string
					if readClientString(value, &fragment) != nil {
						return ErrClientInvocationResponse
					}
					switch key {
					case "name":
						if tool.Function.Name != "" || len(fragment) > 200 {
							return ErrClientInvocationResponse
						}
						tool.Function.Name = fragment
					case "arguments":
						if len(tool.Function.Arguments)+len(fragment) > 524800 {
							return ErrClientInvocationResponse
						}
						tool.Function.Arguments += fragment
					default:
						return ErrClientInvocationResponse
					}
				}
			default:
				return ErrClientInvocationResponse
			}
		}
	}
	return nil
}
