package service

import (
	"bytes"
	"encoding/json"
)

type ClientInvocationToolCall struct {
	CallID    string                     `json:"callId"`
	Name      string                     `json:"name"`
	Arguments map[string]json.RawMessage `json:"arguments"`
}
type ClientInvocationToolResult struct {
	CallID  string          `json:"callId"`
	Outcome string          `json:"outcome"`
	Content json.RawMessage `json:"content"`
}
type ClientInvocationToolRound struct {
	RoundID   string                       `json:"roundId"`
	Text      string                       `json:"text"`
	Reasoning string                       `json:"reasoning,omitempty"`
	Calls     []ClientInvocationToolCall   `json:"calls"`
	Results   []ClientInvocationToolResult `json:"results"`
}

func validClientToolWire(fields map[string]json.RawMessage) bool {
	for _, name := range []string{"tools", "toolRounds"} {
		if raw, ok := fields[name]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return false
		}
	}
	if raw, ok := fields["tools"]; ok {
		var names []string
		if json.Unmarshal(raw, &names) != nil || len(names) == 0 {
			return false
		}
	}
	if raw, ok := fields["toolRounds"]; ok {
		var rounds []map[string]json.RawMessage
		if json.Unmarshal(raw, &rounds) != nil || rounds == nil {
			return false
		}
		for _, round := range rounds {
			var text string
			if readClientString(round["text"], &text) != nil {
				return false
			}
			if value, ok := round["reasoning"]; ok {
				var reasoning string
				if readClientString(value, &reasoning) != nil {
					return false
				}
			}
			delete(round, "reasoning")
			if !clientInvocationFields(round, "roundId", "text", "calls", "results") {
				return false
			}
			var calls, results []map[string]json.RawMessage
			if json.Unmarshal(round["calls"], &calls) != nil || json.Unmarshal(round["results"], &results) != nil {
				return false
			}
			for _, call := range calls {
				if !clientInvocationFields(call, "callId", "name", "arguments") {
					return false
				}
			}
			for _, result := range results {
				if !clientInvocationFields(result, "callId", "outcome", "content") {
					return false
				}
			}
		}
	}
	return true
}

// Two server-owned tool profiles. File tools edit project text files and keep
// their three-round turn. The editing profile is the single auto_cut tool that
// the client forwards to the project's editor; an editing turn routinely needs
// many rounds, so it has no round cap: user Stop, the balance gate and
// ClientInvocationMaxBytes bound it. The profiles never mix in one request.
const clientAutoCutTool = "auto_cut"
const clientFileToolRounds = 3

var clientFileTools = []string{"read_file", "write_file"}
var clientAutoCutOperations = []string{"tools", "call", "state", "settings", "context"}

func clientEditingProfile(tools []string) bool {
	return len(tools) == 1 && tools[0] == clientAutoCutTool
}

// A tool name is parseable if any profile admits it; the request's own
// profile is enforced against its declared tools afterwards.
func validClientToolCall(call ClientInvocationToolCall) bool {
	if !clientIdentifier.MatchString(call.CallID) || call.Arguments == nil {
		return false
	}
	if call.Name == clientAutoCutTool {
		return validClientAutoCutArguments(call.Arguments)
	}
	if !clientContains(clientFileTools, call.Name) {
		return false
	}
	fields := call.Arguments
	var path string
	if readClientString(fields["path"], &path) != nil || len(path) == 0 || len(path) > 500 {
		return false
	}
	if call.Name == "read_file" {
		return clientInvocationFields(fields, "path")
	}
	var content string
	return clientInvocationFields(fields, "path", "content") && readClientString(fields["content"], &content) == nil && len(content) <= 524288
}

// The forwarded editor call stays opaque JSON; only its envelope is checked.
// Extra keys are tolerated (the client forwards only the known ones), so a
// model's harmless addition does not void an already settled paid response.
func validClientAutoCutArguments(fields map[string]json.RawMessage) bool {
	var operation string
	if readClientString(fields["operation"], &operation) != nil || !clientContains(clientAutoCutOperations, operation) {
		return false
	}
	if raw, ok := fields["tool"]; ok || operation == "call" {
		var tool string
		if readClientString(raw, &tool) != nil || len(tool) == 0 || len(tool) > 200 {
			return false
		}
	}
	if raw, ok := fields["arguments"]; ok {
		var arguments map[string]json.RawMessage
		if json.Unmarshal(raw, &arguments) != nil || arguments == nil {
			return false
		}
	}
	if raw, ok := fields["names"]; ok {
		var names []string
		if json.Unmarshal(raw, &names) != nil || len(names) > 12 {
			return false
		}
		for _, name := range names {
			if len(name) == 0 || len(name) > 200 {
				return false
			}
		}
	}
	if raw, ok := fields["query"]; ok {
		var query string
		if readClientString(raw, &query) != nil || len(query) > 500 {
			return false
		}
	}
	return true
}

// The rounds a request may carry under its tool profile; 0 means uncapped.
func clientToolRoundLimit(tools []string) int {
	if clientEditingProfile(tools) {
		return 0
	}
	return clientFileToolRounds
}

func validClientToolHistory(req *ClientInvocationRequest) bool {
	limit := clientToolRoundLimit(req.Tools)
	if len(req.Tools) > 2 || (limit > 0 && len(req.ToolRounds) > limit) || (len(req.ToolRounds) > 0 && len(req.Tools) == 0) {
		return false
	}
	names := map[string]bool{}
	for _, name := range req.Tools {
		if names[name] || (!clientEditingProfile(req.Tools) && !clientContains(clientFileTools, name)) {
			return false
		}
		names[name] = true
	}
	ids := map[string]bool{}
	rounds := map[string]bool{}
	for _, round := range req.ToolRounds {
		if !clientIdentifier.MatchString(round.RoundID) || rounds[round.RoundID] || len(round.Text) > ClientInvocationMaxBytes || len(round.Reasoning) > ClientInvocationMaxBytes || len(round.Calls) == 0 || len(round.Calls) > 8 || len(round.Calls) != len(round.Results) {
			return false
		}
		rounds[round.RoundID] = true
		calls := map[string]bool{}
		for _, call := range round.Calls {
			if !names[call.Name] || !validClientToolCall(call) || ids[call.CallID] {
				return false
			}
			ids[call.CallID] = true
			calls[call.CallID] = true
		}
		for _, result := range round.Results {
			if !calls[result.CallID] || !clientContains([]string{"success", "error"}, result.Outcome) || !validClientInvocationJSON(result.Content) {
				return false
			}
			delete(calls, result.CallID)
		}
	}
	return true
}
func applyClientFileTools(route ClientModelRoute, req *ClientInvocationRequest, payload map[string]any, platform string) error {
	if len(req.Tools) == 0 {
		return nil
	}
	if route.Protocol != "chat-completions" || !route.Capabilities.Tools {
		return ErrClientInvocationParameters
	}
	tools := []any{}
	system := clientFileToolSystemPrompt
	if clientEditingProfile(req.Tools) {
		tools = append(tools, clientAutoCutToolSchema())
		system = clientAutoCutSystemPrompt
	}
	for _, name := range req.Tools {
		if name == clientAutoCutTool {
			continue
		}
		properties := map[string]any{"path": map[string]any{"type": "string", "description": "File path relative to the current project root."}}
		required := []string{"path"}
		description := "Read a UTF-8 text file from the current project."
		if name == "write_file" {
			properties["content"] = map[string]any{"type": "string", "description": "Complete UTF-8 file contents."}
			required = append(required, "content")
			description = "Write or overwrite a UTF-8 text file in the current project."
		}
		tools = append(tools, map[string]any{"type": "function", "function": map[string]any{"name": name, "description": description, "parameters": map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}}})
	}
	payload["tools"] = tools
	if limit := clientToolRoundLimit(req.Tools); limit > 0 && len(req.ToolRounds) == limit {
		payload["tool_choice"] = "none"
	}
	// The initial user text is the existing bounded conversation. Subsequent tool
	// rounds retain native roles and call IDs; text never impersonates tool results.
	messages := []any{map[string]string{"role": "system", "content": system}, payload["messages"].([]map[string]string)[0]}
	for _, round := range req.ToolRounds {
		calls := []any{}
		for _, call := range round.Calls {
			args, _ := json.Marshal(call.Arguments)
			calls = append(calls, map[string]any{"id": call.CallID, "type": "function", "function": map[string]any{"name": call.Name, "arguments": string(args)}})
		}
		assistant := map[string]any{"role": "assistant", "content": round.Text, "tool_calls": calls}
		if platform == PlatformDeepseek {
			reasoning := round.Reasoning
			if reasoning == "" {
				reasoning = deepSeekChatReasoningPlaceholderText
			}
			assistant["reasoning_content"] = reasoning
		}
		messages = append(messages, assistant)
		for _, result := range round.Results {
			messages = append(messages, map[string]any{"role": "tool", "tool_call_id": result.CallID, "content": string(result.Content)})
		}
	}
	payload["messages"] = messages
	return nil
}

const clientFileToolSystemPrompt = "You are the project assistant in AutoDesign. Use the available project file tools when the user asks you to create or modify a page or file. Write complete, usable files rather than only pasting code in the chat. For a new web page, use index.html unless the user specifies a different path. For edits, read the relevant existing file first and preserve unrelated content. File paths are relative to the current project. After successful writes, briefly describe the result; the application provides the file preview. For questions that do not require file changes, answer normally."

// The editor's own agent prompt arrives in the user text from the client; this
// only states how the single forwarding tool reaches it.
const clientAutoCutSystemPrompt = "You are the video editing assistant in AutoDesign, working in the user's current editing project. The only tool is auto_cut, which forwards to the project's real editor. Use operation=tools to list editor tools, names or query to read exact schemas, and operation=call with tool and arguments to run one. Follow the editing instructions in the user message: open an edit session, make the edits, and finish with review_edit_session; only status=applied means the edit was applied. Verify results with focused reads instead of assuming them. Take as many tool rounds as the task needs, and stop calling tools once it is done or cannot proceed, then briefly tell the user what changed. For questions that need no editing, answer normally."

func clientAutoCutToolSchema() map[string]any {
	return map[string]any{"type": "function", "function": map[string]any{
		"name":        clientAutoCutTool,
		"description": "Forward one request to the current project's video editor. operation=tools returns a compact index of editor tools; names (exact tool names) or query (search text) return their full input schemas. operation=call runs tool with arguments unchanged. operation=context re-reads the editor's agent instructions; operation=state reads the saved project; operation=settings reports transcription and provider readiness.",
		"parameters": map[string]any{"type": "object", "properties": map[string]any{
			"operation": map[string]any{"type": "string", "enum": clientAutoCutOperations},
			"tool":      map[string]any{"type": "string", "description": "Editor tool name, required for operation=call."},
			"arguments": map[string]any{"type": "object", "additionalProperties": true, "description": "Arguments for the editor tool, matching its schema."},
			"names":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Exact tool names whose full input schemas are needed."},
			"query":     map[string]any{"type": "string", "description": "Search tool names and descriptions."},
		}, "required": []string{"operation"}},
	}}
}

func parseClientToolCalls(raw json.RawMessage) ([]ClientInvocationToolCall, error) {
	var wire []struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	}
	if json.Unmarshal(raw, &wire) != nil || len(wire) == 0 || len(wire) > 8 {
		return nil, ErrClientInvocationResponse
	}
	calls := []ClientInvocationToolCall{}
	seen := map[string]bool{}
	for _, item := range wire {
		call := ClientInvocationToolCall{CallID: item.ID, Name: item.Function.Name}
		if item.Type != "function" || !validClientInvocationJSON([]byte(item.Function.Arguments)) || json.Unmarshal([]byte(item.Function.Arguments), &call.Arguments) != nil || !validClientToolCall(call) || seen[call.CallID] {
			return nil, ErrClientInvocationResponse
		}
		seen[call.CallID] = true
		calls = append(calls, call)
	}
	return calls, nil
}
