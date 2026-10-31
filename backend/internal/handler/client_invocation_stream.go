package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"strings"
	"sync"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type clientInvocationStreamRunner interface {
	SubmitStream(context.Context, string, string, []byte, service.ClientInvocationTextSink) (*service.ClientInvocationExecution, error)
}

type clientInvocationStreamErrorDTO struct {
	Code string `json:"code"`
}

type clientInvocationStreamEventDTO struct {
	SchemaVersion int                              `json:"schemaVersion"`
	Type          string                           `json:"type"`
	Sequence      int                              `json:"sequence"`
	ProjectID     string                           `json:"projectId"`
	RequestID     string                           `json:"requestId"`
	RouteID       string                           `json:"routeId"`
	Delta         string                           `json:"delta,omitempty"`
	Receipt       *service.ClientInvocationReceipt `json:"receipt,omitempty"`
	Output        *clientInvocationOutputDTO       `json:"output,omitempty"`
	Error         *clientInvocationStreamErrorDTO  `json:"error,omitempty"`
}

type clientInvocationSSEWriter struct {
	// mu serializes the executor's events with the keepalive goroutine.
	mu      sync.Mutex
	c       *gin.Context
	request *service.ClientInvocationRequest
	started bool
	// finished is set by the terminal event; nothing may follow it.
	finished bool
	sequence int
}

// clientInvocationStreamKeepAlive bounds the silence a started stream may show
// the client. Tool arguments stay buffered until the settled terminal, so a
// turn that writes a large file produces no event for minutes while the
// provider is still generating; without a comment the client cannot tell that
// from a dead connection and abandons a request that is still being billed.
var clientInvocationStreamKeepAlive = 15 * time.Second

// keepAlive writes SSE comments until stop is called. It never starts the
// stream: before the first event the response can still be a JSON error.
func (w *clientInvocationSSEWriter) keepAlive() (stop func()) {
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		ticker := time.NewTicker(clientInvocationStreamKeepAlive)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-w.c.Request.Context().Done():
				return
			case <-ticker.C:
				w.mu.Lock()
				if w.started && !w.finished {
					if _, err := w.c.Writer.WriteString(": keepalive\n\n"); err == nil {
						w.c.Writer.Flush()
					}
				}
				w.mu.Unlock()
			}
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}

func clientInvocationWantsStream(accept string) bool {
	for _, candidate := range strings.Split(accept, ",") {
		kind, parameters, err := mime.ParseMediaType(strings.TrimSpace(candidate))
		if err == nil && kind == "text/event-stream" && parameters["q"] != "0" {
			return true
		}
	}
	return false
}

func (w *clientInvocationSSEWriter) write(event clientInvocationStreamEventDTO) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.c.Request.Context().Err(); err != nil {
		return err
	}
	w.sequence++
	event.SchemaVersion, event.Sequence = 1, w.sequence
	event.ProjectID, event.RequestID, event.RouteID = w.request.ProjectID, w.request.RequestID, w.request.RouteID
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if !w.started {
		w.c.Header("Content-Type", "text/event-stream; charset=utf-8")
		w.c.Header("Cache-Control", "no-store")
		w.c.Header("X-Accel-Buffering", "no")
		w.started = true
	}
	if _, err = fmt.Fprintf(w.c.Writer, "event: %s\ndata: %s\n\n", event.Type, raw); err != nil {
		return err
	}
	w.c.Writer.Flush()
	w.finished = event.Type == "terminal"
	return nil
}

func (h *ClientInvocationHandler) invokeStream(c *gin.Context, ctx context.Context, token, routeID string, raw []byte) {
	runner, ok := h.runner.(clientInvocationStreamRunner)
	if !ok {
		response.ErrorFrom(c, service.ErrClientInvocationAdmissionUnavailable)
		return
	}
	request, err := service.ParseClientInvocation(raw)
	if err == nil && request.Operation != "text" {
		err = service.ErrClientInvocationParameters
	}
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	writer := &clientInvocationSSEWriter{c: c, request: request}
	stopKeepAlive := writer.keepAlive()
	defer stopKeepAlive()
	ctx = service.WithClientInvocationReasoningStream(ctx, func(delta string) error {
		return writer.write(clientInvocationStreamEventDTO{Type: "reasoning_delta", Delta: delta})
	})
	execution, err := runner.SubmitStream(ctx, token, routeID, raw, func(delta string) error {
		return writer.write(clientInvocationStreamEventDTO{Type: "text_delta", Delta: delta})
	})
	if err != nil {
		if !writer.started {
			response.ErrorFrom(c, err)
			return
		}
		// Text is provisional until the executor confirms settlement. Any failure
		// after output starts is unknown; never forward provider error details.
		_ = writer.write(clientInvocationStreamEventDTO{Type: "terminal", Error: &clientInvocationStreamErrorDTO{Code: infraerrors.Reason(service.ErrClientInvocationOutcomeUnknown)}})
		return
	}
	if execution == nil || execution.Receipt == nil {
		if !writer.started {
			response.ErrorFrom(c, service.ErrClientInvocationOutcomeUnknown)
		} else {
			_ = writer.write(clientInvocationStreamEventDTO{Type: "terminal", Error: &clientInvocationStreamErrorDTO{Code: "MODEL_REQUEST_OUTCOME_UNKNOWN"}})
		}
		return
	}
	terminal := clientInvocationStreamEventDTO{Type: "terminal", Receipt: execution.Receipt}
	if execution.Result != nil && execution.Receipt.State == service.ClientInvocationSucceeded {
		usage := execution.Result.Usage
		terminal.Output = &clientInvocationOutputDTO{Text: execution.Result.Text, Reasoning: execution.Result.Reasoning, FinishReason: execution.Result.FinishReason, ToolCalls: execution.Result.ToolCalls, Usage: clientInvocationUsageDTO{
			InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, TotalTokens: usage.TotalTokens,
		}}
	}
	_ = writer.write(terminal)
}
