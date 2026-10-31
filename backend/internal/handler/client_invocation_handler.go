package handler

import (
	"context"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type clientInvocationGrantValidator interface {
	Validate(context.Context, string, string, string) (*service.ClientGatewayGrant, error)
}

// clientInvocationRunner is the private composition boundary. Submit owns the
// reserve/execute/replay rule; Get is the recovery query for a lost response.
type clientInvocationRunner interface {
	Submit(context.Context, string, string, []byte) (*service.ClientInvocationExecution, error)
	Get(context.Context, string, string, string) (*service.ClientInvocationReceipt, error)
}

// ClientInvocationHandler is the public HTTP boundary for client model calls.
// Without a runner (client_invocation.enabled=false) it only validates the
// restricted grant and fails closed; nothing is reserved or dispatched.
type ClientInvocationHandler struct {
	grants clientInvocationGrantValidator
	runner clientInvocationRunner
}

func NewClientInvocationHandler(grants clientInvocationGrantValidator) *ClientInvocationHandler {
	return &ClientInvocationHandler{grants: grants}
}

func newClientInvocationHandlerWithRunner(grants clientInvocationGrantValidator, runner clientInvocationRunner) *ClientInvocationHandler {
	return &ClientInvocationHandler{grants: grants, runner: runner}
}

type clientInvocationUsageDTO struct {
	InputTokens  int64 `json:"inputTokens"`
	OutputTokens int64 `json:"outputTokens"`
	TotalTokens  int64 `json:"totalTokens"`
}

type clientInvocationOutputDTO struct {
	Text         string                             `json:"text"`
	Reasoning    string                             `json:"reasoning,omitempty"`
	FinishReason string                             `json:"finishReason,omitempty"`
	ToolCalls    []service.ClientInvocationToolCall `json:"toolCalls,omitempty"`
	Usage        clientInvocationUsageDTO           `json:"usage"`
}

// Amounts stay in the sub ledger. The response carries the durable receipt and,
// only for the attempt that dispatched, the completed text and provider usage.
type clientInvocationResponseDTO struct {
	Receipt *service.ClientInvocationReceipt `json:"receipt"`
	Output  any                              `json:"output,omitempty"`
}

func clientInvocationBearer(c *gin.Context) (string, bool) {
	parts := strings.Fields(c.GetHeader("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	return parts[1], true
}

func (h *ClientInvocationHandler) Invoke(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	token, ok := clientInvocationBearer(c)
	if !ok {
		response.Unauthorized(c, "Authentication required")
		return
	}
	if h == nil || h.grants == nil {
		response.ErrorFrom(c, service.ErrClientInvocationAdmissionUnavailable)
		return
	}
	routeID := c.Param("route_id")
	if h.runner == nil {
		grant, err := h.grants.Validate(c.Request.Context(), token, routeID, "invoke")
		if err != nil {
			response.ErrorFrom(c, err)
			return
		}
		if grant == nil {
			response.ErrorFrom(c, service.ErrClientGatewayUnavailable)
			return
		}
		response.ErrorFrom(c, service.ErrClientInvocationAdmissionUnavailable)
		return
	}
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		response.ErrorFrom(c, service.ErrClientInvocationParameters)
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, service.ClientInvocationMaxBytes+64*1024)
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		response.ErrorFrom(c, service.ErrClientInvocationParameters)
		return
	}
	// The request context stays attached so the stream sink observes client
	// disconnects. The shared gateway may still drain upstream usage; a lost
	// stream is not a refund or a provider-side stop and is never re-dispatched.
	ctx := WithClientInvocationIngressRequest(c.Request.Context(), c.Request, ip.GetTrustedClientIP(c))
	if clientInvocationWantsStream(c.GetHeader("Accept")) {
		h.invokeStream(c, ctx, token, routeID, raw)
		return
	}
	execution, err := h.runner.Submit(ctx, token, routeID, raw)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if execution == nil || execution.Receipt == nil {
		response.ErrorFrom(c, service.ErrClientInvocationOutcomeUnknown)
		return
	}
	body := clientInvocationResponseDTO{Receipt: execution.Receipt}
	if execution.Search != nil {
		body.Output = execution.Search
	}
	if execution.Result != nil {
		usage := execution.Result.Usage
		body.Output = &clientInvocationOutputDTO{Text: execution.Result.Text, Reasoning: execution.Result.Reasoning, FinishReason: execution.Result.FinishReason, ToolCalls: execution.Result.ToolCalls, Usage: clientInvocationUsageDTO{
			InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, TotalTokens: usage.TotalTokens,
		}}
	}
	response.Success(c, body)
}

// GetInvocation reconciles a submission by its original project/request identity.
func (h *ClientInvocationHandler) GetInvocation(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	token, ok := clientInvocationBearer(c)
	if !ok {
		response.Unauthorized(c, "Authentication required")
		return
	}
	if h == nil || h.runner == nil {
		response.ErrorFrom(c, service.ErrClientInvocationAdmissionUnavailable)
		return
	}
	receipt, err := h.runner.Get(c.Request.Context(), token, c.Param("project_id"), c.Param("request_id"))
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, clientInvocationResponseDTO{Receipt: receipt})
}
