package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

var (
	ErrClientInvocationOutcomeUnknown       = infraerrors.New(502, "MODEL_REQUEST_OUTCOME_UNKNOWN", "The request may have completed; check its status before submitting again")
	ErrClientInvocationAdmissionUnavailable = infraerrors.New(503, "MODEL_ADMISSION_UNAVAILABLE", "Model admission is temporarily unavailable")
	// The account cannot pay for the request (balance, usage limit or key
	// quota); the user tops up instead of retrying a temporary outage.
	ErrClientInvocationQuotaExhausted = infraerrors.New(402, "MODEL_QUOTA_EXHAUSTED", "Account balance or usage quota is insufficient")
)

// ClientInvocationDispatcher performs exactly one controlled gateway dispatch
// for the supplied prepared route. It must use ctx for required usage billing,
// return the complete protocol response, and leave retries/failover to the
// existing gateway path.
type ClientInvocationDispatcher interface {
	DispatchClientInvocation(context.Context, *PreparedClientInvocation) ([]byte, error)
}

// ClientInvocationDispatchAdmission binds quota/subscription and concurrency
// checks to the freshly prepared invocation before its durable dispatch claim.
type ClientInvocationDispatchAdmission interface {
	AdmitClientInvocation(context.Context, *PreparedClientInvocation) (dispatchContext context.Context, release func(), err error)
}

type ClientInvocationExecution struct {
	Search  *ClientSearchOutput       `json:"search,omitempty"`
	Receipt *ClientInvocationReceipt  `json:"receipt"`
	Result  *ClientInvocationResponse `json:"result,omitempty"`
}

// ClientInvocationExecutor joins the durable dispatch boundary, protocol
// response parser, and required settlement acknowledgment. It is a service
// primitive; a production route still has to supply quota/concurrency admission
// and a dispatcher backed by the existing gateway.
type ClientInvocationExecutor struct {
	ledger     *ClientInvocationLedgerService
	admission  ClientInvocationDispatchAdmission
	dispatcher ClientInvocationDispatcher
	events     UsageSettlementEventRepository
}

func NewClientInvocationExecutor(ledger *ClientInvocationLedgerService, admission ClientInvocationDispatchAdmission, dispatcher ClientInvocationDispatcher, events UsageSettlementEventRepository) *ClientInvocationExecutor {
	return &ClientInvocationExecutor{ledger: ledger, admission: admission, dispatcher: dispatcher, events: events}
}

func (s *ClientInvocationExecutor) Execute(ctx context.Context, token string, attempt *ClientInvocationAttempt) (*ClientInvocationExecution, error) {
	if s == nil || s.ledger == nil || s.admission == nil || s.dispatcher == nil || s.events == nil {
		return nil, ErrClientGatewayUnavailable
	}
	if attempt == nil || !attempt.Owned() {
		return nil, ErrClientInvocationOwner
	}
	settlementCtx, acknowledgment, err := attempt.UsageSettlementContext(ctx, s.events)
	if err != nil {
		return nil, err
	}
	var release func()
	var dispatchContext context.Context
	prepared, dispatchContext, err := s.ledger.startDispatchWithAdmission(settlementCtx, token, attempt, func(admissionCtx context.Context, fresh *PreparedClientInvocation) (context.Context, error) {
		var admitErr error
		dispatchContext, release, admitErr = s.admission.AdmitClientInvocation(admissionCtx, fresh)
		return dispatchContext, admitErr
	})
	if release != nil {
		defer release()
	}
	if err != nil {
		// StartDispatch can fail after the durable boundary if the grant is
		// revoked concurrently. MarkUnknown only changes already-dispatched rows.
		s.markUnknown(attempt, "dispatch_not_started", err)
		return nil, err
	}
	raw, err := s.dispatcher.DispatchClientInvocation(dispatchContext, prepared)
	if err != nil {
		if rejection := clientInvocationProviderPayloadRejection(dispatchContext); rejection != nil && attempt.record.State == ClientInvocationReserved {
			return nil, rejection
		}
		reason := "provider_dispatch_failed"
		if errors.Is(err, ErrClientInvocationResponse) {
			reason = "response_invalid"
		}
		s.markUnknown(attempt, reason, err)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	if !ClientInvocationProviderPayloadAuthorized(dispatchContext) {
		if rejection := clientInvocationProviderPayloadRejection(dispatchContext); rejection != nil && attempt.record.State == ClientInvocationReserved {
			return nil, rejection
		}
		s.markUnknown(attempt, "provider_payload_unauthorized", nil)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	if prepared.Search != nil {
		var result ClientSearchOutput
		if !validClientInvocationProviderJSON(raw) || json.Unmarshal(raw, &result) != nil || !ValidateClientSearchOutput(&result) || result.Query != prepared.Request.Query || len(result.Sources) > prepared.Request.MaxResults {
			s.markUnknown(attempt, "response_invalid", nil)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		if _, err = acknowledgment.Result(); err != nil {
			s.markUnknown(attempt, "settlement_unconfirmed", err)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		receipt, finishErr := s.ledger.Finish(context.WithoutCancel(ctx), attempt, ClientInvocationOutcome{State: ClientInvocationSucceeded, Search: &result})
		if finishErr != nil {
			s.markUnknown(attempt, "ledger_finish_failed", finishErr)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		return &ClientInvocationExecution{Receipt: receipt, Search: &result}, nil
	}
	result, err := ParseClientInvocationResponseForTools(prepared.Text.Protocol, raw, prepared.Request.Tools)
	if err != nil {
		s.markUnknown(attempt, "response_invalid", err)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	limit := clientToolRoundLimit(prepared.Request.Tools)
	for _, call := range result.ToolCalls {
		if limit > 0 && len(prepared.Request.ToolRounds) >= limit {
			s.markUnknown(attempt, "tool_round_limit_exceeded", nil)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		if !clientContains(prepared.Request.Tools, call.Name) {
			s.markUnknown(attempt, "tool_not_offered", nil)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		for _, round := range prepared.Request.ToolRounds {
			for _, previous := range round.Calls {
				if previous.CallID == call.CallID {
					s.markUnknown(attempt, "tool_call_id_reused", nil)
					return nil, ErrClientInvocationOutcomeUnknown
				}
			}
		}
	}
	if _, err = acknowledgment.Result(); err != nil {
		s.markUnknown(attempt, "settlement_unconfirmed", err)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	text := result.Text
	receipt, err := s.ledger.Finish(context.WithoutCancel(ctx), attempt, ClientInvocationOutcome{State: ClientInvocationSucceeded, Text: &text, FinishReason: result.FinishReason})
	if err != nil {
		s.markUnknown(attempt, "ledger_finish_failed", err)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	if rejected := clientRejectedToolCalls(result.ToolCalls); rejected > 0 {
		logger.L().Warn("client_invocation.tool_calls_rejected", invocationLogFields(attempt, zap.Int("rejected_calls", rejected))...)
	}
	return &ClientInvocationExecution{Receipt: receipt, Result: result}, nil
}

// markUnknown keeps a dispatched attempt non-retryable. The ledger row of an
// unknown attempt carries no outcome, so its reason goes to the log; it holds
// a fixed reason and error code, never provider or user content.
func (s *ClientInvocationExecutor) markUnknown(attempt *ClientInvocationAttempt, reason string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, markErr := s.ledger.MarkUnknown(ctx, attempt)
	fields := invocationLogFields(attempt, zap.String("reason", reason))
	if code := infraerrors.Reason(cause); code != "" {
		fields = append(fields, zap.String("error_reason", code))
	}
	if markErr != nil {
		fields = append(fields, zap.String("mark_error_reason", infraerrors.Reason(markErr)))
	}
	logger.L().Warn("client_invocation.marked_unknown", fields...)
}

func invocationLogFields(attempt *ClientInvocationAttempt, extra ...zap.Field) []zap.Field {
	fields := []zap.Field{zap.String("component", "service.client_invocation")}
	if attempt != nil && attempt.record != nil {
		fields = append(fields, zap.String("invocation_id", attempt.record.InvocationID), zap.String("request_id", attempt.record.RequestID),
			zap.String("project_id", attempt.record.ProjectID), zap.String("route_id", attempt.record.RouteID))
	}
	return append(fields, extra...)
}
