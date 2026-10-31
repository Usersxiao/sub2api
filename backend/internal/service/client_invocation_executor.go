package service

import (
	"context"
	"encoding/json"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

var (
	ErrClientInvocationOutcomeUnknown       = infraerrors.New(502, "MODEL_REQUEST_OUTCOME_UNKNOWN", "The request may have completed; check its status before submitting again")
	ErrClientInvocationAdmissionUnavailable = infraerrors.New(503, "MODEL_ADMISSION_UNAVAILABLE", "Model admission is temporarily unavailable")
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
		s.markUnknown(attempt)
		return nil, err
	}
	raw, err := s.dispatcher.DispatchClientInvocation(dispatchContext, prepared)
	if err != nil {
		if rejection := clientInvocationProviderPayloadRejection(dispatchContext); rejection != nil && attempt.record.State == ClientInvocationReserved {
			return nil, rejection
		}
		s.markUnknown(attempt)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	if !ClientInvocationProviderPayloadAuthorized(dispatchContext) {
		if rejection := clientInvocationProviderPayloadRejection(dispatchContext); rejection != nil && attempt.record.State == ClientInvocationReserved {
			return nil, rejection
		}
		s.markUnknown(attempt)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	if prepared.Search != nil {
		var result ClientSearchOutput
		if !validClientInvocationProviderJSON(raw) || json.Unmarshal(raw, &result) != nil || !ValidateClientSearchOutput(&result) || result.Query != prepared.Request.Query || len(result.Sources) > prepared.Request.MaxResults {
			s.markUnknown(attempt)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		if _, err = acknowledgment.Result(); err != nil {
			s.markUnknown(attempt)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		receipt, finishErr := s.ledger.Finish(context.WithoutCancel(ctx), attempt, ClientInvocationOutcome{State: ClientInvocationSucceeded, Search: &result})
		if finishErr != nil {
			s.markUnknown(attempt)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		return &ClientInvocationExecution{Receipt: receipt, Search: &result}, nil
	}
	result, err := ParseClientInvocationResponse(prepared.Text.Protocol, raw)
	if err != nil {
		s.markUnknown(attempt)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	limit := clientToolRoundLimit(prepared.Request.Tools)
	for _, call := range result.ToolCalls {
		if (limit > 0 && len(prepared.Request.ToolRounds) >= limit) || !clientContains(prepared.Request.Tools, call.Name) {
			s.markUnknown(attempt)
			return nil, ErrClientInvocationOutcomeUnknown
		}
		for _, round := range prepared.Request.ToolRounds {
			for _, previous := range round.Calls {
				if previous.CallID == call.CallID {
					s.markUnknown(attempt)
					return nil, ErrClientInvocationOutcomeUnknown
				}
			}
		}
	}
	if _, err = acknowledgment.Result(); err != nil {
		s.markUnknown(attempt)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	text := result.Text
	receipt, err := s.ledger.Finish(context.WithoutCancel(ctx), attempt, ClientInvocationOutcome{State: ClientInvocationSucceeded, Text: &text, FinishReason: result.FinishReason})
	if err != nil {
		s.markUnknown(attempt)
		return nil, ErrClientInvocationOutcomeUnknown
	}
	return &ClientInvocationExecution{Receipt: receipt, Result: result}, nil
}

func (s *ClientInvocationExecutor) markUnknown(attempt *ClientInvocationAttempt) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = s.ledger.MarkUnknown(ctx, attempt)
}
