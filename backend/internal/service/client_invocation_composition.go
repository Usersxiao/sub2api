package service

import (
	"context"
	"errors"
	"reflect"
)

// ErrClientInvocationCompositionUnavailable indicates that a required private
// executor dependency was not supplied.
var ErrClientInvocationCompositionUnavailable = errors.New("client invocation composition is incomplete")

// ClientInvocationCompositionDependencies are the required server-owned edges
// for the private text executor. The ledger must be durable, and the event
// repository must acknowledge persisted usage before a result can succeed.
type ClientInvocationCompositionDependencies struct {
	Preparer         ClientInvocationPreparer
	GrantValidator   ClientInvocationGrantValidator
	Ledger           ClientInvocationLedgerRepository
	Accounts         ClientInvocationTokenAccountRepository
	OpenAIGateway    *OpenAIGatewayService
	Admission        ClientInvocationDispatchAdmission
	Dispatcher       ClientInvocationDispatcher
	SettlementEvents UsageSettlementEventRepository
}

// ClientInvocationComposition is the single validated assembly boundary used by
// the client invoke route. It is constructed only when client_invocation.enabled
// is set; otherwise the route keeps failing closed after grant validation.
type ClientInvocationComposition struct {
	Ledger   *ClientInvocationLedgerService
	Executor *ClientInvocationExecutor
}

// NewClientInvocationComposition constructs the private ledger and executor
// with the official OpenAI native counter. It does not register a route.
func NewClientInvocationComposition(deps ClientInvocationCompositionDependencies) (*ClientInvocationComposition, error) {
	if isNilClientInvocationDependency(deps.Preparer) ||
		isNilClientInvocationDependency(deps.GrantValidator) ||
		isNilClientInvocationDependency(deps.Ledger) ||
		isNilClientInvocationDependency(deps.Accounts) ||
		deps.OpenAIGateway == nil || isNilClientInvocationDependency(deps.OpenAIGateway.httpUpstream) ||
		isNilClientInvocationDependency(deps.Admission) ||
		isNilClientInvocationDependency(deps.Dispatcher) ||
		isNilClientInvocationDependency(deps.SettlementEvents) {
		return nil, ErrClientInvocationCompositionUnavailable
	}

	counter := NewClientInvocationNativeTokenCounter(deps.Accounts, deps.OpenAIGateway)
	ledger := NewClientInvocationLedgerService(deps.Preparer, deps.GrantValidator, deps.Ledger, counter)
	executor := NewClientInvocationExecutor(ledger, deps.Admission, deps.Dispatcher, deps.SettlementEvents)
	return &ClientInvocationComposition{Ledger: ledger, Executor: executor}, nil
}

// Submit reserves one logical submission and executes it only when this request
// won the durable reservation. A replay of an existing projectId/requestId
// returns its receipt without a result and never dispatches again; dispatched or
// unknown outcomes must be reconciled through Get with the original identity.
func (c *ClientInvocationComposition) Submit(ctx context.Context, token, routeID string, raw []byte) (*ClientInvocationExecution, error) {
	if c == nil || c.Ledger == nil || c.Executor == nil {
		return nil, ErrClientInvocationAdmissionUnavailable
	}
	attempt, err := c.Ledger.Reserve(ctx, token, routeID, raw)
	if err != nil {
		return nil, err
	}
	if attempt == nil || attempt.Receipt == nil {
		return nil, ErrClientInvocationConflict
	}
	if !attempt.Owned() {
		return &ClientInvocationExecution{Receipt: attempt.Receipt}, nil
	}
	return c.Executor.Execute(ctx, token, attempt)
}

// SubmitStream uses the same durable reservation and executor as JSON invoke.
// The sink receives provisional text only; Execute still requires a complete
// protocol result and the same request's durable usage acknowledgment.
func (c *ClientInvocationComposition) SubmitStream(ctx context.Context, token, routeID string, raw []byte, sink ClientInvocationTextSink) (*ClientInvocationExecution, error) {
	if sink == nil {
		return nil, ErrClientInvocationParameters
	}
	return c.Submit(WithClientInvocationTextStream(ctx, sink), token, routeID, raw)
}

// Get returns the durable receipt for the caller's current grant. It is the
// only recovery path after a lost response; it never dispatches.
func (c *ClientInvocationComposition) Get(ctx context.Context, token, projectID, requestID string) (*ClientInvocationReceipt, error) {
	if c == nil || c.Ledger == nil {
		return nil, ErrClientInvocationAdmissionUnavailable
	}
	return c.Ledger.Get(ctx, token, projectID, requestID)
}

func isNilClientInvocationDependency(value any) bool {
	if value == nil {
		return true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}
