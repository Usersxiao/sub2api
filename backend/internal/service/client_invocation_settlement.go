package service

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

// UsageSettlementContext binds accounting to the immutable, server-generated
// invocation identity. Client request IDs alone are only unique within a
// project, while gateway billing deduplication is scoped to an API key.
// This private context authorizes no dispatch or recovery; the execution adapter
// must first acquire StartDispatch and must preserve Result's independent
// unconfirmed state even when a model response has already completed.
func (a *ClientInvocationAttempt) UsageSettlementContext(ctx context.Context, events UsageSettlementEventRepository) (context.Context, *UsageSettlementAcknowledgment, error) {
	if !a.Owned() || a.record == nil || !clientIdentifier.MatchString(a.record.InvocationID) || a.record.BillingIdentityID <= 0 {
		return nil, nil, ErrClientInvocationOwner
	}
	if events == nil {
		return nil, nil, ErrUsageSettlementUnconfirmed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, ctxkey.ClientRequestID, a.record.InvocationID)
	ctx, ack := WithDurableUsageSettlement(ctx, &UsageSettlementBinding{InvocationID: a.record.InvocationID, OwnerToken: a.ownerToken}, a.record.BillingIdentityID, events)
	return ctx, ack, nil
}
