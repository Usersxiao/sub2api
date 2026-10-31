//go:build unit

package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestClientInvocationSettlementUsesStableInvocationNotProjectRequestID(t *testing.T) {
	var contexts []context.Context
	for _, project := range []string{"project-a", "project-b"} {
		svc, prep, _, _, raw := invocationLedgerServiceFixture(t)
		prep.prepared.Request.ProjectID = project
		attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
		require.NoError(t, err)
		parent := context.WithValue(context.Background(), ctxkey.ClientRequestID, "caller-controlled-request-id")
		ctx, ack, err := attempt.UsageSettlementContext(parent, &usageRecoveryEventRepositoryStub{})
		require.NoError(t, err)
		require.True(t, RequiredUsageSettlement(ctx))
		require.Equal(t, attempt.Receipt.InvocationID, ctx.Value(ctxkey.ClientRequestID))
		require.Equal(t, "client:"+attempt.Receipt.InvocationID, resolveUsageBillingRequestID(ctx, "ordinary-upstream-response-id"))
		_, err = ack.Result()
		require.Error(t, err, "a bound context is not a persisted settlement")
		contexts = append(contexts, ctx)
		// Building the context again after a transport retry preserves the event
		// identity, without creating another paid request or exposing a secret.
		retryCtx, _, err := attempt.UsageSettlementContext(nil, &usageRecoveryEventRepositoryStub{})
		require.NoError(t, err)
		require.Equal(t, ctx.Value(ctxkey.ClientRequestID), retryCtx.Value(ctxkey.ClientRequestID))
		encoded, err := json.Marshal(attempt)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(encoded))
	}
	require.NotEqual(t, resolveUsageBillingRequestID(contexts[0], "same-upstream"), resolveUsageBillingRequestID(contexts[1], "same-upstream"))
}

func TestClientInvocationSettlementRejectsUnownedOrInvalidAttempt(t *testing.T) {
	for _, attempt := range []*ClientInvocationAttempt{nil, {}, {record: &ClientInvocationRecord{}},
		{ownerToken: "private", record: &ClientInvocationRecord{InvocationID: "invalid/id", BillingIdentityID: 7}},
		{ownerToken: "private", record: &ClientInvocationRecord{InvocationID: "invocation-id"}},
	} {
		ctx, ack, err := attempt.UsageSettlementContext(context.Background(), &usageRecoveryEventRepositoryStub{})
		require.ErrorIs(t, err, ErrClientInvocationOwner)
		require.Nil(t, ctx)
		require.Nil(t, ack)
	}
}

func TestClientInvocationSettlementRequiresDurableEventRepository(t *testing.T) {
	svc, _, _, _, raw := invocationLedgerServiceFixture(t)
	attempt, err := svc.Reserve(context.Background(), "adg_fixture", "chat", raw)
	require.NoError(t, err)
	ctx, ack, err := attempt.UsageSettlementContext(context.Background(), nil)
	require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
	require.Nil(t, ctx)
	require.Nil(t, ack)
}
