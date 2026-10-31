package handler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func requiredUsageSubmitters(pool *service.UsageRecordWorkerPool) map[string]func(context.Context, service.UsageRecordTask) {
	gateway := &GatewayHandler{usageRecordWorkerPool: pool}
	openai := &OpenAIGatewayHandler{usageRecordWorkerPool: pool}
	return map[string]func(context.Context, service.UsageRecordTask){
		"gateway":           gateway.submitUsageRecordTask,
		"gateway mandatory": gateway.submitMandatoryUsageRecordTask,
		"openai":            openai.submitUsageRecordTask,
		"openai mandatory":  openai.submitMandatoryUsageRecordTask,
		"openai text": func(ctx context.Context, task service.UsageRecordTask) {
			openai.submitOpenAIUsageRecordTask(ctx, &service.OpenAIForwardResult{}, task)
		},
	}
}

func TestRequiredUsageSettlementBypassesSaturatedPool(t *testing.T) {
	pool := service.NewUsageRecordWorkerPoolWithOptions(service.UsageRecordWorkerPoolOptions{
		WorkerCount: 1, QueueSize: 1, TaskTimeout: time.Second,
		OverflowPolicy: "drop", OverflowSamplePercent: 0, AutoScaleEnabled: false,
	})
	t.Cleanup(pool.Stop)
	started, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release) })
	pool.Submit(func(context.Context) { close(started); <-release })
	<-started
	pool.Submit(func(context.Context) {})

	for name, submit := range requiredUsageSubmitters(pool) {
		t.Run(name, func(t *testing.T) {
			ctx, ack := service.WithRequiredUsageSettlement(context.Background(), "client:invocation-fixture", 7)
			called := false
			submit(ctx, func(taskCtx context.Context) {
				called = true
				require.True(t, service.RequiredUsageSettlement(taskCtx))
			})
			require.True(t, called, "the task must finish inline before Submit returns")
			_, err := ack.Result()
			require.Error(t, err, "executing a task alone is not proof of persisted usage and billing")
		})
	}
}

func TestRequiredUsageSettlementSurvivesDisconnectAndKeepsIdentity(t *testing.T) {
	for name, submit := range requiredUsageSubmitters(nil) {
		t.Run(name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			parent = context.WithValue(parent, ctxkey.ClientRequestID, "invocation-stable")
			parent = context.WithValue(parent, ctxkey.RequestID, "transport-only")
			parent, ack := service.WithRequiredUsageSettlement(parent, "client:invocation-stable", 7)
			cancel()
			called := false
			submit(parent, func(ctx context.Context) {
				called = true
				require.NoError(t, ctx.Err())
				require.True(t, service.RequiredUsageSettlement(ctx))
				require.Equal(t, "invocation-stable", ctx.Value(ctxkey.ClientRequestID))
				require.Equal(t, "transport-only", ctx.Value(ctxkey.RequestID))
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.Positive(t, time.Until(deadline))
				require.LessOrEqual(t, time.Until(deadline), 10*time.Second)
			})
			require.True(t, called)
			_, err := ack.Result()
			require.Error(t, err)
		})
	}
}

func TestRequiredUsageSettlementPanicNeverResubmitsOrConfirms(t *testing.T) {
	pool := newUsageRecordTestPool(t)
	for name, submit := range requiredUsageSubmitters(pool) {
		t.Run(name, func(t *testing.T) {
			ctx, ack := service.WithRequiredUsageSettlement(context.Background(), "client:invocation-fixture", 7)
			var calls atomic.Int32
			require.NotPanics(t, func() {
				submit(ctx, func(context.Context) { calls.Add(1); panic("PRIVATE_UPSTREAM_DETAIL") })
			})
			require.EqualValues(t, 1, calls.Load())
			_, err := ack.Result()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE_UPSTREAM_DETAIL")
		})
	}
}

func TestRequiredUsageSettlementNilAndOrdinaryTasks(t *testing.T) {
	require.False(t, submitRequiredUsageRecordTask(nil, nil))
	require.False(t, submitRequiredUsageRecordTask(context.Background(), func(context.Context) { t.Fatal("ordinary task executed by strict policy") }))
	ctx, ack := service.WithRequiredUsageSettlement(context.Background(), "client:invocation-fixture", 7)
	for _, submit := range requiredUsageSubmitters(nil) {
		require.NotPanics(t, func() { submit(ctx, nil) })
	}
	_, err := ack.Result()
	require.Error(t, err)
}
