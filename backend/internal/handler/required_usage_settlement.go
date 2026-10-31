package handler

import (
	"context"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"go.uber.org/zap"
)

// Required internal settlements finish before the gateway returns to its
// adapter. They bypass the optional worker queue and its drop/sample policy.
// Keeping the private acknowledgment on the detached context lets RecordUsage
// confirm actual database writes after a caller disconnects. A task returning
// without confirming those writes never constitutes a settlement acknowledgment.
func submitRequiredUsageRecordTask(parent context.Context, task service.UsageRecordTask) (handled bool) {
	if !service.RequiredUsageSettlement(parent) {
		return false
	}
	handled = true
	if task == nil {
		return true
	}
	task, _ = wrapUsageRecordTaskContext(parent, task)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer cancel()
	defer func() {
		if recover() != nil {
			service.FailRequiredUsageSettlement(parent, service.ErrUsageSettlementUnconfirmed)
			// Keep the acknowledgment unconfirmed. Panic contents can contain
			// upstream data, so only record the fixed diagnostic here.
			logger.L().With(zap.String("component", "handler.gateway.usage")).Error("gateway.required_usage_record_task_panic")
		}
	}()
	task(ctx)
	return true
}
