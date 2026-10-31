package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

// Invalidations follow the durable database effects. They do not replace the
// billing transaction or claim that concurrent cache fills are fenced.
type UsageSettlementCacheInvalidator interface {
	InvalidateUsageSettlement(context.Context, *UsageSettlementEvent) error
}

type UsageSettlementRecoveryResult struct {
	Claimed   int `json:"-"`
	Settled   int `json:"-"`
	Deferred  int `json:"-"`
	Conflicts int `json:"-"`
}

// This worker has no model dispatcher or pricing service. Its only inputs are
// frozen accounting events; a recovery can never acquire inference rights.
type UsageSettlementReconciler struct {
	events           UsageSettlementEventRepository
	billing          UsageBillingRepository
	logs             UsageLogRepository
	caches           UsageSettlementCacheInvalidator
	mu               sync.Mutex
	started, stopped bool
	cancel           context.CancelFunc
	wg               sync.WaitGroup
}

func NewUsageSettlementReconciler(events UsageSettlementEventRepository, billing UsageBillingRepository, logs UsageLogRepository, caches UsageSettlementCacheInvalidator) *UsageSettlementReconciler {
	return &UsageSettlementReconciler{events: events, billing: billing, logs: logs, caches: caches}
}

func (s *UsageSettlementReconciler) Reconcile(ctx context.Context, limit int) (UsageSettlementRecoveryResult, error) {
	var result UsageSettlementRecoveryResult
	if s == nil || s.events == nil || s.billing == nil || s.logs == nil || s.caches == nil || limit < 1 || limit > 100 {
		return result, ErrUsageSettlementUnconfirmed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for range limit {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		token, err := gatewayRandom("settlement-")
		if err != nil {
			return result, ErrUsageSettlementUnconfirmed
		}
		// Claim immediately before processing, rather than letting a batch's
		// later leases expire while earlier database operations are pending.
		events, err := s.events.ClaimPending(ctx, token, 1)
		if err != nil {
			return result, err
		}
		if len(events) == 0 {
			return result, nil
		}
		if len(events) != 1 || events[0] == nil {
			return result, ErrUsageSettlementUnconfirmed
		}
		event := events[0]
		result.Claimed++
		err = s.replay(ctx, event, token)
		if err == nil {
			result.Settled++
			continue
		}
		conflict := errors.Is(err, ErrUsageBillingRequestConflict) || errors.Is(err, ErrUsageSettlementEventConflict) || errors.Is(err, ErrUsageSettlementPayload)
		if conflict {
			result.Conflicts++
		} else {
			result.Deferred++
		}
		// A timed-out operation must still get a bounded opportunity to release
		// its lease. If that fails, expiry makes it recoverable without a send.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		releaseErr := s.events.Release(releaseCtx, event.InvocationID, token, conflict)
		cancel()
		if releaseErr != nil && !errors.Is(releaseErr, ErrUsageSettlementLease) {
			return result, releaseErr
		}
	}
	return result, nil
}

func (s *UsageSettlementReconciler) replay(parent context.Context, event *UsageSettlementEvent, token string) (replayErr error) {
	defer func() {
		if recover() != nil {
			replayErr = ErrUsageSettlementUnconfirmed
		}
	}()
	// Snapshot decoding owns all pointers and revalidates the immutable hash.
	raw, err := EncodeUsageSettlementEvent(event)
	if err != nil {
		return err
	}
	frozen, err := DecodeUsageSettlementEvent(raw)
	if err != nil {
		return err
	}
	if event.State != UsageSettlementPending || event.CreatedAt.IsZero() {
		return ErrUsageSettlementPayload
	}
	frozen.Usage.CreatedAt = event.CreatedAt
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	ctx, ack := WithRequiredUsageSettlement(ctx, frozen.RequestID, frozen.APIKeyID)
	err = func() (recordErr error) {
		defer completeRequiredUsageSettlement(ctx, &recordErr)
		if err := validateRequiredUsageBilling(ctx, frozen.Command, frozen.Usage); err != nil {
			return err
		}
		bill, err := s.billing.Apply(ctx, frozen.Command)
		if err != nil {
			return err
		}
		if err := observeRequiredUsageBilling(ctx, frozen.Command, bill); err != nil {
			return err
		}
		if frozen.SchemaVersion == 2 {
			if err := s.events.RecordPlatformQuota(ctx, event.InvocationID, token); err != nil {
				return err
			}
		}
		if err := writeRequiredUsageLog(ctx, s.logs, frozen.Usage); err != nil {
			return err
		}
		return s.caches.InvalidateUsageSettlement(ctx, frozen)
	}()
	if err != nil {
		return err
	}
	proof, err := ack.Result()
	if err != nil {
		return err
	}
	return s.events.Complete(ctx, event.InvocationID, token, proof)
}

func (s *UsageSettlementReconciler) Start() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopped {
		return
	}
	s.started = true
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			runCtx, cancelRun := context.WithTimeout(ctx, 25*time.Second)
			result, err := s.Reconcile(runCtx, 20)
			cancelRun()
			if ctx.Err() != nil {
				return
			}
			if err != nil || result.Conflicts > 0 {
				// Database errors and frozen metadata stay out of this diagnostic.
				logger.L().With(zap.Int("deferred", result.Deferred), zap.Int("conflicts", result.Conflicts)).Warn("usage_settlement.recovery_incomplete")
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *UsageSettlementReconciler) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.stopped = true
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

type usageSettlementCacheInvalidator struct {
	billing       *BillingCacheService
	subscriptions *SubscriptionService
}

func (s *usageSettlementCacheInvalidator) InvalidateUsageSettlement(ctx context.Context, event *UsageSettlementEvent) error {
	if s == nil || s.billing == nil || event == nil || event.Command == nil || event.Usage == nil {
		return ErrUsageSettlementUnconfirmed
	}
	cmd := event.Command
	var errs []error
	errs = append(errs, s.billing.InvalidateUserBalance(ctx, cmd.UserID), s.billing.InvalidateAPIKeyRateLimit(ctx, cmd.APIKeyID))
	if cmd.SubscriptionID != nil {
		if event.Usage.GroupID == nil || s.subscriptions == nil {
			return ErrUsageSettlementUnconfirmed
		}
		groupID := *event.Usage.GroupID
		s.subscriptions.InvalidateSubCacheSync(cmd.UserID, groupID)
		errs = append(errs, s.billing.InvalidateSubscription(ctx, cmd.UserID, groupID), s.billing.PublishSubscriptionCacheInvalidation(ctx, subCacheKey(cmd.UserID, groupID)))
	}
	return errors.Join(errs...)
}

func ProvideUsageSettlementReconciler(events UsageSettlementEventRepository, billing UsageBillingRepository, logs UsageLogRepository, caches *BillingCacheService, subscriptions *SubscriptionService) *UsageSettlementReconciler {
	svc := NewUsageSettlementReconciler(events, billing, logs, &usageSettlementCacheInvalidator{caches, subscriptions})
	svc.Start()
	return svc
}
