//go:build unit

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type usageRecoveryEventRepositoryStub struct {
	UsageSettlementEventRepository
	event                       *UsageSettlementEvent
	blocked                     bool
	completeErr                 error
	journalErr                  error
	journalCalls                int
	journalEntries              int
	journalFingerprint          string
	claims, completes, releases int
	conflict                    bool
}

func (r *usageRecoveryEventRepositoryStub) RecordPlatformQuota(_ context.Context, id, token string) error {
	r.journalCalls++
	if r.journalErr != nil {
		return r.journalErr
	}
	if r.event == nil || token != r.event.LeaseToken || id != r.event.InvocationID {
		return ErrUsageSettlementLease
	}
	if r.journalFingerprint != "" && r.journalFingerprint != r.event.EventFingerprint {
		return ErrUsageSettlementEventConflict
	}
	if r.journalFingerprint == "" {
		r.journalEntries++
		r.journalFingerprint = r.event.EventFingerprint
	}
	return nil
}

func (r *usageRecoveryEventRepositoryStub) ClaimPending(_ context.Context, token string, limit int) ([]*UsageSettlementEvent, error) {
	r.claims++
	if r.event == nil || r.blocked || r.event.State != UsageSettlementPending {
		return nil, nil
	}
	r.event.LeaseToken = token
	r.blocked = true
	return []*UsageSettlementEvent{r.event}, nil
}
func (r *usageRecoveryEventRepositoryStub) Complete(_ context.Context, id, token string, proof UsageSettlementResult) error {
	r.completes++
	if r.completeErr != nil {
		return r.completeErr
	}
	if r.event.SchemaVersion == 2 && r.journalFingerprint != r.event.EventFingerprint {
		return ErrUsageSettlementUnconfirmed
	}
	if token != r.event.LeaseToken || proof.RequestID != r.event.RequestID || proof.APIKeyID != r.event.APIKeyID || proof.BillingFingerprint != r.event.Command.RequestFingerprint || proof.UsageLogID <= 0 {
		return ErrUsageSettlementLease
	}
	r.event.State = UsageSettlementSettled
	r.event.UsageLogID = proof.UsageLogID
	return nil
}
func (r *usageRecoveryEventRepositoryStub) Release(_ context.Context, id, token string, conflict bool) error {
	r.releases++
	r.conflict = conflict
	if conflict {
		r.event.State = UsageSettlementConflict
	}
	return nil
}

type usageRecoveryBillingStub struct {
	UsageBillingRepository
	mu           sync.Mutex
	dedup        map[string]string
	cost         float64
	calls        int
	err          error
	panicOnApply bool
}

func (r *usageRecoveryBillingStub) Apply(_ context.Context, cmd *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.panicOnApply {
		panic("PRIVATE_BILLING_DETAIL")
	}
	if r.err != nil {
		return nil, r.err
	}
	if fp, ok := r.dedup[cmd.RequestID]; ok {
		if fp != cmd.RequestFingerprint {
			return nil, ErrUsageBillingRequestConflict
		}
		return &UsageBillingApplyResult{Applied: false}, nil
	}
	if r.dedup == nil {
		r.dedup = map[string]string{}
	}
	r.dedup[cmd.RequestID] = cmd.RequestFingerprint
	r.cost += cmd.BalanceCost
	return &UsageBillingApplyResult{Applied: true}, nil
}

type usageRecoveryLogStub struct {
	UsageLogRepository
	row       *UsageLog
	createErr error
	calls     int
}

func (r *usageRecoveryLogStub) Create(_ context.Context, log *UsageLog) (bool, error) {
	r.calls++
	if r.createErr != nil {
		return false, r.createErr
	}
	if r.row != nil {
		log.ID = r.row.ID
		return false, nil
	}
	copy := *log
	copy.ID = 42
	log.ID = 42
	r.row = &copy
	return true, nil
}
func (r *usageRecoveryLogStub) GetByID(_ context.Context, id int64) (*UsageLog, error) {
	return r.row, nil
}

type usageRecoveryCacheStub struct {
	calls int
	err   error
}

type usageRecoveryBlockingEvents struct {
	UsageSettlementEventRepository
	started chan struct{}
}

func (r *usageRecoveryBlockingEvents) ClaimPending(ctx context.Context, _ string, _ int) ([]*UsageSettlementEvent, error) {
	r.started <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestUsageSettlementRecoveryWorkerLifecycle(t *testing.T) {
	events := &usageRecoveryBlockingEvents{started: make(chan struct{}, 2)}
	svc := NewUsageSettlementReconciler(events, &usageRecoveryBillingStub{}, &usageRecoveryLogStub{}, &usageRecoveryCacheStub{})
	svc.Start()
	svc.Start()
	select {
	case <-events.started:
	case <-time.After(time.Second):
		t.Fatal("recovery worker did not start")
	}
	done := make(chan struct{})
	go func() { svc.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery worker did not stop its pending claim")
	}
	svc.Stop()
	svc.Start()
	require.Empty(t, events.started, "start is idempotent and stop prevents restart")
}

func (c *usageRecoveryCacheStub) InvalidateUsageSettlement(context.Context, *UsageSettlementEvent) error {
	c.calls++
	return c.err
}

func usageRecoveryFixture(t *testing.T) (*UsageSettlementReconciler, *usageRecoveryEventRepositoryStub, *usageRecoveryBillingStub, *usageRecoveryLogStub, *usageRecoveryCacheStub) {
	t.Helper()
	groupID := int64(4)
	usage := &UsageLog{UserID: 1, APIKeyID: 2, AccountID: 3, RequestID: "client:invocation-recover", Model: "test-model", RequestedModel: "test-model", GroupID: &groupID,
		InputTokens: 100, OutputTokens: 20, InputCost: 0.75, OutputCost: 0.5, TotalCost: 1.25, ActualCost: 1.25, RateMultiplier: 1, RequestType: RequestTypeSync}
	cmd := &UsageBillingCommand{RequestID: usage.RequestID, APIKeyID: 2, UserID: 1, AccountID: 3, AccountType: AccountTypeAPIKey, Model: usage.Model, InputTokens: 100, OutputTokens: 20, BalanceCost: 1.25}
	cmd.Normalize()
	event, err := FreezeUsageSettlementEvent("invocation-recover", cmd, usage)
	require.NoError(t, err)
	event.CreatedAt = time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC)
	events, billing, logs, caches := &usageRecoveryEventRepositoryStub{event: event}, &usageRecoveryBillingStub{}, &usageRecoveryLogStub{}, &usageRecoveryCacheStub{}
	return NewUsageSettlementReconciler(events, billing, logs, caches), events, billing, logs, caches
}

func usageRecoveryPlatformEvent(t *testing.T, events *usageRecoveryEventRepositoryStub) {
	t.Helper()
	old := events.event
	event, err := FreezeUsageSettlementEventWithPlatform(old.InvocationID, old.Command, old.Usage, PlatformOpenAI)
	require.NoError(t, err)
	event.CreatedAt = old.CreatedAt
	events.event = event
}

func TestUsageSettlementRecoveryRequiresPlatformJournalBeforeUsageAndCompletion(t *testing.T) {
	svc, events, billing, logs, _ := usageRecoveryFixture(t)
	usageRecoveryPlatformEvent(t, events)
	events.journalErr = errors.New("platform journal storage unavailable")
	result, err := svc.Reconcile(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Deferred)
	require.Equal(t, 1.25, billing.cost)
	require.Zero(t, logs.calls)
	require.Zero(t, events.completes)
	require.Zero(t, events.journalEntries)
	events.journalErr = nil
	events.blocked = false
	restarted := NewUsageSettlementReconciler(events, billing, logs, &usageRecoveryCacheStub{})
	result, err = restarted.Reconcile(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Settled)
	require.Equal(t, 1.25, billing.cost)
	require.Equal(t, 1, events.journalEntries)
	require.Equal(t, 2, events.journalCalls)
	require.Equal(t, events.event.EventFingerprint, events.journalFingerprint)
}

func TestUsageSettlementRecoveryPlatformJournalSurvivesLaterPersistenceFailures(t *testing.T) {
	for _, gap := range []string{"usage", "completion"} {
		t.Run(gap, func(t *testing.T) {
			svc, events, billing, logs, _ := usageRecoveryFixture(t)
			usageRecoveryPlatformEvent(t, events)
			if gap == "usage" {
				logs.createErr = errors.New("usage write failed")
			} else {
				events.completeErr = errors.New("completion write failed")
			}
			result, err := svc.Reconcile(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, 1, result.Deferred)
			require.Equal(t, 1, events.journalEntries)
			logs.createErr, events.completeErr, events.blocked = nil, nil, false
			result, err = svc.Reconcile(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, 1, result.Settled)
			require.Equal(t, 1, events.journalEntries, "one immutable journal record across recovery")
			require.Equal(t, 1.25, billing.cost)
		})
	}
}

func TestUsageSettlementRecoveryPlatformJournalConflictStopsRetries(t *testing.T) {
	svc, events, _, logs, _ := usageRecoveryFixture(t)
	usageRecoveryPlatformEvent(t, events)
	events.journalErr = ErrUsageSettlementEventConflict
	result, err := svc.Reconcile(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Conflicts)
	require.True(t, events.conflict)
	require.Zero(t, logs.calls)
	require.Zero(t, events.completes)
}

func TestUsageSettlementRecoveryVersionOneNeverInventsPlatformJournal(t *testing.T) {
	svc, events, _, _, _ := usageRecoveryFixture(t)
	result, err := svc.Reconcile(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Settled)
	require.Zero(t, events.journalCalls)
	require.Zero(t, events.journalEntries)
}

func TestUsageSettlementRecoveryRetriesOnlyFrozenAccountingAfterPartialCommit(t *testing.T) {
	svc, events, billing, logs, caches := usageRecoveryFixture(t)
	logs.createErr = errors.New("temporary usage failure")
	result, err := svc.Reconcile(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, 1, result.Deferred)
	require.Equal(t, 1.25, billing.cost)
	require.Nil(t, logs.row)
	require.Zero(t, events.completes)
	require.Equal(t, 1, events.releases)
	// A new service instance resumes only the immutable event. No pricing or
	// inference dependency exists in this worker's constructor.
	logs.createErr = nil
	events.blocked = false
	restarted := NewUsageSettlementReconciler(events, billing, logs, caches)
	result, err = restarted.Reconcile(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, 1, result.Settled)
	require.Equal(t, 1.25, billing.cost, "the committed event is never charged twice")
	require.Equal(t, 2, billing.calls)
	require.Equal(t, events.event.CreatedAt, logs.row.CreatedAt, "replay preserves original capture time")
	require.Equal(t, UsageSettlementSettled, events.event.State)
	require.Equal(t, 1, caches.calls)
}

func TestUsageSettlementRecoveryReconcilesLostCompletionWithoutDuplicatingUsage(t *testing.T) {
	svc, events, billing, logs, _ := usageRecoveryFixture(t)
	events.completeErr = errors.New("commit response lost")
	result, err := svc.Reconcile(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Deferred)
	require.NotNil(t, logs.row)
	events.completeErr = nil
	events.blocked = false
	result, err = svc.Reconcile(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, result.Settled)
	require.Equal(t, int64(42), events.event.UsageLogID)
	require.Equal(t, 1.25, billing.cost)
	require.Equal(t, 2, logs.calls)
	result, err = svc.Reconcile(context.Background(), 1)
	require.NoError(t, err)
	require.Zero(t, result.Claimed)
}

func TestUsageSettlementRecoveryQuarantinesBillingAndUsageConflicts(t *testing.T) {
	for _, mode := range []string{"billing", "usage", "payload"} {
		t.Run(mode, func(t *testing.T) {
			svc, events, billing, logs, _ := usageRecoveryFixture(t)
			switch mode {
			case "billing":
				billing.dedup = map[string]string{events.event.RequestID: "different-fingerprint"}
			case "usage":
				row := *events.event.Usage
				row.ID = 99
				row.ActualCost = 0
				logs.row = &row
			case "payload":
				events.event.Command.BalanceCost = 99
			}
			result, err := svc.Reconcile(context.Background(), 2)
			require.NoError(t, err)
			require.Equal(t, 1, result.Conflicts)
			require.True(t, events.conflict)
			require.Equal(t, UsageSettlementConflict, events.event.State)
			if mode == "payload" {
				require.Zero(t, billing.calls)
			}
			_, err = svc.Reconcile(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, 1, events.releases)
		})
	}
}

func TestUsageSettlementRecoveryKeepsCacheErrorsAndPanicsPending(t *testing.T) {
	for _, mode := range []string{"cache", "panic"} {
		t.Run(mode, func(t *testing.T) {
			svc, events, billing, _, caches := usageRecoveryFixture(t)
			if mode == "cache" {
				caches.err = errors.New("cache unavailable")
			} else {
				billing.panicOnApply = true
			}
			result, err := svc.Reconcile(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, 1, result.Deferred)
			require.False(t, events.conflict)
			require.Zero(t, events.completes)
		})
	}
}

func TestUsageSettlementRecoveryRejectsMissingDependenciesAndInvalidBatch(t *testing.T) {
	svc, events, _, _, _ := usageRecoveryFixture(t)
	_, err := svc.Reconcile(context.Background(), 101)
	require.Error(t, err)
	require.Zero(t, events.claims)
	svc.caches = nil
	_, err = svc.Reconcile(context.Background(), 1)
	require.Error(t, err)
	require.Zero(t, events.claims)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc, events, _, _, _ = usageRecoveryFixture(t)
	_, err = svc.Reconcile(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, events.claims)
	NewUsageSettlementReconciler(nil, nil, nil, nil).Stop()
}
