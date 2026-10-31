//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

type requiredUsageBillingRepoStub struct {
	UsageBillingRepository
	result *UsageBillingApplyResult
	err    error
	calls  int
	cmd    *UsageBillingCommand
	before func(context.Context)
}

func (r *requiredUsageBillingRepoStub) Apply(ctx context.Context, cmd *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	r.calls++
	r.cmd = cmd
	if r.before != nil {
		r.before(ctx)
	}
	return r.result, r.err
}

type requiredUsageLogRepoStub struct {
	UsageLogRepository
	inserted      bool
	createErr     error
	getErr        error
	noID          bool
	missing       bool
	createCalls   int
	getCalls      int
	bestEffort    int
	queued        *UsageLog
	persisted     *UsageLog
	mutate        func(*UsageLog)
	beforeCreate  func(context.Context)
	beforeGetByID func(context.Context)
}

func (r *requiredUsageLogRepoStub) Create(ctx context.Context, log *UsageLog) (bool, error) {
	r.createCalls++
	if r.beforeCreate != nil {
		r.beforeCreate(ctx)
	}
	if r.createErr != nil {
		return false, r.createErr
	}
	if !r.noID {
		log.ID = 42
	}
	stored := *log
	if r.mutate != nil {
		r.mutate(&stored)
	}
	r.persisted = &stored
	return r.inserted, nil
}

func (r *requiredUsageLogRepoStub) GetByID(ctx context.Context, id int64) (*UsageLog, error) {
	r.getCalls++
	if r.beforeGetByID != nil {
		r.beforeGetByID(ctx)
	}
	if r.missing {
		return nil, r.getErr
	}
	return r.persisted, r.getErr
}

func (r *requiredUsageLogRepoStub) CreateBestEffort(_ context.Context, log *UsageLog) error {
	r.bestEffort++
	r.queued = log
	return nil // A queued write deliberately does not create a persisted row.
}

type requiredUsageFixture struct {
	call     func(context.Context) error
	cfg      *config.Config
	key      *APIKey
	user     *User
	legacy   *openAIRecordUsageUserRepoStub
	model    *string
	platform *string
}

func newRequiredUsageFixture(kind string, logs UsageLogRepository, billing UsageBillingRepository) requiredUsageFixture {
	key := &APIKey{ID: 2, Quota: 100}
	user := &User{ID: 1}
	account := &Account{ID: 3, Type: AccountTypeAPIKey}
	legacy := &openAIRecordUsageUserRepoStub{}
	platform := PlatformOpenAI
	if kind == "gateway" {
		platform = PlatformAnthropic
		svc := newGatewayRecordUsageServiceWithBillingRepoForTest(logs, billing, legacy, &openAIRecordUsageSubRepoStub{})
		result := &ForwardResult{
			RequestID: "web_search:unrelated-upstream-id", Model: "claude-sonnet-4", Duration: time.Second,
			Usage: ClaudeUsage{InputTokens: 1200, OutputTokens: 300},
		}
		return requiredUsageFixture{
			cfg: svc.cfg, key: key, user: user, legacy: legacy, model: &result.Model, platform: &platform,
			call: func(ctx context.Context) error {
				return svc.RecordUsage(ctx, &RecordUsageInput{Result: result, APIKey: key, User: user, Account: account, QuotaPlatform: platform})
			},
		}
	}
	svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(logs, billing, legacy, &openAIRecordUsageSubRepoStub{}, nil)
	result := &OpenAIForwardResult{
		RequestID: "web_search:unrelated-upstream-id", Model: "gpt-5.1", Duration: time.Second,
		Usage: OpenAIUsage{InputTokens: 1200, OutputTokens: 300},
	}
	return requiredUsageFixture{
		cfg: svc.cfg, key: key, user: user, legacy: legacy, model: &result.Model, platform: &platform,
		call: func(ctx context.Context) error {
			return svc.RecordUsage(ctx, &OpenAIRecordUsageInput{Result: result, APIKey: key, User: user, Account: account, QuotaPlatform: platform})
		},
	}
}

func TestRequiredUsageSettlementAcknowledgesCommittedBillingAndMatchingPersistedLog(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, applied := range []bool{true, false} {
			name := kind + "/applied"
			if !applied {
				name = kind + "/deduplicated"
			}
			t.Run(name, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{inserted: applied}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: applied}}
				fixture := newRequiredUsageFixture(kind, logs, billing)
				base := context.WithValue(context.Background(), ctxkey.ClientRequestID, "unrelated-client-id")
				ctx, ack := WithRequiredUsageSettlement(base, "client:invocation-1", fixture.key.ID)
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				billing.before = func(ctx context.Context) {
					require.True(t, RequiredUsageSettlement(ctx))
					_, err := ack.Result()
					require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				}
				logs.beforeCreate = func(ctx context.Context) {
					require.Equal(t, 1, billing.calls)
					require.True(t, RequiredUsageSettlement(ctx))
					_, err := ack.Result()
					require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				}
				logs.beforeGetByID = func(context.Context) {
					_, err := ack.Result()
					require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				}
				require.NoError(t, fixture.call(ctx))
				result, err := ack.Result()
				require.NoError(t, err)
				require.Equal(t, "client:invocation-1", result.RequestID)
				require.Equal(t, fixture.key.ID, result.APIKeyID)
				require.Equal(t, int64(42), result.UsageLogID)
				require.Equal(t, billing.cmd.RequestFingerprint, result.BillingFingerprint)
				require.NotEmpty(t, result.BillingFingerprint)
				require.Equal(t, result.RequestID, logs.persisted.RequestID, "required identity wins over forced upstream and unrelated client IDs")
				require.Equal(t, 1, logs.createCalls)
				require.Equal(t, 1, logs.getCalls)
				require.Zero(t, logs.bestEffort)
				require.Zero(t, fixture.legacy.deductCalls)
			})
		}
	}
}

func TestRequiredUsageSettlementRejectsIncompleteOrFailedPersistence(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, failure := range []string{"no-billing-repository", "nil-billing-result", "billing-error", "no-usage-repository", "create-error", "no-persisted-id", "lookup-error", "lookup-missing"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{inserted: true}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
				var logRepo UsageLogRepository = logs
				var billingRepo UsageBillingRepository = billing
				switch failure {
				case "no-billing-repository":
					billingRepo = nil
				case "nil-billing-result":
					billing.result = nil
				case "billing-error":
					billing.err = errors.New("database billing error")
				case "no-usage-repository":
					logRepo = nil
				case "create-error":
					logs.createErr = errors.New("database create error")
				case "no-persisted-id":
					logs.noID = true
				case "lookup-error":
					logs.getErr = errors.New("database lookup error")
				case "lookup-missing":
					logs.missing = true
				}
				fixture := newRequiredUsageFixture(kind, logRepo, billingRepo)
				ctx, ack := WithRequiredUsageSettlement(context.Background(), "client:invocation-1", fixture.key.ID)
				require.Error(t, fixture.call(ctx))
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				require.Zero(t, logs.bestEffort)
				require.Zero(t, fixture.legacy.deductCalls, "required path must never fall back to legacy deduction")
				if failure == "billing-error" || failure == "nil-billing-result" || failure == "no-billing-repository" {
					require.Zero(t, logs.createCalls, "failed billing must not persist a zero-cost row that poisons a later retry")
				}
			})
		}
	}
}

func TestRequiredUsageSettlementRejectsConflictingPersistedDedupRow(t *testing.T) {
	mutations := map[string]func(*UsageLog){
		"zero-actual-cost": func(log *UsageLog) { log.ActualCost = 0 },
		"user":             func(log *UsageLog) { log.UserID++ },
		"api-key":          func(log *UsageLog) { log.APIKeyID++ },
		"account":          func(log *UsageLog) { log.AccountID++ },
		"request":          func(log *UsageLog) { log.RequestID = "client:another" },
		"model":            func(log *UsageLog) { log.Model = "another" },
		"requested-model":  func(log *UsageLog) { log.RequestedModel = "another" },
		"input-tokens":     func(log *UsageLog) { log.InputTokens++ },
		"output-tokens":    func(log *UsageLog) { log.OutputTokens++ },
		"cache-tokens":     func(log *UsageLog) { log.CacheReadTokens++ },
		"total-cost":       func(log *UsageLog) { log.TotalCost++ },
		"rate-multiplier":  func(log *UsageLog) { log.RateMultiplier++ },
		"group":            func(log *UsageLog) { log.GroupID = i64p(9) },
		"subscription":     func(log *UsageLog) { log.SubscriptionID = i64p(9) },
		"billing-type":     func(log *UsageLog) { log.BillingType = BillingTypeSubscription },
		"consumption-date": func(log *UsageLog) { log.CreatedAt = log.CreatedAt.Add(24 * time.Hour) },
	}
	for _, kind := range []string{"gateway", "openai"} {
		for name, mutate := range mutations {
			t.Run(kind+"/"+name, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{inserted: false, mutate: mutate}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: false}}
				fixture := newRequiredUsageFixture(kind, logs, billing)
				ctx, ack := WithRequiredUsageSettlement(context.Background(), "client:invocation-1", fixture.key.ID)
				recordErr := fixture.call(ctx)
				require.ErrorIs(t, recordErr, ErrUsageSettlementUnconfirmed)
				require.ErrorIs(t, recordErr, ErrUsageSettlementEventConflict)
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				require.Equal(t, 1, logs.getCalls, "dedup ID alone cannot acknowledge a conflicting row")
			})
		}
	}
}

func TestRequiredUsageSettlementRejectsWrongBillingIdentityBeforeApply(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		t.Run(kind, func(t *testing.T) {
			logs := &requiredUsageLogRepoStub{}
			billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
			fixture := newRequiredUsageFixture(kind, logs, billing)
			ctx, ack := WithRequiredUsageSettlement(context.Background(), "client:invocation-1", 99)
			require.ErrorIs(t, fixture.call(ctx), ErrUsageSettlementUnconfirmed)
			_, err := ack.Result()
			require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
			require.Zero(t, billing.calls)
			require.Zero(t, logs.createCalls)
		})
	}
	ctx, _ := WithRequiredUsageSettlement(context.Background(), "client:invocation-1", 2)
	billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
	_, err := applyUsageBilling(ctx, "wrong-request", &UsageLog{RequestID: "wrong-request", UserID: 1, APIKeyID: 2, AccountID: 3}, &postUsageBillingParams{
		Cost: &CostBreakdown{ActualCost: 1}, User: &User{ID: 1}, APIKey: &APIKey{ID: 2}, Account: &Account{ID: 3},
	}, &billingDeps{}, billing)
	require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
	require.Zero(t, billing.calls)
}

func TestRequiredUsageSettlementRejectsMissingPricingAndSimpleMode(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, mode := range []string{"missing-pricing", "simple-mode"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
				fixture := newRequiredUsageFixture(kind, logs, billing)
				if mode == "simple-mode" {
					fixture.cfg.RunMode = config.RunModeSimple
				} else {
					*fixture.model = "unconfigured-model-for-required-settlement"
				}
				ctx, ack := WithRequiredUsageSettlement(context.Background(), "client:invocation-1", fixture.key.ID)
				require.Error(t, fixture.call(ctx))
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				require.Zero(t, billing.calls)
				require.Zero(t, logs.createCalls)
			})
		}
	}
}

func TestRequiredUsageSettlementKeepsFailureAndIncompleteObservationUnconfirmed(t *testing.T) {
	ctx, ack := WithRequiredUsageSettlement(context.Background(), "client:invocation-1", 2)
	require.True(t, RequiredUsageSettlement(context.WithoutCancel(ctx)))
	_, err := ack.Result()
	require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
	FailRequiredUsageSettlement(ctx, errors.New("task panic"))
	logs := &requiredUsageLogRepoStub{}
	billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
	fixture := newRequiredUsageFixture("openai", logs, billing)
	require.Error(t, fixture.call(ctx))
	_, err = ack.Result()
	require.ErrorContains(t, err, "task panic")
	require.Zero(t, billing.calls)

	_, err = (*UsageSettlementAcknowledgment)(nil).Result()
	require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
	for _, invalid := range []struct {
		requestID string
		keyID     int64
	}{{"", 2}, {"client:", 2}, {"upstream:1", 2}, {"client:1", 0}} {
		_, invalidAck := WithRequiredUsageSettlement(context.Background(), invalid.requestID, invalid.keyID)
		_, err := invalidAck.Result()
		require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
	}
}

func TestRequiredUsageSettlementPreservesMandatoryDeadline(t *testing.T) {
	base, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx, _ := WithRequiredUsageSettlement(base, "client:invocation-1", 2)
	detached, detachedCancel := detachedBillingContext(ctx)
	defer detachedCancel()
	expected, _ := base.Deadline()
	actual, ok := detached.Deadline()
	require.True(t, ok)
	require.Equal(t, expected, actual)
	require.True(t, RequiredUsageSettlement(detached))
	cancel()
	require.ErrorIs(t, detached.Err(), context.Canceled)
}

func TestRequiredUsageSettlementPanicNeverAcknowledges(t *testing.T) {
	logs := &requiredUsageLogRepoStub{}
	billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
	fixture := newRequiredUsageFixture("openai", logs, billing)
	ctx, ack := WithRequiredUsageSettlement(context.Background(), "client:invocation-1", fixture.key.ID)
	logs.beforeGetByID = func(context.Context) { panic("lookup panic") }
	require.Panics(t, func() { _ = fixture.call(ctx) })
	_, err := ack.Result()
	require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
}

func TestRequiredUsageSettlementOrdinaryUsageRetainsBestEffortBehavior(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		t.Run(kind, func(t *testing.T) {
			logs := &requiredUsageLogRepoStub{}
			billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
			fixture := newRequiredUsageFixture(kind, logs, billing)
			require.NoError(t, fixture.call(context.Background()))
			require.Equal(t, 1, logs.bestEffort)
			require.Zero(t, logs.createCalls)
			require.Zero(t, logs.getCalls)
		})
	}
}

func TestRequiredUsageSettlementOrdinaryPricingSimpleAndBillingFailureCompatibility(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, mode := range []string{"missing-pricing", "simple-mode", "nil-billing-result", "billing-error"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
				fixture := newRequiredUsageFixture(kind, logs, billing)
				switch mode {
				case "missing-pricing":
					*fixture.model = "unconfigured-model-for-required-settlement"
				case "simple-mode":
					fixture.cfg.RunMode = config.RunModeSimple
				case "nil-billing-result":
					billing.result = nil
				case "billing-error":
					billing.err = errors.New("ordinary billing failure")
				}
				err := fixture.call(context.Background())
				if mode == "billing-error" {
					require.ErrorIs(t, err, billing.err)
					require.Zero(t, logs.queued.ActualCost)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, 1, logs.bestEffort)
				require.Zero(t, logs.createCalls)
				require.Zero(t, logs.getCalls)
				if mode == "simple-mode" {
					require.Zero(t, billing.calls)
				}
			})
		}
	}
}

func TestRequiredUsageSettlementUsesUsageLogDatabasePrecisionAndKeepsReferencesPrivate(t *testing.T) {
	expected := &UsageLog{ActualCost: 0.00007812504, RateMultiplier: 1.12344}
	stored := &UsageLog{ActualCost: 0.0000781250, RateMultiplier: 1.1234}
	require.True(t, usageSettlementLogsMatch(expected, stored))
	stored.ActualCost = 0.0000781251
	require.False(t, usageSettlementLogsMatch(expected, stored), "a difference at the persisted numeric scale is not an epsilon match")
	stored.ActualCost = expected.ActualCost
	stored.RateMultiplier = 1.1235
	require.False(t, usageSettlementLogsMatch(expected, stored))
	payload, err := json.Marshal(UsageSettlementResult{RequestID: "private-request", APIKeyID: 2, UsageLogID: 42, BillingFingerprint: "private-fingerprint"})
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(payload))
}

func TestRequiredUsageSettlementMatchesOriginalConsumptionTimestampAtDatabasePrecision(t *testing.T) {
	base := time.Date(2026, 10, 25, 1, 2, 3, 123456000, time.UTC)
	for _, tc := range []struct {
		name             string
		expected, actual time.Time
		matches          bool
	}{
		{"same-instant-other-zone", base, base.In(time.FixedZone("UTC+8", 8*60*60)), true},
		{"fraction-below-half", base.Add(499 * time.Nanosecond), base, true},
		{"fraction-above-half", base.Add(501 * time.Nanosecond), base.Add(time.Microsecond), true},
		{"half-even-lower", base.Add(500 * time.Nanosecond), base, true},
		{"half-even-upper", base.Add(1500 * time.Nanosecond), base.Add(2 * time.Microsecond), true},
		{"adjacent-persisted-microsecond", base, base.Add(time.Microsecond), false},
		{"different-consumption-date", base, base.Add(24 * time.Hour), false},
		{"both-zero", time.Time{}, time.Time{}, true},
		{"missing-expected-time", time.Time{}, base, false},
		{"missing-actual-time", base, time.Time{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expected := &UsageLog{CreatedAt: tc.expected, ActualCost: 1.25}
			actual := &UsageLog{CreatedAt: tc.actual, ActualCost: 1.25}
			require.Equal(t, tc.matches, UsageSettlementLogsMatch(expected, actual))
		})
	}
}
