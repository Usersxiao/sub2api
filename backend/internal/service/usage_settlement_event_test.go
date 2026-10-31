//go:build unit

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// Captured from the v1 closed wire layout before platform metering was added.
// Keep these bytes and their fingerprint fixed across later codec versions.
const usageSettlementV1Golden = `{"version":1,"invocation_id":"golden-v1","request_id":"client:golden-v1","api_key_id":2,"command":{"request_id":"client:golden-v1","api_key_id":2,"request_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","request_payload_hash":"","user_id":1,"account_id":3,"subscription_id":null,"account_type":"apikey","model":"golden-model","service_tier":"","reasoning_effort":"","billing_type":0,"input_tokens":10,"output_tokens":5,"cache_creation_tokens":0,"cache_read_tokens":0,"image_count":0,"media_type":"","balance_cost":1.25,"subscription_cost":0,"api_key_quota_cost":0,"api_key_rate_limit_cost":0,"account_quota_cost":0,"account_quota_has_daily_limit":false,"account_quota_has_weekly_limit":false},"usage":{"user_id":1,"api_key_id":2,"account_id":3,"request_id":"client:golden-v1","model":"golden-model","requested_model":"golden-model","upstream_model":null,"upstream_response_model":null,"upstream_model_mismatch":null,"channel_id":null,"model_mapping_chain":null,"billing_tier":null,"billing_mode":null,"service_tier":null,"reasoning_effort":null,"requested_reasoning_effort":null,"inbound_endpoint":null,"upstream_endpoint":null,"group_id":null,"subscription_id":null,"input_tokens":10,"output_tokens":5,"cache_creation_tokens":0,"cache_read_tokens":0,"cache_creation_5m_tokens":0,"cache_creation_1h_tokens":0,"image_input_tokens":0,"image_output_tokens":0,"input_cost":0,"output_cost":0,"cache_creation_cost":0,"cache_read_cost":0,"image_input_cost":0,"image_output_cost":0,"total_cost":1.25,"actual_cost":1.25,"rate_multiplier":1,"account_rate_multiplier":null,"account_stats_cost":null,"long_context_billing_applied":false,"billing_type":0,"request_type":1,"stream":false,"openai_ws_mode":false,"native_compaction_v2":false,"duration_ms":null,"first_token_ms":null,"cache_ttl_overridden":false,"image_count":0,"image_size":null,"image_input_size":null,"image_output_size":null,"image_size_source":null,"image_size_breakdown":null,"media_type":null,"video_count":0,"video_resolution":null,"video_duration_seconds":null}}`

func TestUsageSettlementEventV1GoldenBytesAndFingerprintRemainUnchanged(t *testing.T) {
	const fingerprint = "2fa6320287795809676c1919b38a92b18d5751ccd94b802ebd793fad888ead2f"
	event, err := DecodeUsageSettlementEvent([]byte(usageSettlementV1Golden))
	require.NoError(t, err)
	require.Equal(t, 1, event.SchemaVersion)
	require.Nil(t, event.PlatformQuota)
	require.Equal(t, fingerprint, event.EventFingerprint)
	encoded, err := EncodeUsageSettlementEvent(event)
	require.NoError(t, err)
	require.Equal(t, usageSettlementV1Golden, string(encoded))
	legacyFrozen, err := FreezeUsageSettlementEvent(event.InvocationID, event.Command, event.Usage)
	require.NoError(t, err)
	require.Equal(t, 1, legacyFrozen.SchemaVersion)
	require.Equal(t, fingerprint, legacyFrozen.EventFingerprint)
}

func TestUsageSettlementEventV2FreezesPlatformCostAtTenDecimalPlaces(t *testing.T) {
	legacy := settlementEventFixture(t)
	event, err := FreezeUsageSettlementEventWithPlatform(legacy.InvocationID, legacy.Command, legacy.Usage, PlatformOpenAI)
	require.NoError(t, err)
	require.Equal(t, 2, event.SchemaVersion)
	require.Equal(t, PlatformOpenAI, event.PlatformQuota.Platform)
	require.Equal(t, 0.000078125, event.PlatformQuota.CostUSD)
	require.Equal(t, 0.00007813, event.Command.BalanceCost)
	require.NotEqual(t, event.Command.BalanceCost, event.PlatformQuota.CostUSD, "journal retains numeric(20,10) rather than the billing ledger's numeric(20,8)")
	require.NotEqual(t, legacy.EventFingerprint, event.EventFingerprint)
	encoded, err := EncodeUsageSettlementEvent(event)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"version":2`)
	require.Contains(t, string(encoded), `"platform_quota":{"platform":"openai","cost_usd":0.000078125}`)
	decoded, err := DecodeUsageSettlementEvent(encoded)
	require.NoError(t, err)
	require.Equal(t, event.PlatformQuota, decoded.PlatformQuota)
	require.Equal(t, event.EventFingerprint, decoded.EventFingerprint)
	public, err := json.Marshal(event.PlatformQuota)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(public))
}

func TestUsageSettlementEventV2RejectsInvalidPlatformCostAndVersionCombination(t *testing.T) {
	for _, platform := range []string{"", PlatformComposite, "unknown-provider", " openai", "OPENAI"} {
		t.Run("platform-"+platform, func(t *testing.T) {
			legacy := settlementEventFixture(t)
			_, err := FreezeUsageSettlementEventWithPlatform(legacy.InvocationID, legacy.Command, legacy.Usage, platform)
			require.ErrorIs(t, err, ErrUsageSettlementPayload)
		})
	}
	for _, mode := range []string{"missing-v2-intent", "v1-with-intent", "different-cost", "billing-scale-cost", "nan", "overflow", "unknown-platform", "schema-zero"} {
		t.Run(mode, func(t *testing.T) {
			legacy := settlementEventFixture(t)
			event, err := FreezeUsageSettlementEventWithPlatform(legacy.InvocationID, legacy.Command, legacy.Usage, PlatformOpenAI)
			require.NoError(t, err)
			event.EventFingerprint = ""
			switch mode {
			case "missing-v2-intent":
				event.PlatformQuota = nil
			case "v1-with-intent":
				event.SchemaVersion = 1
			case "different-cost":
				event.PlatformQuota.CostUSD++
			case "billing-scale-cost":
				event.PlatformQuota.CostUSD = event.Command.BalanceCost
			case "nan":
				event.PlatformQuota.CostUSD = math.NaN()
			case "overflow":
				event.PlatformQuota.CostUSD = 1e10
			case "unknown-platform":
				event.PlatformQuota.Platform = "unknown-provider"
			case "schema-zero":
				event.SchemaVersion = 0
			}
			_, err = EncodeUsageSettlementEvent(event)
			require.ErrorIs(t, err, ErrUsageSettlementPayload)
		})
	}
}

func settlementEventFixture(t *testing.T) *UsageSettlementEvent {
	t.Helper()
	rate := 1.25
	usage := &UsageLog{
		ID: 999, UserID: 1, APIKeyID: 2, AccountID: 3, RequestID: "client:invocation-1", Model: "gpt-5.1", RequestedModel: "gpt-5.1",
		InputTokens: 10, OutputTokens: 5, InputCost: 0.0000125, OutputCost: 0.00005,
		TotalCost: 0.0000625, ActualCost: 0.000078125, RateMultiplier: rate, AccountRateMultiplier: &rate,
		User: &User{ID: 1, Email: "secret-user@example.test"}, APIKey: &APIKey{ID: 2, Key: "secret-api-key"},
		Account: &Account{ID: 3, Credentials: map[string]any{"api_key": "secret-upstream-key"}}, Group: &Group{ID: 4},
		Subscription: &UserSubscription{ID: 5}, UserAgent: stringPointer("secret-user-agent"), IPAddress: stringPointer("192.0.2.33"),
		SessionID: stringPointer("secret-session-id"), UpstreamRequestID: stringPointer("secret-upstream-request-id"),
		CreatedAt: time.Now(),
	}
	command := buildUsageBillingCommand(usage.RequestID, usage, &postUsageBillingParams{
		Cost: &CostBreakdown{TotalCost: usage.TotalCost, ActualCost: usage.ActualCost},
		User: &User{ID: 1}, APIKey: &APIKey{ID: 2}, Account: &Account{ID: 3, Type: AccountTypeAPIKey}, AccountRateMultiplier: rate,
	})
	event, err := FreezeUsageSettlementEvent("invocation-1", command, usage)
	require.NoError(t, err)
	return event
}

func stringPointer(value string) *string { return &value }

func TestUsageSettlementEventCodecFreezesOnlyClosedAccountingValues(t *testing.T) {
	event := settlementEventFixture(t)
	event.State, event.UsageLogID, event.LeaseToken = UsageSettlementSettled, 42, "private-lease-token"
	event.CreatedAt, event.UpdatedAt = time.Now(), time.Now()
	expires := time.Now().Add(time.Minute)
	event.LeaseExpiresAt = &expires
	encoded, err := EncodeUsageSettlementEvent(event)
	require.NoError(t, err)
	for _, private := range []string{"secret-user", "secret-api-key", "secret-upstream-key", "192.0.2.33", "secret-session-id", "private-lease-token", "owner_token", "lease_token", "created_at", "usage_log_id"} {
		require.NotContains(t, string(encoded), private)
	}
	decoded, err := DecodeUsageSettlementEvent(encoded)
	require.NoError(t, err)
	require.Equal(t, event.EventFingerprint, decoded.EventFingerprint)
	require.Equal(t, UsageSettlementPending, decoded.State)
	require.Empty(t, decoded.LeaseToken)
	require.Zero(t, decoded.UsageLogID)
	require.True(t, decoded.CreatedAt.IsZero())
	require.True(t, decoded.Usage.CreatedAt.IsZero())
	require.Zero(t, decoded.Usage.ID)
	require.Nil(t, decoded.Usage.User)
	require.Nil(t, decoded.Usage.APIKey)
	require.Nil(t, decoded.Usage.Account)
	require.Nil(t, decoded.Usage.Group)
	require.Nil(t, decoded.Usage.Subscription)
	require.Nil(t, decoded.Usage.IPAddress)
	require.Nil(t, decoded.Usage.UserAgent)
	require.Nil(t, decoded.Usage.SessionID)
	require.Nil(t, decoded.Usage.UpstreamRequestID)
	require.Equal(t, event.Command.RequestFingerprint, decoded.Command.RequestFingerprint)
	require.Equal(t, event.Command.BalanceCost, decoded.Command.BalanceCost)
	require.Equal(t, event.Usage.ActualCost, decoded.Usage.ActualCost, "usage keeps the raw pricing snapshot while the command retains normalized deductions")
	public, err := json.Marshal(event)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(public))
}

func TestUsageSettlementEventFreezeDetachesPointersAndIgnoresDynamicMetadata(t *testing.T) {
	event := settlementEventFixture(t)
	groupID := int64(4)
	event.Usage.GroupID = &groupID
	event.Usage.UpstreamModel = stringPointer("gpt-5.1")
	event.Usage.ImageSizeBreakdown = map[string]int{"1024x1024": 2}
	event.EventFingerprint = ""
	frozen, err := FreezeUsageSettlementEvent(event.InvocationID, event.Command, event.Usage)
	require.NoError(t, err)
	groupID = 44
	*event.Usage.UpstreamModel = "mutated"
	event.Usage.ImageSizeBreakdown["1024x1024"] = 99
	require.Equal(t, int64(4), *frozen.Usage.GroupID)
	require.Equal(t, "gpt-5.1", *frozen.Usage.UpstreamModel)
	require.Equal(t, 2, frozen.Usage.ImageSizeBreakdown["1024x1024"])
	first, err := EncodeUsageSettlementEvent(frozen)
	require.NoError(t, err)
	frozen.Usage.CreatedAt = time.Now().Add(time.Hour)
	frozen.Usage.IPAddress = stringPointer("203.0.113.55")
	second, err := EncodeUsageSettlementEvent(frozen)
	require.NoError(t, err)
	require.Equal(t, first, second, "dynamic log timestamps and private request metadata must not change immutable accounting identity")
}

func TestUsageSettlementEventCodecRejectsInvalidOrChangedPayload(t *testing.T) {
	for _, change := range []string{"wrong-invocation", "wrong-key", "wrong-user", "wrong-model", "wrong-tokens", "wrong-charge", "negative-tokens", "nan-cost", "unquantized-command", "bad-fingerprint", "invalid-subscription", "invalid-request-type", "oversized-model", "endpoint-query", "control-character", "oversized-tier"} {
		t.Run(change, func(t *testing.T) {
			event := settlementEventFixture(t)
			event.EventFingerprint = ""
			switch change {
			case "wrong-invocation":
				event.InvocationID = "invocation-2"
			case "wrong-key":
				event.Usage.APIKeyID++
			case "wrong-user":
				event.Command.UserID++
			case "wrong-model":
				event.Usage.Model = "another-model"
			case "wrong-tokens":
				event.Command.OutputTokens++
			case "wrong-charge":
				event.Command.BalanceCost++
			case "negative-tokens":
				event.Command.InputTokens, event.Usage.InputTokens = -1, -1
			case "nan-cost":
				event.Usage.TotalCost = math.NaN()
			case "unquantized-command":
				event.Command.BalanceCost = 0.000078125
			case "bad-fingerprint":
				event.Command.RequestFingerprint = "not-a-fingerprint"
			case "invalid-subscription":
				event.Command.SubscriptionID = i64p(9)
			case "invalid-request-type":
				event.Usage.RequestType = RequestType(300)
			case "oversized-model":
				event.Command.Model, event.Usage.Model = strings.Repeat("x", 101), strings.Repeat("x", 101)
			case "endpoint-query":
				event.Usage.InboundEndpoint = stringPointer("/v1/responses?token=private")
			case "control-character":
				event.Command.Model, event.Usage.Model = "model\tname", "model\tname"
			case "oversized-tier":
				event.Command.ServiceTier = strings.Repeat("x", 17)
				event.Usage.ServiceTier = stringPointer(event.Command.ServiceTier)
			}
			_, err := EncodeUsageSettlementEvent(event)
			require.ErrorIs(t, err, ErrUsageSettlementPayload)
		})
	}
	event := settlementEventFixture(t)
	event.Usage.TotalCost++
	_, err := EncodeUsageSettlementEvent(event)
	require.ErrorIs(t, err, ErrUsageSettlementEventConflict)
}

func TestUsageSettlementEventCodecRejectsUntrustedJSONShapes(t *testing.T) {
	encoded, err := EncodeUsageSettlementEvent(settlementEventFixture(t))
	require.NoError(t, err)
	for _, malformed := range [][]byte{
		nil, []byte(`{}`), []byte(`null`),
		append(append([]byte{}, encoded...), []byte(`{}`)...),
		append([]byte(` `), encoded...),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"unexpected":"field"`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":2`), 1),
		bytes.Replace(encoded, []byte(`"input_tokens":10`), []byte(`"input_tokens":-10`), 1),
		bytes.Repeat([]byte("x"), UsageSettlementMaxPayloadBytes+1),
	} {
		_, err := DecodeUsageSettlementEvent(malformed)
		require.ErrorIs(t, err, ErrUsageSettlementPayload)
	}
}

type durableUsageEventRepoStub struct {
	stageCalls    int
	completeCalls int
	releaseCalls  int
	stageErr      error
	journalErr    error
	journalCalls  int
	completeErr   error
	mode          string
	staged        *UsageSettlementEvent
	completed     UsageSettlementResult
	releasedBad   bool
	beforeStage   func(context.Context, *UsageSettlementEvent)
	beforeApply   func(*UsageSettlementEvent)
	beforeDone    func(context.Context)
	beforeJournal func(context.Context)
}

func (r *durableUsageEventRepoStub) Stage(ctx context.Context, binding *UsageSettlementBinding, event *UsageSettlementEvent, lease string) (*UsageSettlementClaim, error) {
	r.stageCalls++
	if r.beforeStage != nil {
		r.beforeStage(ctx, event)
	}
	if r.stageErr != nil {
		return nil, r.stageErr
	}
	if r.mode == "nil-claim" {
		return nil, nil
	}
	encoded, err := EncodeUsageSettlementEvent(event)
	if err != nil {
		return nil, err
	}
	stored, err := DecodeUsageSettlementEvent(encoded)
	if err != nil {
		return nil, err
	}
	stored.CreatedAt = time.Date(2026, 10, 25, 1, 2, 3, 0, time.UTC)
	stored.UpdatedAt = stored.CreatedAt
	stored.LeaseToken = lease
	expires := time.Now().Add(UsageSettlementLease)
	stored.LeaseExpiresAt = &expires
	owned := true
	switch r.mode {
	case "not-owned":
		owned = false
	case "settled":
		stored.State, stored.UsageLogID, owned = UsageSettlementSettled, 42, false
	case "conflict":
		stored.State, owned = UsageSettlementConflict, false
	case "changed-fingerprint":
		stored.EventFingerprint = strings.Repeat("b", 64)
	case "missing-created-at":
		stored.CreatedAt = time.Time{}
	case "wrong-lease":
		stored.LeaseToken = "wrong"
	case "expired-lease":
		expires = time.Now().Add(-time.Second)
	}
	r.staged = stored
	if r.beforeApply != nil {
		r.beforeApply(stored)
	}
	return &UsageSettlementClaim{Event: stored, Owned: owned}, nil
}

func (*durableUsageEventRepoStub) ClaimPending(context.Context, string, int) ([]*UsageSettlementEvent, error) {
	panic("live usage must not claim recovery work")
}

func (r *durableUsageEventRepoStub) RecordPlatformQuota(ctx context.Context, invocationID, leaseToken string) error {
	r.journalCalls++
	if r.beforeJournal != nil {
		r.beforeJournal(ctx)
	}
	return r.journalErr
}

func (r *durableUsageEventRepoStub) Complete(ctx context.Context, invocationID, lease string, result UsageSettlementResult) error {
	r.completeCalls++
	r.completed = result
	if r.beforeDone != nil {
		r.beforeDone(ctx)
	}
	return r.completeErr
}

func (r *durableUsageEventRepoStub) Release(_ context.Context, invocationID, lease string, conflict bool) error {
	r.releaseCalls++
	r.releasedBad = conflict
	return nil
}

func durableUsageBinding() *UsageSettlementBinding {
	return &UsageSettlementBinding{InvocationID: "invocation-1", OwnerToken: "attempt-" + strings.Repeat("A", 43)}
}

func TestDurableUsageSettlementStagesBeforeBillingAndCompletesBeforeAcknowledgment(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		t.Run(kind, func(t *testing.T) {
			logs := &requiredUsageLogRepoStub{inserted: true}
			billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
			events := &durableUsageEventRepoStub{}
			fixture := newRequiredUsageFixture(kind, logs, billing)
			binding := durableUsageBinding()
			ctx, ack := WithDurableUsageSettlement(context.Background(), binding, fixture.key.ID, events)
			binding.OwnerToken, binding.InvocationID = "changed-owner", "changed-invocation"
			events.beforeStage = func(_ context.Context, event *UsageSettlementEvent) {
				require.Zero(t, billing.calls)
				require.Zero(t, logs.createCalls)
				require.Equal(t, "invocation-1", event.InvocationID)
				require.Equal(t, 2, event.SchemaVersion)
				require.Equal(t, *fixture.platform, event.PlatformQuota.Platform)
			}
			billing.before = func(context.Context) { require.Equal(t, 1, events.stageCalls) }
			events.beforeJournal = func(context.Context) {
				require.Equal(t, 1, billing.calls)
				require.Zero(t, logs.createCalls)
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
			}
			logs.beforeCreate = func(context.Context) { require.Equal(t, 1, events.journalCalls) }
			events.beforeDone = func(context.Context) {
				require.Equal(t, 1, billing.calls)
				require.Equal(t, 1, logs.createCalls)
				require.Equal(t, 1, logs.getCalls)
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
			}
			require.NoError(t, fixture.call(ctx))
			result, err := ack.Result()
			require.NoError(t, err)
			require.Equal(t, result, events.completed)
			require.Equal(t, 1, events.completeCalls)
			require.Equal(t, 1, events.journalCalls)
			require.Equal(t, events.staged.CreatedAt, logs.persisted.CreatedAt)
			require.Zero(t, events.releaseCalls)
		})
	}
}

func TestDurableUsageSettlementRejectsUnpersistedOrUnownedStageBeforeBilling(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, mode := range []string{"stage-error", "nil-claim", "not-owned", "conflict", "changed-fingerprint", "missing-created-at", "wrong-lease", "expired-lease"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
				events := &durableUsageEventRepoStub{mode: mode}
				if mode == "stage-error" {
					events.stageErr = errors.New("stage database failure")
				}
				fixture := newRequiredUsageFixture(kind, logs, billing)
				ctx, ack := WithDurableUsageSettlement(context.Background(), durableUsageBinding(), fixture.key.ID, events)
				require.Error(t, fixture.call(ctx))
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				require.Zero(t, billing.calls)
				require.Zero(t, events.journalCalls)
				require.Zero(t, logs.createCalls)
				require.Zero(t, events.completeCalls)
			})
		}
	}
}

func TestDurableUsageSettlementFailuresRemainRecoverableAndNeverAcknowledged(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, failure := range []string{"billing", "journal", "usage", "persisted-conflict", "consumption-date-conflict", "complete", "panic"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{inserted: true}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
				events := &durableUsageEventRepoStub{}
				switch failure {
				case "billing":
					billing.err = errors.New("billing database failure")
				case "journal":
					events.journalErr = errors.New("platform journal database failure")
				case "usage":
					logs.createErr = errors.New("usage database failure")
				case "persisted-conflict":
					logs.mutate = func(log *UsageLog) { log.ActualCost = 0 }
				case "consumption-date-conflict":
					logs.mutate = func(log *UsageLog) { log.CreatedAt = log.CreatedAt.Add(24 * time.Hour) }
				case "complete":
					events.completeErr = errors.New("event completion failure")
				case "panic":
					logs.beforeGetByID = func(context.Context) { panic("usage lookup panic") }
				}
				fixture := newRequiredUsageFixture(kind, logs, billing)
				ctx, ack := WithDurableUsageSettlement(context.Background(), durableUsageBinding(), fixture.key.ID, events)
				if failure == "panic" {
					require.Panics(t, func() { _ = fixture.call(ctx) })
				} else {
					require.Error(t, fixture.call(ctx))
				}
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				require.Equal(t, 1, events.stageCalls)
				require.Equal(t, 1, events.releaseCalls)
				require.Equal(t, failure == "persisted-conflict" || failure == "consumption-date-conflict", events.releasedBad)
				if failure == "billing" || failure == "journal" {
					require.Zero(t, logs.createCalls)
				}
				if failure != "complete" {
					require.Zero(t, events.completeCalls)
				}
			})
		}
	}
}

func TestDurableUsageSettlementPreviouslySettledEventOnlyReadsPersistedUsage(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		t.Run(kind, func(t *testing.T) {
			logs := &requiredUsageLogRepoStub{}
			billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
			events := &durableUsageEventRepoStub{mode: "settled"}
			events.beforeApply = func(event *UsageSettlementEvent) {
				persisted := *event.Usage
				persisted.ID, persisted.CreatedAt = event.UsageLogID, event.CreatedAt
				logs.persisted = &persisted
			}
			fixture := newRequiredUsageFixture(kind, logs, billing)
			ctx, ack := WithDurableUsageSettlement(context.Background(), durableUsageBinding(), fixture.key.ID, events)
			require.NoError(t, fixture.call(ctx))
			_, err := ack.Result()
			require.NoError(t, err)
			require.Zero(t, billing.calls)
			require.Zero(t, events.journalCalls)
			require.Zero(t, logs.createCalls)
			require.Equal(t, 1, logs.getCalls)
			require.Zero(t, events.completeCalls)
			require.Zero(t, events.releaseCalls)
		})
	}
}

func TestDurableUsageSettlementRejectsMissingRepositoryOrOwnerCapability(t *testing.T) {
	for _, mode := range []string{"missing-repository", "missing-binding", "invalid-owner"} {
		t.Run(mode, func(t *testing.T) {
			var events UsageSettlementEventRepository = &durableUsageEventRepoStub{}
			binding := durableUsageBinding()
			switch mode {
			case "missing-repository":
				events = nil
			case "missing-binding":
				binding = nil
			case "invalid-owner":
				binding.OwnerToken = "not-an-owner-capability"
			}
			logs := &requiredUsageLogRepoStub{}
			billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
			fixture := newRequiredUsageFixture("openai", logs, billing)
			ctx, ack := WithDurableUsageSettlement(context.Background(), binding, fixture.key.ID, events)
			require.Error(t, fixture.call(ctx))
			_, err := ack.Result()
			require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
			require.Zero(t, billing.calls)
		})
	}
}

func TestDurableUsageSettlementRequiresTrustedConcretePlatformBeforeStage(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		for _, platform := range []string{"", PlatformComposite, "unknown-provider", "openai "} {
			t.Run(kind+"/"+platform, func(t *testing.T) {
				logs := &requiredUsageLogRepoStub{}
				billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
				events := &durableUsageEventRepoStub{}
				fixture := newRequiredUsageFixture(kind, logs, billing)
				*fixture.platform = platform
				ctx, ack := WithDurableUsageSettlement(context.Background(), durableUsageBinding(), fixture.key.ID, events)
				require.ErrorIs(t, fixture.call(ctx), ErrUsageSettlementPayload)
				_, err := ack.Result()
				require.ErrorIs(t, err, ErrUsageSettlementUnconfirmed)
				require.Zero(t, events.stageCalls)
				require.Zero(t, billing.calls)
				require.Zero(t, events.journalCalls)
				require.Zero(t, logs.createCalls)
			})
		}
	}
}

func TestDurableUsageSettlementRecordsPlatformJournalAfterConfirmedBillingDedup(t *testing.T) {
	for _, kind := range []string{"gateway", "openai"} {
		t.Run(kind, func(t *testing.T) {
			logs := &requiredUsageLogRepoStub{inserted: false}
			billing := &requiredUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: false}}
			events := &durableUsageEventRepoStub{}
			fixture := newRequiredUsageFixture(kind, logs, billing)
			// The handler's resolved platform may differ from the protocol family
			// and group. The frozen event must preserve that explicit decision.
			*fixture.platform = PlatformAntigravity
			fixture.key.Group = &Group{Platform: PlatformGemini}
			ctx, ack := WithDurableUsageSettlement(context.Background(), durableUsageBinding(), fixture.key.ID, events)
			events.beforeJournal = func(context.Context) {
				require.Equal(t, 1, billing.calls)
				require.Zero(t, logs.createCalls)
				require.Equal(t, PlatformAntigravity, events.staged.PlatformQuota.Platform)
			}
			require.NoError(t, fixture.call(ctx))
			_, err := ack.Result()
			require.NoError(t, err)
			require.Equal(t, 1, events.journalCalls)
			require.Equal(t, 1, events.completeCalls)
		})
	}
}

type durablePlatformLegacyCacheSpy struct {
	BillingCache
	quotaReads, quotaIncrements, balanceInvalidations int
}

func (s *durablePlatformLegacyCacheSpy) GetUserPlatformQuotaCache(context.Context, int64, string) (*UserPlatformQuotaCacheEntry, bool, error) {
	s.quotaReads++
	limit := 10.0
	return &UserPlatformQuotaCacheEntry{SchemaVersion: UserPlatformQuotaCacheSchemaV1, DailyLimitUSD: &limit}, true, nil
}

func (s *durablePlatformLegacyCacheSpy) IncrUserPlatformQuotaUsageCache(context.Context, int64, string, float64, time.Duration, bool) (UserPlatformQuotaIncrementResult, error) {
	s.quotaIncrements++
	return UserPlatformQuotaIncrementResult{Status: QuotaIncrementApplied}, nil
}

func (s *durablePlatformLegacyCacheSpy) IncrBoundUserPlatformQuotaUsageCache(context.Context, UserPlatformQuotaUsageBinding, time.Duration, bool) (UserPlatformQuotaBoundIncrementResult, error) {
	s.quotaIncrements++
	return UserPlatformQuotaBoundIncrementResult{Status: QuotaIncrementApplied, AppliedWindows: QuotaWindowAll}, nil
}

func (s *durablePlatformLegacyCacheSpy) InvalidateUserBalance(context.Context, int64) error {
	s.balanceInvalidations++
	return nil
}

func TestDurableUsageSettlementFinalizeSkipsOnlyLegacyPlatformMetering(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "ordinary"
		if durable {
			name = "durable"
		}
		t.Run(name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Database.UserPlatformQuotaFlusherEnabled = true
			cfg.Billing.UserPlatformQuotaCacheTTLSeconds = 120
			cache := &durablePlatformLegacyCacheSpy{}
			limit := 10.0
			quotaRepo := &fakeQuotaRepo{rec: &UserPlatformQuotaRecord{QuotaRowID: 101, ControlGeneration: 1, UserID: 1, Platform: PlatformOpenAI, DailyLimitUSD: &limit}}
			deps := &billingDeps{
				cfg: cfg, deferredService: &DeferredService{},
				billingCacheService:   &BillingCacheService{cfg: cfg, cache: cache, userPlatformQuotaRepo: quotaRepo},
				userPlatformQuotaRepo: quotaRepo,
			}
			ctx := context.Background()
			if durable {
				ctx, _ = WithDurableUsageSettlement(ctx, durableUsageBinding(), 2, &durableUsageEventRepoStub{})
			}
			newBalance := 0.0
			params := &postUsageBillingParams{
				Cost: &CostBreakdown{ActualCost: 1.25, TotalCost: 1.25}, User: &User{ID: 1}, APIKey: &APIKey{ID: 2},
				Account: &Account{ID: 3}, Platform: PlatformOpenAI,
			}
			cmd := buildUsageBillingCommand("durable-bound-finalize", &UsageLog{BillingType: BillingTypeBalance}, params)
			var err error
			cmd.OrdinaryPlatformQuota, err = FreezeUsageBillingOrdinaryPlatformIntent(cmd, PlatformOpenAI, 1.25, false)
			require.NoError(t, err)
			receipt := validOrdinaryPlatformReceiptForTest(cmd)
			require.NoError(t, ValidateUsageBillingOrdinaryPlatformReceipt(cmd, receipt))
			finalizePostUsageBilling(ctx, cmd, params, deps, &UsageBillingApplyResult{Applied: true, NewBalance: &newBalance, OrdinaryPlatformQuotaReceipt: receipt})
			require.Equal(t, 1, cache.balanceInvalidations, "other financial cache behavior is retained")
			if durable {
				require.Zero(t, cache.quotaReads)
				require.Zero(t, cache.quotaIncrements)
				require.Zero(t, quotaRepo.getCalls)
			} else {
				require.Zero(t, cache.quotaReads)
				require.Zero(t, quotaRepo.getCalls, "Apply receipt replaces a later configuration read")
				require.Equal(t, 1, cache.quotaIncrements)
			}
		})
	}
}
