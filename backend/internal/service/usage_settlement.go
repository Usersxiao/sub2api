package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// ErrUsageSettlementUnconfirmed means that committed billing and its matching
// persisted usage row have not both been observed for the required invocation.
var ErrUsageSettlementUnconfirmed = errors.New("usage settlement is unconfirmed")

type requiredUsageSettlementKey struct{}

// UsageSettlementResult contains only durable settlement references. Amounts and
// internal credentials must not be copied into a public invocation receipt.
type UsageSettlementResult struct {
	RequestID          string `json:"-"`
	APIKeyID           int64  `json:"-"`
	UsageLogID         int64  `json:"-"`
	BillingFingerprint string `json:"-"`
}

// UsageSettlementAcknowledgment is scoped to one invocation. Success requires
// a synchronous billing result, a matching row read back from storage, and a
// successful return from RecordUsage. Absence of an observation is never success.
type UsageSettlementAcknowledgment struct {
	mu        sync.Mutex
	result    UsageSettlementResult
	billed    bool
	persisted bool
	completed bool
	err       error
	durable   *durableUsageSettlementPolicy
}

type durableUsageSettlementPolicy struct {
	repo           UsageSettlementEventRepository
	binding        UsageSettlementBinding
	stageAttempted bool
	event          *UsageSettlementEvent
	leaseToken     string
	alreadySettled bool
}

func WithRequiredUsageSettlement(ctx context.Context, expectedRequestID string, apiKeyID int64) (context.Context, *UsageSettlementAcknowledgment) {
	if ctx == nil {
		ctx = context.Background()
	}
	ack := &UsageSettlementAcknowledgment{result: UsageSettlementResult{
		RequestID: strings.TrimSpace(expectedRequestID), APIKeyID: apiKeyID,
	}}
	if !strings.HasPrefix(ack.result.RequestID, "client:") || len(ack.result.RequestID) == len("client:") || apiKeyID <= 0 {
		ack.err = fmt.Errorf("%w: invalid invocation billing identity", ErrUsageSettlementUnconfirmed)
	}
	return context.WithValue(ctx, requiredUsageSettlementKey{}, ack), ack
}

// WithDurableUsageSettlement is the production policy for owned invocations.
// WithRequiredUsageSettlement remains useful for synchronous, non-durable
// callers, but cannot substitute for this event repository boundary.
func WithDurableUsageSettlement(ctx context.Context, binding *UsageSettlementBinding, apiKeyID int64, repo UsageSettlementEventRepository) (context.Context, *UsageSettlementAcknowledgment) {
	invocationID := ""
	if binding != nil {
		invocationID = binding.InvocationID
	}
	ctx, ack := WithRequiredUsageSettlement(ctx, "client:"+invocationID, apiKeyID)
	ack.durable = &durableUsageSettlementPolicy{repo: repo}
	if binding == nil || repo == nil || !clientIdentifier.MatchString(invocationID) || !validUsageSettlementToken(binding.OwnerToken, "attempt-") {
		FailRequiredUsageSettlement(ctx, ErrUsageSettlementUnconfirmed)
		return ctx, ack
	}
	ack.durable.binding = *binding
	return ctx, ack
}

func validUsageSettlementToken(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, prefix))
	return err == nil && len(raw) == 32 && value == prefix+base64.RawURLEncoding.EncodeToString(raw)
}

func requiredUsageSettlement(ctx context.Context) *UsageSettlementAcknowledgment {
	if ctx == nil {
		return nil
	}
	ack, _ := ctx.Value(requiredUsageSettlementKey{}).(*UsageSettlementAcknowledgment)
	return ack
}

func RequiredUsageSettlement(ctx context.Context) bool {
	return requiredUsageSettlement(ctx) != nil
}

func durableUsageSettlement(ctx context.Context) bool {
	ack := requiredUsageSettlement(ctx)
	return ack != nil && ack.durable != nil
}

func (ack *UsageSettlementAcknowledgment) Result() (UsageSettlementResult, error) {
	if ack == nil {
		return UsageSettlementResult{}, ErrUsageSettlementUnconfirmed
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	if ack.err != nil {
		return UsageSettlementResult{}, ack.err
	}
	if !ack.completed || !ack.billed || !ack.persisted || ack.result.UsageLogID <= 0 || ack.result.BillingFingerprint == "" {
		return UsageSettlementResult{}, ErrUsageSettlementUnconfirmed
	}
	return ack.result, nil
}

// FailRequiredUsageSettlement records a sticky failure, including task panics
// caught by a handler. A later successful-looking observation cannot erase it.
func FailRequiredUsageSettlement(ctx context.Context, err error) {
	ack := requiredUsageSettlement(ctx)
	if ack == nil {
		return
	}
	if err == nil {
		err = ErrUsageSettlementUnconfirmed
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	if ack.err == nil {
		ack.err = errors.Join(ErrUsageSettlementUnconfirmed, err)
	}
}

func requiredUsageSettlementError(ctx context.Context) error {
	ack := requiredUsageSettlement(ctx)
	if ack == nil {
		return nil
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	return ack.err
}

// This function must be deferred directly by RecordUsage so a panic remains a
// failed observation even when the caller recovers it outside the service.
func completeRequiredUsageSettlement(ctx context.Context, recordErr *error) {
	if recovered := recover(); recovered != nil {
		FailRequiredUsageSettlement(ctx, ErrUsageSettlementUnconfirmed)
		releaseDurableUsageSettlement(ctx, ErrUsageSettlementUnconfirmed)
		panic(recovered)
	}
	ack := requiredUsageSettlement(ctx)
	if ack == nil {
		return
	}
	if *recordErr != nil {
		FailRequiredUsageSettlement(ctx, *recordErr)
		releaseDurableUsageSettlement(ctx, *recordErr)
		return
	}
	ack.mu.Lock()
	if ack.err == nil && (!ack.billed || !ack.persisted) {
		ack.err = ErrUsageSettlementUnconfirmed
	}
	if ack.err != nil {
		*recordErr = ack.err
		ack.mu.Unlock()
		releaseDurableUsageSettlement(ctx, *recordErr)
		return
	}
	durable, result := ack.durable, ack.result
	ack.mu.Unlock()
	if durable != nil && !durable.alreadySettled {
		if durable.event == nil || durable.leaseToken == "" {
			*recordErr = ErrUsageSettlementUnconfirmed
		} else {
			*recordErr = durable.repo.Complete(ctx, durable.binding.InvocationID, durable.leaseToken, result)
		}
		if *recordErr != nil {
			FailRequiredUsageSettlement(ctx, *recordErr)
			releaseDurableUsageSettlement(ctx, *recordErr)
			return
		}
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	if ack.err != nil {
		*recordErr = ack.err
		return
	}
	ack.completed = true
}

func releaseDurableUsageSettlement(ctx context.Context, cause error) {
	ack := requiredUsageSettlement(ctx)
	if ack == nil {
		return
	}
	ack.mu.Lock()
	durable := ack.durable
	if durable == nil || durable.event == nil || durable.alreadySettled || durable.leaseToken == "" {
		ack.mu.Unlock()
		return
	}
	invocationID, leaseToken, repo := durable.binding.InvocationID, durable.leaseToken, durable.repo
	ack.mu.Unlock()
	// Release is best effort after an already durable stage. A failed release
	// is recovered through lease expiry, never through another model dispatch.
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	conflict := errors.Is(cause, ErrUsageSettlementEventConflict) || errors.Is(cause, ErrUsageBillingRequestConflict) || errors.Is(cause, ErrUsageSettlementPayload)
	_ = repo.Release(releaseCtx, invocationID, leaseToken, conflict)
}

// stageRequiredUsageSettlement returns the frozen billing command and whether
// an already settled event makes another Apply unnecessary.
func stageRequiredUsageSettlement(ctx context.Context, cmd *UsageBillingCommand, usageLog *UsageLog, platform string) (*UsageBillingCommand, bool, error) {
	ack := requiredUsageSettlement(ctx)
	if ack == nil || ack.durable == nil {
		return cmd, false, nil
	}
	ack.mu.Lock()
	durable := ack.durable
	if ack.err != nil || durable.stageAttempted {
		ack.mu.Unlock()
		return nil, false, ErrUsageSettlementUnconfirmed
	}
	durable.stageAttempted = true
	ack.mu.Unlock()
	event, err := FreezeUsageSettlementEventWithPlatform(durable.binding.InvocationID, cmd, usageLog, platform)
	if err != nil {
		return nil, false, err
	}
	leaseToken, err := gatewayRandom("settlement-")
	if err != nil {
		return nil, false, err
	}
	expectedFingerprint := event.EventFingerprint
	binding := durable.binding
	claim, err := durable.repo.Stage(ctx, &binding, event, leaseToken)
	if err != nil {
		return nil, false, err
	}
	if claim == nil || claim.Event == nil || claim.Event.EventFingerprint != expectedFingerprint {
		return nil, false, ErrUsageSettlementEventConflict
	}
	encoded, err := EncodeUsageSettlementEvent(claim.Event)
	if err != nil {
		return nil, false, err
	}
	frozen, err := DecodeUsageSettlementEvent(encoded)
	if err != nil || frozen.EventFingerprint != expectedFingerprint || claim.Event.CreatedAt.IsZero() {
		return nil, false, ErrUsageSettlementEventConflict
	}
	frozen.CreatedAt = claim.Event.CreatedAt
	frozen.Usage.CreatedAt = claim.Event.CreatedAt
	frozen.State = claim.Event.State
	frozen.UsageLogID = claim.Event.UsageLogID
	if !claim.Owned {
		if frozen.State != UsageSettlementSettled || frozen.UsageLogID <= 0 {
			if frozen.State == UsageSettlementConflict {
				return nil, false, ErrUsageSettlementEventConflict
			}
			return nil, false, ErrUsageSettlementLease
		}
		frozen.Usage.ID = frozen.UsageLogID
		ack.mu.Lock()
		durable.event = frozen
		durable.alreadySettled = true
		ack.billed = true
		ack.result.BillingFingerprint = frozen.Command.RequestFingerprint
		ack.mu.Unlock()
		*usageLog = *frozen.Usage
		return frozen.Command, true, nil
	}
	if frozen.State != UsageSettlementPending || claim.Event.LeaseToken != leaseToken || claim.Event.LeaseExpiresAt == nil || !claim.Event.LeaseExpiresAt.After(time.Now()) {
		return nil, false, ErrUsageSettlementLease
	}
	ack.mu.Lock()
	durable.event = frozen
	durable.leaseToken = leaseToken
	ack.mu.Unlock()
	*usageLog = *frozen.Usage
	return frozen.Command, false, nil
}

func recordRequiredPlatformQuota(ctx context.Context) error {
	ack := requiredUsageSettlement(ctx)
	if ack == nil || ack.durable == nil {
		return nil
	}
	ack.mu.Lock()
	durable := ack.durable
	if ack.err != nil || !ack.billed || durable.event == nil || durable.alreadySettled || durable.leaseToken == "" {
		ack.mu.Unlock()
		return ErrUsageSettlementUnconfirmed
	}
	invocationID, leaseToken, repo := durable.binding.InvocationID, durable.leaseToken, durable.repo
	ack.mu.Unlock()
	return repo.RecordPlatformQuota(ctx, invocationID, leaseToken)
}

func requiredUsageRequestID(ctx context.Context) string {
	ack := requiredUsageSettlement(ctx)
	if ack == nil {
		return ""
	}
	return ack.result.RequestID // immutable after construction
}

func validateRequiredUsageBilling(ctx context.Context, cmd *UsageBillingCommand, usageLog *UsageLog) error {
	ack := requiredUsageSettlement(ctx)
	if ack == nil {
		return nil
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	if ack.err != nil {
		return ack.err
	}
	if cmd == nil || usageLog == nil || cmd.RequestID != ack.result.RequestID || cmd.APIKeyID != ack.result.APIKeyID ||
		usageLog.RequestID != cmd.RequestID || usageLog.APIKeyID != cmd.APIKeyID ||
		usageLog.UserID != cmd.UserID || usageLog.AccountID != cmd.AccountID || cmd.RequestFingerprint == "" {
		return fmt.Errorf("%w: billing identity mismatch", ErrUsageSettlementUnconfirmed)
	}
	if ack.billed && ack.result.BillingFingerprint != cmd.RequestFingerprint {
		return fmt.Errorf("%w: multiple billing fingerprints", ErrUsageSettlementUnconfirmed)
	}
	return nil
}

func observeRequiredUsageBilling(ctx context.Context, cmd *UsageBillingCommand, result *UsageBillingApplyResult) error {
	ack := requiredUsageSettlement(ctx)
	if ack == nil {
		return nil
	}
	if result == nil {
		return fmt.Errorf("%w: missing billing repository result", ErrUsageSettlementUnconfirmed)
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	// A non-nil Applied=false result is the repository's committed dedup result:
	// Apply checks the existing fingerprint and rejects conflicting requests.
	ack.billed = true
	ack.result.BillingFingerprint = cmd.RequestFingerprint
	return nil
}

func writeRequiredUsageLog(ctx context.Context, repo UsageLogRepository, usageLog *UsageLog) error {
	ack := requiredUsageSettlement(ctx)
	if ack == nil {
		return nil
	}
	if err := requiredUsageSettlementError(ctx); err != nil {
		return err
	}
	if repo == nil || usageLog == nil || usageLog.RequestID != ack.result.RequestID || usageLog.APIKeyID != ack.result.APIKeyID {
		return fmt.Errorf("%w: missing usage repository or matching usage log", ErrUsageSettlementUnconfirmed)
	}
	ack.mu.Lock()
	billed := ack.billed
	ack.mu.Unlock()
	if !billed {
		return ErrUsageSettlementUnconfirmed
	}
	expected := *usageLog
	usageCtx, cancel := detachedBillingContext(ctx)
	defer cancel()
	// Create waits for persistence. CreateBestEffort and queue acceptance do not
	// constitute the durable acknowledgment required by client invocations.
	if ack.durable == nil || !ack.durable.alreadySettled {
		if _, err := repo.Create(usageCtx, usageLog); err != nil {
			return errors.Join(ErrUsageSettlementUnconfirmed, err)
		}
	}
	if usageLog.ID <= 0 {
		return fmt.Errorf("%w: missing persisted usage id", ErrUsageSettlementUnconfirmed)
	}
	persisted, err := repo.GetByID(usageCtx, usageLog.ID)
	if err != nil {
		return errors.Join(ErrUsageSettlementUnconfirmed, err)
	}
	// Create may have returned the ID of an older conflicting row without
	// loading its fields. Never acknowledge that row based on its ID alone.
	if persisted == nil || persisted.ID != usageLog.ID || !usageSettlementLogsMatch(&expected, persisted) {
		return errors.Join(ErrUsageSettlementUnconfirmed, ErrUsageSettlementEventConflict)
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	if !ack.billed || ack.err != nil {
		return ErrUsageSettlementUnconfirmed
	}
	if ack.persisted && ack.result.UsageLogID != persisted.ID {
		return fmt.Errorf("%w: multiple persisted usage ids", ErrUsageSettlementUnconfirmed)
	}
	ack.persisted = true
	ack.result.UsageLogID = persisted.ID
	return nil
}

// UsageSettlementLogsMatch validates a stored usage row against the closed
// financial and protocol snapshot, using the database's exact numeric scales.
func UsageSettlementLogsMatch(expected, actual *UsageLog) bool {
	return usageSettlementLogsMatch(expected, actual)
}

func usageSettlementLogsMatch(expected, actual *UsageLog) bool {
	if expected == nil || actual == nil || expected.UserID != actual.UserID || expected.APIKeyID != actual.APIKeyID ||
		expected.AccountID != actual.AccountID || expected.RequestID != actual.RequestID || expected.Model != actual.Model ||
		usageSettlementRequestedModel(expected) != usageSettlementRequestedModel(actual) ||
		!usageSettlementStringEqual(expected.UpstreamModel, actual.UpstreamModel) ||
		!usageSettlementStringEqual(expected.UpstreamResponseModel, actual.UpstreamResponseModel) ||
		!usageSettlementBoolEqual(expected.UpstreamModelMismatch, actual.UpstreamModelMismatch) ||
		!usageSettlementStringEqual(expected.ServiceTier, actual.ServiceTier) ||
		!usageSettlementStringEqual(expected.ReasoningEffort, actual.ReasoningEffort) ||
		!usageSettlementStringEqual(expected.RequestedReasoningEffort, actual.RequestedReasoningEffort) ||
		!usageSettlementStringEqual(expected.InboundEndpoint, actual.InboundEndpoint) ||
		!usageSettlementStringEqual(expected.UpstreamEndpoint, actual.UpstreamEndpoint) ||
		!usageSettlementInt64Equal(expected.GroupID, actual.GroupID) ||
		!usageSettlementInt64Equal(expected.SubscriptionID, actual.SubscriptionID) ||
		!usageSettlementInt64Equal(expected.ChannelID, actual.ChannelID) ||
		expected.InputTokens != actual.InputTokens || expected.OutputTokens != actual.OutputTokens ||
		expected.CacheCreationTokens != actual.CacheCreationTokens || expected.CacheReadTokens != actual.CacheReadTokens ||
		expected.CacheCreation5mTokens != actual.CacheCreation5mTokens || expected.CacheCreation1hTokens != actual.CacheCreation1hTokens ||
		expected.ImageInputTokens != actual.ImageInputTokens || expected.ImageOutputTokens != actual.ImageOutputTokens ||
		expected.ImageCount != actual.ImageCount || expected.VideoCount != actual.VideoCount ||
		expected.BillingType != actual.BillingType || expected.EffectiveRequestType() != actual.EffectiveRequestType() ||
		expected.Stream != actual.Stream || expected.OpenAIWSMode != actual.OpenAIWSMode || expected.NativeCompactionV2 != actual.NativeCompactionV2 ||
		!usageSettlementIntEqual(expected.DurationMs, actual.DurationMs) || !usageSettlementIntEqual(expected.FirstTokenMs, actual.FirstTokenMs) ||
		!usageSettlementStringEqual(expected.BillingMode, actual.BillingMode) ||
		!usageSettlementStringEqual(expected.BillingTier, actual.BillingTier) ||
		!usageSettlementStringEqual(expected.ModelMappingChain, actual.ModelMappingChain) ||
		!usageSettlementStringEqual(expected.ImageSize, actual.ImageSize) ||
		!usageSettlementStringEqual(expected.ImageInputSize, actual.ImageInputSize) ||
		!usageSettlementStringEqual(expected.ImageOutputSize, actual.ImageOutputSize) ||
		!usageSettlementStringEqual(expected.ImageSizeSource, actual.ImageSizeSource) ||
		!usageSettlementStringEqual(expected.MediaType, actual.MediaType) ||
		!maps.Equal(expected.ImageSizeBreakdown, actual.ImageSizeBreakdown) ||
		!usageSettlementStringEqual(expected.VideoResolution, actual.VideoResolution) ||
		!usageSettlementIntEqual(expected.VideoDurationSeconds, actual.VideoDurationSeconds) ||
		!usageSettlementTimestampEqual(expected.CreatedAt, actual.CreatedAt) ||
		expected.CacheTTLOverridden != actual.CacheTTLOverridden || expected.LongContextBillingApplied != actual.LongContextBillingApplied {
		return false
	}
	for _, amounts := range [][2]float64{
		{expected.InputCost, actual.InputCost}, {expected.OutputCost, actual.OutputCost},
		{expected.CacheCreationCost, actual.CacheCreationCost}, {expected.CacheReadCost, actual.CacheReadCost},
		{expected.ImageInputCost, actual.ImageInputCost}, {expected.ImageOutputCost, actual.ImageOutputCost},
		{expected.TotalCost, actual.TotalCost}, {expected.ActualCost, actual.ActualCost},
	} {
		if !usageSettlementDecimalEqual(amounts[0], amounts[1], 10) {
			return false
		}
	}
	return usageSettlementDecimalEqual(expected.RateMultiplier, actual.RateMultiplier, 4) &&
		usageSettlementOptionalDecimalEqual(expected.AccountRateMultiplier, actual.AccountRateMultiplier, 4) &&
		usageSettlementOptionalDecimalEqual(expected.AccountStatsCost, actual.AccountStatsCost, 10)
}

func usageSettlementRequestedModel(log *UsageLog) string {
	if model := strings.TrimSpace(log.RequestedModel); model != "" {
		return model
	}
	return log.Model
}

func usageSettlementStringEqual(a, b *string) bool {
	return optionalStringValue(a) == optionalStringValue(b)
}

func usageSettlementInt64Equal(a, b *int64) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func usageSettlementIntEqual(a, b *int) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func usageSettlementBoolEqual(a, b *bool) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func usageSettlementTimestampEqual(a, b time.Time) bool {
	if a.IsZero() || b.IsZero() {
		return a.IsZero() && b.IsZero()
	}
	return usageSettlementTimestampAtDatabasePrecision(a).Equal(usageSettlementTimestampAtDatabasePrecision(b))
}

func usageSettlementTimestampAtDatabasePrecision(value time.Time) time.Time {
	// lib/pq sends timestamp fractions as text. PostgreSQL stores their nearest
	// microsecond, with ties rounded to the even microsecond. Quantizing to the
	// same fixed scale allows that representation change without a time window
	// that could accept a different consumption timestamp.
	microseconds := value.Nanosecond() / 1000
	remainder := value.Nanosecond() % 1000
	if remainder > 500 || (remainder == 500 && microseconds%2 != 0) {
		microseconds++
	}
	return value.Truncate(time.Second).Add(time.Duration(microseconds) * time.Microsecond)
}

func usageSettlementOptionalDecimalEqual(a, b *float64, scale int32) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && usageSettlementDecimalEqual(*a, *b, scale))
}

func usageSettlementDecimalEqual(a, b float64, scale int32) bool {
	if math.IsNaN(a) || math.IsNaN(b) || math.IsInf(a, 0) || math.IsInf(b, 0) {
		return false
	}
	// Match PostgreSQL's numeric column precision, rather than accepting an
	// epsilon that could hide a different persisted charge.
	return decimal.NewFromFloat(a).Round(scale).Equal(decimal.NewFromFloat(b).Round(scale))
}
