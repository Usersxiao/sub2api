package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/shopspring/decimal"
)

type UsageSettlementState string

const (
	UsageSettlementPending         UsageSettlementState = "pending"
	UsageSettlementSettled         UsageSettlementState = "settled"
	UsageSettlementConflict        UsageSettlementState = "conflict"
	UsageSettlementLease                                = 30 * time.Second
	UsageSettlementMaxPayloadBytes                      = 256 << 10
)

var (
	ErrUsageSettlementEventConflict = errors.New("usage settlement event conflict")
	ErrUsageSettlementLease         = errors.New("usage settlement lease is not owned")
	ErrUsageSettlementPayload       = errors.New("invalid usage settlement payload")
)

// Bindings are private capabilities derived from an owned invocation attempt.
// They are never accepted from a client request or serialized into an event.
type UsageSettlementBinding struct {
	InvocationID string `json:"-"`
	OwnerToken   string `json:"-"`
}

type UsageSettlementEvent struct {
	SchemaVersion    int                           `json:"-"`
	InvocationID     string                        `json:"-"`
	RequestID        string                        `json:"-"`
	APIKeyID         int64                         `json:"-"`
	EventFingerprint string                        `json:"-"`
	Command          *UsageBillingCommand          `json:"-"`
	Usage            *UsageLog                     `json:"-"`
	PlatformQuota    *UsageSettlementPlatformQuota `json:"-"`
	State            UsageSettlementState          `json:"-"`
	UsageLogID       int64                         `json:"-"`
	LeaseToken       string                        `json:"-"`
	LeaseExpiresAt   *time.Time                    `json:"-"`
	CreatedAt        time.Time                     `json:"-"`
	UpdatedAt        time.Time                     `json:"-"`
}

// PlatformQuota is a frozen metering intent. Its independent journal does not
// update the legacy platform counters or imply quota admission is complete.
type UsageSettlementPlatformQuota struct {
	Platform string  `json:"-"`
	CostUSD  float64 `json:"-"`
}

type UsageSettlementClaim struct {
	Event *UsageSettlementEvent `json:"-"`
	Owned bool                  `json:"-"`
}

// Stage persists a frozen event before any billing and fences the live owner.
// ClaimPending may only lease already frozen settlement work. Neither method
// authorizes another model dispatch or another pricing calculation.
type UsageSettlementEventRepository interface {
	Stage(context.Context, *UsageSettlementBinding, *UsageSettlementEvent, string) (*UsageSettlementClaim, error)
	ClaimPending(context.Context, string, int) ([]*UsageSettlementEvent, error)
	RecordPlatformQuota(context.Context, string, string) error
	Complete(context.Context, string, string, UsageSettlementResult) error
	Release(context.Context, string, string, bool) error
}

// These wire structs are a closed list. Never marshal UsageLog itself: its
// relationship graph can contain credentials, and new operational fields must
// not silently become durable accounting payloads.
type usageSettlementCommandWire struct {
	RequestID                  string  `json:"request_id"`
	APIKeyID                   int64   `json:"api_key_id"`
	RequestFingerprint         string  `json:"request_fingerprint"`
	RequestPayloadHash         string  `json:"request_payload_hash"`
	UserID                     int64   `json:"user_id"`
	AccountID                  int64   `json:"account_id"`
	SubscriptionID             *int64  `json:"subscription_id"`
	AccountType                string  `json:"account_type"`
	Model                      string  `json:"model"`
	ServiceTier                string  `json:"service_tier"`
	ReasoningEffort            string  `json:"reasoning_effort"`
	BillingType                int8    `json:"billing_type"`
	InputTokens                int     `json:"input_tokens"`
	OutputTokens               int     `json:"output_tokens"`
	CacheCreationTokens        int     `json:"cache_creation_tokens"`
	CacheReadTokens            int     `json:"cache_read_tokens"`
	ImageCount                 int     `json:"image_count"`
	MediaType                  string  `json:"media_type"`
	BalanceCost                float64 `json:"balance_cost"`
	SubscriptionCost           float64 `json:"subscription_cost"`
	APIKeyQuotaCost            float64 `json:"api_key_quota_cost"`
	APIKeyRateLimitCost        float64 `json:"api_key_rate_limit_cost"`
	AccountQuotaCost           float64 `json:"account_quota_cost"`
	AccountQuotaHasDailyLimit  bool    `json:"account_quota_has_daily_limit"`
	AccountQuotaHasWeeklyLimit bool    `json:"account_quota_has_weekly_limit"`
}

type usageSettlementUsageWire struct {
	UserID                    int64          `json:"user_id"`
	APIKeyID                  int64          `json:"api_key_id"`
	AccountID                 int64          `json:"account_id"`
	RequestID                 string         `json:"request_id"`
	Model                     string         `json:"model"`
	RequestedModel            string         `json:"requested_model"`
	UpstreamModel             *string        `json:"upstream_model"`
	UpstreamResponseModel     *string        `json:"upstream_response_model"`
	UpstreamModelMismatch     *bool          `json:"upstream_model_mismatch"`
	ChannelID                 *int64         `json:"channel_id"`
	ModelMappingChain         *string        `json:"model_mapping_chain"`
	BillingTier               *string        `json:"billing_tier"`
	BillingMode               *string        `json:"billing_mode"`
	ServiceTier               *string        `json:"service_tier"`
	ReasoningEffort           *string        `json:"reasoning_effort"`
	RequestedReasoningEffort  *string        `json:"requested_reasoning_effort"`
	InboundEndpoint           *string        `json:"inbound_endpoint"`
	UpstreamEndpoint          *string        `json:"upstream_endpoint"`
	GroupID                   *int64         `json:"group_id"`
	SubscriptionID            *int64         `json:"subscription_id"`
	InputTokens               int            `json:"input_tokens"`
	OutputTokens              int            `json:"output_tokens"`
	CacheCreationTokens       int            `json:"cache_creation_tokens"`
	CacheReadTokens           int            `json:"cache_read_tokens"`
	CacheCreation5mTokens     int            `json:"cache_creation_5m_tokens"`
	CacheCreation1hTokens     int            `json:"cache_creation_1h_tokens"`
	ImageInputTokens          int            `json:"image_input_tokens"`
	ImageOutputTokens         int            `json:"image_output_tokens"`
	InputCost                 float64        `json:"input_cost"`
	OutputCost                float64        `json:"output_cost"`
	CacheCreationCost         float64        `json:"cache_creation_cost"`
	CacheReadCost             float64        `json:"cache_read_cost"`
	ImageInputCost            float64        `json:"image_input_cost"`
	ImageOutputCost           float64        `json:"image_output_cost"`
	TotalCost                 float64        `json:"total_cost"`
	ActualCost                float64        `json:"actual_cost"`
	RateMultiplier            float64        `json:"rate_multiplier"`
	AccountRateMultiplier     *float64       `json:"account_rate_multiplier"`
	AccountStatsCost          *float64       `json:"account_stats_cost"`
	LongContextBillingApplied bool           `json:"long_context_billing_applied"`
	BillingType               int8           `json:"billing_type"`
	RequestType               RequestType    `json:"request_type"`
	Stream                    bool           `json:"stream"`
	OpenAIWSMode              bool           `json:"openai_ws_mode"`
	NativeCompactionV2        bool           `json:"native_compaction_v2"`
	DurationMs                *int           `json:"duration_ms"`
	FirstTokenMs              *int           `json:"first_token_ms"`
	CacheTTLOverridden        bool           `json:"cache_ttl_overridden"`
	ImageCount                int            `json:"image_count"`
	ImageSize                 *string        `json:"image_size"`
	ImageInputSize            *string        `json:"image_input_size"`
	ImageOutputSize           *string        `json:"image_output_size"`
	ImageSizeSource           *string        `json:"image_size_source"`
	ImageSizeBreakdown        map[string]int `json:"image_size_breakdown"`
	MediaType                 *string        `json:"media_type"`
	VideoCount                int            `json:"video_count"`
	VideoResolution           *string        `json:"video_resolution"`
	VideoDurationSeconds      *int           `json:"video_duration_seconds"`
}

type usageSettlementPayload struct {
	Version       int                               `json:"version"`
	InvocationID  string                            `json:"invocation_id"`
	RequestID     string                            `json:"request_id"`
	APIKeyID      int64                             `json:"api_key_id"`
	Command       usageSettlementCommandWire        `json:"command"`
	Usage         usageSettlementUsageWire          `json:"usage"`
	PlatformQuota *usageSettlementPlatformQuotaWire `json:"platform_quota,omitempty"`
}

type usageSettlementPlatformQuotaWire struct {
	Platform string  `json:"platform"`
	CostUSD  float64 `json:"cost_usd"`
}

// FreezeUsageSettlementEvent copies only the closed accounting schema and
// returns detached data. Subsequent mutations of live gateway objects cannot
// change the payload that a settlement worker will replay.
func FreezeUsageSettlementEvent(invocationID string, cmd *UsageBillingCommand, usage *UsageLog) (*UsageSettlementEvent, error) {
	return freezeUsageSettlementEvent(invocationID, cmd, usage, nil)
}

// FreezeUsageSettlementEventWithPlatform creates the v2 metering intent from a
// concrete platform already resolved by the server's routing handler.
func FreezeUsageSettlementEventWithPlatform(invocationID string, cmd *UsageBillingCommand, usage *UsageLog, platform string) (*UsageSettlementEvent, error) {
	if usage == nil {
		return nil, ErrUsageSettlementPayload
	}
	return freezeUsageSettlementEvent(invocationID, cmd, usage, &UsageSettlementPlatformQuota{Platform: platform, CostUSD: quantizeUsageSettlementPlatformCost(usage.ActualCost)})
}

func freezeUsageSettlementEvent(invocationID string, cmd *UsageBillingCommand, usage *UsageLog, platform *UsageSettlementPlatformQuota) (*UsageSettlementEvent, error) {
	if cmd == nil || usage == nil {
		return nil, ErrUsageSettlementPayload
	}
	frozenCommand := *cmd
	frozenCommand.Normalize()
	frozenUsage := *usage
	frozenUsage.RequestID = strings.TrimSpace(frozenUsage.RequestID)
	frozenUsage.RequestedModel = usageSettlementRequestedModel(&frozenUsage)
	frozenUsage.SyncRequestTypeAndLegacyFields()
	version := 1
	if platform != nil {
		version = 2
	}
	event := &UsageSettlementEvent{SchemaVersion: version, InvocationID: invocationID, RequestID: frozenCommand.RequestID, APIKeyID: frozenCommand.APIKeyID,
		Command: &frozenCommand, Usage: &frozenUsage, PlatformQuota: platform, State: UsageSettlementPending}
	payload, err := EncodeUsageSettlementEvent(event)
	if err != nil {
		return nil, err
	}
	return DecodeUsageSettlementEvent(payload)
}

// EncodeUsageSettlementEvent excludes state, owner/lease capabilities, proofs,
// and event timestamps. EventFingerprint hashes exactly these canonical bytes.
func EncodeUsageSettlementEvent(event *UsageSettlementEvent) ([]byte, error) {
	if event == nil || event.Command == nil || event.Usage == nil {
		return nil, ErrUsageSettlementPayload
	}
	payload := usageSettlementPayload{Version: event.SchemaVersion, InvocationID: event.InvocationID, RequestID: event.RequestID,
		APIKeyID: event.APIKeyID, Command: commandToSettlementWire(event.Command), Usage: usageToSettlementWire(event.Usage)}
	if event.PlatformQuota != nil {
		payload.PlatformQuota = &usageSettlementPlatformQuotaWire{Platform: event.PlatformQuota.Platform, CostUSD: event.PlatformQuota.CostUSD}
	}
	if err := validateUsageSettlementPayload(&payload); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil || len(encoded) > UsageSettlementMaxPayloadBytes {
		return nil, ErrUsageSettlementPayload
	}
	fingerprint := settlementPayloadFingerprint(encoded)
	if event.EventFingerprint != "" && event.EventFingerprint != fingerprint {
		return nil, ErrUsageSettlementEventConflict
	}
	event.EventFingerprint = fingerprint
	return encoded, nil
}

func DecodeUsageSettlementEvent(encoded []byte) (*UsageSettlementEvent, error) {
	if len(encoded) == 0 || len(encoded) > UsageSettlementMaxPayloadBytes {
		return nil, ErrUsageSettlementPayload
	}
	var payload usageSettlementPayload
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return nil, ErrUsageSettlementPayload
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, ErrUsageSettlementPayload
	}
	if err := validateUsageSettlementPayload(&payload); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(payload)
	// Only the encoder's canonical bytes are accepted. This also rejects
	// duplicate fields, missing fields, alternate number forms and trailing data.
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, ErrUsageSettlementPayload
	}
	event := &UsageSettlementEvent{SchemaVersion: payload.Version, InvocationID: payload.InvocationID, RequestID: payload.RequestID, APIKeyID: payload.APIKeyID,
		EventFingerprint: settlementPayloadFingerprint(canonical), Command: commandFromSettlementWire(payload.Command),
		Usage: usageFromSettlementWire(payload.Usage), State: UsageSettlementPending}
	if payload.PlatformQuota != nil {
		event.PlatformQuota = &UsageSettlementPlatformQuota{Platform: payload.PlatformQuota.Platform, CostUSD: payload.PlatformQuota.CostUSD}
	}
	return event, nil
}

func settlementPayloadFingerprint(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func commandToSettlementWire(c *UsageBillingCommand) usageSettlementCommandWire {
	return usageSettlementCommandWire{
		RequestID: c.RequestID, APIKeyID: c.APIKeyID, RequestFingerprint: c.RequestFingerprint, RequestPayloadHash: c.RequestPayloadHash,
		UserID: c.UserID, AccountID: c.AccountID, SubscriptionID: c.SubscriptionID, AccountType: c.AccountType,
		Model: c.Model, ServiceTier: c.ServiceTier, ReasoningEffort: c.ReasoningEffort, BillingType: c.BillingType,
		InputTokens: c.InputTokens, OutputTokens: c.OutputTokens, CacheCreationTokens: c.CacheCreationTokens, CacheReadTokens: c.CacheReadTokens,
		ImageCount: c.ImageCount, MediaType: c.MediaType, BalanceCost: c.BalanceCost, SubscriptionCost: c.SubscriptionCost,
		APIKeyQuotaCost: c.APIKeyQuotaCost, APIKeyRateLimitCost: c.APIKeyRateLimitCost, AccountQuotaCost: c.AccountQuotaCost,
		AccountQuotaHasDailyLimit: c.AccountQuotaHasDailyLimit, AccountQuotaHasWeeklyLimit: c.AccountQuotaHasWeeklyLimit,
	}
}

func commandFromSettlementWire(c usageSettlementCommandWire) *UsageBillingCommand {
	return &UsageBillingCommand{
		RequestID: c.RequestID, APIKeyID: c.APIKeyID, RequestFingerprint: c.RequestFingerprint, RequestPayloadHash: c.RequestPayloadHash,
		UserID: c.UserID, AccountID: c.AccountID, SubscriptionID: c.SubscriptionID, AccountType: c.AccountType,
		Model: c.Model, ServiceTier: c.ServiceTier, ReasoningEffort: c.ReasoningEffort, BillingType: c.BillingType,
		InputTokens: c.InputTokens, OutputTokens: c.OutputTokens, CacheCreationTokens: c.CacheCreationTokens, CacheReadTokens: c.CacheReadTokens,
		ImageCount: c.ImageCount, MediaType: c.MediaType, BalanceCost: c.BalanceCost, SubscriptionCost: c.SubscriptionCost,
		APIKeyQuotaCost: c.APIKeyQuotaCost, APIKeyRateLimitCost: c.APIKeyRateLimitCost, AccountQuotaCost: c.AccountQuotaCost,
		AccountQuotaHasDailyLimit: c.AccountQuotaHasDailyLimit, AccountQuotaHasWeeklyLimit: c.AccountQuotaHasWeeklyLimit,
	}
}

func usageToSettlementWire(u *UsageLog) usageSettlementUsageWire {
	return usageSettlementUsageWire{
		UserID: u.UserID, APIKeyID: u.APIKeyID, AccountID: u.AccountID, RequestID: u.RequestID, Model: u.Model, RequestedModel: u.RequestedModel,
		UpstreamModel: u.UpstreamModel, UpstreamResponseModel: u.UpstreamResponseModel, UpstreamModelMismatch: u.UpstreamModelMismatch,
		ChannelID: u.ChannelID, ModelMappingChain: u.ModelMappingChain, BillingTier: u.BillingTier, BillingMode: u.BillingMode,
		ServiceTier: u.ServiceTier, ReasoningEffort: u.ReasoningEffort, RequestedReasoningEffort: u.RequestedReasoningEffort,
		InboundEndpoint: u.InboundEndpoint, UpstreamEndpoint: u.UpstreamEndpoint, GroupID: u.GroupID, SubscriptionID: u.SubscriptionID,
		InputTokens: u.InputTokens, OutputTokens: u.OutputTokens, CacheCreationTokens: u.CacheCreationTokens, CacheReadTokens: u.CacheReadTokens,
		CacheCreation5mTokens: u.CacheCreation5mTokens, CacheCreation1hTokens: u.CacheCreation1hTokens, ImageInputTokens: u.ImageInputTokens, ImageOutputTokens: u.ImageOutputTokens,
		InputCost: u.InputCost, OutputCost: u.OutputCost, CacheCreationCost: u.CacheCreationCost, CacheReadCost: u.CacheReadCost,
		ImageInputCost: u.ImageInputCost, ImageOutputCost: u.ImageOutputCost, TotalCost: u.TotalCost, ActualCost: u.ActualCost,
		RateMultiplier: u.RateMultiplier, AccountRateMultiplier: u.AccountRateMultiplier, AccountStatsCost: u.AccountStatsCost,
		LongContextBillingApplied: u.LongContextBillingApplied, BillingType: u.BillingType, RequestType: u.RequestType, Stream: u.Stream,
		OpenAIWSMode: u.OpenAIWSMode, NativeCompactionV2: u.NativeCompactionV2, DurationMs: u.DurationMs, FirstTokenMs: u.FirstTokenMs,
		CacheTTLOverridden: u.CacheTTLOverridden, ImageCount: u.ImageCount, ImageSize: u.ImageSize, ImageInputSize: u.ImageInputSize,
		ImageOutputSize: u.ImageOutputSize, ImageSizeSource: u.ImageSizeSource, ImageSizeBreakdown: u.ImageSizeBreakdown, MediaType: u.MediaType,
		VideoCount: u.VideoCount, VideoResolution: u.VideoResolution, VideoDurationSeconds: u.VideoDurationSeconds,
	}
}

func usageFromSettlementWire(u usageSettlementUsageWire) *UsageLog {
	return &UsageLog{
		UserID: u.UserID, APIKeyID: u.APIKeyID, AccountID: u.AccountID, RequestID: u.RequestID, Model: u.Model, RequestedModel: u.RequestedModel,
		UpstreamModel: u.UpstreamModel, UpstreamResponseModel: u.UpstreamResponseModel, UpstreamModelMismatch: u.UpstreamModelMismatch,
		ChannelID: u.ChannelID, ModelMappingChain: u.ModelMappingChain, BillingTier: u.BillingTier, BillingMode: u.BillingMode,
		ServiceTier: u.ServiceTier, ReasoningEffort: u.ReasoningEffort, RequestedReasoningEffort: u.RequestedReasoningEffort,
		InboundEndpoint: u.InboundEndpoint, UpstreamEndpoint: u.UpstreamEndpoint, GroupID: u.GroupID, SubscriptionID: u.SubscriptionID,
		InputTokens: u.InputTokens, OutputTokens: u.OutputTokens, CacheCreationTokens: u.CacheCreationTokens, CacheReadTokens: u.CacheReadTokens,
		CacheCreation5mTokens: u.CacheCreation5mTokens, CacheCreation1hTokens: u.CacheCreation1hTokens, ImageInputTokens: u.ImageInputTokens, ImageOutputTokens: u.ImageOutputTokens,
		InputCost: u.InputCost, OutputCost: u.OutputCost, CacheCreationCost: u.CacheCreationCost, CacheReadCost: u.CacheReadCost,
		ImageInputCost: u.ImageInputCost, ImageOutputCost: u.ImageOutputCost, TotalCost: u.TotalCost, ActualCost: u.ActualCost,
		RateMultiplier: u.RateMultiplier, AccountRateMultiplier: u.AccountRateMultiplier, AccountStatsCost: u.AccountStatsCost,
		LongContextBillingApplied: u.LongContextBillingApplied, BillingType: u.BillingType, RequestType: u.RequestType, Stream: u.Stream,
		OpenAIWSMode: u.OpenAIWSMode, NativeCompactionV2: u.NativeCompactionV2, DurationMs: u.DurationMs, FirstTokenMs: u.FirstTokenMs,
		CacheTTLOverridden: u.CacheTTLOverridden, ImageCount: u.ImageCount, ImageSize: u.ImageSize, ImageInputSize: u.ImageInputSize,
		ImageOutputSize: u.ImageOutputSize, ImageSizeSource: u.ImageSizeSource, ImageSizeBreakdown: u.ImageSizeBreakdown, MediaType: u.MediaType,
		VideoCount: u.VideoCount, VideoResolution: u.VideoResolution, VideoDurationSeconds: u.VideoDurationSeconds,
	}
}

func validateUsageSettlementPayload(p *usageSettlementPayload) error {
	if p == nil || (p.Version != 1 && p.Version != 2) || !clientIdentifier.MatchString(p.InvocationID) || p.RequestID != "client:"+p.InvocationID || len(p.RequestID) > 64 || p.APIKeyID <= 0 {
		return ErrUsageSettlementPayload
	}
	if p.Version == 1 && p.PlatformQuota != nil {
		return ErrUsageSettlementPayload
	}
	if p.Version == 2 && (p.PlatformQuota == nil || !IsAllowedQuotaPlatform(p.PlatformQuota.Platform) ||
		!settlementFiniteAmount(p.PlatformQuota.CostUSD, 1e10) || p.PlatformQuota.CostUSD != quantizeUsageSettlementPlatformCost(p.Usage.ActualCost)) {
		return ErrUsageSettlementPayload
	}
	c, u := &p.Command, &p.Usage
	if c.RequestID != p.RequestID || u.RequestID != p.RequestID || c.APIKeyID != p.APIKeyID || u.APIKeyID != p.APIKeyID ||
		c.UserID <= 0 || c.AccountID <= 0 || c.UserID != u.UserID || c.AccountID != u.AccountID ||
		!settlementHexFingerprint(c.RequestFingerprint, false) || !settlementHexFingerprint(c.RequestPayloadHash, true) ||
		!settlementText(c.AccountType, 32, true) || !settlementText(c.Model, 100, false) || c.Model != u.Model ||
		!settlementText(u.RequestedModel, 100, false) || !settlementText(c.ServiceTier, 16, true) || !settlementText(c.ReasoningEffort, 20, true) ||
		c.ServiceTier != optionalStringValue(u.ServiceTier) || c.ReasoningEffort != optionalStringValue(u.ReasoningEffort) ||
		c.BillingType != u.BillingType || (c.BillingType != BillingTypeBalance && c.BillingType != BillingTypeSubscription) ||
		!usageSettlementInt64Equal(c.SubscriptionID, u.SubscriptionID) ||
		c.InputTokens != u.InputTokens || c.OutputTokens != u.OutputTokens || c.CacheCreationTokens != u.CacheCreationTokens || c.CacheReadTokens != u.CacheReadTokens || c.ImageCount != u.ImageCount ||
		!u.RequestType.IsValid() || u.RequestType == RequestTypeUnknown {
		return ErrUsageSettlementPayload
	}
	for _, id := range []*int64{u.GroupID, u.SubscriptionID, u.ChannelID} {
		if id != nil && *id <= 0 {
			return ErrUsageSettlementPayload
		}
	}
	for _, tokens := range []int{u.InputTokens, u.OutputTokens, u.CacheCreationTokens, u.CacheReadTokens, u.CacheCreation5mTokens, u.CacheCreation1hTokens, u.ImageInputTokens, u.ImageOutputTokens, u.ImageCount, u.VideoCount} {
		if tokens < 0 || int64(tokens) > math.MaxInt32 {
			return ErrUsageSettlementPayload
		}
	}
	for _, value := range []*int{u.DurationMs, u.FirstTokenMs, u.VideoDurationSeconds} {
		if value != nil && (*value < 0 || int64(*value) > math.MaxInt32) {
			return ErrUsageSettlementPayload
		}
	}
	for _, field := range []struct {
		value *string
		limit int
	}{
		{u.UpstreamModel, 100}, {u.UpstreamResponseModel, 200}, {u.BillingTier, 50}, {u.BillingMode, 20},
		{u.ServiceTier, 16}, {u.ReasoningEffort, 20}, {u.RequestedReasoningEffort, 20}, {u.ImageSize, 10},
		{u.ImageInputSize, 32}, {u.ImageOutputSize, 32}, {u.ImageSizeSource, 16}, {u.MediaType, 16}, {u.VideoResolution, 10},
		{u.ModelMappingChain, 500},
	} {
		if field.value != nil && !settlementText(*field.value, field.limit, true) {
			return ErrUsageSettlementPayload
		}
	}
	for _, value := range []*string{u.InboundEndpoint, u.UpstreamEndpoint} {
		if value != nil && (!settlementText(*value, 128, true) || (*value != "" && (!strings.HasPrefix(*value, "/") || strings.ContainsAny(*value, "?#@\\ ")))) {
			return ErrUsageSettlementPayload
		}
	}
	if !settlementText(c.MediaType, 16, true) || c.MediaType != optionalStringValue(u.MediaType) || len(u.ImageSizeBreakdown) > 64 {
		return ErrUsageSettlementPayload
	}
	for key, count := range u.ImageSizeBreakdown {
		if !settlementText(key, 64, false) || count < 0 || int64(count) > math.MaxInt32 {
			return ErrUsageSettlementPayload
		}
	}
	for _, cost := range []float64{u.InputCost, u.OutputCost, u.CacheCreationCost, u.CacheReadCost, u.ImageInputCost, u.ImageOutputCost, u.TotalCost, u.ActualCost} {
		if !settlementFiniteAmount(cost, 1e10) {
			return ErrUsageSettlementPayload
		}
	}
	if !settlementFiniteAmount(u.RateMultiplier, 1e6) || (u.AccountRateMultiplier != nil && !settlementFiniteAmount(*u.AccountRateMultiplier, 1e6)) ||
		(u.AccountStatsCost != nil && !settlementFiniteAmount(*u.AccountStatsCost, 1e10)) {
		return ErrUsageSettlementPayload
	}
	for _, cost := range []float64{c.BalanceCost, c.SubscriptionCost, c.APIKeyQuotaCost, c.APIKeyRateLimitCost, c.AccountQuotaCost} {
		if !settlementFiniteAmount(cost, 1e12) || cost != QuantizeUsageBillingAmount(cost) {
			return ErrUsageSettlementPayload
		}
	}
	actual := QuantizeUsageBillingAmount(u.ActualCost)
	if c.BillingType == BillingTypeBalance {
		if c.BalanceCost != actual || c.SubscriptionCost != 0 {
			return ErrUsageSettlementPayload
		}
	} else if c.SubscriptionID == nil || c.BalanceCost != 0 || (u.TotalCost > 0 && c.SubscriptionCost != actual) || (u.TotalCost == 0 && c.SubscriptionCost != 0) {
		return ErrUsageSettlementPayload
	}
	if (c.APIKeyQuotaCost != 0 && c.APIKeyQuotaCost != actual) || (c.APIKeyRateLimitCost != 0 && c.APIKeyRateLimitCost != actual) {
		return ErrUsageSettlementPayload
	}
	accountMultiplier := 1.0
	if u.AccountRateMultiplier != nil {
		accountMultiplier = *u.AccountRateMultiplier
	}
	if c.AccountQuotaCost != 0 && c.AccountQuotaCost != QuantizeUsageBillingAmount(u.TotalCost*accountMultiplier) {
		return ErrUsageSettlementPayload
	}
	stream, ws := ApplyLegacyRequestFields(u.RequestType, u.Stream, u.OpenAIWSMode)
	if stream != u.Stream || ws != u.OpenAIWSMode {
		return ErrUsageSettlementPayload
	}
	return nil
}

func settlementHexFingerprint(value string, optional bool) bool {
	if value == "" {
		return optional
	}
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func settlementText(value string, limit int, optional bool) bool {
	if !utf8.ValidString(value) || len(value) > limit || strings.TrimSpace(value) != value || (!optional && value == "") {
		return false
	}
	return strings.IndexFunc(value, unicode.IsControl) < 0
}

func settlementFiniteAmount(value, maximum float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value < maximum
}

func quantizeUsageSettlementPlatformCost(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return value
	}
	amount, _ := decimal.NewFromFloat(value).Round(10).Float64()
	return amount
}
