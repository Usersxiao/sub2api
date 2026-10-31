package repository

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

const quotaIncrementAttempts = 3

// A response may be lost after Redis committed the cost. Disable transport
// retries for this command while retaining the shared client/pool and hooks.
type quotaIncrementCommand struct{ *redis.Cmd }

func (*quotaIncrementCommand) NoRetry() bool { return true }

func (cmd *quotaIncrementCommand) Clone() redis.Cmder {
	return &quotaIncrementCommand{Cmd: cmd.Cmd.Clone().(*redis.Cmd)}
}

var _ redis.Cmder = (*quotaIncrementCommand)(nil)

var quotaIncrementObservedFields = [...]string{
	"cache_instance", "version", "quota_row_id", "control_generation", "schema_version",
	"daily_usage", "weekly_usage", "monthly_usage", "daily_limit", "weekly_limit", "monthly_limit",
	"daily_window_start", "weekly_window_start", "monthly_window_start",
}

// This script only compares strings and publishes already validated values.
// Check predictable command errors before the single HSET: Redis Lua does not
// roll back earlier commands when a later command fails. Network/OOM outcomes
// remain separate from this conditional update's normal result statuses.
const incrementUserPlatformQuotaUsageScript = `
local kind = redis.call("TYPE", KEYS[1]).ok
if kind == "none" then return 2 end
if kind ~= "hash" then return redis.error_reply("invalid quota cache key type") end
if ARGV[37] == "1" then
    local dirtykind = redis.call("TYPE", KEYS[2]).ok
    if dirtykind ~= "none" and dirtykind ~= "set" then
        return redis.error_reply("invalid quota dirty key type")
    end
end
local fields = {"cache_instance", "version", "quota_row_id", "control_generation", "schema_version",
    "daily_usage", "weekly_usage", "monthly_usage", "daily_limit", "weekly_limit", "monthly_limit",
    "daily_window_start", "weekly_window_start", "monthly_window_start"}
for i, field in ipairs(fields) do
    local current = redis.call("HGET", KEYS[1], field)
    local present = ARGV[2*i-1] == "1"
    if current == false then
        if present then return 3 end
    elseif not present or current ~= ARGV[2*i] then
        return 3
    end
end
redis.call("HSET", KEYS[1],
    "daily_usage", ARGV[29], "weekly_usage", ARGV[30], "monthly_usage", ARGV[31],
    "daily_window_start", ARGV[32], "weekly_window_start", ARGV[33], "monthly_window_start", ARGV[34],
    "version", ARGV[35])
redis.call("EXPIRE", KEYS[1], ARGV[36])
if ARGV[37] == "1" then
    redis.call("SADD", KEYS[2], ARGV[38])
    redis.call("EXPIRE", KEYS[2], ARGV[39])
end
return 1
`

func quotaIncrementResult(status service.UserPlatformQuotaIncrementStatus) service.UserPlatformQuotaBoundIncrementResult {
	return service.UserPlatformQuotaBoundIncrementResult{Status: status}
}

func (c *billingCache) IncrUserPlatformQuotaUsageCache(ctx context.Context, userID int64, platform string, cost float64, ttl time.Duration, markDirty bool) (service.UserPlatformQuotaIncrementResult, error) {
	return c.incrementUserPlatformQuotaUsageCacheAt(ctx, userID, platform, cost, ttl, markDirty, time.Now, userPlatformQuotaDirtySetKey())
}

// The clock is read on every retry so a contended update cannot carry a window
// decision across a day/week/720-hour boundary. The dirty key parameter permits
// isolated real-Redis tests without touching another test's shared dirty set.
func (c *billingCache) incrementUserPlatformQuotaUsageCacheAt(ctx context.Context, userID int64, platform string, cost float64, ttl time.Duration, markDirty bool, clock func() time.Time, dirtyKey string) (service.UserPlatformQuotaIncrementResult, error) {
	result, err := c.incrementPlatformQuotaUsageCache(ctx, userID, platform, cost, ttl, markDirty, clock, dirtyKey, nil)
	return service.UserPlatformQuotaIncrementResult{Status: result.Status}, err
}

var _ service.UserPlatformQuotaBoundUsageCache = (*billingCache)(nil)

func (c *billingCache) IncrBoundUserPlatformQuotaUsageCache(ctx context.Context, binding service.UserPlatformQuotaUsageBinding, ttl time.Duration, markDirty bool) (service.UserPlatformQuotaBoundIncrementResult, error) {
	return c.incrementBoundUserPlatformQuotaUsageCache(ctx, binding, ttl, markDirty, userPlatformQuotaDirtySetKey())
}

func (c *billingCache) incrementBoundUserPlatformQuotaUsageCache(ctx context.Context, binding service.UserPlatformQuotaUsageBinding, ttl time.Duration, markDirty bool, dirtyKey string) (service.UserPlatformQuotaBoundIncrementResult, error) {
	if err := c.quotaCacheAccessError(); err != nil {
		return service.UserPlatformQuotaBoundIncrementResult{}, err
	}
	if err := service.ValidateUserPlatformQuotaUsageBinding(binding); err != nil {
		return service.UserPlatformQuotaBoundIncrementResult{}, err
	}
	return c.incrementPlatformQuotaUsageCache(ctx, binding.UserID, binding.Platform, binding.CostUSD, ttl, markDirty,
		func() time.Time { return binding.CapturedAt }, dirtyKey, &binding)
}

// A bound mutation uses one immutable Apply clock across all conditional retries.
// It skips each represented window already ahead of that event instead of rolling
// it back. Schema v1 stores Unix-second anchors: this does not establish
// microsecond event ordering, including within a newly rotated monthly second.
func (c *billingCache) incrementPlatformQuotaUsageCache(ctx context.Context, userID int64, platform string, cost float64, ttl time.Duration, markDirty bool, clock func() time.Time, dirtyKey string, binding *service.UserPlatformQuotaUsageBinding) (service.UserPlatformQuotaBoundIncrementResult, error) {
	var zero service.UserPlatformQuotaBoundIncrementResult
	if err := c.quotaCacheAccessError(); err != nil {
		return zero, err
	}
	if userID <= 0 || platform == "" || cost <= 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return zero, invalidUserPlatformQuotaCache("invalid increment input")
	}
	ttlSeconds, err := userPlatformQuotaCacheTTL(ttl)
	if err != nil {
		return zero, err
	}
	if clock == nil || markDirty && dirtyKey == "" {
		return zero, invalidUserPlatformQuotaCache("invalid increment clock/dirty key")
	}
	costDecimal, err := quotaIncrementDecimal(strconv.FormatFloat(cost, 'f', -1, 64), cost)
	if err != nil {
		return zero, err
	}
	key := userPlatformQuotaCacheKey(userID, platform)
	for attempt := 0; attempt < quotaIncrementAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		raw, err := c.rdb.HGetAll(ctx, key).Result()
		if err != nil {
			return zero, err
		}
		if len(raw) == 0 {
			return quotaIncrementResult(service.QuotaIncrementMissing), nil
		}
		// Well-formed older/unknown schemas are an explicit non-application.
		// A malformed schema value remains a detectable corruption error.
		if schema, exists := raw["schema_version"]; !exists {
			return quotaIncrementResult(service.QuotaIncrementIncompatible), nil
		} else if schema != strconv.FormatInt(service.UserPlatformQuotaCacheSchemaV1, 10) {
			value, parseErr := strconv.ParseInt(schema, 10, 64)
			if parseErr != nil || value < 0 || strconv.FormatInt(value, 10) != schema {
				return zero, invalidUserPlatformQuotaCache("schema_version")
			}
			return quotaIncrementResult(service.QuotaIncrementIncompatible), nil
		}
		entry, err := parseUserPlatformQuotaHash(raw)
		if err != nil {
			return zero, err
		}
		if entry == nil || entry.QuotaRowID <= 0 || entry.ControlGeneration <= 0 || !validUserPlatformQuotaCacheInstance(entry.CacheInstance) ||
			entry.DailyWindowStart == nil || entry.WeeklyWindowStart == nil || entry.MonthlyWindowStart == nil {
			return quotaIncrementResult(service.QuotaIncrementIncompatible), nil
		}
		if binding != nil && (entry.QuotaRowID != binding.QuotaRowID || entry.ControlGeneration != binding.ControlGeneration) {
			return quotaIncrementResult(service.QuotaIncrementSuperseded), nil
		}
		if entry.Version == math.MaxInt64 {
			return zero, invalidUserPlatformQuotaCache("increment revision exhausted")
		}
		now := clock()
		if now.Unix() < 0 || now.Unix() > 253402300799 {
			return zero, invalidUserPlatformQuotaCache("increment clock")
		}
		day, week := timezone.StartOfDay(now), timezone.StartOfWeek(now)
		starts := [3]time.Time{*entry.DailyWindowStart, *entry.WeeklyWindowStart, *entry.MonthlyWindowStart}
		observed := [3]float64{entry.DailyUsageUSD, entry.WeeklyUsageUSD, entry.MonthlyUsageUSD}
		usageFields := [3]string{"daily_usage", "weekly_usage", "monthly_usage"}
		var usage [3]string
		var appliedWindows uint8
		for i, field := range usageFields {
			base, err := quotaIncrementDecimal(raw[field], observed[i])
			if err != nil {
				return zero, err
			}
			if starts[i].After(now) {
				if binding == nil {
					return zero, invalidUserPlatformQuotaCache("future increment window")
				}
				usage[i] = raw[field]
				continue
			}
			rotated := i == 0 && starts[i].Before(day) || i == 1 && starts[i].Before(week) || i == 2 && now.Sub(starts[i]) >= 720*time.Hour
			if rotated {
				base, observed[i] = decimal.Zero, 0
				switch i {
				case 0:
					starts[i] = day
				case 1:
					starts[i] = week
				case 2:
					starts[i] = now.UTC().Truncate(time.Second)
				}
			}
			sum := base.Add(costDecimal)
			value, _ := sum.Float64()
			if math.IsNaN(value) || math.IsInf(value, 0) || value <= observed[i] {
				return zero, invalidUserPlatformQuotaCache("increment not representable as a larger finite observation")
			}
			usage[i] = sum.String()
			appliedWindows |= 1 << i
		}
		if appliedWindows == 0 {
			// A read-only obsolete observation must not renew TTL, dirty state or revision.
			return quotaIncrementResult(service.QuotaIncrementObsolete), nil
		}
		args := make([]any, 0, 39)
		for _, field := range quotaIncrementObservedFields {
			value, exists := raw[field]
			present := "0"
			if exists {
				present = "1"
			}
			args = append(args, present, value)
		}
		dirty := "0"
		if markDirty {
			dirty = "1"
		}
		args = append(args, usage[0], usage[1], usage[2], strconv.FormatInt(starts[0].Unix(), 10), strconv.FormatInt(starts[1].Unix(), 10), strconv.FormatInt(starts[2].Unix(), 10),
			strconv.FormatInt(entry.Version+1, 10), strconv.FormatInt(ttlSeconds, 10), dirty, userPlatformQuotaDirtyMember(userID, platform), strconv.Itoa(userPlatformQuotaDirtyTTLSeconds))
		commandArgs := []any{"eval", incrementUserPlatformQuotaUsageScript, 2, key, dirtyKey}
		commandArgs = append(commandArgs, args...)
		command := &quotaIncrementCommand{Cmd: redis.NewCmd(ctx, commandArgs...)}
		command.SetFirstKeyPos(3)
		if err := c.rdb.Process(ctx, command); err != nil {
			return zero, err
		}
		result, err := command.Int64()
		if err != nil {
			return zero, err
		}
		switch result {
		case 1:
			status := service.QuotaIncrementApplied
			if appliedWindows != service.QuotaWindowAll {
				status = service.QuotaIncrementPartial
			}
			return service.UserPlatformQuotaBoundIncrementResult{Status: status, AppliedWindows: appliedWindows}, nil
		case 2:
			return quotaIncrementResult(service.QuotaIncrementMissing), nil
		case 3:
			// Reread and recompute, including the current clock, at most three times.
		default:
			return zero, invalidUserPlatformQuotaCache("unknown increment result")
		}
	}
	return quotaIncrementResult(service.QuotaIncrementContended), nil
}

func quotaIncrementDecimal(raw string, observed float64) (decimal.Decimal, error) {
	if len(raw) == 0 || len(raw) > 2048 {
		return decimal.Zero, invalidUserPlatformQuotaCache("increment decimal length")
	}
	value, err := decimal.NewFromString(raw)
	if err != nil || value.Exponent() < -1074 || value.Exponent() > 308 || value.IsNegative() {
		return decimal.Zero, invalidUserPlatformQuotaCache("increment decimal exponent/value")
	}
	if observed == 0 && !value.IsZero() {
		return decimal.Zero, invalidUserPlatformQuotaCache("increment decimal underflow")
	}
	if math.IsNaN(observed) || math.IsInf(observed, 0) || observed < 0 {
		return decimal.Zero, fmt.Errorf("%w: increment decimal observation", errUserPlatformQuotaCacheInvalid)
	}
	return value, nil
}
