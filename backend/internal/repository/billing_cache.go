package repository

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	billingBalanceKeyPrefix   = "billing:balance:"
	billingSubKeyPrefix       = "billing:sub:"
	billingRateLimitKeyPrefix = "apikey:rate:"
	subCacheInvalidateChannel = "subscription:cache:invalidate"
	billingCacheTTL           = 5 * time.Minute
	billingCacheJitter        = 30 * time.Second
	rateLimitCacheTTL         = 7 * 24 * time.Hour // 7 days matches the longest window

	// Rate limit window durations — must match service.RateLimitWindow* constants.
	rateLimitWindow5h = 5 * time.Hour
	rateLimitWindow1d = 24 * time.Hour
	rateLimitWindow7d = 7 * 24 * time.Hour
)

// jitteredTTL 返回带随机抖动的 TTL，防止缓存雪崩
func jitteredTTL() time.Duration {
	// 只做“减法抖动”，确保实际 TTL 不会超过 billingCacheTTL（避免上界预期被打破）。
	if billingCacheJitter <= 0 {
		return billingCacheTTL
	}
	jitter := time.Duration(rand.IntN(int(billingCacheJitter)))
	return billingCacheTTL - jitter
}

// billingBalanceKey generates the Redis key for user balance cache.
func billingBalanceKey(userID int64) string {
	return fmt.Sprintf("%s%d", billingBalanceKeyPrefix, userID)
}

// billingSubKey generates the Redis key for subscription cache.
func billingSubKey(userID, groupID int64) string {
	return fmt.Sprintf("%s%d:%d", billingSubKeyPrefix, userID, groupID)
}

const (
	subFieldStatus       = "status"
	subFieldExpiresAt    = "expires_at"
	subFieldDailyUsage   = "daily_usage"
	subFieldWeeklyUsage  = "weekly_usage"
	subFieldMonthlyUsage = "monthly_usage"
	subFieldVersion      = "version"
)

// billingRateLimitKey generates the Redis key for API key rate limit cache.
func billingRateLimitKey(keyID int64) string {
	return fmt.Sprintf("%s%d", billingRateLimitKeyPrefix, keyID)
}

const (
	rateLimitFieldUsage5h  = "usage_5h"
	rateLimitFieldUsage1d  = "usage_1d"
	rateLimitFieldUsage7d  = "usage_7d"
	rateLimitFieldWindow5h = "window_5h"
	rateLimitFieldWindow1d = "window_1d"
	rateLimitFieldWindow7d = "window_7d"
)

var (
	deductBalanceScript = redis.NewScript(`
		local current = redis.call('GET', KEYS[1])
		if current == false then
			return 0
		end
		local newVal = tonumber(current) - tonumber(ARGV[1])
		redis.call('SET', KEYS[1], newVal)
		redis.call('EXPIRE', KEYS[1], ARGV[2])
		return 1
	`)

	updateSubUsageScript = redis.NewScript(`
		local exists = redis.call('EXISTS', KEYS[1])
		if exists == 0 then
			return 0
		end
		local cost = tonumber(ARGV[1])
		redis.call('HINCRBYFLOAT', KEYS[1], 'daily_usage', cost)
		redis.call('HINCRBYFLOAT', KEYS[1], 'weekly_usage', cost)
		redis.call('HINCRBYFLOAT', KEYS[1], 'monthly_usage', cost)
		redis.call('EXPIRE', KEYS[1], ARGV[2])
		return 1
	`)

	// updateRateLimitUsageScript atomically increments all three rate limit usage counters
	// with window expiration checking. If a window has expired, its usage is reset to cost
	// (instead of accumulated) and the window timestamp is updated, matching the DB-side
	// IncrementRateLimitUsage semantics.
	//
	// ARGV: [1]=cost, [2]=ttl_seconds, [3]=now_unix, [4]=window_5h_seconds, [5]=window_1d_seconds, [6]=window_7d_seconds
	updateRateLimitUsageScript = redis.NewScript(`
		local exists = redis.call('EXISTS', KEYS[1])
		if exists == 0 then
			return 0
		end
		local cost = tonumber(ARGV[1])
		local now = tonumber(ARGV[3])
		local win5h = tonumber(ARGV[4])
		local win1d = tonumber(ARGV[5])
		local win7d = tonumber(ARGV[6])

		-- Helper: check if window is expired and update usage + window accordingly
		-- Returns nothing, modifies the hash in-place.
		local function update_window(usage_field, window_field, window_duration)
			local w = tonumber(redis.call('HGET', KEYS[1], window_field) or 0)
			if w == 0 or (now - w) >= window_duration then
				-- Window expired or never started: reset usage to cost, start new window
				redis.call('HSET', KEYS[1], usage_field, tostring(cost))
				redis.call('HSET', KEYS[1], window_field, tostring(now))
			else
				-- Window still valid: accumulate
				redis.call('HINCRBYFLOAT', KEYS[1], usage_field, cost)
			end
		end

		update_window('usage_5h', 'window_5h', win5h)
		update_window('usage_1d', 'window_1d', win1d)
		update_window('usage_7d', 'window_7d', win7d)
		redis.call('EXPIRE', KEYS[1], ARGV[2])
		return 1
	`)
)

type billingCache struct {
	rdb         *redis.Client
	quotaDomain *service.UserPlatformQuotaCacheDomain
}

// Legacy construction is retained for compatibility fixtures. Production Wire
// uses ProvideBillingCache and never adopts legacy quota hashes or dirty state.
func NewBillingCache(rdb *redis.Client) service.BillingCache {
	return &billingCache{rdb: rdb}
}

// No persisted domain has completed a baseline handoff yet. Keep every legacy
// quota port closed before any Redis command, including empty batches/dirty
// sets. Redis contents, a cache miss and the flusher flag cannot grant access.
func (c *billingCache) quotaCacheAccessError() error {
	if c == nil {
		return service.ErrUserPlatformQuotaWriterUnavailable
	}
	if c.quotaDomain != nil {
		return service.ErrUserPlatformQuotaCacheHandoffRequired
	}
	return nil
}

func (c *billingCache) GetUserBalance(ctx context.Context, userID int64) (float64, error) {
	key := billingBalanceKey(userID)
	val, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(val, 64)
}

func (c *billingCache) SetUserBalance(ctx context.Context, userID int64, balance float64) error {
	key := billingBalanceKey(userID)
	return c.rdb.Set(ctx, key, balance, jitteredTTL()).Err()
}

func (c *billingCache) DeductUserBalance(ctx context.Context, userID int64, amount float64) error {
	key := billingBalanceKey(userID)
	_, err := deductBalanceScript.Run(ctx, c.rdb, []string{key}, amount, int(jitteredTTL().Seconds())).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("Warning: deduct balance cache failed for user %d: %v", userID, err)
		return err
	}
	return nil
}

func (c *billingCache) InvalidateUserBalance(ctx context.Context, userID int64) error {
	key := billingBalanceKey(userID)
	return c.rdb.Del(ctx, key).Err()
}

func (c *billingCache) GetSubscriptionCache(ctx context.Context, userID, groupID int64) (*service.SubscriptionCacheData, error) {
	key := billingSubKey(userID, groupID)
	result, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, redis.Nil
	}
	return c.parseSubscriptionCache(result)
}

func (c *billingCache) parseSubscriptionCache(data map[string]string) (*service.SubscriptionCacheData, error) {
	result := &service.SubscriptionCacheData{}

	result.Status = data[subFieldStatus]
	if result.Status == "" {
		return nil, errors.New("invalid cache: missing status")
	}

	if expiresStr, ok := data[subFieldExpiresAt]; ok {
		expiresAt, err := strconv.ParseInt(expiresStr, 10, 64)
		if err == nil {
			result.ExpiresAt = time.Unix(expiresAt, 0)
		}
	}

	if dailyStr, ok := data[subFieldDailyUsage]; ok {
		result.DailyUsage, _ = strconv.ParseFloat(dailyStr, 64)
	}

	if weeklyStr, ok := data[subFieldWeeklyUsage]; ok {
		result.WeeklyUsage, _ = strconv.ParseFloat(weeklyStr, 64)
	}

	if monthlyStr, ok := data[subFieldMonthlyUsage]; ok {
		result.MonthlyUsage, _ = strconv.ParseFloat(monthlyStr, 64)
	}

	if versionStr, ok := data[subFieldVersion]; ok {
		result.Version, _ = strconv.ParseInt(versionStr, 10, 64)
	}

	return result, nil
}

func (c *billingCache) SetSubscriptionCache(ctx context.Context, userID, groupID int64, data *service.SubscriptionCacheData) error {
	if data == nil {
		return nil
	}

	key := billingSubKey(userID, groupID)

	fields := map[string]any{
		subFieldStatus:       data.Status,
		subFieldExpiresAt:    data.ExpiresAt.Unix(),
		subFieldDailyUsage:   data.DailyUsage,
		subFieldWeeklyUsage:  data.WeeklyUsage,
		subFieldMonthlyUsage: data.MonthlyUsage,
		subFieldVersion:      data.Version,
	}

	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, jitteredTTL())
	_, err := pipe.Exec(ctx)
	return err
}

func (c *billingCache) UpdateSubscriptionUsage(ctx context.Context, userID, groupID int64, cost float64) error {
	key := billingSubKey(userID, groupID)
	_, err := updateSubUsageScript.Run(ctx, c.rdb, []string{key}, cost, int(jitteredTTL().Seconds())).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("Warning: update subscription usage cache failed for user %d group %d: %v", userID, groupID, err)
		return err
	}
	return nil
}

func (c *billingCache) InvalidateSubscriptionCache(ctx context.Context, userID, groupID int64) error {
	key := billingSubKey(userID, groupID)
	return c.rdb.Del(ctx, key).Err()
}

func (c *billingCache) PublishSubscriptionCacheInvalidation(ctx context.Context, cacheKey string) error {
	return c.rdb.Publish(ctx, subCacheInvalidateChannel, cacheKey).Err()
}

func (c *billingCache) SubscribeSubscriptionCacheInvalidation(ctx context.Context, handler func(cacheKey string)) error {
	pubsub := c.rdb.Subscribe(ctx, subCacheInvalidateChannel)

	_, err := pubsub.Receive(ctx)
	if err != nil {
		_ = pubsub.Close()
		return fmt.Errorf("subscribe to subscription cache invalidation: %w", err)
	}

	go func() {
		defer func() {
			if err := pubsub.Close(); err != nil {
				log.Printf("Warning: failed to close subscription cache invalidation pubsub: %v", err)
			}
		}()

		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				if msg != nil {
					handler(msg.Payload)
				}
			}
		}
	}()

	return nil
}

func (c *billingCache) GetAPIKeyRateLimit(ctx context.Context, keyID int64) (*service.APIKeyRateLimitCacheData, error) {
	key := billingRateLimitKey(keyID)
	result, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, redis.Nil
	}
	data := &service.APIKeyRateLimitCacheData{}
	if v, ok := result[rateLimitFieldUsage5h]; ok {
		data.Usage5h, _ = strconv.ParseFloat(v, 64)
	}
	if v, ok := result[rateLimitFieldUsage1d]; ok {
		data.Usage1d, _ = strconv.ParseFloat(v, 64)
	}
	if v, ok := result[rateLimitFieldUsage7d]; ok {
		data.Usage7d, _ = strconv.ParseFloat(v, 64)
	}
	if v, ok := result[rateLimitFieldWindow5h]; ok {
		data.Window5h, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := result[rateLimitFieldWindow1d]; ok {
		data.Window1d, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := result[rateLimitFieldWindow7d]; ok {
		data.Window7d, _ = strconv.ParseInt(v, 10, 64)
	}
	return data, nil
}

func (c *billingCache) SetAPIKeyRateLimit(ctx context.Context, keyID int64, data *service.APIKeyRateLimitCacheData) error {
	if data == nil {
		return nil
	}
	key := billingRateLimitKey(keyID)
	fields := map[string]any{
		rateLimitFieldUsage5h:  data.Usage5h,
		rateLimitFieldUsage1d:  data.Usage1d,
		rateLimitFieldUsage7d:  data.Usage7d,
		rateLimitFieldWindow5h: data.Window5h,
		rateLimitFieldWindow1d: data.Window1d,
		rateLimitFieldWindow7d: data.Window7d,
	}
	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, rateLimitCacheTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *billingCache) UpdateAPIKeyRateLimitUsage(ctx context.Context, keyID int64, cost float64) error {
	key := billingRateLimitKey(keyID)
	now := time.Now().Unix()
	_, err := updateRateLimitUsageScript.Run(ctx, c.rdb, []string{key},
		cost,
		int(rateLimitCacheTTL.Seconds()),
		now,
		int(rateLimitWindow5h.Seconds()),
		int(rateLimitWindow1d.Seconds()),
		int(rateLimitWindow7d.Seconds()),
	).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("Warning: update rate limit usage cache failed for api key %d: %v", keyID, err)
		return err
	}
	return nil
}

func (c *billingCache) InvalidateAPIKeyRateLimit(ctx context.Context, keyID int64) error {
	key := billingRateLimitKey(keyID)
	return c.rdb.Del(ctx, key).Err()
}

// ============================================
// user × platform quota 缓存
// ============================================

// userPlatformQuotaCacheKey 构造 Redis key
func userPlatformQuotaCacheKey(userID int64, platform string) string {
	return fmt.Sprintf("billing:user_platform_quota:%d:%s", userID, platform)
}

var errUserPlatformQuotaCacheInvalid = errors.New("invalid user platform quota cache")

func invalidUserPlatformQuotaCache(field string) error {
	return fmt.Errorf("%w: %s", errUserPlatformQuotaCacheInvalid, field)
}

func validUserPlatformQuotaCacheInstance(instance string) bool {
	if len(instance) != 64 {
		return false
	}
	for _, c := range instance {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validateUserPlatformQuotaCacheEntry(entry *service.UserPlatformQuotaCacheEntry, complete bool) error {
	if entry == nil {
		return invalidUserPlatformQuotaCache("nil entry")
	}
	if entry.SchemaVersion != 0 && entry.SchemaVersion != service.UserPlatformQuotaCacheSchemaV1 {
		return invalidUserPlatformQuotaCache("schema_version")
	}
	if entry.Version < 0 {
		return invalidUserPlatformQuotaCache("version")
	}
	for _, value := range []float64{entry.DailyUsageUSD, entry.WeeklyUsageUSD, entry.MonthlyUsageUSD} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return invalidUserPlatformQuotaCache("usage")
		}
	}
	for _, value := range []*float64{entry.DailyLimitUSD, entry.WeeklyLimitUSD, entry.MonthlyLimitUSD} {
		if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return invalidUserPlatformQuotaCache("limit")
		}
	}
	if !((entry.QuotaRowID == 0 && entry.ControlGeneration == 0) || (entry.QuotaRowID > 0 && entry.ControlGeneration > 0)) {
		return invalidUserPlatformQuotaCache("flush token")
	}
	if entry.CacheInstance != "" && !validUserPlatformQuotaCacheInstance(entry.CacheInstance) {
		return invalidUserPlatformQuotaCache("cache_instance")
	}
	for _, value := range []*time.Time{entry.DailyWindowStart, entry.WeeklyWindowStart, entry.MonthlyWindowStart} {
		if value != nil && (value.Unix() < 0 || value.Unix() > 253402300799) {
			return invalidUserPlatformQuotaCache("window_start")
		}
		if complete && value == nil {
			return invalidUserPlatformQuotaCache("incomplete window_start")
		}
	}
	if complete && (entry.QuotaRowID <= 0 || entry.ControlGeneration <= 0 || entry.SchemaVersion != service.UserPlatformQuotaCacheSchemaV1) {
		return invalidUserPlatformQuotaCache("incomplete identity")
	}
	return nil
}

// Missing legacy identity/instance is readable, but never becomes a CAS token.
// Malformed values are errors rather than fabricated zero usage/unlimited limits.
func parseUserPlatformQuotaHash(m map[string]string) (*service.UserPlatformQuotaCacheEntry, error) {
	if len(m) == 0 {
		return nil, nil
	}
	parseInt := func(field string, optional bool) (int64, error) {
		raw, exists := m[field]
		if !exists && optional {
			return 0, nil
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 || strconv.FormatInt(value, 10) != raw {
			return 0, invalidUserPlatformQuotaCache(field)
		}
		return value, nil
	}
	entry := new(service.UserPlatformQuotaCacheEntry)
	var err error
	if entry.SchemaVersion, err = parseInt("schema_version", true); err != nil {
		return nil, err
	}
	if entry.Version, err = parseInt("version", entry.SchemaVersion == 0); err != nil {
		return nil, err
	}
	for _, field := range []struct {
		name   string
		target *float64
	}{{"daily_usage", &entry.DailyUsageUSD}, {"weekly_usage", &entry.WeeklyUsageUSD}, {"monthly_usage", &entry.MonthlyUsageUSD}} {
		raw, exists := m[field.name]
		value, parseErr := strconv.ParseFloat(raw, 64)
		if !exists || raw == "" || parseErr != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, invalidUserPlatformQuotaCache(field.name)
		}
		*field.target = value
	}
	for _, field := range []struct {
		name   string
		target **float64
	}{{"daily_limit", &entry.DailyLimitUSD}, {"weekly_limit", &entry.WeeklyLimitUSD}, {"monthly_limit", &entry.MonthlyLimitUSD}} {
		raw, exists := m[field.name]
		if !exists && entry.SchemaVersion == service.UserPlatformQuotaCacheSchemaV1 {
			return nil, invalidUserPlatformQuotaCache(field.name)
		}
		if raw == "" {
			continue
		}
		value, parseErr := strconv.ParseFloat(raw, 64)
		if parseErr != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, invalidUserPlatformQuotaCache(field.name)
		}
		*field.target = &value
	}
	for _, field := range []struct {
		name   string
		target **time.Time
	}{{"daily_window_start", &entry.DailyWindowStart}, {"weekly_window_start", &entry.WeeklyWindowStart}, {"monthly_window_start", &entry.MonthlyWindowStart}} {
		raw, exists := m[field.name]
		if !exists && entry.SchemaVersion == service.UserPlatformQuotaCacheSchemaV1 {
			return nil, invalidUserPlatformQuotaCache(field.name)
		}
		if raw == "" {
			continue
		}
		seconds, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || seconds < 0 || seconds > 253402300799 || strconv.FormatInt(seconds, 10) != raw {
			return nil, invalidUserPlatformQuotaCache(field.name)
		}
		value := time.Unix(seconds, 0).UTC()
		*field.target = &value
	}
	_, hasRow := m["quota_row_id"]
	_, hasGeneration := m["control_generation"]
	if hasRow != hasGeneration {
		return nil, invalidUserPlatformQuotaCache("partial flush token")
	}
	if hasRow {
		if entry.QuotaRowID, err = parseInt("quota_row_id", false); err != nil {
			return nil, err
		}
		if entry.ControlGeneration, err = parseInt("control_generation", false); err != nil {
			return nil, err
		}
	}
	entry.CacheInstance = m["cache_instance"]
	if err := validateUserPlatformQuotaCacheEntry(entry, false); err != nil {
		return nil, err
	}
	return entry, nil
}

func (c *billingCache) GetUserPlatformQuotaCache(ctx context.Context, userID int64, platform string) (*service.UserPlatformQuotaCacheEntry, bool, error) {
	if err := c.quotaCacheAccessError(); err != nil {
		return nil, false, err
	}
	m, err := c.rdb.HGetAll(ctx, userPlatformQuotaCacheKey(userID, platform)).Result()
	if err != nil {
		return nil, false, err
	}
	entry, err := parseUserPlatformQuotaHash(m)
	if err != nil {
		return nil, false, err
	}
	return entry, entry != nil, nil
}

func userPlatformQuotaCacheTTL(ttl time.Duration) (int64, error) {
	if ttl <= 0 || ttl%time.Second != 0 {
		return 0, invalidUserPlatformQuotaCache("ttl must be positive whole seconds")
	}
	return int64(ttl / time.Second), nil
}

func newUserPlatformQuotaCacheInstance() (string, error) {
	var nonce [32]byte
	if _, err := cryptorand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("quota cache instance: %w", err)
	}
	return hex.EncodeToString(nonce[:]), nil
}

// All arguments are validated and formatted before the script. One HSET writes
// the complete entry; no numeric mutation can partly run before a later error.
// Identity/revision comparisons stay decimal strings to preserve int64 precision.
const compareAndSetUserPlatformQuotaCacheScript = `
local kind = redis.call("TYPE", KEYS[1]).ok
if kind ~= "none" and kind ~= "hash" then
    return redis.error_reply("invalid quota cache key type")
end
if ARGV[1] == "nx" then
    if kind ~= "none" then return 0 end
elseif ARGV[1] == "cas" then
    if kind ~= "hash" then return 0 end
    if redis.call("HGET", KEYS[1], "cache_instance") ~= ARGV[2]
       or redis.call("HGET", KEYS[1], "version") ~= ARGV[3]
       or redis.call("HGET", KEYS[1], "quota_row_id") ~= ARGV[4]
       or redis.call("HGET", KEYS[1], "control_generation") ~= ARGV[5]
       or redis.call("HGET", KEYS[1], "schema_version") ~= ARGV[6] then
        return 0
    end
end
redis.call("HSET", KEYS[1],
    "cache_instance", ARGV[8], "version", ARGV[9],
    "quota_row_id", ARGV[10], "control_generation", ARGV[11], "schema_version", ARGV[12],
    "daily_usage", ARGV[13], "weekly_usage", ARGV[14], "monthly_usage", ARGV[15],
    "daily_limit", ARGV[16], "weekly_limit", ARGV[17], "monthly_limit", ARGV[18],
    "daily_window_start", ARGV[19], "weekly_window_start", ARGV[20], "monthly_window_start", ARGV[21])
redis.call("EXPIRE", KEYS[1], ARGV[7])
return 1
`

func quotaCacheWriteArgs(mode string, expected, next *service.UserPlatformQuotaCacheEntry, ttlSeconds int64) []any {
	args := []any{mode, "", "", "", "", "", strconv.FormatInt(ttlSeconds, 10)}
	if expected != nil {
		args[1], args[2], args[3], args[4], args[5] = expected.CacheInstance, strconv.FormatInt(expected.Version, 10),
			strconv.FormatInt(expected.QuotaRowID, 10), strconv.FormatInt(expected.ControlGeneration, 10), strconv.FormatInt(expected.SchemaVersion, 10)
	}
	formatLimit := func(value *float64) string {
		if value == nil {
			return ""
		}
		return strconv.FormatFloat(*value, 'f', -1, 64)
	}
	formatTime := func(value *time.Time) string {
		if value == nil {
			return ""
		}
		return strconv.FormatInt(value.Unix(), 10)
	}
	return append(args, next.CacheInstance, strconv.FormatInt(next.Version, 10), strconv.FormatInt(next.QuotaRowID, 10), strconv.FormatInt(next.ControlGeneration, 10), strconv.FormatInt(next.SchemaVersion, 10),
		strconv.FormatFloat(next.DailyUsageUSD, 'f', -1, 64), strconv.FormatFloat(next.WeeklyUsageUSD, 'f', -1, 64), strconv.FormatFloat(next.MonthlyUsageUSD, 'f', -1, 64),
		formatLimit(next.DailyLimitUSD), formatLimit(next.WeeklyLimitUSD), formatLimit(next.MonthlyLimitUSD),
		formatTime(next.DailyWindowStart), formatTime(next.WeeklyWindowStart), formatTime(next.MonthlyWindowStart))
}

func (c *billingCache) SetUserPlatformQuotaCache(ctx context.Context, userID int64, platform string, entry *service.UserPlatformQuotaCacheEntry, ttl time.Duration) error {
	if err := c.quotaCacheAccessError(); err != nil {
		return err
	}
	if err := validateUserPlatformQuotaCacheEntry(entry, false); err != nil {
		return err
	}
	seconds, err := userPlatformQuotaCacheTTL(ttl)
	if err != nil {
		return err
	}
	next := *entry
	next.CacheInstance, err = newUserPlatformQuotaCacheInstance()
	if err != nil {
		return err
	}
	return c.rdb.Eval(ctx, compareAndSetUserPlatformQuotaCacheScript, []string{userPlatformQuotaCacheKey(userID, platform)}, quotaCacheWriteArgs("set", nil, &next, seconds)...).Err()
}

// CompareAndSetUserPlatformQuotaCache is an optional narrow admission boundary.
// A new cache instance fences delete/recreate and raw SET ABA even at version 0.
func (c *billingCache) CompareAndSetUserPlatformQuotaCache(ctx context.Context, userID int64, platform string, expected, next *service.UserPlatformQuotaCacheEntry, ttl time.Duration) (bool, error) {
	if err := c.quotaCacheAccessError(); err != nil {
		return false, err
	}
	if err := validateUserPlatformQuotaCacheEntry(next, true); err != nil {
		return false, err
	}
	seconds, err := userPlatformQuotaCacheTTL(ttl)
	if err != nil {
		return false, err
	}
	mode := "nx"
	version := int64(0)
	if expected != nil {
		if err := validateUserPlatformQuotaCacheEntry(expected, true); err != nil {
			return false, err
		}
		if !validUserPlatformQuotaCacheInstance(expected.CacheInstance) || expected.Version == math.MaxInt64 {
			return false, invalidUserPlatformQuotaCache("ineligible CAS revision/instance")
		}
		if expected.QuotaRowID != next.QuotaRowID || expected.ControlGeneration != next.ControlGeneration || expected.SchemaVersion != next.SchemaVersion {
			return false, invalidUserPlatformQuotaCache("CAS identity transition")
		}
		mode, version = "cas", expected.Version+1
	}
	replacement := *next
	replacement.Version = version
	replacement.CacheInstance, err = newUserPlatformQuotaCacheInstance()
	if err != nil {
		return false, err
	}
	result, err := c.rdb.Eval(ctx, compareAndSetUserPlatformQuotaCacheScript, []string{userPlatformQuotaCacheKey(userID, platform)}, quotaCacheWriteArgs(mode, expected, &replacement, seconds)...).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (c *billingCache) DeleteUserPlatformQuotaCache(ctx context.Context, userID int64, platform string) error {
	if err := c.quotaCacheAccessError(); err != nil {
		return err
	}
	return c.rdb.Del(ctx, userPlatformQuotaCacheKey(userID, platform)).Err()
}

// userPlatformQuotaDirtySetKey 返回脏集（dirty set）的 Redis key。
// 使用与 userPlatformQuotaCacheKey 相同的前缀 "billing:"。
func userPlatformQuotaDirtySetKey() string { return "billing:" + "upq:dirty" }

// userPlatformQuotaDirtyTTLSeconds 脏集兜底 TTL（秒）：初始 SADD（Lua）与 Readd 共用，
// 确保 flusher 长期停摆时脏集最终过期；正常运行因持续 SADD 不断续期。
const userPlatformQuotaDirtyTTLSeconds = 86400

// userPlatformQuotaDirtyMember 构造脏集成员字符串 "userID:platform"。
func userPlatformQuotaDirtyMember(userID int64, platform string) string {
	return strconv.FormatInt(userID, 10) + ":" + platform
}

// parseUserPlatformQuotaDirtyMember 将脏集成员字符串 "userID:platform" 解析为
// service.UserPlatformQuotaKey。解析失败返回 ok=false。
func parseUserPlatformQuotaDirtyMember(m string) (service.UserPlatformQuotaKey, bool) {
	parts := strings.SplitN(m, ":", 2)
	if len(parts) != 2 {
		return service.UserPlatformQuotaKey{}, false
	}
	uid, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return service.UserPlatformQuotaKey{}, false
	}
	return service.UserPlatformQuotaKey{UserID: uid, Platform: parts[1]}, true
}

// PopDirtyUserPlatformQuotaKeys 从脏集随机弹出最多 n 个 key。
// 脏集为空时返回 (nil, nil)。
func (c *billingCache) PopDirtyUserPlatformQuotaKeys(ctx context.Context, n int) ([]service.UserPlatformQuotaKey, error) {
	if err := c.quotaCacheAccessError(); err != nil {
		return nil, err
	}
	members, err := c.rdb.SPopN(ctx, userPlatformQuotaDirtySetKey(), int64(n)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	keys := make([]service.UserPlatformQuotaKey, 0, len(members))
	for _, m := range members {
		k, ok := parseUserPlatformQuotaDirtyMember(m)
		if !ok {
			log.Printf("billing_cache: skipping invalid dirty member %q", m)
			continue
		}
		keys = append(keys, k)
	}
	return keys, nil
}

// ReaddDirtyUserPlatformQuotaKeys 将 keys 重新加入脏集（flush 失败时回填）。
// 通过 pipeline 同时执行 SAdd + Expire，确保 Readd 后脏集具有兜底 TTL。
// 空切片时直接返回 nil。
func (c *billingCache) ReaddDirtyUserPlatformQuotaKeys(ctx context.Context, keys []service.UserPlatformQuotaKey) error {
	if err := c.quotaCacheAccessError(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	dirtyKey := userPlatformQuotaDirtySetKey()
	members := make([]any, len(keys))
	for i, k := range keys {
		members[i] = userPlatformQuotaDirtyMember(k.UserID, k.Platform)
	}
	pipe := c.rdb.Pipeline()
	pipe.SAdd(ctx, dirtyKey, members...)
	pipe.Expire(ctx, dirtyKey, userPlatformQuotaDirtyTTLSeconds*time.Second)
	_, err := pipe.Exec(ctx)
	return err
}

// BatchGetUserPlatformQuotaCache 通过 Pipeline 批量 HGETALL 获取多个 user×platform 的
// quota cache。返回切片与 keys 顺序、长度对齐；MISS 返回 nil，坏状态整批返回 error。
func (c *billingCache) BatchGetUserPlatformQuotaCache(ctx context.Context, keys []service.UserPlatformQuotaKey) ([]*service.UserPlatformQuotaCacheEntry, error) {
	if err := c.quotaCacheAccessError(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.HGetAll(ctx, userPlatformQuotaCacheKey(k.UserID, k.Platform))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	results := make([]*service.UserPlatformQuotaCacheEntry, len(keys))
	for i, cmd := range cmds {
		m, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		results[i], err = parseUserPlatformQuotaHash(m)
		if err != nil {
			return nil, fmt.Errorf("quota cache batch index %d: %w", i, err)
		}
	}
	return results, nil
}
