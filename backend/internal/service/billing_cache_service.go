package service

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"golang.org/x/sync/singleflight"
)

// 错误定义
// 注：ErrInsufficientBalance在redeem_service.go中定义
// 注：ErrDailyLimitExceeded/ErrWeeklyLimitExceeded/ErrMonthlyLimitExceeded在subscription_service.go中定义
var (
	ErrSubscriptionInvalid       = infraerrors.Forbidden("SUBSCRIPTION_INVALID", "subscription is invalid or expired")
	ErrBillingServiceUnavailable = infraerrors.ServiceUnavailable("BILLING_SERVICE_ERROR", "Billing service temporarily unavailable. Please retry later.")
	// RPM 超限错误。gateway_handler 负责映射为 HTTP 429。
	ErrGroupRPMExceeded = infraerrors.TooManyRequests("GROUP_RPM_EXCEEDED", "group requests-per-minute limit exceeded")
	ErrUserRPMExceeded  = infraerrors.TooManyRequests("USER_RPM_EXCEEDED", "user requests-per-minute limit exceeded")

	// user × platform quota（HTTP 429 Too Many Requests + Retry-After header）。
	// 选用 429 而非 403：限额耗尽属于"暂时性资源用尽，重试可恢复"的场景（RFC 6585），
	// 大量 SDK（如 OpenAI 兼容客户端）只对 429 触发自动退避并读取 Retry-After，
	// 用 403 会被视为"权限不足，重试无意义"导致客户端直接报错且不退避。
	ErrUserPlatformDailyQuotaExhausted   = infraerrors.TooManyRequests("USER_PLATFORM_DAILY_QUOTA_EXHAUSTED", "Daily usage quota exhausted for this platform.")
	ErrUserPlatformWeeklyQuotaExhausted  = infraerrors.TooManyRequests("USER_PLATFORM_WEEKLY_QUOTA_EXHAUSTED", "Weekly usage quota exhausted for this platform.")
	ErrUserPlatformMonthlyQuotaExhausted = infraerrors.TooManyRequests("USER_PLATFORM_MONTHLY_QUOTA_EXHAUSTED", "Monthly usage quota exhausted for this platform.")
)

// subscriptionCacheData 订阅缓存数据结构（内部使用）
type subscriptionCacheData struct {
	Status       string
	ExpiresAt    time.Time
	DailyUsage   float64
	WeeklyUsage  float64
	MonthlyUsage float64
	Version      int64
}

// 缓存写入任务类型
type cacheWriteKind int

const (
	cacheWriteSetBalance cacheWriteKind = iota
	cacheWriteSetSubscription
	cacheWriteUpdateSubscriptionUsage
	cacheWriteDeductBalance
	cacheWriteUpdateRateLimitUsage
)

// 异步缓存写入工作池配置
//
// 性能优化说明：
// 原实现在请求热路径中使用 goroutine 异步更新缓存，存在以下问题：
// 1. 每次请求创建新 goroutine，高并发下产生大量短生命周期 goroutine
// 2. 无法控制并发数量，可能导致 Redis 连接耗尽
// 3. goroutine 创建/销毁带来额外开销
//
// 新实现使用固定大小的工作池：
// 1. 预创建 10 个 worker goroutine，避免频繁创建销毁
// 2. 使用带缓冲的 channel（1000）作为任务队列，平滑写入峰值
// 3. 非阻塞写入，队列满时关键任务同步回退，非关键任务丢弃并告警
// 4. 统一超时控制，避免慢操作阻塞工作池
const (
	cacheWriteWorkerCount     = 10              // 工作协程数量
	cacheWriteBufferSize      = 1000            // 任务队列缓冲大小
	cacheWriteTimeout         = 2 * time.Second // 单个写入操作超时
	cacheWriteDropLogInterval = 5 * time.Second // 丢弃日志节流间隔
	balanceLoadTimeout        = 3 * time.Second
)

// cacheWriteTask 缓存写入任务
type cacheWriteTask struct {
	kind             cacheWriteKind
	userID           int64
	groupID          int64
	apiKeyID         int64
	balance          float64
	amount           float64
	subscriptionData *subscriptionCacheData
}

// apiKeyRateLimitLoader defines the interface for loading rate limit data from DB.
type apiKeyRateLimitLoader interface {
	GetRateLimitData(ctx context.Context, keyID int64) (*APIKeyRateLimitData, error)
}

type subscriptionCacheInvalidationPubSub interface {
	PublishSubscriptionCacheInvalidation(ctx context.Context, cacheKey string) error
	SubscribeSubscriptionCacheInvalidation(ctx context.Context, handler func(cacheKey string)) error
}

// BillingCacheService 计费缓存服务
// 负责余额和订阅数据的缓存管理，提供高性能的计费资格检查
type BillingCacheService struct {
	cache                 BillingCache
	userRepo              UserRepository
	subRepo               UserSubscriptionRepository
	apiKeyRateLimitLoader apiKeyRateLimitLoader
	userRPMCache          UserRPMCache
	userGroupRateRepo     UserGroupRateRepository
	cfg                   *config.Config
	circuitBreaker        *billingCircuitBreaker
	userPlatformQuotaRepo UserPlatformQuotaRepository

	cacheWriteChan     chan cacheWriteTask
	cacheWriteWg       sync.WaitGroup
	cacheWriteStopOnce sync.Once
	cacheWriteMu       sync.RWMutex
	stopped            atomic.Bool
	balanceLoadSF      singleflight.Group
	// 丢弃日志节流计数器（减少高负载下日志噪音）
	cacheWriteDropFullCount     uint64
	cacheWriteDropFullLastLog   int64
	cacheWriteDropClosedCount   uint64
	cacheWriteDropClosedLastLog int64
}

// NewBillingCacheService 创建计费缓存服务
func NewBillingCacheService(
	cache BillingCache,
	userRepo UserRepository,
	subRepo UserSubscriptionRepository,
	apiKeyRepo APIKeyRepository,
	userRPMCache UserRPMCache,
	userGroupRateRepo UserGroupRateRepository,
	cfg *config.Config,
	userPlatformQuotaRepo UserPlatformQuotaRepository,
) *BillingCacheService {
	svc := &BillingCacheService{
		cache:                 cache,
		userRepo:              userRepo,
		subRepo:               subRepo,
		apiKeyRateLimitLoader: apiKeyRepo,
		userRPMCache:          userRPMCache,
		userGroupRateRepo:     userGroupRateRepo,
		cfg:                   cfg,
		userPlatformQuotaRepo: userPlatformQuotaRepo,
	}
	svc.circuitBreaker = newBillingCircuitBreaker(cfg.Billing.CircuitBreaker)
	svc.startCacheWriteWorkers()
	return svc
}

// Stop 关闭缓存写入工作池
func (s *BillingCacheService) Stop() {
	s.cacheWriteStopOnce.Do(func() {
		s.stopped.Store(true)

		s.cacheWriteMu.Lock()
		ch := s.cacheWriteChan
		if ch != nil {
			close(ch)
		}
		s.cacheWriteMu.Unlock()

		if ch == nil {
			return
		}
		s.cacheWriteWg.Wait()

		s.cacheWriteMu.Lock()
		if s.cacheWriteChan == ch {
			s.cacheWriteChan = nil
		}
		s.cacheWriteMu.Unlock()
	})
}

func (s *BillingCacheService) startCacheWriteWorkers() {
	ch := make(chan cacheWriteTask, cacheWriteBufferSize)
	s.cacheWriteChan = ch
	for i := 0; i < cacheWriteWorkerCount; i++ {
		s.cacheWriteWg.Add(1)
		go s.cacheWriteWorker(ch)
	}
}

// enqueueCacheWrite 尝试将任务入队，队列满时返回 false（并记录告警）。
func (s *BillingCacheService) enqueueCacheWrite(task cacheWriteTask) (enqueued bool) {
	if s.stopped.Load() {
		s.logCacheWriteDrop(task, "closed")
		return false
	}

	s.cacheWriteMu.RLock()
	defer s.cacheWriteMu.RUnlock()

	if s.cacheWriteChan == nil {
		s.logCacheWriteDrop(task, "closed")
		return false
	}

	select {
	case s.cacheWriteChan <- task:
		return true
	default:
		// 队列满时不阻塞主流程，交由调用方决定是否同步回退。
		s.logCacheWriteDrop(task, "full")
		return false
	}
}

func (s *BillingCacheService) cacheWriteWorker(ch <-chan cacheWriteTask) {
	defer s.cacheWriteWg.Done()
	for task := range ch {
		ctx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
		switch task.kind {
		case cacheWriteSetBalance:
			s.setBalanceCache(ctx, task.userID, task.balance)
		case cacheWriteSetSubscription:
			s.setSubscriptionCache(ctx, task.userID, task.groupID, task.subscriptionData)
		case cacheWriteUpdateSubscriptionUsage:
			if s.cache != nil {
				if err := s.cache.UpdateSubscriptionUsage(ctx, task.userID, task.groupID, task.amount); err != nil {
					logger.LegacyPrintf("service.billing_cache", "Warning: update subscription cache failed for user %d group %d: %v", task.userID, task.groupID, err)
				}
			}
		case cacheWriteDeductBalance:
			if s.cache != nil {
				if err := s.cache.DeductUserBalance(ctx, task.userID, task.amount); err != nil {
					logger.LegacyPrintf("service.billing_cache", "Warning: deduct balance cache failed for user %d: %v", task.userID, err)
				}
			}
		case cacheWriteUpdateRateLimitUsage:
			if s.cache != nil {
				if err := s.cache.UpdateAPIKeyRateLimitUsage(ctx, task.apiKeyID, task.amount); err != nil {
					logger.LegacyPrintf("service.billing_cache", "Warning: update rate limit usage cache failed for api key %d: %v", task.apiKeyID, err)
				}
			}
		}
		cancel()
	}
}

// cacheWriteKindName 用于日志中的任务类型标识，便于排查丢弃原因。
func cacheWriteKindName(kind cacheWriteKind) string {
	switch kind {
	case cacheWriteSetBalance:
		return "set_balance"
	case cacheWriteSetSubscription:
		return "set_subscription"
	case cacheWriteUpdateSubscriptionUsage:
		return "update_subscription_usage"
	case cacheWriteDeductBalance:
		return "deduct_balance"
	case cacheWriteUpdateRateLimitUsage:
		return "update_rate_limit_usage"
	default:
		return "unknown"
	}
}

// logCacheWriteDrop 使用节流方式记录丢弃情况，并汇总丢弃数量。
func (s *BillingCacheService) logCacheWriteDrop(task cacheWriteTask, reason string) {
	var (
		countPtr *uint64
		lastPtr  *int64
	)
	switch reason {
	case "full":
		countPtr = &s.cacheWriteDropFullCount
		lastPtr = &s.cacheWriteDropFullLastLog
	case "closed":
		countPtr = &s.cacheWriteDropClosedCount
		lastPtr = &s.cacheWriteDropClosedLastLog
	default:
		return
	}

	atomic.AddUint64(countPtr, 1)
	now := time.Now().UnixNano()
	last := atomic.LoadInt64(lastPtr)
	if now-last < int64(cacheWriteDropLogInterval) {
		return
	}
	if !atomic.CompareAndSwapInt64(lastPtr, last, now) {
		return
	}
	dropped := atomic.SwapUint64(countPtr, 0)
	if dropped == 0 {
		return
	}
	logger.LegacyPrintf("service.billing_cache", "Warning: cache write queue %s, dropped %d tasks in last %s (latest kind=%s user %d group %d)",
		reason,
		dropped,
		cacheWriteDropLogInterval,
		cacheWriteKindName(task.kind),
		task.userID,
		task.groupID,
	)
}

// ============================================
// 余额缓存方法
// ============================================

// GetUserBalance 获取用户余额（优先从缓存读取）
func (s *BillingCacheService) GetUserBalance(ctx context.Context, userID int64) (float64, error) {
	if s.cache == nil {
		// Redis不可用，直接查询数据库
		return s.getUserBalanceFromDB(ctx, userID)
	}

	// 尝试从缓存读取
	balance, err := s.cache.GetUserBalance(ctx, userID)
	if err == nil {
		return balance, nil
	}

	// 缓存未命中：singleflight 合并同一 userID 的并发回源请求。
	value, err, _ := s.balanceLoadSF.Do(strconv.FormatInt(userID, 10), func() (any, error) {
		loadCtx, cancel := context.WithTimeout(context.Background(), balanceLoadTimeout)
		defer cancel()

		balance, err := s.getUserBalanceFromDB(loadCtx, userID)
		if err != nil {
			return nil, err
		}

		// 异步建立缓存
		_ = s.enqueueCacheWrite(cacheWriteTask{
			kind:    cacheWriteSetBalance,
			userID:  userID,
			balance: balance,
		})
		return balance, nil
	})
	if err != nil {
		return 0, err
	}
	balance, ok := value.(float64)
	if !ok {
		return 0, fmt.Errorf("unexpected balance type: %T", value)
	}
	return balance, nil
}

// getUserBalanceFromDB 从数据库获取用户余额
func (s *BillingCacheService) getUserBalanceFromDB(ctx context.Context, userID int64) (float64, error) {
	user, err := s.userRepo.GetByID(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("get user balance: %w", err)
	}
	return user.Balance, nil
}

// setBalanceCache 设置余额缓存
func (s *BillingCacheService) setBalanceCache(ctx context.Context, userID int64, balance float64) {
	if s.cache == nil {
		return
	}
	if err := s.cache.SetUserBalance(ctx, userID, balance); err != nil {
		logger.LegacyPrintf("service.billing_cache", "Warning: set balance cache failed for user %d: %v", userID, err)
	}
}

// DeductBalanceCache 扣减余额缓存（同步调用）
func (s *BillingCacheService) DeductBalanceCache(ctx context.Context, userID int64, amount float64) error {
	if s.cache == nil {
		return nil
	}
	return s.cache.DeductUserBalance(ctx, userID, amount)
}

// QueueDeductBalance 异步扣减余额缓存
func (s *BillingCacheService) QueueDeductBalance(userID int64, amount float64) {
	if s.cache == nil {
		return
	}
	// 队列满时同步回退，避免关键扣减被静默丢弃。
	if s.enqueueCacheWrite(cacheWriteTask{
		kind:   cacheWriteDeductBalance,
		userID: userID,
		amount: amount,
	}) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
	defer cancel()
	if err := s.DeductBalanceCache(ctx, userID, amount); err != nil {
		logger.LegacyPrintf("service.billing_cache", "Warning: deduct balance cache fallback failed for user %d: %v", userID, err)
	}
}

// InvalidateUserBalance 失效用户余额缓存
func (s *BillingCacheService) InvalidateUserBalance(ctx context.Context, userID int64) error {
	if s.cache == nil {
		return nil
	}
	if err := s.cache.InvalidateUserBalance(ctx, userID); err != nil {
		logger.LegacyPrintf("service.billing_cache", "Warning: invalidate balance cache failed for user %d: %v", userID, err)
		return err
	}
	return nil
}

// ============================================
// 订阅缓存方法
// ============================================

// GetSubscriptionStatus 获取订阅状态（优先从缓存读取）
func (s *BillingCacheService) GetSubscriptionStatus(ctx context.Context, userID, groupID int64) (*subscriptionCacheData, error) {
	if s.cache == nil {
		return s.getSubscriptionFromDB(ctx, userID, groupID)
	}

	// 尝试从缓存读取
	cacheData, err := s.cache.GetSubscriptionCache(ctx, userID, groupID)
	if err == nil && cacheData != nil {
		return s.convertFromPortsData(cacheData), nil
	}

	// 缓存未命中，从数据库读取
	data, err := s.getSubscriptionFromDB(ctx, userID, groupID)
	if err != nil {
		return nil, err
	}

	// 异步建立缓存
	_ = s.enqueueCacheWrite(cacheWriteTask{
		kind:             cacheWriteSetSubscription,
		userID:           userID,
		groupID:          groupID,
		subscriptionData: data,
	})

	return data, nil
}

func (s *BillingCacheService) convertFromPortsData(data *SubscriptionCacheData) *subscriptionCacheData {
	return &subscriptionCacheData{
		Status:       data.Status,
		ExpiresAt:    data.ExpiresAt,
		DailyUsage:   data.DailyUsage,
		WeeklyUsage:  data.WeeklyUsage,
		MonthlyUsage: data.MonthlyUsage,
		Version:      data.Version,
	}
}

func (s *BillingCacheService) convertToPortsData(data *subscriptionCacheData) *SubscriptionCacheData {
	return &SubscriptionCacheData{
		Status:       data.Status,
		ExpiresAt:    data.ExpiresAt,
		DailyUsage:   data.DailyUsage,
		WeeklyUsage:  data.WeeklyUsage,
		MonthlyUsage: data.MonthlyUsage,
		Version:      data.Version,
	}
}

// getSubscriptionFromDB 从数据库获取订阅数据
func (s *BillingCacheService) getSubscriptionFromDB(ctx context.Context, userID, groupID int64) (*subscriptionCacheData, error) {
	sub, err := s.subRepo.GetActiveByUserIDAndGroupID(ctx, userID, groupID)
	if err != nil {
		return nil, fmt.Errorf("get subscription: %w", err)
	}

	return &subscriptionCacheData{
		Status:       sub.Status,
		ExpiresAt:    sub.ExpiresAt,
		DailyUsage:   sub.DailyUsageUSD,
		WeeklyUsage:  sub.WeeklyUsageUSD,
		MonthlyUsage: sub.MonthlyUsageUSD,
		Version:      sub.UpdatedAt.Unix(),
	}, nil
}

// setSubscriptionCache 设置订阅缓存
func (s *BillingCacheService) setSubscriptionCache(ctx context.Context, userID, groupID int64, data *subscriptionCacheData) {
	if s.cache == nil || data == nil {
		return
	}
	if err := s.cache.SetSubscriptionCache(ctx, userID, groupID, s.convertToPortsData(data)); err != nil {
		logger.LegacyPrintf("service.billing_cache", "Warning: set subscription cache failed for user %d group %d: %v", userID, groupID, err)
	}
}

// UpdateSubscriptionUsage 更新订阅用量缓存（同步调用）
func (s *BillingCacheService) UpdateSubscriptionUsage(ctx context.Context, userID, groupID int64, costUSD float64) error {
	if s.cache == nil {
		return nil
	}
	return s.cache.UpdateSubscriptionUsage(ctx, userID, groupID, costUSD)
}

// QueueUpdateSubscriptionUsage 异步更新订阅用量缓存
func (s *BillingCacheService) QueueUpdateSubscriptionUsage(userID, groupID int64, costUSD float64) {
	if s.cache == nil {
		return
	}
	// 队列满时同步回退，确保订阅用量及时更新。
	if s.enqueueCacheWrite(cacheWriteTask{
		kind:    cacheWriteUpdateSubscriptionUsage,
		userID:  userID,
		groupID: groupID,
		amount:  costUSD,
	}) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
	defer cancel()
	if err := s.UpdateSubscriptionUsage(ctx, userID, groupID, costUSD); err != nil {
		logger.LegacyPrintf("service.billing_cache", "Warning: update subscription cache fallback failed for user %d group %d: %v", userID, groupID, err)
	}
}

// InvalidateSubscription 失效指定订阅缓存
func (s *BillingCacheService) InvalidateSubscription(ctx context.Context, userID, groupID int64) error {
	if s.cache == nil {
		return nil
	}
	if err := s.cache.InvalidateSubscriptionCache(ctx, userID, groupID); err != nil {
		logger.LegacyPrintf("service.billing_cache", "Warning: invalidate subscription cache failed for user %d group %d: %v", userID, groupID, err)
		return err
	}
	return nil
}

func (s *BillingCacheService) PublishSubscriptionCacheInvalidation(ctx context.Context, cacheKey string) error {
	if s.cache == nil {
		return nil
	}
	pubsub, ok := s.cache.(subscriptionCacheInvalidationPubSub)
	if !ok {
		return nil
	}
	return pubsub.PublishSubscriptionCacheInvalidation(ctx, cacheKey)
}

func (s *BillingCacheService) SubscribeSubscriptionCacheInvalidation(ctx context.Context, handler func(cacheKey string)) error {
	if s.cache == nil {
		return nil
	}
	pubsub, ok := s.cache.(subscriptionCacheInvalidationPubSub)
	if !ok {
		return nil
	}
	return pubsub.SubscribeSubscriptionCacheInvalidation(ctx, handler)
}

// InvalidateAPIKeyRateLimit invalidates the Redis rate-limit usage cache for an API key.
func (s *BillingCacheService) InvalidateAPIKeyRateLimit(ctx context.Context, keyID int64) error {
	if s.cache == nil {
		return nil
	}
	if err := s.cache.InvalidateAPIKeyRateLimit(ctx, keyID); err != nil {
		logger.LegacyPrintf("service.billing_cache", "Warning: invalidate api key rate limit cache failed for key %d: %v", keyID, err)
		return err
	}
	return nil
}

// ============================================
// API Key 限速缓存方法
// ============================================

// checkAPIKeyRateLimits checks rate limit windows for an API key.
// It loads usage from Redis cache (falling back to DB on cache miss),
// resets expired windows in-memory and triggers async DB reset,
// and returns an error if any window limit is exceeded.
func (s *BillingCacheService) checkAPIKeyRateLimits(ctx context.Context, apiKey *APIKey) error {
	return s.checkAPIKeyRateLimitsWithPending(ctx, apiKey, 0)
}

// checkAPIKeyRateLimitsWithPending 在窗口用量上叠加 pending（即将扣除的金额）后检查限速窗口。
func (s *BillingCacheService) checkAPIKeyRateLimitsWithPending(ctx context.Context, apiKey *APIKey, pending float64) error {
	if s.cache == nil {
		// No cache: fall back to reading from DB directly
		if s.apiKeyRateLimitLoader == nil {
			return nil
		}
		data, err := s.apiKeyRateLimitLoader.GetRateLimitData(ctx, apiKey.ID)
		if err != nil {
			return nil // Don't block requests on DB errors
		}
		return s.evaluateRateLimits(ctx, apiKey, data.Usage5h, data.Usage1d, data.Usage7d,
			data.Window5hStart, data.Window1dStart, data.Window7dStart, pending)
	}

	cacheData, err := s.cache.GetAPIKeyRateLimit(ctx, apiKey.ID)
	if err != nil {
		// Cache miss: load from DB and populate cache
		if s.apiKeyRateLimitLoader == nil {
			return nil
		}
		dbData, dbErr := s.apiKeyRateLimitLoader.GetRateLimitData(ctx, apiKey.ID)
		if dbErr != nil {
			return nil // Don't block requests on DB errors
		}
		// Build cache entry from DB data
		cacheEntry := &APIKeyRateLimitCacheData{
			Usage5h: dbData.Usage5h,
			Usage1d: dbData.Usage1d,
			Usage7d: dbData.Usage7d,
		}
		if dbData.Window5hStart != nil {
			cacheEntry.Window5h = dbData.Window5hStart.Unix()
		}
		if dbData.Window1dStart != nil {
			cacheEntry.Window1d = dbData.Window1dStart.Unix()
		}
		if dbData.Window7dStart != nil {
			cacheEntry.Window7d = dbData.Window7dStart.Unix()
		}
		_ = s.cache.SetAPIKeyRateLimit(ctx, apiKey.ID, cacheEntry)
		cacheData = cacheEntry
	}

	var w5h, w1d, w7d *time.Time
	if cacheData.Window5h > 0 {
		t := time.Unix(cacheData.Window5h, 0)
		w5h = &t
	}
	if cacheData.Window1d > 0 {
		t := time.Unix(cacheData.Window1d, 0)
		w1d = &t
	}
	if cacheData.Window7d > 0 {
		t := time.Unix(cacheData.Window7d, 0)
		w7d = &t
	}
	return s.evaluateRateLimits(ctx, apiKey, cacheData.Usage5h, cacheData.Usage1d, cacheData.Usage7d, w5h, w1d, w7d, pending)
}

// evaluateRateLimits checks usage (plus pending) against limits, triggering async resets for expired windows.
func (s *BillingCacheService) evaluateRateLimits(ctx context.Context, apiKey *APIKey, usage5h, usage1d, usage7d float64, w5h, w1d, w7d *time.Time, pending float64) error {
	needsReset := false

	// Reset expired windows in-memory for check purposes
	if IsWindowExpired(w5h, RateLimitWindow5h) {
		usage5h = 0
		needsReset = true
	}
	if IsWindowExpired(w1d, RateLimitWindow1d) {
		usage1d = 0
		needsReset = true
	}
	if IsWindowExpired(w7d, RateLimitWindow7d) {
		usage7d = 0
		needsReset = true
	}

	// Trigger async DB reset if any window expired
	if needsReset {
		keyID := apiKey.ID
		go func() {
			resetCtx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
			defer cancel()
			if s.apiKeyRateLimitLoader != nil {
				// Use the repo directly - reset then reload cache
				if loader, ok := s.apiKeyRateLimitLoader.(interface {
					ResetRateLimitWindows(ctx context.Context, id int64) error
				}); ok {
					if err := loader.ResetRateLimitWindows(resetCtx, keyID); err != nil {
						logger.LegacyPrintf("service.billing_cache", "Warning: reset rate limit windows failed for api key %d: %v", keyID, err)
					}
				}
			}
			// Invalidate cache so next request loads fresh data
			if s.cache != nil {
				if err := s.cache.InvalidateAPIKeyRateLimit(resetCtx, keyID); err != nil {
					logger.LegacyPrintf("service.billing_cache", "Warning: invalidate rate limit cache failed for api key %d: %v", keyID, err)
				}
			}
		}()
	}

	// Check limits
	exceeds := func(usage, limit float64) bool {
		return limit > 0 && (usage >= limit || usage+pending > limit)
	}
	if exceeds(usage5h, apiKey.RateLimit5h) {
		return ErrAPIKeyRateLimit5hExceeded
	}
	if exceeds(usage1d, apiKey.RateLimit1d) {
		return ErrAPIKeyRateLimit1dExceeded
	}
	if exceeds(usage7d, apiKey.RateLimit7d) {
		return ErrAPIKeyRateLimit7dExceeded
	}
	return nil
}

// QueueUpdateAPIKeyRateLimitUsage asynchronously updates rate limit usage in the cache.
func (s *BillingCacheService) QueueUpdateAPIKeyRateLimitUsage(apiKeyID int64, cost float64) {
	if s.cache == nil {
		return
	}
	s.enqueueCacheWrite(cacheWriteTask{
		kind:     cacheWriteUpdateRateLimitUsage,
		apiKeyID: apiKeyID,
		amount:   cost,
	})
}

// IncrementUserPlatformQuotaUsage 同步累加 user × platform usage 到 Redis 缓存。
//
// 成功的同步条件提交使后续 preflight 可读取本次 usage；窗口轮换和费用增量
// 共同提交。失败或结果不明不承诺已计量，仍需独立的持久计量与恢复协议。
//
// 仅明确的 Applied 结果成功；caller 记录失败并保留既有 DB 持久化策略。
// 已消费费用使用独立有界 context 记账，错误不在此处重试或重建缓存。
func (s *BillingCacheService) IncrementUserPlatformQuotaUsage(userID int64, platform string, cost float64) error {
	if platform == "" || cost == 0 {
		return nil
	}
	if userID <= 0 {
		return fmt.Errorf("increment user platform quota: invalid user ID %d", userID)
	}
	if cost < 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return fmt.Errorf("increment user platform quota: cost must be finite and nonnegative")
	}
	if s == nil || s.cache == nil {
		return fmt.Errorf("increment user platform quota: cache unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
	defer cancel()
	markDirty := s.cfg != nil && s.cfg.Database.UserPlatformQuotaFlusherEnabled
	result, err := s.cache.IncrUserPlatformQuotaUsageCache(ctx, userID, platform, cost, s.platformQuotaCacheTTL(), markDirty)
	if err != nil {
		return fmt.Errorf("increment user platform quota cache: %w", err)
	}
	if result.Status != QuotaIncrementApplied {
		return fmt.Errorf("increment user platform quota cache not applied: status %q", result.Status)
	}
	return nil
}

// IncrementBoundUserPlatformQuotaUsage uses the committed Apply identity and
// time. Unsupported/partial/obsolete outcomes remain explicit; no legacy writer,
// current-configuration bootstrap or non-idempotent retry is attempted.
func (s *BillingCacheService) IncrementBoundUserPlatformQuotaUsage(binding UserPlatformQuotaUsageBinding) error {
	if err := ValidateUserPlatformQuotaUsageBinding(binding); err != nil {
		return err
	}
	if s == nil || s.cache == nil {
		return fmt.Errorf("increment bound user platform quota: cache unavailable")
	}
	cache, ok := s.cache.(UserPlatformQuotaBoundUsageCache)
	if !ok {
		return fmt.Errorf("increment bound user platform quota: bound cache writer unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
	defer cancel()
	markDirty := s.cfg != nil && s.cfg.Database.UserPlatformQuotaFlusherEnabled
	result, err := cache.IncrBoundUserPlatformQuotaUsageCache(ctx, binding, s.platformQuotaCacheTTL(), markDirty)
	if err != nil {
		return fmt.Errorf("increment bound user platform quota cache: %w", err)
	}
	if result.Status != QuotaIncrementApplied || result.AppliedWindows != QuotaWindowAll {
		return fmt.Errorf("increment bound user platform quota cache unconfirmed: status %q windows %d", result.Status, result.AppliedWindows)
	}
	return nil
}

// ============================================
// 统一检查方法
// ============================================

// CheckBillingEligibility 检查用户是否有资格发起请求
// 余额模式：检查缓存余额 > 0
// 订阅模式：检查缓存用量未超过限额（Group限额从参数传入）
// platform 为请求的目标平台（如 "anthropic"），传空串 "" 时跳过 user × platform quota 检查。
func (s *BillingCacheService) CheckBillingEligibility(ctx context.Context, user *User, apiKey *APIKey, group *Group, subscription *UserSubscription, platform string) error {
	// 简易模式默认跳过所有计费检查. An explicit key-window opt-in keeps
	// balance/subscription/platform checks bypassed while enforcing the three
	// API-key monetary windows from the database source of truth.
	if s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		if s.cfg.SimpleModeKeyRateLimitEnabled {
			return s.checkSimpleModeAPIKeyRateLimits(ctx, apiKey)
		}
		return nil
	}
	if s.circuitBreaker != nil && !s.circuitBreaker.Allow() {
		return ErrBillingServiceUnavailable
	}

	// 判断计费模式
	isSubscriptionMode := group != nil && group.IsSubscriptionType() && subscription != nil

	if isSubscriptionMode {
		if err := s.checkSubscriptionEligibility(ctx, user.ID, group, subscription); err != nil {
			return err
		}
	} else {
		if err := s.checkBalanceEligibility(ctx, user.ID); err != nil {
			return err
		}
	}

	// user × platform quota 仅在 standard（余额）模式生效；订阅模式豁免
	if !isSubscriptionMode {
		if err := s.checkUserPlatformQuotaEligibility(ctx, user.ID, platform); err != nil {
			return err
		}
	}

	// Check API Key rate limits (applies to both billing modes)
	if apiKey != nil && apiKey.HasRateLimits() {
		if err := s.checkAPIKeyRateLimits(ctx, apiKey); err != nil {
			return err
		}
	}

	// RPM 限流：级联回落（Override → Group → User），放在最后以避免为注定失败的请求增加计数。
	if err := s.checkRPM(ctx, user, group); err != nil {
		return err
	}

	return nil
}

// ReserveFixedCharge 为扣费前即可确定金额的费用（倍率后 USD，如显式缓存存储费）做准入并登记在途预留。
//
// 余额模式要求缓存余额覆盖 amount 及用户的其他在途预留；订阅模式要求日 / 周 / 月剩余额度覆盖 amount；
// API Key 总额度与限速窗口同样叠加 amount。订阅与 API Key 的用量取库中权威值，并叠加同一订阅 / Key 上其他
// 在途预留：先登记本次预留再读用量，并发的两笔费用中后读用量的一方必然看到另一方（预留或已落库的扣费）。
// 返回的句柄可能为 nil（未登记任何预留）；调用方须在 handler 结束时调用 HandlerDone，预留在计费任务落库后释放。
// 须在 CheckBillingEligibility 通过后调用；简易模式不校验。
func (s *BillingCacheService) ReserveFixedCharge(ctx context.Context, user *User, apiKey *APIKey, group *Group, subscription *UserSubscription, amount float64) (*InflightReservation, error) {
	if user == nil || amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return nil, nil
	}
	if s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		return nil, nil
	}
	subscriptionBill := group != nil && group.IsSubscriptionType() && subscription != nil
	if !subscriptionBill {
		balance, err := s.GetUserBalance(ctx, user.ID)
		if err != nil {
			return nil, ErrBillingServiceUnavailable.WithCause(err)
		}
		if balance < amount {
			return nil, ErrInsufficientBalance
		}
	}
	res, err := s.reserveInflight(ctx, user, group, subscription, amount, false, false)
	if err != nil {
		return nil, err
	}
	var subscriptionPending, apiKeyPending float64
	if subscriptionBill && (group.HasDailyLimit() || group.HasWeeklyLimit() || group.HasMonthlyLimit()) {
		res, subscriptionPending = s.reserveInflightScope(ctx, res, user.ID, inflightSubscriptionScope(subscription.ID), amount)
	}
	if apiKey != nil && (apiKey.Quota > 0 || apiKey.HasRateLimits()) {
		res, apiKeyPending = s.reserveInflightScope(ctx, res, user.ID, inflightAPIKeyScope(apiKey.ID), amount)
	}
	if subscriptionBill {
		err = s.checkFixedChargeSubscription(ctx, user.ID, group, amount+subscriptionPending)
	}
	if err == nil && apiKey != nil {
		err = s.checkFixedChargeAPIKey(ctx, apiKey, amount+apiKeyPending)
	}
	if err != nil {
		res.Release()
		return nil, err
	}
	return res, nil
}

// checkFixedChargeSubscription 按库中订阅用量校验日 / 周 / 月剩余额度覆盖 pending。
func (s *BillingCacheService) checkFixedChargeSubscription(ctx context.Context, userID int64, group *Group, pending float64) error {
	read := s.GetSubscriptionStatus
	if s.subRepo != nil {
		read = s.getSubscriptionFromDB
	}
	subData, err := read(ctx, userID, group.ID)
	if err != nil {
		return ErrBillingServiceUnavailable.WithCause(err)
	}
	if group.HasDailyLimit() && subData.DailyUsage+pending > *group.DailyLimitUSD {
		return ErrDailyLimitExceeded
	}
	if group.HasWeeklyLimit() && subData.WeeklyUsage+pending > *group.WeeklyLimitUSD {
		return ErrWeeklyLimitExceeded
	}
	if group.HasMonthlyLimit() && subData.MonthlyUsage+pending > *group.MonthlyLimitUSD {
		return ErrMonthlyLimitExceeded
	}
	return nil
}

// checkFixedChargeAPIKey 按库中 Key 用量校验总额度与限速窗口覆盖 pending；未接入仓储时使用请求上下文与限速缓存中的用量。
func (s *BillingCacheService) checkFixedChargeAPIKey(ctx context.Context, apiKey *APIKey, pending float64) error {
	if apiKey.Quota <= 0 && !apiKey.HasRateLimits() {
		return nil
	}
	if s.apiKeyRateLimitLoader == nil {
		if apiKey.Quota > 0 && apiKey.QuotaUsed+pending > apiKey.Quota {
			return ErrAPIKeyQuotaExhausted
		}
		if apiKey.HasRateLimits() {
			return s.checkAPIKeyRateLimitsWithPending(ctx, apiKey, pending)
		}
		return nil
	}
	data, err := s.apiKeyRateLimitLoader.GetRateLimitData(ctx, apiKey.ID)
	if err != nil {
		return ErrBillingServiceUnavailable.WithCause(err)
	}
	exceeds := func(usage, limit float64) bool {
		return limit > 0 && usage+pending > limit
	}
	switch {
	case exceeds(data.QuotaUsed, apiKey.Quota):
		return ErrAPIKeyQuotaExhausted
	case exceeds(data.EffectiveUsage5h(), apiKey.RateLimit5h):
		return ErrAPIKeyRateLimit5hExceeded
	case exceeds(data.EffectiveUsage1d(), apiKey.RateLimit1d):
		return ErrAPIKeyRateLimit1dExceeded
	case exceeds(data.EffectiveUsage7d(), apiKey.RateLimit7d):
		return ErrAPIKeyRateLimit7dExceeded
	}
	return nil
}

// checkSimpleModeAPIKeyRateLimits is deliberately DB-authoritative. Redis
// updates are asynchronous and can be dropped or missed after a committed
// transaction, so using the cache here could let a limited simple-mode key
// continue past its configured window. A read failure fails closed because
// the operator explicitly opted into enforcement.
func (s *BillingCacheService) checkSimpleModeAPIKeyRateLimits(ctx context.Context, apiKey *APIKey) error {
	if apiKey == nil || !apiKey.HasRateLimits() {
		return nil
	}
	if s.apiKeyRateLimitLoader == nil {
		return ErrBillingServiceUnavailable
	}
	data, err := s.apiKeyRateLimitLoader.GetRateLimitData(ctx, apiKey.ID)
	if err != nil || data == nil {
		return ErrBillingServiceUnavailable
	}
	if apiKey.RateLimit5h > 0 && data.EffectiveUsage5h() >= apiKey.RateLimit5h {
		return ErrAPIKeyRateLimit5hExceeded
	}
	if apiKey.RateLimit1d > 0 && data.EffectiveUsage1d() >= apiKey.RateLimit1d {
		return ErrAPIKeyRateLimit1dExceeded
	}
	if apiKey.RateLimit7d > 0 && data.EffectiveUsage7d() >= apiKey.RateLimit7d {
		return ErrAPIKeyRateLimit7dExceeded
	}
	return nil
}

// checkRPM 执行并行 RPM 限流，所有适用的限制同时生效，任一超限即拒绝：
//
//  1. (用户, 分组) rpm_override       — 最细粒度：管理员为特定用户在特定分组设定的专属限额。
//     override=0 表示该用户在该分组免检（绿灯），但 user 级全局上限仍然生效。
//  2. group.rpm_limit                 — 分组级：该分组的统一 RPM 容量（仅当无 override 时生效）。
//  3. user.rpm_limit                  — 用户级全局硬上限：无论 override/group 如何配置，始终生效。
//
// 与旧版"级联互斥"设计不同，新版确保 user.rpm_limit 作为全局天花板不会被 group 或 override 覆盖。
// Redis 故障一律 fail-open（打 warning，不阻塞业务）。
func (s *BillingCacheService) checkRPM(ctx context.Context, user *User, group *Group) error {
	if s == nil || s.userRPMCache == nil || user == nil {
		return nil
	}

	// ── 第一层：分组级检查（override 或 group.rpm_limit） ──
	if group != nil {
		// 解析 override：优先从 auth cache snapshot，nil 时回退 DB。
		var override *int
		if user.UserGroupRPMOverride != nil {
			override = user.UserGroupRPMOverride
		} else if s.userGroupRateRepo != nil {
			dbOverride, err := s.userGroupRateRepo.GetRPMOverrideByUserAndGroup(ctx, user.ID, group.ID)
			if err != nil {
				logger.LegacyPrintf(
					"service.billing_cache",
					"Warning: rpm override lookup failed for user=%d group=%d: %v",
					user.ID, group.ID, err,
				)
			} else {
				override = dbOverride
			}
		}

		if override != nil {
			// override=0 → 该用户在该分组免检（但 user 级仍会在下面检查）。
			if *override > 0 {
				count, incErr := s.userRPMCache.IncrementUserGroupRPM(ctx, user.ID, group.ID)
				if incErr != nil {
					logger.LegacyPrintf(
						"service.billing_cache",
						"Warning: rpm increment (override) failed for user=%d group=%d: %v",
						user.ID, group.ID, incErr,
					)
					// fail-open
				} else if count > *override {
					return ErrGroupRPMExceeded
				}
			}
			// override 命中后跳过 group.rpm_limit（override 替代 group），但不 return——继续检查 user 级。
		} else if group.RPMLimit > 0 {
			// 无 override，检查 group.rpm_limit。
			count, err := s.userRPMCache.IncrementUserGroupRPM(ctx, user.ID, group.ID)
			if err != nil {
				logger.LegacyPrintf(
					"service.billing_cache",
					"Warning: rpm increment (group) failed for user=%d group=%d: %v",
					user.ID, group.ID, err,
				)
				// fail-open
			} else if count > group.RPMLimit {
				return ErrGroupRPMExceeded
			}
		}
	}

	// ── 第二层：用户级全局硬上限（始终生效） ──
	if user.RPMLimit > 0 {
		count, err := s.userRPMCache.IncrementUserRPM(ctx, user.ID)
		if err != nil {
			logger.LegacyPrintf(
				"service.billing_cache",
				"Warning: rpm increment (user) failed for user=%d: %v",
				user.ID, err,
			)
			return nil // fail-open
		}
		if count > user.RPMLimit {
			return ErrUserRPMExceeded
		}
	}

	return nil
}

func (s *BillingCacheService) minimumBalanceReserve() float64 {
	if s == nil || s.cfg == nil || s.cfg.Billing.MinimumBalanceReserve <= 0 {
		return 0
	}
	return s.cfg.Billing.MinimumBalanceReserve
}

func (s *BillingCacheService) balanceBelowEligibilityThreshold(balance float64) bool {
	if balance <= 0 {
		return true
	}
	minimumReserve := s.minimumBalanceReserve()
	return minimumReserve > 0 && balance < minimumReserve
}

// checkBalanceEligibility 检查余额模式资格
func (s *BillingCacheService) checkBalanceEligibility(ctx context.Context, userID int64) error {
	balance, err := s.GetUserBalance(ctx, userID)
	if err != nil {
		if s.circuitBreaker != nil {
			s.circuitBreaker.OnFailure(err)
		}
		logger.LegacyPrintf("service.billing_cache", "ALERT: billing balance check failed for user %d: %v", userID, err)
		return ErrBillingServiceUnavailable.WithCause(err)
	}
	if s.circuitBreaker != nil {
		s.circuitBreaker.OnSuccess()
	}

	if s.balanceBelowEligibilityThreshold(balance) {
		return ErrInsufficientBalance
	}

	return nil
}

// checkSubscriptionEligibility 检查订阅模式资格
func (s *BillingCacheService) checkSubscriptionEligibility(ctx context.Context, userID int64, group *Group, subscription *UserSubscription) error {
	// 获取订阅缓存数据
	subData, err := s.GetSubscriptionStatus(ctx, userID, group.ID)
	if err != nil {
		if s.circuitBreaker != nil {
			s.circuitBreaker.OnFailure(err)
		}
		logger.LegacyPrintf("service.billing_cache", "ALERT: billing subscription check failed for user %d group %d: %v", userID, group.ID, err)
		return ErrBillingServiceUnavailable.WithCause(err)
	}
	if s.circuitBreaker != nil {
		s.circuitBreaker.OnSuccess()
	}

	// 检查订阅状态
	if subData.Status != SubscriptionStatusActive {
		return ErrSubscriptionInvalid
	}

	// 检查是否过期
	if time.Now().After(subData.ExpiresAt) {
		return ErrSubscriptionInvalid
	}

	// 检查限额（使用传入的Group限额配置）
	if group.HasDailyLimit() && subData.DailyUsage >= *group.DailyLimitUSD {
		return ErrDailyLimitExceeded
	}

	if group.HasWeeklyLimit() && subData.WeeklyUsage >= *group.WeeklyLimitUSD {
		return ErrWeeklyLimitExceeded
	}

	if group.HasMonthlyLimit() && subData.MonthlyUsage >= *group.MonthlyLimitUSD {
		return ErrMonthlyLimitExceeded
	}

	return nil
}

type billingCircuitBreakerState int

const (
	billingCircuitClosed billingCircuitBreakerState = iota
	billingCircuitOpen
	billingCircuitHalfOpen
)

type billingCircuitBreaker struct {
	mu                sync.Mutex
	state             billingCircuitBreakerState
	failures          int
	openedAt          time.Time
	failureThreshold  int
	resetTimeout      time.Duration
	halfOpenRequests  int
	halfOpenRemaining int
}

func newBillingCircuitBreaker(cfg config.CircuitBreakerConfig) *billingCircuitBreaker {
	if !cfg.Enabled {
		return nil
	}
	resetTimeout := time.Duration(cfg.ResetTimeoutSeconds) * time.Second
	if resetTimeout <= 0 {
		resetTimeout = 30 * time.Second
	}
	halfOpen := cfg.HalfOpenRequests
	if halfOpen <= 0 {
		halfOpen = 1
	}
	threshold := cfg.FailureThreshold
	if threshold <= 0 {
		threshold = 5
	}
	return &billingCircuitBreaker{
		state:            billingCircuitClosed,
		failureThreshold: threshold,
		resetTimeout:     resetTimeout,
		halfOpenRequests: halfOpen,
	}
}

func (b *billingCircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case billingCircuitClosed:
		return true
	case billingCircuitOpen:
		if time.Since(b.openedAt) < b.resetTimeout {
			return false
		}
		b.state = billingCircuitHalfOpen
		b.halfOpenRemaining = b.halfOpenRequests
		logger.LegacyPrintf("service.billing_cache", "ALERT: billing circuit breaker entering half-open state")
		fallthrough
	case billingCircuitHalfOpen:
		if b.halfOpenRemaining <= 0 {
			return false
		}
		b.halfOpenRemaining--
		return true
	default:
		return false
	}
}

func (b *billingCircuitBreaker) OnFailure(err error) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case billingCircuitOpen:
		return
	case billingCircuitHalfOpen:
		b.state = billingCircuitOpen
		b.openedAt = time.Now()
		b.halfOpenRemaining = 0
		logger.LegacyPrintf("service.billing_cache", "ALERT: billing circuit breaker opened after half-open failure: %v", err)
		return
	default:
		b.failures++
		if b.failures >= b.failureThreshold {
			b.state = billingCircuitOpen
			b.openedAt = time.Now()
			b.halfOpenRemaining = 0
			logger.LegacyPrintf("service.billing_cache", "ALERT: billing circuit breaker opened after %d failures: %v", b.failures, err)
		}
	}
}

func (b *billingCircuitBreaker) OnSuccess() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	previousState := b.state
	previousFailures := b.failures

	b.state = billingCircuitClosed
	b.failures = 0
	b.halfOpenRemaining = 0

	// 只有状态真正发生变化时才记录日志
	if previousState != billingCircuitClosed {
		logger.LegacyPrintf("service.billing_cache", "ALERT: billing circuit breaker closed (was %s)", circuitStateString(previousState))
	} else if previousFailures > 0 {
		logger.LegacyPrintf("service.billing_cache", "INFO: billing circuit breaker failures reset from %d", previousFailures)
	}
}

func circuitStateString(state billingCircuitBreakerState) string {
	switch state {
	case billingCircuitClosed:
		return "closed"
	case billingCircuitOpen:
		return "open"
	case billingCircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// withWindowResetsMetadata 给 quota error 附加 window_resets_at metadata（RFC3339）。
func withWindowResetsMetadata(err error, resetAt time.Time) error {
	appErr, ok := err.(*infraerrors.ApplicationError)
	if !ok || appErr == nil {
		return err
	}
	return appErr.WithMetadata(map[string]string{
		"window_resets_at": resetAt.Format(time.RFC3339),
	})
}

// nextDailyReset 计算下一个日窗口起点（次日全局时区 0 点）。
// 必须与 timezone.StartOfDay 同口径，否则 Retry-After 会偏差。
func nextDailyReset(now time.Time) time.Time {
	return timezone.StartOfDay(now).AddDate(0, 0, 1)
}

// nextWeeklyReset 计算下一个周窗口起点（下周一全局时区 0 点）。
// 必须与 timezone.StartOfWeek 同口径，否则 Retry-After 会偏差。
func nextWeeklyReset(now time.Time) time.Time {
	return timezone.StartOfWeek(now).AddDate(0, 0, 7)
}

// nextMonthlyResetFrom 返回 30 天滚动窗口的下次重置时间（start + 30d）。
// start 为 nil（未初始化）或已过期（now-start >= 30d，与 monthlyQuotaWindowExpired 同口径）时
// 退化为 now+30d：过期窗口会在下次 increment 时重置为 now，下次重置即 now+30d；
// 否则按 start 计算会得到一个过去的时间，使 Retry-After 落回 fallback 并触发客户端紧凑重试。
func nextMonthlyResetFrom(start *time.Time, now time.Time) time.Time {
	if start == nil || now.Sub(*start) >= 30*24*time.Hour {
		return now.Add(30 * 24 * time.Hour)
	}
	return start.Add(30 * 24 * time.Hour)
}

// quotaWindowExpired 判断窗口是否已过期：start 为 nil（未初始化）或在 currWindowStart 之前视为已过期。
func quotaWindowExpired(start *time.Time, currWindowStart time.Time) bool {
	if start == nil {
		return true
	}
	return start.Before(currWindowStart)
}

// monthlyQuotaWindowExpired 判断 30 天滚动月度窗口是否已过期。
// 过期条件：now - start >= 30×24h（与订阅模式 NeedsMonthlyReset 语义一致）。
// start 为 nil 时视为已过期（未初始化窗口）。
func monthlyQuotaWindowExpired(start *time.Time, now time.Time) bool {
	if start == nil {
		return true
	}
	return now.Sub(*start) >= 30*24*time.Hour
}
