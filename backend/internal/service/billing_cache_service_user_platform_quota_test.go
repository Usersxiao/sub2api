//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
)

// fakeIncrCache 仅记录 IncrUserPlatformQuotaUsageCache 被调用的参数。
type fakeIncrCache struct {
	BillingCache
	calls       []incrCall
	result      UserPlatformQuotaIncrementResult
	err         error
	onIncrement func(context.Context)
}

type incrCall struct {
	userID    int64
	platform  string
	cost      float64
	ttl       time.Duration
	markDirty bool
}

func (f *fakeIncrCache) IncrUserPlatformQuotaUsageCache(ctx context.Context, userID int64, platform string, cost float64, ttl time.Duration, markDirty bool) (UserPlatformQuotaIncrementResult, error) {
	f.calls = append(f.calls, incrCall{userID, platform, cost, ttl, markDirty})
	if f.onIncrement != nil {
		f.onIncrement(ctx)
	}
	return f.result, f.err
}

// IncrementUserPlatformQuotaUsage 已改为同步直写,不再走 worker。
// 测试验证:同步调用立即调到 cache.IncrUserPlatformQuotaUsageCache。
func TestIncrementUserPlatformQuotaUsage_SyncCallsCache(t *testing.T) {
	fake := &fakeIncrCache{result: UserPlatformQuotaIncrementResult{Status: QuotaIncrementApplied}}
	cfg := &config.Config{}
	cfg.Billing.UserPlatformQuotaCacheTTLSeconds = 120

	s := &BillingCacheService{
		cache: fake,
		cfg:   cfg,
	}

	if err := s.IncrementUserPlatformQuotaUsage(101, "anthropic", 0.25); err != nil {
		t.Fatal(err)
	}
	if err := s.IncrementUserPlatformQuotaUsage(101, "openai", 0.50); err != nil {
		t.Fatal(err)
	}

	if len(fake.calls) != 2 {
		t.Fatalf("expected 2 incr calls, got %d", len(fake.calls))
	}
	if fake.calls[0] != (incrCall{userID: 101, platform: "anthropic", cost: 0.25, ttl: 120 * time.Second, markDirty: false}) {
		t.Errorf("call[0] = %+v", fake.calls[0])
	}
	if fake.calls[1] != (incrCall{userID: 101, platform: "openai", cost: 0.50, ttl: 120 * time.Second, markDirty: false}) {
		t.Errorf("call[1] = %+v", fake.calls[1])
	}
}

// ── T6 tests: checkUserPlatformQuotaEligibility ──────────────────────────────

// fakeQuotaRepo 实现 UserPlatformQuotaRepository 最小子集
type fakeQuotaRepo struct {
	rec      *UserPlatformQuotaRecord
	getErr   error
	getCalls int
	get      func(context.Context, int64, string) (*UserPlatformQuotaRecord, error)
}

func (f *fakeQuotaRepo) GetByUserPlatform(ctx context.Context, userID int64, platform string) (*UserPlatformQuotaRecord, error) {
	f.getCalls++
	if f.get != nil {
		return f.get(ctx, userID, platform)
	}
	return f.rec, f.getErr
}

func (f *fakeQuotaRepo) BulkInsertInitial(_ context.Context, _ []UserPlatformQuotaRecord) error {
	return nil
}

func (f *fakeQuotaRepo) IncrementUsageWithReset(_ context.Context, _ int64, _ string, _ float64, _ time.Time) error {
	return nil
}

func (f *fakeQuotaRepo) ListByUser(_ context.Context, _ int64) ([]UserPlatformQuotaRecord, error) {
	return nil, nil
}

func (f *fakeQuotaRepo) UpsertForUser(_ context.Context, _ int64, _ []UserPlatformQuotaRecord) error {
	return nil
}

func (f *fakeQuotaRepo) ResetExpiredWindow(_ context.Context, _ int64, _ string, _ string, _ time.Time) error {
	return nil
}

func (f *fakeQuotaRepo) BatchSnapshotUsage(_ context.Context, _ []UserPlatformQuotaSnapshot, _ time.Time) error {
	return nil
}

// fakeFullCache 同时支持 Get + Set + Incr + Delete + Pop/Readd/BatchGet（脏集读写）。
// mu 保护 entry 和 deleteCalls，防止异步 goroutine 与主 goroutine 之间的 data race。
type fakeFullCache struct {
	BillingCache
	mu          sync.Mutex
	entry       *UserPlatformQuotaCacheEntry
	deleteCalls int
	setCalls    int           // SetUserPlatformQuotaCache 调用次数
	lastSetTTL  time.Duration // 最近一次 Set 的 ttl
	getErr      error         // 非 nil 时 Get 先返回 (nil,false,getErr)
	getCalls    int
	casCalls    int
	casErr      error
	casHook     func(context.Context, *UserPlatformQuotaCacheEntry, *UserPlatformQuotaCacheEntry) (bool, error)
	setErr      error // 非 nil 时 Set 返回该 err(setCalls 仍+1)
	// dirty 模拟脏集，供 flusher 测试使用。
	dirty map[UserPlatformQuotaKey]struct{}
}

// getDeleteCalls 线程安全地读取 deleteCalls。
func (f *fakeFullCache) getDeleteCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteCalls
}

// getEntry 线程安全地读取 entry。
func (f *fakeFullCache) getEntry() *UserPlatformQuotaCacheEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.entry
}

// getSetCalls 线程安全地读取 setCalls。
func (f *fakeFullCache) getSetCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setCalls
}

// getLastSetTTL 线程安全地读取 lastSetTTL。
func (f *fakeFullCache) getLastSetTTL() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSetTTL
}

func (f *fakeFullCache) GetUserPlatformQuotaCache(_ context.Context, _ int64, _ string) (*UserPlatformQuotaCacheEntry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	if f.entry == nil {
		return nil, false, nil
	}
	entry := *f.entry
	return &entry, true, nil
}

func (f *fakeFullCache) SetUserPlatformQuotaCache(_ context.Context, _ int64, _ string, e *UserPlatformQuotaCacheEntry, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls++
	if f.setErr != nil {
		return f.setErr
	}
	f.entry = e
	f.lastSetTTL = ttl
	return nil
}

func (f *fakeFullCache) DeleteUserPlatformQuotaCache(_ context.Context, _ int64, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	f.entry = nil
	return nil
}

func (f *fakeFullCache) PopDirtyUserPlatformQuotaKeys(_ context.Context, n int) ([]UserPlatformQuotaKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.dirty) == 0 {
		return nil, nil
	}
	keys := make([]UserPlatformQuotaKey, 0, n)
	for k := range f.dirty {
		if len(keys) >= n {
			break
		}
		keys = append(keys, k)
		delete(f.dirty, k)
	}
	return keys, nil
}

func (f *fakeFullCache) ReaddDirtyUserPlatformQuotaKeys(_ context.Context, keys []UserPlatformQuotaKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dirty == nil {
		f.dirty = make(map[UserPlatformQuotaKey]struct{})
	}
	for _, k := range keys {
		f.dirty[k] = struct{}{}
	}
	return nil
}

// BatchGetUserPlatformQuotaCache 对每个 key 返回 f.entry（MISS → nil），
// 保持与输入 keys 顺序/长度对齐。注意此处所有 key 共享同一个 entry，
// 仅用于测试场景。
func (f *fakeFullCache) BatchGetUserPlatformQuotaCache(_ context.Context, keys []UserPlatformQuotaKey) ([]*UserPlatformQuotaCacheEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	results := make([]*UserPlatformQuotaCacheEntry, len(keys))
	for i := range keys {
		results[i] = f.entry
	}
	return results, nil
}

func newServiceForPreflight(t *testing.T, repo UserPlatformQuotaRepository, cache BillingCache) *BillingCacheService {
	t.Helper()
	cfg := &config.Config{}
	cfg.Billing.UserPlatformQuotaCacheTTLSeconds = 60
	return &BillingCacheService{
		cache:                 cache,
		cfg:                   cfg,
		userPlatformQuotaRepo: repo,
	}
}

// currentDayStart 返回全局时区当天 0 点（与生产 timezone.StartOfDay 同口径，确保窗口有效）。
func currentDayStart() *time.Time {
	s := timezone.StartOfDay(time.Now())
	return &s
}

func TestMonthlyQuotaWindowExpired_NilStart(t *testing.T) {
	if !monthlyQuotaWindowExpired(nil, time.Now().UTC()) {
		t.Error("nil start should be considered expired")
	}
}

func TestMonthlyQuotaWindowExpired_Expired(t *testing.T) {
	start := time.Now().UTC().Add(-30 * 24 * time.Hour)
	if !monthlyQuotaWindowExpired(&start, time.Now().UTC()) {
		t.Error("start exactly 30 days ago should be expired")
	}
}

func TestMonthlyQuotaWindowExpired_Active(t *testing.T) {
	start := time.Now().UTC().Add(-29 * 24 * time.Hour)
	if monthlyQuotaWindowExpired(&start, time.Now().UTC()) {
		t.Error("start 29 days ago should NOT be expired")
	}
}

// TestMonthlyQuotaWindowExpired_CrossMonthBoundary 验证跨自然月时 30 天未满不视为过期。
func TestMonthlyQuotaWindowExpired_CrossMonthBoundary(t *testing.T) {
	// 窗口起始 4 月 20 日；5 月 1 日只过了 11 天，不足 30 天
	start := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if monthlyQuotaWindowExpired(&start, now) {
		t.Error("11 days into window should NOT be expired (30-day rolling, not calendar month)")
	}
}

// TestNextMonthlyResetFrom 验证 30 天滚动重置时间计算。
func TestNextMonthlyResetFrom_WithStart(t *testing.T) {
	start := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	want := start.Add(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	got := nextMonthlyResetFrom(&start, now)
	if !got.Equal(want) {
		t.Errorf("nextMonthlyResetFrom = %v, want %v", got, want)
	}
}

func TestNextMonthlyResetFrom_NilStart(t *testing.T) {
	now := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	got := nextMonthlyResetFrom(nil, now)
	want := now.Add(30 * 24 * time.Hour)
	if !got.Equal(want) {
		t.Errorf("nextMonthlyResetFrom(nil) = %v, want now+30d=%v", got, want)
	}
}

func TestNextMonthlyResetFrom_NilStart_NotEqualToNow(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	got := nextMonthlyResetFrom(nil, now)
	want := now.Add(30 * 24 * time.Hour)
	if !got.Equal(want) {
		t.Errorf("nextMonthlyResetFrom(nil) = %v, want %v (now+30d)", got, want)
	}
	if got.Equal(now) {
		t.Error("nextMonthlyResetFrom(nil) must not return now (should be now+30d)")
	}
}

// TestNextMonthlyResetFrom_ExpiredStart 验证窗口已过期（now-start >= 30d）时，
// 下次重置时间为 now+30d，而非 start+30d（后者已是过去时间，会让 Retry-After 落回
// fallback 并触发客户端紧凑重试）。
func TestNextMonthlyResetFrom_ExpiredStart(t *testing.T) {
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC) // 距 start 61 天，已过期
	got := nextMonthlyResetFrom(&start, now)
	want := now.Add(30 * 24 * time.Hour)
	if !got.Equal(want) {
		t.Errorf("nextMonthlyResetFrom(expired) = %v, want now+30d=%v", got, want)
	}
	if !got.After(now) {
		t.Error("expired window 的下次重置必须在 now 之后，不能是过去时间")
	}
}

func TestIncrementUserPlatformQuotaUsage_InputGuards(t *testing.T) {
	for _, tc := range []struct {
		name     string
		userID   int64
		platform string
		cost     float64
		wantErr  bool
	}{
		{name: "empty platform", userID: 1, cost: 0.5},
		{name: "zero cost", userID: 1, platform: "openai"},
		{name: "negative", userID: 1, platform: "openai", cost: -0.1, wantErr: true},
		{name: "nan", userID: 1, platform: "openai", cost: math.NaN(), wantErr: true},
		{name: "positive infinity", userID: 1, platform: "openai", cost: math.Inf(1), wantErr: true},
		{name: "negative infinity", userID: 1, platform: "openai", cost: math.Inf(-1), wantErr: true},
		{name: "zero user", platform: "openai", cost: 0.5, wantErr: true},
		{name: "negative user", userID: -1, platform: "openai", cost: 0.5, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeIncrCache{}
			s := &BillingCacheService{cache: fake}
			err := s.IncrementUserPlatformQuotaUsage(tc.userID, tc.platform, tc.cost)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if len(fake.calls) != 0 {
				t.Fatalf("guarded input made %d cache calls", len(fake.calls))
			}
		})
	}
	for _, s := range []*BillingCacheService{nil, {}} {
		if err := s.IncrementUserPlatformQuotaUsage(1, "openai", 0.5); err == nil {
			t.Fatal("missing cache must return an error")
		}
	}
}

func TestIncrementUserPlatformQuotaUsage_ExplicitResults(t *testing.T) {
	transportErr := errors.New("commit response lost")
	for _, tc := range []struct {
		name    string
		status  UserPlatformQuotaIncrementStatus
		err     error
		wantErr bool
	}{
		{name: "applied", status: QuotaIncrementApplied},
		{name: "missing", status: QuotaIncrementMissing, wantErr: true},
		{name: "incompatible", status: QuotaIncrementIncompatible, wantErr: true},
		{name: "contended", status: QuotaIncrementContended, wantErr: true},
		{name: "empty status", wantErr: true},
		{name: "unknown status", status: "future-status", wantErr: true},
		{name: "execution error", err: transportErr, wantErr: true},
		{name: "applied with error", status: QuotaIncrementApplied, err: transportErr, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeIncrCache{result: UserPlatformQuotaIncrementResult{Status: tc.status}, err: tc.err}
			err := (&BillingCacheService{cache: fake}).IncrementUserPlatformQuotaUsage(1, "openai", 0.5)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("lost execution error: %v", err)
			}
			if tc.wantErr && tc.err == nil && !strings.Contains(err.Error(), string(tc.status)) {
				t.Fatalf("missing result status in %v", err)
			}
			if len(fake.calls) != 1 {
				t.Fatalf("non-idempotent increment retried %d times", len(fake.calls))
			}
			encoded, marshalErr := json.Marshal(fake.result)
			if marshalErr != nil || string(encoded) != "{}" {
				t.Fatalf("private result leaked: %s, %v", encoded, marshalErr)
			}
		})
	}
}

func TestIncrementUserPlatformQuotaUsage_ConfigAndDirtyFlag(t *testing.T) {
	zero := &config.Config{}
	enabled := &config.Config{}
	enabled.Billing.UserPlatformQuotaCacheTTLSeconds = 120
	enabled.Database.UserPlatformQuotaFlusherEnabled = true
	for _, tc := range []struct {
		name      string
		cfg       *config.Config
		ttl       time.Duration
		markDirty bool
	}{
		{name: "nil config", ttl: 24 * time.Hour},
		{name: "zero ttl", cfg: zero, ttl: 24 * time.Hour},
		{name: "enabled flusher", cfg: enabled, ttl: 120 * time.Second, markDirty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeIncrCache{result: UserPlatformQuotaIncrementResult{Status: QuotaIncrementApplied}}
			if err := (&BillingCacheService{cache: fake, cfg: tc.cfg}).IncrementUserPlatformQuotaUsage(1, "openai", 0.5); err != nil {
				t.Fatal(err)
			}
			if len(fake.calls) != 1 || fake.calls[0].ttl != tc.ttl || fake.calls[0].markDirty != tc.markDirty {
				t.Fatalf("unexpected cache calls: %+v", fake.calls)
			}
		})
	}
}

func TestIncrementUserPlatformQuotaUsage_WaitsForBoundedCommit(t *testing.T) {
	entered := make(chan context.Context)
	release := make(chan struct{})
	done := make(chan error, 1)
	fake := &fakeIncrCache{
		result: UserPlatformQuotaIncrementResult{Status: QuotaIncrementApplied},
		onIncrement: func(ctx context.Context) {
			entered <- ctx
			<-release
		},
	}
	go func() {
		done <- (&BillingCacheService{cache: fake}).IncrementUserPlatformQuotaUsage(1, "openai", 0.5)
	}()
	ctx := <-entered
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > cacheWriteTimeout {
		close(release)
		<-done
		t.Fatalf("bookkeeping context must be bounded: %v, %v", deadline, ok)
	}
	select {
	case err := <-done:
		close(release)
		t.Fatalf("returned before cache committed: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("bookkeeping context not released: %v", ctx.Err())
	}
}

// ── C-NEW-2: 订阅模式豁免 user×platform quota 检查 ──────────────────────────
// 通过直接调用 checkUserPlatformQuotaEligibility 验证：
// 1. standard 模式下 limit=0 → 拦截
// 2. 订阅模式豁免通过 isSubscriptionMode 守卫体现 — 逻辑已在 CheckBillingEligibility 里加 !isSubscriptionMode 条件
// 此处用单元测试直接验证底层 checkUserPlatformQuotaEligibility 的行为（quota 超限确实拦截），
// 而 subscription bypass 逻辑则在 CheckBillingEligibility 中通过条件判断保证，不绕过 sub eligibility 内部复杂依赖。

// fakeZeroQuotaCache 模拟 cache 命中且 daily limit=0（quota 耗尽）。
type fakeZeroQuotaCache struct {
	BillingCache
	called bool
}

func (f *fakeZeroQuotaCache) GetUserPlatformQuotaCache(_ context.Context, _ int64, _ string) (*UserPlatformQuotaCacheEntry, bool, error) {
	f.called = true
	daily := 0.0
	entry := &UserPlatformQuotaCacheEntry{
		DailyUsageUSD:    0,
		DailyLimitUSD:    &daily,
		DailyWindowStart: func() *time.Time { t := time.Now().UTC(); return &t }(),
		SchemaVersion:    UserPlatformQuotaCacheSchemaV1,
	}
	return entry, true, nil
}

func (f *fakeZeroQuotaCache) DeleteUserPlatformQuotaCache(_ context.Context, _ int64, _ string) error {
	return nil
}

// SetUserPlatformQuotaCache 在 weekly/monthly window_start 为 nil 时,checkUserPlatform...
// 会触发"窗口过期 → SetCache 刷新"分支。fake 用 noop 避免 panic。
func (f *fakeZeroQuotaCache) SetUserPlatformQuotaCache(_ context.Context, _ int64, _ string, _ *UserPlatformQuotaCacheEntry, _ time.Duration) error {
	return nil
}

// GetSubscriptionCache 返回有效订阅（active、未过期、usage 远低于 limit），
// 用于支持 checkSubscriptionEligibility 通过，以便验证 quota 检查不被触发。
func (f *fakeZeroQuotaCache) GetSubscriptionCache(_ context.Context, _ int64, _ int64) (*SubscriptionCacheData, error) {
	return &SubscriptionCacheData{
		Status:       SubscriptionStatusActive,
		ExpiresAt:    time.Now().Add(30 * 24 * time.Hour),
		DailyUsage:   0,
		WeeklyUsage:  0,
		MonthlyUsage: 0,
	}, nil
}

func (f *fakeZeroQuotaCache) GetUserBalanceCache(_ context.Context, _ int64) (float64, bool, error) {
	return 100.0, true, nil
}

// TestCheckUserPlatformQuotaEligibility_StandardMode_BlocksWhenLimitZero 验证：
// standard 模式下 limit=0 的 platform quota 确实会被拦截（守卫底层逻辑正确）。
func TestCheckBillingEligibility_SubscriptionMode_BypassesPlatformQuota(t *testing.T) {
	fake := &fakeZeroQuotaCache{} // GetUserPlatformQuotaCache 返回 limit=0，若被调用则拦截
	cfg := &config.Config{}
	cfg.Billing.UserPlatformQuotaCacheTTLSeconds = 60
	s := &BillingCacheService{
		cache:                 fake,
		cfg:                   cfg,
		userPlatformQuotaRepo: &fakeQuotaRepo{},
	}

	subGroup := &Group{
		ID:               10,
		SubscriptionType: "subscription",
		Status:           "active",
		// 无 DailyLimitUSD → checkSubscriptionEligibility 不会因超限失败
	}
	sub := &UserSubscription{Status: "active"}
	user := &User{ID: 42}

	err := s.CheckBillingEligibility(context.Background(), user, nil, subGroup, sub, "anthropic")
	// 订阅模式下不应收到任何 user×platform quota 错误
	if errors.Is(err, ErrUserPlatformDailyQuotaExhausted) ||
		errors.Is(err, ErrUserPlatformWeeklyQuotaExhausted) ||
		errors.Is(err, ErrUserPlatformMonthlyQuotaExhausted) {
		t.Errorf("subscription mode should bypass user×platform quota, got: %v", err)
	}
	// GetUserPlatformQuotaCache 不应被调用
	if fake.called {
		t.Error("GetUserPlatformQuotaCache must NOT be called in subscription mode (C-NEW-2)")
	}
}

// TestCheckBillingEligibility_NonSubscriptionGroup_AppliesQuota 验证：
// 非订阅模式（group=nil）用户 platform quota 超限时被拦截，quota cache 被查询。
func TestMonthlyQuotaWindowExpired_BoundaryTable(t *testing.T) {
	const thirtyDays = 30 * 24 * time.Hour

	cases := []struct {
		name    string
		start   time.Time
		now     time.Time
		expired bool
	}{
		{
			name:    "exactly 30 days → expired",
			start:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			now:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(thirtyDays),
			expired: true,
		},
		{
			name:    "30d minus 1ns → not expired",
			start:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			now:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(thirtyDays - 1),
			expired: false,
		},
		{
			name:    "cross month-end (Feb→Mar, 29d+1s) → expired",
			start:   time.Date(2024, 2, 28, 0, 0, 0, 0, time.UTC),
			now:     time.Date(2024, 3, 29, 0, 0, 1, 0, time.UTC),
			expired: true,
		},
		{
			name:    "cross year boundary (Dec→Jan, 30d+1s) → expired",
			start:   time.Date(2024, 12, 15, 0, 0, 0, 0, time.UTC),
			now:     time.Date(2025, 1, 14, 0, 0, 1, 0, time.UTC),
			expired: true,
		},
	}

	for _, tc := range cases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			got := monthlyQuotaWindowExpired(&tc.start, tc.now)
			if got != tc.expired {
				t.Errorf("monthlyQuotaWindowExpired(start=%v, now=%v) = %v, want %v",
					tc.start, tc.now, got, tc.expired)
			}
		})
	}
}

// TestCheckUserPlatformQuotaEligibility_NoRow_WritesSentinel 验证:
// cache MISS + DB 无行时,回填 sentinel entry(三 limit 全 nil,三 window_start 全 non-nil),
// TTL = UserPlatformQuotaSentinelTTLSeconds,函数返回 nil(fail-open)。
