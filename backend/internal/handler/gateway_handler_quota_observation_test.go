//go:build unit

package handler

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type handlerQuotaObservationCache struct {
	service.BillingCache
	quotaErr   error
	quotaReads int
}

func (c *handlerQuotaObservationCache) GetUserBalance(context.Context, int64) (float64, error) {
	return 100, nil
}

func (c *handlerQuotaObservationCache) GetUserPlatformQuotaCache(context.Context, int64, string) (*service.UserPlatformQuotaCacheEntry, bool, error) {
	c.quotaReads++
	return nil, false, c.quotaErr
}

type handlerQuotaObservationRepository struct {
	service.UserPlatformQuotaRepository
	read  func(context.Context, int64, string) (*service.UserPlatformQuotaRecord, error)
	reads int
}

func (r *handlerQuotaObservationRepository) GetByUserPlatform(ctx context.Context, userID int64, platform string) (*service.UserPlatformQuotaRecord, error) {
	r.reads++
	return r.read(ctx, userID, platform)
}

func TestGatewayHandlerCountTokensQuotaObservationUnavailableStopsBeforeSelection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const privateCause = "quota-observation-private-test-value"
	cases := []struct {
		name           string
		nilRepository  bool
		repositoryErr  error
		cancelDBRead   bool
		cacheErr       error
		flusherEnabled bool
		wantCacheReads int
	}{
		{name: "database failure", repositoryErr: errors.New(privateCause)},
		{name: "missing repository", nilRepository: true},
		{name: "cancelled during successful database read", cancelDBRead: true},
		{name: "redis failure", cacheErr: errors.New(privateCause), wantCacheReads: 1},
		{name: "flusher cache miss requires recovery", flusherEnabled: true, wantCacheReads: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			groupID := int64(9100)
			group := &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformAnthropic, Status: service.StatusActive}
			apiKey := &service.APIKey{
				ID: 9102, UserID: 9103, GroupID: &groupID, Group: group, Status: service.StatusActive,
				User: &service.User{ID: 9103, Balance: 100},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			quotaRepo := &handlerQuotaObservationRepository{
				read: func(readCtx context.Context, userID int64, platform string) (*service.UserPlatformQuotaRecord, error) {
					require.NoError(t, readCtx.Err(), "the quota read must use this live request's context")
					require.Equal(t, apiKey.UserID, userID)
					require.Equal(t, service.PlatformAnthropic, platform)
					if tc.repositoryErr != nil {
						return nil, tc.repositoryErr
					}
					if tc.cancelDBRead {
						cancel()
						// A driver may complete successfully after cancellation. The
						// guard must check the caller context before trusting absence.
						return nil, nil
					}
					now := time.Now()
					limit := 10.0
					return &service.UserPlatformQuotaRecord{
						QuotaRowID: 301, ControlGeneration: 12, UserID: userID, Platform: platform,
						DailyLimitUSD: &limit, DailyUsageUSD: 1,
						DailyWindowStart: &now, WeeklyWindowStart: &now, MonthlyWindowStart: &now,
					}, nil
				},
			}
			var repo service.UserPlatformQuotaRepository = quotaRepo
			if tc.nilRepository {
				repo = nil
			}
			cache := &handlerQuotaObservationCache{quotaErr: tc.cacheErr}
			cfg := &config.Config{Gateway: config.GatewayConfig{MaxBodySize: 1 << 20}}
			cfg.Database.UserPlatformQuotaFlusherEnabled = tc.flusherEnabled
			billing := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, repo)
			t.Cleanup(billing.Stop)

			// Observe the real downstream scheduler rather than replacing the
			// billing service with an error-returning stub.
			schedulerCache := &countingGatewaySchedulerCache{fakeSchedulerCache: &fakeSchedulerCache{}}
			scheduler := service.NewSchedulerSnapshotService(schedulerCache, nil, nil, nil, nil)
			gateway := service.NewGatewayService(
				nil, &fakeGroupRepo{group: group}, nil, nil, nil, nil, nil, nil, nil,
				scheduler, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
			)
			h := &GatewayHandler{gatewayService: gateway, billingCacheService: billing, cfg: cfg}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", bytes.NewBufferString(
				`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hello"}]}`,
			)).WithContext(ctx)
			c.Request.Header.Set("Content-Type", "application/json")
			c.Set(string(middleware.ContextKeyAPIKey), apiKey)
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.UserID, Concurrency: 1})

			require.NotPanics(t, func() { h.CountTokens(c) })

			require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
			require.JSONEq(t, `{"type":"error","error":{"type":"billing_service_error","message":"Billing service temporarily unavailable. Please retry later."}}`, recorder.Body.String())
			require.Empty(t, recorder.Header().Get("Retry-After"))
			require.NotContains(t, recorder.Body.String(), privateCause)
			require.Zero(t, schedulerCache.snapshotCalls.Load(), "unavailable quota observations must stop before selecting an account")
			_, selected := c.Get(opsAccountIDKey)
			require.False(t, selected, "no account may be selected or forwarded after the failed guard")
			require.Equal(t, tc.wantCacheReads, cache.quotaReads)
			if tc.nilRepository {
				require.Zero(t, quotaRepo.reads)
			} else {
				require.Equal(t, 1, quotaRepo.reads)
			}
		})
	}
}
