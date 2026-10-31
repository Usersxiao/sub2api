//go:build unit

package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func clientBillingAdmissionFixture() (*service.APIKey, *service.ClientGatewayGrant) {
	groupID := int64(42)
	consumerID := "consumer-bound"
	identity := &service.APIKey{
		ID: 101, Kind: service.APIKeyKindClientConsumer, ClientConsumerID: &consumerID,
		UserID: 7, GroupID: &groupID, Status: service.StatusActive,
		User:  &service.User{ID: 7, SessionEpoch: 2, Status: service.StatusActive, Role: service.RoleUser, Balance: 10, Concurrency: 3},
		Group: &service.Group{ID: groupID, Hydrated: true, Platform: service.PlatformOpenAI, Status: service.StatusActive},
	}
	grant := &service.ClientGatewayGrant{
		ClientGatewayScope: service.ClientGatewayScope{SchemaVersion: 1, InstanceID: "instance", UpstreamUserID: "7", SID: "sid", SessionEpoch: 2},
		GrantID:            "grant", ClientID: service.ClientGatewayClientID, ConsumerID: consumerID,
		Operations: []string{"invoke"}, ExpiresAt: time.Now().Add(time.Minute),
	}
	return identity, grant
}

func clientBillingAdmissionKeyService(t *testing.T, cfg *config.Config, onTouch func(int64)) *service.APIKeyService {
	t.Helper()
	return service.NewAPIKeyService(&stubApiKeyRepo{
		getByKey: func(context.Context, string) (*service.APIKey, error) {
			t.Fatal("internal accounting identity reached bearer credential lookup")
			return nil, service.ErrAPIKeyNotFound
		},
		updateLastUsed: func(_ context.Context, id int64, _ time.Time) error {
			if onTouch != nil {
				onTouch(id)
			}
			return nil
		},
	}, nil, nil, nil, nil, nil, cfg)
}

func TestClientBillingAdmissionSetsSharedContextWithoutAdvancing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{config.RunModeStandard, config.RunModeSimple} {
		t.Run(mode, func(t *testing.T) {
			identity, grant := clientBillingAdmissionFixture()
			cfg := &config.Config{RunMode: mode}
			var touchedID int64
			svc := clientBillingAdmissionKeyService(t, cfg, func(id int64) { touchedID = id })
			admit := NewClientBillingAdmission(svc, nil, cfg)
			next := false
			router := gin.New()
			router.POST("/v1/responses", func(c *gin.Context) {
				require.True(t, admit(c, identity, grant, identity.Group.ID))
				require.False(t, next, "admission leaves chain advancement to its caller")
				key, ok := GetAPIKeyFromContext(c)
				require.True(t, ok)
				require.Equal(t, identity.ID, key.ID)
				require.Empty(t, key.Key)
				subject, ok := GetAuthSubjectFromContext(c)
				require.True(t, ok)
				require.Equal(t, identity.User.ID, subject.UserID)
				require.Equal(t, identity.User.Concurrency, subject.Concurrency)
				require.Equal(t, identity.User.ID, c.Request.Context().Value(ctxkey.UserID))
				require.Same(t, identity.Group, c.Request.Context().Value(ctxkey.Group))
				c.Next()
			}, func(c *gin.Context) {
				next = true
				c.Status(http.StatusNoContent)
			})
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
			require.Equal(t, http.StatusNoContent, w.Code)
			require.True(t, next)
			require.Equal(t, identity.ID, touchedID)
		})
	}
}

func TestClientBillingAdmissionRejectsUnboundAccountingIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, name := range []string{
		"nil-identity", "ordinary-key", "unknown-kind", "credential", "missing-consumer", "other-consumer", "missing-user", "other-user", "other-user-edge", "deleted-user", "zero-grant-epoch", "stale-user-epoch",
		"missing-group", "other-group", "other-group-edge", "unhydrated-group", "missing-platform", "zero-id", "nil-grant", "other-client", "other-grant-user", "revoked-grant", "expired-grant", "other-operation",
	} {
		t.Run(name, func(t *testing.T) {
			identity, grant := clientBillingAdmissionFixture()
			switch name {
			case "nil-identity":
				identity = nil
			case "ordinary-key":
				identity.Kind = service.APIKeyKindCredential
			case "unknown-kind":
				identity.Kind = "future-kind"
			case "credential":
				identity.Key = "internal:marker-must-stay-in-storage"
			case "missing-consumer":
				identity.ClientConsumerID = nil
			case "other-consumer":
				consumer := "other-consumer"
				identity.ClientConsumerID = &consumer
			case "missing-user":
				identity.User = nil
			case "other-user":
				identity.UserID++
			case "other-user-edge":
				identity.User.ID++
			case "deleted-user":
				now := time.Now()
				identity.User.DeletedAt = &now
			case "zero-grant-epoch":
				grant.SessionEpoch = 0
				identity.User.SessionEpoch = 0
			case "stale-user-epoch":
				identity.User.SessionEpoch++
			case "missing-group":
				identity.Group = nil
			case "other-group":
				groupID := int64(43)
				identity.GroupID = &groupID
			case "other-group-edge":
				identity.Group.ID++
			case "unhydrated-group":
				identity.Group.Hydrated = false
			case "missing-platform":
				identity.Group.Platform = ""
			case "zero-id":
				identity.ID = 0
			case "nil-grant":
				grant = nil
			case "other-client":
				grant.ClientID = "other-client"
			case "other-grant-user":
				grant.UpstreamUserID = strconv.FormatInt(identity.User.ID+1, 10)
			case "revoked-grant":
				now := time.Now()
				grant.RevokedAt = &now
			case "expired-grant":
				grant.ExpiresAt = time.Now().Add(-time.Second)
			case "other-operation":
				grant.Operations = []string{"manage"}
			}
			cfg := &config.Config{RunMode: config.RunModeStandard}
			svc := clientBillingAdmissionKeyService(t, cfg, func(int64) { t.Fatal("unbound identity touched usage state") })
			assertClientBillingAdmissionDenied(t, NewClientBillingAdmission(svc, nil, cfg), identity, grant, "/v1/responses", http.StatusForbidden, "MODEL_ROUTE_REVOKED")
		})
	}
}

func TestClientBillingAdmissionAppliesOrdinaryGatewayPolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, name := range []string{"balance", "disabled-key", "inactive-user", "disabled-group", "deleted-group", "exclusive-group", "quota", "expiry", "ip", "subscription-missing", "subscription-limit", "subscription-expired"} {
		t.Run(name, func(t *testing.T) {
			identity, grant := clientBillingAdmissionFixture()
			cfg := &config.Config{RunMode: config.RunModeStandard}
			status, code := http.StatusForbidden, "INSUFFICIENT_BALANCE"
			var subscriptions *service.SubscriptionService
			switch name {
			case "balance":
				identity.User.Balance = 0
			case "disabled-key":
				identity.Status = service.StatusDisabled
				status, code = http.StatusUnauthorized, "API_KEY_DISABLED"
			case "inactive-user":
				identity.User.Status = service.StatusDisabled
				status, code = http.StatusUnauthorized, "USER_INACTIVE"
			case "disabled-group":
				identity.Group.Status = service.StatusDisabled
				code = "GROUP_DISABLED"
			case "deleted-group":
				identity.Group.Status = "deleted"
				code = "GROUP_DELETED"
			case "exclusive-group":
				identity.Group.IsExclusive = true
				identity.User.AllowedGroups = []int64{}
				code = "GROUP_NOT_ALLOWED"
			case "quota":
				identity.Quota, identity.QuotaUsed = 1, 1
				status, code = http.StatusTooManyRequests, "insufficient_quota"
			case "expiry":
				past := time.Now().Add(-time.Minute)
				identity.ExpiresAt = &past
				code = "API_KEY_EXPIRED"
			case "ip":
				identity.IPBlacklist = []string{"192.0.2.1"}
				code = "ACCESS_DENIED"
			default:
				identity.Group.SubscriptionType = service.SubscriptionTypeSubscription
				now := time.Now()
				sub := &service.UserSubscription{ID: 55, UserID: identity.UserID, GroupID: identity.Group.ID,
					Status: service.SubscriptionStatusActive, ExpiresAt: now.Add(time.Hour),
					DailyWindowStart: &now, WeeklyWindowStart: &now, MonthlyWindowStart: &now}
				code = "SUBSCRIPTION_NOT_FOUND"
				if name == "subscription-limit" {
					limit := 1.0
					identity.Group.DailyLimitUSD = &limit
					sub.DailyUsageUSD = limit + 0.01
					status, code = http.StatusTooManyRequests, "USAGE_LIMIT_EXCEEDED"
				}
				if name == "subscription-expired" {
					sub.ExpiresAt = now.Add(-time.Minute)
					code = "SUBSCRIPTION_INVALID"
				}
				subscriptions = service.NewSubscriptionService(nil, &stubUserSubscriptionRepo{
					getActive: func(_ context.Context, userID, groupID int64) (*service.UserSubscription, error) {
						require.Equal(t, identity.UserID, userID)
						require.Equal(t, identity.Group.ID, groupID)
						if name == "subscription-missing" {
							return nil, service.ErrSubscriptionNotFound
						}
						return sub, nil
					},
				}, nil, nil, cfg)
				t.Cleanup(subscriptions.Stop)
			}
			svc := clientBillingAdmissionKeyService(t, cfg, func(int64) { t.Fatal("denied identity touched usage state") })
			assertClientBillingAdmissionDenied(t, NewClientBillingAdmission(svc, subscriptions, cfg), identity, grant, "/v1/responses", status, code)
		})
	}
}

func TestClientBillingAdmissionRestrictsInvocationPathsAndDependencies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"/v1/usage", "/v1/sub2api/billing", "/v1/images/tasks/task", "/client/invoke/route"} {
		t.Run(path, func(t *testing.T) {
			identity, grant := clientBillingAdmissionFixture()
			cfg := &config.Config{RunMode: config.RunModeStandard}
			svc := clientBillingAdmissionKeyService(t, cfg, func(int64) { t.Fatal("unsupported path touched usage state") })
			assertClientBillingAdmissionDenied(t, NewClientBillingAdmission(svc, nil, cfg), identity, grant, path, http.StatusForbidden, "MODEL_ROUTE_REVOKED")
		})
	}
	for _, dependency := range []string{"api-key-service", "config", "subscription-service"} {
		t.Run(dependency, func(t *testing.T) {
			identity, grant := clientBillingAdmissionFixture()
			cfg := &config.Config{RunMode: config.RunModeStandard}
			svc := clientBillingAdmissionKeyService(t, cfg, nil)
			switch dependency {
			case "api-key-service":
				svc = nil
			case "config":
				cfg = nil
			case "subscription-service":
				identity.Group.SubscriptionType = service.SubscriptionTypeSubscription
			}
			assertClientBillingAdmissionDenied(t, NewClientBillingAdmission(svc, nil, cfg), identity, grant, "/v1/responses", http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE")
		})
	}
}

func assertClientBillingAdmissionDenied(t *testing.T, admit ClientBillingAdmission, identity *service.APIKey, grant *service.ClientGatewayGrant, path string, status int, code string) {
	t.Helper()
	router := gin.New()
	next := false
	router.POST(path, func(c *gin.Context) {
		require.False(t, admit(c, identity, grant, 42))
		require.True(t, c.IsAborted())
		_, ok := GetAPIKeyFromContext(c)
		require.False(t, ok)
	}, func(c *gin.Context) {
		next = true
		c.Status(http.StatusNoContent)
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, nil)
	req.RemoteAddr = "192.0.2.1:1234"
	router.ServeHTTP(w, req)
	require.Equal(t, status, w.Code)
	require.Contains(t, w.Body.String(), code)
	require.False(t, next)
}

func TestClientBillingAdmissionAcceptsCanonicalSearchPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	identity, grant := clientBillingAdmissionFixture()
	cfg := &config.Config{RunMode: config.RunModeStandard}
	svc := clientBillingAdmissionKeyService(t, cfg, nil)
	admit := NewClientBillingAdmission(svc, nil, cfg)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", nil)
	require.True(t, admit(c, identity, grant, identity.Group.ID))
	key, ok := GetAPIKeyFromContext(c)
	require.True(t, ok)
	require.True(t, key.IsClientConsumer())
	require.Empty(t, key.Key)
}
