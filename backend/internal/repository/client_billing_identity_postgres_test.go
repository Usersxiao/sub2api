//go:build unit

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func billingPostgresFixture(t *testing.T) (*sql.DB, *dbent.Client, *service.AuthSession, int64, int64) {
	t.Helper()
	db := authSessionIsolatedDB(t)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	require.NoError(t, client.Schema.Create(context.Background()))
	for _, name := range []string{"191_passkey_credentials.sql", "248_auth_sessions.sql", "249_client_gateway_grants.sql", "250_client_billing_identities.sql"} {
		migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		require.NoError(t, err)
		_, err = db.Exec(string(migration))
		require.NoError(t, err, name)
	}
	u, err := client.User.Create().SetEmail("session@example.test").SetPasswordHash("fixture-password-hash").
		SetBalance(20).Save(context.Background())
	require.NoError(t, err)
	session := createSessionFixture(t, db, NewAuthSessionRepository(db), u.ID, "billing-session")
	var groups [2]int64
	for i := range groups {
		g, err := client.Group.Create().SetName(fmt.Sprintf("billing-group-%d", i)).SetPlatform(service.PlatformOpenAI).Save(context.Background())
		require.NoError(t, err)
		groups[i] = g.ID
	}
	return db, client, session, groups[0], groups[1]
}

func issueBillingGrant(t *testing.T, db *sql.DB, session *service.AuthSession, id string) *service.ClientGatewayGrant {
	t.Helper()
	grant, err := NewClientGatewayRepository(db).Issue(context.Background(), gatewayGrantFixture(session, id), sessionHash(id), "", "")
	require.NoError(t, err)
	return grant
}

func TestClientBillingIdentityPostgresStableConcurrentAndRotated(t *testing.T) {
	db, client, session, groupID, otherGroupID := billingPostgresFixture(t)
	ctx := context.Background()
	grant := issueBillingGrant(t, db, session, "billing-first")
	repo := NewClientBillingIdentityRepository(db, client)
	const concurrency = 8
	start := make(chan struct{})
	results := make(chan *service.APIKey, concurrency)
	errors := make(chan error, concurrency)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			key, err := repo.Resolve(ctx, grant, groupID)
			results <- key
			errors <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	var first *service.APIKey
	for key := range results {
		if first == nil {
			first = key
		}
		require.Equal(t, first.ID, key.ID)
		require.Positive(t, key.ID)
		require.Equal(t, service.APIKeyKindClientConsumer, key.Kind)
		require.Equal(t, grant.ConsumerID, *key.ClientConsumerID)
		require.Empty(t, key.Key)
		require.NotNil(t, key.User)
		require.Equal(t, session.UserID, key.User.ID)
		require.NotNil(t, key.Group)
		require.Equal(t, groupID, key.Group.ID)
	}
	var rows int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM api_keys WHERE kind='client_consumer'`).Scan(&rows))
	require.Equal(t, 1, rows)
	otherGroup, err := repo.Resolve(ctx, grant, otherGroupID)
	require.NoError(t, err)
	require.NotEqual(t, first.ID, otherGroup.ID)
	rotated := issueBillingGrant(t, db, session, "billing-rotated")
	key, err := NewClientBillingIdentityRepository(db, client).Resolve(ctx, rotated, groupID)
	require.NoError(t, err)
	require.Equal(t, first.ID, key.ID)
	_, err = repo.Resolve(ctx, grant, groupID)
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	anotherLogin := createSessionFixture(t, db, NewAuthSessionRepository(db), session.UserID, "billing-new-login")
	relogged := issueBillingGrant(t, db, anotherLogin, "billing-relogged")
	key, err = repo.Resolve(ctx, relogged, groupID)
	require.NoError(t, err)
	require.Equal(t, first.ID, key.ID)
	otherUser, err := client.User.Create().SetEmail("other@example.test").SetPasswordHash("fixture-password-hash").Save(ctx)
	require.NoError(t, err)
	otherSession, err := NewAuthSessionRepository(db).Create(ctx, &service.AuthSession{SessionID: "billing-other-user", UserID: otherUser.ID, Epoch: otherUser.SessionEpoch,
		TokenVersion: 17, BindingHash: "fixture-binding", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}, sessionHash("billing-other-user-token"), otherUser.Email, otherUser.PasswordHash)
	require.NoError(t, err)
	otherGrant := issueBillingGrant(t, db, otherSession, "billing-other-user-grant")
	otherKey, err := repo.Resolve(ctx, otherGrant, groupID)
	require.NoError(t, err)
	require.NotEqual(t, first.ID, otherKey.ID)
	require.NotEqual(t, *first.ClientConsumerID, *otherKey.ClientConsumerID)
}

func TestClientBillingIdentityPostgresRequiresLiveExactGrant(t *testing.T) {
	db, client, session, groupID, _ := billingPostgresFixture(t)
	ctx := context.Background()
	grant := issueBillingGrant(t, db, session, "billing-live")
	repo := NewClientBillingIdentityRepository(db, client)
	for _, mutate := range []func(*service.ClientGatewayGrant){
		func(g *service.ClientGatewayGrant) { g.GrantID = "missing" },
		func(g *service.ClientGatewayGrant) { g.ConsumerID = "other-consumer" },
		func(g *service.ClientGatewayGrant) { g.InstanceID = "other-instance" },
		func(g *service.ClientGatewayGrant) { g.ConfigVersion = "cfg-forged" },
		func(g *service.ClientGatewayGrant) { g.EntitlementVersion = "ent-forged" },
		func(g *service.ClientGatewayGrant) { g.RouteIDs = []string{"forged-route"} },
		func(g *service.ClientGatewayGrant) { g.Operations = []string{"manage"} },
	} {
		candidate := *grant
		mutate(&candidate)
		_, err := repo.Resolve(ctx, &candidate, groupID)
		require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	}
	_, err := repo.Resolve(ctx, grant, groupID+1000)
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	_, err = db.Exec(`UPDATE groups SET status='disabled' WHERE id=$1`, groupID)
	require.NoError(t, err)
	_, err = repo.Resolve(ctx, grant, groupID)
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	_, err = db.Exec(`UPDATE groups SET status='active' WHERE id=$1`, groupID)
	require.NoError(t, err)
	_, err = repo.Resolve(ctx, grant, groupID)
	require.NoError(t, err)
	require.NoError(t, NewAuthSessionRepository(db).RevokeRefresh(ctx, sessionHash("billing-session-token")))
	_, err = repo.Resolve(ctx, grant, groupID)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
}

func TestClientBillingIdentityPostgresExcludedFromCredentialManagementAndHydration(t *testing.T) {
	db, client, session, groupID, otherGroupID := billingPostgresFixture(t)
	ctx := context.Background()
	grant := issueBillingGrant(t, db, session, "billing-isolation")
	internal, err := NewClientBillingIdentityRepository(db, client).Resolve(ctx, grant, groupID)
	require.NoError(t, err)
	var marker string
	require.NoError(t, db.QueryRow(`SELECT key FROM api_keys WHERE id=$1`, internal.ID).Scan(&marker))
	require.Contains(t, marker, service.InternalAPIKeyPrefix)
	public := NewAPIKeyRepository(client, db)
	credential := &service.APIKey{UserID: session.UserID, Key: "sk-visible-credential", Name: "Visible", Status: service.StatusActive, GroupID: &groupID}
	require.NoError(t, public.Create(ctx, credential))
	for _, lookup := range []func() error{
		func() error { _, err := public.GetByID(ctx, internal.ID); return err },
		func() error { _, _, err := public.GetKeyAndOwnerID(ctx, internal.ID); return err },
		func() error { _, err := public.GetByKey(ctx, marker); return err },
		func() error { _, err := public.GetByKeyForAuth(ctx, marker); return err },
		func() error {
			return public.Update(ctx, &service.APIKey{ID: internal.ID, Name: "Changed"}, service.APIKeyUpdateFields{Name: true})
		},
		func() error { return public.Delete(ctx, internal.ID) },
		func() error { return public.DeleteWithAudit(ctx, internal.ID) },
	} {
		require.ErrorIs(t, lookup(), service.ErrAPIKeyNotFound)
	}
	ids, err := public.VerifyOwnership(ctx, session.UserID, []int64{credential.ID, internal.ID})
	require.NoError(t, err)
	require.Equal(t, []int64{credential.ID}, ids)
	count, err := public.CountByUserID(ctx, session.UserID)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	params := pagination.PaginationParams{Page: 1, PageSize: 20}
	keys, page, err := public.ListByUserID(ctx, session.UserID, params, service.APIKeyListFilters{})
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.EqualValues(t, 1, page.Total)
	groupKeys, _, err := public.ListByGroupID(ctx, groupID, params)
	require.NoError(t, err)
	require.Len(t, groupKeys, 1)
	search, err := public.SearchAPIKeys(ctx, session.UserID, "AutoDesign", 10)
	require.NoError(t, err)
	require.Empty(t, search)
	exists, err := public.ExistsByKey(ctx, marker)
	require.NoError(t, err)
	require.False(t, exists)
	for _, candidate := range []*service.APIKey{
		{UserID: session.UserID, Kind: service.APIKeyKindClientConsumer, Key: "sk-forged-internal", Name: "forged"},
		{UserID: session.UserID, Key: "internal:forged-key", Name: "forged"},
		{UserID: session.UserID, ClientConsumerID: &grant.ConsumerID, Key: "sk-forged-binding", Name: "forged"},
	} {
		require.ErrorIs(t, public.Create(ctx, candidate), service.ErrAPIKeyNotFound)
	}
	moved, err := public.UpdateGroupIDByUserAndGroup(ctx, session.UserID, groupID, otherGroupID)
	require.NoError(t, err)
	require.EqualValues(t, 1, moved)
	cleared, err := public.ClearGroupIDByGroupID(ctx, groupID)
	require.NoError(t, err)
	require.Zero(t, cleared)
	row, err := client.APIKey.Get(ctx, internal.ID)
	require.NoError(t, err)
	require.Equal(t, groupID, *row.GroupID)
	require.Equal(t, "AutoDesign", row.Name)
	require.Nil(t, row.DeletedAt)
	usage := newUsageLogRepositoryWithSQL(client, db)
	hydrated, err := usage.loadAPIKeys(ctx, []int64{internal.ID, credential.ID})
	require.NoError(t, err)
	require.NotContains(t, hydrated, internal.ID)
	require.Contains(t, hydrated, credential.ID)
	stats, err := usage.GetUserDashboardStats(ctx, session.UserID)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.TotalAPIKeys)
	require.EqualValues(t, 1, stats.ActiveAPIKeys)
	global := &DashboardStats{}
	require.NoError(t, usage.fillDashboardEntityStats(ctx, global, time.Now().Add(-24*time.Hour), time.Now()))
	require.EqualValues(t, 1, global.TotalAPIKeys)
	require.EqualValues(t, 1, global.ActiveAPIKeys)
}

func TestClientBillingIdentityPostgresSupportsRealUsageFKAndQuotaAccounting(t *testing.T) {
	db, client, session, groupID, _ := billingPostgresFixture(t)
	ctx := context.Background()
	grant := issueBillingGrant(t, db, session, "billing-accounting")
	key, err := NewClientBillingIdentityRepository(db, client).Resolve(ctx, grant, groupID)
	require.NoError(t, err)
	public := NewAPIKeyRepository(client, db)
	used, err := public.IncrementQuotaUsed(ctx, key.ID, 1.25)
	require.NoError(t, err)
	require.Equal(t, 1.25, used)
	state, err := newAPIKeyRepositoryWithSQL(client, db).IncrementQuotaUsedAndGetState(ctx, key.ID, 0.75)
	require.NoError(t, err)
	require.Equal(t, 2.0, state.QuotaUsed)
	accountID := mustCreateAPIKeyRepoAccount(t, ctx, client, "ledger-account")
	mustCreateAPIKeyRepoUsageLog(t, ctx, client, session.UserID, key.ID, accountID, "ledger-request", time.Now(), nil)
	var storedID int64
	require.NoError(t, db.QueryRow(`SELECT api_key_id FROM usage_logs WHERE request_id='ledger-request'`).Scan(&storedID))
	require.Equal(t, key.ID, storedID)
	usage := newUsageLogRepositoryWithSQL(client, db)
	trend, err := usage.GetAPIKeyUsageTrend(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), "hour", 10)
	require.NoError(t, err)
	require.Empty(t, trend, "internal identities must not occupy the ordinary API Key TopN")
	visible := &service.APIKey{UserID: session.UserID, Key: "sk-visible-usage", Name: "Visible usage", Status: service.StatusActive, GroupID: &groupID}
	require.NoError(t, public.Create(ctx, visible))
	mustCreateAPIKeyRepoUsageLog(t, ctx, client, session.UserID, visible.ID, accountID, "visible-ledger-request", time.Now(), nil)
	trend, err = usage.GetAPIKeyUsageTrend(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), "hour", 1)
	require.NoError(t, err)
	require.Len(t, trend, 1)
	require.Equal(t, visible.ID, trend[0].APIKeyID)
	require.Equal(t, visible.Name, trend[0].KeyName)
	stats, err := usage.GetUserStats(ctx, session.UserID, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.TotalRequests, "account usage includes both client consumption and ordinary API keys")
}
