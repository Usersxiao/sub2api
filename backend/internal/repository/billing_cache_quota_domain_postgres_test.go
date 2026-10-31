//go:build unit

package repository

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func migrateBillingQuotaCacheDomain(t *testing.T, db *sql.DB) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "265_user_platform_quota_cache_namespace.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(raw))
	require.NoError(t, err)
}

func quotaDomainPostgresFixture(t *testing.T) *orderedQuotaPGFixture {
	t.Helper()
	f := orderedQuotaPostgresFixture(t)
	migrateBillingQuotaCacheDomain(t, f.db)
	return f
}

func quotaDomainPostgresNamespace(t *testing.T, db *sql.DB) string {
	t.Helper()
	var namespace string
	require.NoError(t, db.QueryRow(`SELECT cache_namespace::text FROM user_platform_quota_writer_authority WHERE id=1`).Scan(&namespace))
	return namespace
}

func TestBillingCacheQuotaDomainPostgresMigrationPreservesAuthorityAndAllocatesNamespace(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	var before, after string
	require.NoError(t, f.db.QueryRow(`SELECT to_jsonb(a)::text FROM user_platform_quota_writer_authority a WHERE id=1`).Scan(&before))
	migrateBillingQuotaCacheDomain(t, f.db)
	require.NoError(t, f.db.QueryRow(`SELECT (to_jsonb(a)-'cache_namespace')::text FROM user_platform_quota_writer_authority a WHERE id=1`).Scan(&after))
	require.JSONEq(t, before, after, "migration preserves mode, epoch and creation time")
	namespace := quotaDomainPostgresNamespace(t, f.db)
	domain := service.UserPlatformQuotaCacheDomain{SchemaVersion: service.UserPlatformQuotaCacheDomainSchemaV2, Namespace: namespace, WriterAuthority: f.authority}
	require.NoError(t, service.ValidateUserPlatformQuotaCacheDomain(domain))
	var uniqueCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM pg_constraint
		WHERE conrelid='user_platform_quota_writer_authority'::regclass
		AND contype='u' AND pg_get_constraintdef(oid)='UNIQUE (cache_namespace)'`).Scan(&uniqueCount))
	require.Equal(t, 1, uniqueCount, "the singleton also carries an explicit namespace uniqueness constraint")
	var notNull bool
	var defaultSQL string
	require.NoError(t, f.db.QueryRow(`SELECT a.attnotnull,pg_get_expr(d.adbin,d.adrelid)
		FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
		WHERE a.attrelid='user_platform_quota_writer_authority'::regclass AND a.attname='cache_namespace'`).Scan(&notNull, &defaultSQL))
	require.True(t, notNull)
	require.Equal(t, "gen_random_uuid()", defaultSQL)
	row := quotaWriterAuthorityDatabaseJSON(t, f.db)
	for _, query := range []string{
		`UPDATE user_platform_quota_writer_authority SET cache_namespace=gen_random_uuid() WHERE id=1`,
		`UPDATE user_platform_quota_writer_authority SET cache_namespace='00000000-0000-0000-0000-000000000000'::uuid WHERE id=1`,
		`UPDATE user_platform_quota_writer_authority SET cache_namespace=NULL WHERE id=1`,
		`UPDATE user_platform_quota_writer_authority SET epoch=epoch+1 WHERE id=1`,
		`UPDATE user_platform_quota_writer_authority SET created_at=clock_timestamp() WHERE id=1`,
		`DELETE FROM user_platform_quota_writer_authority WHERE id=1`,
	} {
		_, err := f.db.Exec(query)
		require.Error(t, err)
		require.Equal(t, row, quotaWriterAuthorityDatabaseJSON(t, f.db))
	}
}

func TestBillingCacheQuotaDomainPostgresFirstAuthorityInitializationUsesNamespaceDefault(t *testing.T) {
	f := quotaWriterAuthorityUninitializedFixture(t)
	migrateOrderedQuotaObservation(t, f)
	migrateBillingQuotaCacheDomain(t, f.db)
	before := quotaWriterAuthorityDatabaseJSON(t, f.db)
	c, observer, hook, _ := quotaDomainRedisFixture(t, "miniredis")
	got, err := ProvideBillingCache(c.rdb, f.db, quotaDomainTestDomain().WriterAuthority)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
	require.Nil(t, got)
	require.Equal(t, before, quotaWriterAuthorityDatabaseJSON(t, f.db), "cache construction never initializes authority")
	for _, namespace := range []any{nil, "00000000-0000-0000-0000-000000000000"} {
		_, err := f.db.Exec(`INSERT INTO user_platform_quota_writer_authority(id,mode,epoch,cache_namespace) VALUES(1,$1,1,$2)`, service.UserPlatformQuotaWriterModeDB, namespace)
		require.Error(t, err)
		require.Equal(t, before, quotaWriterAuthorityDatabaseJSON(t, f.db))
	}
	authority, err := NewUserPlatformQuotaWriterAuthorityRepository(f.db).InitializeUserPlatformQuotaWriterAuthority(context.Background(), service.UserPlatformQuotaWriterModeDB)
	require.NoError(t, err)
	require.Equal(t, quotaDomainTestDomain().WriterAuthority, authority)
	namespace := quotaDomainPostgresNamespace(t, f.db)
	row := quotaWriterAuthorityDatabaseJSON(t, f.db)
	got, err = ProvideBillingCache(c.rdb, f.db, authority)
	require.NoError(t, err)
	concrete, ok := got.(*billingCache)
	require.True(t, ok)
	require.NotNil(t, concrete.quotaDomain)
	require.Equal(t, namespace, concrete.quotaDomain.Namespace)
	require.Equal(t, row, quotaWriterAuthorityDatabaseJSON(t, f.db))
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
	require.Zero(t, observer.DBSize(context.Background()).Val())
}

func TestBillingCacheQuotaDomainPostgresProviderRestartPreservesDomainWithoutRedisAccess(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	c, observer, hook, userID := quotaDomainRedisFixture(t, "miniredis")
	legacy := NewBillingCache(observer).(*billingCache)
	require.NoError(t, legacy.SetUserPlatformQuotaCache(context.Background(), userID, "openai", quotaCacheCASEntry(), time.Minute))
	require.NoError(t, legacy.ReaddDirtyUserPlatformQuotaKeys(context.Background(), []service.UserPlatformQuotaKey{{UserID: userID, Platform: "openai"}}))
	keys := []string{userPlatformQuotaCacheKey(userID, "openai"), userPlatformQuotaDirtySetKey()}
	redisBefore := quotaDomainSnapshot(t, observer, keys)
	row := quotaWriterAuthorityDatabaseJSON(t, f.db)
	namespace := quotaDomainPostgresNamespace(t, f.db)
	for range 3 {
		provided, err := ProvideBillingCache(c.rdb, f.db, f.authority)
		require.NoError(t, err)
		concrete, ok := provided.(*billingCache)
		require.True(t, ok)
		require.Same(t, c.rdb, concrete.rdb)
		require.NotNil(t, concrete.quotaDomain)
		require.Equal(t, service.UserPlatformQuotaCacheDomain{SchemaVersion: service.UserPlatformQuotaCacheDomainSchemaV2, Namespace: namespace, WriterAuthority: f.authority}, *concrete.quotaDomain)
		quotaDomainAssertAllQuotaPortsClosed(t, concrete, context.Background(), userID)
		require.Equal(t, row, quotaWriterAuthorityDatabaseJSON(t, f.db), "restart must not even rewrite an equal authority tuple")
	}
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
	require.Equal(t, redisBefore, quotaDomainSnapshot(t, observer, keys))
}

func TestBillingCacheQuotaDomainPostgresProviderRejectsMissingDependenciesAndAuthority(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	c, _, hook, _ := quotaDomainRedisFixture(t, "miniredis")
	row := quotaWriterAuthorityDatabaseJSON(t, f.db)
	for _, tc := range []struct {
		name      string
		authority service.UserPlatformQuotaWriterAuthority
		want      error
	}{
		{"wrong-fixed-epoch", service.UserPlatformQuotaWriterAuthority{Mode: f.authority.Mode, Epoch: f.authority.Epoch + 1}, service.ErrUserPlatformQuotaWriterMismatch},
		{"zero-epoch", service.UserPlatformQuotaWriterAuthority{Mode: f.authority.Mode}, service.ErrUserPlatformQuotaWriterUnavailable},
		{"snapshot-mode", service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeSnapshot, Epoch: f.authority.Epoch}, service.ErrUserPlatformQuotaWriterModeUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ProvideBillingCache(c.rdb, f.db, tc.authority)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, got)
			require.Equal(t, row, quotaWriterAuthorityDatabaseJSON(t, f.db))
		})
	}
	got, err := ProvideBillingCache(nil, f.db, f.authority)
	require.Error(t, err)
	require.Nil(t, got)
	got, err = ProvideBillingCache(c.rdb, nil, f.authority)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, row, quotaWriterAuthorityDatabaseJSON(t, f.db))
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
}

func TestBillingCacheQuotaDomainPostgresProviderRejectsCorruptNamespaceWithoutRepair(t *testing.T) {
	f := quotaDomainPostgresFixture(t)
	c, _, hook, _ := quotaDomainRedisFixture(t, "miniredis")
	// Deliberately corrupt only this disposable schema to exercise fail-closed
	// reading independently of the production trigger/check protections.
	_, err := f.db.Exec(`ALTER TABLE user_platform_quota_writer_authority DISABLE TRIGGER user_platform_quota_writer_authority_immutable`)
	require.NoError(t, err)
	_, err = f.db.Exec(`ALTER TABLE user_platform_quota_writer_authority DROP CONSTRAINT user_platform_quota_cache_namespace_nonzero`)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE user_platform_quota_writer_authority SET cache_namespace='00000000-0000-0000-0000-000000000000'::uuid WHERE id=1`)
	require.NoError(t, err)
	row := quotaWriterAuthorityDatabaseJSON(t, f.db)
	got, err := ProvideBillingCache(c.rdb, f.db, f.authority)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaCacheDomainInvalid)
	require.Nil(t, got)
	require.Equal(t, row, quotaWriterAuthorityDatabaseJSON(t, f.db))
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
}

func TestBillingCacheQuotaDomainPostgresProviderRejectsUnmigratedSchemaWithoutWrites(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	c, _, hook, _ := quotaDomainRedisFixture(t, "miniredis")
	row := quotaWriterAuthorityDatabaseJSON(t, f.db)
	got, err := ProvideBillingCache(c.rdb, f.db, f.authority)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, row, quotaWriterAuthorityDatabaseJSON(t, f.db))
	require.Zero(t, hook.commands.Load())
	require.Zero(t, hook.pipelines.Load())
}
