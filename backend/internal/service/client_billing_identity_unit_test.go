//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestClientBillingIdentityMarkerRejectedBeforeAuthCachesAndRepository(t *testing.T) {
	cache := &authCacheStub{getAuthCache: func(context.Context, string) (*APIKeyAuthCacheEntry, error) {
		t.Fatal("internal accounting marker reached auth cache")
		return nil, nil
	}}
	repo := &authRepoStub{getByKeyForAuth: func(context.Context, string) (*APIKey, error) {
		t.Fatal("internal accounting marker reached auth repository")
		return nil, nil
	}}
	svc := NewAPIKeyService(repo, nil, nil, nil, nil, cache, &config.Config{APIKeyAuth: config.APIKeyAuthCacheConfig{L2TTLSeconds: 60}})
	_, err := svc.GetByKey(context.Background(), InternalAPIKeyPrefix+"known-database-marker")
	require.ErrorIs(t, err, ErrAPIKeyNotFound)
}

func TestClientBillingIdentityRejectedByAuthSnapshotBoundaries(t *testing.T) {
	svc := &APIKeyService{}
	consumerID := "consumer-test"
	for _, key := range []*APIKey{
		{ID: 1, Kind: APIKeyKindClientConsumer, ClientConsumerID: &consumerID, User: &User{ID: 2}},
		{ID: 1, Kind: APIKeyKindCredential, ClientConsumerID: &consumerID, User: &User{ID: 2}},
		{ID: 1, Kind: APIKeyKindCredential, Key: InternalAPIKeyPrefix + "marker", User: &User{ID: 2}},
		{ID: 1, Kind: "future-internal-kind", User: &User{ID: 2}},
	} {
		require.Nil(t, svc.snapshotFromAPIKey(context.Background(), key))
	}
	for _, kind := range []string{APIKeyKindClientConsumer, "future-internal-kind"} {
		key, used, err := svc.applyAuthCacheEntry("sk-known", &APIKeyAuthCacheEntry{Snapshot: &APIKeyAuthSnapshot{
			Version: apiKeyAuthSnapshotVersion, Kind: kind, APIKeyID: 1, UserID: 2,
		}})
		require.True(t, used)
		require.Nil(t, key)
		require.ErrorIs(t, err, ErrAPIKeyNotFound)
	}
}
