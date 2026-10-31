package dto

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestClientBillingIdentityOmittedFromAPIKeyAndUserDTO(t *testing.T) {
	consumerID := "private-consumer-id"
	internal := service.APIKey{ID: 23, Kind: service.APIKeyKindClientConsumer, ClientConsumerID: &consumerID,
		Key: service.InternalAPIKeyPrefix + "private-marker", Name: "private-ledger-name"}
	require.Nil(t, APIKeyFromService(&internal))
	u := UserFromService(&service.User{ID: 1, APIKeys: []service.APIKey{internal, {
		ID: 24, Kind: service.APIKeyKindCredential, Name: "Visible key", Key: "sk-visible",
	}}})
	require.Len(t, u.APIKeys, 1)
	require.EqualValues(t, 24, u.APIKeys[0].ID)
	encoded, err := json.Marshal(u)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private-")
	for _, malformed := range []*service.APIKey{
		{Kind: service.APIKeyKindCredential, ClientConsumerID: &consumerID},
		{Kind: service.APIKeyKindCredential, Key: service.InternalAPIKeyPrefix + "marker"},
		{Kind: "unknown-internal-kind"},
	} {
		require.Nil(t, APIKeyFromService(malformed))
	}
}
