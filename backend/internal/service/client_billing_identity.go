package service

import "context"

// ClientBillingIdentityRepository resolves a real, non-authenticatable accounting
// row after the caller validates the current grant and resolves its exact group.
// Rows are stable across grant/login rotation; no API-key credential is returned.
type ClientBillingIdentityRepository interface {
	Resolve(ctx context.Context, grant *ClientGatewayGrant, groupID int64) (*APIKey, error)
}
