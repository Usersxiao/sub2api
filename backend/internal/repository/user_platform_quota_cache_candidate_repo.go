package repository

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.UserPlatformQuotaCacheCandidateRepository = (*userPlatformQuotaRepository)(nil)

// GetUserPlatformQuotaCacheCandidate validates an exact database observation
// and its persisted cache domain from one statement snapshot. A candidate is
// data for a future handoff; it certifies no coverage or Redis publication right.
func (r *userPlatformQuotaRepository) GetUserPlatformQuotaCacheCandidate(ctx context.Context, userID int64, platform string) (*service.UserPlatformQuotaCacheCandidate, error) {
	observation, domain, err := r.readUserPlatformQuotaOrderedObservation(ctx, userID, platform, true)
	if err != nil {
		return nil, err
	}
	if observation == nil {
		// The shared reader has already checked authority and namespace. Do not
		// invent a cached absence sentinel or a zero-usage baseline here.
		return nil, nil
	}
	return service.PrepareUserPlatformQuotaCacheCandidate(*domain, observation)
}
