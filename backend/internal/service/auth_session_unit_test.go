//go:build unit

package service

import "context"

// These fixtures exercise service behavior; repository tests own durable storage.
type authSessionCreateStub struct {
	AuthSessionRepository
}

func (r *authSessionCreateStub) Create(_ context.Context, session *AuthSession, _, _, _ string) (*AuthSession, error) {
	copy := *session
	copy.Epoch = 1
	return &copy, nil
}

func (r *authSessionCreateStub) RevokeAll(_ context.Context, _ int64) (int64, error) {
	return 2, nil
}

func withUnitAuthSessions(svc *AuthService) *AuthService {
	svc.SetAuthSessionRepository(&authSessionCreateStub{})
	return svc
}
