package service

import (
	"context"
	"time"
)

// AuthSession is a durable login, distinct from an individual access/refresh token.
// Refresh rotation never extends ExpiresAt; token hashes and credentials are not DTOs.
type AuthSession struct {
	SessionID    string
	UserID       int64
	Epoch        int64
	TokenVersion int64
	BindingHash  string
	CreatedAt    time.Time
	ExpiresAt    time.Time
	RevokedAt    *time.Time
}

// AuthSessionRepository is the authoritative session ledger. Implementations must
// serialize issuance, refresh, user-security updates and revocation per account.
type AuthSessionRepository interface {
	// Create must match the authenticated snapshot's Epoch and credentials against
	// the locked user row; reading a newer epoch must not adopt a stale login.
	Create(ctx context.Context, session *AuthSession, refreshHash, expectedEmail, expectedPasswordHash string) (*AuthSession, error)
	Get(ctx context.Context, userID int64, sid string, epoch int64) (*AuthSession, error)
	Rotate(ctx context.Context, oldHash, newHash, bindingHash string, enforceBinding bool) (*AuthSession, error)
	RevokeSession(ctx context.Context, sid string) error
	RevokeUserSession(ctx context.Context, userID int64, sid string) error
	RevokeRefresh(ctx context.Context, hash string) error
	RevokeAll(ctx context.Context, userID int64) (int64, error)
	List(ctx context.Context, userID int64) ([]AuthSession, error)
}
