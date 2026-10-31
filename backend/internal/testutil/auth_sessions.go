//go:build unit

package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// MemoryAuthSessions is a deterministic session authority for unit fixtures.
// PostgreSQL transaction and security-update behavior is tested by repository tests.
type MemoryAuthSessions struct {
	mu       sync.Mutex
	sessions map[string]service.AuthSession
	refresh  map[string]string
	spent    map[string]bool
	epochs   map[int64]int64
	ReadErr  error
}

func NewAuthSessionRepository() *MemoryAuthSessions {
	return &MemoryAuthSessions{sessions: map[string]service.AuthSession{}, refresh: map[string]string{}, spent: map[string]bool{}, epochs: map[int64]int64{}}
}

func (r *MemoryAuthSessions) Create(_ context.Context, in *service.AuthSession, refreshHash, _, _ string) (*service.AuthSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	session := *in
	if r.epochs[session.UserID] == 0 {
		r.epochs[session.UserID] = 1
	}
	if session.Epoch > 0 && session.Epoch != r.epochs[session.UserID] {
		return nil, service.ErrTokenRevoked
	}
	session.Epoch = r.epochs[session.UserID]
	r.sessions[session.SessionID] = session
	if refreshHash != "" {
		r.refresh[refreshHash] = session.SessionID
	}
	return &session, nil
}

func (r *MemoryAuthSessions) Get(_ context.Context, userID int64, sid string, epoch int64) (*service.AuthSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ReadErr != nil {
		return nil, r.ReadErr
	}
	session, ok := r.sessions[sid]
	if !ok || session.UserID != userID || epoch != session.Epoch || epoch != r.epochs[userID] || session.RevokedAt != nil {
		return nil, service.ErrTokenRevoked
	}
	if !time.Now().Before(session.ExpiresAt) {
		return nil, service.ErrTokenExpired
	}
	return &session, nil
}

func (r *MemoryAuthSessions) Rotate(_ context.Context, oldHash, newHash, bindingHash string, enforceBinding bool) (*service.AuthSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sid, ok := r.refresh[oldHash]
	if !ok {
		return nil, service.ErrRefreshTokenInvalid
	}
	session := r.sessions[sid]
	if r.spent[oldHash] {
		now := time.Now()
		session.RevokedAt = &now
		r.sessions[sid] = session
		return nil, service.ErrRefreshTokenReused
	}
	if session.RevokedAt != nil || session.Epoch != r.epochs[session.UserID] {
		return nil, service.ErrTokenRevoked
	}
	if !time.Now().Before(session.ExpiresAt) {
		return nil, service.ErrRefreshTokenExpired
	}
	if enforceBinding && session.BindingHash != "" && session.BindingHash != bindingHash {
		now := time.Now()
		session.RevokedAt = &now
		r.sessions[sid] = session
		return nil, service.ErrSessionBindingMismatch
	}
	r.spent[oldHash] = true
	r.refresh[newHash] = sid
	return &session, nil
}

func (r *MemoryAuthSessions) RevokeSession(_ context.Context, sid string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if session, ok := r.sessions[sid]; ok {
		now := time.Now()
		session.RevokedAt = &now
		r.sessions[sid] = session
	}
	return nil
}

func (r *MemoryAuthSessions) RevokeUserSession(ctx context.Context, userID int64, sid string) error {
	r.mu.Lock()
	session, ok := r.sessions[sid]
	r.mu.Unlock()
	if !ok || session.UserID != userID {
		return service.ErrTokenRevoked
	}
	return r.RevokeSession(ctx, sid)
}

func (r *MemoryAuthSessions) RevokeRefresh(ctx context.Context, hash string) error {
	r.mu.Lock()
	sid := r.refresh[hash]
	r.mu.Unlock()
	return r.RevokeSession(ctx, sid)
}

func (r *MemoryAuthSessions) RevokeAll(_ context.Context, userID int64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.epochs[userID] == 0 {
		r.epochs[userID] = 1
	}
	r.epochs[userID]++
	return r.epochs[userID], nil
}

func (r *MemoryAuthSessions) List(_ context.Context, userID int64) ([]service.AuthSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var sessions []service.AuthSession
	for _, session := range r.sessions {
		if session.UserID == userID && session.Epoch == r.epochs[userID] && session.RevokedAt == nil && time.Now().Before(session.ExpiresAt) {
			sessions = append(sessions, session)
		}
	}
	return sessions, nil
}
