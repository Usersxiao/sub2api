package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type authSessionRepository struct{ db *sql.DB }

func NewAuthSessionRepository(db *sql.DB) service.AuthSessionRepository {
	return &authSessionRepository{db: db}
}

// All mutations acquire the user row before the session/token rows. The same
// lock is taken by user security updates and their epoch trigger.
type sessionAccount struct {
	epoch                       int64
	email, passwordHash, status string
	deletedAt                   sql.NullTime
}

func lockSessionAccount(ctx context.Context, tx *sql.Tx, userID int64) (*sessionAccount, error) {
	var a sessionAccount
	err := tx.QueryRowContext(ctx, `SELECT session_epoch,email,password_hash,status,deleted_at FROM users WHERE id=$1 FOR UPDATE`, userID).
		Scan(&a.epoch, &a.email, &a.passwordHash, &a.status, &a.deletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrTokenRevoked
	}
	if err != nil {
		return nil, err
	}
	if a.deletedAt.Valid {
		return nil, service.ErrTokenRevoked
	}
	if a.status != service.StatusActive {
		return nil, service.ErrUserNotActive
	}
	return &a, nil
}

const sessionColumns = `s.sid,s.user_id,s.session_epoch,s.token_version,s.binding_hash,s.created_at,s.expires_at,s.revoked_at`

type sessionScanner interface{ Scan(...any) error }

func scanAuthSession(row sessionScanner) (*service.AuthSession, error) {
	var s service.AuthSession
	var revoked sql.NullTime
	err := row.Scan(&s.SessionID, &s.UserID, &s.Epoch, &s.TokenVersion, &s.BindingHash, &s.CreatedAt, &s.ExpiresAt, &revoked)
	if err != nil {
		return nil, err
	}
	if revoked.Valid {
		s.RevokedAt = &revoked.Time
	}
	return &s, nil
}

func (r *authSessionRepository) Create(ctx context.Context, in *service.AuthSession, refreshHash, expectedEmail, expectedPasswordHash string) (*service.AuthSession, error) {
	if in == nil || in.UserID <= 0 || in.SessionID == "" || !in.ExpiresAt.After(in.CreatedAt) {
		return nil, service.ErrInvalidToken
	}
	if in.Epoch <= 0 {
		return nil, service.ErrTokenRevoked
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	a, err := lockSessionAccount(ctx, tx, in.UserID)
	if err != nil {
		return nil, err
	}
	if a.epoch != in.Epoch || a.email != expectedEmail || a.passwordHash != expectedPasswordHash {
		return nil, service.ErrTokenRevoked
	}
	s, err := scanAuthSession(tx.QueryRowContext(ctx, `INSERT INTO auth_sessions AS s
		(sid,user_id,session_epoch,token_version,binding_hash,created_at,expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING `+sessionColumns,
		in.SessionID, in.UserID, a.epoch, in.TokenVersion, in.BindingHash, in.CreatedAt, in.ExpiresAt))
	if err != nil {
		return nil, err
	}
	if refreshHash != "" {
		_, err = tx.ExecContext(ctx, `INSERT INTO auth_refresh_tokens(token_hash,sid,expires_at) VALUES($1,$2,$3)`, refreshHash, s.SessionID, s.ExpiresAt)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s, nil
}

func (r *authSessionRepository) Get(ctx context.Context, userID int64, sid string, epoch int64) (*service.AuthSession, error) {
	var currentEpoch int64
	var status string
	var deleted sql.NullTime
	// A single statement produces a consistent authority snapshot. A missing row,
	// including a pre-upgrade sid, never acquires a new session implicitly.
	row := r.db.QueryRowContext(ctx, `SELECT `+sessionColumns+`,u.session_epoch,u.status,u.deleted_at
		FROM auth_sessions s JOIN users u ON u.id=s.user_id WHERE s.sid=$1 AND s.user_id=$2`, sid, userID)
	var s service.AuthSession
	var revoked sql.NullTime
	err := row.Scan(&s.SessionID, &s.UserID, &s.Epoch, &s.TokenVersion, &s.BindingHash, &s.CreatedAt, &s.ExpiresAt, &revoked, &currentEpoch, &status, &deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrTokenRevoked
	}
	if err != nil {
		return nil, err
	}
	if status != service.StatusActive {
		return nil, service.ErrUserNotActive
	}
	if deleted.Valid || revoked.Valid || epoch <= 0 || s.Epoch != epoch || currentEpoch != epoch {
		return nil, service.ErrTokenRevoked
	}
	if !time.Now().Before(s.ExpiresAt) {
		return nil, service.ErrTokenExpired
	}
	return &s, nil
}

func (r *authSessionRepository) Rotate(ctx context.Context, oldHash, newHash, bindingHash string, enforceBinding bool) (*service.AuthSession, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var userID int64
	var sid string
	err = tx.QueryRowContext(ctx, `SELECT s.user_id,s.sid FROM auth_refresh_tokens t JOIN auth_sessions s ON s.sid=t.sid WHERE t.token_hash=$1`, oldHash).Scan(&userID, &sid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrRefreshTokenInvalid
	}
	if err != nil {
		return nil, err
	}
	a, err := lockSessionAccount(ctx, tx, userID)
	if err != nil {
		return nil, err
	}
	s, err := scanAuthSession(tx.QueryRowContext(ctx, `SELECT `+sessionColumns+` FROM auth_sessions s WHERE s.sid=$1 FOR UPDATE`, sid))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrRefreshTokenInvalid
	}
	if err != nil {
		return nil, err
	}
	if s.RevokedAt != nil || s.Epoch != a.epoch {
		return nil, service.ErrTokenRevoked
	}
	if !time.Now().Before(s.ExpiresAt) {
		return nil, service.ErrRefreshTokenExpired
	}
	var consumed sql.NullTime
	var expiresAt time.Time
	err = tx.QueryRowContext(ctx, `SELECT consumed_at,expires_at FROM auth_refresh_tokens WHERE token_hash=$1 FOR UPDATE`, oldHash).Scan(&consumed, &expiresAt)
	if err != nil {
		return nil, err
	}
	denyAndRevoke := func(reason error) (*service.AuthSession, error) {
		if _, err := tx.ExecContext(ctx, `UPDATE auth_sessions SET revoked_at=clock_timestamp() WHERE sid=$1`, sid); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, reason
	}
	if consumed.Valid {
		return denyAndRevoke(service.ErrRefreshTokenReused)
	}
	if !time.Now().Before(expiresAt) {
		return nil, service.ErrRefreshTokenExpired
	}
	if enforceBinding && s.BindingHash != "" && s.BindingHash != bindingHash {
		return denyAndRevoke(service.ErrSessionBindingMismatch)
	}
	if oldHash == newHash {
		return nil, service.ErrRefreshTokenInvalid
	}
	// Insert first; any unique/check/write failure rolls back both the old-token
	// consumption and the replacement. No partial rotation can be acknowledged.
	_, err = tx.ExecContext(ctx, `INSERT INTO auth_refresh_tokens(token_hash,sid,expires_at) VALUES($1,$2,$3)`, newHash, sid, s.ExpiresAt)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE auth_refresh_tokens SET consumed_at=clock_timestamp(),replaced_by_hash=$2 WHERE token_hash=$1`, oldHash, newHash)
	if err != nil {
		return nil, err
	}
	if bindingHash != s.BindingHash {
		_, err = tx.ExecContext(ctx, `UPDATE auth_sessions SET binding_hash=$2 WHERE sid=$1`, sid, bindingHash)
		if err != nil {
			return nil, err
		}
		s.BindingHash = bindingHash
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s, nil
}

func (r *authSessionRepository) RevokeSession(ctx context.Context, sid string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var userID int64
	err = tx.QueryRowContext(ctx, `SELECT user_id FROM auth_sessions WHERE sid=$1`, sid).Scan(&userID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	// Do not require an active account: logout must also succeed after disable.
	if _, err = tx.ExecContext(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, userID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE auth_sessions SET revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE sid=$1`, sid); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *authSessionRepository) RevokeRefresh(ctx context.Context, hash string) error {
	var sid string
	err := r.db.QueryRowContext(ctx, `SELECT sid FROM auth_refresh_tokens WHERE token_hash=$1`, hash).Scan(&sid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.RevokeSession(ctx, sid)
}

func (r *authSessionRepository) RevokeUserSession(ctx context.Context, userID int64, sid string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, userID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE auth_sessions SET revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE sid=$1 AND user_id=$2`, sid, userID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return service.ErrTokenRevoked
	}
	return tx.Commit()
}

func (r *authSessionRepository) RevokeAll(ctx context.Context, userID int64) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var epoch int64
	err = tx.QueryRowContext(ctx, `UPDATE users SET session_epoch=session_epoch+1 WHERE id=$1 RETURNING session_epoch`, userID).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, service.ErrUserNotFound
	}
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE auth_sessions SET revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE user_id=$1`, userID); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return epoch, nil
}

func (r *authSessionRepository) List(ctx context.Context, userID int64) ([]service.AuthSession, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+sessionColumns+` FROM auth_sessions s JOIN users u ON u.id=s.user_id
		WHERE s.user_id=$1 AND s.session_epoch=u.session_epoch AND s.revoked_at IS NULL AND s.expires_at>clock_timestamp()
		AND u.deleted_at IS NULL AND u.status='active' ORDER BY s.created_at DESC,s.sid LIMIT 100`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sessions := make([]service.AuthSession, 0)
	for rows.Next() {
		s, err := scanAuthSession(rows)
		if err != nil {
			return nil, fmt.Errorf("read session: %w", err)
		}
		sessions = append(sessions, *s)
	}
	return sessions, rows.Err()
}
