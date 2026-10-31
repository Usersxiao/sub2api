//go:build unit

package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// Uses an explicitly supplied disposable PostgreSQL database, with an isolated
// schema per test. There is no default connection to development/production data.
func authSessionIsolatedDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("AUTH_SESSION_TEST_DSN")
	if dsn == "" {
		t.Skip("set AUTH_SESSION_TEST_DSN to a disposable PostgreSQL database")
	}
	base, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	schema := fmt.Sprintf("auth_session_test_%d", time.Now().UnixNano())
	_, err = base.Exec(`CREATE SCHEMA ` + schema)
	require.NoError(t, err)
	db, err := sql.Open("postgres", dsn+" search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(); _, _ = base.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); _ = base.Close() })
	return db
}

func authSessionTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := authSessionIsolatedDB(t)
	_, err := db.Exec(`CREATE TABLE users (
		id BIGSERIAL PRIMARY KEY,email TEXT NOT NULL,password_hash TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'user',status TEXT NOT NULL DEFAULT 'active',
		deleted_at TIMESTAMPTZ,totp_secret_encrypted TEXT,totp_enabled BOOLEAN NOT NULL DEFAULT FALSE);
		CREATE TABLE auth_identities (
		id BIGSERIAL PRIMARY KEY,user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		provider_type TEXT NOT NULL,provider_key TEXT NOT NULL,provider_subject TEXT NOT NULL,
		verified_at TIMESTAMPTZ,issuer TEXT,metadata JSONB NOT NULL DEFAULT '{}');
		CREATE TABLE auth_identity_channels (
		id BIGSERIAL PRIMARY KEY,identity_id BIGINT NOT NULL REFERENCES auth_identities(id) ON DELETE CASCADE,
		provider_type TEXT NOT NULL,provider_key TEXT NOT NULL,channel TEXT NOT NULL,
		channel_app_id TEXT NOT NULL,channel_subject TEXT NOT NULL,metadata JSONB NOT NULL DEFAULT '{}');`)
	require.NoError(t, err)
	for _, name := range []string{"191_passkey_credentials.sql", "248_auth_sessions.sql"} {
		migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		require.NoError(t, err)
		_, err = db.Exec(string(migration))
		require.NoError(t, err, name)
	}
	return db
}

func sessionFixtureUser(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`INSERT INTO users(email,password_hash) VALUES('session@example.test','fixture-password-hash') RETURNING id`).Scan(&id)
	require.NoError(t, err)
	return id
}

func sessionHash(value string) string {
	s := sha256.Sum256([]byte(value))
	return hex.EncodeToString(s[:])
}

func createSessionFixture(t *testing.T, db *sql.DB, repo service.AuthSessionRepository, userID int64, sid string) *service.AuthSession {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	var epoch int64
	require.NoError(t, db.QueryRow(`SELECT session_epoch FROM users WHERE id=$1`, userID).Scan(&epoch))
	s, err := repo.Create(context.Background(), &service.AuthSession{
		SessionID: sid, UserID: userID, Epoch: epoch, TokenVersion: 17, BindingHash: "fixture-binding", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}, sessionHash(sid+"-token"), "session@example.test", "fixture-password-hash")
	require.NoError(t, err)
	return s
}

func TestAuthSessionPostgresRotationIsAtomicAndReplayRevokes(t *testing.T) {
	db := authSessionTestDB(t)
	repo := NewAuthSessionRepository(db)
	userID := sessionFixtureUser(t, db)
	s := createSessionFixture(t, db, repo, userID, "concurrent")
	const concurrency = 8
	start := make(chan struct{})
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := repo.Rotate(context.Background(), sessionHash("concurrent-token"), sessionHash(fmt.Sprintf("replacement-%d", i)), "fixture-binding", true)
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	success, replay := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			success++
		case err == service.ErrRefreshTokenReused:
			replay++
		default:
			require.ErrorIs(t, err, service.ErrTokenRevoked)
		}
	}
	require.Equal(t, 1, success, "one old token creates at most one successor")
	require.Equal(t, 1, replay, "the first duplicate consumes the durable replay evidence")
	_, err := repo.Get(context.Background(), userID, s.SessionID, s.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked, "a replay also denies the winning access token")
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM auth_refresh_tokens`).Scan(&count))
	require.Equal(t, 2, count)
	var replacement string
	require.NoError(t, db.QueryRow(`SELECT replaced_by_hash FROM auth_refresh_tokens WHERE token_hash=$1`, sessionHash("concurrent-token")).Scan(&replacement))
	_, err = repo.Rotate(context.Background(), replacement, sessionHash("third"), "fixture-binding", true)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
}

func TestAuthSessionPostgresRotationRollbackAndAbsoluteExpiry(t *testing.T) {
	db := authSessionTestDB(t)
	repo := NewAuthSessionRepository(db)
	userID := sessionFixtureUser(t, db)
	s := createSessionFixture(t, db, repo, userID, "rollback")
	createSessionFixture(t, db, repo, userID, "occupied")
	ctx := context.Background()
	_, err := repo.Rotate(ctx, sessionHash("rollback-token"), sessionHash("occupied-token"), "fixture-binding", true)
	require.Error(t, err, "a replacement hash collision must roll back consumption")
	var consumed sql.NullTime
	require.NoError(t, db.QueryRow(`SELECT consumed_at FROM auth_refresh_tokens WHERE token_hash=$1`, sessionHash("rollback-token")).Scan(&consumed))
	require.False(t, consumed.Valid)
	rotated, err := repo.Rotate(ctx, sessionHash("rollback-token"), sessionHash("valid-replacement"), "fixture-binding", true)
	require.NoError(t, err)
	require.Equal(t, s.ExpiresAt, rotated.ExpiresAt)
	require.Equal(t, s.Epoch, rotated.Epoch)
	var expires time.Time
	require.NoError(t, db.QueryRow(`SELECT expires_at FROM auth_refresh_tokens WHERE token_hash=$1`, sessionHash("valid-replacement")).Scan(&expires))
	require.Equal(t, s.ExpiresAt, expires)
	_, err = repo.Get(ctx, userID+1, s.SessionID, s.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
}

func TestAuthSessionPostgresCurrentAndAllRevocation(t *testing.T) {
	db := authSessionTestDB(t)
	repo := NewAuthSessionRepository(db)
	userID := sessionFixtureUser(t, db)
	ctx := context.Background()
	a := createSessionFixture(t, db, repo, userID, "device-a")
	b := createSessionFixture(t, db, repo, userID, "device-b")
	require.NoError(t, repo.RevokeRefresh(ctx, sessionHash("device-a-token")))
	_, err := repo.Get(ctx, userID, a.SessionID, a.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
	_, err = repo.Get(ctx, userID, b.SessionID, b.Epoch)
	require.NoError(t, err)
	items, err := repo.List(ctx, userID)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, b.SessionID, items[0].SessionID)
	epoch, err := repo.RevokeAll(ctx, userID)
	require.NoError(t, err)
	require.Greater(t, epoch, b.Epoch)
	_, err = repo.Get(ctx, userID, b.SessionID, b.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
	c := createSessionFixture(t, db, repo, userID, "device-c")
	require.Equal(t, epoch, c.Epoch)
	_, err = repo.Get(ctx, userID, c.SessionID, c.Epoch)
	require.NoError(t, err)
	_, err = repo.Get(ctx, userID, c.SessionID, 0)
	require.ErrorIs(t, err, service.ErrTokenRevoked, "legacy tokens need sign-in")
	otherID := sessionFixtureUser(t, db)
	require.ErrorIs(t, repo.RevokeUserSession(ctx, otherID, c.SessionID), service.ErrTokenRevoked)
	_, err = repo.Get(ctx, userID, c.SessionID, c.Epoch)
	require.NoError(t, err, "another owner cannot revoke this session")
	require.NoError(t, repo.RevokeUserSession(ctx, userID, c.SessionID))
	_, err = repo.Get(ctx, userID, c.SessionID, c.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
}

func TestAuthSessionPostgresDeviceRevokeBeyondListWindow(t *testing.T) {
	db := authSessionTestDB(t)
	repo := NewAuthSessionRepository(db)
	userID := sessionFixtureUser(t, db)
	ctx := context.Background()
	old := createSessionFixture(t, db, repo, userID, "oldest-device")
	_, err := db.Exec(`INSERT INTO auth_sessions(sid,user_id,session_epoch,token_version,created_at,expires_at)
		SELECT 'device-'||i,$1,$2,17,clock_timestamp(),clock_timestamp()+INTERVAL '1 hour' FROM generate_series(1,101) i`, userID, old.Epoch)
	require.NoError(t, err)
	items, err := repo.List(ctx, userID)
	require.NoError(t, err)
	require.Len(t, items, 100)
	for _, item := range items {
		require.NotEqual(t, old.SessionID, item.SessionID)
	}
	require.NoError(t, repo.RevokeUserSession(ctx, userID, old.SessionID))
	_, err = repo.Get(ctx, userID, old.SessionID, old.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
}

func TestAuthSessionPostgresSecurityChangesInvalidate(t *testing.T) {
	for name, mutation := range map[string]string{
		"email": `email='changed@example.test'`, "password": `password_hash='changed-hash'`, "role": `role='admin'`,
		"disable": `status='disabled'`, "delete": `deleted_at=clock_timestamp()`,
		"totp-secret": `totp_secret_encrypted='new-secret'`, "totp-enable": `totp_enabled=TRUE`,
	} {
		t.Run(name, func(t *testing.T) {
			db := authSessionTestDB(t)
			repo := NewAuthSessionRepository(db)
			userID := sessionFixtureUser(t, db)
			ctx := context.Background()
			if name == "totp-secret" {
				_, err := db.Exec(`UPDATE users SET totp_enabled=TRUE WHERE id=$1`, userID)
				require.NoError(t, err)
			}
			s := createSessionFixture(t, db, repo, userID, "security")
			_, err := db.Exec(`UPDATE users SET `+mutation+` WHERE id=$1`, userID)
			require.NoError(t, err)
			var epoch int64
			require.NoError(t, db.QueryRow(`SELECT session_epoch FROM users WHERE id=$1`, userID).Scan(&epoch))
			require.Greater(t, epoch, s.Epoch)
			_, err = repo.Get(ctx, userID, s.SessionID, s.Epoch)
			require.Error(t, err)
			_, err = repo.Rotate(ctx, sessionHash("security-token"), sessionHash("after-security"), "fixture-binding", true)
			require.Error(t, err)
			if name == "disable" {
				_, err = db.Exec(`UPDATE users SET status='active' WHERE id=$1`, userID)
				require.NoError(t, err)
				_, err = repo.Get(ctx, userID, s.SessionID, s.Epoch)
				require.ErrorIs(t, err, service.ErrTokenRevoked)
			}
		})
	}
}

func TestAuthSessionPostgresTotpSetupKeepsConfirmationSession(t *testing.T) {
	db := authSessionTestDB(t)
	repo := NewAuthSessionRepository(db)
	userID := sessionFixtureUser(t, db)
	ctx := context.Background()
	s := createSessionFixture(t, db, repo, userID, "totp-setup")
	_, err := db.Exec(`UPDATE users SET totp_secret_encrypted='pending-secret' WHERE id=$1`, userID)
	require.NoError(t, err)
	_, err = repo.Get(ctx, userID, s.SessionID, s.Epoch)
	require.NoError(t, err, "unconfirmed setup retains the session needed for the confirmation request")
	_, err = db.Exec(`UPDATE users SET totp_enabled=TRUE WHERE id=$1`, userID)
	require.NoError(t, err)
	_, err = repo.Get(ctx, userID, s.SessionID, s.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked, "enabling the factor invalidates old authentication")
	active := createSessionFixture(t, db, repo, userID, "totp-active")
	_, err = db.Exec(`UPDATE users SET totp_secret_encrypted='replacement-secret' WHERE id=$1`, userID)
	require.NoError(t, err)
	_, err = repo.Get(ctx, userID, active.SessionID, active.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked, "replacing an active factor invalidates authentication")
}

func TestAuthSessionPostgresStaleLoginAndBindingFailure(t *testing.T) {
	db := authSessionTestDB(t)
	repo := NewAuthSessionRepository(db)
	userID := sessionFixtureUser(t, db)
	ctx := context.Background()
	s := createSessionFixture(t, db, repo, userID, "binding")
	_, err := repo.Rotate(ctx, sessionHash("binding-token"), sessionHash("other-binding"), "changed-binding", true)
	require.ErrorIs(t, err, service.ErrSessionBindingMismatch)
	_, err = repo.Get(ctx, userID, s.SessionID, s.Epoch)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
	_, err = db.Exec(`UPDATE users SET password_hash='new-password-hash' WHERE id=$1`, userID)
	require.NoError(t, err)
	_, err = repo.Create(ctx, &service.AuthSession{SessionID: "stale", UserID: userID, Epoch: s.Epoch, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}, sessionHash("stale"), "session@example.test", "fixture-password-hash")
	require.ErrorIs(t, err, service.ErrTokenRevoked)
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM auth_sessions WHERE sid='stale'`).Scan(&count))
	require.Zero(t, count)
}

func TestAuthSessionPostgresStaleEpochCannotIssueAfterSecurityEvent(t *testing.T) {
	for _, event := range []string{"revoke-all", "totp"} {
		t.Run(event, func(t *testing.T) {
			db := authSessionTestDB(t)
			repo := NewAuthSessionRepository(db)
			userID := sessionFixtureUser(t, db)
			ctx := context.Background()
			authenticated := createSessionFixture(t, db, repo, userID, "authenticated")
			if event == "revoke-all" {
				_, err := repo.RevokeAll(ctx, userID)
				require.NoError(t, err)
			} else {
				_, err := db.Exec(`UPDATE users SET totp_enabled=TRUE WHERE id=$1`, userID)
				require.NoError(t, err)
			}
			stale := *authenticated
			stale.SessionID = "stale-epoch"
			_, err := repo.Create(ctx, &stale, sessionHash("stale-epoch"), "session@example.test", "fixture-password-hash")
			require.ErrorIs(t, err, service.ErrTokenRevoked, "the same password must not silently adopt the new security epoch")
			stale.SessionID = "missing-epoch"
			stale.Epoch = 0
			_, err = repo.Create(ctx, &stale, sessionHash("missing-epoch"), "session@example.test", "fixture-password-hash")
			require.ErrorIs(t, err, service.ErrTokenRevoked)
			var count int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM auth_sessions WHERE sid IN ('stale-epoch','missing-epoch')`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestAuthSessionPostgresIdentityAndPasskeyEvents(t *testing.T) {
	db := authSessionTestDB(t)
	repo := NewAuthSessionRepository(db)
	userID := sessionFixtureUser(t, db)
	epoch := func() int64 {
		var value int64
		require.NoError(t, db.QueryRow(`SELECT session_epoch FROM users WHERE id=$1`, userID).Scan(&value))
		return value
	}
	before := epoch()
	_, err := db.Exec(`INSERT INTO auth_identities(user_id,provider_type,provider_key,provider_subject,verified_at) VALUES($1,'email','email','session@example.test',clock_timestamp())`, userID)
	require.NoError(t, err)
	require.Greater(t, epoch(), before)
	s := createSessionFixture(t, db, repo, userID, "identity")
	_, err = db.Exec(`UPDATE auth_identities SET metadata='{"display_name":"updated"}',verified_at=clock_timestamp() WHERE user_id=$1`, userID)
	require.NoError(t, err)
	require.Equal(t, s.Epoch, epoch(), "claim refresh does not replace a verified identity")
	_, err = db.Exec(`DELETE FROM auth_identities WHERE user_id=$1`, userID)
	require.NoError(t, err)
	require.Greater(t, epoch(), s.Epoch)
	before = epoch()
	_, err = db.Exec(`INSERT INTO passkey_credentials(user_id,credential_id,credential_data) VALUES($1,'fixture-id','{"id":"id-a","publicKey":"key-a","authenticator":{"signCount":1}}')`, userID)
	require.NoError(t, err)
	require.Greater(t, epoch(), before)
	before = epoch()
	_, err = db.Exec(`UPDATE passkey_credentials SET name='Renamed',last_used_at=clock_timestamp(),credential_data=jsonb_set(credential_data,'{authenticator,signCount}','2') WHERE user_id=$1`, userID)
	require.NoError(t, err)
	require.Equal(t, before, epoch(), "assertion metadata and rename retain sessions")
	_, err = db.Exec(`UPDATE passkey_credentials SET credential_data=jsonb_set(credential_data,'{publicKey}','"key-b"') WHERE user_id=$1`, userID)
	require.NoError(t, err)
	require.Greater(t, epoch(), before)
	before = epoch()
	_, err = db.Exec(`DELETE FROM passkey_credentials WHERE user_id=$1`, userID)
	require.NoError(t, err)
	require.Greater(t, epoch(), before)
	_, err = db.Exec(`INSERT INTO auth_identities(user_id,provider_type,provider_key,provider_subject) VALUES($1,'email','email','cascade')`, userID)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO passkey_credentials(user_id,credential_id,credential_data) VALUES($1,'cascade-id','{}')`, userID)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM users WHERE id=$1`, userID)
	require.NoError(t, err, "cascade identity/passkey triggers tolerate an absent owner")
}

func TestAuthSessionPostgresChannelAndPasskeyHandleEvents(t *testing.T) {
	db := authSessionTestDB(t)
	userID := sessionFixtureUser(t, db)
	otherID := sessionFixtureUser(t, db)
	epoch := func(id int64) int64 {
		var value int64
		require.NoError(t, db.QueryRow(`SELECT session_epoch FROM users WHERE id=$1`, id).Scan(&value))
		return value
	}
	var identityID int64
	require.NoError(t, db.QueryRow(`INSERT INTO auth_identities(user_id,provider_type,provider_key,provider_subject) VALUES($1,'wechat','wechat','subject') RETURNING id`, userID).Scan(&identityID))
	before := epoch(userID)
	_, err := db.Exec(`INSERT INTO auth_identity_channels(identity_id,provider_type,provider_key,channel,channel_app_id,channel_subject) VALUES($1,'wechat','wechat','mobile','fixture-app','fixture-subject')`, identityID)
	require.NoError(t, err)
	require.Greater(t, epoch(userID), before)
	before = epoch(userID)
	_, err = db.Exec(`UPDATE auth_identity_channels SET metadata='{"avatar":"fixture"}' WHERE identity_id=$1`, identityID)
	require.NoError(t, err)
	require.Equal(t, before, epoch(userID))
	_, err = db.Exec(`UPDATE auth_identity_channels SET channel_subject='changed' WHERE identity_id=$1`, identityID)
	require.NoError(t, err)
	require.Greater(t, epoch(userID), before)
	before, otherBefore := epoch(userID), epoch(otherID)
	_, err = db.Exec(`UPDATE auth_identities SET user_id=$2 WHERE id=$1`, identityID, otherID)
	require.NoError(t, err)
	require.Greater(t, epoch(userID), before)
	require.Greater(t, epoch(otherID), otherBefore)
	_, err = db.Exec(`INSERT INTO passkey_user_handles(user_id,user_handle) VALUES($1,'first-handle')`, userID)
	require.NoError(t, err)
	before = epoch(userID)
	_, err = db.Exec(`UPDATE passkey_user_handles SET user_handle='replacement-handle' WHERE user_id=$1`, userID)
	require.NoError(t, err)
	require.Greater(t, epoch(userID), before)
	before = epoch(userID)
	_, err = db.Exec(`DELETE FROM passkey_user_handles WHERE user_id=$1`, userID)
	require.NoError(t, err)
	require.Greater(t, epoch(userID), before)
}

func TestAuthSessionPostgresExpiredSessionDeniesAccessAndRefresh(t *testing.T) {
	db := authSessionTestDB(t)
	repo := NewAuthSessionRepository(db)
	userID := sessionFixtureUser(t, db)
	s := createSessionFixture(t, db, repo, userID, "expired")
	_, err := db.Exec(`UPDATE auth_sessions SET created_at=clock_timestamp()-INTERVAL '2 hours',expires_at=clock_timestamp()-INTERVAL '1 hour' WHERE sid=$1`, s.SessionID)
	require.NoError(t, err)
	_, err = repo.Get(context.Background(), userID, s.SessionID, s.Epoch)
	require.ErrorIs(t, err, service.ErrTokenExpired)
	_, err = repo.Rotate(context.Background(), sessionHash("expired-token"), sessionHash("expired-next"), "fixture-binding", true)
	require.ErrorIs(t, err, service.ErrRefreshTokenExpired)
}

func TestAuthSessionPostgresRefreshRacesWithRevocation(t *testing.T) {
	for _, target := range []string{"current", "all", "password", "disable"} {
		t.Run(target, func(t *testing.T) {
			db := authSessionTestDB(t)
			repo := NewAuthSessionRepository(db)
			userID := sessionFixtureUser(t, db)
			ctx := context.Background()
			s := createSessionFixture(t, db, repo, userID, "race")
			start := make(chan struct{})
			rotated := make(chan error, 1)
			revoked := make(chan error, 1)
			go func() {
				<-start
				_, err := repo.Rotate(ctx, sessionHash("race-token"), sessionHash("race-new"), "fixture-binding", true)
				rotated <- err
			}()
			go func() {
				<-start
				var err error
				switch target {
				case "current":
					err = repo.RevokeSession(ctx, s.SessionID)
				case "all":
					_, err = repo.RevokeAll(ctx, userID)
				case "password":
					_, err = db.Exec(`UPDATE users SET password_hash='changed' WHERE id=$1`, userID)
				case "disable":
					_, err = db.Exec(`UPDATE users SET status='disabled' WHERE id=$1`, userID)
				}
				revoked <- err
			}()
			close(start)
			require.NoError(t, <-revoked)
			if err := <-rotated; err != nil {
				require.True(t, err == service.ErrTokenRevoked || err == service.ErrUserNotActive, "unexpected rotation error: %v", err)
			}
			_, err := repo.Get(ctx, userID, s.SessionID, s.Epoch)
			require.Error(t, err, "neither lock ordering can revive a revoked session")
			_, err = repo.Rotate(ctx, sessionHash("race-new"), sessionHash("race-next"), "fixture-binding", true)
			require.Error(t, err)
		})
	}
}
