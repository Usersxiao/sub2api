//go:build unit

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func gatewayPostgresFixture(t *testing.T) (*sql.DB, service.ClientGatewayRepository, *service.AuthSession) {
	t.Helper()
	db := authSessionTestDB(t)
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "249_client_gateway_grants.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(migration))
	require.NoError(t, err)
	userID := sessionFixtureUser(t, db)
	session := createSessionFixture(t, db, NewAuthSessionRepository(db), userID, "gateway-session")
	return db, NewClientGatewayRepository(db), session
}

func gatewayGrantFixture(session *service.AuthSession, id string) *service.ClientGatewayGrant {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return &service.ClientGatewayGrant{ClientGatewayScope: service.GatewaySessionScope(service.DefaultClientInstanceID, session), GrantID: id, ClientID: service.ClientGatewayClientID, ConsumerID: "candidate-" + id, ConfigVersion: "cfg-1", EntitlementVersion: "ent-1", RouteIDs: []string{"writing"}, Operations: []string{"invoke"}, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute)}
}

func TestClientGatewayPostgresConsumerStableAcrossRotationAndLogin(t *testing.T) {
	db, repo, session := gatewayPostgresFixture(t)
	ctx := context.Background()
	first, err := repo.Issue(ctx, gatewayGrantFixture(session, "first"), sessionHash("first-token"), "", "")
	require.NoError(t, err)
	second, err := repo.Issue(ctx, gatewayGrantFixture(session, "second"), sessionHash("second-token"), first.GrantID, first.ConsumerID)
	require.NoError(t, err)
	require.Equal(t, first.ConsumerID, second.ConsumerID)
	_, err = repo.GetByTokenHash(ctx, sessionHash("first-token"))
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	_, err = repo.GetByTokenHash(ctx, sessionHash("second-token"))
	require.NoError(t, err)
	_, err = repo.Issue(ctx, gatewayGrantFixture(session, "stale"), sessionHash("stale-token"), first.GrantID, first.ConsumerID)
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	_, err = repo.GetByTokenHash(ctx, sessionHash("second-token"))
	require.NoError(t, err)
	newSession := createSessionFixture(t, db, NewAuthSessionRepository(db), session.UserID, "another-login")
	third, err := repo.Issue(ctx, gatewayGrantFixture(newSession, "third"), sessionHash("third-token"), "", first.ConsumerID)
	require.NoError(t, err)
	require.Equal(t, first.ConsumerID, third.ConsumerID)
	// A new repository instance reads the same ledger, without in-memory recovery.
	restarted := NewClientGatewayRepository(db)
	got, err := restarted.GetByTokenHash(ctx, sessionHash("third-token"))
	require.NoError(t, err)
	require.Equal(t, third.ConsumerID, got.ConsumerID)
	var consumers, active int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM client_gateway_consumers`).Scan(&consumers))
	require.Equal(t, 1, consumers)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM client_gateway_grants WHERE revoked_at IS NULL`).Scan(&active))
	require.Equal(t, 2, active) // independent signed-in sessions remain independent
}

func TestClientGatewayPostgresRotationRollsBackOnWriteFailure(t *testing.T) {
	db, repo, session := gatewayPostgresFixture(t)
	ctx := context.Background()
	first, err := repo.Issue(ctx, gatewayGrantFixture(session, "first"), sessionHash("first-token"), "", "")
	require.NoError(t, err)
	_, err = db.Exec(`CREATE FUNCTION reject_gateway_grant() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.grant_id='blocked' THEN RAISE EXCEPTION 'fixture insert fault'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER reject_gateway_grant BEFORE INSERT ON client_gateway_grants FOR EACH ROW EXECUTE FUNCTION reject_gateway_grant();`)
	require.NoError(t, err)
	_, err = repo.Issue(ctx, gatewayGrantFixture(session, "blocked"), sessionHash("blocked-token"), first.GrantID, first.ConsumerID)
	require.Error(t, err)
	_, err = repo.GetByTokenHash(ctx, sessionHash("first-token"))
	require.NoError(t, err)
	_, err = repo.Issue(ctx, gatewayGrantFixture(session, "bad-consumer"), sessionHash("bad-consumer-token"), first.GrantID, "caller-chosen")
	require.ErrorIs(t, err, service.ErrClientGatewayScope)
	_, err = repo.GetByTokenHash(ctx, sessionHash("first-token"))
	require.NoError(t, err)
}

func TestClientGatewayPostgresConcurrentRotationHasOneWinner(t *testing.T) {
	db, repo, session := gatewayPostgresFixture(t)
	ctx := context.Background()
	first, err := repo.Issue(ctx, gatewayGrantFixture(session, "first"), sessionHash("first-token"), "", "")
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	start := make(chan struct{})
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id := fmt.Sprintf("rotate-%d", i)
			_, err := repo.Issue(ctx, gatewayGrantFixture(session, id), sessionHash(id+"-token"), first.GrantID, first.ConsumerID)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	winners := 0
	for err := range errs {
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
		}
	}
	require.Equal(t, 1, winners)
	var active int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM client_gateway_grants WHERE revoked_at IS NULL`).Scan(&active))
	require.Equal(t, 1, active)
}

func TestClientGatewayPostgresInitialIssuanceRetiresPreviousAndSeparatesOwners(t *testing.T) {
	db, repo, session := gatewayPostgresFixture(t)
	ctx := context.Background()
	first, err := repo.Issue(ctx, gatewayGrantFixture(session, "first"), sessionHash("first-token"), "", "")
	require.NoError(t, err)
	_, err = repo.Issue(ctx, gatewayGrantFixture(session, "replacement"), sessionHash("replacement-token"), "", "")
	require.NoError(t, err)
	_, err = repo.GetByTokenHash(ctx, sessionHash("first-token"))
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	otherID := sessionFixtureUser(t, db)
	other := createSessionFixture(t, db, NewAuthSessionRepository(db), otherID, "other-user")
	_, err = repo.Revoke(ctx, other, service.DefaultClientInstanceID, "replacement")
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	_, err = repo.Issue(ctx, gatewayGrantFixture(other, "forged-continuity"), sessionHash("forged-token"), "", first.ConsumerID)
	require.ErrorIs(t, err, service.ErrClientGatewayScope)
	owned, err := repo.Issue(ctx, gatewayGrantFixture(other, "other"), sessionHash("other-token"), "", "")
	require.NoError(t, err)
	require.NotEqual(t, first.ConsumerID, owned.ConsumerID)
	when, err := repo.Revoke(ctx, session, service.DefaultClientInstanceID, "replacement")
	require.NoError(t, err)
	again, err := repo.Revoke(ctx, session, service.DefaultClientInstanceID, "replacement")
	require.NoError(t, err)
	require.Equal(t, when, again)
	_, err = repo.GetByTokenHash(ctx, sessionHash("replacement-token"))
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	_, err = repo.GetByTokenHash(ctx, sessionHash("other-token"))
	require.NoError(t, err)
}

func TestClientGatewayPostgresSessionRevocationAndEpochInvalidate(t *testing.T) {
	for _, mode := range []string{"logout", "epoch", "expired", "grant-expired", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			db, repo, session := gatewayPostgresFixture(t)
			ctx := context.Background()
			_, err := repo.Issue(ctx, gatewayGrantFixture(session, "first"), sessionHash("first-token"), "", "")
			require.NoError(t, err)
			switch mode {
			case "logout":
				err = NewAuthSessionRepository(db).RevokeUserSession(ctx, session.UserID, session.SessionID)
			case "epoch":
				_, err = db.Exec(`UPDATE users SET session_epoch=session_epoch+1 WHERE id=$1`, session.UserID)
			case "expired":
				_, err = db.Exec(`UPDATE auth_sessions SET created_at=clock_timestamp()-interval '2 hours',expires_at=clock_timestamp()-interval '1 hour' WHERE sid=$1`, session.SessionID)
			case "grant-expired":
				_, err = db.Exec(`UPDATE client_gateway_grants SET issued_at=now()-interval '6 minutes',expires_at=now()-interval '1 minute' WHERE grant_id='first'`)
			case "disabled":
				_, err = db.Exec(`UPDATE users SET status='disabled' WHERE id=$1`, session.UserID)
			}
			require.NoError(t, err)
			_, err = repo.GetByTokenHash(ctx, sessionHash("first-token"))
			require.Error(t, err)
			if mode != "grant-expired" {
				_, err = repo.Issue(ctx, gatewayGrantFixture(session, "stale-session"), sessionHash("stale-session-token"), "", "")
				require.Error(t, err)
			}
		})
	}
}

func TestClientGatewayPostgresConcurrentLogoutWinsAdmission(t *testing.T) {
	for round := range 4 {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			db, repo, session := gatewayPostgresFixture(t)
			ctx := context.Background()
			start := make(chan struct{})
			issued := make(chan error, 1)
			revoked := make(chan error, 1)
			go func() {
				<-start
				_, err := repo.Issue(ctx, gatewayGrantFixture(session, "racing"), sessionHash("race-token"), "", "")
				issued <- err
			}()
			go func() {
				<-start
				revoked <- NewAuthSessionRepository(db).RevokeUserSession(ctx, session.UserID, session.SessionID)
			}()
			close(start)
			issueErr := <-issued
			require.NoError(t, <-revoked)
			if issueErr != nil {
				require.ErrorIs(t, issueErr, service.ErrTokenRevoked)
			}
			_, err := repo.GetByTokenHash(ctx, sessionHash("race-token"))
			require.Error(t, err)
		})
	}
}

func TestClientGatewayPostgresRejectsOversizedOrUnboundedGrant(t *testing.T) {
	_, repo, session := gatewayPostgresFixture(t)
	for _, mode := range []string{"ttl", "absolute", "empty", "operation"} {
		t.Run(mode, func(t *testing.T) {
			g := gatewayGrantFixture(session, mode)
			switch mode {
			case "ttl":
				g.ExpiresAt = g.IssuedAt.Add(5*time.Minute + time.Nanosecond)
			case "absolute":
				g.IssuedAt = session.ExpiresAt
				g.ExpiresAt = g.IssuedAt.Add(time.Minute)
			case "empty":
				g.RouteIDs = nil
			case "operation":
				g.Operations = []string{"task:read"}
			}
			_, err := repo.Issue(context.Background(), g, sessionHash(mode), "", "")
			require.Error(t, err)
		})
	}
}
