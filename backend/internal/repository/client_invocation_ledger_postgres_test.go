//go:build unit

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type invocationPostgresFixture struct {
	db      *sql.DB
	client  *dbent.Client
	repo    service.ClientInvocationLedgerRepository
	grant   *service.ClientGatewayGrant
	session *service.AuthSession
	in      *service.ClientInvocationReservation
}

func invocationLedgerFixture(t *testing.T) *invocationPostgresFixture {
	return invocationLedgerFixtureAt(t, true, true)
}

func invocationLedgerFixtureAt(t *testing.T, counterProofMigration, attemptsMigration bool, apiKeySearchMigration ...bool) *invocationPostgresFixture {
	t.Helper()
	db, client, session, groupID, _ := billingPostgresFixture(t)
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "251_client_invocation_ledger.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(migration))
	require.NoError(t, err)
	providerPayloadMigration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "266_client_invocation_provider_payload.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(providerPayloadMigration))
	require.NoError(t, err)
	if counterProofMigration {
		counterProof, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", "267_client_invocation_counter_proof.sql"))
		require.NoError(t, readErr)
		_, err = db.Exec(string(counterProof))
		require.NoError(t, err)
	}
	if attemptsMigration {
		attempts, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", "268_client_invocation_attempts.sql"))
		require.NoError(t, readErr)
		_, err = db.Exec(string(attempts))
		require.NoError(t, err)
		preparedAttempts, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", "269_client_invocation_attempt_prepared.sql"))
		require.NoError(t, readErr)
		_, err = db.Exec(string(preparedAttempts))
		require.NoError(t, err)
		notDispatchedAttempts, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", "270_client_invocation_attempt_not_dispatched.sql"))
		require.NoError(t, readErr)
		_, err = db.Exec(string(notDispatchedAttempts))
		require.NoError(t, err)
		secondAttempt, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", "271_client_invocation_second_attempt.sql"))
		require.NoError(t, readErr)
		_, err = db.Exec(string(secondAttempt))
		require.NoError(t, err)
		searchMigration, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", "272_client_invocation_search.sql"))
		require.NoError(t, readErr)
		_, err = db.Exec(string(searchMigration))
		require.NoError(t, err)
		if len(apiKeySearchMigration) == 0 || apiKeySearchMigration[0] {
			apiKeySearch, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", "273_client_invocation_apikey_search.sql"))
			require.NoError(t, readErr)
			_, err = db.Exec(string(apiKeySearch))
			require.NoError(t, err)
		}

	}
	grant := issueBillingGrant(t, db, session, "invocation-grant")
	key, err := NewClientBillingIdentityRepository(db, client).Resolve(context.Background(), grant, groupID)
	require.NoError(t, err)
	return &invocationPostgresFixture{db, client, NewClientInvocationLedgerRepository(db), grant, session,
		&service.ClientInvocationReservation{InvocationID: "invocation-one", ProjectID: "project-a", RequestID: "request-one",
			RouteID: "writing", Operation: "text", ConfigVersion: grant.ConfigVersion, EntitlementVersion: grant.EntitlementVersion,
			Fingerprint: sessionHash("private input never stored"), BillingIdentityID: key.ID, GroupID: groupID,
			OwnerToken: invocationTestOwner('A')}}
}

func invocationTestOwner(letter byte) string { return "attempt-" + strings.Repeat(string(letter), 43) }

func invocationTestDispatchBinding() service.ClientInvocationDispatchBinding {
	return service.ClientInvocationDispatchBinding{AccountID: 19, Platform: service.PlatformOpenAI,
		ModelID: "provider-model", Protocol: "chat-completions", PayloadHash: sessionHash("provider-payload"),
		CounterProof: &service.ClientInvocationCounterProof{OutputTokens: 1}}
}

func (f *invocationPostgresFixture) reserve(t *testing.T) *service.ClientInvocationRecord {
	t.Helper()
	claim, err := f.repo.Reserve(context.Background(), f.grant, f.in)
	require.NoError(t, err)
	require.True(t, claim.Owned)
	return claim.Record
}

func (f *invocationPostgresFixture) dispatch(t *testing.T) *service.ClientInvocationRecord {
	t.Helper()
	f.reserve(t)
	row, err := f.startDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationDispatched, row.State)
	require.NotNil(t, row.DispatchDeadline)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationDispatched, true, true)
	return row
}

func (f *invocationPostgresFixture) startDispatch(ctx context.Context, grant *service.ClientGatewayGrant, invocationID, ownerToken string, binding service.ClientInvocationDispatchBinding) (*service.ClientInvocationRecord, error) {
	return startInvocationDispatch(ctx, f.repo, grant, invocationID, ownerToken, binding)
}

func startInvocationDispatch(ctx context.Context, repo service.ClientInvocationLedgerRepository, grant *service.ClientGatewayGrant, invocationID, ownerToken string, binding service.ClientInvocationDispatchBinding) (*service.ClientInvocationRecord, error) {
	if _, err := repo.PrepareDispatch(ctx, grant, invocationID, ownerToken, binding); err != nil {
		return nil, err
	}
	return repo.StartDispatch(ctx, grant, invocationID, ownerToken, binding)
}

func assertInvocationAttempt(t *testing.T, f *invocationPostgresFixture, invocationID, state string, hasProof, hasDispatchTime bool) {
	t.Helper()
	var gotState, platform, model, protocol, payloadHash string
	var accountID int64
	var proof []byte
	var dispatchStartedAt sql.NullTime
	var notDispatchedAt sql.NullTime
	err := f.db.QueryRow(`SELECT state,provider_account_id,provider_platform,provider_model,provider_protocol,
		provider_payload_hash,provider_counter_proof,dispatch_started_at,not_dispatched_at FROM client_invocation_attempts
		WHERE invocation_id=$1 AND attempt_no=1`, invocationID).Scan(
		&gotState, &accountID, &platform, &model, &protocol, &payloadHash, &proof, &dispatchStartedAt, &notDispatchedAt)
	require.NoError(t, err)
	require.Equal(t, state, gotState)
	require.Equal(t, int64(19), accountID)
	require.Equal(t, string(service.PlatformOpenAI), platform)
	require.Equal(t, "provider-model", model)
	require.Equal(t, "chat-completions", protocol)
	require.Equal(t, sessionHash("provider-payload"), payloadHash)
	require.Equal(t, hasProof, len(proof) > 0)
	require.Equal(t, hasDispatchTime, dispatchStartedAt.Valid)
	require.Equal(t, state == service.ClientInvocationAttemptNotDispatched, notDispatchedAt.Valid)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1`, invocationID).Scan(&count))
	require.Equal(t, 1, count)
}

func TestClientInvocationLedgerPostgresConcurrentReserveAndDispatch(t *testing.T) {
	f := invocationLedgerFixture(t)
	ctx := context.Background()
	const concurrency = 8
	start := make(chan struct{})
	type result struct {
		claim *service.ClientInvocationClaim
		in    service.ClientInvocationReservation
		err   error
	}
	results := make(chan result, concurrency)
	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			in := *f.in
			in.InvocationID = fmt.Sprintf("invocation-%d", i)
			in.OwnerToken = invocationTestOwner(byte('A' + i))
			claim, err := f.repo.Reserve(ctx, f.grant, &in)
			results <- result{claim, in, err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	var winner service.ClientInvocationReservation
	winners := 0
	var invocationID string
	for result := range results {
		require.NoError(t, result.err)
		if invocationID == "" {
			invocationID = result.claim.Record.InvocationID
		}
		require.Equal(t, invocationID, result.claim.Record.InvocationID)
		if result.claim.Owned {
			winners++
			winner = result.in
		}
	}
	require.Equal(t, 1, winners)
	start = make(chan struct{})
	errors := make(chan error, concurrency)
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := f.startDispatch(ctx, f.grant, winner.InvocationID, winner.OwnerToken, invocationTestDispatchBinding())
			errors <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errors)
	sent := 0
	for err := range errors {
		if err == nil {
			sent++
		} else {
			require.ErrorIs(t, err, service.ErrClientInvocationOwner)
		}
	}
	require.Equal(t, 1, sent)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocations`).Scan(&count))
	require.Equal(t, 1, count)
	var attemptCount int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1`, winner.InvocationID).Scan(&attemptCount))
	require.Equal(t, 1, attemptCount)
	var stored string
	require.NoError(t, f.db.QueryRow(`SELECT row_to_json(i)::text FROM client_invocations i`).Scan(&stored))
	require.NotContains(t, stored, "private input never stored")
	require.NotContains(t, stored, winner.OwnerToken)
	got, err := NewClientInvocationLedgerRepository(f.db).Get(ctx, f.grant, winner.ProjectID, winner.RequestID)
	require.NoError(t, err)
	require.Equal(t, winner.InvocationID, got.InvocationID)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(encoded))
}

func TestClientInvocationAttemptsPostgresRejectSplitState(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.dispatch(t)
	_, err := f.db.Exec(`UPDATE client_invocation_attempts SET state='unknown',updated_at=clock_timestamp()
		WHERE invocation_id=$1 AND attempt_no=1`, f.in.InvocationID)
	require.Error(t, err, "the deferred cross-row check must reject an attempt-only transition")
	_, err = f.db.Exec(`UPDATE client_invocations SET state='unknown',updated_at=clock_timestamp()
		WHERE invocation_id=$1`, f.in.InvocationID)
	require.Error(t, err, "the deferred cross-row check must reject an invocation-only transition")
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationDispatched, true, true)
	var invocationState string
	require.NoError(t, f.db.QueryRow(`SELECT state FROM client_invocations WHERE invocation_id=$1`, f.in.InvocationID).Scan(&invocationState))
	require.Equal(t, service.ClientInvocationDispatched, invocationState)
	_, err = f.db.Exec(`UPDATE client_invocation_attempts SET provider_payload_hash=$1 WHERE invocation_id=$2`,
		sessionHash("rewritten-payload"), f.in.InvocationID)
	require.Error(t, err, "the provider binding is immutable")
	_, err = f.db.Exec(`INSERT INTO client_invocation_attempts
		(invocation_id,attempt_no,state,provider_account_id,provider_platform,provider_model,provider_protocol,
		 provider_payload_hash,provider_counter_proof,dispatch_started_at,updated_at)
		SELECT invocation_id,2,state,provider_account_id,provider_platform,provider_model,provider_protocol,
		 provider_payload_hash,provider_counter_proof,dispatch_started_at,updated_at
		FROM client_invocation_attempts WHERE invocation_id=$1 AND attempt_no=1`, f.in.InvocationID)
	require.Error(t, err, "P8.61 stores attempt 1 without authorizing retries")
}

func TestClientInvocationAttemptsPostgresPreparedIsDurableNoDispatchProof(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.reserve(t)
	binding := invocationTestDispatchBinding()
	_, err := f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.ErrorIs(t, err, service.ErrClientInvocationOwner, "dispatch requires a separately committed prepared attempt")
	row, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationReserved, row.State)
	require.Empty(t, row.ProviderPayloadHash, "the invocation has not crossed the durable dispatch claim")
	assertInvocationAttempt(t, f, f.in.InvocationID, "prepared", true, false)

	var invocationState, attemptState string
	require.NoError(t, f.db.QueryRow(`SELECT state FROM client_invocations WHERE invocation_id=$1`, f.in.InvocationID).Scan(&invocationState))
	require.NoError(t, f.db.QueryRow(`SELECT state FROM client_invocation_attempts WHERE invocation_id=$1`, f.in.InvocationID).Scan(&attemptState))
	require.Equal(t, service.ClientInvocationReserved, invocationState)
	require.Equal(t, "prepared", attemptState)

	changed := binding
	changed.PayloadHash = sessionHash("different-final-provider-payload")
	_, err = f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, changed)
	require.ErrorIs(t, err, service.ErrClientInvocationConflict, "dispatch must claim the exact prepared payload")
	assertInvocationAttempt(t, f, f.in.InvocationID, "prepared", true, false)
	_, err = f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err, "the same prepared binding is idempotently recoverable before the claim")
	row, err = f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationDispatched, row.State)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationDispatched, true, true)
}

func TestClientInvocationAttemptsPostgresNotDispatchedIsDurableAndIdempotent(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.reserve(t)
	binding := invocationTestDispatchBinding()
	_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)

	row, err := f.repo.MarkPreparedNotDispatched(context.Background(), f.in.InvocationID, f.in.OwnerToken)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationReserved, row.State)
	require.Empty(t, row.ProviderPayloadHash)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationAttemptNotDispatched, true, false)

	_, err = f.repo.MarkPreparedNotDispatched(context.Background(), f.in.InvocationID, f.in.OwnerToken)
	require.NoError(t, err, "the durable pre-dispatch result is idempotent")
	_, err = f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.ErrorIs(t, err, service.ErrClientInvocationOwner, "a terminal pre-dispatch attempt cannot be claimed")
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationAttemptNotDispatched, true, false)
}

func TestClientInvocationAttemptsPostgresDispatchedClaimWinsOverNotDispatched(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.dispatch(t)
	_, err := f.repo.MarkPreparedNotDispatched(context.Background(), f.in.InvocationID, f.in.OwnerToken)
	require.ErrorIs(t, err, service.ErrClientInvocationOwner)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationDispatched, true, true)
}

func TestClientInvocationAttemptsPostgresDispatchAndNotDispatchedAreSerialized(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.reserve(t)
	binding := invocationTestDispatchBinding()
	_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)
	start := make(chan struct{})
	dispatchResult := make(chan error, 1)
	markResult := make(chan error, 1)
	go func() {
		<-start
		_, dispatchErr := f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
		dispatchResult <- dispatchErr
	}()
	go func() {
		<-start
		_, markErr := f.repo.MarkPreparedNotDispatched(context.Background(), f.in.InvocationID, f.in.OwnerToken)
		markResult <- markErr
	}()
	close(start)
	dispatchErr, markErr := <-dispatchResult, <-markResult
	if dispatchErr == nil {
		require.ErrorIs(t, markErr, service.ErrClientInvocationOwner)
		assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationDispatched, true, true)
		return
	}
	require.ErrorIs(t, dispatchErr, service.ErrClientInvocationOwner)
	require.NoError(t, markErr)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationAttemptNotDispatched, true, false)
}

func TestClientInvocationAttemptsPostgresExpiredOwnerCanResumeOnlyPreparedBinding(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.reserve(t)
	binding := invocationTestDispatchBinding()
	_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE client_invocations SET created_at=clock_timestamp()-INTERVAL '1 minute',
		lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE invocation_id=$1`, f.in.InvocationID)
	require.NoError(t, err)

	recovered := *f.in
	recovered.OwnerToken = invocationTestOwner('B')
	claim, err := f.repo.Reserve(context.Background(), f.grant, &recovered)
	require.NoError(t, err)
	require.True(t, claim.Owned)
	_, err = f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.ErrorIs(t, err, service.ErrClientInvocationOwner, "the prior owner lost the lease")
	_, err = startInvocationDispatch(context.Background(), f.repo, f.grant, f.in.InvocationID, recovered.OwnerToken, binding)
	require.NoError(t, err, "the new owner may resume the exact durable prepared binding")
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationDispatched, true, true)
}

func TestClientInvocationAttemptsPostgresSecondAttemptRequiresNotDispatchedAndBindsClaim(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.reserve(t)
	firstBinding := invocationTestDispatchBinding()
	_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, firstBinding)
	require.NoError(t, err)

	secondBinding := firstBinding
	secondBinding.AccountID = 20
	secondBinding.ModelID = "alternate-provider-model"
	secondBinding.PayloadHash = sessionHash("alternate-final-provider-payload")
	secondBinding.CounterProof = &service.ClientInvocationCounterProof{OutputTokens: 2}
	_, err = f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, secondBinding)
	require.ErrorIs(t, err, service.ErrClientInvocationConflict, "a prepared attempt is not proof that a new attempt is safe")

	_, err = f.repo.MarkPreparedNotDispatched(context.Background(), f.in.InvocationID, f.in.OwnerToken)
	require.NoError(t, err)
	claim, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, secondBinding)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationReserved, claim.State)
	require.Empty(t, claim.ProviderPayloadHash, "preparation does not claim provider dispatch")

	const concurrency = 6
	start := make(chan struct{})
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, prepareErr := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, secondBinding)
			errs <- prepareErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for prepareErr := range errs {
		require.NoError(t, prepareErr, "the same second binding is idempotently recoverable")
	}
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1`, f.in.InvocationID).Scan(&count))
	require.Equal(t, 2, count)
	var firstState, secondState, secondModel, secondPayloadHash string
	var secondAccountID int64
	require.NoError(t, f.db.QueryRow(`SELECT state FROM client_invocation_attempts WHERE invocation_id=$1 AND attempt_no=1`, f.in.InvocationID).Scan(&firstState))
	require.NoError(t, f.db.QueryRow(`SELECT state,provider_account_id,provider_model,provider_payload_hash FROM client_invocation_attempts
		WHERE invocation_id=$1 AND attempt_no=2`, f.in.InvocationID).Scan(&secondState, &secondAccountID, &secondModel, &secondPayloadHash))
	require.Equal(t, service.ClientInvocationAttemptNotDispatched, firstState)
	require.Equal(t, "prepared", secondState)
	require.Equal(t, int64(20), secondAccountID)
	require.Equal(t, secondBinding.ModelID, secondModel)
	require.Equal(t, secondBinding.PayloadHash, secondPayloadHash)

	changedBinding := secondBinding
	changedBinding.PayloadHash = sessionHash("changed-second-payload")
	_, err = f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, changedBinding)
	require.ErrorIs(t, err, service.ErrClientInvocationConflict, "attempt 2 cannot be rebound after preparation")
	_, err = f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, firstBinding)
	require.ErrorIs(t, err, service.ErrClientInvocationConflict, "dispatch must claim the current attempt's exact binding")
	row, err := f.repo.StartDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, secondBinding)
	require.NoError(t, err)
	require.Equal(t, int16(2), row.ProviderAttemptNo)
	require.Equal(t, secondBinding.PayloadHash, row.ProviderPayloadHash)
	var providerAttemptNo int16
	require.NoError(t, f.db.QueryRow(`SELECT provider_attempt_no FROM client_invocations WHERE invocation_id=$1`, f.in.InvocationID).Scan(&providerAttemptNo))
	require.Equal(t, int16(2), providerAttemptNo)
	var dispatchedAttemptState string
	require.NoError(t, f.db.QueryRow(`SELECT state FROM client_invocation_attempts WHERE invocation_id=$1 AND attempt_no=2`, f.in.InvocationID).Scan(&dispatchedAttemptState))
	require.Equal(t, service.ClientInvocationDispatched, dispatchedAttemptState)

	text := "completed on attempt two"
	_, err = f.repo.Finish(context.Background(), f.in.InvocationID, f.in.OwnerToken, service.ClientInvocationOutcome{State: service.ClientInvocationSucceeded, Text: &text})
	require.NoError(t, err)
	require.NoError(t, f.db.QueryRow(`SELECT state FROM client_invocation_attempts WHERE invocation_id=$1 AND attempt_no=1`, f.in.InvocationID).Scan(&firstState))
	require.NoError(t, f.db.QueryRow(`SELECT state FROM client_invocation_attempts WHERE invocation_id=$1 AND attempt_no=2`, f.in.InvocationID).Scan(&secondState))
	require.Equal(t, service.ClientInvocationAttemptNotDispatched, firstState)
	require.Equal(t, service.ClientInvocationSucceeded, secondState)
}

func TestClientInvocationAttemptsPostgresAttemptThreeIsClosedAndAttemptTwoNeedsProof(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.reserve(t)
	firstBinding := invocationTestDispatchBinding()
	_, err := f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, firstBinding)
	require.NoError(t, err)
	secondBinding := firstBinding
	secondBinding.AccountID = 20
	secondBinding.PayloadHash = sessionHash("attempt-two-payload")
	tx, err := f.db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO client_invocation_attempts
		(invocation_id,attempt_no,state,provider_account_id,provider_platform,provider_model,provider_protocol,
		 provider_payload_hash,provider_counter_proof,dispatch_started_at,updated_at)
		SELECT invocation_id,2,'prepared',provider_account_id,provider_platform,provider_model,provider_protocol,
			$2,$3::jsonb,NULL,clock_timestamp() FROM client_invocation_attempts
		WHERE invocation_id=$1 AND attempt_no=1`, f.in.InvocationID, secondBinding.PayloadHash, `{"output_tokens":1}`)
	require.NoError(t, err)
	err = tx.Commit()
	require.Error(t, err, "deferred database consistency must reject attempt 2 without attempt 1 proof")

	// A separate valid chain reaches the attempt ceiling and rejects attempt 3.
	f = invocationLedgerFixture(t)
	f.reserve(t)
	_, err = f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, firstBinding)
	require.NoError(t, err)
	_, err = f.repo.MarkPreparedNotDispatched(context.Background(), f.in.InvocationID, f.in.OwnerToken)
	require.NoError(t, err)
	_, err = f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, secondBinding)
	require.NoError(t, err)
	_, err = f.repo.MarkPreparedNotDispatched(context.Background(), f.in.InvocationID, f.in.OwnerToken)
	require.NoError(t, err)
	thirdBinding := secondBinding
	thirdBinding.AccountID = 21
	thirdBinding.PayloadHash = sessionHash("attempt-three-payload")
	_, err = f.repo.PrepareDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, thirdBinding)
	require.ErrorIs(t, err, service.ErrClientInvocationOwner)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1`, f.in.InvocationID).Scan(&count))
	require.Equal(t, 2, count)
}

func TestClientInvocationAttemptsPostgresMigrationBackfillsKnownBindings(t *testing.T) {
	f := invocationLedgerFixtureAt(t, false, false)
	seedReserved := func(in service.ClientInvocationReservation) {
		_, err := f.db.Exec(`INSERT INTO client_invocations
			(invocation_id,instance_id,user_id,client_id,consumer_id,project_id,request_id,route_id,operation,
			 config_version,entitlement_version,fingerprint,billing_identity_id,group_id,owner_token_hash,state,
			 created_at,updated_at,lease_expires_at,idempotency_expires_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'reserved',clock_timestamp(),clock_timestamp(),
			 clock_timestamp()+INTERVAL '30 seconds',clock_timestamp()+INTERVAL '7 days')`,
			in.InvocationID, f.grant.InstanceID, f.session.UserID, f.grant.ClientID, f.grant.ConsumerID,
			in.ProjectID, in.RequestID, in.RouteID, in.Operation, in.ConfigVersion, in.EntitlementVersion,
			in.Fingerprint, in.BillingIdentityID, in.GroupID, service.GatewayTokenHash(in.OwnerToken))
		require.NoError(t, err)
	}
	states := []string{service.ClientInvocationDispatched, service.ClientInvocationUnknown,
		service.ClientInvocationSucceeded, service.ClientInvocationFailed}
	for i, state := range states {
		in := *f.in
		in.InvocationID = fmt.Sprintf("historical-invocation-%d", i)
		in.RequestID = fmt.Sprintf("historical-request-%d", i)
		in.OwnerToken = invocationTestOwner(byte('A' + i))
		seedReserved(in)
		var err error
		_, err = f.db.Exec(`UPDATE client_invocations SET state='dispatched',updated_at=clock_timestamp(),
			dispatch_deadline=clock_timestamp()+INTERVAL '15 minutes',provider_account_id=19,provider_platform=$1,
			provider_model='provider-model',provider_protocol='chat-completions',provider_payload_hash=$2
			WHERE invocation_id=$3`, service.PlatformOpenAI, sessionHash("provider-payload"), in.InvocationID)
		require.NoError(t, err)
		if state != service.ClientInvocationDispatched {
			var outcome any
			if state == service.ClientInvocationSucceeded {
				outcome = `{"state":"succeeded","text":"historical"}`
			} else if state == service.ClientInvocationFailed {
				outcome = `{"state":"failed","errorCode":"UPSTREAM_UNAVAILABLE"}`
			}
			_, err = f.db.Exec(`UPDATE client_invocations SET state=$1,updated_at=clock_timestamp(),outcome=$2 WHERE invocation_id=$3`,
				state, outcome, in.InvocationID)
			require.NoError(t, err)
		}
	}
	reserved := *f.in
	reserved.InvocationID, reserved.RequestID, reserved.OwnerToken = "historical-reserved", "historical-reserved-request", invocationTestOwner('Z')
	seedReserved(reserved)

	for _, name := range []string{"267_client_invocation_counter_proof.sql", "268_client_invocation_attempts.sql", "269_client_invocation_attempt_prepared.sql", "270_client_invocation_attempt_not_dispatched.sql", "271_client_invocation_second_attempt.sql"} {
		migration, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		require.NoError(t, readErr)
		_, applyErr := f.db.Exec(string(migration))
		require.NoError(t, applyErr, name)
	}
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts`).Scan(&count))
	require.Equal(t, len(states), count)
	for i, state := range states {
		var gotState string
		var proof []byte
		var dispatchStartedAt sql.NullTime
		err := f.db.QueryRow(`SELECT state,provider_counter_proof,dispatch_started_at FROM client_invocation_attempts
			WHERE invocation_id=$1 AND attempt_no=1`, fmt.Sprintf("historical-invocation-%d", i)).Scan(&gotState, &proof, &dispatchStartedAt)
		require.NoError(t, err)
		require.Equal(t, state, gotState)
		require.Empty(t, proof, "pre-proof history remains unknown rather than receiving fabricated evidence")
		require.False(t, dispatchStartedAt.Valid, "legacy dispatch time was not persisted")
	}
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1`, reserved.InvocationID).Scan(&count))
	require.Zero(t, count)
}

func TestClientInvocationLedgerPostgresRequiresCompleteImmutableProviderBinding(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.reserve(t)

	_, err := f.db.Exec(`UPDATE client_invocations SET state=$1,updated_at=clock_timestamp() WHERE invocation_id=$2`,
		service.ClientInvocationDispatched, f.in.InvocationID)
	require.Error(t, err, "reserved rows must not enter dispatched without a provider payload binding")

	_, err = f.db.Exec(`UPDATE client_invocations SET provider_account_id=19 WHERE invocation_id=$1`, f.in.InvocationID)
	require.Error(t, err, "partial provider payload bindings must be rejected")
	_, err = f.db.Exec(`UPDATE client_invocations SET state='dispatched',updated_at=clock_timestamp(),
		dispatch_deadline=clock_timestamp()+INTERVAL '15 minutes',provider_account_id=19,provider_platform=$1,
		provider_model='provider-model',provider_protocol='chat-completions',provider_payload_hash=$2
		WHERE invocation_id=$3`, service.PlatformOpenAI, sessionHash("provider-payload"), f.in.InvocationID)
	require.Error(t, err, "a provider binding cannot dispatch without its counter decision proof")

	binding := invocationTestDispatchBinding()
	row, err := f.startDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)
	require.Equal(t, binding.AccountID, row.ProviderAccountID)
	require.Equal(t, binding.Platform, row.ProviderPlatform)
	require.Equal(t, binding.ModelID, row.ProviderModel)
	require.Equal(t, binding.Protocol, row.ProviderProtocol)
	require.Equal(t, binding.PayloadHash, row.ProviderPayloadHash)
	require.Equal(t, binding.CounterProof, row.ProviderCounterProof)

	_, err = f.db.Exec(`UPDATE client_invocations SET provider_payload_hash=$1 WHERE invocation_id=$2`,
		sessionHash("changed-provider-payload"), f.in.InvocationID)
	require.Error(t, err, "the dispatch payload binding must remain immutable")
	_, err = f.db.Exec(`UPDATE client_invocations SET provider_counter_proof=$1 WHERE invocation_id=$2`,
		`{"required":false,"output_tokens":2}`, f.in.InvocationID)
	require.Error(t, err, "the dispatch counter proof must remain immutable")
	stored, err := f.repo.Get(context.Background(), f.grant, f.in.ProjectID, f.in.RequestID)
	require.NoError(t, err)
	require.Equal(t, binding.PayloadHash, stored.ProviderPayloadHash)
	require.Equal(t, binding.CounterProof, stored.ProviderCounterProof)
}

func TestClientInvocationLedgerPostgresPersistsExactCounterProof(t *testing.T) {
	f := invocationLedgerFixture(t)
	f.reserve(t)
	inputTokens, contextTokens := int64(32), int64(256)
	binding := invocationTestDispatchBinding()
	binding.CounterProof = &service.ClientInvocationCounterProof{
		Required: true, InputTokens: &inputTokens, OutputTokens: 64, ContextTokens: &contextTokens,
		AuthorityKind: "native", AuthorityID: "responses-counter", AuthorityRevision: "2026-10-26",
	}
	invalid := *binding.CounterProof
	invalidInputTokens := int64(250)
	invalid.InputTokens = &invalidInputTokens
	binding.CounterProof = &invalid
	_, err := f.startDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.ErrorIs(t, err, service.ErrClientGatewayScope)
	binding.CounterProof.InputTokens = &inputTokens
	row, err := f.startDispatch(context.Background(), f.grant, f.in.InvocationID, f.in.OwnerToken, binding)
	require.NoError(t, err)
	require.Equal(t, binding.CounterProof, row.ProviderCounterProof)
	stored, err := f.repo.Get(context.Background(), f.grant, f.in.ProjectID, f.in.RequestID)
	require.NoError(t, err)
	require.Equal(t, binding.CounterProof, stored.ProviderCounterProof)

	_, err = f.db.Exec(`UPDATE client_invocations SET provider_counter_proof=$1 WHERE invocation_id=$2`,
		`{"required":true,"input_tokens":999,"output_tokens":64,"context_tokens":256,"authority_kind":"native","authority_id":"responses-counter","authority_revision":"2026-10-26"}`, f.in.InvocationID)
	require.Error(t, err, "the persisted proof is immutable after the one-time dispatch claim")
}

func TestClientInvocationLedgerPostgresConflictAndBillingBindings(t *testing.T) {
	f := invocationLedgerFixture(t)
	ctx := context.Background()
	f.reserve(t)
	for _, field := range []string{"fingerprint", "billing", "group", "route", "config", "entitlement"} {
		t.Run(field, func(t *testing.T) {
			in := *f.in
			switch field {
			case "fingerprint":
				in.Fingerprint = sessionHash("different input")
			case "billing":
				in.BillingIdentityID += 1000
			case "group":
				in.GroupID += 1000
			case "route":
				in.RouteID = "other-route"
			case "config":
				in.ConfigVersion = "cfg-other"
			case "entitlement":
				in.EntitlementVersion = "ent-other"
			}
			_, err := f.repo.Reserve(ctx, f.grant, &in)
			require.Error(t, err)
			if field == "fingerprint" {
				require.ErrorIs(t, err, service.ErrClientInvocationConflict)
			}
		})
	}
	credential := &service.APIKey{UserID: f.session.UserID, Key: "sk-normal-credential", Name: "Ordinary", Status: service.StatusActive, GroupID: &f.in.GroupID}
	require.NoError(t, NewAPIKeyRepository(f.client, f.db).Create(ctx, credential))
	for _, billingID := range []int64{credential.ID, f.in.BillingIdentityID + 1000} {
		in := *f.in
		in.InvocationID, in.RequestID, in.BillingIdentityID = "forged-invocation", "forged-request", billingID
		_, err := f.repo.Reserve(ctx, f.grant, &in)
		require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
	}
	// A different route/version may itself be authorized. It must still conflict
	// with the existing logical submission rather than create a second charge.
	multiRoute := gatewayGrantFixture(f.session, "multi-route-conflict")
	multiRoute.RouteIDs = []string{"writing", "other-route"}
	multiGrant, err := NewClientGatewayRepository(f.db).Issue(ctx, multiRoute, sessionHash("multi-route-conflict-token"), "", "")
	require.NoError(t, err)
	routeChange := *f.in
	routeChange.RouteID = "other-route"
	_, err = f.repo.Reserve(ctx, multiGrant, &routeChange)
	require.ErrorIs(t, err, service.ErrClientInvocationConflict)
	changedVersion := gatewayGrantFixture(f.session, "new-version-conflict")
	changedVersion.ConfigVersion, changedVersion.EntitlementVersion = "cfg-2", "ent-2"
	changedGrant, err := NewClientGatewayRepository(f.db).Issue(ctx, changedVersion, sessionHash("new-version-conflict-token"), "", "")
	require.NoError(t, err)
	versionChange := *f.in
	versionChange.ConfigVersion, versionChange.EntitlementVersion = changedGrant.ConfigVersion, changedGrant.EntitlementVersion
	_, err = f.repo.Reserve(ctx, changedGrant, &versionChange)
	require.ErrorIs(t, err, service.ErrClientInvocationConflict)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocations`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestClientInvocationLedgerPostgresReclaimsOnlyUndispatchedLease(t *testing.T) {
	f := invocationLedgerFixture(t)
	ctx := context.Background()
	f.reserve(t)
	_, err := f.db.Exec(`UPDATE client_invocations SET created_at=clock_timestamp()-INTERVAL '1 minute',
		lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE invocation_id=$1`, f.in.InvocationID)
	require.NoError(t, err)
	_, err = f.startDispatch(ctx, f.grant, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
	require.ErrorIs(t, err, service.ErrClientInvocationOwner)
	next := *f.in
	next.InvocationID, next.OwnerToken = "unused-new-id", invocationTestOwner('B')
	claim, err := f.repo.Reserve(ctx, f.grant, &next)
	require.NoError(t, err)
	require.True(t, claim.Owned)
	require.Equal(t, f.in.InvocationID, claim.Record.InvocationID)
	_, err = f.startDispatch(ctx, f.grant, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
	require.ErrorIs(t, err, service.ErrClientInvocationOwner)
	_, err = f.startDispatch(ctx, f.grant, f.in.InvocationID, next.OwnerToken, invocationTestDispatchBinding())
	require.NoError(t, err)
	claim, err = f.repo.Reserve(ctx, f.grant, f.in)
	require.NoError(t, err)
	require.False(t, claim.Owned)
	require.Equal(t, service.ClientInvocationDispatched, claim.Record.State)
	_, err = f.db.Exec(`UPDATE client_invocations SET dispatch_deadline=clock_timestamp()-INTERVAL '1 second' WHERE invocation_id=$1`, f.in.InvocationID)
	require.NoError(t, err)
	claim, err = NewClientInvocationLedgerRepository(f.db).Reserve(ctx, f.grant, f.in)
	require.NoError(t, err)
	require.False(t, claim.Owned)
	require.Equal(t, service.ClientInvocationUnknown, claim.Record.State)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationUnknown, true, true)
	_, err = f.startDispatch(ctx, f.grant, f.in.InvocationID, next.OwnerToken, invocationTestDispatchBinding())
	require.ErrorIs(t, err, service.ErrClientInvocationOwner)
}

func TestClientInvocationLedgerPostgresCompletionSurvivesLogoutAndIsImmutable(t *testing.T) {
	f := invocationLedgerFixture(t)
	ctx := context.Background()
	f.dispatch(t)
	require.NoError(t, NewAuthSessionRepository(f.db).RevokeRefresh(ctx, sessionHash("billing-session-token")))
	text := "owned result with a NUL:\x00 and Unicode 中文"
	outcome := service.ClientInvocationOutcome{State: service.ClientInvocationSucceeded, Text: &text}
	_, err := f.repo.Finish(ctx, f.in.InvocationID, invocationTestOwner('B'), outcome)
	require.ErrorIs(t, err, service.ErrClientInvocationOwner)
	row, err := f.repo.Finish(ctx, f.in.InvocationID, f.in.OwnerToken, outcome)
	require.NoError(t, err)
	require.Equal(t, text, *row.Outcome.Text)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationSucceeded, true, true)
	row, err = NewClientInvocationLedgerRepository(f.db).Finish(ctx, f.in.InvocationID, f.in.OwnerToken, outcome)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationSucceeded, row.State)
	different := "different result"
	_, err = f.repo.Finish(ctx, f.in.InvocationID, f.in.OwnerToken, service.ClientInvocationOutcome{State: service.ClientInvocationSucceeded, Text: &different})
	require.ErrorIs(t, err, service.ErrClientInvocationConflict)
	_, err = f.repo.Get(ctx, f.grant, f.in.ProjectID, f.in.RequestID)
	require.ErrorIs(t, err, service.ErrTokenRevoked)
	newSession := createSessionFixture(t, f.db, NewAuthSessionRepository(f.db), f.session.UserID, "invocation-relogin")
	newGrant := issueBillingGrant(t, f.db, newSession, "invocation-new-grant")
	require.Equal(t, f.grant.ConsumerID, newGrant.ConsumerID)
	row, err = NewClientInvocationLedgerRepository(f.db).Get(ctx, newGrant, f.in.ProjectID, f.in.RequestID)
	require.NoError(t, err)
	require.Equal(t, text, *row.Outcome.Text)
}

func TestClientInvocationLedgerPostgresUnknownCanReconcileWithoutResending(t *testing.T) {
	f := invocationLedgerFixture(t)
	ctx := context.Background()
	f.dispatch(t)
	_, err := f.db.Exec(`UPDATE client_invocations SET dispatch_deadline=clock_timestamp()-INTERVAL '1 second' WHERE invocation_id=$1`, f.in.InvocationID)
	require.NoError(t, err)
	row, err := f.repo.Get(ctx, f.grant, f.in.ProjectID, f.in.RequestID)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationUnknown, row.State)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationUnknown, true, true)
	row, err = f.repo.Finish(ctx, f.in.InvocationID, f.in.OwnerToken, service.ClientInvocationOutcome{State: service.ClientInvocationFailed, ErrorCode: "UPSTREAM_UNAVAILABLE"})
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationFailed, row.State)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationFailed, true, true)
	claim, err := f.repo.Reserve(ctx, f.grant, f.in)
	require.NoError(t, err)
	require.False(t, claim.Owned)
	require.Equal(t, row.State, claim.Record.State)
	_, err = f.repo.Finish(ctx, f.in.InvocationID, f.in.OwnerToken, service.ClientInvocationOutcome{State: service.ClientInvocationFailed, ErrorCode: "RAW_PROVIDER_SECRET"})
	require.ErrorIs(t, err, service.ErrClientGatewayScope)
}

func TestClientInvocationLedgerPostgresMarksDispatchUnknownWithoutResending(t *testing.T) {
	f := invocationLedgerFixture(t)
	ctx := context.Background()
	f.reserve(t)
	_, err := f.repo.MarkUnknown(ctx, f.in.InvocationID, f.in.OwnerToken)
	require.ErrorIs(t, err, service.ErrClientInvocationOwner)
	row, err := f.startDispatch(ctx, f.grant, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationDispatched, row.State)
	row, err = f.repo.MarkUnknown(ctx, f.in.InvocationID, f.in.OwnerToken)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationUnknown, row.State)
	require.Nil(t, row.Outcome)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationUnknown, true, true)
	row, err = NewClientInvocationLedgerRepository(f.db).MarkUnknown(ctx, f.in.InvocationID, f.in.OwnerToken)
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationUnknown, row.State)
	_, err = f.repo.MarkUnknown(ctx, f.in.InvocationID, invocationTestOwner('B'))
	require.ErrorIs(t, err, service.ErrClientInvocationOwner)
	row, err = f.repo.Finish(ctx, f.in.InvocationID, f.in.OwnerToken, service.ClientInvocationOutcome{State: service.ClientInvocationFailed, ErrorCode: "UPSTREAM_UNAVAILABLE"})
	require.NoError(t, err)
	require.Equal(t, service.ClientInvocationFailed, row.State)
	assertInvocationAttempt(t, f, f.in.InvocationID, service.ClientInvocationFailed, true, true)
	claim, err := f.repo.Reserve(ctx, f.grant, f.in)
	require.NoError(t, err)
	require.False(t, claim.Owned)
	require.Equal(t, service.ClientInvocationFailed, claim.Record.State)
}

func TestClientInvocationLedgerPostgresOwnerIsolationAndCurrentGrant(t *testing.T) {
	f := invocationLedgerFixture(t)
	ctx := context.Background()
	f.reserve(t)
	for _, project := range []string{"project-b", "missing-project"} {
		_, err := f.repo.Get(ctx, f.grant, project, f.in.RequestID)
		require.ErrorIs(t, err, service.ErrClientInvocationNotFound)
	}
	for _, field := range []string{"consumer", "instance", "config", "epoch", "route"} {
		candidate := *f.grant
		switch field {
		case "consumer":
			candidate.ConsumerID = "other-consumer"
		case "instance":
			candidate.InstanceID = "other-instance"
		case "config":
			candidate.ConfigVersion = "cfg-forged"
		case "epoch":
			candidate.SessionEpoch++
		case "route":
			candidate.RouteIDs = []string{"other-route"}
		}
		_, err := f.repo.Get(ctx, &candidate, f.in.ProjectID, f.in.RequestID)
		require.Error(t, err, field)
		_, err = f.startDispatch(ctx, &candidate, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
		require.Error(t, err, field)
	}
	other, err := f.client.User.Create().SetEmail("other-ledger@example.test").SetPasswordHash("fixture-password-hash").Save(ctx)
	require.NoError(t, err)
	now := time.Now()
	otherSession, err := NewAuthSessionRepository(f.db).Create(ctx, &service.AuthSession{SessionID: "other-ledger-session", UserID: other.ID,
		Epoch: other.SessionEpoch, TokenVersion: 17, BindingHash: "fixture-binding", CreatedAt: now, ExpiresAt: now.Add(time.Hour)},
		sessionHash("other-ledger-token"), other.Email, other.PasswordHash)
	require.NoError(t, err)
	otherGrant := issueBillingGrant(t, f.db, otherSession, "other-ledger-grant")
	_, err = f.repo.Get(ctx, otherGrant, f.in.ProjectID, f.in.RequestID)
	require.ErrorIs(t, err, service.ErrClientInvocationNotFound)
	_, err = f.startDispatch(ctx, otherGrant, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
	require.ErrorIs(t, err, service.ErrClientInvocationNotFound)
	changed := gatewayGrantFixture(f.session, "changed-version")
	changed.ConfigVersion, changed.EntitlementVersion = "cfg-2", "ent-2"
	changedGrant, err := NewClientGatewayRepository(f.db).Issue(ctx, changed, sessionHash("changed-version-token"), "", "")
	require.NoError(t, err)
	row, err := f.repo.Get(ctx, changedGrant, f.in.ProjectID, f.in.RequestID)
	require.NoError(t, err)
	require.Equal(t, f.in.ConfigVersion, row.ConfigVersion)
	_, err = f.startDispatch(ctx, changedGrant, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
	require.ErrorIs(t, err, service.ErrClientGatewayChanged)
	withdrawn := gatewayGrantFixture(f.session, "withdrawn-route")
	withdrawn.RouteIDs = []string{"other-route"}
	withdrawnGrant, err := NewClientGatewayRepository(f.db).Issue(ctx, withdrawn, sessionHash("withdrawn-route-token"), "", "")
	require.NoError(t, err)
	_, err = f.repo.Get(ctx, withdrawnGrant, f.in.ProjectID, f.in.RequestID)
	require.ErrorIs(t, err, service.ErrClientGatewayRevoked)
}

func TestClientInvocationLedgerPostgresExpiredRecoveryRetainsTombstone(t *testing.T) {
	f := invocationLedgerFixture(t)
	ctx := context.Background()
	f.reserve(t)
	_, err := f.db.Exec(`UPDATE client_invocations SET created_at=clock_timestamp()-INTERVAL '8 days',
		lease_expires_at=clock_timestamp()-INTERVAL '7 days',idempotency_expires_at=clock_timestamp()-INTERVAL '1 day'
		WHERE invocation_id=$1`, f.in.InvocationID)
	require.NoError(t, err)
	_, err = f.repo.Get(ctx, f.grant, f.in.ProjectID, f.in.RequestID)
	require.ErrorIs(t, err, service.ErrClientInvocationExpired)
	next := *f.in
	next.InvocationID, next.OwnerToken = "different-task-same-logical-request", invocationTestOwner('B')
	_, err = f.repo.Reserve(ctx, f.grant, &next)
	require.ErrorIs(t, err, service.ErrClientInvocationExpired)
	_, err = f.startDispatch(ctx, f.grant, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
	require.ErrorIs(t, err, service.ErrClientInvocationExpired)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocations`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestClientInvocationLedgerPostgresRevocationAndFailuresNeverDispatch(t *testing.T) {
	for _, failure := range []string{"logout", "disabled", "expired", "transaction-failure", "attempt-insert-failure"} {
		t.Run(failure, func(t *testing.T) {
			f := invocationLedgerFixture(t)
			ctx := context.Background()
			f.reserve(t)
			switch failure {
			case "logout":
				require.NoError(t, NewAuthSessionRepository(f.db).RevokeRefresh(ctx, sessionHash("billing-session-token")))
			case "disabled":
				_, err := f.db.Exec(`UPDATE users SET status='disabled' WHERE id=$1`, f.session.UserID)
				require.NoError(t, err)
			case "expired":
				// Both endpoints must use one instant; two clock_timestamp calls
				// can make a five-minute fixture exceed the DB's strict maximum.
				_, err := f.db.Exec(`WITH timing AS (SELECT clock_timestamp() AS now)
					UPDATE client_gateway_grants SET issued_at=timing.now-INTERVAL '6 minutes',
					expires_at=timing.now-INTERVAL '1 minute' FROM timing WHERE grant_id=$1`, f.grant.GrantID)
				require.NoError(t, err)
			case "transaction-failure":
				_, err := f.db.Exec(`CREATE FUNCTION reject_invocation_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN IF NEW.state='dispatched' THEN RAISE EXCEPTION 'fixture dispatch write fault'; END IF; RETURN NEW; END $$;
					CREATE TRIGGER reject_invocation_dispatch BEFORE UPDATE ON client_invocations FOR EACH ROW EXECUTE FUNCTION reject_invocation_dispatch();`)
				require.NoError(t, err)
			case "attempt-insert-failure":
				_, err := f.db.Exec(`CREATE FUNCTION reject_invocation_attempt_insert() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN RAISE EXCEPTION 'fixture attempt insert fault'; END $$;
					CREATE TRIGGER reject_invocation_attempt_insert BEFORE INSERT ON client_invocation_attempts
					FOR EACH ROW EXECUTE FUNCTION reject_invocation_attempt_insert();`)
				require.NoError(t, err)
			}
			_, err := f.startDispatch(ctx, f.grant, f.in.InvocationID, f.in.OwnerToken, invocationTestDispatchBinding())
			require.Error(t, err)
			var state string
			require.NoError(t, f.db.QueryRow(`SELECT state FROM client_invocations WHERE invocation_id=$1`, f.in.InvocationID).Scan(&state))
			require.Equal(t, service.ClientInvocationReserved, state)
			var attempts int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM client_invocation_attempts WHERE invocation_id=$1`, f.in.InvocationID).Scan(&attempts))
			if failure == "transaction-failure" {
				require.Equal(t, 1, attempts, "a committed preparation remains durable when the later dispatch transaction fails")
				assertInvocationAttempt(t, f, f.in.InvocationID, "prepared", true, false)
			} else {
				require.Zero(t, attempts, "failures before preparation leave no durable attempt")
			}
		})
	}
}
