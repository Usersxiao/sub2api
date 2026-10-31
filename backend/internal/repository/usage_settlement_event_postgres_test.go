//go:build unit

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type settlementEventPGFixture struct {
	*invocationPostgresFixture
	events  service.UsageSettlementEventRepository
	billing service.UsageBillingRepository
	logs    service.UsageLogRepository
	binding *service.UsageSettlementBinding
	event   *service.UsageSettlementEvent
}

// The shared fixture uses only an explicit disposable AUTH_SESSION_TEST_DSN and
// a new isolated schema per test. Apply actual SQL migrations for the columns
// which the repository owns beyond Ent's generated baseline.
func settlementEventPostgresFixture(t *testing.T, dispatched bool) *settlementEventPGFixture {
	t.Helper()
	base := invocationLedgerFixture(t)
	// Ent's Go defaults do not install the SQL timestamp defaults used by
	// the raw batch repository. These tables are empty in this isolated schema;
	// recreate them with the actual foundation and incremental migrations.
	_, err := base.db.Exec(`DROP TABLE batch_image_events,batch_image_items,batch_image_jobs`)
	require.NoError(t, err)
	for _, name := range []string{
		"027_usage_billing_consistency.sql", "046_add_usage_log_reasoning_effort.sql",
		"060_add_usage_log_openai_ws_mode.sql", "061_add_usage_log_request_type.sql",
		"070_add_usage_log_service_tier.sql", "071_add_usage_billing_dedup.sql", "073_add_usage_billing_dedup_archive.sql",
		"074_add_usage_log_endpoints.sql", "089_usage_log_image_output_tokens.sql", "179_usage_log_image_input_tokens.sql",
		"159_batch_image_foundation.sql", "160_batch_image_provider_refs.sql", "161_batch_image_pricing_snapshot.sql",
		"164_batch_image_download_and_user_delete.sql", "166_batch_image_task_name.sql", "169_batch_image_parent_batch.sql",
		"187_add_usage_log_session_id.sql", "231_add_usage_log_requested_reasoning_effort.sql",
		"231_add_usage_log_native_compaction_v2.sql", "232_add_usage_log_upstream_request_id.sql", "252_usage_settlement_events.sql",
		"253_usage_settlement_platform_quota_journal.sql",
		"254_user_platform_quota_client_resets.sql",
		"255_usage_billing_ordinary_platform_quota_journal.sql",
		"256_usage_billing_batch_image_platform_quota_journal.sql",
		"257_batch_image_balance_lifecycle_journal.sql",
		"258_batch_image_native_birth.sql",
		"259_platform_quota_native_integrity_indexes.sql",
		"260_user_platform_quota_snapshot_fence.sql",
		"261_ordinary_platform_quota_control_binding.sql",
		"262_ordinary_platform_quota_deliveries.sql",
	} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		require.NoError(t, err)
		_, err = base.db.Exec(string(raw))
		require.NoError(t, err, name)
	}
	// The account-stats migration also creates channel tables outside this
	// fixture's boundary. Apply its exact usage_logs statement only.
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "101_add_account_stats_pricing.sql"))
	require.NoError(t, err)
	start := strings.Index(string(raw), "ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS account_stats_cost")
	require.NotEqual(t, -1, start)
	_, err = base.db.Exec(strings.SplitN(string(raw)[start:], ";", 2)[0])
	require.NoError(t, err)
	account, err := base.client.Account.Create().SetName("settlement-account").SetPlatform(service.PlatformOpenAI).
		SetType(service.AccountTypeAPIKey).Save(context.Background())
	require.NoError(t, err)
	if dispatched {
		base.dispatch(t)
	} else {
		base.reserve(t)
	}
	cost := 0.00007812504
	groupID := base.in.GroupID
	usage := &service.UsageLog{UserID: base.session.UserID, APIKeyID: base.in.BillingIdentityID, AccountID: account.ID,
		RequestID: "client:" + base.in.InvocationID, Model: "shared-model", GroupID: &groupID,
		InputTokens: 10, OutputTokens: 5, InputCost: cost / 2, OutputCost: cost / 2, TotalCost: cost, ActualCost: cost,
		RateMultiplier: 1.12344, BillingType: service.BillingTypeBalance, RequestType: service.RequestTypeSync}
	command := &service.UsageBillingCommand{RequestID: usage.RequestID, APIKeyID: usage.APIKeyID,
		UserID: usage.UserID, AccountID: account.ID, AccountType: service.AccountTypeAPIKey, Model: usage.Model,
		InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, BalanceCost: cost, RequestPayloadHash: sessionHash("effective private payload")}
	event, err := service.FreezeUsageSettlementEvent(base.in.InvocationID, command, usage)
	require.NoError(t, err)
	return &settlementEventPGFixture{base, NewUsageSettlementEventRepository(base.db), NewUsageBillingRepository(base.client, base.db),
		NewUsageLogRepository(base.client, base.db), &service.UsageSettlementBinding{InvocationID: base.in.InvocationID, OwnerToken: base.in.OwnerToken}, event}
}

func settlementTestLease(letter byte) string {
	return "settlement-" + strings.Repeat(string(letter), 43)
}

func settlementCloneEvent(t *testing.T, event *service.UsageSettlementEvent) *service.UsageSettlementEvent {
	t.Helper()
	raw, err := service.EncodeUsageSettlementEvent(event)
	require.NoError(t, err)
	clone, err := service.DecodeUsageSettlementEvent(raw)
	require.NoError(t, err)
	return clone
}

func (f *settlementEventPGFixture) stage(t *testing.T) *service.UsageSettlementEvent {
	t.Helper()
	claim, err := f.events.Stage(context.Background(), f.binding, f.event, settlementTestLease('A'))
	require.NoError(t, err)
	require.True(t, claim.Owned)
	require.Equal(t, service.UsageSettlementPending, claim.Event.State)
	require.Equal(t, settlementTestLease('A'), claim.Event.LeaseToken)
	require.NotNil(t, claim.Event.LeaseExpiresAt)
	require.True(t, claim.Event.LeaseExpiresAt.After(time.Now()))
	require.Equal(t, claim.Event.CreatedAt, claim.Event.Usage.CreatedAt)
	f.event = claim.Event
	return claim.Event
}

func (f *settlementEventPGFixture) expireLease(t *testing.T) {
	t.Helper()
	_, err := f.db.Exec(`UPDATE usage_settlement_events SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE invocation_id=$1`, f.event.InvocationID)
	require.NoError(t, err)
}

func (f *settlementEventPGFixture) persist(t *testing.T) service.UsageSettlementResult {
	t.Helper()
	_, err := f.billing.Apply(context.Background(), f.event.Command)
	require.NoError(t, err)
	_, err = f.logs.Create(context.Background(), f.event.Usage)
	require.NoError(t, err)
	require.Positive(t, f.event.Usage.ID)
	return service.UsageSettlementResult{RequestID: f.event.RequestID, APIKeyID: f.event.APIKeyID,
		UsageLogID: f.event.Usage.ID, BillingFingerprint: f.event.Command.RequestFingerprint}
}

func (f *settlementEventPGFixture) stageAnother(t *testing.T, id string) *service.UsageSettlementEvent {
	t.Helper()
	next := *f.in
	next.InvocationID, next.RequestID, next.OwnerToken = id, "request-"+id, invocationTestOwner('B')
	_, err := f.repo.Reserve(context.Background(), f.grant, &next)
	require.NoError(t, err)
	_, err = startInvocationDispatch(context.Background(), f.repo, f.grant, next.InvocationID, next.OwnerToken, invocationTestDispatchBinding())
	require.NoError(t, err)
	event := settlementCloneEvent(t, f.event)
	event.InvocationID, event.RequestID = next.InvocationID, "client:"+next.InvocationID
	event.Command.RequestID, event.Usage.RequestID, event.EventFingerprint = event.RequestID, event.RequestID, ""
	claim, err := f.events.Stage(context.Background(), &service.UsageSettlementBinding{InvocationID: next.InvocationID, OwnerToken: next.OwnerToken}, event, settlementTestLease('B'))
	require.NoError(t, err)
	require.True(t, claim.Owned)
	return claim.Event
}

func TestUsageSettlementEventPostgresStagesOnlyOwnedDispatchedWork(t *testing.T) {
	for _, state := range []string{service.ClientInvocationDispatched, service.ClientInvocationUnknown, service.ClientInvocationSucceeded, service.ClientInvocationFailed, service.ClientInvocationReserved} {
		t.Run(state, func(t *testing.T) {
			f := settlementEventPostgresFixture(t, state != service.ClientInvocationReserved)
			if state == service.ClientInvocationUnknown {
				_, err := f.repo.MarkUnknown(context.Background(), f.in.InvocationID, f.in.OwnerToken)
				require.NoError(t, err)
			}
			if state == service.ClientInvocationSucceeded || state == service.ClientInvocationFailed {
				outcome := service.ClientInvocationOutcome{State: service.ClientInvocationFailed, ErrorCode: "UPSTREAM_UNAVAILABLE"}
				if state == service.ClientInvocationSucceeded {
					text := "owned result"
					outcome = service.ClientInvocationOutcome{State: state, Text: &text}
				}
				_, err := f.repo.Finish(context.Background(), f.in.InvocationID, f.in.OwnerToken, outcome)
				require.NoError(t, err)
			}
			// Accounting must still be stageable after the login has been revoked.
			require.NoError(t, NewAuthSessionRepository(f.db).RevokeRefresh(context.Background(), sessionHash("billing-session-token")))
			claim, err := f.events.Stage(context.Background(), f.binding, f.event, settlementTestLease('A'))
			if state == service.ClientInvocationReserved {
				require.ErrorIs(t, err, service.ErrUsageSettlementLease)
				require.Nil(t, claim)
			} else {
				require.NoError(t, err)
				require.True(t, claim.Owned)
			}
		})
	}
}

func TestUsageSettlementEventPostgresRejectsForeignBindingsAndImmutableChanges(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	ctx := context.Background()
	wrong := *f.binding
	wrong.OwnerToken = invocationTestOwner('B')
	_, err := f.events.Stage(ctx, &wrong, f.event, settlementTestLease('A'))
	require.ErrorIs(t, err, service.ErrUsageSettlementLease)
	for _, field := range []string{"key", "user", "group", "invocation"} {
		candidate := settlementCloneEvent(t, f.event)
		switch field {
		case "key":
			candidate.APIKeyID++
			candidate.Command.APIKeyID, candidate.Usage.APIKeyID = candidate.APIKeyID, candidate.APIKeyID
		case "user":
			candidate.Command.UserID++
			candidate.Usage.UserID = candidate.Command.UserID
		case "group":
			*candidate.Usage.GroupID = *candidate.Usage.GroupID + 1
		case "invocation":
			candidate.InvocationID = "other-invocation"
			candidate.RequestID = "client:" + candidate.InvocationID
			candidate.Command.RequestID, candidate.Usage.RequestID = candidate.RequestID, candidate.RequestID
		}
		candidate.EventFingerprint = ""
		_, err = f.events.Stage(ctx, f.binding, candidate, settlementTestLease('A'))
		require.ErrorIs(t, err, service.ErrUsageSettlementEventConflict, field)
	}
	f.stage(t)
	firstPayload, err := service.EncodeUsageSettlementEvent(f.event)
	require.NoError(t, err)
	duplicate, err := NewUsageSettlementEventRepository(f.db).Stage(ctx, f.binding, settlementCloneEvent(t, f.event), settlementTestLease('B'))
	require.NoError(t, err)
	require.False(t, duplicate.Owned)
	require.Empty(t, duplicate.Event.LeaseToken)
	changed := settlementCloneEvent(t, f.event)
	changed.Command.RequestPayloadHash = sessionHash("different payload")
	changed.EventFingerprint = ""
	_, err = f.events.Stage(ctx, f.binding, changed, settlementTestLease('B'))
	require.ErrorIs(t, err, service.ErrUsageSettlementEventConflict)
	var storedPayload, storedHash string
	require.NoError(t, f.db.QueryRow(`SELECT payload,lease_token_hash FROM usage_settlement_events WHERE invocation_id=$1`, f.event.InvocationID).Scan(&storedPayload, &storedHash))
	require.Equal(t, string(firstPayload), storedPayload)
	require.NotContains(t, storedPayload+storedHash, f.binding.OwnerToken)
	require.NotContains(t, storedPayload+storedHash, settlementTestLease('A'))
	encoded, err := json.Marshal(duplicate)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(encoded))
}

func TestUsageSettlementEventPostgresConcurrentStageAndLeaseTakeover(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	raw, err := service.EncodeUsageSettlementEvent(f.event)
	require.NoError(t, err)
	const concurrency = 8
	type result struct {
		claim *service.UsageSettlementClaim
		err   error
	}
	results := make(chan result, concurrency)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			event, decodeErr := service.DecodeUsageSettlementEvent(raw)
			if decodeErr != nil {
				results <- result{err: decodeErr}
				return
			}
			claim, stageErr := f.events.Stage(context.Background(), f.binding, event, settlementTestLease(byte('A'+i)))
			results <- result{claim, stageErr}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	owned := 0
	var oldToken string
	for result := range results {
		require.NoError(t, result.err)
		if result.claim.Owned {
			owned++
			oldToken = result.claim.Event.LeaseToken
		}
	}
	require.Equal(t, 1, owned)
	f.expireLease(t)
	claimed := make(chan []*service.UsageSettlementEvent, concurrency)
	errors := make(chan error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			events, err := NewUsageSettlementEventRepository(f.db).ClaimPending(context.Background(), settlementTestLease(byte('K'+i)), 1)
			claimed <- events
			errors <- err
		}(i)
	}
	wg.Wait()
	close(claimed)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	count := 0
	for events := range claimed {
		count += len(events)
	}
	require.Equal(t, 1, count)
	require.ErrorIs(t, f.events.Release(context.Background(), f.event.InvocationID, oldToken, false), service.ErrUsageSettlementLease)
}

func TestUsageSettlementEventPostgresClaimsAreBoundedAndSkipLockedRows(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	f.stage(t)
	next := f.stageAnother(t, "invocation-two")
	_, err := f.db.Exec(`UPDATE usage_settlement_events SET lease_expires_at=clock_timestamp()-INTERVAL '1 second'`)
	require.NoError(t, err)
	tx, err := f.db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()
	var locked string
	require.NoError(t, tx.QueryRow(`SELECT invocation_id FROM usage_settlement_events WHERE invocation_id=$1 FOR UPDATE`, f.in.InvocationID).Scan(&locked))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, err := f.events.ClaimPending(ctx, settlementTestLease('C'), 1)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, next.InvocationID, events[0].InvocationID)
	for _, limit := range []int{0, -1, 101} {
		_, err := f.events.ClaimPending(context.Background(), settlementTestLease('D'), limit)
		require.Error(t, err)
	}
}

func TestUsageSettlementEventPostgresQuarantinesCorruptRowsBeforeClaimingHealthyWork(t *testing.T) {
	for _, corruption := range []string{"payload", "fingerprint", "binding"} {
		t.Run(corruption, func(t *testing.T) {
			f := settlementEventPostgresFixture(t, true)
			f.stage(t)
			healthy := f.stageAnother(t, "invocation-healthy")
			_, err := f.db.Exec(`UPDATE usage_settlement_events SET lease_expires_at=clock_timestamp()-INTERVAL '1 second'`)
			require.NoError(t, err)
			// Always put the malformed event first, even when requesting one item.
			_, err = f.db.Exec(`UPDATE usage_settlement_events SET lease_expires_at=clock_timestamp()-INTERVAL '2 seconds' WHERE invocation_id=$1`, f.event.InvocationID)
			require.NoError(t, err)
			switch corruption {
			case "payload":
				_, err = f.db.Exec(`UPDATE usage_settlement_events SET payload=$1 WHERE invocation_id=$2`, `{"private":"invalid closed schema"}`, f.event.InvocationID)
			case "fingerprint":
				_, err = f.db.Exec(`UPDATE usage_settlement_events SET event_fingerprint=$1 WHERE invocation_id=$2`, sessionHash("corrupt fingerprint"), f.event.InvocationID)
			case "binding":
				_, err = f.db.Exec(`UPDATE usage_settlement_events SET group_id=group_id+1000 WHERE invocation_id=$1`, f.event.InvocationID)
			}
			require.NoError(t, err)
			var before string
			require.NoError(t, f.db.QueryRow(`SELECT json_build_array(payload,event_fingerprint,api_key_id,user_id,group_id)::text
				FROM usage_settlement_events WHERE invocation_id=$1`, f.event.InvocationID).Scan(&before))
			claimed, err := f.events.ClaimPending(context.Background(), settlementTestLease('C'), 1)
			require.NoError(t, err)
			require.Len(t, claimed, 1)
			require.Equal(t, healthy.InvocationID, claimed[0].InvocationID)
			require.Equal(t, settlementTestLease('C'), claimed[0].LeaseToken)
			var state, code, after string
			var leaseCleared bool
			require.NoError(t, f.db.QueryRow(`SELECT state,last_error_code,lease_expires_at IS NULL,
				json_build_array(payload,event_fingerprint,api_key_id,user_id,group_id)::text
				FROM usage_settlement_events WHERE invocation_id=$1`, f.event.InvocationID).Scan(&state, &code, &leaseCleared, &after))
			require.Equal(t, string(service.UsageSettlementConflict), state)
			require.Equal(t, "SETTLEMENT_CONFLICT", code)
			require.True(t, leaseCleared)
			require.Equal(t, before, after, "quarantine preserves the original private evidence")
			claimed, err = f.events.ClaimPending(context.Background(), settlementTestLease('D'), 1)
			require.NoError(t, err)
			require.Empty(t, claimed, "terminal malformed rows are never replayed")
		})
	}
}

func TestUsageSettlementEventPostgresClaimSQLFailureRollsBackQuarantine(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	f.stage(t)
	healthy := f.stageAnother(t, "invocation-healthy")
	_, err := f.db.Exec(`UPDATE usage_settlement_events SET lease_expires_at=clock_timestamp()-INTERVAL '1 second'`)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE usage_settlement_events SET payload='{}',
		lease_expires_at=clock_timestamp()-INTERVAL '2 seconds' WHERE invocation_id=$1`, f.event.InvocationID)
	require.NoError(t, err)
	_, err = f.db.Exec(`CREATE FUNCTION settlement_fixture_write_failure() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.invocation_id='invocation-healthy' THEN RAISE EXCEPTION 'fixture write failure'; END IF; RETURN NEW; END; $$;
		CREATE TRIGGER settlement_fixture_write_failure BEFORE UPDATE ON usage_settlement_events
		FOR EACH ROW EXECUTE FUNCTION settlement_fixture_write_failure()`)
	require.NoError(t, err)
	claimed, err := f.events.ClaimPending(context.Background(), settlementTestLease('C'), 1)
	require.Error(t, err)
	require.Nil(t, claimed)
	var state string
	var untouched bool
	require.NoError(t, f.db.QueryRow(`SELECT state,last_error_code IS NULL FROM usage_settlement_events WHERE invocation_id=$1`, f.event.InvocationID).Scan(&state, &untouched))
	require.Equal(t, string(service.UsageSettlementPending), state)
	require.True(t, untouched)
	require.NoError(t, f.db.QueryRow(`SELECT lease_token_hash=$1 FROM usage_settlement_events WHERE invocation_id=$2`, sessionHash(settlementTestLease('B')), healthy.InvocationID).Scan(&untouched))
	require.True(t, untouched)
}

func TestUsageSettlementEventPostgresBoundsCorruptBacklogPerClaim(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	f.stage(t)
	for i := range 100 {
		f.stageAnother(t, fmt.Sprintf("invocation-corrupt-%d", i))
	}
	healthy := f.stageAnother(t, "invocation-healthy")
	_, err := f.db.Exec(`UPDATE usage_settlement_events SET lease_expires_at=clock_timestamp()-INTERVAL '1 second'`)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE usage_settlement_events SET payload='{}',
		lease_expires_at=clock_timestamp()-INTERVAL '2 seconds' WHERE invocation_id<>$1`, healthy.InvocationID)
	require.NoError(t, err)
	claimed, err := f.events.ClaimPending(context.Background(), settlementTestLease('C'), 1)
	require.NoError(t, err)
	require.Empty(t, claimed, "a large malformed backlog never creates an unbounded claim transaction")
	var conflicts, pending int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FILTER (WHERE state='conflict'),count(*) FILTER (WHERE state='pending') FROM usage_settlement_events`).Scan(&conflicts, &pending))
	require.Equal(t, 100, conflicts)
	require.Equal(t, 2, pending)
	claimed, err = f.events.ClaimPending(context.Background(), settlementTestLease('D'), 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, healthy.InvocationID, claimed[0].InvocationID)
}

func TestUsageSettlementEventPostgresRequiresRealMatchingProofAndLease(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	f.stage(t)
	ctx := context.Background()
	proof := service.UsageSettlementResult{RequestID: f.event.RequestID, APIKeyID: f.event.APIKeyID, UsageLogID: 999,
		BillingFingerprint: f.event.Command.RequestFingerprint}
	require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof), service.ErrUsageSettlementUnconfirmed)
	proof = f.persist(t)
	require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('B'), proof), service.ErrUsageSettlementLease)
	for _, field := range []string{"key", "request", "fingerprint", "usage"} {
		bad := proof
		switch field {
		case "key":
			bad.APIKeyID++
		case "request":
			bad.RequestID = "client:other"
		case "fingerprint":
			bad.BillingFingerprint = sessionHash("wrong proof")
		case "usage":
			bad.UsageLogID++
		}
		err := f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), bad)
		require.Error(t, err, field)
	}
	// A genuine row ID cannot prove a different frozen amount.
	_, err := f.db.Exec(`UPDATE usage_logs SET actual_cost=actual_cost+1 WHERE id=$1`, proof.UsageLogID)
	require.NoError(t, err)
	require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof), service.ErrUsageSettlementEventConflict)
	_, err = f.db.Exec(`UPDATE usage_logs SET actual_cost=$1 WHERE id=$2`, f.event.Usage.ActualCost, proof.UsageLogID)
	require.NoError(t, err)
	// Correct financial amounts cannot prove consumption on another date.
	_, err = f.db.Exec(`UPDATE usage_logs SET created_at=created_at+INTERVAL '1 day' WHERE id=$1`, proof.UsageLogID)
	require.NoError(t, err)
	require.ErrorIs(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof), service.ErrUsageSettlementEventConflict)
	_, err = f.db.Exec(`UPDATE usage_logs SET created_at=$1 WHERE id=$2`, f.event.CreatedAt, proof.UsageLogID)
	require.NoError(t, err)
	// Exercise lib/pq's actual timestamp text path and PostgreSQL's precision,
	// including both half-way cases and a carry into the following second.
	for _, tc := range []struct {
		name      string
		nanos     int
		persisted int
	}{
		{"below-half", 123456499, 123456000},
		{"even-half", 123456500, 123456000},
		{"above-half", 123456501, 123457000},
		{"odd-half", 123457500, 123458000},
		{"second-carry", 999999500, 1000000000},
	} {
		t.Run("timestamp-"+tc.name, func(t *testing.T) {
			expected := *f.event.Usage
			expected.CreatedAt = time.Date(2026, 10, 25, 12, 0, 0, tc.nanos, time.UTC)
			actual := expected
			require.NoError(t, f.db.QueryRow(`SELECT $1::timestamptz`, expected.CreatedAt).Scan(&actual.CreatedAt))
			require.True(t, time.Date(2026, 10, 25, 12, 0, 0, tc.persisted, time.UTC).Equal(actual.CreatedAt),
				"PostgreSQL timestamp precision differs for %s: %s", tc.name, actual.CreatedAt)
			require.True(t, service.UsageSettlementLogsMatch(&expected, &actual))
		})
	}
	// Cold archived dedup records prove the same durable billing as hot rows.
	_, err = f.db.Exec(`INSERT INTO usage_billing_dedup_archive(request_id,api_key_id,request_fingerprint,created_at)
		SELECT request_id,api_key_id,request_fingerprint,created_at FROM usage_billing_dedup;
		DELETE FROM usage_billing_dedup`)
	require.NoError(t, err)
	require.NoError(t, f.events.Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof))
	require.NoError(t, NewUsageSettlementEventRepository(f.db).Complete(ctx, f.event.InvocationID, settlementTestLease('A'), proof))
	claim, err := f.events.Stage(ctx, f.binding, settlementCloneEvent(t, f.event), settlementTestLease('C'))
	require.NoError(t, err)
	require.False(t, claim.Owned)
	require.Equal(t, service.UsageSettlementSettled, claim.Event.State)
	require.Equal(t, proof.UsageLogID, claim.Event.UsageLogID)
	events, err := f.events.ClaimPending(ctx, settlementTestLease('D'), 10)
	require.NoError(t, err)
	require.Empty(t, events)
}

func TestUsageSettlementEventPostgresReleaseRetainsPayloadAndStopsConflicts(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	f.stage(t)
	ctx := context.Background()
	require.NoError(t, f.events.Release(ctx, f.event.InvocationID, settlementTestLease('A'), false))
	events, err := f.events.ClaimPending(ctx, settlementTestLease('B'), 1)
	require.NoError(t, err)
	require.Empty(t, events, "a transient failure delays the next recovery")
	var code, payload string
	require.NoError(t, f.db.QueryRow(`SELECT last_error_code,payload FROM usage_settlement_events WHERE invocation_id=$1`, f.event.InvocationID).Scan(&code, &payload))
	require.Equal(t, "SETTLEMENT_RETRY", code)
	f.expireLease(t)
	require.ErrorIs(t, f.events.Release(ctx, f.event.InvocationID, settlementTestLease('A'), true), service.ErrUsageSettlementLease)
	events, err = f.events.ClaimPending(ctx, settlementTestLease('B'), 1)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.NoError(t, f.events.Release(ctx, f.event.InvocationID, settlementTestLease('B'), true))
	require.NoError(t, f.events.Release(ctx, f.event.InvocationID, settlementTestLease('B'), true))
	var current string
	require.NoError(t, f.db.QueryRow(`SELECT last_error_code,payload FROM usage_settlement_events WHERE invocation_id=$1`, f.event.InvocationID).Scan(&code, &current))
	require.Equal(t, "SETTLEMENT_CONFLICT", code)
	require.Equal(t, payload, current)
	events, err = f.events.ClaimPending(ctx, settlementTestLease('C'), 100)
	require.NoError(t, err)
	require.Empty(t, events)
}

type settlementPGInvalidator struct{ calls int }

func (s *settlementPGInvalidator) InvalidateUsageSettlement(context.Context, *service.UsageSettlementEvent) error {
	s.calls++
	return nil
}

func TestUsageSettlementEventPostgresRecoversCommittedBillingAfterCrashWithoutDoubleCharge(t *testing.T) {
	for _, crash := range []string{"after-billing", "after-usage"} {
		t.Run(crash, func(t *testing.T) {
			f := settlementEventPostgresFixture(t, true)
			staged := f.stage(t)
			ctx := context.Background()
			bill, err := f.billing.Apply(ctx, staged.Command)
			require.NoError(t, err)
			require.True(t, bill.Applied)
			if crash == "after-usage" {
				_, err = f.logs.Create(ctx, staged.Usage)
				require.NoError(t, err)
			}
			var balanceAfterCommit float64
			require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&balanceAfterCommit))
			require.InDelta(t, 20-service.QuantizeUsageBillingAmount(staged.Usage.ActualCost), balanceAfterCommit, 1e-12)
			f.expireLease(t)
			require.NoError(t, NewAuthSessionRepository(f.db).RevokeRefresh(ctx, sessionHash("billing-session-token")))
			invalidator := &settlementPGInvalidator{}
			worker := service.NewUsageSettlementReconciler(NewUsageSettlementEventRepository(f.db), NewUsageBillingRepository(f.client, f.db),
				NewUsageLogRepository(f.client, f.db), invalidator)
			result, err := worker.Reconcile(ctx, 10)
			require.NoError(t, err)
			require.Equal(t, 1, result.Claimed)
			require.Equal(t, 1, result.Settled)
			require.Zero(t, result.Deferred)
			require.Zero(t, result.Conflicts)
			require.Equal(t, 1, invalidator.calls)
			var finalBalance float64
			var usageCount, billingCount int
			require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&finalBalance))
			require.Equal(t, balanceAfterCommit, finalBalance, "recovery never deducts the frozen command twice")
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1 AND api_key_id=$2`, staged.RequestID, staged.APIKeyID).Scan(&usageCount))
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_billing_dedup WHERE request_id=$1 AND api_key_id=$2`, staged.RequestID, staged.APIKeyID).Scan(&billingCount))
			require.Equal(t, 1, usageCount)
			require.Equal(t, 1, billingCount)
			var createdAt time.Time
			var state string
			require.NoError(t, f.db.QueryRow(`SELECT state,usage_log_id FROM usage_settlement_events WHERE invocation_id=$1`, staged.InvocationID).Scan(&state, &staged.UsageLogID))
			require.Equal(t, string(service.UsageSettlementSettled), state)
			require.NoError(t, f.db.QueryRow(`SELECT created_at FROM usage_logs WHERE id=$1`, staged.UsageLogID).Scan(&createdAt))
			require.True(t, staged.CreatedAt.Equal(createdAt), fmt.Sprintf("original consumption time changed: %s / %s", staged.CreatedAt, createdAt))
			result, err = worker.Reconcile(ctx, 10)
			require.NoError(t, err)
			require.Zero(t, result.Claimed)
		})
	}
}

func TestUsageSettlementEventPostgresRecoveryStopsConflictingBilling(t *testing.T) {
	f := settlementEventPostgresFixture(t, true)
	f.stage(t)
	_, err := f.db.Exec(`INSERT INTO usage_billing_dedup(request_id,api_key_id,request_fingerprint) VALUES($1,$2,$3)`,
		f.event.RequestID, f.event.APIKeyID, sessionHash("foreign billing"))
	require.NoError(t, err)
	f.expireLease(t)
	worker := service.NewUsageSettlementReconciler(f.events, f.billing, f.logs, &settlementPGInvalidator{})
	result, err := worker.Reconcile(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, result.Conflicts)
	require.Zero(t, result.Settled)
	var balance float64
	var state, code string
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, f.session.UserID).Scan(&balance))
	require.Equal(t, float64(20), balance)
	require.NoError(t, f.db.QueryRow(`SELECT state,last_error_code FROM usage_settlement_events WHERE invocation_id=$1`, f.event.InvocationID).Scan(&state, &code))
	require.Equal(t, string(service.UsageSettlementConflict), state)
	require.Equal(t, "SETTLEMENT_CONFLICT", code)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs`).Scan(&count))
	require.Zero(t, count)
}
