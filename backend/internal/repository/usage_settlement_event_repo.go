package repository

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"regexp"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type usageSettlementEventRepository struct{ db *sql.DB }

func NewUsageSettlementEventRepository(db *sql.DB) service.UsageSettlementEventRepository {
	return &usageSettlementEventRepository{db: db}
}

var usageSettlementLeaseToken = regexp.MustCompile(`^settlement-[A-Za-z0-9_-]{43}$`)

const usageSettlementEventColumns = `e.invocation_id,e.request_id,e.api_key_id,e.user_id,e.group_id,
	e.event_fingerprint,e.payload,e.state,e.lease_token_hash,e.lease_expires_at,e.usage_log_id,
	e.billing_fingerprint,e.created_at,e.updated_at`

type usageSettlementEventRow struct {
	event              *service.UsageSettlementEvent
	invocationID       string
	payload            []byte
	userID, groupID    int64
	leaseHash          string
	billingFingerprint string
}

func scanUsageSettlementEvent(scanner sessionScanner) (*usageSettlementEventRow, error) {
	var requestID, fingerprint, state string
	var apiKeyID int64
	var expires sql.NullTime
	var usageID sql.NullInt64
	var billingFingerprint sql.NullString
	var createdAt, updatedAt time.Time
	row := &usageSettlementEventRow{}
	if err := scanner.Scan(&row.invocationID, &requestID, &apiKeyID, &row.userID, &row.groupID,
		&fingerprint, &row.payload, &state, &row.leaseHash, &expires, &usageID,
		&billingFingerprint, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	event, err := service.DecodeUsageSettlementEvent(row.payload)
	if err != nil {
		return row, err
	}
	if event.InvocationID != row.invocationID || event.RequestID != requestID || event.APIKeyID != apiKeyID ||
		event.EventFingerprint != fingerprint || event.Command.UserID != row.userID ||
		event.Usage.GroupID == nil || *event.Usage.GroupID != row.groupID {
		return row, service.ErrUsageSettlementEventConflict
	}
	event.State, event.CreatedAt, event.UpdatedAt = service.UsageSettlementState(state), createdAt, updatedAt
	event.Usage.CreatedAt = createdAt
	if expires.Valid {
		event.LeaseExpiresAt = &expires.Time
	}
	if usageID.Valid {
		event.UsageLogID = usageID.Int64
	}
	if billingFingerprint.Valid {
		row.billingFingerprint = billingFingerprint.String
	}
	if event.State == service.UsageSettlementSettled && row.billingFingerprint != event.Command.RequestFingerprint {
		return row, service.ErrUsageSettlementEventConflict
	}
	row.event = event
	return row, nil
}

func (r *usageSettlementEventRepository) transaction(ctx context.Context) (*sql.Tx, error) {
	if r == nil || r.db == nil {
		return nil, service.ErrUsageSettlementUnconfirmed
	}
	return r.db.BeginTx(ctx, nil)
}

// Stage validates the private attempt directly against its durable invocation.
// It deliberately takes no account/session/grant locks: logout must not discard
// an already dispatched request's accounting work.
func (r *usageSettlementEventRepository) Stage(ctx context.Context, binding *service.UsageSettlementBinding, event *service.UsageSettlementEvent, leaseToken string) (*service.UsageSettlementClaim, error) {
	if binding == nil || !invocationIdentifier.MatchString(binding.InvocationID) ||
		!invocationOwnerToken.MatchString(binding.OwnerToken) || !usageSettlementLeaseToken.MatchString(leaseToken) {
		return nil, service.ErrUsageSettlementLease
	}
	payload, err := service.EncodeUsageSettlementEvent(event)
	if err != nil {
		return nil, err
	}
	if event.InvocationID != binding.InvocationID || event.Usage.GroupID == nil {
		return nil, service.ErrUsageSettlementEventConflict
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var ledgerOwner, ledgerState string
	var userID, apiKeyID, groupID int64
	err = tx.QueryRowContext(ctx, `SELECT user_id,billing_identity_id,group_id,owner_token_hash,state
		FROM client_invocations WHERE invocation_id=$1 FOR UPDATE`, binding.InvocationID).
		Scan(&userID, &apiKeyID, &groupID, &ledgerOwner, &ledgerState)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrUsageSettlementLease
	}
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare([]byte(ledgerOwner), []byte(service.GatewayTokenHash(binding.OwnerToken))) != 1 ||
		(ledgerState != service.ClientInvocationDispatched && ledgerState != service.ClientInvocationUnknown &&
			ledgerState != service.ClientInvocationSucceeded && ledgerState != service.ClientInvocationFailed) {
		return nil, service.ErrUsageSettlementLease
	}
	if event.RequestID != "client:"+binding.InvocationID || event.APIKeyID != apiKeyID ||
		event.Command.APIKeyID != apiKeyID || event.Usage.APIKeyID != apiKeyID ||
		event.Command.UserID != userID || event.Usage.UserID != userID || *event.Usage.GroupID != groupID {
		return nil, service.ErrUsageSettlementEventConflict
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	row, err := scanUsageSettlementEvent(tx.QueryRowContext(ctx, `INSERT INTO usage_settlement_events AS e
		(invocation_id,request_id,api_key_id,user_id,group_id,event_fingerprint,payload,state,lease_token_hash,
		 lease_expires_at,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)
		ON CONFLICT DO NOTHING RETURNING `+usageSettlementEventColumns,
		event.InvocationID, event.RequestID, apiKeyID, userID, groupID, event.EventFingerprint, string(payload),
		service.UsageSettlementPending, service.GatewayTokenHash(leaseToken), now.Add(service.UsageSettlementLease), now))
	owned := err == nil
	if errors.Is(err, sql.ErrNoRows) {
		row, err = scanUsageSettlementEvent(tx.QueryRowContext(ctx, `SELECT `+usageSettlementEventColumns+`
			FROM usage_settlement_events e WHERE e.invocation_id=$1 FOR UPDATE`, event.InvocationID))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrUsageSettlementEventConflict
		}
	}
	if err != nil {
		return nil, err
	}
	if row.event.EventFingerprint != event.EventFingerprint || !bytes.Equal(row.payload, payload) {
		return nil, service.ErrUsageSettlementEventConflict
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if owned {
		row.event.LeaseToken = leaseToken
	}
	return &service.UsageSettlementClaim{Event: row.event, Owned: owned}, nil
}

// ClaimPending leases only frozen accounting payloads, never invocation work.
// SKIP LOCKED avoids waiting on another worker. At most 100 rows are examined
// per transaction, including deterministic corrupt rows quarantined in place.
func (r *usageSettlementEventRepository) ClaimPending(ctx context.Context, leaseToken string, limit int) ([]*service.UsageSettlementEvent, error) {
	if !usageSettlementLeaseToken.MatchString(leaseToken) {
		return nil, service.ErrUsageSettlementLease
	}
	if limit < 1 || limit > 100 {
		return nil, service.ErrUsageSettlementUnconfirmed
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	claimed := make([]*service.UsageSettlementEvent, 0, limit)
	for examined := 0; examined < 100 && len(claimed) < limit; examined++ {
		row, scanErr := scanUsageSettlementEvent(tx.QueryRowContext(ctx, `SELECT `+usageSettlementEventColumns+`
			FROM usage_settlement_events e WHERE e.state=$1 AND e.lease_expires_at<=clock_timestamp()
			ORDER BY e.lease_expires_at,e.created_at,e.invocation_id LIMIT 1 FOR UPDATE SKIP LOCKED`, service.UsageSettlementPending))
		if errors.Is(scanErr, sql.ErrNoRows) {
			break
		}
		if errors.Is(scanErr, service.ErrUsageSettlementPayload) || errors.Is(scanErr, service.ErrUsageSettlementEventConflict) {
			// A malformed frozen event is permanent. Retain its immutable bytes,
			// isolate it with a fixed diagnostic, then try the next ready row.
			if _, err = tx.ExecContext(ctx, `UPDATE usage_settlement_events SET state=$1,
				lease_expires_at=NULL,last_error_code='SETTLEMENT_CONFLICT',attempts=attempts+1,
				updated_at=clock_timestamp() WHERE invocation_id=$2`, service.UsageSettlementConflict, row.invocationID); err != nil {
				return nil, err
			}
			continue
		}
		if scanErr != nil {
			return nil, scanErr
		}
		var expires time.Time
		if err = tx.QueryRowContext(ctx, `UPDATE usage_settlement_events SET lease_token_hash=$1,
			lease_expires_at=clock_timestamp()+($2 * INTERVAL '1 second'),updated_at=clock_timestamp(),
			attempts=attempts+1,last_error_code=NULL WHERE invocation_id=$3 RETURNING lease_expires_at,updated_at`,
			service.GatewayTokenHash(leaseToken), int64(service.UsageSettlementLease/time.Second), row.invocationID).
			Scan(&expires, &row.event.UpdatedAt); err != nil {
			return nil, err
		}
		row.event.LeaseToken = leaseToken
		row.event.LeaseExpiresAt = &expires
		claimed = append(claimed, row.event)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func lockUsageSettlementEvent(ctx context.Context, tx *sql.Tx, invocationID, leaseToken string) (*usageSettlementEventRow, error) {
	row, err := scanUsageSettlementEvent(tx.QueryRowContext(ctx, `SELECT `+usageSettlementEventColumns+`
		FROM usage_settlement_events e WHERE e.invocation_id=$1 FOR UPDATE`, invocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrUsageSettlementLease
	}
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare([]byte(row.leaseHash), []byte(service.GatewayTokenHash(leaseToken))) != 1 {
		return nil, service.ErrUsageSettlementLease
	}
	return row, nil
}

// Complete requires observations from both real storage surfaces. A caller's
// in-memory success flag, queue acceptance, or bare usage ID proves neither.
func (r *usageSettlementEventRepository) Complete(ctx context.Context, invocationID, leaseToken string, proof service.UsageSettlementResult) error {
	if !invocationIdentifier.MatchString(invocationID) || !usageSettlementLeaseToken.MatchString(leaseToken) {
		return service.ErrUsageSettlementLease
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	row, err := lockUsageSettlementEvent(ctx, tx, invocationID, leaseToken)
	if err != nil {
		return err
	}
	event := row.event
	if event.State == service.UsageSettlementConflict {
		return service.ErrUsageSettlementEventConflict
	}
	if proof.RequestID != event.RequestID || proof.APIKeyID != event.APIKeyID || proof.UsageLogID <= 0 ||
		proof.BillingFingerprint != event.Command.RequestFingerprint ||
		(event.State == service.UsageSettlementSettled && (event.UsageLogID != proof.UsageLogID || row.billingFingerprint != proof.BillingFingerprint)) {
		return service.ErrUsageSettlementEventConflict
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return err
	}
	if event.State == service.UsageSettlementPending && (event.LeaseExpiresAt == nil || !event.LeaseExpiresAt.After(now)) {
		return service.ErrUsageSettlementLease
	}
	if err = verifyUsageSettlementBilling(ctx, tx, event); err != nil {
		return err
	}
	actual, err := scanUsageLog(tx.QueryRowContext(ctx, `SELECT `+usageLogSelectColumns+` FROM usage_logs WHERE id=$1`, proof.UsageLogID))
	if errors.Is(err, sql.ErrNoRows) {
		return service.ErrUsageSettlementUnconfirmed
	}
	if err != nil {
		return err
	}
	if actual.ID != proof.UsageLogID || actual.RequestID != event.RequestID || actual.APIKeyID != event.APIKeyID || actual.UserID != row.userID ||
		actual.GroupID == nil || *actual.GroupID != row.groupID || actual.AccountID != event.Command.AccountID ||
		!service.UsageSettlementLogsMatch(event.Usage, actual) {
		return service.ErrUsageSettlementEventConflict
	}
	if err = verifyUsageSettlementPlatformQuota(ctx, tx, event); err != nil {
		return err
	}
	if event.State == service.UsageSettlementSettled {
		return tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `UPDATE usage_settlement_events SET state=$1,usage_log_id=$2,
		billing_fingerprint=$3,lease_expires_at=NULL,updated_at=clock_timestamp(),last_error_code=NULL WHERE invocation_id=$4`,
		service.UsageSettlementSettled, proof.UsageLogID, proof.BillingFingerprint, invocationID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Release preserves the frozen event. Retry failures delay the next lease;
// conflicts stop automatic recovery and retain only a fixed diagnostic code.
func (r *usageSettlementEventRepository) Release(ctx context.Context, invocationID, leaseToken string, conflict bool) error {
	if !invocationIdentifier.MatchString(invocationID) || !usageSettlementLeaseToken.MatchString(leaseToken) {
		return service.ErrUsageSettlementLease
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	row, err := lockUsageSettlementEvent(ctx, tx, invocationID, leaseToken)
	if err != nil {
		return err
	}
	if row.event.State == service.UsageSettlementConflict && conflict {
		return tx.Commit()
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return err
	}
	if row.event.State != service.UsageSettlementPending || row.event.LeaseExpiresAt == nil || !row.event.LeaseExpiresAt.After(now) {
		return service.ErrUsageSettlementLease
	}
	state, code := service.UsageSettlementPending, "SETTLEMENT_RETRY"
	var expires any = now.Add(service.UsageSettlementLease)
	if conflict {
		state, code, expires = service.UsageSettlementConflict, "SETTLEMENT_CONFLICT", nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE usage_settlement_events SET state=$1,last_error_code=$2,
		lease_expires_at=$3,updated_at=$4 WHERE invocation_id=$5`, state, code, expires, now, invocationID)
	if err != nil {
		return err
	}
	return tx.Commit()
}
