package repository

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type clientInvocationLedgerRepository struct{ db *sql.DB }

func NewClientInvocationLedgerRepository(db *sql.DB) service.ClientInvocationLedgerRepository {
	return &clientInvocationLedgerRepository{db: db}
}

var invocationIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$`)
var invocationFingerprint = regexp.MustCompile(`^[0-9a-f]{64}$`)
var invocationOwnerToken = regexp.MustCompile(`^attempt-[A-Za-z0-9_-]{43}$`)
var invocationProviderPlatform = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

const invocationColumns = `i.invocation_id,i.instance_id,i.user_id,i.consumer_id,i.project_id,i.request_id,
	i.route_id,i.operation,i.config_version,i.entitlement_version,i.fingerprint,i.billing_identity_id,i.group_id,
	i.provider_account_id,i.provider_platform,i.provider_model,i.provider_protocol,i.provider_payload_hash,
	i.provider_counter_proof,i.provider_attempt_no,i.state,i.created_at,i.updated_at,i.lease_expires_at,i.dispatch_deadline,i.idempotency_expires_at,i.outcome,i.owner_token_hash`

type invocationLedgerRow struct {
	*service.ClientInvocationRecord
	ownerHash string
}

func scanInvocation(row sessionScanner) (*invocationLedgerRow, error) {
	record := &service.ClientInvocationRecord{}
	out := &invocationLedgerRow{ClientInvocationRecord: record}
	var deadline sql.NullTime
	var providerAccountID sql.NullInt64
	var providerAttemptNo sql.NullInt64
	var outcome []byte
	var counterProof []byte
	var providerPlatform, providerModel, providerProtocol, providerPayloadHash sql.NullString
	err := row.Scan(&record.InvocationID, &record.InstanceID, &record.UserID, &record.ConsumerID,
		&record.ProjectID, &record.RequestID, &record.RouteID, &record.Operation, &record.ConfigVersion,
		&record.EntitlementVersion, &record.Fingerprint, &record.BillingIdentityID, &record.GroupID,
		&providerAccountID, &providerPlatform, &providerModel, &providerProtocol, &providerPayloadHash, &counterProof, &providerAttemptNo,
		&record.State, &record.CreatedAt, &record.UpdatedAt, &record.LeaseExpiresAt, &deadline,
		&record.IdempotencyExpiresAt, &outcome, &out.ownerHash)
	if err != nil {
		return nil, err
	}
	if providerAccountID.Valid {
		record.ProviderAccountID = providerAccountID.Int64
	}
	if providerAttemptNo.Valid {
		record.ProviderAttemptNo = int16(providerAttemptNo.Int64)
	}
	if deadline.Valid {
		record.DispatchDeadline = &deadline.Time
	}
	if providerPlatform.Valid {
		record.ProviderPlatform = providerPlatform.String
	}
	if providerModel.Valid {
		record.ProviderModel = providerModel.String
	}
	if providerProtocol.Valid {
		record.ProviderProtocol = providerProtocol.String
	}
	if providerPayloadHash.Valid {
		record.ProviderPayloadHash = providerPayloadHash.String
	}
	if len(counterProof) > 0 {
		if record.Operation == "search" {
			record.ProviderSearchProof = &service.ClientInvocationSearchProof{}
			if json.Unmarshal(counterProof, record.ProviderSearchProof) != nil || !service.ValidateClientInvocationSearchProof(record.ProviderSearchProof) {
				return nil, service.ErrClientInvocationConflict
			}
		} else {
			record.ProviderCounterProof = &service.ClientInvocationCounterProof{}
			if json.Unmarshal(counterProof, record.ProviderCounterProof) != nil || !service.ValidateClientInvocationCounterProof(record.ProviderCounterProof) {
				return nil, service.ErrClientInvocationConflict
			}
		}
	}
	if len(outcome) > 0 {
		record.Outcome = &service.ClientInvocationOutcome{}
		if err = json.Unmarshal(outcome, record.Outcome); err != nil || !service.ValidateClientInvocationOutcome(*record.Outcome) || record.Outcome.State != record.State {
			return nil, service.ErrClientGatewayUnavailable
		}
	}
	return out, nil
}

func (r *clientInvocationLedgerRepository) transaction(ctx context.Context) (*sql.Tx, error) {
	if r == nil || r.db == nil {
		return nil, service.ErrClientGatewayUnavailable
	}
	return r.db.BeginTx(ctx, nil)
}

// Lock order matches credential issuance, accounting identity resolution and
// logout. A claimed request is never an independent source of user identity.
func lockInvocationGrant(ctx context.Context, tx *sql.Tx, candidate *service.ClientGatewayGrant) (int64, error) {
	if candidate == nil || candidate.SchemaVersion != 1 || candidate.ClientID != service.ClientGatewayClientID ||
		candidate.ConsumerID == "" || candidate.GrantID == "" {
		return 0, service.ErrClientGatewayScope
	}
	userID, err := strconv.ParseInt(candidate.UpstreamUserID, 10, 64)
	if err != nil || userID <= 0 {
		return 0, service.ErrClientGatewayScope
	}
	if _, err = lockGatewaySession(ctx, tx, userID, candidate.SID, candidate.SessionEpoch); err != nil {
		return 0, err
	}
	stored, err := scanGatewayGrant(tx.QueryRowContext(ctx, `SELECT `+gatewayGrantColumns+`
		FROM client_gateway_grants g WHERE g.grant_id=$1 FOR UPDATE`, candidate.GrantID))
	if errors.Is(err, sql.ErrNoRows) {
		return 0, service.ErrClientGatewayRevoked
	}
	if err != nil {
		return 0, err
	}
	if !sameBillingGrant(stored, candidate) || stored.RevokedAt != nil || !time.Now().Before(stored.ExpiresAt) ||
		len(stored.Operations) != 1 || stored.Operations[0] != "invoke" {
		return 0, service.ErrClientGatewayRevoked
	}
	var consumerID string
	err = tx.QueryRowContext(ctx, `SELECT consumer_id FROM client_gateway_consumers
		WHERE consumer_id=$1 AND instance_id=$2 AND user_id=$3 AND client_id=$4 FOR SHARE`,
		stored.ConsumerID, stored.InstanceID, userID, stored.ClientID).Scan(&consumerID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, service.ErrClientGatewayRevoked
	}
	return userID, err
}

func lockInvocationBilling(ctx context.Context, tx *sql.Tx, userID int64, consumerID string, billingID, groupID int64) error {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM groups WHERE id=$1 AND deleted_at IS NULL AND status=$2 FOR SHARE`,
		groupID, service.StatusActive).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return service.ErrClientGatewayRevoked
	}
	if err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT id FROM api_keys WHERE id=$1 AND user_id=$2
		AND kind=$3 AND client_consumer_id=$4 AND group_id=$5 AND deleted_at IS NULL AND status=$6 FOR SHARE`,
		billingID, userID, service.APIKeyKindClientConsumer, consumerID, groupID, service.StatusActive).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return service.ErrClientGatewayRevoked
	}
	return err
}

func invocationNow(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, err
}

func invocationHasRoute(grant *service.ClientGatewayGrant, route string) bool {
	for _, value := range grant.RouteIDs {
		if value == route {
			return true
		}
	}
	return false
}

func validInvocationReservation(in *service.ClientInvocationReservation) bool {
	return in != nil && invocationIdentifier.MatchString(in.InvocationID) && invocationIdentifier.MatchString(in.ProjectID) &&
		invocationIdentifier.MatchString(in.RequestID) && invocationIdentifier.MatchString(in.RouteID) && (in.Operation == "text" || in.Operation == "search") &&
		invocationIdentifier.MatchString(in.ConfigVersion) && invocationIdentifier.MatchString(in.EntitlementVersion) &&
		invocationFingerprint.MatchString(in.Fingerprint) && invocationOwnerToken.MatchString(in.OwnerToken) &&
		in.BillingIdentityID > 0 && in.GroupID > 0
}

func sameInvocationReservation(row *service.ClientInvocationRecord, in *service.ClientInvocationReservation) bool {
	return row.RouteID == in.RouteID && row.Operation == in.Operation && row.ConfigVersion == in.ConfigVersion &&
		row.EntitlementVersion == in.EntitlementVersion && row.Fingerprint == in.Fingerprint &&
		row.BillingIdentityID == in.BillingIdentityID && row.GroupID == in.GroupID
}

// Reserve may replace an expired reservation owner only before StartDispatch.
// No timeout or retention expiry can authorize sending an already sent row.
func (r *clientInvocationLedgerRepository) Reserve(ctx context.Context, grant *service.ClientGatewayGrant, in *service.ClientInvocationReservation) (*service.ClientInvocationClaim, error) {
	if !validInvocationReservation(in) {
		return nil, service.ErrClientGatewayScope
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	userID, err := lockInvocationGrant(ctx, tx, grant)
	if err != nil {
		return nil, err
	}
	if grant.ConfigVersion != in.ConfigVersion || grant.EntitlementVersion != in.EntitlementVersion {
		return nil, service.ErrClientGatewayChanged
	}
	if !invocationHasRoute(grant, in.RouteID) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err = lockInvocationBilling(ctx, tx, userID, grant.ConsumerID, in.BillingIdentityID, in.GroupID); err != nil {
		return nil, err
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	row, err := scanInvocation(tx.QueryRowContext(ctx, `INSERT INTO client_invocations AS i
		(invocation_id,instance_id,user_id,client_id,consumer_id,project_id,request_id,route_id,operation,
		 config_version,entitlement_version,fingerprint,billing_identity_id,group_id,owner_token_hash,state,
		 created_at,updated_at,lease_expires_at,idempotency_expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$17,$18,$19)
		ON CONFLICT DO NOTHING RETURNING `+invocationColumns,
		in.InvocationID, grant.InstanceID, userID, grant.ClientID, grant.ConsumerID, in.ProjectID, in.RequestID,
		in.RouteID, in.Operation, in.ConfigVersion, in.EntitlementVersion, in.Fingerprint, in.BillingIdentityID,
		in.GroupID, service.GatewayTokenHash(in.OwnerToken), service.ClientInvocationReserved, now,
		now.Add(service.ClientInvocationLease), now.Add(service.ClientInvocationRetention)))
	owned := err == nil
	if errors.Is(err, sql.ErrNoRows) {
		row, err = scanInvocation(tx.QueryRowContext(ctx, `SELECT `+invocationColumns+` FROM client_invocations i
			WHERE i.instance_id=$1 AND i.user_id=$2 AND i.consumer_id=$3 AND i.project_id=$4 AND i.request_id=$5 FOR UPDATE`,
			grant.InstanceID, userID, grant.ConsumerID, in.ProjectID, in.RequestID))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrClientInvocationConflict
		}
		if err != nil {
			return nil, err
		}
		if !row.IdempotencyExpiresAt.After(now) {
			return nil, service.ErrClientInvocationExpired
		}
		if !sameInvocationReservation(row.ClientInvocationRecord, in) {
			return nil, service.ErrClientInvocationConflict
		}
		if row.State == service.ClientInvocationReserved && !row.LeaseExpiresAt.After(now) {
			row, err = scanInvocation(tx.QueryRowContext(ctx, `UPDATE client_invocations AS i SET owner_token_hash=$1,
				lease_expires_at=$2,updated_at=$3 WHERE i.invocation_id=$4 RETURNING `+invocationColumns,
				service.GatewayTokenHash(in.OwnerToken), now.Add(service.ClientInvocationLease), now, row.InvocationID))
			owned = err == nil
		} else {
			err = expireInvocationDispatch(ctx, tx, row, now)
		}
	}
	if err != nil {
		return nil, err
	}
	if !time.Now().Before(grant.ExpiresAt) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &service.ClientInvocationClaim{Record: row.ClientInvocationRecord, Owned: owned}, nil
}

// Get returns only an owned request under a newly verified grant. A new config
// version may recover an older receipt, but the route must still be authorized.
func (r *clientInvocationLedgerRepository) Get(ctx context.Context, grant *service.ClientGatewayGrant, projectID, requestID string) (*service.ClientInvocationRecord, error) {
	if !invocationIdentifier.MatchString(projectID) || !invocationIdentifier.MatchString(requestID) {
		return nil, service.ErrClientGatewayScope
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	userID, err := lockInvocationGrant(ctx, tx, grant)
	if err != nil {
		return nil, err
	}
	row, err := scanInvocation(tx.QueryRowContext(ctx, `SELECT `+invocationColumns+` FROM client_invocations i
		WHERE i.instance_id=$1 AND i.user_id=$2 AND i.consumer_id=$3 AND i.project_id=$4 AND i.request_id=$5 FOR UPDATE`,
		grant.InstanceID, userID, grant.ConsumerID, projectID, requestID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationNotFound
	}
	if err != nil {
		return nil, err
	}
	if !invocationHasRoute(grant, row.RouteID) {
		return nil, service.ErrClientGatewayRevoked
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !row.IdempotencyExpiresAt.After(now) {
		return nil, service.ErrClientInvocationExpired
	}
	if err = expireInvocationDispatch(ctx, tx, row, now); err != nil {
		return nil, err
	}
	if !time.Now().Before(grant.ExpiresAt) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return row.ClientInvocationRecord, nil
}

func expireInvocationDispatch(ctx context.Context, tx *sql.Tx, row *invocationLedgerRow, now time.Time) error {
	if row.State != service.ClientInvocationDispatched || row.DispatchDeadline == nil || row.DispatchDeadline.After(now) {
		return nil
	}
	if err := advanceInvocationAttempt(ctx, tx, row, service.ClientInvocationUnknown, now); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE client_invocations SET state=$1,updated_at=$2
		WHERE invocation_id=$3 AND state=$4`, service.ClientInvocationUnknown, now, row.InvocationID, service.ClientInvocationDispatched)
	if err != nil {
		return err
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return service.ErrClientGatewayUnavailable
	}
	row.State, row.UpdatedAt = service.ClientInvocationUnknown, now
	return nil
}

func advanceInvocationAttempt(ctx context.Context, tx *sql.Tx, row *invocationLedgerRow, nextState string, now time.Time) error {
	// Pre-migration dispatches without a durable provider tuple remain reconcilable,
	// but no provider identity or attempt is fabricated for them.
	if row.ProviderPayloadHash == "" {
		return nil
	}
	if row.ProviderAttemptNo < 1 || row.ProviderAttemptNo > 2 {
		return service.ErrClientGatewayUnavailable
	}
	result, err := tx.ExecContext(ctx, `UPDATE client_invocation_attempts SET state=$1,updated_at=$2
		WHERE invocation_id=$3 AND attempt_no=$4 AND state=$5`, nextState, now, row.InvocationID,
		row.ProviderAttemptNo, row.State)
	if err != nil {
		return err
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return service.ErrClientGatewayUnavailable
	}
	return nil
}

func validInvocationDispatchBinding(binding service.ClientInvocationDispatchBinding) bool {
	return binding.AccountID > 0 && invocationProviderPlatform.MatchString(binding.Platform) &&
		strings.TrimSpace(binding.ModelID) != "" && len([]rune(binding.ModelID)) <= 500 &&
		invocationFingerprint.MatchString(binding.PayloadHash) && ((binding.Protocol == service.ClientInvocationSearchProtocol && binding.Platform == service.PlatformOpenAI && binding.CounterProof == nil && service.ValidateClientInvocationSearchProof(binding.SearchProof)) ||
		((binding.Protocol == "messages" || binding.Protocol == "responses" || binding.Protocol == "chat-completions") && binding.SearchProof == nil && service.ValidateClientInvocationCounterProof(binding.CounterProof)))
}

func latestInvocationAttempt(ctx context.Context, tx *sql.Tx, invocationID string) (int16, string, error) {
	var attemptNo int16
	var state string
	err := tx.QueryRowContext(ctx, `SELECT attempt_no,state FROM client_invocation_attempts
		WHERE invocation_id=$1 ORDER BY attempt_no DESC LIMIT 1 FOR UPDATE`, invocationID).Scan(&attemptNo, &state)
	return attemptNo, state, err
}

// PrepareDispatch idempotently prepares the current attempt. Attempt 2 is
// created only after attempt 1 has a durable not-dispatched result.
func (r *clientInvocationLedgerRepository) PrepareDispatch(ctx context.Context, grant *service.ClientGatewayGrant, invocationID, ownerToken string, binding service.ClientInvocationDispatchBinding) (*service.ClientInvocationRecord, error) {
	if !invocationIdentifier.MatchString(invocationID) || !invocationOwnerToken.MatchString(ownerToken) || !validInvocationDispatchBinding(binding) {
		return nil, service.ErrClientGatewayScope
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	userID, err := lockInvocationGrant(ctx, tx, grant)
	if err != nil {
		return nil, err
	}
	row, err := scanInvocation(tx.QueryRowContext(ctx, `SELECT `+invocationColumns+` FROM client_invocations i
		WHERE i.invocation_id=$1 AND i.instance_id=$2 AND i.user_id=$3 AND i.consumer_id=$4 FOR UPDATE`,
		invocationID, grant.InstanceID, userID, grant.ConsumerID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationNotFound
	}
	if err != nil {
		return nil, err
	}
	if (row.Operation == "search") != (binding.SearchProof != nil) {
		return nil, service.ErrClientGatewayScope
	}
	if !invocationHasRoute(grant, row.RouteID) {
		return nil, service.ErrClientGatewayRevoked
	}
	if row.ConfigVersion != grant.ConfigVersion || row.EntitlementVersion != grant.EntitlementVersion {
		return nil, service.ErrClientGatewayChanged
	}
	if subtle.ConstantTimeCompare([]byte(row.ownerHash), []byte(service.GatewayTokenHash(ownerToken))) != 1 ||
		row.State != service.ClientInvocationReserved || row.ProviderPayloadHash != "" {
		return nil, service.ErrClientInvocationOwner
	}
	if err = lockInvocationBilling(ctx, tx, userID, grant.ConsumerID, row.BillingIdentityID, row.GroupID); err != nil {
		return nil, err
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !row.IdempotencyExpiresAt.After(now) {
		return nil, service.ErrClientInvocationExpired
	}
	if !row.LeaseExpiresAt.After(now) {
		return nil, service.ErrClientInvocationOwner
	}
	counterProof, err := marshalInvocationDispatchProof(binding)
	if err != nil {
		return nil, service.ErrClientGatewayUnavailable
	}
	attemptNo := int16(1)
	previousAttemptNo, previousAttemptState, previousAttemptErr := latestInvocationAttempt(ctx, tx, invocationID)
	switch {
	case errors.Is(previousAttemptErr, sql.ErrNoRows):
	case previousAttemptErr != nil:
		return nil, previousAttemptErr
	case previousAttemptState == "prepared":
		attemptNo = previousAttemptNo
	case previousAttemptNo == 1 && previousAttemptState == service.ClientInvocationAttemptNotDispatched:
		attemptNo = 2
	default:
		return nil, service.ErrClientInvocationOwner
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO client_invocation_attempts
		(invocation_id,attempt_no,state,provider_account_id,provider_platform,provider_model,provider_protocol,
		 provider_payload_hash,provider_counter_proof,dispatch_started_at,updated_at)
		VALUES($1,$2,'prepared',$3,$4,$5,$6,$7,$8,NULL,$9) ON CONFLICT(invocation_id,attempt_no) DO NOTHING`,
		invocationID, attemptNo, binding.AccountID, binding.Platform, binding.ModelID, binding.Protocol, binding.PayloadHash, counterProof, now)
	if err != nil {
		return nil, err
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil {
		return nil, affectedErr
	} else if affected == 0 {
		var matches bool
		err = tx.QueryRowContext(ctx, `SELECT state='prepared' AND provider_account_id=$1 AND provider_platform=$2
			AND provider_model=$3 AND provider_protocol=$4 AND provider_payload_hash=$5
			AND provider_counter_proof=$6::jsonb FROM client_invocation_attempts
			WHERE invocation_id=$7 AND attempt_no=$8 FOR UPDATE`, binding.AccountID, binding.Platform,
			binding.ModelID, binding.Protocol, binding.PayloadHash, counterProof, invocationID, attemptNo).Scan(&matches)
		if err != nil || !matches {
			return nil, service.ErrClientInvocationConflict
		}
	}
	if !time.Now().Before(grant.ExpiresAt) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return row.ClientInvocationRecord, nil
}

func (r *clientInvocationLedgerRepository) StartDispatch(ctx context.Context, grant *service.ClientGatewayGrant, invocationID, ownerToken string, binding service.ClientInvocationDispatchBinding) (*service.ClientInvocationRecord, error) {
	if !invocationIdentifier.MatchString(invocationID) || !invocationOwnerToken.MatchString(ownerToken) || !validInvocationDispatchBinding(binding) {
		return nil, service.ErrClientGatewayScope
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	userID, err := lockInvocationGrant(ctx, tx, grant)
	if err != nil {
		return nil, err
	}
	row, err := scanInvocation(tx.QueryRowContext(ctx, `SELECT `+invocationColumns+` FROM client_invocations i
		WHERE i.invocation_id=$1 AND i.instance_id=$2 AND i.user_id=$3 AND i.consumer_id=$4 FOR UPDATE`,
		invocationID, grant.InstanceID, userID, grant.ConsumerID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationNotFound
	}
	if err != nil {
		return nil, err
	}
	if (row.Operation == "search") != (binding.SearchProof != nil) {
		return nil, service.ErrClientGatewayScope
	}
	if !invocationHasRoute(grant, row.RouteID) {
		return nil, service.ErrClientGatewayRevoked
	}
	if row.ConfigVersion != grant.ConfigVersion || row.EntitlementVersion != grant.EntitlementVersion {
		return nil, service.ErrClientGatewayChanged
	}
	if subtle.ConstantTimeCompare([]byte(row.ownerHash), []byte(service.GatewayTokenHash(ownerToken))) != 1 ||
		row.State != service.ClientInvocationReserved {
		return nil, service.ErrClientInvocationOwner
	}
	if err = lockInvocationBilling(ctx, tx, userID, grant.ConsumerID, row.BillingIdentityID, row.GroupID); err != nil {
		return nil, err
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !row.IdempotencyExpiresAt.After(now) {
		return nil, service.ErrClientInvocationExpired
	}
	if !row.LeaseExpiresAt.After(now) {
		return nil, service.ErrClientInvocationOwner
	}
	counterProof, err := marshalInvocationDispatchProof(binding)
	if err != nil {
		return nil, service.ErrClientGatewayUnavailable
	}
	attemptNo, preparedState, err := latestInvocationAttempt(ctx, tx, invocationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationOwner
	}
	if err != nil {
		return nil, err
	}
	if preparedState != "prepared" {
		return nil, service.ErrClientInvocationOwner
	}
	var preparedBindingMatches bool
	err = tx.QueryRowContext(ctx, `SELECT provider_account_id=$2 AND provider_platform=$3 AND provider_model=$4 AND provider_protocol=$5 AND
		provider_payload_hash=$6 AND provider_counter_proof=$7::jsonb
		FROM client_invocation_attempts WHERE invocation_id=$1 AND attempt_no=$8 FOR UPDATE`, invocationID,
		binding.AccountID, binding.Platform, binding.ModelID, binding.Protocol, binding.PayloadHash, counterProof,
		attemptNo).Scan(&preparedBindingMatches)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationOwner
	}
	if err != nil {
		return nil, err
	}
	if !preparedBindingMatches {
		return nil, service.ErrClientInvocationConflict
	}
	row, err = scanInvocation(tx.QueryRowContext(ctx, `UPDATE client_invocations AS i SET state=$1,
		updated_at=$2,dispatch_deadline=$3,provider_account_id=$4,provider_platform=$5,provider_model=$6,
		provider_protocol=$7,provider_payload_hash=$8,provider_counter_proof=$9,provider_attempt_no=$10
		WHERE i.invocation_id=$11 RETURNING `+invocationColumns,
		service.ClientInvocationDispatched, now, now.Add(service.ClientInvocationDeadline), binding.AccountID,
		binding.Platform, binding.ModelID, binding.Protocol, binding.PayloadHash, counterProof, attemptNo, invocationID))
	if err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE client_invocation_attempts SET state=$1,dispatch_started_at=$2,updated_at=$2
		WHERE invocation_id=$3 AND attempt_no=$4 AND state='prepared' AND provider_account_id=$5 AND provider_platform=$6
		AND provider_model=$7 AND provider_protocol=$8 AND provider_payload_hash=$9 AND provider_counter_proof=$10::jsonb`,
		service.ClientInvocationDispatched, now, invocationID, attemptNo, binding.AccountID, binding.Platform,
		binding.ModelID, binding.Protocol, binding.PayloadHash, counterProof)
	if err != nil {
		return nil, err
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, service.ErrClientInvocationConflict
	}
	if !time.Now().Before(grant.ExpiresAt) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return row.ClientInvocationRecord, nil
}

// MarkPreparedNotDispatched terminalizes a prepared attempt only while the
// invocation row is still reserved. The row lock serializes this proof against
// StartDispatch, so exactly one transition can win.
func (r *clientInvocationLedgerRepository) MarkPreparedNotDispatched(ctx context.Context, invocationID, ownerToken string) (*service.ClientInvocationRecord, error) {
	if !invocationIdentifier.MatchString(invocationID) || !invocationOwnerToken.MatchString(ownerToken) {
		return nil, service.ErrClientGatewayScope
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	row, err := scanInvocation(tx.QueryRowContext(ctx, `SELECT `+invocationColumns+` FROM client_invocations i
		WHERE i.invocation_id=$1 FOR UPDATE`, invocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationNotFound
	}
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare([]byte(row.ownerHash), []byte(service.GatewayTokenHash(ownerToken))) != 1 ||
		row.State != service.ClientInvocationReserved || row.ProviderPayloadHash != "" {
		return nil, service.ErrClientInvocationOwner
	}
	attemptNo, attemptState, err := latestInvocationAttempt(ctx, tx, invocationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationOwner
	}
	if err != nil {
		return nil, err
	}
	if attemptState == service.ClientInvocationAttemptNotDispatched {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return row.ClientInvocationRecord, nil
	}
	if attemptState != "prepared" {
		return nil, service.ErrClientInvocationOwner
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE client_invocation_attempts SET state=$1,not_dispatched_at=$2,updated_at=$2
		WHERE invocation_id=$3 AND attempt_no=$4 AND state='prepared' AND not_dispatched_at IS NULL`,
		service.ClientInvocationAttemptNotDispatched, now, invocationID, attemptNo)
	if err != nil {
		return nil, err
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return nil, service.ErrClientInvocationConflict
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return row.ClientInvocationRecord, nil
}

// Finish is internal result persistence, not a public authentication path. Its
// hashed capability remains valid after logout so already paid work can settle.
func (r *clientInvocationLedgerRepository) Finish(ctx context.Context, invocationID, ownerToken string, outcome service.ClientInvocationOutcome) (*service.ClientInvocationRecord, error) {
	if !invocationIdentifier.MatchString(invocationID) || !invocationOwnerToken.MatchString(ownerToken) || !service.ValidateClientInvocationOutcome(outcome) {
		return nil, service.ErrClientGatewayScope
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	row, err := scanInvocation(tx.QueryRowContext(ctx, `SELECT `+invocationColumns+` FROM client_invocations i
		WHERE i.invocation_id=$1 FOR UPDATE`, invocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationNotFound
	}
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare([]byte(row.ownerHash), []byte(service.GatewayTokenHash(ownerToken))) != 1 {
		return nil, service.ErrClientInvocationOwner
	}
	if outcome.State == service.ClientInvocationSucceeded && (row.Operation == "search") != (outcome.Search != nil) {
		return nil, service.ErrClientGatewayScope
	}
	raw, err := json.Marshal(outcome)
	if err != nil {
		return nil, service.ErrClientGatewayScope
	}
	if row.State == service.ClientInvocationSucceeded || row.State == service.ClientInvocationFailed {
		previous, err := json.Marshal(row.Outcome)
		if err != nil || string(previous) != string(raw) {
			return nil, service.ErrClientInvocationConflict
		}
		return row.ClientInvocationRecord, tx.Commit()
	}
	if row.State != service.ClientInvocationDispatched && row.State != service.ClientInvocationUnknown {
		return nil, service.ErrClientInvocationOwner
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err = advanceInvocationAttempt(ctx, tx, row, outcome.State, now); err != nil {
		return nil, err
	}
	row, err = scanInvocation(tx.QueryRowContext(ctx, `UPDATE client_invocations AS i SET state=$1,
		updated_at=$2,outcome=$3 WHERE i.invocation_id=$4 RETURNING `+invocationColumns,
		outcome.State, now, string(raw), invocationID))
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return row.ClientInvocationRecord, nil
}

// MarkUnknown records that dispatch crossed its no-resend boundary but the
// caller cannot prove a completed, durably settled result. A later recovery
// path may attach the original result through Finish; this method never retries.
func (r *clientInvocationLedgerRepository) MarkUnknown(ctx context.Context, invocationID, ownerToken string) (*service.ClientInvocationRecord, error) {
	if !invocationIdentifier.MatchString(invocationID) || !invocationOwnerToken.MatchString(ownerToken) {
		return nil, service.ErrClientGatewayScope
	}
	tx, err := r.transaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	row, err := scanInvocation(tx.QueryRowContext(ctx, `SELECT `+invocationColumns+` FROM client_invocations i
		WHERE i.invocation_id=$1 FOR UPDATE`, invocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientInvocationNotFound
	}
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare([]byte(row.ownerHash), []byte(service.GatewayTokenHash(ownerToken))) != 1 {
		return nil, service.ErrClientInvocationOwner
	}
	if row.State == service.ClientInvocationUnknown {
		return row.ClientInvocationRecord, tx.Commit()
	}
	if row.State != service.ClientInvocationDispatched {
		return nil, service.ErrClientInvocationOwner
	}
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err = advanceInvocationAttempt(ctx, tx, row, service.ClientInvocationUnknown, now); err != nil {
		return nil, err
	}
	row, err = scanInvocation(tx.QueryRowContext(ctx, `UPDATE client_invocations AS i SET state=$1,
		updated_at=$2,outcome=NULL WHERE i.invocation_id=$3 RETURNING `+invocationColumns,
		service.ClientInvocationUnknown, now, invocationID))
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return row.ClientInvocationRecord, nil
}

func marshalInvocationDispatchProof(binding service.ClientInvocationDispatchBinding) ([]byte, error) {
	if binding.SearchProof != nil {
		return json.Marshal(binding.SearchProof)
	}
	return json.Marshal(binding.CounterProof)
}
