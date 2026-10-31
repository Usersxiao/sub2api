package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"
)

type ClientInvocationPreparer interface {
	Prepare(context.Context, string, string, []byte) (*PreparedClientInvocation, error)
}

// This metadata is an internal recovery receipt, not a public ModelTask or a
// settlement acknowledgment. A future HTTP adapter must bind its local principal
// and content authorization before mapping it to the shared task contract.
type ClientInvocationReceipt struct {
	InvocationID         string    `json:"invocationId"`
	ProjectID            string    `json:"projectId"`
	RequestID            string    `json:"requestId"`
	RouteID              string    `json:"routeId"`
	Operation            string    `json:"operation"`
	ConfigVersion        string    `json:"configVersion"`
	EntitlementVersion   string    `json:"entitlementVersion"`
	State                string    `json:"state"`
	CreatedAt            time.Time `json:"createdAt"`
	UpdatedAt            time.Time `json:"updatedAt"`
	IdempotencyExpiresAt time.Time `json:"idempotencyExpiresAt"`
}

// Attempts remain in server memory. No serialization may disclose the dispatch
// capability, prepared identity, request text, or upstream bearer credential.
type ClientInvocationAttempt struct {
	Receipt    *ClientInvocationReceipt  `json:"-"`
	Prepared   *PreparedClientInvocation `json:"-"`
	record     *ClientInvocationRecord
	ownerToken string
	routeID    string
	raw        []byte
}

func (a *ClientInvocationAttempt) Owned() bool { return a != nil && a.ownerToken != "" }

type ClientInvocationLedgerService struct {
	preparer ClientInvocationPreparer
	grants   ClientInvocationGrantValidator
	ledger   ClientInvocationLedgerRepository
	counter  ClientInvocationTokenCounter
	now      func() time.Time
}

func NewClientInvocationLedgerService(preparer ClientInvocationPreparer, grants ClientInvocationGrantValidator, ledger ClientInvocationLedgerRepository, counter ClientInvocationTokenCounter) *ClientInvocationLedgerService {
	return &ClientInvocationLedgerService{preparer: preparer, grants: grants, ledger: ledger, counter: counter, now: time.Now}
}

// Reserve records one logical paid submission, but sends nothing. Only the
// server-owned attempt winning the durable reservation may enter StartDispatch.
func (s *ClientInvocationLedgerService) Reserve(ctx context.Context, token, routeID string, raw []byte) (*ClientInvocationAttempt, error) {
	if s == nil || s.preparer == nil || s.grants == nil || s.ledger == nil {
		return nil, ErrClientGatewayUnavailable
	}
	prepared, err := s.preparer.Prepare(ctx, token, routeID, raw)
	if err != nil {
		return nil, err
	}
	fingerprint, err := ClientInvocationFingerprint(prepared)
	if err != nil {
		return nil, err
	}
	id, err := gatewayRandom("invocation-")
	if err != nil {
		return nil, ErrClientGatewayUnavailable
	}
	owner, err := gatewayRandom("attempt-")
	if err != nil {
		return nil, ErrClientGatewayUnavailable
	}
	req := prepared.Request
	claim, err := s.ledger.Reserve(ctx, prepared.Grant, &ClientInvocationReservation{
		InvocationID: id, ProjectID: req.ProjectID, RequestID: req.RequestID, RouteID: req.RouteID,
		Operation: req.Operation, ConfigVersion: req.ConfigVersion, EntitlementVersion: req.EntitlementVersion,
		Fingerprint: fingerprint, BillingIdentityID: prepared.BillingIdentity.ID, GroupID: prepared.Resolution.GroupID, OwnerToken: owner,
	})
	if err != nil {
		return nil, err
	}
	if claim == nil || !s.recordMatchesGrant(claim.Record, prepared.Grant, req.ProjectID, req.RequestID) ||
		claim.Record.Fingerprint != fingerprint || claim.Record.ConfigVersion != req.ConfigVersion ||
		claim.Record.EntitlementVersion != req.EntitlementVersion || claim.Record.RouteID != req.RouteID ||
		claim.Record.BillingIdentityID != prepared.BillingIdentity.ID || claim.Record.GroupID != prepared.Resolution.GroupID ||
		(claim.Owned && claim.Record.State != ClientInvocationReserved) {
		return nil, ErrClientInvocationConflict
	}
	// Logout and grant rotation during the reservation transaction win before the
	// capability is returned. The abandoned reservation can expire without sending.
	if err = s.recheckGrant(ctx, token, prepared.Grant, req.RouteID); err != nil {
		return nil, err
	}
	immutableRecord := *claim.Record
	attempt := &ClientInvocationAttempt{Receipt: invocationReceipt(claim.Record), record: &immutableRecord}
	if claim.Owned {
		attempt.Prepared, attempt.ownerToken, attempt.routeID, attempt.raw = prepared, owner, routeID, bytes.Clone(raw)
	}
	return attempt, nil
}

// startDispatchWithAdmission reruns preparation and admission, then installs a
// final-payload gate. The gate counts and durably binds the actual provider
// request immediately before the shared transport sends it.
func (s *ClientInvocationLedgerService) startDispatchWithAdmission(ctx context.Context, token string, attempt *ClientInvocationAttempt, admit func(context.Context, *PreparedClientInvocation) (context.Context, error)) (*PreparedClientInvocation, context.Context, error) {
	if s == nil || s.preparer == nil || s.ledger == nil || s.grants == nil {
		return nil, nil, ErrClientGatewayUnavailable
	}
	if !attempt.Owned() || attempt.record == nil || attempt.record.State != ClientInvocationReserved {
		return nil, nil, ErrClientInvocationOwner
	}
	if admit == nil {
		return nil, nil, ErrClientInvocationAdmissionUnavailable
	}
	prepared, err := s.preparer.Prepare(ctx, token, attempt.routeID, attempt.raw)
	if err != nil {
		return nil, nil, err
	}
	fingerprint, err := ClientInvocationFingerprint(prepared)
	if err != nil || fingerprint != attempt.record.Fingerprint {
		return nil, nil, ErrClientInvocationConflict
	}
	dispatchContext, err := admit(ctx, prepared)
	if err != nil {
		return nil, nil, err
	}
	if dispatchContext == nil {
		return nil, nil, ErrClientInvocationAdmissionUnavailable
	}
	gate := &clientInvocationProviderPayloadGate{
		ledger: s, counter: s.counter, ctx: dispatchContext, token: token, attempt: attempt, prepared: prepared,
	}
	dispatchContext = WithClientInvocationProviderPayloadAuthorizer(dispatchContext, gate)
	return prepared, dispatchContext, nil
}

// Finish is for the private execution/settlement adapter. A revoked login must
// not prevent the owner from recording a late result; recovery still requires a
// fresh grant through Get. No output text is copied into the metadata receipt.
func (s *ClientInvocationLedgerService) Finish(ctx context.Context, attempt *ClientInvocationAttempt, outcome ClientInvocationOutcome) (*ClientInvocationReceipt, error) {
	if s == nil || s.ledger == nil {
		return nil, ErrClientGatewayUnavailable
	}
	if !attempt.Owned() || attempt.record == nil || !ValidateClientInvocationOutcome(outcome) {
		return nil, ErrClientInvocationOwner
	}
	record, err := s.ledger.Finish(ctx, attempt.record.InvocationID, attempt.ownerToken, outcome)
	if err != nil {
		return nil, err
	}
	if !sameInvocationRecordIdentity(attempt.record, record) || record.State != outcome.State {
		return nil, ErrClientInvocationConflict
	}
	return invocationReceipt(record), nil
}

// MarkUnknown closes an owned attempt after dispatch when its completion or
// settlement cannot be confirmed. It does not make the attempt retryable.
func (s *ClientInvocationLedgerService) MarkUnknown(ctx context.Context, attempt *ClientInvocationAttempt) (*ClientInvocationReceipt, error) {
	if s == nil || s.ledger == nil {
		return nil, ErrClientGatewayUnavailable
	}
	if !attempt.Owned() || attempt.record == nil {
		return nil, ErrClientInvocationOwner
	}
	record, err := s.ledger.MarkUnknown(ctx, attempt.record.InvocationID, attempt.ownerToken)
	if err != nil {
		return nil, err
	}
	if !sameInvocationRecordIdentity(attempt.record, record) || record.State != ClientInvocationUnknown {
		return nil, ErrClientInvocationConflict
	}
	attempt.record = record
	attempt.Receipt = invocationReceipt(record)
	return attempt.Receipt, nil
}

// MarkPreparedNotDispatched closes a prepared attempt only while the durable
// invocation row remains reserved. A concurrent successful dispatch claim wins.
func (s *ClientInvocationLedgerService) MarkPreparedNotDispatched(ctx context.Context, attempt *ClientInvocationAttempt) error {
	if s == nil || s.ledger == nil {
		return ErrClientGatewayUnavailable
	}
	if !attempt.Owned() || attempt.record == nil {
		return ErrClientInvocationOwner
	}
	record, err := s.ledger.MarkPreparedNotDispatched(ctx, attempt.record.InvocationID, attempt.ownerToken)
	if err != nil {
		return err
	}
	if !sameInvocationRecordIdentity(attempt.record, record) || record.State != ClientInvocationReserved || record.ProviderPayloadHash != "" {
		return ErrClientInvocationConflict
	}
	attempt.record = record
	attempt.Receipt = invocationReceipt(record)
	return nil
}

func (s *ClientInvocationLedgerService) Get(ctx context.Context, token, projectID, requestID string) (*ClientInvocationReceipt, error) {
	if s == nil || s.ledger == nil || s.grants == nil {
		return nil, ErrClientGatewayUnavailable
	}
	if !clientIdentifier.MatchString(projectID) || !clientIdentifier.MatchString(requestID) {
		return nil, ErrClientInvocationNotFound
	}
	grant, err := s.grants.Validate(ctx, token, "", "invoke")
	if err != nil {
		return nil, err
	}
	record, err := s.ledger.Get(ctx, grant, projectID, requestID)
	if err != nil {
		return nil, err
	}
	if !s.recordMatchesGrant(record, grant, projectID, requestID) {
		return nil, ErrClientInvocationNotFound
	}
	if err = s.recheckGrant(ctx, token, grant, record.RouteID); err != nil {
		return nil, err
	}
	return invocationReceipt(record), nil
}

func (s *ClientInvocationLedgerService) recheckGrant(ctx context.Context, token string, grant *ClientGatewayGrant, routeID string) error {
	current, err := s.grants.Validate(ctx, token, routeID, "invoke")
	if err != nil {
		return err
	}
	if !sameClientInvocationGrant(grant, current) {
		return ErrClientGatewayRevoked
	}
	return nil
}

func (s *ClientInvocationLedgerService) recordMatchesGrant(record *ClientInvocationRecord, grant *ClientGatewayGrant, projectID, requestID string) bool {
	if record == nil || grant == nil {
		return false
	}
	userID, err := strconv.ParseInt(grant.UpstreamUserID, 10, 64)
	return err == nil && userID > 0 && record.UserID == userID && record.InstanceID == grant.InstanceID &&
		record.ConsumerID == grant.ConsumerID && record.ProjectID == projectID && record.RequestID == requestID &&
		clientContains(grant.RouteIDs, record.RouteID) && clientContains([]string{"text", "search"}, record.Operation) &&
		clientIdentifier.MatchString(record.InvocationID) && !record.CreatedAt.IsZero() && !record.UpdatedAt.Before(record.CreatedAt) &&
		record.IdempotencyExpiresAt.After(s.now()) && clientContains([]string{
		ClientInvocationReserved, ClientInvocationDispatched, ClientInvocationUnknown, ClientInvocationSucceeded, ClientInvocationFailed,
	}, record.State)
}

func sameInvocationRecordIdentity(first, second *ClientInvocationRecord) bool {
	return first != nil && second != nil && first.InvocationID == second.InvocationID && first.InstanceID == second.InstanceID &&
		first.UserID == second.UserID && first.ConsumerID == second.ConsumerID && first.ProjectID == second.ProjectID &&
		first.RequestID == second.RequestID && first.RouteID == second.RouteID && first.Operation == second.Operation &&
		first.ConfigVersion == second.ConfigVersion && first.EntitlementVersion == second.EntitlementVersion &&
		first.Fingerprint == second.Fingerprint && first.BillingIdentityID == second.BillingIdentityID && first.GroupID == second.GroupID &&
		first.CreatedAt.Equal(second.CreatedAt) && first.IdempotencyExpiresAt.Equal(second.IdempotencyExpiresAt)
}

func invocationReceipt(record *ClientInvocationRecord) *ClientInvocationReceipt {
	return &ClientInvocationReceipt{record.InvocationID, record.ProjectID, record.RequestID, record.RouteID, record.Operation,
		record.ConfigVersion, record.EntitlementVersion, record.State, record.CreatedAt, record.UpdatedAt, record.IdempotencyExpiresAt}
}

// Canonicalize validated effective parameters/payload, not raw JSON spelling.
// Numeric spellings 1/1.0/1e0 and parameter field order represent the same input.
// Original text parts and immutable routing/version/billing decisions remain
// bound, so changing a request or substituting its accounting identity conflicts.
func ClientInvocationFingerprint(prepared *PreparedClientInvocation) (string, error) {
	if prepared != nil && prepared.Search != nil {
		return clientSearchFingerprint(prepared)
	}
	if prepared == nil || prepared.Request == nil || prepared.Grant == nil || prepared.Resolution == nil || prepared.Text == nil ||
		prepared.BillingIdentity == nil || prepared.BillingIdentity.ID <= 0 || prepared.Resolution.GroupID <= 0 ||
		!clientInvocationRouteMatchesBillingIdentity(prepared) || !validClientInvocationRequest(prepared.Request) ||
		len(prepared.Text.Payload) > ClientInvocationMaxBytes || !json.Valid(prepared.Text.Payload) {
		return "", ErrClientInvocationParameters
	}
	effectiveModel, err := clientInvocationEffectiveModel(prepared.Resolution)
	if err != nil || prepared.Text.ModelID != effectiveModel {
		return "", ErrClientInvocationParameters
	}
	var payload any
	if err := json.Unmarshal(prepared.Text.Payload, &payload); err != nil {
		return "", ErrClientInvocationParameters
	}
	payloadObject, ok := payload.(map[string]any)
	if !ok || payloadObject["model"] != effectiveModel {
		return "", ErrClientInvocationParameters
	}
	req := prepared.Request
	var composite *clientCompositeRouteBinding
	if decision := prepared.Resolution.CompositeDecision; decision != nil {
		composite = clientCompositeBinding(*decision)
	}
	channelMapping := prepared.Resolution.ChannelMapping
	accountMappings := prepared.Resolution.AccountMappings
	raw, err := json.Marshal(struct {
		Version              int
		ProjectID, RequestID string
		RouteID, Operation   string
		Config, Entitlement  string
		GroupID, BillingID   int64
		ModelID, Protocol    string
		Platform             string
		ChannelMapping       *clientChannelModelBinding
		AccountMappings      []clientAccountModelBinding
		Composite            *clientCompositeRouteBinding
		Input                []ClientTextInputPart
		Payload              any
	}{1, req.ProjectID, req.RequestID, req.RouteID, req.Operation, req.ConfigVersion, req.EntitlementVersion,
		prepared.Resolution.GroupID, prepared.BillingIdentity.ID, effectiveModel, prepared.Text.Protocol, prepared.Resolution.Platform,
		channelMapping, accountMappings, composite, req.Input, payload})
	if err != nil {
		return "", ErrClientInvocationParameters
	}
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:]), nil
}
