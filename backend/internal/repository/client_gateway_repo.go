package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type clientGatewayRepository struct{ db *sql.DB }

func NewClientGatewayRepository(db *sql.DB) service.ClientGatewayRepository {
	return &clientGatewayRepository{db}
}

const gatewayGrantColumns = `g.grant_id,g.consumer_id,g.instance_id,g.user_id,g.client_id,g.sid,g.session_epoch,g.config_version,g.entitlement_version,g.route_ids,g.operations,g.issued_at,g.expires_at,g.revoked_at`

func scanGatewayGrant(row sessionScanner) (*service.ClientGatewayGrant, error) {
	var g service.ClientGatewayGrant
	var userID int64
	var routes, operations []byte
	var revoked sql.NullTime
	err := row.Scan(&g.GrantID, &g.ConsumerID, &g.InstanceID, &userID, &g.ClientID, &g.SID, &g.SessionEpoch, &g.ConfigVersion, &g.EntitlementVersion, &routes, &operations, &g.IssuedAt, &g.ExpiresAt, &revoked)
	if err != nil {
		return nil, err
	}
	g.SchemaVersion = 1
	g.UpstreamUserID = strconv.FormatInt(userID, 10)
	if err = json.Unmarshal(routes, &g.RouteIDs); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(operations, &g.Operations); err != nil {
		return nil, err
	}
	if revoked.Valid {
		g.RevokedAt = &revoked.Time
	}
	return &g, nil
}

func lockGatewaySession(ctx context.Context, tx *sql.Tx, userID int64, sid string, epoch int64) (*service.AuthSession, error) {
	a, err := lockSessionAccount(ctx, tx, userID)
	if err != nil {
		return nil, err
	}
	s, err := scanAuthSession(tx.QueryRowContext(ctx, `SELECT `+sessionColumns+` FROM auth_sessions s WHERE s.sid=$1 AND s.user_id=$2 FOR UPDATE`, sid, userID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrTokenRevoked
	}
	if err != nil {
		return nil, err
	}
	if epoch <= 0 || a.epoch != epoch || s.Epoch != epoch || s.RevokedAt != nil {
		return nil, service.ErrTokenRevoked
	}
	if !time.Now().Before(s.ExpiresAt) {
		return nil, service.ErrTokenExpired
	}
	return s, nil
}

func (r *clientGatewayRepository) Issue(ctx context.Context, in *service.ClientGatewayGrant, hash, previousGrantID, expectedConsumerID string) (*service.ClientGatewayGrant, error) {
	if in == nil || in.SchemaVersion != 1 || in.ClientID != service.ClientGatewayClientID || in.IssuedAt.IsZero() || !in.ExpiresAt.After(in.IssuedAt) || in.ExpiresAt.Sub(in.IssuedAt) > service.ClientGatewayMaxTTL || len(in.RouteIDs) == 0 || len(in.Operations) != 1 || in.Operations[0] != "invoke" {
		return nil, service.ErrClientGatewayScope
	}
	userID, err := strconv.ParseInt(in.UpstreamUserID, 10, 64)
	if err != nil || userID <= 0 {
		return nil, service.ErrClientGatewayScope
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	s, err := lockGatewaySession(ctx, tx, userID, in.SID, in.SessionEpoch)
	if err != nil {
		return nil, err
	}
	if in.ExpiresAt.After(s.ExpiresAt) || !time.Now().Before(in.ExpiresAt) {
		return nil, service.ErrTokenExpired
	}
	var consumerID string
	err = tx.QueryRowContext(ctx, `INSERT INTO client_gateway_consumers(consumer_id,instance_id,user_id,client_id) VALUES($1,$2,$3,$4)
		ON CONFLICT(instance_id,user_id,client_id) DO UPDATE SET client_id=EXCLUDED.client_id RETURNING consumer_id`, in.ConsumerID, in.InstanceID, userID, in.ClientID).Scan(&consumerID)
	if err != nil {
		return nil, err
	}
	if expectedConsumerID != "" && consumerID != expectedConsumerID {
		return nil, service.ErrClientGatewayScope
	}
	if previousGrantID != "" {
		previous, err := scanGatewayGrant(tx.QueryRowContext(ctx, `SELECT `+gatewayGrantColumns+` FROM client_gateway_grants g WHERE g.grant_id=$1 AND g.user_id=$2 AND g.sid=$3 AND g.instance_id=$4 AND g.client_id=$5 FOR UPDATE`, previousGrantID, userID, in.SID, in.InstanceID, in.ClientID))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrClientGatewayRevoked
		}
		if err != nil {
			return nil, err
		}
		if previous.RevokedAt != nil || previous.SessionEpoch != in.SessionEpoch || previous.ConsumerID != consumerID {
			return nil, service.ErrClientGatewayRevoked
		}
	}
	// No-reference issuance is also a replacement, including parallel first calls.
	_, err = tx.ExecContext(ctx, `UPDATE client_gateway_grants SET revoked_at=clock_timestamp() WHERE instance_id=$1 AND user_id=$2 AND sid=$3 AND client_id=$4 AND revoked_at IS NULL`, in.InstanceID, userID, in.SID, in.ClientID)
	if err != nil {
		return nil, err
	}
	routes, err := json.Marshal(in.RouteIDs)
	if err != nil {
		return nil, err
	}
	operations, err := json.Marshal(in.Operations)
	if err != nil {
		return nil, err
	}
	out, err := scanGatewayGrant(tx.QueryRowContext(ctx, `INSERT INTO client_gateway_grants AS g
		(grant_id,token_hash,consumer_id,instance_id,user_id,client_id,sid,session_epoch,config_version,entitlement_version,route_ids,operations,issued_at,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13,$14) RETURNING `+gatewayGrantColumns,
		in.GrantID, hash, consumerID, in.InstanceID, userID, in.ClientID, in.SID, in.SessionEpoch, in.ConfigVersion, in.EntitlementVersion, string(routes), string(operations), in.IssuedAt, in.ExpiresAt))
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *clientGatewayRepository) GetByTokenHash(ctx context.Context, hash string) (*service.ClientGatewayGrant, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Read only enough to establish the account lock order. Re-read after locking.
	var userID, epoch int64
	var sid string
	err = tx.QueryRowContext(ctx, `SELECT user_id,sid,session_epoch FROM client_gateway_grants WHERE token_hash=$1`, hash).Scan(&userID, &sid, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err != nil {
		return nil, err
	}
	_, err = lockGatewaySession(ctx, tx, userID, sid, epoch)
	if err != nil {
		return nil, err
	}
	g, err := scanGatewayGrant(tx.QueryRowContext(ctx, `SELECT `+gatewayGrantColumns+` FROM client_gateway_grants g WHERE g.token_hash=$1 FOR UPDATE`, hash))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err != nil {
		return nil, err
	}
	if g.RevokedAt != nil || !time.Now().Before(g.ExpiresAt) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return g, nil
}

func (r *clientGatewayRepository) Revoke(ctx context.Context, s *service.AuthSession, instanceID, grantID string) (time.Time, error) {
	if s == nil {
		return time.Time{}, service.ErrClientGatewayScope
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback()
	_, err = lockGatewaySession(ctx, tx, s.UserID, s.SessionID, s.Epoch)
	if err != nil {
		return time.Time{}, err
	}
	var revoked time.Time
	err = tx.QueryRowContext(ctx, `UPDATE client_gateway_grants SET revoked_at=COALESCE(revoked_at,clock_timestamp())
		WHERE grant_id=$1 AND instance_id=$2 AND user_id=$3 AND sid=$4 AND session_epoch=$5 RETURNING revoked_at`, grantID, instanceID, s.UserID, s.SessionID, s.Epoch).Scan(&revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, service.ErrClientGatewayRevoked
	}
	if err != nil {
		return time.Time{}, err
	}
	if err = tx.Commit(); err != nil {
		return time.Time{}, err
	}
	return revoked, nil
}
