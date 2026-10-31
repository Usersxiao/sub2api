package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"slices"
	"strconv"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/apikey"
	"github.com/Wei-Shaw/sub2api/ent/group"
	"github.com/Wei-Shaw/sub2api/ent/user"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type clientBillingIdentityRepository struct {
	db     *sql.DB
	client *dbent.Client
}

func NewClientBillingIdentityRepository(db *sql.DB, client *dbent.Client) service.ClientBillingIdentityRepository {
	return &clientBillingIdentityRepository{db: db, client: client}
}

func (r *clientBillingIdentityRepository) Resolve(ctx context.Context, grant *service.ClientGatewayGrant, groupID int64) (*service.APIKey, error) {
	if grant == nil || grant.SchemaVersion != 1 || grant.ClientID != service.ClientGatewayClientID ||
		grant.ConsumerID == "" || grant.GrantID == "" || groupID <= 0 {
		return nil, service.ErrClientGatewayScope
	}
	userID, err := strconv.ParseInt(grant.UpstreamUserID, 10, 64)
	if err != nil || userID <= 0 {
		return nil, service.ErrClientGatewayScope
	}
	if r == nil || r.db == nil || r.client == nil {
		return nil, service.ErrClientGatewayUnavailable
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Use the same account/session/grant lock order as grant issuance and logout.
	if _, err = lockGatewaySession(ctx, tx, userID, grant.SID, grant.SessionEpoch); err != nil {
		return nil, err
	}
	stored, err := scanGatewayGrant(tx.QueryRowContext(ctx, `SELECT `+gatewayGrantColumns+`
		FROM client_gateway_grants g WHERE g.grant_id=$1 FOR UPDATE`, grant.GrantID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err != nil {
		return nil, err
	}
	if !sameBillingGrant(stored, grant) || stored.RevokedAt != nil || !time.Now().Before(stored.ExpiresAt) ||
		len(stored.Operations) != 1 || stored.Operations[0] != "invoke" {
		return nil, service.ErrClientGatewayRevoked
	}
	var consumerID string
	err = tx.QueryRowContext(ctx, `SELECT consumer_id FROM client_gateway_consumers
		WHERE consumer_id=$1 AND instance_id=$2 AND user_id=$3 AND client_id=$4 FOR SHARE`,
		stored.ConsumerID, stored.InstanceID, userID, stored.ClientID).Scan(&consumerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err != nil {
		return nil, err
	}
	var activeGroupID int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM groups WHERE id=$1 AND deleted_at IS NULL AND status=$2 FOR SHARE`,
		groupID, service.StatusActive).Scan(&activeGroupID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err != nil {
		return nil, err
	}
	// The marker satisfies the legacy non-null key column, but is never a usable
	// credential: both the normal repository and the auth cache reject it.
	entropy := make([]byte, 24)
	if _, err = rand.Read(entropy); err != nil {
		return nil, err
	}
	marker := service.InternalAPIKeyPrefix + base64.RawURLEncoding.EncodeToString(entropy)
	var id int64
	err = tx.QueryRowContext(ctx, `INSERT INTO api_keys
		(user_id,kind,client_consumer_id,key,name,group_id,status,created_at,updated_at)
		VALUES($1,$2,$3,$4,'AutoDesign',$5,$6,clock_timestamp(),clock_timestamp())
		ON CONFLICT(client_consumer_id,group_id) WHERE kind='client_consumer'
		DO UPDATE SET client_consumer_id=EXCLUDED.client_consumer_id
		RETURNING id`, userID, service.APIKeyKindClientConsumer, consumerID, marker, activeGroupID, service.StatusActive).Scan(&id)
	if err != nil {
		return nil, err
	}
	// Hydrate while the live session and grant are still locked. Do not expose a
	// post-commit read that can outlive a completed logout or credential rotation.
	txDriver := entsql.NewDriver(r.client.Driver().Dialect(), entsql.Conn{ExecQuerier: tx})
	txClient := dbent.NewClient(dbent.Driver(txDriver))
	row, err := txClient.APIKey.Query().Where(apikey.IDEQ(id), apikey.KindEQ(service.APIKeyKindClientConsumer),
		apikey.ClientConsumerIDEQ(consumerID), apikey.UserIDEQ(userID), apikey.GroupIDEQ(activeGroupID),
		apikey.DeletedAtIsNil(), apikey.StatusEQ(service.StatusActive)).
		WithUser(func(q *dbent.UserQuery) {
			q.Where(user.DeletedAtIsNil(), user.StatusEQ(service.StatusActive)).
				WithAllowedGroups(func(gq *dbent.GroupQuery) { gq.Select(group.FieldID) })
		}).WithGroup(func(q *dbent.GroupQuery) {
		q.Where(group.DeletedAtIsNil(), group.StatusEQ(service.StatusActive))
	}).Only(ctx)
	if dbent.IsNotFound(err) {
		return nil, service.ErrClientGatewayRevoked
	}
	if err != nil {
		return nil, err
	}
	if row.Edges.User == nil || row.Edges.Group == nil || !time.Now().Before(stored.ExpiresAt) {
		return nil, service.ErrClientGatewayRevoked
	}
	result := apiKeyEntityToService(row)
	result.Key = ""
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func sameBillingGrant(stored, candidate *service.ClientGatewayGrant) bool {
	return stored.ClientGatewayScope == candidate.ClientGatewayScope && stored.GrantID == candidate.GrantID &&
		stored.ClientID == candidate.ClientID && stored.ConsumerID == candidate.ConsumerID &&
		stored.ConfigVersion == candidate.ConfigVersion && stored.EntitlementVersion == candidate.EntitlementVersion &&
		slices.Equal(stored.RouteIDs, candidate.RouteIDs) && slices.Equal(stored.Operations, candidate.Operations) &&
		stored.IssuedAt.Equal(candidate.IssuedAt) && stored.ExpiresAt.Equal(candidate.ExpiresAt) && candidate.RevokedAt == nil
}
