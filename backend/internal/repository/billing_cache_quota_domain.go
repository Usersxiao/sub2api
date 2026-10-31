package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

// ProvideBillingCache binds production billing to the database's durable cache
// domain. Observing that domain never certifies a quota baseline or touches Redis.
func ProvideBillingCache(rdb *redis.Client, db *sql.DB, authority service.UserPlatformQuotaWriterAuthority) (service.BillingCache, error) {
	if rdb == nil {
		return nil, errors.New("billing cache requires a Redis client")
	}
	if db == nil {
		return nil, errors.New("billing cache requires a SQL database")
	}
	if err := service.ValidateUserPlatformQuotaWriterAuthority(authority); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var storedAuthority service.UserPlatformQuotaWriterAuthority
	var namespace string
	err := db.QueryRowContext(ctx, `SELECT mode,epoch,cache_namespace::text FROM user_platform_quota_writer_authority WHERE id=1`).Scan(
		&storedAuthority.Mode, &storedAuthority.Epoch, &namespace,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrUserPlatformQuotaWriterUnavailable
	}
	if err != nil {
		return nil, fmt.Errorf("read billing quota cache domain: %w", err)
	}
	if storedAuthority != authority {
		return nil, service.ErrUserPlatformQuotaWriterMismatch
	}
	domain := service.UserPlatformQuotaCacheDomain{
		SchemaVersion:   service.UserPlatformQuotaCacheDomainSchemaV2,
		Namespace:       namespace,
		WriterAuthority: authority,
	}
	if err := service.ValidateUserPlatformQuotaCacheDomain(domain); err != nil {
		return nil, err
	}
	return &billingCache{rdb: rdb, quotaDomain: &domain}, nil
}
