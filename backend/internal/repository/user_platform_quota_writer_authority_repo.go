package repository

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const quotaWriterActionOrdinaryDelivery = "ordinary_delivery"
const quotaWriterActionAdminReset = "admin_reset"

type userPlatformQuotaWriterAuthorityRepository struct{ db *sql.DB }

func NewUserPlatformQuotaWriterAuthorityRepository(sqlDB *sql.DB) service.UserPlatformQuotaWriterAuthorityRepository {
	return &userPlatformQuotaWriterAuthorityRepository{db: sqlDB}
}

func (r *userPlatformQuotaWriterAuthorityRepository) InitializeUserPlatformQuotaWriterAuthority(ctx context.Context, mode string) (service.UserPlatformQuotaWriterAuthority, error) {
	var zero service.UserPlatformQuotaWriterAuthority
	if mode != service.UserPlatformQuotaWriterModeDB {
		return zero, service.ErrUserPlatformQuotaWriterModeUnsupported
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil || r.db == nil {
		return zero, service.ErrUserPlatformQuotaWriterUnavailable
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO user_platform_quota_writer_authority(id,mode,epoch) VALUES(1,$1,1) ON CONFLICT(id) DO NOTHING`, mode); err != nil {
		return zero, err
	}
	var authority service.UserPlatformQuotaWriterAuthority
	if err = tx.QueryRowContext(ctx, `SELECT mode,epoch FROM user_platform_quota_writer_authority WHERE id=1 FOR SHARE`).Scan(&authority.Mode, &authority.Epoch); err != nil {
		return zero, err
	}
	if err = service.ValidateUserPlatformQuotaWriterAuthority(authority); err != nil {
		return zero, err
	}
	if authority.Mode != mode {
		return zero, service.ErrUserPlatformQuotaWriterMismatch
	}
	if err = tx.Commit(); err != nil {
		return zero, err
	}
	return authority, nil
}

type quotaWriterSQL interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// The handle stays fixed for this repository's lifetime. No operation upgrades
// an old process or historical birth to the authority it happens to read today.
func lockUserPlatformQuotaWriterAuthority(ctx context.Context, client quotaWriterSQL, expected service.UserPlatformQuotaWriterAuthority, action string) error {
	if err := service.ValidateUserPlatformQuotaWriterAuthority(expected); err != nil {
		if expected.Mode == "" && expected.Epoch == 0 {
			return service.ErrUserPlatformQuotaWriterUnavailable
		}
		return err
	}
	if action != "" && action != quotaWriterActionOrdinaryDelivery && action != quotaWriterActionAdminReset {
		return service.ErrUserPlatformQuotaWriterMismatch
	}
	rows, err := client.QueryContext(ctx, `SELECT mode,epoch FROM user_platform_quota_writer_authority WHERE id=1 FOR SHARE`)
	if err != nil {
		return err
	}
	if !rows.Next() {
		readErr := rows.Err()
		_ = rows.Close()
		if readErr != nil {
			return readErr
		}
		return service.ErrUserPlatformQuotaWriterUnavailable
	}
	var actual service.UserPlatformQuotaWriterAuthority
	err = rows.Scan(&actual.Mode, &actual.Epoch)
	readErr := rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if actual != expected {
		return service.ErrUserPlatformQuotaWriterMismatch
	}
	if action == "" {
		return nil
	}
	_, err = client.ExecContext(ctx, `SELECT set_config('sub2api.quota_writer_mode',$1,true),set_config('sub2api.quota_writer_epoch',$2,true),set_config('sub2api.quota_writer_action',$3,true)`, expected.Mode, strconv.FormatInt(expected.Epoch, 10), action)
	return err
}
