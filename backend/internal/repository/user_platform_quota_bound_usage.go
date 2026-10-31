package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/shopspring/decimal"
)

var _ service.UserPlatformQuotaBoundUsageRepository = (*userPlatformQuotaRepository)(nil)

// IncrementBoundUsage is a single, non-replayed delivery attempt for an
// internally validated Apply receipt. It does not claim its source was delivered
// before, nor reconstruct a missing receipt from current configuration. The
// permanent ordinary journal remains the independent durable metering source.
func (r *userPlatformQuotaRepository) IncrementBoundUsage(ctx context.Context, binding service.UserPlatformQuotaUsageBinding) (service.UserPlatformQuotaBoundIncrementResult, error) {
	if r.writerAuthorityConfigured {
		return service.UserPlatformQuotaBoundIncrementResult{}, service.ErrUserPlatformQuotaWriterModeUnsupported
	}
	return r.incrementBoundUsage(ctx, binding)
}

// Only the permanent source consumer invokes this helper in the new domain.
// Public bound/unbound ports cannot borrow a scope from an ambient transaction.
func (r *userPlatformQuotaRepository) incrementBoundUsage(ctx context.Context, binding service.UserPlatformQuotaUsageBinding) (service.UserPlatformQuotaBoundIncrementResult, error) {
	var result service.UserPlatformQuotaBoundIncrementResult
	if err := service.ValidateUserPlatformQuotaUsageBinding(binding); err != nil {
		return result, err
	}
	cost, err := decimal.NewFromString(strconv.FormatFloat(binding.CostUSD, 'f', -1, 64))
	if err != nil {
		return result, fmt.Errorf("bound quota cost: %w", err)
	}
	err = r.withTx(ctx, func(txCtx context.Context, client *dbent.Client) error {
		// Locate by the original physical row, then inspect every control field
		// after the lock wait. A reset or delete/recreate never redirects this
		// delivery to the current (user,platform) row.
		rows, err := client.QueryContext(txCtx, `SELECT control_generation,user_id,platform,deleted_at,
			daily_usage_usd,weekly_usage_usd,monthly_usage_usd,daily_window_start,weekly_window_start,monthly_window_start
			FROM user_platform_quotas WHERE id=$1 FOR UPDATE`, binding.QuotaRowID)
		if err != nil {
			return err
		}
		if !rows.Next() {
			readErr := rows.Err()
			_ = rows.Close()
			if readErr != nil {
				return readErr
			}
			result.Status = service.QuotaIncrementSuperseded
			return nil
		}
		var generation, userID int64
		var platform string
		var deleted sql.NullTime
		var usage [3]string
		var starts [3]sql.NullTime
		err = rows.Scan(&generation, &userID, &platform, &deleted, &usage[0], &usage[1], &usage[2], &starts[0], &starts[1], &starts[2])
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
		if generation != binding.ControlGeneration || userID != binding.UserID || platform != binding.Platform || deleted.Valid {
			result.Status = service.QuotaIncrementSuperseded
			return nil
		}
		day, week := timezone.StartOfDay(binding.CapturedAt), timezone.StartOfWeek(binding.CapturedAt)
		var mask uint8
		for i := range usage {
			base, parseErr := decimal.NewFromString(usage[i])
			if parseErr != nil || base.IsNegative() {
				return fmt.Errorf("invalid bound quota usage window %d", i)
			}
			// This check precedes rotation. An older event must neither add to a
			// later window nor turn that window back and erase newer consumption.
			if starts[i].Valid && binding.CapturedAt.Before(starts[i].Time) {
				continue
			}
			rotated := !starts[i].Valid || i == 0 && starts[i].Time.Before(day) || i == 1 && starts[i].Time.Before(week) ||
				i == 2 && binding.CapturedAt.Sub(starts[i].Time) >= 720*time.Hour
			if rotated {
				base = decimal.Zero
				start := binding.CapturedAt
				if i == 0 {
					start = day
				}
				if i == 1 {
					start = week
				}
				// PostgreSQL retains the captured clock's microseconds for a new
				// month anchor; Redis's legacy start encoding has second precision.
				starts[i] = sql.NullTime{Time: start, Valid: true}
			}
			usage[i] = base.Add(cost).String()
			mask |= uint8(1 << i)
		}
		if mask == 0 {
			result.Status = service.QuotaIncrementObsolete
			return nil
		}
		updated, err := client.ExecContext(txCtx, `UPDATE user_platform_quotas SET
			daily_usage_usd=$5::numeric,weekly_usage_usd=$6::numeric,monthly_usage_usd=$7::numeric,
			daily_window_start=$8,weekly_window_start=$9,monthly_window_start=$10,updated_at=clock_timestamp()
			WHERE id=$1 AND control_generation=$2 AND user_id=$3 AND platform=$4 AND deleted_at IS NULL`,
			binding.QuotaRowID, binding.ControlGeneration, binding.UserID, binding.Platform, usage[0], usage[1], usage[2], starts[0], starts[1], starts[2])
		if err != nil {
			return err
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("bound quota locked row changed before update")
		}
		result.AppliedWindows = mask
		result.Status = service.QuotaIncrementPartial
		if mask == service.QuotaWindowAll {
			result.Status = service.QuotaIncrementApplied
		}
		return nil
	})
	if err != nil {
		return service.UserPlatformQuotaBoundIncrementResult{}, err
	}
	return result, nil
}
