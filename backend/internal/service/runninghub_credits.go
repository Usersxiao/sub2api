package service

import (
	"context"
	"database/sql"
	"errors"
)

// RHCreditsGranter is a thin interface allowing PaymentService to award
// rh_credits on order fulfillment without importing RunningHubService directly.
type RHCreditsGranter interface {
	GrantRH(ctx context.Context, userID int64, amount float64) (float64, error)
	GetSettings(ctx context.Context) (RunningHubSettings, error)
}

// sqlExecer is satisfied by both *sql.DB and *sql.Tx.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *RunningHubService) Credits(ctx context.Context, userID int64) (RunningHubCredits, error) {
	var c RunningHubCredits
	if s.db == nil {
		return c, ErrRunningHubNoDB
	}
	err := s.db.QueryRowContext(ctx,
		`SELECT rh_credits FROM runninghub_user_credits WHERE user_id=$1`, userID).Scan(&c.RH)
	if errors.Is(err, sql.ErrNoRows) {
		return c, nil
	}
	return c, err
}

// GrantRH adds credits to a user and returns the new balance.
func (s *RunningHubService) GrantRH(ctx context.Context, userID int64, amount float64) (float64, error) {
	if amount <= 0 {
		return 0, errors.New("amount must be positive")
	}
	if s.db == nil {
		return 0, ErrRunningHubNoDB
	}
	var bal float64
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO runninghub_user_credits(user_id, rh_credits) VALUES($1,$2)
		 ON CONFLICT(user_id) DO UPDATE
		 SET rh_credits = runninghub_user_credits.rh_credits + EXCLUDED.rh_credits,
		     updated_at = NOW()
		 RETURNING rh_credits`,
		userID, amount).Scan(&bal)
	return bal, err
}

// reserveRH deducts amount only if the balance covers it.
func reserveRH(ctx context.Context, db sqlExecer, userID int64, amount float64) error {
	if amount <= 0 {
		return nil
	}
	var n int
	err := db.QueryRowContext(ctx,
		`UPDATE runninghub_user_credits
		 SET rh_credits = rh_credits - $1, updated_at = NOW()
		 WHERE user_id = $2 AND rh_credits >= $1
		 RETURNING 1`,
		amount, userID).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrRunningHubInsufficient
	}
	return err
}

// adjustRH applies a signed delta unconditionally. Used at settlement, where the
// upstream cost has already been incurred, so the balance may go negative.
func adjustRH(ctx context.Context, db sqlExecer, userID int64, delta float64) error {
	if delta == 0 {
		return nil
	}
	_, err := db.ExecContext(ctx,
		`INSERT INTO runninghub_user_credits(user_id, rh_credits) VALUES($1,$2)
		 ON CONFLICT(user_id) DO UPDATE
		 SET rh_credits = runninghub_user_credits.rh_credits + EXCLUDED.rh_credits,
		     updated_at = NOW()`,
		userID, delta)
	return err
}
