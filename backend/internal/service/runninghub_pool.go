package service

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/lib/pq"
)

// ── Account management ────────────────────────────────────────────────────────

func (s *RunningHubService) ListAccounts(ctx context.Context) ([]RunningHubAccount, error) {
	if s.db == nil {
		return nil, ErrRunningHubNoDB
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, tier, max_concurrency, enabled, in_flight
		 FROM runninghub_accounts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunningHubAccount{} // non-nil so an empty pool encodes as [] rather than null
	for rows.Next() {
		var a RunningHubAccount
		if err := rows.Scan(&a.ID, &a.Name, &a.Tier, &a.MaxConcurrency, &a.Enabled, &a.InFlight); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *RunningHubService) AddAccount(ctx context.Context, name, tier, key string, maxConc int) (int64, error) {
	if s.db == nil {
		return 0, ErrRunningHubNoDB
	}
	if tier != RunningHubTierConsumer && tier != RunningHubTierEnterprise {
		return 0, errors.New("tier must be consumer or enterprise")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return 0, errors.New("api_key is required")
	}
	if maxConc <= 0 {
		maxConc = 3
	}
	var id int64
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO runninghub_accounts(name,tier,api_key,max_concurrency)
		 VALUES($1,$2,$3,$4) RETURNING id`,
		strings.TrimSpace(name), tier, key, maxConc).Scan(&id)
	return id, err
}

type RunningHubAccountUpdate struct {
	Name           *string `json:"name"`
	MaxConcurrency *int    `json:"max_concurrency"`
	Enabled        *bool   `json:"enabled"`
	APIKey         *string `json:"api_key"`
}

func (s *RunningHubService) UpdateAccount(ctx context.Context, id int64, u RunningHubAccountUpdate) error {
	if s.db == nil {
		return ErrRunningHubNoDB
	}
	if u.MaxConcurrency != nil && *u.MaxConcurrency <= 0 {
		return errors.New("max_concurrency must be positive")
	}
	if u.APIKey != nil && strings.TrimSpace(*u.APIKey) == "" {
		u.APIKey = nil // empty means "keep current key"
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE runninghub_accounts SET
		   name            = COALESCE($2, name),
		   max_concurrency = COALESCE($3, max_concurrency),
		   enabled         = COALESCE($4, enabled),
		   api_key         = COALESCE($5, api_key),
		   updated_at      = NOW()
		 WHERE id = $1`,
		id, u.Name, u.MaxConcurrency, u.Enabled, u.APIKey)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrRunningHubNotFound
	}
	return nil
}

// DeleteAccount soft-disables an account so historical task rows keep their FK.
func (s *RunningHubService) DeleteAccount(ctx context.Context, id int64) error {
	f := false
	return s.UpdateAccount(ctx, id, RunningHubAccountUpdate{Enabled: &f})
}

// ── Pool scheduling ───────────────────────────────────────────────────────────

// enabledTiers returns the tiers to try, in priority order: consumer keys are
// cheaper so they go first, enterprise keys absorb the overflow.
func (s *RunningHubService) enabledTiers(ctx context.Context) ([]string, error) {
	st, err := s.GetSettings(ctx)
	if err != nil {
		return nil, err
	}
	var tiers []string
	if st.ConsumerEnabled {
		tiers = append(tiers, RunningHubTierConsumer)
	}
	if st.EnterpriseEnabled {
		tiers = append(tiers, RunningHubTierEnterprise)
	}
	if len(tiers) == 0 {
		return nil, errors.New("RunningHub is disabled: no account tier is enabled")
	}
	return tiers, nil
}

// pickAccount reserves a concurrency slot on the least-loaded enabled account,
// trying tiers in priority order and skipping accounts in exclude. Ties are
// broken by least-recently-picked so load rotates across accounts.
// The caller must call releaseAccount when the slot is no longer needed.
func (s *RunningHubService) pickAccount(ctx context.Context, exclude []int64) (rhAccountRow, error) {
	tiers, err := s.enabledTiers(ctx)
	if err != nil {
		return rhAccountRow{}, err
	}
	if exclude == nil {
		exclude = []int64{}
	}
	for _, tier := range tiers {
		var a rhAccountRow
		err := s.db.QueryRowContext(ctx,
			`WITH picked AS (
			   SELECT id FROM runninghub_accounts
			   WHERE enabled AND tier = $1 AND in_flight < max_concurrency AND NOT (id = ANY($2))
			   ORDER BY in_flight::float / max_concurrency, last_picked_at NULLS FIRST, id
			   FOR UPDATE SKIP LOCKED LIMIT 1
			 )
			 UPDATE runninghub_accounts ra
			 SET in_flight = ra.in_flight + 1, last_picked_at = NOW(), updated_at = NOW()
			 FROM picked
			 WHERE ra.id = picked.id
			 RETURNING ra.id, ra.name, ra.tier, ra.api_key, ra.max_concurrency, ra.enabled, ra.in_flight`,
			tier, pq.Array(exclude)).Scan(&a.ID, &a.Name, &a.Tier, &a.APIKey, &a.MaxConcurrency, &a.Enabled, &a.InFlight)
		if err == nil {
			return a, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return rhAccountRow{}, err
		}
	}
	return rhAccountRow{}, ErrRunningHubPoolBusy
}

// anyAccount returns an enabled account without reserving a slot, for metadata
// calls (apiCallDemo) that don't occupy upstream concurrency.
func (s *RunningHubService) anyAccount(ctx context.Context) (rhAccountRow, error) {
	var a rhAccountRow
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, tier, api_key, max_concurrency, enabled, in_flight
		 FROM runninghub_accounts WHERE enabled
		 ORDER BY (tier = 'consumer') DESC, in_flight, id LIMIT 1`).
		Scan(&a.ID, &a.Name, &a.Tier, &a.APIKey, &a.MaxConcurrency, &a.Enabled, &a.InFlight)
	if errors.Is(err, sql.ErrNoRows) {
		return a, errors.New("no enabled RunningHub account")
	}
	return a, err
}

func (s *RunningHubService) releaseAccount(ctx context.Context, id int64) {
	if id <= 0 {
		return
	}
	_, _ = s.db.ExecContext(context.WithoutCancel(ctx),
		`UPDATE runninghub_accounts SET in_flight = GREATEST(in_flight-1,0), updated_at = NOW()
		 WHERE id = $1`, id)
}
