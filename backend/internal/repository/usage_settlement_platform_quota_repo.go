package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/shopspring/decimal"
)

const usageSettlementPlatformQuotaJournalColumns = `invocation_id,schema_version,event_fingerprint,billing_fingerprint,
	request_id,api_key_id,user_id,platform,cost_usd,captured_at,disposition,quota_row_id,
	daily_limit_usd,weekly_limit_usd,monthly_limit_usd,daily_usage_usd,weekly_usage_usd,monthly_usage_usd,
	daily_window_start,weekly_window_start,monthly_window_start`

type usageSettlementPlatformQuotaJournal struct {
	invocationID, eventFingerprint, billingFingerprint string
	requestID, platform, costUSD, disposition          string
	schemaVersion                                      int
	apiKeyID, userID                                   int64
	capturedAt                                         time.Time
	quotaRowID                                         sql.NullInt64
	// Preserve the database's exact NUMERIC(20,10) observation. A float64
	// round-trip would erase low decimal digits on larger quota values.
	dailyLimit, weeklyLimit, monthlyLimit sql.NullString
	dailyUsage, weeklyUsage, monthlyUsage sql.NullString
	dailyStart, weeklyStart, monthlyStart sql.NullTime
}

func scanUsageSettlementPlatformQuotaJournal(scanner sessionScanner) (*usageSettlementPlatformQuotaJournal, error) {
	journal := &usageSettlementPlatformQuotaJournal{}
	if err := scanner.Scan(&journal.invocationID, &journal.schemaVersion, &journal.eventFingerprint, &journal.billingFingerprint,
		&journal.requestID, &journal.apiKeyID, &journal.userID, &journal.platform, &journal.costUSD, &journal.capturedAt,
		&journal.disposition, &journal.quotaRowID, &journal.dailyLimit, &journal.weeklyLimit, &journal.monthlyLimit,
		&journal.dailyUsage, &journal.weeklyUsage, &journal.monthlyUsage, &journal.dailyStart, &journal.weeklyStart, &journal.monthlyStart); err != nil {
		return nil, err
	}
	return journal, nil
}

func usageSettlementPlatformQuotaJournalMatches(journal *usageSettlementPlatformQuotaJournal, event *service.UsageSettlementEvent) bool {
	if journal == nil || event == nil || event.SchemaVersion != 2 || event.PlatformQuota == nil ||
		journal.invocationID != event.InvocationID || journal.schemaVersion != event.SchemaVersion ||
		journal.eventFingerprint != event.EventFingerprint || journal.billingFingerprint != event.Command.RequestFingerprint ||
		journal.requestID != event.RequestID || journal.apiKeyID != event.APIKeyID || journal.userID != event.Command.UserID ||
		journal.platform != event.PlatformQuota.Platform || !journal.capturedAt.Equal(event.CreatedAt) {
		return false
	}
	cost, err := decimal.NewFromString(journal.costUSD)
	if err != nil || !cost.Equal(decimal.NewFromFloat(event.PlatformQuota.CostUSD)) {
		return false
	}
	if event.Usage.BillingType == service.BillingTypeSubscription {
		return journal.disposition == "subscription_exempt" && !journal.quotaRowID.Valid
	}
	if cost.IsZero() {
		return journal.disposition == "zero_cost" && !journal.quotaRowID.Valid
	}
	hasLimit := journal.dailyLimit.Valid || journal.weeklyLimit.Valid || journal.monthlyLimit.Valid
	if hasLimit {
		return journal.disposition == "metered" && journal.quotaRowID.Valid && journal.quotaRowID.Int64 > 0
	}
	return journal.disposition == "unlimited"
}

// verifyUsageSettlementBilling observes both hot and archived dedup records.
// Every matching durable key must agree; one matching row cannot conceal a
// second conflicting record in the other storage tier.
func verifyUsageSettlementBilling(ctx context.Context, tx *sql.Tx, event *service.UsageSettlementEvent) error {
	var count int
	var matching bool
	err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(bool_and(request_fingerprint=$3),FALSE) FROM (
		SELECT request_fingerprint FROM usage_billing_dedup WHERE request_id=$1 AND api_key_id=$2
		UNION ALL
		SELECT request_fingerprint FROM usage_billing_dedup_archive WHERE request_id=$1 AND api_key_id=$2
	) AS durable_billing`, event.RequestID, event.APIKeyID, event.Command.RequestFingerprint).Scan(&count, &matching)
	if err != nil {
		return err
	}
	if count == 0 {
		return service.ErrUsageSettlementUnconfirmed
	}
	if !matching {
		return service.ErrUsageSettlementEventConflict
	}
	return nil
}

func verifyUsageSettlementPlatformQuota(ctx context.Context, tx *sql.Tx, event *service.UsageSettlementEvent) error {
	if event.SchemaVersion == 1 {
		return nil
	}
	journal, err := scanUsageSettlementPlatformQuotaJournal(tx.QueryRowContext(ctx, `SELECT `+
		usageSettlementPlatformQuotaJournalColumns+` FROM usage_settlement_platform_quota_journal WHERE invocation_id=$1`, event.InvocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return service.ErrUsageSettlementUnconfirmed
	}
	if err != nil {
		return err
	}
	if !usageSettlementPlatformQuotaJournalMatches(journal, event) {
		return service.ErrUsageSettlementEventConflict
	}
	return nil
}

// RecordPlatformQuota records immutable raw metering after real billing has
// committed. It never changes legacy quota counters, windows, caches, or their
// snapshot-flusher state, and grants no platform-limit admission guarantee.
func (r *usageSettlementEventRepository) RecordPlatformQuota(ctx context.Context, invocationID, leaseToken string) error {
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
	now, err := invocationNow(ctx, tx)
	if err != nil {
		return err
	}
	if event.State != service.UsageSettlementPending || event.LeaseExpiresAt == nil || !event.LeaseExpiresAt.After(now) {
		return service.ErrUsageSettlementLease
	}
	// Historical v1 payloads have no trustworthy platform. Leave their recovery
	// unchanged instead of inventing one from a current account or group.
	if event.SchemaVersion == 1 {
		return tx.Commit()
	}
	if event.SchemaVersion != 2 || event.PlatformQuota == nil || !domain.IsConcretePlatform(event.PlatformQuota.Platform) {
		return service.ErrUsageSettlementPayload
	}
	if err = verifyUsageSettlementBilling(ctx, tx, event); err != nil {
		return err
	}
	journal, err := scanUsageSettlementPlatformQuotaJournal(tx.QueryRowContext(ctx, `SELECT `+
		usageSettlementPlatformQuotaJournalColumns+` FROM usage_settlement_platform_quota_journal WHERE invocation_id=$1`, invocationID))
	if err == nil {
		if !usageSettlementPlatformQuotaJournalMatches(journal, event) {
			return service.ErrUsageSettlementEventConflict
		}
		// The first observation wins, including an unlimited/no-row decision.
		// Replays never consult current admin configuration for an existing row.
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	journal = &usageSettlementPlatformQuotaJournal{disposition: "unlimited"}
	if event.Usage.BillingType == service.BillingTypeSubscription {
		journal.disposition = "subscription_exempt"
	} else if event.PlatformQuota.CostUSD == 0 {
		journal.disposition = "zero_cost"
	} else {
		err = tx.QueryRowContext(ctx, `SELECT id,daily_limit_usd,weekly_limit_usd,monthly_limit_usd,
			daily_usage_usd,weekly_usage_usd,monthly_usage_usd,daily_window_start,weekly_window_start,monthly_window_start
			FROM user_platform_quotas WHERE user_id=$1 AND platform=$2 AND deleted_at IS NULL FOR SHARE`,
			event.Command.UserID, event.PlatformQuota.Platform).
			Scan(&journal.quotaRowID, &journal.dailyLimit, &journal.weeklyLimit, &journal.monthlyLimit,
				&journal.dailyUsage, &journal.weeklyUsage, &journal.monthlyUsage, &journal.dailyStart, &journal.weeklyStart, &journal.monthlyStart)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if journal.dailyLimit.Valid || journal.weeklyLimit.Valid || journal.monthlyLimit.Valid {
			journal.disposition = "metered"
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO usage_settlement_platform_quota_journal (`+
		usageSettlementPlatformQuotaJournalColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`,
		event.InvocationID, event.SchemaVersion, event.EventFingerprint, event.Command.RequestFingerprint,
		event.RequestID, event.APIKeyID, event.Command.UserID, event.PlatformQuota.Platform, event.PlatformQuota.CostUSD, event.CreatedAt,
		journal.disposition, journal.quotaRowID, journal.dailyLimit, journal.weeklyLimit, journal.monthlyLimit,
		journal.dailyUsage, journal.weeklyUsage, journal.monthlyUsage, journal.dailyStart, journal.weeklyStart, journal.monthlyStart)
	if err != nil {
		return err
	}
	return tx.Commit()
}
