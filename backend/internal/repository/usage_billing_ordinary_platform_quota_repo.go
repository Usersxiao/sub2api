package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// Batch keeps this original schema; only ordinary source observations gain the
// new generation field. Keep the shared column order stable for its scanner.
const platformQuotaJournalSnapshotColumns = `request_id,api_key_id,user_id,schema_version,billing_fingerprint,
	intent_fingerprint,platform,raw_cost_usd,cost_usd,is_subscription,captured_at,disposition,quota_row_id,
	daily_limit_usd,weekly_limit_usd,monthly_limit_usd,daily_usage_usd,weekly_usage_usd,monthly_usage_usd,
	daily_window_start,weekly_window_start,monthly_window_start`

const ordinaryPlatformQuotaJournalColumns = platformQuotaJournalSnapshotColumns + `,control_generation`

type ordinaryPlatformQuotaJournal struct {
	requestID, billingFingerprint, intentFingerprint string
	platform, rawCostUSD, costUSD, disposition       string
	apiKeyID, userID                                 int64
	schemaVersion                                    int
	isSubscription                                   bool
	capturedAt                                       time.Time
	quotaRowID                                       sql.NullInt64
	controlGeneration                                sql.NullInt64
	dailyLimit, weeklyLimit, monthlyLimit            sql.NullString
	dailyUsage, weeklyUsage, monthlyUsage            sql.NullString
	dailyStart, weeklyStart, monthlyStart            sql.NullTime
}

func scanOrdinaryPlatformQuotaJournal(scanner sessionScanner) (*ordinaryPlatformQuotaJournal, error) {
	j := &ordinaryPlatformQuotaJournal{}
	if err := scanner.Scan(&j.requestID, &j.apiKeyID, &j.userID, &j.schemaVersion, &j.billingFingerprint,
		&j.intentFingerprint, &j.platform, &j.rawCostUSD, &j.costUSD, &j.isSubscription, &j.capturedAt,
		&j.disposition, &j.quotaRowID, &j.dailyLimit, &j.weeklyLimit, &j.monthlyLimit,
		&j.dailyUsage, &j.weeklyUsage, &j.monthlyUsage, &j.dailyStart, &j.weeklyStart, &j.monthlyStart, &j.controlGeneration); err != nil {
		return nil, err
	}
	return j, nil
}

func findOrdinaryPlatformQuotaJournal(ctx context.Context, tx *sql.Tx, requestID string, apiKeyID int64) (*ordinaryPlatformQuotaJournal, error) {
	j, err := scanOrdinaryPlatformQuotaJournal(tx.QueryRowContext(ctx, `SELECT `+ordinaryPlatformQuotaJournalColumns+`
		FROM usage_billing_ordinary_platform_quota_journal WHERE request_id=$1 AND api_key_id=$2`, requestID, apiKeyID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

// A permanent journal remains a billing proof after normal dedup retention has
// removed both tiers. If either tier still exists, every fingerprint must agree.
func verifyOrdinaryPlatformQuotaBilling(ctx context.Context, tx *sql.Tx, cmd *service.UsageBillingCommand) error {
	var matches bool
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(bool_and(request_fingerprint=$3),TRUE) FROM (
		SELECT request_fingerprint FROM usage_billing_dedup WHERE request_id=$1 AND api_key_id=$2
		UNION ALL
		SELECT request_fingerprint FROM usage_billing_dedup_archive WHERE request_id=$1 AND api_key_id=$2
	) AS billing_proofs`, cmd.RequestID, cmd.APIKeyID, cmd.RequestFingerprint).Scan(&matches); err != nil {
		return err
	}
	if !matches {
		return service.ErrUsageBillingRequestConflict
	}
	return nil
}

func verifyOrdinaryPlatformQuotaJournal(ctx context.Context, tx *sql.Tx, cmd *service.UsageBillingCommand, j *ordinaryPlatformQuotaJournal) error {
	if j == nil || j.requestID != cmd.RequestID || j.apiKeyID != cmd.APIKeyID || j.userID != cmd.UserID ||
		j.billingFingerprint != cmd.RequestFingerprint {
		return service.ErrUsageBillingRequestConflict
	}
	stored := &service.UsageBillingOrdinaryPlatformIntent{Version: j.schemaVersion, RequestID: j.requestID,
		APIKeyID: j.apiKeyID, UserID: j.userID, Platform: j.platform, RawCostUSD: j.rawCostUSD,
		CostUSD: j.costUSD, IsSubscription: j.isSubscription, IntentFingerprint: j.intentFingerprint}
	if err := service.ValidateUsageBillingOrdinaryPlatformIntent(cmd, stored); err != nil {
		return service.ErrUsageBillingRequestConflict
	}
	if intent := cmd.OrdinaryPlatformQuota; intent != nil && (intent.Version != stored.Version ||
		intent.RequestID != stored.RequestID || intent.APIKeyID != stored.APIKeyID || intent.UserID != stored.UserID ||
		intent.Platform != stored.Platform || intent.RawCostUSD != stored.RawCostUSD || intent.CostUSD != stored.CostUSD ||
		intent.IsSubscription != stored.IsSubscription || intent.IntentFingerprint != stored.IntentFingerprint) {
		return service.ErrUsageBillingRequestConflict
	}
	return verifyOrdinaryPlatformQuotaBilling(ctx, tx, cmd)
}

// Observe the quota incarnation before taking the consumption clock. The share
// lock serializes this observation with admin resets/deletion, without locking
// API keys ahead of the balance/key updates in the ordinary billing transaction.
func observeOrdinaryPlatformQuota(ctx context.Context, tx *sql.Tx, cmd *service.UsageBillingCommand) (*ordinaryPlatformQuotaJournal, error) {
	i := cmd.OrdinaryPlatformQuota
	j := &ordinaryPlatformQuotaJournal{requestID: i.RequestID, apiKeyID: i.APIKeyID, userID: i.UserID,
		schemaVersion: i.Version, billingFingerprint: cmd.RequestFingerprint, intentFingerprint: i.IntentFingerprint,
		platform: i.Platform, rawCostUSD: i.RawCostUSD, costUSD: i.CostUSD, isSubscription: i.IsSubscription,
		disposition: "unlimited"}
	return observePlatformQuotaSnapshot(ctx, tx, j)
}

func observePlatformQuotaSnapshot(ctx context.Context, tx *sql.Tx, j *ordinaryPlatformQuotaJournal) (*ordinaryPlatformQuotaJournal, error) {
	if j.isSubscription {
		j.disposition = "subscription_exempt"
	} else if j.costUSD == "0.0000000000" {
		j.disposition = "zero_cost"
	} else {
		err := tx.QueryRowContext(ctx, `SELECT id,control_generation,daily_limit_usd,weekly_limit_usd,monthly_limit_usd,
			daily_usage_usd,weekly_usage_usd,monthly_usage_usd,daily_window_start,weekly_window_start,monthly_window_start
			FROM user_platform_quotas WHERE user_id=$1 AND platform=$2 AND deleted_at IS NULL FOR SHARE`,
			j.userID, j.platform).Scan(&j.quotaRowID, &j.controlGeneration, &j.dailyLimit, &j.weeklyLimit, &j.monthlyLimit,
			&j.dailyUsage, &j.weeklyUsage, &j.monthlyUsage, &j.dailyStart, &j.weeklyStart, &j.monthlyStart)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if j.dailyLimit.Valid || j.weeklyLimit.Valid || j.monthlyLimit.Valid {
			j.disposition = "metered"
		}
	}
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&j.capturedAt); err != nil {
		return nil, err
	}
	return j, nil
}

func insertOrdinaryPlatformQuotaJournal(ctx context.Context, tx *sql.Tx, j *ordinaryPlatformQuotaJournal) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO usage_billing_ordinary_platform_quota_journal (`+
		ordinaryPlatformQuotaJournalColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)`,
		j.requestID, j.apiKeyID, j.userID, j.schemaVersion, j.billingFingerprint, j.intentFingerprint,
		j.platform, j.rawCostUSD, j.costUSD, j.isSubscription, j.capturedAt, j.disposition, j.quotaRowID,
		j.dailyLimit, j.weeklyLimit, j.monthlyLimit, j.dailyUsage, j.weeklyUsage, j.monthlyUsage,
		j.dailyStart, j.weeklyStart, j.monthlyStart, j.controlGeneration)
	return err
}

func ordinaryPlatformQuotaReceipt(j *ordinaryPlatformQuotaJournal) *service.UsageBillingOrdinaryPlatformReceipt {
	if j == nil {
		return nil
	}
	return &service.UsageBillingOrdinaryPlatformReceipt{
		Intent: service.UsageBillingOrdinaryPlatformIntent{Version: j.schemaVersion, RequestID: j.requestID,
			APIKeyID: j.apiKeyID, UserID: j.userID, Platform: j.platform, RawCostUSD: j.rawCostUSD, CostUSD: j.costUSD,
			IsSubscription: j.isSubscription, IntentFingerprint: j.intentFingerprint},
		BillingFingerprint: j.billingFingerprint, Disposition: j.disposition,
		QuotaRowID: j.quotaRowID.Int64, ControlGeneration: j.controlGeneration.Int64, CapturedAt: j.capturedAt,
	}
}
