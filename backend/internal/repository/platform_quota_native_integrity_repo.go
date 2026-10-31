package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/shopspring/decimal"
)

type nativeProjectionFacts struct {
	birth       *service.BatchImageNativeBirth
	birthXID    sql.NullString
	witnesses   map[string]*batchImageNativeMoneyOperation
	witnessXIDs map[string]sql.NullString
	proofs      map[string]*service.BatchImageBalanceLifecycleProof
	proofXIDs   map[string]sql.NullString
	platform    *batchImagePlatformQuotaJournal
	platformXID sql.NullString
}

// Native integrity is a prerequisite for the existing partial projection, not
// a certificate for all sources or all provider consumption. Inspect current
// snapshot facts before contribution filters; a missing proof must not turn
// into a smaller total. Birth and witness are the only native membership roots.
func verifyNativePlatformProjection(ctx context.Context, tx *sql.Tx, userID int64, platform string) error {
	if platform != service.PlatformGemini {
		var drift bool
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM usage_billing_batch_image_platform_quota_journal q
			WHERE q.user_id=$1 AND q.platform=$2 AND
			(EXISTS(SELECT 1 FROM batch_image_native_birth b WHERE b.batch_id=q.batch_id)
			 OR EXISTS(SELECT 1 FROM batch_image_native_money_operations w WHERE w.batch_id=q.batch_id)))`, userID, platform).Scan(&drift)
		if err != nil {
			return errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
		}
		if drift {
			return service.ErrPlatformQuotaJournalProjectionConflict
		}
		return nil
	}
	err := verifyNativePlatformProjectionFacts(ctx, tx, userID)
	if err == nil {
		return nil
	}
	if errors.Is(err, service.ErrPlatformQuotaJournalProjectionUnavailable) || errors.Is(err, service.ErrPlatformQuotaJournalProjectionConflict) {
		return err
	}
	if errors.Is(err, service.ErrBatchImageNativeBirthHistory) {
		return errors.Join(service.ErrPlatformQuotaJournalProjectionHistory, err)
	}
	if errors.Is(err, service.ErrBatchImageNativeBirthConflict) || errors.Is(err, service.ErrBatchImageBalanceLifecycleConflict) ||
		errors.Is(err, service.ErrBatchImageBalanceLifecycleInput) || errors.Is(err, service.ErrBatchImageNativeBirthInput) ||
		errors.Is(err, service.ErrBatchImagePlatformQuotaIntent) {
		return errors.Join(service.ErrPlatformQuotaJournalProjectionConflict, err)
	}
	return errors.Join(service.ErrPlatformQuotaJournalProjectionUnavailable, err)
}

func verifyNativePlatformProjectionFacts(ctx context.Context, tx *sql.Tx, userID int64) error {
	rows, err := tx.QueryContext(ctx, `SELECT batch_id FROM (
		SELECT batch_id FROM batch_image_native_birth WHERE user_id=$1
		UNION SELECT batch_id FROM batch_image_native_money_operations WHERE user_id=$1
		UNION SELECT p.batch_id FROM batch_image_balance_lifecycle_journal p WHERE p.user_id=$1 AND
			(EXISTS(SELECT 1 FROM batch_image_native_birth b WHERE b.batch_id=p.batch_id)
			 OR EXISTS(SELECT 1 FROM batch_image_native_money_operations w WHERE w.batch_id=p.batch_id))
		UNION SELECT q.batch_id FROM usage_billing_batch_image_platform_quota_journal q WHERE q.user_id=$1 AND
			(EXISTS(SELECT 1 FROM batch_image_native_birth b WHERE b.batch_id=q.batch_id)
			 OR EXISTS(SELECT 1 FROM batch_image_native_money_operations w WHERE w.batch_id=q.batch_id))
	) native ORDER BY batch_id LIMIT $2`, userID, service.MaxClientPlatformQuotaProjectionEntries+1)
	if err != nil {
		return err
	}
	ids := make([]string, 0)
	facts := make(map[string]*nativeProjectionFacts)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
		if len(ids) > service.MaxClientPlatformQuotaProjectionEntries {
			rows.Close()
			return service.ErrPlatformQuotaJournalProjectionUnavailable
		}
		facts[id] = &nativeProjectionFacts{witnesses: make(map[string]*batchImageNativeMoneyOperation), witnessXIDs: make(map[string]sql.NullString),
			proofs: make(map[string]*service.BatchImageBalanceLifecycleProof), proofXIDs: make(map[string]sql.NullString)}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if err := readNativeProjectionRows(ctx, tx, `SELECT `+batchImageNativeBirthColumns+`,write_xid::text
		FROM batch_image_native_birth WHERE batch_id=ANY($1) LIMIT $2`, ids, len(ids), func(rows *sql.Rows) error {
		b := &service.BatchImageNativeBirth{}
		var xid sql.NullString
		if err := rows.Scan(&b.Version, &b.JobID, &b.BatchID, &b.UserID, &b.APIKeyID, &b.Provider, &b.Model,
			&b.RequestHash, &b.RawHoldUSD, &b.ReserveFingerprint, &b.ProofFingerprint, &xid); err != nil {
			return err
		}
		f := facts[b.BatchID]
		if f == nil || f.birth != nil {
			return service.ErrBatchImageNativeBirthConflict
		}
		if err := service.ValidateBatchImageNativeBirthStoredProof(b); err != nil {
			return err
		}
		f.birth, f.birthXID = b, xid
		return nil
	}); err != nil {
		return err
	}
	if err := readNativeProjectionRows(ctx, tx, `SELECT batch_id,operation,request_id,user_id,api_key_id,
		raw_hold_usd,raw_actual_usd,billing_fingerprint,proof_fingerprint,write_xid::text
		FROM batch_image_native_money_operations WHERE batch_id=ANY($1) LIMIT $2`, ids, 3*len(ids), func(rows *sql.Rows) error {
		w := &batchImageNativeMoneyOperation{}
		var xid sql.NullString
		if err := rows.Scan(&w.batchID, &w.operation, &w.requestID, &w.userID, &w.apiKeyID, &w.rawHoldUSD, &w.rawActualUSD, &w.billingFingerprint, &w.proofFingerprint, &xid); err != nil {
			return err
		}
		f := facts[w.batchID]
		if f == nil || f.witnesses[w.operation] != nil || (w.operation != "reserve" && w.operation != "capture" && w.operation != "release") {
			return service.ErrBatchImageNativeBirthConflict
		}
		f.witnesses[w.operation], f.witnessXIDs[w.operation] = w, xid
		return nil
	}); err != nil {
		return err
	}
	if err := readNativeProjectionRows(ctx, tx, `SELECT `+batchImageBalanceLifecycleColumns+`,write_xid::text
		FROM batch_image_balance_lifecycle_journal WHERE batch_id=ANY($1) LIMIT $2`, ids, 3*len(ids), func(rows *sql.Rows) error {
		p := &service.BatchImageBalanceLifecycleProof{}
		var xid sql.NullString
		if err := rows.Scan(&p.Version, &p.Operation, &p.RequestID, &p.BatchID, &p.UserID, &p.APIKeyID, &p.BillingFingerprint, &p.ProofFingerprint,
			&p.RawHoldUSD, &p.RawActualUSD, &p.Outcome, &p.ReserveOrigin, &p.ReserveFingerprint, &xid); err != nil {
			return err
		}
		f := facts[p.BatchID]
		if f == nil || f.proofs[p.Operation] != nil {
			return service.ErrBatchImageNativeBirthConflict
		}
		if err := service.ValidateBatchImageBalanceLifecycleStoredProof(p); err != nil {
			return err
		}
		f.proofs[p.Operation], f.proofXIDs[p.Operation] = p, xid
		return nil
	}); err != nil {
		return err
	}
	// Reuse the journal scanner without adding metadata to its shared codec.
	if err := readNativeProjectionRows(ctx, tx, `SELECT `+batchImagePlatformQuotaJournalColumns+`
		FROM usage_billing_batch_image_platform_quota_journal WHERE batch_id=ANY($1) LIMIT $2`, ids, len(ids), func(rows *sql.Rows) error {
		j, err := scanBatchImagePlatformQuotaJournal(rows)
		if err != nil {
			return err
		}
		f := facts[j.batchID]
		if f == nil || f.platform != nil {
			return service.ErrBatchImageNativeBirthConflict
		}
		f.platform = j
		return nil
	}); err != nil {
		return err
	}
	if err := readNativeProjectionRows(ctx, tx, `SELECT batch_id,write_xid::text FROM usage_billing_batch_image_platform_quota_journal
		WHERE batch_id=ANY($1) LIMIT $2`, ids, len(ids), func(rows *sql.Rows) error {
		var id string
		var xid sql.NullString
		if err := rows.Scan(&id, &xid); err != nil {
			return err
		}
		facts[id].platformXID = xid
		return nil
	}); err != nil {
		return err
	}
	for _, id := range ids {
		if err := verifyNativeProjectionBatch(facts[id], userID); err != nil {
			return err
		}
	}
	requests := make(map[string]*nativeProjectionFacts, 3*len(ids))
	operations := make(map[string]string, 3*len(ids))
	for _, id := range ids {
		for operation, requestID := range map[string]string{"reserve": service.BatchImageHoldRequestID(id), "capture": service.BatchImageCaptureRequestID(id), "release": service.BatchImageReleaseRequestID(id)} {
			requests[requestID], operations[requestID] = facts[id], operation
		}
	}
	if err := readNativeProjectionRows(ctx, tx, `SELECT d.request_id,d.api_key_id,d.request_fingerprint FROM (
		SELECT request_id,api_key_id,request_fingerprint FROM usage_billing_dedup
		UNION ALL SELECT request_id,api_key_id,request_fingerprint FROM usage_billing_dedup_archive
	) d JOIN batch_image_native_birth b ON b.batch_id=ANY($1) AND d.api_key_id=b.api_key_id AND d.request_id IN
		('batch_image_hold:'||b.batch_id,'batch_image_capture:'||b.batch_id,'batch_image_release:'||b.batch_id) LIMIT $2`, ids, 6*len(ids), func(rows *sql.Rows) error {
		var requestID, fingerprint string
		var keyID int64
		if err := rows.Scan(&requestID, &keyID, &fingerprint); err != nil {
			return err
		}
		f := requests[requestID]
		if f == nil || keyID != f.birth.APIKeyID {
			return service.ErrBatchImageNativeBirthConflict
		}
		proof := f.proofs[operations[requestID]]
		if proof == nil {
			return service.ErrBatchImageNativeBirthHistory
		}
		if proof.BillingFingerprint != fingerprint {
			return service.ErrBatchImageNativeBirthConflict
		}
		return nil
	}); err != nil {
		return err
	}
	// Canonical native IDs also cannot acquire a competing client/ordinary
	// origin outside the final projection's user/platform/time/disposition scope.
	var collision bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM (
		SELECT request_id,api_key_id FROM usage_settlement_platform_quota_journal
		UNION ALL SELECT request_id,api_key_id FROM usage_billing_ordinary_platform_quota_journal
	) j JOIN batch_image_native_birth b ON b.batch_id=ANY($1) AND j.api_key_id=b.api_key_id AND j.request_id IN
		('batch_image_hold:'||b.batch_id,'batch_image_capture:'||b.batch_id,'batch_image_release:'||b.batch_id))`, pq.Array(ids)).Scan(&collision)
	if err != nil {
		return err
	}
	if collision {
		return service.ErrPlatformQuotaJournalProjectionConflict
	}
	return nil
}

func readNativeProjectionRows(ctx context.Context, tx *sql.Tx, query string, ids []string, limit int, scan func(*sql.Rows) error) error {
	rows, err := tx.QueryContext(ctx, query, pq.Array(ids), limit+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		if count > limit {
			return service.ErrPlatformQuotaJournalProjectionUnavailable
		}
		if err := scan(rows); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return rows.Close()
}

func verifyNativeProjectionXID(a, b sql.NullString) error {
	if !a.Valid || !b.Valid || a.String == "" || b.String == "" {
		return service.ErrBatchImageNativeBirthHistory
	}
	if a.String == "0" || b.String == "0" || a.String != b.String {
		return service.ErrBatchImageNativeBirthConflict
	}
	return nil
}

func verifyNativeProjectionBatch(f *nativeProjectionFacts, userID int64) error {
	if f.birth == nil {
		return service.ErrBatchImageNativeBirthHistory
	}
	b := f.birth
	if b.UserID != userID {
		return service.ErrBatchImageNativeBirthConflict
	}
	for _, p := range f.proofs {
		if p.UserID != b.UserID || p.APIKeyID != b.APIKeyID || p.RawHoldUSD != b.RawHoldUSD {
			return service.ErrBatchImageNativeBirthConflict
		}
	}
	if (f.proofs["capture"] != nil && f.proofs["release"] != nil) ||
		(f.witnesses["capture"] != nil && f.witnesses["release"] != nil) {
		return service.ErrBatchImageNativeBirthConflict
	}
	if err := verifyBatchImageNativeReserve(b, f.proofs); err != nil {
		return err
	}
	if err := verifyBatchImageNativeMoneyOperations(b, f.witnesses, f.proofs, false); err != nil {
		return err
	}
	if err := verifyNativeProjectionXID(f.birthXID, f.proofXIDs["reserve"]); err != nil {
		return err
	}
	for operation := range f.proofs {
		if err := verifyNativeProjectionXID(f.proofXIDs[operation], f.witnessXIDs[operation]); err != nil {
			return err
		}
	}
	capture := f.proofs["capture"]
	if capture == nil {
		if f.platform != nil {
			return service.ErrBatchImageNativeBirthHistory
		}
		return nil // A valid open reservation is not a completed/provider-free job.
	}
	if f.platform == nil {
		return service.ErrBatchImageNativeBirthHistory
	}
	j := f.platform
	if j.batchID != b.BatchID || j.userID != b.UserID || j.apiKeyID != b.APIKeyID || j.provider != b.Provider || j.platform != service.PlatformGemini ||
		j.requestID != capture.RequestID || j.rawHoldUSD != b.RawHoldUSD || j.rawCostUSD != capture.RawActualUSD ||
		j.billingFingerprint != capture.BillingFingerprint {
		return service.ErrBatchImageNativeBirthConflict
	}
	if err := verifyNativeProjectionXID(f.platformXID, f.proofXIDs["capture"]); err != nil {
		return err
	}
	intent := &service.BatchImagePlatformQuotaIntent{Version: j.schemaVersion, RequestID: j.requestID, APIKeyID: j.apiKeyID, UserID: j.userID,
		BatchID: j.batchID, Provider: j.provider, Platform: j.platform, RawHoldUSD: j.rawHoldUSD, RawCostUSD: j.rawCostUSD, CostUSD: j.costUSD, IntentFingerprint: j.intentFingerprint}
	if err := service.ValidateBatchImagePlatformQuotaStoredIntent(intent, j.billingFingerprint); err != nil {
		return err
	}
	return verifyNativeProjectionSnapshot(j)
}

// These are internal consistency checks of the frozen observation, not proof
// that its clock, quota incarnation or administrative baseline is authentic.
func verifyNativeProjectionSnapshot(j *batchImagePlatformQuotaJournal) error {
	if j.isSubscription || j.capturedAt.IsZero() || j.capturedAt.Year() < 1 || j.capturedAt.Year() > 9999 {
		return service.ErrBatchImageNativeBirthConflict
	}
	for _, v := range []sql.NullTime{j.dailyStart, j.weeklyStart, j.monthlyStart} {
		if v.Valid && (v.Time.IsZero() || v.Time.Year() < 1 || v.Time.Year() > 9999) {
			return service.ErrBatchImageNativeBirthConflict
		}
	}
	hasLimit := j.dailyLimit.Valid || j.weeklyLimit.Valid || j.monthlyLimit.Valid
	for _, v := range []sql.NullString{j.dailyLimit, j.weeklyLimit, j.monthlyLimit, j.dailyUsage, j.weeklyUsage, j.monthlyUsage} {
		if v.Valid {
			amount, err := decimal.NewFromString(v.String)
			if err != nil || amount.IsNegative() || amount.GreaterThanOrEqual(decimal.New(1, 10)) {
				return service.ErrBatchImageNativeBirthConflict
			}
		}
	}
	if j.quotaRowID.Valid {
		if j.quotaRowID.Int64 <= 0 || !j.dailyUsage.Valid || !j.weeklyUsage.Valid || !j.monthlyUsage.Valid {
			return service.ErrBatchImageNativeBirthConflict
		}
	} else if hasLimit || j.dailyUsage.Valid || j.weeklyUsage.Valid || j.monthlyUsage.Valid || j.dailyStart.Valid || j.weeklyStart.Valid || j.monthlyStart.Valid {
		return service.ErrBatchImageNativeBirthConflict
	}
	cost, err := decimal.NewFromString(j.costUSD)
	if err != nil {
		return service.ErrBatchImageNativeBirthConflict
	}
	switch j.disposition {
	case "metered":
		if !cost.IsPositive() || !j.quotaRowID.Valid || !hasLimit {
			return service.ErrBatchImageNativeBirthConflict
		}
	case "unlimited":
		if !cost.IsPositive() || hasLimit {
			return service.ErrBatchImageNativeBirthConflict
		}
	case "zero_cost":
		if !cost.IsZero() || j.quotaRowID.Valid {
			return service.ErrBatchImageNativeBirthConflict
		}
	default:
		return service.ErrBatchImageNativeBirthConflict
	}
	return nil
}
