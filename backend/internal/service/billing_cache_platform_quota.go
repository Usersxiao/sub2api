package service

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
)

const userPlatformQuotaCASAttempts = 3

// checkUserPlatformQuotaEligibility reads configuration from the caller's DB
// snapshot and conservatively compares the larger DB/Redis usage observation.
// This detects stale controls and malformed observations; it does not certify
// completeness of legacy metering or reconstruct already lost Redis increments.
func (s *BillingCacheService) checkUserPlatformQuotaEligibility(ctx context.Context, userID int64, platform string) error {
	if platform == "" {
		return nil
	}
	if s == nil || s.userPlatformQuotaRepo == nil {
		return quotaObservationUnavailable("quota repository unavailable")
	}
	mutations := 0
	for {
		if err := ctx.Err(); err != nil {
			return ErrBillingServiceUnavailable.WithCause(err)
		}
		// Do not singleflight or detach this read: every request and CAS retry
		// observes current active-row controls through its own caller context.
		rec, err := s.readPlatformQuotaRecord(ctx, userID, platform)
		if err != nil {
			return ErrBillingServiceUnavailable.WithCause(err)
		}
		if err := ctx.Err(); err != nil {
			return ErrBillingServiceUnavailable.WithCause(err)
		}
		if rec == nil {
			// Absence is a fresh DB conclusion. No cached no-row sentinel is read
			// or written, so a later configuration is visible on the next request.
			return nil
		}
		now := time.Now()
		if !validPlatformQuotaRecord(rec, userID, platform, now) {
			return quotaObservationUnavailable("invalid quota database observation")
		}
		if !rec.HasAnyLimit() {
			return nil
		}
		if s.cache == nil {
			return quotaObservationUnavailable("quota cache unavailable")
		}
		entry, found, err := s.cache.GetUserPlatformQuotaCache(ctx, userID, platform)
		if err != nil {
			return ErrBillingServiceUnavailable.WithCause(err)
		}
		if err := ctx.Err(); err != nil {
			return ErrBillingServiceUnavailable.WithCause(err)
		}
		now = time.Now()
		dbUsage := normalizePlatformQuotaUsage(rec.DailyUsageUSD, rec.WeeklyUsageUSD, rec.MonthlyUsageUSD,
			rec.DailyWindowStart, rec.WeeklyWindowStart, rec.MonthlyWindowStart, now)
		var next *UserPlatformQuotaCacheEntry
		if !found && entry == nil {
			// With the flusher enabled DB usage can lag Redis. A missing key has
			// no certified recovery baseline and must not be filled from that DB.
			if s.cfg != nil && s.cfg.Database.UserPlatformQuotaFlusherEnabled {
				return quotaObservationUnavailable("quota cache recovery required")
			}
			next = quotaEntryForObservation(rec, dbUsage)
		} else {
			if !found || !validPlatformQuotaCacheEntry(entry, rec, now) {
				return quotaObservationUnavailable("invalid or stale quota cache observation")
			}
			cacheUsage := normalizePlatformQuotaUsage(entry.DailyUsageUSD, entry.WeeklyUsageUSD, entry.MonthlyUsageUSD,
				entry.DailyWindowStart, entry.WeeklyWindowStart, entry.MonthlyWindowStart, now)
			if !cacheUsage.rotated {
				if err := ctx.Err(); err != nil {
					return ErrBillingServiceUnavailable.WithCause(err)
				}
				return comparePlatformQuotaObservations(rec, dbUsage, cacheUsage, now)
			}
			next = quotaEntryForObservation(rec, cacheUsage)
		}
		if mutations >= userPlatformQuotaCASAttempts {
			return quotaObservationUnavailable("quota cache observation did not converge")
		}
		cas, supported := s.cache.(UserPlatformQuotaCacheCAS)
		if !supported {
			return quotaObservationUnavailable("conditional quota cache writes unavailable")
		}
		// Both success and a collision require a new DB and Redis observation.
		// Never decide from the candidate we attempted to publish.
		_, err = cas.CompareAndSetUserPlatformQuotaCache(ctx, userID, platform, entry, next, s.platformQuotaCacheTTL())
		if err != nil {
			return ErrBillingServiceUnavailable.WithCause(err)
		}
		mutations++
	}
}

func quotaObservationUnavailable(reason string) error {
	return ErrBillingServiceUnavailable.WithCause(errors.New(reason))
}

func (s *BillingCacheService) readPlatformQuotaRecord(ctx context.Context, userID int64, platform string) (*UserPlatformQuotaRecord, error) {
	readCtx, cancel := context.WithTimeout(ctx, balanceLoadTimeout)
	defer cancel()
	rec, err := s.userPlatformQuotaRepo.GetByUserPlatform(readCtx, userID, platform)
	if err != nil {
		return nil, err
	}
	if err := readCtx.Err(); err != nil {
		return nil, err
	}
	return rec, nil
}

func validPlatformQuotaAmount(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validPlatformQuotaLimit(limit *float64) bool {
	return limit == nil || validPlatformQuotaAmount(*limit)
}

func validPlatformQuotaWindow(usage float64, start *time.Time, now time.Time, allowUninitialized bool) bool {
	if start == nil {
		return allowUninitialized && usage == 0
	}
	return !start.After(now)
}

func validPlatformQuotaRecord(rec *UserPlatformQuotaRecord, userID int64, platform string, now time.Time) bool {
	return rec != nil && rec.QuotaRowID > 0 && rec.ControlGeneration > 0 && rec.UserID == userID && rec.Platform == platform &&
		validPlatformQuotaAmount(rec.DailyUsageUSD) && validPlatformQuotaAmount(rec.WeeklyUsageUSD) && validPlatformQuotaAmount(rec.MonthlyUsageUSD) &&
		validPlatformQuotaLimit(rec.DailyLimitUSD) && validPlatformQuotaLimit(rec.WeeklyLimitUSD) && validPlatformQuotaLimit(rec.MonthlyLimitUSD) &&
		validPlatformQuotaWindow(rec.DailyUsageUSD, rec.DailyWindowStart, now, true) &&
		validPlatformQuotaWindow(rec.WeeklyUsageUSD, rec.WeeklyWindowStart, now, true) &&
		validPlatformQuotaWindow(rec.MonthlyUsageUSD, rec.MonthlyWindowStart, now, true)
}

func validPlatformQuotaInstance(instance string) bool {
	if len(instance) != 64 {
		return false
	}
	for _, ch := range instance {
		if !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

func validPlatformQuotaCacheEntry(entry *UserPlatformQuotaCacheEntry, rec *UserPlatformQuotaRecord, now time.Time) bool {
	return entry != nil && entry.SchemaVersion == UserPlatformQuotaCacheSchemaV1 &&
		entry.QuotaRowID == rec.QuotaRowID && entry.ControlGeneration == rec.ControlGeneration &&
		entry.Version >= 0 && entry.Version < math.MaxInt64 && validPlatformQuotaInstance(entry.CacheInstance) &&
		validPlatformQuotaAmount(entry.DailyUsageUSD) && validPlatformQuotaAmount(entry.WeeklyUsageUSD) && validPlatformQuotaAmount(entry.MonthlyUsageUSD) &&
		validPlatformQuotaLimit(entry.DailyLimitUSD) && validPlatformQuotaLimit(entry.WeeklyLimitUSD) && validPlatformQuotaLimit(entry.MonthlyLimitUSD) &&
		validPlatformQuotaWindow(entry.DailyUsageUSD, entry.DailyWindowStart, now, false) &&
		validPlatformQuotaWindow(entry.WeeklyUsageUSD, entry.WeeklyWindowStart, now, false) &&
		validPlatformQuotaWindow(entry.MonthlyUsageUSD, entry.MonthlyWindowStart, now, false)
}

type platformQuotaUsageObservation struct {
	daily, weekly, monthly                float64
	dailyStart, weeklyStart, monthlyStart time.Time
	rotated                               bool
}

func normalizePlatformQuotaUsage(daily, weekly, monthly float64, dailyStart, weeklyStart, monthlyStart *time.Time, now time.Time) platformQuotaUsageObservation {
	out := platformQuotaUsageObservation{daily: daily, weekly: weekly, monthly: monthly}
	if quotaWindowExpired(dailyStart, timezone.StartOfDay(now)) {
		out.daily, out.dailyStart, out.rotated = 0, timezone.StartOfDay(now), true
	} else {
		out.dailyStart = *dailyStart
	}
	if quotaWindowExpired(weeklyStart, timezone.StartOfWeek(now)) {
		out.weekly, out.weeklyStart, out.rotated = 0, timezone.StartOfWeek(now), true
	} else {
		out.weeklyStart = *weeklyStart
	}
	if monthlyQuotaWindowExpired(monthlyStart, now) {
		out.monthly, out.monthlyStart, out.rotated = 0, now, true
	} else {
		out.monthlyStart = *monthlyStart
	}
	return out
}

func quotaEntryForObservation(rec *UserPlatformQuotaRecord, usage platformQuotaUsageObservation) *UserPlatformQuotaCacheEntry {
	return &UserPlatformQuotaCacheEntry{
		QuotaRowID: rec.QuotaRowID, ControlGeneration: rec.ControlGeneration, SchemaVersion: UserPlatformQuotaCacheSchemaV1,
		DailyLimitUSD: rec.DailyLimitUSD, WeeklyLimitUSD: rec.WeeklyLimitUSD, MonthlyLimitUSD: rec.MonthlyLimitUSD,
		DailyUsageUSD: usage.daily, WeeklyUsageUSD: usage.weekly, MonthlyUsageUSD: usage.monthly,
		DailyWindowStart: &usage.dailyStart, WeeklyWindowStart: &usage.weeklyStart, MonthlyWindowStart: &usage.monthlyStart,
	}
}

func comparePlatformQuotaObservations(rec *UserPlatformQuotaRecord, db, cache platformQuotaUsageObservation, now time.Time) error {
	if rec.DailyLimitUSD != nil && math.Max(db.daily, cache.daily) >= *rec.DailyLimitUSD {
		return withWindowResetsMetadata(ErrUserPlatformDailyQuotaExhausted, nextDailyReset(now))
	}
	if rec.WeeklyLimitUSD != nil && math.Max(db.weekly, cache.weekly) >= *rec.WeeklyLimitUSD {
		return withWindowResetsMetadata(ErrUserPlatformWeeklyQuotaExhausted, nextWeeklyReset(now))
	}
	if rec.MonthlyLimitUSD != nil && math.Max(db.monthly, cache.monthly) >= *rec.MonthlyLimitUSD {
		start := cache.monthlyStart
		if db.monthly > cache.monthly {
			start = db.monthlyStart
		}
		return withWindowResetsMetadata(ErrUserPlatformMonthlyQuotaExhausted, nextMonthlyResetFrom(&start, now))
	}
	return nil
}

func (s *BillingCacheService) platformQuotaCacheTTL() time.Duration {
	if s.cfg != nil && s.cfg.Billing.UserPlatformQuotaCacheTTLSeconds > 0 {
		return time.Duration(s.cfg.Billing.UserPlatformQuotaCacheTTLSeconds) * time.Second
	}
	return 24 * time.Hour
}

// HasUserPlatformQuotaLimit never lets a stale no-row sentinel suppress legacy
// metering. Only a successful current DB read may prove that no limit exists.
func (s *BillingCacheService) HasUserPlatformQuotaLimit(ctx context.Context, userID int64, platform string) bool {
	if s != nil && s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		return false
	}
	if s == nil || s.userPlatformQuotaRepo == nil || ctx.Err() != nil {
		return true
	}
	rec, err := s.readPlatformQuotaRecord(ctx, userID, platform)
	if err != nil || ctx.Err() != nil {
		return true
	}
	if rec == nil {
		return false
	}
	if !validPlatformQuotaRecord(rec, userID, platform, time.Now()) {
		return true
	}
	return rec.HasAnyLimit()
}
