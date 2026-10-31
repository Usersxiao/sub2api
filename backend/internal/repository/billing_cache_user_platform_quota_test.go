//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func newMiniRedisCache(t *testing.T) (*billingCache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return &billingCache{rdb: rdb}, mr
}

func TestUserPlatformQuotaCache_GetMissReturnsNotFound(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	entry, ok, err := c.GetUserPlatformQuotaCache(context.Background(), 1, "anthropic")
	if err != nil {
		t.Fatal(err)
	}
	if ok || entry != nil {
		t.Errorf("expected miss, got ok=%v entry=%v", ok, entry)
	}
}

func TestUserPlatformQuotaCache_SetThenGet(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx := context.Background()
	dailyLimit := 20.0
	ts := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	in := &service.UserPlatformQuotaCacheEntry{
		QuotaRowID:        9007199254740993,
		ControlGeneration: 9223372036854775807,
		DailyUsageUSD:     1.5,
		WeeklyUsageUSD:    3.0,
		MonthlyUsageUSD:   10.0,
		Version:           7,
		SchemaVersion:     service.UserPlatformQuotaCacheSchemaV1,
		DailyLimitUSD:     &dailyLimit,
		DailyWindowStart:  &ts,
	}
	if err := c.SetUserPlatformQuotaCache(ctx, 1, "openai", in, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.GetUserPlatformQuotaCache(ctx, 1, "openai")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.DailyUsageUSD != 1.5 || got.WeeklyUsageUSD != 3.0 || got.MonthlyUsageUSD != 10.0 || got.Version != 7 {
		t.Errorf("got = %+v, want %+v", got, in)
	}
	if got.SchemaVersion != service.UserPlatformQuotaCacheSchemaV1 {
		t.Errorf("SchemaVersion = %d, want %d", got.SchemaVersion, service.UserPlatformQuotaCacheSchemaV1)
	}
	if got.QuotaRowID != in.QuotaRowID || got.ControlGeneration != in.ControlGeneration {
		t.Errorf("flush token = (%d,%d), want (%d,%d)", got.QuotaRowID, got.ControlGeneration, in.QuotaRowID, in.ControlGeneration)
	}
	if got.DailyLimitUSD == nil || *got.DailyLimitUSD != dailyLimit {
		t.Errorf("DailyLimitUSD = %v, want %v", got.DailyLimitUSD, dailyLimit)
	}
	if got.DailyWindowStart == nil || !got.DailyWindowStart.Equal(ts) {
		t.Errorf("DailyWindowStart = %v, want %v", got.DailyWindowStart, ts)
	}
}

func TestUserPlatformQuotaCache_NilLimitSetThenGet(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx := context.Background()
	in := &service.UserPlatformQuotaCacheEntry{
		DailyUsageUSD: 1.0,
		SchemaVersion: service.UserPlatformQuotaCacheSchemaV1,
		// DailyLimitUSD nil → 无限额
	}
	if err := c.SetUserPlatformQuotaCache(ctx, 1, "openai", in, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.GetUserPlatformQuotaCache(ctx, 1, "openai")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.DailyLimitUSD != nil {
		t.Errorf("DailyLimitUSD should be nil for unlimited, got %v", got.DailyLimitUSD)
	}
}

func TestUserPlatformQuotaCache_IncrMissIsNoop(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	if _, err := c.IncrUserPlatformQuotaUsageCache(context.Background(), 1, "openai", 0.5, time.Minute, false); err != nil {
		t.Fatal(err)
	}
	_, ok, _ := c.GetUserPlatformQuotaCache(context.Background(), 1, "openai")
	if ok {
		t.Error("expected key absent after no-op incr")
	}
}

func TestUserPlatformQuotaCache_IncrHitAccumulates(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx := context.Background()
	// Complete V1 identity and windows are required for an applied increment.
	entry := quotaCacheCASEntry()
	entry.QuotaRowID, entry.ControlGeneration, entry.Version = 42, 3, 1
	entry.DailyUsageUSD, entry.WeeklyUsageUSD, entry.MonthlyUsageUSD = 0, 0, 0
	_ = c.SetUserPlatformQuotaCache(ctx, 1, "openai", entry, time.Minute)
	if _, err := c.IncrUserPlatformQuotaUsageCache(ctx, 1, "openai", 0.5, time.Minute, false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.IncrUserPlatformQuotaUsageCache(ctx, 1, "openai", 0.25, time.Minute, false); err != nil {
		t.Fatal(err)
	}
	got, _, _ := c.GetUserPlatformQuotaCache(ctx, 1, "openai")
	if got.DailyUsageUSD != 0.75 || got.WeeklyUsageUSD != 0.75 || got.MonthlyUsageUSD != 0.75 {
		t.Errorf("got %+v, want daily/weekly/monthly=0.75", got)
	}
	if got.Version != 3 {
		t.Errorf("version = %d, want 3 (initial 1 + 2 incr)", got.Version)
	}
	if got.QuotaRowID != 42 || got.ControlGeneration != 3 {
		t.Errorf("usage increments changed the database flush token: %+v", got)
	}
}

func TestUserPlatformQuotaCache_Delete(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx := context.Background()
	_ = c.SetUserPlatformQuotaCache(ctx, 1, "openai", &service.UserPlatformQuotaCacheEntry{Version: 1}, time.Minute)
	if err := c.DeleteUserPlatformQuotaCache(ctx, 1, "openai"); err != nil {
		t.Fatal(err)
	}
	_, ok, _ := c.GetUserPlatformQuotaCache(ctx, 1, "openai")
	if ok {
		t.Error("expected miss after delete")
	}
}

func TestUserPlatformQuotaCache_BatchGetPreservesFlushTokensAndOrder(t *testing.T) {
	c, _ := newMiniRedisCache(t)
	ctx := context.Background()
	inputs := []*service.UserPlatformQuotaCacheEntry{
		{QuotaRowID: 9007199254740993, ControlGeneration: 9223372036854775807, SchemaVersion: service.UserPlatformQuotaCacheSchemaV1, DailyUsageUSD: 1.25},
		{QuotaRowID: 41, ControlGeneration: 7, SchemaVersion: service.UserPlatformQuotaCacheSchemaV1, DailyUsageUSD: 2.5},
	}
	for i, in := range inputs {
		if err := c.SetUserPlatformQuotaCache(ctx, int64(i+1), "gemini", in, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	keys := []service.UserPlatformQuotaKey{{UserID: 2, Platform: "gemini"}, {UserID: 3, Platform: "gemini"}, {UserID: 1, Platform: "gemini"}}
	got, err := c.BatchGetUserPlatformQuotaCache(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) || got[0] == nil || got[1] != nil || got[2] == nil {
		t.Fatalf("unexpected batch alignment: %+v", got)
	}
	for i, inputIndex := range map[int]int{0: 1, 2: 0} {
		in := inputs[inputIndex]
		if got[i].QuotaRowID != in.QuotaRowID || got[i].ControlGeneration != in.ControlGeneration || got[i].DailyUsageUSD != in.DailyUsageUSD {
			t.Errorf("batch[%d] = %+v, want %+v", i, got[i], in)
		}
	}
}

func TestUserPlatformQuotaCache_LegacyMissingTokenReadableMalformedTokenFails(t *testing.T) {
	for _, tc := range []struct {
		name       string
		row        string
		generation string
	}{
		{name: "legacy-v1-missing-both"},
		{name: "missing-row", generation: "3"},
		{name: "missing-generation", row: "42"},
		{name: "zero-row", row: "0", generation: "3"},
		{name: "zero-generation", row: "42", generation: "0"},
		{name: "negative-row", row: "-42", generation: "3"},
		{name: "negative-generation", row: "42", generation: "-3"},
		{name: "invalid-row", row: "bad", generation: "3"},
		{name: "invalid-generation", row: "42", generation: "3.5"},
		{name: "overflow-row", row: "9223372036854775808", generation: "3"},
		{name: "overflow-generation", row: "42", generation: "9223372036854775808"},
		{name: "underflow-row", row: "-9223372036854775809", generation: "3"},
		{name: "underflow-generation", row: "42", generation: "-9223372036854775809"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newMiniRedisCache(t)
			ctx := context.Background()
			fields := map[string]any{"schema_version": service.UserPlatformQuotaCacheSchemaV1, "version": 99, "daily_usage": "1.25", "weekly_usage": "0", "monthly_usage": "0", "daily_limit": "20", "weekly_limit": "", "monthly_limit": "", "daily_window_start": "", "weekly_window_start": "", "monthly_window_start": ""}
			if tc.row != "" {
				fields["quota_row_id"] = tc.row
			}
			if tc.generation != "" {
				fields["control_generation"] = tc.generation
			}
			if err := c.rdb.HSet(ctx, userPlatformQuotaCacheKey(1, "gemini"), fields).Err(); err != nil {
				t.Fatal(err)
			}
			entry, ok, err := c.GetUserPlatformQuotaCache(ctx, 1, "gemini")
			if tc.name != "legacy-v1-missing-both" {
				if err == nil || ok || entry != nil {
					t.Fatalf("malformed token admitted: ok=%v entry=%+v err=%v", ok, entry, err)
				}
				batch, batchErr := c.BatchGetUserPlatformQuotaCache(ctx, []service.UserPlatformQuotaKey{{UserID: 1, Platform: "gemini"}})
				if batchErr == nil || batch != nil {
					t.Fatalf("malformed batch admitted: %+v err=%v", batch, batchErr)
				}
				return
			}
			if err != nil || !ok || entry == nil {
				t.Fatalf("legacy get: ok=%v entry=%+v err=%v", ok, entry, err)
			}
			batch, err := c.BatchGetUserPlatformQuotaCache(ctx, []service.UserPlatformQuotaKey{{UserID: 1, Platform: "gemini"}})
			if err != nil || len(batch) != 1 || batch[0] == nil {
				t.Fatalf("legacy batch: entries=%+v err=%v", batch, err)
			}
			for _, got := range []*service.UserPlatformQuotaCacheEntry{entry, batch[0]} {
				if got.QuotaRowID != 0 || got.ControlGeneration != 0 {
					t.Errorf("invalid pair retained a database flush identity: %+v", got)
				}
				if got.SchemaVersion != service.UserPlatformQuotaCacheSchemaV1 || got.Version != 99 || got.DailyUsageUSD != 1.25 || got.DailyLimitUSD == nil || *got.DailyLimitUSD != 20 {
					t.Errorf("invalid token changed legacy read fields: %+v", got)
				}
			}
		})
	}
}

func TestUserPlatformQuotaCache_SetUntaggedClearsIdentityInvalidInputPreservesState(t *testing.T) {
	for _, token := range []struct {
		row        int64
		generation int64
	}{
		{row: 0, generation: 0},
		{row: 42, generation: 0},
		{row: 0, generation: 3},
		{row: -42, generation: 3},
		{row: 42, generation: -3},
	} {
		c, _ := newMiniRedisCache(t)
		ctx := context.Background()
		valid := &service.UserPlatformQuotaCacheEntry{QuotaRowID: 42, ControlGeneration: 3, SchemaVersion: service.UserPlatformQuotaCacheSchemaV1}
		if err := c.SetUserPlatformQuotaCache(ctx, 1, "gemini", valid, time.Minute); err != nil {
			t.Fatal(err)
		}
		untagged := &service.UserPlatformQuotaCacheEntry{QuotaRowID: token.row, ControlGeneration: token.generation, SchemaVersion: service.UserPlatformQuotaCacheSchemaV1, DailyUsageUSD: 1.25}
		setErr := c.SetUserPlatformQuotaCache(ctx, 1, "gemini", untagged, time.Minute)
		if token.row != 0 || token.generation != 0 {
			if setErr == nil {
				t.Fatal("invalid token SET succeeded")
			}
			got, ok, err := c.GetUserPlatformQuotaCache(ctx, 1, "gemini")
			if err != nil || !ok || got.QuotaRowID != 42 || got.ControlGeneration != 3 {
				t.Fatalf("invalid SET changed stored state: %+v %v", got, err)
			}
			continue
		}
		if setErr != nil {
			t.Fatal(setErr)
		}
		fields, err := c.rdb.HMGet(ctx, userPlatformQuotaCacheKey(1, "gemini"), "quota_row_id", "control_generation").Result()
		if err != nil || len(fields) != 2 || fields[0] != "0" || fields[1] != "0" {
			t.Fatalf("stale token fields were not overwritten with zero: fields=%v err=%v", fields, err)
		}
		got, ok, err := c.GetUserPlatformQuotaCache(ctx, 1, "gemini")
		if err != nil || !ok || got == nil || got.QuotaRowID != 0 || got.ControlGeneration != 0 || got.DailyUsageUSD != 1.25 {
			t.Fatalf("untagged read retained prior identity or lost usage: ok=%v entry=%+v err=%v", ok, got, err)
		}
	}
}
