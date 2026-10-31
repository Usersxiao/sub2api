//go:build unit

package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type fakeRepoForAdapter struct {
	upsertCalledWith   []UserPlatformQuotaRecord
	upsertCalledUserID int64
	upsertErr          error
	resetCalledWith    [4]any // userID, platform, window, newStart
	resetErr           error
	record             *UserPlatformQuotaRecord
	records            []UserPlatformQuotaRecord
	snapshots          []UserPlatformQuotaSnapshot
}

func (f *fakeRepoForAdapter) BulkInsertInitial(_ context.Context, _ []UserPlatformQuotaRecord) error {
	return nil
}
func (f *fakeRepoForAdapter) GetByUserPlatform(_ context.Context, _ int64, _ string) (*UserPlatformQuotaRecord, error) {
	return f.record, nil
}
func (f *fakeRepoForAdapter) ListByUser(_ context.Context, _ int64) ([]UserPlatformQuotaRecord, error) {
	return f.records, nil
}
func (f *fakeRepoForAdapter) IncrementUsageWithReset(_ context.Context, _ int64, _ string, _ float64, _ time.Time) error {
	return nil
}
func (f *fakeRepoForAdapter) ResetExpiredWindow(_ context.Context, userID int64, platform string, window string, newStart time.Time) error {
	f.resetCalledWith = [4]any{userID, platform, window, newStart}
	return f.resetErr
}
func (f *fakeRepoForAdapter) UpsertForUser(_ context.Context, userID int64, records []UserPlatformQuotaRecord) error {
	f.upsertCalledUserID = userID
	f.upsertCalledWith = records
	return f.upsertErr
}
func (f *fakeRepoForAdapter) BatchSnapshotUsage(_ context.Context, snapshots []UserPlatformQuotaSnapshot, _ time.Time) error {
	f.snapshots = snapshots
	return nil
}

func TestUserPlatformQuotaAdapterPreservesPrivateControlTokens(t *testing.T) {
	rec := UserPlatformQuotaRecord{QuotaRowID: 91, ControlGeneration: 7, UserID: 42, Platform: "openai", DailyUsageUSD: 3}
	fake := &fakeRepoForAdapter{record: &rec, records: []UserPlatformQuotaRecord{rec}}
	adapter := NewUserPlatformQuotaServiceAdapter(fake)
	got, err := adapter.GetByUserPlatform(context.Background(), 42, "openai")
	require.NoError(t, err)
	require.Equal(t, int64(91), got.QuotaRowID)
	require.Equal(t, int64(7), got.ControlGeneration)
	list, err := adapter.ListByUser(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, []service.UserPlatformQuotaRecord{*got}, list)
	start := time.Now()
	snapshot := service.UserPlatformQuotaSnapshot{QuotaRowID: 91, ControlGeneration: 7, UserID: 42, Platform: "openai", DailyUsageUSD: 3,
		DailyWindowStart: start, WeeklyWindowStart: start, MonthlyWindowStart: start}
	require.NoError(t, adapter.BatchSnapshotUsage(context.Background(), []service.UserPlatformQuotaSnapshot{snapshot}, start))
	require.Equal(t, []UserPlatformQuotaSnapshot{{QuotaRowID: 91, ControlGeneration: 7, UserID: 42, Platform: "openai", DailyUsageUSD: 3,
		DailyWindowStart: start, WeeklyWindowStart: start, MonthlyWindowStart: start}}, fake.snapshots)
	require.Equal(t, rec, toRepoRecords([]service.UserPlatformQuotaRecord{*got})[0])
	for _, value := range []any{rec, *got, snapshot, fake.snapshots[0]} {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.False(t, strings.Contains(string(raw), "QuotaRowID"))
		require.False(t, strings.Contains(string(raw), "ControlGeneration"))
	}
}

func TestGenericAdapter_UpsertForUser_ForwardsRecords(t *testing.T) {
	fake := &fakeRepoForAdapter{}
	adapter := NewUserPlatformQuotaServiceAdapter(fake)

	err := adapter.UpsertForUser(context.Background(), 42, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if fake.upsertCalledUserID != 42 {
		t.Errorf("forwarded userID = %d, want 42", fake.upsertCalledUserID)
	}
}

func TestGenericAdapter_UpsertForUser_PropagatesError(t *testing.T) {
	wantErr := errors.New("boom")
	fake := &fakeRepoForAdapter{upsertErr: wantErr}
	adapter := NewUserPlatformQuotaServiceAdapter(fake)

	err := adapter.UpsertForUser(context.Background(), 1, nil)
	if !errors.Is(err, wantErr) {
		t.Errorf("expected %v, got %v", wantErr, err)
	}
}

func TestGenericAdapter_ResetExpiredWindow_ForwardsAllParams(t *testing.T) {
	fake := &fakeRepoForAdapter{}
	adapter := NewUserPlatformQuotaServiceAdapter(fake)

	now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
	if err := adapter.ResetExpiredWindow(context.Background(), 7, "openai", "weekly", now); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if fake.resetCalledWith[0].(int64) != 7 ||
		fake.resetCalledWith[1].(string) != "openai" ||
		fake.resetCalledWith[2].(string) != "weekly" ||
		!fake.resetCalledWith[3].(time.Time).Equal(now) {
		t.Errorf("forwarded params mismatch: %+v", fake.resetCalledWith)
	}
}

func TestGenericAdapter_ResetExpiredWindow_PropagatesError(t *testing.T) {
	wantErr := errors.New("not found")
	fake := &fakeRepoForAdapter{resetErr: wantErr}
	adapter := NewUserPlatformQuotaServiceAdapter(fake)

	err := adapter.ResetExpiredWindow(context.Background(), 1, "a", "daily", time.Now())
	if !errors.Is(err, wantErr) {
		t.Errorf("expected %v, got %v", wantErr, err)
	}
}
