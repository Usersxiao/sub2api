//go:build unit

package service

import "testing"

func TestFlusher_DisabledStopLeavesDirtySnapshotsUntouched(t *testing.T) {
	cache := &mockQuotaDirtyCache{
		popSequence: [][]UserPlatformQuotaKey{{{UserID: 1, Platform: PlatformOpenAI}}},
		getEntries:  []*UserPlatformQuotaCacheEntry{makeEntry(1, 2, 3)},
	}
	writer := &mockQuotaSnapshotWriter{}
	f := &UserPlatformQuotaUsageFlusher{cache: cache, quotaRepo: writer, enabled: false}
	// A disabled instance has no scheduled timer. Stop must neither require one
	// nor consume residual dirty state and apply an absolute database snapshot.
	f.Start()
	f.Stop()
	f.Stop()
	if cache.popCallIdx != 0 || writer.writeCalls != 0 || len(cache.readdCalled) != 0 {
		t.Fatalf("disabled flusher changed state: pop=%d write=%d readd=%d", cache.popCallIdx, writer.writeCalls, len(cache.readdCalled))
	}
}
