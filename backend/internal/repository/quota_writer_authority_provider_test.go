//go:build unit

package repository

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestQuotaWriterAuthorityProviderRejectsSnapshotBeforeDatabase(t *testing.T) {
	cfg := &config.Config{}
	cfg.Database.UserPlatformQuotaFlusherEnabled = true
	// No database is available: mode selection must fail before initialization,
	// instead of silently selecting a DB mode or starting a snapshot worker.
	authority, err := ProvideUserPlatformQuotaWriterAuthority(nil, cfg)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterModeUnsupported)
	require.Equal(t, service.UserPlatformQuotaWriterAuthority{}, authority)
}

func TestQuotaWriterAuthorityProviderMissingDependenciesFailClosed(t *testing.T) {
	for _, cfg := range []*config.Config{nil, {}} {
		authority, err := ProvideUserPlatformQuotaWriterAuthority(nil, cfg)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
		require.Equal(t, service.UserPlatformQuotaWriterAuthority{}, authority)
	}
}
