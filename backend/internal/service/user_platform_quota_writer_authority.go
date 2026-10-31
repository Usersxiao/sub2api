package service

import (
	"context"
	"errors"
	"fmt"
)

const (
	UserPlatformQuotaWriterModeDB       = "db_increment_v1"
	UserPlatformQuotaWriterModeSnapshot = "cache_snapshot_v1"
)

var (
	ErrUserPlatformQuotaWriterUnavailable     = errors.New("user platform quota writer authority unavailable")
	ErrUserPlatformQuotaWriterModeUnsupported = errors.New("user platform quota writer mode unsupported")
	ErrUserPlatformQuotaWriterMismatch        = errors.New("user platform quota writer authority mismatch")
)

// Authority identifies the persisted DB writer domain. It neither certifies a
// quota usage baseline nor grants a mode transition or a cache snapshot writer.
type UserPlatformQuotaWriterAuthority struct {
	Mode  string `json:"-"`
	Epoch int64  `json:"-"`
}

func ValidateUserPlatformQuotaWriterAuthority(authority UserPlatformQuotaWriterAuthority) error {
	if authority.Mode != UserPlatformQuotaWriterModeDB {
		return fmt.Errorf("%w: %q", ErrUserPlatformQuotaWriterModeUnsupported, authority.Mode)
	}
	if authority.Epoch <= 0 {
		return fmt.Errorf("%w: nonpositive epoch", ErrUserPlatformQuotaWriterUnavailable)
	}
	return nil
}

// Initialization observes or creates the persisted mode. Existing authority
// never changes merely because a process starts with different configuration.
type UserPlatformQuotaWriterAuthorityRepository interface {
	InitializeUserPlatformQuotaWriterAuthority(context.Context, string) (UserPlatformQuotaWriterAuthority, error)
}
