package service

import "errors"

const UserPlatformQuotaCacheDomainSchemaV2 int64 = 2

var (
	ErrUserPlatformQuotaCacheDomainInvalid   = errors.New("invalid user platform quota cache domain")
	ErrUserPlatformQuotaCacheHandoffRequired = errors.New("user platform quota cache baseline handoff required")
)

// The persistent namespace is allocated by the database, independently of
// process lifetime and Redis contents. This identifies the future cache domain;
// it is not a certified baseline, a Redis entry schema or permission to publish.
type UserPlatformQuotaCacheDomain struct {
	SchemaVersion   int64                            `json:"-"`
	Namespace       string                           `json:"-"`
	WriterAuthority UserPlatformQuotaWriterAuthority `json:"-"`
}

func ValidateUserPlatformQuotaCacheDomain(domain UserPlatformQuotaCacheDomain) error {
	if domain.SchemaVersion != UserPlatformQuotaCacheDomainSchemaV2 ||
		!validUserPlatformQuotaObservationLineage(domain.Namespace) ||
		ValidateUserPlatformQuotaWriterAuthority(domain.WriterAuthority) != nil {
		return ErrUserPlatformQuotaCacheDomainInvalid
	}
	return nil
}
