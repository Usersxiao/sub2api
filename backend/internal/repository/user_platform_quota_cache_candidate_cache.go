package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const userPlatformQuotaCacheCandidateMaxBytes = 8192
const userPlatformQuotaCacheCandidateTimeLayout = "2006-01-02T15:04:05.000000Z"

var errUserPlatformQuotaCacheCandidateChanged = errors.New("user platform quota cache candidate changed; reread required")

// This private wire schema is deliberately separate from public DTOs and the
// legacy quota hash. Incarnation belongs to creation, not to each replacement.
// No production initializer or baseline handoff is provided by this codec.
type userPlatformQuotaCacheCandidateEnvelope struct {
	SchemaVersion      int64   `json:"schema_version"`
	Incarnation        string  `json:"incarnation"`
	Namespace          string  `json:"namespace"`
	DomainWriterMode   string  `json:"domain_writer_mode"`
	DomainWriterEpoch  int64   `json:"domain_writer_epoch"`
	TokenWriterMode    string  `json:"token_writer_mode"`
	TokenWriterEpoch   int64   `json:"token_writer_epoch"`
	QuotaRowID         int64   `json:"quota_row_id"`
	ControlGeneration  int64   `json:"control_generation"`
	Lineage            string  `json:"lineage"`
	Revision           int64   `json:"revision"`
	UserID             int64   `json:"user_id"`
	Platform           string  `json:"platform"`
	DailyLimitUSD      *string `json:"daily_limit_usd"`
	WeeklyLimitUSD     *string `json:"weekly_limit_usd"`
	MonthlyLimitUSD    *string `json:"monthly_limit_usd"`
	DailyUsageUSD      string  `json:"daily_usage_usd"`
	WeeklyUsageUSD     string  `json:"weekly_usage_usd"`
	MonthlyUsageUSD    string  `json:"monthly_usage_usd"`
	DailyWindowStart   *string `json:"daily_window_start"`
	WeeklyWindowStart  *string `json:"weekly_window_start"`
	MonthlyWindowStart *string `json:"monthly_window_start"`
	Fingerprint        string  `json:"fingerprint"`
}

func invalidUserPlatformQuotaCacheCandidateEnvelope(reason string) error {
	return fmt.Errorf("%w: %s", service.ErrUserPlatformQuotaCacheCandidateInvalid, reason)
}

func validUserPlatformQuotaCacheCandidateIncarnation(value string) bool {
	if len(value) != 32 {
		return false
	}
	nonzero := false
	for _, ch := range value {
		if !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f') {
			return false
		}
		if ch != '0' {
			nonzero = true
		}
	}
	return nonzero
}

func userPlatformQuotaCacheCandidateKey(cacheDomain service.UserPlatformQuotaCacheDomain, userID int64, platform string) (string, error) {
	if service.ValidateUserPlatformQuotaCacheDomain(cacheDomain) != nil || userID <= 0 || !domain.IsConcretePlatform(platform) {
		return "", invalidUserPlatformQuotaCacheCandidateEnvelope("invalid key scope")
	}
	return "billing:upq:v2:" + cacheDomain.Namespace + ":" + strconv.FormatInt(userID, 10) + ":" + platform, nil
}

func userPlatformQuotaCacheCandidateEnvelopeTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	encoded := value.UTC().Format(userPlatformQuotaCacheCandidateTimeLayout)
	return &encoded
}

func encodeUserPlatformQuotaCacheCandidateEnvelope(candidate *service.UserPlatformQuotaCacheCandidate, incarnation string) ([]byte, error) {
	if _, err := service.CompareUserPlatformQuotaCacheCandidates(candidate, candidate); err != nil {
		return nil, err
	}
	if !validUserPlatformQuotaCacheCandidateIncarnation(incarnation) {
		return nil, invalidUserPlatformQuotaCacheCandidateEnvelope("invalid creation incarnation")
	}
	cacheDomain, observation := candidate.Domain(), candidate.Observation()
	envelope := userPlatformQuotaCacheCandidateEnvelope{
		SchemaVersion: cacheDomain.SchemaVersion, Incarnation: incarnation, Namespace: cacheDomain.Namespace,
		DomainWriterMode: cacheDomain.WriterAuthority.Mode, DomainWriterEpoch: cacheDomain.WriterAuthority.Epoch,
		TokenWriterMode: observation.Token.WriterMode, TokenWriterEpoch: observation.Token.WriterEpoch,
		QuotaRowID: observation.Token.QuotaRowID, ControlGeneration: observation.Token.ControlGeneration,
		Lineage: observation.Token.Lineage, Revision: observation.Token.Revision,
		UserID: observation.UserID, Platform: observation.Platform,
		DailyLimitUSD: observation.DailyLimitUSD, WeeklyLimitUSD: observation.WeeklyLimitUSD, MonthlyLimitUSD: observation.MonthlyLimitUSD,
		DailyUsageUSD: observation.DailyUsageUSD, WeeklyUsageUSD: observation.WeeklyUsageUSD, MonthlyUsageUSD: observation.MonthlyUsageUSD,
		DailyWindowStart:   userPlatformQuotaCacheCandidateEnvelopeTime(observation.DailyWindowStart),
		WeeklyWindowStart:  userPlatformQuotaCacheCandidateEnvelopeTime(observation.WeeklyWindowStart),
		MonthlyWindowStart: userPlatformQuotaCacheCandidateEnvelopeTime(observation.MonthlyWindowStart),
		Fingerprint:        candidate.Fingerprint(),
	}
	encoded, err := json.Marshal(envelope)
	if err != nil || len(encoded) > userPlatformQuotaCacheCandidateMaxBytes {
		return nil, invalidUserPlatformQuotaCacheCandidateEnvelope("invalid encoded size or content")
	}
	return encoded, nil
}

func decodeUserPlatformQuotaCacheCandidateEnvelope(encoded []byte) (*service.UserPlatformQuotaCacheCandidate, string, error) {
	if len(encoded) == 0 || len(encoded) > userPlatformQuotaCacheCandidateMaxBytes {
		return nil, "", invalidUserPlatformQuotaCacheCandidateEnvelope("invalid encoded size")
	}
	var envelope userPlatformQuotaCacheCandidateEnvelope
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return nil, "", invalidUserPlatformQuotaCacheCandidateEnvelope("invalid JSON")
	}
	observation := &service.UserPlatformQuotaOrderedObservation{
		Token: service.UserPlatformQuotaObservationToken{
			WriterMode: envelope.TokenWriterMode, WriterEpoch: envelope.TokenWriterEpoch,
			QuotaRowID: envelope.QuotaRowID, ControlGeneration: envelope.ControlGeneration,
			Lineage: envelope.Lineage, Revision: envelope.Revision,
		},
		UserID: envelope.UserID, Platform: envelope.Platform,
		DailyLimitUSD: envelope.DailyLimitUSD, WeeklyLimitUSD: envelope.WeeklyLimitUSD, MonthlyLimitUSD: envelope.MonthlyLimitUSD,
		DailyUsageUSD: envelope.DailyUsageUSD, WeeklyUsageUSD: envelope.WeeklyUsageUSD, MonthlyUsageUSD: envelope.MonthlyUsageUSD,
	}
	starts := []**time.Time{&observation.DailyWindowStart, &observation.WeeklyWindowStart, &observation.MonthlyWindowStart}
	for i, text := range []*string{envelope.DailyWindowStart, envelope.WeeklyWindowStart, envelope.MonthlyWindowStart} {
		if text == nil {
			continue
		}
		start, err := time.Parse(userPlatformQuotaCacheCandidateTimeLayout, *text)
		if err != nil {
			return nil, "", invalidUserPlatformQuotaCacheCandidateEnvelope("invalid window anchor")
		}
		*starts[i] = &start
	}
	cacheDomain := service.UserPlatformQuotaCacheDomain{
		SchemaVersion: envelope.SchemaVersion, Namespace: envelope.Namespace,
		WriterAuthority: service.UserPlatformQuotaWriterAuthority{Mode: envelope.DomainWriterMode, Epoch: envelope.DomainWriterEpoch},
	}
	candidate, err := service.PrepareUserPlatformQuotaCacheCandidate(cacheDomain, observation)
	if err != nil {
		return nil, "", err
	}
	if candidate.Fingerprint() != envelope.Fingerprint {
		return nil, "", invalidUserPlatformQuotaCacheCandidateEnvelope("fingerprint mismatch")
	}
	canonical, err := encodeUserPlatformQuotaCacheCandidateEnvelope(candidate, envelope.Incarnation)
	if err != nil {
		return nil, "", err
	}
	// Exact re-encoding also rejects duplicate fields, missing/null scalars,
	// alternate number/string spellings, trailing JSON and noncanonical money,
	// timestamps, whitespace or field order. No ambiguous payload is repaired.
	if !bytes.Equal(encoded, canonical) {
		return nil, "", invalidUserPlatformQuotaCacheCandidateEnvelope("noncanonical payload")
	}
	return candidate, envelope.Incarnation, nil
}

// Revision and money never enter Lua arithmetic. The expected bytes include the
// creation incarnation, so a different valid recreation cannot satisfy this CAS.
// Only one SET mutates state; XX protects a key that disappeared before EVAL.
const replaceUserPlatformQuotaCacheCandidateScript = `
local kind = redis.call("TYPE", KEYS[1]).ok
if kind == "none" then return 2 end
if kind ~= "string" then return 4 end
local current = redis.call("GET", KEYS[1])
if current == false then return 2 end
if current ~= ARGV[1] then return 3 end
local replaced = redis.call("SET", KEYS[1], ARGV[2], "XX", "EX", ARGV[3])
if replaced == false then return 2 end
return 1
`

// replaceUserPlatformQuotaCacheCandidate only consumes an already present v2
// entry. It is not wired to production billing, admission or recovery. A miss
// always needs a separately verified handoff; there is no seed/NX/delete path.
func replaceUserPlatformQuotaCacheCandidate(ctx context.Context, rdb *redis.Client, candidate *service.UserPlatformQuotaCacheCandidate, ttl time.Duration) (service.UserPlatformQuotaObservationOrder, error) {
	if _, err := service.CompareUserPlatformQuotaCacheCandidates(candidate, candidate); err != nil {
		return "", err
	}
	if ttl < time.Second || ttl%time.Second != 0 {
		return "", invalidUserPlatformQuotaCacheCandidateEnvelope("TTL must be positive whole seconds")
	}
	if rdb == nil {
		return "", service.ErrUserPlatformQuotaWriterUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	observation := candidate.Observation()
	key, err := userPlatformQuotaCacheCandidateKey(candidate.Domain(), observation.UserID, observation.Platform)
	if err != nil {
		return "", err
	}
	// Reuse the repository command's NoRetry and Clone guarantees for both
	// operations, including clients whose ordinary transport retries are enabled.
	get := &quotaIncrementCommand{Cmd: redis.NewCmd(ctx, "get", key)}
	get.SetFirstKeyPos(1)
	if err := rdb.Process(ctx, get); err != nil {
		if errors.Is(err, redis.Nil) {
			return "", service.ErrUserPlatformQuotaCacheHandoffRequired
		}
		if strings.HasPrefix(err.Error(), "WRONGTYPE") {
			return "", invalidUserPlatformQuotaCacheCandidateEnvelope("invalid Redis key type")
		}
		return "", err
	}
	expected, err := get.Text()
	if err != nil {
		return "", err
	}
	current, incarnation, err := decodeUserPlatformQuotaCacheCandidateEnvelope([]byte(expected))
	if err != nil {
		return "", err
	}
	order, err := service.CompareUserPlatformQuotaCacheCandidates(candidate, current)
	if err != nil {
		return "", err
	}
	if order != service.QuotaObservationNewer {
		return order, nil
	}
	encoded, err := encodeUserPlatformQuotaCacheCandidateEnvelope(candidate, incarnation)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	command := &quotaIncrementCommand{Cmd: redis.NewCmd(ctx, "eval", replaceUserPlatformQuotaCacheCandidateScript, 1, key,
		expected, string(encoded), strconv.FormatInt(int64(ttl/time.Second), 10))}
	command.SetFirstKeyPos(3)
	if err := rdb.Process(ctx, command); err != nil {
		return "", err
	}
	result, err := command.Int64()
	if err != nil {
		return "", err
	}
	switch result {
	case 1:
		return service.QuotaObservationNewer, nil
	case 2:
		return "", service.ErrUserPlatformQuotaCacheHandoffRequired
	case 3:
		return "", errUserPlatformQuotaCacheCandidateChanged
	case 4:
		return "", invalidUserPlatformQuotaCacheCandidateEnvelope("invalid Redis key type")
	default:
		return "", invalidUserPlatformQuotaCacheCandidateEnvelope("unexpected replacement result")
	}
}
