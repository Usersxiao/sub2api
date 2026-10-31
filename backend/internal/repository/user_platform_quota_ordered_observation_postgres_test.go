//go:build unit

package repository

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type orderedQuotaPGFixture struct {
	*settlementEventPGFixture
	authority service.UserPlatformQuotaWriterAuthority
	repo      UserPlatformQuotaRepository
	reader    service.UserPlatformQuotaOrderedObservationRepository
}

func migrateOrderedQuotaObservation(t *testing.T, f *settlementEventPGFixture) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", "264_user_platform_quota_ordered_observation.sql"))
	require.NoError(t, err)
	_, err = f.db.Exec(string(raw))
	require.NoError(t, err)
}

func orderedQuotaReader(t *testing.T, repo UserPlatformQuotaRepository) service.UserPlatformQuotaOrderedObservationRepository {
	t.Helper()
	reader, ok := repo.(service.UserPlatformQuotaOrderedObservationRepository)
	require.True(t, ok)
	return reader
}

func orderedQuotaPostgresFixture(t *testing.T) *orderedQuotaPGFixture {
	t.Helper()
	f, authority := quotaWriterAuthorityFixture(t)
	migrateOrderedQuotaObservation(t, f)
	repo := NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, authority)
	return &orderedQuotaPGFixture{f, authority, repo, orderedQuotaReader(t, repo)}
}

func (f *orderedQuotaPGFixture) seed(t *testing.T, platform string) *service.UserPlatformQuotaOrderedObservation {
	t.Helper()
	limit := 100.0
	require.NoError(t, f.repo.BulkInsertInitial(context.Background(), []UserPlatformQuotaRecord{{
		UserID: f.session.UserID, Platform: platform, DailyLimitUSD: &limit,
	}}))
	return f.read(t, platform)
}

func (f *orderedQuotaPGFixture) read(t *testing.T, platform string) *service.UserPlatformQuotaOrderedObservation {
	t.Helper()
	observation, err := f.reader.GetOrderedUserPlatformQuotaObservation(context.Background(), f.session.UserID, platform)
	require.NoError(t, err)
	require.NotNil(t, observation)
	return observation
}

// Fixture SQL carries the same explicit writer scope as earlier authority tests.
// This does not claim arbitrary owner SQL is a trusted production source.
func (f *orderedQuotaPGFixture) scopedSQL(t *testing.T, query string, args ...any) {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockUserPlatformQuotaWriterAuthority(context.Background(), tx, f.authority, quotaWriterActionAdminReset))
	_, err = tx.Exec(query, args...)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func orderedQuotaSQLState(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pq.Error
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, want, string(pgErr.Code))
}

func TestUserPlatformQuotaOrderedObservationPostgresMigrationPreservesExistingFacts(t *testing.T) {
	f, authority := quotaWriterAuthorityFixture(t)
	repo, rec := quotaWriterAuthorityZeroQuota(t, f, authority)
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockUserPlatformQuotaWriterAuthority(context.Background(), tx, authority, quotaWriterActionAdminReset))
	at := time.Date(2026, 10, 25, 8, 9, 10, 123456000, time.UTC)
	_, err = tx.Exec(`UPDATE user_platform_quotas SET daily_usage_usd=1.1234567890,
		weekly_usage_usd=2.2345678901,monthly_usage_usd=3.3456789012,
		daily_window_start=$2,weekly_window_start=$2,monthly_window_start=$2 WHERE id=$1`, rec.QuotaRowID, at)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	var before, resetBefore string
	require.NoError(t, f.db.QueryRow(`SELECT to_jsonb(q)::text FROM user_platform_quotas q WHERE id=$1`, rec.QuotaRowID).Scan(&before))
	require.NoError(t, f.db.QueryRow(`SELECT COALESCE(jsonb_agg(to_jsonb(s)),'[]'::jsonb)::text FROM user_platform_quota_client_resets s`).Scan(&resetBefore))
	authorityBefore := quotaWriterAuthorityDatabaseJSON(t, f.db)
	migrateOrderedQuotaObservation(t, f)
	var after, resetAfter string
	require.NoError(t, f.db.QueryRow(`SELECT (to_jsonb(q)-'observation_lineage'-'observation_revision')::text
		FROM user_platform_quotas q WHERE id=$1`, rec.QuotaRowID).Scan(&after))
	require.NoError(t, f.db.QueryRow(`SELECT COALESCE(jsonb_agg(to_jsonb(s)),'[]'::jsonb)::text FROM user_platform_quota_client_resets s`).Scan(&resetAfter))
	require.JSONEq(t, before, after)
	require.Equal(t, resetBefore, resetAfter)
	require.Equal(t, authorityBefore, quotaWriterAuthorityDatabaseJSON(t, f.db))
	observation, err := orderedQuotaReader(t, repo).GetOrderedUserPlatformQuotaObservation(context.Background(), rec.UserID, rec.Platform)
	require.NoError(t, err)
	require.NotNil(t, observation)
	require.Equal(t, int64(1), observation.Token.Revision, "migration starts observation ordering without certifying historical coverage")
	require.Equal(t, rec.ControlGeneration, observation.Token.ControlGeneration)
	require.NoError(t, service.ValidateUserPlatformQuotaObservationToken(observation.Token))
	require.Equal(t, "1.1234567890", observation.DailyUsageUSD)
	require.Equal(t, "2.2345678901", observation.WeeklyUsageUSD)
	require.Equal(t, "3.3456789012", observation.MonthlyUsageUSD)
	require.NotNil(t, observation.MonthlyWindowStart)
	require.True(t, at.Equal(*observation.MonthlyWindowStart))
}

func TestUserPlatformQuotaOrderedObservationPostgresFreshRowsConstraintsAndRestart(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	first := f.seed(t, service.PlatformOpenAI)
	second := f.seed(t, service.PlatformGemini)
	for _, observation := range []*service.UserPlatformQuotaOrderedObservation{first, second} {
		require.Equal(t, int64(1), observation.Token.Revision)
		require.Equal(t, f.authority.Mode, observation.Token.WriterMode)
		require.Equal(t, f.authority.Epoch, observation.Token.WriterEpoch)
		require.NoError(t, service.ValidateUserPlatformQuotaObservationToken(observation.Token))
	}
	require.NotEqual(t, first.Token.Lineage, second.Token.Lineage)
	require.NotEqual(t, first.Token.QuotaRowID, second.Token.QuotaRowID)
	restarted := orderedQuotaReader(t, NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, f.authority))
	after, err := restarted.GetOrderedUserPlatformQuotaObservation(nil, f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.Equal(t, first, after, "repository restart neither remints lineage nor advances revision")
	for _, tc := range []struct{ name, columns, values string }{
		{"zero-lineage", "observation_lineage", "'00000000-0000-0000-0000-000000000000'::uuid"},
		{"duplicate-lineage", "observation_lineage", "'" + first.Token.Lineage + "'::uuid"},
		{"zero-revision", "observation_revision", "0"},
		{"negative-revision", "observation_revision", "-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.db.Exec(`INSERT INTO user_platform_quotas(user_id,platform,`+tc.columns+`) VALUES($1,'anthropic',`+tc.values+`)`, f.session.UserID)
			require.Error(t, err)
		})
	}
	require.Equal(t, first, f.read(t, service.PlatformOpenAI))
}

func TestUserPlatformQuotaOrderedObservationPostgresSameGenerationCommitOrder(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockUserPlatformQuotaWriterAuthority(ctx, tx, f.authority, quotaWriterActionOrdinaryDelivery))
	var pid int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	var firstRevision int64
	require.NoError(t, tx.QueryRowContext(ctx, `UPDATE user_platform_quotas SET daily_usage_usd=daily_usage_usd+0.25
		WHERE id=$1 RETURNING observation_revision`, before.Token.QuotaRowID).Scan(&firstRevision))
	require.Equal(t, before.Token.Revision+1, firstRevision)
	done := make(chan error, 1)
	secondRevision := make(chan int64, 1)
	go func() {
		nextTx, err := f.db.BeginTx(ctx, nil)
		if err != nil {
			done <- err
			return
		}
		defer nextTx.Rollback()
		if err = lockUserPlatformQuotaWriterAuthority(ctx, nextTx, f.authority, quotaWriterActionOrdinaryDelivery); err != nil {
			done <- err
			return
		}
		var revision int64
		err = nextTx.QueryRowContext(ctx, `UPDATE user_platform_quotas SET daily_usage_usd=daily_usage_usd+0.25
			WHERE id=$1 RETURNING observation_revision`, before.Token.QuotaRowID).Scan(&revision)
		if err == nil {
			err = nextTx.Commit()
		}
		if err == nil {
			secondRevision <- revision
		}
		done <- err
	}()
	quotaSnapshotWaitForBlock(t, ctx, f.db, pid, done)
	require.NoError(t, tx.Commit())
	quotaSnapshotAwait(t, ctx, done)
	require.Equal(t, firstRevision+1, <-secondRevision)
	after := f.read(t, service.PlatformOpenAI)
	require.Equal(t, before.Token.ControlGeneration, after.Token.ControlGeneration)
	require.Equal(t, before.Token.Lineage, after.Token.Lineage)
	require.Equal(t, before.Token.Revision+2, after.Token.Revision)
	require.Equal(t, "0.5000000000", after.DailyUsageUSD)
	require.Equal(t, "0.0000000000", after.WeeklyUsageUSD)
}

func TestUserPlatformQuotaOrderedObservationPostgresConcurrentConsumersAndReplay(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		name := "same-source"
		if distinct {
			name = "different-sources"
		}
		t.Run(name, func(t *testing.T) {
			f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
			ordinaryQuotaDeliveryFixtureSQL(t, f, receipt, `UPDATE user_platform_quotas SET
				daily_window_start=$2,weekly_window_start=$3,monthly_window_start=$4 WHERE id=$1`,
				receipt.QuotaRowID, timezone.StartOfDay(receipt.CapturedAt), timezone.StartOfWeek(receipt.CapturedAt), receipt.CapturedAt.Add(-time.Hour))
			migrateOrderedQuotaObservation(t, f)
			reader := orderedQuotaReader(t, repo)
			before, err := reader.GetOrderedUserPlatformQuotaObservation(context.Background(), cmd.UserID, receipt.Intent.Platform)
			require.NoError(t, err)
			require.NotNil(t, before)
			const workers = 8
			commands := make([]*service.UsageBillingCommand, workers)
			receipts := make([]*service.UsageBillingOrdinaryPlatformReceipt, workers)
			for i := range workers {
				commands[i], receipts[i] = cmd, receipt
				if distinct && i > 0 {
					commands[i] = ordinaryPlatformQuotaCommand(t, f, fmt.Sprintf("ordered-delivery-%d", i), 0.25)
					commands[i].OrdinaryPlatformQuotaDeliveryMode = service.UsageBillingOrdinaryPlatformDeliveryDBV1
					applied, err := f.billing.Apply(context.Background(), commands[i])
					require.NoError(t, err)
					require.True(t, applied.Applied)
					receipts[i] = applied.OrdinaryPlatformQuotaReceipt
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			type outcome struct {
				result service.UsageBillingOrdinaryPlatformDeliveryResult
				err    error
			}
			results := make(chan outcome, workers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(ctx, commands[i], receipts[i])
					results <- outcome{result, err}
				}()
			}
			joined := make(chan struct{})
			go func() { wg.Wait(); close(joined) }()
			close(start)
		observationLoop:
			for {
				live, err := reader.GetOrderedUserPlatformQuotaObservation(ctx, cmd.UserID, receipt.Intent.Platform)
				require.NoError(t, err)
				require.NotNil(t, live)
				require.Equal(t, before.Token.Lineage, live.Token.Lineage)
				require.Equal(t, before.Token.ControlGeneration, live.Token.ControlGeneration)
				delta := live.Token.Revision - before.Token.Revision
				require.GreaterOrEqual(t, delta, int64(0))
				exact := fmt.Sprintf("%.10f", float64(delta)*0.25)
				require.Equal(t, exact, live.DailyUsageUSD, "token and concurrent consumer state share one MVCC image")
				require.Equal(t, exact, live.WeeklyUsageUSD)
				require.Equal(t, exact, live.MonthlyUsageUSD)
				select {
				case <-joined:
					break observationLoop
				default:
				}
			}
			close(results)
			fresh := 0
			for got := range results {
				require.NoError(t, got.err)
				require.Equal(t, service.QuotaIncrementApplied, got.result.Outcome.Status)
				if !got.result.Replayed {
					fresh++
				}
			}
			want := 1
			if distinct {
				want = workers
			}
			require.Equal(t, want, fresh)
			after, err := reader.GetOrderedUserPlatformQuotaObservation(ctx, cmd.UserID, receipt.Intent.Platform)
			require.NoError(t, err)
			require.NotNil(t, after)
			require.Equal(t, before.Token.Revision+int64(want), after.Token.Revision)
			require.Equal(t, before.Token.Lineage, after.Token.Lineage)
			require.Equal(t, before.Token.ControlGeneration, after.Token.ControlGeneration)
			exact := fmt.Sprintf("%.10f", float64(want)*0.25)
			require.Equal(t, exact, after.DailyUsageUSD)
			require.Equal(t, exact, after.WeeklyUsageUSD)
			require.Equal(t, exact, after.MonthlyUsageUSD)
			for i := range workers {
				replay, err := writer.ConsumeOrdinaryPlatformQuotaUsage(ctx, commands[i], receipts[i])
				require.NoError(t, err)
				require.True(t, replay.Replayed)
			}
			again, err := reader.GetOrderedUserPlatformQuotaObservation(ctx, cmd.UserID, receipt.Intent.Platform)
			require.NoError(t, err)
			require.Equal(t, after, again, "replaying permanent results has no new state or revision effect")
		})
	}
}

func TestUserPlatformQuotaOrderedObservationPostgresRollbackRestoresStateAndRevision(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	rowBefore := boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID)
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockUserPlatformQuotaWriterAuthority(context.Background(), tx, f.authority, quotaWriterActionOrdinaryDelivery))
	var revision int64
	require.NoError(t, tx.QueryRow(`UPDATE user_platform_quotas SET daily_usage_usd=7,weekly_usage_usd=8,
		monthly_usage_usd=9 WHERE id=$1 RETURNING observation_revision`, before.Token.QuotaRowID).Scan(&revision))
	require.Equal(t, before.Token.Revision+1, revision)
	require.NoError(t, tx.Rollback())
	require.Equal(t, rowBefore, boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID))
	require.Equal(t, before, f.read(t, service.PlatformOpenAI))
	f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_usage_usd=1 WHERE id=$1`, before.Token.QuotaRowID)
	require.Equal(t, before.Token.Revision+1, f.read(t, service.PlatformOpenAI).Token.Revision, "rolled-back ordinal is not a committed gap")
}

func TestUserPlatformQuotaOrderedObservationPostgresTerminalFailureRollsBackRevision(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprintf("deferred-%t", deferred), func(t *testing.T) {
			f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
			migrateOrderedQuotaObservation(t, f)
			reader := orderedQuotaReader(t, repo)
			before, err := reader.GetOrderedUserPlatformQuotaObservation(context.Background(), cmd.UserID, receipt.Intent.Platform)
			require.NoError(t, err)
			require.NotNil(t, before)
			birthBefore := ordinaryQuotaDeliveryJSON(t, f, cmd)
			_, err = f.db.Exec(`CREATE FUNCTION ordered_observation_fixture_fail() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'fixture terminal failure'; END; $$`)
			require.NoError(t, err)
			trigger := `CREATE TRIGGER ordered_observation_fixture_fail BEFORE UPDATE ON ordinary_platform_quota_deliveries
				FOR EACH ROW EXECUTE FUNCTION ordered_observation_fixture_fail()`
			if deferred {
				trigger = `CREATE CONSTRAINT TRIGGER ordered_observation_fixture_fail AFTER UPDATE ON ordinary_platform_quota_deliveries
					DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ordered_observation_fixture_fail()`
			}
			_, err = f.db.Exec(trigger)
			require.NoError(t, err)
			writer := ordinaryQuotaDeliveryWriter(t, repo)
			result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
			require.Error(t, err)
			require.Equal(t, service.UsageBillingOrdinaryPlatformDeliveryResult{}, result)
			after, err := reader.GetOrderedUserPlatformQuotaObservation(context.Background(), cmd.UserID, receipt.Intent.Platform)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, birthBefore, ordinaryQuotaDeliveryJSON(t, f, cmd))
			_, err = f.db.Exec(`DROP TRIGGER ordered_observation_fixture_fail ON ordinary_platform_quota_deliveries`)
			require.NoError(t, err)
			_, err = writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
			require.NoError(t, err)
			completed, err := reader.GetOrderedUserPlatformQuotaObservation(context.Background(), cmd.UserID, receipt.Intent.Platform)
			require.NoError(t, err)
			require.Equal(t, before.Token.Revision+1, completed.Token.Revision)
		})
	}
}

func TestUserPlatformQuotaOrderedObservationPostgresRevisionOverflowRollsBack(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	// Explicit owner-only fixture injection reaches the otherwise impractical
	// BIGINT boundary. The trigger is re-enabled before every assertion.
	_, err := f.db.Exec(`ALTER TABLE user_platform_quotas DISABLE TRIGGER user_platform_quota_ordered_observation_before_update`)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET observation_revision=$2 WHERE id=$1`, before.Token.QuotaRowID, int64(math.MaxInt64-1))
	require.NoError(t, err)
	_, err = f.db.Exec(`ALTER TABLE user_platform_quotas ENABLE TRIGGER user_platform_quota_ordered_observation_before_update`)
	require.NoError(t, err)
	f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_usage_usd=1 WHERE id=$1`, before.Token.QuotaRowID)
	require.Equal(t, int64(math.MaxInt64), f.read(t, service.PlatformOpenAI).Token.Revision)
	rowBefore := boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID)
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockUserPlatformQuotaWriterAuthority(context.Background(), tx, f.authority, quotaWriterActionOrdinaryDelivery))
	_, err = tx.Exec(`UPDATE user_platform_quotas SET daily_usage_usd=2 WHERE id=$1`, before.Token.QuotaRowID)
	orderedQuotaSQLState(t, err, "22003")
	require.NoError(t, tx.Rollback())
	require.Equal(t, rowBefore, boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID))
	require.Error(t, f.repo.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "daily", time.Now().UTC()))
	require.Equal(t, rowBefore, boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID), "reset generation and usage roll back on revision exhaustion")
}

func TestUserPlatformQuotaOrderedObservationPostgresMetadataTamperingIsRejected(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	rowBefore := boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID)
	for _, assignment := range []string{
		"observation_lineage=gen_random_uuid()", "observation_revision=observation_revision+1",
		"observation_revision=0", "observation_lineage=NULL", "observation_revision=NULL",
		"observation_lineage='00000000-0000-0000-0000-000000000000'::uuid",
	} {
		t.Run(assignment, func(t *testing.T) {
			_, err := f.db.Exec(`UPDATE user_platform_quotas SET `+assignment+` WHERE id=$1`, before.Token.QuotaRowID)
			orderedQuotaSQLState(t, err, "23514")
			require.Equal(t, rowBefore, boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID))
		})
	}
	t.Run("metadata-and-control-change", func(t *testing.T) {
		_, err := f.db.Exec(`UPDATE user_platform_quotas SET daily_limit_usd=daily_limit_usd+1,
			observation_lineage=gen_random_uuid() WHERE id=$1`, before.Token.QuotaRowID)
		orderedQuotaSQLState(t, err, "23514")
		require.Equal(t, rowBefore, boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID))
	})
	t.Run("metadata-and-metering-change", func(t *testing.T) {
		tx, err := f.db.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		defer tx.Rollback()
		require.NoError(t, lockUserPlatformQuotaWriterAuthority(context.Background(), tx, f.authority, quotaWriterActionOrdinaryDelivery))
		_, err = tx.Exec(`UPDATE user_platform_quotas SET daily_usage_usd=daily_usage_usd+1,
			observation_revision=observation_revision+1 WHERE id=$1`, before.Token.QuotaRowID)
		orderedQuotaSQLState(t, err, "23514")
		require.NoError(t, tx.Rollback())
		require.Equal(t, rowBefore, boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID))
	})
}

func TestUserPlatformQuotaOrderedObservationPostgresNoopAndTimestampDoNotAdvance(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_usage_usd=daily_usage_usd,
		weekly_window_start=weekly_window_start,observation_lineage=observation_lineage,
		observation_revision=observation_revision WHERE id=$1`, before.Token.QuotaRowID)
	require.Equal(t, before, f.read(t, service.PlatformOpenAI))
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET updated_at=updated_at+INTERVAL '1 hour' WHERE id=$1`, before.Token.QuotaRowID)
	require.NoError(t, err)
	require.Equal(t, before, f.read(t, service.PlatformOpenAI))
	_, err = f.db.Exec(`UPDATE user_platform_quotas SET daily_limit_usd=daily_limit_usd WHERE id=$1`, before.Token.QuotaRowID)
	require.NoError(t, err)
	require.Equal(t, before, f.read(t, service.PlatformOpenAI))
}

func TestUserPlatformQuotaOrderedObservationPostgresControlsResetAndRecreate(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	initial := f.seed(t, service.PlatformOpenAI)
	limit := 25.0
	require.NoError(t, f.repo.UpsertForUser(context.Background(), f.session.UserID, []UserPlatformQuotaRecord{{
		UserID: f.session.UserID, Platform: service.PlatformOpenAI, DailyLimitUSD: &limit,
	}}))
	configured := f.read(t, service.PlatformOpenAI)
	require.Equal(t, initial.Token.Lineage, configured.Token.Lineage)
	require.Equal(t, initial.Token.Revision+1, configured.Token.Revision)
	require.Equal(t, initial.Token.ControlGeneration+1, configured.Token.ControlGeneration)
	start := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, f.repo.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "daily", start))
	reset := f.read(t, service.PlatformOpenAI)
	require.Equal(t, configured.Token.Lineage, reset.Token.Lineage)
	require.Greater(t, reset.Token.Revision, configured.Token.Revision)
	require.Equal(t, configured.Token.ControlGeneration+1, reset.Token.ControlGeneration)
	// Even an equal-value administrative reset advances control generation,
	// and the ordering trigger must run after that generation trigger.
	require.NoError(t, f.repo.ResetExpiredWindow(context.Background(), f.session.UserID, service.PlatformOpenAI, "daily", start))
	resetAgain := f.read(t, service.PlatformOpenAI)
	require.Greater(t, resetAgain.Token.Revision, reset.Token.Revision)
	require.Equal(t, reset.Token.ControlGeneration+1, resetAgain.Token.ControlGeneration)
	require.NoError(t, f.repo.UpsertForUser(context.Background(), f.session.UserID, nil))
	missing, err := f.reader.GetOrderedUserPlatformQuotaObservation(context.Background(), f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.Nil(t, missing)
	var deletedRevision int64
	var deletedLineage string
	require.NoError(t, f.db.QueryRow(`SELECT observation_revision,observation_lineage::text FROM user_platform_quotas WHERE id=$1`, initial.Token.QuotaRowID).Scan(&deletedRevision, &deletedLineage))
	require.Greater(t, deletedRevision, resetAgain.Token.Revision)
	require.Equal(t, initial.Token.Lineage, deletedLineage)
	recreated := f.seed(t, service.PlatformOpenAI)
	require.NotEqual(t, initial.Token.QuotaRowID, recreated.Token.QuotaRowID)
	require.NotEqual(t, initial.Token.Lineage, recreated.Token.Lineage)
	require.Equal(t, int64(1), recreated.Token.Revision)
}

func TestUserPlatformQuotaOrderedObservationPostgresScopeAndWindowTransitions(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	at := time.Date(2026, 10, 25, 9, 0, 0, 654321000, time.UTC)
	f.scopedSQL(t, `UPDATE user_platform_quotas SET daily_window_start=$2 WHERE id=$1`, before.Token.QuotaRowID, at)
	window := f.read(t, service.PlatformOpenAI)
	require.Equal(t, before.Token.Revision+1, window.Token.Revision)
	require.Equal(t, before.Token.ControlGeneration, window.Token.ControlGeneration)
	require.Equal(t, before.Token.Lineage, window.Token.Lineage)
	_, err := f.db.Exec(`UPDATE user_platform_quotas SET platform='gemini' WHERE id=$1`, before.Token.QuotaRowID)
	require.NoError(t, err)
	scoped := f.read(t, service.PlatformGemini)
	require.Equal(t, window.Token.Revision+1, scoped.Token.Revision)
	require.Equal(t, window.Token.ControlGeneration+1, scoped.Token.ControlGeneration)
	require.Equal(t, window.Token.Lineage, scoped.Token.Lineage)
	other := f.seed(t, service.PlatformAnthropic)
	require.Equal(t, int64(1), other.Token.Revision)
	require.NotEqual(t, scoped.Token.Lineage, other.Token.Lineage)
	require.Equal(t, scoped, f.read(t, service.PlatformGemini), "another row has an independent order")
}

func TestUserPlatformQuotaOrderedObservationPostgresExactStateAndAtomicRead(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	at := time.Date(2026, 10, 25, 9, 10, 11, 123456000, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, lockUserPlatformQuotaWriterAuthority(ctx, tx, f.authority, quotaWriterActionAdminReset))
	_, err = tx.ExecContext(ctx, `UPDATE user_platform_quotas SET
		daily_limit_usd=1234567890.1234567890,weekly_limit_usd=NULL,monthly_limit_usd=0,
		daily_usage_usd=1234567890.1234567890,weekly_usage_usd=0.0000000001,monthly_usage_usd=9876543210.9876543210,
		daily_window_start=$2,weekly_window_start=$3,monthly_window_start=$4 WHERE id=$1`,
		before.Token.QuotaRowID, at, at.Add(-time.Hour), at.Add(-24*time.Hour))
	require.NoError(t, err)
	// The private SELECT sees one committed MVCC image while this row's new
	// amount, windows, control generation and revision remain uncommitted.
	whilePending, err := f.reader.GetOrderedUserPlatformQuotaObservation(ctx, f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.Equal(t, before, whilePending)
	require.NoError(t, tx.Commit())
	after := f.read(t, service.PlatformOpenAI)
	require.Equal(t, before.Token.Revision+1, after.Token.Revision)
	require.Equal(t, before.Token.ControlGeneration+1, after.Token.ControlGeneration)
	require.Equal(t, before.Token.Lineage, after.Token.Lineage)
	require.NotNil(t, after.DailyLimitUSD)
	require.Equal(t, "1234567890.1234567890", *after.DailyLimitUSD)
	require.Nil(t, after.WeeklyLimitUSD)
	require.NotNil(t, after.MonthlyLimitUSD)
	require.Equal(t, "0.0000000000", *after.MonthlyLimitUSD)
	require.Equal(t, "1234567890.1234567890", after.DailyUsageUSD)
	require.Equal(t, "0.0000000001", after.WeeklyUsageUSD)
	require.Equal(t, "9876543210.9876543210", after.MonthlyUsageUSD)
	for i, start := range []*time.Time{after.DailyWindowStart, after.WeeklyWindowStart, after.MonthlyWindowStart} {
		require.NotNil(t, start)
		want := []time.Time{at, at.Add(-time.Hour), at.Add(-24 * time.Hour)}[i]
		require.True(t, want.Equal(*start), "exact window %d", i)
	}
	rowBefore := boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID)
	for range 2 {
		require.Equal(t, after, f.read(t, service.PlatformOpenAI))
	}
	require.Equal(t, rowBefore, boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID), "observation has no write effect")
}

func TestUserPlatformQuotaOrderedObservationPostgresAuthorityAndMissingRowBoundaries(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	rowBefore := boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID)
	for _, platform := range []string{service.PlatformOpenAI, service.PlatformGemini} {
		t.Run(platform, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				repo UserPlatformQuotaRepository
				want error
			}{
				{"legacy", NewUserPlatformQuotaRepository(f.client), service.ErrUserPlatformQuotaWriterUnavailable},
				{"zero-handle", NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, service.UserPlatformQuotaWriterAuthority{}), service.ErrUserPlatformQuotaWriterModeUnsupported},
				{"other-epoch", NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, service.UserPlatformQuotaWriterAuthority{Mode: f.authority.Mode, Epoch: f.authority.Epoch + 1}), service.ErrUserPlatformQuotaWriterMismatch},
				{"snapshot", NewUserPlatformQuotaRepositoryWithWriterAuthority(f.client, service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeSnapshot, Epoch: f.authority.Epoch}), service.ErrUserPlatformQuotaWriterModeUnsupported},
			} {
				t.Run(tc.name, func(t *testing.T) {
					observation, err := orderedQuotaReader(t, tc.repo).GetOrderedUserPlatformQuotaObservation(context.Background(), f.session.UserID, platform)
					require.ErrorIs(t, err, tc.want)
					require.Nil(t, observation)
				})
			}
		})
	}
	missing, err := f.reader.GetOrderedUserPlatformQuotaObservation(context.Background(), f.session.UserID, service.PlatformGemini)
	require.NoError(t, err)
	require.Nil(t, missing)
	_, err = f.db.Exec(`ALTER TABLE user_platform_quota_writer_authority DISABLE TRIGGER user_platform_quota_writer_authority_immutable`)
	require.NoError(t, err)
	_, err = f.db.Exec(`DELETE FROM user_platform_quota_writer_authority`)
	require.NoError(t, err)
	_, err = f.db.Exec(`ALTER TABLE user_platform_quota_writer_authority ENABLE TRIGGER user_platform_quota_writer_authority_immutable`)
	require.NoError(t, err)
	for _, platform := range []string{service.PlatformOpenAI, service.PlatformGemini} {
		observation, err := f.reader.GetOrderedUserPlatformQuotaObservation(context.Background(), f.session.UserID, platform)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterUnavailable)
		require.Nil(t, observation)
	}
	require.Equal(t, rowBefore, boundQuotaRecordJSON(t, f.db, before.Token.QuotaRowID), "authority read failures leave the physical quota state unchanged")
}

func TestUserPlatformQuotaOrderedObservationPostgresInvalidAmbientAndLegacyWritePorts(t *testing.T) {
	f := orderedQuotaPostgresFixture(t)
	before := f.seed(t, service.PlatformOpenAI)
	for _, tc := range []struct {
		userID   int64
		platform string
	}{
		{0, service.PlatformOpenAI}, {-1, service.PlatformOpenAI}, {f.session.UserID, ""}, {f.session.UserID, "all"},
	} {
		observation, err := f.reader.GetOrderedUserPlatformQuotaObservation(context.Background(), tc.userID, tc.platform)
		require.ErrorIs(t, err, service.ErrUserPlatformQuotaObservationInvalid)
		require.Nil(t, observation)
	}
	tx, err := f.client.Tx(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	ambient := dbent.NewTxContext(context.Background(), tx)
	observation, err := f.reader.GetOrderedUserPlatformQuotaObservation(ambient, f.session.UserID, service.PlatformOpenAI)
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaObservationInvalid)
	require.Nil(t, observation)
	require.NoError(t, tx.Rollback())
	row, err := f.repo.GetByUserPlatform(context.Background(), f.session.UserID, service.PlatformOpenAI)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.ErrorIs(t, f.repo.BatchSnapshotUsage(context.Background(), []UserPlatformQuotaSnapshot{quotaControlSnapshot(row, 7)}, time.Now()), service.ErrUserPlatformQuotaWriterModeUnsupported)
	require.ErrorIs(t, f.repo.IncrementUsageWithReset(context.Background(), row.UserID, row.Platform, 0.25, time.Now()), service.ErrUserPlatformQuotaWriterModeUnsupported)
	_, err = boundQuotaWriter(t, f.repo).IncrementBoundUsage(context.Background(), boundQuotaBinding(row, time.Now()))
	require.ErrorIs(t, err, service.ErrUserPlatformQuotaWriterModeUnsupported)
	require.Equal(t, before, f.read(t, service.PlatformOpenAI), "ordered observation grants no legacy, cache or snapshot writer rights")
}
