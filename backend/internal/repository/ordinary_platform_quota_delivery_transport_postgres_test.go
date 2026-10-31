//go:build unit

package repository

import (
	"context"
	"database/sql"
	"encoding/binary"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// Drop the actual PostgreSQL COMMIT CommandComplete packet before lib/pq can
// observe it. Seeing this server packet proves the transaction committed; this
// is distinct from killing a connection while COMMIT is merely in flight.
type ordinaryDeliveryLostCommitDialer struct {
	armed   atomic.Bool
	dropped atomic.Bool
}

func (d *ordinaryDeliveryLostCommitDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *ordinaryDeliveryLostCommitDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d *ordinaryDeliveryLostCommitDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &ordinaryDeliveryLostCommitConn{Conn: conn, fault: d}, nil
}

type ordinaryDeliveryLostCommitConn struct {
	net.Conn
	fault  *ordinaryDeliveryLostCommitDialer
	commit bool
	buffer []byte
}

func (c *ordinaryDeliveryLostCommitConn) Write(p []byte) (int, error) {
	// lib/pq uses the simple-query protocol for Commit. Only this transaction's
	// final COMMIT is targeted; BEGIN, statements and fixture setup pass through.
	if c.fault.armed.Load() && len(p) >= 6 && p[0] == 'Q' &&
		int(binary.BigEndian.Uint32(p[1:5])) == len(p)-1 &&
		strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(string(p[5:]), "\x00")), "COMMIT") {
		c.commit = true
	}
	return c.Conn.Write(p)
}

func (c *ordinaryDeliveryLostCommitConn) Read(p []byte) (int, error) {
	if !c.commit {
		return c.Conn.Read(p)
	}
	if len(c.buffer) == 0 {
		header := make([]byte, 5)
		if _, err := io.ReadFull(c.Conn, header); err != nil {
			return 0, err
		}
		size := int(binary.BigEndian.Uint32(header[1:5]))
		if size < 4 || size > 1<<20 {
			return 0, io.ErrUnexpectedEOF
		}
		payload := make([]byte, size-4)
		if _, err := io.ReadFull(c.Conn, payload); err != nil {
			return 0, err
		}
		if header[0] == 'C' && string(payload) == "COMMIT\x00" && c.fault.armed.CompareAndSwap(true, false) {
			c.fault.dropped.Store(true)
			_ = c.Conn.Close()
			return 0, io.ErrUnexpectedEOF
		}
		c.buffer = append(header, payload...)
	}
	n := copy(p, c.buffer)
	c.buffer = c.buffer[n:]
	return n, nil
}

func TestOrdinaryPlatformQuotaDeliveryPostgresCommitReplyLostReturnsOriginalResult(t *testing.T) {
	f, repo, cmd, receipt := ordinaryPlatformQuotaDeliveryFixture(t)
	var schema string
	require.NoError(t, f.db.QueryRow(`SELECT current_schema()`).Scan(&schema))
	connector, err := pq.NewConnector(os.Getenv("AUTH_SESSION_TEST_DSN") + " search_path=" + schema)
	require.NoError(t, err)
	fault := &ordinaryDeliveryLostCommitDialer{}
	connector.Dialer(fault)
	db := sql.OpenDB(connector)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	fault.armed.Store(true)
	authority := service.UserPlatformQuotaWriterAuthority{Mode: service.UserPlatformQuotaWriterModeDB, Epoch: receipt.WriterEpoch}
	result, err := ordinaryQuotaDeliveryWriter(t, NewUserPlatformQuotaRepositoryWithWriterAuthority(client, authority)).ConsumeOrdinaryPlatformQuotaUsage(ctx, cmd, receipt)
	require.Error(t, err, "caller must treat an unacknowledged commit as unknown")
	require.True(t, fault.dropped.Load(), "the real server acknowledged COMMIT before the transport discarded its reply")
	require.Equal(t, service.UsageBillingOrdinaryPlatformDeliveryResult{}, result, "no uncommitted-looking success result leaks")
	writer := ordinaryQuotaDeliveryWriter(t, repo)
	stored, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(ctx, cmd, receipt)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, service.OrdinaryPlatformDeliveryCompleted, stored.State)
	require.Equal(t, service.QuotaIncrementApplied, stored.Outcome.Status)
	require.Equal(t, service.QuotaWindowAll, stored.Outcome.AppliedWindows)
	require.NoError(t, service.ValidateUsageBillingOrdinaryPlatformDeliveryResult(cmd, receipt, *stored, false))
	quotaBefore := boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID)
	replayed, err := writer.ConsumeOrdinaryPlatformQuotaUsage(ctx, cmd, receipt)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, stored.State, replayed.State)
	require.Equal(t, stored.SourceFingerprint, replayed.SourceFingerprint)
	require.Equal(t, stored.Outcome, replayed.Outcome)
	require.True(t, stored.CreatedAt.Equal(replayed.CreatedAt))
	require.True(t, stored.CompletedAt.Equal(*replayed.CompletedAt))
	require.Equal(t, quotaBefore, boundQuotaRecordJSON(t, f.db, receipt.QuotaRowID))
	var daily, weekly, monthly string
	require.NoError(t, f.db.QueryRow(`SELECT daily_usage_usd::text,weekly_usage_usd::text,monthly_usage_usd::text FROM user_platform_quotas WHERE id=$1`, receipt.QuotaRowID).Scan(&daily, &weekly, &monthly))
	require.Equal(t, "0.2500000000", daily)
	require.Equal(t, daily, weekly)
	require.Equal(t, daily, monthly)
	balance, used := ordinaryPlatformQuotaBalanceAndKey(t, f, cmd)
	require.Equal(t, "99.75000000", balance)
	require.Equal(t, "0.25000000", used)
}
