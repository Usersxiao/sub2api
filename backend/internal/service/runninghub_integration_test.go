//go:build integration

package service

// Integration tests for the RunningHub pool, credit ledger and settlement.
// They need a PostgreSQL DSN in RH_TEST_DSN; every run creates a throwaway
// schema, applies the RunningHub migrations into it and drops it afterwards,
// so the target database's own data is never touched. RunningHub itself is
// replaced by an in-process fake.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// ── fake RunningHub ───────────────────────────────────────────────────────────

type fakeRH struct {
	mu        sync.Mutex
	full      map[string]bool   // apiKey -> answers run with 421
	status    map[string]string // upstream taskId -> QUEUED/RUNNING/SUCCESS/FAILED
	outputs   map[string]string // upstream taskId -> outputs data JSON
	runs      []fakeRun
	uploads   []string // "apiKey:filename"
	cancelled []string
	seq       int
}

type fakeRun struct {
	Key   string
	Nodes []rhNode
	Inst  string
}

func newFakeRH() *fakeRH {
	return &fakeRH{full: map[string]bool{}, status: map[string]string{}, outputs: map[string]string{}}
}

func (f *fakeRH) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	reply := func(code int, data any) (*http.Response, error) {
		b, _ := json.Marshal(map[string]any{"code": code, "msg": "x", "data": data})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}, nil
	}
	if r.URL.Path == "/task/openapi/upload" {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			return nil, err
		}
		key := r.FormValue("apiKey")
		_, fh, err := r.FormFile("file")
		if err != nil {
			return nil, err
		}
		f.uploads = append(f.uploads, key+":"+fh.Filename)
		return reply(0, map[string]string{"fileName": "api/" + key + "-" + fh.Filename})
	}
	var body struct {
		APIKey       string   `json:"apiKey"`
		TaskID       string   `json:"taskId"`
		NodeInfoList []rhNode `json:"nodeInfoList"`
		InstanceType string   `json:"instanceType"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	switch r.URL.Path {
	case "/task/openapi/ai-app/run":
		if f.full[body.APIKey] {
			return reply(421, nil)
		}
		f.seq++
		id := fmt.Sprintf("up-%d", f.seq)
		f.status[id] = "RUNNING"
		f.runs = append(f.runs, fakeRun{Key: body.APIKey, Nodes: body.NodeInfoList, Inst: body.InstanceType})
		return reply(0, map[string]string{"taskId": id})
	case "/task/openapi/status":
		return reply(0, f.status[body.TaskID])
	case "/task/openapi/outputs":
		if o, ok := f.outputs[body.TaskID]; ok {
			return reply(0, json.RawMessage(o))
		}
		return reply(804, nil)
	case "/task/openapi/cancel":
		f.cancelled = append(f.cancelled, body.TaskID)
		return reply(0, nil)
	}
	return reply(404, nil)
}

func (f *fakeRH) finish(id, status, outputs string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status[id] = status
	if outputs != "" {
		f.outputs[id] = outputs
	}
}

// ── harness ───────────────────────────────────────────────────────────────────

type rhEnv struct {
	t   *testing.T
	db  *sql.DB
	s   *RunningHubService
	rh  *fakeRH
	ctx context.Context
}

func newRHEnv(t *testing.T) *rhEnv {
	t.Helper()
	dsn := os.Getenv("RH_TEST_DSN")
	if dsn == "" {
		t.Skip("RH_TEST_DSN not set")
	}
	ctx := context.Background()
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	schema := "rh_it_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close()
	})

	db, err := sql.Open("postgres", rhDSNWithSchema(dsn, schema))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(ctx, `CREATE TABLE users (id BIGINT PRIMARY KEY)`)
	require.NoError(t, err)
	for _, f := range []string{"243_runninghub.sql", "244_runninghub_workflows.sql", "245_runninghub_avg_running.sql"} {
		b, err := os.ReadFile("../../migrations/" + f)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(b))
		require.NoError(t, err, f)
	}

	rh := newFakeRH()
	s := NewRunningHubService(db)
	s.baseURL = "http://rh.test"
	s.httpClient = &http.Client{Transport: rh}
	s.mediaDir = t.TempDir()
	return &rhEnv{t: t, db: db, s: s, rh: rh, ctx: ctx}
}

func (e *rhEnv) user(id int64, credits float64) {
	_, err := e.db.ExecContext(e.ctx, `INSERT INTO users(id) VALUES($1)`, id)
	require.NoError(e.t, err)
	if credits > 0 {
		_, err = e.s.GrantRH(e.ctx, id, credits)
		require.NoError(e.t, err)
	}
}

func (e *rhEnv) account(name, tier string, maxConc int) int64 {
	id, err := e.s.AddAccount(e.ctx, name, tier, "key-"+name, maxConc)
	require.NoError(e.t, err)
	return id
}

func (e *rhEnv) settings(consumer, enterprise bool, yuanToCoin float64) {
	_, err := e.s.UpdateSettings(e.ctx, RunningHubSettings{ConsumerEnabled: consumer, EnterpriseEnabled: enterprise, YuanToCoin: yuanToCoin})
	require.NoError(e.t, err)
}

// workflow inserts an enabled catalog entry directly (CreateWorkflow is covered separately).
func (e *rhEnv) workflow(webappID string, estimated float64, nodes []rhNode) {
	_, err := e.db.ExecContext(e.ctx,
		`INSERT INTO runninghub_workflows(webapp_id,name,estimated_rh,node_info_list,enabled,instance_type)
		 VALUES($1,$1,$2,$3,true,'plus')`, webappID, estimated, string(mustJSON(nodes)))
	require.NoError(e.t, err)
}

func (e *rhEnv) credits(uid int64) float64 {
	c, err := e.s.Credits(e.ctx, uid)
	require.NoError(e.t, err)
	return c.RH
}

func (e *rhEnv) inFlight(accountID int64) int {
	var n int
	require.NoError(e.t, e.db.QueryRowContext(e.ctx,
		`SELECT in_flight FROM runninghub_accounts WHERE id=$1`, accountID).Scan(&n))
	return n
}

func (e *rhEnv) upstreamID(taskID string) string {
	var id string
	require.NoError(e.t, e.db.QueryRowContext(e.ctx,
		`SELECT upstream_task_id FROM runninghub_tasks WHERE id=$1`, taskID).Scan(&id))
	return id
}

func rhDSNWithSchema(dsn, schema string) string {
	if strings.Contains(dsn, "://") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		return dsn + sep + "search_path=" + schema
	}
	return dsn + " search_path=" + schema
}

var rhTestNodes = []rhNode{
	{"nodeId": "12", "fieldName": "image", "fieldValue": "default.png", "fieldType": "IMAGE"},
	{"nodeId": "20", "fieldName": "value", "fieldValue": "832"},
}

// ── settlement ────────────────────────────────────────────────────────────────

func TestRHIT_SuccessConsumerCoins(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 1000)
	acc := e.account("c1", RunningHubTierConsumer, 3)
	e.workflow("app1", 100, rhTestNodes)

	task, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{RateMultiplier: 1.5})
	require.NoError(t, err)
	require.InDelta(t, 150, task.ReservedRH, 1e-6)
	require.InDelta(t, 850, e.credits(1), 1e-6)
	require.Equal(t, 1, e.inFlight(acc))
	require.Equal(t, "plus", e.rh.runs[0].Inst)

	// Still running: poll leaves it open.
	got, err := e.s.Query(e.ctx, 1, task.ID)
	require.NoError(t, err)
	require.Equal(t, rhStatusRunning, got.Status)
	require.Nil(t, got.SettledAt)

	e.rh.finish(e.upstreamID(task.ID), "SUCCESS", `[{"fileUrl":"https://o/a.mp4","fileType":"mp4","consumeCoins":"40"}]`)
	got, err = e.s.Query(e.ctx, 1, task.ID)
	require.NoError(t, err)
	require.Equal(t, rhStatusSuccess, got.Status)
	require.NotNil(t, got.ActualRH)
	require.InDelta(t, 60, *got.ActualRH, 1e-6) // 40 coins × 1.5
	require.Len(t, got.Outputs, 1)
	require.InDelta(t, 940, e.credits(1), 1e-6)
	require.Equal(t, 0, e.inFlight(acc))

	// Re-query of a settled task must not touch the balance again.
	_, err = e.s.Query(e.ctx, 1, task.ID)
	require.NoError(t, err)
	require.InDelta(t, 940, e.credits(1), 1e-6)
}

func TestRHIT_SuccessEnterpriseMoneyUsesYuanToCoin(t *testing.T) {
	e := newRHEnv(t)
	e.settings(false, true, 10) // ¥1 = 10 coins
	e.user(1, 500)
	e.account("e1", RunningHubTierEnterprise, 5)
	e.workflow("app1", 60, rhTestNodes)

	task, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.NoError(t, err)
	e.rh.finish(e.upstreamID(task.ID), "SUCCESS", `[{"fileUrl":"u","consumeMoney":"2.421","consumeCoins":null}]`)
	got, err := e.s.Query(e.ctx, 1, task.ID)
	require.NoError(t, err)
	require.InDelta(t, 24.21, *got.ActualRH, 1e-6)
	require.InDelta(t, 500-24.21, e.credits(1), 1e-6)
}

func TestRHIT_ChargeAboveReservationGoesNegative(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 60)
	e.account("c1", RunningHubTierConsumer, 3)
	e.workflow("app1", 60, rhTestNodes)

	task, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.NoError(t, err)
	require.InDelta(t, 0, e.credits(1), 1e-6)
	e.rh.finish(e.upstreamID(task.ID), "SUCCESS", `[{"fileUrl":"u","consumeCoins":90}]`)
	_, err = e.s.Query(e.ctx, 1, task.ID)
	require.NoError(t, err)
	require.InDelta(t, -30, e.credits(1), 1e-6)
}

func TestRHIT_FailedIsFreeAndKeepsReason(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 100)
	acc := e.account("c1", RunningHubTierConsumer, 3)
	e.workflow("app1", 60, rhTestNodes)

	task, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.NoError(t, err)
	e.rh.finish(e.upstreamID(task.ID), "FAILED",
		`{"failedReason":{"exception_message":"CUDA out of memory"},"consumeMoney":"0.3"}`)
	got, err := e.s.Query(e.ctx, 1, task.ID)
	require.NoError(t, err)
	require.Equal(t, rhStatusFailed, got.Status)
	require.Equal(t, "CUDA out of memory", got.Error)
	require.InDelta(t, 0, *got.ActualRH, 1e-9)
	require.InDelta(t, 100, e.credits(1), 1e-6)
	require.Equal(t, 0, e.inFlight(acc))
}

func TestRHIT_CancelRefundsAndCancelsUpstream(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 100)
	acc := e.account("c1", RunningHubTierConsumer, 3)
	e.workflow("app1", 60, rhTestNodes)

	task, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.NoError(t, err)
	got, err := e.s.Cancel(e.ctx, 1, task.ID)
	require.NoError(t, err)
	require.Equal(t, rhStatusCancelled, got.Status)
	require.Equal(t, []string{e.upstreamID(task.ID)}, e.rh.cancelled)
	require.InDelta(t, 100, e.credits(1), 1e-6)
	require.Equal(t, 0, e.inFlight(acc))
}

func TestRHIT_TimeoutRefundsViaSettler(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 100)
	acc := e.account("c1", RunningHubTierConsumer, 3)
	e.workflow("app1", 60, rhTestNodes)

	task, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.NoError(t, err)
	_, err = e.db.ExecContext(e.ctx, `UPDATE runninghub_tasks SET created_at = NOW() - INTERVAL '7 hours' WHERE id=$1`, task.ID)
	require.NoError(t, err)

	e.s.settleOpenTasks()
	got, err := e.s.Query(e.ctx, 1, task.ID)
	require.NoError(t, err)
	require.Equal(t, rhStatusTimeout, got.Status)
	require.InDelta(t, 100, e.credits(1), 1e-6)
	require.Equal(t, 0, e.inFlight(acc))
}

func TestRHIT_ConcurrentSettleChargesOnce(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 1000)
	e.account("c1", RunningHubTierConsumer, 3)
	e.workflow("app1", 100, rhTestNodes)

	task, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.NoError(t, err)
	e.rh.finish(e.upstreamID(task.ID), "SUCCESS", `[{"fileUrl":"u","consumeCoins":30}]`)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = e.s.Query(e.ctx, 1, task.ID) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); e.s.settleOpenTasks() }()
	wg.Wait()
	require.InDelta(t, 970, e.credits(1), 1e-6)
}

// ── pool scheduling ───────────────────────────────────────────────────────────

func TestRHIT_QueueMaxedRotatesToNextAccount(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 1000)
	a1 := e.account("c1", RunningHubTierConsumer, 20)
	a2 := e.account("c2", RunningHubTierConsumer, 20)
	e.rh.full["key-c1"] = true // c1 is busy outside sub2api
	e.workflow("app1", 10, rhTestNodes)

	for i := 0; i < 3; i++ {
		_, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
		require.NoError(t, err)
	}
	for _, r := range e.rh.runs {
		require.Equal(t, "key-c2", r.Key)
	}
	require.Equal(t, 0, e.inFlight(a1), "slot taken on a full account must be released")
	require.Equal(t, 3, e.inFlight(a2))
}

func TestRHIT_RoundRobinAndOverflowToEnterprise(t *testing.T) {
	e := newRHEnv(t)
	e.settings(true, true, 1)
	e.user(1, 1000)
	c1 := e.account("c1", RunningHubTierConsumer, 2)
	c2 := e.account("c2", RunningHubTierConsumer, 2)
	ent := e.account("e1", RunningHubTierEnterprise, 10)
	e.workflow("app1", 10, rhTestNodes)

	for i := 0; i < 6; i++ {
		_, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
		require.NoError(t, err)
	}
	require.Equal(t, 2, e.inFlight(c1))
	require.Equal(t, 2, e.inFlight(c2))
	require.Equal(t, 2, e.inFlight(ent))
	// Consumer keys alternate before any overflow.
	require.Equal(t, []string{"key-c1", "key-c2", "key-c1", "key-c2", "key-e1", "key-e1"},
		[]string{e.rh.runs[0].Key, e.rh.runs[1].Key, e.rh.runs[2].Key, e.rh.runs[3].Key, e.rh.runs[4].Key, e.rh.runs[5].Key})
}

func TestRHIT_PoolBusyRefunds(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 100)
	e.account("c1", RunningHubTierConsumer, 1)
	e.workflow("app1", 30, rhTestNodes)

	_, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.NoError(t, err)
	_, err = e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.ErrorIs(t, err, ErrRunningHubPoolBusy)
	require.InDelta(t, 70, e.credits(1), 1e-6)

	// Enterprise tier disabled by default: its accounts are never used.
	e.account("e1", RunningHubTierEnterprise, 5)
	_, err = e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.ErrorIs(t, err, ErrRunningHubPoolBusy)
}

func TestRHIT_ConcurrentSubmitsRespectMaxConcurrency(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 10000)
	acc := e.account("c1", RunningHubTierConsumer, 5)
	e.workflow("app1", 1, rhTestNodes)

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, busy := 0, 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				ok++
			} else if err == ErrRunningHubPoolBusy {
				busy++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 5, ok)
	require.Equal(t, 15, busy)
	require.Equal(t, 5, e.inFlight(acc))
	require.InDelta(t, 10000-5, e.credits(1), 1e-6)
}

func TestRHIT_InsufficientCredits(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 10)
	e.account("c1", RunningHubTierConsumer, 3)
	e.workflow("app1", 60, rhTestNodes)
	_, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.ErrorIs(t, err, ErrRunningHubInsufficient)
	require.Empty(t, e.rh.runs)
	require.InDelta(t, 10, e.credits(1), 1e-6)
}

func TestRHIT_ReconcileInFlight(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 1000)
	acc := e.account("c1", RunningHubTierConsumer, 5)
	e.workflow("app1", 1, rhTestNodes)
	for i := 0; i < 2; i++ {
		_, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
		require.NoError(t, err)
	}
	_, err := e.db.ExecContext(e.ctx, `UPDATE runninghub_accounts SET in_flight=4 WHERE id=$1`, acc)
	require.NoError(t, err)
	e.s.reconcileInFlight(e.ctx)
	require.Equal(t, 2, e.inFlight(acc))
}

// ── media ─────────────────────────────────────────────────────────────────────

func TestRHIT_MediaUploadedPerAccountAndCached(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 1000)
	e.account("c1", RunningHubTierConsumer, 20)
	e.account("c2", RunningHubTierConsumer, 20)
	e.workflow("app1", 1, rhTestNodes)

	m, err := e.s.StageMedia(e.ctx, 1, false, strings.NewReader("png-bytes"), "face.png", "image/png")
	require.NoError(t, err)
	require.Equal(t, "image", m.FileType)
	req := RunningHubSubmitRequest{Nodes: []RunningHubNodeOverride{{NodeID: "12", FieldName: "image", FieldValue: m.Ref}}}

	for i := 0; i < 4; i++ { // round-robin c1,c2,c1,c2
		_, err := e.s.Submit(e.ctx, 1, "app1", req)
		require.NoError(t, err)
	}
	require.ElementsMatch(t, []string{"key-c1:face.png", "key-c2:face.png"}, e.rh.uploads, "one upload per account")
	for _, r := range e.rh.runs {
		require.Equal(t, "api/"+r.Key+"-face.png", r.Nodes[0]["fieldValue"], "file name must belong to the running account")
	}
}

func TestRHIT_MediaReuploadedWhenFirstAccountFull(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 1000)
	e.account("c1", RunningHubTierConsumer, 20)
	e.account("c2", RunningHubTierConsumer, 20)
	e.rh.full["key-c1"] = true
	e.workflow("app1", 1, rhTestNodes)
	m, err := e.s.StageMedia(e.ctx, 1, false, strings.NewReader("x"), "a.wav", "")
	require.NoError(t, err)

	_, err = e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{
		Nodes: []RunningHubNodeOverride{{NodeID: "12", FieldName: "image", FieldValue: m.Ref}}})
	require.NoError(t, err)
	require.Len(t, e.rh.runs, 1)
	require.Equal(t, "api/key-c2-a.wav", e.rh.runs[0].Nodes[0]["fieldValue"])
}

func TestRHIT_MediaOwnership(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 1000)
	e.user(2, 1000)
	e.account("c1", RunningHubTierConsumer, 20)
	e.workflow("app1", 1, rhTestNodes)

	private, err := e.s.StageMedia(e.ctx, 1, false, strings.NewReader("x"), "p.png", "")
	require.NoError(t, err)
	public, err := e.s.StageMedia(e.ctx, 1, true, strings.NewReader("x"), "pub.png", "")
	require.NoError(t, err)
	use := func(ref string) RunningHubSubmitRequest {
		return RunningHubSubmitRequest{Nodes: []RunningHubNodeOverride{{NodeID: "12", FieldName: "image", FieldValue: ref}}}
	}

	// Owner warms the per-account cache; another user must still be refused.
	_, err = e.s.Submit(e.ctx, 1, "app1", use(private.Ref))
	require.NoError(t, err)
	_, err = e.s.Submit(e.ctx, 2, "app1", use(private.Ref))
	require.Error(t, err)
	require.InDelta(t, 1000, e.credits(2), 1e-6, "refused submit must refund")

	_, err = e.s.Submit(e.ctx, 2, "app1", use(public.Ref))
	require.NoError(t, err)

	_, err = e.s.Submit(e.ctx, 2, "app1", use("media:not-a-uuid"))
	require.Error(t, err)
}

// ── catalog / input validation ────────────────────────────────────────────────

func TestRHIT_SubmitValidation(t *testing.T) {
	e := newRHEnv(t)
	e.user(1, 1000)
	e.account("c1", RunningHubTierConsumer, 20)
	e.workflow("app1", 1, rhTestNodes)
	_, err := e.db.ExecContext(e.ctx, `INSERT INTO runninghub_workflows(webapp_id,name) VALUES('hidden','hidden')`)
	require.NoError(t, err)

	_, err = e.s.Submit(e.ctx, 1, "hidden", RunningHubSubmitRequest{})
	require.ErrorIs(t, err, ErrRunningHubNotFound, "disabled workflows are not runnable")

	_, err = e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{
		Nodes: []RunningHubNodeOverride{{NodeID: "99", FieldName: "evil", FieldValue: "x"}}})
	require.Error(t, err)
	require.Empty(t, e.rh.runs)
	require.InDelta(t, 1000, e.credits(1), 1e-6)

	// Other users can't see or cancel the task.
	e.user(2, 0)
	task, err := e.s.Submit(e.ctx, 1, "app1", RunningHubSubmitRequest{})
	require.NoError(t, err)
	_, err = e.s.Query(e.ctx, 2, task.ID)
	require.ErrorIs(t, err, ErrRunningHubNotFound)
	_, err = e.s.Cancel(e.ctx, 2, task.ID)
	require.ErrorIs(t, err, ErrRunningHubNotFound)
	list, err := e.s.ListTasks(e.ctx, 2, 10)
	require.NoError(t, err)
	require.Empty(t, list)
}

func TestRHIT_WorkflowCreateUpdateRefresh(t *testing.T) {
	e := newRHEnv(t)
	e.account("c1", RunningHubTierConsumer, 3)
	demo := `{"code":0,"msg":"ok","data":{"nodeInfoList":[{"nodeId":"12","fieldName":"image","fieldValue":"d.png"},{"nodeId":"20","fieldName":"value","fieldValue":"832"}]}}`
	detail := `{"code":0,"msg":"ok","data":{"name":"InfiniteTalk","instanceType":"plus","avgRunningSeconds":1403}}`
	e.s.httpClient = stubRH(func(r *http.Request) string {
		if r.URL.Path == "/api/webapp/detail" {
			return detail
		}
		return demo
	})

	w, err := e.s.CreateWorkflow(e.ctx, RunningHubWorkflowCreate{WebappID: " app9 "})
	require.NoError(t, err)
	require.Equal(t, "app9", w.WebappID)
	require.False(t, w.Enabled, "new workflows start hidden")
	require.Equal(t, "InfiniteTalk", w.Name)
	require.Equal(t, "plus", w.InstanceType)
	require.Equal(t, 1403, w.AvgRunningSeconds)
	require.InDelta(t, 1403*0.4, w.EstimatedRH, 1e-6, "plus tier bills 0.4 coins/second")
	require.Len(t, w.NodeInfoList, 2)

	// Detail endpoint failing must not block adding; falls back to defaults.
	detail = `{"code":500,"msg":"boom","data":null}`
	w2, err := e.s.CreateWorkflow(e.ctx, RunningHubWorkflowCreate{WebappID: "app10"})
	require.NoError(t, err)
	require.Equal(t, "app10", w2.Name)
	require.InDelta(t, 60, w2.EstimatedRH, 1e-9)
	require.NoError(t, e.s.DeleteWorkflow(e.ctx, w2.ID))

	on := true
	w, err = e.s.UpdateWorkflow(e.ctx, w.ID, RunningHubWorkflowUpdate{Enabled: &on,
		NodeInfoList: &[]rhNode{{"nodeId": "20", "fieldName": "value", "fieldValue": "1280"}}})
	require.NoError(t, err)
	require.True(t, w.Enabled)
	require.Equal(t, "1280", w.NodeInfoList[1]["fieldValue"])
	require.Equal(t, "d.png", w.NodeInfoList[0]["fieldValue"])

	_, err = e.s.UpdateWorkflow(e.ctx, w.ID, RunningHubWorkflowUpdate{
		NodeInfoList: &[]rhNode{{"nodeId": "77", "fieldName": "x", "fieldValue": 1}}})
	require.Error(t, err)

	// Upstream adds a node; refresh overwrites the admin default for node 20.
	demo = `{"code":0,"msg":"ok","data":{"nodeInfoList":[{"nodeId":"12","fieldName":"image","fieldValue":"d.png"},{"nodeId":"20","fieldName":"value","fieldValue":"832"},{"nodeId":"30","fieldName":"text","fieldValue":"hi"}]}}`
	w, err = e.s.RefreshWorkflowNodes(e.ctx, w.ID)
	require.NoError(t, err)
	require.Len(t, w.NodeInfoList, 3)
	require.Equal(t, "832", w.NodeInfoList[1]["fieldValue"])
	require.Equal(t, "hi", w.NodeInfoList[2]["fieldValue"])

	// Upstream removes a node; it disappears from the catalog too.
	demo = `{"code":0,"msg":"ok","data":{"nodeInfoList":[{"nodeId":"20","fieldName":"value","fieldValue":"832"}]}}`
	w, err = e.s.RefreshWorkflowNodes(e.ctx, w.ID)
	require.NoError(t, err)
	require.Len(t, w.NodeInfoList, 1)
	require.Equal(t, "832", w.NodeInfoList[0]["fieldValue"])

	list, err := e.s.ListWorkflows(e.ctx, true)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.NoError(t, e.s.DeleteWorkflow(e.ctx, w.ID))
	require.ErrorIs(t, e.s.DeleteWorkflow(e.ctx, w.ID), ErrRunningHubNotFound)
}
