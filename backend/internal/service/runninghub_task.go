package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	rhStatusQueued    = "queued"
	rhStatusRunning   = "running"
	rhStatusSuccess   = "success"
	rhStatusFailed    = "failed"
	rhStatusCancelled = "cancelled"
	rhStatusTimeout   = "timeout"

	// Tasks still unfinished after this long are abandoned and refunded.
	rhTaskTimeout   = 6 * time.Hour
	rhSettleEvery   = 20 * time.Second
	rhSettleBatch   = 50
	rhMaxPoolTrials = 16
)

type RunningHubSubmitRequest struct {
	Nodes []RunningHubNodeOverride `json:"node_info_list"`
	// Set by the handler from the caller's group rate; never from the client.
	RateMultiplier float64 `json:"-"`
}

type RunningHubTask struct {
	ID             string                 `json:"id"`
	AppID          string                 `json:"app_id"`
	WorkflowName   string                 `json:"workflow_name,omitempty"`
	Status         string                 `json:"status"`
	ReservedRH     float64                `json:"reserved_rh"`
	ActualRH       *float64               `json:"actual_rh,omitempty"`
	Outputs        []RunningHubOutputFile `json:"outputs,omitempty"`
	Error          string                 `json:"error,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	SettledAt      *time.Time             `json:"settled_at,omitempty"`
	upstreamTaskID string
	accountID      int64
	apiKey         string
	userID         int64
	rateMultiplier float64
}

func rhIsTerminal(st string) bool {
	switch st {
	case rhStatusSuccess, rhStatusFailed, rhStatusCancelled, rhStatusTimeout:
		return true
	}
	return false
}

// ── Submit ────────────────────────────────────────────────────────────────────

// Submit runs an enabled catalog workflow for the user. Credits for the estimate
// are held up front and trued-up when the task settles.
func (s *RunningHubService) Submit(ctx context.Context, userID int64, webappID string, req RunningHubSubmitRequest) (RunningHubTask, error) {
	if s.db == nil {
		return RunningHubTask{}, ErrRunningHubNoDB
	}
	w, err := scanRHWorkflow(s.db.QueryRowContext(ctx,
		`SELECT `+rhWorkflowCols+` FROM runninghub_workflows WHERE webapp_id=$1 AND enabled`, webappID))
	if errors.Is(err, sql.ErrNoRows) {
		return RunningHubTask{}, ErrRunningHubNotFound
	}
	if err != nil {
		return RunningHubTask{}, err
	}
	nodes, err := mergeRHNodes(w.NodeInfoList, req.Nodes)
	if err != nil {
		return RunningHubTask{}, err
	}
	mult := req.RateMultiplier
	if mult <= 0 {
		mult = 1
	}
	reserved := w.EstimatedRH * mult

	if err := reserveRH(ctx, s.db, userID, reserved); err != nil {
		return RunningHubTask{}, err
	}
	refund := func() { _ = adjustRH(context.WithoutCancel(ctx), s.db, userID, reserved) }

	a, upID, err := s.dispatch(ctx, userID, w, nodes)
	if err != nil {
		refund()
		return RunningHubTask{}, err
	}

	t := RunningHubTask{
		ID: uuid.New().String(), AppID: w.WebappID, Status: rhStatusQueued,
		ReservedRH: reserved, CreatedAt: time.Now(),
	}
	_, err = s.db.ExecContext(context.WithoutCancel(ctx),
		`INSERT INTO runninghub_tasks(id,user_id,account_id,upstream_task_id,app_id,reserved_rh,rate_multiplier,status)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
		t.ID, userID, a.ID, upID, w.WebappID, reserved, mult, t.Status)
	if err != nil {
		// The upstream task is already running but we can't track it; best-effort cancel.
		_, _ = s.upstreamPost(context.WithoutCancel(ctx), "/task/openapi/cancel", a.APIKey,
			map[string]any{"apiKey": a.APIKey, "taskId": upID})
		s.releaseAccount(ctx, a.ID)
		refund()
		return RunningHubTask{}, err
	}
	return t, nil
}

// dispatch submits to the pool, rotating to the next account whenever one
// reports TASK_QUEUE_MAXED. On success the account slot stays reserved until
// the task settles.
func (s *RunningHubService) dispatch(ctx context.Context, userID int64, w RunningHubWorkflow, nodes []rhNode) (rhAccountRow, string, error) {
	var tried []int64
	for i := 0; i < rhMaxPoolTrials; i++ {
		a, err := s.pickAccount(ctx, tried)
		if err != nil {
			return a, "", err
		}
		tried = append(tried, a.ID)

		// Each attempt needs its own copy: media refs resolve to account-specific names.
		attempt, _ := mergeRHNodes(nodes, nil)
		if err := s.resolveMediaRefs(ctx, userID, a, attempt); err != nil {
			s.releaseAccount(ctx, a.ID)
			if errors.Is(err, errRHQueueMaxed) {
				continue
			}
			return a, "", err
		}
		body := map[string]any{"apiKey": a.APIKey, "webappId": w.WebappID, "nodeInfoList": attempt}
		if w.InstanceType != "" {
			body["instanceType"] = w.InstanceType
		}
		env, err := s.upstreamPost(ctx, "/task/openapi/ai-app/run", a.APIKey, body)
		if errors.Is(err, errRHQueueMaxed) {
			// Our in_flight count can lag reality (e.g. tasks started outside sub2api);
			// move on to the next account immediately.
			s.releaseAccount(ctx, a.ID)
			continue
		}
		if err != nil {
			s.releaseAccount(ctx, a.ID)
			return a, "", err
		}
		var data struct {
			TaskID string `json:"taskId"`
		}
		_ = json.Unmarshal(env.Data, &data)
		if data.TaskID == "" {
			s.releaseAccount(ctx, a.ID)
			return a, "", errors.New("RunningHub returned no taskId")
		}
		return a, data.TaskID, nil
	}
	return rhAccountRow{}, "", ErrRunningHubPoolBusy
}

// ── Query / list ──────────────────────────────────────────────────────────────

const rhTaskSelect = `SELECT t.id, t.user_id, t.app_id, t.status, t.reserved_rh, t.actual_rh, t.rate_multiplier,
       t.outputs_json, COALESCE(t.error_message,''), t.created_at, t.settled_at,
       t.upstream_task_id, a.id, a.api_key, COALESCE(w.name,'')
  FROM runninghub_tasks t JOIN runninghub_accounts a ON a.id = t.account_id
  LEFT JOIN runninghub_workflows w ON w.webapp_id = t.app_id`

func scanRHTask(row interface{ Scan(...any) error }) (RunningHubTask, error) {
	var t RunningHubTask
	var outputs []byte
	err := row.Scan(&t.ID, &t.userID, &t.AppID, &t.Status, &t.ReservedRH, &t.ActualRH, &t.rateMultiplier,
		&outputs, &t.Error, &t.CreatedAt, &t.SettledAt, &t.upstreamTaskID, &t.accountID, &t.apiKey, &t.WorkflowName)
	if err == nil && len(outputs) > 0 {
		t.Outputs = parseRHOutputs(outputs).Files
	}
	return t, err
}

// Query returns the task, polling RunningHub and settling it if still open.
func (s *RunningHubService) Query(ctx context.Context, userID int64, id string) (RunningHubTask, error) {
	if s.db == nil {
		return RunningHubTask{}, ErrRunningHubNoDB
	}
	if _, err := uuid.Parse(id); err != nil {
		return RunningHubTask{}, ErrRunningHubNotFound
	}
	t, err := scanRHTask(s.db.QueryRowContext(ctx, rhTaskSelect+` WHERE t.id=$1 AND t.user_id=$2`, id, userID))
	if errors.Is(err, sql.ErrNoRows) {
		return t, ErrRunningHubNotFound
	}
	if err != nil || t.SettledAt != nil {
		return t, err
	}
	if err := s.advance(ctx, &t); err != nil {
		// Upstream hiccups shouldn't fail the poll; report the last known state.
		log.Printf("[runninghub] poll task %s: %v", t.ID, err)
	}
	return t, nil
}

func (s *RunningHubService) ListTasks(ctx context.Context, userID int64, limit int) ([]RunningHubTask, error) {
	if s.db == nil {
		return nil, ErrRunningHubNoDB
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, rhTaskSelect+` WHERE t.user_id=$1 ORDER BY t.created_at DESC LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunningHubTask{}
	for rows.Next() {
		t, err := scanRHTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ── Cancel ────────────────────────────────────────────────────────────────────

func (s *RunningHubService) Cancel(ctx context.Context, userID int64, id string) (RunningHubTask, error) {
	if s.db == nil {
		return RunningHubTask{}, ErrRunningHubNoDB
	}
	if _, err := uuid.Parse(id); err != nil {
		return RunningHubTask{}, ErrRunningHubNotFound
	}
	t, err := scanRHTask(s.db.QueryRowContext(ctx, rhTaskSelect+` WHERE t.id=$1 AND t.user_id=$2`, id, userID))
	if errors.Is(err, sql.ErrNoRows) {
		return t, ErrRunningHubNotFound
	}
	if err != nil || t.SettledAt != nil {
		return t, err
	}
	if _, err := s.upstreamPost(ctx, "/task/openapi/cancel", t.apiKey,
		map[string]any{"apiKey": t.apiKey, "taskId": t.upstreamTaskID}); err != nil {
		return t, err
	}
	zero := 0.0
	return t, s.settle(ctx, &t, rhStatusCancelled, &zero, rhOutputs{}, "cancelled by user")
}

// ── Settlement ────────────────────────────────────────────────────────────────

// advance polls upstream status and settles the task if it reached a terminal state.
func (s *RunningHubService) advance(ctx context.Context, t *RunningHubTask) error {
	if time.Since(t.CreatedAt) > rhTaskTimeout {
		zero := 0.0
		return s.settle(ctx, t, rhStatusTimeout, &zero, rhOutputs{}, "task timed out")
	}
	env, err := s.upstreamPost(ctx, "/task/openapi/status", t.apiKey,
		map[string]any{"apiKey": t.apiKey, "taskId": t.upstreamTaskID})
	if err != nil {
		return err
	}
	var upStatus string
	_ = json.Unmarshal(env.Data, &upStatus)

	switch strings.ToUpper(upStatus) {
	case "QUEUED", "RUNNING":
		st := strings.ToLower(upStatus)
		if st != t.Status {
			t.Status = st
			_, _ = s.db.ExecContext(ctx,
				`UPDATE runninghub_tasks SET status=$1, updated_at=NOW() WHERE id=$2 AND settled_at IS NULL`, st, t.ID)
		}
		return nil
	case "SUCCESS":
		oenv, err := s.upstreamPost(ctx, "/task/openapi/outputs", t.apiKey,
			map[string]any{"apiKey": t.apiKey, "taskId": t.upstreamTaskID})
		if err != nil {
			return err // outputs not ready yet; retry on next poll
		}
		out := parseRHOutputs(oenv.Data)
		cost, err := s.costInCoins(ctx, out)
		if err != nil {
			return err
		}
		var charge *float64
		if cost != nil {
			c := *cost * t.rateMultiplier
			charge = &c
		}
		return s.settle(ctx, t, rhStatusSuccess, charge, out, "")
	case "FAILED":
		// Fetch outputs for the failure reason and any cost RunningHub reports.
		var out rhOutputs
		reason := "task failed"
		if oenv, err := s.upstreamPost(ctx, "/task/openapi/outputs", t.apiKey,
			map[string]any{"apiKey": t.apiKey, "taskId": t.upstreamTaskID}); err == nil {
			out = parseRHOutputs(oenv.Data)
			if r := rhFailedReason(oenv.Data); r != "" {
				reason = r
			}
		} else {
			var ue *RunningHubUpstreamError
			if errors.As(err, &ue) && ue.Msg != "" {
				reason = ue.Msg
			}
		}
		// Failed tasks are not charged to the user even if upstream bills us.
		zero := 0.0
		return s.settle(ctx, t, rhStatusFailed, &zero, out, reason)
	default:
		return fmt.Errorf("unexpected RunningHub status %q", upStatus)
	}
}

// costInCoins converts the upstream cost to RH coins: consumer keys report
// consumeCoins directly, enterprise keys report consumeMoney in yuan.
func (s *RunningHubService) costInCoins(ctx context.Context, out rhOutputs) (*float64, error) {
	if out.Coins != nil {
		return out.Coins, nil
	}
	if out.Money != nil {
		st, err := s.GetSettings(ctx)
		if err != nil {
			return nil, err
		}
		c := *out.Money * st.YuanToCoin
		return &c, nil
	}
	return nil, nil
}

func rhFailedReason(data json.RawMessage) string {
	var obj map[string]any
	if json.Unmarshal(data, &obj) != nil {
		return ""
	}
	if fr, ok := obj["failedReason"].(map[string]any); ok {
		for _, k := range []string{"exception_message", "exception_type", "node_name"} {
			if v, _ := fr[k].(string); v != "" {
				return v
			}
		}
	}
	return ""
}

// settle finalises a task exactly once: charge is the amount to bill (nil means
// "unknown, keep the reservation"), the difference from the reservation is
// refunded or additionally deducted, and the account slot is released.
func (s *RunningHubService) settle(ctx context.Context, t *RunningHubTask, status string, charge *float64, out rhOutputs, errMsg string) error {
	ctx = context.WithoutCancel(ctx)
	actual := t.ReservedRH
	if charge != nil {
		actual = *charge
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var outputs any
	if len(out.Raw) > 0 {
		outputs = string(out.Raw)
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE runninghub_tasks SET status=$2, actual_rh=$3, consume_coins=$4, consume_money=$5,
		     outputs_json=$6::jsonb, error_message=NULLIF($7,''), settled_at=NOW(), updated_at=NOW()
		 WHERE id=$1 AND settled_at IS NULL`,
		t.ID, status, actual, out.Coins, out.Money, outputs, errMsg)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil // settled concurrently
	}
	if err := adjustRH(ctx, tx, t.userID, t.ReservedRH-actual); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.releaseAccount(ctx, t.accountID)

	now := time.Now()
	t.Status, t.ActualRH, t.Outputs, t.Error, t.SettledAt = status, &actual, out.Files, errMsg, &now
	return nil
}

// runSettler polls open tasks in the background so credits settle and account
// slots free up even when clients stop polling.
func (s *RunningHubService) runSettler() {
	tick := time.NewTicker(rhSettleEvery)
	defer tick.Stop()
	// Periodically reconcile in_flight to fix drift caused by crashes or missed releases.
	reconcileTick := time.NewTicker(5 * time.Minute)
	defer reconcileTick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-reconcileTick.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			s.reconcileInFlight(ctx)
			cancel()
		case <-tick.C:
			s.settleOpenTasks()
		}
	}
}

func (s *RunningHubService) settleOpenTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), rhSettleEvery*3)
	defer cancel()
	rows, err := s.db.QueryContext(ctx,
		rhTaskSelect+` WHERE t.settled_at IS NULL ORDER BY t.created_at LIMIT $1`, rhSettleBatch)
	if err != nil {
		return
	}
	var open []RunningHubTask
	for rows.Next() {
		if t, err := scanRHTask(rows); err == nil {
			open = append(open, t)
		}
	}
	_ = rows.Close()
	for i := range open {
		if err := s.advance(ctx, &open[i]); err != nil {
			log.Printf("[runninghub] settle task %s: %v", open[i].ID, err)
		}
	}
}
