package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// RunningHubWorkflow is a webapp the admin has added to the catalog.
type RunningHubWorkflow struct {
	ID                int64    `json:"id"`
	WebappID          string   `json:"webapp_id"`
	Name              string   `json:"name"`
	Description       string   `json:"description"`
	CoverURL          string   `json:"cover_url"`
	InstanceType      string   `json:"instance_type"`
	EstimatedRH       float64  `json:"estimated_rh"`
	AvgRunningSeconds int      `json:"avg_running_seconds"`
	NodeInfoList      []rhNode `json:"node_info_list"`
	Enabled           bool     `json:"enabled"`
	SortOrder         int      `json:"sort_order"`
	Category          string   `json:"category"`
}

type RunningHubWorkflowCreate struct {
	WebappID     string  `json:"webapp_id"`
	Name         string  `json:"name"`
	Description  string  `json:"description"`
	InstanceType string  `json:"instance_type"`
	EstimatedRH  float64 `json:"estimated_rh"`
	Category     string  `json:"category"`
}

type RunningHubWorkflowUpdate struct {
	Name         *string   `json:"name"`
	Description  *string   `json:"description"`
	CoverURL     *string   `json:"cover_url"`
	InstanceType *string   `json:"instance_type"`
	EstimatedRH  *float64  `json:"estimated_rh"`
	Enabled      *bool     `json:"enabled"`
	SortOrder    *int      `json:"sort_order"`
	NodeInfoList *[]rhNode `json:"node_info_list"`
	Category     *string   `json:"category"`
}

// RunningHubNodeOverride is a client-side value for one node field.
type RunningHubNodeOverride struct {
	NodeID     string `json:"nodeId"`
	FieldName  string `json:"fieldName"`
	FieldValue any    `json:"fieldValue"`
}

const rhWorkflowCols = `id, webapp_id, name, description, cover_url, instance_type, estimated_rh, avg_running_seconds, node_info_list, enabled, sort_order, COALESCE(category,'')`

func scanRHWorkflow(row interface{ Scan(...any) error }) (RunningHubWorkflow, error) {
	var w RunningHubWorkflow
	var nodes []byte
	err := row.Scan(&w.ID, &w.WebappID, &w.Name, &w.Description, &w.CoverURL, &w.InstanceType, &w.EstimatedRH, &w.AvgRunningSeconds, &nodes, &w.Enabled, &w.SortOrder, &w.Category)
	if err != nil {
		return w, err
	}
	if err := json.Unmarshal(nodes, &w.NodeInfoList); err != nil || w.NodeInfoList == nil {
		w.NodeInfoList = []rhNode{}
	}
	return w, nil
}

func (s *RunningHubService) ListWorkflows(ctx context.Context, onlyEnabled bool) ([]RunningHubWorkflow, error) {
	if s.db == nil {
		return nil, ErrRunningHubNoDB
	}
	q := `SELECT ` + rhWorkflowCols + ` FROM runninghub_workflows`
	if onlyEnabled {
		q += ` WHERE enabled`
	}
	rows, err := s.db.QueryContext(ctx, q+` ORDER BY sort_order, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunningHubWorkflow{}
	for rows.Next() {
		w, err := scanRHWorkflow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *RunningHubService) GetWorkflow(ctx context.Context, id int64) (RunningHubWorkflow, error) {
	if s.db == nil {
		return RunningHubWorkflow{}, ErrRunningHubNoDB
	}
	w, err := scanRHWorkflow(s.db.QueryRowContext(ctx,
		`SELECT `+rhWorkflowCols+` FROM runninghub_workflows WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return w, ErrRunningHubNotFound
	}
	return w, err
}

// rhCoinsPerSecond is RunningHub's published per-second coin rate for each
// instance tier (see the official "计费说明" panel).  The official UI computes
// its "预估 ≈ N RH币" the same way: avgRunningSeconds × rate for the selected
// instance — there is no estimate field in the API response.
var rhCoinsPerSecond = map[string]float64{
	"lite":     0.02,
	"standard": 0.2,
	"plus":     0.4,
	"ultra":    0.6,
}

// rhDefaultInstance is the tier RunningHub selects when a webapp does not pin one.
const rhDefaultInstance = "standard"

// rhEstimateCoins mirrors the official estimate: average runtime × tier rate.
// Returns 0 when the runtime is unknown so the caller can fall back.
func rhEstimateCoins(avgSeconds int, instanceType string) float64 {
	if avgSeconds <= 0 {
		return 0
	}
	tier := strings.ToLower(strings.TrimSpace(instanceType))
	rate, ok := rhCoinsPerSecond[tier]
	if !ok {
		rate = rhCoinsPerSecond[rhDefaultInstance]
	}
	return float64(avgSeconds) * rate
}

// CreateWorkflow adds a webapp to the catalog, pulling its node list and app
// detail from RunningHub.  avg_running_seconds and estimated_rh are populated
// automatically from the detail endpoint; the admin can edit them afterwards.
// New workflows start disabled so the admin can review defaults before exposing them.
func (s *RunningHubService) CreateWorkflow(ctx context.Context, in RunningHubWorkflowCreate) (RunningHubWorkflow, error) {
	if s.db == nil {
		return RunningHubWorkflow{}, ErrRunningHubNoDB
	}
	in.WebappID = strings.TrimSpace(in.WebappID)
	if in.WebappID == "" {
		return RunningHubWorkflow{}, errors.New("webapp_id is required")
	}

	a, err := s.anyAccount(ctx)
	if err != nil {
		return RunningHubWorkflow{}, err
	}

	// Fetch detail and nodes in parallel.
	type detailResult struct {
		d   rhAppDetail
		err error
	}
	type nodesResult struct {
		n   []rhNode
		err error
	}
	detailCh := make(chan detailResult, 1)
	nodesCh := make(chan nodesResult, 1)
	go func() {
		d, err := s.fetchAppDetail(ctx, a.APIKey, in.WebappID)
		detailCh <- detailResult{d, err}
	}()
	go func() {
		n, err := s.fetchAppNodes(ctx, a.APIKey, in.WebappID)
		nodesCh <- nodesResult{n, err}
	}()
	dr := <-detailCh
	nr := <-nodesCh
	if nr.err != nil {
		return RunningHubWorkflow{}, fmt.Errorf("fetch webapp nodes: %w", nr.err)
	}
	// detail is not part of the documented OpenAPI; treat it as best-effort.
	detail, nodes := dr.d, nr.n
	if dr.err != nil {
		detail = rhAppDetail{}
	}

	// Prefer caller-supplied values; fall back to what the detail endpoint returned.
	if strings.TrimSpace(in.Name) == "" {
		if detail.Name != "" {
			in.Name = detail.Name
		} else {
			in.Name = in.WebappID
		}
	}
	if in.Description == "" {
		in.Description = detail.Description
	}
	if strings.TrimSpace(in.InstanceType) == "" && detail.InstanceType != "" {
		in.InstanceType = detail.InstanceType
	}
	coverURL := detail.coverURL()

	avgSec := detail.AvgRunningSeconds
	if in.EstimatedRH <= 0 {
		if est := rhEstimateCoins(avgSec, in.InstanceType); est > 0 {
			in.EstimatedRH = est
		} else {
			in.EstimatedRH = 60
		}
	}

	return scanRHWorkflow(s.db.QueryRowContext(ctx,
		`INSERT INTO runninghub_workflows
		    (webapp_id, name, description, cover_url, instance_type, estimated_rh, avg_running_seconds, node_info_list)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING `+rhWorkflowCols,
		in.WebappID, strings.TrimSpace(in.Name), in.Description, coverURL, in.InstanceType,
		in.EstimatedRH, avgSec, mustRHJSON(nodes)))
}

// RefreshWorkflowNodes re-pulls the node list from RunningHub and replaces the
// stored one wholesale, discarding any admin-edited defaults.
func (s *RunningHubService) RefreshWorkflowNodes(ctx context.Context, id int64) (RunningHubWorkflow, error) {
	w, err := s.GetWorkflow(ctx, id)
	if err != nil {
		return w, err
	}
	a, err := s.anyAccount(ctx)
	if err != nil {
		return w, err
	}
	fresh, err := s.fetchAppNodes(ctx, a.APIKey, w.WebappID)
	if err != nil {
		return w, fmt.Errorf("fetch webapp nodes: %w", err)
	}
	// Best-effort: keep the runtime and cover URL current.
	// The estimate itself stays as the admin set it.
	avgSec := w.AvgRunningSeconds
	coverURL := w.CoverURL
	if d, derr := s.fetchAppDetail(ctx, a.APIKey, w.WebappID); derr == nil {
		if d.AvgRunningSeconds > 0 {
			avgSec = d.AvgRunningSeconds
		}
		if d.coverURL() != "" {
			coverURL = d.coverURL()
		}
	}
	// Write the fresh structure directly: UpdateWorkflow only edits values of
	// existing nodes and would reject nodes newly added upstream.
	w, err = scanRHWorkflow(s.db.QueryRowContext(ctx,
		`UPDATE runninghub_workflows
		    SET node_info_list=$2::jsonb, avg_running_seconds=$3, cover_url=$4, updated_at=NOW()
		 WHERE id=$1 RETURNING `+rhWorkflowCols, id, string(mustRHJSON(fresh)), avgSec, coverURL))
	if errors.Is(err, sql.ErrNoRows) {
		return w, ErrRunningHubNotFound
	}
	return w, err
}

func (s *RunningHubService) UpdateWorkflow(ctx context.Context, id int64, u RunningHubWorkflowUpdate) (RunningHubWorkflow, error) {
	if s.db == nil {
		return RunningHubWorkflow{}, ErrRunningHubNoDB
	}
	if u.EstimatedRH != nil && *u.EstimatedRH <= 0 {
		return RunningHubWorkflow{}, errors.New("estimated_rh must be positive")
	}
	var nodes []byte
	if u.NodeInfoList != nil {
		cur, err := s.GetWorkflow(ctx, id)
		if err != nil {
			return cur, err
		}
		// Only fieldValue may change; the node structure comes from RunningHub.
		merged, err := mergeRHNodes(cur.NodeInfoList, overridesFromNodes(*u.NodeInfoList))
		if err != nil {
			return cur, err
		}
		nodes = mustRHJSON(merged)
	}
	w, err := scanRHWorkflow(s.db.QueryRowContext(ctx,
		`UPDATE runninghub_workflows SET
		   name           = COALESCE($2, name),
		   description    = COALESCE($3, description),
		   cover_url      = COALESCE($4, cover_url),
		   instance_type  = COALESCE($5, instance_type),
		   estimated_rh   = COALESCE($6, estimated_rh),
		   enabled        = COALESCE($7, enabled),
		   sort_order     = COALESCE($8, sort_order),
		   node_info_list = COALESCE($9::jsonb, node_info_list),
		   category       = COALESCE($10, category),
		   updated_at     = NOW()
		 WHERE id=$1 RETURNING `+rhWorkflowCols,
		id, u.Name, u.Description, u.CoverURL, u.InstanceType, u.EstimatedRH, u.Enabled, u.SortOrder, nullableJSON(nodes), u.Category))
	if errors.Is(err, sql.ErrNoRows) {
		return w, ErrRunningHubNotFound
	}
	return w, err
}

func (s *RunningHubService) DeleteWorkflow(ctx context.Context, id int64) error {
	if s.db == nil {
		return ErrRunningHubNoDB
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM runninghub_workflows WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrRunningHubNotFound
	}
	return nil
}

// ── node helpers ──────────────────────────────────────────────────────────────

func rhNodeKey(n rhNode) string {
	return fmt.Sprint(n["nodeId"]) + "\x00" + fmt.Sprint(n["fieldName"])
}

func overridesFromNodes(nodes []rhNode) []RunningHubNodeOverride {
	out := make([]RunningHubNodeOverride, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, RunningHubNodeOverride{
			NodeID: fmt.Sprint(n["nodeId"]), FieldName: fmt.Sprint(n["fieldName"]), FieldValue: n["fieldValue"],
		})
	}
	return out
}

// mergeRHNodes returns a deep copy of base with fieldValue replaced by overrides.
// Overrides for fields not present in base are rejected so clients can't inject nodes.
func mergeRHNodes(base []rhNode, overrides []RunningHubNodeOverride) ([]rhNode, error) {
	out := make([]rhNode, len(base))
	idx := make(map[string]int, len(base))
	for i, n := range base {
		cp := make(rhNode, len(n))
		for k, v := range n {
			cp[k] = v
		}
		out[i] = cp
		idx[rhNodeKey(n)] = i
	}
	for _, o := range overrides {
		i, ok := idx[o.NodeID+"\x00"+o.FieldName]
		if !ok {
			return nil, fmt.Errorf("unknown node field %s/%s", o.NodeID, o.FieldName)
		}
		out[i]["fieldValue"] = o.FieldValue
	}
	return out, nil
}

func nullableJSON(b []byte) any {
	if b == nil {
		return nil
	}
	return string(b)
}

func mustRHJSON(v any) []byte { b, _ := json.Marshal(v); return b }
